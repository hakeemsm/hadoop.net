using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>DFSInputStream provides bytes from a named file.</summary>
	/// <remarks>
	/// DFSInputStream provides bytes from a named file.  It handles
	/// negotiation of the namenode and various datanodes as necessary.
	/// </remarks>
	public class DFSInputStream : FSInputStream, ByteBufferReadable, CanSetDropBehind
		, CanSetReadahead, HasEnhancedByteBufferAccess, CanUnbuffer
	{
		[VisibleForTesting]
		public static bool tcpReadsDisabledForTesting = false;

		private long hedgedReadOpsLoopNumForTesting = 0;

		private readonly DFSClient dfsClient;

		private AtomicBoolean closed = new AtomicBoolean(false);

		private readonly string src;

		private readonly bool verifyChecksum;

		private DatanodeInfo currentNode = null;

		private LocatedBlock currentLocatedBlock = null;

		private long pos = 0;

		private long blockEnd = -1;

		private BlockReader blockReader = null;

		private LocatedBlocks locatedBlocks = null;

		private long lastBlockBeingWrittenLength = 0;

		private FileEncryptionInfo fileEncryptionInfo = null;

		private CachingStrategy cachingStrategy;

		private readonly DFSInputStream.ReadStatistics readStatistics = new DFSInputStream.ReadStatistics
			();

		private readonly object infoLock = new object();

		/// <summary>Track the ByteBuffers that we have handed out to readers.</summary>
		/// <remarks>
		/// Track the ByteBuffers that we have handed out to readers.
		/// The value type can be either ByteBufferPool or ClientMmap, depending on
		/// whether we this is a memory-mapped buffer or not.
		/// </remarks>
		private IdentityHashStore<ByteBuffer, object> extendedReadBuffers;

		// state by stateful read only:
		// (protected by lock on this)
		/////
		////
		// state shared by stateful and positional read:
		// (protected by lock on infoLock)
		////
		////
		// lock for state shared between read and pread
		// Note: Never acquire a lock on <this> with this lock held to avoid deadlocks
		//       (it's OK to acquire this lock when the lock on <this> is held)
		private IdentityHashStore<ByteBuffer, object> GetExtendedReadBuffers()
		{
			lock (this)
			{
				if (extendedReadBuffers == null)
				{
					extendedReadBuffers = new IdentityHashStore<ByteBuffer, object>(0);
				}
				return extendedReadBuffers;
			}
		}

		public class ReadStatistics
		{
			public ReadStatistics()
			{
				Clear();
			}

			public ReadStatistics(DFSInputStream.ReadStatistics rhs)
			{
				this.totalBytesRead = rhs.GetTotalBytesRead();
				this.totalLocalBytesRead = rhs.GetTotalLocalBytesRead();
				this.totalShortCircuitBytesRead = rhs.GetTotalShortCircuitBytesRead();
				this.totalZeroCopyBytesRead = rhs.GetTotalZeroCopyBytesRead();
			}

			/// <returns>
			/// The total bytes read.  This will always be at least as
			/// high as the other numbers, since it includes all of them.
			/// </returns>
			public virtual long GetTotalBytesRead()
			{
				return totalBytesRead;
			}

			/// <returns>
			/// The total local bytes read.  This will always be at least
			/// as high as totalShortCircuitBytesRead, since all short-circuit
			/// reads are also local.
			/// </returns>
			public virtual long GetTotalLocalBytesRead()
			{
				return totalLocalBytesRead;
			}

			/// <returns>The total short-circuit local bytes read.</returns>
			public virtual long GetTotalShortCircuitBytesRead()
			{
				return totalShortCircuitBytesRead;
			}

			/// <returns>The total number of zero-copy bytes read.</returns>
			public virtual long GetTotalZeroCopyBytesRead()
			{
				return totalZeroCopyBytesRead;
			}

			/// <returns>The total number of bytes read which were not local.</returns>
			public virtual long GetRemoteBytesRead()
			{
				return totalBytesRead - totalLocalBytesRead;
			}

			internal virtual void AddRemoteBytes(long amt)
			{
				this.totalBytesRead += amt;
			}

			internal virtual void AddLocalBytes(long amt)
			{
				this.totalBytesRead += amt;
				this.totalLocalBytesRead += amt;
			}

			internal virtual void AddShortCircuitBytes(long amt)
			{
				this.totalBytesRead += amt;
				this.totalLocalBytesRead += amt;
				this.totalShortCircuitBytesRead += amt;
			}

			internal virtual void AddZeroCopyBytes(long amt)
			{
				this.totalBytesRead += amt;
				this.totalLocalBytesRead += amt;
				this.totalShortCircuitBytesRead += amt;
				this.totalZeroCopyBytesRead += amt;
			}

			internal virtual void Clear()
			{
				this.totalBytesRead = 0;
				this.totalLocalBytesRead = 0;
				this.totalShortCircuitBytesRead = 0;
				this.totalZeroCopyBytesRead = 0;
			}

			private long totalBytesRead;

			private long totalLocalBytesRead;

			private long totalShortCircuitBytesRead;

			private long totalZeroCopyBytesRead;
		}

		/// <summary>
		/// This variable tracks the number of failures since the start of the
		/// most recent user-facing operation.
		/// </summary>
		/// <remarks>
		/// This variable tracks the number of failures since the start of the
		/// most recent user-facing operation. That is to say, it should be reset
		/// whenever the user makes a call on this stream, and if at any point
		/// during the retry logic, the failure count exceeds a threshold,
		/// the errors will be thrown back to the operation.
		/// Specifically this counts the number of times the client has gone
		/// back to the namenode to get a new list of block locations, and is
		/// capped at maxBlockAcquireFailures
		/// </remarks>
		private int failures = 0;

		private readonly ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes = new ConcurrentHashMap
			<DatanodeInfo, DatanodeInfo>();

		private byte[] oneByteBuf;

		/* XXX Use of CocurrentHashMap is temp fix. Need to fix
		* parallel accesses to DFSInputStream (through ptreads) properly */
		// used for 'int read()'
		internal virtual void AddToDeadNodes(DatanodeInfo dnInfo)
		{
			deadNodes[dnInfo] = dnInfo;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal DFSInputStream(DFSClient dfsClient, string src, bool verifyChecksum)
		{
			this.dfsClient = dfsClient;
			this.verifyChecksum = verifyChecksum;
			this.src = src;
			lock (infoLock)
			{
				this.cachingStrategy = dfsClient.GetDefaultReadCachingStrategy();
			}
			OpenInfo();
		}

		/// <summary>Grab the open-file info from namenode</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal virtual void OpenInfo()
		{
			lock (infoLock)
			{
				lastBlockBeingWrittenLength = FetchLocatedBlocksAndGetLastBlockLength();
				int retriesForLastBlockLength = dfsClient.GetConf().retryTimesForGetLastBlockLength;
				while (retriesForLastBlockLength > 0)
				{
					// Getting last block length as -1 is a special case. When cluster
					// restarts, DNs may not report immediately. At this time partial block
					// locations will not be available with NN for getting the length. Lets
					// retry for 3 times to get the length.
					if (lastBlockBeingWrittenLength == -1)
					{
						DFSClient.Log.Warn("Last block locations not available. " + "Datanodes might not have reported blocks completely."
							 + " Will retry for " + retriesForLastBlockLength + " times");
						WaitFor(dfsClient.GetConf().retryIntervalForGetLastBlockLength);
						lastBlockBeingWrittenLength = FetchLocatedBlocksAndGetLastBlockLength();
					}
					else
					{
						break;
					}
					retriesForLastBlockLength--;
				}
				if (retriesForLastBlockLength == 0)
				{
					throw new IOException("Could not obtain the last block locations.");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WaitFor(int waitTime)
		{
			try
			{
				Sharpen.Thread.Sleep(waitTime);
			}
			catch (Exception)
			{
				throw new IOException("Interrupted while getting the last block length.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private long FetchLocatedBlocksAndGetLastBlockLength()
		{
			LocatedBlocks newInfo = dfsClient.GetLocatedBlocks(src, 0);
			if (DFSClient.Log.IsDebugEnabled())
			{
				DFSClient.Log.Debug("newInfo = " + newInfo);
			}
			if (newInfo == null)
			{
				throw new IOException("Cannot open filename " + src);
			}
			if (locatedBlocks != null)
			{
				IEnumerator<LocatedBlock> oldIter = locatedBlocks.GetLocatedBlocks().GetEnumerator
					();
				IEnumerator<LocatedBlock> newIter = newInfo.GetLocatedBlocks().GetEnumerator();
				while (oldIter.HasNext() && newIter.HasNext())
				{
					if (!oldIter.Next().GetBlock().Equals(newIter.Next().GetBlock()))
					{
						throw new IOException("Blocklist for " + src + " has changed!");
					}
				}
			}
			locatedBlocks = newInfo;
			long lastBlockBeingWrittenLength = 0;
			if (!locatedBlocks.IsLastBlockComplete())
			{
				LocatedBlock last = locatedBlocks.GetLastLocatedBlock();
				if (last != null)
				{
					if (last.GetLocations().Length == 0)
					{
						if (last.GetBlockSize() == 0)
						{
							// if the length is zero, then no data has been written to
							// datanode. So no need to wait for the locations.
							return 0;
						}
						return -1;
					}
					long len = ReadBlockLength(last);
					last.GetBlock().SetNumBytes(len);
					lastBlockBeingWrittenLength = len;
				}
			}
			fileEncryptionInfo = locatedBlocks.GetFileEncryptionInfo();
			return lastBlockBeingWrittenLength;
		}

		/// <summary>Read the block length from one of the datanodes.</summary>
		/// <exception cref="System.IO.IOException"/>
		private long ReadBlockLength(LocatedBlock locatedblock)
		{
			System.Diagnostics.Debug.Assert(locatedblock != null, "LocatedBlock cannot be null"
				);
			int replicaNotFoundCount = locatedblock.GetLocations().Length;
			int timeout = dfsClient.GetConf().socketTimeout;
			List<DatanodeInfo> nodeList = new List<DatanodeInfo>(Arrays.AsList(locatedblock.GetLocations
				()));
			List<DatanodeInfo> retryList = new List<DatanodeInfo>();
			bool isRetry = false;
			StopWatch sw = new StopWatch();
			while (nodeList.Count > 0)
			{
				DatanodeInfo datanode = nodeList.Pop();
				ClientDatanodeProtocol cdp = null;
				try
				{
					cdp = DFSUtil.CreateClientDatanodeProtocolProxy(datanode, dfsClient.GetConfiguration
						(), timeout, dfsClient.GetConf().connectToDnViaHostname, locatedblock);
					long n = cdp.GetReplicaVisibleLength(locatedblock.GetBlock());
					if (n >= 0)
					{
						return n;
					}
				}
				catch (IOException ioe)
				{
					if (ioe is RemoteException)
					{
						if (((RemoteException)ioe).UnwrapRemoteException() is ReplicaNotFoundException)
						{
							// replica is not on the DN. We will treat it as 0 length
							// if no one actually has a replica.
							replicaNotFoundCount--;
						}
						else
						{
							if (((RemoteException)ioe).UnwrapRemoteException() is RetriableException)
							{
								// add to the list to be retried if necessary.
								retryList.AddItem(datanode);
							}
						}
					}
					if (DFSClient.Log.IsDebugEnabled())
					{
						DFSClient.Log.Debug("Failed to getReplicaVisibleLength from datanode " + datanode
							 + " for block " + locatedblock.GetBlock(), ioe);
					}
				}
				finally
				{
					if (cdp != null)
					{
						RPC.StopProxy(cdp);
					}
				}
				// Ran out of nodes, but there are retriable nodes.
				if (nodeList.Count == 0 && retryList.Count > 0)
				{
					Sharpen.Collections.AddAll(nodeList, retryList);
					retryList.Clear();
					isRetry = true;
				}
				if (isRetry)
				{
					// start the stop watch if not already running.
					if (!sw.IsRunning())
					{
						sw.Start();
					}
					try
					{
						Sharpen.Thread.Sleep(500);
					}
					catch (Exception)
					{
						// delay between retries.
						throw new IOException("Interrupted while getting the length.");
					}
				}
				// see if we ran out of retry time
				if (sw.IsRunning() && sw.Now(TimeUnit.Milliseconds) > timeout)
				{
					break;
				}
			}
			// Namenode told us about these locations, but none know about the replica
			// means that we hit the race between pipeline creation start and end.
			// we require all 3 because some other exception could have happened
			// on a DN that has it.  we want to report that error
			if (replicaNotFoundCount == 0)
			{
				return 0;
			}
			throw new IOException("Cannot obtain block length for " + locatedblock);
		}

		public virtual long GetFileLength()
		{
			lock (infoLock)
			{
				return locatedBlocks == null ? 0 : locatedBlocks.GetFileLength() + lastBlockBeingWrittenLength;
			}
		}

		// Short circuit local reads are forbidden for files that are
		// under construction.  See HDFS-2757.
		internal virtual bool ShortCircuitForbidden()
		{
			lock (infoLock)
			{
				return locatedBlocks.IsUnderConstruction();
			}
		}

		/// <summary>Returns the datanode from which the stream is currently reading.</summary>
		public virtual DatanodeInfo GetCurrentDatanode()
		{
			lock (this)
			{
				return currentNode;
			}
		}

		/// <summary>Returns the block containing the target position.</summary>
		public virtual ExtendedBlock GetCurrentBlock()
		{
			lock (this)
			{
				if (currentLocatedBlock == null)
				{
					return null;
				}
				return currentLocatedBlock.GetBlock();
			}
		}

		/// <summary>Return collection of blocks that has already been located.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual IList<LocatedBlock> GetAllBlocks()
		{
			return GetBlockRange(0, GetFileLength());
		}

		/// <summary>Get block at the specified position.</summary>
		/// <remarks>
		/// Get block at the specified position.
		/// Fetch it from the namenode if not cached.
		/// </remarks>
		/// <param name="offset">block corresponding to this offset in file is returned</param>
		/// <returns>located block</returns>
		/// <exception cref="System.IO.IOException"/>
		private LocatedBlock GetBlockAt(long offset)
		{
			lock (infoLock)
			{
				System.Diagnostics.Debug.Assert((locatedBlocks != null), "locatedBlocks is null");
				LocatedBlock blk;
				//check offset
				if (offset < 0 || offset >= GetFileLength())
				{
					throw new IOException("offset < 0 || offset >= getFileLength(), offset=" + offset
						 + ", locatedBlocks=" + locatedBlocks);
				}
				else
				{
					if (offset >= locatedBlocks.GetFileLength())
					{
						// offset to the portion of the last block,
						// which is not known to the name-node yet;
						// getting the last block
						blk = locatedBlocks.GetLastLocatedBlock();
					}
					else
					{
						// search cached blocks first
						int targetBlockIdx = locatedBlocks.FindBlock(offset);
						if (targetBlockIdx < 0)
						{
							// block is not cached
							targetBlockIdx = LocatedBlocks.GetInsertIndex(targetBlockIdx);
							// fetch more blocks
							LocatedBlocks newBlocks = dfsClient.GetLocatedBlocks(src, offset);
							System.Diagnostics.Debug.Assert((newBlocks != null), "Could not find target position "
								 + offset);
							locatedBlocks.InsertRange(targetBlockIdx, newBlocks.GetLocatedBlocks());
						}
						blk = locatedBlocks.Get(targetBlockIdx);
					}
				}
				return blk;
			}
		}

		/// <summary>Fetch a block from namenode and cache it</summary>
		/// <exception cref="System.IO.IOException"/>
		private void FetchBlockAt(long offset)
		{
			lock (infoLock)
			{
				int targetBlockIdx = locatedBlocks.FindBlock(offset);
				if (targetBlockIdx < 0)
				{
					// block is not cached
					targetBlockIdx = LocatedBlocks.GetInsertIndex(targetBlockIdx);
				}
				// fetch blocks
				LocatedBlocks newBlocks = dfsClient.GetLocatedBlocks(src, offset);
				if (newBlocks == null)
				{
					throw new IOException("Could not find target position " + offset);
				}
				locatedBlocks.InsertRange(targetBlockIdx, newBlocks.GetLocatedBlocks());
			}
		}

		/// <summary>Get blocks in the specified range.</summary>
		/// <remarks>
		/// Get blocks in the specified range.
		/// Fetch them from the namenode if not cached. This function
		/// will not get a read request beyond the EOF.
		/// </remarks>
		/// <param name="offset">starting offset in file</param>
		/// <param name="length">length of data</param>
		/// <returns>consequent segment of located blocks</returns>
		/// <exception cref="System.IO.IOException"/>
		private IList<LocatedBlock> GetBlockRange(long offset, long length)
		{
			// getFileLength(): returns total file length
			// locatedBlocks.getFileLength(): returns length of completed blocks
			if (offset >= GetFileLength())
			{
				throw new IOException("Offset: " + offset + " exceeds file length: " + GetFileLength
					());
			}
			lock (infoLock)
			{
				IList<LocatedBlock> blocks;
				long lengthOfCompleteBlk = locatedBlocks.GetFileLength();
				bool readOffsetWithinCompleteBlk = offset < lengthOfCompleteBlk;
				bool readLengthPastCompleteBlk = offset + length > lengthOfCompleteBlk;
				if (readOffsetWithinCompleteBlk)
				{
					//get the blocks of finalized (completed) block range
					blocks = GetFinalizedBlockRange(offset, Math.Min(length, lengthOfCompleteBlk - offset
						));
				}
				else
				{
					blocks = new AList<LocatedBlock>(1);
				}
				// get the blocks from incomplete block range
				if (readLengthPastCompleteBlk)
				{
					blocks.AddItem(locatedBlocks.GetLastLocatedBlock());
				}
				return blocks;
			}
		}

		/// <summary>Get blocks in the specified range.</summary>
		/// <remarks>
		/// Get blocks in the specified range.
		/// Includes only the complete blocks.
		/// Fetch them from the namenode if not cached.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private IList<LocatedBlock> GetFinalizedBlockRange(long offset, long length)
		{
			lock (infoLock)
			{
				System.Diagnostics.Debug.Assert((locatedBlocks != null), "locatedBlocks is null");
				IList<LocatedBlock> blockRange = new AList<LocatedBlock>();
				// search cached blocks first
				int blockIdx = locatedBlocks.FindBlock(offset);
				if (blockIdx < 0)
				{
					// block is not cached
					blockIdx = LocatedBlocks.GetInsertIndex(blockIdx);
				}
				long remaining = length;
				long curOff = offset;
				while (remaining > 0)
				{
					LocatedBlock blk = null;
					if (blockIdx < locatedBlocks.LocatedBlockCount())
					{
						blk = locatedBlocks.Get(blockIdx);
					}
					if (blk == null || curOff < blk.GetStartOffset())
					{
						LocatedBlocks newBlocks;
						newBlocks = dfsClient.GetLocatedBlocks(src, curOff, remaining);
						locatedBlocks.InsertRange(blockIdx, newBlocks.GetLocatedBlocks());
						continue;
					}
					System.Diagnostics.Debug.Assert(curOff >= blk.GetStartOffset(), "Block not found"
						);
					blockRange.AddItem(blk);
					long bytesRead = blk.GetStartOffset() + blk.GetBlockSize() - curOff;
					remaining -= bytesRead;
					curOff += bytesRead;
					blockIdx++;
				}
				return blockRange;
			}
		}

		/// <summary>Open a DataInputStream to a DataNode so that it can be read from.</summary>
		/// <remarks>
		/// Open a DataInputStream to a DataNode so that it can be read from.
		/// We get block ID and the IDs of the destinations at startup, from the namenode.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private DatanodeInfo BlockSeekTo(long target)
		{
			lock (this)
			{
				if (target >= GetFileLength())
				{
					throw new IOException("Attempted to read past end of file");
				}
				// Will be getting a new BlockReader.
				CloseCurrentBlockReader();
				//
				// Connect to best DataNode for desired Block, with potential offset
				//
				DatanodeInfo chosenNode = null;
				int refetchToken = 1;
				// only need to get a new access token once
				int refetchEncryptionKey = 1;
				// only need to get a new encryption key once
				bool connectFailedOnce = false;
				while (true)
				{
					//
					// Compute desired block
					//
					LocatedBlock targetBlock = GetBlockAt(target);
					// update current position
					this.pos = target;
					this.blockEnd = targetBlock.GetStartOffset() + targetBlock.GetBlockSize() - 1;
					this.currentLocatedBlock = targetBlock;
					System.Diagnostics.Debug.Assert((target == pos), "Wrong postion " + pos + " expect "
						 + target);
					long offsetIntoBlock = target - targetBlock.GetStartOffset();
					DFSInputStream.DNAddrPair retval = ChooseDataNode(targetBlock, null);
					chosenNode = retval.info;
					IPEndPoint targetAddr = retval.addr;
					StorageType storageType = retval.storageType;
					try
					{
						ExtendedBlock blk = targetBlock.GetBlock();
						Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> accessToken = targetBlock
							.GetBlockToken();
						CachingStrategy curCachingStrategy;
						bool shortCircuitForbidden;
						lock (infoLock)
						{
							curCachingStrategy = cachingStrategy;
							shortCircuitForbidden = ShortCircuitForbidden();
						}
						blockReader = new BlockReaderFactory(dfsClient.GetConf()).SetInetSocketAddress(targetAddr
							).SetRemotePeerFactory(dfsClient).SetDatanodeInfo(chosenNode).SetStorageType(storageType
							).SetFileName(src).SetBlock(blk).SetBlockToken(accessToken).SetStartOffset(offsetIntoBlock
							).SetVerifyChecksum(verifyChecksum).SetClientName(dfsClient.clientName).SetLength
							(blk.GetNumBytes() - offsetIntoBlock).SetCachingStrategy(curCachingStrategy).SetAllowShortCircuitLocalReads
							(!shortCircuitForbidden).SetClientCacheContext(dfsClient.GetClientContext()).SetUserGroupInformation
							(dfsClient.ugi).SetConfiguration(dfsClient.GetConfiguration()).Build();
						if (connectFailedOnce)
						{
							DFSClient.Log.Info("Successfully connected to " + targetAddr + " for " + blk);
						}
						return chosenNode;
					}
					catch (IOException ex)
					{
						if (ex is InvalidEncryptionKeyException && refetchEncryptionKey > 0)
						{
							DFSClient.Log.Info("Will fetch a new encryption key and retry, " + "encryption key was invalid when connecting to "
								 + targetAddr + " : " + ex);
							// The encryption key used is invalid.
							refetchEncryptionKey--;
							dfsClient.ClearDataEncryptionKey();
						}
						else
						{
							if (refetchToken > 0 && TokenRefetchNeeded(ex, targetAddr))
							{
								refetchToken--;
								FetchBlockAt(target);
							}
							else
							{
								connectFailedOnce = true;
								DFSClient.Log.Warn("Failed to connect to " + targetAddr + " for block" + ", add to deadNodes and continue. "
									 + ex, ex);
								// Put chosen node into dead list, continue
								AddToDeadNodes(chosenNode);
							}
						}
					}
				}
			}
		}

		/// <summary>Close it down!</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				if (!closed.CompareAndSet(false, true))
				{
					DFSClient.Log.Debug("DFSInputStream has been closed already");
					return;
				}
				dfsClient.CheckOpen();
				if ((extendedReadBuffers != null) && (!extendedReadBuffers.IsEmpty()))
				{
					StringBuilder builder = new StringBuilder();
					extendedReadBuffers.VisitAll(new _Visitor_714(builder));
					DFSClient.Log.Warn("closing file " + src + ", but there are still " + "unreleased ByteBuffers allocated by read().  "
						 + "Please release " + builder.ToString() + ".");
				}
				CloseCurrentBlockReader();
				base.Close();
			}
		}

		private sealed class _Visitor_714 : IdentityHashStore.Visitor<ByteBuffer, object>
		{
			public _Visitor_714(StringBuilder builder)
			{
				this.builder = builder;
				this.prefix = string.Empty;
			}

			private string prefix;

			public void Accept(ByteBuffer k, object v)
			{
				builder.Append(this.prefix).Append(k);
				this.prefix = ", ";
			}

			private readonly StringBuilder builder;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			lock (this)
			{
				if (oneByteBuf == null)
				{
					oneByteBuf = new byte[1];
				}
				int ret = Read(oneByteBuf, 0, 1);
				return (ret <= 0) ? -1 : (oneByteBuf[0] & unchecked((int)(0xff)));
			}
		}

		/// <summary>
		/// Wraps different possible read implementations so that readBuffer can be
		/// strategy-agnostic.
		/// </summary>
		private interface ReaderStrategy
		{
			/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
			/// <exception cref="System.IO.IOException"/>
			int DoRead(BlockReader blockReader, int off, int len);
		}

		private void UpdateReadStatistics(DFSInputStream.ReadStatistics readStatistics, int
			 nRead, BlockReader blockReader)
		{
			if (nRead <= 0)
			{
				return;
			}
			lock (infoLock)
			{
				if (blockReader.IsShortCircuit())
				{
					readStatistics.AddShortCircuitBytes(nRead);
				}
				else
				{
					if (blockReader.IsLocal())
					{
						readStatistics.AddLocalBytes(nRead);
					}
					else
					{
						readStatistics.AddRemoteBytes(nRead);
					}
				}
			}
		}

		/// <summary>Used to read bytes into a byte[]</summary>
		private class ByteArrayStrategy : DFSInputStream.ReaderStrategy
		{
			internal readonly byte[] buf;

			public ByteArrayStrategy(DFSInputStream _enclosing, byte[] buf)
			{
				this._enclosing = _enclosing;
				this.buf = buf;
			}

			/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual int DoRead(BlockReader blockReader, int off, int len)
			{
				int nRead = blockReader.Read(this.buf, off, len);
				this._enclosing.UpdateReadStatistics(this._enclosing.readStatistics, nRead, blockReader
					);
				return nRead;
			}

			private readonly DFSInputStream _enclosing;
		}

		/// <summary>Used to read bytes into a user-supplied ByteBuffer</summary>
		private class ByteBufferStrategy : DFSInputStream.ReaderStrategy
		{
			internal readonly ByteBuffer buf;

			internal ByteBufferStrategy(DFSInputStream _enclosing, ByteBuffer buf)
			{
				this._enclosing = _enclosing;
				this.buf = buf;
			}

			/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual int DoRead(BlockReader blockReader, int off, int len)
			{
				int oldpos = this.buf.Position();
				int oldlimit = this.buf.Limit();
				bool success = false;
				try
				{
					int ret = blockReader.Read(this.buf);
					success = true;
					this._enclosing.UpdateReadStatistics(this._enclosing.readStatistics, ret, blockReader
						);
					return ret;
				}
				finally
				{
					if (!success)
					{
						// Reset to original state so that retries work correctly.
						this.buf.Position(oldpos);
						this.buf.Limit(oldlimit);
					}
				}
			}

			private readonly DFSInputStream _enclosing;
		}

		/* This is a used by regular read() and handles ChecksumExceptions.
		* name readBuffer() is chosen to imply similarity to readBuffer() in
		* ChecksumFileSystem
		*/
		/// <exception cref="System.IO.IOException"/>
		private int ReadBuffer(DFSInputStream.ReaderStrategy reader, int off, int len, IDictionary
			<ExtendedBlock, ICollection<DatanodeInfo>> corruptedBlockMap)
		{
			lock (this)
			{
				IOException ioe;
				/* we retry current node only once. So this is set to true only here.
				* Intention is to handle one common case of an error that is not a
				* failure on datanode or client : when DataNode closes the connection
				* since client is idle. If there are other cases of "non-errors" then
				* then a datanode might be retried by setting this to true again.
				*/
				bool retryCurrentNode = true;
				while (true)
				{
					// retry as many times as seekToNewSource allows.
					try
					{
						return reader.DoRead(blockReader, off, len);
					}
					catch (ChecksumException ce)
					{
						DFSClient.Log.Warn("Found Checksum error for " + GetCurrentBlock() + " from " + currentNode
							 + " at " + ce.GetPos());
						ioe = ce;
						retryCurrentNode = false;
						// we want to remember which block replicas we have tried
						AddIntoCorruptedBlockMap(GetCurrentBlock(), currentNode, corruptedBlockMap);
					}
					catch (IOException e)
					{
						if (!retryCurrentNode)
						{
							DFSClient.Log.Warn("Exception while reading from " + GetCurrentBlock() + " of " +
								 src + " from " + currentNode, e);
						}
						ioe = e;
					}
					bool sourceFound = false;
					if (retryCurrentNode)
					{
						/* possibly retry the same node so that transient errors don't
						* result in application level failures (e.g. Datanode could have
						* closed the connection because the client is idle for too long).
						*/
						sourceFound = SeekToBlockSource(pos);
					}
					else
					{
						AddToDeadNodes(currentNode);
						sourceFound = SeekToNewSource(pos);
					}
					if (!sourceFound)
					{
						throw ioe;
					}
					retryCurrentNode = false;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int ReadWithStrategy(DFSInputStream.ReaderStrategy strategy, int off, int
			 len)
		{
			lock (this)
			{
				dfsClient.CheckOpen();
				if (closed.Get())
				{
					throw new IOException("Stream closed");
				}
				IDictionary<ExtendedBlock, ICollection<DatanodeInfo>> corruptedBlockMap = new Dictionary
					<ExtendedBlock, ICollection<DatanodeInfo>>();
				failures = 0;
				if (pos < GetFileLength())
				{
					int retries = 2;
					while (retries > 0)
					{
						try
						{
							// currentNode can be left as null if previous read had a checksum
							// error on the same block. See HDFS-3067
							if (pos > blockEnd || currentNode == null)
							{
								currentNode = BlockSeekTo(pos);
							}
							int realLen = (int)Math.Min(len, (blockEnd - pos + 1L));
							lock (infoLock)
							{
								if (locatedBlocks.IsLastBlockComplete())
								{
									realLen = (int)Math.Min(realLen, locatedBlocks.GetFileLength() - pos);
								}
							}
							int result = ReadBuffer(strategy, off, realLen, corruptedBlockMap);
							if (result >= 0)
							{
								pos += result;
							}
							else
							{
								// got a EOS from reader though we expect more data on it.
								throw new IOException("Unexpected EOS from the reader");
							}
							if (dfsClient.stats != null)
							{
								dfsClient.stats.IncrementBytesRead(result);
							}
							return result;
						}
						catch (ChecksumException ce)
						{
							throw;
						}
						catch (IOException e)
						{
							if (retries == 1)
							{
								DFSClient.Log.Warn("DFS Read", e);
							}
							blockEnd = -1;
							if (currentNode != null)
							{
								AddToDeadNodes(currentNode);
							}
							if (--retries == 0)
							{
								throw;
							}
						}
						finally
						{
							// Check if need to report block replicas corruption either read
							// was successful or ChecksumException occured.
							ReportCheckSumFailure(corruptedBlockMap, currentLocatedBlock.GetLocations().Length
								);
						}
					}
				}
				return -1;
			}
		}

		/// <summary>Read the entire buffer.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] buf, int off, int len)
		{
			lock (this)
			{
				DFSInputStream.ReaderStrategy byteArrayReader = new DFSInputStream.ByteArrayStrategy
					(this, buf);
				TraceScope scope = dfsClient.GetPathTraceScope("DFSInputStream#byteArrayRead", src
					);
				try
				{
					return ReadWithStrategy(byteArrayReader, off, len);
				}
				finally
				{
					scope.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(ByteBuffer buf)
		{
			lock (this)
			{
				DFSInputStream.ReaderStrategy byteBufferReader = new DFSInputStream.ByteBufferStrategy
					(this, buf);
				TraceScope scope = dfsClient.GetPathTraceScope("DFSInputStream#byteBufferRead", src
					);
				try
				{
					return ReadWithStrategy(byteBufferReader, 0, buf.Remaining());
				}
				finally
				{
					scope.Close();
				}
			}
		}

		/// <summary>Add corrupted block replica into map.</summary>
		private void AddIntoCorruptedBlockMap(ExtendedBlock blk, DatanodeInfo node, IDictionary
			<ExtendedBlock, ICollection<DatanodeInfo>> corruptedBlockMap)
		{
			ICollection<DatanodeInfo> dnSet = null;
			if ((corruptedBlockMap.Contains(blk)))
			{
				dnSet = corruptedBlockMap[blk];
			}
			else
			{
				dnSet = new HashSet<DatanodeInfo>();
			}
			if (!dnSet.Contains(node))
			{
				dnSet.AddItem(node);
				corruptedBlockMap[blk] = dnSet;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private DFSInputStream.DNAddrPair ChooseDataNode(LocatedBlock block, ICollection<
			DatanodeInfo> ignoredNodes)
		{
			while (true)
			{
				try
				{
					return GetBestNodeDNAddrPair(block, ignoredNodes);
				}
				catch (IOException ie)
				{
					string errMsg = GetBestNodeDNAddrPairErrorString(block.GetLocations(), deadNodes, 
						ignoredNodes);
					string blockInfo = block.GetBlock() + " file=" + src;
					if (failures >= dfsClient.GetMaxBlockAcquireFailures())
					{
						string description = "Could not obtain block: " + blockInfo;
						DFSClient.Log.Warn(description + errMsg + ". Throwing a BlockMissingException");
						throw new BlockMissingException(src, description, block.GetStartOffset());
					}
					DatanodeInfo[] nodes = block.GetLocations();
					if (nodes == null || nodes.Length == 0)
					{
						DFSClient.Log.Info("No node available for " + blockInfo);
					}
					DFSClient.Log.Info("Could not obtain " + block.GetBlock() + " from any node: " + 
						ie + errMsg + ". Will get new block locations from namenode and retry...");
					try
					{
						// Introducing a random factor to the wait time before another retry.
						// The wait time is dependent on # of failures and a random factor.
						// At the first time of getting a BlockMissingException, the wait time
						// is a random number between 0..3000 ms. If the first retry
						// still fails, we will wait 3000 ms grace period before the 2nd retry.
						// Also at the second retry, the waiting window is expanded to 6000 ms
						// alleviating the request rate from the server. Similarly the 3rd retry
						// will wait 6000ms grace period before retry and the waiting window is
						// expanded to 9000ms. 
						int timeWindow = dfsClient.GetConf().timeWindow;
						double waitTime = timeWindow * failures + timeWindow * (failures + 1) * DFSUtil.GetRandom
							().NextDouble();
						// grace period for the last round of attempt
						// expanding time window for each failure
						DFSClient.Log.Warn("DFS chooseDataNode: got # " + (failures + 1) + " IOException, will wait for "
							 + waitTime + " msec.");
						Sharpen.Thread.Sleep((long)waitTime);
					}
					catch (Exception)
					{
					}
					deadNodes.Clear();
					//2nd option is to remove only nodes[blockId]
					OpenInfo();
					block = GetBlockAt(block.GetStartOffset());
					failures++;
					continue;
				}
			}
		}

		/// <summary>Get the best node from which to stream the data.</summary>
		/// <param name="block">LocatedBlock, containing nodes in priority order.</param>
		/// <param name="ignoredNodes">Do not choose nodes in this array (may be null)</param>
		/// <returns>The DNAddrPair of the best node.</returns>
		/// <exception cref="System.IO.IOException"/>
		private DFSInputStream.DNAddrPair GetBestNodeDNAddrPair(LocatedBlock block, ICollection
			<DatanodeInfo> ignoredNodes)
		{
			DatanodeInfo[] nodes = block.GetLocations();
			StorageType[] storageTypes = block.GetStorageTypes();
			DatanodeInfo chosenNode = null;
			StorageType storageType = null;
			if (nodes != null)
			{
				for (int i = 0; i < nodes.Length; i++)
				{
					if (!deadNodes.Contains(nodes[i]) && (ignoredNodes == null || !ignoredNodes.Contains
						(nodes[i])))
					{
						chosenNode = nodes[i];
						// Storage types are ordered to correspond with nodes, so use the same
						// index to get storage type.
						if (storageTypes != null && i < storageTypes.Length)
						{
							storageType = storageTypes[i];
						}
						break;
					}
				}
			}
			if (chosenNode == null)
			{
				throw new IOException("No live nodes contain block " + block.GetBlock() + " after checking nodes = "
					 + Arrays.ToString(nodes) + ", ignoredNodes = " + ignoredNodes);
			}
			string dnAddr = chosenNode.GetXferAddr(dfsClient.GetConf().connectToDnViaHostname
				);
			if (DFSClient.Log.IsDebugEnabled())
			{
				DFSClient.Log.Debug("Connecting to datanode " + dnAddr);
			}
			IPEndPoint targetAddr = NetUtils.CreateSocketAddr(dnAddr);
			return new DFSInputStream.DNAddrPair(chosenNode, targetAddr, storageType);
		}

		private static string GetBestNodeDNAddrPairErrorString(DatanodeInfo[] nodes, AbstractMap
			<DatanodeInfo, DatanodeInfo> deadNodes, ICollection<DatanodeInfo> ignoredNodes)
		{
			StringBuilder errMsgr = new StringBuilder(" No live nodes contain current block "
				);
			errMsgr.Append("Block locations:");
			foreach (DatanodeInfo datanode in nodes)
			{
				errMsgr.Append(" ");
				errMsgr.Append(datanode.ToString());
			}
			errMsgr.Append(" Dead nodes: ");
			foreach (DatanodeInfo datanode_1 in deadNodes.Keys)
			{
				errMsgr.Append(" ");
				errMsgr.Append(datanode_1.ToString());
			}
			if (ignoredNodes != null)
			{
				errMsgr.Append(" Ignored nodes: ");
				foreach (DatanodeInfo datanode_2 in ignoredNodes)
				{
					errMsgr.Append(" ");
					errMsgr.Append(datanode_2.ToString());
				}
			}
			return errMsgr.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		private void FetchBlockByteRange(LocatedBlock block, long start, long end, byte[]
			 buf, int offset, IDictionary<ExtendedBlock, ICollection<DatanodeInfo>> corruptedBlockMap
			)
		{
			block = GetBlockAt(block.GetStartOffset());
			while (true)
			{
				DFSInputStream.DNAddrPair addressPair = ChooseDataNode(block, null);
				try
				{
					ActualGetFromOneDataNode(addressPair, block, start, end, buf, offset, corruptedBlockMap
						);
					return;
				}
				catch (IOException)
				{
				}
			}
		}

		// Ignore. Already processed inside the function.
		// Loop through to try the next node.
		private Callable<ByteBuffer> GetFromOneDataNode(DFSInputStream.DNAddrPair datanode
			, LocatedBlock block, long start, long end, ByteBuffer bb, IDictionary<ExtendedBlock
			, ICollection<DatanodeInfo>> corruptedBlockMap, int hedgedReadId)
		{
			Span parentSpan = Trace.CurrentSpan();
			return new _Callable_1110(this, bb, hedgedReadId, parentSpan, datanode, block, start
				, end, corruptedBlockMap);
		}

		private sealed class _Callable_1110 : Callable<ByteBuffer>
		{
			public _Callable_1110(DFSInputStream _enclosing, ByteBuffer bb, int hedgedReadId, 
				Span parentSpan, DFSInputStream.DNAddrPair datanode, LocatedBlock block, long start
				, long end, IDictionary<ExtendedBlock, ICollection<DatanodeInfo>> corruptedBlockMap
				)
			{
				this._enclosing = _enclosing;
				this.bb = bb;
				this.hedgedReadId = hedgedReadId;
				this.parentSpan = parentSpan;
				this.datanode = datanode;
				this.block = block;
				this.start = start;
				this.end = end;
				this.corruptedBlockMap = corruptedBlockMap;
			}

			/// <exception cref="System.Exception"/>
			public ByteBuffer Call()
			{
				byte[] buf = ((byte[])bb.Array());
				int offset = bb.Position();
				TraceScope scope = Trace.StartSpan("hedgedRead" + hedgedReadId, parentSpan);
				try
				{
					this._enclosing.ActualGetFromOneDataNode(datanode, block, start, end, buf, offset
						, corruptedBlockMap);
					return bb;
				}
				finally
				{
					scope.Close();
				}
			}

			private readonly DFSInputStream _enclosing;

			private readonly ByteBuffer bb;

			private readonly int hedgedReadId;

			private readonly Span parentSpan;

			private readonly DFSInputStream.DNAddrPair datanode;

			private readonly LocatedBlock block;

			private readonly long start;

			private readonly long end;

			private readonly IDictionary<ExtendedBlock, ICollection<DatanodeInfo>> corruptedBlockMap;
		}

		/// <exception cref="System.IO.IOException"/>
		private void ActualGetFromOneDataNode(DFSInputStream.DNAddrPair datanode, LocatedBlock
			 block, long start, long end, byte[] buf, int offset, IDictionary<ExtendedBlock, 
			ICollection<DatanodeInfo>> corruptedBlockMap)
		{
			DFSClientFaultInjector.Get().StartFetchFromDatanode();
			int refetchToken = 1;
			// only need to get a new access token once
			int refetchEncryptionKey = 1;
			// only need to get a new encryption key once
			while (true)
			{
				// cached block locations may have been updated by chooseDataNode()
				// or fetchBlockAt(). Always get the latest list of locations at the
				// start of the loop.
				CachingStrategy curCachingStrategy;
				bool allowShortCircuitLocalReads;
				block = GetBlockAt(block.GetStartOffset());
				lock (infoLock)
				{
					curCachingStrategy = cachingStrategy;
					allowShortCircuitLocalReads = !ShortCircuitForbidden();
				}
				DatanodeInfo chosenNode = datanode.info;
				IPEndPoint targetAddr = datanode.addr;
				StorageType storageType = datanode.storageType;
				BlockReader reader = null;
				try
				{
					DFSClientFaultInjector.Get().FetchFromDatanodeException();
					Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> blockToken = block.GetBlockToken
						();
					int len = (int)(end - start + 1);
					reader = new BlockReaderFactory(dfsClient.GetConf()).SetInetSocketAddress(targetAddr
						).SetRemotePeerFactory(dfsClient).SetDatanodeInfo(chosenNode).SetStorageType(storageType
						).SetFileName(src).SetBlock(block.GetBlock()).SetBlockToken(blockToken).SetStartOffset
						(start).SetVerifyChecksum(verifyChecksum).SetClientName(dfsClient.clientName).SetLength
						(len).SetCachingStrategy(curCachingStrategy).SetAllowShortCircuitLocalReads(allowShortCircuitLocalReads
						).SetClientCacheContext(dfsClient.GetClientContext()).SetUserGroupInformation(dfsClient
						.ugi).SetConfiguration(dfsClient.GetConfiguration()).Build();
					int nread = reader.ReadAll(buf, offset, len);
					UpdateReadStatistics(readStatistics, nread, reader);
					if (nread != len)
					{
						throw new IOException("truncated return from reader.read(): " + "excpected " + len
							 + ", got " + nread);
					}
					DFSClientFaultInjector.Get().ReadFromDatanodeDelay();
					return;
				}
				catch (ChecksumException e)
				{
					string msg = "fetchBlockByteRange(). Got a checksum exception for " + src + " at "
						 + block.GetBlock() + ":" + e.GetPos() + " from " + chosenNode;
					DFSClient.Log.Warn(msg);
					// we want to remember what we have tried
					AddIntoCorruptedBlockMap(block.GetBlock(), chosenNode, corruptedBlockMap);
					AddToDeadNodes(chosenNode);
					throw new IOException(msg);
				}
				catch (IOException e)
				{
					if (e is InvalidEncryptionKeyException && refetchEncryptionKey > 0)
					{
						DFSClient.Log.Info("Will fetch a new encryption key and retry, " + "encryption key was invalid when connecting to "
							 + targetAddr + " : " + e);
						// The encryption key used is invalid.
						refetchEncryptionKey--;
						dfsClient.ClearDataEncryptionKey();
						continue;
					}
					else
					{
						if (refetchToken > 0 && TokenRefetchNeeded(e, targetAddr))
						{
							refetchToken--;
							try
							{
								FetchBlockAt(block.GetStartOffset());
							}
							catch (IOException)
							{
							}
							// ignore IOE, since we can retry it later in a loop
							continue;
						}
						else
						{
							string msg = "Failed to connect to " + targetAddr + " for file " + src + " for block "
								 + block.GetBlock() + ":" + e;
							DFSClient.Log.Warn("Connection failure: " + msg, e);
							AddToDeadNodes(chosenNode);
							throw new IOException(msg);
						}
					}
				}
				finally
				{
					if (reader != null)
					{
						reader.Close();
					}
				}
			}
		}

		/// <summary>
		/// Like
		/// <see cref="FetchBlockByteRange(Org.Apache.Hadoop.Hdfs.Protocol.LocatedBlock, long, long, byte[], int, System.Collections.Generic.IDictionary{K, V})
		/// 	"/>
		/// except we start up a second, parallel, 'hedged' read
		/// if the first read is taking longer than configured amount of
		/// time.  We then wait on which ever read returns first.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void HedgedFetchBlockByteRange(LocatedBlock block, long start, long end, 
			byte[] buf, int offset, IDictionary<ExtendedBlock, ICollection<DatanodeInfo>> corruptedBlockMap
			)
		{
			AList<Future<ByteBuffer>> futures = new AList<Future<ByteBuffer>>();
			CompletionService<ByteBuffer> hedgedService = new ExecutorCompletionService<ByteBuffer
				>(dfsClient.GetHedgedReadsThreadPool());
			AList<DatanodeInfo> ignored = new AList<DatanodeInfo>();
			ByteBuffer bb = null;
			int len = (int)(end - start + 1);
			int hedgedReadId = 0;
			block = GetBlockAt(block.GetStartOffset());
			while (true)
			{
				// see HDFS-6591, this metric is used to verify/catch unnecessary loops
				hedgedReadOpsLoopNumForTesting++;
				DFSInputStream.DNAddrPair chosenNode = null;
				// there is no request already executing.
				if (futures.IsEmpty())
				{
					// chooseDataNode is a commitment. If no node, we go to
					// the NN to reget block locations. Only go here on first read.
					chosenNode = ChooseDataNode(block, ignored);
					bb = ByteBuffer.Wrap(buf, offset, len);
					Callable<ByteBuffer> getFromDataNodeCallable = GetFromOneDataNode(chosenNode, block
						, start, end, bb, corruptedBlockMap, hedgedReadId++);
					Future<ByteBuffer> firstRequest = hedgedService.Submit(getFromDataNodeCallable);
					futures.AddItem(firstRequest);
					try
					{
						Future<ByteBuffer> future = hedgedService.Poll(dfsClient.GetHedgedReadTimeout(), 
							TimeUnit.Milliseconds);
						if (future != null)
						{
							future.Get();
							return;
						}
						if (DFSClient.Log.IsDebugEnabled())
						{
							DFSClient.Log.Debug("Waited " + dfsClient.GetHedgedReadTimeout() + "ms to read from "
								 + chosenNode.info + "; spawning hedged read");
						}
						// Ignore this node on next go around.
						ignored.AddItem(chosenNode.info);
						dfsClient.GetHedgedReadMetrics().IncHedgedReadOps();
						continue;
					}
					catch (Exception)
					{
					}
					catch (ExecutionException)
					{
					}
				}
				else
				{
					// no need to refresh block locations
					// Ignore
					// Ignore already logged in the call.
					// We are starting up a 'hedged' read. We have a read already
					// ongoing. Call getBestNodeDNAddrPair instead of chooseDataNode.
					// If no nodes to do hedged reads against, pass.
					try
					{
						try
						{
							chosenNode = GetBestNodeDNAddrPair(block, ignored);
						}
						catch (IOException)
						{
							chosenNode = ChooseDataNode(block, ignored);
						}
						bb = ByteBuffer.Allocate(len);
						Callable<ByteBuffer> getFromDataNodeCallable = GetFromOneDataNode(chosenNode, block
							, start, end, bb, corruptedBlockMap, hedgedReadId++);
						Future<ByteBuffer> oneMoreRequest = hedgedService.Submit(getFromDataNodeCallable);
						futures.AddItem(oneMoreRequest);
					}
					catch (IOException ioe)
					{
						if (DFSClient.Log.IsDebugEnabled())
						{
							DFSClient.Log.Debug("Failed getting node for hedged read: " + ioe.Message);
						}
					}
					// if not succeeded. Submit callables for each datanode in a loop, wait
					// for a fixed interval and get the result from the fastest one.
					try
					{
						ByteBuffer result = GetFirstToComplete(hedgedService, futures);
						// cancel the rest.
						CancelAll(futures);
						if (((byte[])result.Array()) != buf)
						{
							// compare the array pointers
							dfsClient.GetHedgedReadMetrics().IncHedgedReadWins();
							System.Array.Copy(((byte[])result.Array()), result.Position(), buf, offset, len);
						}
						else
						{
							dfsClient.GetHedgedReadMetrics().IncHedgedReadOps();
						}
						return;
					}
					catch (Exception)
					{
					}
					// Ignore and retry
					// We got here if exception. Ignore this node on next go around IFF
					// we found a chosenNode to hedge read against.
					if (chosenNode != null && chosenNode.info != null)
					{
						ignored.AddItem(chosenNode.info);
					}
				}
			}
		}

		[VisibleForTesting]
		public virtual long GetHedgedReadOpsLoopNumForTesting()
		{
			return hedgedReadOpsLoopNumForTesting;
		}

		/// <exception cref="System.Exception"/>
		private ByteBuffer GetFirstToComplete(CompletionService<ByteBuffer> hedgedService
			, AList<Future<ByteBuffer>> futures)
		{
			if (futures.IsEmpty())
			{
				throw new Exception("let's retry");
			}
			Future<ByteBuffer> future = null;
			try
			{
				future = hedgedService.Take();
				ByteBuffer bb = future.Get();
				futures.Remove(future);
				return bb;
			}
			catch (ExecutionException)
			{
				// already logged in the Callable
				futures.Remove(future);
			}
			catch (CancellationException)
			{
				// already logged in the Callable
				futures.Remove(future);
			}
			throw new Exception("let's retry");
		}

		private void CancelAll(IList<Future<ByteBuffer>> futures)
		{
			foreach (Future<ByteBuffer> future in futures)
			{
				// Unfortunately, hdfs reads do not take kindly to interruption.
				// Threads return a variety of interrupted-type exceptions but
				// also complaints about invalid pbs -- likely because read
				// is interrupted before gets whole pb.  Also verbose WARN
				// logging.  So, for now, do not interrupt running read.
				future.Cancel(false);
			}
		}

		/// <summary>Should the block access token be refetched on an exception</summary>
		/// <param name="ex">Exception received</param>
		/// <param name="targetAddr">Target datanode address from where exception was received
		/// 	</param>
		/// <returns>
		/// true if block access token has expired or invalid and it should be
		/// refetched
		/// </returns>
		private static bool TokenRefetchNeeded(IOException ex, IPEndPoint targetAddr)
		{
			/*
			* Get a new access token and retry. Retry is needed in 2 cases. 1)
			* When both NN and DN re-started while DFSClient holding a cached
			* access token. 2) In the case that NN fails to update its
			* access key at pre-set interval (by a wide margin) and
			* subsequently restarts. In this case, DN re-registers itself with
			* NN and receives a new access key, but DN will delete the old
			* access key from its memory since it's considered expired based on
			* the estimated expiration date.
			*/
			if (ex is InvalidBlockTokenException || ex is SecretManager.InvalidToken)
			{
				DFSClient.Log.Info("Access token was invalid when connecting to " + targetAddr + 
					" : " + ex);
				return true;
			}
			return false;
		}

		/// <summary>Read bytes starting from the specified position.</summary>
		/// <param name="position">start read from this position</param>
		/// <param name="buffer">read buffer</param>
		/// <param name="offset">offset into buffer</param>
		/// <param name="length">number of bytes to read</param>
		/// <returns>actual number of bytes read</returns>
		/// <exception cref="System.IO.IOException"/>
		public override int Read(long position, byte[] buffer, int offset, int length)
		{
			TraceScope scope = dfsClient.GetPathTraceScope("DFSInputStream#byteArrayPread", src
				);
			try
			{
				return Pread(position, buffer, offset, length);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int Pread(long position, byte[] buffer, int offset, int length)
		{
			// sanity checks
			dfsClient.CheckOpen();
			if (closed.Get())
			{
				throw new IOException("Stream closed");
			}
			failures = 0;
			long filelen = GetFileLength();
			if ((position < 0) || (position >= filelen))
			{
				return -1;
			}
			int realLen = length;
			if ((position + length) > filelen)
			{
				realLen = (int)(filelen - position);
			}
			// determine the block and byte range within the block
			// corresponding to position and realLen
			IList<LocatedBlock> blockRange = GetBlockRange(position, realLen);
			int remaining = realLen;
			IDictionary<ExtendedBlock, ICollection<DatanodeInfo>> corruptedBlockMap = new Dictionary
				<ExtendedBlock, ICollection<DatanodeInfo>>();
			foreach (LocatedBlock blk in blockRange)
			{
				long targetStart = position - blk.GetStartOffset();
				long bytesToRead = Math.Min(remaining, blk.GetBlockSize() - targetStart);
				try
				{
					if (dfsClient.IsHedgedReadsEnabled())
					{
						HedgedFetchBlockByteRange(blk, targetStart, targetStart + bytesToRead - 1, buffer
							, offset, corruptedBlockMap);
					}
					else
					{
						FetchBlockByteRange(blk, targetStart, targetStart + bytesToRead - 1, buffer, offset
							, corruptedBlockMap);
					}
				}
				finally
				{
					// Check and report if any block replicas are corrupted.
					// BlockMissingException may be caught if all block replicas are
					// corrupted.
					ReportCheckSumFailure(corruptedBlockMap, blk.GetLocations().Length);
				}
				remaining -= bytesToRead;
				position += bytesToRead;
				offset += bytesToRead;
			}
			System.Diagnostics.Debug.Assert(remaining == 0, "Wrong number of bytes read.");
			if (dfsClient.stats != null)
			{
				dfsClient.stats.IncrementBytesRead(realLen);
			}
			return realLen;
		}

		/// <summary>DFSInputStream reports checksum failure.</summary>
		/// <remarks>
		/// DFSInputStream reports checksum failure.
		/// Case I : client has tried multiple data nodes and at least one of the
		/// attempts has succeeded. We report the other failures as corrupted block to
		/// namenode.
		/// Case II: client has tried out all data nodes, but all failed. We
		/// only report if the total number of replica is 1. We do not
		/// report otherwise since this maybe due to the client is a handicapped client
		/// (who can not read).
		/// </remarks>
		/// <param name="corruptedBlockMap">map of corrupted blocks</param>
		/// <param name="dataNodeCount">number of data nodes who contains the block replicas</param>
		private void ReportCheckSumFailure(IDictionary<ExtendedBlock, ICollection<DatanodeInfo
			>> corruptedBlockMap, int dataNodeCount)
		{
			if (corruptedBlockMap.IsEmpty())
			{
				return;
			}
			IEnumerator<KeyValuePair<ExtendedBlock, ICollection<DatanodeInfo>>> it = corruptedBlockMap
				.GetEnumerator();
			KeyValuePair<ExtendedBlock, ICollection<DatanodeInfo>> entry = it.Next();
			ExtendedBlock blk = entry.Key;
			ICollection<DatanodeInfo> dnSet = entry.Value;
			if (((dnSet.Count < dataNodeCount) && (dnSet.Count > 0)) || ((dataNodeCount == 1)
				 && (dnSet.Count == dataNodeCount)))
			{
				DatanodeInfo[] locs = new DatanodeInfo[dnSet.Count];
				int i = 0;
				foreach (DatanodeInfo dn in dnSet)
				{
					locs[i++] = dn;
				}
				LocatedBlock[] lblocks = new LocatedBlock[] { new LocatedBlock(blk, locs) };
				dfsClient.ReportChecksumFailure(src, lblocks);
			}
			corruptedBlockMap.Clear();
		}

		/// <exception cref="System.IO.IOException"/>
		public override long Skip(long n)
		{
			if (n > 0)
			{
				long curPos = GetPos();
				long fileLen = GetFileLength();
				if (n + curPos > fileLen)
				{
					n = fileLen - curPos;
				}
				Seek(curPos + n);
				return n;
			}
			return n < 0 ? -1 : 0;
		}

		/// <summary>Seek to a new arbitrary location</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Seek(long targetPos)
		{
			lock (this)
			{
				if (targetPos > GetFileLength())
				{
					throw new EOFException("Cannot seek after EOF");
				}
				if (targetPos < 0)
				{
					throw new EOFException("Cannot seek to negative offset");
				}
				if (closed.Get())
				{
					throw new IOException("Stream is closed!");
				}
				bool done = false;
				if (pos <= targetPos && targetPos <= blockEnd)
				{
					//
					// If this seek is to a positive position in the current
					// block, and this piece of data might already be lying in
					// the TCP buffer, then just eat up the intervening data.
					//
					int diff = (int)(targetPos - pos);
					if (diff <= blockReader.Available())
					{
						try
						{
							pos += blockReader.Skip(diff);
							if (pos == targetPos)
							{
								done = true;
							}
							else
							{
								// The range was already checked. If the block reader returns
								// something unexpected instead of throwing an exception, it is
								// most likely a bug. 
								string errMsg = "BlockReader failed to seek to " + targetPos + ". Instead, it seeked to "
									 + pos + ".";
								DFSClient.Log.Warn(errMsg);
								throw new IOException(errMsg);
							}
						}
						catch (IOException e)
						{
							//make following read to retry
							if (DFSClient.Log.IsDebugEnabled())
							{
								DFSClient.Log.Debug("Exception while seek to " + targetPos + " from " + GetCurrentBlock
									() + " of " + src + " from " + currentNode, e);
							}
						}
					}
				}
				if (!done)
				{
					pos = targetPos;
					blockEnd = -1;
				}
			}
		}

		/// <summary>
		/// Same as
		/// <see cref="SeekToNewSource(long)"/>
		/// except that it does not exclude
		/// the current datanode and might connect to the same node.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private bool SeekToBlockSource(long targetPos)
		{
			currentNode = BlockSeekTo(targetPos);
			return true;
		}

		/// <summary>Seek to given position on a node other than the current node.</summary>
		/// <remarks>
		/// Seek to given position on a node other than the current node.  If
		/// a node other than the current node is found, then returns true.
		/// If another node could not be found, then returns false.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override bool SeekToNewSource(long targetPos)
		{
			lock (this)
			{
				bool markedDead = deadNodes.Contains(currentNode);
				AddToDeadNodes(currentNode);
				DatanodeInfo oldNode = currentNode;
				DatanodeInfo newNode = BlockSeekTo(targetPos);
				if (!markedDead)
				{
					/* remove it from deadNodes. blockSeekTo could have cleared
					* deadNodes and added currentNode again. Thats ok. */
					Sharpen.Collections.Remove(deadNodes, oldNode);
				}
				if (!oldNode.GetDatanodeUuid().Equals(newNode.GetDatanodeUuid()))
				{
					currentNode = newNode;
					return true;
				}
				else
				{
					return false;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetPos()
		{
			lock (this)
			{
				return pos;
			}
		}

		/// <summary>
		/// Return the size of the remaining available bytes
		/// if the size is less than or equal to
		/// <see cref="int.MaxValue"/>
		/// ,
		/// otherwise, return
		/// <see cref="int.MaxValue"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override int Available()
		{
			lock (this)
			{
				if (closed.Get())
				{
					throw new IOException("Stream closed");
				}
				long remaining = GetFileLength() - pos;
				return remaining <= int.MaxValue ? (int)remaining : int.MaxValue;
			}
		}

		/// <summary>We definitely don't support marks</summary>
		public override bool MarkSupported()
		{
			return false;
		}

		public override void Mark(int readLimit)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Reset()
		{
			throw new IOException("Mark/reset not supported");
		}

		/// <summary>Utility class to encapsulate data node info and its address.</summary>
		private sealed class DNAddrPair
		{
			internal readonly DatanodeInfo info;

			internal readonly IPEndPoint addr;

			internal readonly StorageType storageType;

			internal DNAddrPair(DatanodeInfo info, IPEndPoint addr, StorageType storageType)
			{
				this.info = info;
				this.addr = addr;
				this.storageType = storageType;
			}
		}

		/// <summary>Get statistics about the reads which this DFSInputStream has done.</summary>
		public virtual DFSInputStream.ReadStatistics GetReadStatistics()
		{
			lock (infoLock)
			{
				return new DFSInputStream.ReadStatistics(readStatistics);
			}
		}

		/// <summary>Clear statistics about the reads which this DFSInputStream has done.</summary>
		public virtual void ClearReadStatistics()
		{
			lock (infoLock)
			{
				readStatistics.Clear();
			}
		}

		public virtual FileEncryptionInfo GetFileEncryptionInfo()
		{
			lock (infoLock)
			{
				return fileEncryptionInfo;
			}
		}

		private void CloseCurrentBlockReader()
		{
			if (blockReader == null)
			{
				return;
			}
			// Close the current block reader so that the new caching settings can 
			// take effect immediately.
			try
			{
				blockReader.Close();
			}
			catch (IOException e)
			{
				DFSClient.Log.Error("error closing blockReader", e);
			}
			blockReader = null;
			blockEnd = -1;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetReadahead(long readahead)
		{
			lock (this)
			{
				lock (infoLock)
				{
					this.cachingStrategy = new CachingStrategy.Builder(this.cachingStrategy).SetReadahead
						(readahead).Build();
				}
				CloseCurrentBlockReader();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetDropBehind(bool dropBehind)
		{
			lock (this)
			{
				lock (infoLock)
				{
					this.cachingStrategy = new CachingStrategy.Builder(this.cachingStrategy).SetDropBehind
						(dropBehind).Build();
				}
				CloseCurrentBlockReader();
			}
		}

		/// <summary>
		/// The immutable empty buffer we return when we reach EOF when doing a
		/// zero-copy read.
		/// </summary>
		private static readonly ByteBuffer EmptyBuffer = ByteBuffer.AllocateDirect(0).AsReadOnlyBuffer
			();

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual ByteBuffer Read(ByteBufferPool bufferPool, int maxLength, EnumSet<
			ReadOption> opts)
		{
			lock (this)
			{
				if (maxLength == 0)
				{
					return EmptyBuffer;
				}
				else
				{
					if (maxLength < 0)
					{
						throw new ArgumentException("can't read a negative " + "number of bytes.");
					}
				}
				if ((blockReader == null) || (blockEnd == -1))
				{
					if (pos >= GetFileLength())
					{
						return null;
					}
					/*
					* If we don't have a blockReader, or the one we have has no more bytes
					* left to read, we call seekToBlockSource to get a new blockReader and
					* recalculate blockEnd.  Note that we assume we're not at EOF here
					* (we check this above).
					*/
					if ((!SeekToBlockSource(pos)) || (blockReader == null))
					{
						throw new IOException("failed to allocate new BlockReader " + "at position " + pos
							);
					}
				}
				ByteBuffer buffer = null;
				if (dfsClient.GetConf().shortCircuitMmapEnabled)
				{
					buffer = TryReadZeroCopy(maxLength, opts);
				}
				if (buffer != null)
				{
					return buffer;
				}
				buffer = ByteBufferUtil.FallbackRead(this, bufferPool, maxLength);
				if (buffer != null)
				{
					GetExtendedReadBuffers().Put(buffer, bufferPool);
				}
				return buffer;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private ByteBuffer TryReadZeroCopy(int maxLength, EnumSet<ReadOption> opts)
		{
			lock (this)
			{
				// Copy 'pos' and 'blockEnd' to local variables to make it easier for the
				// JVM to optimize this function.
				long curPos = pos;
				long curEnd = blockEnd;
				long blockStartInFile = currentLocatedBlock.GetStartOffset();
				long blockPos = curPos - blockStartInFile;
				// Shorten this read if the end of the block is nearby.
				long length63;
				if ((curPos + maxLength) <= (curEnd + 1))
				{
					length63 = maxLength;
				}
				else
				{
					length63 = 1 + curEnd - curPos;
					if (length63 <= 0)
					{
						if (DFSClient.Log.IsDebugEnabled())
						{
							DFSClient.Log.Debug("Unable to perform a zero-copy read from offset " + curPos + 
								" of " + src + "; " + length63 + " bytes left in block.  " + "blockPos=" + blockPos
								 + "; curPos=" + curPos + "; curEnd=" + curEnd);
						}
						return null;
					}
					if (DFSClient.Log.IsDebugEnabled())
					{
						DFSClient.Log.Debug("Reducing read length from " + maxLength + " to " + length63 
							+ " to avoid going more than one byte " + "past the end of the block.  blockPos="
							 + blockPos + "; curPos=" + curPos + "; curEnd=" + curEnd);
					}
				}
				// Make sure that don't go beyond 31-bit offsets in the MappedByteBuffer.
				int length;
				if (blockPos + length63 <= int.MaxValue)
				{
					length = (int)length63;
				}
				else
				{
					long length31 = int.MaxValue - blockPos;
					if (length31 <= 0)
					{
						// Java ByteBuffers can't be longer than 2 GB, because they use
						// 4-byte signed integers to represent capacity, etc.
						// So we can't mmap the parts of the block higher than the 2 GB offset.
						// FIXME: we could work around this with multiple memory maps.
						// See HDFS-5101.
						if (DFSClient.Log.IsDebugEnabled())
						{
							DFSClient.Log.Debug("Unable to perform a zero-copy read from offset " + curPos + 
								" of " + src + "; 31-bit MappedByteBuffer limit " + "exceeded.  blockPos=" + blockPos
								 + ", curEnd=" + curEnd);
						}
						return null;
					}
					length = (int)length31;
					if (DFSClient.Log.IsDebugEnabled())
					{
						DFSClient.Log.Debug("Reducing read length from " + maxLength + " to " + length + 
							" to avoid 31-bit limit.  " + "blockPos=" + blockPos + "; curPos=" + curPos + "; curEnd="
							 + curEnd);
					}
				}
				ClientMmap clientMmap = blockReader.GetClientMmap(opts);
				if (clientMmap == null)
				{
					if (DFSClient.Log.IsDebugEnabled())
					{
						DFSClient.Log.Debug("unable to perform a zero-copy read from offset " + curPos + 
							" of " + src + "; BlockReader#getClientMmap returned " + "null.");
					}
					return null;
				}
				bool success = false;
				ByteBuffer buffer;
				try
				{
					Seek(curPos + length);
					buffer = clientMmap.GetMappedByteBuffer().AsReadOnlyBuffer();
					buffer.Position((int)blockPos);
					buffer.Limit((int)(blockPos + length));
					GetExtendedReadBuffers().Put(buffer, clientMmap);
					lock (infoLock)
					{
						readStatistics.AddZeroCopyBytes(length);
					}
					if (DFSClient.Log.IsDebugEnabled())
					{
						DFSClient.Log.Debug("readZeroCopy read " + length + " bytes from offset " + curPos
							 + " via the zero-copy read " + "path.  blockEnd = " + blockEnd);
					}
					success = true;
				}
				finally
				{
					if (!success)
					{
						IOUtils.CloseQuietly(clientMmap);
					}
				}
				return buffer;
			}
		}

		public virtual void ReleaseBuffer(ByteBuffer buffer)
		{
			lock (this)
			{
				if (buffer == EmptyBuffer)
				{
					return;
				}
				object val = GetExtendedReadBuffers().Remove(buffer);
				if (val == null)
				{
					throw new ArgumentException("tried to release a buffer " + "that was not created by this stream, "
						 + buffer);
				}
				if (val is ClientMmap)
				{
					IOUtils.CloseQuietly((ClientMmap)val);
				}
				else
				{
					if (val is ByteBufferPool)
					{
						((ByteBufferPool)val).PutBuffer(buffer);
					}
				}
			}
		}

		public virtual void Unbuffer()
		{
			lock (this)
			{
				CloseCurrentBlockReader();
			}
		}
	}
}
