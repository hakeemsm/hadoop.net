using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>BlockReaderLocalLegacy enables local short circuited reads.</summary>
	/// <remarks>
	/// BlockReaderLocalLegacy enables local short circuited reads. If the DFS client is on
	/// the same machine as the datanode, then the client can read files directly
	/// from the local file system rather than going through the datanode for better
	/// performance. <br />
	/// This is the legacy implementation based on HDFS-2246, which requires
	/// permissions on the datanode to be set so that clients can directly access the
	/// blocks. The new implementation based on HDFS-347 should be preferred on UNIX
	/// systems where the required native code has been implemented.<br />
	/// <see cref="BlockReaderLocalLegacy"/>
	/// works as follows:
	/// <ul>
	/// <li>The client performing short circuit reads must be configured at the
	/// datanode.</li>
	/// <li>The client gets the path to the file where block is stored using
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientDatanodeProtocol.GetBlockLocalPathInfo(Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, Org.Apache.Hadoop.Security.Token.Token{T})
	/// 	"/>
	/// RPC call</li>
	/// <li>Client uses kerberos authentication to connect to the datanode over RPC,
	/// if security is enabled.</li>
	/// </ul>
	/// </remarks>
	internal class BlockReaderLocalLegacy : BlockReader
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.BlockReaderLocalLegacy
			));

		private class LocalDatanodeInfo
		{
			private ClientDatanodeProtocol proxy = null;

			private readonly IDictionary<ExtendedBlock, BlockLocalPathInfo> cache;

			internal LocalDatanodeInfo()
			{
				//Stores the cache and proxy for a local datanode.
				int cacheSize = 10000;
				float hashTableLoadFactor = 0.75f;
				int hashTableCapacity = (int)Math.Ceil(cacheSize / hashTableLoadFactor) + 1;
				cache = Sharpen.Collections.SynchronizedMap(new _LinkedHashMap_90(this, cacheSize
					, hashTableCapacity, hashTableLoadFactor, true));
			}

			private sealed class _LinkedHashMap_90 : LinkedHashMap<ExtendedBlock, BlockLocalPathInfo
				>
			{
				public _LinkedHashMap_90(LocalDatanodeInfo _enclosing, int cacheSize, int baseArg1
					, float baseArg2, bool baseArg3)
					: base(baseArg1, baseArg2, baseArg3)
				{
					this._enclosing = _enclosing;
					this.cacheSize = cacheSize;
					this.serialVersionUID = 1;
				}

				private const long serialVersionUID;

				protected override bool RemoveEldestEntry(KeyValuePair<ExtendedBlock, BlockLocalPathInfo
					> eldest)
				{
					return this._enclosing._enclosing._enclosing.Count > cacheSize;
				}

				private readonly LocalDatanodeInfo _enclosing;

				private readonly int cacheSize;
			}

			/// <exception cref="System.IO.IOException"/>
			private ClientDatanodeProtocol GetDatanodeProxy(UserGroupInformation ugi, DatanodeInfo
				 node, Configuration conf, int socketTimeout, bool connectToDnViaHostname)
			{
				lock (this)
				{
					if (proxy == null)
					{
						try
						{
							proxy = ugi.DoAs(new _PrivilegedExceptionAction_107(node, conf, socketTimeout, connectToDnViaHostname
								));
						}
						catch (Exception e)
						{
							Log.Warn("encountered exception ", e);
						}
					}
					return proxy;
				}
			}

			private sealed class _PrivilegedExceptionAction_107 : PrivilegedExceptionAction<ClientDatanodeProtocol
				>
			{
				public _PrivilegedExceptionAction_107(DatanodeInfo node, Configuration conf, int 
					socketTimeout, bool connectToDnViaHostname)
				{
					this.node = node;
					this.conf = conf;
					this.socketTimeout = socketTimeout;
					this.connectToDnViaHostname = connectToDnViaHostname;
				}

				/// <exception cref="System.Exception"/>
				public ClientDatanodeProtocol Run()
				{
					return DFSUtil.CreateClientDatanodeProtocolProxy(node, conf, socketTimeout, connectToDnViaHostname
						);
				}

				private readonly DatanodeInfo node;

				private readonly Configuration conf;

				private readonly int socketTimeout;

				private readonly bool connectToDnViaHostname;
			}

			private void ResetDatanodeProxy()
			{
				lock (this)
				{
					if (null != proxy)
					{
						RPC.StopProxy(proxy);
						proxy = null;
					}
				}
			}

			private BlockLocalPathInfo GetBlockLocalPathInfo(ExtendedBlock b)
			{
				return cache[b];
			}

			private void SetBlockLocalPathInfo(ExtendedBlock b, BlockLocalPathInfo info)
			{
				cache[b] = info;
			}

			private void RemoveBlockLocalPathInfo(ExtendedBlock b)
			{
				Sharpen.Collections.Remove(cache, b);
			}
		}

		private static readonly IDictionary<int, BlockReaderLocalLegacy.LocalDatanodeInfo
			> localDatanodeInfoMap = new Dictionary<int, BlockReaderLocalLegacy.LocalDatanodeInfo
			>();

		private readonly FileInputStream dataIn;

		private readonly FileInputStream checksumIn;

		/// <summary>
		/// Offset from the most recent chunk boundary at which the next read should
		/// take place.
		/// </summary>
		/// <remarks>
		/// Offset from the most recent chunk boundary at which the next read should
		/// take place. Is only set to non-zero at construction time, and is
		/// decremented (usually to 0) by subsequent reads. This avoids having to do a
		/// checksum read at construction to position the read cursor correctly.
		/// </remarks>
		private int offsetFromChunkBoundary;

		private byte[] skipBuf = null;

		/// <summary>
		/// Used for checksummed reads that need to be staged before copying to their
		/// output buffer because they are either a) smaller than the checksum chunk
		/// size or b) issued by the slower read(byte[]...) path
		/// </summary>
		private ByteBuffer slowReadBuff = null;

		private ByteBuffer checksumBuff = null;

		private DataChecksum checksum;

		private readonly bool verifyChecksum;

		private static readonly DirectBufferPool bufferPool = new DirectBufferPool();

		private readonly int bytesPerChecksum;

		private readonly int checksumSize;

		/// <summary>offset in block where reader wants to actually read</summary>
		private long startOffset;

		private readonly string filename;

		private long blockId;

		// Multiple datanodes could be running on the local machine. Store proxies in
		// a map keyed by the ipc port of the datanode.
		// reader for the data file
		// reader for the checksum file
		/// <summary>The only way this object can be instantiated.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static BlockReaderLocalLegacy NewBlockReader(DFSClient.Conf conf, UserGroupInformation
			 userGroupInformation, Configuration configuration, string file, ExtendedBlock blk
			, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token, DatanodeInfo
			 node, long startOffset, long length, StorageType storageType)
		{
			BlockReaderLocalLegacy.LocalDatanodeInfo localDatanodeInfo = GetLocalDatanodeInfo
				(node.GetIpcPort());
			// check the cache first
			BlockLocalPathInfo pathinfo = localDatanodeInfo.GetBlockLocalPathInfo(blk);
			if (pathinfo == null)
			{
				if (userGroupInformation == null)
				{
					userGroupInformation = UserGroupInformation.GetCurrentUser();
				}
				pathinfo = GetBlockPathInfo(userGroupInformation, blk, node, configuration, conf.
					socketTimeout, token, conf.connectToDnViaHostname, storageType);
			}
			// check to see if the file exists. It may so happen that the
			// HDFS file has been deleted and this block-lookup is occurring
			// on behalf of a new HDFS file. This time, the block file could
			// be residing in a different portion of the fs.data.dir directory.
			// In this case, we remove this entry from the cache. The next
			// call to this method will re-populate the cache.
			FileInputStream dataIn = null;
			FileInputStream checksumIn = null;
			BlockReaderLocalLegacy localBlockReader = null;
			bool skipChecksumCheck = conf.skipShortCircuitChecksums || storageType.IsTransient
				();
			try
			{
				// get a local file system
				FilePath blkfile = new FilePath(pathinfo.GetBlockPath());
				dataIn = new FileInputStream(blkfile);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("New BlockReaderLocalLegacy for file " + blkfile + " of size " + blkfile
						.Length() + " startOffset " + startOffset + " length " + length + " short circuit checksum "
						 + !skipChecksumCheck);
				}
				if (!skipChecksumCheck)
				{
					// get the metadata file
					FilePath metafile = new FilePath(pathinfo.GetMetaPath());
					checksumIn = new FileInputStream(metafile);
					DataChecksum checksum = BlockMetadataHeader.ReadDataChecksum(new DataInputStream(
						checksumIn), blk);
					long firstChunkOffset = startOffset - (startOffset % checksum.GetBytesPerChecksum
						());
					localBlockReader = new BlockReaderLocalLegacy(conf, file, blk, token, startOffset
						, length, pathinfo, checksum, true, dataIn, firstChunkOffset, checksumIn);
				}
				else
				{
					localBlockReader = new BlockReaderLocalLegacy(conf, file, blk, token, startOffset
						, length, pathinfo, dataIn);
				}
			}
			catch (IOException e)
			{
				// remove from cache
				localDatanodeInfo.RemoveBlockLocalPathInfo(blk);
				DFSClient.Log.Warn("BlockReaderLocalLegacy: Removing " + blk + " from cache because local file "
					 + pathinfo.GetBlockPath() + " could not be opened.");
				throw;
			}
			finally
			{
				if (localBlockReader == null)
				{
					if (dataIn != null)
					{
						dataIn.Close();
					}
					if (checksumIn != null)
					{
						checksumIn.Close();
					}
				}
			}
			return localBlockReader;
		}

		private static BlockReaderLocalLegacy.LocalDatanodeInfo GetLocalDatanodeInfo(int 
			port)
		{
			lock (typeof(BlockReaderLocalLegacy))
			{
				BlockReaderLocalLegacy.LocalDatanodeInfo ldInfo = localDatanodeInfoMap[port];
				if (ldInfo == null)
				{
					ldInfo = new BlockReaderLocalLegacy.LocalDatanodeInfo();
					localDatanodeInfoMap[port] = ldInfo;
				}
				return ldInfo;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static BlockLocalPathInfo GetBlockPathInfo(UserGroupInformation ugi, ExtendedBlock
			 blk, DatanodeInfo node, Configuration conf, int timeout, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> token, bool connectToDnViaHostname, StorageType storageType
			)
		{
			BlockReaderLocalLegacy.LocalDatanodeInfo localDatanodeInfo = GetLocalDatanodeInfo
				(node.GetIpcPort());
			BlockLocalPathInfo pathinfo = null;
			ClientDatanodeProtocol proxy = localDatanodeInfo.GetDatanodeProxy(ugi, node, conf
				, timeout, connectToDnViaHostname);
			try
			{
				// make RPC to local datanode to find local pathnames of blocks
				pathinfo = proxy.GetBlockLocalPathInfo(blk, token);
				// We cannot cache the path information for a replica on transient storage.
				// If the replica gets evicted, then it moves to a different path.  Then,
				// our next attempt to read from the cached path would fail to find the
				// file.  Additionally, the failure would cause us to disable legacy
				// short-circuit read for all subsequent use in the ClientContext.  Unlike
				// the newer short-circuit read implementation, we have no communication
				// channel for the DataNode to notify the client that the path has been
				// invalidated.  Therefore, our only option is to skip caching.
				if (pathinfo != null && !storageType.IsTransient())
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Cached location of block " + blk + " as " + pathinfo);
					}
					localDatanodeInfo.SetBlockLocalPathInfo(blk, pathinfo);
				}
			}
			catch (IOException e)
			{
				localDatanodeInfo.ResetDatanodeProxy();
				// Reset proxy on error
				throw;
			}
			return pathinfo;
		}

		private static int GetSlowReadBufferNumChunks(int bufferSizeBytes, int bytesPerChecksum
			)
		{
			if (bufferSizeBytes < bytesPerChecksum)
			{
				throw new ArgumentException("Configured BlockReaderLocalLegacy " + "buffer size ("
					 + bufferSizeBytes + ") is not large enough to hold " + "a single chunk (" + bytesPerChecksum
					 + "). Please configure " + DFSConfigKeys.DfsClientReadShortcircuitBufferSizeKey
					 + " appropriately");
			}
			// Round down to nearest chunk size
			return bufferSizeBytes / bytesPerChecksum;
		}

		/// <exception cref="System.IO.IOException"/>
		private BlockReaderLocalLegacy(DFSClient.Conf conf, string hdfsfile, ExtendedBlock
			 block, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token, long
			 startOffset, long length, BlockLocalPathInfo pathinfo, FileInputStream dataIn)
			: this(conf, hdfsfile, block, token, startOffset, length, pathinfo, DataChecksum.
				NewDataChecksum(DataChecksum.Type.Null, 4), false, dataIn, startOffset, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private BlockReaderLocalLegacy(DFSClient.Conf conf, string hdfsfile, ExtendedBlock
			 block, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token, long
			 startOffset, long length, BlockLocalPathInfo pathinfo, DataChecksum checksum, bool
			 verifyChecksum, FileInputStream dataIn, long firstChunkOffset, FileInputStream 
			checksumIn)
		{
			this.filename = hdfsfile;
			this.checksum = checksum;
			this.verifyChecksum = verifyChecksum;
			this.startOffset = Math.Max(startOffset, 0);
			this.blockId = block.GetBlockId();
			bytesPerChecksum = this.checksum.GetBytesPerChecksum();
			checksumSize = this.checksum.GetChecksumSize();
			this.dataIn = dataIn;
			this.checksumIn = checksumIn;
			this.offsetFromChunkBoundary = (int)(startOffset - firstChunkOffset);
			int chunksPerChecksumRead = GetSlowReadBufferNumChunks(conf.shortCircuitBufferSize
				, bytesPerChecksum);
			slowReadBuff = bufferPool.GetBuffer(bytesPerChecksum * chunksPerChecksumRead);
			checksumBuff = bufferPool.GetBuffer(checksumSize * chunksPerChecksumRead);
			// Initially the buffers have nothing to read.
			slowReadBuff.Flip();
			checksumBuff.Flip();
			bool success = false;
			try
			{
				// Skip both input streams to beginning of the chunk containing startOffset
				IOUtils.SkipFully(dataIn, firstChunkOffset);
				if (checksumIn != null)
				{
					long checkSumOffset = (firstChunkOffset / bytesPerChecksum) * checksumSize;
					IOUtils.SkipFully(checksumIn, checkSumOffset);
				}
				success = true;
			}
			finally
			{
				if (!success)
				{
					bufferPool.ReturnBuffer(slowReadBuff);
					bufferPool.ReturnBuffer(checksumBuff);
				}
			}
		}

		/// <summary>Reads bytes into a buffer until EOF or the buffer's limit is reached</summary>
		/// <exception cref="System.IO.IOException"/>
		private int FillBuffer(FileInputStream stream, ByteBuffer buf)
		{
			TraceScope scope = Trace.StartSpan("BlockReaderLocalLegacy#fillBuffer(" + blockId
				 + ")", Sampler.Never);
			try
			{
				int bytesRead = stream.GetChannel().Read(buf);
				if (bytesRead < 0)
				{
					//EOF
					return bytesRead;
				}
				while (buf.Remaining() > 0)
				{
					int n = stream.GetChannel().Read(buf);
					if (n < 0)
					{
						//EOF
						return bytesRead;
					}
					bytesRead += n;
				}
				return bytesRead;
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>
		/// Utility method used by read(ByteBuffer) to partially copy a ByteBuffer into
		/// another.
		/// </summary>
		private void WriteSlice(ByteBuffer from, ByteBuffer to, int length)
		{
			int oldLimit = from.Limit();
			from.Limit(from.Position() + length);
			try
			{
				to.Put(from);
			}
			finally
			{
				from.Limit(oldLimit);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(ByteBuffer buf)
		{
			lock (this)
			{
				int nRead = 0;
				if (verifyChecksum)
				{
					// A 'direct' read actually has three phases. The first drains any
					// remaining bytes from the slow read buffer. After this the read is
					// guaranteed to be on a checksum chunk boundary. If there are still bytes
					// to read, the fast direct path is used for as many remaining bytes as
					// possible, up to a multiple of the checksum chunk size. Finally, any
					// 'odd' bytes remaining at the end of the read cause another slow read to
					// be issued, which involves an extra copy.
					// Every 'slow' read tries to fill the slow read buffer in one go for
					// efficiency's sake. As described above, all non-checksum-chunk-aligned
					// reads will be served from the slower read path.
					if (slowReadBuff.HasRemaining())
					{
						// There are remaining bytes from a small read available. This usually
						// means this read is unaligned, which falls back to the slow path.
						int fromSlowReadBuff = Math.Min(buf.Remaining(), slowReadBuff.Remaining());
						WriteSlice(slowReadBuff, buf, fromSlowReadBuff);
						nRead += fromSlowReadBuff;
					}
					if (buf.Remaining() >= bytesPerChecksum && offsetFromChunkBoundary == 0)
					{
						// Since we have drained the 'small read' buffer, we are guaranteed to
						// be chunk-aligned
						int len = buf.Remaining() - (buf.Remaining() % bytesPerChecksum);
						// There's only enough checksum buffer space available to checksum one
						// entire slow read buffer. This saves keeping the number of checksum
						// chunks around.
						len = Math.Min(len, slowReadBuff.Capacity());
						int oldlimit = buf.Limit();
						buf.Limit(buf.Position() + len);
						int readResult = 0;
						try
						{
							readResult = DoByteBufferRead(buf);
						}
						finally
						{
							buf.Limit(oldlimit);
						}
						if (readResult == -1)
						{
							return nRead;
						}
						else
						{
							nRead += readResult;
							buf.Position(buf.Position() + readResult);
						}
					}
					// offsetFromChunkBoundary > 0 => unaligned read, use slow path to read
					// until chunk boundary
					if ((buf.Remaining() > 0 && buf.Remaining() < bytesPerChecksum) || offsetFromChunkBoundary
						 > 0)
					{
						int toRead = Math.Min(buf.Remaining(), bytesPerChecksum - offsetFromChunkBoundary
							);
						int readResult = FillSlowReadBuffer(toRead);
						if (readResult == -1)
						{
							return nRead;
						}
						else
						{
							int fromSlowReadBuff = Math.Min(readResult, buf.Remaining());
							WriteSlice(slowReadBuff, buf, fromSlowReadBuff);
							nRead += fromSlowReadBuff;
						}
					}
				}
				else
				{
					// Non-checksummed reads are much easier; we can just fill the buffer directly.
					nRead = DoByteBufferRead(buf);
					if (nRead > 0)
					{
						buf.Position(buf.Position() + nRead);
					}
				}
				return nRead;
			}
		}

		/// <summary>
		/// Tries to read as many bytes as possible into supplied buffer, checksumming
		/// each chunk if needed.
		/// </summary>
		/// <remarks>
		/// Tries to read as many bytes as possible into supplied buffer, checksumming
		/// each chunk if needed.
		/// <b>Preconditions:</b>
		/// <ul>
		/// <li>
		/// If checksumming is enabled, buf.remaining must be a multiple of
		/// bytesPerChecksum. Note that this is not a requirement for clients of
		/// read(ByteBuffer) - in the case of non-checksum-sized read requests,
		/// read(ByteBuffer) will substitute a suitably sized buffer to pass to this
		/// method.
		/// </li>
		/// </ul>
		/// <b>Postconditions:</b>
		/// <ul>
		/// <li>buf.limit and buf.mark are unchanged.</li>
		/// <li>buf.position += min(offsetFromChunkBoundary, totalBytesRead) - so the
		/// requested bytes can be read straight from the buffer</li>
		/// </ul>
		/// </remarks>
		/// <param name="buf">
		/// byte buffer to write bytes to. If checksums are not required, buf
		/// can have any number of bytes remaining, otherwise there must be a
		/// multiple of the checksum chunk size remaining.
		/// </param>
		/// <returns>
		/// <tt>max(min(totalBytesRead, len) - offsetFromChunkBoundary, 0)</tt>
		/// that is, the the number of useful bytes (up to the amount
		/// requested) readable from the buffer by the client.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private int DoByteBufferRead(ByteBuffer buf)
		{
			lock (this)
			{
				if (verifyChecksum)
				{
					System.Diagnostics.Debug.Assert(buf.Remaining() % bytesPerChecksum == 0);
				}
				int dataRead = -1;
				int oldpos = buf.Position();
				// Read as much as we can into the buffer.
				dataRead = FillBuffer(dataIn, buf);
				if (dataRead == -1)
				{
					return -1;
				}
				if (verifyChecksum)
				{
					ByteBuffer toChecksum = buf.Duplicate();
					toChecksum.Position(oldpos);
					toChecksum.Limit(oldpos + dataRead);
					checksumBuff.Clear();
					// Equivalent to (int)Math.ceil(toChecksum.remaining() * 1.0 / bytesPerChecksum );
					int numChunks = (toChecksum.Remaining() + bytesPerChecksum - 1) / bytesPerChecksum;
					checksumBuff.Limit(checksumSize * numChunks);
					FillBuffer(checksumIn, checksumBuff);
					checksumBuff.Flip();
					checksum.VerifyChunkedSums(toChecksum, checksumBuff, filename, this.startOffset);
				}
				if (dataRead >= 0)
				{
					buf.Position(oldpos + Math.Min(offsetFromChunkBoundary, dataRead));
				}
				if (dataRead < offsetFromChunkBoundary)
				{
					// yikes, didn't even get enough bytes to honour offset. This can happen
					// even if we are verifying checksums if we are at EOF.
					offsetFromChunkBoundary -= dataRead;
					dataRead = 0;
				}
				else
				{
					dataRead -= offsetFromChunkBoundary;
					offsetFromChunkBoundary = 0;
				}
				return dataRead;
			}
		}

		/// <summary>
		/// Ensures that up to len bytes are available and checksummed in the slow read
		/// buffer.
		/// </summary>
		/// <remarks>
		/// Ensures that up to len bytes are available and checksummed in the slow read
		/// buffer. The number of bytes available to read is returned. If the buffer is
		/// not already empty, the number of remaining bytes is returned and no actual
		/// read happens.
		/// </remarks>
		/// <param name="len">
		/// the maximum number of bytes to make available. After len bytes
		/// are read, the underlying bytestream <b>must</b> be at a checksum
		/// boundary, or EOF. That is, (len + currentPosition) %
		/// bytesPerChecksum == 0.
		/// </param>
		/// <returns>the number of bytes available to read, or -1 if EOF.</returns>
		/// <exception cref="System.IO.IOException"/>
		private int FillSlowReadBuffer(int len)
		{
			lock (this)
			{
				int nRead = -1;
				if (slowReadBuff.HasRemaining())
				{
					// Already got data, good to go.
					nRead = Math.Min(len, slowReadBuff.Remaining());
				}
				else
				{
					// Round a complete read of len bytes (plus any implicit offset) to the
					// next chunk boundary, since we try and read in multiples of a chunk
					int nextChunk = len + offsetFromChunkBoundary + (bytesPerChecksum - ((len + offsetFromChunkBoundary
						) % bytesPerChecksum));
					int limit = Math.Min(nextChunk, slowReadBuff.Capacity());
					System.Diagnostics.Debug.Assert(limit % bytesPerChecksum == 0);
					slowReadBuff.Clear();
					slowReadBuff.Limit(limit);
					nRead = DoByteBufferRead(slowReadBuff);
					if (nRead > 0)
					{
						// So that next time we call slowReadBuff.hasRemaining(), we don't get a
						// false positive.
						slowReadBuff.Limit(nRead + slowReadBuff.Position());
					}
				}
				return nRead;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(byte[] buf, int off, int len)
		{
			lock (this)
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace("read off " + off + " len " + len);
				}
				if (!verifyChecksum)
				{
					return dataIn.Read(buf, off, len);
				}
				int nRead = FillSlowReadBuffer(slowReadBuff.Capacity());
				if (nRead > 0)
				{
					// Possible that buffer is filled with a larger read than we need, since
					// we tried to read as much as possible at once
					nRead = Math.Min(len, nRead);
					slowReadBuff.Get(buf, off, nRead);
				}
				return nRead;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long Skip(long n)
		{
			lock (this)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("skip " + n);
				}
				if (n <= 0)
				{
					return 0;
				}
				if (!verifyChecksum)
				{
					return dataIn.Skip(n);
				}
				// caller made sure newPosition is not beyond EOF.
				int remaining = slowReadBuff.Remaining();
				int position = slowReadBuff.Position();
				int newPosition = position + (int)n;
				// if the new offset is already read into dataBuff, just reposition
				if (n <= remaining)
				{
					System.Diagnostics.Debug.Assert(offsetFromChunkBoundary == 0);
					slowReadBuff.Position(newPosition);
					return n;
				}
				// for small gap, read through to keep the data/checksum in sync
				if (n - remaining <= bytesPerChecksum)
				{
					slowReadBuff.Position(position + remaining);
					if (skipBuf == null)
					{
						skipBuf = new byte[bytesPerChecksum];
					}
					int ret = Read(skipBuf, 0, (int)(n - remaining));
					return (remaining + ret);
				}
				// optimize for big gap: discard the current buffer, skip to
				// the beginning of the appropriate checksum chunk and then
				// read to the middle of that chunk to be in sync with checksums.
				// We can't use this.offsetFromChunkBoundary because we need to know how
				// many bytes of the offset were really read. Calling read(..) with a
				// positive this.offsetFromChunkBoundary causes that many bytes to get
				// silently skipped.
				int myOffsetFromChunkBoundary = newPosition % bytesPerChecksum;
				long toskip = n - remaining - myOffsetFromChunkBoundary;
				slowReadBuff.Position(slowReadBuff.Limit());
				checksumBuff.Position(checksumBuff.Limit());
				IOUtils.SkipFully(dataIn, toskip);
				long checkSumOffset = (toskip / bytesPerChecksum) * checksumSize;
				IOUtils.SkipFully(checksumIn, checkSumOffset);
				// read into the middle of the chunk
				if (skipBuf == null)
				{
					skipBuf = new byte[bytesPerChecksum];
				}
				System.Diagnostics.Debug.Assert(skipBuf.Length == bytesPerChecksum);
				System.Diagnostics.Debug.Assert(myOffsetFromChunkBoundary < bytesPerChecksum);
				int ret_1 = Read(skipBuf, 0, myOffsetFromChunkBoundary);
				if (ret_1 == -1)
				{
					// EOS
					return (toskip + remaining);
				}
				else
				{
					return (toskip + remaining + ret_1);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				IOUtils.Cleanup(Log, dataIn, checksumIn);
				if (slowReadBuff != null)
				{
					bufferPool.ReturnBuffer(slowReadBuff);
					slowReadBuff = null;
				}
				if (checksumBuff != null)
				{
					bufferPool.ReturnBuffer(checksumBuff);
					checksumBuff = null;
				}
				startOffset = -1;
				checksum = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int ReadAll(byte[] buf, int offset, int len)
		{
			return BlockReaderUtil.ReadAll(this, buf, offset, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(byte[] buf, int off, int len)
		{
			BlockReaderUtil.ReadFully(this, buf, off, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Available()
		{
			// We never do network I/O in BlockReaderLocalLegacy.
			return int.MaxValue;
		}

		public virtual bool IsLocal()
		{
			return true;
		}

		public virtual bool IsShortCircuit()
		{
			return true;
		}

		public virtual ClientMmap GetClientMmap(EnumSet<ReadOption> opts)
		{
			return null;
		}
	}
}
