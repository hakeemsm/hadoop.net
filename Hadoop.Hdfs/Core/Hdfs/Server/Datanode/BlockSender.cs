using System;
using System.IO;
using System.Net.Sockets;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Reads a block from the disk and sends it to a recipient.</summary>
	/// <remarks>
	/// Reads a block from the disk and sends it to a recipient.
	/// Data sent from the BlockeSender in the following format:
	/// <br /><b>Data format:</b> <pre>
	/// +--------------------------------------------------+
	/// | ChecksumHeader | Sequence of data PACKETS...     |
	/// +--------------------------------------------------+
	/// </pre>
	/// <b>ChecksumHeader format:</b> <pre>
	/// +--------------------------------------------------+
	/// | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
	/// +--------------------------------------------------+
	/// </pre>
	/// An empty packet is sent to mark the end of block and read completion.
	/// PACKET Contains a packet header, checksum and data. Amount of data
	/// carried is set by BUFFER_SIZE.
	/// <pre>
	/// +-----------------------------------------------------+
	/// | Variable length header. See
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.PacketHeader"/>
	/// |
	/// +-----------------------------------------------------+
	/// | x byte checksum data. x is defined below            |
	/// +-----------------------------------------------------+
	/// | actual data ......                                  |
	/// +-----------------------------------------------------+
	/// Data is made of Chunks. Each chunk is of length &lt;= BYTES_PER_CHECKSUM.
	/// A checksum is calculated for each chunk.
	/// x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM
	/// CHECKSUM_SIZE
	/// CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32)
	/// </pre>
	/// The client reads data until it receives a packet with
	/// "LastPacketInBlock" set to true or with a zero length. If there is
	/// no checksum error, it replies to DataNode with OP_STATUS_CHECKSUM_OK.
	/// </remarks>
	internal class BlockSender : IDisposable
	{
		internal static readonly Log Log = DataNode.Log;

		internal static readonly Log ClientTraceLog = DataNode.ClientTraceLog;

		private static readonly bool is32Bit = Runtime.GetProperty("sun.arch.data.model")
			.Equals("32");

		/// <summary>Minimum buffer used while sending data to clients.</summary>
		/// <remarks>
		/// Minimum buffer used while sending data to clients. Used only if
		/// transferTo() is enabled. 64KB is not that large. It could be larger, but
		/// not sure if there will be much more improvement.
		/// </remarks>
		private const int MinBufferWithTransferto = 64 * 1024;

		private static readonly int TransfertoBufferSize = Math.Max(HdfsConstants.IoFileBufferSize
			, MinBufferWithTransferto);

		/// <summary>the block to read from</summary>
		private readonly ExtendedBlock block;

		/// <summary>Stream to read block data from</summary>
		private InputStream blockIn;

		/// <summary>updated while using transferTo()</summary>
		private long blockInPosition = -1;

		/// <summary>Stream to read checksum</summary>
		private DataInputStream checksumIn;

		/// <summary>Checksum utility</summary>
		private readonly DataChecksum checksum;

		/// <summary>Initial position to read</summary>
		private long initialOffset;

		/// <summary>Current position of read</summary>
		private long offset;

		/// <summary>Position of last byte to read from block file</summary>
		private readonly long endOffset;

		/// <summary>Number of bytes in chunk used for computing checksum</summary>
		private readonly int chunkSize;

		/// <summary>Number bytes of checksum computed for a chunk</summary>
		private readonly int checksumSize;

		/// <summary>If true, failure to read checksum is ignored</summary>
		private readonly bool corruptChecksumOk;

		/// <summary>Sequence number of packet being sent</summary>
		private long seqno;

		/// <summary>Set to true if transferTo is allowed for sending data to the client</summary>
		private readonly bool transferToAllowed;

		/// <summary>Set to true once entire requested byte range has been sent to the client
		/// 	</summary>
		private bool sentEntireByteRange;

		/// <summary>When true, verify checksum while reading from checksum file</summary>
		private readonly bool verifyChecksum;

		/// <summary>Format used to print client trace log messages</summary>
		private readonly string clientTraceFmt;

		private volatile ChunkChecksum lastChunkChecksum = null;

		private DataNode datanode;

		/// <summary>The file descriptor of the block being sent</summary>
		private FileDescriptor blockInFd;

		/// <summary>The reference to the volume where the block is located</summary>
		private FsVolumeReference volumeRef;

		private readonly long readaheadLength;

		private ReadaheadPool.ReadaheadRequest curReadahead;

		private readonly bool alwaysReadahead;

		private readonly bool dropCacheBehindLargeReads;

		private readonly bool dropCacheBehindAllReads;

		private long lastCacheDropOffset;

		[VisibleForTesting]
		internal static long CacheDropIntervalBytes = 1024 * 1024;

		/// <summary>
		/// See {
		/// <see cref="IsLongRead()"/>
		/// </summary>
		private const long LongReadThresholdBytes = 256 * 1024;

		/// <summary>Constructor</summary>
		/// <param name="block">Block that is being read</param>
		/// <param name="startOffset">starting offset to read from</param>
		/// <param name="length">length of data to read</param>
		/// <param name="corruptChecksumOk">if true, corrupt checksum is okay</param>
		/// <param name="verifyChecksum">verify checksum while reading the data</param>
		/// <param name="sendChecksum">send checksum to client.</param>
		/// <param name="datanode">datanode from which the block is being read</param>
		/// <param name="clientTraceFmt">format string used to print client trace logs</param>
		/// <exception cref="System.IO.IOException"/>
		internal BlockSender(ExtendedBlock block, long startOffset, long length, bool corruptChecksumOk
			, bool verifyChecksum, bool sendChecksum, DataNode datanode, string clientTraceFmt
			, CachingStrategy cachingStrategy)
		{
			// Cache-management related fields
			// 1MB
			try
			{
				this.block = block;
				this.corruptChecksumOk = corruptChecksumOk;
				this.verifyChecksum = verifyChecksum;
				this.clientTraceFmt = clientTraceFmt;
				/*
				* If the client asked for the cache to be dropped behind all reads,
				* we honor that.  Otherwise, we use the DataNode defaults.
				* When using DataNode defaults, we use a heuristic where we only
				* drop the cache for large reads.
				*/
				if (cachingStrategy.GetDropBehind() == null)
				{
					this.dropCacheBehindAllReads = false;
					this.dropCacheBehindLargeReads = datanode.GetDnConf().dropCacheBehindReads;
				}
				else
				{
					this.dropCacheBehindAllReads = this.dropCacheBehindLargeReads = cachingStrategy.GetDropBehind
						();
				}
				/*
				* Similarly, if readahead was explicitly requested, we always do it.
				* Otherwise, we read ahead based on the DataNode settings, and only
				* when the reads are large.
				*/
				if (cachingStrategy.GetReadahead() == null)
				{
					this.alwaysReadahead = false;
					this.readaheadLength = datanode.GetDnConf().readaheadLength;
				}
				else
				{
					this.alwaysReadahead = true;
					this.readaheadLength = cachingStrategy.GetReadahead();
				}
				this.datanode = datanode;
				if (verifyChecksum)
				{
					// To simplify implementation, callers may not specify verification
					// without sending.
					Preconditions.CheckArgument(sendChecksum, "If verifying checksum, currently must also send it."
						);
				}
				Replica replica;
				long replicaVisibleLength;
				lock (datanode.data)
				{
					replica = GetReplica(block, datanode);
					replicaVisibleLength = replica.GetVisibleLength();
				}
				// if there is a write in progress
				ChunkChecksum chunkChecksum = null;
				if (replica is ReplicaBeingWritten)
				{
					ReplicaBeingWritten rbw = (ReplicaBeingWritten)replica;
					WaitForMinLength(rbw, startOffset + length);
					chunkChecksum = rbw.GetLastChecksumAndDataLen();
				}
				if (replica.GetGenerationStamp() < block.GetGenerationStamp())
				{
					throw new IOException("Replica gen stamp < block genstamp, block=" + block + ", replica="
						 + replica);
				}
				else
				{
					if (replica.GetGenerationStamp() > block.GetGenerationStamp())
					{
						if (DataNode.Log.IsDebugEnabled())
						{
							DataNode.Log.Debug("Bumping up the client provided" + " block's genstamp to latest "
								 + replica.GetGenerationStamp() + " for block " + block);
						}
						block.SetGenerationStamp(replica.GetGenerationStamp());
					}
				}
				if (replicaVisibleLength < 0)
				{
					throw new IOException("Replica is not readable, block=" + block + ", replica=" + 
						replica);
				}
				if (DataNode.Log.IsDebugEnabled())
				{
					DataNode.Log.Debug("block=" + block + ", replica=" + replica);
				}
				// transferToFully() fails on 32 bit platforms for block sizes >= 2GB,
				// use normal transfer in those cases
				this.transferToAllowed = datanode.GetDnConf().transferToAllowed && (!is32Bit || length
					 <= int.MaxValue);
				// Obtain a reference before reading data
				this.volumeRef = datanode.data.GetVolume(block).ObtainReference();
				/*
				* (corruptChecksumOK, meta_file_exist): operation
				* True,   True: will verify checksum
				* True,  False: No verify, e.g., need to read data from a corrupted file
				* False,  True: will verify checksum
				* False, False: throws IOException file not found
				*/
				DataChecksum csum = null;
				if (verifyChecksum || sendChecksum)
				{
					LengthInputStream metaIn = null;
					bool keepMetaInOpen = false;
					try
					{
						metaIn = datanode.data.GetMetaDataInputStream(block);
						if (!corruptChecksumOk || metaIn != null)
						{
							if (metaIn == null)
							{
								//need checksum but meta-data not found
								throw new FileNotFoundException("Meta-data not found for " + block);
							}
							// The meta file will contain only the header if the NULL checksum
							// type was used, or if the replica was written to transient storage.
							// Checksum verification is not performed for replicas on transient
							// storage.  The header is important for determining the checksum
							// type later when lazy persistence copies the block to non-transient
							// storage and computes the checksum.
							if (metaIn.GetLength() > BlockMetadataHeader.GetHeaderSize())
							{
								checksumIn = new DataInputStream(new BufferedInputStream(metaIn, HdfsConstants.IoFileBufferSize
									));
								csum = BlockMetadataHeader.ReadDataChecksum(checksumIn, block);
								keepMetaInOpen = true;
							}
						}
						else
						{
							Log.Warn("Could not find metadata file for " + block);
						}
					}
					finally
					{
						if (!keepMetaInOpen)
						{
							IOUtils.CloseStream(metaIn);
						}
					}
				}
				if (csum == null)
				{
					// The number of bytes per checksum here determines the alignment
					// of reads: we always start reading at a checksum chunk boundary,
					// even if the checksum type is NULL. So, choosing too big of a value
					// would risk sending too much unnecessary data. 512 (1 disk sector)
					// is likely to result in minimal extra IO.
					csum = DataChecksum.NewDataChecksum(DataChecksum.Type.Null, 512);
				}
				/*
				* If chunkSize is very large, then the metadata file is mostly
				* corrupted. For now just truncate bytesPerchecksum to blockLength.
				*/
				int size = csum.GetBytesPerChecksum();
				if (size > 10 * 1024 * 1024 && size > replicaVisibleLength)
				{
					csum = DataChecksum.NewDataChecksum(csum.GetChecksumType(), Math.Max((int)replicaVisibleLength
						, 10 * 1024 * 1024));
					size = csum.GetBytesPerChecksum();
				}
				chunkSize = size;
				checksum = csum;
				checksumSize = checksum.GetChecksumSize();
				length = length < 0 ? replicaVisibleLength : length;
				// end is either last byte on disk or the length for which we have a 
				// checksum
				long end = chunkChecksum != null ? chunkChecksum.GetDataLength() : replica.GetBytesOnDisk
					();
				if (startOffset < 0 || startOffset > end || (length + startOffset) > end)
				{
					string msg = " Offset " + startOffset + " and length " + length + " don't match block "
						 + block + " ( blockLen " + end + " )";
					Log.Warn(datanode.GetDNRegistrationForBP(block.GetBlockPoolId()) + ":sendBlock() : "
						 + msg);
					throw new IOException(msg);
				}
				// Ensure read offset is position at the beginning of chunk
				offset = startOffset - (startOffset % chunkSize);
				if (length >= 0)
				{
					// Ensure endOffset points to end of chunk.
					long tmpLen = startOffset + length;
					if (tmpLen % chunkSize != 0)
					{
						tmpLen += (chunkSize - tmpLen % chunkSize);
					}
					if (tmpLen < end)
					{
						// will use on-disk checksum here since the end is a stable chunk
						end = tmpLen;
					}
					else
					{
						if (chunkChecksum != null)
						{
							// last chunk is changing. flag that we need to use in-memory checksum 
							this.lastChunkChecksum = chunkChecksum;
						}
					}
				}
				endOffset = end;
				// seek to the right offsets
				if (offset > 0 && checksumIn != null)
				{
					long checksumSkip = (offset / chunkSize) * checksumSize;
					// note blockInStream is seeked when created below
					if (checksumSkip > 0)
					{
						// Should we use seek() for checksum file as well?
						IOUtils.SkipFully(checksumIn, checksumSkip);
					}
				}
				seqno = 0;
				if (DataNode.Log.IsDebugEnabled())
				{
					DataNode.Log.Debug("replica=" + replica);
				}
				blockIn = datanode.data.GetBlockInputStream(block, offset);
				// seek to offset
				if (blockIn is FileInputStream)
				{
					blockInFd = ((FileInputStream)blockIn).GetFD();
				}
				else
				{
					blockInFd = null;
				}
			}
			catch (IOException ioe)
			{
				IOUtils.CloseStream(this);
				IOUtils.CloseStream(blockIn);
				throw;
			}
		}

		/// <summary>close opened files.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			if (blockInFd != null && ((dropCacheBehindAllReads) || (dropCacheBehindLargeReads
				 && IsLongRead())))
			{
				try
				{
					NativeIO.POSIX.GetCacheManipulator().PosixFadviseIfPossible(block.GetBlockName(), 
						blockInFd, lastCacheDropOffset, offset - lastCacheDropOffset, NativeIO.POSIX.PosixFadvDontneed
						);
				}
				catch (Exception e)
				{
					Log.Warn("Unable to drop cache on file close", e);
				}
			}
			if (curReadahead != null)
			{
				curReadahead.Cancel();
			}
			IOException ioe = null;
			if (checksumIn != null)
			{
				try
				{
					checksumIn.Close();
				}
				catch (IOException e)
				{
					// close checksum file
					ioe = e;
				}
				checksumIn = null;
			}
			if (blockIn != null)
			{
				try
				{
					blockIn.Close();
				}
				catch (IOException e)
				{
					// close data file
					ioe = e;
				}
				blockIn = null;
				blockInFd = null;
			}
			if (volumeRef != null)
			{
				IOUtils.Cleanup(null, volumeRef);
				volumeRef = null;
			}
			// throw IOException if there is any
			if (ioe != null)
			{
				throw ioe;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaNotFoundException"
		/// 	/>
		private static Replica GetReplica(ExtendedBlock block, DataNode datanode)
		{
			Replica replica = datanode.data.GetReplica(block.GetBlockPoolId(), block.GetBlockId
				());
			if (replica == null)
			{
				throw new ReplicaNotFoundException(block);
			}
			return replica;
		}

		/// <summary>Wait for rbw replica to reach the length</summary>
		/// <param name="rbw">replica that is being written to</param>
		/// <param name="len">minimum length to reach</param>
		/// <exception cref="System.IO.IOException">on failing to reach the len in given wait time
		/// 	</exception>
		private static void WaitForMinLength(ReplicaBeingWritten rbw, long len)
		{
			// Wait for 3 seconds for rbw replica to reach the minimum length
			for (int i = 0; i < 30 && rbw.GetBytesOnDisk() < len; i++)
			{
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
			}
			long bytesOnDisk = rbw.GetBytesOnDisk();
			if (bytesOnDisk < len)
			{
				throw new IOException(string.Format("Need %d bytes, but only %d bytes available", 
					len, bytesOnDisk));
			}
		}

		/// <summary>Converts an IOExcpetion (not subclasses) to SocketException.</summary>
		/// <remarks>
		/// Converts an IOExcpetion (not subclasses) to SocketException.
		/// This is typically done to indicate to upper layers that the error
		/// was a socket error rather than often more serious exceptions like
		/// disk errors.
		/// </remarks>
		private static IOException IoeToSocketException(IOException ioe)
		{
			if (ioe.GetType().Equals(typeof(IOException)))
			{
				// "se" could be a new class in stead of SocketException.
				IOException se = new SocketException("Original Exception : " + ioe);
				Sharpen.Extensions.InitCause(se, ioe);
				/* Change the stacktrace so that original trace is not truncated
				* when printed.*/
				se.SetStackTrace(ioe.GetStackTrace());
				return se;
			}
			// otherwise just return the same exception.
			return ioe;
		}

		/// <param name="datalen">Length of data</param>
		/// <returns>number of chunks for data of given size</returns>
		private int NumberOfChunks(long datalen)
		{
			return (int)((datalen + chunkSize - 1) / chunkSize);
		}

		/// <summary>Sends a packet with up to maxChunks chunks of data.</summary>
		/// <param name="pkt">buffer used for writing packet data</param>
		/// <param name="maxChunks">maximum number of chunks to send</param>
		/// <param name="out">stream to send data to</param>
		/// <param name="transferTo">use transferTo to send data</param>
		/// <param name="throttler">used for throttling data transfer bandwidth</param>
		/// <exception cref="System.IO.IOException"/>
		private int SendPacket(ByteBuffer pkt, int maxChunks, OutputStream @out, bool transferTo
			, DataTransferThrottler throttler)
		{
			int dataLen = (int)Math.Min(endOffset - offset, (chunkSize * (long)maxChunks));
			int numChunks = NumberOfChunks(dataLen);
			// Number of chunks be sent in the packet
			int checksumDataLen = numChunks * checksumSize;
			int packetLen = dataLen + checksumDataLen + 4;
			bool lastDataPacket = offset + dataLen == endOffset && dataLen > 0;
			// The packet buffer is organized as follows:
			// _______HHHHCCCCD?D?D?D?
			//        ^   ^
			//        |   \ checksumOff
			//        \ headerOff
			// _ padding, since the header is variable-length
			// H = header and length prefixes
			// C = checksums
			// D? = data, if transferTo is false.
			int headerLen = WritePacketHeader(pkt, dataLen, packetLen);
			// Per above, the header doesn't start at the beginning of the
			// buffer
			int headerOff = pkt.Position() - headerLen;
			int checksumOff = pkt.Position();
			byte[] buf = ((byte[])pkt.Array());
			if (checksumSize > 0 && checksumIn != null)
			{
				ReadChecksum(buf, checksumOff, checksumDataLen);
				// write in progress that we need to use to get last checksum
				if (lastDataPacket && lastChunkChecksum != null)
				{
					int start = checksumOff + checksumDataLen - checksumSize;
					byte[] updatedChecksum = lastChunkChecksum.GetChecksum();
					if (updatedChecksum != null)
					{
						System.Array.Copy(updatedChecksum, 0, buf, start, checksumSize);
					}
				}
			}
			int dataOff = checksumOff + checksumDataLen;
			if (!transferTo)
			{
				// normal transfer
				IOUtils.ReadFully(blockIn, buf, dataOff, dataLen);
				if (verifyChecksum)
				{
					VerifyChecksum(buf, dataOff, dataLen, numChunks, checksumOff);
				}
			}
			try
			{
				if (transferTo)
				{
					SocketOutputStream sockOut = (SocketOutputStream)@out;
					// First write header and checksums
					sockOut.Write(buf, headerOff, dataOff - headerOff);
					// no need to flush since we know out is not a buffered stream
					FileChannel fileCh = ((FileInputStream)blockIn).GetChannel();
					LongWritable waitTime = new LongWritable();
					LongWritable transferTime = new LongWritable();
					sockOut.TransferToFully(fileCh, blockInPosition, dataLen, waitTime, transferTime);
					datanode.metrics.AddSendDataPacketBlockedOnNetworkNanos(waitTime.Get());
					datanode.metrics.AddSendDataPacketTransferNanos(transferTime.Get());
					blockInPosition += dataLen;
				}
				else
				{
					// normal transfer
					@out.Write(buf, headerOff, dataOff + dataLen - headerOff);
				}
			}
			catch (IOException e)
			{
				if (e is SocketTimeoutException)
				{
				}
				else
				{
					/*
					* writing to client timed out.  This happens if the client reads
					* part of a block and then decides not to read the rest (but leaves
					* the socket open).
					*
					* Reporting of this case is done in DataXceiver#run
					*/
					/* Exception while writing to the client. Connection closure from
					* the other end is mostly the case and we do not care much about
					* it. But other things can go wrong, especially in transferTo(),
					* which we do not want to ignore.
					*
					* The message parsing below should not be considered as a good
					* coding example. NEVER do it to drive a program logic. NEVER.
					* It was done here because the NIO throws an IOException for EPIPE.
					*/
					string ioem = e.Message;
					if (!ioem.StartsWith("Broken pipe") && !ioem.StartsWith("Connection reset"))
					{
						Log.Error("BlockSender.sendChunks() exception: ", e);
					}
					datanode.GetBlockScanner().MarkSuspectBlock(volumeRef.GetVolume().GetStorageID(), 
						block);
				}
				throw IoeToSocketException(e);
			}
			if (throttler != null)
			{
				// rebalancing so throttle
				throttler.Throttle(packetLen);
			}
			return dataLen;
		}

		/// <summary>Read checksum into given buffer</summary>
		/// <param name="buf">buffer to read the checksum into</param>
		/// <param name="checksumOffset">offset at which to write the checksum into buf</param>
		/// <param name="checksumLen">length of checksum to write</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		private void ReadChecksum(byte[] buf, int checksumOffset, int checksumLen)
		{
			if (checksumSize <= 0 && checksumIn == null)
			{
				return;
			}
			try
			{
				checksumIn.ReadFully(buf, checksumOffset, checksumLen);
			}
			catch (IOException e)
			{
				Log.Warn(" Could not read or failed to veirfy checksum for data" + " at offset " 
					+ offset + " for block " + block, e);
				IOUtils.CloseStream(checksumIn);
				checksumIn = null;
				if (corruptChecksumOk)
				{
					if (checksumOffset < checksumLen)
					{
						// Just fill the array with zeros.
						Arrays.Fill(buf, checksumOffset, checksumLen, unchecked((byte)0));
					}
				}
				else
				{
					throw;
				}
			}
		}

		/// <summary>
		/// Compute checksum for chunks and verify the checksum that is read from
		/// the metadata file is correct.
		/// </summary>
		/// <param name="buf">buffer that has checksum and data</param>
		/// <param name="dataOffset">position where data is written in the buf</param>
		/// <param name="datalen">length of data</param>
		/// <param name="numChunks">number of chunks corresponding to data</param>
		/// <param name="checksumOffset">offset where checksum is written in the buf</param>
		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException">on failed checksum verification
		/// 	</exception>
		public virtual void VerifyChecksum(byte[] buf, int dataOffset, int datalen, int numChunks
			, int checksumOffset)
		{
			int dOff = dataOffset;
			int cOff = checksumOffset;
			int dLeft = datalen;
			for (int i = 0; i < numChunks; i++)
			{
				checksum.Reset();
				int dLen = Math.Min(dLeft, chunkSize);
				checksum.Update(buf, dOff, dLen);
				if (!checksum.Compare(buf, cOff))
				{
					long failedPos = offset + datalen - dLeft;
					throw new ChecksumException("Checksum failed at " + failedPos, failedPos);
				}
				dLeft -= dLen;
				dOff += dLen;
				cOff += checksumSize;
			}
		}

		/// <summary>
		/// sendBlock() is used to read block and its metadata and stream the data to
		/// either a client or to another datanode.
		/// </summary>
		/// <param name="out">stream to which the block is written to</param>
		/// <param name="baseStream">
		/// optional. if non-null, <code>out</code> is assumed to
		/// be a wrapper over this stream. This enables optimizations for
		/// sending the data, e.g.
		/// <see cref="Org.Apache.Hadoop.Net.SocketOutputStream.TransferToFully(Sharpen.FileChannel, long, int)
		/// 	"/>
		/// .
		/// </param>
		/// <param name="throttler">for sending data.</param>
		/// <returns>total bytes read, including checksum data.</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual long SendBlock(DataOutputStream @out, OutputStream baseStream, DataTransferThrottler
			 throttler)
		{
			TraceScope scope = Trace.StartSpan("sendBlock_" + block.GetBlockId(), Sampler.Never
				);
			try
			{
				return DoSendBlock(@out, baseStream, throttler);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private long DoSendBlock(DataOutputStream @out, OutputStream baseStream, DataTransferThrottler
			 throttler)
		{
			if (@out == null)
			{
				throw new IOException("out stream is null");
			}
			initialOffset = offset;
			long totalRead = 0;
			OutputStream streamForSendChunks = @out;
			lastCacheDropOffset = initialOffset;
			if (IsLongRead() && blockInFd != null)
			{
				// Advise that this file descriptor will be accessed sequentially.
				NativeIO.POSIX.GetCacheManipulator().PosixFadviseIfPossible(block.GetBlockName(), 
					blockInFd, 0, 0, NativeIO.POSIX.PosixFadvSequential);
			}
			// Trigger readahead of beginning of file if configured.
			ManageOsCache();
			long startTime = ClientTraceLog.IsDebugEnabled() ? Runtime.NanoTime() : 0;
			try
			{
				int maxChunksPerPacket;
				int pktBufSize = PacketHeader.PktMaxHeaderLen;
				bool transferTo = transferToAllowed && !verifyChecksum && baseStream is SocketOutputStream
					 && blockIn is FileInputStream;
				if (transferTo)
				{
					FileChannel fileChannel = ((FileInputStream)blockIn).GetChannel();
					blockInPosition = fileChannel.Position();
					streamForSendChunks = baseStream;
					maxChunksPerPacket = NumberOfChunks(TransfertoBufferSize);
					// Smaller packet size to only hold checksum when doing transferTo
					pktBufSize += checksumSize * maxChunksPerPacket;
				}
				else
				{
					maxChunksPerPacket = Math.Max(1, NumberOfChunks(HdfsConstants.IoFileBufferSize));
					// Packet size includes both checksum and data
					pktBufSize += (chunkSize + checksumSize) * maxChunksPerPacket;
				}
				ByteBuffer pktBuf = ByteBuffer.Allocate(pktBufSize);
				while (endOffset > offset && !Sharpen.Thread.CurrentThread().IsInterrupted())
				{
					ManageOsCache();
					long len = SendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks, transferTo
						, throttler);
					offset += len;
					totalRead += len + (NumberOfChunks(len) * checksumSize);
					seqno++;
				}
				// If this thread was interrupted, then it did not send the full block.
				if (!Sharpen.Thread.CurrentThread().IsInterrupted())
				{
					try
					{
						// send an empty packet to mark the end of the block
						SendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks, transferTo, throttler
							);
						@out.Flush();
					}
					catch (IOException e)
					{
						//socket error
						throw IoeToSocketException(e);
					}
					sentEntireByteRange = true;
				}
			}
			finally
			{
				if ((clientTraceFmt != null) && ClientTraceLog.IsDebugEnabled())
				{
					long endTime = Runtime.NanoTime();
					ClientTraceLog.Debug(string.Format(clientTraceFmt, totalRead, initialOffset, endTime
						 - startTime));
				}
				Close();
			}
			return totalRead;
		}

		/// <summary>
		/// Manage the OS buffer cache by performing read-ahead
		/// and drop-behind.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void ManageOsCache()
		{
			// We can't manage the cache for this block if we don't have a file
			// descriptor to work with.
			if (blockInFd == null)
			{
				return;
			}
			// Perform readahead if necessary
			if ((readaheadLength > 0) && (datanode.readaheadPool != null) && (alwaysReadahead
				 || IsLongRead()))
			{
				curReadahead = datanode.readaheadPool.ReadaheadStream(clientTraceFmt, blockInFd, 
					offset, readaheadLength, long.MaxValue, curReadahead);
			}
			// Drop what we've just read from cache, since we aren't
			// likely to need it again
			if (dropCacheBehindAllReads || (dropCacheBehindLargeReads && IsLongRead()))
			{
				long nextCacheDropOffset = lastCacheDropOffset + CacheDropIntervalBytes;
				if (offset >= nextCacheDropOffset)
				{
					long dropLength = offset - lastCacheDropOffset;
					NativeIO.POSIX.GetCacheManipulator().PosixFadviseIfPossible(block.GetBlockName(), 
						blockInFd, lastCacheDropOffset, dropLength, NativeIO.POSIX.PosixFadvDontneed);
					lastCacheDropOffset = offset;
				}
			}
		}

		/// <summary>
		/// Returns true if we have done a long enough read for this block to qualify
		/// for the DataNode-wide cache management defaults.
		/// </summary>
		/// <remarks>
		/// Returns true if we have done a long enough read for this block to qualify
		/// for the DataNode-wide cache management defaults.  We avoid applying the
		/// cache management defaults to smaller reads because the overhead would be
		/// too high.
		/// Note that if the client explicitly asked for dropBehind, we will do it
		/// even on short reads.
		/// This is also used to determine when to invoke
		/// posix_fadvise(POSIX_FADV_SEQUENTIAL).
		/// </remarks>
		private bool IsLongRead()
		{
			return (endOffset - initialOffset) > LongReadThresholdBytes;
		}

		/// <summary>
		/// Write packet header into
		/// <paramref name="pkt"/>
		/// ,
		/// return the length of the header written.
		/// </summary>
		private int WritePacketHeader(ByteBuffer pkt, int dataLen, int packetLen)
		{
			pkt.Clear();
			// both syncBlock and syncPacket are false
			PacketHeader header = new PacketHeader(packetLen, offset, seqno, (dataLen == 0), 
				dataLen, false);
			int size = header.GetSerializedSize();
			pkt.Position(PacketHeader.PktMaxHeaderLen - size);
			header.PutInBuffer(pkt);
			return size;
		}

		internal virtual bool DidSendEntireByteRange()
		{
			return sentEntireByteRange;
		}

		/// <returns>the checksum type that will be used with this block transfer.</returns>
		internal virtual DataChecksum GetChecksum()
		{
			return checksum;
		}

		/// <returns>
		/// the offset into the block file where the sender is currently
		/// reading.
		/// </returns>
		internal virtual long GetOffset()
		{
			return offset;
		}
	}
}
