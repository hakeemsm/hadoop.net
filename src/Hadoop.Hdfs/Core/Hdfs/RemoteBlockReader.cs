using System;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	[System.ObsoleteAttribute(@"this is an old implementation that is being left around in case any issues spring up with the new RemoteBlockReader2 implementation. It will be removed in the next release."
		)]
	public class RemoteBlockReader : FSInputChecker, BlockReader
	{
		private readonly Peer peer;

		private readonly DatanodeID datanodeID;

		private readonly DataInputStream @in;

		private DataChecksum checksum;

		/// <summary>offset in block of the last chunk received</summary>
		private long lastChunkOffset = -1;

		private long lastChunkLen = -1;

		private long lastSeqNo = -1;

		/// <summary>offset in block where reader wants to actually read</summary>
		private long startOffset;

		private readonly long blockId;

		/// <summary>
		/// offset in block of of first chunk - may be less than startOffset
		/// if startOffset is not chunk-aligned
		/// </summary>
		private readonly long firstChunkOffset;

		private readonly int bytesPerChecksum;

		private readonly int checksumSize;

		/// <summary>The total number of bytes we need to transfer from the DN.</summary>
		/// <remarks>
		/// The total number of bytes we need to transfer from the DN.
		/// This is the amount that the user has requested plus some padding
		/// at the beginning so that the read can begin on a chunk boundary.
		/// </remarks>
		private readonly long bytesNeededToFinish;

		/// <summary>True if we are reading from a local DataNode.</summary>
		private readonly bool isLocal;

		private bool eos = false;

		private bool sentStatusCode = false;

		internal byte[] skipBuf = null;

		internal ByteBuffer checksumBytes = null;

		/// <summary>Amount of unread data in the current received packet</summary>
		internal int dataLeft = 0;

		private readonly PeerCache peerCache;

		/* FSInputChecker interface */
		/* same interface as inputStream java.io.InputStream#read()
		* used by DFSInputStream#read()
		* This violates one rule when there is a checksum error:
		* "Read should not modify user buffer before successful read"
		* because it first reads the data to user buffer and then checks
		* the checksum.
		*/
		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] buf, int off, int len)
		{
			lock (this)
			{
				// This has to be set here, *before* the skip, since we can
				// hit EOS during the skip, in the case that our entire read
				// is smaller than the checksum chunk.
				bool eosBefore = eos;
				//for the first read, skip the extra bytes at the front.
				if (lastChunkLen < 0 && startOffset > firstChunkOffset && len > 0)
				{
					// Skip these bytes. But don't call this.skip()!
					int toSkip = (int)(startOffset - firstChunkOffset);
					if (skipBuf == null)
					{
						skipBuf = new byte[bytesPerChecksum];
					}
					if (base.Read(skipBuf, 0, toSkip) != toSkip)
					{
						// should never happen
						throw new IOException("Could not skip required number of bytes");
					}
				}
				int nRead = base.Read(buf, off, len);
				// if eos was set in the previous read, send a status code to the DN
				if (eos && !eosBefore && nRead >= 0)
				{
					if (NeedChecksum())
					{
						SendReadResult(peer, DataTransferProtos.Status.ChecksumOk);
					}
					else
					{
						SendReadResult(peer, DataTransferProtos.Status.Success);
					}
				}
				return nRead;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override long Skip(long n)
		{
			lock (this)
			{
				/* How can we make sure we don't throw a ChecksumException, at least
				* in majority of the cases?. This one throws. */
				if (skipBuf == null)
				{
					skipBuf = new byte[bytesPerChecksum];
				}
				long nSkipped = 0;
				while (nSkipped < n)
				{
					int toSkip = (int)Math.Min(n - nSkipped, skipBuf.Length);
					int ret = Read(skipBuf, 0, toSkip);
					if (ret <= 0)
					{
						return nSkipped;
					}
					nSkipped += ret;
				}
				return nSkipped;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			throw new IOException("read() is not expected to be invoked. " + "Use read(buf, off, len) instead."
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool SeekToNewSource(long targetPos)
		{
			/* Checksum errors are handled outside the BlockReader.
			* DFSInputStream does not always call 'seekToNewSource'. In the
			* case of pread(), it just tries a different replica without seeking.
			*/
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Seek(long pos)
		{
			throw new IOException("Seek() is not supported in BlockInputChecker");
		}

		protected override long GetChunkPosition(long pos)
		{
			throw new RuntimeException("getChunkPosition() is not supported, " + "since seek is not required"
				);
		}

		/// <summary>
		/// Makes sure that checksumBytes has enough capacity
		/// and limit is set to the number of checksum bytes needed
		/// to be read.
		/// </summary>
		private void AdjustChecksumBytes(int dataLen)
		{
			int requiredSize = ((dataLen + bytesPerChecksum - 1) / bytesPerChecksum) * checksumSize;
			if (checksumBytes == null || requiredSize > checksumBytes.Capacity())
			{
				checksumBytes = ByteBuffer.Wrap(new byte[requiredSize]);
			}
			else
			{
				checksumBytes.Clear();
			}
			checksumBytes.Limit(requiredSize);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override int ReadChunk(long pos, byte[] buf, int offset, int len, byte[]
			 checksumBuf)
		{
			lock (this)
			{
				TraceScope scope = Trace.StartSpan("RemoteBlockReader#readChunk(" + blockId + ")"
					, Sampler.Never);
				try
				{
					return ReadChunkImpl(pos, buf, offset, len, checksumBuf);
				}
				finally
				{
					scope.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int ReadChunkImpl(long pos, byte[] buf, int offset, int len, byte[] checksumBuf
			)
		{
			lock (this)
			{
				// Read one chunk.
				if (eos)
				{
					// Already hit EOF
					return -1;
				}
				// Read one DATA_CHUNK.
				long chunkOffset = lastChunkOffset;
				if (lastChunkLen > 0)
				{
					chunkOffset += lastChunkLen;
				}
				// pos is relative to the start of the first chunk of the read.
				// chunkOffset is relative to the start of the block.
				// This makes sure that the read passed from FSInputChecker is the
				// for the same chunk we expect to be reading from the DN.
				if ((pos + firstChunkOffset) != chunkOffset)
				{
					throw new IOException("Mismatch in pos : " + pos + " + " + firstChunkOffset + " != "
						 + chunkOffset);
				}
				// Read next packet if the previous packet has been read completely.
				if (dataLeft <= 0)
				{
					//Read packet headers.
					PacketHeader header = new PacketHeader();
					header.ReadFields(@in);
					if (Log.IsDebugEnabled())
					{
						Log.Debug("DFSClient readChunk got header " + header);
					}
					// Sanity check the lengths
					if (!header.SanityCheck(lastSeqNo))
					{
						throw new IOException("BlockReader: error in packet header " + header);
					}
					lastSeqNo = header.GetSeqno();
					dataLeft = header.GetDataLen();
					AdjustChecksumBytes(header.GetDataLen());
					if (header.GetDataLen() > 0)
					{
						IOUtils.ReadFully(@in, ((byte[])checksumBytes.Array()), 0, checksumBytes.Limit());
					}
				}
				// Sanity checks
				System.Diagnostics.Debug.Assert(len >= bytesPerChecksum);
				System.Diagnostics.Debug.Assert(checksum != null);
				System.Diagnostics.Debug.Assert(checksumSize == 0 || (checksumBuf.Length % checksumSize
					 == 0));
				int checksumsToRead;
				int bytesToRead;
				if (checksumSize > 0)
				{
					// How many chunks left in our packet - this is a ceiling
					// since we may have a partial chunk at the end of the file
					int chunksLeft = (dataLeft - 1) / bytesPerChecksum + 1;
					// How many chunks we can fit in databuffer
					//  - note this is a floor since we always read full chunks
					int chunksCanFit = Math.Min(len / bytesPerChecksum, checksumBuf.Length / checksumSize
						);
					// How many chunks should we read
					checksumsToRead = Math.Min(chunksLeft, chunksCanFit);
					// How many bytes should we actually read
					bytesToRead = Math.Min(checksumsToRead * bytesPerChecksum, dataLeft);
				}
				else
				{
					// full chunks
					// in case we have a partial
					// no checksum
					bytesToRead = Math.Min(dataLeft, len);
					checksumsToRead = 0;
				}
				if (bytesToRead > 0)
				{
					// Assert we have enough space
					System.Diagnostics.Debug.Assert(bytesToRead <= len);
					System.Diagnostics.Debug.Assert(checksumBytes.Remaining() >= checksumSize * checksumsToRead
						);
					System.Diagnostics.Debug.Assert(checksumBuf.Length >= checksumSize * checksumsToRead
						);
					IOUtils.ReadFully(@in, buf, offset, bytesToRead);
					checksumBytes.Get(checksumBuf, 0, checksumSize * checksumsToRead);
				}
				dataLeft -= bytesToRead;
				System.Diagnostics.Debug.Assert(dataLeft >= 0);
				lastChunkOffset = chunkOffset;
				lastChunkLen = bytesToRead;
				// If there's no data left in the current packet after satisfying
				// this read, and we have satisfied the client read, we expect
				// an empty packet header from the DN to signify this.
				// Note that pos + bytesToRead may in fact be greater since the
				// DN finishes off the entire last chunk.
				if (dataLeft == 0 && pos + bytesToRead >= bytesNeededToFinish)
				{
					// Read header
					PacketHeader hdr = new PacketHeader();
					hdr.ReadFields(@in);
					if (!hdr.IsLastPacketInBlock() || hdr.GetDataLen() != 0)
					{
						throw new IOException("Expected empty end-of-read packet! Header: " + hdr);
					}
					eos = true;
				}
				if (bytesToRead == 0)
				{
					return -1;
				}
				return bytesToRead;
			}
		}

		private RemoteBlockReader(string file, string bpid, long blockId, DataInputStream
			 @in, DataChecksum checksum, bool verifyChecksum, long startOffset, long firstChunkOffset
			, long bytesToRead, Peer peer, DatanodeID datanodeID, PeerCache peerCache)
			: base(new Path("/" + Block.BlockFilePrefix + blockId + ":" + bpid + ":of:" + file
				), 1, verifyChecksum, checksum.GetChecksumSize() > 0 ? checksum : null, checksum
				.GetBytesPerChecksum(), checksum.GetChecksumSize())
		{
			// Path is used only for printing block and file information in debug
			/*too non path-like?*/
			this.isLocal = DFSClient.IsLocalAddress(NetUtils.CreateSocketAddr(datanodeID.GetXferAddr
				()));
			this.peer = peer;
			this.datanodeID = datanodeID;
			this.@in = @in;
			this.checksum = checksum;
			this.startOffset = Math.Max(startOffset, 0);
			this.blockId = blockId;
			// The total number of bytes that we need to transfer from the DN is
			// the amount that the user wants (bytesToRead), plus the padding at
			// the beginning in order to chunk-align. Note that the DN may elect
			// to send more than this amount if the read starts/ends mid-chunk.
			this.bytesNeededToFinish = bytesToRead + (startOffset - firstChunkOffset);
			this.firstChunkOffset = firstChunkOffset;
			lastChunkOffset = firstChunkOffset;
			lastChunkLen = -1;
			bytesPerChecksum = this.checksum.GetBytesPerChecksum();
			checksumSize = this.checksum.GetChecksumSize();
			this.peerCache = peerCache;
		}

		/// <summary>Create a new BlockReader specifically to satisfy a read.</summary>
		/// <remarks>
		/// Create a new BlockReader specifically to satisfy a read.
		/// This method also sends the OP_READ_BLOCK request.
		/// </remarks>
		/// <param name="file">File location</param>
		/// <param name="block">The block object</param>
		/// <param name="blockToken">The block token for security</param>
		/// <param name="startOffset">The read offset, relative to block head</param>
		/// <param name="len">The number of bytes to read</param>
		/// <param name="bufferSize">The IO buffer size (not the client buffer size)</param>
		/// <param name="verifyChecksum">Whether to verify checksum</param>
		/// <param name="clientName">Client name</param>
		/// <returns>New BlockReader instance, or null on error.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Hdfs.RemoteBlockReader NewBlockReader(string file
			, ExtendedBlock block, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
			> blockToken, long startOffset, long len, int bufferSize, bool verifyChecksum, string
			 clientName, Peer peer, DatanodeID datanodeID, PeerCache peerCache, CachingStrategy
			 cachingStrategy)
		{
			// in and out will be closed when sock is closed (by the caller)
			DataOutputStream @out = new DataOutputStream(new BufferedOutputStream(peer.GetOutputStream
				()));
			new Sender(@out).ReadBlock(block, blockToken, clientName, startOffset, len, verifyChecksum
				, cachingStrategy);
			//
			// Get bytes in block, set streams
			//
			DataInputStream @in = new DataInputStream(new BufferedInputStream(peer.GetInputStream
				(), bufferSize));
			DataTransferProtos.BlockOpResponseProto status = DataTransferProtos.BlockOpResponseProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			RemoteBlockReader2.CheckSuccess(status, peer, block, file);
			DataTransferProtos.ReadOpChecksumInfoProto checksumInfo = status.GetReadOpChecksumInfo
				();
			DataChecksum checksum = DataTransferProtoUtil.FromProto(checksumInfo.GetChecksum(
				));
			//Warning when we get CHECKSUM_NULL?
			// Read the first chunk offset.
			long firstChunkOffset = checksumInfo.GetChunkOffset();
			if (firstChunkOffset < 0 || firstChunkOffset > startOffset || firstChunkOffset <=
				 (startOffset - checksum.GetBytesPerChecksum()))
			{
				throw new IOException("BlockReader: error in first chunk offset (" + firstChunkOffset
					 + ") startOffset is " + startOffset + " for file " + file);
			}
			return new Org.Apache.Hadoop.Hdfs.RemoteBlockReader(file, block.GetBlockPoolId(), 
				block.GetBlockId(), @in, checksum, verifyChecksum, startOffset, firstChunkOffset
				, len, peer, datanodeID, peerCache);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				startOffset = -1;
				checksum = null;
				if (peerCache != null & sentStatusCode)
				{
					peerCache.Put(datanodeID, peer);
				}
				else
				{
					peer.Close();
				}
			}
		}

		// in will be closed when its Socket is closed.
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(byte[] buf, int readOffset, int amtToRead)
		{
			IOUtils.ReadFully(this, buf, readOffset, amtToRead);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int ReadAll(byte[] buf, int offset, int len)
		{
			return ReadFully(this, buf, offset, len);
		}

		/// <summary>
		/// When the reader reaches end of the read, it sends a status response
		/// (e.g.
		/// </summary>
		/// <remarks>
		/// When the reader reaches end of the read, it sends a status response
		/// (e.g. CHECKSUM_OK) to the DN. Failure to do so could lead to the DN
		/// closing our connection (which we will re-open), but won't affect
		/// data correctness.
		/// </remarks>
		internal virtual void SendReadResult(Peer peer, DataTransferProtos.Status statusCode
			)
		{
			System.Diagnostics.Debug.Assert(!sentStatusCode, "already sent status code to " +
				 peer);
			try
			{
				RemoteBlockReader2.WriteReadResult(peer.GetOutputStream(), statusCode);
				sentStatusCode = true;
			}
			catch (IOException e)
			{
				// It's ok not to be able to send this. But something is probably wrong.
				Log.Info("Could not send read status (" + statusCode + ") to datanode " + peer.GetRemoteAddressString
					() + ": " + e.Message);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(ByteBuffer buf)
		{
			throw new NotSupportedException("readDirect unsupported in RemoteBlockReader");
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Available()
		{
			// An optimistic estimate of how much data is available
			// to us without doing network I/O.
			return DFSClient.TcpWindowSize;
		}

		public virtual bool IsLocal()
		{
			return isLocal;
		}

		public virtual bool IsShortCircuit()
		{
			return false;
		}

		public virtual ClientMmap GetClientMmap(EnumSet<ReadOption> opts)
		{
			return null;
		}
	}
}
