using System;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This is a wrapper around connection to datanode
	/// and understands checksum, offset etc.
	/// </summary>
	/// <remarks>
	/// This is a wrapper around connection to datanode
	/// and understands checksum, offset etc.
	/// Terminology:
	/// <dl>
	/// <dt>block</dt>
	/// <dd>The hdfs block, typically large (~64MB).
	/// </dd>
	/// <dt>chunk</dt>
	/// <dd>A block is divided into chunks, each comes with a checksum.
	/// We want transfers to be chunk-aligned, to be able to
	/// verify checksums.
	/// </dd>
	/// <dt>packet</dt>
	/// <dd>A grouping of chunks used for transport. It contains a
	/// header, followed by checksum data, followed by real data.
	/// </dd>
	/// </dl>
	/// Please see DataNode for the RPC specification.
	/// This is a new implementation introduced in Hadoop 0.23 which
	/// is more efficient and simpler than the older BlockReader
	/// implementation. It should be renamed to RemoteBlockReader
	/// once we are confident in it.
	/// </remarks>
	public class RemoteBlockReader2 : BlockReader
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.RemoteBlockReader2
			));

		private readonly Peer peer;

		private readonly DatanodeID datanodeID;

		private readonly PeerCache peerCache;

		private readonly long blockId;

		private readonly ReadableByteChannel @in;

		private DataChecksum checksum;

		private readonly PacketReceiver packetReceiver = new PacketReceiver(true);

		private ByteBuffer curDataSlice = null;

		/// <summary>offset in block of the last chunk received</summary>
		private long lastSeqNo = -1;

		/// <summary>offset in block where reader wants to actually read</summary>
		private long startOffset;

		private readonly string filename;

		private readonly int bytesPerChecksum;

		private readonly int checksumSize;

		/// <summary>The total number of bytes we need to transfer from the DN.</summary>
		/// <remarks>
		/// The total number of bytes we need to transfer from the DN.
		/// This is the amount that the user has requested plus some padding
		/// at the beginning so that the read can begin on a chunk boundary.
		/// </remarks>
		private long bytesNeededToFinish;

		/// <summary>True if we are reading from a local DataNode.</summary>
		private readonly bool isLocal;

		private readonly bool verifyChecksum;

		private bool sentStatusCode = false;

		internal byte[] skipBuf = null;

		internal ByteBuffer checksumBytes = null;

		/// <summary>Amount of unread data in the current received packet</summary>
		internal int dataLeft = 0;

		[VisibleForTesting]
		public virtual Peer GetPeer()
		{
			return peer;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(byte[] buf, int off, int len)
		{
			lock (this)
			{
				UUID randomId = null;
				if (Log.IsTraceEnabled())
				{
					randomId = UUID.RandomUUID();
					Log.Trace(string.Format("Starting read #%s file %s from datanode %s", randomId.ToString
						(), this.filename, this.datanodeID.GetHostName()));
				}
				if (curDataSlice == null || curDataSlice.Remaining() == 0 && bytesNeededToFinish 
					> 0)
				{
					TraceScope scope = Trace.StartSpan("RemoteBlockReader2#readNextPacket(" + blockId
						 + ")", Sampler.Never);
					try
					{
						ReadNextPacket();
					}
					finally
					{
						scope.Close();
					}
				}
				if (Log.IsTraceEnabled())
				{
					Log.Trace(string.Format("Finishing read #" + randomId));
				}
				if (curDataSlice.Remaining() == 0)
				{
					// we're at EOF now
					return -1;
				}
				int nRead = Math.Min(curDataSlice.Remaining(), len);
				curDataSlice.Get(buf, off, nRead);
				return nRead;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(ByteBuffer buf)
		{
			if (curDataSlice == null || curDataSlice.Remaining() == 0 && bytesNeededToFinish 
				> 0)
			{
				TraceScope scope = Trace.StartSpan("RemoteBlockReader2#readNextPacket(" + blockId
					 + ")", Sampler.Never);
				try
				{
					ReadNextPacket();
				}
				finally
				{
					scope.Close();
				}
			}
			if (curDataSlice.Remaining() == 0)
			{
				// we're at EOF now
				return -1;
			}
			int nRead = Math.Min(curDataSlice.Remaining(), buf.Remaining());
			ByteBuffer writeSlice = curDataSlice.Duplicate();
			writeSlice.Limit(writeSlice.Position() + nRead);
			buf.Put(writeSlice);
			curDataSlice.Position(writeSlice.Position());
			return nRead;
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadNextPacket()
		{
			//Read packet headers.
			packetReceiver.ReceiveNextPacket(@in);
			PacketHeader curHeader = packetReceiver.GetHeader();
			curDataSlice = packetReceiver.GetDataSlice();
			System.Diagnostics.Debug.Assert(curDataSlice.Capacity() == curHeader.GetDataLen()
				);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("DFSClient readNextPacket got header " + curHeader);
			}
			// Sanity check the lengths
			if (!curHeader.SanityCheck(lastSeqNo))
			{
				throw new IOException("BlockReader: error in packet header " + curHeader);
			}
			if (curHeader.GetDataLen() > 0)
			{
				int chunks = 1 + (curHeader.GetDataLen() - 1) / bytesPerChecksum;
				int checksumsLen = chunks * checksumSize;
				System.Diagnostics.Debug.Assert(packetReceiver.GetChecksumSlice().Capacity() == checksumsLen
					, "checksum slice capacity=" + packetReceiver.GetChecksumSlice().Capacity() + " checksumsLen="
					 + checksumsLen);
				lastSeqNo = curHeader.GetSeqno();
				if (verifyChecksum && curDataSlice.Remaining() > 0)
				{
					// N.B.: the checksum error offset reported here is actually
					// relative to the start of the block, not the start of the file.
					// This is slightly misleading, but preserves the behavior from
					// the older BlockReader.
					checksum.VerifyChunkedSums(curDataSlice, packetReceiver.GetChecksumSlice(), filename
						, curHeader.GetOffsetInBlock());
				}
				bytesNeededToFinish -= curHeader.GetDataLen();
			}
			// First packet will include some data prior to the first byte
			// the user requested. Skip it.
			if (curHeader.GetOffsetInBlock() < startOffset)
			{
				int newPos = (int)(startOffset - curHeader.GetOffsetInBlock());
				curDataSlice.Position(newPos);
			}
			// If we've now satisfied the whole client read, read one last packet
			// header, which should be empty
			if (bytesNeededToFinish <= 0)
			{
				ReadTrailingEmptyPacket();
				if (verifyChecksum)
				{
					SendReadResult(DataTransferProtos.Status.ChecksumOk);
				}
				else
				{
					SendReadResult(DataTransferProtos.Status.Success);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long Skip(long n)
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
		private void ReadTrailingEmptyPacket()
		{
			if (Log.IsTraceEnabled())
			{
				Log.Trace("Reading empty packet at end of read");
			}
			packetReceiver.ReceiveNextPacket(@in);
			PacketHeader trailer = packetReceiver.GetHeader();
			if (!trailer.IsLastPacketInBlock() || trailer.GetDataLen() != 0)
			{
				throw new IOException("Expected empty end-of-read packet! Header: " + trailer);
			}
		}

		protected internal RemoteBlockReader2(string file, string bpid, long blockId, DataChecksum
			 checksum, bool verifyChecksum, long startOffset, long firstChunkOffset, long bytesToRead
			, Peer peer, DatanodeID datanodeID, PeerCache peerCache)
		{
			this.isLocal = DFSClient.IsLocalAddress(NetUtils.CreateSocketAddr(datanodeID.GetXferAddr
				()));
			// Path is used only for printing block and file information in debug
			this.peer = peer;
			this.datanodeID = datanodeID;
			this.@in = peer.GetInputStreamChannel();
			this.checksum = checksum;
			this.verifyChecksum = verifyChecksum;
			this.startOffset = Math.Max(startOffset, 0);
			this.filename = file;
			this.peerCache = peerCache;
			this.blockId = blockId;
			// The total number of bytes that we need to transfer from the DN is
			// the amount that the user wants (bytesToRead), plus the padding at
			// the beginning in order to chunk-align. Note that the DN may elect
			// to send more than this amount if the read starts/ends mid-chunk.
			this.bytesNeededToFinish = bytesToRead + (startOffset - firstChunkOffset);
			bytesPerChecksum = this.checksum.GetBytesPerChecksum();
			checksumSize = this.checksum.GetChecksumSize();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				packetReceiver.Close();
				startOffset = -1;
				checksum = null;
				if (peerCache != null && sentStatusCode)
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
		internal virtual void SendReadResult(DataTransferProtos.Status statusCode)
		{
			System.Diagnostics.Debug.Assert(!sentStatusCode, "already sent status code to " +
				 peer);
			try
			{
				WriteReadResult(peer.GetOutputStream(), statusCode);
				sentStatusCode = true;
			}
			catch (IOException e)
			{
				// It's ok not to be able to send this. But something is probably wrong.
				Log.Info("Could not send read status (" + statusCode + ") to datanode " + peer.GetRemoteAddressString
					() + ": " + e.Message);
			}
		}

		/// <summary>Serialize the actual read result on the wire.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void WriteReadResult(OutputStream @out, DataTransferProtos.Status
			 statusCode)
		{
			((DataTransferProtos.ClientReadStatusProto)DataTransferProtos.ClientReadStatusProto
				.NewBuilder().SetStatus(statusCode).Build()).WriteDelimitedTo(@out);
			@out.Flush();
		}

		/// <summary>File name to print when accessing a block directly (from servlets)</summary>
		/// <param name="s">Address of the block location</param>
		/// <param name="poolId">Block pool ID of the block</param>
		/// <param name="blockId">Block ID of the block</param>
		/// <returns>string that has a file name for debug purposes</returns>
		public static string GetFileName(IPEndPoint s, string poolId, long blockId)
		{
			return s.ToString() + ":" + poolId + ":" + blockId;
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
		/// <param name="verifyChecksum">Whether to verify checksum</param>
		/// <param name="clientName">Client name</param>
		/// <param name="peer">The Peer to use</param>
		/// <param name="datanodeID">The DatanodeID this peer is connected to</param>
		/// <returns>New BlockReader instance, or null on error.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static BlockReader NewBlockReader(string file, ExtendedBlock block, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, long startOffset, long len, bool verifyChecksum
			, string clientName, Peer peer, DatanodeID datanodeID, PeerCache peerCache, CachingStrategy
			 cachingStrategy)
		{
			// in and out will be closed when sock is closed (by the caller)
			DataOutputStream @out = new DataOutputStream(new BufferedOutputStream(peer.GetOutputStream
				()));
			new Sender(@out).ReadBlock(block, blockToken, clientName, startOffset, len, verifyChecksum
				, cachingStrategy);
			//
			// Get bytes in block
			//
			DataInputStream @in = new DataInputStream(peer.GetInputStream());
			DataTransferProtos.BlockOpResponseProto status = DataTransferProtos.BlockOpResponseProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			CheckSuccess(status, peer, block, file);
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
			return new Org.Apache.Hadoop.Hdfs.RemoteBlockReader2(file, block.GetBlockPoolId()
				, block.GetBlockId(), checksum, verifyChecksum, startOffset, firstChunkOffset, len
				, peer, datanodeID, peerCache);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void CheckSuccess(DataTransferProtos.BlockOpResponseProto status, 
			Peer peer, ExtendedBlock block, string file)
		{
			string logInfo = "for OP_READ_BLOCK" + ", self=" + peer.GetLocalAddressString() +
				 ", remote=" + peer.GetRemoteAddressString() + ", for file " + file + ", for pool "
				 + block.GetBlockPoolId() + " block " + block.GetBlockId() + "_" + block.GetGenerationStamp
				();
			DataTransferProtoUtil.CheckBlockOpStatus(status, logInfo);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Available()
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
