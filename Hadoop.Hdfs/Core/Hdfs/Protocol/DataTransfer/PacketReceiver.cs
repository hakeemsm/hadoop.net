using System;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Primitives;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>Class to handle reading packets one-at-a-time from the wire.</summary>
	/// <remarks>
	/// Class to handle reading packets one-at-a-time from the wire.
	/// These packets are used both for reading and writing data to/from
	/// DataNodes.
	/// </remarks>
	public class PacketReceiver : IDisposable
	{
		/// <summary>The max size of any single packet.</summary>
		/// <remarks>
		/// The max size of any single packet. This prevents OOMEs when
		/// invalid data is sent.
		/// </remarks>
		private const int MaxPacketSize = 16 * 1024 * 1024;

		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.PacketReceiver
			));

		private static readonly DirectBufferPool bufferPool = new DirectBufferPool();

		private readonly bool useDirectBuffers;

		/// <summary>The entirety of the most recently read packet.</summary>
		/// <remarks>
		/// The entirety of the most recently read packet.
		/// The first PKT_LENGTHS_LEN bytes of this buffer are the
		/// length prefixes.
		/// </remarks>
		private ByteBuffer curPacketBuf = null;

		/// <summary>
		/// A slice of
		/// <see cref="curPacketBuf"/>
		/// which contains just the checksums.
		/// </summary>
		private ByteBuffer curChecksumSlice = null;

		/// <summary>
		/// A slice of
		/// <see cref="curPacketBuf"/>
		/// which contains just the data.
		/// </summary>
		private ByteBuffer curDataSlice = null;

		/// <summary>The packet header of the most recently read packet.</summary>
		private PacketHeader curHeader;

		public PacketReceiver(bool useDirectBuffers)
		{
			this.useDirectBuffers = useDirectBuffers;
			ReallocPacketBuf(PacketHeader.PktLengthsLen);
		}

		public virtual PacketHeader GetHeader()
		{
			return curHeader;
		}

		public virtual ByteBuffer GetDataSlice()
		{
			return curDataSlice;
		}

		public virtual ByteBuffer GetChecksumSlice()
		{
			return curChecksumSlice;
		}

		/// <summary>Reads all of the data for the next packet into the appropriate buffers.</summary>
		/// <remarks>
		/// Reads all of the data for the next packet into the appropriate buffers.
		/// The data slice and checksum slice members will be set to point to the
		/// user data and corresponding checksums. The header will be parsed and
		/// set.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReceiveNextPacket(ReadableByteChannel @in)
		{
			DoRead(@in, null);
		}

		/// <seealso cref="ReceiveNextPacket(Sharpen.ReadableByteChannel)"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReceiveNextPacket(InputStream @in)
		{
			DoRead(null, @in);
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoRead(ReadableByteChannel ch, InputStream @in)
		{
			// Each packet looks like:
			//   PLEN    HLEN      HEADER     CHECKSUMS  DATA
			//   32-bit  16-bit   <protobuf>  <variable length>
			//
			// PLEN:      Payload length
			//            = length(PLEN) + length(CHECKSUMS) + length(DATA)
			//            This length includes its own encoded length in
			//            the sum for historical reasons.
			//
			// HLEN:      Header length
			//            = length(HEADER)
			//
			// HEADER:    the actual packet header fields, encoded in protobuf
			// CHECKSUMS: the crcs for the data chunk. May be missing if
			//            checksums were not requested
			// DATA       the actual block data
			Preconditions.CheckState(curHeader == null || !curHeader.IsLastPacketInBlock());
			curPacketBuf.Clear();
			curPacketBuf.Limit(PacketHeader.PktLengthsLen);
			DoReadFully(ch, @in, curPacketBuf);
			curPacketBuf.Flip();
			int payloadLen = curPacketBuf.GetInt();
			if (payloadLen < Ints.Bytes)
			{
				// The "payload length" includes its own length. Therefore it
				// should never be less than 4 bytes
				throw new IOException("Invalid payload length " + payloadLen);
			}
			int dataPlusChecksumLen = payloadLen - Ints.Bytes;
			int headerLen = curPacketBuf.GetShort();
			if (headerLen < 0)
			{
				throw new IOException("Invalid header length " + headerLen);
			}
			if (Log.IsTraceEnabled())
			{
				Log.Trace("readNextPacket: dataPlusChecksumLen = " + dataPlusChecksumLen + " headerLen = "
					 + headerLen);
			}
			// Sanity check the buffer size so we don't allocate too much memory
			// and OOME.
			int totalLen = payloadLen + headerLen;
			if (totalLen < 0 || totalLen > MaxPacketSize)
			{
				throw new IOException("Incorrect value for packet payload size: " + payloadLen);
			}
			// Make sure we have space for the whole packet, and
			// read it.
			ReallocPacketBuf(PacketHeader.PktLengthsLen + dataPlusChecksumLen + headerLen);
			curPacketBuf.Clear();
			curPacketBuf.Position(PacketHeader.PktLengthsLen);
			curPacketBuf.Limit(PacketHeader.PktLengthsLen + dataPlusChecksumLen + headerLen);
			DoReadFully(ch, @in, curPacketBuf);
			curPacketBuf.Flip();
			curPacketBuf.Position(PacketHeader.PktLengthsLen);
			// Extract the header from the front of the buffer (after the length prefixes)
			byte[] headerBuf = new byte[headerLen];
			curPacketBuf.Get(headerBuf);
			if (curHeader == null)
			{
				curHeader = new PacketHeader();
			}
			curHeader.SetFieldsFromData(payloadLen, headerBuf);
			// Compute the sub-slices of the packet
			int checksumLen = dataPlusChecksumLen - curHeader.GetDataLen();
			if (checksumLen < 0)
			{
				throw new IOException("Invalid packet: data length in packet header " + "exceeds data length received. dataPlusChecksumLen="
					 + dataPlusChecksumLen + " header: " + curHeader);
			}
			ReslicePacket(headerLen, checksumLen, curHeader.GetDataLen());
		}

		/// <summary>Rewrite the last-read packet on the wire to the given output stream.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void MirrorPacketTo(DataOutputStream mirrorOut)
		{
			Preconditions.CheckState(!useDirectBuffers, "Currently only supported for non-direct buffers"
				);
			mirrorOut.Write(((byte[])curPacketBuf.Array()), curPacketBuf.ArrayOffset(), curPacketBuf
				.Remaining());
		}

		/// <exception cref="System.IO.IOException"/>
		private static void DoReadFully(ReadableByteChannel ch, InputStream @in, ByteBuffer
			 buf)
		{
			if (ch != null)
			{
				ReadChannelFully(ch, buf);
			}
			else
			{
				Preconditions.CheckState(!buf.IsDirect(), "Must not use direct buffers with InputStream API"
					);
				IOUtils.ReadFully(@in, ((byte[])buf.Array()), buf.ArrayOffset() + buf.Position(), 
					buf.Remaining());
				buf.Position(buf.Position() + buf.Remaining());
			}
		}

		private void ReslicePacket(int headerLen, int checksumsLen, int dataLen)
		{
			// Packet structure (refer to doRead() for details):
			//   PLEN    HLEN      HEADER     CHECKSUMS  DATA
			//   32-bit  16-bit   <protobuf>  <variable length>
			//   |--- lenThroughHeader ----|
			//   |----------- lenThroughChecksums   ----|
			//   |------------------- lenThroughData    ------| 
			int lenThroughHeader = PacketHeader.PktLengthsLen + headerLen;
			int lenThroughChecksums = lenThroughHeader + checksumsLen;
			int lenThroughData = lenThroughChecksums + dataLen;
			System.Diagnostics.Debug.Assert(dataLen >= 0, "invalid datalen: " + dataLen);
			System.Diagnostics.Debug.Assert(curPacketBuf.Position() == lenThroughHeader);
			System.Diagnostics.Debug.Assert(curPacketBuf.Limit() == lenThroughData, "headerLen= "
				 + headerLen + " clen=" + checksumsLen + " dlen=" + dataLen + " rem=" + curPacketBuf
				.Remaining());
			// Slice the checksums.
			curPacketBuf.Position(lenThroughHeader);
			curPacketBuf.Limit(lenThroughChecksums);
			curChecksumSlice = curPacketBuf.Slice();
			// Slice the data.
			curPacketBuf.Position(lenThroughChecksums);
			curPacketBuf.Limit(lenThroughData);
			curDataSlice = curPacketBuf.Slice();
			// Reset buffer to point to the entirety of the packet (including
			// length prefixes)
			curPacketBuf.Position(0);
			curPacketBuf.Limit(lenThroughData);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ReadChannelFully(ReadableByteChannel ch, ByteBuffer buf)
		{
			while (buf.Remaining() > 0)
			{
				int n = ch.Read(buf);
				if (n < 0)
				{
					throw new IOException("Premature EOF reading from " + ch);
				}
			}
		}

		private void ReallocPacketBuf(int atLeastCapacity)
		{
			// Realloc the buffer if this packet is longer than the previous
			// one.
			if (curPacketBuf == null || curPacketBuf.Capacity() < atLeastCapacity)
			{
				ByteBuffer newBuf;
				if (useDirectBuffers)
				{
					newBuf = bufferPool.GetBuffer(atLeastCapacity);
				}
				else
				{
					newBuf = ByteBuffer.Allocate(atLeastCapacity);
				}
				// If reallocing an existing buffer, copy the old packet length
				// prefixes over
				if (curPacketBuf != null)
				{
					curPacketBuf.Flip();
					newBuf.Put(curPacketBuf);
				}
				ReturnPacketBufToPool();
				curPacketBuf = newBuf;
			}
		}

		private void ReturnPacketBufToPool()
		{
			if (curPacketBuf != null && curPacketBuf.IsDirect())
			{
				bufferPool.ReturnBuffer(curPacketBuf);
				curPacketBuf = null;
			}
		}

		public virtual void Close()
		{
			// Closeable
			ReturnPacketBufToPool();
		}

		~PacketReceiver()
		{
			try
			{
				// just in case it didn't get closed, we
				// may as well still try to return the buffer
				ReturnPacketBufToPool();
			}
			finally
			{
				base.Finalize();
			}
		}
	}
}
