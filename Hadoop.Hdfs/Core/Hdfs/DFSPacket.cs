using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>DFSPacket is used by DataStreamer and DFSOutputStream.</summary>
	/// <remarks>
	/// DFSPacket is used by DataStreamer and DFSOutputStream.
	/// DFSOutputStream generates packets and then ask DatStreamer
	/// to send them to datanodes.
	/// </remarks>
	internal class DFSPacket
	{
		public const long HeartBeatSeqno = -1L;

		private static long[] Empty = new long[0];

		private readonly long seqno;

		private readonly long offsetInBlock;

		private bool syncBlock;

		private int numChunks;

		private readonly int maxChunks;

		private byte[] buf;

		private readonly bool lastPacketInBlock;

		/// <summary>
		/// buf is pointed into like follows:
		/// (C is checksum data, D is payload data)
		/// [_________CCCCCCCCC________________DDDDDDDDDDDDDDDD___]
		/// ^        ^               ^               ^
		/// |        checksumPos     dataStart       dataPos
		/// checksumStart
		/// Right before sending, we move the checksum data to immediately precede
		/// the actual data, and then insert the header into the buffer immediately
		/// preceding the checksum data, so we make sure to keep enough space in
		/// front of the checksum data to support the largest conceivable header.
		/// </summary>
		private int checksumStart;

		private int checksumPos;

		private readonly int dataStart;

		private int dataPos;

		private long[] traceParents = Empty;

		private int traceParentsUsed;

		private Span span;

		/// <summary>Create a new packet.</summary>
		/// <param name="buf">the buffer storing data and checksums</param>
		/// <param name="chunksPerPkt">maximum number of chunks per packet.</param>
		/// <param name="offsetInBlock">offset in bytes into the HDFS block.</param>
		/// <param name="seqno">the sequence number of this packet</param>
		/// <param name="checksumSize">the size of checksum</param>
		/// <param name="lastPacketInBlock">if this is the last packet</param>
		internal DFSPacket(byte[] buf, int chunksPerPkt, long offsetInBlock, long seqno, 
			int checksumSize, bool lastPacketInBlock)
		{
			// sequence number of buffer in block
			// offset in block
			// this packet forces the current block to disk
			// number of chunks currently in packet
			// max chunks in packet
			// is this the last packet in block?
			this.lastPacketInBlock = lastPacketInBlock;
			this.numChunks = 0;
			this.offsetInBlock = offsetInBlock;
			this.seqno = seqno;
			this.buf = buf;
			checksumStart = PacketHeader.PktMaxHeaderLen;
			checksumPos = checksumStart;
			dataStart = checksumStart + (chunksPerPkt * checksumSize);
			dataPos = dataStart;
			maxChunks = chunksPerPkt;
		}

		/// <summary>Write data to this packet.</summary>
		/// <param name="inarray">input array of data</param>
		/// <param name="off">the offset of data to write</param>
		/// <param name="len">the length of data to write</param>
		/// <exception cref="Sharpen.ClosedChannelException"/>
		internal virtual void WriteData(byte[] inarray, int off, int len)
		{
			lock (this)
			{
				CheckBuffer();
				if (dataPos + len > buf.Length)
				{
					throw new BufferOverflowException();
				}
				System.Array.Copy(inarray, off, buf, dataPos, len);
				dataPos += len;
			}
		}

		/// <summary>Write checksums to this packet</summary>
		/// <param name="inarray">input array of checksums</param>
		/// <param name="off">the offset of checksums to write</param>
		/// <param name="len">the length of checksums to write</param>
		/// <exception cref="Sharpen.ClosedChannelException"/>
		internal virtual void WriteChecksum(byte[] inarray, int off, int len)
		{
			lock (this)
			{
				CheckBuffer();
				if (len == 0)
				{
					return;
				}
				if (checksumPos + len > dataStart)
				{
					throw new BufferOverflowException();
				}
				System.Array.Copy(inarray, off, buf, checksumPos, len);
				checksumPos += len;
			}
		}

		/// <summary>Write the full packet, including the header, to the given output stream.
		/// 	</summary>
		/// <param name="stm"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void WriteTo(DataOutputStream stm)
		{
			lock (this)
			{
				CheckBuffer();
				int dataLen = dataPos - dataStart;
				int checksumLen = checksumPos - checksumStart;
				int pktLen = HdfsConstants.BytesInInteger + dataLen + checksumLen;
				PacketHeader header = new PacketHeader(pktLen, offsetInBlock, seqno, lastPacketInBlock
					, dataLen, syncBlock);
				if (checksumPos != dataStart)
				{
					// Move the checksum to cover the gap. This can happen for the last
					// packet or during an hflush/hsync call.
					System.Array.Copy(buf, checksumStart, buf, dataStart - checksumLen, checksumLen);
					checksumPos = dataStart;
					checksumStart = checksumPos - checksumLen;
				}
				int headerStart = checksumStart - header.GetSerializedSize();
				System.Diagnostics.Debug.Assert(checksumStart + 1 >= header.GetSerializedSize());
				System.Diagnostics.Debug.Assert(headerStart >= 0);
				System.Diagnostics.Debug.Assert(headerStart + header.GetSerializedSize() == checksumStart
					);
				// Copy the header data into the buffer immediately preceding the checksum
				// data.
				System.Array.Copy(header.GetBytes(), 0, buf, headerStart, header.GetSerializedSize
					());
				// corrupt the data for testing.
				if (DFSClientFaultInjector.Get().CorruptPacket())
				{
					buf[headerStart + header.GetSerializedSize() + checksumLen + dataLen - 1] ^= unchecked(
						(int)(0xff));
				}
				// Write the now contiguous full packet to the output stream.
				stm.Write(buf, headerStart, header.GetSerializedSize() + checksumLen + dataLen);
				// undo corruption.
				if (DFSClientFaultInjector.Get().UncorruptPacket())
				{
					buf[headerStart + header.GetSerializedSize() + checksumLen + dataLen - 1] ^= unchecked(
						(int)(0xff));
				}
			}
		}

		/// <exception cref="Sharpen.ClosedChannelException"/>
		private void CheckBuffer()
		{
			lock (this)
			{
				if (buf == null)
				{
					throw new ClosedChannelException();
				}
			}
		}

		/// <summary>Release the buffer in this packet to ByteArrayManager.</summary>
		/// <param name="bam"/>
		internal virtual void ReleaseBuffer(ByteArrayManager bam)
		{
			lock (this)
			{
				bam.Release(buf);
				buf = null;
			}
		}

		/// <summary>get the packet's last byte's offset in the block</summary>
		/// <returns>the packet's last byte's offset in the block</returns>
		internal virtual long GetLastByteOffsetBlock()
		{
			lock (this)
			{
				return offsetInBlock + dataPos - dataStart;
			}
		}

		/// <summary>Check if this packet is a heart beat packet</summary>
		/// <returns>true if the sequence number is HEART_BEAT_SEQNO</returns>
		internal virtual bool IsHeartbeatPacket()
		{
			return seqno == HeartBeatSeqno;
		}

		/// <summary>check if this packet is the last packet in block</summary>
		/// <returns>true if the packet is the last packet</returns>
		internal virtual bool IsLastPacketInBlock()
		{
			return lastPacketInBlock;
		}

		/// <summary>get sequence number of this packet</summary>
		/// <returns>the sequence number of this packet</returns>
		internal virtual long GetSeqno()
		{
			return seqno;
		}

		/// <summary>get the number of chunks this packet contains</summary>
		/// <returns>the number of chunks in this packet</returns>
		internal virtual int GetNumChunks()
		{
			lock (this)
			{
				return numChunks;
			}
		}

		/// <summary>increase the number of chunks by one</summary>
		internal virtual void IncNumChunks()
		{
			lock (this)
			{
				numChunks++;
			}
		}

		/// <summary>get the maximum number of packets</summary>
		/// <returns>the maximum number of packets</returns>
		internal virtual int GetMaxChunks()
		{
			return maxChunks;
		}

		/// <summary>set if to sync block</summary>
		/// <param name="syncBlock">if to sync block</param>
		internal virtual void SetSyncBlock(bool syncBlock)
		{
			lock (this)
			{
				this.syncBlock = syncBlock;
			}
		}

		public override string ToString()
		{
			return "packet seqno: " + this.seqno + " offsetInBlock: " + this.offsetInBlock + 
				" lastPacketInBlock: " + this.lastPacketInBlock + " lastByteOffsetInBlock: " + this
				.GetLastByteOffsetBlock();
		}

		/// <summary>
		/// Add a trace parent span for this packet.<p/>
		/// Trace parent spans for a packet are the trace spans responsible for
		/// adding data to that packet.
		/// </summary>
		/// <remarks>
		/// Add a trace parent span for this packet.<p/>
		/// Trace parent spans for a packet are the trace spans responsible for
		/// adding data to that packet.  We store them as an array of longs for
		/// efficiency.<p/>
		/// Protected by the DFSOutputStream dataQueue lock.
		/// </remarks>
		public virtual void AddTraceParent(Span span)
		{
			if (span == null)
			{
				return;
			}
			AddTraceParent(span.GetSpanId());
		}

		public virtual void AddTraceParent(long id)
		{
			if (traceParentsUsed == traceParents.Length)
			{
				int newLength = (traceParents.Length == 0) ? 8 : traceParents.Length * 2;
				traceParents = Arrays.CopyOf(traceParents, newLength);
			}
			traceParents[traceParentsUsed] = id;
			traceParentsUsed++;
		}

		/// <summary>
		/// Get the trace parent spans for this packet.<p/>
		/// Will always be non-null.<p/>
		/// Protected by the DFSOutputStream dataQueue lock.
		/// </summary>
		public virtual long[] GetTraceParents()
		{
			// Remove duplicates from the array.
			int len = traceParentsUsed;
			Arrays.Sort(traceParents, 0, len);
			int i = 0;
			int j = 0;
			long prevVal = 0;
			// 0 is not a valid span id
			while (true)
			{
				if (i == len)
				{
					break;
				}
				long val = traceParents[i];
				if (val != prevVal)
				{
					traceParents[j] = val;
					j++;
					prevVal = val;
				}
				i++;
			}
			if (j < traceParents.Length)
			{
				traceParents = Arrays.CopyOf(traceParents, j);
				traceParentsUsed = traceParents.Length;
			}
			return traceParents;
		}

		public virtual void SetTraceSpan(Span span)
		{
			this.span = span;
		}

		public virtual Span GetTraceSpan()
		{
			return span;
		}
	}
}
