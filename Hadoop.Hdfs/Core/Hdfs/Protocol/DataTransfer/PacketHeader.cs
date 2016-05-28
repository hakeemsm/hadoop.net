using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Primitives;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>Header data for each packet that goes through the read/write pipelines.</summary>
	/// <remarks>
	/// Header data for each packet that goes through the read/write pipelines.
	/// Includes all of the information about the packet, excluding checksums and
	/// actual data.
	/// This data includes:
	/// - the offset in bytes into the HDFS block of the data in this packet
	/// - the sequence number of this packet in the pipeline
	/// - whether or not this is the last packet in the pipeline
	/// - the length of the data in this packet
	/// - whether or not this packet should be synced by the DNs.
	/// When serialized, this header is written out as a protocol buffer, preceded
	/// by a 4-byte integer representing the full packet length, and a 2-byte short
	/// representing the header length.
	/// </remarks>
	public class PacketHeader
	{
		private static readonly int MaxProtoSize = ((DataTransferProtos.PacketHeaderProto
			)DataTransferProtos.PacketHeaderProto.NewBuilder().SetOffsetInBlock(0).SetSeqno(
			0).SetLastPacketInBlock(false).SetDataLen(0).SetSyncBlock(false).Build()).GetSerializedSize
			();

		public const int PktLengthsLen = Ints.Bytes + Shorts.Bytes;

		public static readonly int PktMaxHeaderLen = PktLengthsLen + MaxProtoSize;

		private int packetLen;

		private DataTransferProtos.PacketHeaderProto proto;

		public PacketHeader()
		{
		}

		public PacketHeader(int packetLen, long offsetInBlock, long seqno, bool lastPacketInBlock
			, int dataLen, bool syncBlock)
		{
			this.packetLen = packetLen;
			Preconditions.CheckArgument(packetLen >= Ints.Bytes, "packet len %s should always be at least 4 bytes"
				, packetLen);
			DataTransferProtos.PacketHeaderProto.Builder builder = DataTransferProtos.PacketHeaderProto
				.NewBuilder().SetOffsetInBlock(offsetInBlock).SetSeqno(seqno).SetLastPacketInBlock
				(lastPacketInBlock).SetDataLen(dataLen);
			if (syncBlock)
			{
				// Only set syncBlock if it is specified.
				// This is wire-incompatible with Hadoop 2.0.0-alpha due to HDFS-3721
				// because it changes the length of the packet header, and BlockReceiver
				// in that version did not support variable-length headers.
				builder.SetSyncBlock(syncBlock);
			}
			proto = ((DataTransferProtos.PacketHeaderProto)builder.Build());
		}

		public virtual int GetDataLen()
		{
			return proto.GetDataLen();
		}

		public virtual bool IsLastPacketInBlock()
		{
			return proto.GetLastPacketInBlock();
		}

		public virtual long GetSeqno()
		{
			return proto.GetSeqno();
		}

		public virtual long GetOffsetInBlock()
		{
			return proto.GetOffsetInBlock();
		}

		public virtual int GetPacketLen()
		{
			return packetLen;
		}

		public virtual bool GetSyncBlock()
		{
			return proto.GetSyncBlock();
		}

		public override string ToString()
		{
			return "PacketHeader with packetLen=" + packetLen + " header data: " + proto.ToString
				();
		}

		/// <exception cref="Com.Google.Protobuf.InvalidProtocolBufferException"/>
		public virtual void SetFieldsFromData(int packetLen, byte[] headerData)
		{
			this.packetLen = packetLen;
			proto = DataTransferProtos.PacketHeaderProto.ParseFrom(headerData);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(ByteBuffer buf)
		{
			packetLen = buf.GetInt();
			short protoLen = buf.GetShort();
			byte[] data = new byte[protoLen];
			buf.Get(data);
			proto = DataTransferProtos.PacketHeaderProto.ParseFrom(data);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInputStream @in)
		{
			this.packetLen = @in.ReadInt();
			short protoLen = @in.ReadShort();
			byte[] data = new byte[protoLen];
			@in.ReadFully(data);
			proto = DataTransferProtos.PacketHeaderProto.ParseFrom(data);
		}

		/// <returns>
		/// the number of bytes necessary to write out this header,
		/// including the length-prefixing of the payload and header
		/// </returns>
		public virtual int GetSerializedSize()
		{
			return PktLengthsLen + proto.GetSerializedSize();
		}

		/// <summary>Write the header into the buffer.</summary>
		/// <remarks>
		/// Write the header into the buffer.
		/// This requires that PKT_HEADER_LEN bytes are available.
		/// </remarks>
		public virtual void PutInBuffer(ByteBuffer buf)
		{
			System.Diagnostics.Debug.Assert(proto.GetSerializedSize() <= MaxProtoSize, "Expected "
				 + (MaxProtoSize) + " got: " + proto.GetSerializedSize());
			try
			{
				buf.PutInt(packetLen);
				buf.PutShort((short)proto.GetSerializedSize());
				proto.WriteTo(new ByteBufferOutputStream(buf));
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutputStream @out)
		{
			System.Diagnostics.Debug.Assert(proto.GetSerializedSize() <= MaxProtoSize, "Expected "
				 + (MaxProtoSize) + " got: " + proto.GetSerializedSize());
			@out.WriteInt(packetLen);
			@out.WriteShort(proto.GetSerializedSize());
			proto.WriteTo(@out);
		}

		public virtual byte[] GetBytes()
		{
			ByteBuffer buf = ByteBuffer.Allocate(GetSerializedSize());
			PutInBuffer(buf);
			return ((byte[])buf.Array());
		}

		/// <summary>Perform a sanity check on the packet, returning true if it is sane.</summary>
		/// <param name="lastSeqNo">
		/// the previous sequence number received - we expect the current
		/// sequence number to be larger by 1.
		/// </param>
		public virtual bool SanityCheck(long lastSeqNo)
		{
			// We should only have a non-positive data length for the last packet
			if (proto.GetDataLen() <= 0 && !proto.GetLastPacketInBlock())
			{
				return false;
			}
			// The last packet should not contain data
			if (proto.GetLastPacketInBlock() && proto.GetDataLen() != 0)
			{
				return false;
			}
			// Seqnos should always increase by 1 with each packet received
			if (proto.GetSeqno() != lastSeqNo + 1)
			{
				return false;
			}
			return true;
		}

		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.PacketHeader))
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.PacketHeader other = (Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.PacketHeader
				)o;
			return this.proto.Equals(other.proto);
		}

		public override int GetHashCode()
		{
			return (int)proto.GetSeqno();
		}
	}
}
