using System.IO;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>Pipeline Acknowledgment</summary>
	public class PipelineAck
	{
		internal DataTransferProtos.PipelineAckProto proto;

		public const long UnkownSeqno = -2;

		internal const int OobStart = DataTransferProtos.Status.OobRestartValue;

		internal const int OobEnd = DataTransferProtos.Status.OobReserved3Value;

		internal const int NumOobTypes = OobEnd - OobStart + 1;

		internal static readonly long[] OobTimeout;

		[System.Serializable]
		public sealed class ECN
		{
			public static readonly PipelineAck.ECN Disabled = new PipelineAck.ECN(0);

			public static readonly PipelineAck.ECN Supported = new PipelineAck.ECN(1);

			public static readonly PipelineAck.ECN Supported2 = new PipelineAck.ECN(2);

			public static readonly PipelineAck.ECN Congested = new PipelineAck.ECN(3);

			private readonly int value;

			private static readonly PipelineAck.ECN[] Values = Values();

			// the first OOB type
			// the last OOB type
			// place holder for timeout value of each OOB type
			internal static PipelineAck.ECN ValueOf(int value)
			{
				return PipelineAck.ECN.Values[value];
			}

			internal ECN(int value)
			{
				this.value = value;
			}

			public int GetValue()
			{
				return PipelineAck.ECN.value;
			}
		}

		[System.Serializable]
		private sealed class StatusFormat
		{
			public static readonly PipelineAck.StatusFormat Status = new PipelineAck.StatusFormat
				(null, 4);

			public static readonly PipelineAck.StatusFormat Reserved = new PipelineAck.StatusFormat
				(PipelineAck.StatusFormat.Status.Bits, 1);

			public static readonly PipelineAck.StatusFormat EcnBits = new PipelineAck.StatusFormat
				(PipelineAck.StatusFormat.Reserved.Bits, 2);

			private readonly LongBitFormat Bits;

			internal StatusFormat(LongBitFormat prev, int bits)
			{
				PipelineAck.StatusFormat.Bits = new LongBitFormat(Name(), prev, bits, 0);
			}

			internal static DataTransferProtos.Status GetStatus(int header)
			{
				return DataTransferProtos.Status.ValueOf((int)PipelineAck.StatusFormat.Status.Bits
					.Retrieve(header));
			}

			internal static PipelineAck.ECN GetECN(int header)
			{
				return PipelineAck.ECN.ValueOf((int)PipelineAck.StatusFormat.EcnBits.Bits.Retrieve
					(header));
			}

			public static int SetStatus(int old, DataTransferProtos.Status status)
			{
				return (int)PipelineAck.StatusFormat.Status.Bits.Combine(status.GetNumber(), old);
			}

			public static int SetECN(int old, PipelineAck.ECN ecn)
			{
				return (int)PipelineAck.StatusFormat.EcnBits.Bits.Combine(ecn.GetValue(), old);
			}
		}

		static PipelineAck()
		{
			OobTimeout = new long[NumOobTypes];
			HdfsConfiguration conf = new HdfsConfiguration();
			string[] ele = conf.Get(DFSConfigKeys.DfsDatanodeOobTimeoutKey, DFSConfigKeys.DfsDatanodeOobTimeoutDefault
				).Split(",");
			for (int i = 0; i < NumOobTypes; i++)
			{
				OobTimeout[i] = (i < ele.Length) ? long.Parse(ele[i]) : 0;
			}
		}

		/// <summary>default constructor</summary>
		public PipelineAck()
		{
		}

		/// <summary>Constructor assuming no next DN in pipeline</summary>
		/// <param name="seqno">sequence number</param>
		/// <param name="replies">an array of replies</param>
		public PipelineAck(long seqno, int[] replies)
			: this(seqno, replies, 0L)
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="seqno">sequence number</param>
		/// <param name="replies">an array of replies</param>
		/// <param name="downstreamAckTimeNanos">ack RTT in nanoseconds, 0 if no next DN in pipeline
		/// 	</param>
		public PipelineAck(long seqno, int[] replies, long downstreamAckTimeNanos)
		{
			AList<DataTransferProtos.Status> statusList = Lists.NewArrayList();
			AList<int> flagList = Lists.NewArrayList();
			foreach (int r in replies)
			{
				statusList.AddItem(PipelineAck.StatusFormat.GetStatus(r));
				flagList.AddItem(r);
			}
			proto = ((DataTransferProtos.PipelineAckProto)DataTransferProtos.PipelineAckProto
				.NewBuilder().SetSeqno(seqno).AddAllReply(statusList).AddAllFlag(flagList).SetDownstreamAckTimeNanos
				(downstreamAckTimeNanos).Build());
		}

		/// <summary>Get the sequence number</summary>
		/// <returns>the sequence number</returns>
		public virtual long GetSeqno()
		{
			return proto.GetSeqno();
		}

		/// <summary>Get the number of replies</summary>
		/// <returns>the number of replies</returns>
		public virtual short GetNumOfReplies()
		{
			return (short)proto.GetReplyCount();
		}

		/// <summary>get the header flag of ith reply</summary>
		public virtual int GetHeaderFlag(int i)
		{
			if (proto.GetFlagCount() > 0)
			{
				return proto.GetFlag(i);
			}
			else
			{
				return CombineHeader(PipelineAck.ECN.Disabled, proto.GetReply(i));
			}
		}

		public virtual int GetFlag(int i)
		{
			return proto.GetFlag(i);
		}

		/// <summary>Get the time elapsed for downstream ack RTT in nanoseconds</summary>
		/// <returns>time elapsed for downstream ack in nanoseconds, 0 if no next DN in pipeline
		/// 	</returns>
		public virtual long GetDownstreamAckTimeNanos()
		{
			return proto.GetDownstreamAckTimeNanos();
		}

		/// <summary>Check if this ack contains error status</summary>
		/// <returns>true if all statuses are SUCCESS</returns>
		public virtual bool IsSuccess()
		{
			foreach (DataTransferProtos.Status s in proto.GetReplyList())
			{
				if (s != DataTransferProtos.Status.Success)
				{
					return false;
				}
			}
			return true;
		}

		/// <summary>Returns the OOB status if this ack contains one.</summary>
		/// <returns>null if it is not an OOB ack.</returns>
		public virtual DataTransferProtos.Status GetOOBStatus()
		{
			// Normal data transfer acks will have a valid sequence number, so
			// this will return right away in most cases.
			if (GetSeqno() != UnkownSeqno)
			{
				return null;
			}
			foreach (DataTransferProtos.Status s in proto.GetReplyList())
			{
				// The following check is valid because protobuf guarantees to
				// preserve the ordering of enum elements.
				if (s.GetNumber() >= OobStart && s.GetNumber() <= OobEnd)
				{
					return s;
				}
			}
			return null;
		}

		/// <summary>Get the timeout to be used for transmitting the OOB type</summary>
		/// <returns>the timeout in milliseconds</returns>
		/// <exception cref="System.IO.IOException"/>
		public static long GetOOBTimeout(DataTransferProtos.Status status)
		{
			int index = status.GetNumber() - OobStart;
			if (index >= 0 && index < NumOobTypes)
			{
				return OobTimeout[index];
			}
			// Not an OOB.
			throw new IOException("Not an OOB status: " + status);
		}

		/// <summary>Get the Restart OOB ack status</summary>
		public static DataTransferProtos.Status GetRestartOOBStatus()
		{
			return DataTransferProtos.Status.OobRestart;
		}

		/// <summary>return true if it is the restart OOB status code</summary>
		public static bool IsRestartOOBStatus(DataTransferProtos.Status st)
		{
			return st.Equals(DataTransferProtos.Status.OobRestart);
		}

		/// <summary>Writable interface</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(InputStream @in)
		{
			proto = DataTransferProtos.PipelineAckProto.ParseFrom(PBHelper.VintPrefixed(@in));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(OutputStream @out)
		{
			proto.WriteDelimitedTo(@out);
		}

		public override string ToString()
		{
			//Object
			return TextFormat.ShortDebugString(proto);
		}

		public static DataTransferProtos.Status GetStatusFromHeader(int header)
		{
			return PipelineAck.StatusFormat.GetStatus(header);
		}

		public static int SetStatusForHeader(int old, DataTransferProtos.Status status)
		{
			return PipelineAck.StatusFormat.SetStatus(old, status);
		}

		public static int CombineHeader(PipelineAck.ECN ecn, DataTransferProtos.Status status
			)
		{
			int header = 0;
			header = PipelineAck.StatusFormat.SetStatus(header, status);
			header = PipelineAck.StatusFormat.SetECN(header, ecn);
			return header;
		}
	}
}
