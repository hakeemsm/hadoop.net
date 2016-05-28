using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	public class RemoteEditLog : Comparable<Org.Apache.Hadoop.Hdfs.Server.Protocol.RemoteEditLog
		>
	{
		private long startTxId = HdfsConstants.InvalidTxid;

		private long endTxId = HdfsConstants.InvalidTxid;

		private bool isInProgress = false;

		public RemoteEditLog()
		{
		}

		public RemoteEditLog(long startTxId, long endTxId)
		{
			this.startTxId = startTxId;
			this.endTxId = endTxId;
			this.isInProgress = (endTxId == HdfsConstants.InvalidTxid);
		}

		public RemoteEditLog(long startTxId, long endTxId, bool inProgress)
		{
			this.startTxId = startTxId;
			this.endTxId = endTxId;
			this.isInProgress = inProgress;
		}

		public virtual long GetStartTxId()
		{
			return startTxId;
		}

		public virtual long GetEndTxId()
		{
			return endTxId;
		}

		public virtual bool IsInProgress()
		{
			return isInProgress;
		}

		public override string ToString()
		{
			if (!isInProgress)
			{
				return "[" + startTxId + "," + endTxId + "]";
			}
			else
			{
				return "[" + startTxId + "-? (in-progress)]";
			}
		}

		public virtual int CompareTo(Org.Apache.Hadoop.Hdfs.Server.Protocol.RemoteEditLog
			 log)
		{
			return ComparisonChain.Start().Compare(startTxId, log.startTxId).Compare(endTxId, 
				log.endTxId).Result();
		}

		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.Hdfs.Server.Protocol.RemoteEditLog))
			{
				return false;
			}
			return this.CompareTo((Org.Apache.Hadoop.Hdfs.Server.Protocol.RemoteEditLog)o) ==
				 0;
		}

		public override int GetHashCode()
		{
			return (int)(startTxId * endTxId);
		}

		private sealed class _Function_89 : Function<Org.Apache.Hadoop.Hdfs.Server.Protocol.RemoteEditLog
			, long>
		{
			public _Function_89()
			{
			}

			public long Apply(Org.Apache.Hadoop.Hdfs.Server.Protocol.RemoteEditLog log)
			{
				if (null == log)
				{
					return HdfsConstants.InvalidTxid;
				}
				return log.GetStartTxId();
			}
		}

		/// <summary>
		/// Guava <code>Function</code> which applies
		/// <see cref="GetStartTxId()"/>
		/// 
		/// </summary>
		public static readonly Function<Org.Apache.Hadoop.Hdfs.Server.Protocol.RemoteEditLog
			, long> GetStartTxid = new _Function_89();
	}
}
