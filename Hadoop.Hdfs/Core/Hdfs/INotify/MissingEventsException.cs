using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Inotify
{
	[System.Serializable]
	public class MissingEventsException : Exception
	{
		private const long serialVersionUID = 1L;

		private long expectedTxid;

		private long actualTxid;

		public MissingEventsException()
		{
		}

		public MissingEventsException(long expectedTxid, long actualTxid)
		{
			this.expectedTxid = expectedTxid;
			this.actualTxid = actualTxid;
		}

		public virtual long GetExpectedTxid()
		{
			return expectedTxid;
		}

		public virtual long GetActualTxid()
		{
			return actualTxid;
		}

		public override string ToString()
		{
			return "We expected the next batch of events to start with transaction ID " + expectedTxid
				 + ", but it instead started with transaction ID " + actualTxid + ". Most likely the intervening transactions were cleaned "
				 + "up as part of checkpointing.";
		}
	}
}
