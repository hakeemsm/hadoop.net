using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Inotify
{
	/// <summary>A batch of events that all happened on the same transaction ID.</summary>
	public class EventBatch
	{
		private readonly long txid;

		private readonly Event[] events;

		public EventBatch(long txid, Event[] events)
		{
			this.txid = txid;
			this.events = events;
		}

		public virtual long GetTxid()
		{
			return txid;
		}

		public virtual Event[] GetEvents()
		{
			return events;
		}
	}
}
