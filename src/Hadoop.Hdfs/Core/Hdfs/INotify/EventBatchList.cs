using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Inotify
{
	/// <summary>
	/// Contains a list of event batches, the transaction ID in the edit log up to
	/// which we read to produce these events, and the first txid we observed when
	/// producing these events (the last of which is for the purpose of determining
	/// whether we have missed events due to edit deletion).
	/// </summary>
	/// <remarks>
	/// Contains a list of event batches, the transaction ID in the edit log up to
	/// which we read to produce these events, and the first txid we observed when
	/// producing these events (the last of which is for the purpose of determining
	/// whether we have missed events due to edit deletion). Also contains the most
	/// recent txid that the NameNode has sync'ed, so the client can determine how
	/// far behind in the edit log it is.
	/// </remarks>
	public class EventBatchList
	{
		private IList<EventBatch> batches;

		private long firstTxid;

		private long lastTxid;

		private long syncTxid;

		public EventBatchList(IList<EventBatch> batches, long firstTxid, long lastTxid, long
			 syncTxid)
		{
			this.batches = batches;
			this.firstTxid = firstTxid;
			this.lastTxid = lastTxid;
			this.syncTxid = syncTxid;
		}

		public virtual IList<EventBatch> GetBatches()
		{
			return batches;
		}

		public virtual long GetFirstTxid()
		{
			return firstTxid;
		}

		public virtual long GetLastTxid()
		{
			return lastTxid;
		}

		public virtual long GetSyncTxid()
		{
			return syncTxid;
		}
	}
}
