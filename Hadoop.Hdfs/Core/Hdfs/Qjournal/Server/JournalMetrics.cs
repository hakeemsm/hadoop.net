using System.IO;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	/// <summary>
	/// The server-side metrics for a journal from the JournalNode's
	/// perspective.
	/// </summary>
	internal class JournalMetrics
	{
		internal readonly MetricsRegistry registry = new MetricsRegistry("JournalNode");

		internal MutableCounterLong batchesWritten;

		internal MutableCounterLong txnsWritten;

		internal MutableCounterLong bytesWritten;

		internal MutableCounterLong batchesWrittenWhileLagging;

		private readonly int[] QuantileIntervals = new int[] { 1 * 60, 5 * 60, 60 * 60 };

		internal readonly MutableQuantiles[] syncsQuantiles;

		private readonly Journal journal;

		internal JournalMetrics(Journal journal)
		{
			// 1m
			// 5m
			// 1h
			this.journal = journal;
			syncsQuantiles = new MutableQuantiles[QuantileIntervals.Length];
			for (int i = 0; i < syncsQuantiles.Length; i++)
			{
				int interval = QuantileIntervals[i];
				syncsQuantiles[i] = registry.NewQuantiles("syncs" + interval + "s", "Journal sync time"
					, "ops", "latencyMicros", interval);
			}
		}

		public static Org.Apache.Hadoop.Hdfs.Qjournal.Server.JournalMetrics Create(Journal
			 j)
		{
			Org.Apache.Hadoop.Hdfs.Qjournal.Server.JournalMetrics m = new Org.Apache.Hadoop.Hdfs.Qjournal.Server.JournalMetrics
				(j);
			return DefaultMetricsSystem.Instance().Register(m.GetName(), null, m);
		}

		internal virtual string GetName()
		{
			return "Journal-" + journal.GetJournalId();
		}

		public virtual long GetLastWriterEpoch()
		{
			try
			{
				return journal.GetLastWriterEpoch();
			}
			catch (IOException)
			{
				return -1L;
			}
		}

		public virtual long GetLastPromisedEpoch()
		{
			try
			{
				return journal.GetLastPromisedEpoch();
			}
			catch (IOException)
			{
				return -1L;
			}
		}

		public virtual long GetLastWrittenTxId()
		{
			return journal.GetHighestWrittenTxId();
		}

		public virtual long GetCurrentLagTxns()
		{
			try
			{
				return journal.GetCurrentLagTxns();
			}
			catch (IOException)
			{
				return -1L;
			}
		}

		internal virtual void AddSync(long us)
		{
			foreach (MutableQuantiles q in syncsQuantiles)
			{
				q.Add(us);
			}
		}
	}
}
