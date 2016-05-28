using Org.Apache.Hadoop.Metrics;
using Org.Apache.Hadoop.Metrics.Jvm;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	internal class LocalJobRunnerMetrics : Updater
	{
		private readonly MetricsRecord metricsRecord;

		private int numMapTasksLaunched = 0;

		private int numMapTasksCompleted = 0;

		private int numReduceTasksLaunched = 0;

		private int numReduceTasksCompleted = 0;

		private int numWaitingMaps = 0;

		private int numWaitingReduces = 0;

		public LocalJobRunnerMetrics(JobConf conf)
		{
			string sessionId = conf.GetSessionId();
			// Initiate JVM Metrics
			JvmMetrics.Init("JobTracker", sessionId);
			// Create a record for map-reduce metrics
			MetricsContext context = MetricsUtil.GetContext("mapred");
			// record name is jobtracker for compatibility 
			metricsRecord = MetricsUtil.CreateRecord(context, "jobtracker");
			metricsRecord.SetTag("sessionId", sessionId);
			context.RegisterUpdater(this);
		}

		/// <summary>
		/// Since this object is a registered updater, this method will be called
		/// periodically, e.g.
		/// </summary>
		/// <remarks>
		/// Since this object is a registered updater, this method will be called
		/// periodically, e.g. every 5 seconds.
		/// </remarks>
		public virtual void DoUpdates(MetricsContext unused)
		{
			lock (this)
			{
				metricsRecord.IncrMetric("maps_launched", numMapTasksLaunched);
				metricsRecord.IncrMetric("maps_completed", numMapTasksCompleted);
				metricsRecord.IncrMetric("reduces_launched", numReduceTasksLaunched);
				metricsRecord.IncrMetric("reduces_completed", numReduceTasksCompleted);
				metricsRecord.IncrMetric("waiting_maps", numWaitingMaps);
				metricsRecord.IncrMetric("waiting_reduces", numWaitingReduces);
				numMapTasksLaunched = 0;
				numMapTasksCompleted = 0;
				numReduceTasksLaunched = 0;
				numReduceTasksCompleted = 0;
				numWaitingMaps = 0;
				numWaitingReduces = 0;
			}
			metricsRecord.Update();
		}

		public virtual void LaunchMap(TaskAttemptID taskAttemptID)
		{
			lock (this)
			{
				++numMapTasksLaunched;
				DecWaitingMaps(((JobID)taskAttemptID.GetJobID()), 1);
			}
		}

		public virtual void CompleteMap(TaskAttemptID taskAttemptID)
		{
			lock (this)
			{
				++numMapTasksCompleted;
			}
		}

		public virtual void LaunchReduce(TaskAttemptID taskAttemptID)
		{
			lock (this)
			{
				++numReduceTasksLaunched;
				DecWaitingReduces(((JobID)taskAttemptID.GetJobID()), 1);
			}
		}

		public virtual void CompleteReduce(TaskAttemptID taskAttemptID)
		{
			lock (this)
			{
				++numReduceTasksCompleted;
			}
		}

		private void DecWaitingMaps(JobID id, int task)
		{
			lock (this)
			{
				numWaitingMaps -= task;
			}
		}

		private void DecWaitingReduces(JobID id, int task)
		{
			lock (this)
			{
				numWaitingReduces -= task;
			}
		}
	}
}
