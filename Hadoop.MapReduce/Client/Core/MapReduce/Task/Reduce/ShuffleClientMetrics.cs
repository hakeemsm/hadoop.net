using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Metrics;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public class ShuffleClientMetrics : Updater
	{
		private MetricsRecord shuffleMetrics = null;

		private int numFailedFetches = 0;

		private int numSuccessFetches = 0;

		private long numBytes = 0;

		private int numThreadsBusy = 0;

		private readonly int numCopiers;

		internal ShuffleClientMetrics(TaskAttemptID reduceId, JobConf jobConf)
		{
			this.numCopiers = jobConf.GetInt(MRJobConfig.ShuffleParallelCopies, 5);
			MetricsContext metricsContext = MetricsUtil.GetContext("mapred");
			this.shuffleMetrics = MetricsUtil.CreateRecord(metricsContext, "shuffleInput");
			this.shuffleMetrics.SetTag("user", jobConf.GetUser());
			this.shuffleMetrics.SetTag("jobName", jobConf.GetJobName());
			this.shuffleMetrics.SetTag("jobId", reduceId.GetJobID().ToString());
			this.shuffleMetrics.SetTag("taskId", reduceId.ToString());
			this.shuffleMetrics.SetTag("sessionId", jobConf.GetSessionId());
			metricsContext.RegisterUpdater(this);
		}

		public virtual void InputBytes(long numBytes)
		{
			lock (this)
			{
				this.numBytes += numBytes;
			}
		}

		public virtual void FailedFetch()
		{
			lock (this)
			{
				++numFailedFetches;
			}
		}

		public virtual void SuccessFetch()
		{
			lock (this)
			{
				++numSuccessFetches;
			}
		}

		public virtual void ThreadBusy()
		{
			lock (this)
			{
				++numThreadsBusy;
			}
		}

		public virtual void ThreadFree()
		{
			lock (this)
			{
				--numThreadsBusy;
			}
		}

		public virtual void DoUpdates(MetricsContext unused)
		{
			lock (this)
			{
				shuffleMetrics.IncrMetric("shuffle_input_bytes", numBytes);
				shuffleMetrics.IncrMetric("shuffle_failed_fetches", numFailedFetches);
				shuffleMetrics.IncrMetric("shuffle_success_fetches", numSuccessFetches);
				if (numCopiers != 0)
				{
					shuffleMetrics.SetMetric("shuffle_fetchers_busy_percent", 100 * ((float)numThreadsBusy
						 / numCopiers));
				}
				else
				{
					shuffleMetrics.SetMetric("shuffle_fetchers_busy_percent", 0);
				}
				numBytes = 0;
				numSuccessFetches = 0;
				numFailedFetches = 0;
			}
			shuffleMetrics.Update();
		}
	}
}
