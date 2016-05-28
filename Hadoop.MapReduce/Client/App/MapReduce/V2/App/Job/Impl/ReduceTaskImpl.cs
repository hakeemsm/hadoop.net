using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Metrics;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl
{
	public class ReduceTaskImpl : TaskImpl
	{
		private readonly int numMapTasks;

		public ReduceTaskImpl(JobId jobId, int partition, EventHandler eventHandler, Path
			 jobFile, JobConf conf, int numMapTasks, TaskAttemptListener taskAttemptListener
			, Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken, Credentials
			 credentials, Clock clock, int appAttemptId, MRAppMetrics metrics, AppContext appContext
			)
			: base(jobId, TaskType.Reduce, partition, eventHandler, jobFile, conf, taskAttemptListener
				, jobToken, credentials, clock, appAttemptId, metrics, appContext)
		{
			this.numMapTasks = numMapTasks;
		}

		protected internal override int GetMaxAttempts()
		{
			return conf.GetInt(MRJobConfig.ReduceMaxAttempts, 4);
		}

		protected internal override TaskAttemptImpl CreateAttempt()
		{
			return new ReduceTaskAttemptImpl(GetID(), nextAttemptNumber, eventHandler, jobFile
				, partition, numMapTasks, conf, taskAttemptListener, jobToken, credentials, clock
				, appContext);
		}

		public override TaskType GetType()
		{
			return TaskType.Reduce;
		}
	}
}
