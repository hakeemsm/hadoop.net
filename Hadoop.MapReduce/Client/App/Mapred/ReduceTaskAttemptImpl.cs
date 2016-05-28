using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class ReduceTaskAttemptImpl : TaskAttemptImpl
	{
		private readonly int numMapTasks;

		public ReduceTaskAttemptImpl(TaskId id, int attempt, EventHandler eventHandler, Path
			 jobFile, int partition, int numMapTasks, JobConf conf, TaskAttemptListener taskAttemptListener
			, Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken, Credentials
			 credentials, Clock clock, AppContext appContext)
			: base(id, attempt, eventHandler, taskAttemptListener, jobFile, partition, conf, 
				new string[] {  }, jobToken, credentials, clock, appContext)
		{
			this.numMapTasks = numMapTasks;
		}

		protected internal override Task CreateRemoteTask()
		{
			//job file name is set in TaskAttempt, setting it null here
			ReduceTask reduceTask = new ReduceTask(string.Empty, TypeConverter.FromYarn(GetID
				()), partition, numMapTasks, 1);
			// YARN doesn't have the concept of slots per task, set it as 1.
			reduceTask.SetUser(conf.Get(MRJobConfig.UserName));
			reduceTask.SetConf(conf);
			return reduceTask;
		}
	}
}
