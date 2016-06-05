using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class MapTaskAttemptImpl : TaskAttemptImpl
	{
		private readonly JobSplit.TaskSplitMetaInfo splitInfo;

		public MapTaskAttemptImpl(TaskId taskId, int attempt, EventHandler eventHandler, 
			Path jobFile, int partition, JobSplit.TaskSplitMetaInfo splitInfo, JobConf conf, 
			TaskAttemptListener taskAttemptListener, Org.Apache.Hadoop.Security.Token.Token<
			JobTokenIdentifier> jobToken, Credentials credentials, Clock clock, AppContext appContext
			)
			: base(taskId, attempt, eventHandler, taskAttemptListener, jobFile, partition, conf
				, splitInfo.GetLocations(), jobToken, credentials, clock, appContext)
		{
			this.splitInfo = splitInfo;
		}

		protected internal override Task CreateRemoteTask()
		{
			//job file name is set in TaskAttempt, setting it null here
			MapTask mapTask = new MapTask(string.Empty, TypeConverter.FromYarn(GetID()), partition
				, splitInfo.GetSplitIndex(), 1);
			// YARN doesn't have the concept of slots per task, set it as 1.
			mapTask.SetUser(conf.Get(MRJobConfig.UserName));
			mapTask.SetConf(conf);
			return mapTask;
		}
	}
}
