using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Metrics;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl
{
	public class MapTaskImpl : TaskImpl
	{
		private readonly JobSplit.TaskSplitMetaInfo taskSplitMetaInfo;

		public MapTaskImpl(JobId jobId, int partition, EventHandler eventHandler, Path remoteJobConfFile
			, JobConf conf, JobSplit.TaskSplitMetaInfo taskSplitMetaInfo, TaskAttemptListener
			 taskAttemptListener, Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier>
			 jobToken, Credentials credentials, Clock clock, int appAttemptId, MRAppMetrics 
			metrics, AppContext appContext)
			: base(jobId, TaskType.Map, partition, eventHandler, remoteJobConfFile, conf, taskAttemptListener
				, jobToken, credentials, clock, appAttemptId, metrics, appContext)
		{
			this.taskSplitMetaInfo = taskSplitMetaInfo;
		}

		protected internal override int GetMaxAttempts()
		{
			return conf.GetInt(MRJobConfig.MapMaxAttempts, 4);
		}

		protected internal override TaskAttemptImpl CreateAttempt()
		{
			return new MapTaskAttemptImpl(GetID(), nextAttemptNumber, eventHandler, jobFile, 
				partition, taskSplitMetaInfo, conf, taskAttemptListener, jobToken, credentials, 
				clock, appContext);
		}

		public override TaskType GetType()
		{
			return TaskType.Map;
		}

		protected internal virtual JobSplit.TaskSplitMetaInfo GetTaskSplitMetaInfo()
		{
			return this.taskSplitMetaInfo;
		}

		/// <returns>a String formatted as a comma-separated list of splits.</returns>
		protected internal override string GetSplitsAsString()
		{
			string[] splits = GetTaskSplitMetaInfo().GetLocations();
			if (splits == null || splits.Length == 0)
			{
				return string.Empty;
			}
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < splits.Length; i++)
			{
				if (i != 0)
				{
					sb.Append(",");
				}
				sb.Append(splits[i]);
			}
			return sb.ToString();
		}
	}
}
