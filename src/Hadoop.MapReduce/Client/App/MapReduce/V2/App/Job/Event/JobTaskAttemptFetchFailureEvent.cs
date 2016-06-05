using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobTaskAttemptFetchFailureEvent : JobEvent
	{
		private readonly TaskAttemptId reduce;

		private readonly IList<TaskAttemptId> maps;

		public JobTaskAttemptFetchFailureEvent(TaskAttemptId reduce, IList<TaskAttemptId>
			 maps)
			: base(reduce.GetTaskId().GetJobId(), JobEventType.JobTaskAttemptFetchFailure)
		{
			this.reduce = reduce;
			this.maps = maps;
		}

		public virtual IList<TaskAttemptId> GetMaps()
		{
			return maps;
		}

		public virtual TaskAttemptId GetReduce()
		{
			return reduce;
		}
	}
}
