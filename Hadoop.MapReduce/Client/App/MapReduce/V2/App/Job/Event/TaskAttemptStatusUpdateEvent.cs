using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class TaskAttemptStatusUpdateEvent : TaskAttemptEvent
	{
		private TaskAttemptStatusUpdateEvent.TaskAttemptStatus reportedTaskAttemptStatus;

		public TaskAttemptStatusUpdateEvent(TaskAttemptId id, TaskAttemptStatusUpdateEvent.TaskAttemptStatus
			 taskAttemptStatus)
			: base(id, TaskAttemptEventType.TaUpdate)
		{
			this.reportedTaskAttemptStatus = taskAttemptStatus;
		}

		public virtual TaskAttemptStatusUpdateEvent.TaskAttemptStatus GetReportedTaskAttemptStatus
			()
		{
			return reportedTaskAttemptStatus;
		}

		/// <summary>The internal TaskAttemptStatus object corresponding to remote Task status.
		/// 	</summary>
		public class TaskAttemptStatus
		{
			public TaskAttemptId id;

			public float progress;

			public Counters counters;

			public string stateString;

			public Phase phase;

			public IList<TaskAttemptId> fetchFailedMaps;

			public long mapFinishTime;

			public long shuffleFinishTime;

			public long sortFinishTime;

			public TaskAttemptState taskState;
		}
	}
}
