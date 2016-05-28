using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Speculate
{
	public class SpeculatorEvent : AbstractEvent<Speculator.EventType>
	{
		private TaskAttemptStatusUpdateEvent.TaskAttemptStatus reportedStatus;

		private TaskId taskID;

		private int containersNeededChange;

		private JobId jobID;

		public SpeculatorEvent(JobId jobID, long timestamp)
			: base(Speculator.EventType.JobCreate, timestamp)
		{
			// valid for ATTEMPT_STATUS_UPDATE
			// valid for TASK_CONTAINER_NEED_UPDATE
			// valid for CREATE_JOB
			this.jobID = jobID;
		}

		public SpeculatorEvent(TaskAttemptStatusUpdateEvent.TaskAttemptStatus reportedStatus
			, long timestamp)
			: base(Speculator.EventType.AttemptStatusUpdate, timestamp)
		{
			this.reportedStatus = reportedStatus;
		}

		public SpeculatorEvent(TaskAttemptId attemptID, bool flag, long timestamp)
			: base(Speculator.EventType.AttemptStart, timestamp)
		{
			this.reportedStatus = new TaskAttemptStatusUpdateEvent.TaskAttemptStatus();
			this.reportedStatus.id = attemptID;
			this.taskID = attemptID.GetTaskId();
		}

		public SpeculatorEvent(TaskId taskID, int containersNeededChange)
			: base(Speculator.EventType.TaskContainerNeedUpdate)
		{
			/*
			* This c'tor creates a TASK_CONTAINER_NEED_UPDATE event .
			* We send a +1 event when a task enters a state where it wants a container,
			*  and a -1 event when it either gets one or withdraws the request.
			* The per job sum of all these events is the number of containers requested
			*  but not granted.  The intent is that we only do speculations when the
			*  speculation wouldn't compete for containers with tasks which need
			*  to be run.
			*/
			this.taskID = taskID;
			this.containersNeededChange = containersNeededChange;
		}

		public virtual TaskAttemptStatusUpdateEvent.TaskAttemptStatus GetReportedStatus()
		{
			return reportedStatus;
		}

		public virtual int ContainersNeededChange()
		{
			return containersNeededChange;
		}

		public virtual TaskId GetTaskID()
		{
			return taskID;
		}

		public virtual JobId GetJobID()
		{
			return jobID;
		}
	}
}
