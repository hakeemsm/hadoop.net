using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record start of a task attempt</summary>
	public class TaskAttemptStartedEvent : HistoryEvent
	{
		private TaskAttemptStarted datum = new TaskAttemptStarted();

		/// <summary>Create an event to record the start of an attempt</summary>
		/// <param name="attemptId">Id of the attempt</param>
		/// <param name="taskType">Type of task</param>
		/// <param name="startTime">Start time of the attempt</param>
		/// <param name="trackerName">Name of the Task Tracker where attempt is running</param>
		/// <param name="httpPort">The port number of the tracker</param>
		/// <param name="shufflePort">The shuffle port number of the container</param>
		/// <param name="containerId">The containerId for the task attempt.</param>
		/// <param name="locality">The locality of the task attempt</param>
		/// <param name="avataar">The avataar of the task attempt</param>
		public TaskAttemptStartedEvent(TaskAttemptID attemptId, TaskType taskType, long startTime
			, string trackerName, int httpPort, int shufflePort, ContainerId containerId, string
			 locality, string avataar)
		{
			datum.attemptId = new Utf8(attemptId.ToString());
			datum.taskid = new Utf8(attemptId.GetTaskID().ToString());
			datum.startTime = startTime;
			datum.taskType = new Utf8(taskType.ToString());
			datum.trackerName = new Utf8(trackerName);
			datum.httpPort = httpPort;
			datum.shufflePort = shufflePort;
			datum.containerId = new Utf8(containerId.ToString());
			if (locality != null)
			{
				datum.locality = new Utf8(locality);
			}
			if (avataar != null)
			{
				datum.avataar = new Utf8(avataar);
			}
		}

		public TaskAttemptStartedEvent(TaskAttemptID attemptId, TaskType taskType, long startTime
			, string trackerName, int httpPort, int shufflePort, string locality, string avataar
			)
			: this(attemptId, taskType, startTime, trackerName, httpPort, shufflePort, ConverterUtils
				.ToContainerId("container_-1_-1_-1_-1"), locality, avataar)
		{
		}

		internal TaskAttemptStartedEvent()
		{
		}

		// TODO Remove after MrV1 is removed.
		// Using a dummy containerId to prevent jobHistory parse failures.
		public virtual object GetDatum()
		{
			return datum;
		}

		public virtual void SetDatum(object datum)
		{
			this.datum = (TaskAttemptStarted)datum;
		}

		/// <summary>Get the task id</summary>
		public virtual TaskID GetTaskId()
		{
			return TaskID.ForName(datum.taskid.ToString());
		}

		/// <summary>Get the tracker name</summary>
		public virtual string GetTrackerName()
		{
			return datum.trackerName.ToString();
		}

		/// <summary>Get the start time</summary>
		public virtual long GetStartTime()
		{
			return datum.startTime;
		}

		/// <summary>Get the task type</summary>
		public virtual TaskType GetTaskType()
		{
			return TaskType.ValueOf(datum.taskType.ToString());
		}

		/// <summary>Get the HTTP port</summary>
		public virtual int GetHttpPort()
		{
			return datum.httpPort;
		}

		/// <summary>Get the shuffle port</summary>
		public virtual int GetShufflePort()
		{
			return datum.shufflePort;
		}

		/// <summary>Get the attempt id</summary>
		public virtual TaskAttemptID GetTaskAttemptId()
		{
			return TaskAttemptID.ForName(datum.attemptId.ToString());
		}

		/// <summary>Get the event type</summary>
		public virtual EventType GetEventType()
		{
			// Note that the task type can be setup/map/reduce/cleanup but the 
			// attempt-type can only be map/reduce.
			return GetTaskId().GetTaskType() == TaskType.Map ? EventType.MapAttemptStarted : 
				EventType.ReduceAttemptStarted;
		}

		/// <summary>Get the ContainerId</summary>
		public virtual ContainerId GetContainerId()
		{
			return ConverterUtils.ToContainerId(datum.containerId.ToString());
		}

		/// <summary>Get the locality</summary>
		public virtual string GetLocality()
		{
			if (datum.locality != null)
			{
				return datum.locality.ToString();
			}
			return null;
		}

		/// <summary>Get the avataar</summary>
		public virtual string GetAvataar()
		{
			if (datum.avataar != null)
			{
				return datum.avataar.ToString();
			}
			return null;
		}
	}
}
