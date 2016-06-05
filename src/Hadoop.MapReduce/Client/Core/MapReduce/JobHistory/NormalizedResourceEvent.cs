using System;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record the normalized map/reduce requirements.</summary>
	public class NormalizedResourceEvent : HistoryEvent
	{
		private int memory;

		private TaskType taskType;

		/// <summary>Normalized request when sent to the Resource Manager.</summary>
		/// <param name="taskType">the tasktype of the request.</param>
		/// <param name="memory">the normalized memory requirements.</param>
		public NormalizedResourceEvent(TaskType taskType, int memory)
		{
			this.memory = memory;
			this.taskType = taskType;
		}

		/// <summary>the tasktype for the event.</summary>
		/// <returns>the tasktype for the event.</returns>
		public virtual TaskType GetTaskType()
		{
			return this.taskType;
		}

		/// <summary>the normalized memory</summary>
		/// <returns>the normalized memory</returns>
		public virtual int GetMemory()
		{
			return this.memory;
		}

		public virtual EventType GetEventType()
		{
			return EventType.NormalizedResource;
		}

		public virtual object GetDatum()
		{
			throw new NotSupportedException("Not a seriable object");
		}

		public virtual void SetDatum(object datum)
		{
			throw new NotSupportedException("Not a seriable object");
		}
	}
}
