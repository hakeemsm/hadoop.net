using System.IO;
using System.Text;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// This is used to track task completion events on
	/// job tracker.
	/// </summary>
	public class TaskCompletionEvent : Writable
	{
		public enum Status
		{
			Failed,
			Killed,
			Succeeded,
			Obsolete,
			Tipfailed
		}

		private int eventId;

		private string taskTrackerHttp;

		private int taskRunTime;

		private TaskAttemptID taskId;

		internal TaskCompletionEvent.Status status;

		internal bool isMap = false;

		private int idWithinJob;

		public static readonly Org.Apache.Hadoop.Mapreduce.TaskCompletionEvent[] EmptyArray
			 = new Org.Apache.Hadoop.Mapreduce.TaskCompletionEvent[0];

		/// <summary>Default constructor for Writable.</summary>
		public TaskCompletionEvent()
		{
			// using int since runtime is the time difference
			taskId = new TaskAttemptID();
		}

		/// <summary>Constructor.</summary>
		/// <remarks>
		/// Constructor. eventId should be created externally and incremented
		/// per event for each job.
		/// </remarks>
		/// <param name="eventId">
		/// event id, event id should be unique and assigned in
		/// incrementally, starting from 0.
		/// </param>
		/// <param name="taskId">task id</param>
		/// <param name="status">task's status</param>
		/// <param name="taskTrackerHttp">task tracker's host:port for http.</param>
		public TaskCompletionEvent(int eventId, TaskAttemptID taskId, int idWithinJob, bool
			 isMap, TaskCompletionEvent.Status status, string taskTrackerHttp)
		{
			this.taskId = taskId;
			this.idWithinJob = idWithinJob;
			this.isMap = isMap;
			this.eventId = eventId;
			this.status = status;
			this.taskTrackerHttp = taskTrackerHttp;
		}

		/// <summary>Returns event Id.</summary>
		/// <returns>event id</returns>
		public virtual int GetEventId()
		{
			return eventId;
		}

		/// <summary>Returns task id.</summary>
		/// <returns>task id</returns>
		public virtual TaskAttemptID GetTaskAttemptId()
		{
			return taskId;
		}

		/// <summary>
		/// Returns
		/// <see cref="Status"/>
		/// </summary>
		/// <returns>task completion status</returns>
		public virtual TaskCompletionEvent.Status GetStatus()
		{
			return status;
		}

		/// <summary>http location of the tasktracker where this task ran.</summary>
		/// <returns>http location of tasktracker user logs</returns>
		public virtual string GetTaskTrackerHttp()
		{
			return taskTrackerHttp;
		}

		/// <summary>Returns time (in millisec) the task took to complete.</summary>
		public virtual int GetTaskRunTime()
		{
			return taskRunTime;
		}

		/// <summary>Set the task completion time</summary>
		/// <param name="taskCompletionTime">time (in millisec) the task took to complete</param>
		protected internal virtual void SetTaskRunTime(int taskCompletionTime)
		{
			this.taskRunTime = taskCompletionTime;
		}

		/// <summary>set event Id.</summary>
		/// <remarks>set event Id. should be assigned incrementally starting from 0.</remarks>
		/// <param name="eventId"/>
		protected internal virtual void SetEventId(int eventId)
		{
			this.eventId = eventId;
		}

		/// <summary>Sets task id.</summary>
		/// <param name="taskId"/>
		protected internal virtual void SetTaskAttemptId(TaskAttemptID taskId)
		{
			this.taskId = taskId;
		}

		/// <summary>Set task status.</summary>
		/// <param name="status"/>
		protected internal virtual void SetTaskStatus(TaskCompletionEvent.Status status)
		{
			this.status = status;
		}

		/// <summary>Set task tracker http location.</summary>
		/// <param name="taskTrackerHttp"/>
		protected internal virtual void SetTaskTrackerHttp(string taskTrackerHttp)
		{
			this.taskTrackerHttp = taskTrackerHttp;
		}

		public override string ToString()
		{
			StringBuilder buf = new StringBuilder();
			buf.Append("Task Id : ");
			buf.Append(taskId);
			buf.Append(", Status : ");
			buf.Append(status.ToString());
			return buf.ToString();
		}

		public override bool Equals(object o)
		{
			if (o == null)
			{
				return false;
			}
			if (o.GetType().Equals(this.GetType()))
			{
				Org.Apache.Hadoop.Mapreduce.TaskCompletionEvent @event = (Org.Apache.Hadoop.Mapreduce.TaskCompletionEvent
					)o;
				return this.isMap == @event.IsMapTask() && this.eventId == @event.GetEventId() &&
					 this.idWithinJob == @event.IdWithinJob() && this.status.Equals(@event.GetStatus
					()) && this.taskId.Equals(@event.GetTaskAttemptId()) && this.taskRunTime == @event
					.GetTaskRunTime() && this.taskTrackerHttp.Equals(@event.GetTaskTrackerHttp());
			}
			return false;
		}

		public override int GetHashCode()
		{
			return ToString().GetHashCode();
		}

		public virtual bool IsMapTask()
		{
			return isMap;
		}

		public virtual int IdWithinJob()
		{
			return idWithinJob;
		}

		//////////////////////////////////////////////
		// Writable
		//////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			taskId.Write(@out);
			WritableUtils.WriteVInt(@out, idWithinJob);
			@out.WriteBoolean(isMap);
			WritableUtils.WriteEnum(@out, status);
			WritableUtils.WriteString(@out, taskTrackerHttp);
			WritableUtils.WriteVInt(@out, taskRunTime);
			WritableUtils.WriteVInt(@out, eventId);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			taskId.ReadFields(@in);
			idWithinJob = WritableUtils.ReadVInt(@in);
			isMap = @in.ReadBoolean();
			status = WritableUtils.ReadEnum<TaskCompletionEvent.Status>(@in);
			taskTrackerHttp = WritableUtils.ReadString(@in);
			taskRunTime = WritableUtils.ReadVInt(@in);
			eventId = WritableUtils.ReadVInt(@in);
		}
	}
}
