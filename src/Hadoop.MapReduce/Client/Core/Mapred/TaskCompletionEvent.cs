using System;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This is used to track task completion events on
	/// job tracker.
	/// </summary>
	public class TaskCompletionEvent : Org.Apache.Hadoop.Mapreduce.TaskCompletionEvent
	{
		public enum Status
		{
			Failed,
			Killed,
			Succeeded,
			Obsolete,
			Tipfailed
		}

		public static readonly Org.Apache.Hadoop.Mapred.TaskCompletionEvent[] EmptyArray = 
			new Org.Apache.Hadoop.Mapred.TaskCompletionEvent[0];

		/// <summary>Default constructor for Writable.</summary>
		public TaskCompletionEvent()
			: base()
		{
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
			: base(eventId, taskId, idWithinJob, isMap, TaskCompletionEvent.Status.ValueOf(status
				.ToString()), taskTrackerHttp)
		{
		}

		[InterfaceAudience.Private]
		public static Org.Apache.Hadoop.Mapred.TaskCompletionEvent Downgrade(Org.Apache.Hadoop.Mapreduce.TaskCompletionEvent
			 @event)
		{
			return new Org.Apache.Hadoop.Mapred.TaskCompletionEvent(@event.GetEventId(), TaskAttemptID
				.Downgrade(@event.GetTaskAttemptId()), @event.IdWithinJob(), @event.IsMapTask(), 
				TaskCompletionEvent.Status.ValueOf(@event.GetStatus().ToString()), @event.GetTaskTrackerHttp
				());
		}

		/// <summary>Returns task id.</summary>
		/// <returns>task id</returns>
		[System.ObsoleteAttribute(@"use GetTaskAttemptId() instead.")]
		public virtual string GetTaskId()
		{
			return ((TaskAttemptID)GetTaskAttemptId()).ToString();
		}

		/// <summary>Returns task id.</summary>
		/// <returns>task id</returns>
		public override TaskAttemptID GetTaskAttemptId()
		{
			return TaskAttemptID.Downgrade(base.GetTaskAttemptId());
		}

		/// <summary>
		/// Returns
		/// <see cref="Status"/>
		/// </summary>
		/// <returns>task completion status</returns>
		public virtual TaskCompletionEvent.Status GetTaskStatus()
		{
			return TaskCompletionEvent.Status.ValueOf(base.GetStatus().ToString());
		}

		/// <summary>Sets task id.</summary>
		/// <param name="taskId"/>
		[System.ObsoleteAttribute(@"use SetTaskAttemptId(TaskAttemptID) instead.")]
		public virtual void SetTaskId(string taskId)
		{
			this.SetTaskAttemptId(((TaskAttemptID)TaskAttemptID.ForName(taskId)));
		}

		/// <summary>Sets task id.</summary>
		/// <param name="taskId"/>
		[System.ObsoleteAttribute(@"use SetTaskAttemptId(TaskAttemptID) instead.")]
		public virtual void SetTaskID(TaskAttemptID taskId)
		{
			this.SetTaskAttemptId(taskId);
		}

		/// <summary>Sets task id.</summary>
		/// <param name="taskId"/>
		protected internal virtual void SetTaskAttemptId(TaskAttemptID taskId)
		{
			base.SetTaskAttemptId(taskId);
		}

		/// <summary>Set task status.</summary>
		/// <param name="status"/>
		[InterfaceAudience.Private]
		public virtual void SetTaskStatus(TaskCompletionEvent.Status status)
		{
			base.SetTaskStatus(TaskCompletionEvent.Status.ValueOf(status.ToString()));
		}

		/// <summary>Set the task completion time</summary>
		/// <param name="taskCompletionTime">time (in millisec) the task took to complete</param>
		[InterfaceAudience.Private]
		protected internal override void SetTaskRunTime(int taskCompletionTime)
		{
			base.SetTaskRunTime(taskCompletionTime);
		}

		/// <summary>set event Id.</summary>
		/// <remarks>set event Id. should be assigned incrementally starting from 0.</remarks>
		/// <param name="eventId"/>
		[InterfaceAudience.Private]
		protected internal override void SetEventId(int eventId)
		{
			base.SetEventId(eventId);
		}

		/// <summary>Set task tracker http location.</summary>
		/// <param name="taskTrackerHttp"/>
		[InterfaceAudience.Private]
		protected internal override void SetTaskTrackerHttp(string taskTrackerHttp)
		{
			base.SetTaskTrackerHttp(taskTrackerHttp);
		}
	}
}
