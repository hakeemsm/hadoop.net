using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	/// <summary>Tests the state machine with respect to Job/Task/TaskAttempt kill scenarios.
	/// 	</summary>
	public class TestKill
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillJob()
		{
			CountDownLatch latch = new CountDownLatch(1);
			MRApp app = new TestKill.BlockingMRApp(1, 0, latch);
			//this will start the job but job won't complete as task is
			//blocked
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			//wait and vailidate for Job to become RUNNING
			app.WaitForState(job, JobState.Running);
			//send the kill signal to Job
			app.GetContext().GetEventHandler().Handle(new JobEvent(job.GetID(), JobEventType.
				JobKill));
			//unblock Task
			latch.CountDown();
			//wait and validate for Job to be KILLED
			app.WaitForState(job, JobState.Killed);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			NUnit.Framework.Assert.AreEqual("No of tasks is not correct", 1, tasks.Count);
			Task task = tasks.Values.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual("Task state not correct", TaskState.Killed, task.
				GetReport().GetTaskState());
			IDictionary<TaskAttemptId, TaskAttempt> attempts = tasks.Values.GetEnumerator().Next
				().GetAttempts();
			NUnit.Framework.Assert.AreEqual("No of attempts is not correct", 1, attempts.Count
				);
			IEnumerator<TaskAttempt> it = attempts.Values.GetEnumerator();
			NUnit.Framework.Assert.AreEqual("Attempt state not correct", TaskAttemptState.Killed
				, it.Next().GetReport().GetTaskAttemptState());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillTask()
		{
			CountDownLatch latch = new CountDownLatch(1);
			MRApp app = new TestKill.BlockingMRApp(2, 0, latch);
			//this will start the job but job won't complete as Task is blocked
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			//wait and vailidate for Job to become RUNNING
			app.WaitForState(job, JobState.Running);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			NUnit.Framework.Assert.AreEqual("No of tasks is not correct", 2, tasks.Count);
			IEnumerator<Task> it = tasks.Values.GetEnumerator();
			Task task1 = it.Next();
			Task task2 = it.Next();
			//send the kill signal to the first Task
			app.GetContext().GetEventHandler().Handle(new TaskEvent(task1.GetID(), TaskEventType
				.TKill));
			//unblock Task
			latch.CountDown();
			//wait and validate for Job to become SUCCEEDED
			app.WaitForState(job, JobState.Succeeded);
			//first Task is killed and second is Succeeded
			//Job is succeeded
			NUnit.Framework.Assert.AreEqual("Task state not correct", TaskState.Killed, task1
				.GetReport().GetTaskState());
			NUnit.Framework.Assert.AreEqual("Task state not correct", TaskState.Succeeded, task2
				.GetReport().GetTaskState());
			IDictionary<TaskAttemptId, TaskAttempt> attempts = task1.GetAttempts();
			NUnit.Framework.Assert.AreEqual("No of attempts is not correct", 1, attempts.Count
				);
			IEnumerator<TaskAttempt> iter = attempts.Values.GetEnumerator();
			NUnit.Framework.Assert.AreEqual("Attempt state not correct", TaskAttemptState.Killed
				, iter.Next().GetReport().GetTaskAttemptState());
			attempts = task2.GetAttempts();
			NUnit.Framework.Assert.AreEqual("No of attempts is not correct", 1, attempts.Count
				);
			iter = attempts.Values.GetEnumerator();
			NUnit.Framework.Assert.AreEqual("Attempt state not correct", TaskAttemptState.Succeeded
				, iter.Next().GetReport().GetTaskAttemptState());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillTaskWait()
		{
			Dispatcher dispatcher = new _AsyncDispatcher_145();
			// Task is asking the reduce TA to kill itself. 'Create' a race
			// condition. Make the task succeed and then inform the task that
			// TA has succeeded. Once Task gets the TA succeeded event at
			// KILL_WAIT, then relay the actual kill signal to TA
			// When the TA comes and reports that it is done, send the
			// cachedKillEvent
			MRApp app = new _MRApp_183(dispatcher, 1, 1, false, this.GetType().FullName, true
				);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			JobId jobId = app.GetJobId();
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 2, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask = it.Next();
			Task reduceTask = it.Next();
			app.WaitForState(mapTask, TaskState.Running);
			app.WaitForState(reduceTask, TaskState.Running);
			TaskAttempt mapAttempt = mapTask.GetAttempts().Values.GetEnumerator().Next();
			app.WaitForState(mapAttempt, TaskAttemptState.Running);
			TaskAttempt reduceAttempt = reduceTask.GetAttempts().Values.GetEnumerator().Next(
				);
			app.WaitForState(reduceAttempt, TaskAttemptState.Running);
			// Finish map
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapAttempt.GetID()
				, TaskAttemptEventType.TaDone));
			app.WaitForState(mapTask, TaskState.Succeeded);
			// Now kill the job
			app.GetContext().GetEventHandler().Handle(new JobEvent(jobId, JobEventType.JobKill
				));
			app.WaitForInternalState((JobImpl)job, JobStateInternal.Killed);
		}

		private sealed class _AsyncDispatcher_145 : AsyncDispatcher
		{
			public _AsyncDispatcher_145()
			{
			}

			private TaskAttemptEvent cachedKillEvent;

			protected override void Dispatch(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				if (@event is TaskAttemptEvent)
				{
					TaskAttemptEvent killEvent = (TaskAttemptEvent)@event;
					if (killEvent.GetType() == TaskAttemptEventType.TaKill)
					{
						TaskAttemptId taID = killEvent.GetTaskAttemptID();
						if (taID.GetTaskId().GetTaskType() == TaskType.Reduce && taID.GetTaskId().GetId()
							 == 0 && taID.GetId() == 0)
						{
							base.Dispatch(new TaskAttemptEvent(taID, TaskAttemptEventType.TaDone));
							base.Dispatch(new TaskAttemptEvent(taID, TaskAttemptEventType.TaContainerCleaned)
								);
							base.Dispatch(new TaskTAttemptEvent(taID, TaskEventType.TAttemptSucceeded));
							this.cachedKillEvent = killEvent;
							return;
						}
					}
				}
				else
				{
					if (@event is TaskEvent)
					{
						TaskEvent taskEvent = (TaskEvent)@event;
						if (taskEvent.GetType() == TaskEventType.TAttemptSucceeded && this.cachedKillEvent
							 != null)
						{
							base.Dispatch(this.cachedKillEvent);
							return;
						}
					}
				}
				base.Dispatch(@event);
			}
		}

		private sealed class _MRApp_183 : MRApp
		{
			public _MRApp_183(Dispatcher dispatcher, int baseArg1, int baseArg2, bool baseArg3
				, string baseArg4, bool baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly Dispatcher dispatcher;
		}

		internal class MyAsyncDispatch : AsyncDispatcher
		{
			private CountDownLatch latch;

			private TaskAttemptEventType attemptEventTypeToWait;

			internal MyAsyncDispatch(CountDownLatch latch, TaskAttemptEventType attemptEventTypeToWait
				)
				: base()
			{
				this.latch = latch;
				this.attemptEventTypeToWait = attemptEventTypeToWait;
			}

			protected override void Dispatch(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				if (@event is TaskAttemptEvent)
				{
					TaskAttemptEvent attemptEvent = (TaskAttemptEvent)@event;
					TaskAttemptId attemptID = ((TaskAttemptEvent)@event).GetTaskAttemptID();
					if (attemptEvent.GetType() == this.attemptEventTypeToWait && attemptID.GetTaskId(
						).GetId() == 0 && attemptID.GetId() == 0)
					{
						try
						{
							latch.Await();
						}
						catch (Exception e)
						{
							Sharpen.Runtime.PrintStackTrace(e);
						}
					}
				}
				base.Dispatch(@event);
			}
		}

		// This is to test a race condition where JobEventType.JOB_KILL is generated
		// right after TaskAttemptEventType.TA_DONE is generated.
		// TaskImpl's state machine might receive both T_ATTEMPT_SUCCEEDED
		// and T_ATTEMPT_KILLED from the same attempt.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillTaskWaitKillJobAfterTA_DONE()
		{
			CountDownLatch latch = new CountDownLatch(1);
			Dispatcher dispatcher = new TestKill.MyAsyncDispatch(latch, TaskAttemptEventType.
				TaDone);
			MRApp app = new _MRApp_252(dispatcher, 1, 1, false, this.GetType().FullName, true
				);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			JobId jobId = app.GetJobId();
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 2, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask = it.Next();
			Task reduceTask = it.Next();
			app.WaitForState(mapTask, TaskState.Running);
			app.WaitForState(reduceTask, TaskState.Running);
			TaskAttempt mapAttempt = mapTask.GetAttempts().Values.GetEnumerator().Next();
			app.WaitForState(mapAttempt, TaskAttemptState.Running);
			TaskAttempt reduceAttempt = reduceTask.GetAttempts().Values.GetEnumerator().Next(
				);
			app.WaitForState(reduceAttempt, TaskAttemptState.Running);
			// The order in the dispatch event queue, from the oldest to the newest
			// TA_DONE
			// JOB_KILL
			// CONTAINER_REMOTE_CLEANUP ( from TA_DONE's handling )
			// T_KILL ( from JOB_KILL's handling )
			// TA_CONTAINER_CLEANED ( from CONTAINER_REMOTE_CLEANUP's handling )
			// TA_KILL ( from T_KILL's handling )
			// T_ATTEMPT_SUCCEEDED ( from TA_CONTAINER_CLEANED's handling )
			// T_ATTEMPT_KILLED ( from TA_KILL's handling )
			// Finish map
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapAttempt.GetID()
				, TaskAttemptEventType.TaDone));
			// Now kill the job
			app.GetContext().GetEventHandler().Handle(new JobEvent(jobId, JobEventType.JobKill
				));
			//unblock
			latch.CountDown();
			app.WaitForInternalState((JobImpl)job, JobStateInternal.Killed);
		}

		private sealed class _MRApp_252 : MRApp
		{
			public _MRApp_252(Dispatcher dispatcher, int baseArg1, int baseArg2, bool baseArg3
				, string baseArg4, bool baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly Dispatcher dispatcher;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillTaskAttempt()
		{
			CountDownLatch latch = new CountDownLatch(1);
			MRApp app = new TestKill.BlockingMRApp(2, 0, latch);
			//this will start the job but job won't complete as Task is blocked
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			//wait and vailidate for Job to become RUNNING
			app.WaitForState(job, JobState.Running);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			NUnit.Framework.Assert.AreEqual("No of tasks is not correct", 2, tasks.Count);
			IEnumerator<Task> it = tasks.Values.GetEnumerator();
			Task task1 = it.Next();
			Task task2 = it.Next();
			//wait for tasks to become running
			app.WaitForState(task1, TaskState.Scheduled);
			app.WaitForState(task2, TaskState.Scheduled);
			//send the kill signal to the first Task's attempt
			TaskAttempt attempt = task1.GetAttempts().Values.GetEnumerator().Next();
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attempt.GetID(), TaskAttemptEventType
				.TaKill));
			//unblock
			latch.CountDown();
			//wait and validate for Job to become SUCCEEDED
			//job will still succeed
			app.WaitForState(job, JobState.Succeeded);
			//first Task will have two attempts 1st is killed, 2nd Succeeds
			//both Tasks and Job succeeds
			NUnit.Framework.Assert.AreEqual("Task state not correct", TaskState.Succeeded, task1
				.GetReport().GetTaskState());
			NUnit.Framework.Assert.AreEqual("Task state not correct", TaskState.Succeeded, task2
				.GetReport().GetTaskState());
			IDictionary<TaskAttemptId, TaskAttempt> attempts = task1.GetAttempts();
			NUnit.Framework.Assert.AreEqual("No of attempts is not correct", 2, attempts.Count
				);
			IEnumerator<TaskAttempt> iter = attempts.Values.GetEnumerator();
			NUnit.Framework.Assert.AreEqual("Attempt state not correct", TaskAttemptState.Killed
				, iter.Next().GetReport().GetTaskAttemptState());
			NUnit.Framework.Assert.AreEqual("Attempt state not correct", TaskAttemptState.Succeeded
				, iter.Next().GetReport().GetTaskAttemptState());
			attempts = task2.GetAttempts();
			NUnit.Framework.Assert.AreEqual("No of attempts is not correct", 1, attempts.Count
				);
			iter = attempts.Values.GetEnumerator();
			NUnit.Framework.Assert.AreEqual("Attempt state not correct", TaskAttemptState.Succeeded
				, iter.Next().GetReport().GetTaskAttemptState());
		}

		internal class BlockingMRApp : MRApp
		{
			private CountDownLatch latch;

			internal BlockingMRApp(int maps, int reduces, CountDownLatch latch)
				: base(maps, reduces, true, "testKill", true)
			{
				this.latch = latch;
			}

			protected internal override void AttemptLaunched(TaskAttemptId attemptID)
			{
				if (attemptID.GetTaskId().GetId() == 0 && attemptID.GetId() == 0)
				{
					//this blocks the first task's first attempt
					//the subsequent ones are completed
					try
					{
						latch.Await();
					}
					catch (Exception e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
				}
				else
				{
					GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
						.TaDone));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestKill t = new TestKill();
			t.TestKillJob();
			t.TestKillTask();
			t.TestKillTaskAttempt();
		}
	}
}
