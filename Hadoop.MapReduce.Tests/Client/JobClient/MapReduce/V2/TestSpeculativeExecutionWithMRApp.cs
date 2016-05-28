using System.Collections.Generic;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestSpeculativeExecutionWithMRApp
	{
		private const int NumMappers = 5;

		private const int NumReducers = 0;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSpeculateSuccessfulWithoutUpdateEvents()
		{
			Clock actualClock = new SystemClock();
			ControlledClock clock = new ControlledClock(actualClock);
			clock.SetTime(Runtime.CurrentTimeMillis());
			MRApp app = new MRApp(NumMappers, NumReducers, false, "test", true, clock);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration(), 
				true, true);
			app.WaitForState(job, JobState.Running);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			NUnit.Framework.Assert.AreEqual("Num tasks is not correct", NumMappers + NumReducers
				, tasks.Count);
			IEnumerator<Task> taskIter = tasks.Values.GetEnumerator();
			while (taskIter.HasNext())
			{
				app.WaitForState(taskIter.Next(), TaskState.Running);
			}
			// Process the update events
			clock.SetTime(Runtime.CurrentTimeMillis() + 2000);
			EventHandler appEventHandler = app.GetContext().GetEventHandler();
			foreach (KeyValuePair<TaskId, Task> mapTask in tasks)
			{
				foreach (KeyValuePair<TaskAttemptId, TaskAttempt> taskAttempt in mapTask.Value.GetAttempts
					())
				{
					TaskAttemptStatusUpdateEvent.TaskAttemptStatus status = CreateTaskAttemptStatus(taskAttempt
						.Key, (float)0.8, TaskAttemptState.Running);
					TaskAttemptStatusUpdateEvent @event = new TaskAttemptStatusUpdateEvent(taskAttempt
						.Key, status);
					appEventHandler.Handle(@event);
				}
			}
			Random generator = new Random();
			object[] taskValues = Sharpen.Collections.ToArray(tasks.Values);
			Task taskToBeSpeculated = (Task)taskValues[generator.Next(taskValues.Length)];
			// Other than one random task, finish every other task.
			foreach (KeyValuePair<TaskId, Task> mapTask_1 in tasks)
			{
				foreach (KeyValuePair<TaskAttemptId, TaskAttempt> taskAttempt in mapTask_1.Value.
					GetAttempts())
				{
					if (mapTask_1.Key != taskToBeSpeculated.GetID())
					{
						appEventHandler.Handle(new TaskAttemptEvent(taskAttempt.Key, TaskAttemptEventType
							.TaDone));
						appEventHandler.Handle(new TaskAttemptEvent(taskAttempt.Key, TaskAttemptEventType
							.TaContainerCleaned));
						app.WaitForState(taskAttempt.Value, TaskAttemptState.Succeeded);
					}
				}
			}
			GenericTestUtils.WaitFor(new _Supplier_111(taskToBeSpeculated, clock), 1000, 60000
				);
			// finish 1st TA, 2nd will be killed
			TaskAttempt[] ta = MakeFirstAttemptWin(appEventHandler, taskToBeSpeculated);
			VerifySpeculationMessage(app, ta);
			app.WaitForState(Service.STATE.Stopped);
		}

		private sealed class _Supplier_111 : Supplier<bool>
		{
			public _Supplier_111(Task taskToBeSpeculated, ControlledClock clock)
			{
				this.taskToBeSpeculated = taskToBeSpeculated;
				this.clock = clock;
			}

			public bool Get()
			{
				if (taskToBeSpeculated.GetAttempts().Count != 2)
				{
					clock.SetTime(Runtime.CurrentTimeMillis() + 1000);
					return false;
				}
				else
				{
					return true;
				}
			}

			private readonly Task taskToBeSpeculated;

			private readonly ControlledClock clock;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSepculateSuccessfulWithUpdateEvents()
		{
			Clock actualClock = new SystemClock();
			ControlledClock clock = new ControlledClock(actualClock);
			clock.SetTime(Runtime.CurrentTimeMillis());
			MRApp app = new MRApp(NumMappers, NumReducers, false, "test", true, clock);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration(), 
				true, true);
			app.WaitForState(job, JobState.Running);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			NUnit.Framework.Assert.AreEqual("Num tasks is not correct", NumMappers + NumReducers
				, tasks.Count);
			IEnumerator<Task> taskIter = tasks.Values.GetEnumerator();
			while (taskIter.HasNext())
			{
				app.WaitForState(taskIter.Next(), TaskState.Running);
			}
			// Process the update events
			clock.SetTime(Runtime.CurrentTimeMillis() + 1000);
			EventHandler appEventHandler = app.GetContext().GetEventHandler();
			foreach (KeyValuePair<TaskId, Task> mapTask in tasks)
			{
				foreach (KeyValuePair<TaskAttemptId, TaskAttempt> taskAttempt in mapTask.Value.GetAttempts
					())
				{
					TaskAttemptStatusUpdateEvent.TaskAttemptStatus status = CreateTaskAttemptStatus(taskAttempt
						.Key, (float)0.5, TaskAttemptState.Running);
					TaskAttemptStatusUpdateEvent @event = new TaskAttemptStatusUpdateEvent(taskAttempt
						.Key, status);
					appEventHandler.Handle(@event);
				}
			}
			Task speculatedTask = null;
			int numTasksToFinish = NumMappers + NumReducers - 1;
			clock.SetTime(Runtime.CurrentTimeMillis() + 1000);
			foreach (KeyValuePair<TaskId, Task> task in tasks)
			{
				foreach (KeyValuePair<TaskAttemptId, TaskAttempt> taskAttempt in task.Value.GetAttempts
					())
				{
					if (numTasksToFinish > 0)
					{
						appEventHandler.Handle(new TaskAttemptEvent(taskAttempt.Key, TaskAttemptEventType
							.TaDone));
						appEventHandler.Handle(new TaskAttemptEvent(taskAttempt.Key, TaskAttemptEventType
							.TaContainerCleaned));
						numTasksToFinish--;
						app.WaitForState(taskAttempt.Value, TaskAttemptState.Succeeded);
					}
					else
					{
						// The last task is chosen for speculation
						TaskAttemptStatusUpdateEvent.TaskAttemptStatus status = CreateTaskAttemptStatus(taskAttempt
							.Key, (float)0.75, TaskAttemptState.Running);
						speculatedTask = task.Value;
						TaskAttemptStatusUpdateEvent @event = new TaskAttemptStatusUpdateEvent(taskAttempt
							.Key, status);
						appEventHandler.Handle(@event);
					}
				}
			}
			clock.SetTime(Runtime.CurrentTimeMillis() + 15000);
			foreach (KeyValuePair<TaskId, Task> task_1 in tasks)
			{
				foreach (KeyValuePair<TaskAttemptId, TaskAttempt> taskAttempt in task_1.Value.GetAttempts
					())
				{
					if (taskAttempt.Value.GetState() != TaskAttemptState.Succeeded)
					{
						TaskAttemptStatusUpdateEvent.TaskAttemptStatus status = CreateTaskAttemptStatus(taskAttempt
							.Key, (float)0.75, TaskAttemptState.Running);
						TaskAttemptStatusUpdateEvent @event = new TaskAttemptStatusUpdateEvent(taskAttempt
							.Key, status);
						appEventHandler.Handle(@event);
					}
				}
			}
			Task speculatedTaskConst = speculatedTask;
			GenericTestUtils.WaitFor(new _Supplier_205(speculatedTaskConst, clock), 1000, 60000
				);
			TaskAttempt[] ta = MakeFirstAttemptWin(appEventHandler, speculatedTask);
			VerifySpeculationMessage(app, ta);
			app.WaitForState(Service.STATE.Stopped);
		}

		private sealed class _Supplier_205 : Supplier<bool>
		{
			public _Supplier_205(Task speculatedTaskConst, ControlledClock clock)
			{
				this.speculatedTaskConst = speculatedTaskConst;
				this.clock = clock;
			}

			public bool Get()
			{
				if (speculatedTaskConst.GetAttempts().Count != 2)
				{
					clock.SetTime(Runtime.CurrentTimeMillis() + 1000);
					return false;
				}
				else
				{
					return true;
				}
			}

			private readonly Task speculatedTaskConst;

			private readonly ControlledClock clock;
		}

		private static TaskAttempt[] MakeFirstAttemptWin(EventHandler appEventHandler, Task
			 speculatedTask)
		{
			// finish 1st TA, 2nd will be killed
			ICollection<TaskAttempt> attempts = speculatedTask.GetAttempts().Values;
			TaskAttempt[] ta = new TaskAttempt[attempts.Count];
			Sharpen.Collections.ToArray(attempts, ta);
			appEventHandler.Handle(new TaskAttemptEvent(ta[0].GetID(), TaskAttemptEventType.TaDone
				));
			appEventHandler.Handle(new TaskAttemptEvent(ta[0].GetID(), TaskAttemptEventType.TaContainerCleaned
				));
			return ta;
		}

		/// <exception cref="System.Exception"/>
		private static void VerifySpeculationMessage(MRApp app, TaskAttempt[] ta)
		{
			app.WaitForState(ta[0], TaskAttemptState.Succeeded);
		}

		// The speculative attempt may be not killed before the MR job succeeds.
		private TaskAttemptStatusUpdateEvent.TaskAttemptStatus CreateTaskAttemptStatus(TaskAttemptId
			 id, float progress, TaskAttemptState state)
		{
			TaskAttemptStatusUpdateEvent.TaskAttemptStatus status = new TaskAttemptStatusUpdateEvent.TaskAttemptStatus
				();
			status.id = id;
			status.progress = progress;
			status.taskState = state;
			return status;
		}
	}
}
