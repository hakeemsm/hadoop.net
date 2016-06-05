using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class TestFetchFailure
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFetchFailure()
		{
			MRApp app = new MRApp(1, 1, false, this.GetType().FullName, true);
			Configuration conf = new Configuration();
			// map -> reduce -> fetch-failure -> map retry is incompatible with
			// sequential, single-task-attempt approach in uber-AM, so disable:
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 2, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask = it.Next();
			Task reduceTask = it.Next();
			//wait for Task state move to RUNNING
			app.WaitForState(mapTask, TaskState.Running);
			TaskAttempt mapAttempt1 = mapTask.GetAttempts().Values.GetEnumerator().Next();
			app.WaitForState(mapAttempt1, TaskAttemptState.Running);
			//send the done signal to the map attempt
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapAttempt1.GetID(
				), TaskAttemptEventType.TaDone));
			// wait for map success
			app.WaitForState(mapTask, TaskState.Succeeded);
			TaskAttemptCompletionEvent[] events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Num completion events not correct", 1, events.Length
				);
			NUnit.Framework.Assert.AreEqual("Event status not correct", TaskAttemptCompletionEventStatus
				.Succeeded, events[0].GetStatus());
			// wait for reduce to start running
			app.WaitForState(reduceTask, TaskState.Running);
			TaskAttempt reduceAttempt = reduceTask.GetAttempts().Values.GetEnumerator().Next(
				);
			app.WaitForState(reduceAttempt, TaskAttemptState.Running);
			//send 3 fetch failures from reduce to trigger map re execution
			SendFetchFailure(app, reduceAttempt, mapAttempt1);
			SendFetchFailure(app, reduceAttempt, mapAttempt1);
			SendFetchFailure(app, reduceAttempt, mapAttempt1);
			//wait for map Task state move back to RUNNING
			app.WaitForState(mapTask, TaskState.Running);
			//map attempt must have become FAILED
			NUnit.Framework.Assert.AreEqual("Map TaskAttempt state not correct", TaskAttemptState
				.Failed, mapAttempt1.GetState());
			NUnit.Framework.Assert.AreEqual("Num attempts in Map Task not correct", 2, mapTask
				.GetAttempts().Count);
			IEnumerator<TaskAttempt> atIt = mapTask.GetAttempts().Values.GetEnumerator();
			atIt.Next();
			TaskAttempt mapAttempt2 = atIt.Next();
			app.WaitForState(mapAttempt2, TaskAttemptState.Running);
			//send the done signal to the second map attempt
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapAttempt2.GetID(
				), TaskAttemptEventType.TaDone));
			// wait for map success
			app.WaitForState(mapTask, TaskState.Succeeded);
			//send done to reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduceAttempt.GetID
				(), TaskAttemptEventType.TaDone));
			app.WaitForState(job, JobState.Succeeded);
			//previous completion event now becomes obsolete
			NUnit.Framework.Assert.AreEqual("Event status not correct", TaskAttemptCompletionEventStatus
				.Obsolete, events[0].GetStatus());
			events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Num completion events not correct", 4, events.Length
				);
			NUnit.Framework.Assert.AreEqual("Event map attempt id not correct", mapAttempt1.GetID
				(), events[0].GetAttemptId());
			NUnit.Framework.Assert.AreEqual("Event map attempt id not correct", mapAttempt1.GetID
				(), events[1].GetAttemptId());
			NUnit.Framework.Assert.AreEqual("Event map attempt id not correct", mapAttempt2.GetID
				(), events[2].GetAttemptId());
			NUnit.Framework.Assert.AreEqual("Event redude attempt id not correct", reduceAttempt
				.GetID(), events[3].GetAttemptId());
			NUnit.Framework.Assert.AreEqual("Event status not correct for map attempt1", TaskAttemptCompletionEventStatus
				.Obsolete, events[0].GetStatus());
			NUnit.Framework.Assert.AreEqual("Event status not correct for map attempt1", TaskAttemptCompletionEventStatus
				.Failed, events[1].GetStatus());
			NUnit.Framework.Assert.AreEqual("Event status not correct for map attempt2", TaskAttemptCompletionEventStatus
				.Succeeded, events[2].GetStatus());
			NUnit.Framework.Assert.AreEqual("Event status not correct for reduce attempt1", TaskAttemptCompletionEventStatus
				.Succeeded, events[3].GetStatus());
			TaskCompletionEvent[] mapEvents = job.GetMapAttemptCompletionEvents(0, 2);
			TaskCompletionEvent[] convertedEvents = TypeConverter.FromYarn(events);
			NUnit.Framework.Assert.AreEqual("Incorrect number of map events", 2, mapEvents.Length
				);
			Assert.AssertArrayEquals("Unexpected map events", Arrays.CopyOfRange(convertedEvents
				, 0, 2), mapEvents);
			mapEvents = job.GetMapAttemptCompletionEvents(2, 200);
			NUnit.Framework.Assert.AreEqual("Incorrect number of map events", 1, mapEvents.Length
				);
			NUnit.Framework.Assert.AreEqual("Unexpected map event", convertedEvents[2], mapEvents
				[0]);
		}

		/// <summary>
		/// This tests that if a map attempt was failed (say due to fetch failures),
		/// then it gets re-run.
		/// </summary>
		/// <remarks>
		/// This tests that if a map attempt was failed (say due to fetch failures),
		/// then it gets re-run. When the next map attempt is running, if the AM dies,
		/// then, on AM re-run, the AM does not incorrectly remember the first failed
		/// attempt. Currently recovery does not recover running tasks. Effectively,
		/// the AM re-runs the maps from scratch.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFetchFailureWithRecovery()
		{
			int runCount = 0;
			MRApp app = new TestFetchFailure.MRAppWithHistory(1, 1, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			// map -> reduce -> fetch-failure -> map retry is incompatible with
			// sequential, single-task-attempt approach in uber-AM, so disable:
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 2, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask = it.Next();
			Task reduceTask = it.Next();
			//wait for Task state move to RUNNING
			app.WaitForState(mapTask, TaskState.Running);
			TaskAttempt mapAttempt1 = mapTask.GetAttempts().Values.GetEnumerator().Next();
			app.WaitForState(mapAttempt1, TaskAttemptState.Running);
			//send the done signal to the map attempt
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapAttempt1.GetID(
				), TaskAttemptEventType.TaDone));
			// wait for map success
			app.WaitForState(mapTask, TaskState.Succeeded);
			TaskAttemptCompletionEvent[] events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Num completion events not correct", 1, events.Length
				);
			NUnit.Framework.Assert.AreEqual("Event status not correct", TaskAttemptCompletionEventStatus
				.Succeeded, events[0].GetStatus());
			// wait for reduce to start running
			app.WaitForState(reduceTask, TaskState.Running);
			TaskAttempt reduceAttempt = reduceTask.GetAttempts().Values.GetEnumerator().Next(
				);
			app.WaitForState(reduceAttempt, TaskAttemptState.Running);
			//send 3 fetch failures from reduce to trigger map re execution
			SendFetchFailure(app, reduceAttempt, mapAttempt1);
			SendFetchFailure(app, reduceAttempt, mapAttempt1);
			SendFetchFailure(app, reduceAttempt, mapAttempt1);
			//wait for map Task state move back to RUNNING
			app.WaitForState(mapTask, TaskState.Running);
			// Crash the app again.
			app.Stop();
			//rerun
			app = new TestFetchFailure.MRAppWithHistory(1, 1, false, this.GetType().FullName, 
				false, ++runCount);
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, true);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 2, job.GetTasks().Count);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask = it.Next();
			reduceTask = it.Next();
			// the map is not in a SUCCEEDED state after restart of AM
			app.WaitForState(mapTask, TaskState.Running);
			mapAttempt1 = mapTask.GetAttempts().Values.GetEnumerator().Next();
			app.WaitForState(mapAttempt1, TaskAttemptState.Running);
			//send the done signal to the map attempt
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapAttempt1.GetID(
				), TaskAttemptEventType.TaDone));
			// wait for map success
			app.WaitForState(mapTask, TaskState.Succeeded);
			reduceAttempt = reduceTask.GetAttempts().Values.GetEnumerator().Next();
			//send done to reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduceAttempt.GetID
				(), TaskAttemptEventType.TaDone));
			app.WaitForState(job, JobState.Succeeded);
			events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Num completion events not correct", 2, events.Length
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFetchFailureMultipleReduces()
		{
			MRApp app = new MRApp(1, 3, false, this.GetType().FullName, true);
			Configuration conf = new Configuration();
			// map -> reduce -> fetch-failure -> map retry is incompatible with
			// sequential, single-task-attempt approach in uber-AM, so disable:
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 4, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask = it.Next();
			Task reduceTask = it.Next();
			Task reduceTask2 = it.Next();
			Task reduceTask3 = it.Next();
			//wait for Task state move to RUNNING
			app.WaitForState(mapTask, TaskState.Running);
			TaskAttempt mapAttempt1 = mapTask.GetAttempts().Values.GetEnumerator().Next();
			app.WaitForState(mapAttempt1, TaskAttemptState.Running);
			//send the done signal to the map attempt
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapAttempt1.GetID(
				), TaskAttemptEventType.TaDone));
			// wait for map success
			app.WaitForState(mapTask, TaskState.Succeeded);
			TaskAttemptCompletionEvent[] events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Num completion events not correct", 1, events.Length
				);
			NUnit.Framework.Assert.AreEqual("Event status not correct", TaskAttemptCompletionEventStatus
				.Succeeded, events[0].GetStatus());
			// wait for reduce to start running
			app.WaitForState(reduceTask, TaskState.Running);
			app.WaitForState(reduceTask2, TaskState.Running);
			app.WaitForState(reduceTask3, TaskState.Running);
			TaskAttempt reduceAttempt = reduceTask.GetAttempts().Values.GetEnumerator().Next(
				);
			app.WaitForState(reduceAttempt, TaskAttemptState.Running);
			UpdateStatus(app, reduceAttempt, Phase.Shuffle);
			TaskAttempt reduceAttempt2 = reduceTask2.GetAttempts().Values.GetEnumerator().Next
				();
			app.WaitForState(reduceAttempt2, TaskAttemptState.Running);
			UpdateStatus(app, reduceAttempt2, Phase.Shuffle);
			TaskAttempt reduceAttempt3 = reduceTask3.GetAttempts().Values.GetEnumerator().Next
				();
			app.WaitForState(reduceAttempt3, TaskAttemptState.Running);
			UpdateStatus(app, reduceAttempt3, Phase.Shuffle);
			//send 2 fetch failures from reduce to prepare for map re execution
			SendFetchFailure(app, reduceAttempt, mapAttempt1);
			SendFetchFailure(app, reduceAttempt, mapAttempt1);
			//We should not re-launch the map task yet
			NUnit.Framework.Assert.AreEqual(TaskState.Succeeded, mapTask.GetState());
			UpdateStatus(app, reduceAttempt2, Phase.Reduce);
			UpdateStatus(app, reduceAttempt3, Phase.Reduce);
			//send 3rd fetch failures from reduce to trigger map re execution
			SendFetchFailure(app, reduceAttempt, mapAttempt1);
			//wait for map Task state move back to RUNNING
			app.WaitForState(mapTask, TaskState.Running);
			//map attempt must have become FAILED
			NUnit.Framework.Assert.AreEqual("Map TaskAttempt state not correct", TaskAttemptState
				.Failed, mapAttempt1.GetState());
			NUnit.Framework.Assert.AreEqual("Num attempts in Map Task not correct", 2, mapTask
				.GetAttempts().Count);
			IEnumerator<TaskAttempt> atIt = mapTask.GetAttempts().Values.GetEnumerator();
			atIt.Next();
			TaskAttempt mapAttempt2 = atIt.Next();
			app.WaitForState(mapAttempt2, TaskAttemptState.Running);
			//send the done signal to the second map attempt
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapAttempt2.GetID(
				), TaskAttemptEventType.TaDone));
			// wait for map success
			app.WaitForState(mapTask, TaskState.Succeeded);
			//send done to reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduceAttempt.GetID
				(), TaskAttemptEventType.TaDone));
			//send done to reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduceAttempt2.GetID
				(), TaskAttemptEventType.TaDone));
			//send done to reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduceAttempt3.GetID
				(), TaskAttemptEventType.TaDone));
			app.WaitForState(job, JobState.Succeeded);
			//previous completion event now becomes obsolete
			NUnit.Framework.Assert.AreEqual("Event status not correct", TaskAttemptCompletionEventStatus
				.Obsolete, events[0].GetStatus());
			events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Num completion events not correct", 6, events.Length
				);
			NUnit.Framework.Assert.AreEqual("Event map attempt id not correct", mapAttempt1.GetID
				(), events[0].GetAttemptId());
			NUnit.Framework.Assert.AreEqual("Event map attempt id not correct", mapAttempt1.GetID
				(), events[1].GetAttemptId());
			NUnit.Framework.Assert.AreEqual("Event map attempt id not correct", mapAttempt2.GetID
				(), events[2].GetAttemptId());
			NUnit.Framework.Assert.AreEqual("Event reduce attempt id not correct", reduceAttempt
				.GetID(), events[3].GetAttemptId());
			NUnit.Framework.Assert.AreEqual("Event status not correct for map attempt1", TaskAttemptCompletionEventStatus
				.Obsolete, events[0].GetStatus());
			NUnit.Framework.Assert.AreEqual("Event status not correct for map attempt1", TaskAttemptCompletionEventStatus
				.Failed, events[1].GetStatus());
			NUnit.Framework.Assert.AreEqual("Event status not correct for map attempt2", TaskAttemptCompletionEventStatus
				.Succeeded, events[2].GetStatus());
			NUnit.Framework.Assert.AreEqual("Event status not correct for reduce attempt1", TaskAttemptCompletionEventStatus
				.Succeeded, events[3].GetStatus());
			TaskCompletionEvent[] mapEvents = job.GetMapAttemptCompletionEvents(0, 2);
			TaskCompletionEvent[] convertedEvents = TypeConverter.FromYarn(events);
			NUnit.Framework.Assert.AreEqual("Incorrect number of map events", 2, mapEvents.Length
				);
			Assert.AssertArrayEquals("Unexpected map events", Arrays.CopyOfRange(convertedEvents
				, 0, 2), mapEvents);
			mapEvents = job.GetMapAttemptCompletionEvents(2, 200);
			NUnit.Framework.Assert.AreEqual("Incorrect number of map events", 1, mapEvents.Length
				);
			NUnit.Framework.Assert.AreEqual("Unexpected map event", convertedEvents[2], mapEvents
				[0]);
		}

		private void UpdateStatus(MRApp app, TaskAttempt attempt, Phase phase)
		{
			TaskAttemptStatusUpdateEvent.TaskAttemptStatus status = new TaskAttemptStatusUpdateEvent.TaskAttemptStatus
				();
			status.counters = new Counters();
			status.fetchFailedMaps = new AList<TaskAttemptId>();
			status.id = attempt.GetID();
			status.mapFinishTime = 0;
			status.phase = phase;
			status.progress = 0.5f;
			status.shuffleFinishTime = 0;
			status.sortFinishTime = 0;
			status.stateString = "OK";
			status.taskState = attempt.GetState();
			TaskAttemptStatusUpdateEvent @event = new TaskAttemptStatusUpdateEvent(attempt.GetID
				(), status);
			app.GetContext().GetEventHandler().Handle(@event);
		}

		private void SendFetchFailure(MRApp app, TaskAttempt reduceAttempt, TaskAttempt mapAttempt
			)
		{
			app.GetContext().GetEventHandler().Handle(new JobTaskAttemptFetchFailureEvent(reduceAttempt
				.GetID(), Arrays.AsList(new TaskAttemptId[] { mapAttempt.GetID() })));
		}

		internal class MRAppWithHistory : MRApp
		{
			public MRAppWithHistory(int maps, int reduces, bool autoComplete, string testName
				, bool cleanOnStart, int startCount)
				: base(maps, reduces, autoComplete, testName, cleanOnStart, startCount)
			{
			}

			protected internal override EventHandler<JobHistoryEvent> CreateJobHistoryHandler
				(AppContext context)
			{
				JobHistoryEventHandler eventHandler = new JobHistoryEventHandler(context, GetStartCount
					());
				return eventHandler;
			}
		}
	}
}
