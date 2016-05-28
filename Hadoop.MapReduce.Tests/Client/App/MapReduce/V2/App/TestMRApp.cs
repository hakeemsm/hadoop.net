using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.App.Launcher;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	/// <summary>Tests the state machine of MR App.</summary>
	public class TestMRApp
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMapReduce()
		{
			MRApp app = new MRApp(2, 2, true, this.GetType().FullName, true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			NUnit.Framework.Assert.AreEqual(Runtime.GetProperty("user.name"), job.GetUserName
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZeroMaps()
		{
			MRApp app = new MRApp(0, 1, true, this.GetType().FullName, true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZeroMapReduces()
		{
			MRApp app = new MRApp(0, 0, true, this.GetType().FullName, true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			app.WaitForState(job, JobState.Succeeded);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCommitPending()
		{
			MRApp app = new MRApp(1, 0, false, this.GetType().FullName, true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 1, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task task = it.Next();
			app.WaitForState(task, TaskState.Running);
			TaskAttempt attempt = task.GetAttempts().Values.GetEnumerator().Next();
			app.WaitForState(attempt, TaskAttemptState.Running);
			//send the commit pending signal to the task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attempt.GetID(), TaskAttemptEventType
				.TaCommitPending));
			//wait for first attempt to commit pending
			app.WaitForState(attempt, TaskAttemptState.CommitPending);
			//re-send the commit pending signal to the task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attempt.GetID(), TaskAttemptEventType
				.TaCommitPending));
			//the task attempt should be still at COMMIT_PENDING
			app.WaitForState(attempt, TaskAttemptState.CommitPending);
			//send the done signal to the task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task.GetAttempts()
				.Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			app.WaitForState(job, JobState.Succeeded);
		}

		//@Test
		/// <exception cref="System.Exception"/>
		public virtual void TestCompletedMapsForReduceSlowstart()
		{
			MRApp app = new MRApp(2, 1, false, this.GetType().FullName, true);
			Configuration conf = new Configuration();
			//after half of the map completion, reduce will start
			conf.SetFloat(MRJobConfig.CompletedMapsForReduceSlowstart, 0.5f);
			//uberization forces full slowstart (1.0), so disable that
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 3, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task mapTask2 = it.Next();
			Task reduceTask = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			TaskAttempt task1Attempt = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			TaskAttempt task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(task1Attempt, TaskAttemptState.Running);
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			// reduces must be in NEW state
			NUnit.Framework.Assert.AreEqual("Reduce Task state not correct", TaskState.New, reduceTask
				.GetReport().GetTaskState());
			//send the done signal to the 1st map task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask1.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			//wait for first map task to complete
			app.WaitForState(mapTask1, TaskState.Succeeded);
			//Once the first map completes, it will schedule the reduces
			//now reduce must be running
			app.WaitForState(reduceTask, TaskState.Running);
			//send the done signal to 2nd map and the reduce to complete the job
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask2.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduceTask.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			app.WaitForState(job, JobState.Succeeded);
		}

		/// <summary>The test verifies that the AM re-runs maps that have run on bad nodes.</summary>
		/// <remarks>
		/// The test verifies that the AM re-runs maps that have run on bad nodes. It
		/// also verifies that the AM records all success/killed events so that reduces
		/// are notified about map output status changes. It also verifies that the
		/// re-run information is preserved across AM restart
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdatedNodes()
		{
			int runCount = 0;
			MRApp app = new TestMRApp.MRAppWithHistory(this, 2, 2, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			// after half of the map completion, reduce will start
			conf.SetFloat(MRJobConfig.CompletedMapsForReduceSlowstart, 0.5f);
			// uberization forces full slowstart (1.0), so disable that
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 4, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task mapTask2 = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			TaskAttempt task1Attempt = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			TaskAttempt task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			NodeId node1 = task1Attempt.GetNodeId();
			NodeId node2 = task2Attempt.GetNodeId();
			NUnit.Framework.Assert.AreEqual(node1, node2);
			// send the done signal to the task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task2Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			// all maps must be succeeded
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Succeeded);
			TaskAttemptCompletionEvent[] events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Expecting 2 completion events for success", 2, events
				.Length);
			// send updated nodes info
			AList<NodeReport> updatedNodes = new AList<NodeReport>();
			NodeReport nr = RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance<NodeReport
				>();
			nr.SetNodeId(node1);
			nr.SetNodeState(NodeState.Unhealthy);
			updatedNodes.AddItem(nr);
			app.GetContext().GetEventHandler().Handle(new JobUpdatedNodesEvent(job.GetID(), updatedNodes
				));
			app.WaitForState(task1Attempt, TaskAttemptState.Killed);
			app.WaitForState(task2Attempt, TaskAttemptState.Killed);
			events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Expecting 2 more completion events for killed", 
				4, events.Length);
			// all maps must be back to running
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			IEnumerator<TaskAttempt> itr = mapTask1.GetAttempts().Values.GetEnumerator();
			itr.Next();
			task1Attempt = itr.Next();
			// send the done signal to the task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			// map1 must be succeeded. map2 must be running
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Running);
			events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Expecting 1 more completion events for success", 
				5, events.Length);
			// Crash the app again.
			app.Stop();
			// rerun
			// in rerun the 1st map will be recovered from previous run
			app = new TestMRApp.MRAppWithHistory(this, 2, 2, false, this.GetType().FullName, 
				false, ++runCount);
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, true);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 4, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			mapTask2 = it.Next();
			Task reduceTask1 = it.Next();
			Task reduceTask2 = it.Next();
			// map 1 will be recovered, no need to send done
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Running);
			events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Expecting 2 completion events for killed & success of map1"
				, 2, events.Length);
			task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task2Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			app.WaitForState(mapTask2, TaskState.Succeeded);
			events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Expecting 1 more completion events for success", 
				3, events.Length);
			app.WaitForState(reduceTask1, TaskState.Running);
			app.WaitForState(reduceTask2, TaskState.Running);
			TaskAttempt task3Attempt = reduceTask1.GetAttempts().Values.GetEnumerator().Next(
				);
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task3Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			app.WaitForState(reduceTask1, TaskState.Succeeded);
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task3Attempt.GetID
				(), TaskAttemptEventType.TaKill));
			app.WaitForState(reduceTask1, TaskState.Succeeded);
			TaskAttempt task4Attempt = reduceTask2.GetAttempts().Values.GetEnumerator().Next(
				);
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task4Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			app.WaitForState(reduceTask2, TaskState.Succeeded);
			events = job.GetTaskAttemptCompletionEvents(0, 100);
			NUnit.Framework.Assert.AreEqual("Expecting 2 more completion events for reduce success"
				, 5, events.Length);
			// job succeeds
			app.WaitForState(job, JobState.Succeeded);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobError()
		{
			MRApp app = new MRApp(1, 0, false, this.GetType().FullName, true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 1, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task task = it.Next();
			app.WaitForState(task, TaskState.Running);
			//send an invalid event on task at current state
			app.GetContext().GetEventHandler().Handle(new TaskEvent(task.GetID(), TaskEventType
				.TSchedule));
			//this must lead to job error
			app.WaitForState(job, JobState.Error);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobSuccess()
		{
			MRApp app = new MRApp(2, 2, true, this.GetType().FullName, true, false);
			JobImpl job = (JobImpl)app.Submit(new Configuration());
			app.WaitForInternalState(job, JobStateInternal.Succeeded);
			// AM is not unregistered
			NUnit.Framework.Assert.AreEqual(JobState.Running, job.GetState());
			// imitate that AM is unregistered
			app.successfullyUnregistered.Set(true);
			app.WaitForState(job, JobState.Succeeded);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobRebootNotLastRetryOnUnregistrationFailure()
		{
			MRApp app = new MRApp(1, 0, false, this.GetType().FullName, true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 1, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task task = it.Next();
			app.WaitForState(task, TaskState.Running);
			//send an reboot event
			app.GetContext().GetEventHandler().Handle(new JobEvent(job.GetID(), JobEventType.
				JobAmReboot));
			// return exteranl state as RUNNING since otherwise the JobClient will
			// prematurely exit.
			app.WaitForState(job, JobState.Running);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobRebootOnLastRetryOnUnregistrationFailure()
		{
			// make startCount as 2 since this is last retry which equals to
			// DEFAULT_MAX_AM_RETRY
			// The last param mocks the unregistration failure
			MRApp app = new MRApp(1, 0, false, this.GetType().FullName, true, 2, false);
			Configuration conf = new Configuration();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 1, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task task = it.Next();
			app.WaitForState(task, TaskState.Running);
			//send an reboot event
			app.GetContext().GetEventHandler().Handle(new JobEvent(job.GetID(), JobEventType.
				JobAmReboot));
			app.WaitForInternalState((JobImpl)job, JobStateInternal.Reboot);
			// return exteranl state as RUNNING if this is the last retry while
			// unregistration fails
			app.WaitForState(job, JobState.Running);
		}

		private sealed class MRAppWithSpiedJob : MRApp
		{
			private JobImpl spiedJob;

			private MRAppWithSpiedJob(TestMRApp _enclosing, int maps, int reduces, bool autoComplete
				, string testName, bool cleanOnStart)
				: base(maps, reduces, autoComplete, testName, cleanOnStart)
			{
				this._enclosing = _enclosing;
			}

			protected internal override Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job CreateJob(
				Configuration conf, JobStateInternal forcedState, string diagnostic)
			{
				this.spiedJob = Org.Mockito.Mockito.Spy((JobImpl)base.CreateJob(conf, forcedState
					, diagnostic));
				((AppContext)this.GetContext()).GetAllJobs()[this.spiedJob.GetID()] = this.spiedJob;
				return this.spiedJob;
			}

			private readonly TestMRApp _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCountersOnJobFinish()
		{
			TestMRApp.MRAppWithSpiedJob app = new TestMRApp.MRAppWithSpiedJob(this, 1, 1, true
				, this.GetType().FullName, true);
			JobImpl job = (JobImpl)app.Submit(new Configuration());
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			System.Console.Out.WriteLine(job.GetAllCounters());
			// Just call getCounters
			job.GetAllCounters();
			job.GetAllCounters();
			// Should be called only once
			Org.Mockito.Mockito.Verify(job, Org.Mockito.Mockito.Times(1)).ConstructFinalFullcounters
				();
		}

		[NUnit.Framework.Test]
		public virtual void CheckJobStateTypeConversion()
		{
			//verify that all states can be converted without 
			// throwing an exception
			foreach (JobState state in JobState.Values())
			{
				TypeConverter.FromYarn(state);
			}
		}

		[NUnit.Framework.Test]
		public virtual void CheckTaskStateTypeConversion()
		{
			//verify that all states can be converted without 
			// throwing an exception
			foreach (TaskState state in TaskState.Values())
			{
				TypeConverter.FromYarn(state);
			}
		}

		private Container containerObtainedByContainerLauncher;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerPassThrough()
		{
			MRApp app = new _MRApp_494(this, 0, 1, true, this.GetType().FullName, true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(new Configuration());
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			ICollection<Task> tasks = job.GetTasks().Values;
			ICollection<TaskAttempt> taskAttempts = tasks.GetEnumerator().Next().GetAttempts(
				).Values;
			TaskAttemptImpl taskAttempt = (TaskAttemptImpl)taskAttempts.GetEnumerator().Next(
				);
			// Container from RM should pass through to the launcher. Container object
			// should be the same.
			NUnit.Framework.Assert.IsTrue(taskAttempt.container == containerObtainedByContainerLauncher
				);
		}

		private sealed class _MRApp_494 : MRApp
		{
			public _MRApp_494(TestMRApp _enclosing, int baseArg1, int baseArg2, bool baseArg3
				, string baseArg4, bool baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
				this._enclosing = _enclosing;
			}

			protected internal override ContainerLauncher CreateContainerLauncher(AppContext 
				context)
			{
				return new _MockContainerLauncher_497(this);
			}

			private sealed class _MockContainerLauncher_497 : MRApp.MockContainerLauncher
			{
				public _MockContainerLauncher_497(_MRApp_494 _enclosing)
					: base(_enclosing)
				{
					this._enclosing = _enclosing;
				}

				public override void Handle(ContainerLauncherEvent @event)
				{
					if (@event is ContainerRemoteLaunchEvent)
					{
						this._enclosing._enclosing.containerObtainedByContainerLauncher = ((ContainerRemoteLaunchEvent
							)@event).GetAllocatedContainer();
					}
					base.Handle(@event);
				}

				private readonly _MRApp_494 _enclosing;
			}

			private readonly TestMRApp _enclosing;
		}

		private sealed class MRAppWithHistory : MRApp
		{
			public MRAppWithHistory(TestMRApp _enclosing, int maps, int reduces, bool autoComplete
				, string testName, bool cleanOnStart, int startCount)
				: base(maps, reduces, autoComplete, testName, cleanOnStart, startCount)
			{
				this._enclosing = _enclosing;
			}

			protected internal override EventHandler<JobHistoryEvent> CreateJobHistoryHandler
				(AppContext context)
			{
				JobHistoryEventHandler eventHandler = new JobHistoryEventHandler(context, this.GetStartCount
					());
				return eventHandler;
			}

			private readonly TestMRApp _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestMRApp t = new TestMRApp();
			t.TestMapReduce();
			t.TestZeroMapReduces();
			t.TestCommitPending();
			t.TestCompletedMapsForReduceSlowstart();
			t.TestJobError();
			t.TestCountersOnJobFinish();
		}
	}
}
