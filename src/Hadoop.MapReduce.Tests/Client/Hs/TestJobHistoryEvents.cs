using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestJobHistoryEvents
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestJobHistoryEvents));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHistoryEvents()
		{
			Configuration conf = new Configuration();
			MRApp app = new TestJobHistoryEvents.MRAppWithHistory(2, 1, true, this.GetType().
				FullName, true);
			app.Submit(conf);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.GetContext().GetAllJobs().Values
				.GetEnumerator().Next();
			JobId jobId = job.GetID();
			Log.Info("JOBID is " + TypeConverter.FromYarn(jobId).ToString());
			app.WaitForState(job, JobState.Succeeded);
			//make sure all events are flushed 
			app.WaitForState(Service.STATE.Stopped);
			/*
			* Use HistoryContext to read logged events and verify the number of
			* completed maps
			*/
			HistoryContext context = new JobHistory();
			// test start and stop states
			((JobHistory)context).Init(conf);
			((JobHistory)context).Start();
			NUnit.Framework.Assert.IsTrue(context.GetStartTime() > 0);
			NUnit.Framework.Assert.AreEqual(((JobHistory)context).GetServiceState(), Service.STATE
				.Started);
			// get job before stopping JobHistory
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job parsedJob = context.GetJob(jobId);
			// stop JobHistory
			((JobHistory)context).Stop();
			NUnit.Framework.Assert.AreEqual(((JobHistory)context).GetServiceState(), Service.STATE
				.Stopped);
			NUnit.Framework.Assert.AreEqual("CompletedMaps not correct", 2, parsedJob.GetCompletedMaps
				());
			NUnit.Framework.Assert.AreEqual(Runtime.GetProperty("user.name"), parsedJob.GetUserName
				());
			IDictionary<TaskId, Task> tasks = parsedJob.GetTasks();
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, tasks.Count);
			foreach (Task task in tasks.Values)
			{
				VerifyTask(task);
			}
			IDictionary<TaskId, Task> maps = parsedJob.GetTasks(TaskType.Map);
			NUnit.Framework.Assert.AreEqual("No of maps not correct", 2, maps.Count);
			IDictionary<TaskId, Task> reduces = parsedJob.GetTasks(TaskType.Reduce);
			NUnit.Framework.Assert.AreEqual("No of reduces not correct", 1, reduces.Count);
			NUnit.Framework.Assert.AreEqual("CompletedReduce not correct", 1, parsedJob.GetCompletedReduces
				());
			NUnit.Framework.Assert.AreEqual("Job state not currect", JobState.Succeeded, parsedJob
				.GetState());
		}

		/// <summary>Verify that all the events are flushed on stopping the HistoryHandler</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEventsFlushOnStop()
		{
			Configuration conf = new Configuration();
			MRApp app = new TestJobHistoryEvents.MRAppWithSpecialHistoryHandler(1, 0, true, this
				.GetType().FullName, true);
			app.Submit(conf);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.GetContext().GetAllJobs().Values
				.GetEnumerator().Next();
			JobId jobId = job.GetID();
			Log.Info("JOBID is " + TypeConverter.FromYarn(jobId).ToString());
			app.WaitForState(job, JobState.Succeeded);
			// make sure all events are flushed
			app.WaitForState(Service.STATE.Stopped);
			/*
			* Use HistoryContext to read logged events and verify the number of
			* completed maps
			*/
			HistoryContext context = new JobHistory();
			((JobHistory)context).Init(conf);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job parsedJob = context.GetJob(jobId);
			NUnit.Framework.Assert.AreEqual("CompletedMaps not correct", 1, parsedJob.GetCompletedMaps
				());
			IDictionary<TaskId, Task> tasks = parsedJob.GetTasks();
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 1, tasks.Count);
			VerifyTask(tasks.Values.GetEnumerator().Next());
			IDictionary<TaskId, Task> maps = parsedJob.GetTasks(TaskType.Map);
			NUnit.Framework.Assert.AreEqual("No of maps not correct", 1, maps.Count);
			NUnit.Framework.Assert.AreEqual("Job state not currect", JobState.Succeeded, parsedJob
				.GetState());
		}

		[NUnit.Framework.Test]
		public virtual void TestJobHistoryEventHandlerIsFirstServiceToStop()
		{
			MRApp app = new TestJobHistoryEvents.MRAppWithSpecialHistoryHandler(1, 0, true, this
				.GetType().FullName, true);
			Configuration conf = new Configuration();
			app.Init(conf);
			Org.Apache.Hadoop.Service.Service[] services = Sharpen.Collections.ToArray(app.GetServices
				(), new Org.Apache.Hadoop.Service.Service[0]);
			// Verifying that it is the last to be added is same as verifying that it is
			// the first to be stopped. CompositeService related tests already validate
			// this.
			NUnit.Framework.Assert.AreEqual("JobHistoryEventHandler", services[services.Length
				 - 1].GetName());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAssignedQueue()
		{
			Configuration conf = new Configuration();
			MRApp app = new TestJobHistoryEvents.MRAppWithHistory(2, 1, true, this.GetType().
				FullName, true, "assignedQueue");
			app.Submit(conf);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.GetContext().GetAllJobs().Values
				.GetEnumerator().Next();
			JobId jobId = job.GetID();
			Log.Info("JOBID is " + TypeConverter.FromYarn(jobId).ToString());
			app.WaitForState(job, JobState.Succeeded);
			//make sure all events are flushed 
			app.WaitForState(Service.STATE.Stopped);
			/*
			* Use HistoryContext to read logged events and verify the number of
			* completed maps
			*/
			HistoryContext context = new JobHistory();
			// test start and stop states
			((JobHistory)context).Init(conf);
			((JobHistory)context).Start();
			NUnit.Framework.Assert.IsTrue(context.GetStartTime() > 0);
			NUnit.Framework.Assert.AreEqual(((JobHistory)context).GetServiceState(), Service.STATE
				.Started);
			// get job before stopping JobHistory
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job parsedJob = context.GetJob(jobId);
			// stop JobHistory
			((JobHistory)context).Stop();
			NUnit.Framework.Assert.AreEqual(((JobHistory)context).GetServiceState(), Service.STATE
				.Stopped);
			NUnit.Framework.Assert.AreEqual("QueueName not correct", "assignedQueue", parsedJob
				.GetQueueName());
		}

		private void VerifyTask(Task task)
		{
			NUnit.Framework.Assert.AreEqual("Task state not currect", TaskState.Succeeded, task
				.GetState());
			IDictionary<TaskAttemptId, TaskAttempt> attempts = task.GetAttempts();
			NUnit.Framework.Assert.AreEqual("No of attempts not correct", 1, attempts.Count);
			foreach (TaskAttempt attempt in attempts.Values)
			{
				VerifyAttempt(attempt);
			}
		}

		private void VerifyAttempt(TaskAttempt attempt)
		{
			NUnit.Framework.Assert.AreEqual("TaskAttempt state not currect", TaskAttemptState
				.Succeeded, attempt.GetState());
			NUnit.Framework.Assert.IsNotNull(attempt.GetAssignedContainerID());
			//Verify the wrong ctor is not being used. Remove after mrv1 is removed.
			ContainerId fakeCid = MRApp.NewContainerId(-1, -1, -1, -1);
			NUnit.Framework.Assert.IsFalse(attempt.GetAssignedContainerID().Equals(fakeCid));
			//Verify complete contianerManagerAddress
			NUnit.Framework.Assert.AreEqual(MRApp.NmHost + ":" + MRApp.NmPort, attempt.GetAssignedContainerMgrAddress
				());
		}

		internal class MRAppWithHistory : MRApp
		{
			public MRAppWithHistory(int maps, int reduces, bool autoComplete, string testName
				, bool cleanOnStart)
				: base(maps, reduces, autoComplete, testName, cleanOnStart)
			{
			}

			public MRAppWithHistory(int maps, int reduces, bool autoComplete, string testName
				, bool cleanOnStart, string assignedQueue)
				: base(maps, reduces, autoComplete, testName, cleanOnStart, assignedQueue)
			{
			}

			protected override EventHandler<JobHistoryEvent> CreateJobHistoryHandler(AppContext
				 context)
			{
				return new JobHistoryEventHandler(context, GetStartCount());
			}
		}

		/// <summary>MRapp with special HistoryEventHandler that writes events only during stop.
		/// 	</summary>
		/// <remarks>
		/// MRapp with special HistoryEventHandler that writes events only during stop.
		/// This is to simulate events that don't get written by the eventHandling
		/// thread due to say a slow DFS and verify that they are flushed during stop.
		/// </remarks>
		private class MRAppWithSpecialHistoryHandler : MRApp
		{
			public MRAppWithSpecialHistoryHandler(int maps, int reduces, bool autoComplete, string
				 testName, bool cleanOnStart)
				: base(maps, reduces, autoComplete, testName, cleanOnStart)
			{
			}

			protected override EventHandler<JobHistoryEvent> CreateJobHistoryHandler(AppContext
				 context)
			{
				return new _JobHistoryEventHandler_250(context, GetStartCount());
			}

			private sealed class _JobHistoryEventHandler_250 : JobHistoryEventHandler
			{
				public _JobHistoryEventHandler_250(AppContext baseArg1, int baseArg2)
					: base(baseArg1, baseArg2)
				{
				}

				protected override void ServiceStart()
				{
					// Don't start any event draining thread.
					base.eventHandlingThread = new Sharpen.Thread();
					base.eventHandlingThread.Start();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestJobHistoryEvents t = new TestJobHistoryEvents();
			t.TestHistoryEvents();
			t.TestEventsFlushOnStop();
			t.TestJobHistoryEventHandlerIsFirstServiceToStop();
		}
	}
}
