using System;
using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.App.Launcher;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	/// <summary>
	/// Tests the state machine with respect to Job/Task/TaskAttempt failure
	/// scenarios.
	/// </summary>
	public class TestFail
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailTask()
		{
			//First attempt is failed and second attempt is passed
			//The job succeeds.
			MRApp app = new TestFail.MockFirstFailingAttemptMRApp(1, 0);
			Configuration conf = new Configuration();
			// this test requires two task attempts, but uberization overrides max to 1
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Succeeded);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			NUnit.Framework.Assert.AreEqual("Num tasks is not correct", 1, tasks.Count);
			Task task = tasks.Values.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual("Task state not correct", TaskState.Succeeded, task
				.GetReport().GetTaskState());
			IDictionary<TaskAttemptId, TaskAttempt> attempts = tasks.Values.GetEnumerator().Next
				().GetAttempts();
			NUnit.Framework.Assert.AreEqual("Num attempts is not correct", 2, attempts.Count);
			//one attempt must be failed 
			//and another must have succeeded
			IEnumerator<TaskAttempt> it = attempts.Values.GetEnumerator();
			NUnit.Framework.Assert.AreEqual("Attempt state not correct", TaskAttemptState.Failed
				, it.Next().GetReport().GetTaskAttemptState());
			NUnit.Framework.Assert.AreEqual("Attempt state not correct", TaskAttemptState.Succeeded
				, it.Next().GetReport().GetTaskAttemptState());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMapFailureMaxPercent()
		{
			MRApp app = new TestFail.MockFirstFailingTaskMRApp(4, 0);
			Configuration conf = new Configuration();
			//reduce the no of attempts so test run faster
			conf.SetInt(MRJobConfig.MapMaxAttempts, 2);
			conf.SetInt(MRJobConfig.ReduceMaxAttempts, 1);
			conf.SetInt(MRJobConfig.MapFailuresMaxPercent, 20);
			conf.SetInt(MRJobConfig.MapMaxAttempts, 1);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Failed);
			//setting the failure percentage to 25% (1/4 is 25) will
			//make the Job successful
			app = new TestFail.MockFirstFailingTaskMRApp(4, 0);
			conf = new Configuration();
			//reduce the no of attempts so test run faster
			conf.SetInt(MRJobConfig.MapMaxAttempts, 2);
			conf.SetInt(MRJobConfig.ReduceMaxAttempts, 1);
			conf.SetInt(MRJobConfig.MapFailuresMaxPercent, 25);
			conf.SetInt(MRJobConfig.MapMaxAttempts, 1);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Succeeded);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReduceFailureMaxPercent()
		{
			MRApp app = new TestFail.MockFirstFailingTaskMRApp(2, 4);
			Configuration conf = new Configuration();
			//reduce the no of attempts so test run faster
			conf.SetInt(MRJobConfig.MapMaxAttempts, 1);
			conf.SetInt(MRJobConfig.ReduceMaxAttempts, 2);
			conf.SetInt(MRJobConfig.MapFailuresMaxPercent, 50);
			//no failure due to Map
			conf.SetInt(MRJobConfig.MapMaxAttempts, 1);
			conf.SetInt(MRJobConfig.ReduceFailuresMaxpercent, 20);
			conf.SetInt(MRJobConfig.ReduceMaxAttempts, 1);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Failed);
			//setting the failure percentage to 25% (1/4 is 25) will
			//make the Job successful
			app = new TestFail.MockFirstFailingTaskMRApp(2, 4);
			conf = new Configuration();
			//reduce the no of attempts so test run faster
			conf.SetInt(MRJobConfig.MapMaxAttempts, 1);
			conf.SetInt(MRJobConfig.ReduceMaxAttempts, 2);
			conf.SetInt(MRJobConfig.MapFailuresMaxPercent, 50);
			//no failure due to Map
			conf.SetInt(MRJobConfig.MapMaxAttempts, 1);
			conf.SetInt(MRJobConfig.ReduceFailuresMaxpercent, 25);
			conf.SetInt(MRJobConfig.ReduceMaxAttempts, 1);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Succeeded);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTimedOutTask()
		{
			//All Task attempts are timed out, leading to Job failure
			MRApp app = new TestFail.TimeOutTaskMRApp(1, 0);
			Configuration conf = new Configuration();
			int maxAttempts = 2;
			conf.SetInt(MRJobConfig.MapMaxAttempts, maxAttempts);
			// disable uberization (requires entire job to be reattempted, so max for
			// subtask attempts is overridden to 1)
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Failed);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			NUnit.Framework.Assert.AreEqual("Num tasks is not correct", 1, tasks.Count);
			Task task = tasks.Values.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual("Task state not correct", TaskState.Failed, task.
				GetReport().GetTaskState());
			IDictionary<TaskAttemptId, TaskAttempt> attempts = tasks.Values.GetEnumerator().Next
				().GetAttempts();
			NUnit.Framework.Assert.AreEqual("Num attempts is not correct", maxAttempts, attempts
				.Count);
			foreach (TaskAttempt attempt in attempts.Values)
			{
				NUnit.Framework.Assert.AreEqual("Attempt state not correct", TaskAttemptState.Failed
					, attempt.GetReport().GetTaskAttemptState());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskFailWithUnusedContainer()
		{
			MRApp app = new TestFail.MRAppWithFailingTaskAndUnusedContainer();
			Configuration conf = new Configuration();
			int maxAttempts = 1;
			conf.SetInt(MRJobConfig.MapMaxAttempts, maxAttempts);
			// disable uberization (requires entire job to be reattempted, so max for
			// subtask attempts is overridden to 1)
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			NUnit.Framework.Assert.AreEqual("Num tasks is not correct", 1, tasks.Count);
			Task task = tasks.Values.GetEnumerator().Next();
			app.WaitForState(task, TaskState.Scheduled);
			IDictionary<TaskAttemptId, TaskAttempt> attempts = tasks.Values.GetEnumerator().Next
				().GetAttempts();
			NUnit.Framework.Assert.AreEqual("Num attempts is not correct", maxAttempts, attempts
				.Count);
			TaskAttempt attempt = attempts.Values.GetEnumerator().Next();
			app.WaitForInternalState((TaskAttemptImpl)attempt, TaskAttemptStateInternal.Assigned
				);
			app.GetDispatcher().GetEventHandler().Handle(new TaskAttemptEvent(attempt.GetID()
				, TaskAttemptEventType.TaContainerCompleted));
			app.WaitForState(job, JobState.Failed);
		}

		internal class MRAppWithFailingTaskAndUnusedContainer : MRApp
		{
			public MRAppWithFailingTaskAndUnusedContainer()
				: base(1, 0, false, "TaskFailWithUnsedContainer", true)
			{
			}

			protected internal override ContainerLauncher CreateContainerLauncher(AppContext 
				context)
			{
				return new _ContainerLauncherImpl_212(this, context);
			}

			private sealed class _ContainerLauncherImpl_212 : ContainerLauncherImpl
			{
				public _ContainerLauncherImpl_212(MRAppWithFailingTaskAndUnusedContainer _enclosing
					, AppContext baseArg1)
					: base(baseArg1)
				{
					this._enclosing = _enclosing;
				}

				public override void Handle(ContainerLauncherEvent @event)
				{
					switch (@event.GetType())
					{
						case ContainerLauncher.EventType.ContainerRemoteLaunch:
						{
							base.Handle(@event);
							// Unused event and container.
							break;
						}

						case ContainerLauncher.EventType.ContainerRemoteCleanup:
						{
							this._enclosing.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(@event
								.GetTaskAttemptID(), TaskAttemptEventType.TaContainerCleaned));
							break;
						}
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
					 GetCMProxy(string containerMgrBindAddr, ContainerId containerId)
				{
					try
					{
						lock (this)
						{
							Sharpen.Runtime.Wait(this);
						}
					}
					catch (Exception e)
					{
						// Just hang the thread simulating a very slow NM.
						Sharpen.Runtime.PrintStackTrace(e);
					}
					return null;
				}

				private readonly MRAppWithFailingTaskAndUnusedContainer _enclosing;
			}
		}

		internal class TimeOutTaskMRApp : MRApp
		{
			internal TimeOutTaskMRApp(int maps, int reduces)
				: base(maps, reduces, false, "TimeOutTaskMRApp", true)
			{
			}

			protected internal override TaskAttemptListener CreateTaskAttemptListener(AppContext
				 context)
			{
				//This will create the TaskAttemptListener with TaskHeartbeatHandler
				//RPC servers are not started
				//task time out is reduced
				//when attempt times out, heartbeat handler will send the lost event
				//leading to Attempt failure
				return new _TaskAttemptListenerImpl_256(GetContext(), null, null, null);
			}

			private sealed class _TaskAttemptListenerImpl_256 : TaskAttemptListenerImpl
			{
				public _TaskAttemptListenerImpl_256(AppContext baseArg1, JobTokenSecretManager baseArg2
					, RMHeartbeatHandler baseArg3, byte[] baseArg4)
					: base(baseArg1, baseArg2, baseArg3, baseArg4)
				{
				}

				protected internal override void StartRpcServer()
				{
				}

				protected internal override void StopRpcServer()
				{
				}

				public override IPEndPoint GetAddress()
				{
					return NetUtils.CreateSocketAddr("localhost", 1234);
				}

				/// <exception cref="System.Exception"/>
				protected override void ServiceInit(Configuration conf)
				{
					conf.SetInt(MRJobConfig.TaskTimeout, 1 * 1000);
					//reduce timeout
					conf.SetInt(MRJobConfig.TaskTimeoutCheckIntervalMs, 1 * 1000);
					base.ServiceInit(conf);
				}
			}
		}

		internal class MockFirstFailingTaskMRApp : MRApp
		{
			internal MockFirstFailingTaskMRApp(int maps, int reduces)
				: base(maps, reduces, true, "MockFirstFailingTaskMRApp", true)
			{
			}

			//Attempts of first Task are failed
			protected internal override void AttemptLaunched(TaskAttemptId attemptID)
			{
				if (attemptID.GetTaskId().GetId() == 0)
				{
					//check if it is first task
					// send the Fail event
					GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
						.TaFailmsg));
				}
				else
				{
					GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
						.TaDone));
				}
			}
		}

		internal class MockFirstFailingAttemptMRApp : MRApp
		{
			internal MockFirstFailingAttemptMRApp(int maps, int reduces)
				: base(maps, reduces, true, "MockFirstFailingAttemptMRApp", true)
			{
			}

			//First attempt is failed
			protected internal override void AttemptLaunched(TaskAttemptId attemptID)
			{
				if (attemptID.GetTaskId().GetId() == 0 && attemptID.GetId() == 0)
				{
					//check if it is first task's first attempt
					// send the Fail event
					GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
						.TaFailmsg));
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
			TestFail t = new TestFail();
			t.TestFailTask();
			t.TestTimedOutTask();
			t.TestMapFailureMaxPercent();
			t.TestReduceFailureMaxPercent();
			t.TestTaskFailWithUnusedContainer();
		}
	}
}
