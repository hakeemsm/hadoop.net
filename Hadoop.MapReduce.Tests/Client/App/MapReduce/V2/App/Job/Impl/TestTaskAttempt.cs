using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl
{
	public class TestTaskAttempt
	{
		public class StubbedFS : RawLocalFileSystem
		{
			/// <exception cref="System.IO.IOException"/>
			public override FileStatus GetFileStatus(Path f)
			{
				return new FileStatus(1, false, 1, 1, 1, f);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAppHistoryForMap()
		{
			MRApp app = new TestTaskAttempt.FailingAttemptsMRApp(1, 0);
			TestMRAppHistory(app);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAppHistoryForReduce()
		{
			MRApp app = new TestTaskAttempt.FailingAttemptsMRApp(0, 1);
			TestMRAppHistory(app);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAppHistoryForTAFailedInAssigned()
		{
			// test TA_CONTAINER_LAUNCH_FAILED for map
			TestTaskAttempt.FailingAttemptsDuringAssignedMRApp app = new TestTaskAttempt.FailingAttemptsDuringAssignedMRApp
				(1, 0, TaskAttemptEventType.TaContainerLaunchFailed);
			TestTaskAttemptAssignedFailHistory(app);
			// test TA_CONTAINER_LAUNCH_FAILED for reduce
			app = new TestTaskAttempt.FailingAttemptsDuringAssignedMRApp(0, 1, TaskAttemptEventType
				.TaContainerLaunchFailed);
			TestTaskAttemptAssignedFailHistory(app);
			// test TA_CONTAINER_COMPLETED for map
			app = new TestTaskAttempt.FailingAttemptsDuringAssignedMRApp(1, 0, TaskAttemptEventType
				.TaContainerCompleted);
			TestTaskAttemptAssignedFailHistory(app);
			// test TA_CONTAINER_COMPLETED for reduce
			app = new TestTaskAttempt.FailingAttemptsDuringAssignedMRApp(0, 1, TaskAttemptEventType
				.TaContainerCompleted);
			TestTaskAttemptAssignedFailHistory(app);
			// test TA_FAILMSG for map
			app = new TestTaskAttempt.FailingAttemptsDuringAssignedMRApp(1, 0, TaskAttemptEventType
				.TaFailmsg);
			TestTaskAttemptAssignedFailHistory(app);
			// test TA_FAILMSG for reduce
			app = new TestTaskAttempt.FailingAttemptsDuringAssignedMRApp(0, 1, TaskAttemptEventType
				.TaFailmsg);
			TestTaskAttemptAssignedFailHistory(app);
			// test TA_KILL for map
			app = new TestTaskAttempt.FailingAttemptsDuringAssignedMRApp(1, 0, TaskAttemptEventType
				.TaKill);
			TestTaskAttemptAssignedKilledHistory(app);
			// test TA_KILL for reduce
			app = new TestTaskAttempt.FailingAttemptsDuringAssignedMRApp(0, 1, TaskAttemptEventType
				.TaKill);
			TestTaskAttemptAssignedKilledHistory(app);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleRackRequest()
		{
			TaskAttemptImpl.RequestContainerTransition rct = new TaskAttemptImpl.RequestContainerTransition
				(false);
			EventHandler eventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			string[] hosts = new string[3];
			hosts[0] = "host1";
			hosts[1] = "host2";
			hosts[2] = "host3";
			JobSplit.TaskSplitMetaInfo splitInfo = new JobSplit.TaskSplitMetaInfo(hosts, 0, 128
				 * 1024 * 1024l);
			TaskAttemptImpl mockTaskAttempt = CreateMapTaskAttemptImplForTest(eventHandler, splitInfo
				);
			TaskAttemptEvent mockTAEvent = Org.Mockito.Mockito.Mock<TaskAttemptEvent>();
			rct.Transition(mockTaskAttempt, mockTAEvent);
			ArgumentCaptor<Org.Apache.Hadoop.Yarn.Event.Event> arg = ArgumentCaptor.ForClass<
				Org.Apache.Hadoop.Yarn.Event.Event>();
			Org.Mockito.Mockito.Verify(eventHandler, Org.Mockito.Mockito.Times(2)).Handle(arg
				.Capture());
			if (!(arg.GetAllValues()[1] is ContainerRequestEvent))
			{
				NUnit.Framework.Assert.Fail("Second Event not of type ContainerRequestEvent");
			}
			ContainerRequestEvent cre = (ContainerRequestEvent)arg.GetAllValues()[1];
			string[] requestedRacks = cre.GetRacks();
			//Only a single occurrence of /DefaultRack
			NUnit.Framework.Assert.AreEqual(1, requestedRacks.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHostResolveAttempt()
		{
			TaskAttemptImpl.RequestContainerTransition rct = new TaskAttemptImpl.RequestContainerTransition
				(false);
			EventHandler eventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			string[] hosts = new string[3];
			hosts[0] = "192.168.1.1";
			hosts[1] = "host2";
			hosts[2] = "host3";
			JobSplit.TaskSplitMetaInfo splitInfo = new JobSplit.TaskSplitMetaInfo(hosts, 0, 128
				 * 1024 * 1024l);
			TaskAttemptImpl mockTaskAttempt = CreateMapTaskAttemptImplForTest(eventHandler, splitInfo
				);
			TaskAttemptImpl spyTa = Org.Mockito.Mockito.Spy(mockTaskAttempt);
			Org.Mockito.Mockito.When(spyTa.ResolveHost(hosts[0])).ThenReturn("host1");
			spyTa.dataLocalHosts = spyTa.ResolveHosts(splitInfo.GetLocations());
			TaskAttemptEvent mockTAEvent = Org.Mockito.Mockito.Mock<TaskAttemptEvent>();
			rct.Transition(spyTa, mockTAEvent);
			Org.Mockito.Mockito.Verify(spyTa).ResolveHost(hosts[0]);
			ArgumentCaptor<Org.Apache.Hadoop.Yarn.Event.Event> arg = ArgumentCaptor.ForClass<
				Org.Apache.Hadoop.Yarn.Event.Event>();
			Org.Mockito.Mockito.Verify(eventHandler, Org.Mockito.Mockito.Times(2)).Handle(arg
				.Capture());
			if (!(arg.GetAllValues()[1] is ContainerRequestEvent))
			{
				NUnit.Framework.Assert.Fail("Second Event not of type ContainerRequestEvent");
			}
			IDictionary<string, bool> expected = new Dictionary<string, bool>();
			expected["host1"] = true;
			expected["host2"] = true;
			expected["host3"] = true;
			ContainerRequestEvent cre = (ContainerRequestEvent)arg.GetAllValues()[1];
			string[] requestedHosts = cre.GetHosts();
			foreach (string h in requestedHosts)
			{
				Sharpen.Collections.Remove(expected, h);
			}
			NUnit.Framework.Assert.AreEqual(0, expected.Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMillisCountersUpdate()
		{
			VerifyMillisCounters(2048, 2048, 1024);
			VerifyMillisCounters(2048, 1024, 1024);
			VerifyMillisCounters(10240, 1024, 2048);
		}

		/// <exception cref="System.Exception"/>
		public virtual void VerifyMillisCounters(int mapMemMb, int reduceMemMb, int minContainerSize
			)
		{
			Clock actualClock = new SystemClock();
			ControlledClock clock = new ControlledClock(actualClock);
			clock.SetTime(10);
			MRApp app = new MRApp(1, 1, false, "testSlotMillisCounterUpdate", true, clock);
			Configuration conf = new Configuration();
			conf.SetInt(MRJobConfig.MapMemoryMb, mapMemMb);
			conf.SetInt(MRJobConfig.ReduceMemoryMb, reduceMemMb);
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, minContainerSize);
			app.SetClusterInfo(new ClusterInfo(Resource.NewInstance(10240, 1)));
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			NUnit.Framework.Assert.AreEqual("Num tasks is not correct", 2, tasks.Count);
			IEnumerator<Task> taskIter = tasks.Values.GetEnumerator();
			Task mTask = taskIter.Next();
			app.WaitForState(mTask, TaskState.Running);
			Task rTask = taskIter.Next();
			app.WaitForState(rTask, TaskState.Running);
			IDictionary<TaskAttemptId, TaskAttempt> mAttempts = mTask.GetAttempts();
			NUnit.Framework.Assert.AreEqual("Num attempts is not correct", 1, mAttempts.Count
				);
			IDictionary<TaskAttemptId, TaskAttempt> rAttempts = rTask.GetAttempts();
			NUnit.Framework.Assert.AreEqual("Num attempts is not correct", 1, rAttempts.Count
				);
			TaskAttempt mta = mAttempts.Values.GetEnumerator().Next();
			TaskAttempt rta = rAttempts.Values.GetEnumerator().Next();
			app.WaitForState(mta, TaskAttemptState.Running);
			app.WaitForState(rta, TaskAttemptState.Running);
			clock.SetTime(11);
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mta.GetID(), TaskAttemptEventType
				.TaDone));
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(rta.GetID(), TaskAttemptEventType
				.TaDone));
			app.WaitForState(job, JobState.Succeeded);
			NUnit.Framework.Assert.AreEqual(mta.GetFinishTime(), 11);
			NUnit.Framework.Assert.AreEqual(mta.GetLaunchTime(), 10);
			NUnit.Framework.Assert.AreEqual(rta.GetFinishTime(), 11);
			NUnit.Framework.Assert.AreEqual(rta.GetLaunchTime(), 10);
			Counters counters = job.GetAllCounters();
			NUnit.Framework.Assert.AreEqual((int)Math.Ceil((float)mapMemMb / minContainerSize
				), counters.FindCounter(JobCounter.SlotsMillisMaps).GetValue());
			NUnit.Framework.Assert.AreEqual((int)Math.Ceil((float)reduceMemMb / minContainerSize
				), counters.FindCounter(JobCounter.SlotsMillisReduces).GetValue());
			NUnit.Framework.Assert.AreEqual(1, counters.FindCounter(JobCounter.MillisMaps).GetValue
				());
			NUnit.Framework.Assert.AreEqual(1, counters.FindCounter(JobCounter.MillisReduces)
				.GetValue());
			NUnit.Framework.Assert.AreEqual(mapMemMb, counters.FindCounter(JobCounter.MbMillisMaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(reduceMemMb, counters.FindCounter(JobCounter.MbMillisReduces
				).GetValue());
			NUnit.Framework.Assert.AreEqual(1, counters.FindCounter(JobCounter.VcoresMillisMaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(1, counters.FindCounter(JobCounter.VcoresMillisReduces
				).GetValue());
		}

		private TaskAttemptImpl CreateMapTaskAttemptImplForTest(EventHandler eventHandler
			, JobSplit.TaskSplitMetaInfo taskSplitMetaInfo)
		{
			Clock clock = new SystemClock();
			return CreateMapTaskAttemptImplForTest(eventHandler, taskSplitMetaInfo, clock);
		}

		private TaskAttemptImpl CreateMapTaskAttemptImplForTest(EventHandler eventHandler
			, JobSplit.TaskSplitMetaInfo taskSplitMetaInfo, Clock clock)
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			JobConf jobConf = new JobConf();
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, taskSplitMetaInfo, jobConf, taListener, null, null, clock, null);
			return taImpl;
		}

		/// <exception cref="System.Exception"/>
		private void TestMRAppHistory(MRApp app)
		{
			Configuration conf = new Configuration();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Failed);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			NUnit.Framework.Assert.AreEqual("Num tasks is not correct", 1, tasks.Count);
			Task task = tasks.Values.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual("Task state not correct", TaskState.Failed, task.
				GetReport().GetTaskState());
			IDictionary<TaskAttemptId, TaskAttempt> attempts = tasks.Values.GetEnumerator().Next
				().GetAttempts();
			NUnit.Framework.Assert.AreEqual("Num attempts is not correct", 4, attempts.Count);
			IEnumerator<TaskAttempt> it = attempts.Values.GetEnumerator();
			TaskAttemptReport report = it.Next().GetReport();
			NUnit.Framework.Assert.AreEqual("Attempt state not correct", TaskAttemptState.Failed
				, report.GetTaskAttemptState());
			NUnit.Framework.Assert.AreEqual("Diagnostic Information is not Correct", "Test Diagnostic Event"
				, report.GetDiagnosticInfo());
			report = it.Next().GetReport();
			NUnit.Framework.Assert.AreEqual("Attempt state not correct", TaskAttemptState.Failed
				, report.GetTaskAttemptState());
		}

		/// <exception cref="System.Exception"/>
		private void TestTaskAttemptAssignedFailHistory(TestTaskAttempt.FailingAttemptsDuringAssignedMRApp
			 app)
		{
			Configuration conf = new Configuration();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Failed);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			NUnit.Framework.Assert.IsTrue("No Ta Started JH Event", app.GetTaStartJHEvent());
			NUnit.Framework.Assert.IsTrue("No Ta Failed JH Event", app.GetTaFailedJHEvent());
		}

		/// <exception cref="System.Exception"/>
		private void TestTaskAttemptAssignedKilledHistory(TestTaskAttempt.FailingAttemptsDuringAssignedMRApp
			 app)
		{
			Configuration conf = new Configuration();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			Task task = tasks.Values.GetEnumerator().Next();
			app.WaitForState(task, TaskState.Scheduled);
			IDictionary<TaskAttemptId, TaskAttempt> attempts = task.GetAttempts();
			TaskAttempt attempt = attempts.Values.GetEnumerator().Next();
			app.WaitForState(attempt, TaskAttemptState.Killed);
			NUnit.Framework.Assert.IsTrue("No Ta Started JH Event", app.GetTaStartJHEvent());
			NUnit.Framework.Assert.IsTrue("No Ta Killed JH Event", app.GetTaKilledJHEvent());
		}

		internal class FailingAttemptsMRApp : MRApp
		{
			internal FailingAttemptsMRApp(int maps, int reduces)
				: base(maps, reduces, true, "FailingAttemptsMRApp", true)
			{
			}

			protected internal override void AttemptLaunched(TaskAttemptId attemptID)
			{
				GetContext().GetEventHandler().Handle(new TaskAttemptDiagnosticsUpdateEvent(attemptID
					, "Test Diagnostic Event"));
				GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
					.TaFailmsg));
			}

			protected internal override EventHandler<JobHistoryEvent> CreateJobHistoryHandler
				(AppContext context)
			{
				return new _EventHandler_394();
			}

			private sealed class _EventHandler_394 : EventHandler<JobHistoryEvent>
			{
				public _EventHandler_394()
				{
				}

				public void Handle(JobHistoryEvent @event)
				{
					if (@event.GetType() == EventType.MapAttemptFailed)
					{
						TaskAttemptUnsuccessfulCompletion datum = (TaskAttemptUnsuccessfulCompletion)@event
							.GetHistoryEvent().GetDatum();
						NUnit.Framework.Assert.AreEqual("Diagnostic Information is not Correct", "Test Diagnostic Event"
							, datum.Get(8).ToString());
					}
				}
			}
		}

		internal class FailingAttemptsDuringAssignedMRApp : MRApp
		{
			internal FailingAttemptsDuringAssignedMRApp(int maps, int reduces, TaskAttemptEventType
				 @event)
				: base(maps, reduces, true, "FailingAttemptsMRApp", true)
			{
				sendFailEvent = @event;
			}

			internal TaskAttemptEventType sendFailEvent;

			protected internal override void ContainerLaunched(TaskAttemptId attemptID, int shufflePort
				)
			{
			}

			//do nothing, not send TA_CONTAINER_LAUNCHED event
			protected internal override void AttemptLaunched(TaskAttemptId attemptID)
			{
				GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, sendFailEvent
					));
			}

			private bool receiveTaStartJHEvent = false;

			private bool receiveTaFailedJHEvent = false;

			private bool receiveTaKilledJHEvent = false;

			public virtual bool GetTaStartJHEvent()
			{
				return receiveTaStartJHEvent;
			}

			public virtual bool GetTaFailedJHEvent()
			{
				return receiveTaFailedJHEvent;
			}

			public virtual bool GetTaKilledJHEvent()
			{
				return receiveTaKilledJHEvent;
			}

			protected internal override EventHandler<JobHistoryEvent> CreateJobHistoryHandler
				(AppContext context)
			{
				return new _EventHandler_447(this);
			}

			private sealed class _EventHandler_447 : EventHandler<JobHistoryEvent>
			{
				public _EventHandler_447(FailingAttemptsDuringAssignedMRApp _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public void Handle(JobHistoryEvent @event)
				{
					if (@event.GetType() == EventType.MapAttemptFailed)
					{
						this._enclosing.receiveTaFailedJHEvent = true;
					}
					else
					{
						if (@event.GetType() == EventType.MapAttemptKilled)
						{
							this._enclosing.receiveTaKilledJHEvent = true;
						}
						else
						{
							if (@event.GetType() == EventType.MapAttemptStarted)
							{
								this._enclosing.receiveTaStartJHEvent = true;
							}
							else
							{
								if (@event.GetType() == EventType.ReduceAttemptFailed)
								{
									this._enclosing.receiveTaFailedJHEvent = true;
								}
								else
								{
									if (@event.GetType() == EventType.ReduceAttemptKilled)
									{
										this._enclosing.receiveTaKilledJHEvent = true;
									}
									else
									{
										if (@event.GetType() == EventType.ReduceAttemptStarted)
										{
											this._enclosing.receiveTaStartJHEvent = true;
										}
									}
								}
							}
						}
					}
				}

				private readonly FailingAttemptsDuringAssignedMRApp _enclosing;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLaunchFailedWhileKilling()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			TestTaskAttempt.MockEventHandler eventHandler = new TestTaskAttempt.MockEventHandler
				();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttempt.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, "10");
			JobSplit.TaskSplitMetaInfo splits = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			Org.Mockito.Mockito.When(splits.GetLocations()).ThenReturn(new string[] { "127.0.0.1"
				 });
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, splits, jobConf, taListener, new Org.Apache.Hadoop.Security.Token.Token(), new 
				Credentials(), new SystemClock(), null);
			NodeId nid = NodeId.NewInstance("127.0.0.1", 0);
			ContainerId contId = ContainerId.NewContainerId(appAttemptId, 3);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(contId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nid);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaSchedule));
			taImpl.Handle(new TaskAttemptContainerAssignedEvent(attemptId, container, Org.Mockito.Mockito.Mock
				<IDictionary>()));
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaKill));
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaContainerCleaned
				));
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaContainerLaunchFailed
				));
			NUnit.Framework.Assert.IsFalse(eventHandler.internalError);
			NUnit.Framework.Assert.AreEqual("Task attempt is not assigned on the local node", 
				Locality.NodeLocal, taImpl.GetLocality());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerCleanedWhileRunning()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			TestTaskAttempt.MockEventHandler eventHandler = new TestTaskAttempt.MockEventHandler
				();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttempt.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, "10");
			JobSplit.TaskSplitMetaInfo splits = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			Org.Mockito.Mockito.When(splits.GetLocations()).ThenReturn(new string[] { "127.0.0.1"
				 });
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			ClusterInfo clusterInfo = Org.Mockito.Mockito.Mock<ClusterInfo>();
			Resource resource = Org.Mockito.Mockito.Mock<Resource>();
			Org.Mockito.Mockito.When(appCtx.GetClusterInfo()).ThenReturn(clusterInfo);
			Org.Mockito.Mockito.When(resource.GetMemory()).ThenReturn(1024);
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, splits, jobConf, taListener, new Org.Apache.Hadoop.Security.Token.Token(), new 
				Credentials(), new SystemClock(), appCtx);
			NodeId nid = NodeId.NewInstance("127.0.0.2", 0);
			ContainerId contId = ContainerId.NewContainerId(appAttemptId, 3);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(contId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nid);
			Org.Mockito.Mockito.When(container.GetNodeHttpAddress()).ThenReturn("localhost:0"
				);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaSchedule));
			taImpl.Handle(new TaskAttemptContainerAssignedEvent(attemptId, container, Org.Mockito.Mockito.Mock
				<IDictionary>()));
			taImpl.Handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in running state", taImpl.GetState
				(), TaskAttemptState.Running);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaContainerCleaned
				));
			NUnit.Framework.Assert.IsFalse("InternalError occurred trying to handle TA_CONTAINER_CLEANED"
				, eventHandler.internalError);
			NUnit.Framework.Assert.AreEqual("Task attempt is not assigned on the local rack", 
				Locality.RackLocal, taImpl.GetLocality());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerCleanedWhileCommitting()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			TestTaskAttempt.MockEventHandler eventHandler = new TestTaskAttempt.MockEventHandler
				();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttempt.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, "10");
			JobSplit.TaskSplitMetaInfo splits = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			Org.Mockito.Mockito.When(splits.GetLocations()).ThenReturn(new string[] {  });
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			ClusterInfo clusterInfo = Org.Mockito.Mockito.Mock<ClusterInfo>();
			Resource resource = Org.Mockito.Mockito.Mock<Resource>();
			Org.Mockito.Mockito.When(appCtx.GetClusterInfo()).ThenReturn(clusterInfo);
			Org.Mockito.Mockito.When(resource.GetMemory()).ThenReturn(1024);
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, splits, jobConf, taListener, new Org.Apache.Hadoop.Security.Token.Token(), new 
				Credentials(), new SystemClock(), appCtx);
			NodeId nid = NodeId.NewInstance("127.0.0.1", 0);
			ContainerId contId = ContainerId.NewContainerId(appAttemptId, 3);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(contId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nid);
			Org.Mockito.Mockito.When(container.GetNodeHttpAddress()).ThenReturn("localhost:0"
				);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaSchedule));
			taImpl.Handle(new TaskAttemptContainerAssignedEvent(attemptId, container, Org.Mockito.Mockito.Mock
				<IDictionary>()));
			taImpl.Handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaCommitPending
				));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in commit pending state", taImpl
				.GetState(), TaskAttemptState.CommitPending);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaContainerCleaned
				));
			NUnit.Framework.Assert.IsFalse("InternalError occurred trying to handle TA_CONTAINER_CLEANED"
				, eventHandler.internalError);
			NUnit.Framework.Assert.AreEqual("Task attempt is assigned locally", Locality.OffSwitch
				, taImpl.GetLocality());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoubleTooManyFetchFailure()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			TestTaskAttempt.MockEventHandler eventHandler = new TestTaskAttempt.MockEventHandler
				();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttempt.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, "10");
			JobSplit.TaskSplitMetaInfo splits = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			Org.Mockito.Mockito.When(splits.GetLocations()).ThenReturn(new string[] { "127.0.0.1"
				 });
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			ClusterInfo clusterInfo = Org.Mockito.Mockito.Mock<ClusterInfo>();
			Resource resource = Org.Mockito.Mockito.Mock<Resource>();
			Org.Mockito.Mockito.When(appCtx.GetClusterInfo()).ThenReturn(clusterInfo);
			Org.Mockito.Mockito.When(resource.GetMemory()).ThenReturn(1024);
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, splits, jobConf, taListener, new Org.Apache.Hadoop.Security.Token.Token(), new 
				Credentials(), new SystemClock(), appCtx);
			NodeId nid = NodeId.NewInstance("127.0.0.1", 0);
			ContainerId contId = ContainerId.NewContainerId(appAttemptId, 3);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(contId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nid);
			Org.Mockito.Mockito.When(container.GetNodeHttpAddress()).ThenReturn("localhost:0"
				);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaSchedule));
			taImpl.Handle(new TaskAttemptContainerAssignedEvent(attemptId, container, Org.Mockito.Mockito.Mock
				<IDictionary>()));
			taImpl.Handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaDone));
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaContainerCleaned
				));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in succeeded state", taImpl.
				GetState(), TaskAttemptState.Succeeded);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaTooManyFetchFailure
				));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in FAILED state", taImpl.GetState
				(), TaskAttemptState.Failed);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaTooManyFetchFailure
				));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in FAILED state, still", taImpl
				.GetState(), TaskAttemptState.Failed);
			NUnit.Framework.Assert.IsFalse("InternalError occurred trying to handle TA_CONTAINER_CLEANED"
				, eventHandler.internalError);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppDiognosticEventOnUnassignedTask()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			TestTaskAttempt.MockEventHandler eventHandler = new TestTaskAttempt.MockEventHandler
				();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttempt.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, "10");
			JobSplit.TaskSplitMetaInfo splits = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			Org.Mockito.Mockito.When(splits.GetLocations()).ThenReturn(new string[] { "127.0.0.1"
				 });
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			ClusterInfo clusterInfo = Org.Mockito.Mockito.Mock<ClusterInfo>();
			Resource resource = Org.Mockito.Mockito.Mock<Resource>();
			Org.Mockito.Mockito.When(appCtx.GetClusterInfo()).ThenReturn(clusterInfo);
			Org.Mockito.Mockito.When(resource.GetMemory()).ThenReturn(1024);
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, splits, jobConf, taListener, new Org.Apache.Hadoop.Security.Token.Token(), new 
				Credentials(), new SystemClock(), appCtx);
			NodeId nid = NodeId.NewInstance("127.0.0.1", 0);
			ContainerId contId = ContainerId.NewContainerId(appAttemptId, 3);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(contId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nid);
			Org.Mockito.Mockito.When(container.GetNodeHttpAddress()).ThenReturn("localhost:0"
				);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaSchedule));
			taImpl.Handle(new TaskAttemptDiagnosticsUpdateEvent(attemptId, "Task got killed")
				);
			NUnit.Framework.Assert.IsFalse("InternalError occurred trying to handle TA_DIAGNOSTICS_UPDATE on assigned task"
				, eventHandler.internalError);
			try
			{
				taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaKill));
				NUnit.Framework.Assert.IsTrue("No exception on UNASSIGNED STATE KILL event", true
					);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.IsFalse("Exception not expected for UNASSIGNED STATE KILL event"
					, true);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTooManyFetchFailureAfterKill()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			TestTaskAttempt.MockEventHandler eventHandler = new TestTaskAttempt.MockEventHandler
				();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttempt.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, "10");
			JobSplit.TaskSplitMetaInfo splits = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			Org.Mockito.Mockito.When(splits.GetLocations()).ThenReturn(new string[] { "127.0.0.1"
				 });
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			ClusterInfo clusterInfo = Org.Mockito.Mockito.Mock<ClusterInfo>();
			Resource resource = Org.Mockito.Mockito.Mock<Resource>();
			Org.Mockito.Mockito.When(appCtx.GetClusterInfo()).ThenReturn(clusterInfo);
			Org.Mockito.Mockito.When(resource.GetMemory()).ThenReturn(1024);
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, splits, jobConf, taListener, Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>(), new Credentials(), new SystemClock(), appCtx);
			NodeId nid = NodeId.NewInstance("127.0.0.1", 0);
			ContainerId contId = ContainerId.NewContainerId(appAttemptId, 3);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(contId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nid);
			Org.Mockito.Mockito.When(container.GetNodeHttpAddress()).ThenReturn("localhost:0"
				);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaSchedule));
			taImpl.Handle(new TaskAttemptContainerAssignedEvent(attemptId, container, Org.Mockito.Mockito.Mock
				<IDictionary>()));
			taImpl.Handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaDone));
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaContainerCleaned
				));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in succeeded state", taImpl.
				GetState(), TaskAttemptState.Succeeded);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaKill));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in KILLED state", taImpl.GetState
				(), TaskAttemptState.Killed);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaTooManyFetchFailure
				));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in KILLED state, still", taImpl
				.GetState(), TaskAttemptState.Killed);
			NUnit.Framework.Assert.IsFalse("InternalError occurred trying to handle TA_CONTAINER_CLEANED"
				, eventHandler.internalError);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppDiognosticEventOnNewTask()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			TestTaskAttempt.MockEventHandler eventHandler = new TestTaskAttempt.MockEventHandler
				();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttempt.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, "10");
			JobSplit.TaskSplitMetaInfo splits = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			Org.Mockito.Mockito.When(splits.GetLocations()).ThenReturn(new string[] { "127.0.0.1"
				 });
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			ClusterInfo clusterInfo = Org.Mockito.Mockito.Mock<ClusterInfo>();
			Resource resource = Org.Mockito.Mockito.Mock<Resource>();
			Org.Mockito.Mockito.When(appCtx.GetClusterInfo()).ThenReturn(clusterInfo);
			Org.Mockito.Mockito.When(resource.GetMemory()).ThenReturn(1024);
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, splits, jobConf, taListener, new Org.Apache.Hadoop.Security.Token.Token(), new 
				Credentials(), new SystemClock(), appCtx);
			NodeId nid = NodeId.NewInstance("127.0.0.1", 0);
			ContainerId contId = ContainerId.NewContainerId(appAttemptId, 3);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(contId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nid);
			Org.Mockito.Mockito.When(container.GetNodeHttpAddress()).ThenReturn("localhost:0"
				);
			taImpl.Handle(new TaskAttemptDiagnosticsUpdateEvent(attemptId, "Task got killed")
				);
			NUnit.Framework.Assert.IsFalse("InternalError occurred trying to handle TA_DIAGNOSTICS_UPDATE on assigned task"
				, eventHandler.internalError);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFetchFailureAttemptFinishTime()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			TestTaskAttempt.MockEventHandler eventHandler = new TestTaskAttempt.MockEventHandler
				();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttempt.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, "10");
			JobSplit.TaskSplitMetaInfo splits = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			Org.Mockito.Mockito.When(splits.GetLocations()).ThenReturn(new string[] { "127.0.0.1"
				 });
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			ClusterInfo clusterInfo = Org.Mockito.Mockito.Mock<ClusterInfo>();
			Org.Mockito.Mockito.When(appCtx.GetClusterInfo()).ThenReturn(clusterInfo);
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, splits, jobConf, taListener, Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>(), new Credentials(), new SystemClock(), appCtx);
			NodeId nid = NodeId.NewInstance("127.0.0.1", 0);
			ContainerId contId = ContainerId.NewContainerId(appAttemptId, 3);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(contId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nid);
			Org.Mockito.Mockito.When(container.GetNodeHttpAddress()).ThenReturn("localhost:0"
				);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaSchedule));
			taImpl.Handle(new TaskAttemptContainerAssignedEvent(attemptId, container, Org.Mockito.Mockito.Mock
				<IDictionary>()));
			taImpl.Handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaDone));
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaContainerCleaned
				));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in succeeded state", taImpl.
				GetState(), TaskAttemptState.Succeeded);
			NUnit.Framework.Assert.IsTrue("Task Attempt finish time is not greater than 0", taImpl
				.GetFinishTime() > 0);
			long finishTime = taImpl.GetFinishTime();
			Sharpen.Thread.Sleep(5);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaTooManyFetchFailure
				));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in Too Many Fetch Failure state"
				, taImpl.GetState(), TaskAttemptState.Failed);
			NUnit.Framework.Assert.AreEqual("After TA_TOO_MANY_FETCH_FAILURE," + " Task attempt finish time is not the same "
				, finishTime, Sharpen.Extensions.ValueOf(taImpl.GetFinishTime()));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerKillAfterAssigned()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			TestTaskAttempt.MockEventHandler eventHandler = new TestTaskAttempt.MockEventHandler
				();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttempt.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, "10");
			JobSplit.TaskSplitMetaInfo splits = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			Org.Mockito.Mockito.When(splits.GetLocations()).ThenReturn(new string[] { "127.0.0.1"
				 });
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			ClusterInfo clusterInfo = Org.Mockito.Mockito.Mock<ClusterInfo>();
			Resource resource = Org.Mockito.Mockito.Mock<Resource>();
			Org.Mockito.Mockito.When(appCtx.GetClusterInfo()).ThenReturn(clusterInfo);
			Org.Mockito.Mockito.When(resource.GetMemory()).ThenReturn(1024);
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, splits, jobConf, taListener, new Org.Apache.Hadoop.Security.Token.Token(), new 
				Credentials(), new SystemClock(), appCtx);
			NodeId nid = NodeId.NewInstance("127.0.0.2", 0);
			ContainerId contId = ContainerId.NewContainerId(appAttemptId, 3);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(contId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nid);
			Org.Mockito.Mockito.When(container.GetNodeHttpAddress()).ThenReturn("localhost:0"
				);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaSchedule));
			taImpl.Handle(new TaskAttemptContainerAssignedEvent(attemptId, container, Org.Mockito.Mockito.Mock
				<IDictionary>()));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in assinged state", taImpl.GetInternalState
				(), TaskAttemptStateInternal.Assigned);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaKill));
			NUnit.Framework.Assert.AreEqual("Task should be in KILLED state", TaskAttemptStateInternal
				.KillContainerCleanup, taImpl.GetInternalState());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerKillWhileRunning()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			TestTaskAttempt.MockEventHandler eventHandler = new TestTaskAttempt.MockEventHandler
				();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttempt.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, "10");
			JobSplit.TaskSplitMetaInfo splits = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			Org.Mockito.Mockito.When(splits.GetLocations()).ThenReturn(new string[] { "127.0.0.1"
				 });
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			ClusterInfo clusterInfo = Org.Mockito.Mockito.Mock<ClusterInfo>();
			Resource resource = Org.Mockito.Mockito.Mock<Resource>();
			Org.Mockito.Mockito.When(appCtx.GetClusterInfo()).ThenReturn(clusterInfo);
			Org.Mockito.Mockito.When(resource.GetMemory()).ThenReturn(1024);
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, splits, jobConf, taListener, new Org.Apache.Hadoop.Security.Token.Token(), new 
				Credentials(), new SystemClock(), appCtx);
			NodeId nid = NodeId.NewInstance("127.0.0.2", 0);
			ContainerId contId = ContainerId.NewContainerId(appAttemptId, 3);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(contId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nid);
			Org.Mockito.Mockito.When(container.GetNodeHttpAddress()).ThenReturn("localhost:0"
				);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaSchedule));
			taImpl.Handle(new TaskAttemptContainerAssignedEvent(attemptId, container, Org.Mockito.Mockito.Mock
				<IDictionary>()));
			taImpl.Handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in running state", taImpl.GetState
				(), TaskAttemptState.Running);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaKill));
			NUnit.Framework.Assert.IsFalse("InternalError occurred trying to handle TA_KILL", 
				eventHandler.internalError);
			NUnit.Framework.Assert.AreEqual("Task should be in KILLED state", TaskAttemptStateInternal
				.KillContainerCleanup, taImpl.GetInternalState());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerKillWhileCommitPending()
		{
			ApplicationId appId = ApplicationId.NewInstance(1, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			TestTaskAttempt.MockEventHandler eventHandler = new TestTaskAttempt.MockEventHandler
				();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttempt.StubbedFS), typeof(FileSystem
				));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, "10");
			JobSplit.TaskSplitMetaInfo splits = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			Org.Mockito.Mockito.When(splits.GetLocations()).ThenReturn(new string[] { "127.0.0.1"
				 });
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			ClusterInfo clusterInfo = Org.Mockito.Mockito.Mock<ClusterInfo>();
			Resource resource = Org.Mockito.Mockito.Mock<Resource>();
			Org.Mockito.Mockito.When(appCtx.GetClusterInfo()).ThenReturn(clusterInfo);
			Org.Mockito.Mockito.When(resource.GetMemory()).ThenReturn(1024);
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, splits, jobConf, taListener, new Org.Apache.Hadoop.Security.Token.Token(), new 
				Credentials(), new SystemClock(), appCtx);
			NodeId nid = NodeId.NewInstance("127.0.0.2", 0);
			ContainerId contId = ContainerId.NewContainerId(appAttemptId, 3);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(contId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nid);
			Org.Mockito.Mockito.When(container.GetNodeHttpAddress()).ThenReturn("localhost:0"
				);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaSchedule));
			taImpl.Handle(new TaskAttemptContainerAssignedEvent(attemptId, container, Org.Mockito.Mockito.Mock
				<IDictionary>()));
			taImpl.Handle(new TaskAttemptContainerLaunchedEvent(attemptId, 0));
			NUnit.Framework.Assert.AreEqual("Task attempt is not in running state", taImpl.GetState
				(), TaskAttemptState.Running);
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaCommitPending
				));
			NUnit.Framework.Assert.AreEqual("Task should be in COMMIT_PENDING state", TaskAttemptStateInternal
				.CommitPending, taImpl.GetInternalState());
			taImpl.Handle(new TaskAttemptEvent(attemptId, TaskAttemptEventType.TaKill));
			NUnit.Framework.Assert.IsFalse("InternalError occurred trying to handle TA_KILL", 
				eventHandler.internalError);
			NUnit.Framework.Assert.AreEqual("Task should be in KILLED state", TaskAttemptStateInternal
				.KillContainerCleanup, taImpl.GetInternalState());
		}

		public class MockEventHandler : EventHandler
		{
			public bool internalError;

			public virtual void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				if (@event is JobEvent)
				{
					JobEvent je = ((JobEvent)@event);
					if (JobEventType.InternalError == je.GetType())
					{
						internalError = true;
					}
				}
			}
		}
	}
}
