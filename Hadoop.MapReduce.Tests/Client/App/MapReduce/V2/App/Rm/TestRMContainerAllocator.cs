using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	public class TestRMContainerAllocator
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestRMContainerAllocator
			));

		internal static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		[SetUp]
		public virtual void Setup()
		{
			TestRMContainerAllocator.MyContainerAllocator.GetJobUpdatedNodeEvents().Clear();
			TestRMContainerAllocator.MyContainerAllocator.GetTaskAttemptKillEvents().Clear();
			// make each test create a fresh user to avoid leaking tokens between tests
			UserGroupInformation.SetLoginUser(null);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			DefaultMetricsSystem.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimple()
		{
			Log.Info("Running testSimple");
			Configuration conf = new Configuration();
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob);
			// add resources to scheduler
			MockNM nodeManager1 = rm.RegisterNode("h1:1234", 10240);
			MockNM nodeManager2 = rm.RegisterNode("h2:1234", 10240);
			MockNM nodeManager3 = rm.RegisterNode("h3:1234", 10240);
			dispatcher.Await();
			// create the container request
			ContainerRequestEvent event1 = CreateReq(jobId, 1, 1024, new string[] { "h1" });
			allocator.SendRequest(event1);
			// send 1 more request with different resource req
			ContainerRequestEvent event2 = CreateReq(jobId, 2, 1024, new string[] { "h2" });
			allocator.SendRequest(event2);
			// this tells the scheduler about the requests
			// as nodes are not added, no allocations
			IList<TaskAttemptContainerAssignedEvent> assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			NUnit.Framework.Assert.AreEqual(4, rm.GetMyFifoScheduler().lastAsk.Count);
			// send another request with different resource and priority
			ContainerRequestEvent event3 = CreateReq(jobId, 3, 1024, new string[] { "h3" });
			allocator.SendRequest(event3);
			// this tells the scheduler about the requests
			// as nodes are not added, no allocations
			assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			NUnit.Framework.Assert.AreEqual(3, rm.GetMyFifoScheduler().lastAsk.Count);
			// update resources in scheduler
			nodeManager1.NodeHeartbeat(true);
			// Node heartbeat
			nodeManager2.NodeHeartbeat(true);
			// Node heartbeat
			nodeManager3.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual(0, rm.GetMyFifoScheduler().lastAsk.Count);
			CheckAssignments(new ContainerRequestEvent[] { event1, event2, event3 }, assigned
				, false);
			// check that the assigned container requests are cancelled
			assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual(5, rm.GetMyFifoScheduler().lastAsk.Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMapNodeLocality()
		{
			// test checks that ordering of allocated containers list from the RM does 
			// not affect the map->container assignment done by the AM. If there is a 
			// node local container available for a map then it should be assigned to 
			// that container and not a rack-local container that happened to be seen 
			// earlier in the allocated containers list from the RM.
			// Regression test for MAPREDUCE-4893
			Log.Info("Running testMapNodeLocality");
			Configuration conf = new Configuration();
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob);
			// add resources to scheduler
			MockNM nodeManager1 = rm.RegisterNode("h1:1234", 3072);
			// can assign 2 maps 
			rm.RegisterNode("h2:1234", 10240);
			// wont heartbeat on node local node
			MockNM nodeManager3 = rm.RegisterNode("h3:1234", 1536);
			// assign 1 map
			dispatcher.Await();
			// create the container requests for maps
			ContainerRequestEvent event1 = CreateReq(jobId, 1, 1024, new string[] { "h1" });
			allocator.SendRequest(event1);
			ContainerRequestEvent event2 = CreateReq(jobId, 2, 1024, new string[] { "h1" });
			allocator.SendRequest(event2);
			ContainerRequestEvent event3 = CreateReq(jobId, 3, 1024, new string[] { "h2" });
			allocator.SendRequest(event3);
			// this tells the scheduler about the requests
			// as nodes are not added, no allocations
			IList<TaskAttemptContainerAssignedEvent> assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			// update resources in scheduler
			// Node heartbeat from rack-local first. This makes node h3 the first in the
			// list of allocated containers but it should not be assigned to task1.
			nodeManager3.NodeHeartbeat(true);
			// Node heartbeat from node-local next. This allocates 2 node local 
			// containers for task1 and task2. These should be matched with those tasks.
			nodeManager1.NodeHeartbeat(true);
			dispatcher.Await();
			assigned = allocator.Schedule();
			dispatcher.Await();
			CheckAssignments(new ContainerRequestEvent[] { event1, event2, event3 }, assigned
				, false);
			// remove the rack-local assignment that should have happened for task3
			foreach (TaskAttemptContainerAssignedEvent @event in assigned)
			{
				if (@event.GetTaskAttemptID().Equals(event3.GetAttemptID()))
				{
					assigned.Remove(@event);
					NUnit.Framework.Assert.IsTrue(@event.GetContainer().GetNodeId().GetHost().Equals(
						"h3"));
					break;
				}
			}
			CheckAssignments(new ContainerRequestEvent[] { event1, event2 }, assigned, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResource()
		{
			Log.Info("Running testResource");
			Configuration conf = new Configuration();
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob);
			// add resources to scheduler
			MockNM nodeManager1 = rm.RegisterNode("h1:1234", 10240);
			MockNM nodeManager2 = rm.RegisterNode("h2:1234", 10240);
			MockNM nodeManager3 = rm.RegisterNode("h3:1234", 10240);
			dispatcher.Await();
			// create the container request
			ContainerRequestEvent event1 = CreateReq(jobId, 1, 1024, new string[] { "h1" });
			allocator.SendRequest(event1);
			// send 1 more request with different resource req
			ContainerRequestEvent event2 = CreateReq(jobId, 2, 2048, new string[] { "h2" });
			allocator.SendRequest(event2);
			// this tells the scheduler about the requests
			// as nodes are not added, no allocations
			IList<TaskAttemptContainerAssignedEvent> assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			// update resources in scheduler
			nodeManager1.NodeHeartbeat(true);
			// Node heartbeat
			nodeManager2.NodeHeartbeat(true);
			// Node heartbeat
			nodeManager3.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			assigned = allocator.Schedule();
			dispatcher.Await();
			CheckAssignments(new ContainerRequestEvent[] { event1, event2 }, assigned, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReducerRampdownDiagnostics()
		{
			Log.Info("Running tesReducerRampdownDiagnostics");
			Configuration conf = new Configuration();
			conf.SetFloat(MRJobConfig.CompletedMapsForReduceSlowstart, 0.0f);
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			string host = "host1";
			MockNM nm = rm.RegisterNode(string.Format("%s:1234", host), 2048);
			nm.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob);
			// add resources to scheduler
			dispatcher.Await();
			// create the container request
			string[] locations = new string[] { host };
			allocator.SendRequest(CreateReq(jobId, 0, 1024, locations, false, true));
			for (int i = 0; i < 1; )
			{
				dispatcher.Await();
				i += allocator.Schedule().Count;
				nm.NodeHeartbeat(true);
			}
			allocator.SendRequest(CreateReq(jobId, 0, 1024, locations, true, false));
			while (TestRMContainerAllocator.MyContainerAllocator.GetTaskAttemptKillEvents().Count
				 == 0)
			{
				dispatcher.Await();
				allocator.Schedule().Count;
				nm.NodeHeartbeat(true);
			}
			string killEventMessage = TestRMContainerAllocator.MyContainerAllocator.GetTaskAttemptKillEvents
				()[0].GetMessage();
			NUnit.Framework.Assert.IsTrue("No reducer rampDown preemption message", killEventMessage
				.Contains(RMContainerAllocator.RampdownDiagnostic));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPreemptReducers()
		{
			Log.Info("Running testPreemptReducers");
			Configuration conf = new Configuration();
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob, new SystemClock());
			allocator.SetMapResourceRequest(BuilderUtils.NewResource(1024, 1));
			allocator.SetReduceResourceRequest(BuilderUtils.NewResource(1024, 1));
			RMContainerAllocator.AssignedRequests assignedRequests = allocator.GetAssignedRequests
				();
			RMContainerAllocator.ScheduledRequests scheduledRequests = allocator.GetScheduledRequests
				();
			ContainerRequestEvent event1 = CreateReq(jobId, 1, 2048, new string[] { "h1" }, false
				, false);
			scheduledRequests.maps[Org.Mockito.Mockito.Mock<TaskAttemptId>()] = new RMContainerRequestor.ContainerRequest
				(event1, null);
			assignedRequests.reduces[Org.Mockito.Mockito.Mock<TaskAttemptId>()] = Org.Mockito.Mockito.Mock
				<Container>();
			allocator.PreemptReducesIfNeeded();
			NUnit.Framework.Assert.AreEqual("The reducer is not preempted", 1, assignedRequests
				.preemptionWaitingReduces.Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNonAggressivelyPreemptReducers()
		{
			Log.Info("Running testNonAggressivelyPreemptReducers");
			int preemptThreshold = 2;
			//sec
			Configuration conf = new Configuration();
			conf.SetInt(MRJobConfig.MrJobReducerPreemptDelaySec, preemptThreshold);
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			ControlledClock clock = new ControlledClock(null);
			clock.SetTime(1);
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob, clock);
			allocator.SetMapResourceRequest(BuilderUtils.NewResource(1024, 1));
			allocator.SetReduceResourceRequest(BuilderUtils.NewResource(1024, 1));
			RMContainerAllocator.AssignedRequests assignedRequests = allocator.GetAssignedRequests
				();
			RMContainerAllocator.ScheduledRequests scheduledRequests = allocator.GetScheduledRequests
				();
			ContainerRequestEvent event1 = CreateReq(jobId, 1, 2048, new string[] { "h1" }, false
				, false);
			scheduledRequests.maps[Org.Mockito.Mockito.Mock<TaskAttemptId>()] = new RMContainerRequestor.ContainerRequest
				(event1, null, clock.GetTime());
			assignedRequests.reduces[Org.Mockito.Mockito.Mock<TaskAttemptId>()] = Org.Mockito.Mockito.Mock
				<Container>();
			clock.SetTime(clock.GetTime() + 1);
			allocator.PreemptReducesIfNeeded();
			NUnit.Framework.Assert.AreEqual("The reducer is aggressively preeempted", 0, assignedRequests
				.preemptionWaitingReduces.Count);
			clock.SetTime(clock.GetTime() + (preemptThreshold) * 1000);
			allocator.PreemptReducesIfNeeded();
			NUnit.Framework.Assert.AreEqual("The reducer is not preeempted", 1, assignedRequests
				.preemptionWaitingReduces.Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMapReduceScheduling()
		{
			Log.Info("Running testMapReduceScheduling");
			Configuration conf = new Configuration();
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob);
			// add resources to scheduler
			MockNM nodeManager1 = rm.RegisterNode("h1:1234", 1024);
			MockNM nodeManager2 = rm.RegisterNode("h2:1234", 10240);
			MockNM nodeManager3 = rm.RegisterNode("h3:1234", 10240);
			dispatcher.Await();
			// create the container request
			// send MAP request
			ContainerRequestEvent event1 = CreateReq(jobId, 1, 2048, new string[] { "h1", "h2"
				 }, true, false);
			allocator.SendRequest(event1);
			// send REDUCE request
			ContainerRequestEvent event2 = CreateReq(jobId, 2, 3000, new string[] { "h1" }, false
				, true);
			allocator.SendRequest(event2);
			// send MAP request
			ContainerRequestEvent event3 = CreateReq(jobId, 3, 2048, new string[] { "h3" }, false
				, false);
			allocator.SendRequest(event3);
			// this tells the scheduler about the requests
			// as nodes are not added, no allocations
			IList<TaskAttemptContainerAssignedEvent> assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			// update resources in scheduler
			nodeManager1.NodeHeartbeat(true);
			// Node heartbeat
			nodeManager2.NodeHeartbeat(true);
			// Node heartbeat
			nodeManager3.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			assigned = allocator.Schedule();
			dispatcher.Await();
			CheckAssignments(new ContainerRequestEvent[] { event1, event3 }, assigned, false);
			// validate that no container is assigned to h1 as it doesn't have 2048
			foreach (TaskAttemptContainerAssignedEvent assig in assigned)
			{
				NUnit.Framework.Assert.IsFalse("Assigned count not correct", "h1".Equals(assig.GetContainer
					().GetNodeId().GetHost()));
			}
		}

		private class MyResourceManager : MockRM
		{
			private static long fakeClusterTimeStamp = Runtime.CurrentTimeMillis();

			public MyResourceManager(Configuration conf)
				: base(conf)
			{
			}

			public MyResourceManager(Configuration conf, RMStateStore store)
				: base(conf, store)
			{
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				base.ServiceStart();
				// Ensure that the application attempt IDs for all the tests are the same
				// The application attempt IDs will be used as the login user names
				TestRMContainerAllocator.MyResourceManager.SetClusterTimeStamp(fakeClusterTimeStamp
					);
			}

			protected override Dispatcher CreateDispatcher()
			{
				return new DrainDispatcher();
			}

			protected override EventHandler<SchedulerEvent> CreateSchedulerEventDispatcher()
			{
				// Dispatch inline for test sanity
				return new _EventHandler_669(this);
			}

			private sealed class _EventHandler_669 : EventHandler<SchedulerEvent>
			{
				public _EventHandler_669(MyResourceManager _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public void Handle(SchedulerEvent @event)
				{
					this._enclosing.scheduler.Handle(@event);
				}

				private readonly MyResourceManager _enclosing;
			}

			protected override ResourceScheduler CreateScheduler()
			{
				return new TestRMContainerAllocator.MyFifoScheduler(this.GetRMContext());
			}

			internal virtual TestRMContainerAllocator.MyFifoScheduler GetMyFifoScheduler()
			{
				return (TestRMContainerAllocator.MyFifoScheduler)scheduler;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReportedAppProgress()
		{
			Log.Info("Running testReportedAppProgress");
			Configuration conf = new Configuration();
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher rmDispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp rmApp = rm.SubmitApp(1024);
			rmDispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 21504);
			amNodeManager.NodeHeartbeat(true);
			rmDispatcher.Await();
			ApplicationAttemptId appAttemptId = rmApp.GetCurrentAppAttempt().GetAppAttemptId(
				);
			rm.SendAMLaunched(appAttemptId);
			rmDispatcher.Await();
			MRApp mrApp = new _MRApp_711(rm, appAttemptId, appAttemptId, ContainerId.NewContainerId
				(appAttemptId, 0), 10, 10, false, this.GetType().FullName, true, 1);
			NUnit.Framework.Assert.AreEqual(0.0, rmApp.GetProgress(), 0.0);
			mrApp.Submit(conf);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = mrApp.GetContext().GetAllJobs().
				GetEnumerator().Next().Value;
			DrainDispatcher amDispatcher = (DrainDispatcher)mrApp.GetDispatcher();
			TestRMContainerAllocator.MyContainerAllocator allocator = (TestRMContainerAllocator.MyContainerAllocator
				)mrApp.GetContainerAllocator();
			mrApp.WaitForInternalState((JobImpl)job, JobStateInternal.Running);
			amDispatcher.Await();
			// Wait till all map-attempts request for containers
			foreach (Task t in job.GetTasks().Values)
			{
				if (t.GetType() == TaskType.Map)
				{
					mrApp.WaitForInternalState((TaskAttemptImpl)t.GetAttempts().Values.GetEnumerator(
						).Next(), TaskAttemptStateInternal.Unassigned);
				}
			}
			amDispatcher.Await();
			allocator.Schedule();
			rmDispatcher.Await();
			amNodeManager.NodeHeartbeat(true);
			rmDispatcher.Await();
			allocator.Schedule();
			rmDispatcher.Await();
			// Wait for all map-tasks to be running
			foreach (Task t_1 in job.GetTasks().Values)
			{
				if (t_1.GetType() == TaskType.Map)
				{
					mrApp.WaitForState(t_1, TaskState.Running);
				}
			}
			allocator.Schedule();
			// Send heartbeat
			rmDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(0.05f, job.GetProgress(), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.05f, rmApp.GetProgress(), 0.001f);
			// Finish off 1 map.
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			FinishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 1);
			allocator.Schedule();
			rmDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(0.095f, job.GetProgress(), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.095f, rmApp.GetProgress(), 0.001f);
			// Finish off 7 more so that map-progress is 80%
			FinishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 7);
			allocator.Schedule();
			rmDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(0.41f, job.GetProgress(), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.41f, rmApp.GetProgress(), 0.001f);
			// Finish off the 2 remaining maps
			FinishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 2);
			allocator.Schedule();
			rmDispatcher.Await();
			amNodeManager.NodeHeartbeat(true);
			rmDispatcher.Await();
			allocator.Schedule();
			rmDispatcher.Await();
			// Wait for all reduce-tasks to be running
			foreach (Task t_2 in job.GetTasks().Values)
			{
				if (t_2.GetType() == TaskType.Reduce)
				{
					mrApp.WaitForState(t_2, TaskState.Running);
				}
			}
			// Finish off 2 reduces
			FinishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 2);
			allocator.Schedule();
			rmDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(0.59f, job.GetProgress(), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.59f, rmApp.GetProgress(), 0.001f);
			// Finish off the remaining 8 reduces.
			FinishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 8);
			allocator.Schedule();
			rmDispatcher.Await();
			// Remaining is JobCleanup
			NUnit.Framework.Assert.AreEqual(0.95f, job.GetProgress(), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.95f, rmApp.GetProgress(), 0.001f);
		}

		private sealed class _MRApp_711 : MRApp
		{
			public _MRApp_711(TestRMContainerAllocator.MyResourceManager rm, ApplicationAttemptId
				 appAttemptId, ApplicationAttemptId baseArg1, ContainerId baseArg2, int baseArg3
				, int baseArg4, bool baseArg5, string baseArg6, bool baseArg7, int baseArg8)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7, baseArg8
					)
			{
				this.rm = rm;
				this.appAttemptId = appAttemptId;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return new DrainDispatcher();
			}

			protected internal override ContainerAllocator CreateContainerAllocator(ClientService
				 clientService, AppContext context)
			{
				return new TestRMContainerAllocator.MyContainerAllocator(rm, appAttemptId, context
					);
			}

			private readonly TestRMContainerAllocator.MyResourceManager rm;

			private readonly ApplicationAttemptId appAttemptId;
		}

		/// <exception cref="System.Exception"/>
		private void FinishNextNTasks(DrainDispatcher rmDispatcher, MockNM node, MRApp mrApp
			, IEnumerator<Task> it, int nextN)
		{
			Task task;
			for (int i = 0; i < nextN; i++)
			{
				task = it.Next();
				FinishTask(rmDispatcher, node, mrApp, task);
			}
		}

		/// <exception cref="System.Exception"/>
		private void FinishTask(DrainDispatcher rmDispatcher, MockNM node, MRApp mrApp, Task
			 task)
		{
			TaskAttempt attempt = task.GetAttempts().Values.GetEnumerator().Next();
			IList<ContainerStatus> contStatus = new AList<ContainerStatus>(1);
			contStatus.AddItem(ContainerStatus.NewInstance(attempt.GetAssignedContainerID(), 
				ContainerState.Complete, string.Empty, 0));
			IDictionary<ApplicationId, IList<ContainerStatus>> statusUpdate = new Dictionary<
				ApplicationId, IList<ContainerStatus>>(1);
			statusUpdate[mrApp.GetAppID()] = contStatus;
			node.NodeHeartbeat(statusUpdate, true);
			rmDispatcher.Await();
			mrApp.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attempt.GetID(), 
				TaskAttemptEventType.TaDone));
			mrApp.WaitForState(task, TaskState.Succeeded);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReportedAppProgressWithOnlyMaps()
		{
			Log.Info("Running testReportedAppProgressWithOnlyMaps");
			Configuration conf = new Configuration();
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher rmDispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp rmApp = rm.SubmitApp(1024);
			rmDispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 11264);
			amNodeManager.NodeHeartbeat(true);
			rmDispatcher.Await();
			ApplicationAttemptId appAttemptId = rmApp.GetCurrentAppAttempt().GetAppAttemptId(
				);
			rm.SendAMLaunched(appAttemptId);
			rmDispatcher.Await();
			MRApp mrApp = new _MRApp_863(rm, appAttemptId, appAttemptId, ContainerId.NewContainerId
				(appAttemptId, 0), 10, 0, false, this.GetType().FullName, true, 1);
			NUnit.Framework.Assert.AreEqual(0.0, rmApp.GetProgress(), 0.0);
			mrApp.Submit(conf);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = mrApp.GetContext().GetAllJobs().
				GetEnumerator().Next().Value;
			DrainDispatcher amDispatcher = (DrainDispatcher)mrApp.GetDispatcher();
			TestRMContainerAllocator.MyContainerAllocator allocator = (TestRMContainerAllocator.MyContainerAllocator
				)mrApp.GetContainerAllocator();
			mrApp.WaitForInternalState((JobImpl)job, JobStateInternal.Running);
			amDispatcher.Await();
			// Wait till all map-attempts request for containers
			foreach (Task t in job.GetTasks().Values)
			{
				mrApp.WaitForInternalState((TaskAttemptImpl)t.GetAttempts().Values.GetEnumerator(
					).Next(), TaskAttemptStateInternal.Unassigned);
			}
			amDispatcher.Await();
			allocator.Schedule();
			rmDispatcher.Await();
			amNodeManager.NodeHeartbeat(true);
			rmDispatcher.Await();
			allocator.Schedule();
			rmDispatcher.Await();
			// Wait for all map-tasks to be running
			foreach (Task t_1 in job.GetTasks().Values)
			{
				mrApp.WaitForState(t_1, TaskState.Running);
			}
			allocator.Schedule();
			// Send heartbeat
			rmDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(0.05f, job.GetProgress(), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.05f, rmApp.GetProgress(), 0.001f);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			// Finish off 1 map so that map-progress is 10%
			FinishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 1);
			allocator.Schedule();
			rmDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(0.14f, job.GetProgress(), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.14f, rmApp.GetProgress(), 0.001f);
			// Finish off 5 more map so that map-progress is 60%
			FinishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 5);
			allocator.Schedule();
			rmDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(0.59f, job.GetProgress(), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.59f, rmApp.GetProgress(), 0.001f);
			// Finish off remaining map so that map-progress is 100%
			FinishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 4);
			allocator.Schedule();
			rmDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(0.95f, job.GetProgress(), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.95f, rmApp.GetProgress(), 0.001f);
		}

		private sealed class _MRApp_863 : MRApp
		{
			public _MRApp_863(TestRMContainerAllocator.MyResourceManager rm, ApplicationAttemptId
				 appAttemptId, ApplicationAttemptId baseArg1, ContainerId baseArg2, int baseArg3
				, int baseArg4, bool baseArg5, string baseArg6, bool baseArg7, int baseArg8)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7, baseArg8
					)
			{
				this.rm = rm;
				this.appAttemptId = appAttemptId;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return new DrainDispatcher();
			}

			protected internal override ContainerAllocator CreateContainerAllocator(ClientService
				 clientService, AppContext context)
			{
				return new TestRMContainerAllocator.MyContainerAllocator(rm, appAttemptId, context
					);
			}

			private readonly TestRMContainerAllocator.MyResourceManager rm;

			private readonly ApplicationAttemptId appAttemptId;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdatedNodes()
		{
			Configuration conf = new Configuration();
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob);
			// add resources to scheduler
			MockNM nm1 = rm.RegisterNode("h1:1234", 10240);
			MockNM nm2 = rm.RegisterNode("h2:1234", 10240);
			dispatcher.Await();
			// create the map container request
			ContainerRequestEvent @event = CreateReq(jobId, 1, 1024, new string[] { "h1" });
			allocator.SendRequest(@event);
			TaskAttemptId attemptId = @event.GetAttemptID();
			TaskAttempt mockTaskAttempt = Org.Mockito.Mockito.Mock<TaskAttempt>();
			Org.Mockito.Mockito.When(mockTaskAttempt.GetNodeId()).ThenReturn(nm1.GetNodeId());
			Task mockTask = Org.Mockito.Mockito.Mock<Task>();
			Org.Mockito.Mockito.When(mockTask.GetAttempt(attemptId)).ThenReturn(mockTaskAttempt
				);
			Org.Mockito.Mockito.When(mockJob.GetTask(attemptId.GetTaskId())).ThenReturn(mockTask
				);
			// this tells the scheduler about the requests
			IList<TaskAttemptContainerAssignedEvent> assigned = allocator.Schedule();
			dispatcher.Await();
			nm1.NodeHeartbeat(true);
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual(1, TestRMContainerAllocator.MyContainerAllocator.
				GetJobUpdatedNodeEvents().Count);
			NUnit.Framework.Assert.AreEqual(3, TestRMContainerAllocator.MyContainerAllocator.
				GetJobUpdatedNodeEvents()[0].GetUpdatedNodes().Count);
			TestRMContainerAllocator.MyContainerAllocator.GetJobUpdatedNodeEvents().Clear();
			// get the assignment
			assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual(1, assigned.Count);
			NUnit.Framework.Assert.AreEqual(nm1.GetNodeId(), assigned[0].GetContainer().GetNodeId
				());
			// no updated nodes reported
			NUnit.Framework.Assert.IsTrue(TestRMContainerAllocator.MyContainerAllocator.GetJobUpdatedNodeEvents
				().IsEmpty());
			NUnit.Framework.Assert.IsTrue(TestRMContainerAllocator.MyContainerAllocator.GetTaskAttemptKillEvents
				().IsEmpty());
			// mark nodes bad
			nm1.NodeHeartbeat(false);
			nm2.NodeHeartbeat(false);
			dispatcher.Await();
			// schedule response returns updated nodes
			assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual(0, assigned.Count);
			// updated nodes are reported
			NUnit.Framework.Assert.AreEqual(1, TestRMContainerAllocator.MyContainerAllocator.
				GetJobUpdatedNodeEvents().Count);
			NUnit.Framework.Assert.AreEqual(1, TestRMContainerAllocator.MyContainerAllocator.
				GetTaskAttemptKillEvents().Count);
			NUnit.Framework.Assert.AreEqual(2, TestRMContainerAllocator.MyContainerAllocator.
				GetJobUpdatedNodeEvents()[0].GetUpdatedNodes().Count);
			NUnit.Framework.Assert.AreEqual(attemptId, TestRMContainerAllocator.MyContainerAllocator
				.GetTaskAttemptKillEvents()[0].GetTaskAttemptID());
			TestRMContainerAllocator.MyContainerAllocator.GetJobUpdatedNodeEvents().Clear();
			TestRMContainerAllocator.MyContainerAllocator.GetTaskAttemptKillEvents().Clear();
			assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual(0, assigned.Count);
			// no updated nodes reported
			NUnit.Framework.Assert.IsTrue(TestRMContainerAllocator.MyContainerAllocator.GetJobUpdatedNodeEvents
				().IsEmpty());
			NUnit.Framework.Assert.IsTrue(TestRMContainerAllocator.MyContainerAllocator.GetTaskAttemptKillEvents
				().IsEmpty());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlackListedNodes()
		{
			Log.Info("Running testBlackListedNodes");
			Configuration conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobNodeBlacklistingEnable, true);
			conf.SetInt(MRJobConfig.MaxTaskFailuresPerTracker, 1);
			conf.SetInt(MRJobConfig.MrAmIgnoreBlacklistingBlacklistedNodePerecent, -1);
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob);
			// add resources to scheduler
			MockNM nodeManager1 = rm.RegisterNode("h1:1234", 10240);
			MockNM nodeManager2 = rm.RegisterNode("h2:1234", 10240);
			MockNM nodeManager3 = rm.RegisterNode("h3:1234", 10240);
			dispatcher.Await();
			// create the container request
			ContainerRequestEvent event1 = CreateReq(jobId, 1, 1024, new string[] { "h1" });
			allocator.SendRequest(event1);
			// send 1 more request with different resource req
			ContainerRequestEvent event2 = CreateReq(jobId, 2, 1024, new string[] { "h2" });
			allocator.SendRequest(event2);
			// send another request with different resource and priority
			ContainerRequestEvent event3 = CreateReq(jobId, 3, 1024, new string[] { "h3" });
			allocator.SendRequest(event3);
			// this tells the scheduler about the requests
			// as nodes are not added, no allocations
			IList<TaskAttemptContainerAssignedEvent> assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			// Send events to blacklist nodes h1 and h2
			ContainerFailedEvent f1 = CreateFailEvent(jobId, 1, "h1", false);
			allocator.SendFailure(f1);
			ContainerFailedEvent f2 = CreateFailEvent(jobId, 1, "h2", false);
			allocator.SendFailure(f2);
			// update resources in scheduler
			nodeManager1.NodeHeartbeat(true);
			// Node heartbeat
			nodeManager2.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			assigned = allocator.Schedule();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			AssertBlacklistAdditionsAndRemovals(2, 0, rm);
			// mark h1/h2 as bad nodes
			nodeManager1.NodeHeartbeat(false);
			nodeManager2.NodeHeartbeat(false);
			dispatcher.Await();
			assigned = allocator.Schedule();
			dispatcher.Await();
			AssertBlacklistAdditionsAndRemovals(0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			nodeManager3.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			assigned = allocator.Schedule();
			dispatcher.Await();
			AssertBlacklistAdditionsAndRemovals(0, 0, rm);
			NUnit.Framework.Assert.IsTrue("No of assignments must be 3", assigned.Count == 3);
			// validate that all containers are assigned to h3
			foreach (TaskAttemptContainerAssignedEvent assig in assigned)
			{
				NUnit.Framework.Assert.IsTrue("Assigned container host not correct", "h3".Equals(
					assig.GetContainer().GetNodeId().GetHost()));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIgnoreBlacklisting()
		{
			Log.Info("Running testIgnoreBlacklisting");
			Configuration conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobNodeBlacklistingEnable, true);
			conf.SetInt(MRJobConfig.MaxTaskFailuresPerTracker, 1);
			conf.SetInt(MRJobConfig.MrAmIgnoreBlacklistingBlacklistedNodePerecent, 33);
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM[] nodeManagers = new MockNM[10];
			int nmNum = 0;
			IList<TaskAttemptContainerAssignedEvent> assigned = null;
			nodeManagers[nmNum] = RegisterNodeManager(nmNum++, rm, dispatcher);
			nodeManagers[0].NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob);
			// Known=1, blacklisted=0, ignore should be false - assign first container
			assigned = GetContainerOnHost(jobId, 1, 1024, new string[] { "h1" }, nodeManagers
				[0], dispatcher, allocator, 0, 0, 0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 1", 1, assigned.Count);
			Log.Info("Failing container _1 on H1 (Node should be blacklisted and" + " ignore blacklisting enabled"
				);
			// Send events to blacklist nodes h1 and h2
			ContainerFailedEvent f1 = CreateFailEvent(jobId, 1, "h1", false);
			allocator.SendFailure(f1);
			// Test single node.
			// Known=1, blacklisted=1, ignore should be true - assign 0
			// Because makeRemoteRequest will not be aware of it until next call
			// The current call will send blacklisted node "h1" to RM
			assigned = GetContainerOnHost(jobId, 2, 1024, new string[] { "h1" }, nodeManagers
				[0], dispatcher, allocator, 1, 0, 0, 1, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			// Known=1, blacklisted=1, ignore should be true - assign 1
			assigned = GetContainerOnHost(jobId, 2, 1024, new string[] { "h1" }, nodeManagers
				[0], dispatcher, allocator, 0, 0, 0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 1", 1, assigned.Count);
			nodeManagers[nmNum] = RegisterNodeManager(nmNum++, rm, dispatcher);
			// Known=2, blacklisted=1, ignore should be true - assign 1 anyway.
			assigned = GetContainerOnHost(jobId, 3, 1024, new string[] { "h2" }, nodeManagers
				[1], dispatcher, allocator, 0, 0, 0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 1", 1, assigned.Count);
			nodeManagers[nmNum] = RegisterNodeManager(nmNum++, rm, dispatcher);
			// Known=3, blacklisted=1, ignore should be true - assign 1 anyway.
			assigned = GetContainerOnHost(jobId, 4, 1024, new string[] { "h3" }, nodeManagers
				[2], dispatcher, allocator, 0, 0, 0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 1", 1, assigned.Count);
			// Known=3, blacklisted=1, ignore should be true - assign 1
			assigned = GetContainerOnHost(jobId, 5, 1024, new string[] { "h1" }, nodeManagers
				[0], dispatcher, allocator, 0, 0, 0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 1", 1, assigned.Count);
			nodeManagers[nmNum] = RegisterNodeManager(nmNum++, rm, dispatcher);
			// Known=4, blacklisted=1, ignore should be false - assign 1 anyway
			assigned = GetContainerOnHost(jobId, 6, 1024, new string[] { "h4" }, nodeManagers
				[3], dispatcher, allocator, 0, 0, 1, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 1", 1, assigned.Count);
			// Test blacklisting re-enabled.
			// Known=4, blacklisted=1, ignore should be false - no assignment on h1
			assigned = GetContainerOnHost(jobId, 7, 1024, new string[] { "h1" }, nodeManagers
				[0], dispatcher, allocator, 0, 0, 0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			// RMContainerRequestor would have created a replacement request.
			// Blacklist h2
			ContainerFailedEvent f2 = CreateFailEvent(jobId, 3, "h2", false);
			allocator.SendFailure(f2);
			// Test ignore blacklisting re-enabled
			// Known=4, blacklisted=2, ignore should be true. Should assign 0
			// container for the same reason above.
			assigned = GetContainerOnHost(jobId, 8, 1024, new string[] { "h1" }, nodeManagers
				[0], dispatcher, allocator, 1, 0, 0, 2, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			// Known=4, blacklisted=2, ignore should be true. Should assign 2
			// containers.
			assigned = GetContainerOnHost(jobId, 8, 1024, new string[] { "h1" }, nodeManagers
				[0], dispatcher, allocator, 0, 0, 0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 2", 2, assigned.Count);
			// Known=4, blacklisted=2, ignore should be true.
			assigned = GetContainerOnHost(jobId, 9, 1024, new string[] { "h2" }, nodeManagers
				[1], dispatcher, allocator, 0, 0, 0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 1", 1, assigned.Count);
			// Test blacklist while ignore blacklisting enabled
			ContainerFailedEvent f3 = CreateFailEvent(jobId, 4, "h3", false);
			allocator.SendFailure(f3);
			nodeManagers[nmNum] = RegisterNodeManager(nmNum++, rm, dispatcher);
			// Known=5, blacklisted=3, ignore should be true.
			assigned = GetContainerOnHost(jobId, 10, 1024, new string[] { "h3" }, nodeManagers
				[2], dispatcher, allocator, 0, 0, 0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 1", 1, assigned.Count);
			// Assign on 5 more nodes - to re-enable blacklisting
			for (int i = 0; i < 5; i++)
			{
				nodeManagers[nmNum] = RegisterNodeManager(nmNum++, rm, dispatcher);
				assigned = GetContainerOnHost(jobId, 11 + i, 1024, new string[] { (5 + i).ToString
					() }, nodeManagers[4 + i], dispatcher, allocator, 0, 0, (i == 4 ? 3 : 0), 0, rm);
				NUnit.Framework.Assert.AreEqual("No of assignments must be 1", 1, assigned.Count);
			}
			// Test h3 (blacklisted while ignoring blacklisting) is blacklisted.
			assigned = GetContainerOnHost(jobId, 20, 1024, new string[] { "h3" }, nodeManagers
				[2], dispatcher, allocator, 0, 0, 0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
		}

		/// <exception cref="System.Exception"/>
		private MockNM RegisterNodeManager(int i, TestRMContainerAllocator.MyResourceManager
			 rm, DrainDispatcher dispatcher)
		{
			MockNM nm = rm.RegisterNode("h" + (i + 1) + ":1234", 10240);
			dispatcher.Await();
			return nm;
		}

		/// <exception cref="System.Exception"/>
		private IList<TaskAttemptContainerAssignedEvent> GetContainerOnHost(JobId jobId, 
			int taskAttemptId, int memory, string[] hosts, MockNM mockNM, DrainDispatcher dispatcher
			, TestRMContainerAllocator.MyContainerAllocator allocator, int expectedAdditions1
			, int expectedRemovals1, int expectedAdditions2, int expectedRemovals2, TestRMContainerAllocator.MyResourceManager
			 rm)
		{
			ContainerRequestEvent reqEvent = CreateReq(jobId, taskAttemptId, memory, hosts);
			allocator.SendRequest(reqEvent);
			// Send the request to the RM
			IList<TaskAttemptContainerAssignedEvent> assigned = allocator.Schedule();
			dispatcher.Await();
			AssertBlacklistAdditionsAndRemovals(expectedAdditions1, expectedRemovals1, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			// Heartbeat from the required nodeManager
			mockNM.NodeHeartbeat(true);
			dispatcher.Await();
			assigned = allocator.Schedule();
			dispatcher.Await();
			AssertBlacklistAdditionsAndRemovals(expectedAdditions2, expectedRemovals2, rm);
			return assigned;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlackListedNodesWithSchedulingToThatNode()
		{
			Log.Info("Running testBlackListedNodesWithSchedulingToThatNode");
			Configuration conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobNodeBlacklistingEnable, true);
			conf.SetInt(MRJobConfig.MaxTaskFailuresPerTracker, 1);
			conf.SetInt(MRJobConfig.MrAmIgnoreBlacklistingBlacklistedNodePerecent, -1);
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob);
			// add resources to scheduler
			MockNM nodeManager1 = rm.RegisterNode("h1:1234", 10240);
			MockNM nodeManager3 = rm.RegisterNode("h3:1234", 10240);
			dispatcher.Await();
			Log.Info("Requesting 1 Containers _1 on H1");
			// create the container request
			ContainerRequestEvent event1 = CreateReq(jobId, 1, 1024, new string[] { "h1" });
			allocator.SendRequest(event1);
			Log.Info("RM Heartbeat (to send the container requests)");
			// this tells the scheduler about the requests
			// as nodes are not added, no allocations
			IList<TaskAttemptContainerAssignedEvent> assigned = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			Log.Info("h1 Heartbeat (To actually schedule the containers)");
			// update resources in scheduler
			nodeManager1.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			Log.Info("RM Heartbeat (To process the scheduled containers)");
			assigned = allocator.Schedule();
			dispatcher.Await();
			AssertBlacklistAdditionsAndRemovals(0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 1", 1, assigned.Count);
			Log.Info("Failing container _1 on H1 (should blacklist the node)");
			// Send events to blacklist nodes h1 and h2
			ContainerFailedEvent f1 = CreateFailEvent(jobId, 1, "h1", false);
			allocator.SendFailure(f1);
			//At this stage, a request should be created for a fast fail map
			//Create a FAST_FAIL request for a previously failed map.
			ContainerRequestEvent event1f = CreateReq(jobId, 1, 1024, new string[] { "h1" }, 
				true, false);
			allocator.SendRequest(event1f);
			//Update the Scheduler with the new requests.
			assigned = allocator.Schedule();
			dispatcher.Await();
			AssertBlacklistAdditionsAndRemovals(1, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			// send another request with different resource and priority
			ContainerRequestEvent event3 = CreateReq(jobId, 3, 1024, new string[] { "h1", "h3"
				 });
			allocator.SendRequest(event3);
			//Allocator is aware of prio:5 container, and prio:20 (h1+h3) container.
			//RM is only aware of the prio:5 container
			Log.Info("h1 Heartbeat (To actually schedule the containers)");
			// update resources in scheduler
			nodeManager1.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			Log.Info("RM Heartbeat (To process the scheduled containers)");
			assigned = allocator.Schedule();
			dispatcher.Await();
			AssertBlacklistAdditionsAndRemovals(0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			//RMContainerAllocator gets assigned a p:5 on a blacklisted node.
			//Send a release for the p:5 container + another request.
			Log.Info("RM Heartbeat (To process the re-scheduled containers)");
			assigned = allocator.Schedule();
			dispatcher.Await();
			AssertBlacklistAdditionsAndRemovals(0, 0, rm);
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assigned.Count);
			//Hearbeat from H3 to schedule on this host.
			Log.Info("h3 Heartbeat (To re-schedule the containers)");
			nodeManager3.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			Log.Info("RM Heartbeat (To process the re-scheduled containers for H3)");
			assigned = allocator.Schedule();
			AssertBlacklistAdditionsAndRemovals(0, 0, rm);
			dispatcher.Await();
			// For debugging
			foreach (TaskAttemptContainerAssignedEvent assig in assigned)
			{
				Log.Info(assig.GetTaskAttemptID() + " assgined to " + assig.GetContainer().GetId(
					) + " with priority " + assig.GetContainer().GetPriority());
			}
			NUnit.Framework.Assert.AreEqual("No of assignments must be 2", 2, assigned.Count);
			// validate that all containers are assigned to h3
			foreach (TaskAttemptContainerAssignedEvent assig_1 in assigned)
			{
				NUnit.Framework.Assert.AreEqual("Assigned container " + assig_1.GetContainer().GetId
					() + " host not correct", "h3", assig_1.GetContainer().GetNodeId().GetHost());
			}
		}

		private static void AssertBlacklistAdditionsAndRemovals(int expectedAdditions, int
			 expectedRemovals, TestRMContainerAllocator.MyResourceManager rm)
		{
			NUnit.Framework.Assert.AreEqual(expectedAdditions, rm.GetMyFifoScheduler().lastBlacklistAdditions
				.Count);
			NUnit.Framework.Assert.AreEqual(expectedRemovals, rm.GetMyFifoScheduler().lastBlacklistRemovals
				.Count);
		}

		private static void AssertAsksAndReleases(int expectedAsk, int expectedRelease, TestRMContainerAllocator.MyResourceManager
			 rm)
		{
			NUnit.Framework.Assert.AreEqual(expectedAsk, rm.GetMyFifoScheduler().lastAsk.Count
				);
			NUnit.Framework.Assert.AreEqual(expectedRelease, rm.GetMyFifoScheduler().lastRelease
				.Count);
		}

		private class MyFifoScheduler : FifoScheduler
		{
			public MyFifoScheduler(RMContext rmContext)
				: base()
			{
				try
				{
					Configuration conf = new Configuration();
					Reinitialize(conf, rmContext);
				}
				catch (IOException ie)
				{
					Log.Info("add application failed with ", ie);
					System.Diagnostics.Debug.Assert((false));
				}
			}

			internal IList<ResourceRequest> lastAsk = null;

			internal IList<ContainerId> lastRelease = null;

			internal IList<string> lastBlacklistAdditions;

			internal IList<string> lastBlacklistRemovals;

			// override this to copy the objects otherwise FifoScheduler updates the
			// numContainers in same objects as kept by RMContainerAllocator
			public override Allocation Allocate(ApplicationAttemptId applicationAttemptId, IList
				<ResourceRequest> ask, IList<ContainerId> release, IList<string> blacklistAdditions
				, IList<string> blacklistRemovals)
			{
				lock (this)
				{
					IList<ResourceRequest> askCopy = new AList<ResourceRequest>();
					foreach (ResourceRequest req in ask)
					{
						ResourceRequest reqCopy = ResourceRequest.NewInstance(req.GetPriority(), req.GetResourceName
							(), req.GetCapability(), req.GetNumContainers(), req.GetRelaxLocality());
						askCopy.AddItem(reqCopy);
					}
					lastAsk = ask;
					lastRelease = release;
					lastBlacklistAdditions = blacklistAdditions;
					lastBlacklistRemovals = blacklistRemovals;
					return base.Allocate(applicationAttemptId, askCopy, release, blacklistAdditions, 
						blacklistRemovals);
				}
			}
		}

		private ContainerRequestEvent CreateReq(JobId jobId, int taskAttemptId, int memory
			, string[] hosts)
		{
			return CreateReq(jobId, taskAttemptId, memory, hosts, false, false);
		}

		private ContainerRequestEvent CreateReq(JobId jobId, int taskAttemptId, int memory
			, string[] hosts, bool earlierFailedAttempt, bool reduce)
		{
			TaskId taskId;
			if (reduce)
			{
				taskId = MRBuilderUtils.NewTaskId(jobId, 0, TaskType.Reduce);
			}
			else
			{
				taskId = MRBuilderUtils.NewTaskId(jobId, 0, TaskType.Map);
			}
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, taskAttemptId);
			Resource containerNeed = Resource.NewInstance(memory, 1);
			if (earlierFailedAttempt)
			{
				return ContainerRequestEvent.CreateContainerRequestEventForFailedContainer(attemptId
					, containerNeed);
			}
			return new ContainerRequestEvent(attemptId, containerNeed, hosts, new string[] { 
				NetworkTopology.DefaultRack });
		}

		private ContainerFailedEvent CreateFailEvent(JobId jobId, int taskAttemptId, string
			 host, bool reduce)
		{
			TaskId taskId;
			if (reduce)
			{
				taskId = MRBuilderUtils.NewTaskId(jobId, 0, TaskType.Reduce);
			}
			else
			{
				taskId = MRBuilderUtils.NewTaskId(jobId, 0, TaskType.Map);
			}
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, taskAttemptId);
			return new ContainerFailedEvent(attemptId, host);
		}

		private ContainerAllocatorEvent CreateDeallocateEvent(JobId jobId, int taskAttemptId
			, bool reduce)
		{
			TaskId taskId;
			if (reduce)
			{
				taskId = MRBuilderUtils.NewTaskId(jobId, 0, TaskType.Reduce);
			}
			else
			{
				taskId = MRBuilderUtils.NewTaskId(jobId, 0, TaskType.Map);
			}
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(taskId, taskAttemptId);
			return new ContainerAllocatorEvent(attemptId, ContainerAllocator.EventType.ContainerDeallocate
				);
		}

		private void CheckAssignments(ContainerRequestEvent[] requests, IList<TaskAttemptContainerAssignedEvent
			> assignments, bool checkHostMatch)
		{
			NUnit.Framework.Assert.IsNotNull("Container not assigned", assignments);
			NUnit.Framework.Assert.AreEqual("Assigned count not correct", requests.Length, assignments
				.Count);
			// check for uniqueness of containerIDs
			ICollection<ContainerId> containerIds = new HashSet<ContainerId>();
			foreach (TaskAttemptContainerAssignedEvent assigned in assignments)
			{
				containerIds.AddItem(assigned.GetContainer().GetId());
			}
			NUnit.Framework.Assert.AreEqual("Assigned containers must be different", assignments
				.Count, containerIds.Count);
			// check for all assignment
			foreach (ContainerRequestEvent req in requests)
			{
				TaskAttemptContainerAssignedEvent assigned_1 = null;
				foreach (TaskAttemptContainerAssignedEvent ass in assignments)
				{
					if (ass.GetTaskAttemptID().Equals(req.GetAttemptID()))
					{
						assigned_1 = ass;
						break;
					}
				}
				CheckAssignment(req, assigned_1, checkHostMatch);
			}
		}

		private void CheckAssignment(ContainerRequestEvent request, TaskAttemptContainerAssignedEvent
			 assigned, bool checkHostMatch)
		{
			NUnit.Framework.Assert.IsNotNull("Nothing assigned to attempt " + request.GetAttemptID
				(), assigned);
			NUnit.Framework.Assert.AreEqual("assigned to wrong attempt", request.GetAttemptID
				(), assigned.GetTaskAttemptID());
			if (checkHostMatch)
			{
				NUnit.Framework.Assert.IsTrue("Not assigned to requested host", Arrays.AsList(request
					.GetHosts()).Contains(assigned.GetContainer().GetNodeId().GetHost()));
			}
		}

		private class MyContainerAllocator : RMContainerAllocator
		{
			internal static readonly IList<TaskAttemptContainerAssignedEvent> events = new AList
				<TaskAttemptContainerAssignedEvent>();

			internal static readonly IList<TaskAttemptKillEvent> taskAttemptKillEvents = new 
				AList<TaskAttemptKillEvent>();

			internal static readonly IList<JobUpdatedNodesEvent> jobUpdatedNodeEvents = new AList
				<JobUpdatedNodesEvent>();

			internal static readonly IList<JobEvent> jobEvents = new AList<JobEvent>();

			private TestRMContainerAllocator.MyResourceManager rm;

			private bool isUnregistered = false;

			private AllocateResponse allocateResponse;

			// Mock RMContainerAllocator
			// Instead of talking to remote Scheduler,uses the local Scheduler
			private static AppContext CreateAppContext(ApplicationAttemptId appAttemptId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				 job)
			{
				AppContext context = Org.Mockito.Mockito.Mock<AppContext>();
				ApplicationId appId = appAttemptId.GetApplicationId();
				Org.Mockito.Mockito.When(context.GetApplicationID()).ThenReturn(appId);
				Org.Mockito.Mockito.When(context.GetApplicationAttemptId()).ThenReturn(appAttemptId
					);
				Org.Mockito.Mockito.When(context.GetJob(Matchers.IsA<JobId>())).ThenReturn(job);
				Org.Mockito.Mockito.When(context.GetClusterInfo()).ThenReturn(new ClusterInfo(Resource
					.NewInstance(10240, 1)));
				Org.Mockito.Mockito.When(context.GetEventHandler()).ThenReturn(new _EventHandler_1626
					());
				// Only capture interesting events.
				return context;
			}

			private sealed class _EventHandler_1626 : EventHandler
			{
				public _EventHandler_1626()
				{
				}

				public void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
				{
					if (@event is TaskAttemptContainerAssignedEvent)
					{
						TestRMContainerAllocator.MyContainerAllocator.events.AddItem((TaskAttemptContainerAssignedEvent
							)@event);
					}
					else
					{
						if (@event is TaskAttemptKillEvent)
						{
							TestRMContainerAllocator.MyContainerAllocator.taskAttemptKillEvents.AddItem((TaskAttemptKillEvent
								)@event);
						}
						else
						{
							if (@event is JobUpdatedNodesEvent)
							{
								TestRMContainerAllocator.MyContainerAllocator.jobUpdatedNodeEvents.AddItem((JobUpdatedNodesEvent
									)@event);
							}
							else
							{
								if (@event is JobEvent)
								{
									TestRMContainerAllocator.MyContainerAllocator.jobEvents.AddItem((JobEvent)@event);
								}
							}
						}
					}
				}
			}

			private static AppContext CreateAppContext(ApplicationAttemptId appAttemptId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				 job, Clock clock)
			{
				AppContext context = CreateAppContext(appAttemptId, job);
				Org.Mockito.Mockito.When(context.GetClock()).ThenReturn(clock);
				return context;
			}

			private static ClientService CreateMockClientService()
			{
				ClientService service = Org.Mockito.Mockito.Mock<ClientService>();
				Org.Mockito.Mockito.When(service.GetBindAddress()).ThenReturn(NetUtils.CreateSocketAddr
					("localhost:4567"));
				Org.Mockito.Mockito.When(service.GetHttpPort()).ThenReturn(890);
				return service;
			}

			internal MyContainerAllocator(TestRMContainerAllocator.MyResourceManager rm, ApplicationAttemptId
				 appAttemptId, AppContext context)
				: base(CreateMockClientService(), context)
			{
				// Use this constructor when using a real job.
				this.rm = rm;
			}

			public MyContainerAllocator(TestRMContainerAllocator.MyResourceManager rm, Configuration
				 conf, ApplicationAttemptId appAttemptId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				 job)
				: base(CreateMockClientService(), CreateAppContext(appAttemptId, job))
			{
				// Use this constructor when you are using a mocked job.
				this.rm = rm;
				base.Init(conf);
				base.Start();
			}

			public MyContainerAllocator(TestRMContainerAllocator.MyResourceManager rm, Configuration
				 conf, ApplicationAttemptId appAttemptId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				 job, Clock clock)
				: base(CreateMockClientService(), CreateAppContext(appAttemptId, job, clock))
			{
				this.rm = rm;
				base.Init(conf);
				base.Start();
			}

			protected internal override ApplicationMasterProtocol CreateSchedulerProxy()
			{
				return this.rm.GetApplicationMasterService();
			}

			protected internal override void Register()
			{
				ApplicationAttemptId attemptId = GetContext().GetApplicationAttemptId();
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = rm.GetRMContext
					().GetRMApps()[attemptId.GetApplicationId()].GetRMAppAttempt(attemptId).GetAMRMToken
					();
				try
				{
					UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
					ugi.AddTokenIdentifier(token.DecodeIdentifier());
				}
				catch (IOException e)
				{
					throw new YarnRuntimeException(e);
				}
				base.Register();
			}

			protected internal override void Unregister()
			{
				isUnregistered = true;
			}

			protected internal override Resource GetMaxContainerCapability()
			{
				return Resource.NewInstance(10240, 1);
			}

			public virtual void SendRequest(ContainerRequestEvent req)
			{
				SendRequests(Arrays.AsList(new ContainerRequestEvent[] { req }));
			}

			public virtual void SendRequests(IList<ContainerRequestEvent> reqs)
			{
				foreach (ContainerRequestEvent req in reqs)
				{
					base.HandleEvent(req);
				}
			}

			public virtual void SendFailure(ContainerFailedEvent f)
			{
				base.HandleEvent(f);
			}

			public virtual void SendDeallocate(ContainerAllocatorEvent f)
			{
				base.HandleEvent(f);
			}

			// API to be used by tests
			/// <exception cref="System.Exception"/>
			public virtual IList<TaskAttemptContainerAssignedEvent> Schedule()
			{
				// before doing heartbeat with RM, drain all the outstanding events to
				// ensure all the requests before this heartbeat is to be handled
				GenericTestUtils.WaitFor(new _Supplier_1737(this), 100, 10000);
				// run the scheduler
				base.Heartbeat();
				IList<TaskAttemptContainerAssignedEvent> result = new AList<TaskAttemptContainerAssignedEvent
					>(events);
				events.Clear();
				return result;
			}

			private sealed class _Supplier_1737 : Supplier<bool>
			{
				public _Supplier_1737(MyContainerAllocator _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public bool Get()
				{
					return this._enclosing.eventQueue.IsEmpty();
				}

				private readonly MyContainerAllocator _enclosing;
			}

			internal static IList<TaskAttemptKillEvent> GetTaskAttemptKillEvents()
			{
				return taskAttemptKillEvents;
			}

			internal static IList<JobUpdatedNodesEvent> GetJobUpdatedNodeEvents()
			{
				return jobUpdatedNodeEvents;
			}

			protected internal override void StartAllocatorThread()
			{
			}

			// override to NOT start thread
			protected internal override bool IsApplicationMasterRegistered()
			{
				return base.IsApplicationMasterRegistered();
			}

			public virtual bool IsUnregistered()
			{
				return isUnregistered;
			}

			public virtual void UpdateSchedulerProxy(TestRMContainerAllocator.MyResourceManager
				 rm)
			{
				scheduler = rm.GetApplicationMasterService();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			protected internal override AllocateResponse MakeRemoteRequest()
			{
				allocateResponse = base.MakeRemoteRequest();
				return allocateResponse;
			}
		}

		private class MyContainerAllocator2 : TestRMContainerAllocator.MyContainerAllocator
		{
			public MyContainerAllocator2(TestRMContainerAllocator.MyResourceManager rm, Configuration
				 conf, ApplicationAttemptId appAttemptId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				 job)
				: base(rm, conf, appAttemptId, job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			protected internal override AllocateResponse MakeRemoteRequest()
			{
				throw new IOException("for testing");
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReduceScheduling()
		{
			int totalMaps = 10;
			int succeededMaps = 1;
			int scheduledMaps = 10;
			int scheduledReduces = 0;
			int assignedMaps = 2;
			int assignedReduces = 0;
			Resource mapResourceReqt = BuilderUtils.NewResource(1024, 1);
			Resource reduceResourceReqt = BuilderUtils.NewResource(2 * 1024, 1);
			int numPendingReduces = 4;
			float maxReduceRampupLimit = 0.5f;
			float reduceSlowStart = 0.2f;
			RMContainerAllocator allocator = Org.Mockito.Mockito.Mock<RMContainerAllocator>();
			Org.Mockito.Mockito.DoCallRealMethod().When(allocator).ScheduleReduces(Matchers.AnyInt
				(), Matchers.AnyInt(), Matchers.AnyInt(), Matchers.AnyInt(), Matchers.AnyInt(), 
				Matchers.AnyInt(), Matchers.Any<Resource>(), Matchers.Any<Resource>(), Matchers.AnyInt
				(), Matchers.AnyFloat(), Matchers.AnyFloat());
			Org.Mockito.Mockito.DoReturn(EnumSet.Of(YarnServiceProtos.SchedulerResourceTypes.
				Memory)).When(allocator).GetSchedulerResourceTypes();
			// Test slow-start
			allocator.ScheduleReduces(totalMaps, succeededMaps, scheduledMaps, scheduledReduces
				, assignedMaps, assignedReduces, mapResourceReqt, reduceResourceReqt, numPendingReduces
				, maxReduceRampupLimit, reduceSlowStart);
			Org.Mockito.Mockito.Verify(allocator, Org.Mockito.Mockito.Never()).SetIsReduceStarted
				(true);
			// verify slow-start still in effect when no more maps need to
			// be scheduled but some have yet to complete
			allocator.ScheduleReduces(totalMaps, succeededMaps, 0, scheduledReduces, totalMaps
				 - succeededMaps, assignedReduces, mapResourceReqt, reduceResourceReqt, numPendingReduces
				, maxReduceRampupLimit, reduceSlowStart);
			Org.Mockito.Mockito.Verify(allocator, Org.Mockito.Mockito.Never()).SetIsReduceStarted
				(true);
			Org.Mockito.Mockito.Verify(allocator, Org.Mockito.Mockito.Never()).ScheduleAllReduces
				();
			succeededMaps = 3;
			Org.Mockito.Mockito.DoReturn(BuilderUtils.NewResource(0, 0)).When(allocator).GetResourceLimit
				();
			allocator.ScheduleReduces(totalMaps, succeededMaps, scheduledMaps, scheduledReduces
				, assignedMaps, assignedReduces, mapResourceReqt, reduceResourceReqt, numPendingReduces
				, maxReduceRampupLimit, reduceSlowStart);
			Org.Mockito.Mockito.Verify(allocator, Org.Mockito.Mockito.Times(1)).SetIsReduceStarted
				(true);
			// Test reduce ramp-up
			Org.Mockito.Mockito.DoReturn(BuilderUtils.NewResource(100 * 1024, 100 * 1)).When(
				allocator).GetResourceLimit();
			allocator.ScheduleReduces(totalMaps, succeededMaps, scheduledMaps, scheduledReduces
				, assignedMaps, assignedReduces, mapResourceReqt, reduceResourceReqt, numPendingReduces
				, maxReduceRampupLimit, reduceSlowStart);
			Org.Mockito.Mockito.Verify(allocator).RampUpReduces(Matchers.AnyInt());
			Org.Mockito.Mockito.Verify(allocator, Org.Mockito.Mockito.Never()).RampDownReduces
				(Matchers.AnyInt());
			// Test reduce ramp-down
			scheduledReduces = 3;
			Org.Mockito.Mockito.DoReturn(BuilderUtils.NewResource(10 * 1024, 10 * 1)).When(allocator
				).GetResourceLimit();
			allocator.ScheduleReduces(totalMaps, succeededMaps, scheduledMaps, scheduledReduces
				, assignedMaps, assignedReduces, mapResourceReqt, reduceResourceReqt, numPendingReduces
				, maxReduceRampupLimit, reduceSlowStart);
			Org.Mockito.Mockito.Verify(allocator).RampDownReduces(Matchers.AnyInt());
			// Test reduce ramp-down for when there are scheduled maps
			// Since we have two scheduled Maps, rampDownReducers 
			// should be invoked twice.
			scheduledMaps = 2;
			assignedReduces = 2;
			Org.Mockito.Mockito.DoReturn(BuilderUtils.NewResource(10 * 1024, 10 * 1)).When(allocator
				).GetResourceLimit();
			allocator.ScheduleReduces(totalMaps, succeededMaps, scheduledMaps, scheduledReduces
				, assignedMaps, assignedReduces, mapResourceReqt, reduceResourceReqt, numPendingReduces
				, maxReduceRampupLimit, reduceSlowStart);
			Org.Mockito.Mockito.Verify(allocator, Org.Mockito.Mockito.Times(2)).RampDownReduces
				(Matchers.AnyInt());
			Org.Mockito.Mockito.DoReturn(EnumSet.Of(YarnServiceProtos.SchedulerResourceTypes.
				Memory, YarnServiceProtos.SchedulerResourceTypes.Cpu)).When(allocator).GetSchedulerResourceTypes
				();
			// Test ramp-down when enough memory but not enough cpu resource
			scheduledMaps = 10;
			assignedReduces = 0;
			Org.Mockito.Mockito.DoReturn(BuilderUtils.NewResource(100 * 1024, 5 * 1)).When(allocator
				).GetResourceLimit();
			allocator.ScheduleReduces(totalMaps, succeededMaps, scheduledMaps, scheduledReduces
				, assignedMaps, assignedReduces, mapResourceReqt, reduceResourceReqt, numPendingReduces
				, maxReduceRampupLimit, reduceSlowStart);
			Org.Mockito.Mockito.Verify(allocator, Org.Mockito.Mockito.Times(3)).RampDownReduces
				(Matchers.AnyInt());
			// Test ramp-down when enough cpu but not enough memory resource
			Org.Mockito.Mockito.DoReturn(BuilderUtils.NewResource(10 * 1024, 100 * 1)).When(allocator
				).GetResourceLimit();
			allocator.ScheduleReduces(totalMaps, succeededMaps, scheduledMaps, scheduledReduces
				, assignedMaps, assignedReduces, mapResourceReqt, reduceResourceReqt, numPendingReduces
				, maxReduceRampupLimit, reduceSlowStart);
			Org.Mockito.Mockito.Verify(allocator, Org.Mockito.Mockito.Times(4)).RampDownReduces
				(Matchers.AnyInt());
		}

		private class RecalculateContainerAllocator : TestRMContainerAllocator.MyContainerAllocator
		{
			public bool recalculatedReduceSchedule = false;

			public RecalculateContainerAllocator(TestRMContainerAllocator.MyResourceManager rm
				, Configuration conf, ApplicationAttemptId appAttemptId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				 job)
				: base(rm, conf, appAttemptId, job)
			{
			}

			public override void ScheduleReduces(int totalMaps, int completedMaps, int scheduledMaps
				, int scheduledReduces, int assignedMaps, int assignedReduces, Resource mapResourceReqt
				, Resource reduceResourceReqt, int numPendingReduces, float maxReduceRampupLimit
				, float reduceSlowStart)
			{
				recalculatedReduceSchedule = true;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCompletedTasksRecalculateSchedule()
		{
			Log.Info("Running testCompletedTasksRecalculateSchedule");
			Configuration conf = new Configuration();
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			// Make a node to register so as to launch the AM.
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(job.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport(
				jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			Org.Mockito.Mockito.DoReturn(10).When(job).GetTotalMaps();
			Org.Mockito.Mockito.DoReturn(10).When(job).GetTotalReduces();
			Org.Mockito.Mockito.DoReturn(0).When(job).GetCompletedMaps();
			TestRMContainerAllocator.RecalculateContainerAllocator allocator = new TestRMContainerAllocator.RecalculateContainerAllocator
				(rm, conf, appAttemptId, job);
			allocator.Schedule();
			allocator.recalculatedReduceSchedule = false;
			allocator.Schedule();
			NUnit.Framework.Assert.IsFalse("Unexpected recalculate of reduce schedule", allocator
				.recalculatedReduceSchedule);
			Org.Mockito.Mockito.DoReturn(1).When(job).GetCompletedMaps();
			allocator.Schedule();
			NUnit.Framework.Assert.IsTrue("Expected recalculate of reduce schedule", allocator
				.recalculatedReduceSchedule);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHeartbeatHandler()
		{
			Log.Info("Running testHeartbeatHandler");
			Configuration conf = new Configuration();
			conf.SetInt(MRJobConfig.MrAmToRmHeartbeatIntervalMs, 1);
			ControlledClock clock = new ControlledClock(new SystemClock());
			AppContext appContext = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Mockito.Mockito.When(appContext.GetClock()).ThenReturn(clock);
			Org.Mockito.Mockito.When(appContext.GetApplicationID()).ThenReturn(ApplicationId.
				NewInstance(1, 1));
			RMContainerAllocator allocator = new _RMContainerAllocator_1995(Org.Mockito.Mockito.Mock
				<ClientService>(), appContext);
			allocator.Init(conf);
			allocator.Start();
			clock.SetTime(5);
			int timeToWaitMs = 5000;
			while (allocator.GetLastHeartbeatTime() != 5 && timeToWaitMs > 0)
			{
				Sharpen.Thread.Sleep(10);
				timeToWaitMs -= 10;
			}
			NUnit.Framework.Assert.AreEqual(5, allocator.GetLastHeartbeatTime());
			clock.SetTime(7);
			timeToWaitMs = 5000;
			while (allocator.GetLastHeartbeatTime() != 7 && timeToWaitMs > 0)
			{
				Sharpen.Thread.Sleep(10);
				timeToWaitMs -= 10;
			}
			NUnit.Framework.Assert.AreEqual(7, allocator.GetLastHeartbeatTime());
			AtomicBoolean callbackCalled = new AtomicBoolean(false);
			allocator.RunOnNextHeartbeat(new _Runnable_2026(callbackCalled));
			clock.SetTime(8);
			timeToWaitMs = 5000;
			while (allocator.GetLastHeartbeatTime() != 8 && timeToWaitMs > 0)
			{
				Sharpen.Thread.Sleep(10);
				timeToWaitMs -= 10;
			}
			NUnit.Framework.Assert.AreEqual(8, allocator.GetLastHeartbeatTime());
			NUnit.Framework.Assert.IsTrue(callbackCalled.Get());
		}

		private sealed class _RMContainerAllocator_1995 : RMContainerAllocator
		{
			public _RMContainerAllocator_1995(ClientService baseArg1, AppContext baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			protected internal override void Register()
			{
			}

			protected internal override ApplicationMasterProtocol CreateSchedulerProxy()
			{
				return Org.Mockito.Mockito.Mock<ApplicationMasterProtocol>();
			}

			/// <exception cref="System.Exception"/>
			protected internal override void Heartbeat()
			{
				lock (this)
				{
				}
			}
		}

		private sealed class _Runnable_2026 : Runnable
		{
			public _Runnable_2026(AtomicBoolean callbackCalled)
			{
				this.callbackCalled = callbackCalled;
			}

			public void Run()
			{
				callbackCalled.Set(true);
			}

			private readonly AtomicBoolean callbackCalled;
		}

		[NUnit.Framework.Test]
		public virtual void TestCompletedContainerEvent()
		{
			RMContainerAllocator allocator = new RMContainerAllocator(Org.Mockito.Mockito.Mock
				<ClientService>(), Org.Mockito.Mockito.Mock<AppContext>());
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(MRBuilderUtils.NewTaskId
				(MRBuilderUtils.NewJobId(1, 1, 1), 1, TaskType.Map), 1);
			ApplicationId applicationId = ApplicationId.NewInstance(1, 1);
			ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 1);
			ContainerId containerId = ContainerId.NewContainerId(applicationAttemptId, 1);
			ContainerStatus status = ContainerStatus.NewInstance(containerId, ContainerState.
				Running, string.Empty, 0);
			ContainerStatus abortedStatus = ContainerStatus.NewInstance(containerId, ContainerState
				.Running, string.Empty, ContainerExitStatus.Aborted);
			TaskAttemptEvent @event = allocator.CreateContainerFinishedEvent(status, attemptId
				);
			NUnit.Framework.Assert.AreEqual(TaskAttemptEventType.TaContainerCompleted, @event
				.GetType());
			TaskAttemptEvent abortedEvent = allocator.CreateContainerFinishedEvent(abortedStatus
				, attemptId);
			NUnit.Framework.Assert.AreEqual(TaskAttemptEventType.TaKill, abortedEvent.GetType
				());
			ContainerId containerId2 = ContainerId.NewContainerId(applicationAttemptId, 2);
			ContainerStatus status2 = ContainerStatus.NewInstance(containerId2, ContainerState
				.Running, string.Empty, 0);
			ContainerStatus preemptedStatus = ContainerStatus.NewInstance(containerId2, ContainerState
				.Running, string.Empty, ContainerExitStatus.Preempted);
			TaskAttemptEvent event2 = allocator.CreateContainerFinishedEvent(status2, attemptId
				);
			NUnit.Framework.Assert.AreEqual(TaskAttemptEventType.TaContainerCompleted, event2
				.GetType());
			TaskAttemptEvent abortedEvent2 = allocator.CreateContainerFinishedEvent(preemptedStatus
				, attemptId);
			NUnit.Framework.Assert.AreEqual(TaskAttemptEventType.TaKill, abortedEvent2.GetType
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnregistrationOnlyIfRegistered()
		{
			Configuration conf = new Configuration();
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher rmDispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp rmApp = rm.SubmitApp(1024);
			rmDispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 11264);
			amNodeManager.NodeHeartbeat(true);
			rmDispatcher.Await();
			ApplicationAttemptId appAttemptId = rmApp.GetCurrentAppAttempt().GetAppAttemptId(
				);
			rm.SendAMLaunched(appAttemptId);
			rmDispatcher.Await();
			MRApp mrApp = new _MRApp_2110(rm, appAttemptId, appAttemptId, ContainerId.NewContainerId
				(appAttemptId, 0), 10, 0, false, this.GetType().FullName, true, 1);
			mrApp.Submit(conf);
			DrainDispatcher amDispatcher = (DrainDispatcher)mrApp.GetDispatcher();
			TestRMContainerAllocator.MyContainerAllocator allocator = (TestRMContainerAllocator.MyContainerAllocator
				)mrApp.GetContainerAllocator();
			amDispatcher.Await();
			NUnit.Framework.Assert.IsTrue(allocator.IsApplicationMasterRegistered());
			mrApp.Stop();
			NUnit.Framework.Assert.IsTrue(allocator.IsUnregistered());
		}

		private sealed class _MRApp_2110 : MRApp
		{
			public _MRApp_2110(TestRMContainerAllocator.MyResourceManager rm, ApplicationAttemptId
				 appAttemptId, ApplicationAttemptId baseArg1, ContainerId baseArg2, int baseArg3
				, int baseArg4, bool baseArg5, string baseArg6, bool baseArg7, int baseArg8)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7, baseArg8
					)
			{
				this.rm = rm;
				this.appAttemptId = appAttemptId;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return new DrainDispatcher();
			}

			protected internal override ContainerAllocator CreateContainerAllocator(ClientService
				 clientService, AppContext context)
			{
				return new TestRMContainerAllocator.MyContainerAllocator(rm, appAttemptId, context
					);
			}

			private readonly TestRMContainerAllocator.MyResourceManager rm;

			private readonly ApplicationAttemptId appAttemptId;
		}

		// Step-1 : AM send allocate request for 2 ContainerRequests and 1
		// blackListeNode
		// Step-2 : 2 containers are allocated by RM.
		// Step-3 : AM Send 1 containerRequest(event3) and 1 releaseRequests to
		// RM
		// Step-4 : On RM restart, AM(does not know RM is restarted) sends
		// additional containerRequest(event4) and blacklisted nodes.
		// Intern RM send resync command
		// Step-5 : On Resync,AM sends all outstanding
		// asks,release,blacklistAaddition
		// and another containerRequest(event5)
		// Step-6 : RM allocates containers i.e event3,event4 and cRequest5
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMContainerAllocatorResendsRequestsOnRMRestart()
		{
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.RecoveryEnabled, "true");
			conf.Set(YarnConfiguration.RmStore, typeof(MemoryRMStateStore).FullName);
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
			conf.SetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled, true);
			conf.SetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs, 0);
			conf.SetBoolean(MRJobConfig.MrAmJobNodeBlacklistingEnable, true);
			conf.SetInt(MRJobConfig.MaxTaskFailuresPerTracker, 1);
			conf.SetInt(MRJobConfig.MrAmIgnoreBlacklistingBlacklistedNodePerecent, -1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			TestRMContainerAllocator.MyResourceManager rm1 = new TestRMContainerAllocator.MyResourceManager
				(conf, memStore);
			rm1.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm1.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm1.SubmitApp(1024);
			dispatcher.Await();
			MockNM nm1 = new MockNM("h1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			nm1.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm1.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm1, conf, appAttemptId, mockJob);
			// Step-1 : AM send allocate request for 2 ContainerRequests and 1
			// blackListeNode
			// create the container request
			// send MAP request
			ContainerRequestEvent event1 = CreateReq(jobId, 1, 1024, new string[] { "h1" });
			allocator.SendRequest(event1);
			ContainerRequestEvent event2 = CreateReq(jobId, 2, 2048, new string[] { "h1", "h2"
				 });
			allocator.SendRequest(event2);
			// Send events to blacklist h2
			ContainerFailedEvent f1 = CreateFailEvent(jobId, 1, "h2", false);
			allocator.SendFailure(f1);
			// send allocate request and 1 blacklisted nodes
			IList<TaskAttemptContainerAssignedEvent> assignedContainers = allocator.Schedule(
				);
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assignedContainers
				.Count);
			// Why ask is 3, not 4? --> ask from blacklisted node h2 is removed
			AssertAsksAndReleases(3, 0, rm1);
			AssertBlacklistAdditionsAndRemovals(1, 0, rm1);
			nm1.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			// Step-2 : 2 containers are allocated by RM.
			assignedContainers = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 2", 2, assignedContainers
				.Count);
			AssertAsksAndReleases(0, 0, rm1);
			AssertBlacklistAdditionsAndRemovals(0, 0, rm1);
			assignedContainers = allocator.Schedule();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assignedContainers
				.Count);
			AssertAsksAndReleases(3, 0, rm1);
			AssertBlacklistAdditionsAndRemovals(0, 0, rm1);
			// Step-3 : AM Send 1 containerRequest(event3) and 1 releaseRequests to
			// RM
			// send container request
			ContainerRequestEvent event3 = CreateReq(jobId, 3, 1000, new string[] { "h1" });
			allocator.SendRequest(event3);
			// send deallocate request
			ContainerAllocatorEvent deallocate1 = CreateDeallocateEvent(jobId, 1, false);
			allocator.SendDeallocate(deallocate1);
			assignedContainers = allocator.Schedule();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, assignedContainers
				.Count);
			AssertAsksAndReleases(3, 1, rm1);
			AssertBlacklistAdditionsAndRemovals(0, 0, rm1);
			// Phase-2 start 2nd RM is up
			TestRMContainerAllocator.MyResourceManager rm2 = new TestRMContainerAllocator.MyResourceManager
				(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			allocator.UpdateSchedulerProxy(rm2);
			dispatcher = (DrainDispatcher)rm2.GetRMContext().GetDispatcher();
			// NM should be rebooted on heartbeat, even first heartbeat for nm2
			NodeHeartbeatResponse hbResponse = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(NodeAction.Resync, hbResponse.GetNodeAction());
			// new NM to represent NM re-register
			nm1 = new MockNM("h1:1234", 10240, rm2.GetResourceTrackerService());
			nm1.RegisterNode();
			nm1.NodeHeartbeat(true);
			dispatcher.Await();
			// Step-4 : On RM restart, AM(does not know RM is restarted) sends
			// additional containerRequest(event4) and blacklisted nodes.
			// Intern RM send resync command
			// send deallocate request, release=1
			ContainerAllocatorEvent deallocate2 = CreateDeallocateEvent(jobId, 2, false);
			allocator.SendDeallocate(deallocate2);
			// Send events to blacklist nodes h3
			ContainerFailedEvent f2 = CreateFailEvent(jobId, 1, "h3", false);
			allocator.SendFailure(f2);
			ContainerRequestEvent event4 = CreateReq(jobId, 4, 2000, new string[] { "h1", "h2"
				 });
			allocator.SendRequest(event4);
			// send allocate request to 2nd RM and get resync command
			allocator.Schedule();
			dispatcher.Await();
			// Step-5 : On Resync,AM sends all outstanding
			// asks,release,blacklistAaddition
			// and another containerRequest(event5)
			ContainerRequestEvent event5 = CreateReq(jobId, 5, 3000, new string[] { "h1", "h2"
				, "h3" });
			allocator.SendRequest(event5);
			// send all outstanding request again.
			assignedContainers = allocator.Schedule();
			dispatcher.Await();
			AssertAsksAndReleases(3, 2, rm2);
			AssertBlacklistAdditionsAndRemovals(2, 0, rm2);
			nm1.NodeHeartbeat(true);
			dispatcher.Await();
			// Step-6 : RM allocates containers i.e event3,event4 and cRequest5
			assignedContainers = allocator.Schedule();
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("Number of container should be 3", 3, assignedContainers
				.Count);
			foreach (TaskAttemptContainerAssignedEvent assig in assignedContainers)
			{
				NUnit.Framework.Assert.IsTrue("Assigned count not correct", "h1".Equals(assig.GetContainer
					().GetNodeId().GetHost()));
			}
			rm1.Stop();
			rm2.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMUnavailable()
		{
			Configuration conf = new Configuration();
			conf.SetInt(MRJobConfig.MrAmToRmWaitIntervalMs, 0);
			TestRMContainerAllocator.MyResourceManager rm1 = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm1.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm1.GetRMContext().GetDispatcher();
			RMApp app = rm1.SubmitApp(1024);
			dispatcher.Await();
			MockNM nm1 = new MockNM("h1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			nm1.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm1.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator2 allocator = new TestRMContainerAllocator.MyContainerAllocator2
				(rm1, conf, appAttemptId, mockJob);
			allocator.jobEvents.Clear();
			try
			{
				allocator.Schedule();
				NUnit.Framework.Assert.Fail("Should Have Exception");
			}
			catch (RMContainerAllocationException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Could not contact RM after"));
			}
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("Should Have 1 Job Event", 1, allocator.jobEvents
				.Count);
			JobEvent @event = allocator.jobEvents[0];
			NUnit.Framework.Assert.IsTrue("Should Reboot", @event.GetType().Equals(JobEventType
				.JobAmReboot));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMRMTokenUpdate()
		{
			Log.Info("Running testAMRMTokenUpdate");
			string rmAddr = "somermaddress:1234";
			Configuration conf = new YarnConfiguration();
			conf.SetLong(YarnConfiguration.RmAmrmTokenMasterKeyRollingIntervalSecs, 8);
			conf.SetLong(YarnConfiguration.RmAmExpiryIntervalMs, 2000);
			conf.Set(YarnConfiguration.RmSchedulerAddress, rmAddr);
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			AMRMTokenSecretManager secretMgr = rm.GetRMContext().GetAMRMTokenSecretManager();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			ApplicationId appId = app.GetApplicationId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> oldToken = rm.GetRMContext
				().GetRMApps()[appId].GetRMAppAttempt(appAttemptId).GetAMRMToken();
			NUnit.Framework.Assert.IsNotNull("app should have a token", oldToken);
			UserGroupInformation testUgi = UserGroupInformation.CreateUserForTesting("someuser"
				, new string[0]);
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> newToken = testUgi.DoAs
				(new _PrivilegedExceptionAction_2410(rm, conf, appAttemptId, mockJob, oldToken, 
				appId));
			// Keep heartbeating until RM thinks the token has been updated
			// verify there is only one AMRM token in the UGI and it matches the
			// updated token from the RM
			int tokenCount = 0;
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> ugiToken = null;
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in testUgi
				.GetTokens())
			{
				if (AMRMTokenIdentifier.KindName.Equals(token.GetKind()))
				{
					ugiToken = token;
					++tokenCount;
				}
			}
			NUnit.Framework.Assert.AreEqual("too many AMRM tokens", 1, tokenCount);
			Assert.AssertArrayEquals("token identifier not updated", newToken.GetIdentifier()
				, ugiToken.GetIdentifier());
			Assert.AssertArrayEquals("token password not updated", newToken.GetPassword(), ugiToken
				.GetPassword());
			NUnit.Framework.Assert.AreEqual("AMRM token service not updated", new Text(rmAddr
				), ugiToken.GetService());
		}

		private sealed class _PrivilegedExceptionAction_2410 : PrivilegedExceptionAction<
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier>>
		{
			public _PrivilegedExceptionAction_2410(TestRMContainerAllocator.MyResourceManager
				 rm, Configuration conf, ApplicationAttemptId appAttemptId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				 mockJob, Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> oldToken, 
				ApplicationId appId)
			{
				this.rm = rm;
				this.conf = conf;
				this.appAttemptId = appAttemptId;
				this.mockJob = mockJob;
				this.oldToken = oldToken;
				this.appId = appId;
			}

			/// <exception cref="System.Exception"/>
			public Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> Run()
			{
				TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
					(rm, conf, appAttemptId, mockJob);
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> currentToken = oldToken;
				long startTime = Time.MonotonicNow();
				while (currentToken == oldToken)
				{
					if (Time.MonotonicNow() - startTime > 20000)
					{
						NUnit.Framework.Assert.Fail("Took to long to see AMRM token change");
					}
					Sharpen.Thread.Sleep(100);
					allocator.Schedule();
					currentToken = rm.GetRMContext().GetRMApps()[appId].GetRMAppAttempt(appAttemptId)
						.GetAMRMToken();
				}
				return currentToken;
			}

			private readonly TestRMContainerAllocator.MyResourceManager rm;

			private readonly Configuration conf;

			private readonly ApplicationAttemptId appAttemptId;

			private readonly Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob;

			private readonly Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> oldToken;

			private readonly ApplicationId appId;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConcurrentTaskLimits()
		{
			int MapLimit = 3;
			int ReduceLimit = 1;
			Log.Info("Running testConcurrentTaskLimits");
			Configuration conf = new Configuration();
			conf.SetInt(MRJobConfig.JobRunningMapLimit, MapLimit);
			conf.SetInt(MRJobConfig.JobRunningReduceLimit, ReduceLimit);
			conf.SetFloat(MRJobConfig.CompletedMapsForReduceSlowstart, 1.0f);
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MockScheduler mockScheduler = new TestRMContainerAllocator.MockScheduler
				(appAttemptId);
			TestRMContainerAllocator.MyContainerAllocator allocator = new _MyContainerAllocator_2472
				(mockScheduler, null, conf, appAttemptId, mockJob);
			// create some map requests
			ContainerRequestEvent[] reqMapEvents = new ContainerRequestEvent[5];
			for (int i = 0; i < reqMapEvents.Length; ++i)
			{
				reqMapEvents[i] = CreateReq(jobId, i, 1024, new string[] { "h" + i });
			}
			allocator.SendRequests(Arrays.AsList(reqMapEvents));
			// create some reduce requests
			ContainerRequestEvent[] reqReduceEvents = new ContainerRequestEvent[2];
			for (int i_1 = 0; i_1 < reqReduceEvents.Length; ++i_1)
			{
				reqReduceEvents[i_1] = CreateReq(jobId, i_1, 1024, new string[] {  }, false, true
					);
			}
			allocator.SendRequests(Arrays.AsList(reqReduceEvents));
			allocator.Schedule();
			// verify all of the host-specific asks were sent plus one for the
			// default rack and one for the ANY request
			NUnit.Framework.Assert.AreEqual(reqMapEvents.Length + 2, mockScheduler.lastAsk.Count
				);
			// verify AM is only asking for the map limit overall
			NUnit.Framework.Assert.AreEqual(MapLimit, mockScheduler.lastAnyAskMap);
			// assign a map task and verify we do not ask for any more maps
			ContainerId cid0 = mockScheduler.AssignContainer("h0", false);
			allocator.Schedule();
			allocator.Schedule();
			NUnit.Framework.Assert.AreEqual(2, mockScheduler.lastAnyAskMap);
			// complete the map task and verify that we ask for one more
			mockScheduler.CompleteContainer(cid0);
			allocator.Schedule();
			allocator.Schedule();
			NUnit.Framework.Assert.AreEqual(3, mockScheduler.lastAnyAskMap);
			// assign three more maps and verify we ask for no more maps
			ContainerId cid1 = mockScheduler.AssignContainer("h1", false);
			ContainerId cid2 = mockScheduler.AssignContainer("h2", false);
			ContainerId cid3 = mockScheduler.AssignContainer("h3", false);
			allocator.Schedule();
			allocator.Schedule();
			NUnit.Framework.Assert.AreEqual(0, mockScheduler.lastAnyAskMap);
			// complete two containers and verify we only asked for one more
			// since at that point all maps should be scheduled/completed
			mockScheduler.CompleteContainer(cid2);
			mockScheduler.CompleteContainer(cid3);
			allocator.Schedule();
			allocator.Schedule();
			NUnit.Framework.Assert.AreEqual(1, mockScheduler.lastAnyAskMap);
			// allocate the last container and complete the first one
			// and verify there are no more map asks.
			mockScheduler.CompleteContainer(cid1);
			ContainerId cid4 = mockScheduler.AssignContainer("h4", false);
			allocator.Schedule();
			allocator.Schedule();
			NUnit.Framework.Assert.AreEqual(0, mockScheduler.lastAnyAskMap);
			// complete the last map
			mockScheduler.CompleteContainer(cid4);
			allocator.Schedule();
			allocator.Schedule();
			NUnit.Framework.Assert.AreEqual(0, mockScheduler.lastAnyAskMap);
			// verify only reduce limit being requested
			NUnit.Framework.Assert.AreEqual(ReduceLimit, mockScheduler.lastAnyAskReduce);
			// assign a reducer and verify ask goes to zero
			cid0 = mockScheduler.AssignContainer("h0", true);
			allocator.Schedule();
			allocator.Schedule();
			NUnit.Framework.Assert.AreEqual(0, mockScheduler.lastAnyAskReduce);
			// complete the reducer and verify we ask for another
			mockScheduler.CompleteContainer(cid0);
			allocator.Schedule();
			allocator.Schedule();
			NUnit.Framework.Assert.AreEqual(1, mockScheduler.lastAnyAskReduce);
			// assign a reducer and verify ask goes to zero
			cid0 = mockScheduler.AssignContainer("h0", true);
			allocator.Schedule();
			allocator.Schedule();
			NUnit.Framework.Assert.AreEqual(0, mockScheduler.lastAnyAskReduce);
			// complete the reducer and verify no more reducers
			mockScheduler.CompleteContainer(cid0);
			allocator.Schedule();
			allocator.Schedule();
			NUnit.Framework.Assert.AreEqual(0, mockScheduler.lastAnyAskReduce);
			allocator.Close();
		}

		private sealed class _MyContainerAllocator_2472 : TestRMContainerAllocator.MyContainerAllocator
		{
			public _MyContainerAllocator_2472(TestRMContainerAllocator.MockScheduler mockScheduler
				, TestRMContainerAllocator.MyResourceManager baseArg1, Configuration baseArg2, ApplicationAttemptId
				 baseArg3, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job baseArg4)
				: base(baseArg1, baseArg2, baseArg3, baseArg4)
			{
				this.mockScheduler = mockScheduler;
			}

			protected internal override void Register()
			{
			}

			protected internal override ApplicationMasterProtocol CreateSchedulerProxy()
			{
				return mockScheduler;
			}

			private readonly TestRMContainerAllocator.MockScheduler mockScheduler;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAttemptNotFoundCausesRMCommunicatorException()
		{
			Configuration conf = new Configuration();
			TestRMContainerAllocator.MyResourceManager rm = new TestRMContainerAllocator.MyResourceManager
				(conf);
			rm.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm.SubmitApp(1024);
			dispatcher.Await();
			MockNM amNodeManager = rm.RegisterNode("amNM:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			JobId jobId = MRBuilderUtils.NewJobId(appAttemptId.GetApplicationId(), 0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetReport()).ThenReturn(MRBuilderUtils.NewJobReport
				(jobId, "job", "user", JobState.Running, 0, 0, 0, 0, 0, 0, 0, "jobfile", null, false
				, string.Empty));
			TestRMContainerAllocator.MyContainerAllocator allocator = new TestRMContainerAllocator.MyContainerAllocator
				(rm, conf, appAttemptId, mockJob);
			// Now kill the application
			rm.KillApp(app.GetApplicationId());
			allocator.Schedule();
		}

		private class MockScheduler : ApplicationMasterProtocol
		{
			internal ApplicationAttemptId attemptId;

			internal long nextContainerId = 10;

			internal IList<ResourceRequest> lastAsk = null;

			internal int lastAnyAskMap = 0;

			internal int lastAnyAskReduce = 0;

			internal IList<ContainerStatus> containersToComplete = new AList<ContainerStatus>
				();

			internal IList<Container> containersToAllocate = new AList<Container>();

			public MockScheduler(ApplicationAttemptId attemptId)
			{
				this.attemptId = attemptId;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterApplicationMasterResponse RegisterApplicationMaster(RegisterApplicationMasterRequest
				 request)
			{
				return RegisterApplicationMasterResponse.NewInstance(Resource.NewInstance(512, 1)
					, Resource.NewInstance(512000, 1024), Sharpen.Collections.EmptyMap<ApplicationAccessType
					, string>(), ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString("fake_key")), Sharpen.Collections
					.EmptyList<Container>(), "default", Sharpen.Collections.EmptyList<NMToken>());
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual FinishApplicationMasterResponse FinishApplicationMaster(FinishApplicationMasterRequest
				 request)
			{
				return FinishApplicationMasterResponse.NewInstance(false);
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual AllocateResponse Allocate(AllocateRequest request)
			{
				lastAsk = request.GetAskList();
				foreach (ResourceRequest req in lastAsk)
				{
					if (ResourceRequest.Any.Equals(req.GetResourceName()))
					{
						Priority priority = req.GetPriority();
						if (priority.Equals(RMContainerAllocator.PriorityMap))
						{
							lastAnyAskMap = req.GetNumContainers();
						}
						else
						{
							if (priority.Equals(RMContainerAllocator.PriorityReduce))
							{
								lastAnyAskReduce = req.GetNumContainers();
							}
						}
					}
				}
				AllocateResponse response = AllocateResponse.NewInstance(request.GetResponseId(), 
					containersToComplete, containersToAllocate, Sharpen.Collections.EmptyList<NodeReport
					>(), Resource.NewInstance(512000, 1024), null, 10, null, Sharpen.Collections.EmptyList
					<NMToken>());
				containersToComplete.Clear();
				containersToAllocate.Clear();
				return response;
			}

			public virtual ContainerId AssignContainer(string nodeName, bool isReduce)
			{
				ContainerId containerId = ContainerId.NewContainerId(attemptId, nextContainerId++
					);
				Priority priority = isReduce ? RMContainerAllocator.PriorityReduce : RMContainerAllocator
					.PriorityMap;
				Container container = Container.NewInstance(containerId, NodeId.NewInstance(nodeName
					, 1234), nodeName + ":5678", Resource.NewInstance(1024, 1), priority, null);
				containersToAllocate.AddItem(container);
				return containerId;
			}

			public virtual void CompleteContainer(ContainerId containerId)
			{
				containersToComplete.AddItem(ContainerStatus.NewInstance(containerId, ContainerState
					.Complete, string.Empty, 0));
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestRMContainerAllocator t = new TestRMContainerAllocator();
			t.TestSimple();
			t.TestResource();
			t.TestMapReduceScheduling();
			t.TestReportedAppProgress();
			t.TestReportedAppProgressWithOnlyMaps();
			t.TestBlackListedNodes();
			t.TestCompletedTasksRecalculateSchedule();
			t.TestAMRMTokenUpdate();
		}
	}
}
