using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs
{
	public class TestRMApplicationHistoryWriter
	{
		private static int MaxRetries = 10;

		private RMApplicationHistoryWriter writer;

		private ApplicationHistoryStore store;

		private IList<TestRMApplicationHistoryWriter.CounterDispatcher> dispatchers = new 
			AList<TestRMApplicationHistoryWriter.CounterDispatcher>();

		[SetUp]
		public virtual void Setup()
		{
			store = new MemoryApplicationHistoryStore();
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.ApplicationHistoryEnabled, true);
			conf.SetClass(YarnConfiguration.ApplicationHistoryStore, typeof(MemoryApplicationHistoryStore
				), typeof(ApplicationHistoryStore));
			writer = new _RMApplicationHistoryWriter_86(this);
			writer.Init(conf);
			writer.Start();
		}

		private sealed class _RMApplicationHistoryWriter_86 : RMApplicationHistoryWriter
		{
			public _RMApplicationHistoryWriter_86(TestRMApplicationHistoryWriter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected internal override ApplicationHistoryStore CreateApplicationHistoryStore
				(Configuration conf)
			{
				return this._enclosing.store;
			}

			protected internal override Dispatcher CreateDispatcher(Configuration conf)
			{
				_T2081512330 dispatcher = new _T2081512330(this, conf.GetInt(YarnConfiguration.RmHistoryWriterMultiThreadedDispatcherPoolSize
					, YarnConfiguration.DefaultRmHistoryWriterMultiThreadedDispatcherPoolSize));
				dispatcher.SetDrainEventsOnStop();
				return dispatcher;
			}

			internal class _T2081512330 : RMApplicationHistoryWriter.MultiThreadedDispatcher
			{
				public _T2081512330(_RMApplicationHistoryWriter_86 _enclosing, int num)
					: base(num)
				{
					this._enclosing = _enclosing;
				}

				protected internal override AsyncDispatcher CreateDispatcher()
				{
					TestRMApplicationHistoryWriter.CounterDispatcher dispatcher = new TestRMApplicationHistoryWriter.CounterDispatcher
						();
					this._enclosing._enclosing.dispatchers.AddItem(dispatcher);
					return dispatcher;
				}

				private readonly _RMApplicationHistoryWriter_86 _enclosing;
			}

			private readonly TestRMApplicationHistoryWriter _enclosing;
		}

		[TearDown]
		public virtual void TearDown()
		{
			writer.Stop();
		}

		private static RMApp CreateRMApp(ApplicationId appId)
		{
			RMApp app = Org.Mockito.Mockito.Mock<RMApp>();
			Org.Mockito.Mockito.When(app.GetApplicationId()).ThenReturn(appId);
			Org.Mockito.Mockito.When(app.GetName()).ThenReturn("test app");
			Org.Mockito.Mockito.When(app.GetApplicationType()).ThenReturn("test app type");
			Org.Mockito.Mockito.When(app.GetUser()).ThenReturn("test user");
			Org.Mockito.Mockito.When(app.GetQueue()).ThenReturn("test queue");
			Org.Mockito.Mockito.When(app.GetSubmitTime()).ThenReturn(0L);
			Org.Mockito.Mockito.When(app.GetStartTime()).ThenReturn(1L);
			Org.Mockito.Mockito.When(app.GetFinishTime()).ThenReturn(2L);
			Org.Mockito.Mockito.When(app.GetDiagnostics()).ThenReturn(new StringBuilder("test diagnostics info"
				));
			Org.Mockito.Mockito.When(app.GetFinalApplicationStatus()).ThenReturn(FinalApplicationStatus
				.Undefined);
			return app;
		}

		private static RMAppAttempt CreateRMAppAttempt(ApplicationAttemptId appAttemptId)
		{
			RMAppAttempt appAttempt = Org.Mockito.Mockito.Mock<RMAppAttempt>();
			Org.Mockito.Mockito.When(appAttempt.GetAppAttemptId()).ThenReturn(appAttemptId);
			Org.Mockito.Mockito.When(appAttempt.GetHost()).ThenReturn("test host");
			Org.Mockito.Mockito.When(appAttempt.GetRpcPort()).ThenReturn(-100);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(ContainerId.NewContainerId
				(appAttemptId, 1));
			Org.Mockito.Mockito.When(appAttempt.GetMasterContainer()).ThenReturn(container);
			Org.Mockito.Mockito.When(appAttempt.GetDiagnostics()).ThenReturn("test diagnostics info"
				);
			Org.Mockito.Mockito.When(appAttempt.GetTrackingUrl()).ThenReturn("test url");
			Org.Mockito.Mockito.When(appAttempt.GetFinalApplicationStatus()).ThenReturn(FinalApplicationStatus
				.Undefined);
			return appAttempt;
		}

		private static RMContainer CreateRMContainer(ContainerId containerId)
		{
			RMContainer container = Org.Mockito.Mockito.Mock<RMContainer>();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(containerId);
			Org.Mockito.Mockito.When(container.GetAllocatedNode()).ThenReturn(NodeId.NewInstance
				("test host", -100));
			Org.Mockito.Mockito.When(container.GetAllocatedResource()).ThenReturn(Resource.NewInstance
				(-1, -1));
			Org.Mockito.Mockito.When(container.GetAllocatedPriority()).ThenReturn(Priority.Undefined
				);
			Org.Mockito.Mockito.When(container.GetCreationTime()).ThenReturn(0L);
			Org.Mockito.Mockito.When(container.GetFinishTime()).ThenReturn(1L);
			Org.Mockito.Mockito.When(container.GetDiagnosticsInfo()).ThenReturn("test diagnostics info"
				);
			Org.Mockito.Mockito.When(container.GetLogURL()).ThenReturn("test log url");
			Org.Mockito.Mockito.When(container.GetContainerExitStatus()).ThenReturn(-1);
			Org.Mockito.Mockito.When(container.GetContainerState()).ThenReturn(ContainerState
				.Complete);
			return container;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultStoreSetup()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.ApplicationHistoryEnabled, true);
			RMApplicationHistoryWriter writer = new RMApplicationHistoryWriter();
			writer.Init(conf);
			writer.Start();
			try
			{
				NUnit.Framework.Assert.IsFalse(writer.historyServiceEnabled);
				NUnit.Framework.Assert.IsNull(writer.writer);
			}
			finally
			{
				writer.Stop();
				writer.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteApplication()
		{
			RMApp app = CreateRMApp(ApplicationId.NewInstance(0, 1));
			writer.ApplicationStarted(app);
			ApplicationHistoryData appHD = null;
			for (int i = 0; i < MaxRetries; ++i)
			{
				appHD = store.GetApplication(ApplicationId.NewInstance(0, 1));
				if (appHD != null)
				{
					break;
				}
				else
				{
					Sharpen.Thread.Sleep(100);
				}
			}
			NUnit.Framework.Assert.IsNotNull(appHD);
			NUnit.Framework.Assert.AreEqual("test app", appHD.GetApplicationName());
			NUnit.Framework.Assert.AreEqual("test app type", appHD.GetApplicationType());
			NUnit.Framework.Assert.AreEqual("test user", appHD.GetUser());
			NUnit.Framework.Assert.AreEqual("test queue", appHD.GetQueue());
			NUnit.Framework.Assert.AreEqual(0L, appHD.GetSubmitTime());
			NUnit.Framework.Assert.AreEqual(1L, appHD.GetStartTime());
			writer.ApplicationFinished(app, RMAppState.Finished);
			for (int i_1 = 0; i_1 < MaxRetries; ++i_1)
			{
				appHD = store.GetApplication(ApplicationId.NewInstance(0, 1));
				if (appHD.GetYarnApplicationState() != null)
				{
					break;
				}
				else
				{
					Sharpen.Thread.Sleep(100);
				}
			}
			NUnit.Framework.Assert.AreEqual(2L, appHD.GetFinishTime());
			NUnit.Framework.Assert.AreEqual("test diagnostics info", appHD.GetDiagnosticsInfo
				());
			NUnit.Framework.Assert.AreEqual(FinalApplicationStatus.Undefined, appHD.GetFinalApplicationStatus
				());
			NUnit.Framework.Assert.AreEqual(YarnApplicationState.Finished, appHD.GetYarnApplicationState
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteApplicationAttempt()
		{
			RMAppAttempt appAttempt = CreateRMAppAttempt(ApplicationAttemptId.NewInstance(ApplicationId
				.NewInstance(0, 1), 1));
			writer.ApplicationAttemptStarted(appAttempt);
			ApplicationAttemptHistoryData appAttemptHD = null;
			for (int i = 0; i < MaxRetries; ++i)
			{
				appAttemptHD = store.GetApplicationAttempt(ApplicationAttemptId.NewInstance(ApplicationId
					.NewInstance(0, 1), 1));
				if (appAttemptHD != null)
				{
					break;
				}
				else
				{
					Sharpen.Thread.Sleep(100);
				}
			}
			NUnit.Framework.Assert.IsNotNull(appAttemptHD);
			NUnit.Framework.Assert.AreEqual("test host", appAttemptHD.GetHost());
			NUnit.Framework.Assert.AreEqual(-100, appAttemptHD.GetRPCPort());
			NUnit.Framework.Assert.AreEqual(ContainerId.NewContainerId(ApplicationAttemptId.NewInstance
				(ApplicationId.NewInstance(0, 1), 1), 1), appAttemptHD.GetMasterContainerId());
			writer.ApplicationAttemptFinished(appAttempt, RMAppAttemptState.Finished);
			for (int i_1 = 0; i_1 < MaxRetries; ++i_1)
			{
				appAttemptHD = store.GetApplicationAttempt(ApplicationAttemptId.NewInstance(ApplicationId
					.NewInstance(0, 1), 1));
				if (appAttemptHD.GetYarnApplicationAttemptState() != null)
				{
					break;
				}
				else
				{
					Sharpen.Thread.Sleep(100);
				}
			}
			NUnit.Framework.Assert.AreEqual("test diagnostics info", appAttemptHD.GetDiagnosticsInfo
				());
			NUnit.Framework.Assert.AreEqual("test url", appAttemptHD.GetTrackingURL());
			NUnit.Framework.Assert.AreEqual(FinalApplicationStatus.Undefined, appAttemptHD.GetFinalApplicationStatus
				());
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Finished, appAttemptHD
				.GetYarnApplicationAttemptState());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteContainer()
		{
			RMContainer container = CreateRMContainer(ContainerId.NewContainerId(ApplicationAttemptId
				.NewInstance(ApplicationId.NewInstance(0, 1), 1), 1));
			writer.ContainerStarted(container);
			ContainerHistoryData containerHD = null;
			for (int i = 0; i < MaxRetries; ++i)
			{
				containerHD = store.GetContainer(ContainerId.NewContainerId(ApplicationAttemptId.
					NewInstance(ApplicationId.NewInstance(0, 1), 1), 1));
				if (containerHD != null)
				{
					break;
				}
				else
				{
					Sharpen.Thread.Sleep(100);
				}
			}
			NUnit.Framework.Assert.IsNotNull(containerHD);
			NUnit.Framework.Assert.AreEqual(NodeId.NewInstance("test host", -100), containerHD
				.GetAssignedNode());
			NUnit.Framework.Assert.AreEqual(Resource.NewInstance(-1, -1), containerHD.GetAllocatedResource
				());
			NUnit.Framework.Assert.AreEqual(Priority.Undefined, containerHD.GetPriority());
			NUnit.Framework.Assert.AreEqual(0L, container.GetCreationTime());
			writer.ContainerFinished(container);
			for (int i_1 = 0; i_1 < MaxRetries; ++i_1)
			{
				containerHD = store.GetContainer(ContainerId.NewContainerId(ApplicationAttemptId.
					NewInstance(ApplicationId.NewInstance(0, 1), 1), 1));
				if (containerHD.GetContainerState() != null)
				{
					break;
				}
				else
				{
					Sharpen.Thread.Sleep(100);
				}
			}
			NUnit.Framework.Assert.AreEqual("test diagnostics info", containerHD.GetDiagnosticsInfo
				());
			NUnit.Framework.Assert.AreEqual(-1, containerHD.GetContainerExitStatus());
			NUnit.Framework.Assert.AreEqual(ContainerState.Complete, containerHD.GetContainerState
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestParallelWrite()
		{
			IList<ApplicationId> appIds = new AList<ApplicationId>();
			for (int i = 0; i < 10; ++i)
			{
				Random rand = new Random(i);
				ApplicationId appId = ApplicationId.NewInstance(0, rand.Next());
				appIds.AddItem(appId);
				RMApp app = CreateRMApp(appId);
				writer.ApplicationStarted(app);
				for (int j = 1; j <= 10; ++j)
				{
					ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, j);
					RMAppAttempt appAttempt = CreateRMAppAttempt(appAttemptId);
					writer.ApplicationAttemptStarted(appAttempt);
					for (int k = 1; k <= 10; ++k)
					{
						ContainerId containerId = ContainerId.NewContainerId(appAttemptId, k);
						RMContainer container = CreateRMContainer(containerId);
						writer.ContainerStarted(container);
						writer.ContainerFinished(container);
					}
					writer.ApplicationAttemptFinished(appAttempt, RMAppAttemptState.Finished);
				}
				writer.ApplicationFinished(app, RMAppState.Finished);
			}
			for (int i_1 = 0; i_1 < MaxRetries; ++i_1)
			{
				if (AllEventsHandled(20 * 10 * 10 + 20 * 10 + 20))
				{
					break;
				}
				else
				{
					Sharpen.Thread.Sleep(500);
				}
			}
			NUnit.Framework.Assert.IsTrue(AllEventsHandled(20 * 10 * 10 + 20 * 10 + 20));
			// Validate all events of one application are handled by one dispatcher
			foreach (ApplicationId appId_1 in appIds)
			{
				NUnit.Framework.Assert.IsTrue(HandledByOne(appId_1));
			}
		}

		private bool AllEventsHandled(int expected)
		{
			int actual = 0;
			foreach (TestRMApplicationHistoryWriter.CounterDispatcher dispatcher in dispatchers)
			{
				foreach (int count in dispatcher.counts.Values)
				{
					actual += count;
				}
			}
			return actual == expected;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMWritingMassiveHistoryForFairSche()
		{
			//test WritingMassiveHistory for Fair Scheduler.
			TestRMWritingMassiveHistory(true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMWritingMassiveHistoryForCapacitySche()
		{
			//test WritingMassiveHistory for Capacity Scheduler.
			TestRMWritingMassiveHistory(false);
		}

		/// <exception cref="System.Exception"/>
		private void TestRMWritingMassiveHistory(bool isFS)
		{
			// 1. Show RM can run with writing history data
			// 2. Test additional workload of processing history events
			YarnConfiguration conf = new YarnConfiguration();
			if (isFS)
			{
				conf.SetBoolean(FairSchedulerConfiguration.AssignMultiple, true);
				conf.Set("yarn.resourcemanager.scheduler.class", typeof(FairScheduler).FullName);
			}
			else
			{
				conf.Set("yarn.resourcemanager.scheduler.class", typeof(CapacityScheduler).FullName
					);
			}
			// don't process history events
			MockRM rm = new _MockRM_399(conf);
			long startTime1 = Runtime.CurrentTimeMillis();
			TestRMWritingMassiveHistory(rm);
			long finishTime1 = Runtime.CurrentTimeMillis();
			long elapsedTime1 = finishTime1 - startTime1;
			rm = new MockRM(conf);
			long startTime2 = Runtime.CurrentTimeMillis();
			TestRMWritingMassiveHistory(rm);
			long finishTime2 = Runtime.CurrentTimeMillis();
			long elapsedTime2 = finishTime2 - startTime2;
			// No more than 10% additional workload
			// Should be much less, but computation time is fluctuated
			NUnit.Framework.Assert.IsTrue(elapsedTime2 - elapsedTime1 < elapsedTime1 / 10);
		}

		private sealed class _MockRM_399 : MockRM
		{
			public _MockRM_399(Configuration baseArg1)
				: base(baseArg1)
			{
			}

			protected internal override RMApplicationHistoryWriter CreateRMApplicationHistoryWriter
				()
			{
				return new _RMApplicationHistoryWriter_402();
			}

			private sealed class _RMApplicationHistoryWriter_402 : RMApplicationHistoryWriter
			{
				public _RMApplicationHistoryWriter_402()
				{
				}

				public override void ApplicationStarted(RMApp app)
				{
				}

				public override void ApplicationFinished(RMApp app, RMAppState finalState)
				{
				}

				public override void ApplicationAttemptStarted(RMAppAttempt appAttempt)
				{
				}

				public override void ApplicationAttemptFinished(RMAppAttempt appAttempt, RMAppAttemptState
					 finalState)
				{
				}

				public override void ContainerStarted(RMContainer container)
				{
				}

				public override void ContainerFinished(RMContainer container)
				{
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestRMWritingMassiveHistory(MockRM rm)
		{
			rm.Start();
			MockNM nm = rm.RegisterNode("127.0.0.1:1234", 1024 * 10100);
			RMApp app = rm.SubmitApp(1024);
			nm.NodeHeartbeat(true);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			am.RegisterAppAttempt();
			int request = 10000;
			am.Allocate("127.0.0.1", 1024, request, new AList<ContainerId>());
			nm.NodeHeartbeat(true);
			IList<Container> allocated = am.Allocate(new AList<ResourceRequest>(), new AList<
				ContainerId>()).GetAllocatedContainers();
			int waitCount = 0;
			int allocatedSize = allocated.Count;
			while (allocatedSize < request && waitCount++ < 200)
			{
				Sharpen.Thread.Sleep(300);
				allocated = am.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>()).GetAllocatedContainers
					();
				allocatedSize += allocated.Count;
				nm.NodeHeartbeat(true);
			}
			NUnit.Framework.Assert.AreEqual(request, allocatedSize);
			am.UnregisterAppAttempt();
			am.WaitForState(RMAppAttemptState.Finishing);
			nm.NodeHeartbeat(am.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am.WaitForState(RMAppAttemptState.Finished);
			NodeHeartbeatResponse resp = nm.NodeHeartbeat(true);
			IList<ContainerId> cleaned = resp.GetContainersToCleanup();
			int cleanedSize = cleaned.Count;
			waitCount = 0;
			while (cleanedSize < allocatedSize && waitCount++ < 200)
			{
				Sharpen.Thread.Sleep(300);
				resp = nm.NodeHeartbeat(true);
				cleaned = resp.GetContainersToCleanup();
				cleanedSize += cleaned.Count;
			}
			NUnit.Framework.Assert.AreEqual(allocatedSize, cleanedSize);
			rm.WaitForState(app.GetApplicationId(), RMAppState.Finished);
			rm.Stop();
		}

		private bool HandledByOne(ApplicationId appId)
		{
			int count = 0;
			foreach (TestRMApplicationHistoryWriter.CounterDispatcher dispatcher in dispatchers)
			{
				if (dispatcher.counts.Contains(appId))
				{
					++count;
				}
			}
			return count == 1;
		}

		private class CounterDispatcher : AsyncDispatcher
		{
			private IDictionary<ApplicationId, int> counts = new Dictionary<ApplicationId, int
				>();

			protected override void Dispatch(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				if (@event is WritingApplicationHistoryEvent)
				{
					WritingApplicationHistoryEvent ashEvent = (WritingApplicationHistoryEvent)@event;
					switch (ashEvent.GetType())
					{
						case WritingHistoryEventType.AppStart:
						{
							IncrementCounts(((WritingApplicationStartEvent)@event).GetApplicationId());
							break;
						}

						case WritingHistoryEventType.AppFinish:
						{
							IncrementCounts(((WritingApplicationFinishEvent)@event).GetApplicationId());
							break;
						}

						case WritingHistoryEventType.AppAttemptStart:
						{
							IncrementCounts(((WritingApplicationAttemptStartEvent)@event).GetApplicationAttemptId
								().GetApplicationId());
							break;
						}

						case WritingHistoryEventType.AppAttemptFinish:
						{
							IncrementCounts(((WritingApplicationAttemptFinishEvent)@event).GetApplicationAttemptId
								().GetApplicationId());
							break;
						}

						case WritingHistoryEventType.ContainerStart:
						{
							IncrementCounts(((WritingContainerStartEvent)@event).GetContainerId().GetApplicationAttemptId
								().GetApplicationId());
							break;
						}

						case WritingHistoryEventType.ContainerFinish:
						{
							IncrementCounts(((WritingContainerFinishEvent)@event).GetContainerId().GetApplicationAttemptId
								().GetApplicationId());
							break;
						}
					}
				}
				base.Dispatch(@event);
			}

			private void IncrementCounts(ApplicationId appId)
			{
				int val = counts[appId];
				if (val == null)
				{
					counts[appId] = 1;
				}
				else
				{
					counts[appId] = val + 1;
				}
			}
		}
	}
}
