using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestFSAppAttempt : FairSchedulerTestBase
	{
		private class MockClock : Clock
		{
			private long time = 0;

			public virtual long GetTime()
			{
				return this.time;
			}

			public virtual void Tick(int seconds)
			{
				this.time = this.time + seconds * 1000;
			}

			internal MockClock(TestFSAppAttempt _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestFSAppAttempt _enclosing;
		}

		[SetUp]
		public virtual void Setup()
		{
			Configuration conf = CreateConfiguration();
			resourceManager = new MockRM(conf);
			resourceManager.Start();
			scheduler = (FairScheduler)resourceManager.GetResourceScheduler();
		}

		[NUnit.Framework.Test]
		public virtual void TestDelayScheduling()
		{
			FSLeafQueue queue = Org.Mockito.Mockito.Mock<FSLeafQueue>();
			Priority prio = Org.Mockito.Mockito.Mock<Priority>();
			Org.Mockito.Mockito.When(prio.GetPriority()).ThenReturn(1);
			double nodeLocalityThreshold = .5;
			double rackLocalityThreshold = .6;
			ApplicationAttemptId applicationAttemptId = CreateAppAttemptId(1, 1);
			RMContext rmContext = resourceManager.GetRMContext();
			FSAppAttempt schedulerApp = new FSAppAttempt(scheduler, applicationAttemptId, "user1"
				, queue, null, rmContext);
			// Default level should be node-local
			NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, schedulerApp.GetAllowedLocalityLevel
				(prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
			// First five scheduling opportunities should remain node local
			for (int i = 0; i < 5; i++)
			{
				schedulerApp.AddSchedulingOpportunity(prio);
				NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, schedulerApp.GetAllowedLocalityLevel
					(prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
			}
			// After five it should switch to rack local
			schedulerApp.AddSchedulingOpportunity(prio);
			NUnit.Framework.Assert.AreEqual(NodeType.RackLocal, schedulerApp.GetAllowedLocalityLevel
				(prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
			// Manually set back to node local
			schedulerApp.ResetAllowedLocalityLevel(prio, NodeType.NodeLocal);
			schedulerApp.ResetSchedulingOpportunities(prio);
			NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, schedulerApp.GetAllowedLocalityLevel
				(prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
			// Now escalate again to rack-local, then to off-switch
			for (int i_1 = 0; i_1 < 5; i_1++)
			{
				schedulerApp.AddSchedulingOpportunity(prio);
				NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, schedulerApp.GetAllowedLocalityLevel
					(prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
			}
			schedulerApp.AddSchedulingOpportunity(prio);
			NUnit.Framework.Assert.AreEqual(NodeType.RackLocal, schedulerApp.GetAllowedLocalityLevel
				(prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
			for (int i_2 = 0; i_2 < 6; i_2++)
			{
				schedulerApp.AddSchedulingOpportunity(prio);
				NUnit.Framework.Assert.AreEqual(NodeType.RackLocal, schedulerApp.GetAllowedLocalityLevel
					(prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
			}
			schedulerApp.AddSchedulingOpportunity(prio);
			NUnit.Framework.Assert.AreEqual(NodeType.OffSwitch, schedulerApp.GetAllowedLocalityLevel
				(prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelaySchedulingForContinuousScheduling()
		{
			FSLeafQueue queue = scheduler.GetQueueManager().GetLeafQueue("queue", true);
			Priority prio = Org.Mockito.Mockito.Mock<Priority>();
			Org.Mockito.Mockito.When(prio.GetPriority()).ThenReturn(1);
			TestFSAppAttempt.MockClock clock = new TestFSAppAttempt.MockClock(this);
			scheduler.SetClock(clock);
			long nodeLocalityDelayMs = 5 * 1000L;
			// 5 seconds
			long rackLocalityDelayMs = 6 * 1000L;
			// 6 seconds
			RMContext rmContext = resourceManager.GetRMContext();
			ApplicationAttemptId applicationAttemptId = CreateAppAttemptId(1, 1);
			FSAppAttempt schedulerApp = new FSAppAttempt(scheduler, applicationAttemptId, "user1"
				, queue, null, rmContext);
			// Default level should be node-local
			NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, schedulerApp.GetAllowedLocalityLevelByTime
				(prio, nodeLocalityDelayMs, rackLocalityDelayMs, clock.GetTime()));
			// after 4 seconds should remain node local
			clock.Tick(4);
			NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, schedulerApp.GetAllowedLocalityLevelByTime
				(prio, nodeLocalityDelayMs, rackLocalityDelayMs, clock.GetTime()));
			// after 6 seconds should switch to rack local
			clock.Tick(2);
			NUnit.Framework.Assert.AreEqual(NodeType.RackLocal, schedulerApp.GetAllowedLocalityLevelByTime
				(prio, nodeLocalityDelayMs, rackLocalityDelayMs, clock.GetTime()));
			// manually set back to node local
			schedulerApp.ResetAllowedLocalityLevel(prio, NodeType.NodeLocal);
			schedulerApp.ResetSchedulingOpportunities(prio, clock.GetTime());
			NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, schedulerApp.GetAllowedLocalityLevelByTime
				(prio, nodeLocalityDelayMs, rackLocalityDelayMs, clock.GetTime()));
			// Now escalate again to rack-local, then to off-switch
			clock.Tick(6);
			NUnit.Framework.Assert.AreEqual(NodeType.RackLocal, schedulerApp.GetAllowedLocalityLevelByTime
				(prio, nodeLocalityDelayMs, rackLocalityDelayMs, clock.GetTime()));
			clock.Tick(7);
			NUnit.Framework.Assert.AreEqual(NodeType.OffSwitch, schedulerApp.GetAllowedLocalityLevelByTime
				(prio, nodeLocalityDelayMs, rackLocalityDelayMs, clock.GetTime()));
		}

		[NUnit.Framework.Test]
		public virtual void TestLocalityLevelWithoutDelays()
		{
			FSLeafQueue queue = Org.Mockito.Mockito.Mock<FSLeafQueue>();
			Priority prio = Org.Mockito.Mockito.Mock<Priority>();
			Org.Mockito.Mockito.When(prio.GetPriority()).ThenReturn(1);
			RMContext rmContext = resourceManager.GetRMContext();
			ApplicationAttemptId applicationAttemptId = CreateAppAttemptId(1, 1);
			FSAppAttempt schedulerApp = new FSAppAttempt(scheduler, applicationAttemptId, "user1"
				, queue, null, rmContext);
			NUnit.Framework.Assert.AreEqual(NodeType.OffSwitch, schedulerApp.GetAllowedLocalityLevel
				(prio, 10, -1.0, -1.0));
		}

		[NUnit.Framework.Test]
		public virtual void TestHeadroom()
		{
			FairScheduler mockScheduler = Org.Mockito.Mockito.Mock<FairScheduler>();
			Org.Mockito.Mockito.When(mockScheduler.GetClock()).ThenReturn(scheduler.GetClock(
				));
			FSLeafQueue mockQueue = Org.Mockito.Mockito.Mock<FSLeafQueue>();
			Resource queueMaxResources = Resource.NewInstance(5 * 1024, 3);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource queueFairShare = Resources.CreateResource
				(4096, 2);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource queueUsage = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2048, 2);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource queueStarvation = Resources.Subtract(
				queueFairShare, queueUsage);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource queueMaxResourcesAvailable = Resources
				.Subtract(queueMaxResources, queueUsage);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(8192, 8);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterUsage = Resources.CreateResource
				(2048, 2);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterAvailable = Resources.Subtract
				(clusterResource, clusterUsage);
			QueueMetrics fakeRootQueueMetrics = Org.Mockito.Mockito.Mock<QueueMetrics>();
			Org.Mockito.Mockito.When(mockQueue.GetMaxShare()).ThenReturn(queueMaxResources);
			Org.Mockito.Mockito.When(mockQueue.GetFairShare()).ThenReturn(queueFairShare);
			Org.Mockito.Mockito.When(mockQueue.GetResourceUsage()).ThenReturn(queueUsage);
			Org.Mockito.Mockito.When(mockScheduler.GetClusterResource()).ThenReturn(clusterResource
				);
			Org.Mockito.Mockito.When(fakeRootQueueMetrics.GetAllocatedResources()).ThenReturn
				(clusterUsage);
			Org.Mockito.Mockito.When(mockScheduler.GetRootQueueMetrics()).ThenReturn(fakeRootQueueMetrics
				);
			ApplicationAttemptId applicationAttemptId = CreateAppAttemptId(1, 1);
			RMContext rmContext = resourceManager.GetRMContext();
			FSAppAttempt schedulerApp = new FSAppAttempt(mockScheduler, applicationAttemptId, 
				"user1", mockQueue, null, rmContext);
			// Min of Memory and CPU across cluster and queue is used in
			// DominantResourceFairnessPolicy
			Org.Mockito.Mockito.When(mockQueue.GetPolicy()).ThenReturn(SchedulingPolicy.GetInstance
				(typeof(DominantResourceFairnessPolicy)));
			VerifyHeadroom(schedulerApp, Min(queueStarvation.GetMemory(), clusterAvailable.GetMemory
				(), queueMaxResourcesAvailable.GetMemory()), Min(queueStarvation.GetVirtualCores
				(), clusterAvailable.GetVirtualCores(), queueMaxResourcesAvailable.GetVirtualCores
				()));
			// Fair and Fifo ignore CPU of queue, so use cluster available CPU
			Org.Mockito.Mockito.When(mockQueue.GetPolicy()).ThenReturn(SchedulingPolicy.GetInstance
				(typeof(FairSharePolicy)));
			VerifyHeadroom(schedulerApp, Min(queueStarvation.GetMemory(), clusterAvailable.GetMemory
				(), queueMaxResourcesAvailable.GetMemory()), Math.Min(clusterAvailable.GetVirtualCores
				(), queueMaxResourcesAvailable.GetVirtualCores()));
			Org.Mockito.Mockito.When(mockQueue.GetPolicy()).ThenReturn(SchedulingPolicy.GetInstance
				(typeof(FifoPolicy)));
			VerifyHeadroom(schedulerApp, Min(queueStarvation.GetMemory(), clusterAvailable.GetMemory
				(), queueMaxResourcesAvailable.GetMemory()), Math.Min(clusterAvailable.GetVirtualCores
				(), queueMaxResourcesAvailable.GetVirtualCores()));
		}

		private static int Min(int value1, int value2, int value3)
		{
			return Math.Min(Math.Min(value1, value2), value3);
		}

		protected internal virtual void VerifyHeadroom(FSAppAttempt schedulerApp, int expectedMemory
			, int expectedCPU)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource headroom = schedulerApp.GetHeadroom();
			NUnit.Framework.Assert.AreEqual(expectedMemory, headroom.GetMemory());
			NUnit.Framework.Assert.AreEqual(expectedCPU, headroom.GetVirtualCores());
		}
	}
}
