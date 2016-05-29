using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestFairSchedulerPreemption : FairSchedulerTestBase
	{
		private static readonly string AllocFile = new FilePath(TestDir, typeof(TestFairSchedulerPreemption
			).FullName + ".xml").GetAbsolutePath();

		private FairSchedulerTestBase.MockClock clock;

		private class StubbedFairScheduler : FairScheduler
		{
			public int lastPreemptMemory = -1;

			protected internal override void PreemptResources(Resource toPreempt)
			{
				lastPreemptMemory = toPreempt.GetMemory();
			}

			public virtual void ResetLastPreemptResources()
			{
				lastPreemptMemory = -1;
			}
		}

		public override Configuration CreateConfiguration()
		{
			Configuration conf = base.CreateConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(TestFairSchedulerPreemption.StubbedFairScheduler
				), typeof(ResourceScheduler));
			conf.SetBoolean(FairSchedulerConfiguration.Preemption, true);
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = CreateConfiguration();
			clock = new FairSchedulerTestBase.MockClock();
		}

		[TearDown]
		public virtual void Teardown()
		{
			if (resourceManager != null)
			{
				resourceManager.Stop();
				resourceManager = null;
			}
			conf = null;
		}

		private void StartResourceManager(float utilizationThreshold)
		{
			conf.SetFloat(FairSchedulerConfiguration.PreemptionThreshold, utilizationThreshold
				);
			resourceManager = new MockRM(conf);
			resourceManager.Start();
			NUnit.Framework.Assert.IsTrue(resourceManager.GetResourceScheduler() is TestFairSchedulerPreemption.StubbedFairScheduler
				);
			scheduler = (FairScheduler)resourceManager.GetResourceScheduler();
			scheduler.SetClock(clock);
			scheduler.updateInterval = 60 * 1000;
		}

		private void RegisterNodeAndSubmitApp(int memory, int vcores, int appContainers, 
			int appMemory)
		{
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(memory, vcores), 
				1, "node1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			NUnit.Framework.Assert.AreEqual("Incorrect amount of resources in the cluster", memory
				, scheduler.rootMetrics.GetAvailableMB());
			NUnit.Framework.Assert.AreEqual("Incorrect amount of resources in the cluster", vcores
				, scheduler.rootMetrics.GetAvailableVirtualCores());
			CreateSchedulingRequest(appMemory, "queueA", "user1", appContainers);
			scheduler.Update();
			// Sufficient node check-ins to fully schedule containers
			for (int i = 0; i < 3; i++)
			{
				NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
				scheduler.Handle(nodeUpdate1);
			}
			NUnit.Framework.Assert.AreEqual("app1's request is not met", memory - appContainers
				 * appMemory, scheduler.rootMetrics.GetAvailableMB());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreemptionWithFreeResources()
		{
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"default\">");
			@out.WriteLine("<maxResources>0mb,0vcores</maxResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<weight>1</weight>");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<weight>1</weight>");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.Write("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>"
				);
			@out.Write("<fairSharePreemptionTimeout>10</fairSharePreemptionTimeout>");
			@out.WriteLine("</allocations>");
			@out.Close();
			StartResourceManager(0f);
			// Create node with 4GB memory and 4 vcores
			RegisterNodeAndSubmitApp(4 * 1024, 4, 2, 1024);
			// Verify submitting another request triggers preemption
			CreateSchedulingRequest(1024, "queueB", "user1", 1, 1);
			scheduler.Update();
			clock.Tick(6);
			((TestFairSchedulerPreemption.StubbedFairScheduler)scheduler).ResetLastPreemptResources
				();
			scheduler.PreemptTasksIfNecessary();
			NUnit.Framework.Assert.AreEqual("preemptResources() should have been called", 1024
				, ((TestFairSchedulerPreemption.StubbedFairScheduler)scheduler).lastPreemptMemory
				);
			resourceManager.Stop();
			StartResourceManager(0.8f);
			// Create node with 4GB memory and 4 vcores
			RegisterNodeAndSubmitApp(4 * 1024, 4, 3, 1024);
			// Verify submitting another request doesn't trigger preemption
			CreateSchedulingRequest(1024, "queueB", "user1", 1, 1);
			scheduler.Update();
			clock.Tick(6);
			((TestFairSchedulerPreemption.StubbedFairScheduler)scheduler).ResetLastPreemptResources
				();
			scheduler.PreemptTasksIfNecessary();
			NUnit.Framework.Assert.AreEqual("preemptResources() should not have been called", 
				-1, ((TestFairSchedulerPreemption.StubbedFairScheduler)scheduler).lastPreemptMemory
				);
			resourceManager.Stop();
			StartResourceManager(0.7f);
			// Create node with 4GB memory and 4 vcores
			RegisterNodeAndSubmitApp(4 * 1024, 4, 3, 1024);
			// Verify submitting another request triggers preemption
			CreateSchedulingRequest(1024, "queueB", "user1", 1, 1);
			scheduler.Update();
			clock.Tick(6);
			((TestFairSchedulerPreemption.StubbedFairScheduler)scheduler).ResetLastPreemptResources
				();
			scheduler.PreemptTasksIfNecessary();
			NUnit.Framework.Assert.AreEqual("preemptResources() should have been called", 1024
				, ((TestFairSchedulerPreemption.StubbedFairScheduler)scheduler).lastPreemptMemory
				);
		}
	}
}
