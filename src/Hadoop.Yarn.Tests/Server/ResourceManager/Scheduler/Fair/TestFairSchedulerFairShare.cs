using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestFairSchedulerFairShare : FairSchedulerTestBase
	{
		private static readonly string AllocFile = new FilePath(TestDir, typeof(TestFairSchedulerFairShare
			).FullName + ".xml").GetAbsolutePath();

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = CreateConfiguration();
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
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

		/// <exception cref="System.IO.IOException"/>
		private void CreateClusterWithQueuesAndOneNode(int mem, string policy)
		{
			CreateClusterWithQueuesAndOneNode(mem, 0, policy);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateClusterWithQueuesAndOneNode(int mem, int vCores, string policy
			)
		{
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"root\" >");
			@out.WriteLine("   <queue name=\"parentA\" >");
			@out.WriteLine("       <weight>8</weight>");
			@out.WriteLine("       <queue name=\"childA1\" />");
			@out.WriteLine("       <queue name=\"childA2\" />");
			@out.WriteLine("       <queue name=\"childA3\" />");
			@out.WriteLine("       <queue name=\"childA4\" />");
			@out.WriteLine("   </queue>");
			@out.WriteLine("   <queue name=\"parentB\" >");
			@out.WriteLine("       <weight>1</weight>");
			@out.WriteLine("       <queue name=\"childB1\" />");
			@out.WriteLine("       <queue name=\"childB2\" />");
			@out.WriteLine("   </queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<defaultQueueSchedulingPolicy>" + policy + "</defaultQueueSchedulingPolicy>"
				);
			@out.WriteLine("</allocations>");
			@out.Close();
			resourceManager = new MockRM(conf);
			resourceManager.Start();
			scheduler = (FairScheduler)resourceManager.GetResourceScheduler();
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(mem, vCores), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareNoAppsRunning()
		{
			int nodeCapacity = 16 * 1024;
			CreateClusterWithQueuesAndOneNode(nodeCapacity, "fair");
			scheduler.Update();
			// No apps are running in the cluster,verify if fair share is zero
			// for all queues under parentA and parentB.
			ICollection<FSLeafQueue> leafQueues = scheduler.GetQueueManager().GetLeafQueues();
			foreach (FSLeafQueue leaf in leafQueues)
			{
				if (leaf.GetName().StartsWith("root.parentA"))
				{
					NUnit.Framework.Assert.AreEqual(0, (double)leaf.GetFairShare().GetMemory() / nodeCapacity
						, 0);
				}
				else
				{
					if (leaf.GetName().StartsWith("root.parentB"))
					{
						NUnit.Framework.Assert.AreEqual(0, (double)leaf.GetFairShare().GetMemory() / nodeCapacity
							, 0);
					}
				}
			}
			VerifySteadyFairShareMemory(leafQueues, nodeCapacity);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareOneAppRunning()
		{
			int nodeCapacity = 16 * 1024;
			CreateClusterWithQueuesAndOneNode(nodeCapacity, "fair");
			// Run a app in a childA1. Verify whether fair share is 100% in childA1,
			// since it is the only active queue.
			// Also verify if fair share is 0 for childA2. since no app is
			// running in it.
			CreateSchedulingRequest(2 * 1024, "root.parentA.childA1", "user1");
			scheduler.Update();
			NUnit.Framework.Assert.AreEqual(100, (double)scheduler.GetQueueManager().GetLeafQueue
				("root.parentA.childA1", false).GetFairShare().GetMemory() / nodeCapacity * 100, 
				0.1);
			NUnit.Framework.Assert.AreEqual(0, (double)scheduler.GetQueueManager().GetLeafQueue
				("root.parentA.childA2", false).GetFairShare().GetMemory() / nodeCapacity, 0.1);
			VerifySteadyFairShareMemory(scheduler.GetQueueManager().GetLeafQueues(), nodeCapacity
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareMultipleActiveQueuesUnderSameParent()
		{
			int nodeCapacity = 16 * 1024;
			CreateClusterWithQueuesAndOneNode(nodeCapacity, "fair");
			// Run apps in childA1,childA2,childA3
			CreateSchedulingRequest(2 * 1024, "root.parentA.childA1", "user1");
			CreateSchedulingRequest(2 * 1024, "root.parentA.childA2", "user2");
			CreateSchedulingRequest(2 * 1024, "root.parentA.childA3", "user3");
			scheduler.Update();
			// Verify if fair share is 100 / 3 = 33%
			for (int i = 1; i <= 3; i++)
			{
				NUnit.Framework.Assert.AreEqual(33, (double)scheduler.GetQueueManager().GetLeafQueue
					("root.parentA.childA" + i, false).GetFairShare().GetMemory() / nodeCapacity * 100
					, .9);
			}
			VerifySteadyFairShareMemory(scheduler.GetQueueManager().GetLeafQueues(), nodeCapacity
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareMultipleActiveQueuesUnderDifferentParent()
		{
			int nodeCapacity = 16 * 1024;
			CreateClusterWithQueuesAndOneNode(nodeCapacity, "fair");
			// Run apps in childA1,childA2 which are under parentA
			CreateSchedulingRequest(2 * 1024, "root.parentA.childA1", "user1");
			CreateSchedulingRequest(3 * 1024, "root.parentA.childA2", "user2");
			// Run app in childB1 which is under parentB
			CreateSchedulingRequest(1 * 1024, "root.parentB.childB1", "user3");
			// Run app in root.default queue
			CreateSchedulingRequest(1 * 1024, "root.default", "user4");
			scheduler.Update();
			// The two active child queues under parentA would
			// get fair share of 80/2=40%
			for (int i = 1; i <= 2; i++)
			{
				NUnit.Framework.Assert.AreEqual(40, (double)scheduler.GetQueueManager().GetLeafQueue
					("root.parentA.childA" + i, false).GetFairShare().GetMemory() / nodeCapacity * 100
					, .9);
			}
			// The child queue under parentB would get a fair share of 10%,
			// basically all of parentB's fair share
			NUnit.Framework.Assert.AreEqual(10, (double)scheduler.GetQueueManager().GetLeafQueue
				("root.parentB.childB1", false).GetFairShare().GetMemory() / nodeCapacity * 100, 
				.9);
			VerifySteadyFairShareMemory(scheduler.GetQueueManager().GetLeafQueues(), nodeCapacity
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareResetsToZeroWhenAppsComplete()
		{
			int nodeCapacity = 16 * 1024;
			CreateClusterWithQueuesAndOneNode(nodeCapacity, "fair");
			// Run apps in childA1,childA2 which are under parentA
			ApplicationAttemptId app1 = CreateSchedulingRequest(2 * 1024, "root.parentA.childA1"
				, "user1");
			ApplicationAttemptId app2 = CreateSchedulingRequest(3 * 1024, "root.parentA.childA2"
				, "user2");
			scheduler.Update();
			// Verify if both the active queues under parentA get 50% fair
			// share
			for (int i = 1; i <= 2; i++)
			{
				NUnit.Framework.Assert.AreEqual(50, (double)scheduler.GetQueueManager().GetLeafQueue
					("root.parentA.childA" + i, false).GetFairShare().GetMemory() / nodeCapacity * 100
					, .9);
			}
			// Let app under childA1 complete. This should cause the fair share
			// of queue childA1 to be reset to zero,since the queue has no apps running.
			// Queue childA2's fair share would increase to 100% since its the only
			// active queue.
			AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent
				(app1, RMAppAttemptState.Finished, false);
			scheduler.Handle(appRemovedEvent1);
			scheduler.Update();
			NUnit.Framework.Assert.AreEqual(0, (double)scheduler.GetQueueManager().GetLeafQueue
				("root.parentA.childA1", false).GetFairShare().GetMemory() / nodeCapacity * 100, 
				0);
			NUnit.Framework.Assert.AreEqual(100, (double)scheduler.GetQueueManager().GetLeafQueue
				("root.parentA.childA2", false).GetFairShare().GetMemory() / nodeCapacity * 100, 
				0.1);
			VerifySteadyFairShareMemory(scheduler.GetQueueManager().GetLeafQueues(), nodeCapacity
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareWithDRFMultipleActiveQueuesUnderDifferentParent(
			)
		{
			int nodeMem = 16 * 1024;
			int nodeVCores = 10;
			CreateClusterWithQueuesAndOneNode(nodeMem, nodeVCores, "drf");
			// Run apps in childA1,childA2 which are under parentA
			CreateSchedulingRequest(2 * 1024, "root.parentA.childA1", "user1");
			CreateSchedulingRequest(3 * 1024, "root.parentA.childA2", "user2");
			// Run app in childB1 which is under parentB
			CreateSchedulingRequest(1 * 1024, "root.parentB.childB1", "user3");
			// Run app in root.default queue
			CreateSchedulingRequest(1 * 1024, "root.default", "user4");
			scheduler.Update();
			// The two active child queues under parentA would
			// get 80/2=40% memory and vcores
			for (int i = 1; i <= 2; i++)
			{
				NUnit.Framework.Assert.AreEqual(40, (double)scheduler.GetQueueManager().GetLeafQueue
					("root.parentA.childA" + i, false).GetFairShare().GetMemory() / nodeMem * 100, .9
					);
				NUnit.Framework.Assert.AreEqual(40, (double)scheduler.GetQueueManager().GetLeafQueue
					("root.parentA.childA" + i, false).GetFairShare().GetVirtualCores() / nodeVCores
					 * 100, .9);
			}
			// The only active child queue under parentB would get 10% memory and vcores
			NUnit.Framework.Assert.AreEqual(10, (double)scheduler.GetQueueManager().GetLeafQueue
				("root.parentB.childB1", false).GetFairShare().GetMemory() / nodeMem * 100, .9);
			NUnit.Framework.Assert.AreEqual(10, (double)scheduler.GetQueueManager().GetLeafQueue
				("root.parentB.childB1", false).GetFairShare().GetVirtualCores() / nodeVCores * 
				100, .9);
			ICollection<FSLeafQueue> leafQueues = scheduler.GetQueueManager().GetLeafQueues();
			foreach (FSLeafQueue leaf in leafQueues)
			{
				if (leaf.GetName().StartsWith("root.parentA"))
				{
					NUnit.Framework.Assert.AreEqual(0.2, (double)leaf.GetSteadyFairShare().GetMemory(
						) / nodeMem, 0.001);
					NUnit.Framework.Assert.AreEqual(0.2, (double)leaf.GetSteadyFairShare().GetVirtualCores
						() / nodeVCores, 0.001);
				}
				else
				{
					if (leaf.GetName().StartsWith("root.parentB"))
					{
						NUnit.Framework.Assert.AreEqual(0.05, (double)leaf.GetSteadyFairShare().GetMemory
							() / nodeMem, 0.001);
						NUnit.Framework.Assert.AreEqual(0.1, (double)leaf.GetSteadyFairShare().GetVirtualCores
							() / nodeVCores, 0.001);
					}
				}
			}
		}

		/// <summary>
		/// Verify whether steady fair shares for all leaf queues still follow
		/// their weight, not related to active/inactive status.
		/// </summary>
		/// <param name="leafQueues"/>
		/// <param name="nodeCapacity"/>
		private void VerifySteadyFairShareMemory(ICollection<FSLeafQueue> leafQueues, int
			 nodeCapacity)
		{
			foreach (FSLeafQueue leaf in leafQueues)
			{
				if (leaf.GetName().StartsWith("root.parentA"))
				{
					NUnit.Framework.Assert.AreEqual(0.2, (double)leaf.GetSteadyFairShare().GetMemory(
						) / nodeCapacity, 0.001);
				}
				else
				{
					if (leaf.GetName().StartsWith("root.parentB"))
					{
						NUnit.Framework.Assert.AreEqual(0.05, (double)leaf.GetSteadyFairShare().GetMemory
							() / nodeCapacity, 0.001);
					}
				}
			}
		}
	}
}
