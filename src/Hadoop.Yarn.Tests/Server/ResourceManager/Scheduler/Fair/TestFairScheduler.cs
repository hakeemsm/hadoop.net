using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestFairScheduler : FairSchedulerTestBase
	{
		private static readonly string AllocFile = new FilePath(TestDir, "test-queues").GetAbsolutePath
			();

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			scheduler = new FairScheduler();
			conf = CreateConfiguration();
			resourceManager = new ResourceManager();
			resourceManager.Init(conf);
			// TODO: This test should really be using MockRM. For now starting stuff
			// that is needed at a bare minimum.
			((AsyncDispatcher)resourceManager.GetRMContext().GetDispatcher()).Start();
			resourceManager.GetRMContext().GetStateStore().Start();
			// to initialize the master key
			resourceManager.GetRMContext().GetContainerTokenSecretManager().RollMasterKey();
			scheduler.SetRMContext(resourceManager.GetRMContext());
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (scheduler != null)
			{
				scheduler.Stop();
				scheduler = null;
			}
			if (resourceManager != null)
			{
				resourceManager.Stop();
				resourceManager = null;
			}
			QueueMetrics.ClearQueueMetrics();
			DefaultMetricsSystem.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestConfValidation()
		{
			scheduler = new FairScheduler();
			Configuration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, 2048);
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, 1024);
			try
			{
				scheduler.ServiceInit(conf);
				NUnit.Framework.Assert.Fail("Exception is expected because the min memory allocation is"
					 + " larger than the max memory allocation.");
			}
			catch (YarnRuntimeException e)
			{
				// Exception is expected.
				NUnit.Framework.Assert.IsTrue("The thrown exception is not the expected one.", e.
					Message.StartsWith("Invalid resource scheduler memory"));
			}
			conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationVcores, 2);
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores, 1);
			try
			{
				scheduler.ServiceInit(conf);
				NUnit.Framework.Assert.Fail("Exception is expected because the min vcores allocation is"
					 + " larger than the max vcores allocation.");
			}
			catch (YarnRuntimeException e)
			{
				// Exception is expected.
				NUnit.Framework.Assert.IsTrue("The thrown exception is not the expected one.", e.
					Message.StartsWith("Invalid resource scheduler vcores"));
			}
		}

		// TESTS
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLoadConfigurationOnInitialize()
		{
			conf.SetBoolean(FairSchedulerConfiguration.AssignMultiple, true);
			conf.SetInt(FairSchedulerConfiguration.MaxAssign, 3);
			conf.SetBoolean(FairSchedulerConfiguration.SizeBasedWeight, true);
			conf.SetFloat(FairSchedulerConfiguration.LocalityThresholdNode, .5f);
			conf.SetFloat(FairSchedulerConfiguration.LocalityThresholdRack, .7f);
			conf.SetBoolean(FairSchedulerConfiguration.ContinuousSchedulingEnabled, true);
			conf.SetInt(FairSchedulerConfiguration.ContinuousSchedulingSleepMs, 10);
			conf.SetInt(FairSchedulerConfiguration.LocalityDelayRackMs, 5000);
			conf.SetInt(FairSchedulerConfiguration.LocalityDelayNodeMs, 5000);
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, 1024);
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, 512);
			conf.SetInt(FairSchedulerConfiguration.RmSchedulerIncrementAllocationMb, 128);
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			NUnit.Framework.Assert.AreEqual(true, scheduler.assignMultiple);
			NUnit.Framework.Assert.AreEqual(3, scheduler.maxAssign);
			NUnit.Framework.Assert.AreEqual(true, scheduler.sizeBasedWeight);
			NUnit.Framework.Assert.AreEqual(.5, scheduler.nodeLocalityThreshold, .01);
			NUnit.Framework.Assert.AreEqual(.7, scheduler.rackLocalityThreshold, .01);
			NUnit.Framework.Assert.IsTrue("The continuous scheduling should be enabled", scheduler
				.continuousSchedulingEnabled);
			NUnit.Framework.Assert.AreEqual(10, scheduler.continuousSchedulingSleepMs);
			NUnit.Framework.Assert.AreEqual(5000, scheduler.nodeLocalityDelayMs);
			NUnit.Framework.Assert.AreEqual(5000, scheduler.rackLocalityDelayMs);
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetMaximumResourceCapability().GetMemory
				());
			NUnit.Framework.Assert.AreEqual(512, scheduler.GetMinimumResourceCapability().GetMemory
				());
			NUnit.Framework.Assert.AreEqual(128, scheduler.GetIncrementResourceCapability().GetMemory
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNonMinZeroResourcesSettings()
		{
			scheduler = new FairScheduler();
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, 256);
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationVcores, 1);
			conf.SetInt(FairSchedulerConfiguration.RmSchedulerIncrementAllocationMb, 512);
			conf.SetInt(FairSchedulerConfiguration.RmSchedulerIncrementAllocationVcores, 2);
			scheduler.Init(conf);
			scheduler.Reinitialize(conf, null);
			NUnit.Framework.Assert.AreEqual(256, scheduler.GetMinimumResourceCapability().GetMemory
				());
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetMinimumResourceCapability().GetVirtualCores
				());
			NUnit.Framework.Assert.AreEqual(512, scheduler.GetIncrementResourceCapability().GetMemory
				());
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetIncrementResourceCapability().GetVirtualCores
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMinZeroResourcesSettings()
		{
			scheduler = new FairScheduler();
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, 0);
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationVcores, 0);
			conf.SetInt(FairSchedulerConfiguration.RmSchedulerIncrementAllocationMb, 512);
			conf.SetInt(FairSchedulerConfiguration.RmSchedulerIncrementAllocationVcores, 2);
			scheduler.Init(conf);
			scheduler.Reinitialize(conf, null);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetMinimumResourceCapability().GetMemory
				());
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetMinimumResourceCapability().GetVirtualCores
				());
			NUnit.Framework.Assert.AreEqual(512, scheduler.GetIncrementResourceCapability().GetMemory
				());
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetIncrementResourceCapability().GetVirtualCores
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAggregateCapacityTracking()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetClusterResource().GetMemory());
			// Add another node
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(512), 2, "127.0.0.2"
				);
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			NUnit.Framework.Assert.AreEqual(1536, scheduler.GetClusterResource().GetMemory());
			// Remove the first node
			NodeRemovedSchedulerEvent nodeEvent3 = new NodeRemovedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent3);
			NUnit.Framework.Assert.AreEqual(512, scheduler.GetClusterResource().GetMemory());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleFairShareCalculation()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add one big node (only care about aggregate capacity)
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(10 * 1024), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Have two queues which want entire cluster capacity
			CreateSchedulingRequest(10 * 1024, "queue1", "user1");
			CreateSchedulingRequest(10 * 1024, "queue2", "user1");
			CreateSchedulingRequest(10 * 1024, "root.default", "user1");
			scheduler.Update();
			scheduler.GetQueueManager().GetRootQueue().SetSteadyFairShare(scheduler.GetClusterResource
				());
			scheduler.GetQueueManager().GetRootQueue().RecomputeSteadyShares();
			ICollection<FSLeafQueue> queues = scheduler.GetQueueManager().GetLeafQueues();
			NUnit.Framework.Assert.AreEqual(3, queues.Count);
			// Divided three ways - between the two queues and the default queue
			foreach (FSLeafQueue p in queues)
			{
				NUnit.Framework.Assert.AreEqual(3414, p.GetFairShare().GetMemory());
				NUnit.Framework.Assert.AreEqual(3414, p.GetMetrics().GetFairShareMB());
				NUnit.Framework.Assert.AreEqual(3414, p.GetSteadyFairShare().GetMemory());
				NUnit.Framework.Assert.AreEqual(3414, p.GetMetrics().GetSteadyFairShareMB());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareWithMaxResources()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			// set queueA and queueB maxResources,
			// the sum of queueA and queueB maxResources is more than
			// Integer.MAX_VALUE.
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<maxResources>1073741824 mb 1000 vcores</maxResources>");
			@out.WriteLine("<weight>.25</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<maxResources>1073741824 mb 1000 vcores</maxResources>");
			@out.WriteLine("<weight>.75</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add one big node (only care about aggregate capacity)
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Queue A wants 1 * 1024.
			CreateSchedulingRequest(1 * 1024, "queueA", "user1");
			// Queue B wants 6 * 1024
			CreateSchedulingRequest(6 * 1024, "queueB", "user1");
			scheduler.Update();
			FSLeafQueue queue = scheduler.GetQueueManager().GetLeafQueue("queueA", false);
			// queueA's weight is 0.25, so its fair share should be 2 * 1024.
			NUnit.Framework.Assert.AreEqual(2 * 1024, queue.GetFairShare().GetMemory());
			// queueB's weight is 0.75, so its fair share should be 6 * 1024.
			queue = scheduler.GetQueueManager().GetLeafQueue("queueB", false);
			NUnit.Framework.Assert.AreEqual(6 * 1024, queue.GetFairShare().GetMemory());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareWithZeroWeight()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			// set queueA and queueB weight zero.
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<weight>0.0</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<weight>0.0</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add one big node (only care about aggregate capacity)
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Queue A wants 2 * 1024.
			CreateSchedulingRequest(2 * 1024, "queueA", "user1");
			// Queue B wants 6 * 1024
			CreateSchedulingRequest(6 * 1024, "queueB", "user1");
			scheduler.Update();
			FSLeafQueue queue = scheduler.GetQueueManager().GetLeafQueue("queueA", false);
			// queueA's weight is 0.0, so its fair share should be 0.
			NUnit.Framework.Assert.AreEqual(0, queue.GetFairShare().GetMemory());
			// queueB's weight is 0.0, so its fair share should be 0.
			queue = scheduler.GetQueueManager().GetLeafQueue("queueB", false);
			NUnit.Framework.Assert.AreEqual(0, queue.GetFairShare().GetMemory());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareWithZeroWeightNoneZeroMinRes()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			// set queueA and queueB weight zero.
			// set queueA and queueB minResources 1.
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<minResources>1 mb 1 vcores</minResources>");
			@out.WriteLine("<weight>0.0</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<minResources>1 mb 1 vcores</minResources>");
			@out.WriteLine("<weight>0.0</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add one big node (only care about aggregate capacity)
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Queue A wants 2 * 1024.
			CreateSchedulingRequest(2 * 1024, "queueA", "user1");
			// Queue B wants 6 * 1024
			CreateSchedulingRequest(6 * 1024, "queueB", "user1");
			scheduler.Update();
			FSLeafQueue queue = scheduler.GetQueueManager().GetLeafQueue("queueA", false);
			// queueA's weight is 0.0 and minResources is 1,
			// so its fair share should be 1 (minShare).
			NUnit.Framework.Assert.AreEqual(1, queue.GetFairShare().GetMemory());
			// queueB's weight is 0.0 and minResources is 1,
			// so its fair share should be 1 (minShare).
			queue = scheduler.GetQueueManager().GetLeafQueue("queueB", false);
			NUnit.Framework.Assert.AreEqual(1, queue.GetFairShare().GetMemory());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareWithNoneZeroWeightNoneZeroMinRes()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			// set queueA and queueB weight 0.5.
			// set queueA and queueB minResources 1024.
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<minResources>1024 mb 1 vcores</minResources>");
			@out.WriteLine("<weight>0.5</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<minResources>1024 mb 1 vcores</minResources>");
			@out.WriteLine("<weight>0.5</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add one big node (only care about aggregate capacity)
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Queue A wants 4 * 1024.
			CreateSchedulingRequest(4 * 1024, "queueA", "user1");
			// Queue B wants 4 * 1024
			CreateSchedulingRequest(4 * 1024, "queueB", "user1");
			scheduler.Update();
			FSLeafQueue queue = scheduler.GetQueueManager().GetLeafQueue("queueA", false);
			// queueA's weight is 0.5 and minResources is 1024,
			// so its fair share should be 4096.
			NUnit.Framework.Assert.AreEqual(4096, queue.GetFairShare().GetMemory());
			// queueB's weight is 0.5 and minResources is 1024,
			// so its fair share should be 4096.
			queue = scheduler.GetQueueManager().GetLeafQueue("queueB", false);
			NUnit.Framework.Assert.AreEqual(4096, queue.GetFairShare().GetMemory());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueInfo()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<weight>.25</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<weight>.75</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add one big node (only care about aggregate capacity)
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Queue A wants 1 * 1024.
			CreateSchedulingRequest(1 * 1024, "queueA", "user1");
			// Queue B wants 6 * 1024
			CreateSchedulingRequest(6 * 1024, "queueB", "user1");
			scheduler.Update();
			// Capacity should be the same as weight of Queue,
			// because the sum of all active Queues' weight are 1.
			// Before NodeUpdate Event, CurrentCapacity should be 0
			QueueInfo queueInfo = scheduler.GetQueueInfo("queueA", false, false);
			NUnit.Framework.Assert.AreEqual(0.25f, queueInfo.GetCapacity(), 0.0f);
			NUnit.Framework.Assert.AreEqual(0.0f, queueInfo.GetCurrentCapacity(), 0.0f);
			queueInfo = scheduler.GetQueueInfo("queueB", false, false);
			NUnit.Framework.Assert.AreEqual(0.75f, queueInfo.GetCapacity(), 0.0f);
			NUnit.Framework.Assert.AreEqual(0.0f, queueInfo.GetCurrentCapacity(), 0.0f);
			// Each NodeUpdate Event will only assign one container.
			// To assign two containers, call handle NodeUpdate Event twice.
			NodeUpdateSchedulerEvent nodeEvent2 = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(nodeEvent2);
			scheduler.Handle(nodeEvent2);
			// After NodeUpdate Event, CurrentCapacity for queueA should be 1/2=0.5
			// and CurrentCapacity for queueB should be 6/6=1.
			queueInfo = scheduler.GetQueueInfo("queueA", false, false);
			NUnit.Framework.Assert.AreEqual(0.25f, queueInfo.GetCapacity(), 0.0f);
			NUnit.Framework.Assert.AreEqual(0.5f, queueInfo.GetCurrentCapacity(), 0.0f);
			queueInfo = scheduler.GetQueueInfo("queueB", false, false);
			NUnit.Framework.Assert.AreEqual(0.75f, queueInfo.GetCapacity(), 0.0f);
			NUnit.Framework.Assert.AreEqual(1.0f, queueInfo.GetCurrentCapacity(), 0.0f);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleHierarchicalFairShareCalculation()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add one big node (only care about aggregate capacity)
			int capacity = 10 * 24;
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(capacity), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Have two queues which want entire cluster capacity
			CreateSchedulingRequest(10 * 1024, "parent.queue2", "user1");
			CreateSchedulingRequest(10 * 1024, "parent.queue3", "user1");
			CreateSchedulingRequest(10 * 1024, "root.default", "user1");
			scheduler.Update();
			scheduler.GetQueueManager().GetRootQueue().SetSteadyFairShare(scheduler.GetClusterResource
				());
			scheduler.GetQueueManager().GetRootQueue().RecomputeSteadyShares();
			QueueManager queueManager = scheduler.GetQueueManager();
			ICollection<FSLeafQueue> queues = queueManager.GetLeafQueues();
			NUnit.Framework.Assert.AreEqual(3, queues.Count);
			FSLeafQueue queue1 = queueManager.GetLeafQueue("default", true);
			FSLeafQueue queue2 = queueManager.GetLeafQueue("parent.queue2", true);
			FSLeafQueue queue3 = queueManager.GetLeafQueue("parent.queue3", true);
			NUnit.Framework.Assert.AreEqual(capacity / 2, queue1.GetFairShare().GetMemory());
			NUnit.Framework.Assert.AreEqual(capacity / 2, queue1.GetMetrics().GetFairShareMB(
				));
			NUnit.Framework.Assert.AreEqual(capacity / 2, queue1.GetSteadyFairShare().GetMemory
				());
			NUnit.Framework.Assert.AreEqual(capacity / 2, queue1.GetMetrics().GetSteadyFairShareMB
				());
			NUnit.Framework.Assert.AreEqual(capacity / 4, queue2.GetFairShare().GetMemory());
			NUnit.Framework.Assert.AreEqual(capacity / 4, queue2.GetMetrics().GetFairShareMB(
				));
			NUnit.Framework.Assert.AreEqual(capacity / 4, queue2.GetSteadyFairShare().GetMemory
				());
			NUnit.Framework.Assert.AreEqual(capacity / 4, queue2.GetMetrics().GetSteadyFairShareMB
				());
			NUnit.Framework.Assert.AreEqual(capacity / 4, queue3.GetFairShare().GetMemory());
			NUnit.Framework.Assert.AreEqual(capacity / 4, queue3.GetMetrics().GetFairShareMB(
				));
			NUnit.Framework.Assert.AreEqual(capacity / 4, queue3.GetSteadyFairShare().GetMemory
				());
			NUnit.Framework.Assert.AreEqual(capacity / 4, queue3.GetMetrics().GetSteadyFairShareMB
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHierarchicalQueuesSimilarParents()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			QueueManager queueManager = scheduler.GetQueueManager();
			FSLeafQueue leafQueue = queueManager.GetLeafQueue("parent.child", true);
			NUnit.Framework.Assert.AreEqual(2, queueManager.GetLeafQueues().Count);
			NUnit.Framework.Assert.IsNotNull(leafQueue);
			NUnit.Framework.Assert.AreEqual("root.parent.child", leafQueue.GetName());
			FSLeafQueue leafQueue2 = queueManager.GetLeafQueue("parent", true);
			NUnit.Framework.Assert.IsNull(leafQueue2);
			NUnit.Framework.Assert.AreEqual(2, queueManager.GetLeafQueues().Count);
			FSLeafQueue leafQueue3 = queueManager.GetLeafQueue("parent.child.grandchild", true
				);
			NUnit.Framework.Assert.IsNull(leafQueue3);
			NUnit.Framework.Assert.AreEqual(2, queueManager.GetLeafQueues().Count);
			FSLeafQueue leafQueue4 = queueManager.GetLeafQueue("parent.sister", true);
			NUnit.Framework.Assert.IsNotNull(leafQueue4);
			NUnit.Framework.Assert.AreEqual("root.parent.sister", leafQueue4.GetName());
			NUnit.Framework.Assert.AreEqual(3, queueManager.GetLeafQueues().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSchedulerRootQueueMetrics()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024));
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Queue 1 requests full capacity of node
			CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			// Now queue 2 requests likewise
			CreateSchedulingRequest(1024, "queue2", "user1", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// Make sure reserved memory gets updated correctly
			NUnit.Framework.Assert.AreEqual(1024, scheduler.rootMetrics.GetReservedMB());
			// Now another node checks in with capacity
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024));
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			scheduler.Handle(updateEvent2);
			// The old reservation should still be there...
			NUnit.Framework.Assert.AreEqual(1024, scheduler.rootMetrics.GetReservedMB());
			// ... but it should disappear when we update the first node.
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(0, scheduler.rootMetrics.GetReservedMB());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSimpleContainerAllocation()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024, 4), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Add another node
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(512, 2), 2, "127.0.0.2"
				);
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			CreateSchedulingRequest(512, 2, "queue1", "user1", 2);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			// Asked for less than increment allocation.
			NUnit.Framework.Assert.AreEqual(FairSchedulerConfiguration.DefaultRmSchedulerIncrementAllocationMb
				, scheduler.GetQueueManager().GetQueue("queue1").GetResourceUsage().GetMemory());
			NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
			scheduler.Handle(updateEvent2);
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetQueueManager().GetQueue("queue1"
				).GetResourceUsage().GetMemory());
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetQueueManager().GetQueue("queue1")
				.GetResourceUsage().GetVirtualCores());
			// verify metrics
			QueueMetrics queue1Metrics = scheduler.GetQueueManager().GetQueue("queue1").GetMetrics
				();
			NUnit.Framework.Assert.AreEqual(1024, queue1Metrics.GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(2, queue1Metrics.GetAllocatedVirtualCores());
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetRootQueueMetrics().GetAllocatedMB
				());
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetRootQueueMetrics().GetAllocatedVirtualCores
				());
			NUnit.Framework.Assert.AreEqual(512, scheduler.GetRootQueueMetrics().GetAvailableMB
				());
			NUnit.Framework.Assert.AreEqual(4, scheduler.GetRootQueueMetrics().GetAvailableVirtualCores
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSimpleContainerReservation()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Queue 1 requests full capacity of node
			CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			// Make sure queue 1 is allocated app capacity
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetQueueManager().GetQueue("queue1"
				).GetResourceUsage().GetMemory());
			// Now queue 2 requests likewise
			ApplicationAttemptId attId = CreateSchedulingRequest(1024, "queue2", "user1", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// Make sure queue 2 is waiting with a reservation
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetQueueManager().GetQueue("queue2")
				.GetResourceUsage().GetMemory());
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetSchedulerApp(attId).GetCurrentReservation
				().GetMemory());
			// Now another node checks in with capacity
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 2, "127.0.0.2"
				);
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			scheduler.Handle(updateEvent2);
			// Make sure this goes to queue 2
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetQueueManager().GetQueue("queue2"
				).GetResourceUsage().GetMemory());
			// The old reservation should still be there...
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetSchedulerApp(attId).GetCurrentReservation
				().GetMemory());
			// ... but it should disappear when we update the first node.
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId).GetCurrentReservation
				().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerReservationAttemptExceedingQueueMax()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"root\">");
			@out.WriteLine("<queue name=\"queue1\">");
			@out.WriteLine("<maxResources>2048mb,5vcores</maxResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queue2\">");
			@out.WriteLine("<maxResources>2048mb,10vcores</maxResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(3072, 5), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Queue 1 requests full capacity of the queue
			CreateSchedulingRequest(2048, "queue1", "user1", 1);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			// Make sure queue 1 is allocated app capacity
			NUnit.Framework.Assert.AreEqual(2048, scheduler.GetQueueManager().GetQueue("queue1"
				).GetResourceUsage().GetMemory());
			// Now queue 2 requests likewise
			CreateSchedulingRequest(1024, "queue2", "user2", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// Make sure queue 2 is allocated app capacity
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetQueueManager().GetQueue("queue2"
				).GetResourceUsage().GetMemory());
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// Ensure the reservation does not get created as allocated memory of
			// queue1 exceeds max
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId1).GetCurrentReservation
				().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerReservationNotExceedingQueueMax()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"root\">");
			@out.WriteLine("<queue name=\"queue1\">");
			@out.WriteLine("<maxResources>3072mb,10vcores</maxResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queue2\">");
			@out.WriteLine("<maxResources>2048mb,10vcores</maxResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(3072, 5), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Queue 1 requests full capacity of the queue
			CreateSchedulingRequest(2048, "queue1", "user1", 1);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			// Make sure queue 1 is allocated app capacity
			NUnit.Framework.Assert.AreEqual(2048, scheduler.GetQueueManager().GetQueue("queue1"
				).GetResourceUsage().GetMemory());
			// Now queue 2 requests likewise
			CreateSchedulingRequest(1024, "queue2", "user2", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// Make sure queue 2 is allocated app capacity
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetQueueManager().GetQueue("queue2"
				).GetResourceUsage().GetMemory());
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// Make sure queue 1 is waiting with a reservation
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetSchedulerApp(attId1).GetCurrentReservation
				().GetMemory());
			// Exercise checks that reservation fits
			scheduler.Handle(updateEvent);
			// Ensure the reservation still exists as allocated memory of queue1 doesn't
			// exceed max
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetSchedulerApp(attId1).GetCurrentReservation
				().GetMemory());
			// Now reduce max Resources of queue1 down to 2048
			@out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"root\">");
			@out.WriteLine("<queue name=\"queue1\">");
			@out.WriteLine("<maxResources>2048mb,10vcores</maxResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queue2\">");
			@out.WriteLine("<maxResources>2048mb,10vcores</maxResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			CreateSchedulingRequest(1024, "queue2", "user2", 1);
			scheduler.Handle(updateEvent);
			// Make sure allocated memory of queue1 doesn't exceed its maximum
			NUnit.Framework.Assert.AreEqual(2048, scheduler.GetQueueManager().GetQueue("queue1"
				).GetResourceUsage().GetMemory());
			//the reservation of queue1 should be reclaim
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId1).GetCurrentReservation
				().GetMemory());
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetQueueManager().GetQueue("queue2"
				).GetResourceUsage().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUserAsDefaultQueue()
		{
			conf.Set(FairSchedulerConfiguration.UserAsDefaultQueue, "true");
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			ApplicationAttemptId appAttemptId = CreateAppAttemptId(1, 1);
			CreateApplicationWithAMResource(appAttemptId, "default", "user1", null);
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetQueueManager().GetLeafQueue("user1"
				, true).GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetQueueManager().GetLeafQueue("default"
				, true).GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual("root.user1", resourceManager.GetRMContext().GetRMApps
				()[appAttemptId.GetApplicationId()].GetQueue());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotUserAsDefaultQueue()
		{
			conf.Set(FairSchedulerConfiguration.UserAsDefaultQueue, "false");
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			ApplicationAttemptId appAttemptId = CreateAppAttemptId(1, 1);
			CreateApplicationWithAMResource(appAttemptId, "default", "user2", null);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetQueueManager().GetLeafQueue("user1"
				, true).GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetQueueManager().GetLeafQueue("default"
				, true).GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetQueueManager().GetLeafQueue("user2"
				, true).GetNumRunnableApps());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyQueueName()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// only default queue
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetQueueManager().GetLeafQueues().Count
				);
			// submit app with empty queue
			ApplicationAttemptId appAttemptId = CreateAppAttemptId(1, 1);
			AppAddedSchedulerEvent appAddedEvent = new AppAddedSchedulerEvent(appAttemptId.GetApplicationId
				(), string.Empty, "user1");
			scheduler.Handle(appAddedEvent);
			// submission rejected
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetQueueManager().GetLeafQueues().Count
				);
			NUnit.Framework.Assert.IsNull(scheduler.GetSchedulerApp(appAttemptId));
			NUnit.Framework.Assert.AreEqual(0, resourceManager.GetRMContext().GetRMApps().Count
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueuNameWithPeriods()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// only default queue
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetQueueManager().GetLeafQueues().Count
				);
			// submit app with queue name (.A)
			ApplicationAttemptId appAttemptId1 = CreateAppAttemptId(1, 1);
			AppAddedSchedulerEvent appAddedEvent1 = new AppAddedSchedulerEvent(appAttemptId1.
				GetApplicationId(), ".A", "user1");
			scheduler.Handle(appAddedEvent1);
			// submission rejected
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetQueueManager().GetLeafQueues().Count
				);
			NUnit.Framework.Assert.IsNull(scheduler.GetSchedulerApp(appAttemptId1));
			NUnit.Framework.Assert.AreEqual(0, resourceManager.GetRMContext().GetRMApps().Count
				);
			// submit app with queue name (A.)
			ApplicationAttemptId appAttemptId2 = CreateAppAttemptId(2, 1);
			AppAddedSchedulerEvent appAddedEvent2 = new AppAddedSchedulerEvent(appAttemptId2.
				GetApplicationId(), "A.", "user1");
			scheduler.Handle(appAddedEvent2);
			// submission rejected
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetQueueManager().GetLeafQueues().Count
				);
			NUnit.Framework.Assert.IsNull(scheduler.GetSchedulerApp(appAttemptId2));
			NUnit.Framework.Assert.AreEqual(0, resourceManager.GetRMContext().GetRMApps().Count
				);
			// submit app with queue name (A.B)
			ApplicationAttemptId appAttemptId3 = CreateAppAttemptId(3, 1);
			AppAddedSchedulerEvent appAddedEvent3 = new AppAddedSchedulerEvent(appAttemptId3.
				GetApplicationId(), "A.B", "user1");
			scheduler.Handle(appAddedEvent3);
			// submission accepted
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetQueueManager().GetLeafQueues().Count
				);
			NUnit.Framework.Assert.IsNull(scheduler.GetSchedulerApp(appAttemptId3));
			NUnit.Framework.Assert.AreEqual(0, resourceManager.GetRMContext().GetRMApps().Count
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAssignToQueue()
		{
			conf.Set(FairSchedulerConfiguration.UserAsDefaultQueue, "true");
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.New);
			RMApp rmApp2 = new MockRMApp(1, 1, RMAppState.New);
			FSLeafQueue queue1 = scheduler.AssignToQueue(rmApp1, "default", "asterix");
			FSLeafQueue queue2 = scheduler.AssignToQueue(rmApp2, "notdefault", "obelix");
			// assert FSLeafQueue's name is the correct name is the one set in the RMApp
			NUnit.Framework.Assert.AreEqual(rmApp1.GetQueue(), queue1.GetName());
			NUnit.Framework.Assert.AreEqual("root.asterix", rmApp1.GetQueue());
			NUnit.Framework.Assert.AreEqual(rmApp2.GetQueue(), queue2.GetName());
			NUnit.Framework.Assert.AreEqual("root.notdefault", rmApp2.GetQueue());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAssignToNonLeafQueueReturnsNull()
		{
			conf.Set(FairSchedulerConfiguration.UserAsDefaultQueue, "true");
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			scheduler.GetQueueManager().GetLeafQueue("root.child1.granchild", true);
			scheduler.GetQueueManager().GetLeafQueue("root.child2", true);
			RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.New);
			RMApp rmApp2 = new MockRMApp(1, 1, RMAppState.New);
			// Trying to assign to non leaf queue would return null
			NUnit.Framework.Assert.IsNull(scheduler.AssignToQueue(rmApp1, "root.child1", "tintin"
				));
			NUnit.Framework.Assert.IsNotNull(scheduler.AssignToQueue(rmApp2, "root.child2", "snowy"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueuePlacementWithPolicy()
		{
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(SimpleGroupsMapping
				), typeof(GroupMappingServiceProvider));
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			ApplicationAttemptId appId;
			IList<QueuePlacementRule> rules = new AList<QueuePlacementRule>();
			rules.AddItem(new QueuePlacementRule.Specified().Initialize(true, null));
			rules.AddItem(new QueuePlacementRule.User().Initialize(false, null));
			rules.AddItem(new QueuePlacementRule.PrimaryGroup().Initialize(false, null));
			rules.AddItem(new QueuePlacementRule.SecondaryGroupExistingQueue().Initialize(false
				, null));
			rules.AddItem(new QueuePlacementRule.Default().Initialize(true, null));
			ICollection<string> queues = Sets.NewHashSet("root.user1", "root.user3group", "root.user4subgroup1"
				, "root.user4subgroup2", "root.user5subgroup2");
			IDictionary<FSQueueType, ICollection<string>> configuredQueues = new Dictionary<FSQueueType
				, ICollection<string>>();
			configuredQueues[FSQueueType.Leaf] = queues;
			configuredQueues[FSQueueType.Parent] = new HashSet<string>();
			scheduler.GetAllocationConfiguration().placementPolicy = new QueuePlacementPolicy
				(rules, configuredQueues, conf);
			appId = CreateSchedulingRequest(1024, "somequeue", "user1");
			NUnit.Framework.Assert.AreEqual("root.somequeue", scheduler.GetSchedulerApp(appId
				).GetQueueName());
			appId = CreateSchedulingRequest(1024, "default", "user1");
			NUnit.Framework.Assert.AreEqual("root.user1", scheduler.GetSchedulerApp(appId).GetQueueName
				());
			appId = CreateSchedulingRequest(1024, "default", "user3");
			NUnit.Framework.Assert.AreEqual("root.user3group", scheduler.GetSchedulerApp(appId
				).GetQueueName());
			appId = CreateSchedulingRequest(1024, "default", "user4");
			NUnit.Framework.Assert.AreEqual("root.user4subgroup1", scheduler.GetSchedulerApp(
				appId).GetQueueName());
			appId = CreateSchedulingRequest(1024, "default", "user5");
			NUnit.Framework.Assert.AreEqual("root.user5subgroup2", scheduler.GetSchedulerApp(
				appId).GetQueueName());
			appId = CreateSchedulingRequest(1024, "default", "otheruser");
			NUnit.Framework.Assert.AreEqual("root.default", scheduler.GetSchedulerApp(appId).
				GetQueueName());
			// test without specified as first rule
			rules = new AList<QueuePlacementRule>();
			rules.AddItem(new QueuePlacementRule.User().Initialize(false, null));
			rules.AddItem(new QueuePlacementRule.Specified().Initialize(true, null));
			rules.AddItem(new QueuePlacementRule.Default().Initialize(true, null));
			scheduler.GetAllocationConfiguration().placementPolicy = new QueuePlacementPolicy
				(rules, configuredQueues, conf);
			appId = CreateSchedulingRequest(1024, "somequeue", "user1");
			NUnit.Framework.Assert.AreEqual("root.user1", scheduler.GetSchedulerApp(appId).GetQueueName
				());
			appId = CreateSchedulingRequest(1024, "somequeue", "otheruser");
			NUnit.Framework.Assert.AreEqual("root.somequeue", scheduler.GetSchedulerApp(appId
				).GetQueueName());
			appId = CreateSchedulingRequest(1024, "default", "otheruser");
			NUnit.Framework.Assert.AreEqual("root.default", scheduler.GetSchedulerApp(appId).
				GetQueueName());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareWithMinAlloc()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<minResources>2048mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add one big node (only care about aggregate capacity)
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(3 * 1024), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			CreateSchedulingRequest(2 * 1024, "queueA", "user1");
			CreateSchedulingRequest(2 * 1024, "queueB", "user1");
			scheduler.Update();
			ICollection<FSLeafQueue> queues = scheduler.GetQueueManager().GetLeafQueues();
			NUnit.Framework.Assert.AreEqual(3, queues.Count);
			foreach (FSLeafQueue p in queues)
			{
				if (p.GetName().Equals("root.queueA"))
				{
					NUnit.Framework.Assert.AreEqual(1024, p.GetFairShare().GetMemory());
				}
				else
				{
					if (p.GetName().Equals("root.queueB"))
					{
						NUnit.Framework.Assert.AreEqual(2048, p.GetFairShare().GetMemory());
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNestedUserQueue()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(SimpleGroupsMapping
				), typeof(GroupMappingServiceProvider));
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"user1group\" type=\"parent\">");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queuePlacementPolicy>");
			@out.WriteLine("<rule name=\"specified\" create=\"false\" />");
			@out.WriteLine("<rule name=\"nestedUserQueue\">");
			@out.WriteLine("     <rule name=\"primaryGroup\" create=\"false\" />");
			@out.WriteLine("</rule>");
			@out.WriteLine("<rule name=\"default\" />");
			@out.WriteLine("</queuePlacementPolicy>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.New);
			FSLeafQueue user1Leaf = scheduler.AssignToQueue(rmApp1, "root.default", "user1");
			NUnit.Framework.Assert.AreEqual("root.user1group.user1", user1Leaf.GetName());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFairShareAndWeightsInNestedUserQueueRule()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"parentq\" type=\"parent\">");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queuePlacementPolicy>");
			@out.WriteLine("<rule name=\"nestedUserQueue\">");
			@out.WriteLine("     <rule name=\"specified\" create=\"false\" />");
			@out.WriteLine("</rule>");
			@out.WriteLine("<rule name=\"default\" />");
			@out.WriteLine("</queuePlacementPolicy>");
			@out.WriteLine("</allocations>");
			@out.Close();
			RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.New);
			RMApp rmApp2 = new MockRMApp(1, 1, RMAppState.New);
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			int capacity = 16 * 1024;
			// create node with 16 G
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(capacity), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// user1,user2 submit their apps to parentq and create user queues
			CreateSchedulingRequest(10 * 1024, "root.parentq", "user1");
			CreateSchedulingRequest(10 * 1024, "root.parentq", "user2");
			// user3 submits app in default queue
			CreateSchedulingRequest(10 * 1024, "root.default", "user3");
			scheduler.Update();
			scheduler.GetQueueManager().GetRootQueue().SetSteadyFairShare(scheduler.GetClusterResource
				());
			scheduler.GetQueueManager().GetRootQueue().RecomputeSteadyShares();
			ICollection<FSLeafQueue> leafQueues = scheduler.GetQueueManager().GetLeafQueues();
			foreach (FSLeafQueue leaf in leafQueues)
			{
				if (leaf.GetName().Equals("root.parentq.user1") || leaf.GetName().Equals("root.parentq.user2"
					))
				{
					// assert that the fair share is 1/4th node1's capacity
					NUnit.Framework.Assert.AreEqual(capacity / 4, leaf.GetFairShare().GetMemory());
					// assert that the steady fair share is 1/4th node1's capacity
					NUnit.Framework.Assert.AreEqual(capacity / 4, leaf.GetSteadyFairShare().GetMemory
						());
					// assert weights are equal for both the user queues
					NUnit.Framework.Assert.AreEqual(1.0, leaf.GetWeights().GetWeight(ResourceType.Memory
						), 0);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSteadyFairShareWithReloadAndNodeAddRemove()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<defaultQueueSchedulingPolicy>fair</defaultQueueSchedulingPolicy>"
				);
			@out.WriteLine("<queue name=\"root\">");
			@out.WriteLine("  <schedulingPolicy>drf</schedulingPolicy>");
			@out.WriteLine("  <queue name=\"child1\">");
			@out.WriteLine("    <weight>1</weight>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("  <queue name=\"child2\">");
			@out.WriteLine("    <weight>1</weight>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// The steady fair share for all queues should be 0
			QueueManager queueManager = scheduler.GetQueueManager();
			NUnit.Framework.Assert.AreEqual(0, queueManager.GetLeafQueue("child1", false).GetSteadyFairShare
				().GetMemory());
			NUnit.Framework.Assert.AreEqual(0, queueManager.GetLeafQueue("child2", false).GetSteadyFairShare
				().GetMemory());
			// Add one node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(6144), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			NUnit.Framework.Assert.AreEqual(6144, scheduler.GetClusterResource().GetMemory());
			// The steady fair shares for all queues should be updated
			NUnit.Framework.Assert.AreEqual(2048, queueManager.GetLeafQueue("child1", false).
				GetSteadyFairShare().GetMemory());
			NUnit.Framework.Assert.AreEqual(2048, queueManager.GetLeafQueue("child2", false).
				GetSteadyFairShare().GetMemory());
			// Reload the allocation configuration file
			@out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<defaultQueueSchedulingPolicy>fair</defaultQueueSchedulingPolicy>"
				);
			@out.WriteLine("<queue name=\"root\">");
			@out.WriteLine("  <schedulingPolicy>drf</schedulingPolicy>");
			@out.WriteLine("  <queue name=\"child1\">");
			@out.WriteLine("    <weight>1</weight>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("  <queue name=\"child2\">");
			@out.WriteLine("    <weight>2</weight>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("  <queue name=\"child3\">");
			@out.WriteLine("    <weight>2</weight>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// The steady fair shares for all queues should be updated
			NUnit.Framework.Assert.AreEqual(1024, queueManager.GetLeafQueue("child1", false).
				GetSteadyFairShare().GetMemory());
			NUnit.Framework.Assert.AreEqual(2048, queueManager.GetLeafQueue("child2", false).
				GetSteadyFairShare().GetMemory());
			NUnit.Framework.Assert.AreEqual(2048, queueManager.GetLeafQueue("child3", false).
				GetSteadyFairShare().GetMemory());
			// Remove the node, steady fair shares should back to 0
			NodeRemovedSchedulerEvent nodeEvent2 = new NodeRemovedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent2);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetClusterResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0, queueManager.GetLeafQueue("child1", false).GetSteadyFairShare
				().GetMemory());
			NUnit.Framework.Assert.AreEqual(0, queueManager.GetLeafQueue("child2", false).GetSteadyFairShare
				().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSteadyFairShareWithQueueCreatedRuntime()
		{
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(SimpleGroupsMapping
				), typeof(GroupMappingServiceProvider));
			conf.Set(FairSchedulerConfiguration.UserAsDefaultQueue, "true");
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add one node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(6144), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			NUnit.Framework.Assert.AreEqual(6144, scheduler.GetClusterResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(6144, scheduler.GetQueueManager().GetRootQueue().
				GetSteadyFairShare().GetMemory());
			NUnit.Framework.Assert.AreEqual(6144, scheduler.GetQueueManager().GetLeafQueue("default"
				, false).GetSteadyFairShare().GetMemory());
			// Submit one application
			ApplicationAttemptId appAttemptId1 = CreateAppAttemptId(1, 1);
			CreateApplicationWithAMResource(appAttemptId1, "default", "user1", null);
			NUnit.Framework.Assert.AreEqual(3072, scheduler.GetQueueManager().GetLeafQueue("default"
				, false).GetSteadyFairShare().GetMemory());
			NUnit.Framework.Assert.AreEqual(3072, scheduler.GetQueueManager().GetLeafQueue("user1"
				, false).GetSteadyFairShare().GetMemory());
		}

		/// <summary>Make allocation requests and ensure they are reflected in queue demand.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueDemandCalculation()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			ApplicationAttemptId id11 = CreateAppAttemptId(1, 1);
			CreateMockRMApp(id11);
			scheduler.AddApplication(id11.GetApplicationId(), "root.queue1", "user1", false);
			scheduler.AddApplicationAttempt(id11, false, false);
			ApplicationAttemptId id21 = CreateAppAttemptId(2, 1);
			CreateMockRMApp(id21);
			scheduler.AddApplication(id21.GetApplicationId(), "root.queue2", "user1", false);
			scheduler.AddApplicationAttempt(id21, false, false);
			ApplicationAttemptId id22 = CreateAppAttemptId(2, 2);
			CreateMockRMApp(id22);
			scheduler.AddApplication(id22.GetApplicationId(), "root.queue2", "user1", false);
			scheduler.AddApplicationAttempt(id22, false, false);
			int minReqSize = FairSchedulerConfiguration.DefaultRmSchedulerIncrementAllocationMb;
			// First ask, queue1 requests 1 large (minReqSize * 2).
			IList<ResourceRequest> ask1 = new AList<ResourceRequest>();
			ResourceRequest request1 = CreateResourceRequest(minReqSize * 2, ResourceRequest.
				Any, 1, 1, true);
			ask1.AddItem(request1);
			scheduler.Allocate(id11, ask1, new AList<ContainerId>(), null, null);
			// Second ask, queue2 requests 1 large + (2 * minReqSize)
			IList<ResourceRequest> ask2 = new AList<ResourceRequest>();
			ResourceRequest request2 = CreateResourceRequest(2 * minReqSize, "foo", 1, 1, false
				);
			ResourceRequest request3 = CreateResourceRequest(minReqSize, "bar", 1, 2, false);
			ask2.AddItem(request2);
			ask2.AddItem(request3);
			scheduler.Allocate(id21, ask2, new AList<ContainerId>(), null, null);
			// Third ask, queue2 requests 1 large
			IList<ResourceRequest> ask3 = new AList<ResourceRequest>();
			ResourceRequest request4 = CreateResourceRequest(2 * minReqSize, ResourceRequest.
				Any, 1, 1, true);
			ask3.AddItem(request4);
			scheduler.Allocate(id22, ask3, new AList<ContainerId>(), null, null);
			scheduler.Update();
			NUnit.Framework.Assert.AreEqual(2 * minReqSize, scheduler.GetQueueManager().GetQueue
				("root.queue1").GetDemand().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * minReqSize + 2 * minReqSize + (2 * minReqSize
				), scheduler.GetQueueManager().GetQueue("root.queue2").GetDemand().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppAdditionAndRemoval()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			ApplicationAttemptId attemptId = CreateAppAttemptId(1, 1);
			AppAddedSchedulerEvent appAddedEvent = new AppAddedSchedulerEvent(attemptId.GetApplicationId
				(), "default", "user1");
			scheduler.Handle(appAddedEvent);
			AppAttemptAddedSchedulerEvent attemptAddedEvent = new AppAttemptAddedSchedulerEvent
				(CreateAppAttemptId(1, 1), false);
			scheduler.Handle(attemptAddedEvent);
			// Scheduler should have two queues (the default and the one created for user1)
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetQueueManager().GetLeafQueues().Count
				);
			// That queue should have one app
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetQueueManager().GetLeafQueue("user1"
				, true).GetNumRunnableApps());
			AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent
				(CreateAppAttemptId(1, 1), RMAppAttemptState.Finished, false);
			// Now remove app
			scheduler.Handle(appRemovedEvent1);
			// Queue should have no apps
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetQueueManager().GetLeafQueue("user1"
				, true).GetNumRunnableApps());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void TestHierarchicalQueueAllocationFileParsing()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<minResources>2048mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<minResources>2048mb,0vcores</minResources>");
			@out.WriteLine("<queue name=\"queueC\">");
			@out.WriteLine("<minResources>2048mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueD\">");
			@out.WriteLine("<minResources>2048mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			QueueManager queueManager = scheduler.GetQueueManager();
			ICollection<FSLeafQueue> leafQueues = queueManager.GetLeafQueues();
			NUnit.Framework.Assert.AreEqual(4, leafQueues.Count);
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("queueA", false));
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("queueB.queueC", false
				));
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("queueB.queueD", false
				));
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("default", false));
			// Make sure querying for queues didn't create any new ones:
			NUnit.Framework.Assert.AreEqual(4, leafQueues.Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConfigureRootQueue()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<defaultQueueSchedulingPolicy>fair</defaultQueueSchedulingPolicy>"
				);
			@out.WriteLine("<queue name=\"root\">");
			@out.WriteLine("  <schedulingPolicy>drf</schedulingPolicy>");
			@out.WriteLine("  <queue name=\"child1\">");
			@out.WriteLine("    <minResources>1024mb,1vcores</minResources>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("  <queue name=\"child2\">");
			@out.WriteLine("    <minResources>1024mb,4vcores</minResources>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("  <fairSharePreemptionTimeout>100</fairSharePreemptionTimeout>");
			@out.WriteLine("  <minSharePreemptionTimeout>120</minSharePreemptionTimeout>");
			@out.WriteLine("  <fairSharePreemptionThreshold>.5</fairSharePreemptionThreshold>"
				);
			@out.WriteLine("</queue>");
			@out.WriteLine("<defaultFairSharePreemptionTimeout>300</defaultFairSharePreemptionTimeout>"
				);
			@out.WriteLine("<defaultMinSharePreemptionTimeout>200</defaultMinSharePreemptionTimeout>"
				);
			@out.WriteLine("<defaultFairSharePreemptionThreshold>.6</defaultFairSharePreemptionThreshold>"
				);
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			QueueManager queueManager = scheduler.GetQueueManager();
			FSQueue root = queueManager.GetRootQueue();
			NUnit.Framework.Assert.IsTrue(root.GetPolicy() is DominantResourceFairnessPolicy);
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("child1", false));
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("child2", false));
			NUnit.Framework.Assert.AreEqual(100000, root.GetFairSharePreemptionTimeout());
			NUnit.Framework.Assert.AreEqual(120000, root.GetMinSharePreemptionTimeout());
			NUnit.Framework.Assert.AreEqual(0.5f, root.GetFairSharePreemptionThreshold(), 0.01
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestChoiceOfPreemptedContainers()
		{
			conf.SetLong(FairSchedulerConfiguration.PreemptionInterval, 5000);
			conf.SetLong(FairSchedulerConfiguration.WaitTimeBeforeKill, 10000);
			conf.Set(FairSchedulerConfiguration.AllocationFile + ".allocation.file", AllocFile
				);
			conf.Set(FairSchedulerConfiguration.UserAsDefaultQueue, "false");
			FairSchedulerTestBase.MockClock clock = new FairSchedulerTestBase.MockClock();
			scheduler.SetClock(clock);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<weight>.25</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<weight>.25</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueC\">");
			@out.WriteLine("<weight>.25</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"default\">");
			@out.WriteLine("<weight>.25</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Create two nodes
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(4 * 1024, 4), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(4 * 1024, 4), 2, 
				"127.0.0.2");
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			// Queue A and B each request two applications
			ApplicationAttemptId app1 = CreateSchedulingRequest(1 * 1024, 1, "queueA", "user1"
				, 1, 1);
			CreateSchedulingRequestExistingApplication(1 * 1024, 1, 2, app1);
			ApplicationAttemptId app2 = CreateSchedulingRequest(1 * 1024, 1, "queueA", "user1"
				, 1, 3);
			CreateSchedulingRequestExistingApplication(1 * 1024, 1, 4, app2);
			ApplicationAttemptId app3 = CreateSchedulingRequest(1 * 1024, 1, "queueB", "user1"
				, 1, 1);
			CreateSchedulingRequestExistingApplication(1 * 1024, 1, 2, app3);
			ApplicationAttemptId app4 = CreateSchedulingRequest(1 * 1024, 1, "queueB", "user1"
				, 1, 3);
			CreateSchedulingRequestExistingApplication(1 * 1024, 1, 4, app4);
			scheduler.Update();
			scheduler.GetQueueManager().GetLeafQueue("queueA", true).SetPolicy(SchedulingPolicy
				.Parse("fifo"));
			scheduler.GetQueueManager().GetLeafQueue("queueB", true).SetPolicy(SchedulingPolicy
				.Parse("fair"));
			// Sufficient node check-ins to fully schedule containers
			NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
			NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
			for (int i = 0; i < 4; i++)
			{
				scheduler.Handle(nodeUpdate1);
				scheduler.Handle(nodeUpdate2);
			}
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetSchedulerApp(app1).GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetSchedulerApp(app2).GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetSchedulerApp(app3).GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetSchedulerApp(app4).GetLiveContainers
				().Count);
			// Now new requests arrive from queueC and default
			CreateSchedulingRequest(1 * 1024, 1, "queueC", "user1", 1, 1);
			CreateSchedulingRequest(1 * 1024, 1, "queueC", "user1", 1, 1);
			CreateSchedulingRequest(1 * 1024, 1, "default", "user1", 1, 1);
			CreateSchedulingRequest(1 * 1024, 1, "default", "user1", 1, 1);
			scheduler.Update();
			// We should be able to claw back one container from queueA and queueB each.
			scheduler.PreemptResources(Resources.CreateResource(2 * 1024));
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetSchedulerApp(app1).GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetSchedulerApp(app3).GetLiveContainers
				().Count);
			// First verify we are adding containers to preemption list for the app.
			// For queueA (fifo), app2 is selected.
			// For queueB (fair), app4 is selected.
			NUnit.Framework.Assert.IsTrue("App2 should have container to be preempted", !Sharpen.Collections
				.Disjoint(scheduler.GetSchedulerApp(app2).GetLiveContainers(), scheduler.GetSchedulerApp
				(app2).GetPreemptionContainers()));
			NUnit.Framework.Assert.IsTrue("App4 should have container to be preempted", !Sharpen.Collections
				.Disjoint(scheduler.GetSchedulerApp(app2).GetLiveContainers(), scheduler.GetSchedulerApp
				(app2).GetPreemptionContainers()));
			// Pretend 15 seconds have passed
			clock.Tick(15);
			// Trigger a kill by insisting we want containers back
			scheduler.PreemptResources(Resources.CreateResource(2 * 1024));
			// At this point the containers should have been killed (since we are not simulating AM)
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(app2).GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(app4).GetLiveContainers
				().Count);
			// Inside each app, containers are sorted according to their priorities.
			// Containers with priority 4 are preempted for app2 and app4.
			ICollection<RMContainer> set = new HashSet<RMContainer>();
			foreach (RMContainer container in scheduler.GetSchedulerApp(app2).GetLiveContainers
				())
			{
				if (container.GetAllocatedPriority().GetPriority() == 4)
				{
					set.AddItem(container);
				}
			}
			foreach (RMContainer container_1 in scheduler.GetSchedulerApp(app4).GetLiveContainers
				())
			{
				if (container_1.GetAllocatedPriority().GetPriority() == 4)
				{
					set.AddItem(container_1);
				}
			}
			NUnit.Framework.Assert.IsTrue("Containers with priority=4 in app2 and app4 should be "
				 + "preempted.", set.IsEmpty());
			// Trigger a kill by insisting we want containers back
			scheduler.PreemptResources(Resources.CreateResource(2 * 1024));
			// Pretend 15 seconds have passed
			clock.Tick(15);
			// We should be able to claw back another container from A and B each.
			// For queueA (fifo), continue preempting from app2.
			// For queueB (fair), even app4 has a lowest priority container with p=4, it
			// still preempts from app3 as app3 is most over fair share.
			scheduler.PreemptResources(Resources.CreateResource(2 * 1024));
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetSchedulerApp(app1).GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(app2).GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(app3).GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(app4).GetLiveContainers
				().Count);
			// Now A and B are below fair share, so preemption shouldn't do anything
			scheduler.PreemptResources(Resources.CreateResource(2 * 1024));
			NUnit.Framework.Assert.IsTrue("App1 should have no container to be preempted", scheduler
				.GetSchedulerApp(app1).GetPreemptionContainers().IsEmpty());
			NUnit.Framework.Assert.IsTrue("App2 should have no container to be preempted", scheduler
				.GetSchedulerApp(app2).GetPreemptionContainers().IsEmpty());
			NUnit.Framework.Assert.IsTrue("App3 should have no container to be preempted", scheduler
				.GetSchedulerApp(app3).GetPreemptionContainers().IsEmpty());
			NUnit.Framework.Assert.IsTrue("App4 should have no container to be preempted", scheduler
				.GetSchedulerApp(app4).GetPreemptionContainers().IsEmpty());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreemptionIsNotDelayedToNextRound()
		{
			conf.SetLong(FairSchedulerConfiguration.PreemptionInterval, 5000);
			conf.SetLong(FairSchedulerConfiguration.WaitTimeBeforeKill, 10000);
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			conf.Set(FairSchedulerConfiguration.UserAsDefaultQueue, "false");
			FairSchedulerTestBase.MockClock clock = new FairSchedulerTestBase.MockClock();
			scheduler.SetClock(clock);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<weight>8</weight>");
			@out.WriteLine("<queue name=\"queueA1\" />");
			@out.WriteLine("<queue name=\"queueA2\" />");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<weight>2</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<defaultFairSharePreemptionTimeout>10</defaultFairSharePreemptionTimeout>"
				);
			@out.WriteLine("<defaultFairSharePreemptionThreshold>.5</defaultFairSharePreemptionThreshold>"
				);
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node of 8G
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Run apps in queueA.A1 and queueB
			ApplicationAttemptId app1 = CreateSchedulingRequest(1 * 1024, 1, "queueA.queueA1"
				, "user1", 7, 1);
			// createSchedulingRequestExistingApplication(1 * 1024, 1, 2, app1);
			ApplicationAttemptId app2 = CreateSchedulingRequest(1 * 1024, 1, "queueB", "user2"
				, 1, 1);
			scheduler.Update();
			NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
			for (int i = 0; i < 8; i++)
			{
				scheduler.Handle(nodeUpdate1);
			}
			// verify if the apps got the containers they requested
			NUnit.Framework.Assert.AreEqual(7, scheduler.GetSchedulerApp(app1).GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(app2).GetLiveContainers
				().Count);
			// Now submit an app in queueA.queueA2
			ApplicationAttemptId app3 = CreateSchedulingRequest(1 * 1024, 1, "queueA.queueA2"
				, "user3", 7, 1);
			scheduler.Update();
			// Let 11 sec pass
			clock.Tick(11);
			scheduler.Update();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource toPreempt = scheduler.ResToPreempt(scheduler
				.GetQueueManager().GetLeafQueue("queueA.queueA2", false), clock.GetTime());
			NUnit.Framework.Assert.AreEqual(3277, toPreempt.GetMemory());
			// verify if the 3 containers required by queueA2 are preempted in the same
			// round
			scheduler.PreemptResources(toPreempt);
			NUnit.Framework.Assert.AreEqual(3, scheduler.GetSchedulerApp(app1).GetPreemptionContainers
				().Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPreemptionDecision()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			FairSchedulerTestBase.MockClock clock = new FairSchedulerTestBase.MockClock();
			scheduler.SetClock(clock);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"default\">");
			@out.WriteLine("<maxResources>0mb,0vcores</maxResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<weight>.25</weight>");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<weight>.25</weight>");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueC\">");
			@out.WriteLine("<weight>.25</weight>");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueD\">");
			@out.WriteLine("<weight>.25</weight>");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>"
				);
			@out.WriteLine("<defaultFairSharePreemptionTimeout>10</defaultFairSharePreemptionTimeout>"
				);
			@out.WriteLine("<defaultFairSharePreemptionThreshold>.5</defaultFairSharePreemptionThreshold>"
				);
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Create four nodes
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(2 * 1024, 2), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(2 * 1024, 2), 2, 
				"127.0.0.2");
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			RMNode node3 = MockNodes.NewNodeInfo(1, Resources.CreateResource(2 * 1024, 2), 3, 
				"127.0.0.3");
			NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
			scheduler.Handle(nodeEvent3);
			// Queue A and B each request three containers
			ApplicationAttemptId app1 = CreateSchedulingRequest(1 * 1024, "queueA", "user1", 
				1, 1);
			ApplicationAttemptId app2 = CreateSchedulingRequest(1 * 1024, "queueA", "user1", 
				1, 2);
			ApplicationAttemptId app3 = CreateSchedulingRequest(1 * 1024, "queueA", "user1", 
				1, 3);
			ApplicationAttemptId app4 = CreateSchedulingRequest(1 * 1024, "queueB", "user1", 
				1, 1);
			ApplicationAttemptId app5 = CreateSchedulingRequest(1 * 1024, "queueB", "user1", 
				1, 2);
			ApplicationAttemptId app6 = CreateSchedulingRequest(1 * 1024, "queueB", "user1", 
				1, 3);
			scheduler.Update();
			// Sufficient node check-ins to fully schedule containers
			for (int i = 0; i < 2; i++)
			{
				NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
				scheduler.Handle(nodeUpdate1);
				NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
				scheduler.Handle(nodeUpdate2);
				NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
				scheduler.Handle(nodeUpdate3);
			}
			// Now new requests arrive from queues C and D
			ApplicationAttemptId app7 = CreateSchedulingRequest(1 * 1024, "queueC", "user1", 
				1, 1);
			ApplicationAttemptId app8 = CreateSchedulingRequest(1 * 1024, "queueC", "user1", 
				1, 2);
			ApplicationAttemptId app9 = CreateSchedulingRequest(1 * 1024, "queueC", "user1", 
				1, 3);
			ApplicationAttemptId app10 = CreateSchedulingRequest(1 * 1024, "queueD", "user1", 
				1, 1);
			ApplicationAttemptId app11 = CreateSchedulingRequest(1 * 1024, "queueD", "user1", 
				1, 2);
			ApplicationAttemptId app12 = CreateSchedulingRequest(1 * 1024, "queueD", "user1", 
				1, 3);
			scheduler.Update();
			FSLeafQueue schedC = scheduler.GetQueueManager().GetLeafQueue("queueC", true);
			FSLeafQueue schedD = scheduler.GetQueueManager().GetLeafQueue("queueD", true);
			NUnit.Framework.Assert.IsTrue(Resources.Equals(Resources.None(), scheduler.ResToPreempt
				(schedC, clock.GetTime())));
			NUnit.Framework.Assert.IsTrue(Resources.Equals(Resources.None(), scheduler.ResToPreempt
				(schedD, clock.GetTime())));
			// After minSharePreemptionTime has passed, they should want to preempt min
			// share.
			clock.Tick(6);
			NUnit.Framework.Assert.AreEqual(1024, scheduler.ResToPreempt(schedC, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(1024, scheduler.ResToPreempt(schedD, clock.GetTime
				()).GetMemory());
			// After fairSharePreemptionTime has passed, they should want to preempt
			// fair share.
			scheduler.Update();
			clock.Tick(6);
			NUnit.Framework.Assert.AreEqual(1536, scheduler.ResToPreempt(schedC, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(1536, scheduler.ResToPreempt(schedD, clock.GetTime
				()).GetMemory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreemptionDecisionWithVariousTimeout()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			FairSchedulerTestBase.MockClock clock = new FairSchedulerTestBase.MockClock();
			scheduler.SetClock(clock);
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
			@out.WriteLine("<weight>2</weight>");
			@out.WriteLine("<minSharePreemptionTimeout>10</minSharePreemptionTimeout>");
			@out.WriteLine("<fairSharePreemptionTimeout>25</fairSharePreemptionTimeout>");
			@out.WriteLine("<queue name=\"queueB1\">");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("<minSharePreemptionTimeout>5</minSharePreemptionTimeout>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB2\">");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("<fairSharePreemptionTimeout>20</fairSharePreemptionTimeout>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueC\">");
			@out.WriteLine("<weight>1</weight>");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.Write("<defaultMinSharePreemptionTimeout>15</defaultMinSharePreemptionTimeout>"
				);
			@out.Write("<defaultFairSharePreemptionTimeout>30</defaultFairSharePreemptionTimeout>"
				);
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Check the min/fair share preemption timeout for each queue
			QueueManager queueMgr = scheduler.GetQueueManager();
			NUnit.Framework.Assert.AreEqual(30000, queueMgr.GetQueue("root").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(30000, queueMgr.GetQueue("default").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(30000, queueMgr.GetQueue("queueA").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(25000, queueMgr.GetQueue("queueB").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(25000, queueMgr.GetQueue("queueB.queueB1").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(20000, queueMgr.GetQueue("queueB.queueB2").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(30000, queueMgr.GetQueue("queueC").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(15000, queueMgr.GetQueue("root").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(15000, queueMgr.GetQueue("default").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(15000, queueMgr.GetQueue("queueA").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(10000, queueMgr.GetQueue("queueB").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(5000, queueMgr.GetQueue("queueB.queueB1").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(10000, queueMgr.GetQueue("queueB.queueB2").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(15000, queueMgr.GetQueue("queueC").GetMinSharePreemptionTimeout
				());
			// Create one big node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(6 * 1024, 6), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Queue A takes all resources
			for (int i = 0; i < 6; i++)
			{
				CreateSchedulingRequest(1 * 1024, "queueA", "user1", 1, 1);
			}
			scheduler.Update();
			// Sufficient node check-ins to fully schedule containers
			NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
			for (int i_1 = 0; i_1 < 6; i_1++)
			{
				scheduler.Handle(nodeUpdate1);
			}
			// Now new requests arrive from queues B1, B2 and C
			CreateSchedulingRequest(1 * 1024, "queueB.queueB1", "user1", 1, 1);
			CreateSchedulingRequest(1 * 1024, "queueB.queueB1", "user1", 1, 2);
			CreateSchedulingRequest(1 * 1024, "queueB.queueB1", "user1", 1, 3);
			CreateSchedulingRequest(1 * 1024, "queueB.queueB2", "user1", 1, 1);
			CreateSchedulingRequest(1 * 1024, "queueB.queueB2", "user1", 1, 2);
			CreateSchedulingRequest(1 * 1024, "queueB.queueB2", "user1", 1, 3);
			CreateSchedulingRequest(1 * 1024, "queueC", "user1", 1, 1);
			CreateSchedulingRequest(1 * 1024, "queueC", "user1", 1, 2);
			CreateSchedulingRequest(1 * 1024, "queueC", "user1", 1, 3);
			scheduler.Update();
			FSLeafQueue queueB1 = queueMgr.GetLeafQueue("queueB.queueB1", true);
			FSLeafQueue queueB2 = queueMgr.GetLeafQueue("queueB.queueB2", true);
			FSLeafQueue queueC = queueMgr.GetLeafQueue("queueC", true);
			NUnit.Framework.Assert.IsTrue(Resources.Equals(Resources.None(), scheduler.ResToPreempt
				(queueB1, clock.GetTime())));
			NUnit.Framework.Assert.IsTrue(Resources.Equals(Resources.None(), scheduler.ResToPreempt
				(queueB2, clock.GetTime())));
			NUnit.Framework.Assert.IsTrue(Resources.Equals(Resources.None(), scheduler.ResToPreempt
				(queueC, clock.GetTime())));
			// After 5 seconds, queueB1 wants to preempt min share
			scheduler.Update();
			clock.Tick(6);
			NUnit.Framework.Assert.AreEqual(1024, scheduler.ResToPreempt(queueB1, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(0, scheduler.ResToPreempt(queueB2, clock.GetTime(
				)).GetMemory());
			NUnit.Framework.Assert.AreEqual(0, scheduler.ResToPreempt(queueC, clock.GetTime()
				).GetMemory());
			// After 10 seconds, queueB2 wants to preempt min share
			scheduler.Update();
			clock.Tick(5);
			NUnit.Framework.Assert.AreEqual(1024, scheduler.ResToPreempt(queueB1, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(1024, scheduler.ResToPreempt(queueB2, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(0, scheduler.ResToPreempt(queueC, clock.GetTime()
				).GetMemory());
			// After 15 seconds, queueC wants to preempt min share
			scheduler.Update();
			clock.Tick(5);
			NUnit.Framework.Assert.AreEqual(1024, scheduler.ResToPreempt(queueB1, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(1024, scheduler.ResToPreempt(queueB2, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(1024, scheduler.ResToPreempt(queueC, clock.GetTime
				()).GetMemory());
			// After 20 seconds, queueB2 should want to preempt fair share
			scheduler.Update();
			clock.Tick(5);
			NUnit.Framework.Assert.AreEqual(1024, scheduler.ResToPreempt(queueB1, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(1536, scheduler.ResToPreempt(queueB2, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(1024, scheduler.ResToPreempt(queueC, clock.GetTime
				()).GetMemory());
			// After 25 seconds, queueB1 should want to preempt fair share
			scheduler.Update();
			clock.Tick(5);
			NUnit.Framework.Assert.AreEqual(1536, scheduler.ResToPreempt(queueB1, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(1536, scheduler.ResToPreempt(queueB2, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(1024, scheduler.ResToPreempt(queueC, clock.GetTime
				()).GetMemory());
			// After 30 seconds, queueC should want to preempt fair share
			scheduler.Update();
			clock.Tick(5);
			NUnit.Framework.Assert.AreEqual(1536, scheduler.ResToPreempt(queueB1, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(1536, scheduler.ResToPreempt(queueB2, clock.GetTime
				()).GetMemory());
			NUnit.Framework.Assert.AreEqual(1536, scheduler.ResToPreempt(queueC, clock.GetTime
				()).GetMemory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBackwardsCompatiblePreemptionConfiguration()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"default\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<queue name=\"queueB1\">");
			@out.WriteLine("<minSharePreemptionTimeout>5</minSharePreemptionTimeout>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB2\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueC\">");
			@out.WriteLine("</queue>");
			@out.Write("<defaultMinSharePreemptionTimeout>15</defaultMinSharePreemptionTimeout>"
				);
			@out.Write("<defaultFairSharePreemptionTimeout>30</defaultFairSharePreemptionTimeout>"
				);
			@out.Write("<fairSharePreemptionTimeout>40</fairSharePreemptionTimeout>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Check the min/fair share preemption timeout for each queue
			QueueManager queueMgr = scheduler.GetQueueManager();
			NUnit.Framework.Assert.AreEqual(30000, queueMgr.GetQueue("root").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(30000, queueMgr.GetQueue("default").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(30000, queueMgr.GetQueue("queueA").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(30000, queueMgr.GetQueue("queueB").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(30000, queueMgr.GetQueue("queueB.queueB1").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(30000, queueMgr.GetQueue("queueB.queueB2").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(30000, queueMgr.GetQueue("queueC").GetFairSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(15000, queueMgr.GetQueue("root").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(15000, queueMgr.GetQueue("default").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(15000, queueMgr.GetQueue("queueA").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(15000, queueMgr.GetQueue("queueB").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(5000, queueMgr.GetQueue("queueB.queueB1").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(15000, queueMgr.GetQueue("queueB.queueB2").GetMinSharePreemptionTimeout
				());
			NUnit.Framework.Assert.AreEqual(15000, queueMgr.GetQueue("queueC").GetMinSharePreemptionTimeout
				());
			// If both exist, we take the default one
			@out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"default\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<queue name=\"queueB1\">");
			@out.WriteLine("<minSharePreemptionTimeout>5</minSharePreemptionTimeout>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB2\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueC\">");
			@out.WriteLine("</queue>");
			@out.Write("<defaultMinSharePreemptionTimeout>15</defaultMinSharePreemptionTimeout>"
				);
			@out.Write("<defaultFairSharePreemptionTimeout>25</defaultFairSharePreemptionTimeout>"
				);
			@out.Write("<fairSharePreemptionTimeout>30</fairSharePreemptionTimeout>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			NUnit.Framework.Assert.AreEqual(25000, queueMgr.GetQueue("root").GetFairSharePreemptionTimeout
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreemptionVariablesForQueueCreatedRuntime()
		{
			conf.Set(FairSchedulerConfiguration.UserAsDefaultQueue, "true");
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Set preemption variables for the root queue
			FSParentQueue root = scheduler.GetQueueManager().GetRootQueue();
			root.SetMinSharePreemptionTimeout(10000);
			root.SetFairSharePreemptionTimeout(15000);
			root.SetFairSharePreemptionThreshold(.6f);
			// User1 submits one application
			ApplicationAttemptId appAttemptId = CreateAppAttemptId(1, 1);
			CreateApplicationWithAMResource(appAttemptId, "default", "user1", null);
			// The user1 queue should inherit the configurations from the root queue
			FSLeafQueue userQueue = scheduler.GetQueueManager().GetLeafQueue("user1", true);
			NUnit.Framework.Assert.AreEqual(1, userQueue.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(10000, userQueue.GetMinSharePreemptionTimeout());
			NUnit.Framework.Assert.AreEqual(15000, userQueue.GetFairSharePreemptionTimeout());
			NUnit.Framework.Assert.AreEqual(.6f, userQueue.GetFairSharePreemptionThreshold(), 
				0.001);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMultipleContainersWaitingForReservation()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Request full capacity of node
			CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue2", "user2", 1);
			ApplicationAttemptId attId2 = CreateSchedulingRequest(1024, "queue3", "user3", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// One container should get reservation and the other should get nothing
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetSchedulerApp(attId1).GetCurrentReservation
				().GetMemory());
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId2).GetCurrentReservation
				().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUserMaxRunningApps()
		{
			// Set max running apps
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<user name=\"user1\">");
			@out.WriteLine("<maxRunningApps>1</maxRunningApps>");
			@out.WriteLine("</user>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8192, 8), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Request for app 1
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			// App 1 should be running
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attId1).GetLiveContainers
				().Count);
			ApplicationAttemptId attId2 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 2 should not be running
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId2).GetLiveContainers
				().Count);
			// Request another container for app 1
			CreateSchedulingRequestExistingApplication(1024, 1, attId1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// Request should be fulfilled
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetSchedulerApp(attId1).GetLiveContainers
				().Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIncreaseQueueMaxRunningAppsOnTheFly()
		{
			string allocBefore = "<?xml version=\"1.0\"?>" + "<allocations>" + "<queue name=\"root\">"
				 + "<queue name=\"queue1\">" + "<maxRunningApps>1</maxRunningApps>" + "</queue>"
				 + "</queue>" + "</allocations>";
			string allocAfter = "<?xml version=\"1.0\"?>" + "<allocations>" + "<queue name=\"root\">"
				 + "<queue name=\"queue1\">" + "<maxRunningApps>3</maxRunningApps>" + "</queue>"
				 + "</queue>" + "</allocations>";
			TestIncreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIncreaseUserMaxRunningAppsOnTheFly()
		{
			string allocBefore = "<?xml version=\"1.0\"?>" + "<allocations>" + "<queue name=\"root\">"
				 + "<queue name=\"queue1\">" + "<maxRunningApps>10</maxRunningApps>" + "</queue>"
				 + "</queue>" + "<user name=\"user1\">" + "<maxRunningApps>1</maxRunningApps>" +
				 "</user>" + "</allocations>";
			string allocAfter = "<?xml version=\"1.0\"?>" + "<allocations>" + "<queue name=\"root\">"
				 + "<queue name=\"queue1\">" + "<maxRunningApps>10</maxRunningApps>" + "</queue>"
				 + "</queue>" + "<user name=\"user1\">" + "<maxRunningApps>3</maxRunningApps>" +
				 "</user>" + "</allocations>";
			TestIncreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
		}

		/// <exception cref="System.Exception"/>
		private void TestIncreaseQueueSettingOnTheFlyInternal(string allocBefore, string 
			allocAfter)
		{
			// Set max running apps
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine(allocBefore);
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8192, 8), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Request for app 1
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			// App 1 should be running
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attId1).GetLiveContainers
				().Count);
			ApplicationAttemptId attId2 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			ApplicationAttemptId attId3 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			ApplicationAttemptId attId4 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 2 should not be running
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId2).GetLiveContainers
				().Count);
			// App 3 should not be running
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId3).GetLiveContainers
				().Count);
			// App 4 should not be running
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId4).GetLiveContainers
				().Count);
			@out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine(allocAfter);
			@out.Close();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 2 should be running
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attId2).GetLiveContainers
				().Count);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 3 should be running
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attId3).GetLiveContainers
				().Count);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 4 should not be running
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId4).GetLiveContainers
				().Count);
			// Now remove app 1
			AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent
				(attId1, RMAppAttemptState.Finished, false);
			scheduler.Handle(appRemovedEvent1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 4 should be running
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attId4).GetLiveContainers
				().Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDecreaseQueueMaxRunningAppsOnTheFly()
		{
			string allocBefore = "<?xml version=\"1.0\"?>" + "<allocations>" + "<queue name=\"root\">"
				 + "<queue name=\"queue1\">" + "<maxRunningApps>3</maxRunningApps>" + "</queue>"
				 + "</queue>" + "</allocations>";
			string allocAfter = "<?xml version=\"1.0\"?>" + "<allocations>" + "<queue name=\"root\">"
				 + "<queue name=\"queue1\">" + "<maxRunningApps>1</maxRunningApps>" + "</queue>"
				 + "</queue>" + "</allocations>";
			TestDecreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDecreaseUserMaxRunningAppsOnTheFly()
		{
			string allocBefore = "<?xml version=\"1.0\"?>" + "<allocations>" + "<queue name=\"root\">"
				 + "<queue name=\"queue1\">" + "<maxRunningApps>10</maxRunningApps>" + "</queue>"
				 + "</queue>" + "<user name=\"user1\">" + "<maxRunningApps>3</maxRunningApps>" +
				 "</user>" + "</allocations>";
			string allocAfter = "<?xml version=\"1.0\"?>" + "<allocations>" + "<queue name=\"root\">"
				 + "<queue name=\"queue1\">" + "<maxRunningApps>10</maxRunningApps>" + "</queue>"
				 + "</queue>" + "<user name=\"user1\">" + "<maxRunningApps>1</maxRunningApps>" +
				 "</user>" + "</allocations>";
			TestDecreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
		}

		/// <exception cref="System.Exception"/>
		private void TestDecreaseQueueSettingOnTheFlyInternal(string allocBefore, string 
			allocAfter)
		{
			// Set max running apps
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine(allocBefore);
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8192, 8), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Request for app 1
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			// App 1 should be running
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attId1).GetLiveContainers
				().Count);
			ApplicationAttemptId attId2 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			ApplicationAttemptId attId3 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			ApplicationAttemptId attId4 = CreateSchedulingRequest(1024, "queue1", "user1", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 2 should be running
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attId2).GetLiveContainers
				().Count);
			// App 3 should be running
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attId3).GetLiveContainers
				().Count);
			// App 4 should not be running
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId4).GetLiveContainers
				().Count);
			@out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine(allocAfter);
			@out.Close();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 2 should still be running
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attId2).GetLiveContainers
				().Count);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 3 should still be running
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attId3).GetLiveContainers
				().Count);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 4 should not be running
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId4).GetLiveContainers
				().Count);
			// Now remove app 1
			AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent
				(attId1, RMAppAttemptState.Finished, false);
			scheduler.Handle(appRemovedEvent1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 4 should not be running
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId4).GetLiveContainers
				().Count);
			// Now remove app 2
			appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(attId2, RMAppAttemptState.
				Finished, false);
			scheduler.Handle(appRemovedEvent1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 4 should not be running
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetSchedulerApp(attId4).GetLiveContainers
				().Count);
			// Now remove app 3
			appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(attId3, RMAppAttemptState.
				Finished, false);
			scheduler.Handle(appRemovedEvent1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// App 4 should be running now
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attId4).GetLiveContainers
				().Count);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReservationWhileMultiplePriorities()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// Add a node
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024, 4), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			ApplicationAttemptId attId = CreateSchedulingRequest(1024, 4, "queue1", "user1", 
				1, 2);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			FSAppAttempt app = scheduler.GetSchedulerApp(attId);
			NUnit.Framework.Assert.AreEqual(1, app.GetLiveContainers().Count);
			ContainerId containerId = scheduler.GetSchedulerApp(attId).GetLiveContainers().GetEnumerator
				().Next().GetContainerId();
			// Cause reservation to be created
			CreateSchedulingRequestExistingApplication(1024, 4, 2, attId);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetRootQueueMetrics().GetAvailableMB
				());
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetRootQueueMetrics().GetAvailableVirtualCores
				());
			// Create request at higher priority
			CreateSchedulingRequestExistingApplication(1024, 4, 1, attId);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app.GetLiveContainers().Count);
			// Reserved container should still be at lower priority
			foreach (RMContainer container in app.GetReservedContainers())
			{
				NUnit.Framework.Assert.AreEqual(2, container.GetReservedPriority().GetPriority());
			}
			// Complete container
			scheduler.Allocate(attId, new AList<ResourceRequest>(), Arrays.AsList(containerId
				), null, null);
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetRootQueueMetrics().GetAvailableMB
				());
			NUnit.Framework.Assert.AreEqual(4, scheduler.GetRootQueueMetrics().GetAvailableVirtualCores
				());
			// Schedule at opening
			scheduler.Update();
			scheduler.Handle(updateEvent);
			// Reserved container (at lower priority) should be run
			ICollection<RMContainer> liveContainers = app.GetLiveContainers();
			NUnit.Framework.Assert.AreEqual(1, liveContainers.Count);
			foreach (RMContainer liveContainer in liveContainers)
			{
				NUnit.Framework.Assert.AreEqual(2, liveContainer.GetContainer().GetPriority().GetPriority
					());
			}
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetRootQueueMetrics().GetAvailableMB
				());
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetRootQueueMetrics().GetAvailableVirtualCores
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclSubmitApplication()
		{
			// Set acl's
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"root\">");
			@out.WriteLine("  <aclSubmitApps> </aclSubmitApps>");
			@out.WriteLine("  <aclAdministerApps> </aclAdministerApps>");
			@out.WriteLine("  <queue name=\"queue1\">");
			@out.WriteLine("    <aclSubmitApps>norealuserhasthisname</aclSubmitApps>");
			@out.WriteLine("    <aclAdministerApps>norealuserhasthisname</aclAdministerApps>"
				);
			@out.WriteLine("  </queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1", "norealuserhasthisname"
				, 1);
			ApplicationAttemptId attId2 = CreateSchedulingRequest(1024, "queue1", "norealuserhasthisname2"
				, 1);
			FSAppAttempt app1 = scheduler.GetSchedulerApp(attId1);
			NUnit.Framework.Assert.IsNotNull("The application was not allowed", app1);
			FSAppAttempt app2 = scheduler.GetSchedulerApp(attId2);
			NUnit.Framework.Assert.IsNull("The application was allowed", app2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMultipleNodesSingleRackRequest()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 1, "127.0.0.1"
				);
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 2, "127.0.0.2"
				);
			RMNode node3 = MockNodes.NewNodeInfo(2, Resources.CreateResource(1024), 3, "127.0.0.3"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			ApplicationAttemptId attemptId = CreateAppAttemptId(this.AppId++, this.AttemptId++
				);
			CreateMockRMApp(attemptId);
			scheduler.AddApplication(attemptId.GetApplicationId(), "queue1", "user1", false);
			scheduler.AddApplicationAttempt(attemptId, false, false);
			// 1 request with 2 nodes on the same rack. another request with 1 node on
			// a different rack
			IList<ResourceRequest> asks = new AList<ResourceRequest>();
			asks.AddItem(CreateResourceRequest(1024, node1.GetHostName(), 1, 1, true));
			asks.AddItem(CreateResourceRequest(1024, node2.GetHostName(), 1, 1, true));
			asks.AddItem(CreateResourceRequest(1024, node3.GetHostName(), 1, 1, true));
			asks.AddItem(CreateResourceRequest(1024, node1.GetRackName(), 1, 1, true));
			asks.AddItem(CreateResourceRequest(1024, node3.GetRackName(), 1, 1, true));
			asks.AddItem(CreateResourceRequest(1024, ResourceRequest.Any, 1, 2, true));
			scheduler.Allocate(attemptId, asks, new AList<ContainerId>(), null, null);
			// node 1 checks in
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent1 = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent1);
			// should assign node local
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(attemptId).GetLiveContainers
				().Count);
			// node 2 checks in
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
			scheduler.Handle(updateEvent2);
			// should assign rack local
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetSchedulerApp(attemptId).GetLiveContainers
				().Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFifoWithinQueue()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(3072, 3), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			// Even if submitted at exact same time, apps will be deterministically
			// ordered by name.
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1", "user1", 2);
			ApplicationAttemptId attId2 = CreateSchedulingRequest(1024, "queue1", "user1", 2);
			FSAppAttempt app1 = scheduler.GetSchedulerApp(attId1);
			FSAppAttempt app2 = scheduler.GetSchedulerApp(attId2);
			FSLeafQueue queue1 = scheduler.GetQueueManager().GetLeafQueue("queue1", true);
			queue1.SetPolicy(new FifoPolicy());
			scheduler.Update();
			// First two containers should go to app 1, third should go to app 2.
			// Because tests set assignmultiple to false, each heartbeat assigns a single
			// container.
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app1.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual(0, app2.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(2, app1.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual(0, app2.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(2, app1.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual(1, app2.GetLiveContainers().Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMaxAssign()
		{
			conf.SetBoolean(FairSchedulerConfiguration.AssignMultiple, true);
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node = MockNodes.NewNodeInfo(1, Resources.CreateResource(16384, 16), 0, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
			scheduler.Handle(nodeEvent);
			ApplicationAttemptId attId = CreateSchedulingRequest(1024, "root.default", "user"
				, 8);
			FSAppAttempt app = scheduler.GetSchedulerApp(attId);
			// set maxAssign to 2: only 2 containers should be allocated
			scheduler.maxAssign = 2;
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Incorrect number of containers allocated", 2, app
				.GetLiveContainers().Count);
			// set maxAssign to -1: all remaining containers should be allocated
			scheduler.maxAssign = -1;
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Incorrect number of containers allocated", 8, app
				.GetLiveContainers().Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMaxAssignWithZeroMemoryContainers()
		{
			conf.SetBoolean(FairSchedulerConfiguration.AssignMultiple, true);
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, 0);
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node = MockNodes.NewNodeInfo(1, Resources.CreateResource(16384, 16), 0, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
			scheduler.Handle(nodeEvent);
			ApplicationAttemptId attId = CreateSchedulingRequest(0, 1, "root.default", "user"
				, 8);
			FSAppAttempt app = scheduler.GetSchedulerApp(attId);
			// set maxAssign to 2: only 2 containers should be allocated
			scheduler.maxAssign = 2;
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Incorrect number of containers allocated", 2, app
				.GetLiveContainers().Count);
			// set maxAssign to -1: all remaining containers should be allocated
			scheduler.maxAssign = -1;
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Incorrect number of containers allocated", 8, app
				.GetLiveContainers().Count);
		}

		/// <summary>
		/// Test to verify the behavior of
		/// <see cref="Schedulable.AssignContainer(FSSchedulerNode)"/>
		/// )
		/// Create two queues under root (fifoQueue and fairParent), and two queues
		/// under fairParent (fairChild1 and fairChild2). Submit two apps to the
		/// fifoQueue and one each to the fairChild* queues, all apps requiring 4
		/// containers each of the total 16 container capacity
		/// Assert the number of containers for each app after 4, 8, 12 and 16 updates.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAssignContainer()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			string user = "user1";
			string fifoQueue = "fifo";
			string fairParent = "fairParent";
			string fairChild1 = fairParent + ".fairChild1";
			string fairChild2 = fairParent + ".fairChild2";
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8192, 8), 1, "127.0.0.1"
				);
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8192, 8), 2, "127.0.0.2"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			scheduler.Handle(nodeEvent1);
			scheduler.Handle(nodeEvent2);
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, fifoQueue, user, 4);
			ApplicationAttemptId attId2 = CreateSchedulingRequest(1024, fairChild1, user, 4);
			ApplicationAttemptId attId3 = CreateSchedulingRequest(1024, fairChild2, user, 4);
			ApplicationAttemptId attId4 = CreateSchedulingRequest(1024, fifoQueue, user, 4);
			FSAppAttempt app1 = scheduler.GetSchedulerApp(attId1);
			FSAppAttempt app2 = scheduler.GetSchedulerApp(attId2);
			FSAppAttempt app3 = scheduler.GetSchedulerApp(attId3);
			FSAppAttempt app4 = scheduler.GetSchedulerApp(attId4);
			scheduler.GetQueueManager().GetLeafQueue(fifoQueue, true).SetPolicy(SchedulingPolicy
				.Parse("fifo"));
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent1 = new NodeUpdateSchedulerEvent(node1);
			NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
			for (int i = 0; i < 8; i++)
			{
				scheduler.Handle(updateEvent1);
				scheduler.Handle(updateEvent2);
				if ((i + 1) % 2 == 0)
				{
					// 4 node updates: fifoQueue should have received 2, and fairChild*
					// should have received one each
					string Err = "Wrong number of assigned containers after " + (i + 1) + " updates";
					if (i < 4)
					{
						// app1 req still not met
						NUnit.Framework.Assert.AreEqual(Err, (i + 1), app1.GetLiveContainers().Count);
						NUnit.Framework.Assert.AreEqual(Err, 0, app4.GetLiveContainers().Count);
					}
					else
					{
						// app1 req has been met, app4 should be served now
						NUnit.Framework.Assert.AreEqual(Err, 4, app1.GetLiveContainers().Count);
						NUnit.Framework.Assert.AreEqual(Err, (i - 3), app4.GetLiveContainers().Count);
					}
					NUnit.Framework.Assert.AreEqual(Err, (i + 1) / 2, app2.GetLiveContainers().Count);
					NUnit.Framework.Assert.AreEqual(Err, (i + 1) / 2, app3.GetLiveContainers().Count);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotAllowSubmitApplication()
		{
			// Set acl's
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"root\">");
			@out.WriteLine("  <aclSubmitApps> </aclSubmitApps>");
			@out.WriteLine("  <aclAdministerApps> </aclAdministerApps>");
			@out.WriteLine("  <queue name=\"queue1\">");
			@out.WriteLine("    <aclSubmitApps>userallow</aclSubmitApps>");
			@out.WriteLine("    <aclAdministerApps>userallow</aclAdministerApps>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			int appId = this.AppId++;
			string user = "usernotallow";
			string queue = "queue1";
			ApplicationId applicationId = MockApps.NewAppID(appId);
			string name = MockApps.NewAppName();
			ApplicationMasterService masterService = new ApplicationMasterService(resourceManager
				.GetRMContext(), scheduler);
			ApplicationSubmissionContext submissionContext = new ApplicationSubmissionContextPBImpl
				();
			ContainerLaunchContext clc = BuilderUtils.NewContainerLaunchContext(null, null, null
				, null, null, null);
			submissionContext.SetApplicationId(applicationId);
			submissionContext.SetAMContainerSpec(clc);
			RMApp application = new RMAppImpl(applicationId, resourceManager.GetRMContext(), 
				conf, name, user, queue, submissionContext, scheduler, masterService, Runtime.CurrentTimeMillis
				(), "YARN", null, null);
			resourceManager.GetRMContext().GetRMApps().PutIfAbsent(applicationId, application
				);
			application.Handle(new RMAppEvent(applicationId, RMAppEventType.Start));
			int MaxTries = 20;
			int numTries = 0;
			while (!application.GetState().Equals(RMAppState.Submitted) && numTries < MaxTries
				)
			{
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception ex)
				{
					Sharpen.Runtime.PrintStackTrace(ex);
				}
				numTries++;
			}
			NUnit.Framework.Assert.AreEqual("The application doesn't reach SUBMITTED.", RMAppState
				.Submitted, application.GetState());
			ApplicationAttemptId attId = ApplicationAttemptId.NewInstance(applicationId, this
				.AttemptId++);
			scheduler.AddApplication(attId.GetApplicationId(), queue, user, false);
			numTries = 0;
			while (application.GetFinishTime() == 0 && numTries < MaxTries)
			{
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception ex)
				{
					Sharpen.Runtime.PrintStackTrace(ex);
				}
				numTries++;
			}
			NUnit.Framework.Assert.AreEqual(FinalApplicationStatus.Failed, application.GetFinalApplicationStatus
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveNodeUpdatesRootQueueMetrics()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetRootQueueMetrics().GetAvailableMB
				());
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetRootQueueMetrics().GetAvailableVirtualCores
				());
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024, 4), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent addEvent = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(addEvent);
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetRootQueueMetrics().GetAvailableMB
				());
			NUnit.Framework.Assert.AreEqual(4, scheduler.GetRootQueueMetrics().GetAvailableVirtualCores
				());
			scheduler.Update();
			// update shouldn't change things
			NUnit.Framework.Assert.AreEqual(1024, scheduler.GetRootQueueMetrics().GetAvailableMB
				());
			NUnit.Framework.Assert.AreEqual(4, scheduler.GetRootQueueMetrics().GetAvailableVirtualCores
				());
			NodeRemovedSchedulerEvent removeEvent = new NodeRemovedSchedulerEvent(node1);
			scheduler.Handle(removeEvent);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetRootQueueMetrics().GetAvailableMB
				());
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetRootQueueMetrics().GetAvailableVirtualCores
				());
			scheduler.Update();
			// update shouldn't change things
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetRootQueueMetrics().GetAvailableMB
				());
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetRootQueueMetrics().GetAvailableVirtualCores
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStrictLocality()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 2, "127.0.0.2"
				);
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1", "user1", 0);
			ResourceRequest nodeRequest = CreateResourceRequest(1024, node1.GetHostName(), 1, 
				1, true);
			ResourceRequest rackRequest = CreateResourceRequest(1024, node1.GetRackName(), 1, 
				1, false);
			ResourceRequest anyRequest = CreateResourceRequest(1024, ResourceRequest.Any, 1, 
				1, false);
			CreateSchedulingRequestExistingApplication(nodeRequest, attId1);
			CreateSchedulingRequestExistingApplication(rackRequest, attId1);
			CreateSchedulingRequestExistingApplication(anyRequest, attId1);
			scheduler.Update();
			NodeUpdateSchedulerEvent node1UpdateEvent = new NodeUpdateSchedulerEvent(node1);
			NodeUpdateSchedulerEvent node2UpdateEvent = new NodeUpdateSchedulerEvent(node2);
			// no matter how many heartbeats, node2 should never get a container
			FSAppAttempt app = scheduler.GetSchedulerApp(attId1);
			for (int i = 0; i < 10; i++)
			{
				scheduler.Handle(node2UpdateEvent);
				NUnit.Framework.Assert.AreEqual(0, app.GetLiveContainers().Count);
				NUnit.Framework.Assert.AreEqual(0, app.GetReservedContainers().Count);
			}
			// then node1 should get the container
			scheduler.Handle(node1UpdateEvent);
			NUnit.Framework.Assert.AreEqual(1, app.GetLiveContainers().Count);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCancelStrictLocality()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 2, "127.0.0.2"
				);
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1", "user1", 0);
			ResourceRequest nodeRequest = CreateResourceRequest(1024, node1.GetHostName(), 1, 
				1, true);
			ResourceRequest rackRequest = CreateResourceRequest(1024, "rack1", 1, 1, false);
			ResourceRequest anyRequest = CreateResourceRequest(1024, ResourceRequest.Any, 1, 
				1, false);
			CreateSchedulingRequestExistingApplication(nodeRequest, attId1);
			CreateSchedulingRequestExistingApplication(rackRequest, attId1);
			CreateSchedulingRequestExistingApplication(anyRequest, attId1);
			scheduler.Update();
			NodeUpdateSchedulerEvent node2UpdateEvent = new NodeUpdateSchedulerEvent(node2);
			// no matter how many heartbeats, node2 should never get a container
			FSAppAttempt app = scheduler.GetSchedulerApp(attId1);
			for (int i = 0; i < 10; i++)
			{
				scheduler.Handle(node2UpdateEvent);
				NUnit.Framework.Assert.AreEqual(0, app.GetLiveContainers().Count);
			}
			// relax locality
			IList<ResourceRequest> update = Arrays.AsList(CreateResourceRequest(1024, node1.GetHostName
				(), 1, 0, true), CreateResourceRequest(1024, "rack1", 1, 0, true), CreateResourceRequest
				(1024, ResourceRequest.Any, 1, 1, true));
			scheduler.Allocate(attId1, update, new AList<ContainerId>(), null, null);
			// then node2 should get the container
			scheduler.Handle(node2UpdateEvent);
			NUnit.Framework.Assert.AreEqual(1, app.GetLiveContainers().Count);
		}

		/// <summary>
		/// If we update our ask to strictly request a node, it doesn't make sense to keep
		/// a reservation on another.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReservationsStrictLocality()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 1, "127.0.0.1"
				);
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 2, "127.0.0.2"
				);
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent2);
			ApplicationAttemptId attId = CreateSchedulingRequest(1024, "queue1", "user1", 0);
			FSAppAttempt app = scheduler.GetSchedulerApp(attId);
			ResourceRequest nodeRequest = CreateResourceRequest(1024, node2.GetHostName(), 1, 
				2, true);
			ResourceRequest rackRequest = CreateResourceRequest(1024, "rack1", 1, 2, true);
			ResourceRequest anyRequest = CreateResourceRequest(1024, ResourceRequest.Any, 1, 
				2, false);
			CreateSchedulingRequestExistingApplication(nodeRequest, attId);
			CreateSchedulingRequestExistingApplication(rackRequest, attId);
			CreateSchedulingRequestExistingApplication(anyRequest, attId);
			scheduler.Update();
			NodeUpdateSchedulerEvent nodeUpdateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(nodeUpdateEvent);
			NUnit.Framework.Assert.AreEqual(1, app.GetLiveContainers().Count);
			scheduler.Handle(nodeUpdateEvent);
			NUnit.Framework.Assert.AreEqual(1, app.GetReservedContainers().Count);
			// now, make our request node-specific (on a different node)
			rackRequest = CreateResourceRequest(1024, "rack1", 1, 1, false);
			anyRequest = CreateResourceRequest(1024, ResourceRequest.Any, 1, 1, false);
			scheduler.Allocate(attId, Arrays.AsList(rackRequest, anyRequest), new AList<ContainerId
				>(), null, null);
			scheduler.Handle(nodeUpdateEvent);
			NUnit.Framework.Assert.AreEqual(0, app.GetReservedContainers().Count);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNoMoreCpuOnNode()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(2048, 1), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			ApplicationAttemptId attId = CreateSchedulingRequest(1024, 1, "default", "user1", 
				2);
			FSAppAttempt app = scheduler.GetSchedulerApp(attId);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app.GetLiveContainers().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBasicDRFAssignment()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node = MockNodes.NewNodeInfo(1, BuilderUtils.NewResource(8192, 5));
			NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
			scheduler.Handle(nodeEvent);
			ApplicationAttemptId appAttId1 = CreateSchedulingRequest(2048, 1, "queue1", "user1"
				, 2);
			FSAppAttempt app1 = scheduler.GetSchedulerApp(appAttId1);
			ApplicationAttemptId appAttId2 = CreateSchedulingRequest(1024, 2, "queue1", "user1"
				, 2);
			FSAppAttempt app2 = scheduler.GetSchedulerApp(appAttId2);
			DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
			drfPolicy.Initialize(scheduler.GetClusterResource());
			scheduler.GetQueueManager().GetQueue("queue1").SetPolicy(drfPolicy);
			scheduler.Update();
			// First both apps get a container
			// Then the first gets another container because its dominant share of
			// 2048/8192 is less than the other's of 2/5
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app1.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual(0, app2.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app1.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual(1, app2.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(2, app1.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual(1, app2.GetLiveContainers().Count);
		}

		/// <summary>Two apps on one queue, one app on another</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBasicDRFWithQueues()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node = MockNodes.NewNodeInfo(1, BuilderUtils.NewResource(8192, 7), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
			scheduler.Handle(nodeEvent);
			ApplicationAttemptId appAttId1 = CreateSchedulingRequest(3072, 1, "queue1", "user1"
				, 2);
			FSAppAttempt app1 = scheduler.GetSchedulerApp(appAttId1);
			ApplicationAttemptId appAttId2 = CreateSchedulingRequest(2048, 2, "queue1", "user1"
				, 2);
			FSAppAttempt app2 = scheduler.GetSchedulerApp(appAttId2);
			ApplicationAttemptId appAttId3 = CreateSchedulingRequest(1024, 2, "queue2", "user1"
				, 2);
			FSAppAttempt app3 = scheduler.GetSchedulerApp(appAttId3);
			DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
			drfPolicy.Initialize(scheduler.GetClusterResource());
			scheduler.GetQueueManager().GetQueue("root").SetPolicy(drfPolicy);
			scheduler.GetQueueManager().GetQueue("queue1").SetPolicy(drfPolicy);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app1.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app3.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(2, app3.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app2.GetLiveContainers().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDRFHierarchicalQueues()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node = MockNodes.NewNodeInfo(1, BuilderUtils.NewResource(12288, 12), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
			scheduler.Handle(nodeEvent);
			ApplicationAttemptId appAttId1 = CreateSchedulingRequest(3074, 1, "queue1.subqueue1"
				, "user1", 2);
			Sharpen.Thread.Sleep(3);
			// so that start times will be different
			FSAppAttempt app1 = scheduler.GetSchedulerApp(appAttId1);
			ApplicationAttemptId appAttId2 = CreateSchedulingRequest(1024, 3, "queue1.subqueue1"
				, "user1", 2);
			Sharpen.Thread.Sleep(3);
			// so that start times will be different
			FSAppAttempt app2 = scheduler.GetSchedulerApp(appAttId2);
			ApplicationAttemptId appAttId3 = CreateSchedulingRequest(2048, 2, "queue1.subqueue2"
				, "user1", 2);
			Sharpen.Thread.Sleep(3);
			// so that start times will be different
			FSAppAttempt app3 = scheduler.GetSchedulerApp(appAttId3);
			ApplicationAttemptId appAttId4 = CreateSchedulingRequest(1024, 2, "queue2", "user1"
				, 2);
			Sharpen.Thread.Sleep(3);
			// so that start times will be different
			FSAppAttempt app4 = scheduler.GetSchedulerApp(appAttId4);
			DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
			drfPolicy.Initialize(scheduler.GetClusterResource());
			scheduler.GetQueueManager().GetQueue("root").SetPolicy(drfPolicy);
			scheduler.GetQueueManager().GetQueue("queue1").SetPolicy(drfPolicy);
			scheduler.GetQueueManager().GetQueue("queue1.subqueue1").SetPolicy(drfPolicy);
			scheduler.Update();
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
			scheduler.Handle(updateEvent);
			// app1 gets first container because it asked first
			NUnit.Framework.Assert.AreEqual(1, app1.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			// app4 gets second container because it's on queue2
			NUnit.Framework.Assert.AreEqual(1, app4.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			// app4 gets another container because queue2's dominant share of memory
			// is still less than queue1's of cpu
			NUnit.Framework.Assert.AreEqual(2, app4.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			// app3 gets one because queue1 gets one and queue1.subqueue2 is behind
			// queue1.subqueue1
			NUnit.Framework.Assert.AreEqual(1, app3.GetLiveContainers().Count);
			scheduler.Handle(updateEvent);
			// app4 would get another one, but it doesn't have any requests
			// queue1.subqueue2 is still using less than queue1.subqueue1, so it
			// gets another
			NUnit.Framework.Assert.AreEqual(2, app3.GetLiveContainers().Count);
			// queue1.subqueue1 is behind again, so it gets one, which it gives to app2
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app2.GetLiveContainers().Count);
			// at this point, we've used all our CPU up, so nobody else should get a container
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(1, app1.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual(1, app2.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual(2, app3.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual(2, app4.GetLiveContainers().Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHostPortNodeName()
		{
			conf.SetBoolean(YarnConfiguration.RmSchedulerIncludePortInNodeName, true);
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 1, "127.0.0.1"
				, 1);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024), 2, "127.0.0.1"
				, 2);
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1", "user1", 0);
			ResourceRequest nodeRequest = CreateResourceRequest(1024, node1.GetNodeID().GetHost
				() + ":" + node1.GetNodeID().GetPort(), 1, 1, true);
			ResourceRequest rackRequest = CreateResourceRequest(1024, node1.GetRackName(), 1, 
				1, false);
			ResourceRequest anyRequest = CreateResourceRequest(1024, ResourceRequest.Any, 1, 
				1, false);
			CreateSchedulingRequestExistingApplication(nodeRequest, attId1);
			CreateSchedulingRequestExistingApplication(rackRequest, attId1);
			CreateSchedulingRequestExistingApplication(anyRequest, attId1);
			scheduler.Update();
			NodeUpdateSchedulerEvent node1UpdateEvent = new NodeUpdateSchedulerEvent(node1);
			NodeUpdateSchedulerEvent node2UpdateEvent = new NodeUpdateSchedulerEvent(node2);
			// no matter how many heartbeats, node2 should never get a container  
			FSAppAttempt app = scheduler.GetSchedulerApp(attId1);
			for (int i = 0; i < 10; i++)
			{
				scheduler.Handle(node2UpdateEvent);
				NUnit.Framework.Assert.AreEqual(0, app.GetLiveContainers().Count);
				NUnit.Framework.Assert.AreEqual(0, app.GetReservedContainers().Count);
			}
			// then node1 should get the container  
			scheduler.Handle(node1UpdateEvent);
			NUnit.Framework.Assert.AreEqual(1, app.GetLiveContainers().Count);
		}

		private void VerifyAppRunnable(ApplicationAttemptId attId, bool runnable)
		{
			FSAppAttempt app = scheduler.GetSchedulerApp(attId);
			FSLeafQueue queue = ((FSLeafQueue)app.GetQueue());
			NUnit.Framework.Assert.AreEqual(runnable, queue.IsRunnableApp(app));
			NUnit.Framework.Assert.AreEqual(!runnable, queue.IsNonRunnableApp(app));
		}

		private void VerifyQueueNumRunnable(string queueName, int numRunnableInQueue, int
			 numNonRunnableInQueue)
		{
			FSLeafQueue queue = scheduler.GetQueueManager().GetLeafQueue(queueName, false);
			NUnit.Framework.Assert.AreEqual(numRunnableInQueue, queue.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(numNonRunnableInQueue, queue.GetNumNonRunnableApps
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUserAndQueueMaxRunningApps()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queue1\">");
			@out.WriteLine("<maxRunningApps>2</maxRunningApps>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<user name=\"user1\">");
			@out.WriteLine("<maxRunningApps>1</maxRunningApps>");
			@out.WriteLine("</user>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// exceeds no limits
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1", "user1");
			VerifyAppRunnable(attId1, true);
			VerifyQueueNumRunnable("queue1", 1, 0);
			// exceeds user limit
			ApplicationAttemptId attId2 = CreateSchedulingRequest(1024, "queue2", "user1");
			VerifyAppRunnable(attId2, false);
			VerifyQueueNumRunnable("queue2", 0, 1);
			// exceeds no limits
			ApplicationAttemptId attId3 = CreateSchedulingRequest(1024, "queue1", "user2");
			VerifyAppRunnable(attId3, true);
			VerifyQueueNumRunnable("queue1", 2, 0);
			// exceeds queue limit
			ApplicationAttemptId attId4 = CreateSchedulingRequest(1024, "queue1", "user2");
			VerifyAppRunnable(attId4, false);
			VerifyQueueNumRunnable("queue1", 2, 1);
			// Remove app 1 and both app 2 and app 4 should becomes runnable in its place
			AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent
				(attId1, RMAppAttemptState.Finished, false);
			scheduler.Handle(appRemovedEvent1);
			VerifyAppRunnable(attId2, true);
			VerifyQueueNumRunnable("queue2", 1, 0);
			VerifyAppRunnable(attId4, true);
			VerifyQueueNumRunnable("queue1", 2, 0);
			// A new app to queue1 should not be runnable
			ApplicationAttemptId attId5 = CreateSchedulingRequest(1024, "queue1", "user2");
			VerifyAppRunnable(attId5, false);
			VerifyQueueNumRunnable("queue1", 2, 1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueMaxAMShare()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queue1\">");
			@out.WriteLine("<maxAMShare>0.2</maxAMShare>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node = MockNodes.NewNodeInfo(1, Resources.CreateResource(20480, 20), 0, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
			scheduler.Handle(nodeEvent);
			scheduler.Update();
			FSLeafQueue queue1 = scheduler.GetQueueManager().GetLeafQueue("queue1", true);
			NUnit.Framework.Assert.AreEqual("Queue queue1's fair share should be 0", 0, queue1
				.GetFairShare().GetMemory());
			CreateSchedulingRequest(1 * 1024, "root.default", "user1");
			scheduler.Update();
			scheduler.Handle(updateEvent);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource amResource1 = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource amResource2 = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2048, 2);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource amResource3 = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1860, 2);
			int amPriority = RMAppAttemptImpl.AmContainerPriority.GetPriority();
			// Exceeds no limits
			ApplicationAttemptId attId1 = CreateAppAttemptId(1, 1);
			CreateApplicationWithAMResource(attId1, "queue1", "user1", amResource1);
			CreateSchedulingRequestExistingApplication(1024, 1, amPriority, attId1);
			FSAppAttempt app1 = scheduler.GetSchedulerApp(attId1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application1's AM requests 1024 MB memory", 1024
				, app1.GetAMResource().GetMemory());
			NUnit.Framework.Assert.AreEqual("Application1's AM should be running", 1, app1.GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 1024 MB memory"
				, 1024, queue1.GetAmResourceUsage().GetMemory());
			// Exceeds no limits
			ApplicationAttemptId attId2 = CreateAppAttemptId(2, 1);
			CreateApplicationWithAMResource(attId2, "queue1", "user1", amResource1);
			CreateSchedulingRequestExistingApplication(1024, 1, amPriority, attId2);
			FSAppAttempt app2 = scheduler.GetSchedulerApp(attId2);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application2's AM requests 1024 MB memory", 1024
				, app2.GetAMResource().GetMemory());
			NUnit.Framework.Assert.AreEqual("Application2's AM should be running", 1, app2.GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 2048 MB memory"
				, 2048, queue1.GetAmResourceUsage().GetMemory());
			// Exceeds queue limit
			ApplicationAttemptId attId3 = CreateAppAttemptId(3, 1);
			CreateApplicationWithAMResource(attId3, "queue1", "user1", amResource1);
			CreateSchedulingRequestExistingApplication(1024, 1, amPriority, attId3);
			FSAppAttempt app3 = scheduler.GetSchedulerApp(attId3);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application3's AM requests 1024 MB memory", 1024
				, app3.GetAMResource().GetMemory());
			NUnit.Framework.Assert.AreEqual("Application3's AM should not be running", 0, app3
				.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 2048 MB memory"
				, 2048, queue1.GetAmResourceUsage().GetMemory());
			// Still can run non-AM container
			CreateSchedulingRequestExistingApplication(1024, 1, attId1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application1 should have two running containers"
				, 2, app1.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 2048 MB memory"
				, 2048, queue1.GetAmResourceUsage().GetMemory());
			// Remove app1, app3's AM should become running
			AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent
				(attId1, RMAppAttemptState.Finished, false);
			scheduler.Update();
			scheduler.Handle(appRemovedEvent1);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application1's AM should be finished", 0, app1.GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual("Application3's AM should be running", 1, app3.GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 2048 MB memory"
				, 2048, queue1.GetAmResourceUsage().GetMemory());
			// Exceeds queue limit
			ApplicationAttemptId attId4 = CreateAppAttemptId(4, 1);
			CreateApplicationWithAMResource(attId4, "queue1", "user1", amResource2);
			CreateSchedulingRequestExistingApplication(2048, 2, amPriority, attId4);
			FSAppAttempt app4 = scheduler.GetSchedulerApp(attId4);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application4's AM requests 2048 MB memory", 2048
				, app4.GetAMResource().GetMemory());
			NUnit.Framework.Assert.AreEqual("Application4's AM should not be running", 0, app4
				.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 2048 MB memory"
				, 2048, queue1.GetAmResourceUsage().GetMemory());
			// Exceeds queue limit
			ApplicationAttemptId attId5 = CreateAppAttemptId(5, 1);
			CreateApplicationWithAMResource(attId5, "queue1", "user1", amResource2);
			CreateSchedulingRequestExistingApplication(2048, 2, amPriority, attId5);
			FSAppAttempt app5 = scheduler.GetSchedulerApp(attId5);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application5's AM requests 2048 MB memory", 2048
				, app5.GetAMResource().GetMemory());
			NUnit.Framework.Assert.AreEqual("Application5's AM should not be running", 0, app5
				.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 2048 MB memory"
				, 2048, queue1.GetAmResourceUsage().GetMemory());
			// Remove un-running app doesn't affect others
			AppAttemptRemovedSchedulerEvent appRemovedEvent4 = new AppAttemptRemovedSchedulerEvent
				(attId4, RMAppAttemptState.Killed, false);
			scheduler.Handle(appRemovedEvent4);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application5's AM should not be running", 0, app5
				.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 2048 MB memory"
				, 2048, queue1.GetAmResourceUsage().GetMemory());
			// Remove app2 and app3, app5's AM should become running
			AppAttemptRemovedSchedulerEvent appRemovedEvent2 = new AppAttemptRemovedSchedulerEvent
				(attId2, RMAppAttemptState.Finished, false);
			AppAttemptRemovedSchedulerEvent appRemovedEvent3 = new AppAttemptRemovedSchedulerEvent
				(attId3, RMAppAttemptState.Finished, false);
			scheduler.Handle(appRemovedEvent2);
			scheduler.Handle(appRemovedEvent3);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application2's AM should be finished", 0, app2.GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual("Application3's AM should be finished", 0, app3.GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual("Application5's AM should be running", 1, app5.GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 2048 MB memory"
				, 2048, queue1.GetAmResourceUsage().GetMemory());
			// Check amResource normalization
			ApplicationAttemptId attId6 = CreateAppAttemptId(6, 1);
			CreateApplicationWithAMResource(attId6, "queue1", "user1", amResource3);
			CreateSchedulingRequestExistingApplication(1860, 2, amPriority, attId6);
			FSAppAttempt app6 = scheduler.GetSchedulerApp(attId6);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application6's AM should not be running", 0, app6
				.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual("Application6's AM requests 2048 MB memory", 2048
				, app6.GetAMResource().GetMemory());
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 2048 MB memory"
				, 2048, queue1.GetAmResourceUsage().GetMemory());
			// Remove all apps
			AppAttemptRemovedSchedulerEvent appRemovedEvent5 = new AppAttemptRemovedSchedulerEvent
				(attId5, RMAppAttemptState.Finished, false);
			AppAttemptRemovedSchedulerEvent appRemovedEvent6 = new AppAttemptRemovedSchedulerEvent
				(attId6, RMAppAttemptState.Finished, false);
			scheduler.Handle(appRemovedEvent5);
			scheduler.Handle(appRemovedEvent6);
			scheduler.Update();
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 0", 0, queue1
				.GetAmResourceUsage().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueMaxAMShareDefault()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queue1\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queue2\">");
			@out.WriteLine("<maxAMShare>0.4</maxAMShare>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queue3\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queue4\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queue5\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			RMNode node = MockNodes.NewNodeInfo(1, Resources.CreateResource(8192, 20), 0, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
			scheduler.Handle(nodeEvent);
			scheduler.Update();
			FSLeafQueue queue1 = scheduler.GetQueueManager().GetLeafQueue("queue1", true);
			NUnit.Framework.Assert.AreEqual("Queue queue1's fair share should be 0", 0, queue1
				.GetFairShare().GetMemory());
			FSLeafQueue queue2 = scheduler.GetQueueManager().GetLeafQueue("queue2", true);
			NUnit.Framework.Assert.AreEqual("Queue queue2's fair share should be 0", 0, queue2
				.GetFairShare().GetMemory());
			FSLeafQueue queue3 = scheduler.GetQueueManager().GetLeafQueue("queue3", true);
			NUnit.Framework.Assert.AreEqual("Queue queue3's fair share should be 0", 0, queue3
				.GetFairShare().GetMemory());
			FSLeafQueue queue4 = scheduler.GetQueueManager().GetLeafQueue("queue4", true);
			NUnit.Framework.Assert.AreEqual("Queue queue4's fair share should be 0", 0, queue4
				.GetFairShare().GetMemory());
			FSLeafQueue queue5 = scheduler.GetQueueManager().GetLeafQueue("queue5", true);
			NUnit.Framework.Assert.AreEqual("Queue queue5's fair share should be 0", 0, queue5
				.GetFairShare().GetMemory());
			IList<string> queues = Arrays.AsList("root.queue3", "root.queue4", "root.queue5");
			foreach (string queue in queues)
			{
				CreateSchedulingRequest(1 * 1024, queue, "user1");
				scheduler.Update();
				scheduler.Handle(updateEvent);
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Resource amResource1 = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1);
			int amPriority = RMAppAttemptImpl.AmContainerPriority.GetPriority();
			// The fair share is 2048 MB, and the default maxAMShare is 0.5f,
			// so the AM is accepted.
			ApplicationAttemptId attId1 = CreateAppAttemptId(1, 1);
			CreateApplicationWithAMResource(attId1, "queue1", "test1", amResource1);
			CreateSchedulingRequestExistingApplication(1024, 1, amPriority, attId1);
			FSAppAttempt app1 = scheduler.GetSchedulerApp(attId1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application1's AM requests 1024 MB memory", 1024
				, app1.GetAMResource().GetMemory());
			NUnit.Framework.Assert.AreEqual("Application1's AM should be running", 1, app1.GetLiveContainers
				().Count);
			NUnit.Framework.Assert.AreEqual("Queue1's AM resource usage should be 1024 MB memory"
				, 1024, queue1.GetAmResourceUsage().GetMemory());
			// Now the fair share is 1639 MB, and the maxAMShare is 0.4f,
			// so the AM is not accepted.
			ApplicationAttemptId attId2 = CreateAppAttemptId(2, 1);
			CreateApplicationWithAMResource(attId2, "queue2", "test1", amResource1);
			CreateSchedulingRequestExistingApplication(1024, 1, amPriority, attId2);
			FSAppAttempt app2 = scheduler.GetSchedulerApp(attId2);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Application2's AM requests 1024 MB memory", 1024
				, app2.GetAMResource().GetMemory());
			NUnit.Framework.Assert.AreEqual("Application2's AM should not be running", 0, app2
				.GetLiveContainers().Count);
			NUnit.Framework.Assert.AreEqual("Queue2's AM resource usage should be 0 MB memory"
				, 0, queue2.GetAmResourceUsage().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxRunningAppsHierarchicalQueues()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			FairSchedulerTestBase.MockClock clock = new FairSchedulerTestBase.MockClock();
			scheduler.SetClock(clock);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queue1\">");
			@out.WriteLine("  <maxRunningApps>3</maxRunningApps>");
			@out.WriteLine("  <queue name=\"sub1\"></queue>");
			@out.WriteLine("  <queue name=\"sub2\"></queue>");
			@out.WriteLine("  <queue name=\"sub3\">");
			@out.WriteLine("    <maxRunningApps>1</maxRunningApps>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			// exceeds no limits
			ApplicationAttemptId attId1 = CreateSchedulingRequest(1024, "queue1.sub1", "user1"
				);
			VerifyAppRunnable(attId1, true);
			VerifyQueueNumRunnable("queue1.sub1", 1, 0);
			clock.Tick(10);
			// exceeds no limits
			ApplicationAttemptId attId2 = CreateSchedulingRequest(1024, "queue1.sub3", "user1"
				);
			VerifyAppRunnable(attId2, true);
			VerifyQueueNumRunnable("queue1.sub3", 1, 0);
			clock.Tick(10);
			// exceeds no limits
			ApplicationAttemptId attId3 = CreateSchedulingRequest(1024, "queue1.sub2", "user1"
				);
			VerifyAppRunnable(attId3, true);
			VerifyQueueNumRunnable("queue1.sub2", 1, 0);
			clock.Tick(10);
			// exceeds queue1 limit
			ApplicationAttemptId attId4 = CreateSchedulingRequest(1024, "queue1.sub2", "user1"
				);
			VerifyAppRunnable(attId4, false);
			VerifyQueueNumRunnable("queue1.sub2", 1, 1);
			clock.Tick(10);
			// exceeds sub3 limit
			ApplicationAttemptId attId5 = CreateSchedulingRequest(1024, "queue1.sub3", "user1"
				);
			VerifyAppRunnable(attId5, false);
			VerifyQueueNumRunnable("queue1.sub3", 1, 1);
			clock.Tick(10);
			// Even though the app was removed from sub3, the app from sub2 gets to go
			// because it came in first
			AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent
				(attId2, RMAppAttemptState.Finished, false);
			scheduler.Handle(appRemovedEvent1);
			VerifyAppRunnable(attId4, true);
			VerifyQueueNumRunnable("queue1.sub2", 2, 0);
			VerifyAppRunnable(attId5, false);
			VerifyQueueNumRunnable("queue1.sub3", 0, 1);
			// Now test removal of a non-runnable app
			AppAttemptRemovedSchedulerEvent appRemovedEvent2 = new AppAttemptRemovedSchedulerEvent
				(attId5, RMAppAttemptState.Killed, true);
			scheduler.Handle(appRemovedEvent2);
			NUnit.Framework.Assert.AreEqual(0, scheduler.maxRunningEnforcer.usersNonRunnableApps
				.Get("user1").Count);
			// verify app gone in queue accounting
			VerifyQueueNumRunnable("queue1.sub3", 0, 0);
			// verify it doesn't become runnable when there would be space for it
			AppAttemptRemovedSchedulerEvent appRemovedEvent3 = new AppAttemptRemovedSchedulerEvent
				(attId4, RMAppAttemptState.Finished, true);
			scheduler.Handle(appRemovedEvent3);
			VerifyQueueNumRunnable("queue1.sub2", 1, 0);
			VerifyQueueNumRunnable("queue1.sub3", 0, 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContinuousScheduling()
		{
			// set continuous scheduling enabled
			scheduler = new FairScheduler();
			Configuration conf = CreateConfiguration();
			conf.SetBoolean(FairSchedulerConfiguration.ContinuousSchedulingEnabled, true);
			scheduler.SetRMContext(resourceManager.GetRMContext());
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			NUnit.Framework.Assert.IsTrue("Continuous scheduling should be enabled.", scheduler
				.IsContinuousSchedulingEnabled());
			// Add two nodes
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 2, 
				"127.0.0.2");
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			// available resource
			NUnit.Framework.Assert.AreEqual(scheduler.GetClusterResource().GetMemory(), 16 * 
				1024);
			NUnit.Framework.Assert.AreEqual(scheduler.GetClusterResource().GetVirtualCores(), 
				16);
			// send application request
			ApplicationAttemptId appAttemptId = CreateAppAttemptId(this.AppId++, this.AttemptId
				++);
			CreateMockRMApp(appAttemptId);
			scheduler.AddApplication(appAttemptId.GetApplicationId(), "queue11", "user11", false
				);
			scheduler.AddApplicationAttempt(appAttemptId, false, false);
			IList<ResourceRequest> ask = new AList<ResourceRequest>();
			ResourceRequest request = CreateResourceRequest(1024, 1, ResourceRequest.Any, 1, 
				1, true);
			ask.AddItem(request);
			scheduler.Allocate(appAttemptId, ask, new AList<ContainerId>(), null, null);
			// waiting for continuous_scheduler_sleep_time
			// at least one pass
			Sharpen.Thread.Sleep(scheduler.GetConf().GetContinuousSchedulingSleepMs() + 500);
			FSAppAttempt app = scheduler.GetSchedulerApp(appAttemptId);
			// Wait until app gets resources.
			while (app.GetCurrentConsumption().Equals(Resources.None()))
			{
			}
			// check consumption
			NUnit.Framework.Assert.AreEqual(1024, app.GetCurrentConsumption().GetMemory());
			NUnit.Framework.Assert.AreEqual(1, app.GetCurrentConsumption().GetVirtualCores());
			// another request
			request = CreateResourceRequest(1024, 1, ResourceRequest.Any, 2, 1, true);
			ask.Clear();
			ask.AddItem(request);
			scheduler.Allocate(appAttemptId, ask, new AList<ContainerId>(), null, null);
			// Wait until app gets resources
			while (app.GetCurrentConsumption().Equals(Resources.CreateResource(1024, 1)))
			{
			}
			NUnit.Framework.Assert.AreEqual(2048, app.GetCurrentConsumption().GetMemory());
			NUnit.Framework.Assert.AreEqual(2, app.GetCurrentConsumption().GetVirtualCores());
			// 2 containers should be assigned to 2 nodes
			ICollection<NodeId> nodes = new HashSet<NodeId>();
			IEnumerator<RMContainer> it = app.GetLiveContainers().GetEnumerator();
			while (it.HasNext())
			{
				nodes.AddItem(it.Next().GetContainer().GetNodeId());
			}
			NUnit.Framework.Assert.AreEqual(2, nodes.Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContinuousSchedulingWithNodeRemoved()
		{
			// Disable continuous scheduling, will invoke continuous scheduling once manually
			scheduler.Init(conf);
			scheduler.Start();
			NUnit.Framework.Assert.IsTrue("Continuous scheduling should be disabled.", !scheduler
				.IsContinuousSchedulingEnabled());
			// Add two nodes
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			RMNode node2 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 2, 
				"127.0.0.2");
			NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
			scheduler.Handle(nodeEvent2);
			NUnit.Framework.Assert.AreEqual("We should have two alive nodes.", 2, scheduler.GetNumClusterNodes
				());
			// Remove one node
			NodeRemovedSchedulerEvent removeNode1 = new NodeRemovedSchedulerEvent(node1);
			scheduler.Handle(removeNode1);
			NUnit.Framework.Assert.AreEqual("We should only have one alive node.", 1, scheduler
				.GetNumClusterNodes());
			// Invoke the continuous scheduling once
			try
			{
				scheduler.ContinuousSchedulingAttempt();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("Exception happened when doing continuous scheduling. "
					 + e.ToString());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContinuousSchedulingInterruptedException()
		{
			scheduler.Init(conf);
			scheduler.Start();
			FairScheduler spyScheduler = Org.Mockito.Mockito.Spy(scheduler);
			NUnit.Framework.Assert.IsTrue("Continuous scheduling should be disabled.", !spyScheduler
				.IsContinuousSchedulingEnabled());
			// Add one nodes
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			spyScheduler.Handle(nodeEvent1);
			NUnit.Framework.Assert.AreEqual("We should have one alive node.", 1, spyScheduler
				.GetNumClusterNodes());
			Exception ie = new Exception();
			Org.Mockito.Mockito.DoThrow(new YarnRuntimeException(ie)).When(spyScheduler).AttemptScheduling
				(Matchers.IsA<FSSchedulerNode>());
			// Invoke the continuous scheduling once
			try
			{
				spyScheduler.ContinuousSchedulingAttempt();
				NUnit.Framework.Assert.Fail("Expected InterruptedException to stop schedulingThread"
					);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.AreEqual(ie, e);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDontAllowUndeclaredPools()
		{
			conf.SetBoolean(FairSchedulerConfiguration.AllowUndeclaredPools, false);
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"jerry\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			QueueManager queueManager = scheduler.GetQueueManager();
			FSLeafQueue jerryQueue = queueManager.GetLeafQueue("jerry", false);
			FSLeafQueue defaultQueue = queueManager.GetLeafQueue("default", false);
			// Should get put into jerry
			CreateSchedulingRequest(1024, "jerry", "someuser");
			NUnit.Framework.Assert.AreEqual(1, jerryQueue.GetNumRunnableApps());
			// Should get forced into default
			CreateSchedulingRequest(1024, "newqueue", "someuser");
			NUnit.Framework.Assert.AreEqual(1, jerryQueue.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, defaultQueue.GetNumRunnableApps());
			// Would get put into someuser because of user-as-default-queue, but should
			// be forced into default
			CreateSchedulingRequest(1024, "default", "someuser");
			NUnit.Framework.Assert.AreEqual(1, jerryQueue.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(2, defaultQueue.GetNumRunnableApps());
			// Should get put into jerry because of user-as-default-queue
			CreateSchedulingRequest(1024, "default", "jerry");
			NUnit.Framework.Assert.AreEqual(2, jerryQueue.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(2, defaultQueue.GetNumRunnableApps());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSchedulingOnRemovedNode()
		{
			// Disable continuous scheduling, will invoke continuous scheduling manually
			scheduler.Init(conf);
			scheduler.Start();
			NUnit.Framework.Assert.IsTrue("Continuous scheduling should be disabled.", !scheduler
				.IsContinuousSchedulingEnabled());
			ApplicationAttemptId id11 = CreateAppAttemptId(1, 1);
			CreateMockRMApp(id11);
			scheduler.AddApplication(id11.GetApplicationId(), "root.queue1", "user1", false);
			scheduler.AddApplicationAttempt(id11, false, false);
			IList<ResourceRequest> ask1 = new AList<ResourceRequest>();
			ResourceRequest request1 = CreateResourceRequest(1024, 8, ResourceRequest.Any, 1, 
				1, true);
			ask1.AddItem(request1);
			scheduler.Allocate(id11, ask1, new AList<ContainerId>(), null, null);
			string hostName = "127.0.0.1";
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(8 * 1024, 8), 1, 
				hostName);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			FSSchedulerNode node = (FSSchedulerNode)scheduler.GetSchedulerNode(node1.GetNodeID
				());
			NodeRemovedSchedulerEvent removeNode1 = new NodeRemovedSchedulerEvent(node1);
			scheduler.Handle(removeNode1);
			scheduler.AttemptScheduling(node);
			AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent
				(id11, RMAppAttemptState.Finished, false);
			scheduler.Handle(appRemovedEvent1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultRuleInitializesProperlyWhenPolicyNotConfigured()
		{
			// This test verifies if default rule in queue placement policy
			// initializes properly when policy is not configured and
			// undeclared pools is not allowed.
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			conf.SetBoolean(FairSchedulerConfiguration.AllowUndeclaredPools, false);
			// Create an alloc file with no queue placement policy
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("</allocations>");
			@out.Close();
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			IList<QueuePlacementRule> rules = scheduler.allocConf.placementPolicy.GetRules();
			foreach (QueuePlacementRule rule in rules)
			{
				if (rule is QueuePlacementRule.Default)
				{
					QueuePlacementRule.Default defaultRule = (QueuePlacementRule.Default)rule;
					NUnit.Framework.Assert.IsNotNull(defaultRule.defaultQueueName);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecoverRequestAfterPreemption()
		{
			conf.SetLong(FairSchedulerConfiguration.WaitTimeBeforeKill, 10);
			FairSchedulerTestBase.MockClock clock = new FairSchedulerTestBase.MockClock();
			scheduler.SetClock(clock);
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			Priority priority = Priority.NewInstance(20);
			string host = "127.0.0.1";
			int Gb = 1024;
			// Create Node and raised Node Added event
			RMNode node = MockNodes.NewNodeInfo(1, Resources.CreateResource(16 * 1024, 4), 0, 
				host);
			NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
			scheduler.Handle(nodeEvent);
			// Create 3 container requests and place it in ask
			IList<ResourceRequest> ask = new AList<ResourceRequest>();
			ResourceRequest nodeLocalRequest = CreateResourceRequest(Gb, 1, host, priority.GetPriority
				(), 1, true);
			ResourceRequest rackLocalRequest = CreateResourceRequest(Gb, 1, node.GetRackName(
				), priority.GetPriority(), 1, true);
			ResourceRequest offRackRequest = CreateResourceRequest(Gb, 1, ResourceRequest.Any
				, priority.GetPriority(), 1, true);
			ask.AddItem(nodeLocalRequest);
			ask.AddItem(rackLocalRequest);
			ask.AddItem(offRackRequest);
			// Create Request and update
			ApplicationAttemptId appAttemptId = CreateSchedulingRequest("queueA", "user1", ask
				);
			scheduler.Update();
			// Sufficient node check-ins to fully schedule containers
			NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
			scheduler.Handle(nodeUpdate);
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetSchedulerApp(appAttemptId).GetLiveContainers
				().Count);
			FSAppAttempt app = scheduler.GetSchedulerApp(appAttemptId);
			// ResourceRequest will be empty once NodeUpdate is completed
			NUnit.Framework.Assert.IsNull(app.GetResourceRequest(priority, host));
			ContainerId containerId1 = ContainerId.NewContainerId(appAttemptId, 1);
			RMContainer rmContainer = app.GetRMContainer(containerId1);
			// Create a preempt event and register for preemption
			scheduler.WarnOrKillContainer(rmContainer);
			// Wait for few clock ticks
			clock.Tick(5);
			// preempt now
			scheduler.WarnOrKillContainer(rmContainer);
			// Trigger container rescheduled event
			scheduler.Handle(new ContainerRescheduledEvent(rmContainer));
			IList<ResourceRequest> requests = rmContainer.GetResourceRequests();
			// Once recovered, resource request will be present again in app
			NUnit.Framework.Assert.AreEqual(3, requests.Count);
			foreach (ResourceRequest request in requests)
			{
				NUnit.Framework.Assert.AreEqual(1, app.GetResourceRequest(priority, request.GetResourceName
					()).GetNumContainers());
			}
			// Send node heartbeat
			scheduler.Update();
			scheduler.Handle(nodeUpdate);
			IList<Container> containers = scheduler.Allocate(appAttemptId, Sharpen.Collections
				.EmptyList<ResourceRequest>(), Sharpen.Collections.EmptyList<ContainerId>(), null
				, null).GetContainers();
			// Now with updated ResourceRequest, a container is allocated for AM.
			NUnit.Framework.Assert.IsTrue(containers.Count == 1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlacklistNodes()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			int Gb = 1024;
			string host = "127.0.0.1";
			RMNode node = MockNodes.NewNodeInfo(1, Resources.CreateResource(16 * Gb, 16), 0, 
				host);
			NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
			scheduler.Handle(nodeEvent);
			ApplicationAttemptId appAttemptId = CreateSchedulingRequest(Gb, "root.default", "user"
				, 1);
			FSAppAttempt app = scheduler.GetSchedulerApp(appAttemptId);
			// Verify the blacklist can be updated independent of requesting containers
			scheduler.Allocate(appAttemptId, Sharpen.Collections.EmptyList<ResourceRequest>()
				, Sharpen.Collections.EmptyList<ContainerId>(), Sharpen.Collections.SingletonList
				(host), null);
			NUnit.Framework.Assert.IsTrue(app.IsBlacklisted(host));
			scheduler.Allocate(appAttemptId, Sharpen.Collections.EmptyList<ResourceRequest>()
				, Sharpen.Collections.EmptyList<ContainerId>(), null, Sharpen.Collections.SingletonList
				(host));
			NUnit.Framework.Assert.IsFalse(scheduler.GetSchedulerApp(appAttemptId).IsBlacklisted
				(host));
			IList<ResourceRequest> update = Arrays.AsList(CreateResourceRequest(Gb, node.GetHostName
				(), 1, 0, true));
			// Verify a container does not actually get placed on the blacklisted host
			scheduler.Allocate(appAttemptId, update, Sharpen.Collections.EmptyList<ContainerId
				>(), Sharpen.Collections.SingletonList(host), null);
			NUnit.Framework.Assert.IsTrue(app.IsBlacklisted(host));
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Incorrect number of containers allocated", 0, app
				.GetLiveContainers().Count);
			// Verify a container gets placed on the empty blacklist
			scheduler.Allocate(appAttemptId, update, Sharpen.Collections.EmptyList<ContainerId
				>(), null, Sharpen.Collections.SingletonList(host));
			NUnit.Framework.Assert.IsFalse(app.IsBlacklisted(host));
			CreateSchedulingRequest(Gb, "root.default", "user", 1);
			scheduler.Update();
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual("Incorrect number of containers allocated", 1, app
				.GetLiveContainers().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAppsInQueue()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			ApplicationAttemptId appAttId1 = CreateSchedulingRequest(1024, 1, "queue1.subqueue1"
				, "user1");
			ApplicationAttemptId appAttId2 = CreateSchedulingRequest(1024, 1, "queue1.subqueue2"
				, "user1");
			ApplicationAttemptId appAttId3 = CreateSchedulingRequest(1024, 1, "default", "user1"
				);
			IList<ApplicationAttemptId> apps = scheduler.GetAppsInQueue("queue1.subqueue1");
			NUnit.Framework.Assert.AreEqual(1, apps.Count);
			NUnit.Framework.Assert.AreEqual(appAttId1, apps[0]);
			// with and without root prefix should work
			apps = scheduler.GetAppsInQueue("root.queue1.subqueue1");
			NUnit.Framework.Assert.AreEqual(1, apps.Count);
			NUnit.Framework.Assert.AreEqual(appAttId1, apps[0]);
			apps = scheduler.GetAppsInQueue("user1");
			NUnit.Framework.Assert.AreEqual(1, apps.Count);
			NUnit.Framework.Assert.AreEqual(appAttId3, apps[0]);
			// with and without root prefix should work
			apps = scheduler.GetAppsInQueue("root.user1");
			NUnit.Framework.Assert.AreEqual(1, apps.Count);
			NUnit.Framework.Assert.AreEqual(appAttId3, apps[0]);
			// apps in subqueues should be included
			apps = scheduler.GetAppsInQueue("queue1");
			NUnit.Framework.Assert.AreEqual(2, apps.Count);
			ICollection<ApplicationAttemptId> appAttIds = Sets.NewHashSet(apps[0], apps[1]);
			NUnit.Framework.Assert.IsTrue(appAttIds.Contains(appAttId1));
			NUnit.Framework.Assert.IsTrue(appAttIds.Contains(appAttId2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddAndRemoveAppFromFairScheduler()
		{
			AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode> scheduler = (AbstractYarnScheduler
				<SchedulerApplicationAttempt, SchedulerNode>)resourceManager.GetResourceScheduler
				();
			TestSchedulerUtils.VerifyAppAddedAndRemovedFromScheduler(scheduler.GetSchedulerApplications
				(), scheduler, "default");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveRunnableApp()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			QueueManager queueMgr = scheduler.GetQueueManager();
			FSLeafQueue oldQueue = queueMgr.GetLeafQueue("queue1", true);
			FSLeafQueue targetQueue = queueMgr.GetLeafQueue("queue2", true);
			ApplicationAttemptId appAttId = CreateSchedulingRequest(1024, 1, "queue1", "user1"
				, 3);
			ApplicationId appId = appAttId.GetApplicationId();
			RMNode node = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024));
			NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
			scheduler.Handle(nodeEvent);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(1024, 1), oldQueue.GetResourceUsage());
			scheduler.Update();
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(3072, 3), oldQueue.GetDemand());
			scheduler.MoveApplication(appId, "queue2");
			FSAppAttempt app = scheduler.GetSchedulerApp(appAttId);
			NUnit.Framework.Assert.AreSame(targetQueue, ((FSLeafQueue)app.GetQueue()));
			NUnit.Framework.Assert.IsFalse(oldQueue.IsRunnableApp(app));
			NUnit.Framework.Assert.IsTrue(targetQueue.IsRunnableApp(app));
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), oldQueue.GetResourceUsage());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(1024, 1), targetQueue.GetResourceUsage());
			NUnit.Framework.Assert.AreEqual(0, oldQueue.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, targetQueue.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, queueMgr.GetRootQueue().GetNumRunnableApps());
			scheduler.Update();
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), oldQueue.GetDemand());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(3072, 3), targetQueue.GetDemand());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveNonRunnableApp()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			QueueManager queueMgr = scheduler.GetQueueManager();
			FSLeafQueue oldQueue = queueMgr.GetLeafQueue("queue1", true);
			FSLeafQueue targetQueue = queueMgr.GetLeafQueue("queue2", true);
			scheduler.GetAllocationConfiguration().queueMaxApps["root.queue1"] = 0;
			scheduler.GetAllocationConfiguration().queueMaxApps["root.queue2"] = 0;
			ApplicationAttemptId appAttId = CreateSchedulingRequest(1024, 1, "queue1", "user1"
				, 3);
			NUnit.Framework.Assert.AreEqual(0, oldQueue.GetNumRunnableApps());
			scheduler.MoveApplication(appAttId.GetApplicationId(), "queue2");
			NUnit.Framework.Assert.AreEqual(0, oldQueue.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(0, targetQueue.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(0, queueMgr.GetRootQueue().GetNumRunnableApps());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveMakesAppRunnable()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			QueueManager queueMgr = scheduler.GetQueueManager();
			FSLeafQueue oldQueue = queueMgr.GetLeafQueue("queue1", true);
			FSLeafQueue targetQueue = queueMgr.GetLeafQueue("queue2", true);
			scheduler.GetAllocationConfiguration().queueMaxApps["root.queue1"] = 0;
			ApplicationAttemptId appAttId = CreateSchedulingRequest(1024, 1, "queue1", "user1"
				, 3);
			FSAppAttempt app = scheduler.GetSchedulerApp(appAttId);
			NUnit.Framework.Assert.IsTrue(oldQueue.IsNonRunnableApp(app));
			scheduler.MoveApplication(appAttId.GetApplicationId(), "queue2");
			NUnit.Framework.Assert.IsFalse(oldQueue.IsNonRunnableApp(app));
			NUnit.Framework.Assert.IsFalse(targetQueue.IsNonRunnableApp(app));
			NUnit.Framework.Assert.IsTrue(targetQueue.IsRunnableApp(app));
			NUnit.Framework.Assert.AreEqual(1, targetQueue.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, queueMgr.GetRootQueue().GetNumRunnableApps());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMoveWouldViolateMaxAppsConstraints()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			QueueManager queueMgr = scheduler.GetQueueManager();
			queueMgr.GetLeafQueue("queue2", true);
			scheduler.GetAllocationConfiguration().queueMaxApps["root.queue2"] = 0;
			ApplicationAttemptId appAttId = CreateSchedulingRequest(1024, 1, "queue1", "user1"
				, 3);
			scheduler.MoveApplication(appAttId.GetApplicationId(), "queue2");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMoveWouldViolateMaxResourcesConstraints()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			QueueManager queueMgr = scheduler.GetQueueManager();
			FSLeafQueue oldQueue = queueMgr.GetLeafQueue("queue1", true);
			queueMgr.GetLeafQueue("queue2", true);
			scheduler.GetAllocationConfiguration().maxQueueResources["root.queue2"] = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1);
			ApplicationAttemptId appAttId = CreateSchedulingRequest(1024, 1, "queue1", "user1"
				, 3);
			RMNode node = MockNodes.NewNodeInfo(1, Resources.CreateResource(2048, 2));
			NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
			scheduler.Handle(nodeEvent);
			scheduler.Handle(updateEvent);
			scheduler.Handle(updateEvent);
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(2048, 2), oldQueue.GetResourceUsage());
			scheduler.MoveApplication(appAttId.GetApplicationId(), "queue2");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMoveToNonexistentQueue()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			scheduler.GetQueueManager().GetLeafQueue("queue1", true);
			ApplicationAttemptId appAttId = CreateSchedulingRequest(1024, 1, "queue1", "user1"
				, 3);
			scheduler.MoveApplication(appAttId.GetApplicationId(), "queue2");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLowestCommonAncestorForNonRootParent()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			FSLeafQueue aQueue = Org.Mockito.Mockito.Mock<FSLeafQueue>();
			FSLeafQueue bQueue = Org.Mockito.Mockito.Mock<FSLeafQueue>();
			Org.Mockito.Mockito.When(aQueue.GetName()).ThenReturn("root.queue1.a");
			Org.Mockito.Mockito.When(bQueue.GetName()).ThenReturn("root.queue1.b");
			QueueManager queueManager = scheduler.GetQueueManager();
			FSParentQueue queue1 = queueManager.GetParentQueue("queue1", true);
			queue1.AddChildQueue(aQueue);
			queue1.AddChildQueue(bQueue);
			FSQueue ancestorQueue = scheduler.FindLowestCommonAncestorQueue(aQueue, bQueue);
			NUnit.Framework.Assert.AreEqual(ancestorQueue, queue1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLowestCommonAncestorRootParent()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			FSLeafQueue aQueue = Org.Mockito.Mockito.Mock<FSLeafQueue>();
			FSLeafQueue bQueue = Org.Mockito.Mockito.Mock<FSLeafQueue>();
			Org.Mockito.Mockito.When(aQueue.GetName()).ThenReturn("root.a");
			Org.Mockito.Mockito.When(bQueue.GetName()).ThenReturn("root.b");
			QueueManager queueManager = scheduler.GetQueueManager();
			FSParentQueue queue1 = queueManager.GetParentQueue("root", false);
			queue1.AddChildQueue(aQueue);
			queue1.AddChildQueue(bQueue);
			FSQueue ancestorQueue = scheduler.FindLowestCommonAncestorQueue(aQueue, bQueue);
			NUnit.Framework.Assert.AreEqual(ancestorQueue, queue1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLowestCommonAncestorDeeperHierarchy()
		{
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, resourceManager.GetRMContext());
			FSQueue aQueue = Org.Mockito.Mockito.Mock<FSLeafQueue>();
			FSQueue bQueue = Org.Mockito.Mockito.Mock<FSLeafQueue>();
			FSQueue a1Queue = Org.Mockito.Mockito.Mock<FSLeafQueue>();
			FSQueue b1Queue = Org.Mockito.Mockito.Mock<FSLeafQueue>();
			Org.Mockito.Mockito.When(a1Queue.GetName()).ThenReturn("root.queue1.a.a1");
			Org.Mockito.Mockito.When(b1Queue.GetName()).ThenReturn("root.queue1.b.b1");
			Org.Mockito.Mockito.When(aQueue.GetChildQueues()).ThenReturn(Arrays.AsList(a1Queue
				));
			Org.Mockito.Mockito.When(bQueue.GetChildQueues()).ThenReturn(Arrays.AsList(b1Queue
				));
			QueueManager queueManager = scheduler.GetQueueManager();
			FSParentQueue queue1 = queueManager.GetParentQueue("queue1", true);
			queue1.AddChildQueue(aQueue);
			queue1.AddChildQueue(bQueue);
			FSQueue ancestorQueue = scheduler.FindLowestCommonAncestorQueue(a1Queue, b1Queue);
			NUnit.Framework.Assert.AreEqual(ancestorQueue, queue1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestThreadLifeCycle()
		{
			conf.SetBoolean(FairSchedulerConfiguration.ContinuousSchedulingEnabled, true);
			scheduler.Init(conf);
			scheduler.Start();
			Sharpen.Thread updateThread = scheduler.updateThread;
			Sharpen.Thread schedulingThread = scheduler.schedulingThread;
			NUnit.Framework.Assert.IsTrue(updateThread.IsAlive());
			NUnit.Framework.Assert.IsTrue(schedulingThread.IsAlive());
			scheduler.Stop();
			int numRetries = 100;
			while (numRetries-- > 0 && (updateThread.IsAlive() || schedulingThread.IsAlive())
				)
			{
				Sharpen.Thread.Sleep(50);
			}
			Assert.AssertNotEquals("One of the threads is still alive", 0, numRetries);
		}

		[NUnit.Framework.Test]
		public virtual void TestPerfMetricsInited()
		{
			scheduler.Init(conf);
			scheduler.Start();
			MetricsCollectorImpl collector = new MetricsCollectorImpl();
			scheduler.fsOpDurations.GetMetrics(collector, true);
			NUnit.Framework.Assert.AreEqual("Incorrect number of perf metrics", 1, collector.
				GetRecords().Count);
		}
	}
}
