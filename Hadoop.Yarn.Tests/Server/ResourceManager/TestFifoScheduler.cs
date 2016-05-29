using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestFifoScheduler
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestFifoScheduler));

		private readonly int Gb = 1024;

		private static YarnConfiguration conf;

		[BeforeClass]
		public static void Setup()
		{
			conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestConfValidation()
		{
			FifoScheduler scheduler = new FifoScheduler();
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
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllocateContainerOnNodeWithoutOffSwitchSpecified()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			MockRM rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 6 * Gb);
			RMApp app1 = rm.SubmitApp(2048);
			// kick the scheduling, 2 GB given to AM1, remaining 4GB on nm1
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			MockAM am1 = rm.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			// add request for containers
			IList<ResourceRequest> requests = new AList<ResourceRequest>();
			requests.AddItem(am1.CreateResourceReq("127.0.0.1", 1 * Gb, 1, 1));
			requests.AddItem(am1.CreateResourceReq("/default-rack", 1 * Gb, 1, 1));
			am1.Allocate(requests, null);
			// send the request
			try
			{
				// kick the schedule
				nm1.NodeHeartbeat(true);
			}
			catch (ArgumentNullException)
			{
				NUnit.Framework.Assert.Fail("NPE when allocating container on node but " + "forget to set off-switch request should be handled"
					);
			}
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Test()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			MockRM rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 6 * Gb);
			MockNM nm2 = rm.RegisterNode("127.0.0.2:5678", 4 * Gb);
			RMApp app1 = rm.SubmitApp(2048);
			// kick the scheduling, 2 GB given to AM1, remaining 4GB on nm1
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			MockAM am1 = rm.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			SchedulerNodeReport report_nm1 = rm.GetResourceScheduler().GetNodeReport(nm1.GetNodeId
				());
			NUnit.Framework.Assert.AreEqual(2 * Gb, report_nm1.GetUsedResource().GetMemory());
			RMApp app2 = rm.SubmitApp(2048);
			// kick the scheduling, 2GB given to AM, remaining 2 GB on nm2
			nm2.NodeHeartbeat(true);
			RMAppAttempt attempt2 = app2.GetCurrentAppAttempt();
			MockAM am2 = rm.SendAMLaunched(attempt2.GetAppAttemptId());
			am2.RegisterAppAttempt();
			SchedulerNodeReport report_nm2 = rm.GetResourceScheduler().GetNodeReport(nm2.GetNodeId
				());
			NUnit.Framework.Assert.AreEqual(2 * Gb, report_nm2.GetUsedResource().GetMemory());
			// add request for containers
			am1.AddRequests(new string[] { "127.0.0.1", "127.0.0.2" }, Gb, 1, 1);
			AllocateResponse alloc1Response = am1.Schedule();
			// send the request
			// add request for containers
			am2.AddRequests(new string[] { "127.0.0.1", "127.0.0.2" }, 3 * Gb, 0, 1);
			AllocateResponse alloc2Response = am2.Schedule();
			// send the request
			// kick the scheduler, 1 GB and 3 GB given to AM1 and AM2, remaining 0
			nm1.NodeHeartbeat(true);
			while (alloc1Response.GetAllocatedContainers().Count < 1)
			{
				Log.Info("Waiting for containers to be created for app 1...");
				Sharpen.Thread.Sleep(1000);
				alloc1Response = am1.Schedule();
			}
			while (alloc2Response.GetAllocatedContainers().Count < 1)
			{
				Log.Info("Waiting for containers to be created for app 2...");
				Sharpen.Thread.Sleep(1000);
				alloc2Response = am2.Schedule();
			}
			// kick the scheduler, nothing given remaining 2 GB.
			nm2.NodeHeartbeat(true);
			IList<Container> allocated1 = alloc1Response.GetAllocatedContainers();
			NUnit.Framework.Assert.AreEqual(1, allocated1.Count);
			NUnit.Framework.Assert.AreEqual(1 * Gb, allocated1[0].GetResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(nm1.GetNodeId(), allocated1[0].GetNodeId());
			IList<Container> allocated2 = alloc2Response.GetAllocatedContainers();
			NUnit.Framework.Assert.AreEqual(1, allocated2.Count);
			NUnit.Framework.Assert.AreEqual(3 * Gb, allocated2[0].GetResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(nm1.GetNodeId(), allocated2[0].GetNodeId());
			report_nm1 = rm.GetResourceScheduler().GetNodeReport(nm1.GetNodeId());
			report_nm2 = rm.GetResourceScheduler().GetNodeReport(nm2.GetNodeId());
			NUnit.Framework.Assert.AreEqual(0, report_nm1.GetAvailableResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, report_nm2.GetAvailableResource().GetMemory
				());
			NUnit.Framework.Assert.AreEqual(6 * Gb, report_nm1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, report_nm2.GetUsedResource().GetMemory());
			Container c1 = allocated1[0];
			NUnit.Framework.Assert.AreEqual(Gb, c1.GetResource().GetMemory());
			ContainerStatus containerStatus = BuilderUtils.NewContainerStatus(c1.GetId(), ContainerState
				.Complete, string.Empty, 0);
			nm1.ContainerStatus(containerStatus);
			int waitCount = 0;
			while (attempt1.GetJustFinishedContainers().Count < 1 && waitCount++ != 20)
			{
				Log.Info("Waiting for containers to be finished for app 1... Tried " + waitCount 
					+ " times already..");
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.AreEqual(1, attempt1.GetJustFinishedContainers().Count);
			NUnit.Framework.Assert.AreEqual(1, am1.Schedule().GetCompletedContainersStatuses(
				).Count);
			report_nm1 = rm.GetResourceScheduler().GetNodeReport(nm1.GetNodeId());
			NUnit.Framework.Assert.AreEqual(5 * Gb, report_nm1.GetUsedResource().GetMemory());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeUpdateBeforeAppAttemptInit()
		{
			FifoScheduler scheduler = new FifoScheduler();
			MockRM rm = new MockRM(conf);
			scheduler.SetRMContext(rm.GetRMContext());
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, rm.GetRMContext());
			RMNode node = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024, 4), 1, "127.0.0.1"
				);
			scheduler.Handle(new NodeAddedSchedulerEvent(node));
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			scheduler.AddApplication(appId, "queue1", "user1", false);
			NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
			try
			{
				scheduler.Handle(updateEvent);
			}
			catch (ArgumentNullException)
			{
				NUnit.Framework.Assert.Fail();
			}
			ApplicationAttemptId attId = ApplicationAttemptId.NewInstance(appId, 1);
			scheduler.AddApplicationAttempt(attId, false, false);
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		private void TestMinimumAllocation(YarnConfiguration conf, int testAlloc)
		{
			MockRM rm = new MockRM(conf);
			rm.Start();
			// Register node1
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 6 * Gb);
			// Submit an application
			RMApp app1 = rm.SubmitApp(testAlloc);
			// kick the scheduling
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			MockAM am1 = rm.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			SchedulerNodeReport report_nm1 = rm.GetResourceScheduler().GetNodeReport(nm1.GetNodeId
				());
			int checkAlloc = conf.GetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationMb);
			NUnit.Framework.Assert.AreEqual(checkAlloc, report_nm1.GetUsedResource().GetMemory
				());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultMinimumAllocation()
		{
			// Test with something lesser than default
			TestMinimumAllocation(new YarnConfiguration(TestFifoScheduler.conf), YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationMb / 2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonDefaultMinimumAllocation()
		{
			// Set custom min-alloc to test tweaking it
			int allocMB = 1536;
			YarnConfiguration conf = new YarnConfiguration(TestFifoScheduler.conf);
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, allocMB);
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, allocMB * 10);
			// Test for something lesser than this.
			TestMinimumAllocation(conf, allocMB / 2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReconnectedNode()
		{
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			conf.SetQueues("default", new string[] { "default" });
			conf.SetCapacity("default", 100);
			FifoScheduler fs = new FifoScheduler();
			fs.Init(conf);
			fs.Start();
			// mock rmContext to avoid NPE.
			RMContext context = Org.Mockito.Mockito.Mock<RMContext>();
			fs.Reinitialize(conf, null);
			fs.SetRMContext(context);
			RMNode n1 = MockNodes.NewNodeInfo(0, MockNodes.NewResource(4 * Gb), 1, "127.0.0.2"
				);
			RMNode n2 = MockNodes.NewNodeInfo(0, MockNodes.NewResource(2 * Gb), 2, "127.0.0.3"
				);
			fs.Handle(new NodeAddedSchedulerEvent(n1));
			fs.Handle(new NodeAddedSchedulerEvent(n2));
			fs.Handle(new NodeUpdateSchedulerEvent(n1));
			NUnit.Framework.Assert.AreEqual(6 * Gb, fs.GetRootQueueMetrics().GetAvailableMB()
				);
			// reconnect n1 with downgraded memory
			n1 = MockNodes.NewNodeInfo(0, MockNodes.NewResource(2 * Gb), 1, "127.0.0.2");
			fs.Handle(new NodeRemovedSchedulerEvent(n1));
			fs.Handle(new NodeAddedSchedulerEvent(n1));
			fs.Handle(new NodeUpdateSchedulerEvent(n1));
			NUnit.Framework.Assert.AreEqual(4 * Gb, fs.GetRootQueueMetrics().GetAvailableMB()
				);
			fs.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBlackListNodes()
		{
			Configuration conf = new Configuration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			rm.Start();
			FifoScheduler fs = (FifoScheduler)rm.GetResourceScheduler();
			int rack_num_0 = 0;
			int rack_num_1 = 1;
			// Add 4 nodes in 2 racks
			// host_0_0 in rack0
			string host_0_0 = "127.0.0.1";
			RMNode n1 = MockNodes.NewNodeInfo(rack_num_0, MockNodes.NewResource(4 * Gb), 1, host_0_0
				);
			fs.Handle(new NodeAddedSchedulerEvent(n1));
			// host_0_1 in rack0
			string host_0_1 = "127.0.0.2";
			RMNode n2 = MockNodes.NewNodeInfo(rack_num_0, MockNodes.NewResource(4 * Gb), 1, host_0_1
				);
			fs.Handle(new NodeAddedSchedulerEvent(n2));
			// host_1_0 in rack1
			string host_1_0 = "127.0.0.3";
			RMNode n3 = MockNodes.NewNodeInfo(rack_num_1, MockNodes.NewResource(4 * Gb), 1, host_1_0
				);
			fs.Handle(new NodeAddedSchedulerEvent(n3));
			// host_1_1 in rack1
			string host_1_1 = "127.0.0.4";
			RMNode n4 = MockNodes.NewNodeInfo(rack_num_1, MockNodes.NewResource(4 * Gb), 1, host_1_1
				);
			fs.Handle(new NodeAddedSchedulerEvent(n4));
			// Add one application
			ApplicationId appId1 = BuilderUtils.NewApplicationId(100, 1);
			ApplicationAttemptId appAttemptId1 = BuilderUtils.NewApplicationAttemptId(appId1, 
				1);
			CreateMockRMApp(appAttemptId1, rm.GetRMContext());
			SchedulerEvent appEvent = new AppAddedSchedulerEvent(appId1, "queue", "user");
			fs.Handle(appEvent);
			SchedulerEvent attemptEvent = new AppAttemptAddedSchedulerEvent(appAttemptId1, false
				);
			fs.Handle(attemptEvent);
			IList<ContainerId> emptyId = new AList<ContainerId>();
			IList<ResourceRequest> emptyAsk = new AList<ResourceRequest>();
			// Allow rack-locality for rack_1, but blacklist host_1_0
			// Set up resource requests
			// Ask for a 1 GB container for app 1
			IList<ResourceRequest> ask1 = new AList<ResourceRequest>();
			ask1.AddItem(BuilderUtils.NewResourceRequest(BuilderUtils.NewPriority(0), "rack1"
				, BuilderUtils.NewResource(Gb, 1), 1));
			ask1.AddItem(BuilderUtils.NewResourceRequest(BuilderUtils.NewPriority(0), ResourceRequest
				.Any, BuilderUtils.NewResource(Gb, 1), 1));
			fs.Allocate(appAttemptId1, ask1, emptyId, Sharpen.Collections.SingletonList(host_1_0
				), null);
			// Trigger container assignment
			fs.Handle(new NodeUpdateSchedulerEvent(n3));
			// Get the allocation for the application and verify no allocation on blacklist node
			Allocation allocation1 = fs.Allocate(appAttemptId1, emptyAsk, emptyId, null, null
				);
			NUnit.Framework.Assert.AreEqual("allocation1", 0, allocation1.GetContainers().Count
				);
			// verify host_1_1 can get allocated as not in blacklist
			fs.Handle(new NodeUpdateSchedulerEvent(n4));
			Allocation allocation2 = fs.Allocate(appAttemptId1, emptyAsk, emptyId, null, null
				);
			NUnit.Framework.Assert.AreEqual("allocation2", 1, allocation2.GetContainers().Count
				);
			IList<Container> containerList = allocation2.GetContainers();
			foreach (Container container in containerList)
			{
				NUnit.Framework.Assert.AreEqual("Container is allocated on n4", container.GetNodeId
					(), n4.GetNodeID());
			}
			// Ask for a 1 GB container again for app 1
			IList<ResourceRequest> ask2 = new AList<ResourceRequest>();
			// this time, rack0 is also in blacklist, so only host_1_1 is available to
			// be assigned
			ask2.AddItem(BuilderUtils.NewResourceRequest(BuilderUtils.NewPriority(0), ResourceRequest
				.Any, BuilderUtils.NewResource(Gb, 1), 1));
			fs.Allocate(appAttemptId1, ask2, emptyId, Sharpen.Collections.SingletonList("rack0"
				), null);
			// verify n1 is not qualified to be allocated
			fs.Handle(new NodeUpdateSchedulerEvent(n1));
			Allocation allocation3 = fs.Allocate(appAttemptId1, emptyAsk, emptyId, null, null
				);
			NUnit.Framework.Assert.AreEqual("allocation3", 0, allocation3.GetContainers().Count
				);
			// verify n2 is not qualified to be allocated
			fs.Handle(new NodeUpdateSchedulerEvent(n2));
			Allocation allocation4 = fs.Allocate(appAttemptId1, emptyAsk, emptyId, null, null
				);
			NUnit.Framework.Assert.AreEqual("allocation4", 0, allocation4.GetContainers().Count
				);
			// verify n3 is not qualified to be allocated
			fs.Handle(new NodeUpdateSchedulerEvent(n3));
			Allocation allocation5 = fs.Allocate(appAttemptId1, emptyAsk, emptyId, null, null
				);
			NUnit.Framework.Assert.AreEqual("allocation5", 0, allocation5.GetContainers().Count
				);
			fs.Handle(new NodeUpdateSchedulerEvent(n4));
			Allocation allocation6 = fs.Allocate(appAttemptId1, emptyAsk, emptyId, null, null
				);
			NUnit.Framework.Assert.AreEqual("allocation6", 1, allocation6.GetContainers().Count
				);
			containerList = allocation6.GetContainers();
			foreach (Container container_1 in containerList)
			{
				NUnit.Framework.Assert.AreEqual("Container is allocated on n4", container_1.GetNodeId
					(), n4.GetNodeID());
			}
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHeadroom()
		{
			Configuration conf = new Configuration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			rm.Start();
			FifoScheduler fs = (FifoScheduler)rm.GetResourceScheduler();
			// Add a node
			RMNode n1 = MockNodes.NewNodeInfo(0, MockNodes.NewResource(4 * Gb), 1, "127.0.0.2"
				);
			fs.Handle(new NodeAddedSchedulerEvent(n1));
			// Add two applications
			ApplicationId appId1 = BuilderUtils.NewApplicationId(100, 1);
			ApplicationAttemptId appAttemptId1 = BuilderUtils.NewApplicationAttemptId(appId1, 
				1);
			CreateMockRMApp(appAttemptId1, rm.GetRMContext());
			SchedulerEvent appEvent = new AppAddedSchedulerEvent(appId1, "queue", "user");
			fs.Handle(appEvent);
			SchedulerEvent attemptEvent = new AppAttemptAddedSchedulerEvent(appAttemptId1, false
				);
			fs.Handle(attemptEvent);
			ApplicationId appId2 = BuilderUtils.NewApplicationId(200, 2);
			ApplicationAttemptId appAttemptId2 = BuilderUtils.NewApplicationAttemptId(appId2, 
				1);
			CreateMockRMApp(appAttemptId2, rm.GetRMContext());
			SchedulerEvent appEvent2 = new AppAddedSchedulerEvent(appId2, "queue", "user");
			fs.Handle(appEvent2);
			SchedulerEvent attemptEvent2 = new AppAttemptAddedSchedulerEvent(appAttemptId2, false
				);
			fs.Handle(attemptEvent2);
			IList<ContainerId> emptyId = new AList<ContainerId>();
			IList<ResourceRequest> emptyAsk = new AList<ResourceRequest>();
			// Set up resource requests
			// Ask for a 1 GB container for app 1
			IList<ResourceRequest> ask1 = new AList<ResourceRequest>();
			ask1.AddItem(BuilderUtils.NewResourceRequest(BuilderUtils.NewPriority(0), ResourceRequest
				.Any, BuilderUtils.NewResource(Gb, 1), 1));
			fs.Allocate(appAttemptId1, ask1, emptyId, null, null);
			// Ask for a 2 GB container for app 2
			IList<ResourceRequest> ask2 = new AList<ResourceRequest>();
			ask2.AddItem(BuilderUtils.NewResourceRequest(BuilderUtils.NewPriority(0), ResourceRequest
				.Any, BuilderUtils.NewResource(2 * Gb, 1), 1));
			fs.Allocate(appAttemptId2, ask2, emptyId, null, null);
			// Trigger container assignment
			fs.Handle(new NodeUpdateSchedulerEvent(n1));
			// Get the allocation for the applications and verify headroom
			Allocation allocation1 = fs.Allocate(appAttemptId1, emptyAsk, emptyId, null, null
				);
			NUnit.Framework.Assert.AreEqual("Allocation headroom", 1 * Gb, allocation1.GetResourceLimit
				().GetMemory());
			Allocation allocation2 = fs.Allocate(appAttemptId2, emptyAsk, emptyId, null, null
				);
			NUnit.Framework.Assert.AreEqual("Allocation headroom", 1 * Gb, allocation2.GetResourceLimit
				().GetMemory());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceOverCommit()
		{
			MockRM rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 4 * Gb);
			RMApp app1 = rm.SubmitApp(2048);
			// kick the scheduling, 2 GB given to AM1, remaining 2GB on nm1
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			MockAM am1 = rm.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			SchedulerNodeReport report_nm1 = rm.GetResourceScheduler().GetNodeReport(nm1.GetNodeId
				());
			// check node report, 2 GB used and 2 GB available
			NUnit.Framework.Assert.AreEqual(2 * Gb, report_nm1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, report_nm1.GetAvailableResource().GetMemory
				());
			// add request for containers
			am1.AddRequests(new string[] { "127.0.0.1", "127.0.0.2" }, 2 * Gb, 1, 1);
			AllocateResponse alloc1Response = am1.Schedule();
			// send the request
			// kick the scheduler, 2 GB given to AM1, resource remaining 0
			nm1.NodeHeartbeat(true);
			while (alloc1Response.GetAllocatedContainers().Count < 1)
			{
				Log.Info("Waiting for containers to be created for app 1...");
				Sharpen.Thread.Sleep(1000);
				alloc1Response = am1.Schedule();
			}
			IList<Container> allocated1 = alloc1Response.GetAllocatedContainers();
			NUnit.Framework.Assert.AreEqual(1, allocated1.Count);
			NUnit.Framework.Assert.AreEqual(2 * Gb, allocated1[0].GetResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(nm1.GetNodeId(), allocated1[0].GetNodeId());
			report_nm1 = rm.GetResourceScheduler().GetNodeReport(nm1.GetNodeId());
			// check node report, 4 GB used and 0 GB available
			NUnit.Framework.Assert.AreEqual(0, report_nm1.GetAvailableResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(4 * Gb, report_nm1.GetUsedResource().GetMemory());
			// check container is assigned with 2 GB.
			Container c1 = allocated1[0];
			NUnit.Framework.Assert.AreEqual(2 * Gb, c1.GetResource().GetMemory());
			// update node resource to 2 GB, so resource is over-consumed.
			IDictionary<NodeId, ResourceOption> nodeResourceMap = new Dictionary<NodeId, ResourceOption
				>();
			nodeResourceMap[nm1.GetNodeId()] = ResourceOption.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2 * Gb, 1), -1);
			UpdateNodeResourceRequest request = UpdateNodeResourceRequest.NewInstance(nodeResourceMap
				);
			AdminService @as = rm.adminService;
			@as.UpdateNodeResource(request);
			// Now, the used resource is still 4 GB, and available resource is minus value.
			report_nm1 = rm.GetResourceScheduler().GetNodeReport(nm1.GetNodeId());
			NUnit.Framework.Assert.AreEqual(4 * Gb, report_nm1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(-2 * Gb, report_nm1.GetAvailableResource().GetMemory
				());
			// Check container can complete successfully in case of resource over-commitment.
			ContainerStatus containerStatus = BuilderUtils.NewContainerStatus(c1.GetId(), ContainerState
				.Complete, string.Empty, 0);
			nm1.ContainerStatus(containerStatus);
			int waitCount = 0;
			while (attempt1.GetJustFinishedContainers().Count < 1 && waitCount++ != 20)
			{
				Log.Info("Waiting for containers to be finished for app 1... Tried " + waitCount 
					+ " times already..");
				Sharpen.Thread.Sleep(100);
			}
			NUnit.Framework.Assert.AreEqual(1, attempt1.GetJustFinishedContainers().Count);
			NUnit.Framework.Assert.AreEqual(1, am1.Schedule().GetCompletedContainersStatuses(
				).Count);
			report_nm1 = rm.GetResourceScheduler().GetNodeReport(nm1.GetNodeId());
			NUnit.Framework.Assert.AreEqual(2 * Gb, report_nm1.GetUsedResource().GetMemory());
			// As container return 2 GB back, the available resource becomes 0 again.
			NUnit.Framework.Assert.AreEqual(0 * Gb, report_nm1.GetAvailableResource().GetMemory
				());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestFifoScheduler t = new TestFifoScheduler();
			t.Test();
			t.TestDefaultMinimumAllocation();
			t.TestNonDefaultMinimumAllocation();
			t.TestReconnectedNode();
		}

		private RMAppImpl CreateMockRMApp(ApplicationAttemptId attemptId, RMContext context
			)
		{
			RMAppImpl app = Org.Mockito.Mockito.Mock<RMAppImpl>();
			Org.Mockito.Mockito.When(app.GetApplicationId()).ThenReturn(attemptId.GetApplicationId
				());
			RMAppAttemptImpl attempt = Org.Mockito.Mockito.Mock<RMAppAttemptImpl>();
			Org.Mockito.Mockito.When(attempt.GetAppAttemptId()).ThenReturn(attemptId);
			RMAppAttemptMetrics attemptMetric = Org.Mockito.Mockito.Mock<RMAppAttemptMetrics>
				();
			Org.Mockito.Mockito.When(attempt.GetRMAppAttemptMetrics()).ThenReturn(attemptMetric
				);
			Org.Mockito.Mockito.When(app.GetCurrentAppAttempt()).ThenReturn(attempt);
			context.GetRMApps().PutIfAbsent(attemptId.GetApplicationId(), app);
			return app;
		}
	}
}
