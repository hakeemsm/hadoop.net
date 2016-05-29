using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestCapacityScheduler
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.TestCapacityScheduler
			));

		private readonly int Gb = 1024;

		private const string A = CapacitySchedulerConfiguration.Root + ".a";

		private const string B = CapacitySchedulerConfiguration.Root + ".b";

		private const string A1 = A + ".a1";

		private const string A2 = A + ".a2";

		private const string B1 = B + ".b1";

		private const string B2 = B + ".b2";

		private const string B3 = B + ".b3";

		private static float ACapacity = 10.5f;

		private static float BCapacity = 89.5f;

		private static float A1Capacity = 30;

		private static float A2Capacity = 70;

		private static float B1Capacity = 79.2f;

		private static float B2Capacity = 0.8f;

		private static float B3Capacity = 20;

		private ResourceManager resourceManager = null;

		private RMContext mockContext;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			resourceManager = new _ResourceManager_172();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(csConf);
			YarnConfiguration conf = new YarnConfiguration(csConf);
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			resourceManager.Init(conf);
			resourceManager.GetRMContext().GetContainerTokenSecretManager().RollMasterKey();
			resourceManager.GetRMContext().GetNMTokenSecretManager().RollMasterKey();
			((AsyncDispatcher)resourceManager.GetRMContext().GetDispatcher()).Start();
			mockContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(mockContext.GetConfigurationProvider()).ThenReturn(new LocalConfigurationProvider
				());
		}

		private sealed class _ResourceManager_172 : ResourceManager
		{
			public _ResourceManager_172()
			{
			}

			protected internal override RMNodeLabelsManager CreateNodeLabelManager()
			{
				RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
				mgr.Init(this.GetConfig());
				return mgr;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (resourceManager != null)
			{
				resourceManager.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestConfValidation()
		{
			ResourceScheduler scheduler = new CapacityScheduler();
			scheduler.SetRMContext(resourceManager.GetRMContext());
			Configuration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, 2048);
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, 1024);
			try
			{
				scheduler.Reinitialize(conf, mockContext);
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
				scheduler.Reinitialize(conf, mockContext);
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

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private NodeManager RegisterNode(string hostName, int containerManagerPort, int httpPort
			, string rackName, Resource capability)
		{
			NodeManager nm = new NodeManager(hostName, containerManagerPort, httpPort, rackName
				, capability, resourceManager);
			NodeAddedSchedulerEvent nodeAddEvent1 = new NodeAddedSchedulerEvent(resourceManager
				.GetRMContext().GetRMNodes()[nm.GetNodeId()]);
			resourceManager.GetResourceScheduler().Handle(nodeAddEvent1);
			return nm;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCapacityScheduler()
		{
			Log.Info("--- START: testCapacityScheduler ---");
			// Register node1
			string host_0 = "host_0";
			NodeManager nm_0 = RegisterNode(host_0, 1234, 2345, NetworkTopology.DefaultRack, 
				Resources.CreateResource(4 * Gb, 1));
			// Register node2
			string host_1 = "host_1";
			NodeManager nm_1 = RegisterNode(host_1, 1234, 2345, NetworkTopology.DefaultRack, 
				Resources.CreateResource(2 * Gb, 1));
			// ResourceRequest priorities
			Priority priority_0 = Priority.Create(0);
			Priority priority_1 = Priority.Create(1);
			// Submit an application
			Application application_0 = new Application("user_0", "a1", resourceManager);
			application_0.Submit();
			application_0.AddNodeManager(host_0, 1234, nm_0);
			application_0.AddNodeManager(host_1, 1234, nm_1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_0 = Resources.CreateResource
				(1 * Gb, 1);
			application_0.AddResourceRequestSpec(priority_1, capability_0_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_1 = Resources.CreateResource
				(2 * Gb, 1);
			application_0.AddResourceRequestSpec(priority_0, capability_0_1);
			Task task_0_0 = new Task(application_0, priority_1, new string[] { host_0, host_1
				 });
			application_0.AddTask(task_0_0);
			// Submit another application
			Application application_1 = new Application("user_1", "b2", resourceManager);
			application_1.Submit();
			application_1.AddNodeManager(host_0, 1234, nm_0);
			application_1.AddNodeManager(host_1, 1234, nm_1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_1_0 = Resources.CreateResource
				(3 * Gb, 1);
			application_1.AddResourceRequestSpec(priority_1, capability_1_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_1_1 = Resources.CreateResource
				(2 * Gb, 1);
			application_1.AddResourceRequestSpec(priority_0, capability_1_1);
			Task task_1_0 = new Task(application_1, priority_1, new string[] { host_0, host_1
				 });
			application_1.AddTask(task_1_0);
			// Send resource requests to the scheduler
			application_0.Schedule();
			application_1.Schedule();
			// Send a heartbeat to kick the tires on the Scheduler
			Log.Info("Kick!");
			// task_0_0 and task_1_0 allocated, used=4G
			NodeUpdate(nm_0);
			// nothing allocated
			NodeUpdate(nm_1);
			// Get allocations from the scheduler
			application_0.Schedule();
			// task_0_0 
			CheckApplicationResourceUsage(1 * Gb, application_0);
			application_1.Schedule();
			// task_1_0
			CheckApplicationResourceUsage(3 * Gb, application_1);
			CheckNodeResourceUsage(4 * Gb, nm_0);
			// task_0_0 (1G) and task_1_0 (3G)
			CheckNodeResourceUsage(0 * Gb, nm_1);
			// no tasks, 2G available
			Log.Info("Adding new tasks...");
			Task task_1_1 = new Task(application_1, priority_0, new string[] { ResourceRequest
				.Any });
			application_1.AddTask(task_1_1);
			application_1.Schedule();
			Task task_0_1 = new Task(application_0, priority_0, new string[] { host_0, host_1
				 });
			application_0.AddTask(task_0_1);
			application_0.Schedule();
			// Send a heartbeat to kick the tires on the Scheduler
			Log.Info("Sending hb from " + nm_0.GetHostName());
			// nothing new, used=4G
			NodeUpdate(nm_0);
			Log.Info("Sending hb from " + nm_1.GetHostName());
			// task_0_1 is prefer as locality, used=2G
			NodeUpdate(nm_1);
			// Get allocations from the scheduler
			Log.Info("Trying to allocate...");
			application_0.Schedule();
			CheckApplicationResourceUsage(1 * Gb, application_0);
			application_1.Schedule();
			CheckApplicationResourceUsage(5 * Gb, application_1);
			NodeUpdate(nm_0);
			NodeUpdate(nm_1);
			CheckNodeResourceUsage(4 * Gb, nm_0);
			CheckNodeResourceUsage(2 * Gb, nm_1);
			Log.Info("--- END: testCapacityScheduler ---");
		}

		private void NodeUpdate(NodeManager nm)
		{
			RMNode node = resourceManager.GetRMContext().GetRMNodes()[nm.GetNodeId()];
			// Send a heartbeat to kick the tires on the Scheduler
			NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
			resourceManager.GetResourceScheduler().Handle(nodeUpdate);
		}

		private CapacitySchedulerConfiguration SetupQueueConfiguration(CapacitySchedulerConfiguration
			 conf)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b" });
			conf.SetCapacity(A, ACapacity);
			conf.SetCapacity(B, BCapacity);
			// Define 2nd-level queues
			conf.SetQueues(A, new string[] { "a1", "a2" });
			conf.SetCapacity(A1, A1Capacity);
			conf.SetUserLimitFactor(A1, 100.0f);
			conf.SetCapacity(A2, A2Capacity);
			conf.SetUserLimitFactor(A2, 100.0f);
			conf.SetQueues(B, new string[] { "b1", "b2", "b3" });
			conf.SetCapacity(B1, B1Capacity);
			conf.SetUserLimitFactor(B1, 100.0f);
			conf.SetCapacity(B2, B2Capacity);
			conf.SetUserLimitFactor(B2, 100.0f);
			conf.SetCapacity(B3, B3Capacity);
			conf.SetUserLimitFactor(B3, 100.0f);
			Log.Info("Setup top-level queues a and b");
			return conf;
		}

		[NUnit.Framework.Test]
		public virtual void TestMaximumCapacitySetup()
		{
			float delta = 0.0000001f;
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			NUnit.Framework.Assert.AreEqual(CapacitySchedulerConfiguration.MaximumCapacityValue
				, conf.GetNonLabeledQueueMaximumCapacity(A), delta);
			conf.SetMaximumCapacity(A, 50.0f);
			NUnit.Framework.Assert.AreEqual(50.0f, conf.GetNonLabeledQueueMaximumCapacity(A), 
				delta);
			conf.SetMaximumCapacity(A, -1);
			NUnit.Framework.Assert.AreEqual(CapacitySchedulerConfiguration.MaximumCapacityValue
				, conf.GetNonLabeledQueueMaximumCapacity(A), delta);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshQueues()
		{
			CapacityScheduler cs = new CapacityScheduler();
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(conf), new NMTokenSecretManagerInRM(conf), new ClientToAMTokenSecretManagerInRM
				(), null);
			SetupQueueConfiguration(conf);
			cs.SetConf(new YarnConfiguration());
			cs.SetRMContext(resourceManager.GetRMContext());
			cs.Init(conf);
			cs.Start();
			cs.Reinitialize(conf, rmContext);
			CheckQueueCapacities(cs, ACapacity, BCapacity);
			conf.SetCapacity(A, 80f);
			conf.SetCapacity(B, 20f);
			cs.Reinitialize(conf, mockContext);
			CheckQueueCapacities(cs, 80f, 20f);
			cs.Stop();
		}

		internal virtual void CheckQueueCapacities(CapacityScheduler cs, float capacityA, 
			float capacityB)
		{
			CSQueue rootQueue = cs.GetRootQueue();
			CSQueue queueA = FindQueue(rootQueue, A);
			CSQueue queueB = FindQueue(rootQueue, B);
			CSQueue queueA1 = FindQueue(queueA, A1);
			CSQueue queueA2 = FindQueue(queueA, A2);
			CSQueue queueB1 = FindQueue(queueB, B1);
			CSQueue queueB2 = FindQueue(queueB, B2);
			CSQueue queueB3 = FindQueue(queueB, B3);
			float capA = capacityA / 100.0f;
			float capB = capacityB / 100.0f;
			CheckQueueCapacity(queueA, capA, capA, 1.0f, 1.0f);
			CheckQueueCapacity(queueB, capB, capB, 1.0f, 1.0f);
			CheckQueueCapacity(queueA1, A1Capacity / 100.0f, (A1Capacity / 100.0f) * capA, 1.0f
				, 1.0f);
			CheckQueueCapacity(queueA2, A2Capacity / 100.0f, (A2Capacity / 100.0f) * capA, 1.0f
				, 1.0f);
			CheckQueueCapacity(queueB1, B1Capacity / 100.0f, (B1Capacity / 100.0f) * capB, 1.0f
				, 1.0f);
			CheckQueueCapacity(queueB2, B2Capacity / 100.0f, (B2Capacity / 100.0f) * capB, 1.0f
				, 1.0f);
			CheckQueueCapacity(queueB3, B3Capacity / 100.0f, (B3Capacity / 100.0f) * capB, 1.0f
				, 1.0f);
		}

		private void CheckQueueCapacity(CSQueue q, float expectedCapacity, float expectedAbsCapacity
			, float expectedMaxCapacity, float expectedAbsMaxCapacity)
		{
			float epsilon = 1e-5f;
			NUnit.Framework.Assert.AreEqual("capacity", expectedCapacity, q.GetCapacity(), epsilon
				);
			NUnit.Framework.Assert.AreEqual("absolute capacity", expectedAbsCapacity, q.GetAbsoluteCapacity
				(), epsilon);
			NUnit.Framework.Assert.AreEqual("maximum capacity", expectedMaxCapacity, q.GetMaximumCapacity
				(), epsilon);
			NUnit.Framework.Assert.AreEqual("absolute maximum capacity", expectedAbsMaxCapacity
				, q.GetAbsoluteMaximumCapacity(), epsilon);
		}

		private CSQueue FindQueue(CSQueue root, string queuePath)
		{
			if (root.GetQueuePath().Equals(queuePath))
			{
				return root;
			}
			IList<CSQueue> childQueues = root.GetChildQueues();
			if (childQueues != null)
			{
				foreach (CSQueue q in childQueues)
				{
					if (queuePath.StartsWith(q.GetQueuePath()))
					{
						CSQueue result = FindQueue(q, queuePath);
						if (result != null)
						{
							return result;
						}
					}
				}
			}
			return null;
		}

		private void CheckApplicationResourceUsage(int expected, Application application)
		{
			NUnit.Framework.Assert.AreEqual(expected, application.GetUsedResources().GetMemory
				());
		}

		private void CheckNodeResourceUsage(int expected, NodeManager node)
		{
			NUnit.Framework.Assert.AreEqual(expected, node.GetUsed().GetMemory());
			node.CheckResourceUsage();
		}

		/// <summary>
		/// Test that parseQueue throws an exception when two leaf queues have the
		/// same name
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestParseQueue()
		{
			CapacityScheduler cs = new CapacityScheduler();
			cs.SetConf(new YarnConfiguration());
			cs.SetRMContext(resourceManager.GetRMContext());
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(conf);
			cs.Init(conf);
			cs.Start();
			conf.SetQueues(CapacitySchedulerConfiguration.Root + ".a.a1", new string[] { "b1"
				 });
			conf.SetCapacity(CapacitySchedulerConfiguration.Root + ".a.a1.b1", 100.0f);
			conf.SetUserLimitFactor(CapacitySchedulerConfiguration.Root + ".a.a1.b1", 100.0f);
			cs.Reinitialize(conf, new RMContextImpl(null, null, null, null, null, null, new RMContainerTokenSecretManager
				(conf), new NMTokenSecretManagerInRM(conf), new ClientToAMTokenSecretManagerInRM
				(), null));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReconnectedNode()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(csConf);
			CapacityScheduler cs = new CapacityScheduler();
			cs.SetConf(new YarnConfiguration());
			cs.SetRMContext(resourceManager.GetRMContext());
			cs.Init(csConf);
			cs.Start();
			cs.Reinitialize(csConf, new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null));
			RMNode n1 = MockNodes.NewNodeInfo(0, MockNodes.NewResource(4 * Gb), 1);
			RMNode n2 = MockNodes.NewNodeInfo(0, MockNodes.NewResource(2 * Gb), 2);
			cs.Handle(new NodeAddedSchedulerEvent(n1));
			cs.Handle(new NodeAddedSchedulerEvent(n2));
			NUnit.Framework.Assert.AreEqual(6 * Gb, cs.GetClusterResource().GetMemory());
			// reconnect n1 with downgraded memory
			n1 = MockNodes.NewNodeInfo(0, MockNodes.NewResource(2 * Gb), 1);
			cs.Handle(new NodeRemovedSchedulerEvent(n1));
			cs.Handle(new NodeAddedSchedulerEvent(n1));
			NUnit.Framework.Assert.AreEqual(4 * Gb, cs.GetClusterResource().GetMemory());
			cs.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshQueuesWithNewQueue()
		{
			CapacityScheduler cs = new CapacityScheduler();
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(conf);
			cs.SetConf(new YarnConfiguration());
			cs.SetRMContext(resourceManager.GetRMContext());
			cs.Init(conf);
			cs.Start();
			cs.Reinitialize(conf, new RMContextImpl(null, null, null, null, null, null, new RMContainerTokenSecretManager
				(conf), new NMTokenSecretManagerInRM(conf), new ClientToAMTokenSecretManagerInRM
				(), null));
			CheckQueueCapacities(cs, ACapacity, BCapacity);
			// Add a new queue b4
			string B4 = B + ".b4";
			float B4Capacity = 10;
			B3Capacity -= B4Capacity;
			try
			{
				conf.SetCapacity(A, 80f);
				conf.SetCapacity(B, 20f);
				conf.SetQueues(B, new string[] { "b1", "b2", "b3", "b4" });
				conf.SetCapacity(B1, B1Capacity);
				conf.SetCapacity(B2, B2Capacity);
				conf.SetCapacity(B3, B3Capacity);
				conf.SetCapacity(B4, B4Capacity);
				cs.Reinitialize(conf, mockContext);
				CheckQueueCapacities(cs, 80f, 20f);
				// Verify parent for B4
				CSQueue rootQueue = cs.GetRootQueue();
				CSQueue queueB = FindQueue(rootQueue, B);
				CSQueue queueB4 = FindQueue(queueB, B4);
				NUnit.Framework.Assert.AreEqual(queueB, queueB4.GetParent());
			}
			finally
			{
				B3Capacity += B4Capacity;
				cs.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCapacitySchedulerInfo()
		{
			QueueInfo queueInfo = resourceManager.GetResourceScheduler().GetQueueInfo("a", true
				, true);
			NUnit.Framework.Assert.AreEqual(queueInfo.GetQueueName(), "a");
			NUnit.Framework.Assert.AreEqual(queueInfo.GetChildQueues().Count, 2);
			IList<QueueUserACLInfo> userACLInfo = resourceManager.GetResourceScheduler().GetQueueUserAclInfo
				();
			NUnit.Framework.Assert.IsNotNull(userACLInfo);
			foreach (QueueUserACLInfo queueUserACLInfo in userACLInfo)
			{
				NUnit.Framework.Assert.AreEqual(GetQueueCount(userACLInfo, queueUserACLInfo.GetQueueName
					()), 1);
			}
		}

		private int GetQueueCount(IList<QueueUserACLInfo> queueInformation, string queueName
			)
		{
			int result = 0;
			foreach (QueueUserACLInfo queueUserACLInfo in queueInformation)
			{
				if (queueName.Equals(queueUserACLInfo.GetQueueName()))
				{
					result++;
				}
			}
			return result;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlackListNodes()
		{
			Configuration conf = new Configuration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			rm.Start();
			CapacityScheduler cs = (CapacityScheduler)rm.GetResourceScheduler();
			string host = "127.0.0.1";
			RMNode node = MockNodes.NewNodeInfo(0, MockNodes.NewResource(4 * Gb), 1, host);
			cs.Handle(new NodeAddedSchedulerEvent(node));
			ApplicationId appId = BuilderUtils.NewApplicationId(100, 1);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			RMAppAttemptMetrics attemptMetric = new RMAppAttemptMetrics(appAttemptId, rm.GetRMContext
				());
			RMAppImpl app = Org.Mockito.Mockito.Mock<RMAppImpl>();
			Org.Mockito.Mockito.When(app.GetApplicationId()).ThenReturn(appId);
			RMAppAttemptImpl attempt = Org.Mockito.Mockito.Mock<RMAppAttemptImpl>();
			Org.Mockito.Mockito.When(attempt.GetAppAttemptId()).ThenReturn(appAttemptId);
			Org.Mockito.Mockito.When(attempt.GetRMAppAttemptMetrics()).ThenReturn(attemptMetric
				);
			Org.Mockito.Mockito.When(app.GetCurrentAppAttempt()).ThenReturn(attempt);
			rm.GetRMContext().GetRMApps()[appId] = app;
			SchedulerEvent addAppEvent = new AppAddedSchedulerEvent(appId, "default", "user");
			cs.Handle(addAppEvent);
			SchedulerEvent addAttemptEvent = new AppAttemptAddedSchedulerEvent(appAttemptId, 
				false);
			cs.Handle(addAttemptEvent);
			// Verify the blacklist can be updated independent of requesting containers
			cs.Allocate(appAttemptId, Sharpen.Collections.EmptyList<ResourceRequest>(), Sharpen.Collections
				.EmptyList<ContainerId>(), Sharpen.Collections.SingletonList(host), null);
			NUnit.Framework.Assert.IsTrue(cs.GetApplicationAttempt(appAttemptId).IsBlacklisted
				(host));
			cs.Allocate(appAttemptId, Sharpen.Collections.EmptyList<ResourceRequest>(), Sharpen.Collections
				.EmptyList<ContainerId>(), null, Sharpen.Collections.SingletonList(host));
			NUnit.Framework.Assert.IsFalse(cs.GetApplicationAttempt(appAttemptId).IsBlacklisted
				(host));
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceOverCommit()
		{
			Configuration conf = new Configuration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
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
				Sharpen.Thread.Sleep(100);
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
			AdminService @as = ((MockRM)rm).GetAdminService();
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
			// Verify no NPE is trigger in schedule after resource is updated.
			am1.AddRequests(new string[] { "127.0.0.1", "127.0.0.2" }, 3 * Gb, 1, 1);
			alloc1Response = am1.Schedule();
			NUnit.Framework.Assert.AreEqual("Shouldn't have enough resource to allocate containers"
				, 0, alloc1Response.GetAllocatedContainers().Count);
			int times = 0;
			// try 10 times as scheduling is async process.
			while (alloc1Response.GetAllocatedContainers().Count < 1 && times++ < 10)
			{
				Log.Info("Waiting for containers to be allocated for app 1... Tried " + times + " times already.."
					);
				Sharpen.Thread.Sleep(100);
			}
			NUnit.Framework.Assert.AreEqual("Shouldn't have enough resource to allocate containers"
				, 0, alloc1Response.GetAllocatedContainers().Count);
			rm.Stop();
		}

		public virtual void TestApplicationComparator()
		{
			CapacityScheduler cs = new CapacityScheduler();
			IComparer<FiCaSchedulerApp> appComparator = cs.GetApplicationComparator();
			ApplicationId id1 = ApplicationId.NewInstance(1, 1);
			ApplicationId id2 = ApplicationId.NewInstance(1, 2);
			ApplicationId id3 = ApplicationId.NewInstance(2, 1);
			//same clusterId
			FiCaSchedulerApp app1 = Org.Mockito.Mockito.Mock<FiCaSchedulerApp>();
			Org.Mockito.Mockito.When(app1.GetApplicationId()).ThenReturn(id1);
			FiCaSchedulerApp app2 = Org.Mockito.Mockito.Mock<FiCaSchedulerApp>();
			Org.Mockito.Mockito.When(app2.GetApplicationId()).ThenReturn(id2);
			FiCaSchedulerApp app3 = Org.Mockito.Mockito.Mock<FiCaSchedulerApp>();
			Org.Mockito.Mockito.When(app3.GetApplicationId()).ThenReturn(id3);
			NUnit.Framework.Assert.IsTrue(appComparator.Compare(app1, app2) < 0);
			//different clusterId
			NUnit.Framework.Assert.IsTrue(appComparator.Compare(app1, app3) < 0);
			NUnit.Framework.Assert.IsTrue(appComparator.Compare(app2, app3) < 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAppsInQueue()
		{
			Application application_0 = new Application("user_0", "a1", resourceManager);
			application_0.Submit();
			Application application_1 = new Application("user_0", "a2", resourceManager);
			application_1.Submit();
			Application application_2 = new Application("user_0", "b2", resourceManager);
			application_2.Submit();
			ResourceScheduler scheduler = resourceManager.GetResourceScheduler();
			IList<ApplicationAttemptId> appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			IList<ApplicationAttemptId> appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(application_0.GetApplicationAttemptId
				()));
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(application_1.GetApplicationAttemptId
				()));
			NUnit.Framework.Assert.AreEqual(2, appsInA.Count);
			IList<ApplicationAttemptId> appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(application_0.GetApplicationAttemptId
				()));
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(application_1.GetApplicationAttemptId
				()));
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(application_2.GetApplicationAttemptId
				()));
			NUnit.Framework.Assert.AreEqual(3, appsInRoot.Count);
			NUnit.Framework.Assert.IsNull(scheduler.GetAppsInQueue("nonexistentqueue"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddAndRemoveAppFromCapacityScheduler()
		{
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(conf);
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode> cs = (AbstractYarnScheduler
				<SchedulerApplicationAttempt, SchedulerNode>)rm.GetResourceScheduler();
			SchedulerApplication<SchedulerApplicationAttempt> app = TestSchedulerUtils.VerifyAppAddedAndRemovedFromScheduler
				(cs.GetSchedulerApplications(), cs, "a1");
			NUnit.Framework.Assert.AreEqual("a1", app.GetQueue().GetQueueName());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAsyncScheduling()
		{
			Configuration conf = new Configuration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			rm.Start();
			CapacityScheduler cs = (CapacityScheduler)rm.GetResourceScheduler();
			int Nodes = 100;
			// Register nodes
			for (int i = 0; i < Nodes; ++i)
			{
				string host = "192.168.1." + i;
				RMNode node = MockNodes.NewNodeInfo(0, MockNodes.NewResource(4 * Gb), 1, host);
				cs.Handle(new NodeAddedSchedulerEvent(node));
			}
			// Now directly exercise the scheduling loop
			for (int i_1 = 0; i_1 < Nodes; ++i_1)
			{
				CapacityScheduler.Schedule(cs);
			}
		}

		/// <exception cref="System.Exception"/>
		private MockAM LaunchAM(RMApp app, MockRM rm, MockNM nm)
		{
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			nm.NodeHeartbeat(true);
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			am.RegisterAppAttempt();
			rm.WaitForState(app.GetApplicationId(), RMAppState.Running);
			return am;
		}

		/// <exception cref="System.Exception"/>
		private void WaitForAppPreemptionInfo(RMApp app, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 preempted, int numAMPreempted, int numTaskPreempted, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 currentAttemptPreempted, bool currentAttemptAMPreempted, int numLatestAttemptTaskPreempted
			)
		{
			while (true)
			{
				RMAppMetrics appPM = app.GetRMAppMetrics();
				RMAppAttemptMetrics attemptPM = app.GetCurrentAppAttempt().GetRMAppAttemptMetrics
					();
				if (appPM.GetResourcePreempted().Equals(preempted) && appPM.GetNumAMContainersPreempted
					() == numAMPreempted && appPM.GetNumNonAMContainersPreempted() == numTaskPreempted
					 && attemptPM.GetResourcePreempted().Equals(currentAttemptPreempted) && app.GetCurrentAppAttempt
					().GetRMAppAttemptMetrics().GetIsPreempted() == currentAttemptAMPreempted && attemptPM
					.GetNumNonAMContainersPreempted() == numLatestAttemptTaskPreempted)
				{
					return;
				}
				Sharpen.Thread.Sleep(500);
			}
		}

		/// <exception cref="System.Exception"/>
		private void WaitForNewAttemptCreated(RMApp app, ApplicationAttemptId previousAttemptId
			)
		{
			while (app.GetCurrentAppAttempt().Equals(previousAttemptId))
			{
				Sharpen.Thread.Sleep(500);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAllocateDoesNotBlockOnSchedulerLock()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			TestAMAuthorization.MyContainerManager containerManager = new TestAMAuthorization.MyContainerManager
				();
			TestAMAuthorization.MockRMWithAMS rm = new TestAMAuthorization.MockRMWithAMS(conf
				, containerManager);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("localhost:1234", 5120);
			IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
				, string>(2);
			acls[ApplicationAccessType.ViewApp] = "*";
			RMApp app = rm.SubmitApp(1024, "appname", "appuser", acls);
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			ApplicationAttemptId applicationAttemptId = attempt.GetAppAttemptId();
			int msecToWait = 10000;
			int msecToSleep = 100;
			while (attempt.GetAppAttemptState() != RMAppAttemptState.Launched && msecToWait >
				 0)
			{
				Log.Info("Waiting for AppAttempt to reach LAUNCHED state. " + "Current state is "
					 + attempt.GetAppAttemptState());
				Sharpen.Thread.Sleep(msecToSleep);
				msecToWait -= msecToSleep;
			}
			NUnit.Framework.Assert.AreEqual(attempt.GetAppAttemptState(), RMAppAttemptState.Launched
				);
			// Create a client to the RM.
			YarnRPC rpc = YarnRPC.Create(conf);
			UserGroupInformation currentUser = UserGroupInformation.CreateRemoteUser(applicationAttemptId
				.ToString());
			Credentials credentials = containerManager.GetContainerCredentials();
			IPEndPoint rmBindAddress = rm.GetApplicationMasterService().GetBindAddress();
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> amRMToken = TestAMAuthorization.MockRMWithAMS
				.SetupAndReturnAMRMToken(rmBindAddress, credentials.GetAllTokens());
			currentUser.AddToken(amRMToken);
			ApplicationMasterProtocol client = currentUser.DoAs(new _PrivilegedAction_962(rpc
				, rmBindAddress, conf));
			RegisterApplicationMasterRequest request = RegisterApplicationMasterRequest.NewInstance
				("localhost", 12345, string.Empty);
			client.RegisterApplicationMaster(request);
			// grab the scheduler lock from another thread
			// and verify an allocate call in this thread doesn't block on it
			CapacityScheduler cs = (CapacityScheduler)rm.GetResourceScheduler();
			CyclicBarrier barrier = new CyclicBarrier(2);
			Sharpen.Thread otherThread = new Sharpen.Thread(new _Runnable_978(cs, barrier));
			otherThread.Start();
			barrier.Await();
			AllocateRequest allocateRequest = AllocateRequest.NewInstance(0, 0.0f, null, null
				, null);
			client.Allocate(allocateRequest);
			barrier.Await();
			otherThread.Join();
			rm.Stop();
		}

		private sealed class _PrivilegedAction_962 : PrivilegedAction<ApplicationMasterProtocol
			>
		{
			public _PrivilegedAction_962(YarnRPC rpc, IPEndPoint rmBindAddress, YarnConfiguration
				 conf)
			{
				this.rpc = rpc;
				this.rmBindAddress = rmBindAddress;
				this.conf = conf;
			}

			public ApplicationMasterProtocol Run()
			{
				return (ApplicationMasterProtocol)rpc.GetProxy(typeof(ApplicationMasterProtocol), 
					rmBindAddress, conf);
			}

			private readonly YarnRPC rpc;

			private readonly IPEndPoint rmBindAddress;

			private readonly YarnConfiguration conf;
		}

		private sealed class _Runnable_978 : Runnable
		{
			public _Runnable_978(CapacityScheduler cs, CyclicBarrier barrier)
			{
				this.cs = cs;
				this.barrier = barrier;
			}

			public void Run()
			{
				lock (cs)
				{
					try
					{
						barrier.Await();
						barrier.Await();
					}
					catch (Exception e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
					catch (BrokenBarrierException e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
				}
			}

			private readonly CapacityScheduler cs;

			private readonly CyclicBarrier barrier;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNumClusterNodes()
		{
			YarnConfiguration conf = new YarnConfiguration();
			CapacityScheduler cs = new CapacityScheduler();
			cs.SetConf(conf);
			RMContext rmContext = TestUtils.GetMockRMContext();
			cs.SetRMContext(rmContext);
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(csConf);
			cs.Init(csConf);
			cs.Start();
			NUnit.Framework.Assert.AreEqual(0, cs.GetNumClusterNodes());
			RMNode n1 = MockNodes.NewNodeInfo(0, MockNodes.NewResource(4 * Gb), 1);
			RMNode n2 = MockNodes.NewNodeInfo(0, MockNodes.NewResource(2 * Gb), 2);
			cs.Handle(new NodeAddedSchedulerEvent(n1));
			cs.Handle(new NodeAddedSchedulerEvent(n2));
			NUnit.Framework.Assert.AreEqual(2, cs.GetNumClusterNodes());
			cs.Handle(new NodeRemovedSchedulerEvent(n1));
			NUnit.Framework.Assert.AreEqual(1, cs.GetNumClusterNodes());
			cs.Handle(new NodeAddedSchedulerEvent(n1));
			NUnit.Framework.Assert.AreEqual(2, cs.GetNumClusterNodes());
			cs.Handle(new NodeRemovedSchedulerEvent(n2));
			cs.Handle(new NodeRemovedSchedulerEvent(n1));
			NUnit.Framework.Assert.AreEqual(0, cs.GetNumClusterNodes());
			cs.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPreemptionInfo()
		{
			Configuration conf = new Configuration();
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 3);
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			int ContainerMemory = 1024;
			// start RM
			MockRM rm1 = new MockRM(conf);
			rm1.Start();
			// get scheduler
			CapacityScheduler cs = (CapacityScheduler)rm1.GetResourceScheduler();
			// start NM
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(ContainerMemory);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			// get scheduler app
			FiCaSchedulerApp schedulerAppAttempt = cs.GetSchedulerApplications()[app0.GetApplicationId
				()].GetCurrentAppAttempt();
			// allocate some containers and launch them
			IList<Container> allocatedContainers = am0.AllocateAndWaitForContainers(3, ContainerMemory
				, nm1);
			// kill the 3 containers
			foreach (Container c in allocatedContainers)
			{
				cs.KillContainer(schedulerAppAttempt.GetRMContainer(c.GetId()));
			}
			// check values
			WaitForAppPreemptionInfo(app0, Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(ContainerMemory * 3, 3), 0, 3, Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(ContainerMemory * 3, 3), false, 3);
			// kill app0-attempt0 AM container
			cs.KillContainer(schedulerAppAttempt.GetRMContainer(app0.GetCurrentAppAttempt().GetMasterContainer
				().GetId()));
			// wait for app0 failed
			WaitForNewAttemptCreated(app0, am0.GetApplicationAttemptId());
			// check values
			WaitForAppPreemptionInfo(app0, Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(ContainerMemory * 4, 4), 1, 3, Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), false, 0);
			// launch app0-attempt1
			MockAM am1 = LaunchAM(app0, rm1, nm1);
			schedulerAppAttempt = cs.GetSchedulerApplications()[app0.GetApplicationId()].GetCurrentAppAttempt
				();
			// allocate some containers and launch them
			allocatedContainers = am1.AllocateAndWaitForContainers(3, ContainerMemory, nm1);
			foreach (Container c_1 in allocatedContainers)
			{
				cs.KillContainer(schedulerAppAttempt.GetRMContainer(c_1.GetId()));
			}
			// check values
			WaitForAppPreemptionInfo(app0, Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(ContainerMemory * 7, 7), 1, 6, Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(ContainerMemory * 3, 3), false, 3);
			rm1.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecoverRequestAfterPreemption()
		{
			Configuration conf = new Configuration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			MockRM rm1 = new MockRM(conf);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("127.0.0.1:1234", 8000);
			RMApp app1 = rm1.SubmitApp(1024);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			CapacityScheduler cs = (CapacityScheduler)rm1.GetResourceScheduler();
			// request a container.
			am1.Allocate("127.0.0.1", 1024, 1, new AList<ContainerId>());
			ContainerId containerId1 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 2);
			rm1.WaitForState(nm1, containerId1, RMContainerState.Allocated);
			RMContainer rmContainer = cs.GetRMContainer(containerId1);
			IList<ResourceRequest> requests = rmContainer.GetResourceRequests();
			FiCaSchedulerApp app = cs.GetApplicationAttempt(am1.GetApplicationAttemptId());
			FiCaSchedulerNode node = cs.GetNode(rmContainer.GetAllocatedNode());
			foreach (ResourceRequest request in requests)
			{
				// Skip the OffRack and RackLocal resource requests.
				if (request.GetResourceName().Equals(node.GetRackName()) || request.GetResourceName
					().Equals(ResourceRequest.Any))
				{
					continue;
				}
				// Already the node local resource request is cleared from RM after
				// allocation.
				NUnit.Framework.Assert.IsNull(app.GetResourceRequest(request.GetPriority(), request
					.GetResourceName()));
			}
			// Call killContainer to preempt the container
			cs.KillContainer(rmContainer);
			NUnit.Framework.Assert.AreEqual(3, requests.Count);
			foreach (ResourceRequest request_1 in requests)
			{
				// Resource request must have added back in RM after preempt event
				// handling.
				NUnit.Framework.Assert.AreEqual(1, app.GetResourceRequest(request_1.GetPriority()
					, request_1.GetResourceName()).GetNumContainers());
			}
			// New container will be allocated and will move to ALLOCATED state
			ContainerId containerId2 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 3);
			rm1.WaitForState(nm1, containerId2, RMContainerState.Allocated);
			// allocate container
			IList<Container> containers = am1.Allocate(new AList<ResourceRequest>(), new AList
				<ContainerId>()).GetAllocatedContainers();
			// Now with updated ResourceRequest, a container is allocated for AM.
			NUnit.Framework.Assert.IsTrue(containers.Count == 1);
		}

		private MockRM SetUpMove()
		{
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			return SetUpMove(conf);
		}

		private MockRM SetUpMove(Configuration config)
		{
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(config);
			SetupQueueConfiguration(conf);
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			rm.Start();
			return rm;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveAppBasic()
		{
			MockRM rm = SetUpMove();
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm.GetResourceScheduler(
				);
			// submit an app
			RMApp app = rm.SubmitApp(Gb, "test-move-1", "user_0", null, "a1");
			ApplicationAttemptId appAttemptId = rm.GetApplicationReport(app.GetApplicationId(
				)).GetCurrentApplicationAttemptId();
			// check preconditions
			IList<ApplicationAttemptId> appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			string queue = scheduler.GetApplicationAttempt(appsInA1[0]).GetQueue().GetQueueName
				();
			NUnit.Framework.Assert.IsTrue(queue.Equals("a1"));
			IList<ApplicationAttemptId> appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			IList<ApplicationAttemptId> appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			IList<ApplicationAttemptId> appsInB1 = scheduler.GetAppsInQueue("b1");
			NUnit.Framework.Assert.IsTrue(appsInB1.IsEmpty());
			IList<ApplicationAttemptId> appsInB = scheduler.GetAppsInQueue("b");
			NUnit.Framework.Assert.IsTrue(appsInB.IsEmpty());
			// now move the app
			scheduler.MoveApplication(app.GetApplicationId(), "b1");
			// check postconditions
			appsInB1 = scheduler.GetAppsInQueue("b1");
			NUnit.Framework.Assert.AreEqual(1, appsInB1.Count);
			queue = scheduler.GetApplicationAttempt(appsInB1[0]).GetQueue().GetQueueName();
			NUnit.Framework.Assert.IsTrue(queue.Equals("b1"));
			appsInB = scheduler.GetAppsInQueue("b");
			NUnit.Framework.Assert.IsTrue(appsInB.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInB.Count);
			appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.IsTrue(appsInA1.IsEmpty());
			appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.IsEmpty());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveAppSameParent()
		{
			MockRM rm = SetUpMove();
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm.GetResourceScheduler(
				);
			// submit an app
			RMApp app = rm.SubmitApp(Gb, "test-move-1", "user_0", null, "a1");
			ApplicationAttemptId appAttemptId = rm.GetApplicationReport(app.GetApplicationId(
				)).GetCurrentApplicationAttemptId();
			// check preconditions
			IList<ApplicationAttemptId> appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			string queue = scheduler.GetApplicationAttempt(appsInA1[0]).GetQueue().GetQueueName
				();
			NUnit.Framework.Assert.IsTrue(queue.Equals("a1"));
			IList<ApplicationAttemptId> appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			IList<ApplicationAttemptId> appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			IList<ApplicationAttemptId> appsInA2 = scheduler.GetAppsInQueue("a2");
			NUnit.Framework.Assert.IsTrue(appsInA2.IsEmpty());
			// now move the app
			scheduler.MoveApplication(app.GetApplicationId(), "a2");
			// check postconditions
			appsInA2 = scheduler.GetAppsInQueue("a2");
			NUnit.Framework.Assert.AreEqual(1, appsInA2.Count);
			queue = scheduler.GetApplicationAttempt(appsInA2[0]).GetQueue().GetQueueName();
			NUnit.Framework.Assert.IsTrue(queue.Equals("a2"));
			appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.IsTrue(appsInA1.IsEmpty());
			appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveAppForMoveToQueueWithFreeCap()
		{
			ResourceScheduler scheduler = resourceManager.GetResourceScheduler();
			// Register node1
			string host_0 = "host_0";
			NodeManager nm_0 = RegisterNode(host_0, 1234, 2345, NetworkTopology.DefaultRack, 
				Resources.CreateResource(4 * Gb, 1));
			// Register node2
			string host_1 = "host_1";
			NodeManager nm_1 = RegisterNode(host_1, 1234, 2345, NetworkTopology.DefaultRack, 
				Resources.CreateResource(2 * Gb, 1));
			// ResourceRequest priorities
			Priority priority_0 = Priority.Create(0);
			Priority priority_1 = Priority.Create(1);
			// Submit application_0
			Application application_0 = new Application("user_0", "a1", resourceManager);
			application_0.Submit();
			// app + app attempt event sent to scheduler
			application_0.AddNodeManager(host_0, 1234, nm_0);
			application_0.AddNodeManager(host_1, 1234, nm_1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_0 = Resources.CreateResource
				(1 * Gb, 1);
			application_0.AddResourceRequestSpec(priority_1, capability_0_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_1 = Resources.CreateResource
				(2 * Gb, 1);
			application_0.AddResourceRequestSpec(priority_0, capability_0_1);
			Task task_0_0 = new Task(application_0, priority_1, new string[] { host_0, host_1
				 });
			application_0.AddTask(task_0_0);
			// Submit application_1
			Application application_1 = new Application("user_1", "b2", resourceManager);
			application_1.Submit();
			// app + app attempt event sent to scheduler
			application_1.AddNodeManager(host_0, 1234, nm_0);
			application_1.AddNodeManager(host_1, 1234, nm_1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_1_0 = Resources.CreateResource
				(1 * Gb, 1);
			application_1.AddResourceRequestSpec(priority_1, capability_1_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_1_1 = Resources.CreateResource
				(2 * Gb, 1);
			application_1.AddResourceRequestSpec(priority_0, capability_1_1);
			Task task_1_0 = new Task(application_1, priority_1, new string[] { host_0, host_1
				 });
			application_1.AddTask(task_1_0);
			// Send resource requests to the scheduler
			application_0.Schedule();
			// allocate
			application_1.Schedule();
			// allocate
			// task_0_0 task_1_0 allocated, used=2G
			NodeUpdate(nm_0);
			// nothing allocated
			NodeUpdate(nm_1);
			// Get allocations from the scheduler
			application_0.Schedule();
			// task_0_0
			CheckApplicationResourceUsage(1 * Gb, application_0);
			application_1.Schedule();
			// task_1_0
			CheckApplicationResourceUsage(1 * Gb, application_1);
			CheckNodeResourceUsage(2 * Gb, nm_0);
			// task_0_0 (1G) and task_1_0 (1G) 2G
			// available
			CheckNodeResourceUsage(0 * Gb, nm_1);
			// no tasks, 2G available
			// move app from a1(30% cap of total 10.5% cap) to b1(79,2% cap of 89,5%
			// total cap)
			scheduler.MoveApplication(application_0.GetApplicationId(), "b1");
			// 2GB 1C
			Task task_1_1 = new Task(application_1, priority_0, new string[] { ResourceRequest
				.Any });
			application_1.AddTask(task_1_1);
			application_1.Schedule();
			// 2GB 1C
			Task task_0_1 = new Task(application_0, priority_0, new string[] { host_0, host_1
				 });
			application_0.AddTask(task_0_1);
			application_0.Schedule();
			// prev 2G used free 2G
			NodeUpdate(nm_0);
			// prev 0G used free 2G
			NodeUpdate(nm_1);
			// Get allocations from the scheduler
			application_1.Schedule();
			CheckApplicationResourceUsage(3 * Gb, application_1);
			// Get allocations from the scheduler
			application_0.Schedule();
			CheckApplicationResourceUsage(3 * Gb, application_0);
			CheckNodeResourceUsage(4 * Gb, nm_0);
			CheckNodeResourceUsage(2 * Gb, nm_1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveAppSuccess()
		{
			ResourceScheduler scheduler = resourceManager.GetResourceScheduler();
			// Register node1
			string host_0 = "host_0";
			NodeManager nm_0 = RegisterNode(host_0, 1234, 2345, NetworkTopology.DefaultRack, 
				Resources.CreateResource(5 * Gb, 1));
			// Register node2
			string host_1 = "host_1";
			NodeManager nm_1 = RegisterNode(host_1, 1234, 2345, NetworkTopology.DefaultRack, 
				Resources.CreateResource(5 * Gb, 1));
			// ResourceRequest priorities
			Priority priority_0 = Priority.Create(0);
			Priority priority_1 = Priority.Create(1);
			// Submit application_0
			Application application_0 = new Application("user_0", "a1", resourceManager);
			application_0.Submit();
			// app + app attempt event sent to scheduler
			application_0.AddNodeManager(host_0, 1234, nm_0);
			application_0.AddNodeManager(host_1, 1234, nm_1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_0 = Resources.CreateResource
				(3 * Gb, 1);
			application_0.AddResourceRequestSpec(priority_1, capability_0_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_1 = Resources.CreateResource
				(2 * Gb, 1);
			application_0.AddResourceRequestSpec(priority_0, capability_0_1);
			Task task_0_0 = new Task(application_0, priority_1, new string[] { host_0, host_1
				 });
			application_0.AddTask(task_0_0);
			// Submit application_1
			Application application_1 = new Application("user_1", "b2", resourceManager);
			application_1.Submit();
			// app + app attempt event sent to scheduler
			application_1.AddNodeManager(host_0, 1234, nm_0);
			application_1.AddNodeManager(host_1, 1234, nm_1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_1_0 = Resources.CreateResource
				(1 * Gb, 1);
			application_1.AddResourceRequestSpec(priority_1, capability_1_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_1_1 = Resources.CreateResource
				(2 * Gb, 1);
			application_1.AddResourceRequestSpec(priority_0, capability_1_1);
			Task task_1_0 = new Task(application_1, priority_1, new string[] { host_0, host_1
				 });
			application_1.AddTask(task_1_0);
			// Send resource requests to the scheduler
			application_0.Schedule();
			// allocate
			application_1.Schedule();
			// allocate
			// b2 can only run 1 app at a time
			scheduler.MoveApplication(application_0.GetApplicationId(), "b2");
			NodeUpdate(nm_0);
			NodeUpdate(nm_1);
			// Get allocations from the scheduler
			application_0.Schedule();
			// task_0_0
			CheckApplicationResourceUsage(0 * Gb, application_0);
			application_1.Schedule();
			// task_1_0
			CheckApplicationResourceUsage(1 * Gb, application_1);
			// task_1_0 (1G) application_0 moved to b2 with max running app 1 so it is
			// not scheduled
			CheckNodeResourceUsage(1 * Gb, nm_0);
			CheckNodeResourceUsage(0 * Gb, nm_1);
			// lets move application_0 to a queue where it can run
			scheduler.MoveApplication(application_0.GetApplicationId(), "a2");
			application_0.Schedule();
			NodeUpdate(nm_1);
			// Get allocations from the scheduler
			application_0.Schedule();
			// task_0_0
			CheckApplicationResourceUsage(3 * Gb, application_0);
			CheckNodeResourceUsage(1 * Gb, nm_0);
			CheckNodeResourceUsage(3 * Gb, nm_1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMoveAppViolateQueueState()
		{
			resourceManager = new _ResourceManager_1526();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(csConf);
			StringBuilder qState = new StringBuilder();
			qState.Append(CapacitySchedulerConfiguration.Prefix).Append(B).Append(CapacitySchedulerConfiguration
				.Dot).Append(CapacitySchedulerConfiguration.State);
			csConf.Set(qState.ToString(), QueueState.Stopped.ToString());
			YarnConfiguration conf = new YarnConfiguration(csConf);
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			resourceManager.Init(conf);
			resourceManager.GetRMContext().GetContainerTokenSecretManager().RollMasterKey();
			resourceManager.GetRMContext().GetNMTokenSecretManager().RollMasterKey();
			((AsyncDispatcher)resourceManager.GetRMContext().GetDispatcher()).Start();
			mockContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(mockContext.GetConfigurationProvider()).ThenReturn(new LocalConfigurationProvider
				());
			ResourceScheduler scheduler = resourceManager.GetResourceScheduler();
			// Register node1
			string host_0 = "host_0";
			NodeManager nm_0 = RegisterNode(host_0, 1234, 2345, NetworkTopology.DefaultRack, 
				Resources.CreateResource(6 * Gb, 1));
			// ResourceRequest priorities
			Priority priority_0 = Priority.Create(0);
			Priority priority_1 = Priority.Create(1);
			// Submit application_0
			Application application_0 = new Application("user_0", "a1", resourceManager);
			application_0.Submit();
			// app + app attempt event sent to scheduler
			application_0.AddNodeManager(host_0, 1234, nm_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_0 = Resources.CreateResource
				(3 * Gb, 1);
			application_0.AddResourceRequestSpec(priority_1, capability_0_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_1 = Resources.CreateResource
				(2 * Gb, 1);
			application_0.AddResourceRequestSpec(priority_0, capability_0_1);
			Task task_0_0 = new Task(application_0, priority_1, new string[] { host_0 });
			application_0.AddTask(task_0_0);
			// Send resource requests to the scheduler
			application_0.Schedule();
			// allocate
			// task_0_0 allocated
			NodeUpdate(nm_0);
			// Get allocations from the scheduler
			application_0.Schedule();
			// task_0_0
			CheckApplicationResourceUsage(3 * Gb, application_0);
			CheckNodeResourceUsage(3 * Gb, nm_0);
			// b2 queue contains 3GB consumption app,
			// add another 3GB will hit max capacity limit on queue b
			scheduler.MoveApplication(application_0.GetApplicationId(), "b1");
		}

		private sealed class _ResourceManager_1526 : ResourceManager
		{
			public _ResourceManager_1526()
			{
			}

			protected internal override RMNodeLabelsManager CreateNodeLabelManager()
			{
				RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
				mgr.Init(this.GetConfig());
				return mgr;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveAppQueueMetricsCheck()
		{
			ResourceScheduler scheduler = resourceManager.GetResourceScheduler();
			// Register node1
			string host_0 = "host_0";
			NodeManager nm_0 = RegisterNode(host_0, 1234, 2345, NetworkTopology.DefaultRack, 
				Resources.CreateResource(5 * Gb, 1));
			// Register node2
			string host_1 = "host_1";
			NodeManager nm_1 = RegisterNode(host_1, 1234, 2345, NetworkTopology.DefaultRack, 
				Resources.CreateResource(5 * Gb, 1));
			// ResourceRequest priorities
			Priority priority_0 = Priority.Create(0);
			Priority priority_1 = Priority.Create(1);
			// Submit application_0
			Application application_0 = new Application("user_0", "a1", resourceManager);
			application_0.Submit();
			// app + app attempt event sent to scheduler
			application_0.AddNodeManager(host_0, 1234, nm_0);
			application_0.AddNodeManager(host_1, 1234, nm_1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_0 = Resources.CreateResource
				(3 * Gb, 1);
			application_0.AddResourceRequestSpec(priority_1, capability_0_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_1 = Resources.CreateResource
				(2 * Gb, 1);
			application_0.AddResourceRequestSpec(priority_0, capability_0_1);
			Task task_0_0 = new Task(application_0, priority_1, new string[] { host_0, host_1
				 });
			application_0.AddTask(task_0_0);
			// Submit application_1
			Application application_1 = new Application("user_1", "b2", resourceManager);
			application_1.Submit();
			// app + app attempt event sent to scheduler
			application_1.AddNodeManager(host_0, 1234, nm_0);
			application_1.AddNodeManager(host_1, 1234, nm_1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_1_0 = Resources.CreateResource
				(1 * Gb, 1);
			application_1.AddResourceRequestSpec(priority_1, capability_1_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_1_1 = Resources.CreateResource
				(2 * Gb, 1);
			application_1.AddResourceRequestSpec(priority_0, capability_1_1);
			Task task_1_0 = new Task(application_1, priority_1, new string[] { host_0, host_1
				 });
			application_1.AddTask(task_1_0);
			// Send resource requests to the scheduler
			application_0.Schedule();
			// allocate
			application_1.Schedule();
			// allocate
			NodeUpdate(nm_0);
			NodeUpdate(nm_1);
			CapacityScheduler cs = (CapacityScheduler)resourceManager.GetResourceScheduler();
			CSQueue origRootQ = cs.GetRootQueue();
			CapacitySchedulerInfo oldInfo = new CapacitySchedulerInfo(origRootQ);
			int origNumAppsA = GetNumAppsInQueue("a", origRootQ.GetChildQueues());
			int origNumAppsRoot = origRootQ.GetNumApplications();
			scheduler.MoveApplication(application_0.GetApplicationId(), "a2");
			CSQueue newRootQ = cs.GetRootQueue();
			int newNumAppsA = GetNumAppsInQueue("a", newRootQ.GetChildQueues());
			int newNumAppsRoot = newRootQ.GetNumApplications();
			CapacitySchedulerInfo newInfo = new CapacitySchedulerInfo(newRootQ);
			CapacitySchedulerLeafQueueInfo origOldA1 = (CapacitySchedulerLeafQueueInfo)GetQueueInfo
				("a1", oldInfo.GetQueues());
			CapacitySchedulerLeafQueueInfo origNewA1 = (CapacitySchedulerLeafQueueInfo)GetQueueInfo
				("a1", newInfo.GetQueues());
			CapacitySchedulerLeafQueueInfo targetOldA2 = (CapacitySchedulerLeafQueueInfo)GetQueueInfo
				("a2", oldInfo.GetQueues());
			CapacitySchedulerLeafQueueInfo targetNewA2 = (CapacitySchedulerLeafQueueInfo)GetQueueInfo
				("a2", newInfo.GetQueues());
			// originally submitted here
			NUnit.Framework.Assert.AreEqual(1, origOldA1.GetNumApplications());
			NUnit.Framework.Assert.AreEqual(1, origNumAppsA);
			NUnit.Framework.Assert.AreEqual(2, origNumAppsRoot);
			// after the move
			NUnit.Framework.Assert.AreEqual(0, origNewA1.GetNumApplications());
			NUnit.Framework.Assert.AreEqual(1, newNumAppsA);
			NUnit.Framework.Assert.AreEqual(2, newNumAppsRoot);
			// original consumption on a1
			NUnit.Framework.Assert.AreEqual(3 * Gb, origOldA1.GetResourcesUsed().GetMemory());
			NUnit.Framework.Assert.AreEqual(1, origOldA1.GetResourcesUsed().GetvCores());
			NUnit.Framework.Assert.AreEqual(0, origNewA1.GetResourcesUsed().GetMemory());
			// after the move
			NUnit.Framework.Assert.AreEqual(0, origNewA1.GetResourcesUsed().GetvCores());
			// after the move
			// app moved here with live containers
			NUnit.Framework.Assert.AreEqual(3 * Gb, targetNewA2.GetResourcesUsed().GetMemory(
				));
			NUnit.Framework.Assert.AreEqual(1, targetNewA2.GetResourcesUsed().GetvCores());
			// it was empty before the move
			NUnit.Framework.Assert.AreEqual(0, targetOldA2.GetNumApplications());
			NUnit.Framework.Assert.AreEqual(0, targetOldA2.GetResourcesUsed().GetMemory());
			NUnit.Framework.Assert.AreEqual(0, targetOldA2.GetResourcesUsed().GetvCores());
			// after the app moved here
			NUnit.Framework.Assert.AreEqual(1, targetNewA2.GetNumApplications());
			// 1 container on original queue before move
			NUnit.Framework.Assert.AreEqual(1, origOldA1.GetNumContainers());
			// after the move the resource released
			NUnit.Framework.Assert.AreEqual(0, origNewA1.GetNumContainers());
			// and moved to the new queue
			NUnit.Framework.Assert.AreEqual(1, targetNewA2.GetNumContainers());
			// which originally didn't have any
			NUnit.Framework.Assert.AreEqual(0, targetOldA2.GetNumContainers());
			// 1 user with 3GB
			NUnit.Framework.Assert.AreEqual(3 * Gb, origOldA1.GetUsers().GetUsersList()[0].GetResourcesUsed
				().GetMemory());
			// 1 user with 1 core
			NUnit.Framework.Assert.AreEqual(1, origOldA1.GetUsers().GetUsersList()[0].GetResourcesUsed
				().GetvCores());
			// user ha no more running app in the orig queue
			NUnit.Framework.Assert.AreEqual(0, origNewA1.GetUsers().GetUsersList().Count);
			// 1 user with 3GB
			NUnit.Framework.Assert.AreEqual(3 * Gb, targetNewA2.GetUsers().GetUsersList()[0].
				GetResourcesUsed().GetMemory());
			// 1 user with 1 core
			NUnit.Framework.Assert.AreEqual(1, targetNewA2.GetUsers().GetUsersList()[0].GetResourcesUsed
				().GetvCores());
			// Get allocations from the scheduler
			application_0.Schedule();
			// task_0_0
			CheckApplicationResourceUsage(3 * Gb, application_0);
			application_1.Schedule();
			// task_1_0
			CheckApplicationResourceUsage(1 * Gb, application_1);
			// task_1_0 (1G) application_0 moved to b2 with max running app 1 so it is
			// not scheduled
			CheckNodeResourceUsage(4 * Gb, nm_0);
			CheckNodeResourceUsage(0 * Gb, nm_1);
		}

		private int GetNumAppsInQueue(string name, IList<CSQueue> queues)
		{
			foreach (CSQueue queue in queues)
			{
				if (queue.GetQueueName().Equals(name))
				{
					return queue.GetNumApplications();
				}
			}
			return -1;
		}

		private CapacitySchedulerQueueInfo GetQueueInfo(string name, CapacitySchedulerQueueInfoList
			 info)
		{
			if (info != null)
			{
				foreach (CapacitySchedulerQueueInfo queueInfo in info.GetQueueInfoList())
				{
					if (queueInfo.GetQueueName().Equals(name))
					{
						return queueInfo;
					}
					else
					{
						CapacitySchedulerQueueInfo result = GetQueueInfo(name, queueInfo.GetQueues());
						if (result == null)
						{
							continue;
						}
						return result;
					}
				}
			}
			return null;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveAllApps()
		{
			MockRM rm = SetUpMove();
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm.GetResourceScheduler(
				);
			// submit an app
			RMApp app = rm.SubmitApp(Gb, "test-move-1", "user_0", null, "a1");
			ApplicationAttemptId appAttemptId = rm.GetApplicationReport(app.GetApplicationId(
				)).GetCurrentApplicationAttemptId();
			// check preconditions
			IList<ApplicationAttemptId> appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			IList<ApplicationAttemptId> appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			string queue = scheduler.GetApplicationAttempt(appsInA1[0]).GetQueue().GetQueueName
				();
			NUnit.Framework.Assert.IsTrue(queue.Equals("a1"));
			IList<ApplicationAttemptId> appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			IList<ApplicationAttemptId> appsInB1 = scheduler.GetAppsInQueue("b1");
			NUnit.Framework.Assert.IsTrue(appsInB1.IsEmpty());
			IList<ApplicationAttemptId> appsInB = scheduler.GetAppsInQueue("b");
			NUnit.Framework.Assert.IsTrue(appsInB.IsEmpty());
			// now move the app
			scheduler.MoveAllApps("a1", "b1");
			// check postconditions
			Sharpen.Thread.Sleep(1000);
			appsInB1 = scheduler.GetAppsInQueue("b1");
			NUnit.Framework.Assert.AreEqual(1, appsInB1.Count);
			queue = scheduler.GetApplicationAttempt(appsInB1[0]).GetQueue().GetQueueName();
			NUnit.Framework.Assert.IsTrue(queue.Equals("b1"));
			appsInB = scheduler.GetAppsInQueue("b");
			NUnit.Framework.Assert.IsTrue(appsInB.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInB.Count);
			appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.IsTrue(appsInA1.IsEmpty());
			appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.IsEmpty());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveAllAppsInvalidDestination()
		{
			MockRM rm = SetUpMove();
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm.GetResourceScheduler(
				);
			// submit an app
			RMApp app = rm.SubmitApp(Gb, "test-move-1", "user_0", null, "a1");
			ApplicationAttemptId appAttemptId = rm.GetApplicationReport(app.GetApplicationId(
				)).GetCurrentApplicationAttemptId();
			// check preconditions
			IList<ApplicationAttemptId> appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			IList<ApplicationAttemptId> appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			IList<ApplicationAttemptId> appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			IList<ApplicationAttemptId> appsInB1 = scheduler.GetAppsInQueue("b1");
			NUnit.Framework.Assert.IsTrue(appsInB1.IsEmpty());
			IList<ApplicationAttemptId> appsInB = scheduler.GetAppsInQueue("b");
			NUnit.Framework.Assert.IsTrue(appsInB.IsEmpty());
			// now move the app
			try
			{
				scheduler.MoveAllApps("a1", "DOES_NOT_EXIST");
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException)
			{
			}
			// expected
			// check postconditions, app should still be in a1
			appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			appsInB1 = scheduler.GetAppsInQueue("b1");
			NUnit.Framework.Assert.IsTrue(appsInB1.IsEmpty());
			appsInB = scheduler.GetAppsInQueue("b");
			NUnit.Framework.Assert.IsTrue(appsInB.IsEmpty());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveAllAppsInvalidSource()
		{
			MockRM rm = SetUpMove();
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm.GetResourceScheduler(
				);
			// submit an app
			RMApp app = rm.SubmitApp(Gb, "test-move-1", "user_0", null, "a1");
			ApplicationAttemptId appAttemptId = rm.GetApplicationReport(app.GetApplicationId(
				)).GetCurrentApplicationAttemptId();
			// check preconditions
			IList<ApplicationAttemptId> appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			IList<ApplicationAttemptId> appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			IList<ApplicationAttemptId> appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			IList<ApplicationAttemptId> appsInB1 = scheduler.GetAppsInQueue("b1");
			NUnit.Framework.Assert.IsTrue(appsInB1.IsEmpty());
			IList<ApplicationAttemptId> appsInB = scheduler.GetAppsInQueue("b");
			NUnit.Framework.Assert.IsTrue(appsInB.IsEmpty());
			// now move the app
			try
			{
				scheduler.MoveAllApps("DOES_NOT_EXIST", "b1");
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException)
			{
			}
			// expected
			// check postconditions, app should still be in a1
			appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			appsInB1 = scheduler.GetAppsInQueue("b1");
			NUnit.Framework.Assert.IsTrue(appsInB1.IsEmpty());
			appsInB = scheduler.GetAppsInQueue("b");
			NUnit.Framework.Assert.IsTrue(appsInB.IsEmpty());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillAllAppsInQueue()
		{
			MockRM rm = SetUpMove();
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm.GetResourceScheduler(
				);
			// submit an app
			RMApp app = rm.SubmitApp(Gb, "test-move-1", "user_0", null, "a1");
			ApplicationAttemptId appAttemptId = rm.GetApplicationReport(app.GetApplicationId(
				)).GetCurrentApplicationAttemptId();
			// check preconditions
			IList<ApplicationAttemptId> appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			IList<ApplicationAttemptId> appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			string queue = scheduler.GetApplicationAttempt(appsInA1[0]).GetQueue().GetQueueName
				();
			NUnit.Framework.Assert.IsTrue(queue.Equals("a1"));
			IList<ApplicationAttemptId> appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			// now kill the app
			scheduler.KillAllAppsInQueue("a1");
			// check postconditions
			rm.WaitForState(app.GetApplicationId(), RMAppState.Killed);
			appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.IsEmpty());
			appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.IsTrue(appsInA1.IsEmpty());
			appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.IsEmpty());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillAllAppsInvalidSource()
		{
			MockRM rm = SetUpMove();
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm.GetResourceScheduler(
				);
			// submit an app
			RMApp app = rm.SubmitApp(Gb, "test-move-1", "user_0", null, "a1");
			ApplicationAttemptId appAttemptId = rm.GetApplicationReport(app.GetApplicationId(
				)).GetCurrentApplicationAttemptId();
			// check preconditions
			IList<ApplicationAttemptId> appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			IList<ApplicationAttemptId> appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			IList<ApplicationAttemptId> appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			// now kill the app
			try
			{
				scheduler.KillAllAppsInQueue("DOES_NOT_EXIST");
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException)
			{
			}
			// expected
			// check postconditions, app should still be in a1
			appsInA1 = scheduler.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			rm.Stop();
		}

		// Test to ensure that we don't carry out reservation on nodes
		// that have no CPU available when using the DominantResourceCalculator
		/// <exception cref="System.Exception"/>
		public virtual void TestAppReservationWithDominantResourceCalculator()
		{
			CapacitySchedulerConfiguration csconf = new CapacitySchedulerConfiguration();
			csconf.SetResourceComparator(typeof(DominantResourceCalculator));
			YarnConfiguration conf = new YarnConfiguration(csconf);
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 10 * Gb, 1);
			// register extra nodes to bump up cluster resource
			MockNM nm2 = rm.RegisterNode("127.0.0.1:1235", 10 * Gb, 4);
			rm.RegisterNode("127.0.0.1:1236", 10 * Gb, 4);
			RMApp app1 = rm.SubmitApp(1024);
			// kick the scheduling
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			MockAM am1 = rm.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			SchedulerNodeReport report_nm1 = rm.GetResourceScheduler().GetNodeReport(nm1.GetNodeId
				());
			// check node report
			NUnit.Framework.Assert.AreEqual(1 * Gb, report_nm1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(9 * Gb, report_nm1.GetAvailableResource().GetMemory
				());
			// add request for containers
			am1.AddRequests(new string[] { "127.0.0.1", "127.0.0.2" }, 1 * Gb, 1, 1);
			am1.Schedule();
			// send the request
			// kick the scheduler, container reservation should not happen
			nm1.NodeHeartbeat(true);
			Sharpen.Thread.Sleep(1000);
			AllocateResponse allocResponse = am1.Schedule();
			ApplicationResourceUsageReport report = rm.GetResourceScheduler().GetAppResourceUsageReport
				(attempt1.GetAppAttemptId());
			NUnit.Framework.Assert.AreEqual(0, allocResponse.GetAllocatedContainers().Count);
			NUnit.Framework.Assert.AreEqual(0, report.GetNumReservedContainers());
			// container should get allocated on this node
			nm2.NodeHeartbeat(true);
			while (allocResponse.GetAllocatedContainers().Count == 0)
			{
				Sharpen.Thread.Sleep(100);
				allocResponse = am1.Schedule();
			}
			report = rm.GetResourceScheduler().GetAppResourceUsageReport(attempt1.GetAppAttemptId
				());
			NUnit.Framework.Assert.AreEqual(1, allocResponse.GetAllocatedContainers().Count);
			NUnit.Framework.Assert.AreEqual(0, report.GetNumReservedContainers());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreemptionDisabled()
		{
			CapacityScheduler cs = new CapacityScheduler();
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			conf.SetBoolean(YarnConfiguration.RmSchedulerEnableMonitors, true);
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(conf), new NMTokenSecretManagerInRM(conf), new ClientToAMTokenSecretManagerInRM
				(), null);
			SetupQueueConfiguration(conf);
			cs.SetConf(new YarnConfiguration());
			cs.SetRMContext(resourceManager.GetRMContext());
			cs.Init(conf);
			cs.Start();
			cs.Reinitialize(conf, rmContext);
			CSQueue rootQueue = cs.GetRootQueue();
			CSQueue queueB = FindQueue(rootQueue, B);
			CSQueue queueB2 = FindQueue(queueB, B2);
			// When preemption turned on for the whole system
			// (yarn.resourcemanager.scheduler.monitor.enable=true), and with no other 
			// preemption properties set, queue root.b.b2 should be preemptable.
			NUnit.Framework.Assert.IsFalse("queue " + B2 + " should default to preemptable", 
				queueB2.GetPreemptionDisabled());
			// Disable preemption at the root queue level.
			// The preemption property should be inherited from root all the
			// way down so that root.b.b2 should NOT be preemptable.
			conf.SetPreemptionDisabled(rootQueue.GetQueuePath(), true);
			cs.Reinitialize(conf, rmContext);
			NUnit.Framework.Assert.IsTrue("queue " + B2 + " should have inherited non-preemptability from root"
				, queueB2.GetPreemptionDisabled());
			// Enable preemption for root (grandparent) but disable for root.b (parent).
			// root.b.b2 should inherit property from parent and NOT be preemptable
			conf.SetPreemptionDisabled(rootQueue.GetQueuePath(), false);
			conf.SetPreemptionDisabled(queueB.GetQueuePath(), true);
			cs.Reinitialize(conf, rmContext);
			NUnit.Framework.Assert.IsTrue("queue " + B2 + " should have inherited non-preemptability from parent"
				, queueB2.GetPreemptionDisabled());
			// When preemption is turned on for root.b.b2, it should be preemptable
			// even though preemption is disabled on root.b (parent).
			conf.SetPreemptionDisabled(queueB2.GetQueuePath(), false);
			cs.Reinitialize(conf, rmContext);
			NUnit.Framework.Assert.IsFalse("queue " + B2 + " should have been preemptable", queueB2
				.GetPreemptionDisabled());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshQueuesMaxAllocationRefresh()
		{
			// queue refresh should not allow changing the maximum allocation setting
			// per queue to be smaller than previous setting
			CapacityScheduler cs = new CapacityScheduler();
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(conf);
			cs.SetConf(new YarnConfiguration());
			cs.SetRMContext(resourceManager.GetRMContext());
			cs.Init(conf);
			cs.Start();
			cs.Reinitialize(conf, mockContext);
			CheckQueueCapacities(cs, ACapacity, BCapacity);
			NUnit.Framework.Assert.AreEqual("max allocation in CS", YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb
				, cs.GetMaximumResourceCapability().GetMemory());
			NUnit.Framework.Assert.AreEqual("max allocation for A1", YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb
				, conf.GetMaximumAllocationPerQueue(A1).GetMemory());
			NUnit.Framework.Assert.AreEqual("max allocation", YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb
				, conf.GetMaximumAllocation().GetMemory());
			CSQueue rootQueue = cs.GetRootQueue();
			CSQueue queueA = FindQueue(rootQueue, A);
			CSQueue queueA1 = FindQueue(queueA, A1);
			NUnit.Framework.Assert.AreEqual("queue max allocation", ((LeafQueue)queueA1).GetMaximumAllocation
				().GetMemory(), 8192);
			SetMaxAllocMb(conf, A1, 4096);
			try
			{
				cs.Reinitialize(conf, mockContext);
				NUnit.Framework.Assert.Fail("should have thrown exception");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue("max allocation exception", e.InnerException.ToString
					().Contains("not be decreased"));
			}
			SetMaxAllocMb(conf, A1, 8192);
			cs.Reinitialize(conf, mockContext);
			SetMaxAllocVcores(conf, A1, YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores
				 - 1);
			try
			{
				cs.Reinitialize(conf, mockContext);
				NUnit.Framework.Assert.Fail("should have thrown exception");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue("max allocation exception", e.InnerException.ToString
					().Contains("not be decreased"));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshQueuesMaxAllocationPerQueueLarge()
		{
			// verify we can't set the allocation per queue larger then cluster setting
			CapacityScheduler cs = new CapacityScheduler();
			cs.SetConf(new YarnConfiguration());
			cs.SetRMContext(resourceManager.GetRMContext());
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(conf);
			cs.Init(conf);
			cs.Start();
			// change max allocation for B3 queue to be larger then cluster max
			SetMaxAllocMb(conf, B3, YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb +
				 2048);
			try
			{
				cs.Reinitialize(conf, mockContext);
				NUnit.Framework.Assert.Fail("should have thrown exception");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue("maximum allocation exception", e.InnerException.Message
					.Contains("maximum allocation"));
			}
			SetMaxAllocMb(conf, B3, YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb);
			cs.Reinitialize(conf, mockContext);
			SetMaxAllocVcores(conf, B3, YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores
				 + 1);
			try
			{
				cs.Reinitialize(conf, mockContext);
				NUnit.Framework.Assert.Fail("should have thrown exception");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue("maximum allocation exception", e.InnerException.Message
					.Contains("maximum allocation"));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshQueuesMaxAllocationRefreshLarger()
		{
			// queue refresh should allow max allocation per queue to go larger
			CapacityScheduler cs = new CapacityScheduler();
			cs.SetConf(new YarnConfiguration());
			cs.SetRMContext(resourceManager.GetRMContext());
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(conf);
			SetMaxAllocMb(conf, YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb);
			SetMaxAllocVcores(conf, YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores
				);
			SetMaxAllocMb(conf, A1, 4096);
			SetMaxAllocVcores(conf, A1, 2);
			cs.Init(conf);
			cs.Start();
			cs.Reinitialize(conf, mockContext);
			CheckQueueCapacities(cs, ACapacity, BCapacity);
			NUnit.Framework.Assert.AreEqual("max capability MB in CS", YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb
				, cs.GetMaximumResourceCapability().GetMemory());
			NUnit.Framework.Assert.AreEqual("max capability vcores in CS", YarnConfiguration.
				DefaultRmSchedulerMaximumAllocationVcores, cs.GetMaximumResourceCapability().GetVirtualCores
				());
			NUnit.Framework.Assert.AreEqual("max allocation MB A1", 4096, conf.GetMaximumAllocationPerQueue
				(A1).GetMemory());
			NUnit.Framework.Assert.AreEqual("max allocation vcores A1", 2, conf.GetMaximumAllocationPerQueue
				(A1).GetVirtualCores());
			NUnit.Framework.Assert.AreEqual("cluster max allocation MB", YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb
				, conf.GetMaximumAllocation().GetMemory());
			NUnit.Framework.Assert.AreEqual("cluster max allocation vcores", YarnConfiguration
				.DefaultRmSchedulerMaximumAllocationVcores, conf.GetMaximumAllocation().GetVirtualCores
				());
			CSQueue rootQueue = cs.GetRootQueue();
			CSQueue queueA = FindQueue(rootQueue, A);
			CSQueue queueA1 = FindQueue(queueA, A1);
			NUnit.Framework.Assert.AreEqual("queue max allocation", ((LeafQueue)queueA1).GetMaximumAllocation
				().GetMemory(), 4096);
			SetMaxAllocMb(conf, A1, 6144);
			SetMaxAllocVcores(conf, A1, 3);
			cs.Reinitialize(conf, null);
			// conf will have changed but we shouldn't be able to change max allocation
			// for the actual queue
			NUnit.Framework.Assert.AreEqual("max allocation MB A1", 6144, conf.GetMaximumAllocationPerQueue
				(A1).GetMemory());
			NUnit.Framework.Assert.AreEqual("max allocation vcores A1", 3, conf.GetMaximumAllocationPerQueue
				(A1).GetVirtualCores());
			NUnit.Framework.Assert.AreEqual("max allocation MB cluster", YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb
				, conf.GetMaximumAllocation().GetMemory());
			NUnit.Framework.Assert.AreEqual("max allocation vcores cluster", YarnConfiguration
				.DefaultRmSchedulerMaximumAllocationVcores, conf.GetMaximumAllocation().GetVirtualCores
				());
			NUnit.Framework.Assert.AreEqual("queue max allocation MB", 6144, ((LeafQueue)queueA1
				).GetMaximumAllocation().GetMemory());
			NUnit.Framework.Assert.AreEqual("queue max allocation vcores", 3, ((LeafQueue)queueA1
				).GetMaximumAllocation().GetVirtualCores());
			NUnit.Framework.Assert.AreEqual("max capability MB cluster", YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb
				, cs.GetMaximumResourceCapability().GetMemory());
			NUnit.Framework.Assert.AreEqual("cluster max capability vcores", YarnConfiguration
				.DefaultRmSchedulerMaximumAllocationVcores, cs.GetMaximumResourceCapability().GetVirtualCores
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshQueuesMaxAllocationCSError()
		{
			// Try to refresh the cluster level max allocation size to be smaller
			// and it should error out
			CapacityScheduler cs = new CapacityScheduler();
			cs.SetConf(new YarnConfiguration());
			cs.SetRMContext(resourceManager.GetRMContext());
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(conf);
			SetMaxAllocMb(conf, 10240);
			SetMaxAllocVcores(conf, 10);
			SetMaxAllocMb(conf, A1, 4096);
			SetMaxAllocVcores(conf, A1, 4);
			cs.Init(conf);
			cs.Start();
			cs.Reinitialize(conf, mockContext);
			CheckQueueCapacities(cs, ACapacity, BCapacity);
			NUnit.Framework.Assert.AreEqual("max allocation MB in CS", 10240, cs.GetMaximumResourceCapability
				().GetMemory());
			NUnit.Framework.Assert.AreEqual("max allocation vcores in CS", 10, cs.GetMaximumResourceCapability
				().GetVirtualCores());
			SetMaxAllocMb(conf, 6144);
			try
			{
				cs.Reinitialize(conf, mockContext);
				NUnit.Framework.Assert.Fail("should have thrown exception");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue("max allocation exception", e.InnerException.ToString
					().Contains("not be decreased"));
			}
			SetMaxAllocMb(conf, 10240);
			cs.Reinitialize(conf, mockContext);
			SetMaxAllocVcores(conf, 8);
			try
			{
				cs.Reinitialize(conf, mockContext);
				NUnit.Framework.Assert.Fail("should have thrown exception");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue("max allocation exception", e.InnerException.ToString
					().Contains("not be decreased"));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshQueuesMaxAllocationCSLarger()
		{
			// Try to refresh the cluster level max allocation size to be larger
			// and verify that if there is no setting per queue it uses the
			// cluster level setting.
			CapacityScheduler cs = new CapacityScheduler();
			cs.SetConf(new YarnConfiguration());
			cs.SetRMContext(resourceManager.GetRMContext());
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(conf);
			SetMaxAllocMb(conf, 10240);
			SetMaxAllocVcores(conf, 10);
			SetMaxAllocMb(conf, A1, 4096);
			SetMaxAllocVcores(conf, A1, 4);
			cs.Init(conf);
			cs.Start();
			cs.Reinitialize(conf, mockContext);
			CheckQueueCapacities(cs, ACapacity, BCapacity);
			NUnit.Framework.Assert.AreEqual("max allocation MB in CS", 10240, cs.GetMaximumResourceCapability
				().GetMemory());
			NUnit.Framework.Assert.AreEqual("max allocation vcores in CS", 10, cs.GetMaximumResourceCapability
				().GetVirtualCores());
			CSQueue rootQueue = cs.GetRootQueue();
			CSQueue queueA = FindQueue(rootQueue, A);
			CSQueue queueB = FindQueue(rootQueue, B);
			CSQueue queueA1 = FindQueue(queueA, A1);
			CSQueue queueA2 = FindQueue(queueA, A2);
			CSQueue queueB2 = FindQueue(queueB, B2);
			NUnit.Framework.Assert.AreEqual("queue A1 max allocation MB", 4096, ((LeafQueue)queueA1
				).GetMaximumAllocation().GetMemory());
			NUnit.Framework.Assert.AreEqual("queue A1 max allocation vcores", 4, ((LeafQueue)
				queueA1).GetMaximumAllocation().GetVirtualCores());
			NUnit.Framework.Assert.AreEqual("queue A2 max allocation MB", 10240, ((LeafQueue)
				queueA2).GetMaximumAllocation().GetMemory());
			NUnit.Framework.Assert.AreEqual("queue A2 max allocation vcores", 10, ((LeafQueue
				)queueA2).GetMaximumAllocation().GetVirtualCores());
			NUnit.Framework.Assert.AreEqual("queue B2 max allocation MB", 10240, ((LeafQueue)
				queueB2).GetMaximumAllocation().GetMemory());
			NUnit.Framework.Assert.AreEqual("queue B2 max allocation vcores", 10, ((LeafQueue
				)queueB2).GetMaximumAllocation().GetVirtualCores());
			SetMaxAllocMb(conf, 12288);
			SetMaxAllocVcores(conf, 12);
			cs.Reinitialize(conf, null);
			// cluster level setting should change and any queues without
			// per queue setting
			NUnit.Framework.Assert.AreEqual("max allocation MB in CS", 12288, cs.GetMaximumResourceCapability
				().GetMemory());
			NUnit.Framework.Assert.AreEqual("max allocation vcores in CS", 12, cs.GetMaximumResourceCapability
				().GetVirtualCores());
			NUnit.Framework.Assert.AreEqual("queue A1 max MB allocation", 4096, ((LeafQueue)queueA1
				).GetMaximumAllocation().GetMemory());
			NUnit.Framework.Assert.AreEqual("queue A1 max vcores allocation", 4, ((LeafQueue)
				queueA1).GetMaximumAllocation().GetVirtualCores());
			NUnit.Framework.Assert.AreEqual("queue A2 max MB allocation", 12288, ((LeafQueue)
				queueA2).GetMaximumAllocation().GetMemory());
			NUnit.Framework.Assert.AreEqual("queue A2 max vcores allocation", 12, ((LeafQueue
				)queueA2).GetMaximumAllocation().GetVirtualCores());
			NUnit.Framework.Assert.AreEqual("queue B2 max MB allocation", 12288, ((LeafQueue)
				queueB2).GetMaximumAllocation().GetMemory());
			NUnit.Framework.Assert.AreEqual("queue B2 max vcores allocation", 12, ((LeafQueue
				)queueB2).GetMaximumAllocation().GetVirtualCores());
		}

		/// <exception cref="System.Exception"/>
		private void WaitContainerAllocated(MockAM am, int mem, int nContainer, int startContainerId
			, MockRM rm, MockNM nm)
		{
			for (int cId = startContainerId; cId < startContainerId + nContainer; cId++)
			{
				am.Allocate("*", mem, 1, new AList<ContainerId>());
				ContainerId containerId = ContainerId.NewContainerId(am.GetApplicationAttemptId()
					, cId);
				NUnit.Framework.Assert.IsTrue(rm.WaitForState(nm, containerId, RMContainerState.Allocated
					, 10 * 1000));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHierarchyQueuesCurrentLimits()
		{
			/*
			* Queue tree:
			*          Root
			*        /     \
			*       A       B
			*      / \    / | \
			*     A1 A2  B1 B2 B3
			*/
			YarnConfiguration conf = new YarnConfiguration(SetupQueueConfiguration(new CapacitySchedulerConfiguration
				()));
			conf.SetBoolean(CapacitySchedulerConfiguration.EnableUserMetrics, true);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			MockRM rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 100 * Gb, rm1.GetResourceTrackerService
				());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(1 * Gb, "app", "user", null, "b1");
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			WaitContainerAllocated(am1, 1 * Gb, 1, 2, rm1, nm1);
			// Maximum resoure of b1 is 100 * 0.895 * 0.792 = 71 GB
			// 2 GBs used by am, so it's 71 - 2 = 69G.
			NUnit.Framework.Assert.AreEqual(69 * Gb, am1.DoHeartbeat().GetAvailableResources(
				).GetMemory());
			RMApp app2 = rm1.SubmitApp(1 * Gb, "app", "user", null, "b2");
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm1, nm1);
			// Allocate 5 containers, each one is 8 GB in am2 (40 GB in total)
			WaitContainerAllocated(am2, 8 * Gb, 5, 2, rm1, nm1);
			// Allocated one more container with 1 GB resource in b1
			WaitContainerAllocated(am1, 1 * Gb, 1, 3, rm1, nm1);
			// Total is 100 GB, 
			// B2 uses 41 GB (5 * 8GB containers and 1 AM container)
			// B1 uses 3 GB (2 * 1GB containers and 1 AM container)
			// Available is 100 - 41 - 3 = 56 GB
			NUnit.Framework.Assert.AreEqual(56 * Gb, am1.DoHeartbeat().GetAvailableResources(
				).GetMemory());
			// Now we submit app3 to a1 (in higher level hierarchy), to see if headroom
			// of app1 (in queue b1) updated correctly
			RMApp app3 = rm1.SubmitApp(1 * Gb, "app", "user", null, "a1");
			MockAM am3 = MockRM.LaunchAndRegisterAM(app3, rm1, nm1);
			// Allocate 3 containers, each one is 8 GB in am3 (24 GB in total)
			WaitContainerAllocated(am3, 8 * Gb, 3, 2, rm1, nm1);
			// Allocated one more container with 4 GB resource in b1
			WaitContainerAllocated(am1, 1 * Gb, 1, 4, rm1, nm1);
			// Total is 100 GB, 
			// B2 uses 41 GB (5 * 8GB containers and 1 AM container)
			// B1 uses 4 GB (3 * 1GB containers and 1 AM container)
			// A1 uses 25 GB (3 * 8GB containers and 1 AM container)
			// Available is 100 - 41 - 4 - 25 = 30 GB
			NUnit.Framework.Assert.AreEqual(30 * Gb, am1.DoHeartbeat().GetAvailableResources(
				).GetMemory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestParentQueueMaxCapsAreRespected()
		{
			/*
			* Queue tree:
			*          Root
			*        /     \
			*       A       B
			*      / \
			*     A1 A2
			*/
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			csConf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b" });
			csConf.SetCapacity(A, 50);
			csConf.SetMaximumCapacity(A, 50);
			csConf.SetCapacity(B, 50);
			// Define 2nd-level queues
			csConf.SetQueues(A, new string[] { "a1", "a2" });
			csConf.SetCapacity(A1, 50);
			csConf.SetUserLimitFactor(A1, 100.0f);
			csConf.SetCapacity(A2, 50);
			csConf.SetUserLimitFactor(A2, 100.0f);
			csConf.SetCapacity(B1, B1Capacity);
			csConf.SetUserLimitFactor(B1, 100.0f);
			YarnConfiguration conf = new YarnConfiguration(csConf);
			conf.SetBoolean(CapacitySchedulerConfiguration.EnableUserMetrics, true);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			MockRM rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 24 * Gb, rm1.GetResourceTrackerService(
				));
			nm1.RegisterNode();
			// Launch app1 in a1, resource usage is 1GB (am) + 4GB * 2 = 9GB 
			RMApp app1 = rm1.SubmitApp(1 * Gb, "app", "user", null, "a1");
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			WaitContainerAllocated(am1, 4 * Gb, 2, 2, rm1, nm1);
			// Try to launch app2 in a2, asked 2GB, should success 
			RMApp app2 = rm1.SubmitApp(2 * Gb, "app", "user", null, "a2");
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm1, nm1);
			try
			{
				// Try to allocate a container, a's usage=11G/max=12
				// a1's usage=9G/max=12
				// a2's usage=2G/max=12
				// In this case, if a2 asked 2G, should fail.
				WaitContainerAllocated(am2, 2 * Gb, 1, 2, rm1, nm1);
			}
			catch (Exception)
			{
				// Expected, return;
				return;
			}
			NUnit.Framework.Assert.Fail("Shouldn't successfully allocate containers for am2, "
				 + "queue-a's max capacity will be violated if container allocated");
		}

		// Verifies headroom passed to ApplicationMaster has been updated in
		// RMAppAttemptMetrics
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationHeadRoom()
		{
			Configuration conf = new Configuration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			rm.Start();
			CapacityScheduler cs = (CapacityScheduler)rm.GetResourceScheduler();
			ApplicationId appId = BuilderUtils.NewApplicationId(100, 1);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			RMAppAttemptMetrics attemptMetric = new RMAppAttemptMetrics(appAttemptId, rm.GetRMContext
				());
			RMAppImpl app = Org.Mockito.Mockito.Mock<RMAppImpl>();
			Org.Mockito.Mockito.When(app.GetApplicationId()).ThenReturn(appId);
			RMAppAttemptImpl attempt = Org.Mockito.Mockito.Mock<RMAppAttemptImpl>();
			Org.Mockito.Mockito.When(attempt.GetAppAttemptId()).ThenReturn(appAttemptId);
			Org.Mockito.Mockito.When(attempt.GetRMAppAttemptMetrics()).ThenReturn(attemptMetric
				);
			Org.Mockito.Mockito.When(app.GetCurrentAppAttempt()).ThenReturn(attempt);
			rm.GetRMContext().GetRMApps()[appId] = app;
			SchedulerEvent addAppEvent = new AppAddedSchedulerEvent(appId, "default", "user");
			cs.Handle(addAppEvent);
			SchedulerEvent addAttemptEvent = new AppAttemptAddedSchedulerEvent(appAttemptId, 
				false);
			cs.Handle(addAttemptEvent);
			Allocation allocate = cs.Allocate(appAttemptId, Sharpen.Collections.EmptyList<ResourceRequest
				>(), Sharpen.Collections.EmptyList<ContainerId>(), null, null);
			NUnit.Framework.Assert.IsNotNull(attempt);
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), allocate.GetResourceLimit());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), attemptMetric.GetApplicationAttemptHeadroom());
			// Add a node to cluster
			Org.Apache.Hadoop.Yarn.Api.Records.Resource newResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(4 * Gb, 1);
			RMNode node = MockNodes.NewNodeInfo(0, newResource, 1, "127.0.0.1");
			cs.Handle(new NodeAddedSchedulerEvent(node));
			allocate = cs.Allocate(appAttemptId, Sharpen.Collections.EmptyList<ResourceRequest
				>(), Sharpen.Collections.EmptyList<ContainerId>(), null, null);
			// All resources should be sent as headroom
			NUnit.Framework.Assert.AreEqual(newResource, allocate.GetResourceLimit());
			NUnit.Framework.Assert.AreEqual(newResource, attemptMetric.GetApplicationAttemptHeadroom
				());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHeadRoomCalculationWithDRC()
		{
			// test with total cluster resource of 20GB memory and 20 vcores.
			// the queue where two apps running has user limit 0.8
			// allocate 10GB memory and 1 vcore to app 1.
			// app 1 should have headroom
			// 20GB*0.8 - 10GB = 6GB memory available and 15 vcores.
			// allocate 1GB memory and 1 vcore to app2.
			// app 2 should have headroom 20GB - 10 - 1 = 1GB memory,
			// and 20*0.8 - 1 = 15 vcores.
			CapacitySchedulerConfiguration csconf = new CapacitySchedulerConfiguration();
			csconf.SetResourceComparator(typeof(DominantResourceCalculator));
			YarnConfiguration conf = new YarnConfiguration(csconf);
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			rm.Start();
			CapacityScheduler cs = (CapacityScheduler)rm.GetResourceScheduler();
			LeafQueue qb = (LeafQueue)cs.GetQueue("default");
			qb.SetUserLimitFactor((float)0.8);
			// add app 1
			ApplicationId appId = BuilderUtils.NewApplicationId(100, 1);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			RMAppAttemptMetrics attemptMetric = new RMAppAttemptMetrics(appAttemptId, rm.GetRMContext
				());
			RMAppImpl app = Org.Mockito.Mockito.Mock<RMAppImpl>();
			Org.Mockito.Mockito.When(app.GetApplicationId()).ThenReturn(appId);
			RMAppAttemptImpl attempt = Org.Mockito.Mockito.Mock<RMAppAttemptImpl>();
			Org.Mockito.Mockito.When(attempt.GetAppAttemptId()).ThenReturn(appAttemptId);
			Org.Mockito.Mockito.When(attempt.GetRMAppAttemptMetrics()).ThenReturn(attemptMetric
				);
			Org.Mockito.Mockito.When(app.GetCurrentAppAttempt()).ThenReturn(attempt);
			rm.GetRMContext().GetRMApps()[appId] = app;
			SchedulerEvent addAppEvent = new AppAddedSchedulerEvent(appId, "default", "user1"
				);
			cs.Handle(addAppEvent);
			SchedulerEvent addAttemptEvent = new AppAttemptAddedSchedulerEvent(appAttemptId, 
				false);
			cs.Handle(addAttemptEvent);
			// add app 2
			ApplicationId appId2 = BuilderUtils.NewApplicationId(100, 2);
			ApplicationAttemptId appAttemptId2 = BuilderUtils.NewApplicationAttemptId(appId2, 
				1);
			RMAppAttemptMetrics attemptMetric2 = new RMAppAttemptMetrics(appAttemptId2, rm.GetRMContext
				());
			RMAppImpl app2 = Org.Mockito.Mockito.Mock<RMAppImpl>();
			Org.Mockito.Mockito.When(app2.GetApplicationId()).ThenReturn(appId2);
			RMAppAttemptImpl attempt2 = Org.Mockito.Mockito.Mock<RMAppAttemptImpl>();
			Org.Mockito.Mockito.When(attempt2.GetAppAttemptId()).ThenReturn(appAttemptId2);
			Org.Mockito.Mockito.When(attempt2.GetRMAppAttemptMetrics()).ThenReturn(attemptMetric2
				);
			Org.Mockito.Mockito.When(app2.GetCurrentAppAttempt()).ThenReturn(attempt2);
			rm.GetRMContext().GetRMApps()[appId2] = app2;
			addAppEvent = new AppAddedSchedulerEvent(appId2, "default", "user2");
			cs.Handle(addAppEvent);
			addAttemptEvent = new AppAttemptAddedSchedulerEvent(appAttemptId2, false);
			cs.Handle(addAttemptEvent);
			// add nodes  to cluster, so cluster have 20GB and 20 vcores
			Org.Apache.Hadoop.Yarn.Api.Records.Resource newResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(10 * Gb, 10);
			RMNode node = MockNodes.NewNodeInfo(0, newResource, 1, "127.0.0.1");
			cs.Handle(new NodeAddedSchedulerEvent(node));
			Org.Apache.Hadoop.Yarn.Api.Records.Resource newResource2 = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(10 * Gb, 10);
			RMNode node2 = MockNodes.NewNodeInfo(0, newResource2, 1, "127.0.0.2");
			cs.Handle(new NodeAddedSchedulerEvent(node2));
			FiCaSchedulerApp fiCaApp1 = cs.GetSchedulerApplications()[app.GetApplicationId()]
				.GetCurrentAppAttempt();
			FiCaSchedulerApp fiCaApp2 = cs.GetSchedulerApplications()[app2.GetApplicationId()
				].GetCurrentAppAttempt();
			Priority u0Priority = TestUtils.CreateMockPriority(1);
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
			// allocate container for app1 with 10GB memory and 1 vcore
			fiCaApp1.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 10 * Gb, 1, true, u0Priority, recordFactory)));
			cs.Handle(new NodeUpdateSchedulerEvent(node));
			cs.Handle(new NodeUpdateSchedulerEvent(node2));
			NUnit.Framework.Assert.AreEqual(6 * Gb, fiCaApp1.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(15, fiCaApp1.GetHeadroom().GetVirtualCores());
			// allocate container for app2 with 1GB memory and 1 vcore
			fiCaApp2.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 1, true, u0Priority, recordFactory)));
			cs.Handle(new NodeUpdateSchedulerEvent(node));
			cs.Handle(new NodeUpdateSchedulerEvent(node2));
			NUnit.Framework.Assert.AreEqual(9 * Gb, fiCaApp2.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(15, fiCaApp2.GetHeadroom().GetVirtualCores());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultNodeLabelExpressionQueueConfig()
		{
			CapacityScheduler cs = new CapacityScheduler();
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(conf);
			conf.SetDefaultNodeLabelExpression("root.a", " x");
			conf.SetDefaultNodeLabelExpression("root.b", " y ");
			cs.SetConf(new YarnConfiguration());
			cs.SetRMContext(resourceManager.GetRMContext());
			cs.Init(conf);
			cs.Start();
			QueueInfo queueInfoA = cs.GetQueueInfo("a", true, false);
			NUnit.Framework.Assert.AreEqual(queueInfoA.GetQueueName(), "a");
			NUnit.Framework.Assert.AreEqual(queueInfoA.GetDefaultNodeLabelExpression(), "x");
			QueueInfo queueInfoB = cs.GetQueueInfo("b", true, false);
			NUnit.Framework.Assert.AreEqual(queueInfoB.GetQueueName(), "b");
			NUnit.Framework.Assert.AreEqual(queueInfoB.GetDefaultNodeLabelExpression(), "y");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMLimitUsage()
		{
			CapacitySchedulerConfiguration config = new CapacitySchedulerConfiguration();
			config.Set(CapacitySchedulerConfiguration.ResourceCalculatorClass, typeof(DefaultResourceCalculator
				).FullName);
			VerifyAMLimitForLeafQueue(config);
			config.Set(CapacitySchedulerConfiguration.ResourceCalculatorClass, typeof(DominantResourceCalculator
				).FullName);
			VerifyAMLimitForLeafQueue(config);
		}

		/// <exception cref="System.Exception"/>
		private void VerifyAMLimitForLeafQueue(CapacitySchedulerConfiguration config)
		{
			MockRM rm = SetUpMove(config);
			string queueName = "a1";
			string userName = "user_0";
			ResourceScheduler scheduler = rm.GetRMContext().GetScheduler();
			LeafQueue queueA = (LeafQueue)((CapacityScheduler)scheduler).GetQueue(queueName);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource amResourceLimit = queueA.GetAMResourceLimit
				();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource amResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(amResourceLimit.GetMemory() + 1, amResourceLimit.GetVirtualCores() 
				+ 1);
			rm.SubmitApp(amResource.GetMemory(), "app-1", userName, null, queueName);
			rm.SubmitApp(amResource.GetMemory(), "app-1", userName, null, queueName);
			// When AM limit is exceeded, 1 applications will be activated.Rest all
			// applications will be in pending
			NUnit.Framework.Assert.AreEqual("PendingApplications should be 1", 1, queueA.GetNumPendingApplications
				());
			NUnit.Framework.Assert.AreEqual("Active applications should be 1", 1, queueA.GetNumActiveApplications
				());
			NUnit.Framework.Assert.AreEqual("User PendingApplications should be 1", 1, queueA
				.GetUser(userName).GetPendingApplications());
			NUnit.Framework.Assert.AreEqual("User Active applications should be 1", 1, queueA
				.GetUser(userName).GetActiveApplications());
			rm.Stop();
		}

		private void SetMaxAllocMb(Configuration conf, int maxAllocMb)
		{
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, maxAllocMb);
		}

		private void SetMaxAllocMb(CapacitySchedulerConfiguration conf, string queueName, 
			int maxAllocMb)
		{
			string propName = CapacitySchedulerConfiguration.GetQueuePrefix(queueName) + CapacitySchedulerConfiguration
				.MaximumAllocationMb;
			conf.SetInt(propName, maxAllocMb);
		}

		private void SetMaxAllocVcores(Configuration conf, int maxAllocVcores)
		{
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores, maxAllocVcores);
		}

		private void SetMaxAllocVcores(CapacitySchedulerConfiguration conf, string queueName
			, int maxAllocVcores)
		{
			string propName = CapacitySchedulerConfiguration.GetQueuePrefix(queueName) + CapacitySchedulerConfiguration
				.MaximumAllocationVcores;
			conf.SetInt(propName, maxAllocVcores);
		}

		private class SleepHandler : EventHandler<SchedulerEvent>
		{
			internal bool sleepFlag = false;

			internal int sleepTime = 20;

			public virtual void Handle(SchedulerEvent @event)
			{
				try
				{
					if (this.sleepFlag)
					{
						Sharpen.Thread.Sleep(this.sleepTime);
					}
				}
				catch (Exception)
				{
				}
			}

			internal SleepHandler(TestCapacityScheduler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestCapacityScheduler _enclosing;
		}

		private ResourceTrackerService GetPrivateResourceTrackerService(Dispatcher privateDispatcher
			, TestCapacityScheduler.SleepHandler sleepHandler)
		{
			Configuration conf = new Configuration();
			ResourceTrackerService privateResourceTrackerService;
			RMContext privateContext = new RMContextImpl(privateDispatcher, null, null, null, 
				null, null, null, null, null, null);
			privateContext.SetNodeLabelManager(Org.Mockito.Mockito.Mock<RMNodeLabelsManager>(
				));
			privateDispatcher.Register(typeof(SchedulerEventType), sleepHandler);
			privateDispatcher.Register(typeof(SchedulerEventType), resourceManager.GetResourceScheduler
				());
			privateDispatcher.Register(typeof(RMNodeEventType), new ResourceManager.NodeEventDispatcher
				(privateContext));
			((Org.Apache.Hadoop.Service.Service)privateDispatcher).Init(conf);
			((Org.Apache.Hadoop.Service.Service)privateDispatcher).Start();
			NMLivelinessMonitor nmLivelinessMonitor = new NMLivelinessMonitor(privateDispatcher
				);
			nmLivelinessMonitor.Init(conf);
			nmLivelinessMonitor.Start();
			NodesListManager nodesListManager = new NodesListManager(privateContext);
			nodesListManager.Init(conf);
			RMContainerTokenSecretManager containerTokenSecretManager = new RMContainerTokenSecretManager
				(conf);
			containerTokenSecretManager.Start();
			NMTokenSecretManagerInRM nmTokenSecretManager = new NMTokenSecretManagerInRM(conf
				);
			nmTokenSecretManager.Start();
			privateResourceTrackerService = new ResourceTrackerService(privateContext, nodesListManager
				, nmLivelinessMonitor, containerTokenSecretManager, nmTokenSecretManager);
			privateResourceTrackerService.Init(conf);
			privateResourceTrackerService.Start();
			resourceManager.GetResourceScheduler().SetRMContext(privateContext);
			return privateResourceTrackerService;
		}

		/// <summary>
		/// Test the behaviour of the capacity scheduler when a node reconnects
		/// with changed capabilities.
		/// </summary>
		/// <remarks>
		/// Test the behaviour of the capacity scheduler when a node reconnects
		/// with changed capabilities. This test is to catch any race conditions
		/// that might occur due to the use of the RMNode object.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodemanagerReconnect()
		{
			DrainDispatcher privateDispatcher = new DrainDispatcher();
			TestCapacityScheduler.SleepHandler sleepHandler = new TestCapacityScheduler.SleepHandler
				(this);
			ResourceTrackerService privateResourceTrackerService = GetPrivateResourceTrackerService
				(privateDispatcher, sleepHandler);
			// Register node1
			string hostname1 = "localhost1";
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability = BuilderUtils.NewResource
				(4096, 4);
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
			RegisterNodeManagerRequest request1 = recordFactory.NewRecordInstance<RegisterNodeManagerRequest
				>();
			NodeId nodeId1 = NodeId.NewInstance(hostname1, 0);
			request1.SetNodeId(nodeId1);
			request1.SetHttpPort(0);
			request1.SetResource(capability);
			privateResourceTrackerService.RegisterNodeManager(request1);
			privateDispatcher.Await();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = resourceManager.GetResourceScheduler
				().GetClusterResource();
			NUnit.Framework.Assert.AreEqual("Initial cluster resources don't match", capability
				, clusterResource);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource newCapability = BuilderUtils.NewResource
				(1024, 1);
			RegisterNodeManagerRequest request2 = recordFactory.NewRecordInstance<RegisterNodeManagerRequest
				>();
			request2.SetNodeId(nodeId1);
			request2.SetHttpPort(0);
			request2.SetResource(newCapability);
			// hold up the disaptcher and register the same node with lower capability
			sleepHandler.sleepFlag = true;
			privateResourceTrackerService.RegisterNodeManager(request2);
			privateDispatcher.Await();
			NUnit.Framework.Assert.AreEqual("Cluster resources don't match", newCapability, resourceManager
				.GetResourceScheduler().GetClusterResource());
			privateResourceTrackerService.Stop();
		}
	}
}
