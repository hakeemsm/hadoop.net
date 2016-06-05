using System.Collections.Generic;
using System.Reflection;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo
{
	public class TestFifoScheduler
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo.TestFifoScheduler
			));

		private readonly int Gb = 1024;

		private ResourceManager resourceManager = null;

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			resourceManager = new ResourceManager();
			Configuration conf = new Configuration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
				));
			resourceManager.Init(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			resourceManager.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private NodeManager RegisterNode(string hostName, int containerManagerPort, int nmHttpPort
			, string rackName, Resource capability)
		{
			return new NodeManager(hostName, containerManagerPort, nmHttpPort, rackName, capability
				, resourceManager);
		}

		private ApplicationAttemptId CreateAppAttemptId(int appId, int attemptId)
		{
			ApplicationId appIdImpl = ApplicationId.NewInstance(0, appId);
			ApplicationAttemptId attId = ApplicationAttemptId.NewInstance(appIdImpl, attemptId
				);
			return attId;
		}

		private ResourceRequest CreateResourceRequest(int memory, string host, int priority
			, int numContainers)
		{
			ResourceRequest request = recordFactory.NewRecordInstance<ResourceRequest>();
			request.SetCapability(Resources.CreateResource(memory));
			request.SetResourceName(host);
			request.SetNumContainers(numContainers);
			Priority prio = recordFactory.NewRecordInstance<Priority>();
			prio.SetPriority(priority);
			request.SetPriority(prio);
			return request;
		}

		public virtual void TestFifoSchedulerCapacityWhenNoNMs()
		{
			FifoScheduler scheduler = new FifoScheduler();
			QueueInfo queueInfo = scheduler.GetQueueInfo(null, false, false);
			NUnit.Framework.Assert.AreEqual(0.0f, queueInfo.GetCurrentCapacity(), 0.0f);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppAttemptMetrics()
		{
			AsyncDispatcher dispatcher = new InlineDispatcher();
			FifoScheduler scheduler = new FifoScheduler();
			RMApplicationHistoryWriter writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>();
			RMContext rmContext = new RMContextImpl(dispatcher, null, null, null, null, null, 
				null, null, null, scheduler);
			((RMContextImpl)rmContext).SetSystemMetricsPublisher(Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>());
			Configuration conf = new Configuration();
			scheduler.SetRMContext(rmContext);
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, rmContext);
			QueueMetrics metrics = scheduler.GetRootQueueMetrics();
			int beforeAppsSubmitted = metrics.GetAppsSubmitted();
			ApplicationId appId = BuilderUtils.NewApplicationId(200, 1);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			SchedulerEvent appEvent = new AppAddedSchedulerEvent(appId, "queue", "user");
			scheduler.Handle(appEvent);
			SchedulerEvent attemptEvent = new AppAttemptAddedSchedulerEvent(appAttemptId, false
				);
			scheduler.Handle(attemptEvent);
			appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 2);
			SchedulerEvent attemptEvent2 = new AppAttemptAddedSchedulerEvent(appAttemptId, false
				);
			scheduler.Handle(attemptEvent2);
			int afterAppsSubmitted = metrics.GetAppsSubmitted();
			NUnit.Framework.Assert.AreEqual(1, afterAppsSubmitted - beforeAppsSubmitted);
			scheduler.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNodeLocalAssignment()
		{
			AsyncDispatcher dispatcher = new InlineDispatcher();
			Configuration conf = new Configuration();
			RMContainerTokenSecretManager containerTokenSecretManager = new RMContainerTokenSecretManager
				(conf);
			containerTokenSecretManager.RollMasterKey();
			NMTokenSecretManagerInRM nmTokenSecretManager = new NMTokenSecretManagerInRM(conf
				);
			nmTokenSecretManager.RollMasterKey();
			RMApplicationHistoryWriter writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>();
			FifoScheduler scheduler = new FifoScheduler();
			RMContext rmContext = new RMContextImpl(dispatcher, null, null, null, null, null, 
				containerTokenSecretManager, nmTokenSecretManager, null, scheduler);
			rmContext.SetSystemMetricsPublisher(Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>());
			rmContext.SetRMApplicationHistoryWriter(Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>());
			((RMContextImpl)rmContext).SetYarnConfiguration(new YarnConfiguration());
			scheduler.SetRMContext(rmContext);
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(new Configuration(), rmContext);
			RMNode node0 = MockNodes.NewNodeInfo(1, Resources.CreateResource(1024 * 64), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node0);
			scheduler.Handle(nodeEvent1);
			int _appId = 1;
			int _appAttemptId = 1;
			ApplicationAttemptId appAttemptId = CreateAppAttemptId(_appId, _appAttemptId);
			CreateMockRMApp(appAttemptId, rmContext);
			AppAddedSchedulerEvent appEvent = new AppAddedSchedulerEvent(appAttemptId.GetApplicationId
				(), "queue1", "user1");
			scheduler.Handle(appEvent);
			AppAttemptAddedSchedulerEvent attemptEvent = new AppAttemptAddedSchedulerEvent(appAttemptId
				, false);
			scheduler.Handle(attemptEvent);
			int memory = 64;
			int nConts = 3;
			int priority = 20;
			IList<ResourceRequest> ask = new AList<ResourceRequest>();
			ResourceRequest nodeLocal = CreateResourceRequest(memory, node0.GetHostName(), priority
				, nConts);
			ResourceRequest rackLocal = CreateResourceRequest(memory, node0.GetRackName(), priority
				, nConts);
			ResourceRequest any = CreateResourceRequest(memory, ResourceRequest.Any, priority
				, nConts);
			ask.AddItem(nodeLocal);
			ask.AddItem(rackLocal);
			ask.AddItem(any);
			scheduler.Allocate(appAttemptId, ask, new AList<ContainerId>(), null, null);
			NodeUpdateSchedulerEvent node0Update = new NodeUpdateSchedulerEvent(node0);
			// Before the node update event, there are 3 local requests outstanding
			NUnit.Framework.Assert.AreEqual(3, nodeLocal.GetNumContainers());
			scheduler.Handle(node0Update);
			// After the node update event, check that there are no more local requests
			// outstanding
			NUnit.Framework.Assert.AreEqual(0, nodeLocal.GetNumContainers());
			//Also check that the containers were scheduled
			SchedulerAppReport info = scheduler.GetSchedulerAppInfo(appAttemptId);
			NUnit.Framework.Assert.AreEqual(3, info.GetLiveContainers().Count);
			scheduler.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUpdateResourceOnNode()
		{
			AsyncDispatcher dispatcher = new InlineDispatcher();
			Configuration conf = new Configuration();
			RMContainerTokenSecretManager containerTokenSecretManager = new RMContainerTokenSecretManager
				(conf);
			containerTokenSecretManager.RollMasterKey();
			NMTokenSecretManagerInRM nmTokenSecretManager = new NMTokenSecretManagerInRM(conf
				);
			nmTokenSecretManager.RollMasterKey();
			RMApplicationHistoryWriter writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>();
			FifoScheduler scheduler = new _FifoScheduler_275(this);
			RMContext rmContext = new RMContextImpl(dispatcher, null, null, null, null, null, 
				containerTokenSecretManager, nmTokenSecretManager, null, scheduler);
			rmContext.SetSystemMetricsPublisher(Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>());
			rmContext.SetRMApplicationHistoryWriter(Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>());
			((RMContextImpl)rmContext).SetYarnConfiguration(new YarnConfiguration());
			scheduler.SetRMContext(rmContext);
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(new Configuration(), rmContext);
			RMNode node0 = MockNodes.NewNodeInfo(1, Resources.CreateResource(2048, 4), 1, "127.0.0.1"
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node0);
			scheduler.Handle(nodeEvent1);
			MethodInfo method = Sharpen.Runtime.GetDeclaredMethod(scheduler.GetType(), "getNodes"
				);
			IDictionary<NodeId, FiCaSchedulerNode> schedulerNodes = (IDictionary<NodeId, FiCaSchedulerNode
				>)method.Invoke(scheduler);
			NUnit.Framework.Assert.AreEqual(schedulerNodes.Values.Count, 1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource newResource = Resources.CreateResource
				(1024, 4);
			NodeResourceUpdateSchedulerEvent node0ResourceUpdate = new NodeResourceUpdateSchedulerEvent
				(node0, ResourceOption.NewInstance(newResource, RMNode.OverCommitTimeoutMillisDefault
				));
			scheduler.Handle(node0ResourceUpdate);
			// SchedulerNode's total resource and available resource are changed.
			NUnit.Framework.Assert.AreEqual(schedulerNodes[node0.GetNodeID()].GetTotalResource
				().GetMemory(), 1024);
			NUnit.Framework.Assert.AreEqual(schedulerNodes[node0.GetNodeID()].GetAvailableResource
				().GetMemory(), 1024);
			QueueInfo queueInfo = scheduler.GetQueueInfo(null, false, false);
			NUnit.Framework.Assert.AreEqual(0.0f, queueInfo.GetCurrentCapacity(), 0.0f);
			int _appId = 1;
			int _appAttemptId = 1;
			ApplicationAttemptId appAttemptId = CreateAppAttemptId(_appId, _appAttemptId);
			CreateMockRMApp(appAttemptId, rmContext);
			AppAddedSchedulerEvent appEvent = new AppAddedSchedulerEvent(appAttemptId.GetApplicationId
				(), "queue1", "user1");
			scheduler.Handle(appEvent);
			AppAttemptAddedSchedulerEvent attemptEvent = new AppAttemptAddedSchedulerEvent(appAttemptId
				, false);
			scheduler.Handle(attemptEvent);
			int memory = 1024;
			int priority = 1;
			IList<ResourceRequest> ask = new AList<ResourceRequest>();
			ResourceRequest nodeLocal = CreateResourceRequest(memory, node0.GetHostName(), priority
				, 1);
			ResourceRequest rackLocal = CreateResourceRequest(memory, node0.GetRackName(), priority
				, 1);
			ResourceRequest any = CreateResourceRequest(memory, ResourceRequest.Any, priority
				, 1);
			ask.AddItem(nodeLocal);
			ask.AddItem(rackLocal);
			ask.AddItem(any);
			scheduler.Allocate(appAttemptId, ask, new AList<ContainerId>(), null, null);
			// Before the node update event, there are one local request
			NUnit.Framework.Assert.AreEqual(1, nodeLocal.GetNumContainers());
			NodeUpdateSchedulerEvent node0Update = new NodeUpdateSchedulerEvent(node0);
			// Now schedule.
			scheduler.Handle(node0Update);
			// After the node update event, check no local request
			NUnit.Framework.Assert.AreEqual(0, nodeLocal.GetNumContainers());
			// Also check that one container was scheduled
			SchedulerAppReport info = scheduler.GetSchedulerAppInfo(appAttemptId);
			NUnit.Framework.Assert.AreEqual(1, info.GetLiveContainers().Count);
			// And check the default Queue now is full.
			queueInfo = scheduler.GetQueueInfo(null, false, false);
			NUnit.Framework.Assert.AreEqual(1.0f, queueInfo.GetCurrentCapacity(), 0.0f);
		}

		private sealed class _FifoScheduler_275 : FifoScheduler
		{
			public _FifoScheduler_275(TestFifoScheduler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public IDictionary<NodeId, FiCaSchedulerNode> GetNodes()
			{
				return this._enclosing._enclosing.nodes;
			}

			private readonly TestFifoScheduler _enclosing;
		}

		//  @Test
		/// <exception cref="System.Exception"/>
		public virtual void TestFifoScheduler()
		{
			Log.Info("--- START: testFifoScheduler ---");
			int Gb = 1024;
			// Register node1
			string host_0 = "host_0";
			NodeManager nm_0 = RegisterNode(host_0, 1234, 2345, NetworkTopology.DefaultRack, 
				Resources.CreateResource(4 * Gb, 1));
			nm_0.Heartbeat();
			// Register node2
			string host_1 = "host_1";
			NodeManager nm_1 = RegisterNode(host_1, 1234, 2345, NetworkTopology.DefaultRack, 
				Resources.CreateResource(2 * Gb, 1));
			nm_1.Heartbeat();
			// ResourceRequest priorities
			Priority priority_0 = Priority.Create(0);
			Priority priority_1 = Priority.Create(1);
			// Submit an application
			Application application_0 = new Application("user_0", resourceManager);
			application_0.Submit();
			application_0.AddNodeManager(host_0, 1234, nm_0);
			application_0.AddNodeManager(host_1, 1234, nm_1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_0 = Resources.CreateResource
				(Gb);
			application_0.AddResourceRequestSpec(priority_1, capability_0_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_0_1 = Resources.CreateResource
				(2 * Gb);
			application_0.AddResourceRequestSpec(priority_0, capability_0_1);
			Task task_0_0 = new Task(application_0, priority_1, new string[] { host_0, host_1
				 });
			application_0.AddTask(task_0_0);
			// Submit another application
			Application application_1 = new Application("user_1", resourceManager);
			application_1.Submit();
			application_1.AddNodeManager(host_0, 1234, nm_0);
			application_1.AddNodeManager(host_1, 1234, nm_1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_1_0 = Resources.CreateResource
				(3 * Gb);
			application_1.AddResourceRequestSpec(priority_1, capability_1_0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability_1_1 = Resources.CreateResource
				(4 * Gb);
			application_1.AddResourceRequestSpec(priority_0, capability_1_1);
			Task task_1_0 = new Task(application_1, priority_1, new string[] { host_0, host_1
				 });
			application_1.AddTask(task_1_0);
			// Send resource requests to the scheduler
			Log.Info("Send resource requests to the scheduler");
			application_0.Schedule();
			application_1.Schedule();
			// Send a heartbeat to kick the tires on the Scheduler
			Log.Info("Send a heartbeat to kick the tires on the Scheduler... " + "nm0 -> task_0_0 and task_1_0 allocated, used=4G "
				 + "nm1 -> nothing allocated");
			nm_0.Heartbeat();
			// task_0_0 and task_1_0 allocated, used=4G
			nm_1.Heartbeat();
			// nothing allocated
			// Get allocations from the scheduler
			application_0.Schedule();
			// task_0_0 
			CheckApplicationResourceUsage(Gb, application_0);
			application_1.Schedule();
			// task_1_0
			CheckApplicationResourceUsage(3 * Gb, application_1);
			nm_0.Heartbeat();
			nm_1.Heartbeat();
			CheckNodeResourceUsage(4 * Gb, nm_0);
			// task_0_0 (1G) and task_1_0 (3G)
			CheckNodeResourceUsage(0 * Gb, nm_1);
			// no tasks, 2G available
			Log.Info("Adding new tasks...");
			Task task_1_1 = new Task(application_1, priority_1, new string[] { ResourceRequest
				.Any });
			application_1.AddTask(task_1_1);
			Task task_1_2 = new Task(application_1, priority_1, new string[] { ResourceRequest
				.Any });
			application_1.AddTask(task_1_2);
			Task task_1_3 = new Task(application_1, priority_0, new string[] { ResourceRequest
				.Any });
			application_1.AddTask(task_1_3);
			application_1.Schedule();
			Task task_0_1 = new Task(application_0, priority_1, new string[] { host_0, host_1
				 });
			application_0.AddTask(task_0_1);
			Task task_0_2 = new Task(application_0, priority_1, new string[] { host_0, host_1
				 });
			application_0.AddTask(task_0_2);
			Task task_0_3 = new Task(application_0, priority_0, new string[] { ResourceRequest
				.Any });
			application_0.AddTask(task_0_3);
			application_0.Schedule();
			// Send a heartbeat to kick the tires on the Scheduler
			Log.Info("Sending hb from " + nm_0.GetHostName());
			nm_0.Heartbeat();
			// nothing new, used=4G
			Log.Info("Sending hb from " + nm_1.GetHostName());
			nm_1.Heartbeat();
			// task_0_3, used=2G
			// Get allocations from the scheduler
			Log.Info("Trying to allocate...");
			application_0.Schedule();
			CheckApplicationResourceUsage(3 * Gb, application_0);
			application_1.Schedule();
			CheckApplicationResourceUsage(3 * Gb, application_1);
			nm_0.Heartbeat();
			nm_1.Heartbeat();
			CheckNodeResourceUsage(4 * Gb, nm_0);
			CheckNodeResourceUsage(2 * Gb, nm_1);
			// Complete tasks
			Log.Info("Finishing up task_0_0");
			application_0.FinishTask(task_0_0);
			// Now task_0_1
			application_0.Schedule();
			application_1.Schedule();
			nm_0.Heartbeat();
			nm_1.Heartbeat();
			CheckApplicationResourceUsage(3 * Gb, application_0);
			CheckApplicationResourceUsage(3 * Gb, application_1);
			CheckNodeResourceUsage(4 * Gb, nm_0);
			CheckNodeResourceUsage(2 * Gb, nm_1);
			Log.Info("Finishing up task_1_0");
			application_1.FinishTask(task_1_0);
			// Now task_0_2
			application_0.Schedule();
			// final overcommit for app0 caused here
			application_1.Schedule();
			nm_0.Heartbeat();
			// final overcommit for app0 occurs here
			nm_1.Heartbeat();
			CheckApplicationResourceUsage(4 * Gb, application_0);
			CheckApplicationResourceUsage(0 * Gb, application_1);
			//checkNodeResourceUsage(1*GB, nm_0);  // final over-commit -> rm.node->1G, test.node=2G
			CheckNodeResourceUsage(2 * Gb, nm_1);
			Log.Info("Finishing up task_0_3");
			application_0.FinishTask(task_0_3);
			// No more
			application_0.Schedule();
			application_1.Schedule();
			nm_0.Heartbeat();
			nm_1.Heartbeat();
			CheckApplicationResourceUsage(2 * Gb, application_0);
			CheckApplicationResourceUsage(0 * Gb, application_1);
			//checkNodeResourceUsage(2*GB, nm_0);  // final over-commit, rm.node->1G, test.node->2G
			CheckNodeResourceUsage(0 * Gb, nm_1);
			Log.Info("Finishing up task_0_1");
			application_0.FinishTask(task_0_1);
			application_0.Schedule();
			application_1.Schedule();
			nm_0.Heartbeat();
			nm_1.Heartbeat();
			CheckApplicationResourceUsage(1 * Gb, application_0);
			CheckApplicationResourceUsage(0 * Gb, application_1);
			Log.Info("Finishing up task_0_2");
			application_0.FinishTask(task_0_2);
			// now task_1_3 can go!
			application_0.Schedule();
			application_1.Schedule();
			nm_0.Heartbeat();
			nm_1.Heartbeat();
			CheckApplicationResourceUsage(0 * Gb, application_0);
			CheckApplicationResourceUsage(4 * Gb, application_1);
			Log.Info("Finishing up task_1_3");
			application_1.FinishTask(task_1_3);
			// now task_1_1
			application_0.Schedule();
			application_1.Schedule();
			nm_0.Heartbeat();
			nm_1.Heartbeat();
			CheckApplicationResourceUsage(0 * Gb, application_0);
			CheckApplicationResourceUsage(3 * Gb, application_1);
			Log.Info("Finishing up task_1_1");
			application_1.FinishTask(task_1_1);
			application_0.Schedule();
			application_1.Schedule();
			nm_0.Heartbeat();
			nm_1.Heartbeat();
			CheckApplicationResourceUsage(0 * Gb, application_0);
			CheckApplicationResourceUsage(3 * Gb, application_1);
			Log.Info("--- END: testFifoScheduler ---");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlackListNodes()
		{
			Configuration conf = new Configuration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			rm.Start();
			FifoScheduler fs = (FifoScheduler)rm.GetResourceScheduler();
			string host = "127.0.0.1";
			RMNode node = MockNodes.NewNodeInfo(0, MockNodes.NewResource(4 * Gb), 1, host);
			fs.Handle(new NodeAddedSchedulerEvent(node));
			ApplicationId appId = BuilderUtils.NewApplicationId(100, 1);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			CreateMockRMApp(appAttemptId, rm.GetRMContext());
			SchedulerEvent appEvent = new AppAddedSchedulerEvent(appId, "default", "user");
			fs.Handle(appEvent);
			SchedulerEvent attemptEvent = new AppAttemptAddedSchedulerEvent(appAttemptId, false
				);
			fs.Handle(attemptEvent);
			// Verify the blacklist can be updated independent of requesting containers
			fs.Allocate(appAttemptId, Sharpen.Collections.EmptyList<ResourceRequest>(), Sharpen.Collections
				.EmptyList<ContainerId>(), Sharpen.Collections.SingletonList(host), null);
			NUnit.Framework.Assert.IsTrue(fs.GetApplicationAttempt(appAttemptId).IsBlacklisted
				(host));
			fs.Allocate(appAttemptId, Sharpen.Collections.EmptyList<ResourceRequest>(), Sharpen.Collections
				.EmptyList<ContainerId>(), null, Sharpen.Collections.SingletonList(host));
			NUnit.Framework.Assert.IsFalse(fs.GetApplicationAttempt(appAttemptId).IsBlacklisted
				(host));
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAppsInQueue()
		{
			Application application_0 = new Application("user_0", resourceManager);
			application_0.Submit();
			Application application_1 = new Application("user_0", resourceManager);
			application_1.Submit();
			ResourceScheduler scheduler = resourceManager.GetResourceScheduler();
			IList<ApplicationAttemptId> appsInDefault = scheduler.GetAppsInQueue("default");
			NUnit.Framework.Assert.IsTrue(appsInDefault.Contains(application_0.GetApplicationAttemptId
				()));
			NUnit.Framework.Assert.IsTrue(appsInDefault.Contains(application_1.GetApplicationAttemptId
				()));
			NUnit.Framework.Assert.AreEqual(2, appsInDefault.Count);
			NUnit.Framework.Assert.IsNull(scheduler.GetAppsInQueue("someotherqueue"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddAndRemoveAppFromFiFoScheduler()
		{
			Configuration conf = new Configuration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode> fs = (AbstractYarnScheduler
				<SchedulerApplicationAttempt, SchedulerNode>)rm.GetResourceScheduler();
			TestSchedulerUtils.VerifyAppAddedAndRemovedFromScheduler(fs.GetSchedulerApplications
				(), fs, "queue");
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

		/// <exception cref="System.Exception"/>
		public static void Main(string[] arg)
		{
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo.TestFifoScheduler t = 
				new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo.TestFifoScheduler
				();
			t.SetUp();
			t.TestFifoScheduler();
			t.TearDown();
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
