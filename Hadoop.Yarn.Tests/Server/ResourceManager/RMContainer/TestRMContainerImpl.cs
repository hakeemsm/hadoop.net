using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer
{
	public class TestRMContainerImpl
	{
		[NUnit.Framework.Test]
		public virtual void TestReleaseWhileRunning()
		{
			DrainDispatcher drainDispatcher = new DrainDispatcher();
			EventHandler<RMAppAttemptEvent> appAttemptEventHandler = Org.Mockito.Mockito.Mock
				<EventHandler>();
			EventHandler generic = Org.Mockito.Mockito.Mock<EventHandler>();
			drainDispatcher.Register(typeof(RMAppAttemptEventType), appAttemptEventHandler);
			drainDispatcher.Register(typeof(RMNodeEventType), generic);
			drainDispatcher.Init(new YarnConfiguration());
			drainDispatcher.Start();
			NodeId nodeId = BuilderUtils.NewNodeId("host", 3425);
			ApplicationId appId = BuilderUtils.NewApplicationId(1, 1);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			ContainerId containerId = BuilderUtils.NewContainerId(appAttemptId, 1);
			ContainerAllocationExpirer expirer = Org.Mockito.Mockito.Mock<ContainerAllocationExpirer
				>();
			Resource resource = BuilderUtils.NewResource(512, 1);
			Priority priority = BuilderUtils.NewPriority(5);
			Container container = BuilderUtils.NewContainer(containerId, nodeId, "host:3465", 
				resource, priority, null);
			ConcurrentMap<ApplicationId, RMApp> rmApps = Org.Mockito.Mockito.Spy(new ConcurrentHashMap
				<ApplicationId, RMApp>());
			RMApp rmApp = Org.Mockito.Mockito.Mock<RMApp>();
			Org.Mockito.Mockito.When(rmApp.GetRMAppAttempt((ApplicationAttemptId)Matchers.Any
				())).ThenReturn(null);
			Org.Mockito.Mockito.DoReturn(rmApp).When(rmApps)[(ApplicationId)Matchers.Any()];
			RMApplicationHistoryWriter writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>();
			SystemMetricsPublisher publisher = Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(rmContext.GetDispatcher()).ThenReturn(drainDispatcher);
			Org.Mockito.Mockito.When(rmContext.GetContainerAllocationExpirer()).ThenReturn(expirer
				);
			Org.Mockito.Mockito.When(rmContext.GetRMApplicationHistoryWriter()).ThenReturn(writer
				);
			Org.Mockito.Mockito.When(rmContext.GetRMApps()).ThenReturn(rmApps);
			Org.Mockito.Mockito.When(rmContext.GetSystemMetricsPublisher()).ThenReturn(publisher
				);
			Org.Mockito.Mockito.When(rmContext.GetYarnConfiguration()).ThenReturn(new YarnConfiguration
				());
			RMContainer rmContainer = new RMContainerImpl(container, appAttemptId, nodeId, "user"
				, rmContext);
			NUnit.Framework.Assert.AreEqual(RMContainerState.New, rmContainer.GetState());
			NUnit.Framework.Assert.AreEqual(resource, rmContainer.GetAllocatedResource());
			NUnit.Framework.Assert.AreEqual(nodeId, rmContainer.GetAllocatedNode());
			NUnit.Framework.Assert.AreEqual(priority, rmContainer.GetAllocatedPriority());
			Org.Mockito.Mockito.Verify(writer).ContainerStarted(Matchers.Any<RMContainer>());
			Org.Mockito.Mockito.Verify(publisher).ContainerCreated(Matchers.Any<RMContainer>(
				), Matchers.AnyLong());
			rmContainer.Handle(new RMContainerEvent(containerId, RMContainerEventType.Start));
			drainDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(RMContainerState.Allocated, rmContainer.GetState(
				));
			rmContainer.Handle(new RMContainerEvent(containerId, RMContainerEventType.Acquired
				));
			drainDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(RMContainerState.Acquired, rmContainer.GetState()
				);
			rmContainer.Handle(new RMContainerEvent(containerId, RMContainerEventType.Launched
				));
			drainDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(RMContainerState.Running, rmContainer.GetState());
			NUnit.Framework.Assert.AreEqual("http://host:3465/node/containerlogs/container_1_0001_01_000001/user"
				, rmContainer.GetLogURL());
			// In RUNNING state. Verify RELEASED and associated actions.
			Org.Mockito.Mockito.Reset(appAttemptEventHandler);
			ContainerStatus containerStatus = SchedulerUtils.CreateAbnormalContainerStatus(containerId
				, SchedulerUtils.ReleasedContainer);
			rmContainer.Handle(new RMContainerFinishedEvent(containerId, containerStatus, RMContainerEventType
				.Released));
			drainDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(RMContainerState.Released, rmContainer.GetState()
				);
			NUnit.Framework.Assert.AreEqual(SchedulerUtils.ReleasedContainer, rmContainer.GetDiagnosticsInfo
				());
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.Aborted, rmContainer.GetContainerExitStatus
				());
			NUnit.Framework.Assert.AreEqual(ContainerState.Complete, rmContainer.GetContainerState
				());
			Org.Mockito.Mockito.Verify(writer).ContainerFinished(Matchers.Any<RMContainer>());
			Org.Mockito.Mockito.Verify(publisher).ContainerFinished(Matchers.Any<RMContainer>
				(), Matchers.AnyLong());
			ArgumentCaptor<RMAppAttemptContainerFinishedEvent> captor = ArgumentCaptor.ForClass
				<RMAppAttemptContainerFinishedEvent>();
			Org.Mockito.Mockito.Verify(appAttemptEventHandler).Handle(captor.Capture());
			RMAppAttemptContainerFinishedEvent cfEvent = captor.GetValue();
			NUnit.Framework.Assert.AreEqual(appAttemptId, cfEvent.GetApplicationAttemptId());
			NUnit.Framework.Assert.AreEqual(containerStatus, cfEvent.GetContainerStatus());
			NUnit.Framework.Assert.AreEqual(RMAppAttemptEventType.ContainerFinished, cfEvent.
				GetType());
			// In RELEASED state. A FINIHSED event may come in.
			rmContainer.Handle(new RMContainerFinishedEvent(containerId, SchedulerUtils.CreateAbnormalContainerStatus
				(containerId, "FinishedContainer"), RMContainerEventType.Finished));
			NUnit.Framework.Assert.AreEqual(RMContainerState.Released, rmContainer.GetState()
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestExpireWhileRunning()
		{
			DrainDispatcher drainDispatcher = new DrainDispatcher();
			EventHandler<RMAppAttemptEvent> appAttemptEventHandler = Org.Mockito.Mockito.Mock
				<EventHandler>();
			EventHandler generic = Org.Mockito.Mockito.Mock<EventHandler>();
			drainDispatcher.Register(typeof(RMAppAttemptEventType), appAttemptEventHandler);
			drainDispatcher.Register(typeof(RMNodeEventType), generic);
			drainDispatcher.Init(new YarnConfiguration());
			drainDispatcher.Start();
			NodeId nodeId = BuilderUtils.NewNodeId("host", 3425);
			ApplicationId appId = BuilderUtils.NewApplicationId(1, 1);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			ContainerId containerId = BuilderUtils.NewContainerId(appAttemptId, 1);
			ContainerAllocationExpirer expirer = Org.Mockito.Mockito.Mock<ContainerAllocationExpirer
				>();
			Resource resource = BuilderUtils.NewResource(512, 1);
			Priority priority = BuilderUtils.NewPriority(5);
			Container container = BuilderUtils.NewContainer(containerId, nodeId, "host:3465", 
				resource, priority, null);
			RMApplicationHistoryWriter writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>();
			SystemMetricsPublisher publisher = Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(rmContext.GetDispatcher()).ThenReturn(drainDispatcher);
			Org.Mockito.Mockito.When(rmContext.GetContainerAllocationExpirer()).ThenReturn(expirer
				);
			Org.Mockito.Mockito.When(rmContext.GetRMApplicationHistoryWriter()).ThenReturn(writer
				);
			Org.Mockito.Mockito.When(rmContext.GetSystemMetricsPublisher()).ThenReturn(publisher
				);
			Org.Mockito.Mockito.When(rmContext.GetYarnConfiguration()).ThenReturn(new YarnConfiguration
				());
			RMContainer rmContainer = new RMContainerImpl(container, appAttemptId, nodeId, "user"
				, rmContext);
			NUnit.Framework.Assert.AreEqual(RMContainerState.New, rmContainer.GetState());
			NUnit.Framework.Assert.AreEqual(resource, rmContainer.GetAllocatedResource());
			NUnit.Framework.Assert.AreEqual(nodeId, rmContainer.GetAllocatedNode());
			NUnit.Framework.Assert.AreEqual(priority, rmContainer.GetAllocatedPriority());
			Org.Mockito.Mockito.Verify(writer).ContainerStarted(Matchers.Any<RMContainer>());
			Org.Mockito.Mockito.Verify(publisher).ContainerCreated(Matchers.Any<RMContainer>(
				), Matchers.AnyLong());
			rmContainer.Handle(new RMContainerEvent(containerId, RMContainerEventType.Start));
			drainDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(RMContainerState.Allocated, rmContainer.GetState(
				));
			rmContainer.Handle(new RMContainerEvent(containerId, RMContainerEventType.Acquired
				));
			drainDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(RMContainerState.Acquired, rmContainer.GetState()
				);
			rmContainer.Handle(new RMContainerEvent(containerId, RMContainerEventType.Launched
				));
			drainDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(RMContainerState.Running, rmContainer.GetState());
			NUnit.Framework.Assert.AreEqual("http://host:3465/node/containerlogs/container_1_0001_01_000001/user"
				, rmContainer.GetLogURL());
			// In RUNNING state. Verify EXPIRE and associated actions.
			Org.Mockito.Mockito.Reset(appAttemptEventHandler);
			ContainerStatus containerStatus = SchedulerUtils.CreateAbnormalContainerStatus(containerId
				, SchedulerUtils.ExpiredContainer);
			rmContainer.Handle(new RMContainerFinishedEvent(containerId, containerStatus, RMContainerEventType
				.Expire));
			drainDispatcher.Await();
			NUnit.Framework.Assert.AreEqual(RMContainerState.Running, rmContainer.GetState());
			Org.Mockito.Mockito.Verify(writer, Org.Mockito.Mockito.Never()).ContainerFinished
				(Matchers.Any<RMContainer>());
			Org.Mockito.Mockito.Verify(publisher, Org.Mockito.Mockito.Never()).ContainerFinished
				(Matchers.Any<RMContainer>(), Matchers.AnyLong());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExistenceOfResourceRequestInRMContainer()
		{
			Configuration conf = new Configuration();
			MockRM rm1 = new MockRM(conf);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("unknownhost:1234", 8000);
			RMApp app1 = rm1.SubmitApp(1024);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			ResourceScheduler scheduler = rm1.GetResourceScheduler();
			// request a container.
			am1.Allocate("127.0.0.1", 1024, 1, new AList<ContainerId>());
			ContainerId containerId2 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 2);
			rm1.WaitForState(nm1, containerId2, RMContainerState.Allocated);
			// Verify whether list of ResourceRequest is present in RMContainer
			// while moving to ALLOCATED state
			NUnit.Framework.Assert.IsNotNull(scheduler.GetRMContainer(containerId2).GetResourceRequests
				());
			// Allocate container
			am1.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>()).GetAllocatedContainers
				();
			rm1.WaitForState(nm1, containerId2, RMContainerState.Acquired);
			// After RMContainer moving to ACQUIRED state, list of ResourceRequest will
			// be empty
			NUnit.Framework.Assert.IsNull(scheduler.GetRMContainer(containerId2).GetResourceRequests
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStoreAllContainerMetrics()
		{
			Configuration conf = new Configuration();
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MockRM rm1 = new MockRM(conf);
			SystemMetricsPublisher publisher = Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>();
			rm1.GetRMContext().SetSystemMetricsPublisher(publisher);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("unknownhost:1234", 8000);
			RMApp app1 = rm1.SubmitApp(1024);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Running);
			// request a container.
			am1.Allocate("127.0.0.1", 1024, 1, new AList<ContainerId>());
			ContainerId containerId2 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 2);
			rm1.WaitForState(nm1, containerId2, RMContainerState.Allocated);
			am1.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>()).GetAllocatedContainers
				();
			rm1.WaitForState(nm1, containerId2, RMContainerState.Acquired);
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 2, ContainerState.Running);
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 2, ContainerState.Complete);
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Complete);
			rm1.WaitForState(nm1, containerId2, RMContainerState.Completed);
			rm1.Stop();
			// RMContainer should be publishing system metrics for all containers.
			// Since there is 1 AM container and 1 non-AM container, there should be 2
			// container created events and 2 container finished events.
			Org.Mockito.Mockito.Verify(publisher, Org.Mockito.Mockito.Times(2)).ContainerCreated
				(Matchers.Any<RMContainer>(), Matchers.AnyLong());
			Org.Mockito.Mockito.Verify(publisher, Org.Mockito.Mockito.Times(2)).ContainerFinished
				(Matchers.Any<RMContainer>(), Matchers.AnyLong());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStoreOnlyAMContainerMetrics()
		{
			Configuration conf = new Configuration();
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			conf.SetBoolean(YarnConfiguration.ApplicationHistorySaveNonAmContainerMetaInfo, false
				);
			MockRM rm1 = new MockRM(conf);
			SystemMetricsPublisher publisher = Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>();
			rm1.GetRMContext().SetSystemMetricsPublisher(publisher);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("unknownhost:1234", 8000);
			RMApp app1 = rm1.SubmitApp(1024);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Running);
			// request a container.
			am1.Allocate("127.0.0.1", 1024, 1, new AList<ContainerId>());
			ContainerId containerId2 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 2);
			rm1.WaitForState(nm1, containerId2, RMContainerState.Allocated);
			am1.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>()).GetAllocatedContainers
				();
			rm1.WaitForState(nm1, containerId2, RMContainerState.Acquired);
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 2, ContainerState.Running);
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 2, ContainerState.Complete);
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Complete);
			rm1.WaitForState(nm1, containerId2, RMContainerState.Completed);
			rm1.Stop();
			// RMContainer should be publishing system metrics only for AM container.
			Org.Mockito.Mockito.Verify(publisher, Org.Mockito.Mockito.Times(1)).ContainerCreated
				(Matchers.Any<RMContainer>(), Matchers.AnyLong());
			Org.Mockito.Mockito.Verify(publisher, Org.Mockito.Mockito.Times(1)).ContainerFinished
				(Matchers.Any<RMContainer>(), Matchers.AnyLong());
		}
	}
}
