using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestUtils
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestUtils));

		/// <summary>
		/// Get a mock
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.RMContext"/>
		/// for use in test cases.
		/// </summary>
		/// <returns>
		/// a mock
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.RMContext"/>
		/// for use in test cases
		/// </returns>
		public static RMContext GetMockRMContext()
		{
			// Null dispatcher
			Dispatcher nullDispatcher = new _Dispatcher_73();
			// No op 
			ContainerAllocationExpirer cae = new ContainerAllocationExpirer(nullDispatcher);
			Configuration conf = new Configuration();
			RMApplicationHistoryWriter writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>();
			RMContextImpl rmContext = new RMContextImpl(nullDispatcher, cae, null, null, null
				, new AMRMTokenSecretManager(conf, null), new RMContainerTokenSecretManager(conf
				), new NMTokenSecretManagerInRM(conf), new ClientToAMTokenSecretManagerInRM());
			RMNodeLabelsManager nlm = Org.Mockito.Mockito.Mock<RMNodeLabelsManager>();
			Org.Mockito.Mockito.When(nlm.GetQueueResource(Matchers.Any<string>(), Matchers.Any
				<Set>(), Matchers.Any<Resource>())).ThenAnswer(new _Answer_105());
			Org.Mockito.Mockito.When(nlm.GetResourceByLabel(Matchers.Any<string>(), Matchers.Any
				<Resource>())).ThenAnswer(new _Answer_114());
			rmContext.SetNodeLabelManager(nlm);
			rmContext.SetSystemMetricsPublisher(Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>());
			rmContext.SetRMApplicationHistoryWriter(Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>());
			return rmContext;
		}

		private sealed class _EventHandler_75 : EventHandler
		{
			public _EventHandler_75()
			{
			}

			public void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
			}
		}

		private sealed class _Dispatcher_73 : Dispatcher
		{
			public _Dispatcher_73()
			{
				this.handler = new _EventHandler_75();
			}

			private readonly EventHandler handler;

			public override void Register(Type eventType, EventHandler handler)
			{
			}

			public override EventHandler GetEventHandler()
			{
				return this.handler;
			}
		}

		private sealed class _Answer_105 : Answer<Resource>
		{
			public _Answer_105()
			{
			}

			/// <exception cref="System.Exception"/>
			public Resource Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				return (Resource)args[2];
			}
		}

		private sealed class _Answer_114 : Answer<Resource>
		{
			public _Answer_114()
			{
			}

			/// <exception cref="System.Exception"/>
			public Resource Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				return (Resource)args[1];
			}
		}

		/// <summary>Hook to spy on queues.</summary>
		internal class SpyHook : CapacityScheduler.QueueHook
		{
			public override CSQueue Hook(CSQueue queue)
			{
				return Org.Mockito.Mockito.Spy(queue);
			}
		}

		public static TestUtils.SpyHook spyHook = new TestUtils.SpyHook();

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		public static Priority CreateMockPriority(int priority)
		{
			//    Priority p = mock(Priority.class);
			//    when(p.getPriority()).thenReturn(priority);
			Priority p = recordFactory.NewRecordInstance<Priority>();
			p.SetPriority(priority);
			return p;
		}

		public static ResourceRequest CreateResourceRequest(string resourceName, int memory
			, int numContainers, bool relaxLocality, Priority priority, RecordFactory recordFactory
			)
		{
			ResourceRequest request = recordFactory.NewRecordInstance<ResourceRequest>();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability = Resources.CreateResource
				(memory, 1);
			request.SetNumContainers(numContainers);
			request.SetResourceName(resourceName);
			request.SetCapability(capability);
			request.SetRelaxLocality(relaxLocality);
			request.SetPriority(priority);
			return request;
		}

		public static ApplicationId GetMockApplicationId(int appId)
		{
			ApplicationId applicationId = Org.Mockito.Mockito.Mock<ApplicationId>();
			Org.Mockito.Mockito.When(applicationId.GetClusterTimestamp()).ThenReturn(0L);
			Org.Mockito.Mockito.When(applicationId.GetId()).ThenReturn(appId);
			return applicationId;
		}

		public static ApplicationAttemptId GetMockApplicationAttemptId(int appId, int attemptId
			)
		{
			ApplicationId applicationId = BuilderUtils.NewApplicationId(0l, appId);
			ApplicationAttemptId applicationAttemptId = Org.Mockito.Mockito.Mock<ApplicationAttemptId
				>();
			Org.Mockito.Mockito.When(applicationAttemptId.GetApplicationId()).ThenReturn(applicationId
				);
			Org.Mockito.Mockito.When(applicationAttemptId.GetAttemptId()).ThenReturn(attemptId
				);
			return applicationAttemptId;
		}

		public static FiCaSchedulerNode GetMockNode(string host, string rack, int port, int
			 capability)
		{
			NodeId nodeId = Org.Mockito.Mockito.Mock<NodeId>();
			Org.Mockito.Mockito.When(nodeId.GetHost()).ThenReturn(host);
			Org.Mockito.Mockito.When(nodeId.GetPort()).ThenReturn(port);
			RMNode rmNode = Org.Mockito.Mockito.Mock<RMNode>();
			Org.Mockito.Mockito.When(rmNode.GetNodeID()).ThenReturn(nodeId);
			Org.Mockito.Mockito.When(rmNode.GetTotalCapability()).ThenReturn(Resources.CreateResource
				(capability, 1));
			Org.Mockito.Mockito.When(rmNode.GetNodeAddress()).ThenReturn(host + ":" + port);
			Org.Mockito.Mockito.When(rmNode.GetHostName()).ThenReturn(host);
			Org.Mockito.Mockito.When(rmNode.GetRackName()).ThenReturn(rack);
			FiCaSchedulerNode node = Org.Mockito.Mockito.Spy(new FiCaSchedulerNode(rmNode, false
				));
			Log.Info("node = " + host + " avail=" + node.GetAvailableResource());
			return node;
		}

		public static ContainerId GetMockContainerId(FiCaSchedulerApp application)
		{
			ContainerId containerId = Org.Mockito.Mockito.Mock<ContainerId>();
			Org.Mockito.Mockito.DoReturn(application.GetApplicationAttemptId()).When(containerId
				).GetApplicationAttemptId();
			long id = application.GetNewContainerId();
			Org.Mockito.Mockito.DoReturn((int)id).When(containerId).GetId();
			Org.Mockito.Mockito.DoReturn(id).When(containerId).GetContainerId();
			return containerId;
		}

		public static Container GetMockContainer(ContainerId containerId, NodeId nodeId, 
			Org.Apache.Hadoop.Yarn.Api.Records.Resource resource, Priority priority)
		{
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(containerId);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nodeId);
			Org.Mockito.Mockito.When(container.GetResource()).ThenReturn(resource);
			Org.Mockito.Mockito.When(container.GetPriority()).ThenReturn(priority);
			return container;
		}
	}
}
