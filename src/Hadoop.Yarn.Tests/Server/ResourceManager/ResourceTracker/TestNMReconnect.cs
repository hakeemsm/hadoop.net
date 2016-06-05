using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resourcetracker
{
	public class TestNMReconnect
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private IList<RMNodeEvent> rmNodeEvents = new AList<RMNodeEvent>();

		private Dispatcher dispatcher;

		private RMContextImpl context;

		private class TestRMNodeEventDispatcher : EventHandler<RMNodeEvent>
		{
			public virtual void Handle(RMNodeEvent @event)
			{
				this._enclosing.rmNodeEvents.AddItem(@event);
			}

			internal TestRMNodeEventDispatcher(TestNMReconnect _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNMReconnect _enclosing;
		}

		internal ResourceTrackerService resourceTrackerService;

		[SetUp]
		public virtual void SetUp()
		{
			Configuration conf = new Configuration();
			// Dispatcher that processes events inline
			dispatcher = new InlineDispatcher();
			dispatcher.Register(typeof(RMNodeEventType), new TestNMReconnect.TestRMNodeEventDispatcher
				(this));
			context = new RMContextImpl(dispatcher, null, null, null, null, null, null, null, 
				null, null);
			dispatcher.Register(typeof(SchedulerEventType), new InlineDispatcher.EmptyEventHandler
				());
			dispatcher.Register(typeof(RMNodeEventType), new ResourceManager.NodeEventDispatcher
				(context));
			NMLivelinessMonitor nmLivelinessMonitor = new NMLivelinessMonitor(dispatcher);
			nmLivelinessMonitor.Init(conf);
			nmLivelinessMonitor.Start();
			NodesListManager nodesListManager = new NodesListManager(context);
			nodesListManager.Init(conf);
			RMContainerTokenSecretManager containerTokenSecretManager = new RMContainerTokenSecretManager
				(conf);
			containerTokenSecretManager.Start();
			NMTokenSecretManagerInRM nmTokenSecretManager = new NMTokenSecretManagerInRM(conf
				);
			nmTokenSecretManager.Start();
			resourceTrackerService = new ResourceTrackerService(context, nodesListManager, nmLivelinessMonitor
				, containerTokenSecretManager, nmTokenSecretManager);
			resourceTrackerService.Init(conf);
			resourceTrackerService.Start();
		}

		[TearDown]
		public virtual void TearDown()
		{
			resourceTrackerService.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReconnect()
		{
			string hostname1 = "localhost1";
			Resource capability = BuilderUtils.NewResource(1024, 1);
			RegisterNodeManagerRequest request1 = recordFactory.NewRecordInstance<RegisterNodeManagerRequest
				>();
			NodeId nodeId1 = NodeId.NewInstance(hostname1, 0);
			request1.SetNodeId(nodeId1);
			request1.SetHttpPort(0);
			request1.SetResource(capability);
			resourceTrackerService.RegisterNodeManager(request1);
			NUnit.Framework.Assert.AreEqual(RMNodeEventType.Started, rmNodeEvents[0].GetType(
				));
			rmNodeEvents.Clear();
			resourceTrackerService.RegisterNodeManager(request1);
			NUnit.Framework.Assert.AreEqual(RMNodeEventType.Reconnected, rmNodeEvents[0].GetType
				());
			rmNodeEvents.Clear();
			resourceTrackerService.RegisterNodeManager(request1);
			capability = BuilderUtils.NewResource(1024, 2);
			request1.SetResource(capability);
			NUnit.Framework.Assert.AreEqual(RMNodeEventType.Reconnected, rmNodeEvents[0].GetType
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCompareRMNodeAfterReconnect()
		{
			Configuration yarnConf = new YarnConfiguration();
			CapacityScheduler scheduler = new CapacityScheduler();
			scheduler.SetConf(yarnConf);
			ConfigurationProvider configurationProvider = ConfigurationProviderFactory.GetConfigurationProvider
				(yarnConf);
			configurationProvider.Init(yarnConf);
			context.SetConfigurationProvider(configurationProvider);
			RMNodeLabelsManager nlm = new RMNodeLabelsManager();
			nlm.Init(yarnConf);
			nlm.Start();
			context.SetNodeLabelManager(nlm);
			scheduler.SetRMContext(context);
			scheduler.Init(yarnConf);
			scheduler.Start();
			dispatcher.Register(typeof(SchedulerEventType), scheduler);
			string hostname1 = "localhost1";
			Resource capability = BuilderUtils.NewResource(4096, 4);
			RegisterNodeManagerRequest request1 = recordFactory.NewRecordInstance<RegisterNodeManagerRequest
				>();
			NodeId nodeId1 = NodeId.NewInstance(hostname1, 0);
			request1.SetNodeId(nodeId1);
			request1.SetHttpPort(0);
			request1.SetResource(capability);
			resourceTrackerService.RegisterNodeManager(request1);
			NUnit.Framework.Assert.IsNotNull(context.GetRMNodes()[nodeId1]);
			// verify Scheduler and RMContext use same RMNode reference.
			NUnit.Framework.Assert.IsTrue(scheduler.GetSchedulerNode(nodeId1).GetRMNode() == 
				context.GetRMNodes()[nodeId1]);
			NUnit.Framework.Assert.AreEqual(context.GetRMNodes()[nodeId1].GetTotalCapability(
				), capability);
			Resource capability1 = BuilderUtils.NewResource(2048, 2);
			request1.SetResource(capability1);
			resourceTrackerService.RegisterNodeManager(request1);
			NUnit.Framework.Assert.IsNotNull(context.GetRMNodes()[nodeId1]);
			// verify Scheduler and RMContext use same RMNode reference
			// after reconnect.
			NUnit.Framework.Assert.IsTrue(scheduler.GetSchedulerNode(nodeId1).GetRMNode() == 
				context.GetRMNodes()[nodeId1]);
			// verify RMNode's capability is changed.
			NUnit.Framework.Assert.AreEqual(context.GetRMNodes()[nodeId1].GetTotalCapability(
				), capability1);
			nlm.Stop();
			scheduler.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMNodeStatusAfterReconnect()
		{
			// The node(127.0.0.1:1234) reconnected with RM. When it registered with
			// RM, RM set its lastNodeHeartbeatResponse's id to 0 asynchronously. But
			// the node's heartbeat come before RM succeeded setting the id to 0.
			DrainDispatcher dispatcher = new DrainDispatcher();
			MockRM rm = new _MockRM_204(dispatcher);
			rm.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm.GetResourceTrackerService());
			nm1.RegisterNode();
			int i = 0;
			while (i < 3)
			{
				nm1.NodeHeartbeat(true);
				dispatcher.Await();
				i++;
			}
			MockNM nm2 = new MockNM("127.0.0.1:1234", 15120, rm.GetResourceTrackerService());
			nm2.RegisterNode();
			RMNode rmNode = rm.GetRMContext().GetRMNodes()[nm2.GetNodeId()];
			nm2.NodeHeartbeat(true);
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("Node is Not in Running state.", NodeState.Running
				, rmNode.GetState());
			rm.Stop();
		}

		private sealed class _MockRM_204 : MockRM
		{
			public _MockRM_204(DrainDispatcher dispatcher)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly DrainDispatcher dispatcher;
		}
	}
}
