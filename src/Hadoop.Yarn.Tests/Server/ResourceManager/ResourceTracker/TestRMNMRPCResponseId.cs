using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resourcetracker
{
	public class TestRMNMRPCResponseId
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		internal ResourceTrackerService resourceTrackerService;

		private NodeId nodeId;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Configuration conf = new Configuration();
			// Dispatcher that processes events inline
			Dispatcher dispatcher = new InlineDispatcher();
			dispatcher.Register(typeof(SchedulerEventType), new _EventHandler_65());
			// ignore
			RMContext context = new RMContextImpl(dispatcher, null, null, null, null, null, new 
				RMContainerTokenSecretManager(conf), new NMTokenSecretManagerInRM(conf), null, null
				);
			dispatcher.Register(typeof(RMNodeEventType), new ResourceManager.NodeEventDispatcher
				(context));
			NodesListManager nodesListManager = new NodesListManager(context);
			nodesListManager.Init(conf);
			context.GetContainerTokenSecretManager().RollMasterKey();
			context.GetNMTokenSecretManager().RollMasterKey();
			resourceTrackerService = new ResourceTrackerService(context, nodesListManager, new 
				NMLivelinessMonitor(dispatcher), context.GetContainerTokenSecretManager(), context
				.GetNMTokenSecretManager());
			resourceTrackerService.Init(conf);
		}

		private sealed class _EventHandler_65 : EventHandler<Org.Apache.Hadoop.Yarn.Event.Event
			>
		{
			public _EventHandler_65()
			{
			}

			public void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
			}
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
		}

		/* do nothing */
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestRPCResponseId()
		{
			string node = "localhost";
			Resource capability = BuilderUtils.NewResource(1024, 1);
			RegisterNodeManagerRequest request = recordFactory.NewRecordInstance<RegisterNodeManagerRequest
				>();
			nodeId = NodeId.NewInstance(node, 1234);
			request.SetNodeId(nodeId);
			request.SetHttpPort(0);
			request.SetResource(capability);
			RegisterNodeManagerRequest request1 = recordFactory.NewRecordInstance<RegisterNodeManagerRequest
				>();
			request1.SetNodeId(nodeId);
			request1.SetHttpPort(0);
			request1.SetResource(capability);
			resourceTrackerService.RegisterNodeManager(request1);
			NodeStatus nodeStatus = recordFactory.NewRecordInstance<NodeStatus>();
			nodeStatus.SetNodeId(nodeId);
			NodeHealthStatus nodeHealthStatus = recordFactory.NewRecordInstance<NodeHealthStatus
				>();
			nodeHealthStatus.SetIsNodeHealthy(true);
			nodeStatus.SetNodeHealthStatus(nodeHealthStatus);
			NodeHeartbeatRequest nodeHeartBeatRequest = recordFactory.NewRecordInstance<NodeHeartbeatRequest
				>();
			nodeHeartBeatRequest.SetNodeStatus(nodeStatus);
			nodeStatus.SetResponseId(0);
			NodeHeartbeatResponse response = resourceTrackerService.NodeHeartbeat(nodeHeartBeatRequest
				);
			NUnit.Framework.Assert.IsTrue(response.GetResponseId() == 1);
			nodeStatus.SetResponseId(response.GetResponseId());
			response = resourceTrackerService.NodeHeartbeat(nodeHeartBeatRequest);
			NUnit.Framework.Assert.IsTrue(response.GetResponseId() == 2);
			/* try calling with less response id */
			response = resourceTrackerService.NodeHeartbeat(nodeHeartBeatRequest);
			NUnit.Framework.Assert.IsTrue(response.GetResponseId() == 2);
			nodeStatus.SetResponseId(0);
			response = resourceTrackerService.NodeHeartbeat(nodeHeartBeatRequest);
			NUnit.Framework.Assert.IsTrue(NodeAction.Resync.Equals(response.GetNodeAction()));
			NUnit.Framework.Assert.AreEqual("Too far behind rm response id:2 nm response id:0"
				, response.GetDiagnosticsMessage());
		}
	}
}
