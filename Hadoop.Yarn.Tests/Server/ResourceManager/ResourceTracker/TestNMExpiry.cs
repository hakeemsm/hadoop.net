using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
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
	public class TestNMExpiry
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resourcetracker.TestNMExpiry
			));

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		internal ResourceTrackerService resourceTrackerService;

		private class TestNmLivelinessMonitor : NMLivelinessMonitor
		{
			public TestNmLivelinessMonitor(TestNMExpiry _enclosing, Dispatcher dispatcher)
				: base(dispatcher)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
				conf.SetLong(YarnConfiguration.RmNmExpiryIntervalMs, 1000);
				base.ServiceInit(conf);
			}

			private readonly TestNMExpiry _enclosing;
		}

		[SetUp]
		public virtual void SetUp()
		{
			Configuration conf = new Configuration();
			// Dispatcher that processes events inline
			Dispatcher dispatcher = new InlineDispatcher();
			RMContext context = new RMContextImpl(dispatcher, null, null, null, null, null, null
				, null, null, null);
			dispatcher.Register(typeof(SchedulerEventType), new InlineDispatcher.EmptyEventHandler
				());
			dispatcher.Register(typeof(RMNodeEventType), new ResourceManager.NodeEventDispatcher
				(context));
			NMLivelinessMonitor nmLivelinessMonitor = new TestNMExpiry.TestNmLivelinessMonitor
				(this, dispatcher);
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

		private class ThirdNodeHeartBeatThread : Sharpen.Thread
		{
			public override void Run()
			{
				int lastResponseID = 0;
				while (!this._enclosing.stopT)
				{
					try
					{
						NodeStatus nodeStatus = TestNMExpiry.recordFactory.NewRecordInstance<NodeStatus>(
							);
						nodeStatus.SetNodeId(this._enclosing.request3.GetNodeId());
						nodeStatus.SetResponseId(lastResponseID);
						nodeStatus.SetNodeHealthStatus(TestNMExpiry.recordFactory.NewRecordInstance<NodeHealthStatus
							>());
						nodeStatus.GetNodeHealthStatus().SetIsNodeHealthy(true);
						NodeHeartbeatRequest request = TestNMExpiry.recordFactory.NewRecordInstance<NodeHeartbeatRequest
							>();
						request.SetNodeStatus(nodeStatus);
						lastResponseID = this._enclosing.resourceTrackerService.NodeHeartbeat(request).GetResponseId
							();
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception e)
					{
						TestNMExpiry.Log.Info("failed to heartbeat ", e);
					}
				}
			}

			internal ThirdNodeHeartBeatThread(TestNMExpiry _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNMExpiry _enclosing;
		}

		internal bool stopT = false;

		internal RegisterNodeManagerRequest request3;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNMExpiry()
		{
			string hostname1 = "localhost1";
			string hostname2 = "localhost2";
			string hostname3 = "localhost3";
			Resource capability = BuilderUtils.NewResource(1024, 1);
			RegisterNodeManagerRequest request1 = recordFactory.NewRecordInstance<RegisterNodeManagerRequest
				>();
			NodeId nodeId1 = NodeId.NewInstance(hostname1, 0);
			request1.SetNodeId(nodeId1);
			request1.SetHttpPort(0);
			request1.SetResource(capability);
			resourceTrackerService.RegisterNodeManager(request1);
			RegisterNodeManagerRequest request2 = recordFactory.NewRecordInstance<RegisterNodeManagerRequest
				>();
			NodeId nodeId2 = NodeId.NewInstance(hostname2, 0);
			request2.SetNodeId(nodeId2);
			request2.SetHttpPort(0);
			request2.SetResource(capability);
			resourceTrackerService.RegisterNodeManager(request2);
			int waitCount = 0;
			while (ClusterMetrics.GetMetrics().GetNumLostNMs() != 2 && waitCount++ < 20)
			{
				lock (this)
				{
					Sharpen.Runtime.Wait(this, 100);
				}
			}
			NUnit.Framework.Assert.AreEqual(2, ClusterMetrics.GetMetrics().GetNumLostNMs());
			request3 = recordFactory.NewRecordInstance<RegisterNodeManagerRequest>();
			NodeId nodeId3 = NodeId.NewInstance(hostname3, 0);
			request3.SetNodeId(nodeId3);
			request3.SetHttpPort(0);
			request3.SetResource(capability);
			resourceTrackerService.RegisterNodeManager(request3);
			/* test to see if hostanme 3 does not expire */
			stopT = false;
			new TestNMExpiry.ThirdNodeHeartBeatThread(this).Start();
			NUnit.Framework.Assert.AreEqual(2, ClusterMetrics.GetMetrics().GetNumLostNMs());
			stopT = true;
		}
	}
}
