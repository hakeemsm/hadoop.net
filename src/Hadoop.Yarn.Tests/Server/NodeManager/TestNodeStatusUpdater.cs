using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestNodeStatusUpdater
	{
		static TestNodeStatusUpdater()
		{
			// temp fix until metrics system can auto-detect itself running in unit test:
			DefaultMetricsSystem.SetMiniClusterMode(true);
		}

		internal static readonly Log Log = LogFactory.GetLog(typeof(TestNodeStatusUpdater
			));

		internal static readonly FilePath basedir = new FilePath("target", typeof(TestNodeStatusUpdater
			).FullName);

		internal static readonly FilePath nmLocalDir = new FilePath(basedir, "nm0");

		internal static readonly FilePath tmpDir = new FilePath(basedir, "tmpDir");

		internal static readonly FilePath remoteLogsDir = new FilePath(basedir, "remotelogs"
			);

		internal static readonly FilePath logsDir = new FilePath(basedir, "logs");

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		internal volatile int heartBeatID = 0;

		internal volatile Exception nmStartError = null;

		private readonly IList<NodeId> registeredNodes = new AList<NodeId>();

		private bool triggered = false;

		private Configuration conf;

		private NodeManager nm;

		private AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			nmLocalDir.Mkdirs();
			tmpDir.Mkdirs();
			logsDir.Mkdirs();
			remoteLogsDir.Mkdirs();
			conf = CreateNMConfig();
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			this.registeredNodes.Clear();
			heartBeatID = 0;
			ServiceOperations.Stop(nm);
			assertionFailedInThread.Set(false);
			DefaultMetricsSystem.Shutdown();
		}

		public static MasterKey CreateMasterKey()
		{
			MasterKey masterKey = new MasterKeyPBImpl();
			masterKey.SetKeyId(123);
			masterKey.SetBytes(ByteBuffer.Wrap(new byte[] { 123 }));
			return masterKey;
		}

		private class MyResourceTracker : ResourceTracker
		{
			private readonly Context context;

			public MyResourceTracker(TestNodeStatusUpdater _enclosing, Context context)
			{
				this._enclosing = _enclosing;
				this.context = context;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
				 request)
			{
				NodeId nodeId = request.GetNodeId();
				Resource resource = request.GetResource();
				TestNodeStatusUpdater.Log.Info("Registering " + nodeId.ToString());
				// NOTE: this really should be checking against the config value
				IPEndPoint expected = NetUtils.GetConnectAddress(this._enclosing.conf.GetSocketAddr
					(YarnConfiguration.NmAddress, null, -1));
				NUnit.Framework.Assert.AreEqual(NetUtils.GetHostPortString(expected), nodeId.ToString
					());
				NUnit.Framework.Assert.AreEqual(5 * 1024, resource.GetMemory());
				this._enclosing.registeredNodes.AddItem(nodeId);
				RegisterNodeManagerResponse response = TestNodeStatusUpdater.recordFactory.NewRecordInstance
					<RegisterNodeManagerResponse>();
				response.SetContainerTokenMasterKey(TestNodeStatusUpdater.CreateMasterKey());
				response.SetNMTokenMasterKey(TestNodeStatusUpdater.CreateMasterKey());
				return response;
			}

			private IDictionary<ApplicationId, IList<ContainerStatus>> GetAppToContainerStatusMap
				(IList<ContainerStatus> containers)
			{
				IDictionary<ApplicationId, IList<ContainerStatus>> map = new Dictionary<ApplicationId
					, IList<ContainerStatus>>();
				foreach (ContainerStatus cs in containers)
				{
					ApplicationId applicationId = cs.GetContainerId().GetApplicationAttemptId().GetApplicationId
						();
					IList<ContainerStatus> appContainers = map[applicationId];
					if (appContainers == null)
					{
						appContainers = new AList<ContainerStatus>();
						map[applicationId] = appContainers;
					}
					appContainers.AddItem(cs);
				}
				return map;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
			{
				NodeStatus nodeStatus = request.GetNodeStatus();
				TestNodeStatusUpdater.Log.Info("Got heartbeat number " + this._enclosing.heartBeatID
					);
				NodeManagerMetrics mockMetrics = Org.Mockito.Mockito.Mock<NodeManagerMetrics>();
				Dispatcher mockDispatcher = Org.Mockito.Mockito.Mock<Dispatcher>();
				EventHandler mockEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
				Org.Mockito.Mockito.When(mockDispatcher.GetEventHandler()).ThenReturn(mockEventHandler
					);
				NMStateStoreService stateStore = new NMNullStateStoreService();
				nodeStatus.SetResponseId(this._enclosing.heartBeatID++);
				IDictionary<ApplicationId, IList<ContainerStatus>> appToContainers = this.GetAppToContainerStatusMap
					(nodeStatus.GetContainersStatuses());
				ApplicationId appId1 = ApplicationId.NewInstance(0, 1);
				ApplicationId appId2 = ApplicationId.NewInstance(0, 2);
				if (this._enclosing.heartBeatID == 1)
				{
					NUnit.Framework.Assert.AreEqual(0, nodeStatus.GetContainersStatuses().Count);
					// Give a container to the NM.
					ApplicationAttemptId appAttemptID = ApplicationAttemptId.NewInstance(appId1, 0);
					ContainerId firstContainerID = ContainerId.NewContainerId(appAttemptID, this._enclosing
						.heartBeatID);
					ContainerLaunchContext launchContext = TestNodeStatusUpdater.recordFactory.NewRecordInstance
						<ContainerLaunchContext>();
					Resource resource = BuilderUtils.NewResource(2, 1);
					long currentTime = Runtime.CurrentTimeMillis();
					string user = "testUser";
					ContainerTokenIdentifier containerToken = BuilderUtils.NewContainerTokenIdentifier
						(BuilderUtils.NewContainerToken(firstContainerID, Sharpen.Extensions.GetAddressByName
						("localhost").ToString(), 1234, user, resource, currentTime + 10000, 123, Sharpen.Runtime.GetBytesForString
						("password"), currentTime));
					Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
						 = new ContainerImpl(this._enclosing.conf, mockDispatcher, stateStore, launchContext
						, null, mockMetrics, containerToken);
					this.context.GetContainers()[firstContainerID] = container;
				}
				else
				{
					if (this._enclosing.heartBeatID == 2)
					{
						// Checks on the RM end
						NUnit.Framework.Assert.AreEqual("Number of applications should only be one!", 1, 
							nodeStatus.GetContainersStatuses().Count);
						NUnit.Framework.Assert.AreEqual("Number of container for the app should be one!", 
							1, appToContainers[appId1].Count);
						// Checks on the NM end
						ConcurrentMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
							> activeContainers = this.context.GetContainers();
						NUnit.Framework.Assert.AreEqual(1, activeContainers.Count);
						// Give another container to the NM.
						ApplicationAttemptId appAttemptID = ApplicationAttemptId.NewInstance(appId2, 0);
						ContainerId secondContainerID = ContainerId.NewContainerId(appAttemptID, this._enclosing
							.heartBeatID);
						ContainerLaunchContext launchContext = TestNodeStatusUpdater.recordFactory.NewRecordInstance
							<ContainerLaunchContext>();
						long currentTime = Runtime.CurrentTimeMillis();
						string user = "testUser";
						Resource resource = BuilderUtils.NewResource(3, 1);
						ContainerTokenIdentifier containerToken = BuilderUtils.NewContainerTokenIdentifier
							(BuilderUtils.NewContainerToken(secondContainerID, Sharpen.Extensions.GetAddressByName
							("localhost").ToString(), 1234, user, resource, currentTime + 10000, 123, Sharpen.Runtime.GetBytesForString
							("password"), currentTime));
						Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
							 = new ContainerImpl(this._enclosing.conf, mockDispatcher, stateStore, launchContext
							, null, mockMetrics, containerToken);
						this.context.GetContainers()[secondContainerID] = container;
					}
					else
					{
						if (this._enclosing.heartBeatID == 3)
						{
							// Checks on the RM end
							NUnit.Framework.Assert.AreEqual("Number of applications should have two!", 2, appToContainers
								.Count);
							NUnit.Framework.Assert.AreEqual("Number of container for the app-1 should be only one!"
								, 1, appToContainers[appId1].Count);
							NUnit.Framework.Assert.AreEqual("Number of container for the app-2 should be only one!"
								, 1, appToContainers[appId2].Count);
							// Checks on the NM end
							ConcurrentMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
								> activeContainers = this.context.GetContainers();
							NUnit.Framework.Assert.AreEqual(2, activeContainers.Count);
						}
					}
				}
				NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.NewNodeHeartbeatResponse
					(this._enclosing.heartBeatID, null, null, null, null, null, 1000L);
				return nhResponse;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private class MyNodeStatusUpdater : NodeStatusUpdaterImpl
		{
			public ResourceTracker resourceTracker;

			private Context context;

			public MyNodeStatusUpdater(TestNodeStatusUpdater _enclosing, Context context, Dispatcher
				 dispatcher, NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics)
				: base(context, dispatcher, healthChecker, metrics)
			{
				this._enclosing = _enclosing;
				this.context = context;
				this.resourceTracker = new TestNodeStatusUpdater.MyResourceTracker(this, this.context
					);
			}

			protected internal override ResourceTracker GetRMClient()
			{
				return this.resourceTracker;
			}

			protected internal override void StopRMProxy()
			{
				return;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private class MyNodeStatusUpdater2 : NodeStatusUpdaterImpl
		{
			public ResourceTracker resourceTracker;

			public MyNodeStatusUpdater2(TestNodeStatusUpdater _enclosing, Context context, Dispatcher
				 dispatcher, NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics)
				: base(context, dispatcher, healthChecker, metrics)
			{
				this._enclosing = _enclosing;
				// Test NodeStatusUpdater sends the right container statuses each time it
				// heart beats.
				this.resourceTracker = new TestNodeStatusUpdater.MyResourceTracker4(this, context
					);
			}

			protected internal override ResourceTracker GetRMClient()
			{
				return this.resourceTracker;
			}

			protected internal override void StopRMProxy()
			{
				return;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private class MyNodeStatusUpdater3 : NodeStatusUpdaterImpl
		{
			public ResourceTracker resourceTracker;

			private Context context;

			public MyNodeStatusUpdater3(TestNodeStatusUpdater _enclosing, Context context, Dispatcher
				 dispatcher, NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics)
				: base(context, dispatcher, healthChecker, metrics)
			{
				this._enclosing = _enclosing;
				this.context = context;
				this.resourceTracker = new TestNodeStatusUpdater.MyResourceTracker3(this, this.context
					);
			}

			protected internal override ResourceTracker GetRMClient()
			{
				return this.resourceTracker;
			}

			protected internal override void StopRMProxy()
			{
				return;
			}

			protected internal override bool IsTokenKeepAliveEnabled(Configuration conf)
			{
				return true;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private class MyNodeStatusUpdater4 : NodeStatusUpdaterImpl
		{
			private readonly long rmStartIntervalMS;

			private readonly bool rmNeverStart;

			public ResourceTracker resourceTracker;

			public MyNodeStatusUpdater4(TestNodeStatusUpdater _enclosing, Context context, Dispatcher
				 dispatcher, NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics, 
				long rmStartIntervalMS, bool rmNeverStart)
				: base(context, dispatcher, healthChecker, metrics)
			{
				this._enclosing = _enclosing;
				this.rmStartIntervalMS = rmStartIntervalMS;
				this.rmNeverStart = rmNeverStart;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				//record the startup time
				base.ServiceStart();
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override ResourceTracker GetRMClient()
			{
				RetryPolicy retryPolicy = RMProxy.CreateRetryPolicy(this._enclosing.conf);
				this.resourceTracker = (ResourceTracker)RetryProxy.Create<ResourceTracker>(new TestNodeStatusUpdater.MyResourceTracker6
					(this, this.rmStartIntervalMS, this.rmNeverStart), retryPolicy);
				return this.resourceTracker;
			}

			private bool IsTriggered()
			{
				return this._enclosing.triggered;
			}

			protected internal override void StopRMProxy()
			{
				return;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private class MyNodeStatusUpdater5 : NodeStatusUpdaterImpl
		{
			private ResourceTracker resourceTracker;

			private Configuration conf;

			public MyNodeStatusUpdater5(TestNodeStatusUpdater _enclosing, Context context, Dispatcher
				 dispatcher, NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics, 
				Configuration conf)
				: base(context, dispatcher, healthChecker, metrics)
			{
				this._enclosing = _enclosing;
				this.resourceTracker = new TestNodeStatusUpdater.MyResourceTracker5(this);
				this.conf = conf;
			}

			protected internal override ResourceTracker GetRMClient()
			{
				RetryPolicy retryPolicy = RMProxy.CreateRetryPolicy(this.conf);
				return (ResourceTracker)RetryProxy.Create<ResourceTracker>(this.resourceTracker, 
					retryPolicy);
			}

			protected internal override void StopRMProxy()
			{
				return;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private class MyNodeManager : NodeManager
		{
			private TestNodeStatusUpdater.MyNodeStatusUpdater3 nodeStatusUpdater;

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				this.nodeStatusUpdater = new TestNodeStatusUpdater.MyNodeStatusUpdater3(this, context
					, dispatcher, healthChecker, this.metrics);
				return this.nodeStatusUpdater;
			}

			public override NodeStatusUpdater GetNodeStatusUpdater()
			{
				return this.nodeStatusUpdater;
			}

			internal MyNodeManager(TestNodeStatusUpdater _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private class MyNodeManager2 : NodeManager
		{
			public bool isStopped = false;

			private NodeStatusUpdater nodeStatusUpdater;

			private CyclicBarrier syncBarrier;

			private Configuration conf;

			public MyNodeManager2(TestNodeStatusUpdater _enclosing, CyclicBarrier syncBarrier
				, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.syncBarrier = syncBarrier;
				this.conf = conf;
			}

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				this.nodeStatusUpdater = new TestNodeStatusUpdater.MyNodeStatusUpdater5(this, context
					, dispatcher, healthChecker, this.metrics, this.conf);
				return this.nodeStatusUpdater;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				System.Console.Out.WriteLine("Called stooppppp");
				base.ServiceStop();
				this.isStopped = true;
				ConcurrentMap<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					> applications = this.GetNMContext().GetApplications();
				// ensure that applications are empty
				if (!applications.IsEmpty())
				{
					this._enclosing.assertionFailedInThread.Set(true);
				}
				this.syncBarrier.Await(10000, TimeUnit.Milliseconds);
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private class MyResourceTracker2 : ResourceTracker
		{
			public NodeAction heartBeatNodeAction = NodeAction.Normal;

			public NodeAction registerNodeAction = NodeAction.Normal;

			public string shutDownMessage = string.Empty;

			public string rmVersion = "3.0.1";

			//
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
				 request)
			{
				RegisterNodeManagerResponse response = TestNodeStatusUpdater.recordFactory.NewRecordInstance
					<RegisterNodeManagerResponse>();
				response.SetNodeAction(this.registerNodeAction);
				response.SetContainerTokenMasterKey(TestNodeStatusUpdater.CreateMasterKey());
				response.SetNMTokenMasterKey(TestNodeStatusUpdater.CreateMasterKey());
				response.SetDiagnosticsMessage(this.shutDownMessage);
				response.SetRMVersion(this.rmVersion);
				return response;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
			{
				NodeStatus nodeStatus = request.GetNodeStatus();
				nodeStatus.SetResponseId(this._enclosing.heartBeatID++);
				NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.NewNodeHeartbeatResponse
					(this._enclosing.heartBeatID, this.heartBeatNodeAction, null, null, null, null, 
					1000L);
				nhResponse.SetDiagnosticsMessage(this.shutDownMessage);
				return nhResponse;
			}

			internal MyResourceTracker2(TestNodeStatusUpdater _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private class MyResourceTracker3 : ResourceTracker
		{
			public NodeAction heartBeatNodeAction = NodeAction.Normal;

			public NodeAction registerNodeAction = NodeAction.Normal;

			private IDictionary<ApplicationId, IList<long>> keepAliveRequests = new Dictionary
				<ApplicationId, IList<long>>();

			private ApplicationId appId = BuilderUtils.NewApplicationId(1, 1);

			private readonly Context context;

			internal MyResourceTracker3(TestNodeStatusUpdater _enclosing, Context context)
			{
				this._enclosing = _enclosing;
				this.context = context;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
				 request)
			{
				RegisterNodeManagerResponse response = TestNodeStatusUpdater.recordFactory.NewRecordInstance
					<RegisterNodeManagerResponse>();
				response.SetNodeAction(this.registerNodeAction);
				response.SetContainerTokenMasterKey(TestNodeStatusUpdater.CreateMasterKey());
				response.SetNMTokenMasterKey(TestNodeStatusUpdater.CreateMasterKey());
				return response;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
			{
				TestNodeStatusUpdater.Log.Info("Got heartBeatId: [" + this._enclosing.heartBeatID
					 + "]");
				NodeStatus nodeStatus = request.GetNodeStatus();
				nodeStatus.SetResponseId(this._enclosing.heartBeatID++);
				NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.NewNodeHeartbeatResponse
					(this._enclosing.heartBeatID, this.heartBeatNodeAction, null, null, null, null, 
					1000L);
				if (nodeStatus.GetKeepAliveApplications() != null && nodeStatus.GetKeepAliveApplications
					().Count > 0)
				{
					foreach (ApplicationId appId in nodeStatus.GetKeepAliveApplications())
					{
						IList<long> list = this.keepAliveRequests[appId];
						if (list == null)
						{
							list = new List<long>();
							this.keepAliveRequests[appId] = list;
						}
						list.AddItem(Runtime.CurrentTimeMillis());
					}
				}
				if (this._enclosing.heartBeatID == 2)
				{
					TestNodeStatusUpdater.Log.Info("Sending FINISH_APP for application: [" + this.appId
						 + "]");
					this.context.GetApplications()[this.appId] = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
						>();
					nhResponse.AddAllApplicationsToCleanup(Sharpen.Collections.SingletonList(this.appId
						));
				}
				return nhResponse;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private Credentials expectedCredentials = new Credentials();

		private class MyResourceTracker4 : ResourceTracker
		{
			public NodeAction registerNodeAction = NodeAction.Normal;

			public NodeAction heartBeatNodeAction = NodeAction.Normal;

			private Context context;

			private readonly ContainerStatus containerStatus2 = TestNodeStatusUpdater.CreateContainerStatus
				(2, ContainerState.Running);

			private readonly ContainerStatus containerStatus3 = TestNodeStatusUpdater.CreateContainerStatus
				(3, ContainerState.Complete);

			private readonly ContainerStatus containerStatus4 = TestNodeStatusUpdater.CreateContainerStatus
				(4, ContainerState.Running);

			private readonly ContainerStatus containerStatus5 = TestNodeStatusUpdater.CreateContainerStatus
				(5, ContainerState.Complete);

			public MyResourceTracker4(TestNodeStatusUpdater _enclosing, Context context)
			{
				this._enclosing = _enclosing;
				// Test NodeStatusUpdater sends the right container statuses each time it
				// heart beats.
				// create app Credentials
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token1 = new Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>();
				token1.SetKind(new Text("kind1"));
				this._enclosing.expectedCredentials.AddToken(new Text("token1"), token1);
				this.context = context;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
				 request)
			{
				RegisterNodeManagerResponse response = TestNodeStatusUpdater.recordFactory.NewRecordInstance
					<RegisterNodeManagerResponse>();
				response.SetNodeAction(this.registerNodeAction);
				response.SetContainerTokenMasterKey(TestNodeStatusUpdater.CreateMasterKey());
				response.SetNMTokenMasterKey(TestNodeStatusUpdater.CreateMasterKey());
				return response;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
			{
				IList<ContainerId> finishedContainersPulledByAM = new AList<ContainerId>();
				try
				{
					if (this._enclosing.heartBeatID == 0)
					{
						NUnit.Framework.Assert.AreEqual(0, request.GetNodeStatus().GetContainersStatuses(
							).Count);
						NUnit.Framework.Assert.AreEqual(0, this.context.GetContainers().Count);
					}
					else
					{
						if (this._enclosing.heartBeatID == 1)
						{
							IList<ContainerStatus> statuses = request.GetNodeStatus().GetContainersStatuses();
							NUnit.Framework.Assert.AreEqual(2, statuses.Count);
							NUnit.Framework.Assert.AreEqual(2, this.context.GetContainers().Count);
							bool container2Exist = false;
							bool container3Exist = false;
							foreach (ContainerStatus status in statuses)
							{
								if (status.GetContainerId().Equals(this.containerStatus2.GetContainerId()))
								{
									NUnit.Framework.Assert.IsTrue(status.GetState().Equals(this.containerStatus2.GetState
										()));
									container2Exist = true;
								}
								if (status.GetContainerId().Equals(this.containerStatus3.GetContainerId()))
								{
									NUnit.Framework.Assert.IsTrue(status.GetState().Equals(this.containerStatus3.GetState
										()));
									container3Exist = true;
								}
							}
							NUnit.Framework.Assert.IsTrue(container2Exist && container3Exist);
							// should throw exception that can be retried by the
							// nodeStatusUpdaterRunnable, otherwise nm just shuts down and the
							// test passes.
							throw new YarnRuntimeException("Lost the heartbeat response");
						}
						else
						{
							if (this._enclosing.heartBeatID == 2 || this._enclosing.heartBeatID == 3)
							{
								IList<ContainerStatus> statuses = request.GetNodeStatus().GetContainersStatuses();
								if (this._enclosing.heartBeatID == 2)
								{
									// NM should send completed containers again, since the last
									// heartbeat is lost.
									NUnit.Framework.Assert.AreEqual(4, statuses.Count);
								}
								else
								{
									// NM should not send completed containers again, since the last
									// heartbeat is successful.
									NUnit.Framework.Assert.AreEqual(2, statuses.Count);
								}
								NUnit.Framework.Assert.AreEqual(4, this.context.GetContainers().Count);
								bool container2Exist = false;
								bool container3Exist = false;
								bool container4Exist = false;
								bool container5Exist = false;
								foreach (ContainerStatus status in statuses)
								{
									if (status.GetContainerId().Equals(this.containerStatus2.GetContainerId()))
									{
										NUnit.Framework.Assert.IsTrue(status.GetState().Equals(this.containerStatus2.GetState
											()));
										container2Exist = true;
									}
									if (status.GetContainerId().Equals(this.containerStatus3.GetContainerId()))
									{
										NUnit.Framework.Assert.IsTrue(status.GetState().Equals(this.containerStatus3.GetState
											()));
										container3Exist = true;
									}
									if (status.GetContainerId().Equals(this.containerStatus4.GetContainerId()))
									{
										NUnit.Framework.Assert.IsTrue(status.GetState().Equals(this.containerStatus4.GetState
											()));
										container4Exist = true;
									}
									if (status.GetContainerId().Equals(this.containerStatus5.GetContainerId()))
									{
										NUnit.Framework.Assert.IsTrue(status.GetState().Equals(this.containerStatus5.GetState
											()));
										container5Exist = true;
									}
								}
								if (this._enclosing.heartBeatID == 2)
								{
									NUnit.Framework.Assert.IsTrue(container2Exist && container3Exist && container4Exist
										 && container5Exist);
								}
								else
								{
									// NM do not send completed containers again
									NUnit.Framework.Assert.IsTrue(container2Exist && !container3Exist && container4Exist
										 && !container5Exist);
								}
								if (this._enclosing.heartBeatID == 3)
								{
									finishedContainersPulledByAM.AddItem(this.containerStatus3.GetContainerId());
								}
							}
							else
							{
								if (this._enclosing.heartBeatID == 4)
								{
									IList<ContainerStatus> statuses = request.GetNodeStatus().GetContainersStatuses();
									NUnit.Framework.Assert.AreEqual(2, statuses.Count);
									// Container 3 is acked by AM, hence removed from context
									NUnit.Framework.Assert.AreEqual(3, this.context.GetContainers().Count);
									bool container3Exist = false;
									foreach (ContainerStatus status in statuses)
									{
										if (status.GetContainerId().Equals(this.containerStatus3.GetContainerId()))
										{
											container3Exist = true;
										}
									}
									NUnit.Framework.Assert.IsFalse(container3Exist);
								}
							}
						}
					}
				}
				catch (Exception error)
				{
					Sharpen.Runtime.PrintStackTrace(error);
					this._enclosing.assertionFailedInThread.Set(true);
				}
				finally
				{
					this._enclosing.heartBeatID++;
				}
				NodeStatus nodeStatus = request.GetNodeStatus();
				nodeStatus.SetResponseId(this._enclosing.heartBeatID);
				NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.NewNodeHeartbeatResponse
					(this._enclosing.heartBeatID, this.heartBeatNodeAction, null, null, null, null, 
					1000L);
				nhResponse.AddContainersToBeRemovedFromNM(finishedContainersPulledByAM);
				IDictionary<ApplicationId, ByteBuffer> appCredentials = new Dictionary<ApplicationId
					, ByteBuffer>();
				DataOutputBuffer dob = new DataOutputBuffer();
				this._enclosing.expectedCredentials.WriteTokenStorageToStream(dob);
				ByteBuffer byteBuffer1 = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
				appCredentials[ApplicationId.NewInstance(1234, 1)] = byteBuffer1;
				nhResponse.SetSystemCredentialsForApps(appCredentials);
				return nhResponse;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private class MyResourceTracker5 : ResourceTracker
		{
			public NodeAction registerNodeAction = NodeAction.Normal;

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
				 request)
			{
				RegisterNodeManagerResponse response = TestNodeStatusUpdater.recordFactory.NewRecordInstance
					<RegisterNodeManagerResponse>();
				response.SetNodeAction(this.registerNodeAction);
				response.SetContainerTokenMasterKey(TestNodeStatusUpdater.CreateMasterKey());
				response.SetNMTokenMasterKey(TestNodeStatusUpdater.CreateMasterKey());
				return response;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
			{
				this._enclosing.heartBeatID++;
				if (this._enclosing.heartBeatID == 1)
				{
					// EOFException should be retried as well.
					throw new EOFException("NodeHeartbeat exception");
				}
				else
				{
					throw new ConnectException("NodeHeartbeat exception");
				}
			}

			internal MyResourceTracker5(TestNodeStatusUpdater _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		private class MyResourceTracker6 : ResourceTracker
		{
			private long rmStartIntervalMS;

			private bool rmNeverStart;

			private readonly long waitStartTime;

			public MyResourceTracker6(TestNodeStatusUpdater _enclosing, long rmStartIntervalMS
				, bool rmNeverStart)
			{
				this._enclosing = _enclosing;
				this.rmStartIntervalMS = rmStartIntervalMS;
				this.rmNeverStart = rmNeverStart;
				this.waitStartTime = Runtime.CurrentTimeMillis();
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
				 request)
			{
				if (Runtime.CurrentTimeMillis() - this.waitStartTime <= this.rmStartIntervalMS ||
					 this.rmNeverStart)
				{
					throw new ConnectException("Faking RM start failure as start " + "delay timer has not expired."
						);
				}
				else
				{
					NodeId nodeId = request.GetNodeId();
					Resource resource = request.GetResource();
					TestNodeStatusUpdater.Log.Info("Registering " + nodeId.ToString());
					// NOTE: this really should be checking against the config value
					IPEndPoint expected = NetUtils.GetConnectAddress(this._enclosing.conf.GetSocketAddr
						(YarnConfiguration.NmAddress, null, -1));
					NUnit.Framework.Assert.AreEqual(NetUtils.GetHostPortString(expected), nodeId.ToString
						());
					NUnit.Framework.Assert.AreEqual(5 * 1024, resource.GetMemory());
					this._enclosing.registeredNodes.AddItem(nodeId);
					RegisterNodeManagerResponse response = TestNodeStatusUpdater.recordFactory.NewRecordInstance
						<RegisterNodeManagerResponse>();
					this._enclosing.triggered = true;
					return response;
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
			{
				NodeStatus nodeStatus = request.GetNodeStatus();
				nodeStatus.SetResponseId(this._enclosing.heartBeatID++);
				NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.NewNodeHeartbeatResponse
					(this._enclosing.heartBeatID, NodeAction.Normal, null, null, null, null, 1000L);
				return nhResponse;
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		[SetUp]
		public virtual void ClearError()
		{
			nmStartError = null;
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void DeleteBaseDir()
		{
			FileContext lfs = FileContext.GetLocalFSFileContext();
			lfs.Delete(new Path(basedir.GetPath()), true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecentlyFinishedContainers()
		{
			NodeManager nm = new NodeManager();
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(NodeStatusUpdaterImpl.YarnNodemanagerDurationToTrackStoppedContainers, "10000"
				);
			nm.Init(conf);
			NodeStatusUpdaterImpl nodeStatusUpdater = (NodeStatusUpdaterImpl)nm.GetNodeStatusUpdater
				();
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			ContainerId cId = ContainerId.NewContainerId(appAttemptId, 0);
			nm.GetNMContext().GetApplications().PutIfAbsent(appId, Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>());
			nm.GetNMContext().GetContainers().PutIfAbsent(cId, Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>());
			nodeStatusUpdater.AddCompletedContainer(cId);
			NUnit.Framework.Assert.IsTrue(nodeStatusUpdater.IsContainerRecentlyStopped(cId));
			Sharpen.Collections.Remove(nm.GetNMContext().GetContainers(), cId);
			long time1 = Runtime.CurrentTimeMillis();
			int waitInterval = 15;
			while (waitInterval-- > 0 && nodeStatusUpdater.IsContainerRecentlyStopped(cId))
			{
				nodeStatusUpdater.RemoveVeryOldStoppedContainersFromCache();
				Sharpen.Thread.Sleep(1000);
			}
			long time2 = Runtime.CurrentTimeMillis();
			// By this time the container will be removed from cache. need to verify.
			NUnit.Framework.Assert.IsFalse(nodeStatusUpdater.IsContainerRecentlyStopped(cId));
			NUnit.Framework.Assert.IsTrue((time2 - time1) >= 10000 && (time2 - time1) <= 250000
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRemovePreviousCompletedContainersFromContext()
		{
			NodeManager nm = new NodeManager();
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(NodeStatusUpdaterImpl.YarnNodemanagerDurationToTrackStoppedContainers, "10000"
				);
			nm.Init(conf);
			NodeStatusUpdaterImpl nodeStatusUpdater = (NodeStatusUpdaterImpl)nm.GetNodeStatusUpdater
				();
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			ContainerId cId = ContainerId.NewContainerId(appAttemptId, 1);
			Org.Apache.Hadoop.Yarn.Api.Records.Token containerToken = BuilderUtils.NewContainerToken
				(cId, "anyHost", 1234, "anyUser", BuilderUtils.NewResource(1024, 1), 0, 123, Sharpen.Runtime.GetBytesForString
				("password"), 0);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container anyCompletedContainer
				 = new _ContainerImpl_893(conf, null, null, null, null, null, BuilderUtils.NewContainerTokenIdentifier
				(containerToken));
			ContainerId runningContainerId = ContainerId.NewContainerId(appAttemptId, 3);
			Org.Apache.Hadoop.Yarn.Api.Records.Token runningContainerToken = BuilderUtils.NewContainerToken
				(runningContainerId, "anyHost", 1234, "anyUser", BuilderUtils.NewResource(1024, 
				1), 0, 123, Sharpen.Runtime.GetBytesForString("password"), 0);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container runningContainer
				 = new _ContainerImpl_914(conf, null, null, null, null, null, BuilderUtils.NewContainerTokenIdentifier
				(runningContainerToken));
			nm.GetNMContext().GetApplications().PutIfAbsent(appId, Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>());
			nm.GetNMContext().GetContainers()[cId] = anyCompletedContainer;
			nm.GetNMContext().GetContainers()[runningContainerId] = runningContainer;
			NUnit.Framework.Assert.AreEqual(2, nodeStatusUpdater.GetContainerStatuses().Count
				);
			IList<ContainerId> ackedContainers = new AList<ContainerId>();
			ackedContainers.AddItem(cId);
			ackedContainers.AddItem(runningContainerId);
			nodeStatusUpdater.RemoveOrTrackCompletedContainersFromContext(ackedContainers);
			ICollection<ContainerId> containerIdSet = new HashSet<ContainerId>();
			IList<ContainerStatus> containerStatuses = nodeStatusUpdater.GetContainerStatuses
				();
			foreach (ContainerStatus status in containerStatuses)
			{
				containerIdSet.AddItem(status.GetContainerId());
			}
			NUnit.Framework.Assert.AreEqual(1, containerStatuses.Count);
			// completed container is removed;
			NUnit.Framework.Assert.IsFalse(containerIdSet.Contains(cId));
			// running container is not removed;
			NUnit.Framework.Assert.IsTrue(containerIdSet.Contains(runningContainerId));
		}

		private sealed class _ContainerImpl_893 : ContainerImpl
		{
			public _ContainerImpl_893(Configuration baseArg1, Dispatcher baseArg2, NMStateStoreService
				 baseArg3, ContainerLaunchContext baseArg4, Credentials baseArg5, NodeManagerMetrics
				 baseArg6, ContainerTokenIdentifier baseArg7)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
			{
			}

			public override ContainerState GetCurrentState()
			{
				return ContainerState.Complete;
			}

			public override ContainerState GetContainerState()
			{
				return ContainerState.Done;
			}
		}

		private sealed class _ContainerImpl_914 : ContainerImpl
		{
			public _ContainerImpl_914(Configuration baseArg1, Dispatcher baseArg2, NMStateStoreService
				 baseArg3, ContainerLaunchContext baseArg4, Credentials baseArg5, NodeManagerMetrics
				 baseArg6, ContainerTokenIdentifier baseArg7)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
			{
			}

			public override ContainerState GetCurrentState()
			{
				return ContainerState.Running;
			}

			public override ContainerState GetContainerState()
			{
				return ContainerState.Running;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCompletedContainersIsRecentlyStopped()
		{
			NodeManager nm = new NodeManager();
			nm.Init(conf);
			NodeStatusUpdaterImpl nodeStatusUpdater = (NodeStatusUpdaterImpl)nm.GetNodeStatusUpdater
				();
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 completedApp = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			Org.Mockito.Mockito.When(completedApp.GetApplicationState()).ThenReturn(ApplicationState
				.Finished);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 1);
			Org.Apache.Hadoop.Yarn.Api.Records.Token containerToken = BuilderUtils.NewContainerToken
				(containerId, "host", 1234, "user", BuilderUtils.NewResource(1024, 1), 0, 123, Sharpen.Runtime.GetBytesForString
				("password"), 0);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container completedContainer
				 = new _ContainerImpl_972(conf, null, null, null, null, null, BuilderUtils.NewContainerTokenIdentifier
				(containerToken));
			nm.GetNMContext().GetApplications().PutIfAbsent(appId, completedApp);
			nm.GetNMContext().GetContainers()[containerId] = completedContainer;
			NUnit.Framework.Assert.AreEqual(1, nodeStatusUpdater.GetContainerStatuses().Count
				);
			NUnit.Framework.Assert.IsTrue(nodeStatusUpdater.IsContainerRecentlyStopped(containerId
				));
		}

		private sealed class _ContainerImpl_972 : ContainerImpl
		{
			public _ContainerImpl_972(Configuration baseArg1, Dispatcher baseArg2, NMStateStoreService
				 baseArg3, ContainerLaunchContext baseArg4, Credentials baseArg5, NodeManagerMetrics
				 baseArg6, ContainerTokenIdentifier baseArg7)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
			{
			}

			public override ContainerState GetCurrentState()
			{
				return ContainerState.Complete;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCleanedupApplicationContainerCleanup()
		{
			NodeManager nm = new NodeManager();
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(NodeStatusUpdaterImpl.YarnNodemanagerDurationToTrackStoppedContainers, "1000000"
				);
			nm.Init(conf);
			NodeStatusUpdaterImpl nodeStatusUpdater = (NodeStatusUpdaterImpl)nm.GetNodeStatusUpdater
				();
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			ContainerId cId = ContainerId.NewContainerId(appAttemptId, 1);
			Org.Apache.Hadoop.Yarn.Api.Records.Token containerToken = BuilderUtils.NewContainerToken
				(cId, "anyHost", 1234, "anyUser", BuilderUtils.NewResource(1024, 1), 0, 123, Sharpen.Runtime.GetBytesForString
				("password"), 0);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container anyCompletedContainer
				 = new _ContainerImpl_1009(conf, null, null, null, null, null, BuilderUtils.NewContainerTokenIdentifier
				(containerToken));
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 application = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			Org.Mockito.Mockito.When(application.GetApplicationState()).ThenReturn(ApplicationState
				.Running);
			nm.GetNMContext().GetApplications().PutIfAbsent(appId, application);
			nm.GetNMContext().GetContainers()[cId] = anyCompletedContainer;
			NUnit.Framework.Assert.AreEqual(1, nodeStatusUpdater.GetContainerStatuses().Count
				);
			Org.Mockito.Mockito.When(application.GetApplicationState()).ThenReturn(ApplicationState
				.FinishingContainersWait);
			// The completed container will be saved in case of lost heartbeat.
			NUnit.Framework.Assert.AreEqual(1, nodeStatusUpdater.GetContainerStatuses().Count
				);
			NUnit.Framework.Assert.AreEqual(1, nodeStatusUpdater.GetContainerStatuses().Count
				);
			nm.GetNMContext().GetContainers()[cId] = anyCompletedContainer;
			Sharpen.Collections.Remove(nm.GetNMContext().GetApplications(), appId);
			// The completed container will be saved in case of lost heartbeat.
			NUnit.Framework.Assert.AreEqual(1, nodeStatusUpdater.GetContainerStatuses().Count
				);
			NUnit.Framework.Assert.AreEqual(1, nodeStatusUpdater.GetContainerStatuses().Count
				);
		}

		private sealed class _ContainerImpl_1009 : ContainerImpl
		{
			public _ContainerImpl_1009(Configuration baseArg1, Dispatcher baseArg2, NMStateStoreService
				 baseArg3, ContainerLaunchContext baseArg4, Credentials baseArg5, NodeManagerMetrics
				 baseArg6, ContainerTokenIdentifier baseArg7)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
			{
			}

			public override ContainerState GetCurrentState()
			{
				return ContainerState.Complete;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNMRegistration()
		{
			nm = new _NodeManager_1039();
			YarnConfiguration conf = CreateNMConfig();
			nm.Init(conf);
			// verify that the last service is the nodeStatusUpdater (ie registration
			// with RM)
			object[] services = Sharpen.Collections.ToArray(nm.GetServices());
			object lastService = services[services.Length - 1];
			NUnit.Framework.Assert.IsTrue("last service is NOT the node status updater", lastService
				 is NodeStatusUpdater);
			new _Thread_1058(this).Start();
			System.Console.Out.WriteLine(" ----- thread already started.." + nm.GetServiceState
				());
			int waitCount = 0;
			while (nm.GetServiceState() == Service.STATE.Inited && waitCount++ != 50)
			{
				Log.Info("Waiting for NM to start..");
				if (nmStartError != null)
				{
					Log.Error("Error during startup. ", nmStartError);
					NUnit.Framework.Assert.Fail(nmStartError.InnerException.Message);
				}
				Sharpen.Thread.Sleep(2000);
			}
			if (nm.GetServiceState() != Service.STATE.Started)
			{
				// NM could have failed.
				NUnit.Framework.Assert.Fail("NodeManager failed to start");
			}
			waitCount = 0;
			while (heartBeatID <= 3 && waitCount++ != 200)
			{
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.IsFalse(heartBeatID <= 3);
			NUnit.Framework.Assert.AreEqual("Number of registered NMs is wrong!!", 1, this.registeredNodes
				.Count);
			nm.Stop();
		}

		private sealed class _NodeManager_1039 : NodeManager
		{
			public _NodeManager_1039()
			{
			}

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				return new TestNodeStatusUpdater.MyNodeStatusUpdater(this, context, dispatcher, healthChecker
					, this.metrics);
			}
		}

		private sealed class _Thread_1058 : Sharpen.Thread
		{
			public _Thread_1058(TestNodeStatusUpdater _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				try
				{
					this._enclosing.nm.Start();
				}
				catch (Exception e)
				{
					this._enclosing.nmStartError = e;
					throw new YarnRuntimeException(e);
				}
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStopReentrant()
		{
			AtomicInteger numCleanups = new AtomicInteger(0);
			nm = new _NodeManager_1100(numCleanups);
			YarnConfiguration conf = CreateNMConfig();
			nm.Init(conf);
			nm.Start();
			int waitCount = 0;
			while (heartBeatID < 1 && waitCount++ != 200)
			{
				Sharpen.Thread.Sleep(500);
			}
			NUnit.Framework.Assert.IsFalse(heartBeatID < 1);
			// Meanwhile call stop directly as the shutdown hook would
			nm.Stop();
			// NM takes a while to reach the STOPPED state.
			waitCount = 0;
			while (nm.GetServiceState() != Service.STATE.Stopped && waitCount++ != 20)
			{
				Log.Info("Waiting for NM to stop..");
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.AreEqual(Service.STATE.Stopped, nm.GetServiceState());
			NUnit.Framework.Assert.AreEqual(numCleanups.Get(), 1);
		}

		private sealed class _NodeManager_1100 : NodeManager
		{
			public _NodeManager_1100(AtomicInteger numCleanups)
			{
				this.numCleanups = numCleanups;
			}

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				TestNodeStatusUpdater.MyNodeStatusUpdater myNodeStatusUpdater = new TestNodeStatusUpdater.MyNodeStatusUpdater
					(this, context, dispatcher, healthChecker, this.metrics);
				TestNodeStatusUpdater.MyResourceTracker2 myResourceTracker2 = new TestNodeStatusUpdater.MyResourceTracker2
					(this);
				myResourceTracker2.heartBeatNodeAction = NodeAction.Shutdown;
				myNodeStatusUpdater.resourceTracker = myResourceTracker2;
				return myNodeStatusUpdater;
			}

			protected internal override ContainerManagerImpl CreateContainerManager(Context context
				, ContainerExecutor exec, DeletionService del, NodeStatusUpdater nodeStatusUpdater
				, ApplicationACLsManager aclsManager, LocalDirsHandlerService dirsHandler)
			{
				return new _ContainerManagerImpl_1119(numCleanups, context, exec, del, nodeStatusUpdater
					, this.metrics, aclsManager, dirsHandler);
			}

			private sealed class _ContainerManagerImpl_1119 : ContainerManagerImpl
			{
				public _ContainerManagerImpl_1119(AtomicInteger numCleanups, Context baseArg1, ContainerExecutor
					 baseArg2, DeletionService baseArg3, NodeStatusUpdater baseArg4, NodeManagerMetrics
					 baseArg5, ApplicationACLsManager baseArg6, LocalDirsHandlerService baseArg7)
					: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
				{
					this.numCleanups = numCleanups;
				}

				public override void CleanUpApplicationsOnNMShutDown()
				{
					base.CleanUpApplicationsOnNMShutDown();
					numCleanups.IncrementAndGet();
				}

				private readonly AtomicInteger numCleanups;
			}

			private readonly AtomicInteger numCleanups;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeDecommision()
		{
			nm = GetNodeManager(NodeAction.Shutdown);
			YarnConfiguration conf = CreateNMConfig();
			nm.Init(conf);
			NUnit.Framework.Assert.AreEqual(Service.STATE.Inited, nm.GetServiceState());
			nm.Start();
			int waitCount = 0;
			while (heartBeatID < 1 && waitCount++ != 200)
			{
				Sharpen.Thread.Sleep(500);
			}
			NUnit.Framework.Assert.IsFalse(heartBeatID < 1);
			NUnit.Framework.Assert.IsTrue(nm.GetNMContext().GetDecommissioned());
			// NM takes a while to reach the STOPPED state.
			waitCount = 0;
			while (nm.GetServiceState() != Service.STATE.Stopped && waitCount++ != 20)
			{
				Log.Info("Waiting for NM to stop..");
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.AreEqual(Service.STATE.Stopped, nm.GetServiceState());
		}

		private abstract class NodeManagerWithCustomNodeStatusUpdater : NodeManager
		{
			private NodeStatusUpdater updater;

			private NodeManagerWithCustomNodeStatusUpdater(TestNodeStatusUpdater _enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				this.updater = this.CreateUpdater(context, dispatcher, healthChecker);
				return this.updater;
			}

			public virtual NodeStatusUpdater GetUpdater()
			{
				return this.updater;
			}

			internal abstract NodeStatusUpdater CreateUpdater(Context context, Dispatcher dispatcher
				, NodeHealthCheckerService healthChecker);

			private readonly TestNodeStatusUpdater _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNMShutdownForRegistrationFailure()
		{
			nm = new _NodeManagerWithCustomNodeStatusUpdater_1205();
			VerifyNodeStartFailure("Recieved SHUTDOWN signal from Resourcemanager ," + "Registration of NodeManager failed, "
				 + "Message from ResourceManager: RM Shutting Down Node");
		}

		private sealed class _NodeManagerWithCustomNodeStatusUpdater_1205 : TestNodeStatusUpdater.NodeManagerWithCustomNodeStatusUpdater
		{
			public _NodeManagerWithCustomNodeStatusUpdater_1205()
			{
			}

			internal override NodeStatusUpdater CreateUpdater(Context context, Dispatcher dispatcher
				, NodeHealthCheckerService healthChecker)
			{
				TestNodeStatusUpdater.MyNodeStatusUpdater nodeStatusUpdater = new TestNodeStatusUpdater.MyNodeStatusUpdater
					(this, context, dispatcher, healthChecker, this.metrics);
				TestNodeStatusUpdater.MyResourceTracker2 myResourceTracker2 = new TestNodeStatusUpdater.MyResourceTracker2
					(this);
				myResourceTracker2.registerNodeAction = NodeAction.Shutdown;
				myResourceTracker2.shutDownMessage = "RM Shutting Down Node";
				nodeStatusUpdater.resourceTracker = myResourceTracker2;
				return nodeStatusUpdater;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNMConnectionToRM()
		{
			long delta = 50000;
			long connectionWaitMs = 5000;
			long connectionRetryIntervalMs = 1000;
			//Waiting for rmStartIntervalMS, RM will be started
			long rmStartIntervalMS = 2 * 1000;
			conf.SetLong(YarnConfiguration.ResourcemanagerConnectMaxWaitMs, connectionWaitMs);
			conf.SetLong(YarnConfiguration.ResourcemanagerConnectRetryIntervalMs, connectionRetryIntervalMs
				);
			//Test NM try to connect to RM Several times, but finally fail
			TestNodeStatusUpdater.NodeManagerWithCustomNodeStatusUpdater nmWithUpdater;
			nm = nmWithUpdater = new _NodeManagerWithCustomNodeStatusUpdater_1238(rmStartIntervalMS
				);
			nm.Init(conf);
			long waitStartTime = Runtime.CurrentTimeMillis();
			try
			{
				nm.Start();
				NUnit.Framework.Assert.Fail("NM should have failed to start due to RM connect failure"
					);
			}
			catch (Exception e)
			{
				long t = Runtime.CurrentTimeMillis();
				long duration = t - waitStartTime;
				bool waitTimeValid = (duration >= connectionWaitMs) && (duration < (connectionWaitMs
					 + delta));
				if (!waitTimeValid)
				{
					//either the exception was too early, or it had a different cause.
					//reject with the inner stack trace
					throw new Exception("NM should have tried re-connecting to RM during " + "period of at least "
						 + connectionWaitMs + " ms, but " + "stopped retrying within " + (connectionWaitMs
						 + delta) + " ms: " + e, e);
				}
			}
			//Test NM connect to RM, fail at first several attempts,
			//but finally success.
			nm = nmWithUpdater = new _NodeManagerWithCustomNodeStatusUpdater_1270(rmStartIntervalMS
				);
			nm.Init(conf);
			NodeStatusUpdater updater = nmWithUpdater.GetUpdater();
			NUnit.Framework.Assert.IsNotNull("Updater not yet created ", updater);
			waitStartTime = Runtime.CurrentTimeMillis();
			try
			{
				nm.Start();
			}
			catch (Exception ex)
			{
				Log.Error("NM should have started successfully " + "after connecting to RM.", ex);
				throw;
			}
			long duration_1 = Runtime.CurrentTimeMillis() - waitStartTime;
			TestNodeStatusUpdater.MyNodeStatusUpdater4 myUpdater = (TestNodeStatusUpdater.MyNodeStatusUpdater4
				)updater;
			NUnit.Framework.Assert.IsTrue("NM started before updater triggered", myUpdater.IsTriggered
				());
			NUnit.Framework.Assert.IsTrue("NM should have connected to RM after " + "the start interval of "
				 + rmStartIntervalMS + ": actual " + duration_1 + " " + myUpdater, (duration_1 >=
				 rmStartIntervalMS));
			NUnit.Framework.Assert.IsTrue("NM should have connected to RM less than " + (rmStartIntervalMS
				 + delta) + " milliseconds of RM starting up: actual " + duration_1 + " " + myUpdater
				, (duration_1 < (rmStartIntervalMS + delta)));
		}

		private sealed class _NodeManagerWithCustomNodeStatusUpdater_1238 : TestNodeStatusUpdater.NodeManagerWithCustomNodeStatusUpdater
		{
			public _NodeManagerWithCustomNodeStatusUpdater_1238(long rmStartIntervalMS)
			{
				this.rmStartIntervalMS = rmStartIntervalMS;
			}

			internal override NodeStatusUpdater CreateUpdater(Context context, Dispatcher dispatcher
				, NodeHealthCheckerService healthChecker)
			{
				NodeStatusUpdater nodeStatusUpdater = new TestNodeStatusUpdater.MyNodeStatusUpdater4
					(this, context, dispatcher, healthChecker, this.metrics, rmStartIntervalMS, true
					);
				return nodeStatusUpdater;
			}

			private readonly long rmStartIntervalMS;
		}

		private sealed class _NodeManagerWithCustomNodeStatusUpdater_1270 : TestNodeStatusUpdater.NodeManagerWithCustomNodeStatusUpdater
		{
			public _NodeManagerWithCustomNodeStatusUpdater_1270(long rmStartIntervalMS)
			{
				this.rmStartIntervalMS = rmStartIntervalMS;
			}

			internal override NodeStatusUpdater CreateUpdater(Context context, Dispatcher dispatcher
				, NodeHealthCheckerService healthChecker)
			{
				NodeStatusUpdater nodeStatusUpdater = new TestNodeStatusUpdater.MyNodeStatusUpdater4
					(this, context, dispatcher, healthChecker, this.metrics, rmStartIntervalMS, false
					);
				return nodeStatusUpdater;
			}

			private readonly long rmStartIntervalMS;
		}

		/// <summary>
		/// Verifies that if for some reason NM fails to start ContainerManager RPC
		/// server, RM is oblivious to NM's presence.
		/// </summary>
		/// <remarks>
		/// Verifies that if for some reason NM fails to start ContainerManager RPC
		/// server, RM is oblivious to NM's presence. The behaviour is like this
		/// because otherwise, NM will report to RM even if all its servers are not
		/// started properly, RM will think that the NM is alive and will retire the NM
		/// only after NM_EXPIRY interval. See MAPREDUCE-2749.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoRegistrationWhenNMServicesFail()
		{
			nm = new _NodeManager_1317();
			// Simulating failure of starting RPC server
			VerifyNodeStartFailure("Starting of RPC Server failed");
		}

		private sealed class _NodeManager_1317 : NodeManager
		{
			public _NodeManager_1317()
			{
			}

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				return new TestNodeStatusUpdater.MyNodeStatusUpdater(this, context, dispatcher, healthChecker
					, this.metrics);
			}

			protected internal override ContainerManagerImpl CreateContainerManager(Context context
				, ContainerExecutor exec, DeletionService del, NodeStatusUpdater nodeStatusUpdater
				, ApplicationACLsManager aclsManager, LocalDirsHandlerService diskhandler)
			{
				return new _ContainerManagerImpl_1332(context, exec, del, nodeStatusUpdater, this
					.metrics, aclsManager, diskhandler);
			}

			private sealed class _ContainerManagerImpl_1332 : ContainerManagerImpl
			{
				public _ContainerManagerImpl_1332(Context baseArg1, ContainerExecutor baseArg2, DeletionService
					 baseArg3, NodeStatusUpdater baseArg4, NodeManagerMetrics baseArg5, ApplicationACLsManager
					 baseArg6, LocalDirsHandlerService baseArg7)
					: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
				{
				}

				protected override void ServiceStart()
				{
					throw new YarnRuntimeException("Starting of RPC Server failed");
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationKeepAlive()
		{
			TestNodeStatusUpdater.MyNodeManager nm = new TestNodeStatusUpdater.MyNodeManager(
				this);
			try
			{
				YarnConfiguration conf = CreateNMConfig();
				conf.SetBoolean(YarnConfiguration.LogAggregationEnabled, true);
				conf.SetLong(YarnConfiguration.RmNmExpiryIntervalMs, 4000l);
				nm.Init(conf);
				nm.Start();
				// HB 2 -> app cancelled by RM.
				while (heartBeatID < 12)
				{
					Sharpen.Thread.Sleep(1000l);
				}
				TestNodeStatusUpdater.MyResourceTracker3 rt = (TestNodeStatusUpdater.MyResourceTracker3
					)((TestNodeStatusUpdater.MyNodeStatusUpdater3)nm.GetNodeStatusUpdater()).GetRMClient
					();
				Sharpen.Collections.Remove(rt.context.GetApplications(), rt.appId);
				NUnit.Framework.Assert.AreEqual(1, rt.keepAliveRequests.Count);
				int numKeepAliveRequests = rt.keepAliveRequests[rt.appId].Count;
				Log.Info("Number of Keep Alive Requests: [" + numKeepAliveRequests + "]");
				NUnit.Framework.Assert.IsTrue(numKeepAliveRequests == 2 || numKeepAliveRequests ==
					 3);
				while (heartBeatID < 20)
				{
					Sharpen.Thread.Sleep(1000l);
				}
				int numKeepAliveRequests2 = rt.keepAliveRequests[rt.appId].Count;
				NUnit.Framework.Assert.AreEqual(numKeepAliveRequests, numKeepAliveRequests2);
			}
			finally
			{
				if (nm.GetServiceState() == Service.STATE.Started)
				{
					nm.Stop();
				}
			}
		}

		/// <summary>
		/// Test completed containerStatus get back up when heart beat lost, and will
		/// be sent via next heart beat.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCompletedContainerStatusBackup()
		{
			nm = new _NodeManager_1383();
			YarnConfiguration conf = CreateNMConfig();
			nm.Init(conf);
			nm.Start();
			int waitCount = 0;
			while (heartBeatID <= 4 && waitCount++ != 20)
			{
				Sharpen.Thread.Sleep(500);
			}
			if (heartBeatID <= 4)
			{
				NUnit.Framework.Assert.Fail("Failed to get all heartbeats in time, " + "heartbeatID:"
					 + heartBeatID);
			}
			if (assertionFailedInThread.Get())
			{
				NUnit.Framework.Assert.Fail("ContainerStatus Backup failed");
			}
			NUnit.Framework.Assert.IsNotNull(nm.GetNMContext().GetSystemCredentialsForApps()[
				ApplicationId.NewInstance(1234, 1)].GetToken(new Text("token1")));
			nm.Stop();
		}

		private sealed class _NodeManager_1383 : NodeManager
		{
			public _NodeManager_1383()
			{
			}

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				TestNodeStatusUpdater.MyNodeStatusUpdater2 myNodeStatusUpdater = new TestNodeStatusUpdater.MyNodeStatusUpdater2
					(this, context, dispatcher, healthChecker, this.metrics);
				return myNodeStatusUpdater;
			}

			protected internal override NodeManager.NMContext CreateNMContext(NMContainerTokenSecretManager
				 containerTokenSecretManager, NMTokenSecretManagerInNM nmTokenSecretManager, NMStateStoreService
				 store)
			{
				return new TestNodeStatusUpdater.MyNMContext(this, containerTokenSecretManager, nmTokenSecretManager
					);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNodeStatusUpdaterRetryAndNMShutdown()
		{
			long connectionWaitSecs = 1000;
			long connectionRetryIntervalMs = 1000;
			YarnConfiguration conf = CreateNMConfig();
			conf.SetLong(YarnConfiguration.ResourcemanagerConnectMaxWaitMs, connectionWaitSecs
				);
			conf.SetLong(YarnConfiguration.ResourcemanagerConnectRetryIntervalMs, connectionRetryIntervalMs
				);
			conf.SetLong(YarnConfiguration.NmSleepDelayBeforeSigkillMs, 5000);
			conf.SetLong(YarnConfiguration.NmLogRetainSeconds, 1);
			CyclicBarrier syncBarrier = new CyclicBarrier(2);
			nm = new TestNodeStatusUpdater.MyNodeManager2(this, syncBarrier, conf);
			nm.Init(conf);
			nm.Start();
			// start a container
			ContainerId cId = TestNodeManagerShutdown.CreateContainerId();
			FileContext localFS = FileContext.GetLocalFSFileContext();
			TestNodeManagerShutdown.StartContainer(nm, cId, localFS, nmLocalDir, new FilePath
				("start_file.txt"));
			try
			{
				syncBarrier.Await(10000, TimeUnit.Milliseconds);
			}
			catch (Exception)
			{
			}
			NUnit.Framework.Assert.IsFalse("Containers not cleaned up when NM stopped", assertionFailedInThread
				.Get());
			NUnit.Framework.Assert.IsTrue(((TestNodeStatusUpdater.MyNodeManager2)nm).isStopped
				);
			NUnit.Framework.Assert.IsTrue("calculate heartBeatCount based on" + " connectionWaitSecs and RetryIntervalSecs"
				, heartBeatID == 2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMVersionLessThanMinimum()
		{
			AtomicInteger numCleanups = new AtomicInteger(0);
			YarnConfiguration conf = CreateNMConfig();
			conf.Set(YarnConfiguration.NmResourcemanagerMinimumVersion, "3.0.0");
			nm = new _NodeManager_1462(numCleanups);
			nm.Init(conf);
			nm.Start();
			// NM takes a while to reach the STARTED state.
			int waitCount = 0;
			while (nm.GetServiceState() != Service.STATE.Started && waitCount++ != 20)
			{
				Log.Info("Waiting for NM to stop..");
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.IsTrue(nm.GetServiceState() == Service.STATE.Started);
			nm.Stop();
		}

		private sealed class _NodeManager_1462 : NodeManager
		{
			public _NodeManager_1462(AtomicInteger numCleanups)
			{
				this.numCleanups = numCleanups;
			}

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				TestNodeStatusUpdater.MyNodeStatusUpdater myNodeStatusUpdater = new TestNodeStatusUpdater.MyNodeStatusUpdater
					(this, context, dispatcher, healthChecker, this.metrics);
				TestNodeStatusUpdater.MyResourceTracker2 myResourceTracker2 = new TestNodeStatusUpdater.MyResourceTracker2
					(this);
				myResourceTracker2.heartBeatNodeAction = NodeAction.Normal;
				myResourceTracker2.rmVersion = "3.0.0";
				myNodeStatusUpdater.resourceTracker = myResourceTracker2;
				return myNodeStatusUpdater;
			}

			protected internal override ContainerManagerImpl CreateContainerManager(Context context
				, ContainerExecutor exec, DeletionService del, NodeStatusUpdater nodeStatusUpdater
				, ApplicationACLsManager aclsManager, LocalDirsHandlerService dirsHandler)
			{
				return new _ContainerManagerImpl_1482(numCleanups, context, exec, del, nodeStatusUpdater
					, this.metrics, aclsManager, dirsHandler);
			}

			private sealed class _ContainerManagerImpl_1482 : ContainerManagerImpl
			{
				public _ContainerManagerImpl_1482(AtomicInteger numCleanups, Context baseArg1, ContainerExecutor
					 baseArg2, DeletionService baseArg3, NodeStatusUpdater baseArg4, NodeManagerMetrics
					 baseArg5, ApplicationACLsManager baseArg6, LocalDirsHandlerService baseArg7)
					: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
				{
					this.numCleanups = numCleanups;
				}

				public override void CleanUpApplicationsOnNMShutDown()
				{
					base.CleanUpApplicationsOnNMShutDown();
					numCleanups.IncrementAndGet();
				}

				private readonly AtomicInteger numCleanups;
			}

			private readonly AtomicInteger numCleanups;
		}

		[NUnit.Framework.Test]
		public virtual void TestConcurrentAccessToSystemCredentials()
		{
			IDictionary<ApplicationId, ByteBuffer> testCredentials = new Dictionary<ApplicationId
				, ByteBuffer>();
			ByteBuffer byteBuffer = ByteBuffer.Wrap(new byte[300]);
			ApplicationId applicationId = ApplicationId.NewInstance(123456, 120);
			testCredentials[applicationId] = byteBuffer;
			IList<Exception> exceptions = Sharpen.Collections.SynchronizedList(new AList<Exception
				>());
			int NumThreads = 10;
			CountDownLatch allDone = new CountDownLatch(NumThreads);
			ExecutorService threadPool = Executors.NewFixedThreadPool(NumThreads);
			AtomicBoolean stop = new AtomicBoolean(false);
			try
			{
				for (int i = 0; i < NumThreads; i++)
				{
					threadPool.Submit(new _Runnable_1525(stop, testCredentials, exceptions, allDone));
				}
				int testTimeout = 2;
				NUnit.Framework.Assert.IsTrue("Timeout waiting for more than " + testTimeout + " "
					 + "seconds", allDone.Await(testTimeout, TimeUnit.Seconds));
			}
			catch (Exception ie)
			{
				exceptions.AddItem(ie);
			}
			finally
			{
				threadPool.ShutdownNow();
			}
			NUnit.Framework.Assert.IsTrue("Test failed with exception(s)" + exceptions, exceptions
				.IsEmpty());
		}

		private sealed class _Runnable_1525 : Runnable
		{
			public _Runnable_1525(AtomicBoolean stop, IDictionary<ApplicationId, ByteBuffer> 
				testCredentials, IList<Exception> exceptions, CountDownLatch allDone)
			{
				this.stop = stop;
				this.testCredentials = testCredentials;
				this.exceptions = exceptions;
				this.allDone = allDone;
			}

			public void Run()
			{
				try
				{
					for (int i = 0; i < 100 && !stop.Get(); i++)
					{
						NodeHeartbeatResponse nodeHeartBeatResponse = YarnServerBuilderUtils.NewNodeHeartbeatResponse
							(0, NodeAction.Normal, null, null, null, null, 0);
						nodeHeartBeatResponse.SetSystemCredentialsForApps(testCredentials);
						YarnServerCommonServiceProtos.NodeHeartbeatResponseProto proto = ((NodeHeartbeatResponsePBImpl
							)nodeHeartBeatResponse).GetProto();
						NUnit.Framework.Assert.IsNotNull(proto);
					}
				}
				catch (Exception t)
				{
					exceptions.AddItem(t);
					stop.Set(true);
				}
				finally
				{
					allDone.CountDown();
				}
			}

			private readonly AtomicBoolean stop;

			private readonly IDictionary<ApplicationId, ByteBuffer> testCredentials;

			private readonly IList<Exception> exceptions;

			private readonly CountDownLatch allDone;
		}

		private class MyNMContext : NodeManager.NMContext
		{
			public MyNMContext(TestNodeStatusUpdater _enclosing, NMContainerTokenSecretManager
				 containerTokenSecretManager, NMTokenSecretManagerInNM nmTokenSecretManager)
				: base(containerTokenSecretManager, nmTokenSecretManager, null, null, new NMNullStateStoreService
					())
			{
				this._enclosing = _enclosing;
			}

			// Add new containers info into NM context each time node heart beats.
			public override ConcurrentMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				> GetContainers()
			{
				if (this._enclosing.heartBeatID == 0)
				{
					return this.containers;
				}
				else
				{
					if (this._enclosing.heartBeatID == 1)
					{
						ContainerStatus containerStatus2 = TestNodeStatusUpdater.CreateContainerStatus(2, 
							ContainerState.Running);
						this.PutMockContainer(containerStatus2);
						ContainerStatus containerStatus3 = TestNodeStatusUpdater.CreateContainerStatus(3, 
							ContainerState.Complete);
						this.PutMockContainer(containerStatus3);
						return this.containers;
					}
					else
					{
						if (this._enclosing.heartBeatID == 2)
						{
							ContainerStatus containerStatus4 = TestNodeStatusUpdater.CreateContainerStatus(4, 
								ContainerState.Running);
							this.PutMockContainer(containerStatus4);
							ContainerStatus containerStatus5 = TestNodeStatusUpdater.CreateContainerStatus(5, 
								ContainerState.Complete);
							this.PutMockContainer(containerStatus5);
							return this.containers;
						}
						else
						{
							if (this._enclosing.heartBeatID == 3 || this._enclosing.heartBeatID == 4)
							{
								return this.containers;
							}
							else
							{
								this.containers.Clear();
								return this.containers;
							}
						}
					}
				}
			}

			private void PutMockContainer(ContainerStatus containerStatus)
			{
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
					 = TestNodeStatusUpdater.GetMockContainer(containerStatus);
				this.containers[containerStatus.GetContainerId()] = container;
				this.applications.PutIfAbsent(containerStatus.GetContainerId().GetApplicationAttemptId
					().GetApplicationId(), Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>());
			}

			private readonly TestNodeStatusUpdater _enclosing;
		}

		public static ContainerStatus CreateContainerStatus(int id, ContainerState containerState
			)
		{
			ApplicationId applicationId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 1);
			ContainerId contaierId = ContainerId.NewContainerId(applicationAttemptId, id);
			ContainerStatus containerStatus = BuilderUtils.NewContainerStatus(contaierId, containerState
				, "test_containerStatus: id=" + id + ", containerState: " + containerState, 0);
			return containerStatus;
		}

		public static Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 GetMockContainer(ContainerStatus containerStatus)
		{
			ContainerImpl container = Org.Mockito.Mockito.Mock<ContainerImpl>();
			Org.Mockito.Mockito.When(container.CloneAndGetContainerStatus()).ThenReturn(containerStatus
				);
			Org.Mockito.Mockito.When(container.GetCurrentState()).ThenReturn(containerStatus.
				GetState());
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(containerStatus.GetContainerId
				());
			if (containerStatus.GetState().Equals(ContainerState.Complete))
			{
				Org.Mockito.Mockito.When(container.GetContainerState()).ThenReturn(ContainerState
					.Done);
			}
			else
			{
				if (containerStatus.GetState().Equals(ContainerState.Running))
				{
					Org.Mockito.Mockito.When(container.GetContainerState()).ThenReturn(ContainerState
						.Running);
				}
			}
			return container;
		}

		/// <exception cref="System.Exception"/>
		private void VerifyNodeStartFailure(string errMessage)
		{
			NUnit.Framework.Assert.IsNotNull("nm is null", nm);
			YarnConfiguration conf = CreateNMConfig();
			nm.Init(conf);
			try
			{
				nm.Start();
				NUnit.Framework.Assert.Fail("NM should have failed to start. Didn't get exception!!"
					);
			}
			catch (Exception e)
			{
				//the version in trunk looked in the cause for equality
				// and assumed failures were nested.
				//this version assumes that error strings propagate to the base and
				//use a contains() test only. It should be less brittle
				if (!e.Message.Contains(errMessage))
				{
					throw;
				}
			}
			// the service should be stopped
			NUnit.Framework.Assert.AreEqual("NM state is wrong!", Service.STATE.Stopped, nm.GetServiceState
				());
			NUnit.Framework.Assert.AreEqual("Number of registered nodes is wrong!", 0, this.registeredNodes
				.Count);
		}

		private YarnConfiguration CreateNMConfig()
		{
			YarnConfiguration conf = new YarnConfiguration();
			string localhostAddress = null;
			try
			{
				localhostAddress = Sharpen.Extensions.GetAddressByName("localhost").ToString();
			}
			catch (UnknownHostException e)
			{
				NUnit.Framework.Assert.Fail("Unable to get localhost address: " + e.Message);
			}
			conf.SetInt(YarnConfiguration.NmPmemMb, 5 * 1024);
			// 5GB
			conf.Set(YarnConfiguration.NmAddress, localhostAddress + ":12345");
			conf.Set(YarnConfiguration.NmLocalizerAddress, localhostAddress + ":12346");
			conf.Set(YarnConfiguration.NmLogDirs, logsDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmRemoteAppLogDir, remoteLogsDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmLocalDirs, nmLocalDir.GetAbsolutePath());
			conf.SetLong(YarnConfiguration.NmLogRetainSeconds, 1);
			return conf;
		}

		private NodeManager GetNodeManager(NodeAction nodeHeartBeatAction)
		{
			return new _NodeManager_1686(nodeHeartBeatAction);
		}

		private sealed class _NodeManager_1686 : NodeManager
		{
			public _NodeManager_1686(NodeAction nodeHeartBeatAction)
			{
				this.nodeHeartBeatAction = nodeHeartBeatAction;
			}

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				TestNodeStatusUpdater.MyNodeStatusUpdater myNodeStatusUpdater = new TestNodeStatusUpdater.MyNodeStatusUpdater
					(this, context, dispatcher, healthChecker, this.metrics);
				TestNodeStatusUpdater.MyResourceTracker2 myResourceTracker2 = new TestNodeStatusUpdater.MyResourceTracker2
					(this);
				myResourceTracker2.heartBeatNodeAction = nodeHeartBeatAction;
				myNodeStatusUpdater.resourceTracker = myResourceTracker2;
				return myNodeStatusUpdater;
			}

			private readonly NodeAction nodeHeartBeatAction;
		}
	}
}
