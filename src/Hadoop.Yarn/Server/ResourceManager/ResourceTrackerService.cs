using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class ResourceTrackerService : AbstractService, ResourceTracker
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.ResourceTrackerService
			));

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private readonly RMContext rmContext;

		private readonly NodesListManager nodesListManager;

		private readonly NMLivelinessMonitor nmLivelinessMonitor;

		private readonly RMContainerTokenSecretManager containerTokenSecretManager;

		private readonly NMTokenSecretManagerInRM nmTokenSecretManager;

		private long nextHeartBeatInterval;

		private Org.Apache.Hadoop.Ipc.Server server;

		private IPEndPoint resourceTrackerAddress;

		private string minimumNodeManagerVersion;

		private static readonly NodeHeartbeatResponse resync = recordFactory.NewRecordInstance
			<NodeHeartbeatResponse>();

		private static readonly NodeHeartbeatResponse shutDown = recordFactory.NewRecordInstance
			<NodeHeartbeatResponse>();

		private int minAllocMb;

		private int minAllocVcores;

		static ResourceTrackerService()
		{
			resync.SetNodeAction(NodeAction.Resync);
			shutDown.SetNodeAction(NodeAction.Shutdown);
		}

		public ResourceTrackerService(RMContext rmContext, NodesListManager nodesListManager
			, NMLivelinessMonitor nmLivelinessMonitor, RMContainerTokenSecretManager containerTokenSecretManager
			, NMTokenSecretManagerInRM nmTokenSecretManager)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.ResourceTrackerService
				).FullName)
		{
			this.rmContext = rmContext;
			this.nodesListManager = nodesListManager;
			this.nmLivelinessMonitor = nmLivelinessMonitor;
			this.containerTokenSecretManager = containerTokenSecretManager;
			this.nmTokenSecretManager = nmTokenSecretManager;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			resourceTrackerAddress = conf.GetSocketAddr(YarnConfiguration.RmBindHost, YarnConfiguration
				.RmResourceTrackerAddress, YarnConfiguration.DefaultRmResourceTrackerAddress, YarnConfiguration
				.DefaultRmResourceTrackerPort);
			RackResolver.Init(conf);
			nextHeartBeatInterval = conf.GetLong(YarnConfiguration.RmNmHeartbeatIntervalMs, YarnConfiguration
				.DefaultRmNmHeartbeatIntervalMs);
			if (nextHeartBeatInterval <= 0)
			{
				throw new YarnRuntimeException("Invalid Configuration. " + YarnConfiguration.RmNmHeartbeatIntervalMs
					 + " should be larger than 0.");
			}
			minAllocMb = conf.GetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationMb);
			minAllocVcores = conf.GetInt(YarnConfiguration.RmSchedulerMinimumAllocationVcores
				, YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
			minimumNodeManagerVersion = conf.Get(YarnConfiguration.RmNodemanagerMinimumVersion
				, YarnConfiguration.DefaultRmNodemanagerMinimumVersion);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			base.ServiceStart();
			// ResourceTrackerServer authenticates NodeManager via Kerberos if
			// security is enabled, so no secretManager.
			Configuration conf = GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			this.server = rpc.GetServer(typeof(ResourceTracker), this, resourceTrackerAddress
				, conf, null, conf.GetInt(YarnConfiguration.RmResourceTrackerClientThreadCount, 
				YarnConfiguration.DefaultRmResourceTrackerClientThreadCount));
			// Enable service authorization?
			if (conf.GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, false
				))
			{
				InputStream inputStream = this.rmContext.GetConfigurationProvider().GetConfigurationInputStream
					(conf, YarnConfiguration.HadoopPolicyConfigurationFile);
				if (inputStream != null)
				{
					conf.AddResource(inputStream);
				}
				RefreshServiceAcls(conf, RMPolicyProvider.GetInstance());
			}
			this.server.Start();
			conf.UpdateConnectAddr(YarnConfiguration.RmBindHost, YarnConfiguration.RmResourceTrackerAddress
				, YarnConfiguration.DefaultRmResourceTrackerAddress, server.GetListenerAddress()
				);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (this.server != null)
			{
				this.server.Stop();
			}
			base.ServiceStop();
		}

		/// <summary>Helper method to handle received ContainerStatus.</summary>
		/// <remarks>
		/// Helper method to handle received ContainerStatus. If this corresponds to
		/// the completion of a master-container of a managed AM,
		/// we call the handler for RMAppAttemptContainerFinishedEvent.
		/// </remarks>
		[VisibleForTesting]
		internal virtual void HandleNMContainerStatus(NMContainerStatus containerStatus, 
			NodeId nodeId)
		{
			ApplicationAttemptId appAttemptId = containerStatus.GetContainerId().GetApplicationAttemptId
				();
			RMApp rmApp = rmContext.GetRMApps()[appAttemptId.GetApplicationId()];
			if (rmApp == null)
			{
				Log.Error("Received finished container : " + containerStatus.GetContainerId() + " for unknown application "
					 + appAttemptId.GetApplicationId() + " Skipping.");
				return;
			}
			if (rmApp.GetApplicationSubmissionContext().GetUnmanagedAM())
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Ignoring container completion status for unmanaged AM " + rmApp.GetApplicationId
						());
				}
				return;
			}
			RMAppAttempt rmAppAttempt = rmApp.GetRMAppAttempt(appAttemptId);
			Container masterContainer = rmAppAttempt.GetMasterContainer();
			if (masterContainer.GetId().Equals(containerStatus.GetContainerId()) && containerStatus
				.GetContainerState() == ContainerState.Complete)
			{
				ContainerStatus status = ContainerStatus.NewInstance(containerStatus.GetContainerId
					(), containerStatus.GetContainerState(), containerStatus.GetDiagnostics(), containerStatus
					.GetContainerExitStatus());
				// sending master container finished event.
				RMAppAttemptContainerFinishedEvent evt = new RMAppAttemptContainerFinishedEvent(appAttemptId
					, status, nodeId);
				rmContext.GetDispatcher().GetEventHandler().Handle(evt);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
			 request)
		{
			NodeId nodeId = request.GetNodeId();
			string host = nodeId.GetHost();
			int cmPort = nodeId.GetPort();
			int httpPort = request.GetHttpPort();
			Resource capability = request.GetResource();
			string nodeManagerVersion = request.GetNMVersion();
			RegisterNodeManagerResponse response = recordFactory.NewRecordInstance<RegisterNodeManagerResponse
				>();
			if (!minimumNodeManagerVersion.Equals("NONE"))
			{
				if (minimumNodeManagerVersion.Equals("EqualToRM"))
				{
					minimumNodeManagerVersion = YarnVersionInfo.GetVersion();
				}
				if ((nodeManagerVersion == null) || (VersionUtil.CompareVersions(nodeManagerVersion
					, minimumNodeManagerVersion)) < 0)
				{
					string message = "Disallowed NodeManager Version " + nodeManagerVersion + ", is less than the minimum version "
						 + minimumNodeManagerVersion + " sending SHUTDOWN signal to " + "NodeManager.";
					Log.Info(message);
					response.SetDiagnosticsMessage(message);
					response.SetNodeAction(NodeAction.Shutdown);
					return response;
				}
			}
			// Check if this node is a 'valid' node
			if (!this.nodesListManager.IsValidNode(host))
			{
				string message = "Disallowed NodeManager from  " + host + ", Sending SHUTDOWN signal to the NodeManager.";
				Log.Info(message);
				response.SetDiagnosticsMessage(message);
				response.SetNodeAction(NodeAction.Shutdown);
				return response;
			}
			// Check if this node has minimum allocations
			if (capability.GetMemory() < minAllocMb || capability.GetVirtualCores() < minAllocVcores)
			{
				string message = "NodeManager from  " + host + " doesn't satisfy minimum allocations, Sending SHUTDOWN"
					 + " signal to the NodeManager.";
				Log.Info(message);
				response.SetDiagnosticsMessage(message);
				response.SetNodeAction(NodeAction.Shutdown);
				return response;
			}
			response.SetContainerTokenMasterKey(containerTokenSecretManager.GetCurrentKey());
			response.SetNMTokenMasterKey(nmTokenSecretManager.GetCurrentKey());
			RMNode rmNode = new RMNodeImpl(nodeId, rmContext, host, cmPort, httpPort, Resolve
				(host), capability, nodeManagerVersion);
			RMNode oldNode = this.rmContext.GetRMNodes().PutIfAbsent(nodeId, rmNode);
			if (oldNode == null)
			{
				this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMNodeStartedEvent(nodeId
					, request.GetNMContainerStatuses(), request.GetRunningApplications()));
			}
			else
			{
				Log.Info("Reconnect from the node at: " + host);
				this.nmLivelinessMonitor.Unregister(nodeId);
				// Reset heartbeat ID since node just restarted.
				oldNode.ResetLastNodeHeartBeatResponse();
				this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMNodeReconnectEvent(
					nodeId, rmNode, request.GetRunningApplications(), request.GetNMContainerStatuses
					()));
			}
			// On every node manager register we will be clearing NMToken keys if
			// present for any running application.
			this.nmTokenSecretManager.RemoveNodeKey(nodeId);
			this.nmLivelinessMonitor.Register(nodeId);
			// Handle received container status, this should be processed after new
			// RMNode inserted
			if (!rmContext.IsWorkPreservingRecoveryEnabled())
			{
				if (!request.GetNMContainerStatuses().IsEmpty())
				{
					Log.Info("received container statuses on node manager register :" + request.GetNMContainerStatuses
						());
					foreach (NMContainerStatus status in request.GetNMContainerStatuses())
					{
						HandleNMContainerStatus(status, nodeId);
					}
				}
			}
			string message_1 = "NodeManager from node " + host + "(cmPort: " + cmPort + " httpPort: "
				 + httpPort + ") " + "registered with capability: " + capability + ", assigned nodeId "
				 + nodeId;
			Log.Info(message_1);
			response.SetNodeAction(NodeAction.Normal);
			response.SetRMIdentifier(ResourceManager.GetClusterTimeStamp());
			response.SetRMVersion(YarnVersionInfo.GetVersion());
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
		{
			NodeStatus remoteNodeStatus = request.GetNodeStatus();
			NodeId nodeId = remoteNodeStatus.GetNodeId();
			// 1. Check if it's a valid (i.e. not excluded) node
			if (!this.nodesListManager.IsValidNode(nodeId.GetHost()))
			{
				string message = "Disallowed NodeManager nodeId: " + nodeId + " hostname: " + nodeId
					.GetHost();
				Log.Info(message);
				shutDown.SetDiagnosticsMessage(message);
				return shutDown;
			}
			// 2. Check if it's a registered node
			RMNode rmNode = this.rmContext.GetRMNodes()[nodeId];
			if (rmNode == null)
			{
				/* node does not exist */
				string message = "Node not found resyncing " + remoteNodeStatus.GetNodeId();
				Log.Info(message);
				resync.SetDiagnosticsMessage(message);
				return resync;
			}
			// Send ping
			this.nmLivelinessMonitor.ReceivedPing(nodeId);
			// 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
			NodeHeartbeatResponse lastNodeHeartbeatResponse = rmNode.GetLastNodeHeartBeatResponse
				();
			if (remoteNodeStatus.GetResponseId() + 1 == lastNodeHeartbeatResponse.GetResponseId
				())
			{
				Log.Info("Received duplicate heartbeat from node " + rmNode.GetNodeAddress() + " responseId="
					 + remoteNodeStatus.GetResponseId());
				return lastNodeHeartbeatResponse;
			}
			else
			{
				if (remoteNodeStatus.GetResponseId() + 1 < lastNodeHeartbeatResponse.GetResponseId
					())
				{
					string message = "Too far behind rm response id:" + lastNodeHeartbeatResponse.GetResponseId
						() + " nm response id:" + remoteNodeStatus.GetResponseId();
					Log.Info(message);
					resync.SetDiagnosticsMessage(message);
					// TODO: Just sending reboot is not enough. Think more.
					this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMNodeEvent(nodeId, RMNodeEventType
						.Rebooting));
					return resync;
				}
			}
			// Heartbeat response
			NodeHeartbeatResponse nodeHeartBeatResponse = YarnServerBuilderUtils.NewNodeHeartbeatResponse
				(lastNodeHeartbeatResponse.GetResponseId() + 1, NodeAction.Normal, null, null, null
				, null, nextHeartBeatInterval);
			rmNode.UpdateNodeHeartbeatResponseForCleanup(nodeHeartBeatResponse);
			PopulateKeys(request, nodeHeartBeatResponse);
			ConcurrentMap<ApplicationId, ByteBuffer> systemCredentials = rmContext.GetSystemCredentialsForApps
				();
			if (!systemCredentials.IsEmpty())
			{
				nodeHeartBeatResponse.SetSystemCredentialsForApps(systemCredentials);
			}
			// 4. Send status to RMNode, saving the latest response.
			this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMNodeStatusEvent(nodeId
				, remoteNodeStatus.GetNodeHealthStatus(), remoteNodeStatus.GetContainersStatuses
				(), remoteNodeStatus.GetKeepAliveApplications(), nodeHeartBeatResponse));
			return nodeHeartBeatResponse;
		}

		private void PopulateKeys(NodeHeartbeatRequest request, NodeHeartbeatResponse nodeHeartBeatResponse
			)
		{
			// Check if node's masterKey needs to be updated and if the currentKey has
			// roller over, send it across
			// ContainerTokenMasterKey
			MasterKey nextMasterKeyForNode = this.containerTokenSecretManager.GetNextKey();
			if (nextMasterKeyForNode != null && (request.GetLastKnownContainerTokenMasterKey(
				).GetKeyId() != nextMasterKeyForNode.GetKeyId()))
			{
				nodeHeartBeatResponse.SetContainerTokenMasterKey(nextMasterKeyForNode);
			}
			// NMTokenMasterKey
			nextMasterKeyForNode = this.nmTokenSecretManager.GetNextKey();
			if (nextMasterKeyForNode != null && (request.GetLastKnownNMTokenMasterKey().GetKeyId
				() != nextMasterKeyForNode.GetKeyId()))
			{
				nodeHeartBeatResponse.SetNMTokenMasterKey(nextMasterKeyForNode);
			}
		}

		/// <summary>resolving the network topology.</summary>
		/// <param name="hostName">the hostname of this node.</param>
		/// <returns>
		/// the resolved
		/// <see cref="Org.Apache.Hadoop.Net.Node"/>
		/// for this nodemanager.
		/// </returns>
		public static Node Resolve(string hostName)
		{
			return RackResolver.Resolve(hostName);
		}

		internal virtual void RefreshServiceAcls(Configuration configuration, PolicyProvider
			 policyProvider)
		{
			this.server.RefreshServiceAclWithLoadedConfiguration(configuration, policyProvider
				);
		}

		[VisibleForTesting]
		public virtual Org.Apache.Hadoop.Ipc.Server GetServer()
		{
			return this.server;
		}
	}
}
