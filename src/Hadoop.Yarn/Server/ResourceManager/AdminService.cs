using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.HA.Proto;
using Org.Apache.Hadoop.HA.ProtocolPB;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class AdminService : CompositeService, HAServiceProtocol, ResourceManagerAdministrationProtocol
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.AdminService
			));

		private readonly RMContext rmContext;

		private readonly ResourceManager rm;

		private string rmId;

		private bool autoFailoverEnabled;

		private EmbeddedElectorService embeddedElector;

		private RPC.Server server;

		private IPEndPoint masterServiceBindAddress;

		private YarnAuthorizationProvider authorizer;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private UserGroupInformation daemonUser;

		public AdminService(ResourceManager rm, RMContext rmContext)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.AdminService).FullName
				)
		{
			// Address to use for binding. May be a wildcard address.
			this.rm = rm;
			this.rmContext = rmContext;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			if (rmContext.IsHAEnabled())
			{
				autoFailoverEnabled = HAUtil.IsAutomaticFailoverEnabled(conf);
				if (autoFailoverEnabled)
				{
					if (HAUtil.IsAutomaticFailoverEmbedded(conf))
					{
						embeddedElector = CreateEmbeddedElectorService();
						AddIfService(embeddedElector);
					}
				}
			}
			masterServiceBindAddress = conf.GetSocketAddr(YarnConfiguration.RmBindHost, YarnConfiguration
				.RmAdminAddress, YarnConfiguration.DefaultRmAdminAddress, YarnConfiguration.DefaultRmAdminPort
				);
			daemonUser = UserGroupInformation.GetCurrentUser();
			authorizer = YarnAuthorizationProvider.GetInstance(conf);
			authorizer.SetAdmins(GetAdminAclList(conf), UserGroupInformation.GetCurrentUser()
				);
			rmId = conf.Get(YarnConfiguration.RmHaId);
			base.ServiceInit(conf);
		}

		private AccessControlList GetAdminAclList(Configuration conf)
		{
			AccessControlList aclList = new AccessControlList(conf.Get(YarnConfiguration.YarnAdminAcl
				, YarnConfiguration.DefaultYarnAdminAcl));
			aclList.AddUser(daemonUser.GetShortUserName());
			return aclList;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			StartServer();
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			StopServer();
			base.ServiceStop();
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void StartServer()
		{
			Configuration conf = GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			this.server = (RPC.Server)rpc.GetServer(typeof(ResourceManagerAdministrationProtocol
				), this, masterServiceBindAddress, conf, null, conf.GetInt(YarnConfiguration.RmAdminClientThreadCount
				, YarnConfiguration.DefaultRmAdminClientThreadCount));
			// Enable service authorization?
			if (conf.GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, false
				))
			{
				RefreshServiceAcls(GetConfiguration(conf, YarnConfiguration.HadoopPolicyConfigurationFile
					), RMPolicyProvider.GetInstance());
			}
			if (rmContext.IsHAEnabled())
			{
				RPC.SetProtocolEngine(conf, typeof(HAServiceProtocolPB), typeof(ProtobufRpcEngine
					));
				HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator = new HAServiceProtocolServerSideTranslatorPB
					(this);
				BlockingService haPbService = HAServiceProtocolProtos.HAServiceProtocolService.NewReflectiveBlockingService
					(haServiceProtocolXlator);
				server.AddProtocol(RPC.RpcKind.RpcProtocolBuffer, typeof(HAServiceProtocol), haPbService
					);
			}
			this.server.Start();
			conf.UpdateConnectAddr(YarnConfiguration.RmBindHost, YarnConfiguration.RmAdminAddress
				, YarnConfiguration.DefaultRmAdminAddress, server.GetListenerAddress());
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void StopServer()
		{
			if (this.server != null)
			{
				this.server.Stop();
			}
		}

		protected internal virtual EmbeddedElectorService CreateEmbeddedElectorService()
		{
			return new EmbeddedElectorService(rmContext);
		}

		[InterfaceAudience.Private]
		internal virtual void ResetLeaderElection()
		{
			if (embeddedElector != null)
			{
				embeddedElector.ResetLeaderElection();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private UserGroupInformation CheckAccess(string method)
		{
			return RMServerUtils.VerifyAdminAccess(authorizer, method, Log);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private UserGroupInformation CheckAcls(string method)
		{
			try
			{
				return CheckAccess(method);
			}
			catch (IOException ioe)
			{
				throw RPCUtil.GetRemoteException(ioe);
			}
		}

		/// <summary>Check that a request to change this node's HA state is valid.</summary>
		/// <remarks>
		/// Check that a request to change this node's HA state is valid.
		/// In particular, verifies that, if auto failover is enabled, non-forced
		/// requests from the HAAdmin CLI are rejected, and vice versa.
		/// </remarks>
		/// <param name="req">the request to check</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the request is disallowed
		/// 	</exception>
		private void CheckHaStateChange(HAServiceProtocol.StateChangeRequestInfo req)
		{
			switch (req.GetSource())
			{
				case HAServiceProtocol.RequestSource.RequestByUser:
				{
					if (autoFailoverEnabled)
					{
						throw new AccessControlException("Manual failover for this ResourceManager is disallowed, "
							 + "because automatic failover is enabled.");
					}
					break;
				}

				case HAServiceProtocol.RequestSource.RequestByUserForced:
				{
					if (autoFailoverEnabled)
					{
						Log.Warn("Allowing manual failover from " + Org.Apache.Hadoop.Ipc.Server.GetRemoteAddress
							() + " even though automatic failover is enabled, because the user " + "specified the force flag"
							);
					}
					break;
				}

				case HAServiceProtocol.RequestSource.RequestByZkfc:
				{
					if (!autoFailoverEnabled)
					{
						throw new AccessControlException("Request from ZK failover controller at " + Org.Apache.Hadoop.Ipc.Server
							.GetRemoteAddress() + " denied " + "since automatic failover is not enabled");
					}
					break;
				}
			}
		}

		private bool IsRMActive()
		{
			lock (this)
			{
				return HAServiceProtocol.HAServiceState.Active == rmContext.GetHAServiceState();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		private void ThrowStandbyException()
		{
			throw new StandbyException("ResourceManager " + rmId + " is not Active!");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void MonitorHealth()
		{
			lock (this)
			{
				CheckAccess("monitorHealth");
				if (IsRMActive() && !rm.AreActiveServicesRunning())
				{
					throw new HealthCheckFailedException("Active ResourceManager services are not running!"
						);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TransitionToActive(HAServiceProtocol.StateChangeRequestInfo reqInfo
			)
		{
			lock (this)
			{
				// call refreshAdminAcls before HA state transition
				// for the case that adminAcls have been updated in previous active RM
				try
				{
					RefreshAdminAcls(false);
				}
				catch (YarnException ex)
				{
					throw new ServiceFailedException("Can not execute refreshAdminAcls", ex);
				}
				UserGroupInformation user = CheckAccess("transitionToActive");
				CheckHaStateChange(reqInfo);
				try
				{
					rm.TransitionToActive();
				}
				catch (Exception e)
				{
					RMAuditLogger.LogFailure(user.GetShortUserName(), "transitionToActive", string.Empty
						, "RMHAProtocolService", "Exception transitioning to active");
					throw new ServiceFailedException("Error when transitioning to Active mode", e);
				}
				try
				{
					// call all refresh*s for active RM to get the updated configurations.
					RefreshAll();
				}
				catch (Exception e)
				{
					Log.Error("RefreshAll failed so firing fatal event", e);
					rmContext.GetDispatcher().GetEventHandler().Handle(new RMFatalEvent(RMFatalEventType
						.TransitionToActiveFailed, e));
					throw new ServiceFailedException("Error on refreshAll during transistion to Active"
						, e);
				}
				RMAuditLogger.LogSuccess(user.GetShortUserName(), "transitionToActive", "RMHAProtocolService"
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TransitionToStandby(HAServiceProtocol.StateChangeRequestInfo 
			reqInfo)
		{
			lock (this)
			{
				// call refreshAdminAcls before HA state transition
				// for the case that adminAcls have been updated in previous active RM
				try
				{
					RefreshAdminAcls(false);
				}
				catch (YarnException ex)
				{
					throw new ServiceFailedException("Can not execute refreshAdminAcls", ex);
				}
				UserGroupInformation user = CheckAccess("transitionToStandby");
				CheckHaStateChange(reqInfo);
				try
				{
					rm.TransitionToStandby(true);
					RMAuditLogger.LogSuccess(user.GetShortUserName(), "transitionToStandby", "RMHAProtocolService"
						);
				}
				catch (Exception e)
				{
					RMAuditLogger.LogFailure(user.GetShortUserName(), "transitionToStandby", string.Empty
						, "RMHAProtocolService", "Exception transitioning to standby");
					throw new ServiceFailedException("Error when transitioning to Standby mode", e);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual HAServiceStatus GetServiceStatus()
		{
			lock (this)
			{
				CheckAccess("getServiceState");
				HAServiceProtocol.HAServiceState haState = rmContext.GetHAServiceState();
				HAServiceStatus ret = new HAServiceStatus(haState);
				if (IsRMActive() || haState == HAServiceProtocol.HAServiceState.Standby)
				{
					ret.SetReadyToBecomeActive();
				}
				else
				{
					ret.SetNotReadyToBecomeActive("State is " + haState);
				}
				return ret;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		public virtual RefreshQueuesResponse RefreshQueues(RefreshQueuesRequest request)
		{
			string argName = "refreshQueues";
			string msg = "refresh queues.";
			UserGroupInformation user = CheckAcls(argName);
			CheckRMStatus(user.GetShortUserName(), argName, msg);
			RefreshQueuesResponse response = recordFactory.NewRecordInstance<RefreshQueuesResponse
				>();
			try
			{
				rmContext.GetScheduler().Reinitialize(GetConfig(), this.rmContext);
				// refresh the reservation system
				ReservationSystem rSystem = rmContext.GetReservationSystem();
				if (rSystem != null)
				{
					rSystem.Reinitialize(GetConfig(), rmContext);
				}
				RMAuditLogger.LogSuccess(user.GetShortUserName(), argName, "AdminService");
				return response;
			}
			catch (IOException ioe)
			{
				throw LogAndWrapException(ioe, user.GetShortUserName(), argName, msg);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		public virtual RefreshNodesResponse RefreshNodes(RefreshNodesRequest request)
		{
			string argName = "refreshNodes";
			string msg = "refresh nodes.";
			UserGroupInformation user = CheckAcls("refreshNodes");
			CheckRMStatus(user.GetShortUserName(), argName, msg);
			try
			{
				Configuration conf = GetConfiguration(new Configuration(false), YarnConfiguration
					.YarnSiteConfigurationFile);
				rmContext.GetNodesListManager().RefreshNodes(conf);
				RMAuditLogger.LogSuccess(user.GetShortUserName(), argName, "AdminService");
				return recordFactory.NewRecordInstance<RefreshNodesResponse>();
			}
			catch (IOException ioe)
			{
				throw LogAndWrapException(ioe, user.GetShortUserName(), argName, msg);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RefreshSuperUserGroupsConfigurationResponse RefreshSuperUserGroupsConfiguration
			(RefreshSuperUserGroupsConfigurationRequest request)
		{
			string argName = "refreshSuperUserGroupsConfiguration";
			UserGroupInformation user = CheckAcls(argName);
			CheckRMStatus(user.GetShortUserName(), argName, "refresh super-user-groups.");
			// Accept hadoop common configs in core-site.xml as well as RM specific
			// configurations in yarn-site.xml
			Configuration conf = GetConfiguration(new Configuration(false), YarnConfiguration
				.CoreSiteConfigurationFile, YarnConfiguration.YarnSiteConfigurationFile);
			RMServerUtils.ProcessRMProxyUsersConf(conf);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			RMAuditLogger.LogSuccess(user.GetShortUserName(), argName, "AdminService");
			return recordFactory.NewRecordInstance<RefreshSuperUserGroupsConfigurationResponse
				>();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RefreshUserToGroupsMappingsResponse RefreshUserToGroupsMappings(RefreshUserToGroupsMappingsRequest
			 request)
		{
			string argName = "refreshUserToGroupsMappings";
			UserGroupInformation user = CheckAcls(argName);
			CheckRMStatus(user.GetShortUserName(), argName, "refresh user-groups.");
			Groups.GetUserToGroupsMappingService(GetConfiguration(new Configuration(false), YarnConfiguration
				.CoreSiteConfigurationFile)).Refresh();
			RMAuditLogger.LogSuccess(user.GetShortUserName(), argName, "AdminService");
			return recordFactory.NewRecordInstance<RefreshUserToGroupsMappingsResponse>();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RefreshAdminAclsResponse RefreshAdminAcls(RefreshAdminAclsRequest 
			request)
		{
			return RefreshAdminAcls(true);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private RefreshAdminAclsResponse RefreshAdminAcls(bool checkRMHAState)
		{
			string argName = "refreshAdminAcls";
			UserGroupInformation user = CheckAcls(argName);
			if (checkRMHAState)
			{
				CheckRMStatus(user.GetShortUserName(), argName, "refresh Admin ACLs.");
			}
			Configuration conf = GetConfiguration(new Configuration(false), YarnConfiguration
				.YarnSiteConfigurationFile);
			authorizer.SetAdmins(GetAdminAclList(conf), UserGroupInformation.GetCurrentUser()
				);
			RMAuditLogger.LogSuccess(user.GetShortUserName(), argName, "AdminService");
			return recordFactory.NewRecordInstance<RefreshAdminAclsResponse>();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RefreshServiceAclsResponse RefreshServiceAcls(RefreshServiceAclsRequest
			 request)
		{
			if (!GetConfig().GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization
				, false))
			{
				throw RPCUtil.GetRemoteException(new IOException("Service Authorization (" + CommonConfigurationKeysPublic
					.HadoopSecurityAuthorization + ") not enabled."));
			}
			string argName = "refreshServiceAcls";
			UserGroupInformation user = CheckAcls(argName);
			CheckRMStatus(user.GetShortUserName(), argName, "refresh Service ACLs.");
			PolicyProvider policyProvider = RMPolicyProvider.GetInstance();
			Configuration conf = GetConfiguration(new Configuration(false), YarnConfiguration
				.HadoopPolicyConfigurationFile);
			RefreshServiceAcls(conf, policyProvider);
			rmContext.GetClientRMService().RefreshServiceAcls(conf, policyProvider);
			rmContext.GetApplicationMasterService().RefreshServiceAcls(conf, policyProvider);
			rmContext.GetResourceTrackerService().RefreshServiceAcls(conf, policyProvider);
			RMAuditLogger.LogSuccess(user.GetShortUserName(), argName, "AdminService");
			return recordFactory.NewRecordInstance<RefreshServiceAclsResponse>();
		}

		private void RefreshServiceAcls(Configuration configuration, PolicyProvider policyProvider
			)
		{
			lock (this)
			{
				this.server.RefreshServiceAclWithLoadedConfiguration(configuration, policyProvider
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string[] GetGroupsForUser(string user)
		{
			return UserGroupInformation.CreateRemoteUser(user).GetGroupNames();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual UpdateNodeResourceResponse UpdateNodeResource(UpdateNodeResourceRequest
			 request)
		{
			string argName = "updateNodeResource";
			UserGroupInformation user = CheckAcls(argName);
			CheckRMStatus(user.GetShortUserName(), argName, "update node resource.");
			IDictionary<NodeId, ResourceOption> nodeResourceMap = request.GetNodeResourceMap(
				);
			ICollection<NodeId> nodeIds = nodeResourceMap.Keys;
			// verify nodes are all valid first. 
			// if any invalid nodes, throw exception instead of partially updating
			// valid nodes.
			foreach (NodeId nodeId in nodeIds)
			{
				RMNode node = this.rmContext.GetRMNodes()[nodeId];
				if (node == null)
				{
					Log.Error("Resource update get failed on all nodes due to change " + "resource on an unrecognized node: "
						 + nodeId);
					throw RPCUtil.GetRemoteException("Resource update get failed on all nodes due to change resource "
						 + "on an unrecognized node: " + nodeId);
				}
			}
			// do resource update on each node.
			// Notice: it is still possible to have invalid NodeIDs as nodes decommission
			// may happen just at the same time. This time, only log and skip absent
			// nodes without throwing any exceptions.
			bool allSuccess = true;
			foreach (KeyValuePair<NodeId, ResourceOption> entry in nodeResourceMap)
			{
				ResourceOption newResourceOption = entry.Value;
				NodeId nodeId_1 = entry.Key;
				RMNode node = this.rmContext.GetRMNodes()[nodeId_1];
				if (node == null)
				{
					Log.Warn("Resource update get failed on an unrecognized node: " + nodeId_1);
					allSuccess = false;
				}
				else
				{
					// update resource to RMNode
					this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMNodeResourceUpdateEvent
						(nodeId_1, newResourceOption));
					Log.Info("Update resource on node(" + node.GetNodeID() + ") with resource(" + newResourceOption
						.ToString() + ")");
				}
			}
			if (allSuccess)
			{
				RMAuditLogger.LogSuccess(user.GetShortUserName(), argName, "AdminService");
			}
			UpdateNodeResourceResponse response = UpdateNodeResourceResponse.NewInstance();
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private Configuration GetConfiguration(Configuration conf, params string[] confFileNames
			)
		{
			lock (this)
			{
				foreach (string confFileName in confFileNames)
				{
					InputStream confFileInputStream = this.rmContext.GetConfigurationProvider().GetConfigurationInputStream
						(conf, confFileName);
					if (confFileInputStream != null)
					{
						conf.AddResource(confFileInputStream);
					}
				}
				return conf;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		private void RefreshAll()
		{
			try
			{
				RefreshQueues(RefreshQueuesRequest.NewInstance());
				RefreshNodes(RefreshNodesRequest.NewInstance());
				RefreshSuperUserGroupsConfiguration(RefreshSuperUserGroupsConfigurationRequest.NewInstance
					());
				RefreshUserToGroupsMappings(RefreshUserToGroupsMappingsRequest.NewInstance());
				if (GetConfig().GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization
					, false))
				{
					RefreshServiceAcls(RefreshServiceAclsRequest.NewInstance());
				}
			}
			catch (Exception ex)
			{
				throw new ServiceFailedException(ex.Message);
			}
		}

		// only for testing
		[VisibleForTesting]
		public virtual AccessControlList GetAccessControlList()
		{
			return ((ConfiguredYarnAuthorizer)authorizer).GetAdminAcls();
		}

		[VisibleForTesting]
		public virtual RPC.Server GetServer()
		{
			return this.server;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual AddToClusterNodeLabelsResponse AddToClusterNodeLabels(AddToClusterNodeLabelsRequest
			 request)
		{
			string argName = "addToClusterNodeLabels";
			string msg = "add labels.";
			UserGroupInformation user = CheckAcls(argName);
			CheckRMStatus(user.GetShortUserName(), argName, msg);
			AddToClusterNodeLabelsResponse response = recordFactory.NewRecordInstance<AddToClusterNodeLabelsResponse
				>();
			try
			{
				rmContext.GetNodeLabelManager().AddToCluserNodeLabels(request.GetNodeLabels());
				RMAuditLogger.LogSuccess(user.GetShortUserName(), argName, "AdminService");
				return response;
			}
			catch (IOException ioe)
			{
				throw LogAndWrapException(ioe, user.GetShortUserName(), argName, msg);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoveFromClusterNodeLabelsResponse RemoveFromClusterNodeLabels(RemoveFromClusterNodeLabelsRequest
			 request)
		{
			string argName = "removeFromClusterNodeLabels";
			string msg = "remove labels.";
			UserGroupInformation user = CheckAcls(argName);
			CheckRMStatus(user.GetShortUserName(), argName, msg);
			RemoveFromClusterNodeLabelsResponse response = recordFactory.NewRecordInstance<RemoveFromClusterNodeLabelsResponse
				>();
			try
			{
				rmContext.GetNodeLabelManager().RemoveFromClusterNodeLabels(request.GetNodeLabels
					());
				RMAuditLogger.LogSuccess(user.GetShortUserName(), argName, "AdminService");
				return response;
			}
			catch (IOException ioe)
			{
				throw LogAndWrapException(ioe, user.GetShortUserName(), argName, msg);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ReplaceLabelsOnNodeResponse ReplaceLabelsOnNode(ReplaceLabelsOnNodeRequest
			 request)
		{
			string argName = "replaceLabelsOnNode";
			string msg = "set node to labels.";
			UserGroupInformation user = CheckAcls(argName);
			CheckRMStatus(user.GetShortUserName(), argName, msg);
			ReplaceLabelsOnNodeResponse response = recordFactory.NewRecordInstance<ReplaceLabelsOnNodeResponse
				>();
			try
			{
				rmContext.GetNodeLabelManager().ReplaceLabelsOnNode(request.GetNodeToLabels());
				RMAuditLogger.LogSuccess(user.GetShortUserName(), argName, "AdminService");
				return response;
			}
			catch (IOException ioe)
			{
				throw LogAndWrapException(ioe, user.GetShortUserName(), argName, msg);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		private void CheckRMStatus(string user, string argName, string msg)
		{
			if (!IsRMActive())
			{
				RMAuditLogger.LogFailure(user, argName, string.Empty, "AdminService", "ResourceManager is not active. Can not "
					 + msg);
				ThrowStandbyException();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private YarnException LogAndWrapException(IOException ioe, string user, string argName
			, string msg)
		{
			Log.Info("Exception " + msg, ioe);
			RMAuditLogger.LogFailure(user, argName, string.Empty, "AdminService", "Exception "
				 + msg);
			return RPCUtil.GetRemoteException(ioe);
		}

		public virtual string GetHAZookeeperConnectionState()
		{
			if (!rmContext.IsHAEnabled())
			{
				return "ResourceManager HA is not enabled.";
			}
			else
			{
				if (!autoFailoverEnabled)
				{
					return "Auto Failover is not enabled.";
				}
			}
			return this.embeddedElector.GetHAZookeeperConnectionState();
		}
	}
}
