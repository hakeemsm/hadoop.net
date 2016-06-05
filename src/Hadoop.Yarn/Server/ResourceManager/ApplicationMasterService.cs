using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class ApplicationMasterService : AbstractService, ApplicationMasterProtocol
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.ApplicationMasterService
			));

		private readonly AMLivelinessMonitor amLivelinessMonitor;

		private YarnScheduler rScheduler;

		private IPEndPoint bindAddress;

		private Org.Apache.Hadoop.Ipc.Server server;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private readonly ConcurrentMap<ApplicationAttemptId, ApplicationMasterService.AllocateResponseLock
			> responseMap = new ConcurrentHashMap<ApplicationAttemptId, ApplicationMasterService.AllocateResponseLock
			>();

		private readonly RMContext rmContext;

		public ApplicationMasterService(RMContext rmContext, YarnScheduler scheduler)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.ApplicationMasterService
				).FullName)
		{
			this.amLivelinessMonitor = rmContext.GetAMLivelinessMonitor();
			this.rScheduler = scheduler;
			this.rmContext = rmContext;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Configuration conf = GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			IPEndPoint masterServiceAddress = conf.GetSocketAddr(YarnConfiguration.RmBindHost
				, YarnConfiguration.RmSchedulerAddress, YarnConfiguration.DefaultRmSchedulerAddress
				, YarnConfiguration.DefaultRmSchedulerPort);
			Configuration serverConf = conf;
			// If the auth is not-simple, enforce it to be token-based.
			serverConf = new Configuration(conf);
			serverConf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, SaslRpcServer.AuthMethod
				.Token.ToString());
			this.server = rpc.GetServer(typeof(ApplicationMasterProtocol), this, masterServiceAddress
				, serverConf, this.rmContext.GetAMRMTokenSecretManager(), serverConf.GetInt(YarnConfiguration
				.RmSchedulerClientThreadCount, YarnConfiguration.DefaultRmSchedulerClientThreadCount
				));
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
			this.bindAddress = conf.UpdateConnectAddr(YarnConfiguration.RmBindHost, YarnConfiguration
				.RmSchedulerAddress, YarnConfiguration.DefaultRmSchedulerAddress, server.GetListenerAddress
				());
			base.ServiceStart();
		}

		[InterfaceAudience.Private]
		public virtual IPEndPoint GetBindAddress()
		{
			return this.bindAddress;
		}

		// Obtain the needed AMRMTokenIdentifier from the remote-UGI. RPC layer
		// currently sets only the required id, but iterate through anyways just to be
		// sure.
		/// <exception cref="System.IO.IOException"/>
		private AMRMTokenIdentifier SelectAMRMTokenIdentifier(UserGroupInformation remoteUgi
			)
		{
			AMRMTokenIdentifier result = null;
			ICollection<TokenIdentifier> tokenIds = remoteUgi.GetTokenIdentifiers();
			foreach (TokenIdentifier tokenId in tokenIds)
			{
				if (tokenId is AMRMTokenIdentifier)
				{
					result = (AMRMTokenIdentifier)tokenId;
					break;
				}
			}
			return result;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private AMRMTokenIdentifier AuthorizeRequest()
		{
			UserGroupInformation remoteUgi;
			try
			{
				remoteUgi = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException e)
			{
				string msg = "Cannot obtain the user-name for authorizing ApplicationMaster. " + 
					"Got exception: " + StringUtils.StringifyException(e);
				Log.Warn(msg);
				throw RPCUtil.GetRemoteException(msg);
			}
			bool tokenFound = false;
			string message = string.Empty;
			AMRMTokenIdentifier appTokenIdentifier = null;
			try
			{
				appTokenIdentifier = SelectAMRMTokenIdentifier(remoteUgi);
				if (appTokenIdentifier == null)
				{
					tokenFound = false;
					message = "No AMRMToken found for user " + remoteUgi.GetUserName();
				}
				else
				{
					tokenFound = true;
				}
			}
			catch (IOException)
			{
				tokenFound = false;
				message = "Got exception while looking for AMRMToken for user " + remoteUgi.GetUserName
					();
			}
			if (!tokenFound)
			{
				Log.Warn(message);
				throw RPCUtil.GetRemoteException(message);
			}
			return appTokenIdentifier;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RegisterApplicationMasterResponse RegisterApplicationMaster(RegisterApplicationMasterRequest
			 request)
		{
			AMRMTokenIdentifier amrmTokenIdentifier = AuthorizeRequest();
			ApplicationAttemptId applicationAttemptId = amrmTokenIdentifier.GetApplicationAttemptId
				();
			ApplicationId appID = applicationAttemptId.GetApplicationId();
			ApplicationMasterService.AllocateResponseLock Lock = responseMap[applicationAttemptId
				];
			if (Lock == null)
			{
				RMAuditLogger.LogFailure(this.rmContext.GetRMApps()[appID].GetUser(), RMAuditLogger.AuditConstants
					.RegisterAm, "Application doesn't exist in cache " + applicationAttemptId, "ApplicationMasterService"
					, "Error in registering application master", appID, applicationAttemptId);
				ThrowApplicationDoesNotExistInCacheException(applicationAttemptId);
			}
			// Allow only one thread in AM to do registerApp at a time.
			lock (Lock)
			{
				AllocateResponse lastResponse = Lock.GetAllocateResponse();
				if (HasApplicationMasterRegistered(applicationAttemptId))
				{
					string message = "Application Master is already registered : " + appID;
					Log.Warn(message);
					RMAuditLogger.LogFailure(this.rmContext.GetRMApps()[appID].GetUser(), RMAuditLogger.AuditConstants
						.RegisterAm, string.Empty, "ApplicationMasterService", message, appID, applicationAttemptId
						);
					throw new InvalidApplicationMasterRequestException(message);
				}
				this.amLivelinessMonitor.ReceivedPing(applicationAttemptId);
				RMApp app = this.rmContext.GetRMApps()[appID];
				// Setting the response id to 0 to identify if the
				// application master is register for the respective attemptid
				lastResponse.SetResponseId(0);
				Lock.SetAllocateResponse(lastResponse);
				Log.Info("AM registration " + applicationAttemptId);
				this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppAttemptRegistrationEvent
					(applicationAttemptId, request.GetHost(), request.GetRpcPort(), request.GetTrackingUrl
					()));
				RMAuditLogger.LogSuccess(app.GetUser(), RMAuditLogger.AuditConstants.RegisterAm, 
					"ApplicationMasterService", appID, applicationAttemptId);
				// Pick up min/max resource from scheduler...
				RegisterApplicationMasterResponse response = recordFactory.NewRecordInstance<RegisterApplicationMasterResponse
					>();
				response.SetMaximumResourceCapability(rScheduler.GetMaximumResourceCapability(app
					.GetQueue()));
				response.SetApplicationACLs(app.GetRMAppAttempt(applicationAttemptId).GetSubmissionContext
					().GetAMContainerSpec().GetApplicationACLs());
				response.SetQueue(app.GetQueue());
				if (UserGroupInformation.IsSecurityEnabled())
				{
					Log.Info("Setting client token master key");
					response.SetClientToAMTokenMasterKey(ByteBuffer.Wrap(rmContext.GetClientToAMTokenSecretManager
						().GetMasterKey(applicationAttemptId).GetEncoded()));
				}
				// For work-preserving AM restart, retrieve previous attempts' containers
				// and corresponding NM tokens.
				if (app.GetApplicationSubmissionContext().GetKeepContainersAcrossApplicationAttempts
					())
				{
					IList<Container> transferredContainers = ((AbstractYarnScheduler)rScheduler).GetTransferredContainers
						(applicationAttemptId);
					if (!transferredContainers.IsEmpty())
					{
						response.SetContainersFromPreviousAttempts(transferredContainers);
						IList<NMToken> nmTokens = new AList<NMToken>();
						foreach (Container container in transferredContainers)
						{
							try
							{
								NMToken token = rmContext.GetNMTokenSecretManager().CreateAndGetNMToken(app.GetUser
									(), applicationAttemptId, container);
								if (null != token)
								{
									nmTokens.AddItem(token);
								}
							}
							catch (ArgumentException e)
							{
								// if it's a DNS issue, throw UnknowHostException directly and
								// that
								// will be automatically retried by RMProxy in RPC layer.
								if (e.InnerException is UnknownHostException)
								{
									throw (UnknownHostException)e.InnerException;
								}
							}
						}
						response.SetNMTokensFromPreviousAttempts(nmTokens);
						Log.Info("Application " + appID + " retrieved " + transferredContainers.Count + " containers from previous"
							 + " attempts and " + nmTokens.Count + " NM tokens.");
					}
				}
				response.SetSchedulerResourceTypes(rScheduler.GetSchedulingResourceTypes());
				return response;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual FinishApplicationMasterResponse FinishApplicationMaster(FinishApplicationMasterRequest
			 request)
		{
			ApplicationAttemptId applicationAttemptId = AuthorizeRequest().GetApplicationAttemptId
				();
			ApplicationId appId = applicationAttemptId.GetApplicationId();
			RMApp rmApp = rmContext.GetRMApps()[applicationAttemptId.GetApplicationId()];
			// checking whether the app exits in RMStateStore at first not to throw
			// ApplicationDoesNotExistInCacheException before and after
			// RM work-preserving restart.
			if (rmApp.IsAppFinalStateStored())
			{
				Log.Info(rmApp.GetApplicationId() + " unregistered successfully. ");
				return FinishApplicationMasterResponse.NewInstance(true);
			}
			ApplicationMasterService.AllocateResponseLock Lock = responseMap[applicationAttemptId
				];
			if (Lock == null)
			{
				ThrowApplicationDoesNotExistInCacheException(applicationAttemptId);
			}
			// Allow only one thread in AM to do finishApp at a time.
			lock (Lock)
			{
				if (!HasApplicationMasterRegistered(applicationAttemptId))
				{
					string message = "Application Master is trying to unregister before registering for: "
						 + appId;
					Log.Error(message);
					RMAuditLogger.LogFailure(this.rmContext.GetRMApps()[appId].GetUser(), RMAuditLogger.AuditConstants
						.UnregisterAm, string.Empty, "ApplicationMasterService", message, appId, applicationAttemptId
						);
					throw new ApplicationMasterNotRegisteredException(message);
				}
				this.amLivelinessMonitor.ReceivedPing(applicationAttemptId);
				rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppAttemptUnregistrationEvent
					(applicationAttemptId, request.GetTrackingUrl(), request.GetFinalApplicationStatus
					(), request.GetDiagnostics()));
				// For UnmanagedAMs, return true so they don't retry
				return FinishApplicationMasterResponse.NewInstance(rmApp.GetApplicationSubmissionContext
					().GetUnmanagedAM());
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidApplicationMasterRequestException
		/// 	"/>
		private void ThrowApplicationDoesNotExistInCacheException(ApplicationAttemptId appAttemptId
			)
		{
			string message = "Application doesn't exist in cache " + appAttemptId;
			Log.Error(message);
			throw new InvalidApplicationMasterRequestException(message);
		}

		/// <param name="appAttemptId"/>
		/// <returns>true if application is registered for the respective attemptid</returns>
		public virtual bool HasApplicationMasterRegistered(ApplicationAttemptId appAttemptId
			)
		{
			bool hasApplicationMasterRegistered = false;
			ApplicationMasterService.AllocateResponseLock lastResponse = responseMap[appAttemptId
				];
			if (lastResponse != null)
			{
				lock (lastResponse)
				{
					if (lastResponse.GetAllocateResponse() != null && lastResponse.GetAllocateResponse
						().GetResponseId() >= 0)
					{
						hasApplicationMasterRegistered = true;
					}
				}
			}
			return hasApplicationMasterRegistered;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual AllocateResponse Allocate(AllocateRequest request)
		{
			AMRMTokenIdentifier amrmTokenIdentifier = AuthorizeRequest();
			ApplicationAttemptId appAttemptId = amrmTokenIdentifier.GetApplicationAttemptId();
			ApplicationId applicationId = appAttemptId.GetApplicationId();
			this.amLivelinessMonitor.ReceivedPing(appAttemptId);
			/* check if its in cache */
			ApplicationMasterService.AllocateResponseLock Lock = responseMap[appAttemptId];
			if (Lock == null)
			{
				string message = "Application attempt " + appAttemptId + " doesn't exist in ApplicationMasterService cache.";
				Log.Error(message);
				throw new ApplicationAttemptNotFoundException(message);
			}
			lock (Lock)
			{
				AllocateResponse lastResponse = Lock.GetAllocateResponse();
				if (!HasApplicationMasterRegistered(appAttemptId))
				{
					string message = "AM is not registered for known application attempt: " + appAttemptId
						 + " or RM had restarted after AM registered . AM should re-register.";
					Log.Info(message);
					RMAuditLogger.LogFailure(this.rmContext.GetRMApps()[appAttemptId.GetApplicationId
						()].GetUser(), RMAuditLogger.AuditConstants.AmAllocate, string.Empty, "ApplicationMasterService"
						, message, applicationId, appAttemptId);
					throw new ApplicationMasterNotRegisteredException(message);
				}
				if ((request.GetResponseId() + 1) == lastResponse.GetResponseId())
				{
					/* old heartbeat */
					return lastResponse;
				}
				else
				{
					if (request.GetResponseId() + 1 < lastResponse.GetResponseId())
					{
						string message = "Invalid responseId in AllocateRequest from application attempt: "
							 + appAttemptId + ", expect responseId to be " + (lastResponse.GetResponseId() +
							 1);
						throw new InvalidApplicationMasterRequestException(message);
					}
				}
				//filter illegal progress values
				float filteredProgress = request.GetProgress();
				if (float.IsNaN(filteredProgress) || filteredProgress == float.NegativeInfinity ||
					 filteredProgress < 0)
				{
					request.SetProgress(0);
				}
				else
				{
					if (filteredProgress > 1 || filteredProgress == float.PositiveInfinity)
					{
						request.SetProgress(1);
					}
				}
				// Send the status update to the appAttempt.
				this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppAttemptStatusupdateEvent
					(appAttemptId, request.GetProgress()));
				IList<ResourceRequest> ask = request.GetAskList();
				IList<ContainerId> release = request.GetReleaseList();
				ResourceBlacklistRequest blacklistRequest = request.GetResourceBlacklistRequest();
				IList<string> blacklistAdditions = (blacklistRequest != null) ? blacklistRequest.
					GetBlacklistAdditions() : Sharpen.Collections.EmptyList;
				IList<string> blacklistRemovals = (blacklistRequest != null) ? blacklistRequest.GetBlacklistRemovals
					() : Sharpen.Collections.EmptyList;
				RMApp app = this.rmContext.GetRMApps()[applicationId];
				// set label expression for Resource Requests if resourceName=ANY 
				ApplicationSubmissionContext asc = app.GetApplicationSubmissionContext();
				foreach (ResourceRequest req in ask)
				{
					if (null == req.GetNodeLabelExpression() && ResourceRequest.Any.Equals(req.GetResourceName
						()))
					{
						req.SetNodeLabelExpression(asc.GetNodeLabelExpression());
					}
				}
				// sanity check
				try
				{
					RMServerUtils.NormalizeAndValidateRequests(ask, rScheduler.GetMaximumResourceCapability
						(), app.GetQueue(), rScheduler, rmContext);
				}
				catch (InvalidResourceRequestException e)
				{
					Log.Warn("Invalid resource ask by application " + appAttemptId, e);
					throw;
				}
				try
				{
					RMServerUtils.ValidateBlacklistRequest(blacklistRequest);
				}
				catch (InvalidResourceBlacklistRequestException e)
				{
					Log.Warn("Invalid blacklist request by application " + appAttemptId, e);
					throw;
				}
				// In the case of work-preserving AM restart, it's possible for the
				// AM to release containers from the earlier attempt.
				if (!app.GetApplicationSubmissionContext().GetKeepContainersAcrossApplicationAttempts
					())
				{
					try
					{
						RMServerUtils.ValidateContainerReleaseRequest(release, appAttemptId);
					}
					catch (InvalidContainerReleaseException e)
					{
						Log.Warn("Invalid container release by application " + appAttemptId, e);
						throw;
					}
				}
				// Send new requests to appAttempt.
				Allocation allocation = this.rScheduler.Allocate(appAttemptId, ask, release, blacklistAdditions
					, blacklistRemovals);
				if (!blacklistAdditions.IsEmpty() || !blacklistRemovals.IsEmpty())
				{
					Log.Info("blacklist are updated in Scheduler." + "blacklistAdditions: " + blacklistAdditions
						 + ", " + "blacklistRemovals: " + blacklistRemovals);
				}
				RMAppAttempt appAttempt = app.GetRMAppAttempt(appAttemptId);
				AllocateResponse allocateResponse = recordFactory.NewRecordInstance<AllocateResponse
					>();
				if (!allocation.GetContainers().IsEmpty())
				{
					allocateResponse.SetNMTokens(allocation.GetNMTokens());
				}
				// update the response with the deltas of node status changes
				IList<RMNode> updatedNodes = new AList<RMNode>();
				if (app.PullRMNodeUpdates(updatedNodes) > 0)
				{
					IList<NodeReport> updatedNodeReports = new AList<NodeReport>();
					foreach (RMNode rmNode in updatedNodes)
					{
						SchedulerNodeReport schedulerNodeReport = rScheduler.GetNodeReport(rmNode.GetNodeID
							());
						Resource used = BuilderUtils.NewResource(0, 0);
						int numContainers = 0;
						if (schedulerNodeReport != null)
						{
							used = schedulerNodeReport.GetUsedResource();
							numContainers = schedulerNodeReport.GetNumContainers();
						}
						NodeId nodeId = rmNode.GetNodeID();
						NodeReport report = BuilderUtils.NewNodeReport(nodeId, rmNode.GetState(), rmNode.
							GetHttpAddress(), rmNode.GetRackName(), used, rmNode.GetTotalCapability(), numContainers
							, rmNode.GetHealthReport(), rmNode.GetLastHealthReportTime(), rmNode.GetNodeLabels
							());
						updatedNodeReports.AddItem(report);
					}
					allocateResponse.SetUpdatedNodes(updatedNodeReports);
				}
				allocateResponse.SetAllocatedContainers(allocation.GetContainers());
				allocateResponse.SetCompletedContainersStatuses(appAttempt.PullJustFinishedContainers
					());
				allocateResponse.SetResponseId(lastResponse.GetResponseId() + 1);
				allocateResponse.SetAvailableResources(allocation.GetResourceLimit());
				allocateResponse.SetNumClusterNodes(this.rScheduler.GetNumClusterNodes());
				// add preemption to the allocateResponse message (if any)
				allocateResponse.SetPreemptionMessage(GeneratePreemptionMessage(allocation));
				// update AMRMToken if the token is rolled-up
				MasterKeyData nextMasterKey = this.rmContext.GetAMRMTokenSecretManager().GetNextMasterKeyData
					();
				if (nextMasterKey != null && nextMasterKey.GetMasterKey().GetKeyId() != amrmTokenIdentifier
					.GetKeyId())
				{
					RMAppAttemptImpl appAttemptImpl = (RMAppAttemptImpl)appAttempt;
					Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken = appAttempt
						.GetAMRMToken();
					if (nextMasterKey.GetMasterKey().GetKeyId() != appAttemptImpl.GetAMRMTokenKeyId())
					{
						Log.Info("The AMRMToken has been rolled-over. Send new AMRMToken back" + " to application: "
							 + applicationId);
						amrmToken = rmContext.GetAMRMTokenSecretManager().CreateAndGetAMRMToken(appAttemptId
							);
						appAttemptImpl.SetAMRMToken(amrmToken);
					}
					allocateResponse.SetAMRMToken(Org.Apache.Hadoop.Yarn.Api.Records.Token.NewInstance
						(amrmToken.GetIdentifier(), amrmToken.GetKind().ToString(), amrmToken.GetPassword
						(), amrmToken.GetService().ToString()));
				}
				/*
				* As we are updating the response inside the lock object so we don't
				* need to worry about unregister call occurring in between (which
				* removes the lock object).
				*/
				Lock.SetAllocateResponse(allocateResponse);
				return allocateResponse;
			}
		}

		private PreemptionMessage GeneratePreemptionMessage(Allocation allocation)
		{
			PreemptionMessage pMsg = null;
			// assemble strict preemption request
			if (allocation.GetStrictContainerPreemptions() != null)
			{
				pMsg = recordFactory.NewRecordInstance<PreemptionMessage>();
				StrictPreemptionContract pStrict = recordFactory.NewRecordInstance<StrictPreemptionContract
					>();
				ICollection<PreemptionContainer> pCont = new HashSet<PreemptionContainer>();
				foreach (ContainerId cId in allocation.GetStrictContainerPreemptions())
				{
					PreemptionContainer pc = recordFactory.NewRecordInstance<PreemptionContainer>();
					pc.SetId(cId);
					pCont.AddItem(pc);
				}
				pStrict.SetContainers(pCont);
				pMsg.SetStrictContract(pStrict);
			}
			// assemble negotiable preemption request
			if (allocation.GetResourcePreemptions() != null && allocation.GetResourcePreemptions
				().Count > 0 && allocation.GetContainerPreemptions() != null && allocation.GetContainerPreemptions
				().Count > 0)
			{
				if (pMsg == null)
				{
					pMsg = recordFactory.NewRecordInstance<PreemptionMessage>();
				}
				PreemptionContract contract = recordFactory.NewRecordInstance<PreemptionContract>
					();
				ICollection<PreemptionContainer> pCont = new HashSet<PreemptionContainer>();
				foreach (ContainerId cId in allocation.GetContainerPreemptions())
				{
					PreemptionContainer pc = recordFactory.NewRecordInstance<PreemptionContainer>();
					pc.SetId(cId);
					pCont.AddItem(pc);
				}
				IList<PreemptionResourceRequest> pRes = new AList<PreemptionResourceRequest>();
				foreach (ResourceRequest crr in allocation.GetResourcePreemptions())
				{
					PreemptionResourceRequest prr = recordFactory.NewRecordInstance<PreemptionResourceRequest
						>();
					prr.SetResourceRequest(crr);
					pRes.AddItem(prr);
				}
				contract.SetContainers(pCont);
				contract.SetResourceRequest(pRes);
				pMsg.SetContract(contract);
			}
			return pMsg;
		}

		public virtual void RegisterAppAttempt(ApplicationAttemptId attemptId)
		{
			AllocateResponse response = recordFactory.NewRecordInstance<AllocateResponse>();
			// set response id to -1 before application master for the following
			// attemptID get registered
			response.SetResponseId(-1);
			Log.Info("Registering app attempt : " + attemptId);
			responseMap[attemptId] = new ApplicationMasterService.AllocateResponseLock(response
				);
			rmContext.GetNMTokenSecretManager().RegisterApplicationAttempt(attemptId);
		}

		public virtual void UnregisterAttempt(ApplicationAttemptId attemptId)
		{
			Log.Info("Unregistering app attempt : " + attemptId);
			Sharpen.Collections.Remove(responseMap, attemptId);
			rmContext.GetNMTokenSecretManager().UnregisterApplicationAttempt(attemptId);
		}

		public virtual void RefreshServiceAcls(Configuration configuration, PolicyProvider
			 policyProvider)
		{
			this.server.RefreshServiceAclWithLoadedConfiguration(configuration, policyProvider
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

		public class AllocateResponseLock
		{
			private AllocateResponse response;

			public AllocateResponseLock(AllocateResponse response)
			{
				this.response = response;
			}

			public virtual AllocateResponse GetAllocateResponse()
			{
				lock (this)
				{
					return response;
				}
			}

			public virtual void SetAllocateResponse(AllocateResponse response)
			{
				lock (this)
				{
					this.response = response;
				}
			}
		}

		[VisibleForTesting]
		public virtual Org.Apache.Hadoop.Ipc.Server GetServer()
		{
			return this.server;
		}
	}
}
