using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Sharedcache;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager
{
	public class ContainerManagerImpl : CompositeService, ServiceStateChangeListener, 
		ContainerManagementProtocol, EventHandler<ContainerManagerEvent>
	{
		/// <summary>Extra duration to wait for applications to be killed on shutdown.</summary>
		private const int ShutdownCleanupSlopMs = 1000;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.ContainerManagerImpl
			));

		internal readonly Context context;

		private readonly ContainersMonitor containersMonitor;

		private Org.Apache.Hadoop.Ipc.Server server;

		private readonly ResourceLocalizationService rsrcLocalizationSrvc;

		private readonly ContainersLauncher containersLauncher;

		private readonly AuxServices auxiliaryServices;

		private readonly NodeManagerMetrics metrics;

		private readonly NodeStatusUpdater nodeStatusUpdater;

		protected internal LocalDirsHandlerService dirsHandler;

		protected internal readonly AsyncDispatcher dispatcher;

		private readonly ApplicationACLsManager aclsManager;

		private readonly DeletionService deletionService;

		private AtomicBoolean blockNewContainerRequests = new AtomicBoolean(false);

		private bool serviceStopped = false;

		private readonly ReentrantReadWriteLock.ReadLock readLock;

		private readonly ReentrantReadWriteLock.WriteLock writeLock;

		private long waitForContainersOnShutdownMillis;

		public ContainerManagerImpl(Context context, ContainerExecutor exec, DeletionService
			 deletionContext, NodeStatusUpdater nodeStatusUpdater, NodeManagerMetrics metrics
			, ApplicationACLsManager aclsManager, LocalDirsHandlerService dirsHandler)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.ContainerManagerImpl
				).FullName)
		{
			this.context = context;
			this.dirsHandler = dirsHandler;
			// ContainerManager level dispatcher.
			dispatcher = new AsyncDispatcher();
			this.deletionService = deletionContext;
			this.metrics = metrics;
			rsrcLocalizationSrvc = CreateResourceLocalizationService(exec, deletionContext, context
				);
			AddService(rsrcLocalizationSrvc);
			containersLauncher = CreateContainersLauncher(context, exec);
			AddService(containersLauncher);
			this.nodeStatusUpdater = nodeStatusUpdater;
			this.aclsManager = aclsManager;
			// Start configurable services
			auxiliaryServices = new AuxServices();
			auxiliaryServices.RegisterServiceListener(this);
			AddService(auxiliaryServices);
			this.containersMonitor = new ContainersMonitorImpl(exec, dispatcher, this.context
				);
			AddService(this.containersMonitor);
			dispatcher.Register(typeof(ContainerEventType), new ContainerManagerImpl.ContainerEventDispatcher
				(this));
			dispatcher.Register(typeof(ApplicationEventType), new ContainerManagerImpl.ApplicationEventDispatcher
				(this));
			dispatcher.Register(typeof(LocalizationEventType), rsrcLocalizationSrvc);
			dispatcher.Register(typeof(AuxServicesEventType), auxiliaryServices);
			dispatcher.Register(typeof(ContainersMonitorEventType), containersMonitor);
			dispatcher.Register(typeof(ContainersLauncherEventType), containersLauncher);
			AddService(dispatcher);
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			this.readLock = Lock.ReadLock();
			this.writeLock = Lock.WriteLock();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			LogHandler logHandler = CreateLogHandler(conf, this.context, this.deletionService
				);
			AddIfService(logHandler);
			dispatcher.Register(typeof(LogHandlerEventType), logHandler);
			// add the shared cache upload service (it will do nothing if the shared
			// cache is disabled)
			SharedCacheUploadService sharedCacheUploader = CreateSharedCacheUploaderService();
			AddService(sharedCacheUploader);
			dispatcher.Register(typeof(SharedCacheUploadEventType), sharedCacheUploader);
			waitForContainersOnShutdownMillis = conf.GetLong(YarnConfiguration.NmSleepDelayBeforeSigkillMs
				, YarnConfiguration.DefaultNmSleepDelayBeforeSigkillMs) + conf.GetLong(YarnConfiguration
				.NmProcessKillWaitMs, YarnConfiguration.DefaultNmProcessKillWaitMs) + ShutdownCleanupSlopMs;
			base.ServiceInit(conf);
			Recover();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private void Recover()
		{
			NMStateStoreService stateStore = context.GetNMStateStore();
			if (stateStore.CanRecover())
			{
				rsrcLocalizationSrvc.RecoverLocalizedResources(stateStore.LoadLocalizationState()
					);
				NMStateStoreService.RecoveredApplicationsState appsState = stateStore.LoadApplicationsState
					();
				foreach (YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto proto
					 in appsState.GetApplications())
				{
					RecoverApplication(proto);
				}
				foreach (NMStateStoreService.RecoveredContainerState rcs in stateStore.LoadContainersState
					())
				{
					RecoverContainer(rcs);
				}
				string diagnostic = "Application marked finished during recovery";
				foreach (ApplicationId appId in appsState.GetFinishedApplications())
				{
					dispatcher.GetEventHandler().Handle(new ApplicationFinishEvent(appId, diagnostic)
						);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void RecoverApplication(YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
			 p)
		{
			ApplicationId appId = new ApplicationIdPBImpl(p.GetId());
			Credentials creds = new Credentials();
			creds.ReadTokenStorageStream(new DataInputStream(p.GetCredentials().NewInput()));
			IList<YarnProtos.ApplicationACLMapProto> aclProtoList = p.GetAclsList();
			IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
				, string>(aclProtoList.Count);
			foreach (YarnProtos.ApplicationACLMapProto aclProto in aclProtoList)
			{
				acls[ProtoUtils.ConvertFromProtoFormat(aclProto.GetAccessType())] = aclProto.GetAcl
					();
			}
			LogAggregationContext logAggregationContext = null;
			if (p.GetLogAggregationContext() != null)
			{
				logAggregationContext = new LogAggregationContextPBImpl(p.GetLogAggregationContext
					());
			}
			Log.Info("Recovering application " + appId);
			ApplicationImpl app = new ApplicationImpl(dispatcher, p.GetUser(), appId, creds, 
				context);
			context.GetApplications()[appId] = app;
			app.Handle(new ApplicationInitEvent(appId, acls, logAggregationContext));
		}

		/// <exception cref="System.IO.IOException"/>
		private void RecoverContainer(NMStateStoreService.RecoveredContainerState rcs)
		{
			StartContainerRequest req = rcs.GetStartRequest();
			ContainerLaunchContext launchContext = req.GetContainerLaunchContext();
			ContainerTokenIdentifier token = BuilderUtils.NewContainerTokenIdentifier(req.GetContainerToken
				());
			ContainerId containerId = token.GetContainerID();
			ApplicationId appId = containerId.GetApplicationAttemptId().GetApplicationId();
			Log.Info("Recovering " + containerId + " in state " + rcs.GetStatus() + " with exit code "
				 + rcs.GetExitCode());
			if (context.GetApplications().Contains(appId))
			{
				Credentials credentials = ParseCredentials(launchContext);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
					 = new ContainerImpl(GetConfig(), dispatcher, context.GetNMStateStore(), req.GetContainerLaunchContext
					(), credentials, metrics, token, rcs.GetStatus(), rcs.GetExitCode(), rcs.GetDiagnostics
					(), rcs.GetKilled());
				context.GetContainers()[containerId] = container;
				dispatcher.GetEventHandler().Handle(new ApplicationContainerInitEvent(container));
			}
			else
			{
				if (rcs.GetStatus() != NMStateStoreService.RecoveredContainerStatus.Completed)
				{
					Log.Warn(containerId + " has no corresponding application!");
				}
				Log.Info("Adding " + containerId + " to recently stopped containers");
				nodeStatusUpdater.AddCompletedContainer(containerId);
			}
		}

		/// <exception cref="System.Exception"/>
		private void WaitForRecoveredContainers()
		{
			int sleepMsec = 100;
			int waitIterations = 100;
			IList<ContainerId> newContainers = new AList<ContainerId>();
			while (--waitIterations >= 0)
			{
				newContainers.Clear();
				foreach (Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					 container in context.GetContainers().Values)
				{
					if (container.GetContainerState() == ContainerState.New)
					{
						newContainers.AddItem(container.GetContainerId());
					}
				}
				if (newContainers.IsEmpty())
				{
					break;
				}
				Log.Info("Waiting for containers: " + newContainers);
				Sharpen.Thread.Sleep(sleepMsec);
			}
			if (waitIterations < 0)
			{
				Log.Warn("Timeout waiting for recovered containers");
			}
		}

		protected internal virtual LogHandler CreateLogHandler(Configuration conf, Context
			 context, DeletionService deletionService)
		{
			if (conf.GetBoolean(YarnConfiguration.LogAggregationEnabled, YarnConfiguration.DefaultLogAggregationEnabled
				))
			{
				return new LogAggregationService(this.dispatcher, context, deletionService, dirsHandler
					);
			}
			else
			{
				return new NonAggregatingLogHandler(this.dispatcher, deletionService, dirsHandler
					, context.GetNMStateStore());
			}
		}

		public virtual ContainersMonitor GetContainersMonitor()
		{
			return this.containersMonitor;
		}

		protected internal virtual ResourceLocalizationService CreateResourceLocalizationService
			(ContainerExecutor exec, DeletionService deletionContext, Context context)
		{
			return new ResourceLocalizationService(this.dispatcher, exec, deletionContext, dirsHandler
				, context);
		}

		protected internal virtual SharedCacheUploadService CreateSharedCacheUploaderService
			()
		{
			return new SharedCacheUploadService();
		}

		protected internal virtual ContainersLauncher CreateContainersLauncher(Context context
			, ContainerExecutor exec)
		{
			return new ContainersLauncher(context, this.dispatcher, exec, dirsHandler, this);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			// Enqueue user dirs in deletion context
			Configuration conf = GetConfig();
			IPEndPoint initialAddress = conf.GetSocketAddr(YarnConfiguration.NmBindHost, YarnConfiguration
				.NmAddress, YarnConfiguration.DefaultNmAddress, YarnConfiguration.DefaultNmPort);
			bool usingEphemeralPort = (initialAddress.Port == 0);
			if (context.GetNMStateStore().CanRecover() && usingEphemeralPort)
			{
				throw new ArgumentException("Cannot support recovery with an " + "ephemeral server port. Check the setting of "
					 + YarnConfiguration.NmAddress);
			}
			// If recovering then delay opening the RPC service until the recovery
			// of resources and containers have completed, otherwise requests from
			// clients during recovery can interfere with the recovery process.
			bool delayedRpcServerStart = context.GetNMStateStore().CanRecover();
			Configuration serverConf = new Configuration(conf);
			// always enforce it to be token-based.
			serverConf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, SaslRpcServer.AuthMethod
				.Token.ToString());
			YarnRPC rpc = YarnRPC.Create(conf);
			server = rpc.GetServer(typeof(ContainerManagementProtocol), this, initialAddress, 
				serverConf, this.context.GetNMTokenSecretManager(), conf.GetInt(YarnConfiguration
				.NmContainerMgrThreadCount, YarnConfiguration.DefaultNmContainerMgrThreadCount));
			// Enable service authorization?
			if (conf.GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, false
				))
			{
				RefreshServiceAcls(conf, new NMPolicyProvider());
			}
			Log.Info("Blocking new container-requests as container manager rpc" + " server is still starting."
				);
			this.SetBlockNewContainerRequests(true);
			string bindHost = conf.Get(YarnConfiguration.NmBindHost);
			string nmAddress = conf.GetTrimmed(YarnConfiguration.NmAddress);
			string hostOverride = null;
			if (bindHost != null && !bindHost.IsEmpty() && nmAddress != null && !nmAddress.IsEmpty
				())
			{
				//a bind-host case with an address, to support overriding the first
				//hostname found when querying for our hostname with the specified
				//address, combine the specified address with the actual port listened
				//on by the server
				hostOverride = nmAddress.Split(":")[0];
			}
			// setup node ID
			IPEndPoint connectAddress;
			if (delayedRpcServerStart)
			{
				connectAddress = NetUtils.GetConnectAddress(initialAddress);
			}
			else
			{
				server.Start();
				connectAddress = NetUtils.GetConnectAddress(server);
			}
			NodeId nodeId = BuildNodeId(connectAddress, hostOverride);
			((NodeManager.NMContext)context).SetNodeId(nodeId);
			this.context.GetNMTokenSecretManager().SetNodeId(nodeId);
			this.context.GetContainerTokenSecretManager().SetNodeId(nodeId);
			// start remaining services
			base.ServiceStart();
			if (delayedRpcServerStart)
			{
				WaitForRecoveredContainers();
				server.Start();
				// check that the node ID is as previously advertised
				connectAddress = NetUtils.GetConnectAddress(server);
				NodeId serverNode = BuildNodeId(connectAddress, hostOverride);
				if (!serverNode.Equals(nodeId))
				{
					throw new IOException("Node mismatch after server started, expected '" + nodeId +
						 "' but found '" + serverNode + "'");
				}
			}
			Log.Info("ContainerManager started at " + connectAddress);
			Log.Info("ContainerManager bound to " + initialAddress);
		}

		private NodeId BuildNodeId(IPEndPoint connectAddress, string hostOverride)
		{
			if (hostOverride != null)
			{
				connectAddress = NetUtils.GetConnectAddress(new IPEndPoint(hostOverride, connectAddress
					.Port));
			}
			return NodeId.NewInstance(connectAddress.Address.ToString(), connectAddress.Port);
		}

		internal virtual void RefreshServiceAcls(Configuration configuration, PolicyProvider
			 policyProvider)
		{
			this.server.RefreshServiceAcl(configuration, policyProvider);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			SetBlockNewContainerRequests(true);
			this.writeLock.Lock();
			try
			{
				serviceStopped = true;
				if (context != null)
				{
					CleanUpApplicationsOnNMShutDown();
				}
			}
			finally
			{
				this.writeLock.Unlock();
			}
			if (auxiliaryServices.GetServiceState() == Service.STATE.Started)
			{
				auxiliaryServices.UnregisterServiceListener(this);
			}
			if (server != null)
			{
				server.Stop();
			}
			base.ServiceStop();
		}

		public virtual void CleanUpApplicationsOnNMShutDown()
		{
			IDictionary<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				> applications = this.context.GetApplications();
			if (applications.IsEmpty())
			{
				return;
			}
			Log.Info("Applications still running : " + applications.Keys);
			if (this.context.GetNMStateStore().CanRecover() && !this.context.GetDecommissioned
				())
			{
				// do not cleanup apps as they can be recovered on restart
				return;
			}
			IList<ApplicationId> appIds = new AList<ApplicationId>(applications.Keys);
			this.Handle(new CMgrCompletedAppsEvent(appIds, CMgrCompletedAppsEvent.Reason.OnShutdown
				));
			Log.Info("Waiting for Applications to be Finished");
			long waitStartTime = Runtime.CurrentTimeMillis();
			while (!applications.IsEmpty() && Runtime.CurrentTimeMillis() - waitStartTime < waitForContainersOnShutdownMillis
				)
			{
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception ex)
				{
					Log.Warn("Interrupted while sleeping on applications finish on shutdown", ex);
				}
			}
			// All applications Finished
			if (applications.IsEmpty())
			{
				Log.Info("All applications in FINISHED state");
			}
			else
			{
				Log.Info("Done waiting for Applications to be Finished. Still alive: " + applications
					.Keys);
			}
		}

		public virtual void CleanupContainersOnNMResync()
		{
			IDictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				> containers = context.GetContainers();
			if (containers.IsEmpty())
			{
				return;
			}
			Log.Info("Containers still running on " + CMgrCompletedContainersEvent.Reason.OnNodemanagerResync
				 + " : " + containers.Keys);
			IList<ContainerId> containerIds = new AList<ContainerId>(containers.Keys);
			Log.Info("Waiting for containers to be killed");
			this.Handle(new CMgrCompletedContainersEvent(containerIds, CMgrCompletedContainersEvent.Reason
				.OnNodemanagerResync));
			/*
			* We will wait till all the containers change their state to COMPLETE. We
			* will not remove the container statuses from nm context because these
			* are used while re-registering node manager with resource manager.
			*/
			bool allContainersCompleted = false;
			while (!containers.IsEmpty() && !allContainersCompleted)
			{
				allContainersCompleted = true;
				foreach (KeyValuePair<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					> container in containers)
				{
					if (((ContainerImpl)container.Value).GetCurrentState() != ContainerState.Complete)
					{
						allContainersCompleted = false;
						try
						{
							Sharpen.Thread.Sleep(1000);
						}
						catch (Exception ex)
						{
							Log.Warn("Interrupted while sleeping on container kill on resync", ex);
						}
						break;
					}
				}
			}
			// All containers killed
			if (allContainersCompleted)
			{
				Log.Info("All containers in DONE state");
			}
			else
			{
				Log.Info("Done waiting for containers to be killed. Still alive: " + containers.Keys
					);
			}
		}

		// Get the remoteUGI corresponding to the api call.
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		protected internal virtual UserGroupInformation GetRemoteUgi()
		{
			UserGroupInformation remoteUgi;
			try
			{
				remoteUgi = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException e)
			{
				string msg = "Cannot obtain the user-name. Got exception: " + StringUtils.StringifyException
					(e);
				Log.Warn(msg);
				throw RPCUtil.GetRemoteException(msg);
			}
			return remoteUgi;
		}

		// Obtain the needed ContainerTokenIdentifier from the remote-UGI. RPC layer
		// currently sets only the required id, but iterate through anyways just to
		// be sure.
		[InterfaceAudience.Private]
		[VisibleForTesting]
		protected internal virtual NMTokenIdentifier SelectNMTokenIdentifier(UserGroupInformation
			 remoteUgi)
		{
			ICollection<TokenIdentifier> tokenIdentifiers = remoteUgi.GetTokenIdentifiers();
			NMTokenIdentifier resultId = null;
			foreach (TokenIdentifier id in tokenIdentifiers)
			{
				if (id is NMTokenIdentifier)
				{
					resultId = (NMTokenIdentifier)id;
					break;
				}
			}
			return resultId;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		protected internal virtual void AuthorizeUser(UserGroupInformation remoteUgi, NMTokenIdentifier
			 nmTokenIdentifier)
		{
			if (!remoteUgi.GetUserName().Equals(nmTokenIdentifier.GetApplicationAttemptId().ToString
				()))
			{
				throw RPCUtil.GetRemoteException("Expected applicationAttemptId: " + remoteUgi.GetUserName
					() + "Found: " + nmTokenIdentifier.GetApplicationAttemptId());
			}
		}

		/// <param name="containerTokenIdentifier">of the container to be started</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		protected internal virtual void AuthorizeStartRequest(NMTokenIdentifier nmTokenIdentifier
			, ContainerTokenIdentifier containerTokenIdentifier)
		{
			ContainerId containerId = containerTokenIdentifier.GetContainerID();
			string containerIDStr = containerId.ToString();
			bool unauthorized = false;
			StringBuilder messageBuilder = new StringBuilder("Unauthorized request to start container. "
				);
			if (!nmTokenIdentifier.GetApplicationAttemptId().GetApplicationId().Equals(containerId
				.GetApplicationAttemptId().GetApplicationId()))
			{
				unauthorized = true;
				messageBuilder.Append("\nNMToken for application attempt : ").Append(nmTokenIdentifier
					.GetApplicationAttemptId()).Append(" was used for starting container with container token"
					).Append(" issued for application attempt : ").Append(containerId.GetApplicationAttemptId
					());
			}
			else
			{
				if (!this.context.GetContainerTokenSecretManager().IsValidStartContainerRequest(containerTokenIdentifier
					))
				{
					// Is the container being relaunched? Or RPC layer let startCall with
					// tokens generated off old-secret through?
					unauthorized = true;
					messageBuilder.Append("\n Attempt to relaunch the same ").Append("container with id "
						).Append(containerIDStr).Append(".");
				}
				else
				{
					if (containerTokenIdentifier.GetExpiryTimeStamp() < Runtime.CurrentTimeMillis())
					{
						// Ensure the token is not expired.
						unauthorized = true;
						messageBuilder.Append("\nThis token is expired. current time is ").Append(Runtime
							.CurrentTimeMillis()).Append(" found ").Append(containerTokenIdentifier.GetExpiryTimeStamp
							());
						messageBuilder.Append("\nNote: System times on machines may be out of sync.").Append
							(" Check system time and time zones.");
					}
				}
			}
			if (unauthorized)
			{
				string msg = messageBuilder.ToString();
				Log.Error(msg);
				throw RPCUtil.GetRemoteException(msg);
			}
		}

		/// <summary>Start a list of containers on this NodeManager.</summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual StartContainersResponse StartContainers(StartContainersRequest requests
			)
		{
			if (blockNewContainerRequests.Get())
			{
				throw new NMNotYetReadyException("Rejecting new containers as NodeManager has not"
					 + " yet connected with ResourceManager");
			}
			UserGroupInformation remoteUgi = GetRemoteUgi();
			NMTokenIdentifier nmTokenIdentifier = SelectNMTokenIdentifier(remoteUgi);
			AuthorizeUser(remoteUgi, nmTokenIdentifier);
			IList<ContainerId> succeededContainers = new AList<ContainerId>();
			IDictionary<ContainerId, SerializedException> failedContainers = new Dictionary<ContainerId
				, SerializedException>();
			foreach (StartContainerRequest request in requests.GetStartContainerRequests())
			{
				ContainerId containerId = null;
				try
				{
					ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils.NewContainerTokenIdentifier
						(request.GetContainerToken());
					VerifyAndGetContainerTokenIdentifier(request.GetContainerToken(), containerTokenIdentifier
						);
					containerId = containerTokenIdentifier.GetContainerID();
					StartContainerInternal(nmTokenIdentifier, containerTokenIdentifier, request);
					succeededContainers.AddItem(containerId);
				}
				catch (YarnException e)
				{
					failedContainers[containerId] = SerializedException.NewInstance(e);
				}
				catch (SecretManager.InvalidToken ie)
				{
					failedContainers[containerId] = SerializedException.NewInstance(ie);
					throw;
				}
				catch (IOException e)
				{
					throw RPCUtil.GetRemoteException(e);
				}
			}
			return StartContainersResponse.NewInstance(GetAuxServiceMetaData(), succeededContainers
				, failedContainers);
		}

		private YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto BuildAppProto
			(ApplicationId appId, string user, Credentials credentials, IDictionary<ApplicationAccessType
			, string> appAcls, LogAggregationContext logAggregationContext)
		{
			YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto.Builder builder
				 = YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto.NewBuilder
				();
			builder.SetId(((ApplicationIdPBImpl)appId).GetProto());
			builder.SetUser(user);
			if (logAggregationContext != null)
			{
				builder.SetLogAggregationContext(((LogAggregationContextPBImpl)logAggregationContext
					).GetProto());
			}
			builder.ClearCredentials();
			if (credentials != null)
			{
				DataOutputBuffer dob = new DataOutputBuffer();
				try
				{
					credentials.WriteTokenStorageToStream(dob);
					builder.SetCredentials(ByteString.CopyFrom(dob.GetData()));
				}
				catch (IOException e)
				{
					// should not occur
					Log.Error("Cannot serialize credentials", e);
				}
			}
			builder.ClearAcls();
			if (appAcls != null)
			{
				foreach (KeyValuePair<ApplicationAccessType, string> acl in appAcls)
				{
					YarnProtos.ApplicationACLMapProto p = ((YarnProtos.ApplicationACLMapProto)YarnProtos.ApplicationACLMapProto
						.NewBuilder().SetAccessType(ProtoUtils.ConvertToProtoFormat(acl.Key)).SetAcl(acl
						.Value).Build());
					builder.AddAcls(p);
				}
			}
			return ((YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto)builder
				.Build());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void StartContainerInternal(NMTokenIdentifier nmTokenIdentifier, ContainerTokenIdentifier
			 containerTokenIdentifier, StartContainerRequest request)
		{
			/*
			* 1) It should save the NMToken into NMTokenSecretManager. This is done
			* here instead of RPC layer because at the time of opening/authenticating
			* the connection it doesn't know what all RPC calls user will make on it.
			* Also new NMToken is issued only at startContainer (once it gets renewed).
			*
			* 2) It should validate containerToken. Need to check below things. a) It
			* is signed by correct master key (part of retrieve password). b) It
			* belongs to correct Node Manager (part of retrieve password). c) It has
			* correct RMIdentifier. d) It is not expired.
			*/
			AuthorizeStartRequest(nmTokenIdentifier, containerTokenIdentifier);
			if (containerTokenIdentifier.GetRMIdentifier() != nodeStatusUpdater.GetRMIdentifier
				())
			{
				// Is the container coming from unknown RM
				StringBuilder sb = new StringBuilder("\nContainer ");
				sb.Append(containerTokenIdentifier.GetContainerID().ToString()).Append(" rejected as it is allocated by a previous RM"
					);
				throw new InvalidContainerException(sb.ToString());
			}
			// update NMToken
			UpdateNMTokenIdentifier(nmTokenIdentifier);
			ContainerId containerId = containerTokenIdentifier.GetContainerID();
			string containerIdStr = containerId.ToString();
			string user = containerTokenIdentifier.GetApplicationSubmitter();
			Log.Info("Start request for " + containerIdStr + " by user " + user);
			ContainerLaunchContext launchContext = request.GetContainerLaunchContext();
			IDictionary<string, ByteBuffer> serviceData = GetAuxServiceMetaData();
			if (launchContext.GetServiceData() != null && !launchContext.GetServiceData().IsEmpty
				())
			{
				foreach (KeyValuePair<string, ByteBuffer> meta in launchContext.GetServiceData())
				{
					if (null == serviceData[meta.Key])
					{
						throw new InvalidAuxServiceException("The auxService:" + meta.Key + " does not exist"
							);
					}
				}
			}
			Credentials credentials = ParseCredentials(launchContext);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = new ContainerImpl(GetConfig(), this.dispatcher, context.GetNMStateStore(), launchContext
				, credentials, metrics, containerTokenIdentifier);
			ApplicationId applicationID = containerId.GetApplicationAttemptId().GetApplicationId
				();
			if (context.GetContainers().PutIfAbsent(containerId, container) != null)
			{
				NMAuditLogger.LogFailure(user, NMAuditLogger.AuditConstants.StartContainer, "ContainerManagerImpl"
					, "Container already running on this node!", applicationID, containerId);
				throw RPCUtil.GetRemoteException("Container " + containerIdStr + " already is running on this node!!"
					);
			}
			this.readLock.Lock();
			try
			{
				if (!serviceStopped)
				{
					// Create the application
					Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
						 application = new ApplicationImpl(dispatcher, user, applicationID, credentials, 
						context);
					if (null == context.GetApplications().PutIfAbsent(applicationID, application))
					{
						Log.Info("Creating a new application reference for app " + applicationID);
						LogAggregationContext logAggregationContext = containerTokenIdentifier.GetLogAggregationContext
							();
						IDictionary<ApplicationAccessType, string> appAcls = container.GetLaunchContext()
							.GetApplicationACLs();
						context.GetNMStateStore().StoreApplication(applicationID, BuildAppProto(applicationID
							, user, credentials, appAcls, logAggregationContext));
						dispatcher.GetEventHandler().Handle(new ApplicationInitEvent(applicationID, appAcls
							, logAggregationContext));
					}
					this.context.GetNMStateStore().StoreContainer(containerId, request);
					dispatcher.GetEventHandler().Handle(new ApplicationContainerInitEvent(container));
					this.context.GetContainerTokenSecretManager().StartContainerSuccessful(containerTokenIdentifier
						);
					NMAuditLogger.LogSuccess(user, NMAuditLogger.AuditConstants.StartContainer, "ContainerManageImpl"
						, applicationID, containerId);
					// TODO launchedContainer misplaced -> doesn't necessarily mean a container
					// launch. A finished Application will not launch containers.
					metrics.LaunchedContainer();
					metrics.AllocateContainer(containerTokenIdentifier.GetResource());
				}
				else
				{
					throw new YarnException("Container start failed as the NodeManager is " + "in the process of shutting down"
						);
				}
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		protected internal virtual ContainerTokenIdentifier VerifyAndGetContainerTokenIdentifier
			(Org.Apache.Hadoop.Yarn.Api.Records.Token token, ContainerTokenIdentifier containerTokenIdentifier
			)
		{
			byte[] password = context.GetContainerTokenSecretManager().RetrievePassword(containerTokenIdentifier
				);
			byte[] tokenPass = ((byte[])token.GetPassword().Array());
			if (password == null || tokenPass == null || !Arrays.Equals(password, tokenPass))
			{
				throw new SecretManager.InvalidToken("Invalid container token used for starting container on : "
					 + context.GetNodeId().ToString());
			}
			return containerTokenIdentifier;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		protected internal virtual void UpdateNMTokenIdentifier(NMTokenIdentifier nmTokenIdentifier
			)
		{
			context.GetNMTokenSecretManager().AppAttemptStartContainer(nmTokenIdentifier);
		}

		/// <exception cref="System.IO.IOException"/>
		private Credentials ParseCredentials(ContainerLaunchContext launchContext)
		{
			Credentials credentials = new Credentials();
			// //////////// Parse credentials
			ByteBuffer tokens = launchContext.GetTokens();
			if (tokens != null)
			{
				DataInputByteBuffer buf = new DataInputByteBuffer();
				tokens.Rewind();
				buf.Reset(tokens);
				credentials.ReadTokenStorageStream(buf);
				if (Log.IsDebugEnabled())
				{
					foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> tk in credentials
						.GetAllTokens())
					{
						Log.Debug(tk.GetService() + " = " + tk.ToString());
					}
				}
			}
			// //////////// End of parsing credentials
			return credentials;
		}

		/// <summary>Stop a list of containers running on this NodeManager.</summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual StopContainersResponse StopContainers(StopContainersRequest requests
			)
		{
			IList<ContainerId> succeededRequests = new AList<ContainerId>();
			IDictionary<ContainerId, SerializedException> failedRequests = new Dictionary<ContainerId
				, SerializedException>();
			UserGroupInformation remoteUgi = GetRemoteUgi();
			NMTokenIdentifier identifier = SelectNMTokenIdentifier(remoteUgi);
			foreach (ContainerId id in requests.GetContainerIds())
			{
				try
				{
					StopContainerInternal(identifier, id);
					succeededRequests.AddItem(id);
				}
				catch (YarnException e)
				{
					failedRequests[id] = SerializedException.NewInstance(e);
				}
			}
			return StopContainersResponse.NewInstance(succeededRequests, failedRequests);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void StopContainerInternal(NMTokenIdentifier nmTokenIdentifier, ContainerId
			 containerID)
		{
			string containerIDStr = containerID.ToString();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = this.context.GetContainers()[containerID];
			Log.Info("Stopping container with container Id: " + containerIDStr);
			AuthorizeGetAndStopContainerRequest(containerID, container, true, nmTokenIdentifier
				);
			if (container == null)
			{
				if (!nodeStatusUpdater.IsContainerRecentlyStopped(containerID))
				{
					throw RPCUtil.GetRemoteException("Container " + containerIDStr + " is not handled by this NodeManager"
						);
				}
			}
			else
			{
				context.GetNMStateStore().StoreContainerKilled(containerID);
				dispatcher.GetEventHandler().Handle(new ContainerKillEvent(containerID, ContainerExitStatus
					.KilledByAppmaster, "Container killed by the ApplicationMaster."));
				NMAuditLogger.LogSuccess(container.GetUser(), NMAuditLogger.AuditConstants.StopContainer
					, "ContainerManageImpl", containerID.GetApplicationAttemptId().GetApplicationId(
					), containerID);
				// TODO: Move this code to appropriate place once kill_container is
				// implemented.
				nodeStatusUpdater.SendOutofBandHeartBeat();
			}
		}

		/// <summary>Get a list of container statuses running on this NodeManager</summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetContainerStatusesResponse GetContainerStatuses(GetContainerStatusesRequest
			 request)
		{
			IList<ContainerStatus> succeededRequests = new AList<ContainerStatus>();
			IDictionary<ContainerId, SerializedException> failedRequests = new Dictionary<ContainerId
				, SerializedException>();
			UserGroupInformation remoteUgi = GetRemoteUgi();
			NMTokenIdentifier identifier = SelectNMTokenIdentifier(remoteUgi);
			foreach (ContainerId id in request.GetContainerIds())
			{
				try
				{
					ContainerStatus status = GetContainerStatusInternal(id, identifier);
					succeededRequests.AddItem(status);
				}
				catch (YarnException e)
				{
					failedRequests[id] = SerializedException.NewInstance(e);
				}
			}
			return GetContainerStatusesResponse.NewInstance(succeededRequests, failedRequests
				);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private ContainerStatus GetContainerStatusInternal(ContainerId containerID, NMTokenIdentifier
			 nmTokenIdentifier)
		{
			string containerIDStr = containerID.ToString();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = this.context.GetContainers()[containerID];
			Log.Info("Getting container-status for " + containerIDStr);
			AuthorizeGetAndStopContainerRequest(containerID, container, false, nmTokenIdentifier
				);
			if (container == null)
			{
				if (nodeStatusUpdater.IsContainerRecentlyStopped(containerID))
				{
					throw RPCUtil.GetRemoteException("Container " + containerIDStr + " was recently stopped on node manager."
						);
				}
				else
				{
					throw RPCUtil.GetRemoteException("Container " + containerIDStr + " is not handled by this NodeManager"
						);
				}
			}
			ContainerStatus containerStatus = container.CloneAndGetContainerStatus();
			Log.Info("Returning " + containerStatus);
			return containerStatus;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		protected internal virtual void AuthorizeGetAndStopContainerRequest(ContainerId containerId
			, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container, bool stopRequest, NMTokenIdentifier identifier)
		{
			/*
			* For get/stop container status; we need to verify that 1) User (NMToken)
			* application attempt only has started container. 2) Requested containerId
			* belongs to the same application attempt (NMToken) which was used. (Note:-
			* This will prevent user in knowing another application's containers).
			*/
			ApplicationId nmTokenAppId = identifier.GetApplicationAttemptId().GetApplicationId
				();
			if ((!nmTokenAppId.Equals(containerId.GetApplicationAttemptId().GetApplicationId(
				))) || (container != null && !nmTokenAppId.Equals(container.GetContainerId().GetApplicationAttemptId
				().GetApplicationId())))
			{
				if (stopRequest)
				{
					Log.Warn(identifier.GetApplicationAttemptId() + " attempted to stop non-application container : "
						 + container.GetContainerId());
					NMAuditLogger.LogFailure("UnknownUser", NMAuditLogger.AuditConstants.StopContainer
						, "ContainerManagerImpl", "Trying to stop unknown container!", nmTokenAppId, container
						.GetContainerId());
				}
				else
				{
					Log.Warn(identifier.GetApplicationAttemptId() + " attempted to get status for non-application container : "
						 + container.GetContainerId());
				}
			}
		}

		internal class ContainerEventDispatcher : EventHandler<ContainerEvent>
		{
			public virtual void Handle(ContainerEvent @event)
			{
				IDictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					> containers = this._enclosing.context.GetContainers();
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
					containers[@event.GetContainerID()];
				if (c != null)
				{
					c.Handle(@event);
				}
				else
				{
					ContainerManagerImpl.Log.Warn("Event " + @event + " sent to absent container " + 
						@event.GetContainerID());
				}
			}

			internal ContainerEventDispatcher(ContainerManagerImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ContainerManagerImpl _enclosing;
		}

		internal class ApplicationEventDispatcher : EventHandler<ApplicationEvent>
		{
			public virtual void Handle(ApplicationEvent @event)
			{
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app = this._enclosing.context.GetApplications()[@event.GetApplicationID()];
				if (app != null)
				{
					app.Handle(@event);
				}
				else
				{
					ContainerManagerImpl.Log.Warn("Event " + @event + " sent to absent application " 
						+ @event.GetApplicationID());
				}
			}

			internal ApplicationEventDispatcher(ContainerManagerImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ContainerManagerImpl _enclosing;
		}

		public virtual void Handle(ContainerManagerEvent @event)
		{
			switch (@event.GetType())
			{
				case ContainerManagerEventType.FinishApps:
				{
					CMgrCompletedAppsEvent appsFinishedEvent = (CMgrCompletedAppsEvent)@event;
					foreach (ApplicationId appID in appsFinishedEvent.GetAppsToCleanup())
					{
						string diagnostic = string.Empty;
						if (appsFinishedEvent.GetReason() == CMgrCompletedAppsEvent.Reason.OnShutdown)
						{
							diagnostic = "Application killed on shutdown";
						}
						else
						{
							if (appsFinishedEvent.GetReason() == CMgrCompletedAppsEvent.Reason.ByResourcemanager)
							{
								diagnostic = "Application killed by ResourceManager";
							}
						}
						try
						{
							this.context.GetNMStateStore().StoreFinishedApplication(appID);
						}
						catch (IOException e)
						{
							Log.Error("Unable to update application state in store", e);
						}
						this.dispatcher.GetEventHandler().Handle(new ApplicationFinishEvent(appID, diagnostic
							));
					}
					break;
				}

				case ContainerManagerEventType.FinishContainers:
				{
					CMgrCompletedContainersEvent containersFinishedEvent = (CMgrCompletedContainersEvent
						)@event;
					foreach (ContainerId container in containersFinishedEvent.GetContainersToCleanup(
						))
					{
						this.dispatcher.GetEventHandler().Handle(new ContainerKillEvent(container, ContainerExitStatus
							.KilledByResourcemanager, "Container Killed by ResourceManager"));
					}
					break;
				}

				default:
				{
					throw new YarnRuntimeException("Got an unknown ContainerManagerEvent type: " + @event
						.GetType());
				}
			}
		}

		public virtual void SetBlockNewContainerRequests(bool blockNewContainerRequests)
		{
			this.blockNewContainerRequests.Set(blockNewContainerRequests);
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual bool GetBlockNewContainerRequestsStatus()
		{
			return this.blockNewContainerRequests.Get();
		}

		public virtual void StateChanged(Org.Apache.Hadoop.Service.Service service)
		{
		}

		// TODO Auto-generated method stub
		public virtual Context GetContext()
		{
			return this.context;
		}

		public virtual IDictionary<string, ByteBuffer> GetAuxServiceMetaData()
		{
			return this.auxiliaryServices.GetMetaData();
		}
	}
}
