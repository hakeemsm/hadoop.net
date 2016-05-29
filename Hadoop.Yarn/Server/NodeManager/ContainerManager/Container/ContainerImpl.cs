using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Sharedcache;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.State;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public class ContainerImpl : Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
	{
		private readonly Lock readLock;

		private readonly Lock writeLock;

		private readonly Dispatcher dispatcher;

		private readonly NMStateStoreService stateStore;

		private readonly Credentials credentials;

		private readonly NodeManagerMetrics metrics;

		private readonly ContainerLaunchContext launchContext;

		private readonly ContainerTokenIdentifier containerTokenIdentifier;

		private readonly ContainerId containerId;

		private readonly Resource resource;

		private readonly string user;

		private int exitCode = ContainerExitStatus.Invalid;

		private readonly StringBuilder diagnostics;

		private bool wasLaunched;

		private long containerLaunchStartTime;

		private static Clock clock = new SystemClock();

		/// <summary>The NM-wide configuration - not specific to this container</summary>
		private readonly Configuration daemonConf;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.ContainerImpl
			));

		private readonly IDictionary<LocalResourceRequest, IList<string>> pendingResources
			 = new Dictionary<LocalResourceRequest, IList<string>>();

		private readonly IDictionary<Path, IList<string>> localizedResources = new Dictionary
			<Path, IList<string>>();

		private readonly IList<LocalResourceRequest> publicRsrcs = new AList<LocalResourceRequest
			>();

		private readonly IList<LocalResourceRequest> privateRsrcs = new AList<LocalResourceRequest
			>();

		private readonly IList<LocalResourceRequest> appRsrcs = new AList<LocalResourceRequest
			>();

		private readonly IDictionary<LocalResourceRequest, Path> resourcesToBeUploaded = 
			new ConcurrentHashMap<LocalResourceRequest, Path>();

		private readonly IDictionary<LocalResourceRequest, bool> resourcesUploadPolicies = 
			new ConcurrentHashMap<LocalResourceRequest, bool>();

		private NMStateStoreService.RecoveredContainerStatus recoveredStatus = NMStateStoreService.RecoveredContainerStatus
			.Requested;

		private bool recoveredAsKilled = false;

		public ContainerImpl(Configuration conf, Dispatcher dispatcher, NMStateStoreService
			 stateStore, ContainerLaunchContext launchContext, Credentials creds, NodeManagerMetrics
			 metrics, ContainerTokenIdentifier containerTokenIdentifier)
		{
			// whether container has been recovered after a restart
			// whether container was marked as killed after recovery
			this.daemonConf = conf;
			this.dispatcher = dispatcher;
			this.stateStore = stateStore;
			this.launchContext = launchContext;
			this.containerTokenIdentifier = containerTokenIdentifier;
			this.containerId = containerTokenIdentifier.GetContainerID();
			this.resource = containerTokenIdentifier.GetResource();
			this.diagnostics = new StringBuilder();
			this.credentials = creds;
			this.metrics = metrics;
			user = containerTokenIdentifier.GetApplicationSubmitter();
			ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
			this.readLock = readWriteLock.ReadLock();
			this.writeLock = readWriteLock.WriteLock();
			stateMachine = stateMachineFactory.Make(this);
		}

		public ContainerImpl(Configuration conf, Dispatcher dispatcher, NMStateStoreService
			 stateStore, ContainerLaunchContext launchContext, Credentials creds, NodeManagerMetrics
			 metrics, ContainerTokenIdentifier containerTokenIdentifier, NMStateStoreService.RecoveredContainerStatus
			 recoveredStatus, int exitCode, string diagnostics, bool wasKilled)
			: this(conf, dispatcher, stateStore, launchContext, creds, metrics, containerTokenIdentifier
				)
		{
			// constructor for a recovered container
			this.recoveredStatus = recoveredStatus;
			this.exitCode = exitCode;
			this.recoveredAsKilled = wasKilled;
			this.diagnostics.Append(diagnostics);
		}

		private static readonly ContainerImpl.ContainerDiagnosticsUpdateTransition UpdateDiagnosticsTransition
			 = new ContainerImpl.ContainerDiagnosticsUpdateTransition();

		private static StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.ContainerImpl
			, ContainerState, ContainerEventType, ContainerEvent> stateMachineFactory = new 
			StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.ContainerImpl
			, ContainerState, ContainerEventType, ContainerEvent>(ContainerState.New).AddTransition
			(ContainerState.New, EnumSet.Of(ContainerState.Localizing, ContainerState.Localized
			, ContainerState.LocalizationFailed, ContainerState.Done), ContainerEventType.InitContainer
			, new ContainerImpl.RequestResourcesTransition()).AddTransition(ContainerState.New
			, ContainerState.New, ContainerEventType.UpdateDiagnosticsMsg, UpdateDiagnosticsTransition
			).AddTransition(ContainerState.New, ContainerState.Done, ContainerEventType.KillContainer
			, new ContainerImpl.KillOnNewTransition()).AddTransition(ContainerState.Localizing
			, EnumSet.Of(ContainerState.Localizing, ContainerState.Localized), ContainerEventType
			.ResourceLocalized, new ContainerImpl.LocalizedTransition()).AddTransition(ContainerState
			.Localizing, ContainerState.LocalizationFailed, ContainerEventType.ResourceFailed
			, new ContainerImpl.ResourceFailedTransition()).AddTransition(ContainerState.Localizing
			, ContainerState.Localizing, ContainerEventType.UpdateDiagnosticsMsg, UpdateDiagnosticsTransition
			).AddTransition(ContainerState.Localizing, ContainerState.Killing, ContainerEventType
			.KillContainer, new ContainerImpl.KillDuringLocalizationTransition()).AddTransition
			(ContainerState.LocalizationFailed, ContainerState.Done, ContainerEventType.ContainerResourcesCleanedup
			, new ContainerImpl.LocalizationFailedToDoneTransition()).AddTransition(ContainerState
			.LocalizationFailed, ContainerState.LocalizationFailed, ContainerEventType.UpdateDiagnosticsMsg
			, UpdateDiagnosticsTransition).AddTransition(ContainerState.LocalizationFailed, 
			ContainerState.LocalizationFailed, ContainerEventType.KillContainer).AddTransition
			(ContainerState.LocalizationFailed, ContainerState.LocalizationFailed, ContainerEventType
			.ResourceLocalized).AddTransition(ContainerState.LocalizationFailed, ContainerState
			.LocalizationFailed, ContainerEventType.ResourceFailed).AddTransition(ContainerState
			.Localized, ContainerState.Running, ContainerEventType.ContainerLaunched, new ContainerImpl.LaunchTransition
			()).AddTransition(ContainerState.Localized, ContainerState.ExitedWithFailure, ContainerEventType
			.ContainerExitedWithFailure, new ContainerImpl.ExitedWithFailureTransition(true)
			).AddTransition(ContainerState.Localized, ContainerState.Localized, ContainerEventType
			.UpdateDiagnosticsMsg, UpdateDiagnosticsTransition).AddTransition(ContainerState
			.Localized, ContainerState.Killing, ContainerEventType.KillContainer, new ContainerImpl.KillTransition
			()).AddTransition(ContainerState.Running, ContainerState.ExitedWithSuccess, ContainerEventType
			.ContainerExitedWithSuccess, new ContainerImpl.ExitedWithSuccessTransition(true)
			).AddTransition(ContainerState.Running, ContainerState.ExitedWithFailure, ContainerEventType
			.ContainerExitedWithFailure, new ContainerImpl.ExitedWithFailureTransition(true)
			).AddTransition(ContainerState.Running, ContainerState.Running, ContainerEventType
			.UpdateDiagnosticsMsg, UpdateDiagnosticsTransition).AddTransition(ContainerState
			.Running, ContainerState.Killing, ContainerEventType.KillContainer, new ContainerImpl.KillTransition
			()).AddTransition(ContainerState.Running, ContainerState.ExitedWithFailure, ContainerEventType
			.ContainerKilledOnRequest, new ContainerImpl.KilledExternallyTransition()).AddTransition
			(ContainerState.ExitedWithSuccess, ContainerState.Done, ContainerEventType.ContainerResourcesCleanedup
			, new ContainerImpl.ExitedWithSuccessToDoneTransition()).AddTransition(ContainerState
			.ExitedWithSuccess, ContainerState.ExitedWithSuccess, ContainerEventType.UpdateDiagnosticsMsg
			, UpdateDiagnosticsTransition).AddTransition(ContainerState.ExitedWithSuccess, ContainerState
			.ExitedWithSuccess, ContainerEventType.KillContainer).AddTransition(ContainerState
			.ExitedWithFailure, ContainerState.Done, ContainerEventType.ContainerResourcesCleanedup
			, new ContainerImpl.ExitedWithFailureToDoneTransition()).AddTransition(ContainerState
			.ExitedWithFailure, ContainerState.ExitedWithFailure, ContainerEventType.UpdateDiagnosticsMsg
			, UpdateDiagnosticsTransition).AddTransition(ContainerState.ExitedWithFailure, ContainerState
			.ExitedWithFailure, ContainerEventType.KillContainer).AddTransition(ContainerState
			.Killing, ContainerState.ContainerCleanedupAfterKill, ContainerEventType.ContainerKilledOnRequest
			, new ContainerImpl.ContainerKilledTransition()).AddTransition(ContainerState.Killing
			, ContainerState.Killing, ContainerEventType.ResourceLocalized, new ContainerImpl.LocalizedResourceDuringKillTransition
			()).AddTransition(ContainerState.Killing, ContainerState.Killing, ContainerEventType
			.ResourceFailed).AddTransition(ContainerState.Killing, ContainerState.Killing, ContainerEventType
			.UpdateDiagnosticsMsg, UpdateDiagnosticsTransition).AddTransition(ContainerState
			.Killing, ContainerState.Killing, ContainerEventType.KillContainer).AddTransition
			(ContainerState.Killing, ContainerState.ExitedWithSuccess, ContainerEventType.ContainerExitedWithSuccess
			, new ContainerImpl.ExitedWithSuccessTransition(false)).AddTransition(ContainerState
			.Killing, ContainerState.ExitedWithFailure, ContainerEventType.ContainerExitedWithFailure
			, new ContainerImpl.ExitedWithFailureTransition(false)).AddTransition(ContainerState
			.Killing, ContainerState.Done, ContainerEventType.ContainerResourcesCleanedup, new 
			ContainerImpl.KillingToDoneTransition()).AddTransition(ContainerState.Killing, ContainerState
			.Killing, ContainerEventType.ContainerLaunched).AddTransition(ContainerState.ContainerCleanedupAfterKill
			, ContainerState.Done, ContainerEventType.ContainerResourcesCleanedup, new ContainerImpl.ContainerCleanedupAfterKillToDoneTransition
			()).AddTransition(ContainerState.ContainerCleanedupAfterKill, ContainerState.ContainerCleanedupAfterKill
			, ContainerEventType.UpdateDiagnosticsMsg, UpdateDiagnosticsTransition).AddTransition
			(ContainerState.ContainerCleanedupAfterKill, ContainerState.ContainerCleanedupAfterKill
			, EnumSet.Of(ContainerEventType.KillContainer, ContainerEventType.ContainerExitedWithSuccess
			, ContainerEventType.ContainerExitedWithFailure)).AddTransition(ContainerState.Done
			, ContainerState.Done, ContainerEventType.KillContainer).AddTransition(ContainerState
			.Done, ContainerState.Done, ContainerEventType.InitContainer).AddTransition(ContainerState
			.Done, ContainerState.Done, ContainerEventType.UpdateDiagnosticsMsg, UpdateDiagnosticsTransition
			).AddTransition(ContainerState.Done, ContainerState.Done, EnumSet.Of(ContainerEventType
			.ResourceFailed, ContainerEventType.ContainerExitedWithSuccess, ContainerEventType
			.ContainerExitedWithFailure)).InstallTopology();

		private readonly StateMachine<ContainerState, ContainerEventType, ContainerEvent>
			 stateMachine;

		// State Machine for each container.
		// From NEW State
		// From LOCALIZING State
		// From LOCALIZATION_FAILED State
		// container not launched so kill is a no-op
		// container cleanup triggers a release of all resources
		// regardless of whether they were localized or not
		// LocalizedResource handles release event in all states
		// From LOCALIZED State
		// From RUNNING State
		// From CONTAINER_EXITED_WITH_SUCCESS State
		// From EXITED_WITH_FAILURE State
		// From KILLING State.
		// Handle a launched container during killing stage is a no-op
		// as cleanup container is always handled after launch container event
		// in the container launcher
		// From CONTAINER_CLEANEDUP_AFTER_KILL State.
		// From DONE
		// This transition may result when
		// we notify container of failed localization if localizer thread (for
		// that container) fails for some reason
		// create the topology tables
		public virtual ContainerState GetCurrentState()
		{
			switch (stateMachine.GetCurrentState())
			{
				case ContainerState.New:
				case ContainerState.Localizing:
				case ContainerState.LocalizationFailed:
				case ContainerState.Localized:
				case ContainerState.Running:
				case ContainerState.ExitedWithSuccess:
				case ContainerState.ExitedWithFailure:
				case ContainerState.Killing:
				case ContainerState.ContainerCleanedupAfterKill:
				case ContainerState.ContainerResourcesCleaningup:
				{
					return ContainerState.Running;
				}

				case ContainerState.Done:
				default:
				{
					return ContainerState.Complete;
				}
			}
		}

		public virtual string GetUser()
		{
			this.readLock.Lock();
			try
			{
				return this.user;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual IDictionary<Path, IList<string>> GetLocalizedResources()
		{
			this.readLock.Lock();
			try
			{
				if (ContainerState.Localized == GetContainerState())
				{
					return localizedResources;
				}
				else
				{
					return null;
				}
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual Credentials GetCredentials()
		{
			this.readLock.Lock();
			try
			{
				return credentials;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual ContainerState GetContainerState()
		{
			this.readLock.Lock();
			try
			{
				return stateMachine.GetCurrentState();
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual ContainerLaunchContext GetLaunchContext()
		{
			this.readLock.Lock();
			try
			{
				return launchContext;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual ContainerStatus CloneAndGetContainerStatus()
		{
			this.readLock.Lock();
			try
			{
				return BuilderUtils.NewContainerStatus(this.containerId, GetCurrentState(), diagnostics
					.ToString(), exitCode);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual NMContainerStatus GetNMContainerStatus()
		{
			this.readLock.Lock();
			try
			{
				return NMContainerStatus.NewInstance(this.containerId, GetCurrentState(), GetResource
					(), diagnostics.ToString(), exitCode, containerTokenIdentifier.GetPriority(), containerTokenIdentifier
					.GetCreationTime());
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual ContainerId GetContainerId()
		{
			return this.containerId;
		}

		public virtual Resource GetResource()
		{
			return this.resource;
		}

		public virtual ContainerTokenIdentifier GetContainerTokenIdentifier()
		{
			this.readLock.Lock();
			try
			{
				return this.containerTokenIdentifier;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		private void SendFinishedEvents()
		{
			// Inform the application
			EventHandler eventHandler = dispatcher.GetEventHandler();
			eventHandler.Handle(new ApplicationContainerFinishedEvent(containerId));
			// Remove the container from the resource-monitor
			eventHandler.Handle(new ContainerStopMonitoringEvent(containerId));
			// Tell the logService too
			eventHandler.Handle(new LogHandlerContainerFinishedEvent(containerId, exitCode));
		}

		private void SendLaunchEvent()
		{
			// dispatcher not typed
			ContainersLauncherEventType launcherEvent = ContainersLauncherEventType.LaunchContainer;
			if (recoveredStatus == NMStateStoreService.RecoveredContainerStatus.Launched)
			{
				// try to recover a container that was previously launched
				launcherEvent = ContainersLauncherEventType.RecoverContainer;
			}
			containerLaunchStartTime = clock.GetTime();
			dispatcher.GetEventHandler().Handle(new ContainersLauncherEvent(this, launcherEvent
				));
		}

		// Inform the ContainersMonitor to start monitoring the container's
		// resource usage.
		private void SendContainerMonitorStartEvent()
		{
			// dispatcher not typed
			long pmemBytes = GetResource().GetMemory() * 1024 * 1024L;
			float pmemRatio = daemonConf.GetFloat(YarnConfiguration.NmVmemPmemRatio, YarnConfiguration
				.DefaultNmVmemPmemRatio);
			long vmemBytes = (long)(pmemRatio * pmemBytes);
			int cpuVcores = GetResource().GetVirtualCores();
			dispatcher.GetEventHandler().Handle(new ContainerStartMonitoringEvent(containerId
				, vmemBytes, pmemBytes, cpuVcores));
		}

		private void AddDiagnostics(params string[] diags)
		{
			foreach (string s in diags)
			{
				this.diagnostics.Append(s);
			}
			try
			{
				stateStore.StoreContainerDiagnostics(containerId, diagnostics);
			}
			catch (IOException e)
			{
				Log.Warn("Unable to update diagnostics in state store for " + containerId, e);
			}
		}

		public virtual void Cleanup()
		{
			// dispatcher not typed
			IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> rsrc = new 
				Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
			if (!publicRsrcs.IsEmpty())
			{
				rsrc[LocalResourceVisibility.Public] = publicRsrcs;
			}
			if (!privateRsrcs.IsEmpty())
			{
				rsrc[LocalResourceVisibility.Private] = privateRsrcs;
			}
			if (!appRsrcs.IsEmpty())
			{
				rsrc[LocalResourceVisibility.Application] = appRsrcs;
			}
			dispatcher.GetEventHandler().Handle(new ContainerLocalizationCleanupEvent(this, rsrc
				));
		}

		internal class ContainerTransition : SingleArcTransition<ContainerImpl, ContainerEvent
			>
		{
			public virtual void Transition(ContainerImpl container, ContainerEvent @event)
			{
			}
			// Just drain the event and change the state.
		}

		/// <summary>
		/// State transition when a NEW container receives the INIT_CONTAINER
		/// message.
		/// </summary>
		/// <remarks>
		/// State transition when a NEW container receives the INIT_CONTAINER
		/// message.
		/// If there are resources to localize, sends a
		/// ContainerLocalizationRequest (INIT_CONTAINER_RESOURCES)
		/// to the ResourceLocalizationManager and enters LOCALIZING state.
		/// If there are no resources to localize, sends LAUNCH_CONTAINER event
		/// and enters LOCALIZED state directly.
		/// If there are any invalid resources specified, enters LOCALIZATION_FAILED
		/// directly.
		/// </remarks>
		internal class RequestResourcesTransition : MultipleArcTransition<ContainerImpl, 
			ContainerEvent, ContainerState>
		{
			// dispatcher not typed
			public virtual ContainerState Transition(ContainerImpl container, ContainerEvent 
				@event)
			{
				if (container.recoveredStatus == NMStateStoreService.RecoveredContainerStatus.Completed)
				{
					container.SendFinishedEvents();
					return ContainerState.Done;
				}
				else
				{
					if (container.recoveredAsKilled && container.recoveredStatus == NMStateStoreService.RecoveredContainerStatus
						.Requested)
					{
						// container was killed but never launched
						container.metrics.KilledContainer();
						NMAuditLogger.LogSuccess(container.user, NMAuditLogger.AuditConstants.FinishKilledContainer
							, "ContainerImpl", container.containerId.GetApplicationAttemptId().GetApplicationId
							(), container.containerId);
						container.metrics.ReleaseContainer(container.resource);
						container.SendFinishedEvents();
						return ContainerState.Done;
					}
				}
				ContainerLaunchContext ctxt = container.launchContext;
				container.metrics.InitingContainer();
				container.dispatcher.GetEventHandler().Handle(new AuxServicesEvent(AuxServicesEventType
					.ContainerInit, container));
				// Inform the AuxServices about the opaque serviceData
				IDictionary<string, ByteBuffer> csd = ctxt.GetServiceData();
				if (csd != null)
				{
					// This can happen more than once per Application as each container may
					// have distinct service data
					foreach (KeyValuePair<string, ByteBuffer> service in csd)
					{
						container.dispatcher.GetEventHandler().Handle(new AuxServicesEvent(AuxServicesEventType
							.ApplicationInit, container.user, container.containerId.GetApplicationAttemptId(
							).GetApplicationId(), service.Key.ToString(), service.Value));
					}
				}
				// Send requests for public, private resources
				IDictionary<string, LocalResource> cntrRsrc = ctxt.GetLocalResources();
				if (!cntrRsrc.IsEmpty())
				{
					try
					{
						foreach (KeyValuePair<string, LocalResource> rsrc in cntrRsrc)
						{
							try
							{
								LocalResourceRequest req = new LocalResourceRequest(rsrc.Value);
								IList<string> links = container.pendingResources[req];
								if (links == null)
								{
									links = new AList<string>();
									container.pendingResources[req] = links;
								}
								links.AddItem(rsrc.Key);
								StoreSharedCacheUploadPolicy(container, req, rsrc.Value.GetShouldBeUploadedToSharedCache
									());
								switch (rsrc.Value.GetVisibility())
								{
									case LocalResourceVisibility.Public:
									{
										container.publicRsrcs.AddItem(req);
										break;
									}

									case LocalResourceVisibility.Private:
									{
										container.privateRsrcs.AddItem(req);
										break;
									}

									case LocalResourceVisibility.Application:
									{
										container.appRsrcs.AddItem(req);
										break;
									}
								}
							}
							catch (URISyntaxException e)
							{
								Log.Info("Got exception parsing " + rsrc.Key + " and value " + rsrc.Value);
								throw;
							}
						}
					}
					catch (URISyntaxException e)
					{
						// malformed resource; abort container launch
						Log.Warn("Failed to parse resource-request", e);
						container.Cleanup();
						container.metrics.EndInitingContainer();
						return ContainerState.LocalizationFailed;
					}
					IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> req_1 = new 
						LinkedHashMap<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
					if (!container.publicRsrcs.IsEmpty())
					{
						req_1[LocalResourceVisibility.Public] = container.publicRsrcs;
					}
					if (!container.privateRsrcs.IsEmpty())
					{
						req_1[LocalResourceVisibility.Private] = container.privateRsrcs;
					}
					if (!container.appRsrcs.IsEmpty())
					{
						req_1[LocalResourceVisibility.Application] = container.appRsrcs;
					}
					container.dispatcher.GetEventHandler().Handle(new ContainerLocalizationRequestEvent
						(container, req_1));
					return ContainerState.Localizing;
				}
				else
				{
					container.SendLaunchEvent();
					container.metrics.EndInitingContainer();
					return ContainerState.Localized;
				}
			}
		}

		/// <summary>
		/// Store the resource's shared cache upload policies
		/// Given LocalResourceRequest can be shared across containers in
		/// LocalResourcesTrackerImpl, we preserve the upload policies here.
		/// </summary>
		/// <remarks>
		/// Store the resource's shared cache upload policies
		/// Given LocalResourceRequest can be shared across containers in
		/// LocalResourcesTrackerImpl, we preserve the upload policies here.
		/// In addition, it is possible for the application to create several
		/// "identical" LocalResources as part of
		/// ContainerLaunchContext.setLocalResources with different symlinks.
		/// There is a corner case where these "identical" local resources have
		/// different upload policies. For that scenario, upload policy will be set to
		/// true as long as there is at least one LocalResource entry with
		/// upload policy set to true.
		/// </remarks>
		private static void StoreSharedCacheUploadPolicy(ContainerImpl container, LocalResourceRequest
			 resourceRequest, bool uploadPolicy)
		{
			bool storedUploadPolicy = container.resourcesUploadPolicies[resourceRequest];
			if (storedUploadPolicy == null || (!storedUploadPolicy && uploadPolicy))
			{
				container.resourcesUploadPolicies[resourceRequest] = uploadPolicy;
			}
		}

		/// <summary>
		/// Transition when one of the requested resources for this container
		/// has been successfully localized.
		/// </summary>
		internal class LocalizedTransition : MultipleArcTransition<ContainerImpl, ContainerEvent
			, ContainerState>
		{
			public virtual ContainerState Transition(ContainerImpl container, ContainerEvent 
				@event)
			{
				ContainerResourceLocalizedEvent rsrcEvent = (ContainerResourceLocalizedEvent)@event;
				LocalResourceRequest resourceRequest = rsrcEvent.GetResource();
				Path location = rsrcEvent.GetLocation();
				IList<string> syms = Sharpen.Collections.Remove(container.pendingResources, resourceRequest
					);
				if (null == syms)
				{
					Log.Warn("Localized unknown resource " + resourceRequest + " for container " + container
						.containerId);
					System.Diagnostics.Debug.Assert(false);
					// fail container?
					return ContainerState.Localizing;
				}
				container.localizedResources[location] = syms;
				// check to see if this resource should be uploaded to the shared cache
				// as well
				if (ShouldBeUploadedToSharedCache(container, resourceRequest))
				{
					container.resourcesToBeUploaded[resourceRequest] = location;
				}
				if (!container.pendingResources.IsEmpty())
				{
					return ContainerState.Localizing;
				}
				container.dispatcher.GetEventHandler().Handle(new ContainerLocalizationEvent(LocalizationEventType
					.ContainerResourcesLocalized, container));
				container.SendLaunchEvent();
				container.metrics.EndInitingContainer();
				// If this is a recovered container that has already launched, skip
				// uploading resources to the shared cache. We do this to avoid uploading
				// the same resources multiple times. The tradeoff is that in the case of
				// a recovered container, there is a chance that resources don't get
				// uploaded into the shared cache. This is OK because resources are not
				// acknowledged by the SCM until they have been uploaded by the node
				// manager.
				if (container.recoveredStatus != NMStateStoreService.RecoveredContainerStatus.Launched
					 && container.recoveredStatus != NMStateStoreService.RecoveredContainerStatus.Completed)
				{
					// kick off uploads to the shared cache
					container.dispatcher.GetEventHandler().Handle(new SharedCacheUploadEvent(container
						.resourcesToBeUploaded, container.GetLaunchContext(), container.GetUser(), SharedCacheUploadEventType
						.Upload));
				}
				return ContainerState.Localized;
			}
		}

		/// <summary>
		/// Transition from LOCALIZED state to RUNNING state upon receiving
		/// a CONTAINER_LAUNCHED event
		/// </summary>
		internal class LaunchTransition : ContainerImpl.ContainerTransition
		{
			public override void Transition(ContainerImpl container, ContainerEvent @event)
			{
				container.SendContainerMonitorStartEvent();
				container.metrics.RunningContainer();
				container.wasLaunched = true;
				long duration = clock.GetTime() - container.containerLaunchStartTime;
				container.metrics.AddContainerLaunchDuration(duration);
				if (container.recoveredAsKilled)
				{
					Log.Info("Killing " + container.containerId + " due to recovered as killed");
					container.AddDiagnostics("Container recovered as killed.\n");
					container.dispatcher.GetEventHandler().Handle(new ContainersLauncherEvent(container
						, ContainersLauncherEventType.CleanupContainer));
				}
			}
		}

		/// <summary>
		/// Transition from RUNNING or KILLING state to EXITED_WITH_SUCCESS state
		/// upon EXITED_WITH_SUCCESS message.
		/// </summary>
		internal class ExitedWithSuccessTransition : ContainerImpl.ContainerTransition
		{
			internal bool clCleanupRequired;

			public ExitedWithSuccessTransition(bool clCleanupRequired)
			{
				// dispatcher not typed
				this.clCleanupRequired = clCleanupRequired;
			}

			public override void Transition(ContainerImpl container, ContainerEvent @event)
			{
				// Set exit code to 0 on success    	
				container.exitCode = 0;
				// TODO: Add containerWorkDir to the deletion service.
				if (clCleanupRequired)
				{
					container.dispatcher.GetEventHandler().Handle(new ContainersLauncherEvent(container
						, ContainersLauncherEventType.CleanupContainer));
				}
				container.Cleanup();
			}
		}

		/// <summary>
		/// Transition to EXITED_WITH_FAILURE state upon
		/// CONTAINER_EXITED_WITH_FAILURE state.
		/// </summary>
		internal class ExitedWithFailureTransition : ContainerImpl.ContainerTransition
		{
			internal bool clCleanupRequired;

			public ExitedWithFailureTransition(bool clCleanupRequired)
			{
				// dispatcher not typed
				this.clCleanupRequired = clCleanupRequired;
			}

			public override void Transition(ContainerImpl container, ContainerEvent @event)
			{
				ContainerExitEvent exitEvent = (ContainerExitEvent)@event;
				container.exitCode = exitEvent.GetExitCode();
				if (exitEvent.GetDiagnosticInfo() != null)
				{
					container.AddDiagnostics(exitEvent.GetDiagnosticInfo(), "\n");
				}
				// TODO: Add containerWorkDir to the deletion service.
				// TODO: Add containerOuputDir to the deletion service.
				if (clCleanupRequired)
				{
					container.dispatcher.GetEventHandler().Handle(new ContainersLauncherEvent(container
						, ContainersLauncherEventType.CleanupContainer));
				}
				container.Cleanup();
			}
		}

		/// <summary>Transition to EXITED_WITH_FAILURE upon receiving KILLED_ON_REQUEST</summary>
		internal class KilledExternallyTransition : ContainerImpl.ExitedWithFailureTransition
		{
			internal KilledExternallyTransition()
				: base(true)
			{
			}

			public override void Transition(ContainerImpl container, ContainerEvent @event)
			{
				base.Transition(container, @event);
				container.AddDiagnostics("Killed by external signal\n");
			}
		}

		/// <summary>
		/// Transition from LOCALIZING to LOCALIZATION_FAILED upon receiving
		/// RESOURCE_FAILED event.
		/// </summary>
		internal class ResourceFailedTransition : SingleArcTransition<ContainerImpl, ContainerEvent
			>
		{
			public virtual void Transition(ContainerImpl container, ContainerEvent @event)
			{
				ContainerResourceFailedEvent rsrcFailedEvent = (ContainerResourceFailedEvent)@event;
				container.AddDiagnostics(rsrcFailedEvent.GetDiagnosticMessage(), "\n");
				// Inform the localizer to decrement reference counts and cleanup
				// resources.
				container.Cleanup();
				container.metrics.EndInitingContainer();
			}
		}

		/// <summary>
		/// Transition from LOCALIZING to KILLING upon receiving
		/// KILL_CONTAINER event.
		/// </summary>
		internal class KillDuringLocalizationTransition : SingleArcTransition<ContainerImpl
			, ContainerEvent>
		{
			public virtual void Transition(ContainerImpl container, ContainerEvent @event)
			{
				// Inform the localizer to decrement reference counts and cleanup
				// resources.
				container.Cleanup();
				container.metrics.EndInitingContainer();
				ContainerKillEvent killEvent = (ContainerKillEvent)@event;
				container.exitCode = killEvent.GetContainerExitStatus();
				container.AddDiagnostics(killEvent.GetDiagnostic(), "\n");
				container.AddDiagnostics("Container is killed before being launched.\n");
			}
		}

		/// <summary>
		/// Remain in KILLING state when receiving a RESOURCE_LOCALIZED request
		/// while in the process of killing.
		/// </summary>
		internal class LocalizedResourceDuringKillTransition : SingleArcTransition<ContainerImpl
			, ContainerEvent>
		{
			public virtual void Transition(ContainerImpl container, ContainerEvent @event)
			{
				ContainerResourceLocalizedEvent rsrcEvent = (ContainerResourceLocalizedEvent)@event;
				IList<string> syms = Sharpen.Collections.Remove(container.pendingResources, rsrcEvent
					.GetResource());
				if (null == syms)
				{
					Log.Warn("Localized unknown resource " + rsrcEvent.GetResource() + " for container "
						 + container.containerId);
					System.Diagnostics.Debug.Assert(false);
					// fail container?
					return;
				}
				container.localizedResources[rsrcEvent.GetLocation()] = syms;
			}
		}

		/// <summary>
		/// Transitions upon receiving KILL_CONTAINER:
		/// - LOCALIZED -&gt; KILLING
		/// - RUNNING -&gt; KILLING
		/// </summary>
		internal class KillTransition : SingleArcTransition<ContainerImpl, ContainerEvent
			>
		{
			// dispatcher not typed
			public virtual void Transition(ContainerImpl container, ContainerEvent @event)
			{
				// Kill the process/process-grp
				container.dispatcher.GetEventHandler().Handle(new ContainersLauncherEvent(container
					, ContainersLauncherEventType.CleanupContainer));
				ContainerKillEvent killEvent = (ContainerKillEvent)@event;
				container.AddDiagnostics(killEvent.GetDiagnostic(), "\n");
				container.exitCode = killEvent.GetContainerExitStatus();
			}
		}

		/// <summary>
		/// Transition from KILLING to CONTAINER_CLEANEDUP_AFTER_KILL
		/// upon receiving CONTAINER_KILLED_ON_REQUEST.
		/// </summary>
		internal class ContainerKilledTransition : SingleArcTransition<ContainerImpl, ContainerEvent
			>
		{
			public virtual void Transition(ContainerImpl container, ContainerEvent @event)
			{
				ContainerExitEvent exitEvent = (ContainerExitEvent)@event;
				if (container.HasDefaultExitCode())
				{
					container.exitCode = exitEvent.GetExitCode();
				}
				if (exitEvent.GetDiagnosticInfo() != null)
				{
					container.AddDiagnostics(exitEvent.GetDiagnosticInfo(), "\n");
				}
				// The process/process-grp is killed. Decrement reference counts and
				// cleanup resources
				container.Cleanup();
			}
		}

		/// <summary>
		/// Handle the following transitions:
		/// - {LOCALIZATION_FAILED, EXITED_WITH_SUCCESS, EXITED_WITH_FAILURE,
		/// KILLING, CONTAINER_CLEANEDUP_AFTER_KILL}
		/// -&gt; DONE upon CONTAINER_RESOURCES_CLEANEDUP
		/// </summary>
		internal class ContainerDoneTransition : SingleArcTransition<ContainerImpl, ContainerEvent
			>
		{
			public virtual void Transition(ContainerImpl container, ContainerEvent @event)
			{
				container.metrics.ReleaseContainer(container.resource);
				container.SendFinishedEvents();
				//if the current state is NEW it means the CONTAINER_INIT was never 
				// sent for the event, thus no need to send the CONTAINER_STOP
				if (container.GetCurrentState() != ContainerState.New)
				{
					container.dispatcher.GetEventHandler().Handle(new AuxServicesEvent(AuxServicesEventType
						.ContainerStop, container));
				}
			}
		}

		/// <summary>
		/// Handle the following transition:
		/// - NEW -&gt; DONE upon KILL_CONTAINER
		/// </summary>
		internal class KillOnNewTransition : ContainerImpl.ContainerDoneTransition
		{
			public override void Transition(ContainerImpl container, ContainerEvent @event)
			{
				ContainerKillEvent killEvent = (ContainerKillEvent)@event;
				container.exitCode = killEvent.GetContainerExitStatus();
				container.AddDiagnostics(killEvent.GetDiagnostic(), "\n");
				container.AddDiagnostics("Container is killed before being launched.\n");
				container.metrics.KilledContainer();
				NMAuditLogger.LogSuccess(container.user, NMAuditLogger.AuditConstants.FinishKilledContainer
					, "ContainerImpl", container.containerId.GetApplicationAttemptId().GetApplicationId
					(), container.containerId);
				base.Transition(container, @event);
			}
		}

		/// <summary>
		/// Handle the following transition:
		/// - LOCALIZATION_FAILED -&gt; DONE upon CONTAINER_RESOURCES_CLEANEDUP
		/// </summary>
		internal class LocalizationFailedToDoneTransition : ContainerImpl.ContainerDoneTransition
		{
			public override void Transition(ContainerImpl container, ContainerEvent @event)
			{
				container.metrics.FailedContainer();
				NMAuditLogger.LogFailure(container.user, NMAuditLogger.AuditConstants.FinishFailedContainer
					, "ContainerImpl", "Container failed with state: " + container.GetContainerState
					(), container.containerId.GetApplicationAttemptId().GetApplicationId(), container
					.containerId);
				base.Transition(container, @event);
			}
		}

		/// <summary>
		/// Handle the following transition:
		/// - EXITED_WITH_SUCCESS -&gt; DONE upon CONTAINER_RESOURCES_CLEANEDUP
		/// </summary>
		internal class ExitedWithSuccessToDoneTransition : ContainerImpl.ContainerDoneTransition
		{
			public override void Transition(ContainerImpl container, ContainerEvent @event)
			{
				container.metrics.EndRunningContainer();
				container.metrics.CompletedContainer();
				NMAuditLogger.LogSuccess(container.user, NMAuditLogger.AuditConstants.FinishSuccessContainer
					, "ContainerImpl", container.containerId.GetApplicationAttemptId().GetApplicationId
					(), container.containerId);
				base.Transition(container, @event);
			}
		}

		/// <summary>
		/// Handle the following transition:
		/// - EXITED_WITH_FAILURE -&gt; DONE upon CONTAINER_RESOURCES_CLEANEDUP
		/// </summary>
		internal class ExitedWithFailureToDoneTransition : ContainerImpl.ContainerDoneTransition
		{
			public override void Transition(ContainerImpl container, ContainerEvent @event)
			{
				if (container.wasLaunched)
				{
					container.metrics.EndRunningContainer();
				}
				container.metrics.FailedContainer();
				NMAuditLogger.LogFailure(container.user, NMAuditLogger.AuditConstants.FinishFailedContainer
					, "ContainerImpl", "Container failed with state: " + container.GetContainerState
					(), container.containerId.GetApplicationAttemptId().GetApplicationId(), container
					.containerId);
				base.Transition(container, @event);
			}
		}

		/// <summary>
		/// Handle the following transition:
		/// - KILLING -&gt; DONE upon CONTAINER_RESOURCES_CLEANEDUP
		/// </summary>
		internal class KillingToDoneTransition : ContainerImpl.ContainerDoneTransition
		{
			public override void Transition(ContainerImpl container, ContainerEvent @event)
			{
				container.metrics.KilledContainer();
				NMAuditLogger.LogSuccess(container.user, NMAuditLogger.AuditConstants.FinishKilledContainer
					, "ContainerImpl", container.containerId.GetApplicationAttemptId().GetApplicationId
					(), container.containerId);
				base.Transition(container, @event);
			}
		}

		/// <summary>
		/// Handle the following transition:
		/// CONTAINER_CLEANEDUP_AFTER_KILL -&gt; DONE upon CONTAINER_RESOURCES_CLEANEDUP
		/// </summary>
		internal class ContainerCleanedupAfterKillToDoneTransition : ContainerImpl.ContainerDoneTransition
		{
			public override void Transition(ContainerImpl container, ContainerEvent @event)
			{
				if (container.wasLaunched)
				{
					container.metrics.EndRunningContainer();
				}
				container.metrics.KilledContainer();
				NMAuditLogger.LogSuccess(container.user, NMAuditLogger.AuditConstants.FinishKilledContainer
					, "ContainerImpl", container.containerId.GetApplicationAttemptId().GetApplicationId
					(), container.containerId);
				base.Transition(container, @event);
			}
		}

		/// <summary>Update diagnostics, staying in the same state.</summary>
		internal class ContainerDiagnosticsUpdateTransition : SingleArcTransition<ContainerImpl
			, ContainerEvent>
		{
			public virtual void Transition(ContainerImpl container, ContainerEvent @event)
			{
				ContainerDiagnosticsUpdateEvent updateEvent = (ContainerDiagnosticsUpdateEvent)@event;
				container.AddDiagnostics(updateEvent.GetDiagnosticsUpdate(), "\n");
				try
				{
					container.stateStore.StoreContainerDiagnostics(container.containerId, container.diagnostics
						);
				}
				catch (IOException e)
				{
					Log.Warn("Unable to update state store diagnostics for " + container.containerId, 
						e);
				}
			}
		}

		public virtual void Handle(ContainerEvent @event)
		{
			try
			{
				this.writeLock.Lock();
				ContainerId containerID = @event.GetContainerID();
				Log.Debug("Processing " + containerID + " of type " + @event.GetType());
				ContainerState oldState = stateMachine.GetCurrentState();
				ContainerState newState = null;
				try
				{
					newState = stateMachine.DoTransition(@event.GetType(), @event);
				}
				catch (InvalidStateTransitonException e)
				{
					Log.Warn("Can't handle this event at current state: Current: [" + oldState + "], eventType: ["
						 + @event.GetType() + "]", e);
				}
				if (oldState != newState)
				{
					Log.Info("Container " + containerID + " transitioned from " + oldState + " to " +
						 newState);
				}
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		public override string ToString()
		{
			this.readLock.Lock();
			try
			{
				return ConverterUtils.ToString(this.containerId);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		private bool HasDefaultExitCode()
		{
			return (this.exitCode == ContainerExitStatus.Invalid);
		}

		/// <summary>
		/// Returns whether the specific resource should be uploaded to the shared
		/// cache.
		/// </summary>
		private static bool ShouldBeUploadedToSharedCache(ContainerImpl container, LocalResourceRequest
			 resource)
		{
			return container.resourcesUploadPolicies[resource];
		}
	}
}
