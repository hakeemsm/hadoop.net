using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Http.Lib;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Security.Http;
using Org.Apache.Hadoop.Yarn.Server.Webproxy;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	/// <summary>The ResourceManager is the main class that is a set of components.</summary>
	/// <remarks>
	/// The ResourceManager is the main class that is a set of components.
	/// "I am the ResourceManager. All your resources belong to us..."
	/// </remarks>
	public class ResourceManager : CompositeService, Recoverable
	{
		/// <summary>Priority of the ResourceManager shutdown hook.</summary>
		public const int ShutdownHookPriority = 30;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.ResourceManager
			));

		private static long clusterTimeStamp = Runtime.CurrentTimeMillis();

		/// <summary>"Always On" services.</summary>
		/// <remarks>
		/// "Always On" services. Services that need to run always irrespective of
		/// the HA state of the RM.
		/// </remarks>
		[VisibleForTesting]
		protected internal RMContextImpl rmContext;

		private Dispatcher rmDispatcher;

		[VisibleForTesting]
		protected internal AdminService adminService;

		/// <summary>"Active" services.</summary>
		/// <remarks>
		/// "Active" services. Services that need to run only on the Active RM.
		/// These services are managed (initialized, started, stopped) by the
		/// <see cref="Org.Apache.Hadoop.Service.CompositeService"/>
		/// RMActiveServices.
		/// RM is active when (1) HA is disabled, or (2) HA is enabled and the RM is
		/// in Active state.
		/// </remarks>
		protected internal ResourceManager.RMActiveServices activeServices;

		protected internal RMSecretManagerService rmSecretManagerService;

		protected internal ResourceScheduler scheduler;

		protected internal ReservationSystem reservationSystem;

		private ClientRMService clientRM;

		protected internal ApplicationMasterService masterService;

		protected internal NMLivelinessMonitor nmLivelinessMonitor;

		protected internal NodesListManager nodesListManager;

		protected internal RMAppManager rmAppManager;

		protected internal ApplicationACLsManager applicationACLsManager;

		protected internal QueueACLsManager queueACLsManager;

		private WebApp webApp;

		private AppReportFetcher fetcher = null;

		protected internal ResourceTrackerService resourceTracker;

		[VisibleForTesting]
		protected internal string webAppAddress;

		private ConfigurationProvider configurationProvider = null;

		/// <summary>End of Active services</summary>
		private Configuration conf;

		private UserGroupInformation rmLoginUGI;

		public ResourceManager()
			: base("ResourceManager")
		{
		}

		public virtual RMContext GetRMContext()
		{
			return this.rmContext;
		}

		public static long GetClusterTimeStamp()
		{
			return clusterTimeStamp;
		}

		[VisibleForTesting]
		protected internal static void SetClusterTimeStamp(long timestamp)
		{
			clusterTimeStamp = timestamp;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.conf = conf;
			this.rmContext = new RMContextImpl();
			this.configurationProvider = ConfigurationProviderFactory.GetConfigurationProvider
				(conf);
			this.configurationProvider.Init(this.conf);
			rmContext.SetConfigurationProvider(configurationProvider);
			// load core-site.xml
			InputStream coreSiteXMLInputStream = this.configurationProvider.GetConfigurationInputStream
				(this.conf, YarnConfiguration.CoreSiteConfigurationFile);
			if (coreSiteXMLInputStream != null)
			{
				this.conf.AddResource(coreSiteXMLInputStream);
			}
			// Do refreshUserToGroupsMappings with loaded core-site.xml
			Groups.GetUserToGroupsMappingServiceWithLoadedConfiguration(this.conf).Refresh();
			// Do refreshSuperUserGroupsConfiguration with loaded core-site.xml
			// Or use RM specific configurations to overwrite the common ones first
			// if they exist
			RMServerUtils.ProcessRMProxyUsersConf(conf);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(this.conf);
			// load yarn-site.xml
			InputStream yarnSiteXMLInputStream = this.configurationProvider.GetConfigurationInputStream
				(this.conf, YarnConfiguration.YarnSiteConfigurationFile);
			if (yarnSiteXMLInputStream != null)
			{
				this.conf.AddResource(yarnSiteXMLInputStream);
			}
			ValidateConfigs(this.conf);
			// Set HA configuration should be done before login
			this.rmContext.SetHAEnabled(HAUtil.IsHAEnabled(this.conf));
			if (this.rmContext.IsHAEnabled())
			{
				HAUtil.VerifyAndSetConfiguration(this.conf);
			}
			// Set UGI and do login
			// If security is enabled, use login user
			// If security is not enabled, use current user
			this.rmLoginUGI = UserGroupInformation.GetCurrentUser();
			try
			{
				DoSecureLogin();
			}
			catch (IOException ie)
			{
				throw new YarnRuntimeException("Failed to login", ie);
			}
			// register the handlers for all AlwaysOn services using setupDispatcher().
			rmDispatcher = SetupDispatcher();
			AddIfService(rmDispatcher);
			rmContext.SetDispatcher(rmDispatcher);
			adminService = CreateAdminService();
			AddService(adminService);
			rmContext.SetRMAdminService(adminService);
			rmContext.SetYarnConfiguration(conf);
			CreateAndInitActiveServices();
			webAppAddress = WebAppUtils.GetWebAppBindURL(this.conf, YarnConfiguration.RmBindHost
				, WebAppUtils.GetRMWebAppURLWithoutScheme(this.conf));
			RMApplicationHistoryWriter rmApplicationHistoryWriter = CreateRMApplicationHistoryWriter
				();
			AddService(rmApplicationHistoryWriter);
			rmContext.SetRMApplicationHistoryWriter(rmApplicationHistoryWriter);
			SystemMetricsPublisher systemMetricsPublisher = CreateSystemMetricsPublisher();
			AddService(systemMetricsPublisher);
			rmContext.SetSystemMetricsPublisher(systemMetricsPublisher);
			base.ServiceInit(this.conf);
		}

		protected internal virtual QueueACLsManager CreateQueueACLsManager(ResourceScheduler
			 scheduler, Configuration conf)
		{
			return new QueueACLsManager(scheduler, conf);
		}

		[VisibleForTesting]
		protected internal virtual void SetRMStateStore(RMStateStore rmStore)
		{
			rmStore.SetRMDispatcher(rmDispatcher);
			rmStore.SetResourceManager(this);
			rmContext.SetStateStore(rmStore);
		}

		protected internal virtual EventHandler<SchedulerEvent> CreateSchedulerEventDispatcher
			()
		{
			return new ResourceManager.SchedulerEventDispatcher(this.scheduler);
		}

		protected internal virtual Dispatcher CreateDispatcher()
		{
			return new AsyncDispatcher();
		}

		protected internal virtual ResourceScheduler CreateScheduler()
		{
			string schedulerClassName = conf.Get(YarnConfiguration.RmScheduler, YarnConfiguration
				.DefaultRmScheduler);
			Log.Info("Using Scheduler: " + schedulerClassName);
			try
			{
				Type schedulerClazz = Sharpen.Runtime.GetType(schedulerClassName);
				if (typeof(ResourceScheduler).IsAssignableFrom(schedulerClazz))
				{
					return (ResourceScheduler)ReflectionUtils.NewInstance(schedulerClazz, this.conf);
				}
				else
				{
					throw new YarnRuntimeException("Class: " + schedulerClassName + " not instance of "
						 + typeof(ResourceScheduler).GetCanonicalName());
				}
			}
			catch (TypeLoadException e)
			{
				throw new YarnRuntimeException("Could not instantiate Scheduler: " + schedulerClassName
					, e);
			}
		}

		protected internal virtual ReservationSystem CreateReservationSystem()
		{
			string reservationClassName = conf.Get(YarnConfiguration.RmReservationSystemClass
				, AbstractReservationSystem.GetDefaultReservationSystem(scheduler));
			if (reservationClassName == null)
			{
				return null;
			}
			Log.Info("Using ReservationSystem: " + reservationClassName);
			try
			{
				Type reservationClazz = Sharpen.Runtime.GetType(reservationClassName);
				if (typeof(ReservationSystem).IsAssignableFrom(reservationClazz))
				{
					return (ReservationSystem)ReflectionUtils.NewInstance(reservationClazz, this.conf
						);
				}
				else
				{
					throw new YarnRuntimeException("Class: " + reservationClassName + " not instance of "
						 + typeof(ReservationSystem).GetCanonicalName());
				}
			}
			catch (TypeLoadException e)
			{
				throw new YarnRuntimeException("Could not instantiate ReservationSystem: " + reservationClassName
					, e);
			}
		}

		protected internal virtual ApplicationMasterLauncher CreateAMLauncher()
		{
			return new ApplicationMasterLauncher(this.rmContext);
		}

		private NMLivelinessMonitor CreateNMLivelinessMonitor()
		{
			return new NMLivelinessMonitor(this.rmContext.GetDispatcher());
		}

		protected internal virtual AMLivelinessMonitor CreateAMLivelinessMonitor()
		{
			return new AMLivelinessMonitor(this.rmDispatcher);
		}

		/// <exception cref="Sharpen.InstantiationException"/>
		/// <exception cref="System.MemberAccessException"/>
		protected internal virtual RMNodeLabelsManager CreateNodeLabelManager()
		{
			return new RMNodeLabelsManager();
		}

		protected internal virtual DelegationTokenRenewer CreateDelegationTokenRenewer()
		{
			return new DelegationTokenRenewer();
		}

		protected internal virtual RMAppManager CreateRMAppManager()
		{
			return new RMAppManager(this.rmContext, this.scheduler, this.masterService, this.
				applicationACLsManager, this.conf);
		}

		protected internal virtual RMApplicationHistoryWriter CreateRMApplicationHistoryWriter
			()
		{
			return new RMApplicationHistoryWriter();
		}

		protected internal virtual SystemMetricsPublisher CreateSystemMetricsPublisher()
		{
			return new SystemMetricsPublisher();
		}

		// sanity check for configurations
		protected internal static void ValidateConfigs(Configuration conf)
		{
			// validate max-attempts
			int globalMaxAppAttempts = conf.GetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration
				.DefaultRmAmMaxAttempts);
			if (globalMaxAppAttempts <= 0)
			{
				throw new YarnRuntimeException("Invalid global max attempts configuration" + ", "
					 + YarnConfiguration.RmAmMaxAttempts + "=" + globalMaxAppAttempts + ", it should be a positive integer."
					);
			}
			// validate expireIntvl >= heartbeatIntvl
			long expireIntvl = conf.GetLong(YarnConfiguration.RmNmExpiryIntervalMs, YarnConfiguration
				.DefaultRmNmExpiryIntervalMs);
			long heartbeatIntvl = conf.GetLong(YarnConfiguration.RmNmHeartbeatIntervalMs, YarnConfiguration
				.DefaultRmNmHeartbeatIntervalMs);
			if (expireIntvl < heartbeatIntvl)
			{
				throw new YarnRuntimeException("Nodemanager expiry interval should be no" + " less than heartbeat interval, "
					 + YarnConfiguration.RmNmExpiryIntervalMs + "=" + expireIntvl + ", " + YarnConfiguration
					.RmNmHeartbeatIntervalMs + "=" + heartbeatIntvl);
			}
		}

		/// <summary>RMActiveServices handles all the Active services in the RM.</summary>
		public class RMActiveServices : CompositeService
		{
			private DelegationTokenRenewer delegationTokenRenewer;

			private EventHandler<SchedulerEvent> schedulerDispatcher;

			private ApplicationMasterLauncher applicationMasterLauncher;

			private ContainerAllocationExpirer containerAllocationExpirer;

			private ResourceManager rm;

			private bool recoveryEnabled;

			private RMActiveServiceContext activeServiceContext;

			internal RMActiveServices(ResourceManager _enclosing, ResourceManager rm)
				: base("RMActiveServices")
			{
				this._enclosing = _enclosing;
				this.rm = rm;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration configuration)
			{
				this.activeServiceContext = new RMActiveServiceContext();
				this._enclosing.rmContext.SetActiveServiceContext(this.activeServiceContext);
				this._enclosing.conf.SetBoolean(Dispatcher.DispatcherExitOnErrorKey, true);
				this._enclosing.rmSecretManagerService = this._enclosing.CreateRMSecretManagerService
					();
				this.AddService(this._enclosing.rmSecretManagerService);
				this.containerAllocationExpirer = new ContainerAllocationExpirer(this._enclosing.
					rmDispatcher);
				this.AddService(this.containerAllocationExpirer);
				this._enclosing.rmContext.SetContainerAllocationExpirer(this.containerAllocationExpirer
					);
				AMLivelinessMonitor amLivelinessMonitor = this._enclosing.CreateAMLivelinessMonitor
					();
				this.AddService(amLivelinessMonitor);
				this._enclosing.rmContext.SetAMLivelinessMonitor(amLivelinessMonitor);
				AMLivelinessMonitor amFinishingMonitor = this._enclosing.CreateAMLivelinessMonitor
					();
				this.AddService(amFinishingMonitor);
				this._enclosing.rmContext.SetAMFinishingMonitor(amFinishingMonitor);
				RMNodeLabelsManager nlm = this._enclosing.CreateNodeLabelManager();
				nlm.SetRMContext(this._enclosing.rmContext);
				this.AddService(nlm);
				this._enclosing.rmContext.SetNodeLabelManager(nlm);
				bool isRecoveryEnabled = this._enclosing.conf.GetBoolean(YarnConfiguration.RecoveryEnabled
					, YarnConfiguration.DefaultRmRecoveryEnabled);
				RMStateStore rmStore = null;
				if (isRecoveryEnabled)
				{
					this.recoveryEnabled = true;
					rmStore = RMStateStoreFactory.GetStore(this._enclosing.conf);
					bool isWorkPreservingRecoveryEnabled = this._enclosing.conf.GetBoolean(YarnConfiguration
						.RmWorkPreservingRecoveryEnabled, YarnConfiguration.DefaultRmWorkPreservingRecoveryEnabled
						);
					this._enclosing.rmContext.SetWorkPreservingRecoveryEnabled(isWorkPreservingRecoveryEnabled
						);
				}
				else
				{
					this.recoveryEnabled = false;
					rmStore = new NullRMStateStore();
				}
				try
				{
					rmStore.Init(this._enclosing.conf);
					rmStore.SetRMDispatcher(this._enclosing.rmDispatcher);
					rmStore.SetResourceManager(this.rm);
				}
				catch (Exception e)
				{
					// the Exception from stateStore.init() needs to be handled for
					// HA and we need to give up master status if we got fenced
					ResourceManager.Log.Error("Failed to init state store", e);
					throw;
				}
				this._enclosing.rmContext.SetStateStore(rmStore);
				if (UserGroupInformation.IsSecurityEnabled())
				{
					this.delegationTokenRenewer = this._enclosing.CreateDelegationTokenRenewer();
					this._enclosing.rmContext.SetDelegationTokenRenewer(this.delegationTokenRenewer);
				}
				// Register event handler for NodesListManager
				this._enclosing.nodesListManager = new NodesListManager(this._enclosing.rmContext
					);
				this._enclosing.rmDispatcher.Register(typeof(NodesListManagerEventType), this._enclosing
					.nodesListManager);
				this.AddService(this._enclosing.nodesListManager);
				this._enclosing.rmContext.SetNodesListManager(this._enclosing.nodesListManager);
				// Initialize the scheduler
				this._enclosing.scheduler = this._enclosing.CreateScheduler();
				this._enclosing.scheduler.SetRMContext(this._enclosing.rmContext);
				this.AddIfService(this._enclosing.scheduler);
				this._enclosing.rmContext.SetScheduler(this._enclosing.scheduler);
				this.schedulerDispatcher = this._enclosing.CreateSchedulerEventDispatcher();
				this.AddIfService(this.schedulerDispatcher);
				this._enclosing.rmDispatcher.Register(typeof(SchedulerEventType), this.schedulerDispatcher
					);
				// Register event handler for RmAppEvents
				this._enclosing.rmDispatcher.Register(typeof(RMAppEventType), new ResourceManager.ApplicationEventDispatcher
					(this._enclosing.rmContext));
				// Register event handler for RmAppAttemptEvents
				this._enclosing.rmDispatcher.Register(typeof(RMAppAttemptEventType), new ResourceManager.ApplicationAttemptEventDispatcher
					(this._enclosing.rmContext));
				// Register event handler for RmNodes
				this._enclosing.rmDispatcher.Register(typeof(RMNodeEventType), new ResourceManager.NodeEventDispatcher
					(this._enclosing.rmContext));
				this._enclosing.nmLivelinessMonitor = this._enclosing.CreateNMLivelinessMonitor();
				this.AddService(this._enclosing.nmLivelinessMonitor);
				this._enclosing.resourceTracker = this._enclosing.CreateResourceTrackerService();
				this.AddService(this._enclosing.resourceTracker);
				this._enclosing.rmContext.SetResourceTrackerService(this._enclosing.resourceTracker
					);
				DefaultMetricsSystem.Initialize("ResourceManager");
				JvmMetrics.InitSingleton("ResourceManager", null);
				// Initialize the Reservation system
				if (this._enclosing.conf.GetBoolean(YarnConfiguration.RmReservationSystemEnable, 
					YarnConfiguration.DefaultRmReservationSystemEnable))
				{
					this._enclosing.reservationSystem = this._enclosing.CreateReservationSystem();
					if (this._enclosing.reservationSystem != null)
					{
						this._enclosing.reservationSystem.SetRMContext(this._enclosing.rmContext);
						this.AddIfService(this._enclosing.reservationSystem);
						this._enclosing.rmContext.SetReservationSystem(this._enclosing.reservationSystem);
						ResourceManager.Log.Info("Initialized Reservation system");
					}
				}
				// creating monitors that handle preemption
				this.CreatePolicyMonitors();
				this._enclosing.masterService = this._enclosing.CreateApplicationMasterService();
				this.AddService(this._enclosing.masterService);
				this._enclosing.rmContext.SetApplicationMasterService(this._enclosing.masterService
					);
				this._enclosing.applicationACLsManager = new ApplicationACLsManager(this._enclosing
					.conf);
				this._enclosing.queueACLsManager = this._enclosing.CreateQueueACLsManager(this._enclosing
					.scheduler, this._enclosing.conf);
				this._enclosing.rmAppManager = this._enclosing.CreateRMAppManager();
				// Register event handler for RMAppManagerEvents
				this._enclosing.rmDispatcher.Register(typeof(RMAppManagerEventType), this._enclosing
					.rmAppManager);
				this._enclosing.clientRM = this._enclosing.CreateClientRMService();
				this.AddService(this._enclosing.clientRM);
				this._enclosing.rmContext.SetClientRMService(this._enclosing.clientRM);
				this.applicationMasterLauncher = this._enclosing.CreateAMLauncher();
				this._enclosing.rmDispatcher.Register(typeof(AMLauncherEventType), this.applicationMasterLauncher
					);
				this.AddService(this.applicationMasterLauncher);
				if (UserGroupInformation.IsSecurityEnabled())
				{
					this.AddService(this.delegationTokenRenewer);
					this.delegationTokenRenewer.SetRMContext(this._enclosing.rmContext);
				}
				new RMNMInfo(this._enclosing.rmContext, this._enclosing.scheduler);
				base.ServiceInit(this._enclosing.conf);
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				RMStateStore rmStore = this._enclosing.rmContext.GetStateStore();
				// The state store needs to start irrespective of recoveryEnabled as apps
				// need events to move to further states.
				rmStore.Start();
				if (this.recoveryEnabled)
				{
					try
					{
						ResourceManager.Log.Info("Recovery started");
						rmStore.CheckVersion();
						if (this._enclosing.rmContext.IsWorkPreservingRecoveryEnabled())
						{
							this._enclosing.rmContext.SetEpoch(rmStore.GetAndIncrementEpoch());
						}
						RMStateStore.RMState state = rmStore.LoadState();
						this._enclosing.Recover(state);
						ResourceManager.Log.Info("Recovery ended");
					}
					catch (Exception e)
					{
						// the Exception from loadState() needs to be handled for
						// HA and we need to give up master status if we got fenced
						ResourceManager.Log.Error("Failed to load/recover state", e);
						throw;
					}
				}
				base.ServiceStart();
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				base.ServiceStop();
				DefaultMetricsSystem.Shutdown();
				if (this._enclosing.rmContext != null)
				{
					RMStateStore store = this._enclosing.rmContext.GetStateStore();
					try
					{
						store.Close();
					}
					catch (Exception e)
					{
						ResourceManager.Log.Error("Error closing store.", e);
					}
				}
			}

			protected internal virtual void CreatePolicyMonitors()
			{
				if (this._enclosing.scheduler is PreemptableResourceScheduler && this._enclosing.
					conf.GetBoolean(YarnConfiguration.RmSchedulerEnableMonitors, YarnConfiguration.DefaultRmSchedulerEnableMonitors
					))
				{
					ResourceManager.Log.Info("Loading policy monitors");
					IList<SchedulingEditPolicy> policies = this._enclosing.conf.GetInstances<SchedulingEditPolicy
						>(YarnConfiguration.RmSchedulerMonitorPolicies);
					if (policies.Count > 0)
					{
						foreach (SchedulingEditPolicy policy in policies)
						{
							ResourceManager.Log.Info("LOADING SchedulingEditPolicy:" + policy.GetPolicyName()
								);
							// periodically check whether we need to take action to guarantee
							// constraints
							SchedulingMonitor mon = new SchedulingMonitor(this._enclosing.rmContext, policy);
							this.AddService(mon);
						}
					}
					else
					{
						ResourceManager.Log.Warn("Policy monitors configured (" + YarnConfiguration.RmSchedulerEnableMonitors
							 + ") but none specified (" + YarnConfiguration.RmSchedulerMonitorPolicies + ")"
							);
					}
				}
			}

			private readonly ResourceManager _enclosing;
		}

		public class SchedulerEventDispatcher : AbstractService, EventHandler<SchedulerEvent
			>
		{
			private readonly ResourceScheduler scheduler;

			private readonly BlockingQueue<SchedulerEvent> eventQueue = new LinkedBlockingQueue
				<SchedulerEvent>();

			private readonly Sharpen.Thread eventProcessor;

			private volatile bool stopped = false;

			private bool shouldExitOnError = false;

			public SchedulerEventDispatcher(ResourceScheduler scheduler)
				: base(typeof(ResourceManager.SchedulerEventDispatcher).FullName)
			{
				this.scheduler = scheduler;
				this.eventProcessor = new Sharpen.Thread(new ResourceManager.SchedulerEventDispatcher.EventProcessor
					(this));
				this.eventProcessor.SetName("ResourceManager Event Processor");
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
				this.shouldExitOnError = conf.GetBoolean(Dispatcher.DispatcherExitOnErrorKey, Dispatcher
					.DefaultDispatcherExitOnError);
				base.ServiceInit(conf);
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				this.eventProcessor.Start();
				base.ServiceStart();
			}

			private sealed class EventProcessor : Runnable
			{
				public void Run()
				{
					SchedulerEvent @event;
					while (!this._enclosing.stopped && !Sharpen.Thread.CurrentThread().IsInterrupted(
						))
					{
						try
						{
							@event = this._enclosing.eventQueue.Take();
						}
						catch (Exception e)
						{
							ResourceManager.Log.Error("Returning, interrupted : " + e);
							return;
						}
						// TODO: Kill RM.
						try
						{
							this._enclosing.scheduler.Handle(@event);
						}
						catch (Exception t)
						{
							// An error occurred, but we are shutting down anyway.
							// If it was an InterruptedException, the very act of 
							// shutdown could have caused it and is probably harmless.
							if (this._enclosing.stopped)
							{
								ResourceManager.Log.Warn("Exception during shutdown: ", t);
								break;
							}
							ResourceManager.Log.Fatal("Error in handling event type " + @event.GetType() + " to the scheduler"
								, t);
							if (this._enclosing.shouldExitOnError && !ShutdownHookManager.Get().IsShutdownInProgress
								())
							{
								ResourceManager.Log.Info("Exiting, bbye..");
								System.Environment.Exit(-1);
							}
						}
					}
				}

				internal EventProcessor(SchedulerEventDispatcher _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly SchedulerEventDispatcher _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				this.stopped = true;
				this.eventProcessor.Interrupt();
				try
				{
					this.eventProcessor.Join();
				}
				catch (Exception e)
				{
					throw new YarnRuntimeException(e);
				}
				base.ServiceStop();
			}

			public virtual void Handle(SchedulerEvent @event)
			{
				try
				{
					int qSize = eventQueue.Count;
					if (qSize != 0 && qSize % 1000 == 0)
					{
						Log.Info("Size of scheduler event-queue is " + qSize);
					}
					int remCapacity = eventQueue.RemainingCapacity();
					if (remCapacity < 1000)
					{
						Log.Info("Very low remaining capacity on scheduler event queue: " + remCapacity);
					}
					this.eventQueue.Put(@event);
				}
				catch (Exception)
				{
					Log.Info("Interrupted. Trying to exit gracefully.");
				}
			}
		}

		public class RMFatalEventDispatcher : EventHandler<RMFatalEvent>
		{
			public virtual void Handle(RMFatalEvent @event)
			{
				Log.Fatal("Received a " + typeof(RMFatalEvent).FullName + " of type " + @event.GetType
					().ToString() + ". Cause:\n" + @event.GetCause());
				ExitUtil.Terminate(1, @event.GetCause());
			}
		}

		public virtual void HandleTransitionToStandBy()
		{
			if (rmContext.IsHAEnabled())
			{
				try
				{
					// Transition to standby and reinit active services
					Log.Info("Transitioning RM to Standby mode");
					TransitionToStandby(true);
					adminService.ResetLeaderElection();
					return;
				}
				catch (Exception e)
				{
					Log.Fatal("Failed to transition RM to Standby mode.");
					ExitUtil.Terminate(1, e);
				}
			}
		}

		public sealed class ApplicationEventDispatcher : EventHandler<RMAppEvent>
		{
			private readonly RMContext rmContext;

			public ApplicationEventDispatcher(RMContext rmContext)
			{
				this.rmContext = rmContext;
			}

			public void Handle(RMAppEvent @event)
			{
				ApplicationId appID = @event.GetApplicationId();
				RMApp rmApp = this.rmContext.GetRMApps()[appID];
				if (rmApp != null)
				{
					try
					{
						rmApp.Handle(@event);
					}
					catch (Exception t)
					{
						Log.Error("Error in handling event type " + @event.GetType() + " for application "
							 + appID, t);
					}
				}
			}
		}

		public sealed class ApplicationAttemptEventDispatcher : EventHandler<RMAppAttemptEvent
			>
		{
			private readonly RMContext rmContext;

			public ApplicationAttemptEventDispatcher(RMContext rmContext)
			{
				this.rmContext = rmContext;
			}

			public void Handle(RMAppAttemptEvent @event)
			{
				ApplicationAttemptId appAttemptID = @event.GetApplicationAttemptId();
				ApplicationId appAttemptId = appAttemptID.GetApplicationId();
				RMApp rmApp = this.rmContext.GetRMApps()[appAttemptId];
				if (rmApp != null)
				{
					RMAppAttempt rmAppAttempt = rmApp.GetRMAppAttempt(appAttemptID);
					if (rmAppAttempt != null)
					{
						try
						{
							rmAppAttempt.Handle(@event);
						}
						catch (Exception t)
						{
							Log.Error("Error in handling event type " + @event.GetType() + " for applicationAttempt "
								 + appAttemptId, t);
						}
					}
				}
			}
		}

		public sealed class NodeEventDispatcher : EventHandler<RMNodeEvent>
		{
			private readonly RMContext rmContext;

			public NodeEventDispatcher(RMContext rmContext)
			{
				this.rmContext = rmContext;
			}

			public void Handle(RMNodeEvent @event)
			{
				NodeId nodeId = @event.GetNodeId();
				RMNode node = this.rmContext.GetRMNodes()[nodeId];
				if (node != null)
				{
					try
					{
						((EventHandler<RMNodeEvent>)node).Handle(@event);
					}
					catch (Exception t)
					{
						Log.Error("Error in handling event type " + @event.GetType() + " for node " + nodeId
							, t);
					}
				}
			}
		}

		protected internal virtual void StartWepApp()
		{
			// Use the customized yarn filter instead of the standard kerberos filter to
			// allow users to authenticate using delegation tokens
			// 4 conditions need to be satisfied -
			// 1. security is enabled
			// 2. http auth type is set to kerberos
			// 3. "yarn.resourcemanager.webapp.use-yarn-filter" override is set to true
			// 4. hadoop.http.filter.initializers container AuthenticationFilterInitializer
			Configuration conf = GetConfig();
			bool enableCorsFilter = conf.GetBoolean(YarnConfiguration.RmWebappEnableCorsFilter
				, YarnConfiguration.DefaultRmWebappEnableCorsFilter);
			bool useYarnAuthenticationFilter = conf.GetBoolean(YarnConfiguration.RmWebappDelegationTokenAuthFilter
				, YarnConfiguration.DefaultRmWebappDelegationTokenAuthFilter);
			string authPrefix = "hadoop.http.authentication.";
			string authTypeKey = authPrefix + "type";
			string filterInitializerConfKey = "hadoop.http.filter.initializers";
			string actualInitializers = string.Empty;
			Type[] initializersClasses = conf.GetClasses(filterInitializerConfKey);
			// setup CORS
			if (enableCorsFilter)
			{
				conf.SetBoolean(HttpCrossOriginFilterInitializer.Prefix + HttpCrossOriginFilterInitializer
					.EnabledSuffix, true);
			}
			bool hasHadoopAuthFilterInitializer = false;
			bool hasRMAuthFilterInitializer = false;
			if (initializersClasses != null)
			{
				foreach (Type initializer in initializersClasses)
				{
					if (initializer.FullName.Equals(typeof(AuthenticationFilterInitializer).FullName))
					{
						hasHadoopAuthFilterInitializer = true;
					}
					if (initializer.FullName.Equals(typeof(RMAuthenticationFilterInitializer).FullName
						))
					{
						hasRMAuthFilterInitializer = true;
					}
				}
				if (UserGroupInformation.IsSecurityEnabled() && useYarnAuthenticationFilter && hasHadoopAuthFilterInitializer
					 && conf.Get(authTypeKey, string.Empty).Equals(KerberosAuthenticationHandler.Type
					))
				{
					AList<string> target = new AList<string>();
					foreach (Type filterInitializer in initializersClasses)
					{
						if (filterInitializer.FullName.Equals(typeof(AuthenticationFilterInitializer).FullName
							))
						{
							if (hasRMAuthFilterInitializer == false)
							{
								target.AddItem(typeof(RMAuthenticationFilterInitializer).FullName);
							}
							continue;
						}
						target.AddItem(filterInitializer.FullName);
					}
					actualInitializers = StringUtils.Join(",", target);
					Log.Info("Using RM authentication filter(kerberos/delegation-token)" + " for RM webapp authentication"
						);
					RMAuthenticationFilter.SetDelegationTokenSecretManager(GetClientRMService().rmDTSecretManager
						);
					conf.Set(filterInitializerConfKey, actualInitializers);
				}
			}
			// if security is not enabled and the default filter initializer has not 
			// been set, set the initializer to include the
			// RMAuthenticationFilterInitializer which in turn will set up the simple
			// auth filter.
			string initializers = conf.Get(filterInitializerConfKey);
			if (!UserGroupInformation.IsSecurityEnabled())
			{
				if (initializersClasses == null || initializersClasses.Length == 0)
				{
					conf.Set(filterInitializerConfKey, typeof(RMAuthenticationFilterInitializer).FullName
						);
					conf.Set(authTypeKey, "simple");
				}
				else
				{
					if (initializers.Equals(typeof(StaticUserWebFilter).FullName))
					{
						conf.Set(filterInitializerConfKey, typeof(RMAuthenticationFilterInitializer).FullName
							 + "," + initializers);
						conf.Set(authTypeKey, "simple");
					}
				}
			}
			WebApps.Builder<ApplicationMasterService> builder = WebApps.$for<ApplicationMasterService
				>("cluster", masterService, "ws").With(conf).WithHttpSpnegoPrincipalKey(YarnConfiguration
				.RmWebappSpnegoUserNameKey).WithHttpSpnegoKeytabKey(YarnConfiguration.RmWebappSpnegoKeytabFileKey
				).At(webAppAddress);
			string proxyHostAndPort = WebAppUtils.GetProxyHostAndPort(conf);
			if (WebAppUtils.GetResolvedRMWebAppURLWithoutScheme(conf).Equals(proxyHostAndPort
				))
			{
				if (HAUtil.IsHAEnabled(conf))
				{
					fetcher = new AppReportFetcher(conf);
				}
				else
				{
					fetcher = new AppReportFetcher(conf, GetClientRMService());
				}
				builder.WithServlet(ProxyUriUtils.ProxyServletName, ProxyUriUtils.ProxyPathSpec, 
					typeof(WebAppProxyServlet));
				builder.WithAttribute(WebAppProxy.FetcherAttribute, fetcher);
				string[] proxyParts = proxyHostAndPort.Split(":");
				builder.WithAttribute(WebAppProxy.ProxyHostAttribute, proxyParts[0]);
			}
			webApp = builder.Start(new RMWebApp(this));
		}

		/// <summary>
		/// Helper method to create and init
		/// <see cref="activeServices"/>
		/// . This creates an
		/// instance of
		/// <see cref="RMActiveServices"/>
		/// and initializes it.
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected internal virtual void CreateAndInitActiveServices()
		{
			activeServices = new ResourceManager.RMActiveServices(this, this);
			activeServices.Init(conf);
		}

		/// <summary>
		/// Helper method to start
		/// <see cref="activeServices"/>
		/// .
		/// </summary>
		/// <exception cref="System.Exception"/>
		internal virtual void StartActiveServices()
		{
			if (activeServices != null)
			{
				clusterTimeStamp = Runtime.CurrentTimeMillis();
				activeServices.Start();
			}
		}

		/// <summary>
		/// Helper method to stop
		/// <see cref="activeServices"/>
		/// .
		/// </summary>
		/// <exception cref="System.Exception"/>
		internal virtual void StopActiveServices()
		{
			if (activeServices != null)
			{
				activeServices.Stop();
				activeServices = null;
			}
		}

		/// <exception cref="System.Exception"/>
		internal virtual void Reinitialize(bool initialize)
		{
			ClusterMetrics.Destroy();
			QueueMetrics.ClearQueueMetrics();
			if (initialize)
			{
				ResetDispatcher();
				CreateAndInitActiveServices();
			}
		}

		[VisibleForTesting]
		protected internal virtual bool AreActiveServicesRunning()
		{
			return activeServices != null && activeServices.IsInState(Service.STATE.Started);
		}

		/// <exception cref="System.Exception"/>
		internal virtual void TransitionToActive()
		{
			lock (this)
			{
				if (rmContext.GetHAServiceState() == HAServiceProtocol.HAServiceState.Active)
				{
					Log.Info("Already in active state");
					return;
				}
				Log.Info("Transitioning to active state");
				this.rmLoginUGI.DoAs(new _PrivilegedExceptionAction_1001(this));
				rmContext.SetHAServiceState(HAServiceProtocol.HAServiceState.Active);
				Log.Info("Transitioned to active state");
			}
		}

		private sealed class _PrivilegedExceptionAction_1001 : PrivilegedExceptionAction<
			Void>
		{
			public _PrivilegedExceptionAction_1001(ResourceManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				try
				{
					this._enclosing.StartActiveServices();
					return null;
				}
				catch (Exception e)
				{
					this._enclosing.Reinitialize(true);
					throw;
				}
			}

			private readonly ResourceManager _enclosing;
		}

		/// <exception cref="System.Exception"/>
		internal virtual void TransitionToStandby(bool initialize)
		{
			lock (this)
			{
				if (rmContext.GetHAServiceState() == HAServiceProtocol.HAServiceState.Standby)
				{
					Log.Info("Already in standby state");
					return;
				}
				Log.Info("Transitioning to standby state");
				HAServiceProtocol.HAServiceState state = rmContext.GetHAServiceState();
				rmContext.SetHAServiceState(HAServiceProtocol.HAServiceState.Standby);
				if (state == HAServiceProtocol.HAServiceState.Active)
				{
					StopActiveServices();
					Reinitialize(initialize);
				}
				Log.Info("Transitioned to standby state");
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			if (this.rmContext.IsHAEnabled())
			{
				TransitionToStandby(true);
			}
			else
			{
				TransitionToActive();
			}
			StartWepApp();
			if (GetConfig().GetBoolean(YarnConfiguration.IsMiniYarnCluster, false))
			{
				int port = webApp.Port();
				WebAppUtils.SetRMWebAppPort(conf, port);
			}
			base.ServiceStart();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void DoSecureLogin()
		{
			IPEndPoint socAddr = GetBindAddress(conf);
			SecurityUtil.Login(this.conf, YarnConfiguration.RmKeytab, YarnConfiguration.RmPrincipal
				, socAddr.GetHostName());
			// if security is enable, set rmLoginUGI as UGI of loginUser
			if (UserGroupInformation.IsSecurityEnabled())
			{
				this.rmLoginUGI = UserGroupInformation.GetLoginUser();
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (webApp != null)
			{
				webApp.Stop();
			}
			if (fetcher != null)
			{
				fetcher.Stop();
			}
			if (configurationProvider != null)
			{
				configurationProvider.Close();
			}
			base.ServiceStop();
			TransitionToStandby(false);
			rmContext.SetHAServiceState(HAServiceProtocol.HAServiceState.Stopping);
		}

		protected internal virtual ResourceTrackerService CreateResourceTrackerService()
		{
			return new ResourceTrackerService(this.rmContext, this.nodesListManager, this.nmLivelinessMonitor
				, this.rmContext.GetContainerTokenSecretManager(), this.rmContext.GetNMTokenSecretManager
				());
		}

		protected internal virtual ClientRMService CreateClientRMService()
		{
			return new ClientRMService(this.rmContext, scheduler, this.rmAppManager, this.applicationACLsManager
				, this.queueACLsManager, this.rmContext.GetRMDelegationTokenSecretManager());
		}

		protected internal virtual ApplicationMasterService CreateApplicationMasterService
			()
		{
			return new ApplicationMasterService(this.rmContext, scheduler);
		}

		protected internal virtual AdminService CreateAdminService()
		{
			return new AdminService(this, rmContext);
		}

		protected internal virtual RMSecretManagerService CreateRMSecretManagerService()
		{
			return new RMSecretManagerService(conf, rmContext);
		}

		[InterfaceAudience.Private]
		public virtual ClientRMService GetClientRMService()
		{
			return this.clientRM;
		}

		/// <summary>return the scheduler.</summary>
		/// <returns>the scheduler for the Resource Manager.</returns>
		[InterfaceAudience.Private]
		public virtual ResourceScheduler GetResourceScheduler()
		{
			return this.scheduler;
		}

		/// <summary>return the resource tracking component.</summary>
		/// <returns>the resource tracking component.</returns>
		[InterfaceAudience.Private]
		public virtual ResourceTrackerService GetResourceTrackerService()
		{
			return this.resourceTracker;
		}

		[InterfaceAudience.Private]
		public virtual ApplicationMasterService GetApplicationMasterService()
		{
			return this.masterService;
		}

		[InterfaceAudience.Private]
		public virtual ApplicationACLsManager GetApplicationACLsManager()
		{
			return this.applicationACLsManager;
		}

		[InterfaceAudience.Private]
		public virtual QueueACLsManager GetQueueACLsManager()
		{
			return this.queueACLsManager;
		}

		[InterfaceAudience.Private]
		internal virtual WebApp GetWebapp()
		{
			return this.webApp;
		}

		/// <exception cref="System.Exception"/>
		public virtual void Recover(RMStateStore.RMState state)
		{
			// recover RMdelegationTokenSecretManager
			rmContext.GetRMDelegationTokenSecretManager().Recover(state);
			// recover AMRMTokenSecretManager
			rmContext.GetAMRMTokenSecretManager().Recover(state);
			// recover applications
			rmAppManager.Recover(state);
			SetSchedulerRecoveryStartAndWaitTime(state, conf);
		}

		public static void Main(string[] argv)
		{
			Sharpen.Thread.SetDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler
				());
			StringUtils.StartupShutdownMessage(typeof(ResourceManager), argv, Log);
			try
			{
				Configuration conf = new YarnConfiguration();
				GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
				argv = hParser.GetRemainingArgs();
				// If -format-state-store, then delete RMStateStore; else startup normally
				if (argv.Length == 1 && argv[0].Equals("-format-state-store"))
				{
					DeleteRMStateStore(conf);
				}
				else
				{
					ResourceManager resourceManager = new ResourceManager();
					ShutdownHookManager.Get().AddShutdownHook(new CompositeService.CompositeServiceShutdownHook
						(resourceManager), ShutdownHookPriority);
					resourceManager.Init(conf);
					resourceManager.Start();
				}
			}
			catch (Exception t)
			{
				Log.Fatal("Error starting ResourceManager", t);
				System.Environment.Exit(-1);
			}
		}

		/// <summary>Register the handlers for alwaysOn services</summary>
		private Dispatcher SetupDispatcher()
		{
			Dispatcher dispatcher = CreateDispatcher();
			dispatcher.Register(typeof(RMFatalEventType), new ResourceManager.RMFatalEventDispatcher
				());
			return dispatcher;
		}

		private void ResetDispatcher()
		{
			Dispatcher dispatcher = SetupDispatcher();
			((Org.Apache.Hadoop.Service.Service)dispatcher).Init(this.conf);
			((Org.Apache.Hadoop.Service.Service)dispatcher).Start();
			RemoveService((Org.Apache.Hadoop.Service.Service)rmDispatcher);
			// Need to stop previous rmDispatcher before assigning new dispatcher
			// otherwise causes "AsyncDispatcher event handler" thread leak
			((Org.Apache.Hadoop.Service.Service)rmDispatcher).Stop();
			rmDispatcher = dispatcher;
			AddIfService(rmDispatcher);
			rmContext.SetDispatcher(rmDispatcher);
		}

		private void SetSchedulerRecoveryStartAndWaitTime(RMStateStore.RMState state, Configuration
			 conf)
		{
			if (!state.GetApplicationState().IsEmpty())
			{
				long waitTime = conf.GetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs
					, YarnConfiguration.DefaultRmWorkPreservingRecoverySchedulingWaitMs);
				rmContext.SetSchedulerRecoveryStartAndWaitTime(waitTime);
			}
		}

		/// <summary>Retrieve RM bind address from configuration</summary>
		/// <param name="conf"/>
		/// <returns>InetSocketAddress</returns>
		public static IPEndPoint GetBindAddress(Configuration conf)
		{
			return conf.GetSocketAddr(YarnConfiguration.RmAddress, YarnConfiguration.DefaultRmAddress
				, YarnConfiguration.DefaultRmPort);
		}

		/// <summary>Deletes the RMStateStore</summary>
		/// <param name="conf"/>
		/// <exception cref="System.Exception"/>
		private static void DeleteRMStateStore(Configuration conf)
		{
			RMStateStore rmStore = RMStateStoreFactory.GetStore(conf);
			rmStore.Init(conf);
			rmStore.Start();
			try
			{
				Log.Info("Deleting ResourceManager state store...");
				rmStore.DeleteStore();
				Log.Info("State store deleted");
			}
			finally
			{
				rmStore.Stop();
			}
		}
	}
}
