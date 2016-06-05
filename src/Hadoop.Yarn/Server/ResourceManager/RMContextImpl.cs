using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class RMContextImpl : RMContext
	{
		private Dispatcher rmDispatcher;

		private bool isHAEnabled;

		private HAServiceProtocol.HAServiceState haServiceState = HAServiceProtocol.HAServiceState
			.Initializing;

		private AdminService adminService;

		private ConfigurationProvider configurationProvider;

		private RMActiveServiceContext activeServiceContext;

		private Configuration yarnConfiguration;

		private RMApplicationHistoryWriter rmApplicationHistoryWriter;

		private SystemMetricsPublisher systemMetricsPublisher;

		/// <summary>Default constructor.</summary>
		/// <remarks>
		/// Default constructor. To be used in conjunction with setter methods for
		/// individual fields.
		/// </remarks>
		public RMContextImpl()
		{
		}

		[VisibleForTesting]
		public RMContextImpl(Dispatcher rmDispatcher, ContainerAllocationExpirer containerAllocationExpirer
			, AMLivelinessMonitor amLivelinessMonitor, AMLivelinessMonitor amFinishingMonitor
			, DelegationTokenRenewer delegationTokenRenewer, AMRMTokenSecretManager appTokenSecretManager
			, RMContainerTokenSecretManager containerTokenSecretManager, NMTokenSecretManagerInRM
			 nmTokenSecretManager, ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager
			, ResourceScheduler scheduler)
			: this()
		{
			// helper constructor for tests
			this.SetDispatcher(rmDispatcher);
			SetActiveServiceContext(new RMActiveServiceContext(rmDispatcher, containerAllocationExpirer
				, amLivelinessMonitor, amFinishingMonitor, delegationTokenRenewer, appTokenSecretManager
				, containerTokenSecretManager, nmTokenSecretManager, clientToAMTokenSecretManager
				, scheduler));
			ConfigurationProvider provider = new LocalConfigurationProvider();
			SetConfigurationProvider(provider);
		}

		[VisibleForTesting]
		public RMContextImpl(Dispatcher rmDispatcher, ContainerAllocationExpirer containerAllocationExpirer
			, AMLivelinessMonitor amLivelinessMonitor, AMLivelinessMonitor amFinishingMonitor
			, DelegationTokenRenewer delegationTokenRenewer, AMRMTokenSecretManager appTokenSecretManager
			, RMContainerTokenSecretManager containerTokenSecretManager, NMTokenSecretManagerInRM
			 nmTokenSecretManager, ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager
			)
			: this(rmDispatcher, containerAllocationExpirer, amLivelinessMonitor, amFinishingMonitor
				, delegationTokenRenewer, appTokenSecretManager, containerTokenSecretManager, nmTokenSecretManager
				, clientToAMTokenSecretManager, null)
		{
		}

		// helper constructor for tests
		public virtual Dispatcher GetDispatcher()
		{
			return this.rmDispatcher;
		}

		public virtual RMStateStore GetStateStore()
		{
			return activeServiceContext.GetStateStore();
		}

		public virtual ConcurrentMap<ApplicationId, RMApp> GetRMApps()
		{
			return activeServiceContext.GetRMApps();
		}

		public virtual ConcurrentMap<NodeId, RMNode> GetRMNodes()
		{
			return activeServiceContext.GetRMNodes();
		}

		public virtual ConcurrentMap<string, RMNode> GetInactiveRMNodes()
		{
			return activeServiceContext.GetInactiveRMNodes();
		}

		public virtual ContainerAllocationExpirer GetContainerAllocationExpirer()
		{
			return activeServiceContext.GetContainerAllocationExpirer();
		}

		public virtual AMLivelinessMonitor GetAMLivelinessMonitor()
		{
			return activeServiceContext.GetAMLivelinessMonitor();
		}

		public virtual AMLivelinessMonitor GetAMFinishingMonitor()
		{
			return activeServiceContext.GetAMFinishingMonitor();
		}

		public virtual DelegationTokenRenewer GetDelegationTokenRenewer()
		{
			return activeServiceContext.GetDelegationTokenRenewer();
		}

		public virtual AMRMTokenSecretManager GetAMRMTokenSecretManager()
		{
			return activeServiceContext.GetAMRMTokenSecretManager();
		}

		public virtual RMContainerTokenSecretManager GetContainerTokenSecretManager()
		{
			return activeServiceContext.GetContainerTokenSecretManager();
		}

		public virtual NMTokenSecretManagerInRM GetNMTokenSecretManager()
		{
			return activeServiceContext.GetNMTokenSecretManager();
		}

		public virtual ResourceScheduler GetScheduler()
		{
			return activeServiceContext.GetScheduler();
		}

		public virtual ReservationSystem GetReservationSystem()
		{
			return activeServiceContext.GetReservationSystem();
		}

		public virtual NodesListManager GetNodesListManager()
		{
			return activeServiceContext.GetNodesListManager();
		}

		public virtual ClientToAMTokenSecretManagerInRM GetClientToAMTokenSecretManager()
		{
			return activeServiceContext.GetClientToAMTokenSecretManager();
		}

		public virtual AdminService GetRMAdminService()
		{
			return this.adminService;
		}

		[VisibleForTesting]
		public virtual void SetStateStore(RMStateStore store)
		{
			activeServiceContext.SetStateStore(store);
		}

		public virtual ClientRMService GetClientRMService()
		{
			return activeServiceContext.GetClientRMService();
		}

		public virtual ApplicationMasterService GetApplicationMasterService()
		{
			return activeServiceContext.GetApplicationMasterService();
		}

		public virtual ResourceTrackerService GetResourceTrackerService()
		{
			return activeServiceContext.GetResourceTrackerService();
		}

		internal virtual void SetHAEnabled(bool isHAEnabled)
		{
			this.isHAEnabled = isHAEnabled;
		}

		internal virtual void SetHAServiceState(HAServiceProtocol.HAServiceState haServiceState
			)
		{
			lock (haServiceState)
			{
				this.haServiceState = haServiceState;
			}
		}

		internal virtual void SetDispatcher(Dispatcher dispatcher)
		{
			this.rmDispatcher = dispatcher;
		}

		internal virtual void SetRMAdminService(AdminService adminService)
		{
			this.adminService = adminService;
		}

		public virtual void SetClientRMService(ClientRMService clientRMService)
		{
			activeServiceContext.SetClientRMService(clientRMService);
		}

		public virtual RMDelegationTokenSecretManager GetRMDelegationTokenSecretManager()
		{
			return activeServiceContext.GetRMDelegationTokenSecretManager();
		}

		public virtual void SetRMDelegationTokenSecretManager(RMDelegationTokenSecretManager
			 delegationTokenSecretManager)
		{
			activeServiceContext.SetRMDelegationTokenSecretManager(delegationTokenSecretManager
				);
		}

		internal virtual void SetContainerAllocationExpirer(ContainerAllocationExpirer containerAllocationExpirer
			)
		{
			activeServiceContext.SetContainerAllocationExpirer(containerAllocationExpirer);
		}

		internal virtual void SetAMLivelinessMonitor(AMLivelinessMonitor amLivelinessMonitor
			)
		{
			activeServiceContext.SetAMLivelinessMonitor(amLivelinessMonitor);
		}

		internal virtual void SetAMFinishingMonitor(AMLivelinessMonitor amFinishingMonitor
			)
		{
			activeServiceContext.SetAMFinishingMonitor(amFinishingMonitor);
		}

		internal virtual void SetContainerTokenSecretManager(RMContainerTokenSecretManager
			 containerTokenSecretManager)
		{
			activeServiceContext.SetContainerTokenSecretManager(containerTokenSecretManager);
		}

		internal virtual void SetNMTokenSecretManager(NMTokenSecretManagerInRM nmTokenSecretManager
			)
		{
			activeServiceContext.SetNMTokenSecretManager(nmTokenSecretManager);
		}

		internal virtual void SetScheduler(ResourceScheduler scheduler)
		{
			activeServiceContext.SetScheduler(scheduler);
		}

		internal virtual void SetReservationSystem(ReservationSystem reservationSystem)
		{
			activeServiceContext.SetReservationSystem(reservationSystem);
		}

		internal virtual void SetDelegationTokenRenewer(DelegationTokenRenewer delegationTokenRenewer
			)
		{
			activeServiceContext.SetDelegationTokenRenewer(delegationTokenRenewer);
		}

		internal virtual void SetClientToAMTokenSecretManager(ClientToAMTokenSecretManagerInRM
			 clientToAMTokenSecretManager)
		{
			activeServiceContext.SetClientToAMTokenSecretManager(clientToAMTokenSecretManager
				);
		}

		internal virtual void SetAMRMTokenSecretManager(AMRMTokenSecretManager amRMTokenSecretManager
			)
		{
			activeServiceContext.SetAMRMTokenSecretManager(amRMTokenSecretManager);
		}

		internal virtual void SetNodesListManager(NodesListManager nodesListManager)
		{
			activeServiceContext.SetNodesListManager(nodesListManager);
		}

		internal virtual void SetApplicationMasterService(ApplicationMasterService applicationMasterService
			)
		{
			activeServiceContext.SetApplicationMasterService(applicationMasterService);
		}

		internal virtual void SetResourceTrackerService(ResourceTrackerService resourceTrackerService
			)
		{
			activeServiceContext.SetResourceTrackerService(resourceTrackerService);
		}

		public virtual bool IsHAEnabled()
		{
			return isHAEnabled;
		}

		public virtual HAServiceProtocol.HAServiceState GetHAServiceState()
		{
			lock (haServiceState)
			{
				return haServiceState;
			}
		}

		public virtual void SetWorkPreservingRecoveryEnabled(bool enabled)
		{
			activeServiceContext.SetWorkPreservingRecoveryEnabled(enabled);
		}

		public virtual bool IsWorkPreservingRecoveryEnabled()
		{
			return activeServiceContext.IsWorkPreservingRecoveryEnabled();
		}

		public virtual RMApplicationHistoryWriter GetRMApplicationHistoryWriter()
		{
			return this.rmApplicationHistoryWriter;
		}

		public virtual void SetSystemMetricsPublisher(SystemMetricsPublisher systemMetricsPublisher
			)
		{
			this.systemMetricsPublisher = systemMetricsPublisher;
		}

		public virtual SystemMetricsPublisher GetSystemMetricsPublisher()
		{
			return this.systemMetricsPublisher;
		}

		public virtual void SetRMApplicationHistoryWriter(RMApplicationHistoryWriter rmApplicationHistoryWriter
			)
		{
			this.rmApplicationHistoryWriter = rmApplicationHistoryWriter;
		}

		public virtual ConfigurationProvider GetConfigurationProvider()
		{
			return this.configurationProvider;
		}

		public virtual void SetConfigurationProvider(ConfigurationProvider configurationProvider
			)
		{
			this.configurationProvider = configurationProvider;
		}

		public virtual long GetEpoch()
		{
			return activeServiceContext.GetEpoch();
		}

		internal virtual void SetEpoch(long epoch)
		{
			activeServiceContext.SetEpoch(epoch);
		}

		public virtual RMNodeLabelsManager GetNodeLabelManager()
		{
			return activeServiceContext.GetNodeLabelManager();
		}

		public virtual void SetNodeLabelManager(RMNodeLabelsManager mgr)
		{
			activeServiceContext.SetNodeLabelManager(mgr);
		}

		public virtual void SetSchedulerRecoveryStartAndWaitTime(long waitTime)
		{
			activeServiceContext.SetSchedulerRecoveryStartAndWaitTime(waitTime);
		}

		public virtual bool IsSchedulerReadyForAllocatingContainers()
		{
			return activeServiceContext.IsSchedulerReadyForAllocatingContainers();
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual void SetSystemClock(Clock clock)
		{
			activeServiceContext.SetSystemClock(clock);
		}

		public virtual ConcurrentMap<ApplicationId, ByteBuffer> GetSystemCredentialsForApps
			()
		{
			return activeServiceContext.GetSystemCredentialsForApps();
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual RMActiveServiceContext GetActiveServiceContext()
		{
			return activeServiceContext;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetActiveServiceContext(RMActiveServiceContext activeServiceContext
			)
		{
			this.activeServiceContext = activeServiceContext;
		}

		public virtual Configuration GetYarnConfiguration()
		{
			return this.yarnConfiguration;
		}

		public virtual void SetYarnConfiguration(Configuration yarnConfiguration)
		{
			this.yarnConfiguration = yarnConfiguration;
		}
	}
}
