using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
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
	/// <summary>
	/// The RMActiveServiceContext is the class that maintains all the
	/// RMActiveService contexts.This is expected to be used only by ResourceManager
	/// and RMContext.
	/// </summary>
	public class RMActiveServiceContext
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.RMActiveServiceContext
			));

		private readonly ConcurrentMap<ApplicationId, RMApp> applications = new ConcurrentHashMap
			<ApplicationId, RMApp>();

		private readonly ConcurrentMap<NodeId, RMNode> nodes = new ConcurrentHashMap<NodeId
			, RMNode>();

		private readonly ConcurrentMap<string, RMNode> inactiveNodes = new ConcurrentHashMap
			<string, RMNode>();

		private readonly ConcurrentMap<ApplicationId, ByteBuffer> systemCredentials = new 
			ConcurrentHashMap<ApplicationId, ByteBuffer>();

		private bool isWorkPreservingRecoveryEnabled;

		private AMLivelinessMonitor amLivelinessMonitor;

		private AMLivelinessMonitor amFinishingMonitor;

		private RMStateStore stateStore = null;

		private ContainerAllocationExpirer containerAllocationExpirer;

		private DelegationTokenRenewer delegationTokenRenewer;

		private AMRMTokenSecretManager amRMTokenSecretManager;

		private RMContainerTokenSecretManager containerTokenSecretManager;

		private NMTokenSecretManagerInRM nmTokenSecretManager;

		private ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager;

		private ClientRMService clientRMService;

		private RMDelegationTokenSecretManager rmDelegationTokenSecretManager;

		private ResourceScheduler scheduler;

		private ReservationSystem reservationSystem;

		private NodesListManager nodesListManager;

		private ResourceTrackerService resourceTrackerService;

		private ApplicationMasterService applicationMasterService;

		private RMNodeLabelsManager nodeLabelManager;

		private long epoch;

		private Clock systemClock = new SystemClock();

		private long schedulerRecoveryStartTime = 0;

		private long schedulerRecoveryWaitTime = 0;

		private bool printLog = true;

		private bool isSchedulerReady = false;

		public RMActiveServiceContext()
		{
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public RMActiveServiceContext(Dispatcher rmDispatcher, ContainerAllocationExpirer
			 containerAllocationExpirer, AMLivelinessMonitor amLivelinessMonitor, AMLivelinessMonitor
			 amFinishingMonitor, DelegationTokenRenewer delegationTokenRenewer, AMRMTokenSecretManager
			 appTokenSecretManager, RMContainerTokenSecretManager containerTokenSecretManager
			, NMTokenSecretManagerInRM nmTokenSecretManager, ClientToAMTokenSecretManagerInRM
			 clientToAMTokenSecretManager, ResourceScheduler scheduler)
			: this()
		{
			this.SetContainerAllocationExpirer(containerAllocationExpirer);
			this.SetAMLivelinessMonitor(amLivelinessMonitor);
			this.SetAMFinishingMonitor(amFinishingMonitor);
			this.SetDelegationTokenRenewer(delegationTokenRenewer);
			this.SetAMRMTokenSecretManager(appTokenSecretManager);
			this.SetContainerTokenSecretManager(containerTokenSecretManager);
			this.SetNMTokenSecretManager(nmTokenSecretManager);
			this.SetClientToAMTokenSecretManager(clientToAMTokenSecretManager);
			this.SetScheduler(scheduler);
			RMStateStore nullStore = new NullRMStateStore();
			nullStore.SetRMDispatcher(rmDispatcher);
			try
			{
				nullStore.Init(new YarnConfiguration());
				SetStateStore(nullStore);
			}
			catch (Exception)
			{
				System.Diagnostics.Debug.Assert(false);
			}
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual void SetStateStore(RMStateStore store)
		{
			stateStore = store;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual ClientRMService GetClientRMService()
		{
			return clientRMService;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual ApplicationMasterService GetApplicationMasterService()
		{
			return applicationMasterService;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual ResourceTrackerService GetResourceTrackerService()
		{
			return resourceTrackerService;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual RMStateStore GetStateStore()
		{
			return stateStore;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual ConcurrentMap<ApplicationId, RMApp> GetRMApps()
		{
			return this.applications;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual ConcurrentMap<NodeId, RMNode> GetRMNodes()
		{
			return this.nodes;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual ConcurrentMap<string, RMNode> GetInactiveRMNodes()
		{
			return this.inactiveNodes;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual ContainerAllocationExpirer GetContainerAllocationExpirer()
		{
			return this.containerAllocationExpirer;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual AMLivelinessMonitor GetAMLivelinessMonitor()
		{
			return this.amLivelinessMonitor;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual AMLivelinessMonitor GetAMFinishingMonitor()
		{
			return this.amFinishingMonitor;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual DelegationTokenRenewer GetDelegationTokenRenewer()
		{
			return delegationTokenRenewer;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual AMRMTokenSecretManager GetAMRMTokenSecretManager()
		{
			return this.amRMTokenSecretManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual RMContainerTokenSecretManager GetContainerTokenSecretManager()
		{
			return this.containerTokenSecretManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual NMTokenSecretManagerInRM GetNMTokenSecretManager()
		{
			return this.nmTokenSecretManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual ResourceScheduler GetScheduler()
		{
			return this.scheduler;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual ReservationSystem GetReservationSystem()
		{
			return this.reservationSystem;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual NodesListManager GetNodesListManager()
		{
			return this.nodesListManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual ClientToAMTokenSecretManagerInRM GetClientToAMTokenSecretManager()
		{
			return this.clientToAMTokenSecretManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual void SetClientRMService(ClientRMService clientRMService)
		{
			this.clientRMService = clientRMService;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual RMDelegationTokenSecretManager GetRMDelegationTokenSecretManager()
		{
			return this.rmDelegationTokenSecretManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual void SetRMDelegationTokenSecretManager(RMDelegationTokenSecretManager
			 delegationTokenSecretManager)
		{
			this.rmDelegationTokenSecretManager = delegationTokenSecretManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetContainerAllocationExpirer(ContainerAllocationExpirer containerAllocationExpirer
			)
		{
			this.containerAllocationExpirer = containerAllocationExpirer;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetAMLivelinessMonitor(AMLivelinessMonitor amLivelinessMonitor
			)
		{
			this.amLivelinessMonitor = amLivelinessMonitor;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetAMFinishingMonitor(AMLivelinessMonitor amFinishingMonitor
			)
		{
			this.amFinishingMonitor = amFinishingMonitor;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetContainerTokenSecretManager(RMContainerTokenSecretManager
			 containerTokenSecretManager)
		{
			this.containerTokenSecretManager = containerTokenSecretManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetNMTokenSecretManager(NMTokenSecretManagerInRM nmTokenSecretManager
			)
		{
			this.nmTokenSecretManager = nmTokenSecretManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetScheduler(ResourceScheduler scheduler)
		{
			this.scheduler = scheduler;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetReservationSystem(ReservationSystem reservationSystem)
		{
			this.reservationSystem = reservationSystem;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetDelegationTokenRenewer(DelegationTokenRenewer delegationTokenRenewer
			)
		{
			this.delegationTokenRenewer = delegationTokenRenewer;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetClientToAMTokenSecretManager(ClientToAMTokenSecretManagerInRM
			 clientToAMTokenSecretManager)
		{
			this.clientToAMTokenSecretManager = clientToAMTokenSecretManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetAMRMTokenSecretManager(AMRMTokenSecretManager amRMTokenSecretManager
			)
		{
			this.amRMTokenSecretManager = amRMTokenSecretManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetNodesListManager(NodesListManager nodesListManager)
		{
			this.nodesListManager = nodesListManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetApplicationMasterService(ApplicationMasterService applicationMasterService
			)
		{
			this.applicationMasterService = applicationMasterService;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetResourceTrackerService(ResourceTrackerService resourceTrackerService
			)
		{
			this.resourceTrackerService = resourceTrackerService;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual void SetWorkPreservingRecoveryEnabled(bool enabled)
		{
			this.isWorkPreservingRecoveryEnabled = enabled;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual bool IsWorkPreservingRecoveryEnabled()
		{
			return this.isWorkPreservingRecoveryEnabled;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual long GetEpoch()
		{
			return this.epoch;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual void SetEpoch(long epoch)
		{
			this.epoch = epoch;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual RMNodeLabelsManager GetNodeLabelManager()
		{
			return nodeLabelManager;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual void SetNodeLabelManager(RMNodeLabelsManager mgr)
		{
			nodeLabelManager = mgr;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual void SetSchedulerRecoveryStartAndWaitTime(long waitTime)
		{
			this.schedulerRecoveryStartTime = systemClock.GetTime();
			this.schedulerRecoveryWaitTime = waitTime;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual bool IsSchedulerReadyForAllocatingContainers()
		{
			if (isSchedulerReady)
			{
				return isSchedulerReady;
			}
			isSchedulerReady = (systemClock.GetTime() - schedulerRecoveryStartTime) > schedulerRecoveryWaitTime;
			if (!isSchedulerReady && printLog)
			{
				Log.Info("Skip allocating containers. Scheduler is waiting for recovery.");
				printLog = false;
			}
			if (isSchedulerReady)
			{
				Log.Info("Scheduler recovery is done. Start allocating new containers.");
			}
			return isSchedulerReady;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual void SetSystemClock(Clock clock)
		{
			this.systemClock = clock;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual ConcurrentMap<ApplicationId, ByteBuffer> GetSystemCredentialsForApps
			()
		{
			return systemCredentials;
		}
	}
}
