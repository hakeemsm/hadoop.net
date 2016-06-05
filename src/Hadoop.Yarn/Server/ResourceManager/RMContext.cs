using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
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
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	/// <summary>Context of the ResourceManager.</summary>
	public interface RMContext
	{
		Dispatcher GetDispatcher();

		bool IsHAEnabled();

		HAServiceProtocol.HAServiceState GetHAServiceState();

		RMStateStore GetStateStore();

		ConcurrentMap<ApplicationId, RMApp> GetRMApps();

		ConcurrentMap<ApplicationId, ByteBuffer> GetSystemCredentialsForApps();

		ConcurrentMap<string, RMNode> GetInactiveRMNodes();

		ConcurrentMap<NodeId, RMNode> GetRMNodes();

		AMLivelinessMonitor GetAMLivelinessMonitor();

		AMLivelinessMonitor GetAMFinishingMonitor();

		ContainerAllocationExpirer GetContainerAllocationExpirer();

		DelegationTokenRenewer GetDelegationTokenRenewer();

		AMRMTokenSecretManager GetAMRMTokenSecretManager();

		RMContainerTokenSecretManager GetContainerTokenSecretManager();

		NMTokenSecretManagerInRM GetNMTokenSecretManager();

		ResourceScheduler GetScheduler();

		NodesListManager GetNodesListManager();

		ClientToAMTokenSecretManagerInRM GetClientToAMTokenSecretManager();

		AdminService GetRMAdminService();

		ClientRMService GetClientRMService();

		ApplicationMasterService GetApplicationMasterService();

		ResourceTrackerService GetResourceTrackerService();

		void SetClientRMService(ClientRMService clientRMService);

		RMDelegationTokenSecretManager GetRMDelegationTokenSecretManager();

		void SetRMDelegationTokenSecretManager(RMDelegationTokenSecretManager delegationTokenSecretManager
			);

		RMApplicationHistoryWriter GetRMApplicationHistoryWriter();

		void SetRMApplicationHistoryWriter(RMApplicationHistoryWriter rmApplicationHistoryWriter
			);

		void SetSystemMetricsPublisher(SystemMetricsPublisher systemMetricsPublisher);

		SystemMetricsPublisher GetSystemMetricsPublisher();

		ConfigurationProvider GetConfigurationProvider();

		bool IsWorkPreservingRecoveryEnabled();

		RMNodeLabelsManager GetNodeLabelManager();

		void SetNodeLabelManager(RMNodeLabelsManager mgr);

		long GetEpoch();

		ReservationSystem GetReservationSystem();

		bool IsSchedulerReadyForAllocatingContainers();

		Configuration GetYarnConfiguration();
	}
}
