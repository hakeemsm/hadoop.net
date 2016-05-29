using System;
using System.Collections.Generic;
using System.Net;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Conf
{
	public class YarnConfiguration : Configuration
	{
		[InterfaceAudience.Private]
		public const string CsConfigurationFile = "capacity-scheduler.xml";

		[InterfaceAudience.Private]
		public const string HadoopPolicyConfigurationFile = "hadoop-policy.xml";

		[InterfaceAudience.Private]
		public const string YarnSiteConfigurationFile = "yarn-site.xml";

		private const string YarnDefaultConfigurationFile = "yarn-default.xml";

		[InterfaceAudience.Private]
		public const string CoreSiteConfigurationFile = "core-site.xml";

		[InterfaceAudience.Private]
		public static readonly IList<string> RmConfigurationFiles = Sharpen.Collections.UnmodifiableList
			(Arrays.AsList(CsConfigurationFile, HadoopPolicyConfigurationFile, YarnSiteConfigurationFile
			, CoreSiteConfigurationFile));

		[InterfaceStability.Evolving]
		public const int ApplicationMaxTags = 10;

		[InterfaceStability.Evolving]
		public const int ApplicationMaxTagLength = 100;

		static YarnConfiguration()
		{
			AddDeprecatedKeys();
			Configuration.AddDefaultResource(YarnDefaultConfigurationFile);
			Configuration.AddDefaultResource(YarnSiteConfigurationFile);
		}

		private static void AddDeprecatedKeys()
		{
			Configuration.AddDeprecations(new Configuration.DeprecationDelta[] { new Configuration.DeprecationDelta
				("yarn.client.max-nodemanagers-proxies", NmClientMaxNmProxies) });
		}

		public const string YarnPrefix = "yarn.";

		/// <summary>Delay before deleting resource to ease debugging of NM issues</summary>
		public const string DebugNmDeleteDelaySec = Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration
			.NmPrefix + "delete.debug-delay-sec";

		public const string IpcPrefix = YarnPrefix + "ipc.";

		/// <summary>Factory to create client IPC classes.</summary>
		public const string IpcClientFactoryClass = IpcPrefix + "client.factory.class";

		public const string DefaultIpcClientFactoryClass = "org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl";

		/// <summary>Factory to create server IPC classes.</summary>
		public const string IpcServerFactoryClass = IpcPrefix + "server.factory.class";

		public const string DefaultIpcServerFactoryClass = "org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl";

		/// <summary>Factory to create serializeable records.</summary>
		public const string IpcRecordFactoryClass = IpcPrefix + "record.factory.class";

		public const string DefaultIpcRecordFactoryClass = "org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl";

		/// <summary>RPC class implementation</summary>
		public const string IpcRpcImpl = IpcPrefix + "rpc.class";

		public const string DefaultIpcRpcImpl = "org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC";

		public const string RmPrefix = "yarn.resourcemanager.";

		public const string RmClusterId = RmPrefix + "cluster-id";

		public const string RmHostname = RmPrefix + "hostname";

		/// <summary>The address of the applications manager interface in the RM.</summary>
		public const string RmAddress = RmPrefix + "address";

		public const int DefaultRmPort = 8032;

		public const string DefaultRmAddress = "0.0.0.0:" + DefaultRmPort;

		/// <summary>The actual bind address for the RM.</summary>
		public const string RmBindHost = RmPrefix + "bind-host";

		/// <summary>The number of threads used to handle applications manager requests.</summary>
		public const string RmClientThreadCount = RmPrefix + "client.thread-count";

		public const int DefaultRmClientThreadCount = 50;

		/// <summary>Number of threads used to launch/cleanup AM.</summary>
		public const string RmAmlauncherThreadCount = RmPrefix + "amlauncher.thread-count";

		public const int DefaultRmAmlauncherThreadCount = 50;

		/// <summary>Retry times to connect with NM.</summary>
		public const string RmNodemanagerConnectRetires = RmPrefix + "nodemanager-connect-retries";

		public const int DefaultRmNodemanagerConnectRetires = 10;

		/// <summary>The Kerberos principal for the resource manager.</summary>
		public const string RmPrincipal = RmPrefix + "principal";

		/// <summary>The address of the scheduler interface.</summary>
		public const string RmSchedulerAddress = RmPrefix + "scheduler.address";

		public const int DefaultRmSchedulerPort = 8030;

		public const string DefaultRmSchedulerAddress = "0.0.0.0:" + DefaultRmSchedulerPort;

		/// <summary>Miniumum request grant-able by the RM scheduler.</summary>
		public const string RmSchedulerMinimumAllocationMb = YarnPrefix + "scheduler.minimum-allocation-mb";

		public const int DefaultRmSchedulerMinimumAllocationMb = 1024;

		public const string RmSchedulerMinimumAllocationVcores = YarnPrefix + "scheduler.minimum-allocation-vcores";

		public const int DefaultRmSchedulerMinimumAllocationVcores = 1;

		/// <summary>Maximum request grant-able by the RM scheduler.</summary>
		public const string RmSchedulerMaximumAllocationMb = YarnPrefix + "scheduler.maximum-allocation-mb";

		public const int DefaultRmSchedulerMaximumAllocationMb = 8192;

		public const string RmSchedulerMaximumAllocationVcores = YarnPrefix + "scheduler.maximum-allocation-vcores";

		public const int DefaultRmSchedulerMaximumAllocationVcores = 4;

		/// <summary>Number of threads to handle scheduler interface.</summary>
		public const string RmSchedulerClientThreadCount = RmPrefix + "scheduler.client.thread-count";

		public const int DefaultRmSchedulerClientThreadCount = 50;

		/// <summary>If the port should be included or not in the node name.</summary>
		/// <remarks>
		/// If the port should be included or not in the node name. The node name
		/// is used by the scheduler for resource requests allocation location
		/// matching. Typically this is just the hostname, using the port is needed
		/// when using minicluster and specific NM are required.
		/// </remarks>
		public const string RmSchedulerIncludePortInNodeName = YarnPrefix + "scheduler.include-port-in-node-name";

		public const bool DefaultRmSchedulerUsePortForNodeName = false;

		/// <summary>Enable Resource Manager webapp ui actions</summary>
		public const string RmWebappUiActionsEnabled = RmPrefix + "webapp.ui-actions.enabled";

		public const bool DefaultRmWebappUiActionsEnabled = true;

		/// <summary>Whether the RM should enable Reservation System</summary>
		public const string RmReservationSystemEnable = RmPrefix + "reservation-system.enable";

		public const bool DefaultRmReservationSystemEnable = false;

		/// <summary>The class to use as the Reservation System.</summary>
		public const string RmReservationSystemClass = RmPrefix + "reservation-system.class";

		/// <summary>The PlanFollower for the Reservation System.</summary>
		public const string RmReservationSystemPlanFollower = RmPrefix + "reservation-system.plan.follower";

		/// <summary>The step size of the Reservation System.</summary>
		public const string RmReservationSystemPlanFollowerTimeStep = RmPrefix + "reservation-system.planfollower.time-step";

		public const long DefaultRmReservationSystemPlanFollowerTimeStep = 1000L;

		/// <summary>Enable periodic monitor threads.</summary>
		/// <seealso cref="RmSchedulerMonitorPolicies"/>
		public const string RmSchedulerEnableMonitors = RmPrefix + "scheduler.monitor.enable";

		public const bool DefaultRmSchedulerEnableMonitors = false;

		/// <summary>List of SchedulingEditPolicy classes affecting the scheduler.</summary>
		public const string RmSchedulerMonitorPolicies = RmPrefix + "scheduler.monitor.policies";

		/// <summary>The address of the RM web application.</summary>
		public const string RmWebappAddress = RmPrefix + "webapp.address";

		public const int DefaultRmWebappPort = 8088;

		public const string DefaultRmWebappAddress = "0.0.0.0:" + DefaultRmWebappPort;

		/// <summary>The https address of the RM web application.</summary>
		public const string RmWebappHttpsAddress = RmPrefix + "webapp.https.address";

		public const bool YarnSslClientHttpsNeedAuthDefault = false;

		public const string YarnSslServerResourceDefault = "ssl-server.xml";

		public const int DefaultRmWebappHttpsPort = 8090;

		public const string DefaultRmWebappHttpsAddress = "0.0.0.0:" + DefaultRmWebappHttpsPort;

		public const string RmResourceTrackerAddress = RmPrefix + "resource-tracker.address";

		public const int DefaultRmResourceTrackerPort = 8031;

		public const string DefaultRmResourceTrackerAddress = "0.0.0.0:" + DefaultRmResourceTrackerPort;

		/// <summary>The expiry interval for application master reporting.</summary>
		public const string RmAmExpiryIntervalMs = YarnPrefix + "am.liveness-monitor.expiry-interval-ms";

		public const int DefaultRmAmExpiryIntervalMs = 600000;

		/// <summary>How long to wait until a node manager is considered dead.</summary>
		public const string RmNmExpiryIntervalMs = YarnPrefix + "nm.liveness-monitor.expiry-interval-ms";

		public const int DefaultRmNmExpiryIntervalMs = 600000;

		/// <summary>Are acls enabled.</summary>
		public const string YarnAclEnable = YarnPrefix + "acl.enable";

		public const bool DefaultYarnAclEnable = false;

		/// <summary>ACL of who can be admin of YARN cluster.</summary>
		public const string YarnAdminAcl = YarnPrefix + "admin.acl";

		public const string DefaultYarnAdminAcl = "*";

		/// <summary>ACL used in case none is found.</summary>
		/// <remarks>ACL used in case none is found. Allows nothing.</remarks>
		public const string DefaultYarnAppAcl = " ";

		/// <summary>Enable/disable intermediate-data encryption at YARN level.</summary>
		/// <remarks>
		/// Enable/disable intermediate-data encryption at YARN level. For now, this
		/// only is used by the FileSystemRMStateStore to setup right file-system
		/// security attributes.
		/// </remarks>
		[InterfaceAudience.Private]
		public const string YarnIntermediateDataEncryption = YarnPrefix + "intermediate-data-encryption.enable";

		[InterfaceAudience.Private]
		public static readonly bool DefaultYarnIntermediateDataEncryption = false;

		/// <summary>The address of the RM admin interface.</summary>
		public const string RmAdminAddress = RmPrefix + "admin.address";

		public const int DefaultRmAdminPort = 8033;

		public const string DefaultRmAdminAddress = "0.0.0.0:" + DefaultRmAdminPort;

		/// <summary>Number of threads used to handle RM admin interface.</summary>
		public const string RmAdminClientThreadCount = RmPrefix + "admin.client.thread-count";

		public const int DefaultRmAdminClientThreadCount = 1;

		/// <summary>The maximum number of application attempts.</summary>
		/// <remarks>
		/// The maximum number of application attempts.
		/// It's a global setting for all application masters.
		/// </remarks>
		public const string RmAmMaxAttempts = RmPrefix + "am.max-attempts";

		public const int DefaultRmAmMaxAttempts = 2;

		/// <summary>The keytab for the resource manager.</summary>
		public const string RmKeytab = RmPrefix + "keytab";

		/// <summary>The kerberos principal to be used for spnego filter for RM.</summary>
		public const string RmWebappSpnegoUserNameKey = RmPrefix + "webapp.spnego-principal";

		/// <summary>The kerberos keytab to be used for spnego filter for RM.</summary>
		public const string RmWebappSpnegoKeytabFileKey = RmPrefix + "webapp.spnego-keytab-file";

		/// <summary>
		/// Flag to enable override of the default kerberos authentication filter with
		/// the RM authentication filter to allow authentication using delegation
		/// tokens(fallback to kerberos if the tokens are missing).
		/// </summary>
		/// <remarks>
		/// Flag to enable override of the default kerberos authentication filter with
		/// the RM authentication filter to allow authentication using delegation
		/// tokens(fallback to kerberos if the tokens are missing). Only applicable
		/// when the http authentication type is kerberos.
		/// </remarks>
		public const string RmWebappDelegationTokenAuthFilter = RmPrefix + "webapp.delegation-token-auth-filter.enabled";

		public const bool DefaultRmWebappDelegationTokenAuthFilter = true;

		/// <summary>Enable cross origin (CORS) support.</summary>
		public const string RmWebappEnableCorsFilter = RmPrefix + "webapp.cross-origin.enabled";

		public const bool DefaultRmWebappEnableCorsFilter = false;

		/// <summary>How long to wait until a container is considered dead.</summary>
		public const string RmContainerAllocExpiryIntervalMs = RmPrefix + "rm.container-allocation.expiry-interval-ms";

		public const int DefaultRmContainerAllocExpiryIntervalMs = 600000;

		/// <summary>Path to file with nodes to include.</summary>
		public const string RmNodesIncludeFilePath = RmPrefix + "nodes.include-path";

		public const string DefaultRmNodesIncludeFilePath = string.Empty;

		/// <summary>Path to file with nodes to exclude.</summary>
		public const string RmNodesExcludeFilePath = RmPrefix + "nodes.exclude-path";

		public const string DefaultRmNodesExcludeFilePath = string.Empty;

		/// <summary>Number of threads to handle resource tracker calls.</summary>
		public const string RmResourceTrackerClientThreadCount = RmPrefix + "resource-tracker.client.thread-count";

		public const int DefaultRmResourceTrackerClientThreadCount = 50;

		/// <summary>The class to use as the resource scheduler.</summary>
		public const string RmScheduler = RmPrefix + "scheduler.class";

		public const string DefaultRmScheduler = "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler";

		/// <summary>RM set next Heartbeat interval for NM</summary>
		public const string RmNmHeartbeatIntervalMs = RmPrefix + "nodemanagers.heartbeat-interval-ms";

		public const long DefaultRmNmHeartbeatIntervalMs = 1000;

		/// <summary>Number of worker threads that write the history data.</summary>
		public const string RmHistoryWriterMultiThreadedDispatcherPoolSize = RmPrefix + "history-writer.multi-threaded-dispatcher.pool-size";

		public const int DefaultRmHistoryWriterMultiThreadedDispatcherPoolSize = 10;

		/// <summary>
		/// The setting that controls whether yarn system metrics is published on the
		/// timeline server or not by RM.
		/// </summary>
		public const string RmSystemMetricsPublisherEnabled = RmPrefix + "system-metrics-publisher.enabled";

		public const bool DefaultRmSystemMetricsPublisherEnabled = false;

		public const string RmSystemMetricsPublisherDispatcherPoolSize = RmPrefix + "system-metrics-publisher.dispatcher.pool-size";

		public const int DefaultRmSystemMetricsPublisherDispatcherPoolSize = 10;

		public const string RmDelegationKeyUpdateIntervalKey = RmPrefix + "delegation.key.update-interval";

		public const long RmDelegationKeyUpdateIntervalDefault = 24 * 60 * 60 * 1000;

		public const string RmDelegationTokenRenewIntervalKey = RmPrefix + "delegation.token.renew-interval";

		public const long RmDelegationTokenRenewIntervalDefault = 24 * 60 * 60 * 1000;

		public const string RmDelegationTokenMaxLifetimeKey = RmPrefix + "delegation.token.max-lifetime";

		public const long RmDelegationTokenMaxLifetimeDefault = 7 * 24 * 60 * 60 * 1000;

		public const string RecoveryEnabled = RmPrefix + "recovery.enabled";

		public const bool DefaultRmRecoveryEnabled = false;

		public const string YarnFailFast = YarnPrefix + "fail-fast";

		public const bool DefaultYarnFailFast = false;

		public const string RmFailFast = RmPrefix + "fail-fast";

		[InterfaceAudience.Private]
		public const string RmWorkPreservingRecoveryEnabled = RmPrefix + "work-preserving-recovery.enabled";

		[InterfaceAudience.Private]
		public const bool DefaultRmWorkPreservingRecoveryEnabled = true;

		public const string RmWorkPreservingRecoverySchedulingWaitMs = RmPrefix + "work-preserving-recovery.scheduling-wait-ms";

		public const long DefaultRmWorkPreservingRecoverySchedulingWaitMs = 10000;

		/// <summary>Zookeeper interaction configs</summary>
		public const string RmZkPrefix = RmPrefix + "zk-";

		public const string RmZkAddress = RmZkPrefix + "address";

		public const string RmZkNumRetries = RmZkPrefix + "num-retries";

		public const int DefaultZkRmNumRetries = 1000;

		public const string RmZkRetryIntervalMs = RmZkPrefix + "retry-interval-ms";

		public const long DefaultRmZkRetryIntervalMs = 1000;

		public const string RmZkTimeoutMs = RmZkPrefix + "timeout-ms";

		public const int DefaultRmZkTimeoutMs = 10000;

		public const string RmZkAcl = RmZkPrefix + "acl";

		public const string DefaultRmZkAcl = "world:anyone:rwcda";

		public const string RmZkAuth = RmZkPrefix + "auth";

		public const string ZkStateStorePrefix = RmPrefix + "zk-state-store.";

		/// <summary>Parent znode path under which ZKRMStateStore will create znodes</summary>
		public const string ZkRmStateStoreParentPath = ZkStateStorePrefix + "parent-path";

		public const string DefaultZkRmStateStoreParentPath = "/rmstore";

		/// <summary>Root node ACLs for fencing</summary>
		public const string ZkRmStateStoreRootNodeAcl = ZkStateStorePrefix + "root-node.acl";

		/// <summary>HA related configs</summary>
		public const string RmHaPrefix = RmPrefix + "ha.";

		public const string RmHaEnabled = RmHaPrefix + "enabled";

		public const bool DefaultRmHaEnabled = false;

		public const string RmHaIds = RmHaPrefix + "rm-ids";

		public const string RmHaId = RmHaPrefix + "id";

		/// <summary>Store the related configuration files in File System</summary>
		public const string FsBasedRmConfStore = RmPrefix + "configuration.file-system-based-store";

		public const string DefaultFsBasedRmConfStore = "/yarn/conf";

		public const string RmConfigurationProviderClass = RmPrefix + "configuration.provider-class";

		public const string DefaultRmConfigurationProviderClass = "org.apache.hadoop.yarn.LocalConfigurationProvider";

		public const string YarnAuthorizationProvider = YarnPrefix + "authorization-provider";

		private static readonly IList<string> RmServicesAddressConfKeysHttp = Sharpen.Collections
			.UnmodifiableList(Arrays.AsList(RmAddress, RmSchedulerAddress, RmAdminAddress, RmResourceTrackerAddress
			, RmWebappAddress));

		private static readonly IList<string> RmServicesAddressConfKeysHttps = Sharpen.Collections
			.UnmodifiableList(Arrays.AsList(RmAddress, RmSchedulerAddress, RmAdminAddress, RmResourceTrackerAddress
			, RmWebappHttpsAddress));

		public const string AutoFailoverPrefix = RmHaPrefix + "automatic-failover.";

		public const string AutoFailoverEnabled = AutoFailoverPrefix + "enabled";

		public const bool DefaultAutoFailoverEnabled = true;

		public const string AutoFailoverEmbedded = AutoFailoverPrefix + "embedded";

		public const bool DefaultAutoFailoverEmbedded = true;

		public const string AutoFailoverZkBasePath = AutoFailoverPrefix + "zk-base-path";

		public const string DefaultAutoFailoverZkBasePath = "/yarn-leader-election";

		public const string ClientFailoverPrefix = YarnPrefix + "client.failover-";

		public const string ClientFailoverProxyProvider = ClientFailoverPrefix + "proxy-provider";

		public const string DefaultClientFailoverProxyProvider = "org.apache.hadoop.yarn.client.ConfiguredRMFailoverProxyProvider";

		public const string ClientFailoverMaxAttempts = ClientFailoverPrefix + "max-attempts";

		public const string ClientFailoverSleeptimeBaseMs = ClientFailoverPrefix + "sleep-base-ms";

		public const string ClientFailoverSleeptimeMaxMs = ClientFailoverPrefix + "sleep-max-ms";

		public const string ClientFailoverRetries = ClientFailoverPrefix + "retries";

		public const int DefaultClientFailoverRetries = 0;

		public const string ClientFailoverRetriesOnSocketTimeouts = ClientFailoverPrefix 
			+ "retries-on-socket-timeouts";

		public const int DefaultClientFailoverRetriesOnSocketTimeouts = 0;

		/// <summary>The class to use as the persistent store.</summary>
		public const string RmStore = RmPrefix + "store.class";

		/// <summary>URI for FileSystemRMStateStore</summary>
		public const string FsRmStateStoreUri = RmPrefix + "fs.state-store.uri";

		public const string FsRmStateStoreRetryPolicySpec = RmPrefix + "fs.state-store.retry-policy-spec";

		public const string DefaultFsRmStateStoreRetryPolicySpec = "2000, 500";

		public const string FsRmStateStoreNumRetries = RmPrefix + "fs.state-store.num-retries";

		public const int DefaultFsRmStateStoreNumRetries = 0;

		public const string FsRmStateStoreRetryIntervalMs = RmPrefix + "fs.state-store.retry-interval-ms";

		public const long DefaultFsRmStateStoreRetryIntervalMs = 1000L;

		public const string RmLeveldbStorePath = RmPrefix + "leveldb-state-store.path";

		/// <summary>The maximum number of completed applications RM keeps.</summary>
		public const string RmMaxCompletedApplications = RmPrefix + "max-completed-applications";

		public const int DefaultRmMaxCompletedApplications = 10000;

		/// <summary>
		/// The maximum number of completed applications RM state store keeps, by
		/// default equals to DEFAULT_RM_MAX_COMPLETED_APPLICATIONS
		/// </summary>
		public const string RmStateStoreMaxCompletedApplications = RmPrefix + "state-store.max-completed-applications";

		public const int DefaultRmStateStoreMaxCompletedApplications = DefaultRmMaxCompletedApplications;

		/// <summary>Default application name</summary>
		public const string DefaultApplicationName = "N/A";

		/// <summary>Default application type</summary>
		public const string DefaultApplicationType = "YARN";

		/// <summary>Default application type length</summary>
		public const int ApplicationTypeLength = 20;

		/// <summary>Default queue name</summary>
		public const string DefaultQueueName = "default";

		/// <summary>Buckets (in minutes) for the number of apps running in each queue.</summary>
		public const string RmMetricsRuntimeBuckets = RmPrefix + "metrics.runtime.buckets";

		/// <summary>Default sizes of the runtime metric buckets in minutes.</summary>
		public const string DefaultRmMetricsRuntimeBuckets = "60,300,1440";

		public const string RmAmrmTokenMasterKeyRollingIntervalSecs = RmPrefix + "am-rm-tokens.master-key-rolling-interval-secs";

		public const long DefaultRmAmrmTokenMasterKeyRollingIntervalSecs = 24 * 60 * 60;

		public const string RmContainerTokenMasterKeyRollingIntervalSecs = RmPrefix + "container-tokens.master-key-rolling-interval-secs";

		public const long DefaultRmContainerTokenMasterKeyRollingIntervalSecs = 24 * 60 *
			 60;

		public const string RmNmtokenMasterKeyRollingIntervalSecs = RmPrefix + "nm-tokens.master-key-rolling-interval-secs";

		public const long DefaultRmNmtokenMasterKeyRollingIntervalSecs = 24 * 60 * 60;

		public const string RmNodemanagerMinimumVersion = RmPrefix + "nodemanager.minimum.version";

		public const string DefaultRmNodemanagerMinimumVersion = "NONE";

		/// <summary>RM proxy users' prefix</summary>
		public const string RmProxyUserPrefix = RmPrefix + "proxyuser.";

		/// <summary>Prefix for all node manager configs.</summary>
		public const string NmPrefix = "yarn.nodemanager.";

		/// <summary>Environment variables that will be sent to containers.</summary>
		public const string NmAdminUserEnv = NmPrefix + "admin-env";

		public const string DefaultNmAdminUserEnv = "MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX";

		/// <summary>Environment variables that containers may override rather than use NodeManager's default.
		/// 	</summary>
		public const string NmEnvWhitelist = NmPrefix + "env-whitelist";

		public static readonly string DefaultNmEnvWhitelist = StringUtils.Join(",", Arrays
			.AsList(ApplicationConstants.Environment.JavaHome.Key(), ApplicationConstants.Environment
			.HadoopCommonHome.Key(), ApplicationConstants.Environment.HadoopHdfsHome.Key(), 
			ApplicationConstants.Environment.HadoopConfDir.Key(), ApplicationConstants.Environment
			.ClasspathPrependDistcache.Key(), ApplicationConstants.Environment.HadoopYarnHome
			.Key()));

		/// <summary>address of node manager IPC.</summary>
		public const string NmAddress = NmPrefix + "address";

		public const int DefaultNmPort = 0;

		public const string DefaultNmAddress = "0.0.0.0:" + DefaultNmPort;

		/// <summary>The actual bind address or the NM.</summary>
		public const string NmBindHost = NmPrefix + "bind-host";

		/// <summary>who will execute(launch) the containers.</summary>
		public const string NmContainerExecutor = NmPrefix + "container-executor.class";

		/// <summary>Adjustment to make to the container os scheduling priority.</summary>
		/// <remarks>
		/// Adjustment to make to the container os scheduling priority.
		/// The valid values for this could vary depending on the platform.
		/// On Linux, higher values mean run the containers at a less
		/// favorable priority than the NM.
		/// The value specified is an int.
		/// </remarks>
		public const string NmContainerExecutorSchedPriority = NmPrefix + "container-executor.os.sched.priority.adjustment";

		public const int DefaultNmContainerExecutorSchedPriority = 0;

		/// <summary>Number of threads container manager uses.</summary>
		public const string NmContainerMgrThreadCount = NmPrefix + "container-manager.thread-count";

		public const int DefaultNmContainerMgrThreadCount = 20;

		/// <summary>Number of threads used in cleanup.</summary>
		public const string NmDeleteThreadCount = NmPrefix + "delete.thread-count";

		public const int DefaultNmDeleteThreadCount = 4;

		/// <summary>Keytab for NM.</summary>
		public const string NmKeytab = NmPrefix + "keytab";

		/// <summary>List of directories to store localized files in.</summary>
		public const string NmLocalDirs = NmPrefix + "local-dirs";

		public const string DefaultNmLocalDirs = "/tmp/nm-local-dir";

		/// <summary>
		/// Number of files in each localized directories
		/// Avoid tuning this too low.
		/// </summary>
		public const string NmLocalCacheMaxFilesPerDirectory = NmPrefix + "local-cache.max-files-per-directory";

		public const int DefaultNmLocalCacheMaxFilesPerDirectory = 8192;

		/// <summary>Address where the localizer IPC is.</summary>
		public const string NmLocalizerAddress = NmPrefix + "localizer.address";

		public const int DefaultNmLocalizerPort = 8040;

		public const string DefaultNmLocalizerAddress = "0.0.0.0:" + DefaultNmLocalizerPort;

		/// <summary>Interval in between cache cleanups.</summary>
		public const string NmLocalizerCacheCleanupIntervalMs = NmPrefix + "localizer.cache.cleanup.interval-ms";

		public const long DefaultNmLocalizerCacheCleanupIntervalMs = 10 * 60 * 1000;

		/// <summary>Target size of localizer cache in MB, per nodemanager.</summary>
		/// <remarks>
		/// Target size of localizer cache in MB, per nodemanager. It is a target
		/// retention size that only includes resources with PUBLIC and PRIVATE
		/// visibility and excludes resources with APPLICATION visibility
		/// </remarks>
		public const string NmLocalizerCacheTargetSizeMb = NmPrefix + "localizer.cache.target-size-mb";

		public const long DefaultNmLocalizerCacheTargetSizeMb = 10 * 1024;

		/// <summary>Number of threads to handle localization requests.</summary>
		public const string NmLocalizerClientThreadCount = NmPrefix + "localizer.client.thread-count";

		public const int DefaultNmLocalizerClientThreadCount = 5;

		/// <summary>Number of threads to use for localization fetching.</summary>
		public const string NmLocalizerFetchThreadCount = NmPrefix + "localizer.fetch.thread-count";

		public const int DefaultNmLocalizerFetchThreadCount = 4;

		/// <summary>Where to store container logs.</summary>
		public const string NmLogDirs = NmPrefix + "log-dirs";

		public const string DefaultNmLogDirs = "/tmp/logs";

		public const string NmResourcemanagerMinimumVersion = NmPrefix + "resourcemanager.minimum.version";

		public const string DefaultNmResourcemanagerMinimumVersion = "NONE";

		/// <summary>Interval at which the delayed token removal thread runs</summary>
		public const string RmDelayedDelegationTokenRemovalIntervalMs = RmPrefix + "delayed.delegation-token.removal-interval-ms";

		public const long DefaultRmDelayedDelegationTokenRemovalIntervalMs = 30000l;

		/// <summary>Delegation Token renewer thread count</summary>
		public const string RmDelegationTokenRenewerThreadCount = RmPrefix + "delegation-token-renewer.thread-count";

		public const int DefaultRmDelegationTokenRenewerThreadCount = 50;

		public const string RmProxyUserPrivilegesEnabled = RmPrefix + "proxy-user-privileges.enabled";

		public static bool DefaultRmProxyUserPrivilegesEnabled = false;

		/// <summary>Whether to enable log aggregation</summary>
		public const string LogAggregationEnabled = YarnPrefix + "log-aggregation-enable";

		public const bool DefaultLogAggregationEnabled = false;

		/// <summary>How long to wait before deleting aggregated logs, -1 disables.</summary>
		/// <remarks>
		/// How long to wait before deleting aggregated logs, -1 disables.
		/// Be careful set this too small and you will spam the name node.
		/// </remarks>
		public const string LogAggregationRetainSeconds = YarnPrefix + "log-aggregation.retain-seconds";

		public const long DefaultLogAggregationRetainSeconds = -1;

		/// <summary>How long to wait between aggregated log retention checks.</summary>
		/// <remarks>
		/// How long to wait between aggregated log retention checks. If set to
		/// a value
		/// <literal>&lt;=</literal>
		/// 0 then the value is computed as one-tenth of the
		/// log retention setting. Be careful set this too small and you will spam
		/// the name node.
		/// </remarks>
		public const string LogAggregationRetainCheckIntervalSeconds = YarnPrefix + "log-aggregation.retain-check-interval-seconds";

		public const long DefaultLogAggregationRetainCheckIntervalSeconds = -1;

		/// <summary>Number of seconds to retain logs on the NodeManager.</summary>
		/// <remarks>
		/// Number of seconds to retain logs on the NodeManager. Only applicable if Log
		/// aggregation is disabled
		/// </remarks>
		public const string NmLogRetainSeconds = NmPrefix + "log.retain-seconds";

		public const long DefaultNmLogRetainSeconds = 3 * 60 * 60;

		/// <summary>Define how often NMs wake up and upload log files</summary>
		public const string NmLogAggregationRollMonitoringIntervalSeconds = NmPrefix + "log-aggregation.roll-monitoring-interval-seconds";

		public const long DefaultNmLogAggregationRollMonitoringIntervalSeconds = -1;

		/// <summary>Number of threads used in log cleanup.</summary>
		/// <remarks>
		/// Number of threads used in log cleanup. Only applicable if Log aggregation
		/// is disabled
		/// </remarks>
		public const string NmLogDeletionThreadsCount = NmPrefix + "log.deletion-threads-count";

		public const int DefaultNmLogDeleteThreadCount = 4;

		/// <summary>Where to aggregate logs to.</summary>
		public const string NmRemoteAppLogDir = NmPrefix + "remote-app-log-dir";

		public const string DefaultNmRemoteAppLogDir = "/tmp/logs";

		/// <summary>
		/// The remote log dir will be created at
		/// NM_REMOTE_APP_LOG_DIR/${user}/NM_REMOTE_APP_LOG_DIR_SUFFIX/${appId}
		/// </summary>
		public const string NmRemoteAppLogDirSuffix = NmPrefix + "remote-app-log-dir-suffix";

		public const string DefaultNmRemoteAppLogDirSuffix = "logs";

		public const string YarnLogServerUrl = YarnPrefix + "log.server.url";

		public const string YarnTrackingUrlGenerator = YarnPrefix + "tracking.url.generator";

		/// <summary>Amount of memory in GB that can be allocated for containers.</summary>
		public const string NmPmemMb = NmPrefix + "resource.memory-mb";

		public const int DefaultNmPmemMb = 8 * 1024;

		/// <summary>Specifies whether physical memory check is enabled.</summary>
		public const string NmPmemCheckEnabled = NmPrefix + "pmem-check-enabled";

		public const bool DefaultNmPmemCheckEnabled = true;

		/// <summary>Specifies whether physical memory check is enabled.</summary>
		public const string NmVmemCheckEnabled = NmPrefix + "vmem-check-enabled";

		public const bool DefaultNmVmemCheckEnabled = true;

		/// <summary>Conversion ratio for physical memory to virtual memory.</summary>
		public const string NmVmemPmemRatio = NmPrefix + "vmem-pmem-ratio";

		public const float DefaultNmVmemPmemRatio = 2.1f;

		/// <summary>Number of Virtual CPU Cores which can be allocated for containers.</summary>
		public const string NmVcores = NmPrefix + "resource.cpu-vcores";

		public const int DefaultNmVcores = 8;

		/// <summary>Percentage of overall CPU which can be allocated for containers.</summary>
		public const string NmResourcePercentagePhysicalCpuLimit = NmPrefix + "resource.percentage-physical-cpu-limit";

		public const int DefaultNmResourcePercentagePhysicalCpuLimit = 100;

		/// <summary>NM Webapp address.</summary>
		public const string NmWebappAddress = NmPrefix + "webapp.address";

		public const int DefaultNmWebappPort = 8042;

		public const string DefaultNmWebappAddress = "0.0.0.0:" + DefaultNmWebappPort;

		/// <summary>NM Webapp https address.</summary>
		public const string NmWebappHttpsAddress = NmPrefix + "webapp.https.address";

		public const int DefaultNmWebappHttpsPort = 8044;

		public const string DefaultNmWebappHttpsAddress = "0.0.0.0:" + DefaultNmWebappHttpsPort;

		/// <summary>Enable/disable CORS filter.</summary>
		public const string NmWebappEnableCorsFilter = NmPrefix + "webapp.cross-origin.enabled";

		public const bool DefaultNmWebappEnableCorsFilter = false;

		/// <summary>How often to monitor containers.</summary>
		public const string NmContainerMonIntervalMs = NmPrefix + "container-monitor.interval-ms";

		public const int DefaultNmContainerMonIntervalMs = 3000;

		/// <summary>Class that calculates containers current resource utilization.</summary>
		public const string NmContainerMonResourceCalculator = NmPrefix + "container-monitor.resource-calculator.class";

		/// <summary>Class that calculates process tree resource utilization.</summary>
		public const string NmContainerMonProcessTree = NmPrefix + "container-monitor.process-tree.class";

		public const string ProcfsUseSmapsBasedRssEnabled = NmPrefix + "container-monitor.procfs-tree.smaps-based-rss.enabled";

		public const bool DefaultProcfsUseSmapsBasedRssEnabled = false;

		/// <summary>Enable/disable container metrics.</summary>
		[InterfaceAudience.Private]
		public const string NmContainerMetricsEnable = NmPrefix + "container-metrics.enable";

		[InterfaceAudience.Private]
		public const bool DefaultNmContainerMetricsEnable = true;

		/// <summary>Container metrics flush period.</summary>
		/// <remarks>Container metrics flush period. -1 for flush on completion.</remarks>
		[InterfaceAudience.Private]
		public const string NmContainerMetricsPeriodMs = NmPrefix + "container-metrics.period-ms";

		[InterfaceAudience.Private]
		public const int DefaultNmContainerMetricsPeriodMs = -1;

		/// <summary>The delay time ms to unregister container metrics after completion.</summary>
		[InterfaceAudience.Private]
		public const string NmContainerMetricsUnregisterDelayMs = NmPrefix + "container-metrics.unregister-delay-ms";

		[InterfaceAudience.Private]
		public const int DefaultNmContainerMetricsUnregisterDelayMs = 10000;

		/// <summary>Prefix for all node manager disk health checker configs.</summary>
		private const string NmDiskHealthCheckPrefix = "yarn.nodemanager.disk-health-checker.";

		/// <summary>Enable/Disable disks' health checker.</summary>
		/// <remarks>
		/// Enable/Disable disks' health checker. Default is true. An expert level
		/// configuration property.
		/// </remarks>
		public const string NmDiskHealthCheckEnable = NmDiskHealthCheckPrefix + "enable";

		/// <summary>Frequency of running disks' health checker.</summary>
		public const string NmDiskHealthCheckIntervalMs = NmDiskHealthCheckPrefix + "interval-ms";

		/// <summary>By default, disks' health is checked every 2 minutes.</summary>
		public const long DefaultNmDiskHealthCheckIntervalMs = 2 * 60 * 1000;

		/// <summary>
		/// The minimum fraction of number of disks to be healthy for the nodemanager
		/// to launch new containers.
		/// </summary>
		/// <remarks>
		/// The minimum fraction of number of disks to be healthy for the nodemanager
		/// to launch new containers. This applies to nm-local-dirs and nm-log-dirs.
		/// </remarks>
		public const string NmMinHealthyDisksFraction = NmDiskHealthCheckPrefix + "min-healthy-disks";

		/// <summary>
		/// By default, at least 25% of disks are to be healthy to say that the node is
		/// healthy in terms of disks.
		/// </summary>
		public const float DefaultNmMinHealthyDisksFraction = 0.25F;

		/// <summary>
		/// The maximum percentage of disk space that can be used after which a disk is
		/// marked as offline.
		/// </summary>
		/// <remarks>
		/// The maximum percentage of disk space that can be used after which a disk is
		/// marked as offline. Values can range from 0.0 to 100.0. If the value is
		/// greater than or equal to 100, NM will check for full disk. This applies to
		/// nm-local-dirs and nm-log-dirs.
		/// </remarks>
		public const string NmMaxPerDiskUtilizationPercentage = NmDiskHealthCheckPrefix +
			 "max-disk-utilization-per-disk-percentage";

		/// <summary>By default, 90% of the disk can be used before it is marked as offline.</summary>
		public const float DefaultNmMaxPerDiskUtilizationPercentage = 90.0F;

		/// <summary>The minimum space that must be available on a local dir for it to be used.
		/// 	</summary>
		/// <remarks>
		/// The minimum space that must be available on a local dir for it to be used.
		/// This applies to nm-local-dirs and nm-log-dirs.
		/// </remarks>
		public const string NmMinPerDiskFreeSpaceMb = NmDiskHealthCheckPrefix + "min-free-space-per-disk-mb";

		/// <summary>By default, all of the disk can be used before it is marked as offline.</summary>
		public const long DefaultNmMinPerDiskFreeSpaceMb = 0;

		/// <summary>Frequency of running node health script.</summary>
		public const string NmHealthCheckIntervalMs = NmPrefix + "health-checker.interval-ms";

		public const long DefaultNmHealthCheckIntervalMs = 10 * 60 * 1000;

		/// <summary>Health check script time out period.</summary>
		public const string NmHealthCheckScriptTimeoutMs = NmPrefix + "health-checker.script.timeout-ms";

		public const long DefaultNmHealthCheckScriptTimeoutMs = 2 * DefaultNmHealthCheckIntervalMs;

		/// <summary>The health check script to run.</summary>
		public const string NmHealthCheckScriptPath = NmPrefix + "health-checker.script.path";

		/// <summary>The arguments to pass to the health check script.</summary>
		public const string NmHealthCheckScriptOpts = NmPrefix + "health-checker.script.opts";

		/// <summary>The Docker image name(For DockerContainerExecutor).</summary>
		public const string NmDockerContainerExecutorImageName = NmPrefix + "docker-container-executor.image-name";

		/// <summary>The name of the docker executor (For DockerContainerExecutor).</summary>
		public const string NmDockerContainerExecutorExecName = NmPrefix + "docker-container-executor.exec-name";

		/// <summary>The default docker executor (For DockerContainerExecutor).</summary>
		public const string NmDefaultDockerContainerExecutorExecName = "/usr/bin/docker";

		/// <summary>The path to the Linux container executor.</summary>
		public const string NmLinuxContainerExecutorPath = NmPrefix + "linux-container-executor.path";

		/// <summary>The UNIX group that the linux-container-executor should run as.</summary>
		/// <remarks>
		/// The UNIX group that the linux-container-executor should run as.
		/// This is intended to be set as part of container-executor.cfg.
		/// </remarks>
		public const string NmLinuxContainerGroup = NmPrefix + "linux-container-executor.group";

		/// <summary>
		/// If linux-container-executor should limit itself to one user
		/// when running in non-secure mode.
		/// </summary>
		public const string NmNonsecureModeLimitUsers = NmPrefix + "linux-container-executor.nonsecure-mode.limit-users";

		public const bool DefaultNmNonsecureModeLimitUsers = true;

		/// <summary>
		/// The UNIX user that containers will run as when Linux-container-executor
		/// is used in nonsecure mode (a use case for this is using cgroups).
		/// </summary>
		public const string NmNonsecureModeLocalUserKey = NmPrefix + "linux-container-executor.nonsecure-mode.local-user";

		public const string DefaultNmNonsecureModeLocalUser = "nobody";

		/// <summary>
		/// The allowed pattern for UNIX user names enforced by
		/// Linux-container-executor when used in nonsecure mode (use case for this
		/// is using cgroups).
		/// </summary>
		/// <remarks>
		/// The allowed pattern for UNIX user names enforced by
		/// Linux-container-executor when used in nonsecure mode (use case for this
		/// is using cgroups). The default value is taken from /usr/sbin/adduser
		/// </remarks>
		public const string NmNonsecureModeUserPatternKey = NmPrefix + "linux-container-executor.nonsecure-mode.user-pattern";

		public const string DefaultNmNonsecureModeUserPattern = "^[_.A-Za-z0-9][-@_.A-Za-z0-9]{0,255}?[$]?$";

		/// <summary>
		/// The type of resource enforcement to use with the
		/// linux container executor.
		/// </summary>
		public const string NmLinuxContainerResourcesHandler = NmPrefix + "linux-container-executor.resources-handler.class";

		/// <summary>The path the linux container executor should use for cgroups</summary>
		public const string NmLinuxContainerCgroupsHierarchy = NmPrefix + "linux-container-executor.cgroups.hierarchy";

		/// <summary>Whether the linux container executor should mount cgroups if not found</summary>
		public const string NmLinuxContainerCgroupsMount = NmPrefix + "linux-container-executor.cgroups.mount";

		/// <summary>Where the linux container executor should mount cgroups if not found</summary>
		public const string NmLinuxContainerCgroupsMountPath = NmPrefix + "linux-container-executor.cgroups.mount-path";

		/// <summary>
		/// Whether the apps should run in strict resource usage mode(not allowed to
		/// use spare CPU)
		/// </summary>
		public const string NmLinuxContainerCgroupsStrictResourceUsage = NmPrefix + "linux-container-executor.cgroups.strict-resource-usage";

		public const bool DefaultNmLinuxContainerCgroupsStrictResourceUsage = false;

		/// <summary>
		/// Interval of time the linux container executor should try cleaning up
		/// cgroups entry when cleaning up a container.
		/// </summary>
		/// <remarks>
		/// Interval of time the linux container executor should try cleaning up
		/// cgroups entry when cleaning up a container. This is required due to what
		/// it seems a race condition because the SIGTERM/SIGKILL is asynch.
		/// </remarks>
		public const string NmLinuxContainerCgroupsDeleteTimeout = NmPrefix + "linux-container-executor.cgroups.delete-timeout-ms";

		public const long DefaultNmLinuxContainerCgroupsDeleteTimeout = 1000;

		/// <summary>Delay between attempts to remove linux cgroup.</summary>
		public const string NmLinuxContainerCgroupsDeleteDelay = NmPrefix + "linux-container-executor.cgroups.delete-delay-ms";

		public const long DefaultNmLinuxContainerCgroupsDeleteDelay = 20;

		/// <summary>
		/// Indicates if memory and CPU limits will be set for the Windows Job
		/// Object for the containers launched by the default container executor.
		/// </summary>
		public const string NmWindowsContainerMemoryLimitEnabled = NmPrefix + "windows-container.memory-limit.enabled";

		public const bool DefaultNmWindowsContainerMemoryLimitEnabled = false;

		public const string NmWindowsContainerCpuLimitEnabled = NmPrefix + "windows-container.cpu-limit.enabled";

		public const bool DefaultNmWindowsContainerCpuLimitEnabled = false;

		/// <summary>/* The Windows group that the windows-secure-container-executor should run as.
		/// 	</summary>
		public const string NmWindowsSecureContainerGroup = NmPrefix + "windows-secure-container-executor.group";

		/// <summary>T-file compression types used to compress aggregated logs.</summary>
		public const string NmLogAggCompressionType = NmPrefix + "log-aggregation.compression-type";

		public const string DefaultNmLogAggCompressionType = "none";

		/// <summary>The kerberos principal for the node manager.</summary>
		public const string NmPrincipal = NmPrefix + "principal";

		public const string NmAuxServices = NmPrefix + "aux-services";

		public const string NmAuxServiceFmt = NmPrefix + "aux-services.%s.class";

		public const string NmUserHomeDir = NmPrefix + "user-home-dir";

		/// <summary>The kerberos principal to be used for spnego filter for NM.</summary>
		public const string NmWebappSpnegoUserNameKey = NmPrefix + "webapp.spnego-principal";

		/// <summary>The kerberos keytab to be used for spnego filter for NM.</summary>
		public const string NmWebappSpnegoKeytabFileKey = NmPrefix + "webapp.spnego-keytab-file";

		public const string DefaultNmUserHomeDir = "/home/";

		public const string NmRecoveryPrefix = NmPrefix + "recovery.";

		public const string NmRecoveryEnabled = NmRecoveryPrefix + "enabled";

		public const bool DefaultNmRecoveryEnabled = false;

		public const string NmRecoveryDir = NmRecoveryPrefix + "dir";

		public const string ProxyPrefix = "yarn.web-proxy.";

		/// <summary>The kerberos principal for the proxy.</summary>
		public const string ProxyPrincipal = ProxyPrefix + "principal";

		/// <summary>Keytab for Proxy.</summary>
		public const string ProxyKeytab = ProxyPrefix + "keytab";

		/// <summary>The address for the web proxy.</summary>
		public const string ProxyAddress = ProxyPrefix + "address";

		public const int DefaultProxyPort = 9099;

		public const string DefaultProxyAddress = "0.0.0.0:" + DefaultProxyPort;

		/// <summary>YARN Service Level Authorization</summary>
		public const string YarnSecurityServiceAuthorizationResourcetrackerProtocol = "security.resourcetracker.protocol.acl";

		public const string YarnSecurityServiceAuthorizationApplicationclientProtocol = "security.applicationclient.protocol.acl";

		public const string YarnSecurityServiceAuthorizationResourcemanagerAdministrationProtocol
			 = "security.resourcemanager-administration.protocol.acl";

		public const string YarnSecurityServiceAuthorizationApplicationmasterProtocol = "security.applicationmaster.protocol.acl";

		public const string YarnSecurityServiceAuthorizationContainerManagementProtocol = 
			"security.containermanagement.protocol.acl";

		public const string YarnSecurityServiceAuthorizationResourceLocalizer = "security.resourcelocalizer.protocol.acl";

		public const string YarnSecurityServiceAuthorizationApplicationhistoryProtocol = 
			"security.applicationhistory.protocol.acl";

		/// <summary>No.</summary>
		/// <remarks>
		/// No. of milliseconds to wait between sending a SIGTERM and SIGKILL
		/// to a running container
		/// </remarks>
		public const string NmSleepDelayBeforeSigkillMs = NmPrefix + "sleep-delay-before-sigkill.ms";

		public const long DefaultNmSleepDelayBeforeSigkillMs = 250;

		/// <summary>
		/// Max time to wait for a process to come up when trying to cleanup
		/// container resources
		/// </summary>
		public const string NmProcessKillWaitMs = NmPrefix + "process-kill-wait.ms";

		public const long DefaultNmProcessKillWaitMs = 2000;

		/// <summary>Max time to wait to establish a connection to RM</summary>
		public const string ResourcemanagerConnectMaxWaitMs = RmPrefix + "connect.max-wait.ms";

		public const long DefaultResourcemanagerConnectMaxWaitMs = 15 * 60 * 1000;

		/// <summary>Time interval between each attempt to connect to RM</summary>
		public const string ResourcemanagerConnectRetryIntervalMs = RmPrefix + "connect.retry-interval.ms";

		public const long DefaultResourcemanagerConnectRetryIntervalMs = 30 * 1000;

		public const string DispatcherDrainEventsTimeout = YarnPrefix + "dispatcher.drain-events.timeout";

		public const long DefaultDispatcherDrainEventsTimeout = 300000;

		/// <summary>CLASSPATH for YARN applications.</summary>
		/// <remarks>
		/// CLASSPATH for YARN applications. A comma-separated list of CLASSPATH
		/// entries
		/// </remarks>
		public const string YarnApplicationClasspath = YarnPrefix + "application.classpath";

		/// <summary>Default platform-agnostic CLASSPATH for YARN applications.</summary>
		/// <remarks>
		/// Default platform-agnostic CLASSPATH for YARN applications. A
		/// comma-separated list of CLASSPATH entries. The parameter expansion marker
		/// will be replaced with real parameter expansion marker ('%' for Windows and
		/// '$' for Linux) by NodeManager on container launch. For example: {{VAR}}
		/// will be replaced as $VAR on Linux, and %VAR% on Windows.
		/// </remarks>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static readonly string[] DefaultYarnCrossPlatformApplicationClasspath = new 
			string[] { ApplicationConstants.Environment.HadoopConfDir.$$(), ApplicationConstants.Environment
			.HadoopCommonHome.$$() + "/share/hadoop/common/*", ApplicationConstants.Environment
			.HadoopCommonHome.$$() + "/share/hadoop/common/lib/*", ApplicationConstants.Environment
			.HadoopHdfsHome.$$() + "/share/hadoop/hdfs/*", ApplicationConstants.Environment.
			HadoopHdfsHome.$$() + "/share/hadoop/hdfs/lib/*", ApplicationConstants.Environment
			.HadoopYarnHome.$$() + "/share/hadoop/yarn/*", ApplicationConstants.Environment.
			HadoopYarnHome.$$() + "/share/hadoop/yarn/lib/*" };

		/// <summary>
		/// <p>
		/// Default platform-specific CLASSPATH for YARN applications.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Default platform-specific CLASSPATH for YARN applications. A
		/// comma-separated list of CLASSPATH entries constructed based on the client
		/// OS environment expansion syntax.
		/// </p>
		/// <p>
		/// Note: Use
		/// <see cref="DefaultYarnCrossPlatformApplicationClasspath"/>
		/// for
		/// cross-platform practice i.e. submit an application from a Windows client to
		/// a Linux/Unix server or vice versa.
		/// </p>
		/// </remarks>
		public static readonly string[] DefaultYarnApplicationClasspath = new string[] { 
			ApplicationConstants.Environment.HadoopConfDir.$(), ApplicationConstants.Environment
			.HadoopCommonHome.$() + "/share/hadoop/common/*", ApplicationConstants.Environment
			.HadoopCommonHome.$() + "/share/hadoop/common/lib/*", ApplicationConstants.Environment
			.HadoopHdfsHome.$() + "/share/hadoop/hdfs/*", ApplicationConstants.Environment.HadoopHdfsHome
			.$() + "/share/hadoop/hdfs/lib/*", ApplicationConstants.Environment.HadoopYarnHome
			.$() + "/share/hadoop/yarn/*", ApplicationConstants.Environment.HadoopYarnHome.$
			() + "/share/hadoop/yarn/lib/*" };

		/// <summary>Container temp directory</summary>
		public const string DefaultContainerTempDir = "./tmp";

		public const string IsMiniYarnCluster = YarnPrefix + "is.minicluster";

		public const string YarnMcPrefix = YarnPrefix + "minicluster.";

		/// <summary>Whether to use fixed ports with the minicluster.</summary>
		public const string YarnMiniclusterFixedPorts = YarnMcPrefix + "fixed.ports";

		/// <summary>
		/// Default is false to be able to run tests concurrently without port
		/// conflicts.
		/// </summary>
		public const bool DefaultYarnMiniclusterFixedPorts = false;

		/// <summary>Whether the NM should use RPC to connect to the RM.</summary>
		/// <remarks>
		/// Whether the NM should use RPC to connect to the RM. Default is false.
		/// Can be set to true only when using fixed ports.
		/// </remarks>
		public const string YarnMiniclusterUseRpc = YarnMcPrefix + "use-rpc";

		public const bool DefaultYarnMiniclusterUseRpc = false;

		/// <summary>
		/// Whether users are explicitly trying to control resource monitoring
		/// configuration for the MiniYARNCluster.
		/// </summary>
		/// <remarks>
		/// Whether users are explicitly trying to control resource monitoring
		/// configuration for the MiniYARNCluster. Disabled by default.
		/// </remarks>
		public const string YarnMiniclusterControlResourceMonitoring = YarnMcPrefix + "control-resource-monitoring";

		public const bool DefaultYarnMiniclusterControlResourceMonitoring = false;

		/// <summary>Allow changing the memory for the NodeManager in the MiniYARNCluster</summary>
		public const string YarnMiniclusterNmPmemMb = YarnMcPrefix + Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration
			.NmPmemMb;

		public const int DefaultYarnMiniclusterNmPmemMb = 4 * 1024;

		/// <summary>The log directory for the containers</summary>
		public const string YarnAppContainerLogDir = YarnPrefix + "app.container.log.dir";

		public const string YarnAppContainerLogSize = YarnPrefix + "app.container.log.filesize";

		public const string YarnAppContainerLogBackups = YarnPrefix + "app.container.log.backups";

		public const string TimelineServicePrefix = YarnPrefix + "timeline-service.";

		/// <summary>
		/// Comma seperated list of names for UIs hosted in the timeline server
		/// (For pluggable UIs).
		/// </summary>
		public const string TimelineServiceUiNames = TimelineServicePrefix + "ui-names";

		/// <summary>Relative web path that will serve up this UI (For pluggable UIs).</summary>
		public const string TimelineServiceUiWebPathPrefix = TimelineServicePrefix + "ui-web-path.";

		/// <summary>
		/// Path to war file or static content directory for this UI
		/// (For pluggable UIs).
		/// </summary>
		public const string TimelineServiceUiOnDiskPathPrefix = TimelineServicePrefix + "ui-on-disk-path.";

		[InterfaceAudience.Private]
		public const string ApplicationHistoryPrefix = TimelineServicePrefix + "generic-application-history.";

		/// <summary>
		/// The setting that controls whether application history service is
		/// enabled or not.
		/// </summary>
		[InterfaceAudience.Private]
		public const string ApplicationHistoryEnabled = ApplicationHistoryPrefix + "enabled";

		[InterfaceAudience.Private]
		public const bool DefaultApplicationHistoryEnabled = false;

		/// <summary>Application history store class</summary>
		[InterfaceAudience.Private]
		public const string ApplicationHistoryStore = ApplicationHistoryPrefix + "store-class";

		/// <summary>Save container meta-info in the application history store.</summary>
		[InterfaceAudience.Private]
		public const string ApplicationHistorySaveNonAmContainerMetaInfo = ApplicationHistoryPrefix
			 + "save-non-am-container-meta-info";

		[InterfaceAudience.Private]
		public const bool DefaultApplicationHistorySaveNonAmContainerMetaInfo = true;

		/// <summary>URI for FileSystemApplicationHistoryStore</summary>
		[InterfaceAudience.Private]
		public const string FsApplicationHistoryStoreUri = ApplicationHistoryPrefix + "fs-history-store.uri";

		/// <summary>T-file compression types used to compress history data.</summary>
		[InterfaceAudience.Private]
		public const string FsApplicationHistoryStoreCompressionType = ApplicationHistoryPrefix
			 + "fs-history-store.compression-type";

		[InterfaceAudience.Private]
		public const string DefaultFsApplicationHistoryStoreCompressionType = "none";

		/// <summary>The setting that controls whether timeline service is enabled or not.</summary>
		public const string TimelineServiceEnabled = TimelineServicePrefix + "enabled";

		public const bool DefaultTimelineServiceEnabled = false;

		/// <summary>host:port address for timeline service RPC APIs.</summary>
		public const string TimelineServiceAddress = TimelineServicePrefix + "address";

		public const int DefaultTimelineServicePort = 10200;

		public const string DefaultTimelineServiceAddress = "0.0.0.0:" + DefaultTimelineServicePort;

		/// <summary>The listening endpoint for the timeline service application.</summary>
		public const string TimelineServiceBindHost = TimelineServicePrefix + "bind-host";

		/// <summary>The number of threads to handle client RPC API requests.</summary>
		public const string TimelineServiceHandlerThreadCount = TimelineServicePrefix + "handler-thread-count";

		public const int DefaultTimelineServiceClientThreadCount = 10;

		/// <summary>The address of the timeline service web application.</summary>
		public const string TimelineServiceWebappAddress = TimelineServicePrefix + "webapp.address";

		public const int DefaultTimelineServiceWebappPort = 8188;

		public const string DefaultTimelineServiceWebappAddress = "0.0.0.0:" + DefaultTimelineServiceWebappPort;

		/// <summary>The https address of the timeline service web application.</summary>
		public const string TimelineServiceWebappHttpsAddress = TimelineServicePrefix + "webapp.https.address";

		public const int DefaultTimelineServiceWebappHttpsPort = 8190;

		public const string DefaultTimelineServiceWebappHttpsAddress = "0.0.0.0:" + DefaultTimelineServiceWebappHttpsPort;

		/// <summary>
		/// Defines the max number of applications could be fetched using
		/// REST API or application history protocol and shown in timeline
		/// server web ui.
		/// </summary>
		public const string ApplicationHistoryMaxApps = ApplicationHistoryPrefix + "max-applications";

		public const long DefaultApplicationHistoryMaxApps = 10000;

		/// <summary>Timeline service store class</summary>
		public const string TimelineServiceStore = TimelineServicePrefix + "store-class";

		/// <summary>Timeline service enable data age off</summary>
		public const string TimelineServiceTtlEnable = TimelineServicePrefix + "ttl-enable";

		/// <summary>Timeline service length of time to retain data</summary>
		public const string TimelineServiceTtlMs = TimelineServicePrefix + "ttl-ms";

		public const long DefaultTimelineServiceTtlMs = 1000 * 60 * 60 * 24 * 7;

		public const string TimelineServiceLeveldbPrefix = TimelineServicePrefix + "leveldb-timeline-store.";

		/// <summary>Timeline service leveldb path</summary>
		public const string TimelineServiceLeveldbPath = TimelineServiceLeveldbPrefix + "path";

		/// <summary>Timeline service leveldb read cache (uncompressed blocks)</summary>
		public const string TimelineServiceLeveldbReadCacheSize = TimelineServiceLeveldbPrefix
			 + "read-cache-size";

		public const long DefaultTimelineServiceLeveldbReadCacheSize = 100 * 1024 * 1024;

		/// <summary>Timeline service leveldb start time read cache (number of entities)</summary>
		public const string TimelineServiceLeveldbStartTimeReadCacheSize = TimelineServiceLeveldbPrefix
			 + "start-time-read-cache-size";

		public const int DefaultTimelineServiceLeveldbStartTimeReadCacheSize = 10000;

		/// <summary>Timeline service leveldb start time write cache (number of entities)</summary>
		public const string TimelineServiceLeveldbStartTimeWriteCacheSize = TimelineServiceLeveldbPrefix
			 + "start-time-write-cache-size";

		public const int DefaultTimelineServiceLeveldbStartTimeWriteCacheSize = 10000;

		/// <summary>Timeline service leveldb interval to wait between deletion rounds</summary>
		public const string TimelineServiceLeveldbTtlIntervalMs = TimelineServiceLeveldbPrefix
			 + "ttl-interval-ms";

		public const long DefaultTimelineServiceLeveldbTtlIntervalMs = 1000 * 60 * 5;

		/// <summary>The Kerberos principal for the timeline server.</summary>
		public const string TimelineServicePrincipal = TimelineServicePrefix + "principal";

		/// <summary>The Kerberos keytab for the timeline server.</summary>
		public const string TimelineServiceKeytab = TimelineServicePrefix + "keytab";

		/// <summary>Enables cross origin support for timeline server.</summary>
		public const string TimelineServiceHttpCrossOriginEnabled = TimelineServicePrefix
			 + "http-cross-origin.enabled";

		/// <summary>Default value for cross origin support for timeline server.</summary>
		public const bool TimelineServiceHttpCrossOriginEnabledDefault = false;

		/// <summary>Timeline client settings</summary>
		public const string TimelineServiceClientPrefix = TimelineServicePrefix + "client.";

		/// <summary>Timeline client call, max retries (-1 means no limit)</summary>
		public const string TimelineServiceClientMaxRetries = TimelineServiceClientPrefix
			 + "max-retries";

		public const int DefaultTimelineServiceClientMaxRetries = 30;

		/// <summary>Timeline client call, retry interval</summary>
		public const string TimelineServiceClientRetryIntervalMs = TimelineServiceClientPrefix
			 + "retry-interval-ms";

		public const long DefaultTimelineServiceClientRetryIntervalMs = 1000;

		/// <summary>Timeline client policy for whether connections are fatal</summary>
		public const string TimelineServiceClientBestEffort = TimelineServiceClientPrefix
			 + "best-effort";

		public const bool DefaultTimelineServiceClientBestEffort = false;

		/// <summary>Flag to enable recovery of timeline service</summary>
		public const string TimelineServiceRecoveryEnabled = TimelineServicePrefix + "recovery.enabled";

		public const bool DefaultTimelineServiceRecoveryEnabled = false;

		/// <summary>Timeline service state store class</summary>
		public const string TimelineServiceStateStoreClass = TimelineServicePrefix + "state-store-class";

		public const string TimelineServiceLeveldbStateStorePrefix = TimelineServicePrefix
			 + "leveldb-state-store.";

		/// <summary>Timeline service state store leveldb path</summary>
		public const string TimelineServiceLeveldbStateStorePath = TimelineServiceLeveldbStateStorePrefix
			 + "path";

		public const string TimelineDelegationKeyUpdateInterval = TimelineServicePrefix +
			 "delegation.key.update-interval";

		public const long DefaultTimelineDelegationKeyUpdateInterval = 24 * 60 * 60 * 1000;

		public const string TimelineDelegationTokenRenewInterval = TimelineServicePrefix 
			+ "delegation.token.renew-interval";

		public const long DefaultTimelineDelegationTokenRenewInterval = 24 * 60 * 60 * 1000;

		public const string TimelineDelegationTokenMaxLifetime = TimelineServicePrefix + 
			"delegation.token.max-lifetime";

		public const long DefaultTimelineDelegationTokenMaxLifetime = 7 * 24 * 60 * 60 * 
			1000;

		public const string SharedCachePrefix = "yarn.sharedcache.";

		/// <summary>whether the shared cache is enabled/disabled</summary>
		public const string SharedCacheEnabled = SharedCachePrefix + "enabled";

		public const bool DefaultSharedCacheEnabled = false;

		/// <summary>The config key for the shared cache root directory.</summary>
		public const string SharedCacheRoot = SharedCachePrefix + "root-dir";

		public const string DefaultSharedCacheRoot = "/sharedcache";

		/// <summary>
		/// The config key for the level of nested directories before getting to the
		/// checksum directory.
		/// </summary>
		public const string SharedCacheNestedLevel = SharedCachePrefix + "nested-level";

		public const int DefaultSharedCacheNestedLevel = 3;

		public const string ScmStorePrefix = SharedCachePrefix + "store.";

		public const string ScmStoreClass = ScmStorePrefix + "class";

		public const string DefaultScmStoreClass = "org.apache.hadoop.yarn.server.sharedcachemanager.store.InMemorySCMStore";

		public const string ScmAppCheckerClass = SharedCachePrefix + "app-checker.class";

		public const string DefaultScmAppCheckerClass = "org.apache.hadoop.yarn.server.sharedcachemanager.RemoteAppChecker";

		/// <summary>The address of the SCM admin interface.</summary>
		public const string ScmAdminAddress = SharedCachePrefix + "admin.address";

		public const int DefaultScmAdminPort = 8047;

		public const string DefaultScmAdminAddress = "0.0.0.0:" + DefaultScmAdminPort;

		/// <summary>Number of threads used to handle SCM admin interface.</summary>
		public const string ScmAdminClientThreadCount = SharedCachePrefix + "admin.thread-count";

		public const int DefaultScmAdminClientThreadCount = 1;

		/// <summary>The address of the SCM web application.</summary>
		public const string ScmWebappAddress = SharedCachePrefix + "webapp.address";

		public const int DefaultScmWebappPort = 8788;

		public const string DefaultScmWebappAddress = "0.0.0.0:" + DefaultScmWebappPort;

		public const string InMemoryStorePrefix = ScmStorePrefix + "in-memory.";

		/// <summary>
		/// A resource in the InMemorySCMStore is considered stale if the time since
		/// the last reference exceeds the staleness period.
		/// </summary>
		/// <remarks>
		/// A resource in the InMemorySCMStore is considered stale if the time since
		/// the last reference exceeds the staleness period. This value is specified in
		/// minutes.
		/// </remarks>
		public const string InMemoryStalenessPeriodMins = InMemoryStorePrefix + "staleness-period-mins";

		public const int DefaultInMemoryStalenessPeriodMins = 7 * 24 * 60;

		/// <summary>
		/// Initial delay before the in-memory store runs its first check to remove
		/// dead initial applications.
		/// </summary>
		/// <remarks>
		/// Initial delay before the in-memory store runs its first check to remove
		/// dead initial applications. Specified in minutes.
		/// </remarks>
		public const string InMemoryInitialDelayMins = InMemoryStorePrefix + "initial-delay-mins";

		public const int DefaultInMemoryInitialDelayMins = 10;

		/// <summary>
		/// The frequency at which the in-memory store checks to remove dead initial
		/// applications.
		/// </summary>
		/// <remarks>
		/// The frequency at which the in-memory store checks to remove dead initial
		/// applications. Specified in minutes.
		/// </remarks>
		public const string InMemoryCheckPeriodMins = InMemoryStorePrefix + "check-period-mins";

		public const int DefaultInMemoryCheckPeriodMins = 12 * 60;

		private const string ScmCleanerPrefix = SharedCachePrefix + "cleaner.";

		/// <summary>The frequency at which a cleaner task runs.</summary>
		/// <remarks>The frequency at which a cleaner task runs. Specified in minutes.</remarks>
		public const string ScmCleanerPeriodMins = ScmCleanerPrefix + "period-mins";

		public const int DefaultScmCleanerPeriodMins = 24 * 60;

		/// <summary>Initial delay before the first cleaner task is scheduled.</summary>
		/// <remarks>
		/// Initial delay before the first cleaner task is scheduled. Specified in
		/// minutes.
		/// </remarks>
		public const string ScmCleanerInitialDelayMins = ScmCleanerPrefix + "initial-delay-mins";

		public const int DefaultScmCleanerInitialDelayMins = 10;

		/// <summary>The time to sleep between processing each shared cache resource.</summary>
		/// <remarks>
		/// The time to sleep between processing each shared cache resource. Specified
		/// in milliseconds.
		/// </remarks>
		public const string ScmCleanerResourceSleepMs = ScmCleanerPrefix + "resource-sleep-ms";

		public const long DefaultScmCleanerResourceSleepMs = 0L;

		/// <summary>The address of the node manager interface in the SCM.</summary>
		public const string ScmUploaderServerAddress = SharedCachePrefix + "uploader.server.address";

		public const int DefaultScmUploaderServerPort = 8046;

		public const string DefaultScmUploaderServerAddress = "0.0.0.0:" + DefaultScmUploaderServerPort;

		/// <summary>
		/// The number of SCM threads used to handle notify requests from the node
		/// manager.
		/// </summary>
		public const string ScmUploaderServerThreadCount = SharedCachePrefix + "uploader.server.thread-count";

		public const int DefaultScmUploaderServerThreadCount = 50;

		/// <summary>The address of the client interface in the SCM.</summary>
		public const string ScmClientServerAddress = SharedCachePrefix + "client-server.address";

		public const int DefaultScmClientServerPort = 8045;

		public const string DefaultScmClientServerAddress = "0.0.0.0:" + DefaultScmClientServerPort;

		/// <summary>The number of threads used to handle shared cache manager requests.</summary>
		public const string ScmClientServerThreadCount = SharedCachePrefix + "client-server.thread-count";

		public const int DefaultScmClientServerThreadCount = 50;

		/// <summary>the checksum algorithm implementation</summary>
		public const string SharedCacheChecksumAlgoImpl = SharedCachePrefix + "checksum.algo.impl";

		public const string DefaultSharedCacheChecksumAlgoImpl = "org.apache.hadoop.yarn.sharedcache.ChecksumSHA256Impl";

		/// <summary>The replication factor for the node manager uploader for the shared cache.
		/// 	</summary>
		public const string SharedCacheNmUploaderReplicationFactor = SharedCachePrefix + 
			"nm.uploader.replication.factor";

		public const int DefaultSharedCacheNmUploaderReplicationFactor = 10;

		public const string SharedCacheNmUploaderThreadCount = SharedCachePrefix + "nm.uploader.thread-count";

		public const int DefaultSharedCacheNmUploaderThreadCount = 20;

		/// <summary>Use YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS instead.</summary>
		/// <remarks>
		/// Use YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS instead.
		/// The interval of the yarn client's querying application state after
		/// application submission. The unit is millisecond.
		/// </remarks>
		[Obsolete]
		public const string YarnClientAppSubmissionPollIntervalMs = YarnPrefix + "client.app-submission.poll-interval";

		/// <summary>
		/// The interval that the yarn client library uses to poll the completion
		/// status of the asynchronous API of application client protocol.
		/// </summary>
		public const string YarnClientApplicationClientProtocolPollIntervalMs = YarnPrefix
			 + "client.application-client-protocol.poll-interval-ms";

		public const long DefaultYarnClientApplicationClientProtocolPollIntervalMs = 200;

		/// <summary>
		/// The duration that the yarn client library waits, cumulatively across polls,
		/// for an expected state change to occur.
		/// </summary>
		/// <remarks>
		/// The duration that the yarn client library waits, cumulatively across polls,
		/// for an expected state change to occur. Defaults to -1, which indicates no
		/// limit.
		/// </remarks>
		public const string YarnClientApplicationClientProtocolPollTimeoutMs = YarnPrefix
			 + "client.application-client-protocol.poll-timeout-ms";

		public const long DefaultYarnClientApplicationClientProtocolPollTimeoutMs = -1;

		/// <summary>
		/// Max number of threads in NMClientAsync to process container management
		/// events
		/// </summary>
		public const string NmClientAsyncThreadPoolMaxSize = YarnPrefix + "client.nodemanager-client-async.thread-pool-max-size";

		public const int DefaultNmClientAsyncThreadPoolMaxSize = 500;

		/// <summary>Maximum number of proxy connections to cache for node managers.</summary>
		/// <remarks>
		/// Maximum number of proxy connections to cache for node managers. If set
		/// to a value greater than zero then the cache is enabled and the NMClient
		/// and MRAppMaster will cache the specified number of node manager proxies.
		/// There will be at max one proxy per node manager. Ex. configuring it to a
		/// value of 5 will make sure that client will at max have 5 proxies cached
		/// with 5 different node managers. These connections for these proxies will
		/// be timed out if idle for more than the system wide idle timeout period.
		/// Note that this could cause issues on large clusters as many connections
		/// could linger simultaneously and lead to a large number of connection
		/// threads. The token used for authentication will be used only at
		/// connection creation time. If a new token is received then the earlier
		/// connection should be closed in order to use the new token. This and
		/// <see cref="NmClientAsyncThreadPoolMaxSize"/>
		/// are related
		/// and should be in sync (no need for them to be equal).
		/// If the value of this property is zero then the connection cache is
		/// disabled and connections will use a zero idle timeout to prevent too
		/// many connection threads on large clusters.
		/// </remarks>
		public const string NmClientMaxNmProxies = YarnPrefix + "client.max-cached-nodemanagers-proxies";

		public const int DefaultNmClientMaxNmProxies = 0;

		/// <summary>Max time to wait to establish a connection to NM</summary>
		public const string ClientNmConnectMaxWaitMs = YarnPrefix + "client.nodemanager-connect.max-wait-ms";

		public const long DefaultClientNmConnectMaxWaitMs = 3 * 60 * 1000;

		/// <summary>Time interval between each attempt to connect to NM</summary>
		public const string ClientNmConnectRetryIntervalMs = YarnPrefix + "client.nodemanager-connect.retry-interval-ms";

		public const long DefaultClientNmConnectRetryIntervalMs = 10 * 1000;

		public const string YarnHttpPolicyKey = YarnPrefix + "http.policy";

		public static readonly string YarnHttpPolicyDefault = HttpConfig.Policy.HttpOnly.
			ToString();

		/// <summary>Node-labels configurations</summary>
		public const string NodeLabelsPrefix = YarnPrefix + "node-labels.";

		/// <summary>URI for NodeLabelManager</summary>
		public const string FsNodeLabelsStoreRootDir = NodeLabelsPrefix + "fs-store.root-dir";

		public const string FsNodeLabelsStoreRetryPolicySpec = NodeLabelsPrefix + "fs-store.retry-policy-spec";

		public const string DefaultFsNodeLabelsStoreRetryPolicySpec = "2000, 500";

		/// <summary>
		/// Flag to indicate if the node labels feature enabled, by default it's
		/// disabled
		/// </summary>
		public const string NodeLabelsEnabled = NodeLabelsPrefix + "enabled";

		public const bool DefaultNodeLabelsEnabled = false;

		public YarnConfiguration()
			: base()
		{
		}

		public YarnConfiguration(Configuration conf)
			: base(conf)
		{
			//Configurations
			////////////////////////////////
			// IPC Configs
			////////////////////////////////
			////////////////////////////////
			// Resource Manager Configs
			////////////////////////////////
			//RM delegation token related keys
			// 1 day
			// 1 day
			// 7 days
			////////////////////////////////
			// RM state store configs
			////////////////////////////////
			////////////////////////////////
			// Node Manager Configs
			////////////////////////////////
			////////////////////////////////
			// Web Proxy Configs
			////////////////////////////////
			////////////////////////////////
			// Timeline Service Configs
			////////////////////////////////
			// mark app-history related configs @Private as application history is going
			// to be integrated into the timeline service
			// Timeline delegation token related keys
			// 1 day
			// 1 day
			// 7 days
			// ///////////////////////////////
			// Shared Cache Configs
			// ///////////////////////////////
			// common configs
			// Shared Cache Manager Configs
			// In-memory SCM store configuration
			// SCM Cleaner service configuration
			// node manager (uploader) configs
			////////////////////////////////
			// Other Configs
			////////////////////////////////
			if (!(conf is Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration))
			{
				this.ReloadConfiguration();
			}
		}

		[InterfaceAudience.Private]
		public static IList<string> GetServiceAddressConfKeys(Configuration conf)
		{
			return UseHttps(conf) ? RmServicesAddressConfKeysHttps : RmServicesAddressConfKeysHttp;
		}

		/// <summary>
		/// Get the socket address for <code>name</code> property as a
		/// <code>InetSocketAddress</code>.
		/// </summary>
		/// <remarks>
		/// Get the socket address for <code>name</code> property as a
		/// <code>InetSocketAddress</code>. On a HA cluster,
		/// this fetches the address corresponding to the RM identified by
		/// <see cref="RmHaId"/>
		/// .
		/// </remarks>
		/// <param name="name">property name.</param>
		/// <param name="defaultAddress">the default value</param>
		/// <param name="defaultPort">the default port</param>
		/// <returns>InetSocketAddress</returns>
		public override IPEndPoint GetSocketAddr(string name, string defaultAddress, int 
			defaultPort)
		{
			string address;
			if (HAUtil.IsHAEnabled(this) && GetServiceAddressConfKeys(this).Contains(name))
			{
				address = HAUtil.GetConfValueForRMInstance(name, defaultAddress, this);
			}
			else
			{
				address = Get(name, defaultAddress);
			}
			return NetUtils.CreateSocketAddr(address, defaultPort, name);
		}

		public override IPEndPoint UpdateConnectAddr(string name, IPEndPoint addr)
		{
			string prefix = name;
			if (HAUtil.IsHAEnabled(this))
			{
				prefix = HAUtil.AddSuffix(prefix, HAUtil.GetRMHAId(this));
			}
			return base.UpdateConnectAddr(prefix, addr);
		}

		[InterfaceAudience.Private]
		public static int GetRMDefaultPortNumber(string addressPrefix, Configuration conf
			)
		{
			if (addressPrefix.Equals(Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.RmAddress))
			{
				return Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.DefaultRmPort;
			}
			else
			{
				if (addressPrefix.Equals(Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.RmSchedulerAddress
					))
				{
					return Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.DefaultRmSchedulerPort;
				}
				else
				{
					if (addressPrefix.Equals(Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.RmWebappAddress
						))
					{
						return Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.DefaultRmWebappPort;
					}
					else
					{
						if (addressPrefix.Equals(Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.RmWebappHttpsAddress
							))
						{
							return Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.DefaultRmWebappHttpsPort;
						}
						else
						{
							if (addressPrefix.Equals(Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.RmResourceTrackerAddress
								))
							{
								return Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.DefaultRmResourceTrackerPort;
							}
							else
							{
								if (addressPrefix.Equals(Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.RmAdminAddress
									))
								{
									return Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.DefaultRmAdminPort;
								}
								else
								{
									throw new HadoopIllegalArgumentException("Invalid RM RPC address Prefix: " + addressPrefix
										 + ". The valid value should be one of " + GetServiceAddressConfKeys(conf));
								}
							}
						}
					}
				}
			}
		}

		public static bool UseHttps(Configuration conf)
		{
			return HttpConfig.Policy.HttpsOnly == HttpConfig.Policy.FromString(conf.Get(YarnHttpPolicyKey
				, YarnHttpPolicyDefault));
		}

		public static bool ShouldRMFailFast(Configuration conf)
		{
			return conf.GetBoolean(Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.RmFailFast, 
				conf.GetBoolean(Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.YarnFailFast, Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration
				.DefaultYarnFailFast));
		}

		[InterfaceAudience.Private]
		public static string GetClusterId(Configuration conf)
		{
			string clusterId = conf.Get(Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.RmClusterId
				);
			if (clusterId == null)
			{
				throw new HadoopIllegalArgumentException("Configuration doesn't specify " + Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration
					.RmClusterId);
			}
			return clusterId;
		}
	}
}
