using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// This class contains constants for configuration keys used
	/// in the common code.
	/// </summary>
	/// <remarks>
	/// This class contains constants for configuration keys used
	/// in the common code.
	/// It inherits all the publicly documented configuration keys
	/// and adds unsupported keys.
	/// </remarks>
	public class CommonConfigurationKeys : CommonConfigurationKeysPublic
	{
		/// <summary>Default location for user home directories</summary>
		public const string FsHomeDirKey = "fs.homeDir";

		/// <summary>Default value for FS_HOME_DIR_KEY</summary>
		public const string FsHomeDirDefault = "/user";

		/// <summary>Default umask for files created in HDFS</summary>
		public const string FsPermissionsUmaskKey = "fs.permissions.umask-mode";

		/// <summary>Default value for FS_PERMISSIONS_UMASK_KEY</summary>
		public const int FsPermissionsUmaskDefault = 0x12;

		/// <summary>How often does RPC client send pings to RPC server</summary>
		public const string IpcPingIntervalKey = "ipc.ping.interval";

		/// <summary>Default value for IPC_PING_INTERVAL_KEY</summary>
		public const int IpcPingIntervalDefault = 60000;

		/// <summary>Enables pings from RPC client to the server</summary>
		public const string IpcClientPingKey = "ipc.client.ping";

		/// <summary>Default value of IPC_CLIENT_PING_KEY</summary>
		public const bool IpcClientPingDefault = true;

		/// <summary>Responses larger than this will be logged</summary>
		public const string IpcServerRpcMaxResponseSizeKey = "ipc.server.max.response.size";

		/// <summary>Default value for IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY</summary>
		public const int IpcServerRpcMaxResponseSizeDefault = 1024 * 1024;

		/// <summary>Number of threads in RPC server reading from the socket</summary>
		public const string IpcServerRpcReadThreadsKey = "ipc.server.read.threadpool.size";

		/// <summary>Default value for IPC_SERVER_RPC_READ_THREADS_KEY</summary>
		public const int IpcServerRpcReadThreadsDefault = 1;

		/// <summary>Number of pending connections that may be queued per socket reader</summary>
		public const string IpcServerRpcReadConnectionQueueSizeKey = "ipc.server.read.connection-queue.size";

		/// <summary>Default value for IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE</summary>
		public const int IpcServerRpcReadConnectionQueueSizeDefault = 100;

		public const string IpcMaximumDataLength = "ipc.maximum.data.length";

		public const int IpcMaximumDataLengthDefault = 64 * 1024 * 1024;

		/// <summary>How many calls per handler are allowed in the queue.</summary>
		public const string IpcServerHandlerQueueSizeKey = "ipc.server.handler.queue.size";

		/// <summary>Default value for IPC_SERVER_HANDLER_QUEUE_SIZE_KEY</summary>
		public const int IpcServerHandlerQueueSizeDefault = 100;

		/// <summary>CallQueue related settings.</summary>
		/// <remarks>
		/// CallQueue related settings. These are not used directly, but rather
		/// combined with a namespace and port. For instance:
		/// IPC_CALLQUEUE_NAMESPACE + ".8020." + IPC_CALLQUEUE_IMPL_KEY
		/// </remarks>
		public const string IpcCallqueueNamespace = "ipc";

		public const string IpcCallqueueImplKey = "callqueue.impl";

		public const string IpcCallqueueIdentityProviderKey = "identity-provider.impl";

		/// <summary>
		/// This is for specifying the implementation for the mappings from
		/// hostnames to the racks they belong to
		/// </summary>
		public const string NetTopologyConfiguredNodeMappingKey = "net.topology.configured.node.mapping";

		/// <summary>Supported compression codec classes</summary>
		public const string IoCompressionCodecsKey = "io.compression.codecs";

		/// <summary>Internal buffer size for Lzo compressor/decompressors</summary>
		public const string IoCompressionCodecLzoBuffersizeKey = "io.compression.codec.lzo.buffersize";

		/// <summary>Default value for IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY</summary>
		public const int IoCompressionCodecLzoBuffersizeDefault = 64 * 1024;

		/// <summary>Internal buffer size for Snappy compressor/decompressors</summary>
		public const string IoCompressionCodecSnappyBuffersizeKey = "io.compression.codec.snappy.buffersize";

		/// <summary>Default value for IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY</summary>
		public const int IoCompressionCodecSnappyBuffersizeDefault = 256 * 1024;

		/// <summary>Internal buffer size for Lz4 compressor/decompressors</summary>
		public const string IoCompressionCodecLz4BuffersizeKey = "io.compression.codec.lz4.buffersize";

		/// <summary>Default value for IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY</summary>
		public const int IoCompressionCodecLz4BuffersizeDefault = 256 * 1024;

		/// <summary>Use lz4hc(slow but with high compression ratio) for lz4 compression</summary>
		public const string IoCompressionCodecLz4Uselz4hcKey = "io.compression.codec.lz4.use.lz4hc";

		/// <summary>Default value for IO_COMPRESSION_CODEC_USELZ4HC_KEY</summary>
		public const bool IoCompressionCodecLz4Uselz4hcDefault = false;

		/// <summary>Service Authorization</summary>
		public const string HadoopSecurityServiceAuthorizationDefaultAcl = "security.service.authorization.default.acl";

		public const string HadoopSecurityServiceAuthorizationDefaultBlockedAcl = "security.service.authorization.default.acl.blocked";

		public const string HadoopSecurityServiceAuthorizationRefreshPolicy = "security.refresh.policy.protocol.acl";

		public const string HadoopSecurityServiceAuthorizationGetUserMappings = "security.get.user.mappings.protocol.acl";

		public const string HadoopSecurityServiceAuthorizationRefreshUserMappings = "security.refresh.user.mappings.protocol.acl";

		public const string HadoopSecurityServiceAuthorizationRefreshCallqueue = "security.refresh.callqueue.protocol.acl";

		public const string HadoopSecurityServiceAuthorizationGenericRefresh = "security.refresh.generic.protocol.acl";

		public const string HadoopSecurityServiceAuthorizationTracing = "security.trace.protocol.acl";

		public const string SecurityHaServiceProtocolAcl = "security.ha.service.protocol.acl";

		public const string SecurityZkfcProtocolAcl = "security.zkfc.protocol.acl";

		public const string SecurityClientProtocolAcl = "security.client.protocol.acl";

		public const string SecurityClientDatanodeProtocolAcl = "security.client.datanode.protocol.acl";

		public const string SecurityDatanodeProtocolAcl = "security.datanode.protocol.acl";

		public const string SecurityInterDatanodeProtocolAcl = "security.inter.datanode.protocol.acl";

		public const string SecurityNamenodeProtocolAcl = "security.namenode.protocol.acl";

		public const string SecurityQjournalServiceProtocolAcl = "security.qjournal.service.protocol.acl";

		public const string HadoopSecurityTokenServiceUseIp = "hadoop.security.token.service.use_ip";

		public const bool HadoopSecurityTokenServiceUseIpDefault = true;

		/// <summary>How often to retry connecting to the service.</summary>
		public const string HaHmConnectRetryIntervalKey = "ha.health-monitor.connect-retry-interval.ms";

		public const long HaHmConnectRetryIntervalDefault = 1000;

		public const string HaHmCheckIntervalKey = "ha.health-monitor.check-interval.ms";

		public const long HaHmCheckIntervalDefault = 1000;

		public const string HaHmSleepAfterDisconnectKey = "ha.health-monitor.sleep-after-disconnect.ms";

		public const long HaHmSleepAfterDisconnectDefault = 1000;

		public const string HaHmRpcTimeoutKey = "ha.health-monitor.rpc-timeout.ms";

		public const int HaHmRpcTimeoutDefault = 45000;

		public const string HaFcNewActiveTimeoutKey = "ha.failover-controller.new-active.rpc-timeout.ms";

		public const int HaFcNewActiveTimeoutDefault = 60000;

		public const string HaFcGracefulFenceTimeoutKey = "ha.failover-controller.graceful-fence.rpc-timeout.ms";

		public const int HaFcGracefulFenceTimeoutDefault = 5000;

		public const string HaFcGracefulFenceConnectionRetries = "ha.failover-controller.graceful-fence.connection.retries";

		public const int HaFcGracefulFenceConnectionRetriesDefault = 1;

		/// <summary>number of zookeeper operation retry times in ActiveStandbyElector</summary>
		public const string HaFcElectorZkOpRetriesKey = "ha.failover-controller.active-standby-elector.zk.op.retries";

		public const int HaFcElectorZkOpRetriesDefault = 3;

		public const string HaFcCliCheckTimeoutKey = "ha.failover-controller.cli-check.rpc-timeout.ms";

		public const int HaFcCliCheckTimeoutDefault = 20000;

		/// <summary>Static user web-filter properties.</summary>
		/// <remarks>
		/// Static user web-filter properties.
		/// See
		/// <see cref="Org.Apache.Hadoop.Http.Lib.StaticUserWebFilter"/>
		/// .
		/// </remarks>
		public const string HadoopHttpStaticUser = "hadoop.http.staticuser.user";

		public const string DefaultHadoopHttpStaticUser = "dr.who";

		/// <summary>User-&gt;groups static mapping to override the groups lookup</summary>
		public const string HadoopUserGroupStaticOverrides = "hadoop.user.group.static.mapping.overrides";

		public const string HadoopUserGroupStaticOverridesDefault = "dr.who=;";

		/// <summary>Enable/Disable aliases serving from jetty</summary>
		public const string HadoopJettyLogsServeAliases = "hadoop.jetty.logs.serve.aliases";

		public const bool DefaultHadoopJettyLogsServeAliases = true;

		public const string KerberosTicketCachePath = "hadoop.security.kerberos.ticket.cache.path";

		public const string HadoopSecurityUidNameCacheTimeoutKey = "hadoop.security.uid.cache.secs";

		public const long HadoopSecurityUidNameCacheTimeoutDefault = 4 * 60 * 60;

		public const string IpcClientFallbackToSimpleAuthAllowedKey = "ipc.client.fallback-to-simple-auth-allowed";

		public const bool IpcClientFallbackToSimpleAuthAllowedDefault = false;

		public const string IpcClientConnectMaxRetriesOnSaslKey = "ipc.client.connect.max.retries.on.sasl";

		public const int IpcClientConnectMaxRetriesOnSaslDefault = 5;

		/// <summary>How often the server scans for idle connections</summary>
		public const string IpcClientConnectionIdlescanintervalKey = "ipc.client.connection.idle-scan-interval.ms";

		/// <summary>Default value for IPC_SERVER_CONNECTION_IDLE_SCAN_INTERVAL_KEY</summary>
		public const int IpcClientConnectionIdlescanintervalDefault = 10000;

		public const string HadoopUserGroupMetricsPercentilesIntervals = "hadoop.user.group.metrics.percentiles.intervals";

		public const string RpcMetricsQuantileEnable = "rpc.metrics.quantile.enable";

		public const bool RpcMetricsQuantileEnableDefault = false;

		public const string RpcMetricsPercentilesIntervalsKey = "rpc.metrics.percentiles.intervals";

		/// <summary>Allowed hosts for nfs exports</summary>
		public const string NfsExportsAllowedHostsSeparator = ";";

		public const string NfsExportsAllowedHostsKey = "nfs.exports.allowed.hosts";

		public const string NfsExportsAllowedHostsKeyDefault = "* rw";
		// 1 min
		/* How often to check the service. */
		/* How long to sleep after an unexpected RPC error. */
		/* Timeout for the actual monitorHealth() calls. */
		/* Timeout that the FC waits for the new active to become active */
		/* Timeout that the FC waits for the old active to go to standby */
		/* FC connection retries for graceful fencing */
		/* Timeout that the CLI (manual) FC waits for monitorHealth, getServiceState */
		/* Path to the Kerberos ticket cache.  Setting this will force
		* UserGroupInformation to use only this ticket cache file when creating a
		* FileSystem instance.
		*/
		// 4 hours
	}
}
