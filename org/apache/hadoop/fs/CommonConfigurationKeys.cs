using Sharpen;

namespace org.apache.hadoop.fs
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
	public class CommonConfigurationKeys : org.apache.hadoop.fs.CommonConfigurationKeysPublic
	{
		/// <summary>Default location for user home directories</summary>
		public const string FS_HOME_DIR_KEY = "fs.homeDir";

		/// <summary>Default value for FS_HOME_DIR_KEY</summary>
		public const string FS_HOME_DIR_DEFAULT = "/user";

		/// <summary>Default umask for files created in HDFS</summary>
		public const string FS_PERMISSIONS_UMASK_KEY = "fs.permissions.umask-mode";

		/// <summary>Default value for FS_PERMISSIONS_UMASK_KEY</summary>
		public const int FS_PERMISSIONS_UMASK_DEFAULT = 0x12;

		/// <summary>How often does RPC client send pings to RPC server</summary>
		public const string IPC_PING_INTERVAL_KEY = "ipc.ping.interval";

		/// <summary>Default value for IPC_PING_INTERVAL_KEY</summary>
		public const int IPC_PING_INTERVAL_DEFAULT = 60000;

		/// <summary>Enables pings from RPC client to the server</summary>
		public const string IPC_CLIENT_PING_KEY = "ipc.client.ping";

		/// <summary>Default value of IPC_CLIENT_PING_KEY</summary>
		public const bool IPC_CLIENT_PING_DEFAULT = true;

		/// <summary>Responses larger than this will be logged</summary>
		public const string IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY = "ipc.server.max.response.size";

		/// <summary>Default value for IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY</summary>
		public const int IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT = 1024 * 1024;

		/// <summary>Number of threads in RPC server reading from the socket</summary>
		public const string IPC_SERVER_RPC_READ_THREADS_KEY = "ipc.server.read.threadpool.size";

		/// <summary>Default value for IPC_SERVER_RPC_READ_THREADS_KEY</summary>
		public const int IPC_SERVER_RPC_READ_THREADS_DEFAULT = 1;

		/// <summary>Number of pending connections that may be queued per socket reader</summary>
		public const string IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY = "ipc.server.read.connection-queue.size";

		/// <summary>Default value for IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE</summary>
		public const int IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_DEFAULT = 100;

		public const string IPC_MAXIMUM_DATA_LENGTH = "ipc.maximum.data.length";

		public const int IPC_MAXIMUM_DATA_LENGTH_DEFAULT = 64 * 1024 * 1024;

		/// <summary>How many calls per handler are allowed in the queue.</summary>
		public const string IPC_SERVER_HANDLER_QUEUE_SIZE_KEY = "ipc.server.handler.queue.size";

		/// <summary>Default value for IPC_SERVER_HANDLER_QUEUE_SIZE_KEY</summary>
		public const int IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT = 100;

		/// <summary>CallQueue related settings.</summary>
		/// <remarks>
		/// CallQueue related settings. These are not used directly, but rather
		/// combined with a namespace and port. For instance:
		/// IPC_CALLQUEUE_NAMESPACE + ".8020." + IPC_CALLQUEUE_IMPL_KEY
		/// </remarks>
		public const string IPC_CALLQUEUE_NAMESPACE = "ipc";

		public const string IPC_CALLQUEUE_IMPL_KEY = "callqueue.impl";

		public const string IPC_CALLQUEUE_IDENTITY_PROVIDER_KEY = "identity-provider.impl";

		/// <summary>
		/// This is for specifying the implementation for the mappings from
		/// hostnames to the racks they belong to
		/// </summary>
		public const string NET_TOPOLOGY_CONFIGURED_NODE_MAPPING_KEY = "net.topology.configured.node.mapping";

		/// <summary>Supported compression codec classes</summary>
		public const string IO_COMPRESSION_CODECS_KEY = "io.compression.codecs";

		/// <summary>Internal buffer size for Lzo compressor/decompressors</summary>
		public const string IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY = "io.compression.codec.lzo.buffersize";

		/// <summary>Default value for IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY</summary>
		public const int IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT = 64 * 1024;

		/// <summary>Internal buffer size for Snappy compressor/decompressors</summary>
		public const string IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY = "io.compression.codec.snappy.buffersize";

		/// <summary>Default value for IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY</summary>
		public const int IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT = 256 * 1024;

		/// <summary>Internal buffer size for Lz4 compressor/decompressors</summary>
		public const string IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY = "io.compression.codec.lz4.buffersize";

		/// <summary>Default value for IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY</summary>
		public const int IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT = 256 * 1024;

		/// <summary>Use lz4hc(slow but with high compression ratio) for lz4 compression</summary>
		public const string IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY = "io.compression.codec.lz4.use.lz4hc";

		/// <summary>Default value for IO_COMPRESSION_CODEC_USELZ4HC_KEY</summary>
		public const bool IO_COMPRESSION_CODEC_LZ4_USELZ4HC_DEFAULT = false;

		/// <summary>Service Authorization</summary>
		public const string HADOOP_SECURITY_SERVICE_AUTHORIZATION_DEFAULT_ACL = "security.service.authorization.default.acl";

		public const string HADOOP_SECURITY_SERVICE_AUTHORIZATION_DEFAULT_BLOCKED_ACL = "security.service.authorization.default.acl.blocked";

		public const string HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_POLICY = "security.refresh.policy.protocol.acl";

		public const string HADOOP_SECURITY_SERVICE_AUTHORIZATION_GET_USER_MAPPINGS = "security.get.user.mappings.protocol.acl";

		public const string HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_USER_MAPPINGS = 
			"security.refresh.user.mappings.protocol.acl";

		public const string HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_CALLQUEUE = "security.refresh.callqueue.protocol.acl";

		public const string HADOOP_SECURITY_SERVICE_AUTHORIZATION_GENERIC_REFRESH = "security.refresh.generic.protocol.acl";

		public const string HADOOP_SECURITY_SERVICE_AUTHORIZATION_TRACING = "security.trace.protocol.acl";

		public const string SECURITY_HA_SERVICE_PROTOCOL_ACL = "security.ha.service.protocol.acl";

		public const string SECURITY_ZKFC_PROTOCOL_ACL = "security.zkfc.protocol.acl";

		public const string SECURITY_CLIENT_PROTOCOL_ACL = "security.client.protocol.acl";

		public const string SECURITY_CLIENT_DATANODE_PROTOCOL_ACL = "security.client.datanode.protocol.acl";

		public const string SECURITY_DATANODE_PROTOCOL_ACL = "security.datanode.protocol.acl";

		public const string SECURITY_INTER_DATANODE_PROTOCOL_ACL = "security.inter.datanode.protocol.acl";

		public const string SECURITY_NAMENODE_PROTOCOL_ACL = "security.namenode.protocol.acl";

		public const string SECURITY_QJOURNAL_SERVICE_PROTOCOL_ACL = "security.qjournal.service.protocol.acl";

		public const string HADOOP_SECURITY_TOKEN_SERVICE_USE_IP = "hadoop.security.token.service.use_ip";

		public const bool HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT = true;

		/// <summary>How often to retry connecting to the service.</summary>
		public const string HA_HM_CONNECT_RETRY_INTERVAL_KEY = "ha.health-monitor.connect-retry-interval.ms";

		public const long HA_HM_CONNECT_RETRY_INTERVAL_DEFAULT = 1000;

		public const string HA_HM_CHECK_INTERVAL_KEY = "ha.health-monitor.check-interval.ms";

		public const long HA_HM_CHECK_INTERVAL_DEFAULT = 1000;

		public const string HA_HM_SLEEP_AFTER_DISCONNECT_KEY = "ha.health-monitor.sleep-after-disconnect.ms";

		public const long HA_HM_SLEEP_AFTER_DISCONNECT_DEFAULT = 1000;

		public const string HA_HM_RPC_TIMEOUT_KEY = "ha.health-monitor.rpc-timeout.ms";

		public const int HA_HM_RPC_TIMEOUT_DEFAULT = 45000;

		public const string HA_FC_NEW_ACTIVE_TIMEOUT_KEY = "ha.failover-controller.new-active.rpc-timeout.ms";

		public const int HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT = 60000;

		public const string HA_FC_GRACEFUL_FENCE_TIMEOUT_KEY = "ha.failover-controller.graceful-fence.rpc-timeout.ms";

		public const int HA_FC_GRACEFUL_FENCE_TIMEOUT_DEFAULT = 5000;

		public const string HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES = "ha.failover-controller.graceful-fence.connection.retries";

		public const int HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES_DEFAULT = 1;

		/// <summary>number of zookeeper operation retry times in ActiveStandbyElector</summary>
		public const string HA_FC_ELECTOR_ZK_OP_RETRIES_KEY = "ha.failover-controller.active-standby-elector.zk.op.retries";

		public const int HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT = 3;

		public const string HA_FC_CLI_CHECK_TIMEOUT_KEY = "ha.failover-controller.cli-check.rpc-timeout.ms";

		public const int HA_FC_CLI_CHECK_TIMEOUT_DEFAULT = 20000;

		/// <summary>Static user web-filter properties.</summary>
		/// <remarks>
		/// Static user web-filter properties.
		/// See
		/// <see cref="org.apache.hadoop.http.lib.StaticUserWebFilter"/>
		/// .
		/// </remarks>
		public const string HADOOP_HTTP_STATIC_USER = "hadoop.http.staticuser.user";

		public const string DEFAULT_HADOOP_HTTP_STATIC_USER = "dr.who";

		/// <summary>User-&gt;groups static mapping to override the groups lookup</summary>
		public const string HADOOP_USER_GROUP_STATIC_OVERRIDES = "hadoop.user.group.static.mapping.overrides";

		public const string HADOOP_USER_GROUP_STATIC_OVERRIDES_DEFAULT = "dr.who=;";

		/// <summary>Enable/Disable aliases serving from jetty</summary>
		public const string HADOOP_JETTY_LOGS_SERVE_ALIASES = "hadoop.jetty.logs.serve.aliases";

		public const bool DEFAULT_HADOOP_JETTY_LOGS_SERVE_ALIASES = true;

		public const string KERBEROS_TICKET_CACHE_PATH = "hadoop.security.kerberos.ticket.cache.path";

		public const string HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY = "hadoop.security.uid.cache.secs";

		public const long HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT = 4 * 60 * 60;

		public const string IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY = "ipc.client.fallback-to-simple-auth-allowed";

		public const bool IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT = false;

		public const string IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY = "ipc.client.connect.max.retries.on.sasl";

		public const int IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_DEFAULT = 5;

		/// <summary>How often the server scans for idle connections</summary>
		public const string IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY = "ipc.client.connection.idle-scan-interval.ms";

		/// <summary>Default value for IPC_SERVER_CONNECTION_IDLE_SCAN_INTERVAL_KEY</summary>
		public const int IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT = 10000;

		public const string HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS = "hadoop.user.group.metrics.percentiles.intervals";

		public const string RPC_METRICS_QUANTILE_ENABLE = "rpc.metrics.quantile.enable";

		public const bool RPC_METRICS_QUANTILE_ENABLE_DEFAULT = false;

		public const string RPC_METRICS_PERCENTILES_INTERVALS_KEY = "rpc.metrics.percentiles.intervals";

		/// <summary>Allowed hosts for nfs exports</summary>
		public const string NFS_EXPORTS_ALLOWED_HOSTS_SEPARATOR = ";";

		public const string NFS_EXPORTS_ALLOWED_HOSTS_KEY = "nfs.exports.allowed.hosts";

		public const string NFS_EXPORTS_ALLOWED_HOSTS_KEY_DEFAULT = "* rw";
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
