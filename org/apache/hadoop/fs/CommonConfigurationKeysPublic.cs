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
	/// It includes all publicly documented configuration keys. In general
	/// this class should not be used directly (use CommonConfigurationKeys
	/// instead)
	/// </remarks>
	public class CommonConfigurationKeysPublic
	{
		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IO_NATIVE_LIB_AVAILABLE_KEY = "io.native.lib.available";

		/// <summary>Default value for IO_NATIVE_LIB_AVAILABLE_KEY</summary>
		public const bool IO_NATIVE_LIB_AVAILABLE_DEFAULT = true;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY = "net.topology.script.number.args";

		/// <summary>Default value for NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY</summary>
		public const int NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_DEFAULT = 100;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FS_DEFAULT_NAME_KEY = "fs.defaultFS";

		/// <summary>Default value for FS_DEFAULT_NAME_KEY</summary>
		public const string FS_DEFAULT_NAME_DEFAULT = "file:///";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FS_DF_INTERVAL_KEY = "fs.df.interval";

		/// <summary>Default value for FS_DF_INTERVAL_KEY</summary>
		public const long FS_DF_INTERVAL_DEFAULT = 60000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FS_DU_INTERVAL_KEY = "fs.du.interval";

		/// <summary>Default value for FS_DU_INTERVAL_KEY</summary>
		public const long FS_DU_INTERVAL_DEFAULT = 600000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY = "fs.client.resolve.remote.symlinks";

		/// <summary>Default value for FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY</summary>
		public const bool FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_DEFAULT = true;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY = "net.topology.script.file.name";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY = "net.topology.node.switch.mapping.impl";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string NET_TOPOLOGY_IMPL_KEY = "net.topology.impl";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY = "net.topology.table.file.name";

		public const string NET_DEPENDENCY_SCRIPT_FILE_NAME_KEY = "net.topology.dependency.script.file.name";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FS_TRASH_CHECKPOINT_INTERVAL_KEY = "fs.trash.checkpoint.interval";

		/// <summary>Default value for FS_TRASH_CHECKPOINT_INTERVAL_KEY</summary>
		public const long FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT = 0;

		/// <summary>Not used anywhere, looks like default value for FS_LOCAL_BLOCK_SIZE</summary>
		public const long FS_LOCAL_BLOCK_SIZE_DEFAULT = 32 * 1024 * 1024;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FS_AUTOMATIC_CLOSE_KEY = "fs.automatic.close";

		/// <summary>Default value for FS_AUTOMATIC_CLOSE_KEY</summary>
		public const bool FS_AUTOMATIC_CLOSE_DEFAULT = true;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FS_FILE_IMPL_KEY = "fs.file.impl";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FS_FTP_HOST_KEY = "fs.ftp.host";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FS_FTP_HOST_PORT_KEY = "fs.ftp.host.port";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FS_TRASH_INTERVAL_KEY = "fs.trash.interval";

		/// <summary>Default value for FS_TRASH_INTERVAL_KEY</summary>
		public const long FS_TRASH_INTERVAL_DEFAULT = 0;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IO_MAPFILE_BLOOM_SIZE_KEY = "io.mapfile.bloom.size";

		/// <summary>Default value for IO_MAPFILE_BLOOM_SIZE_KEY</summary>
		public const int IO_MAPFILE_BLOOM_SIZE_DEFAULT = 1024 * 1024;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IO_MAPFILE_BLOOM_ERROR_RATE_KEY = "io.mapfile.bloom.error.rate";

		/// <summary>Default value for IO_MAPFILE_BLOOM_ERROR_RATE_KEY</summary>
		public const float IO_MAPFILE_BLOOM_ERROR_RATE_DEFAULT = 0.005f;

		/// <summary>Codec class that implements Lzo compression algorithm</summary>
		public const string IO_COMPRESSION_CODEC_LZO_CLASS_KEY = "io.compression.codec.lzo.class";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IO_MAP_INDEX_INTERVAL_KEY = "io.map.index.interval";

		/// <summary>Default value for IO_MAP_INDEX_INTERVAL_DEFAULT</summary>
		public const int IO_MAP_INDEX_INTERVAL_DEFAULT = 128;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IO_MAP_INDEX_SKIP_KEY = "io.map.index.skip";

		/// <summary>Default value for IO_MAP_INDEX_SKIP_KEY</summary>
		public const int IO_MAP_INDEX_SKIP_DEFAULT = 0;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY = "io.seqfile.compress.blocksize";

		/// <summary>Default value for IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY</summary>
		public const int IO_SEQFILE_COMPRESS_BLOCKSIZE_DEFAULT = 1000000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IO_FILE_BUFFER_SIZE_KEY = "io.file.buffer.size";

		/// <summary>Default value for IO_FILE_BUFFER_SIZE_KEY</summary>
		public const int IO_FILE_BUFFER_SIZE_DEFAULT = 4096;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IO_SKIP_CHECKSUM_ERRORS_KEY = "io.skip.checksum.errors";

		/// <summary>Default value for IO_SKIP_CHECKSUM_ERRORS_KEY</summary>
		public const bool IO_SKIP_CHECKSUM_ERRORS_DEFAULT = false;

		[System.ObsoleteAttribute(@"Moved to mapreduce, see mapreduce.task.io.sort.mb in mapred-default.xml See https://issues.apache.org/jira/browse/HADOOP-6801"
			)]
		public const string IO_SORT_MB_KEY = "io.sort.mb";

		/// <summary>Default value for IO_SORT_MB_DEFAULT</summary>
		public const int IO_SORT_MB_DEFAULT = 100;

		[System.ObsoleteAttribute(@"Moved to mapreduce, see mapreduce.task.io.sort.factor in mapred-default.xml See https://issues.apache.org/jira/browse/HADOOP-6801"
			)]
		public const string IO_SORT_FACTOR_KEY = "io.sort.factor";

		/// <summary>Default value for IO_SORT_FACTOR_DEFAULT</summary>
		public const int IO_SORT_FACTOR_DEFAULT = 100;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IO_SERIALIZATIONS_KEY = "io.serializations";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string TFILE_IO_CHUNK_SIZE_KEY = "tfile.io.chunk.size";

		/// <summary>Default value for TFILE_IO_CHUNK_SIZE_DEFAULT</summary>
		public const int TFILE_IO_CHUNK_SIZE_DEFAULT = 1024 * 1024;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string TFILE_FS_INPUT_BUFFER_SIZE_KEY = "tfile.fs.input.buffer.size";

		/// <summary>Default value for TFILE_FS_INPUT_BUFFER_SIZE_KEY</summary>
		public const int TFILE_FS_INPUT_BUFFER_SIZE_DEFAULT = 256 * 1024;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string TFILE_FS_OUTPUT_BUFFER_SIZE_KEY = "tfile.fs.output.buffer.size";

		/// <summary>Default value for TFILE_FS_OUTPUT_BUFFER_SIZE_KEY</summary>
		public const int TFILE_FS_OUTPUT_BUFFER_SIZE_DEFAULT = 256 * 1024;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY = "ipc.client.connection.maxidletime";

		/// <summary>Default value for IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY</summary>
		public const int IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT = 10000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IPC_CLIENT_CONNECT_TIMEOUT_KEY = "ipc.client.connect.timeout";

		/// <summary>Default value for IPC_CLIENT_CONNECT_TIMEOUT_KEY</summary>
		public const int IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT = 20000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IPC_CLIENT_CONNECT_MAX_RETRIES_KEY = "ipc.client.connect.max.retries";

		/// <summary>Default value for IPC_CLIENT_CONNECT_MAX_RETRIES_KEY</summary>
		public const int IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT = 10;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY = "ipc.client.connect.retry.interval";

		/// <summary>Default value for IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY</summary>
		public const int IPC_CLIENT_CONNECT_RETRY_INTERVAL_DEFAULT = 1000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY = "ipc.client.connect.max.retries.on.timeouts";

		/// <summary>Default value for IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY</summary>
		public const int IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 45;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IPC_CLIENT_TCPNODELAY_KEY = "ipc.client.tcpnodelay";

		/// <summary>Defalt value for IPC_CLIENT_TCPNODELAY_KEY</summary>
		public const bool IPC_CLIENT_TCPNODELAY_DEFAULT = true;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IPC_SERVER_LISTEN_QUEUE_SIZE_KEY = "ipc.server.listen.queue.size";

		/// <summary>Default value for IPC_SERVER_LISTEN_QUEUE_SIZE_KEY</summary>
		public const int IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT = 128;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IPC_CLIENT_KILL_MAX_KEY = "ipc.client.kill.max";

		/// <summary>Default value for IPC_CLIENT_KILL_MAX_KEY</summary>
		public const int IPC_CLIENT_KILL_MAX_DEFAULT = 10;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IPC_CLIENT_IDLETHRESHOLD_KEY = "ipc.client.idlethreshold";

		/// <summary>Default value for IPC_CLIENT_IDLETHRESHOLD_DEFAULT</summary>
		public const int IPC_CLIENT_IDLETHRESHOLD_DEFAULT = 4000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IPC_SERVER_TCPNODELAY_KEY = "ipc.server.tcpnodelay";

		/// <summary>Default value for IPC_SERVER_TCPNODELAY_KEY</summary>
		public const bool IPC_SERVER_TCPNODELAY_DEFAULT = true;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IPC_SERVER_MAX_CONNECTIONS_KEY = "ipc.server.max.connections";

		/// <summary>Default value for IPC_SERVER_MAX_CONNECTIONS_KEY</summary>
		public const int IPC_SERVER_MAX_CONNECTIONS_DEFAULT = 0;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY = "hadoop.rpc.socket.factory.class.default";

		public const string HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT = "org.apache.hadoop.net.StandardSocketFactory";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SOCKS_SERVER_KEY = "hadoop.socks.server";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_UTIL_HASH_TYPE_KEY = "hadoop.util.hash.type";

		/// <summary>Default value for HADOOP_UTIL_HASH_TYPE_KEY</summary>
		public const string HADOOP_UTIL_HASH_TYPE_DEFAULT = "murmur";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_GROUP_MAPPING = "hadoop.security.group.mapping";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_GROUPS_CACHE_SECS = "hadoop.security.groups.cache.secs";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const long HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT = 300;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS = "hadoop.security.groups.negative-cache.secs";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const long HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS_DEFAULT = 30;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_GROUPS_CACHE_WARN_AFTER_MS = "hadoop.security.groups.cache.warn.after.ms";

		public const long HADOOP_SECURITY_GROUPS_CACHE_WARN_AFTER_MS_DEFAULT = 5000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN = "hadoop.security.instrumentation.requires.admin";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_SERVICE_USER_NAME_KEY = "hadoop.security.service.user.name.key";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_AUTH_TO_LOCAL = "hadoop.security.auth_to_local";

		[System.Obsolete]
		public const string HADOOP_SSL_ENABLED_KEY = "hadoop.ssl.enabled";

		[System.Obsolete]
		public const bool HADOOP_SSL_ENABLED_DEFAULT = false;

		[System.Obsolete]
		public const string HTTP_POLICY_HTTP_ONLY = "HTTP_ONLY";

		[System.Obsolete]
		public const string HTTP_POLICY_HTTPS_ONLY = "HTTPS_ONLY";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_RPC_PROTECTION = "hadoop.rpc.protection";

		/// <summary>Class to override Sasl Properties for a connection</summary>
		public const string HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS = "hadoop.security.saslproperties.resolver.class";

		public const string HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX = "hadoop.security.crypto.codec.classes";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY = "hadoop.security.crypto.cipher.suite";

		public const string HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT = "AES/CTR/NoPadding";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY = "hadoop.security.crypto.jce.provider";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY = "hadoop.security.crypto.buffer.size";

		/// <summary>Defalt value for HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY</summary>
		public const int HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT = 8192;

		/// <summary>Class to override Impersonation provider</summary>
		public const string HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS = "hadoop.security.impersonation.provider.class";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string KMS_CLIENT_ENC_KEY_CACHE_SIZE = "hadoop.security.kms.client.encrypted.key.cache.size";

		/// <summary>Default value for KMS_CLIENT_ENC_KEY_CACHE_SIZE</summary>
		public const int KMS_CLIENT_ENC_KEY_CACHE_SIZE_DEFAULT = 500;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK = "hadoop.security.kms.client.encrypted.key.cache.low-watermark";

		/// <summary>Default value for KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK</summary>
		public const float KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK_DEFAULT = 0.3f;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS = "hadoop.security.kms.client.encrypted.key.cache.num.refill.threads";

		/// <summary>Default value for KMS_CLIENT_ENC_KEY_NUM_REFILL_THREADS</summary>
		public const int KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS_DEFAULT = 2;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS = "hadoop.security.kms.client.encrypted.key.cache.expiry";

		/// <summary>Default value for KMS_CLIENT_ENC_KEY_CACHE_EXPIRY (12 hrs)</summary>
		public const int KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_DEFAULT = 43200000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY = "hadoop.security.java.secure.random.algorithm";

		/// <summary>Defalt value for HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY</summary>
		public const string HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT = "SHA1PRNG";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_SECURE_RANDOM_IMPL_KEY = "hadoop.security.secure.random.impl";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY = "hadoop.security.random.device.file.path";

		public const string HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT = "/dev/urandom";
		// The Keys
		//FS keys
		//Defaults are not specified for following keys
		// TBD: Code is still using hardcoded values (e.g. "fs.automatic.close")
		// instead of constant (e.g. FS_AUTOMATIC_CLOSE_KEY)
		//
		// 10s
		// 20s
		// HTTP policies to be used in configuration
		// Use HttpPolicy.name() instead
		//  <!-- KMSClientProvider configurations -->
	}
}
