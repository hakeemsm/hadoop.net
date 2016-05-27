using System;
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
		public const string IoNativeLibAvailableKey = "io.native.lib.available";

		/// <summary>Default value for IO_NATIVE_LIB_AVAILABLE_KEY</summary>
		public const bool IoNativeLibAvailableDefault = true;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string NetTopologyScriptNumberArgsKey = "net.topology.script.number.args";

		/// <summary>Default value for NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY</summary>
		public const int NetTopologyScriptNumberArgsDefault = 100;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FsDefaultNameKey = "fs.defaultFS";

		/// <summary>Default value for FS_DEFAULT_NAME_KEY</summary>
		public const string FsDefaultNameDefault = "file:///";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FsDfIntervalKey = "fs.df.interval";

		/// <summary>Default value for FS_DF_INTERVAL_KEY</summary>
		public const long FsDfIntervalDefault = 60000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FsDuIntervalKey = "fs.du.interval";

		/// <summary>Default value for FS_DU_INTERVAL_KEY</summary>
		public const long FsDuIntervalDefault = 600000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FsClientResolveRemoteSymlinksKey = "fs.client.resolve.remote.symlinks";

		/// <summary>Default value for FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY</summary>
		public const bool FsClientResolveRemoteSymlinksDefault = true;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string NetTopologyScriptFileNameKey = "net.topology.script.file.name";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string NetTopologyNodeSwitchMappingImplKey = "net.topology.node.switch.mapping.impl";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string NetTopologyImplKey = "net.topology.impl";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string NetTopologyTableMappingFileKey = "net.topology.table.file.name";

		public const string NetDependencyScriptFileNameKey = "net.topology.dependency.script.file.name";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FsTrashCheckpointIntervalKey = "fs.trash.checkpoint.interval";

		/// <summary>Default value for FS_TRASH_CHECKPOINT_INTERVAL_KEY</summary>
		public const long FsTrashCheckpointIntervalDefault = 0;

		/// <summary>Not used anywhere, looks like default value for FS_LOCAL_BLOCK_SIZE</summary>
		public const long FsLocalBlockSizeDefault = 32 * 1024 * 1024;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FsAutomaticCloseKey = "fs.automatic.close";

		/// <summary>Default value for FS_AUTOMATIC_CLOSE_KEY</summary>
		public const bool FsAutomaticCloseDefault = true;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FsFileImplKey = "fs.file.impl";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FsFtpHostKey = "fs.ftp.host";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FsFtpHostPortKey = "fs.ftp.host.port";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string FsTrashIntervalKey = "fs.trash.interval";

		/// <summary>Default value for FS_TRASH_INTERVAL_KEY</summary>
		public const long FsTrashIntervalDefault = 0;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IoMapfileBloomSizeKey = "io.mapfile.bloom.size";

		/// <summary>Default value for IO_MAPFILE_BLOOM_SIZE_KEY</summary>
		public const int IoMapfileBloomSizeDefault = 1024 * 1024;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IoMapfileBloomErrorRateKey = "io.mapfile.bloom.error.rate";

		/// <summary>Default value for IO_MAPFILE_BLOOM_ERROR_RATE_KEY</summary>
		public const float IoMapfileBloomErrorRateDefault = 0.005f;

		/// <summary>Codec class that implements Lzo compression algorithm</summary>
		public const string IoCompressionCodecLzoClassKey = "io.compression.codec.lzo.class";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IoMapIndexIntervalKey = "io.map.index.interval";

		/// <summary>Default value for IO_MAP_INDEX_INTERVAL_DEFAULT</summary>
		public const int IoMapIndexIntervalDefault = 128;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IoMapIndexSkipKey = "io.map.index.skip";

		/// <summary>Default value for IO_MAP_INDEX_SKIP_KEY</summary>
		public const int IoMapIndexSkipDefault = 0;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IoSeqfileCompressBlocksizeKey = "io.seqfile.compress.blocksize";

		/// <summary>Default value for IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY</summary>
		public const int IoSeqfileCompressBlocksizeDefault = 1000000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IoFileBufferSizeKey = "io.file.buffer.size";

		/// <summary>Default value for IO_FILE_BUFFER_SIZE_KEY</summary>
		public const int IoFileBufferSizeDefault = 4096;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IoSkipChecksumErrorsKey = "io.skip.checksum.errors";

		/// <summary>Default value for IO_SKIP_CHECKSUM_ERRORS_KEY</summary>
		public const bool IoSkipChecksumErrorsDefault = false;

		[System.ObsoleteAttribute(@"Moved to mapreduce, see mapreduce.task.io.sort.mb in mapred-default.xml See https://issues.apache.org/jira/browse/HADOOP-6801"
			)]
		public const string IoSortMbKey = "io.sort.mb";

		/// <summary>Default value for IO_SORT_MB_DEFAULT</summary>
		public const int IoSortMbDefault = 100;

		[System.ObsoleteAttribute(@"Moved to mapreduce, see mapreduce.task.io.sort.factor in mapred-default.xml See https://issues.apache.org/jira/browse/HADOOP-6801"
			)]
		public const string IoSortFactorKey = "io.sort.factor";

		/// <summary>Default value for IO_SORT_FACTOR_DEFAULT</summary>
		public const int IoSortFactorDefault = 100;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IoSerializationsKey = "io.serializations";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string TfileIoChunkSizeKey = "tfile.io.chunk.size";

		/// <summary>Default value for TFILE_IO_CHUNK_SIZE_DEFAULT</summary>
		public const int TfileIoChunkSizeDefault = 1024 * 1024;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string TfileFsInputBufferSizeKey = "tfile.fs.input.buffer.size";

		/// <summary>Default value for TFILE_FS_INPUT_BUFFER_SIZE_KEY</summary>
		public const int TfileFsInputBufferSizeDefault = 256 * 1024;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string TfileFsOutputBufferSizeKey = "tfile.fs.output.buffer.size";

		/// <summary>Default value for TFILE_FS_OUTPUT_BUFFER_SIZE_KEY</summary>
		public const int TfileFsOutputBufferSizeDefault = 256 * 1024;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IpcClientConnectionMaxidletimeKey = "ipc.client.connection.maxidletime";

		/// <summary>Default value for IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY</summary>
		public const int IpcClientConnectionMaxidletimeDefault = 10000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IpcClientConnectTimeoutKey = "ipc.client.connect.timeout";

		/// <summary>Default value for IPC_CLIENT_CONNECT_TIMEOUT_KEY</summary>
		public const int IpcClientConnectTimeoutDefault = 20000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IpcClientConnectMaxRetriesKey = "ipc.client.connect.max.retries";

		/// <summary>Default value for IPC_CLIENT_CONNECT_MAX_RETRIES_KEY</summary>
		public const int IpcClientConnectMaxRetriesDefault = 10;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IpcClientConnectRetryIntervalKey = "ipc.client.connect.retry.interval";

		/// <summary>Default value for IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY</summary>
		public const int IpcClientConnectRetryIntervalDefault = 1000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IpcClientConnectMaxRetriesOnSocketTimeoutsKey = "ipc.client.connect.max.retries.on.timeouts";

		/// <summary>Default value for IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY</summary>
		public const int IpcClientConnectMaxRetriesOnSocketTimeoutsDefault = 45;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IpcClientTcpnodelayKey = "ipc.client.tcpnodelay";

		/// <summary>Defalt value for IPC_CLIENT_TCPNODELAY_KEY</summary>
		public const bool IpcClientTcpnodelayDefault = true;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IpcServerListenQueueSizeKey = "ipc.server.listen.queue.size";

		/// <summary>Default value for IPC_SERVER_LISTEN_QUEUE_SIZE_KEY</summary>
		public const int IpcServerListenQueueSizeDefault = 128;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IpcClientKillMaxKey = "ipc.client.kill.max";

		/// <summary>Default value for IPC_CLIENT_KILL_MAX_KEY</summary>
		public const int IpcClientKillMaxDefault = 10;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IpcClientIdlethresholdKey = "ipc.client.idlethreshold";

		/// <summary>Default value for IPC_CLIENT_IDLETHRESHOLD_DEFAULT</summary>
		public const int IpcClientIdlethresholdDefault = 4000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IpcServerTcpnodelayKey = "ipc.server.tcpnodelay";

		/// <summary>Default value for IPC_SERVER_TCPNODELAY_KEY</summary>
		public const bool IpcServerTcpnodelayDefault = true;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string IpcServerMaxConnectionsKey = "ipc.server.max.connections";

		/// <summary>Default value for IPC_SERVER_MAX_CONNECTIONS_KEY</summary>
		public const int IpcServerMaxConnectionsDefault = 0;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopRpcSocketFactoryClassDefaultKey = "hadoop.rpc.socket.factory.class.default";

		public const string HadoopRpcSocketFactoryClassDefaultDefault = "org.apache.hadoop.net.StandardSocketFactory";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSocksServerKey = "hadoop.socks.server";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopUtilHashTypeKey = "hadoop.util.hash.type";

		/// <summary>Default value for HADOOP_UTIL_HASH_TYPE_KEY</summary>
		public const string HadoopUtilHashTypeDefault = "murmur";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityGroupMapping = "hadoop.security.group.mapping";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityGroupsCacheSecs = "hadoop.security.groups.cache.secs";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const long HadoopSecurityGroupsCacheSecsDefault = 300;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityGroupsNegativeCacheSecs = "hadoop.security.groups.negative-cache.secs";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const long HadoopSecurityGroupsNegativeCacheSecsDefault = 30;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityGroupsCacheWarnAfterMs = "hadoop.security.groups.cache.warn.after.ms";

		public const long HadoopSecurityGroupsCacheWarnAfterMsDefault = 5000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityAuthentication = "hadoop.security.authentication";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityAuthorization = "hadoop.security.authorization";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityInstrumentationRequiresAdmin = "hadoop.security.instrumentation.requires.admin";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityServiceUserNameKey = "hadoop.security.service.user.name.key";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityAuthToLocal = "hadoop.security.auth_to_local";

		[Obsolete]
		public const string HadoopSslEnabledKey = "hadoop.ssl.enabled";

		[Obsolete]
		public const bool HadoopSslEnabledDefault = false;

		[Obsolete]
		public const string HttpPolicyHttpOnly = "HTTP_ONLY";

		[Obsolete]
		public const string HttpPolicyHttpsOnly = "HTTPS_ONLY";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopRpcProtection = "hadoop.rpc.protection";

		/// <summary>Class to override Sasl Properties for a connection</summary>
		public const string HadoopSecuritySaslPropsResolverClass = "hadoop.security.saslproperties.resolver.class";

		public const string HadoopSecurityCryptoCodecClassesKeyPrefix = "hadoop.security.crypto.codec.classes";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityCryptoCipherSuiteKey = "hadoop.security.crypto.cipher.suite";

		public const string HadoopSecurityCryptoCipherSuiteDefault = "AES/CTR/NoPadding";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityCryptoJceProviderKey = "hadoop.security.crypto.jce.provider";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityCryptoBufferSizeKey = "hadoop.security.crypto.buffer.size";

		/// <summary>Defalt value for HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY</summary>
		public const int HadoopSecurityCryptoBufferSizeDefault = 8192;

		/// <summary>Class to override Impersonation provider</summary>
		public const string HadoopSecurityImpersonationProviderClass = "hadoop.security.impersonation.provider.class";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string KmsClientEncKeyCacheSize = "hadoop.security.kms.client.encrypted.key.cache.size";

		/// <summary>Default value for KMS_CLIENT_ENC_KEY_CACHE_SIZE</summary>
		public const int KmsClientEncKeyCacheSizeDefault = 500;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string KmsClientEncKeyCacheLowWatermark = "hadoop.security.kms.client.encrypted.key.cache.low-watermark";

		/// <summary>Default value for KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK</summary>
		public const float KmsClientEncKeyCacheLowWatermarkDefault = 0.3f;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string KmsClientEncKeyCacheNumRefillThreads = "hadoop.security.kms.client.encrypted.key.cache.num.refill.threads";

		/// <summary>Default value for KMS_CLIENT_ENC_KEY_NUM_REFILL_THREADS</summary>
		public const int KmsClientEncKeyCacheNumRefillThreadsDefault = 2;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string KmsClientEncKeyCacheExpiryMs = "hadoop.security.kms.client.encrypted.key.cache.expiry";

		/// <summary>Default value for KMS_CLIENT_ENC_KEY_CACHE_EXPIRY (12 hrs)</summary>
		public const int KmsClientEncKeyCacheExpiryDefault = 43200000;

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecurityJavaSecureRandomAlgorithmKey = "hadoop.security.java.secure.random.algorithm";

		/// <summary>Defalt value for HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY</summary>
		public const string HadoopSecurityJavaSecureRandomAlgorithmDefault = "SHA1PRNG";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecuritySecureRandomImplKey = "hadoop.security.secure.random.impl";

		/// <summary>
		/// See &lt;a href="
		/// <docRoot/>
		/// /../core-default.html"&gt;core-default.xml</a>
		/// </summary>
		public const string HadoopSecuritySecureRandomDeviceFilePathKey = "hadoop.security.random.device.file.path";

		public const string HadoopSecuritySecureRandomDeviceFilePathDefault = "/dev/urandom";
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
