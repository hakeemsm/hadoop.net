using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.Net;
using Javax.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Tracing;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// DFSClient can connect to a Hadoop Filesystem and
	/// perform basic file tasks.
	/// </summary>
	/// <remarks>
	/// DFSClient can connect to a Hadoop Filesystem and
	/// perform basic file tasks.  It uses the ClientProtocol
	/// to communicate with a NameNode daemon, and connects
	/// directly to DataNodes to read/write block data.
	/// Hadoop DFS users should obtain an instance of
	/// DistributedFileSystem, which uses DFSClient to handle
	/// filesystem tasks.
	/// </remarks>
	public class DFSClient : IDisposable, RemotePeerFactory, DataEncryptionKeyFactory
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.DFSClient
			));

		public const long ServerDefaultsValidityPeriod = 60 * 60 * 1000L;

		internal const int TcpWindowSize = 128 * 1024;

		private readonly Configuration conf;

		private readonly DFSClient.Conf dfsClientConf;

		internal readonly ClientProtocol namenode;

		private Text dtService;

		internal readonly UserGroupInformation ugi;

		internal volatile bool clientRunning = true;

		internal volatile long lastLeaseRenewal;

		private volatile FsServerDefaults serverDefaults;

		private volatile long serverDefaultsLastUpdate;

		internal readonly string clientName;

		internal readonly SocketFactory socketFactory;

		internal readonly ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure;

		internal readonly FileSystem.Statistics stats;

		private readonly string authority;

		private readonly Random r = new Random();

		private EndPoint[] localInterfaceAddrs;

		private DataEncryptionKey encryptionKey;

		internal readonly SaslDataTransferClient saslClient;

		private readonly CachingStrategy defaultReadCachingStrategy;

		private readonly CachingStrategy defaultWriteCachingStrategy;

		private readonly ClientContext clientContext;

		private volatile long hedgedReadThresholdMillis;

		private static readonly DFSHedgedReadMetrics HedgedReadMetric = new DFSHedgedReadMetrics
			();

		private static ThreadPoolExecutor HedgedReadThreadPool;

		private readonly Sampler<object> traceSampler;

		/// <summary>DFSClient configuration</summary>
		public class Conf
		{
			internal readonly int hdfsTimeout;

			internal readonly int maxFailoverAttempts;

			internal readonly int maxRetryAttempts;

			internal readonly int failoverSleepBaseMillis;

			internal readonly int failoverSleepMaxMillis;

			internal readonly int maxBlockAcquireFailures;

			internal readonly int confTime;

			internal readonly int ioBufferSize;

			internal readonly Options.ChecksumOpt defaultChecksumOpt;

			internal readonly int writePacketSize;

			internal readonly int writeMaxPackets;

			internal readonly ByteArrayManager.Conf writeByteArrayManagerConf;

			internal readonly int socketTimeout;

			internal readonly int socketCacheCapacity;

			internal readonly long socketCacheExpiry;

			internal readonly long excludedNodesCacheExpiry;

			/// <summary>Wait time window (in msec) if BlockMissingException is caught</summary>
			internal readonly int timeWindow;

			internal readonly int nCachedConnRetry;

			internal readonly int nBlockWriteRetry;

			internal readonly int nBlockWriteLocateFollowingRetry;

			internal readonly long defaultBlockSize;

			internal readonly long prefetchSize;

			internal readonly short defaultReplication;

			internal readonly string taskId;

			internal readonly FsPermission uMask;

			internal readonly bool connectToDnViaHostname;

			internal readonly bool getHdfsBlocksMetadataEnabled;

			internal readonly int getFileBlockStorageLocationsNumThreads;

			internal readonly int getFileBlockStorageLocationsTimeoutMs;

			internal readonly int retryTimesForGetLastBlockLength;

			internal readonly int retryIntervalForGetLastBlockLength;

			internal readonly long datanodeRestartTimeout;

			internal readonly long dfsclientSlowIoWarningThresholdMs;

			internal readonly bool useLegacyBlockReader;

			internal readonly bool useLegacyBlockReaderLocal;

			internal readonly string domainSocketPath;

			internal readonly bool skipShortCircuitChecksums;

			internal readonly int shortCircuitBufferSize;

			internal readonly bool shortCircuitLocalReads;

			internal readonly bool domainSocketDataTraffic;

			internal readonly int shortCircuitStreamsCacheSize;

			internal readonly long shortCircuitStreamsCacheExpiryMs;

			internal readonly int shortCircuitSharedMemoryWatcherInterruptCheckMs;

			internal readonly bool shortCircuitMmapEnabled;

			internal readonly int shortCircuitMmapCacheSize;

			internal readonly long shortCircuitMmapCacheExpiryMs;

			internal readonly long shortCircuitMmapCacheRetryTimeout;

			internal readonly long shortCircuitCacheStaleThresholdMs;

			internal readonly long keyProviderCacheExpiryMs;

			public BlockReaderFactory.FailureInjector brfFailureInjector = new BlockReaderFactory.FailureInjector
				();

			public Conf(Configuration conf)
			{
				// 1 hour
				// 128 KB
				/* The service used for delegation tokens */
				// timeout value for a DFS operation.
				// The hdfsTimeout is currently the same as the ipc timeout 
				hdfsTimeout = Client.GetTimeout(conf);
				maxFailoverAttempts = conf.GetInt(DFSConfigKeys.DfsClientFailoverMaxAttemptsKey, 
					DFSConfigKeys.DfsClientFailoverMaxAttemptsDefault);
				maxRetryAttempts = conf.GetInt(DFSConfigKeys.DfsClientRetryMaxAttemptsKey, DFSConfigKeys
					.DfsClientRetryMaxAttemptsDefault);
				failoverSleepBaseMillis = conf.GetInt(DFSConfigKeys.DfsClientFailoverSleeptimeBaseKey
					, DFSConfigKeys.DfsClientFailoverSleeptimeBaseDefault);
				failoverSleepMaxMillis = conf.GetInt(DFSConfigKeys.DfsClientFailoverSleeptimeMaxKey
					, DFSConfigKeys.DfsClientFailoverSleeptimeMaxDefault);
				maxBlockAcquireFailures = conf.GetInt(DFSConfigKeys.DfsClientMaxBlockAcquireFailuresKey
					, DFSConfigKeys.DfsClientMaxBlockAcquireFailuresDefault);
				confTime = conf.GetInt(DFSConfigKeys.DfsDatanodeSocketWriteTimeoutKey, HdfsServerConstants
					.WriteTimeout);
				ioBufferSize = conf.GetInt(CommonConfigurationKeysPublic.IoFileBufferSizeKey, CommonConfigurationKeysPublic
					.IoFileBufferSizeDefault);
				defaultChecksumOpt = GetChecksumOptFromConf(conf);
				socketTimeout = conf.GetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, HdfsServerConstants
					.ReadTimeout);
				writePacketSize = conf.GetInt(DFSConfigKeys.DfsClientWritePacketSizeKey, DFSConfigKeys
					.DfsClientWritePacketSizeDefault);
				writeMaxPackets = conf.GetInt(DFSConfigKeys.DfsClientWriteMaxPacketsInFlightKey, 
					DFSConfigKeys.DfsClientWriteMaxPacketsInFlightDefault);
				bool byteArrayManagerEnabled = conf.GetBoolean(DFSConfigKeys.DfsClientWriteByteArrayManagerEnabledKey
					, DFSConfigKeys.DfsClientWriteByteArrayManagerEnabledDefault);
				if (!byteArrayManagerEnabled)
				{
					writeByteArrayManagerConf = null;
				}
				else
				{
					int countThreshold = conf.GetInt(DFSConfigKeys.DfsClientWriteByteArrayManagerCountThresholdKey
						, DFSConfigKeys.DfsClientWriteByteArrayManagerCountThresholdDefault);
					int countLimit = conf.GetInt(DFSConfigKeys.DfsClientWriteByteArrayManagerCountLimitKey
						, DFSConfigKeys.DfsClientWriteByteArrayManagerCountLimitDefault);
					long countResetTimePeriodMs = conf.GetLong(DFSConfigKeys.DfsClientWriteByteArrayManagerCountResetTimePeriodMsKey
						, DFSConfigKeys.DfsClientWriteByteArrayManagerCountResetTimePeriodMsDefault);
					writeByteArrayManagerConf = new ByteArrayManager.Conf(countThreshold, countLimit, 
						countResetTimePeriodMs);
				}
				defaultBlockSize = conf.GetLongBytes(DFSConfigKeys.DfsBlockSizeKey, DFSConfigKeys
					.DfsBlockSizeDefault);
				defaultReplication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, DFSConfigKeys
					.DfsReplicationDefault);
				taskId = conf.Get("mapreduce.task.attempt.id", "NONMAPREDUCE");
				socketCacheCapacity = conf.GetInt(DFSConfigKeys.DfsClientSocketCacheCapacityKey, 
					DFSConfigKeys.DfsClientSocketCacheCapacityDefault);
				socketCacheExpiry = conf.GetLong(DFSConfigKeys.DfsClientSocketCacheExpiryMsecKey, 
					DFSConfigKeys.DfsClientSocketCacheExpiryMsecDefault);
				excludedNodesCacheExpiry = conf.GetLong(DFSConfigKeys.DfsClientWriteExcludeNodesCacheExpiryInterval
					, DFSConfigKeys.DfsClientWriteExcludeNodesCacheExpiryIntervalDefault);
				prefetchSize = conf.GetLong(DFSConfigKeys.DfsClientReadPrefetchSizeKey, 10 * defaultBlockSize
					);
				timeWindow = conf.GetInt(DFSConfigKeys.DfsClientRetryWindowBase, 3000);
				nCachedConnRetry = conf.GetInt(DFSConfigKeys.DfsClientCachedConnRetryKey, DFSConfigKeys
					.DfsClientCachedConnRetryDefault);
				nBlockWriteRetry = conf.GetInt(DFSConfigKeys.DfsClientBlockWriteRetriesKey, DFSConfigKeys
					.DfsClientBlockWriteRetriesDefault);
				nBlockWriteLocateFollowingRetry = conf.GetInt(DFSConfigKeys.DfsClientBlockWriteLocatefollowingblockRetriesKey
					, DFSConfigKeys.DfsClientBlockWriteLocatefollowingblockRetriesDefault);
				uMask = FsPermission.GetUMask(conf);
				connectToDnViaHostname = conf.GetBoolean(DFSConfigKeys.DfsClientUseDnHostname, DFSConfigKeys
					.DfsClientUseDnHostnameDefault);
				getHdfsBlocksMetadataEnabled = conf.GetBoolean(DFSConfigKeys.DfsHdfsBlocksMetadataEnabled
					, DFSConfigKeys.DfsHdfsBlocksMetadataEnabledDefault);
				getFileBlockStorageLocationsNumThreads = conf.GetInt(DFSConfigKeys.DfsClientFileBlockStorageLocationsNumThreads
					, DFSConfigKeys.DfsClientFileBlockStorageLocationsNumThreadsDefault);
				getFileBlockStorageLocationsTimeoutMs = conf.GetInt(DFSConfigKeys.DfsClientFileBlockStorageLocationsTimeoutMs
					, DFSConfigKeys.DfsClientFileBlockStorageLocationsTimeoutMsDefault);
				retryTimesForGetLastBlockLength = conf.GetInt(DFSConfigKeys.DfsClientRetryTimesGetLastBlockLength
					, DFSConfigKeys.DfsClientRetryTimesGetLastBlockLengthDefault);
				retryIntervalForGetLastBlockLength = conf.GetInt(DFSConfigKeys.DfsClientRetryIntervalGetLastBlockLength
					, DFSConfigKeys.DfsClientRetryIntervalGetLastBlockLengthDefault);
				useLegacyBlockReader = conf.GetBoolean(DFSConfigKeys.DfsClientUseLegacyBlockreader
					, DFSConfigKeys.DfsClientUseLegacyBlockreaderDefault);
				useLegacyBlockReaderLocal = conf.GetBoolean(DFSConfigKeys.DfsClientUseLegacyBlockreaderlocal
					, DFSConfigKeys.DfsClientUseLegacyBlockreaderlocalDefault);
				shortCircuitLocalReads = conf.GetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey
					, DFSConfigKeys.DfsClientReadShortcircuitDefault);
				domainSocketDataTraffic = conf.GetBoolean(DFSConfigKeys.DfsClientDomainSocketDataTraffic
					, DFSConfigKeys.DfsClientDomainSocketDataTrafficDefault);
				domainSocketPath = conf.GetTrimmed(DFSConfigKeys.DfsDomainSocketPathKey, DFSConfigKeys
					.DfsDomainSocketPathDefault);
				if (BlockReaderLocal.Log.IsDebugEnabled())
				{
					BlockReaderLocal.Log.Debug(DFSConfigKeys.DfsClientUseLegacyBlockreaderlocal + " = "
						 + useLegacyBlockReaderLocal);
					BlockReaderLocal.Log.Debug(DFSConfigKeys.DfsClientReadShortcircuitKey + " = " + shortCircuitLocalReads
						);
					BlockReaderLocal.Log.Debug(DFSConfigKeys.DfsClientDomainSocketDataTraffic + " = "
						 + domainSocketDataTraffic);
					BlockReaderLocal.Log.Debug(DFSConfigKeys.DfsDomainSocketPathKey + " = " + domainSocketPath
						);
				}
				skipShortCircuitChecksums = conf.GetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey
					, DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumDefault);
				shortCircuitBufferSize = conf.GetInt(DFSConfigKeys.DfsClientReadShortcircuitBufferSizeKey
					, DFSConfigKeys.DfsClientReadShortcircuitBufferSizeDefault);
				shortCircuitStreamsCacheSize = conf.GetInt(DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheSizeKey
					, DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheSizeDefault);
				shortCircuitStreamsCacheExpiryMs = conf.GetLong(DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheExpiryMsKey
					, DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheExpiryMsDefault);
				shortCircuitMmapEnabled = conf.GetBoolean(DFSConfigKeys.DfsClientMmapEnabled, DFSConfigKeys
					.DfsClientMmapEnabledDefault);
				shortCircuitMmapCacheSize = conf.GetInt(DFSConfigKeys.DfsClientMmapCacheSize, DFSConfigKeys
					.DfsClientMmapCacheSizeDefault);
				shortCircuitMmapCacheExpiryMs = conf.GetLong(DFSConfigKeys.DfsClientMmapCacheTimeoutMs
					, DFSConfigKeys.DfsClientMmapCacheTimeoutMsDefault);
				shortCircuitMmapCacheRetryTimeout = conf.GetLong(DFSConfigKeys.DfsClientMmapRetryTimeoutMs
					, DFSConfigKeys.DfsClientMmapRetryTimeoutMsDefault);
				shortCircuitCacheStaleThresholdMs = conf.GetLong(DFSConfigKeys.DfsClientShortCircuitReplicaStaleThresholdMs
					, DFSConfigKeys.DfsClientShortCircuitReplicaStaleThresholdMsDefault);
				shortCircuitSharedMemoryWatcherInterruptCheckMs = conf.GetInt(DFSConfigKeys.DfsShortCircuitSharedMemoryWatcherInterruptCheckMs
					, DFSConfigKeys.DfsShortCircuitSharedMemoryWatcherInterruptCheckMsDefault);
				datanodeRestartTimeout = conf.GetLong(DFSConfigKeys.DfsClientDatanodeRestartTimeoutKey
					, DFSConfigKeys.DfsClientDatanodeRestartTimeoutDefault) * 1000;
				dfsclientSlowIoWarningThresholdMs = conf.GetLong(DFSConfigKeys.DfsClientSlowIoWarningThresholdKey
					, DFSConfigKeys.DfsClientSlowIoWarningThresholdDefault);
				keyProviderCacheExpiryMs = conf.GetLong(DFSConfigKeys.DfsClientKeyProviderCacheExpiryMs
					, DFSConfigKeys.DfsClientKeyProviderCacheExpiryDefault);
			}

			public virtual bool IsUseLegacyBlockReaderLocal()
			{
				return useLegacyBlockReaderLocal;
			}

			public virtual string GetDomainSocketPath()
			{
				return domainSocketPath;
			}

			public virtual bool IsShortCircuitLocalReads()
			{
				return shortCircuitLocalReads;
			}

			public virtual bool IsDomainSocketDataTraffic()
			{
				return domainSocketDataTraffic;
			}

			private DataChecksum.Type GetChecksumType(Configuration conf)
			{
				string checksum = conf.Get(DFSConfigKeys.DfsChecksumTypeKey, DFSConfigKeys.DfsChecksumTypeDefault
					);
				try
				{
					return DataChecksum.Type.ValueOf(checksum);
				}
				catch (ArgumentException)
				{
					Log.Warn("Bad checksum type: " + checksum + ". Using default " + DFSConfigKeys.DfsChecksumTypeDefault
						);
					return DataChecksum.Type.ValueOf(DFSConfigKeys.DfsChecksumTypeDefault);
				}
			}

			// Construct a checksum option from conf
			private Options.ChecksumOpt GetChecksumOptFromConf(Configuration conf)
			{
				DataChecksum.Type type = GetChecksumType(conf);
				int bytesPerChecksum = conf.GetInt(DFSConfigKeys.DfsBytesPerChecksumKey, DFSConfigKeys
					.DfsBytesPerChecksumDefault);
				return new Options.ChecksumOpt(type, bytesPerChecksum);
			}

			// create a DataChecksum with the default option.
			/// <exception cref="System.IO.IOException"/>
			private DataChecksum CreateChecksum()
			{
				return CreateChecksum(null);
			}

			private DataChecksum CreateChecksum(Options.ChecksumOpt userOpt)
			{
				// Fill in any missing field with the default.
				Options.ChecksumOpt myOpt = Options.ChecksumOpt.ProcessChecksumOpt(defaultChecksumOpt
					, userOpt);
				DataChecksum dataChecksum = DataChecksum.NewDataChecksum(myOpt.GetChecksumType(), 
					myOpt.GetBytesPerChecksum());
				if (dataChecksum == null)
				{
					throw new HadoopIllegalArgumentException("Invalid checksum type: userOpt=" + userOpt
						 + ", default=" + defaultChecksumOpt + ", effective=null");
				}
				return dataChecksum;
			}
		}

		public virtual DFSClient.Conf GetConf()
		{
			return dfsClientConf;
		}

		internal virtual Configuration GetConfiguration()
		{
			return conf;
		}

		/// <summary>
		/// A map from file names to
		/// <see cref="DFSOutputStream"/>
		/// objects
		/// that are currently being written by this client.
		/// Note that a file can only be written by a single client.
		/// </summary>
		private readonly IDictionary<long, DFSOutputStream> filesBeingWritten = new Dictionary
			<long, DFSOutputStream>();

		/// <summary>Same as this(NameNode.getAddress(conf), conf);</summary>
		/// <seealso cref="DFSClient(System.Net.IPEndPoint, Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Deprecated at 0.21")]
		public DFSClient(Configuration conf)
			: this(NameNode.GetAddress(conf), conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public DFSClient(IPEndPoint address, Configuration conf)
			: this(NameNode.GetUri(address), conf)
		{
		}

		/// <summary>Same as this(nameNodeUri, conf, null);</summary>
		/// <seealso cref="DFSClient(Sharpen.URI, Org.Apache.Hadoop.Conf.Configuration, Org.Apache.Hadoop.FS.FileSystem.Statistics)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public DFSClient(URI nameNodeUri, Configuration conf)
			: this(nameNodeUri, conf, null)
		{
		}

		/// <summary>Same as this(nameNodeUri, null, conf, stats);</summary>
		/// <seealso cref="DFSClient(Sharpen.URI, Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol, Org.Apache.Hadoop.Conf.Configuration, Org.Apache.Hadoop.FS.FileSystem.Statistics)
		/// 	"></seealso>
		/// <exception cref="System.IO.IOException"/>
		public DFSClient(URI nameNodeUri, Configuration conf, FileSystem.Statistics stats
			)
			: this(nameNodeUri, null, conf, stats)
		{
		}

		/// <summary>Create a new DFSClient connected to the given nameNodeUri or rpcNamenode.
		/// 	</summary>
		/// <remarks>
		/// Create a new DFSClient connected to the given nameNodeUri or rpcNamenode.
		/// If HA is enabled and a positive value is set for
		/// <see cref="DFSConfigKeys.DfsClientTestDropNamenodeResponseNumKey"/>
		/// in the
		/// configuration, the DFSClient will use
		/// <see cref="Org.Apache.Hadoop.IO.Retry.LossyRetryInvocationHandler{T}"/>
		/// as its RetryInvocationHandler. Otherwise one of nameNodeUri or rpcNamenode
		/// must be null.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public DFSClient(URI nameNodeUri, ClientProtocol rpcNamenode, Configuration conf, 
			FileSystem.Statistics stats)
		{
			SpanReceiverHost.Get(conf, DFSConfigKeys.DfsClientHtracePrefix);
			traceSampler = new SamplerBuilder(TraceUtils.WrapHadoopConf(DFSConfigKeys.DfsClientHtracePrefix
				, conf)).Build();
			// Copy only the required DFSClient configuration
			this.dfsClientConf = new DFSClient.Conf(conf);
			if (this.dfsClientConf.useLegacyBlockReaderLocal)
			{
				Log.Debug("Using legacy short-circuit local reads.");
			}
			this.conf = conf;
			this.stats = stats;
			this.socketFactory = NetUtils.GetSocketFactory(conf, typeof(ClientProtocol));
			this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.Get(conf);
			this.ugi = UserGroupInformation.GetCurrentUser();
			this.authority = nameNodeUri == null ? "null" : nameNodeUri.GetAuthority();
			this.clientName = "DFSClient_" + dfsClientConf.taskId + "_" + DFSUtil.GetRandom()
				.Next() + "_" + Sharpen.Thread.CurrentThread().GetId();
			int numResponseToDrop = conf.GetInt(DFSConfigKeys.DfsClientTestDropNamenodeResponseNumKey
				, DFSConfigKeys.DfsClientTestDropNamenodeResponseNumDefault);
			NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo = null;
			AtomicBoolean nnFallbackToSimpleAuth = new AtomicBoolean(false);
			if (numResponseToDrop > 0)
			{
				// This case is used for testing.
				Log.Warn(DFSConfigKeys.DfsClientTestDropNamenodeResponseNumKey + " is set to " + 
					numResponseToDrop + ", this hacked client will proactively drop responses");
				proxyInfo = NameNodeProxies.CreateProxyWithLossyRetryHandler<ClientProtocol>(conf
					, nameNodeUri, numResponseToDrop, nnFallbackToSimpleAuth);
			}
			if (proxyInfo != null)
			{
				this.dtService = proxyInfo.GetDelegationTokenService();
				this.namenode = proxyInfo.GetProxy();
			}
			else
			{
				if (rpcNamenode != null)
				{
					// This case is used for testing.
					Preconditions.CheckArgument(nameNodeUri == null);
					this.namenode = rpcNamenode;
					dtService = null;
				}
				else
				{
					Preconditions.CheckArgument(nameNodeUri != null, "null URI");
					proxyInfo = NameNodeProxies.CreateProxy<ClientProtocol>(conf, nameNodeUri, nnFallbackToSimpleAuth
						);
					this.dtService = proxyInfo.GetDelegationTokenService();
					this.namenode = proxyInfo.GetProxy();
				}
			}
			string[] localInterfaces = conf.GetTrimmedStrings(DFSConfigKeys.DfsClientLocalInterfaces
				);
			localInterfaceAddrs = GetLocalInterfaceAddrs(localInterfaces);
			if (Log.IsDebugEnabled() && 0 != localInterfaces.Length)
			{
				Log.Debug("Using local interfaces [" + Joiner.On(',').Join(localInterfaces) + "] with addresses ["
					 + Joiner.On(',').Join(localInterfaceAddrs) + "]");
			}
			bool readDropBehind = (conf.Get(DFSConfigKeys.DfsClientCacheDropBehindReads) == null
				) ? null : conf.GetBoolean(DFSConfigKeys.DfsClientCacheDropBehindReads, false);
			long readahead = (conf.Get(DFSConfigKeys.DfsClientCacheReadahead) == null) ? null
				 : conf.GetLong(DFSConfigKeys.DfsClientCacheReadahead, 0);
			bool writeDropBehind = (conf.Get(DFSConfigKeys.DfsClientCacheDropBehindWrites) ==
				 null) ? null : conf.GetBoolean(DFSConfigKeys.DfsClientCacheDropBehindWrites, false
				);
			this.defaultReadCachingStrategy = new CachingStrategy(readDropBehind, readahead);
			this.defaultWriteCachingStrategy = new CachingStrategy(writeDropBehind, readahead
				);
			this.clientContext = ClientContext.Get(conf.Get(DFSConfigKeys.DfsClientContext, DFSConfigKeys
				.DfsClientContextDefault), dfsClientConf);
			this.hedgedReadThresholdMillis = conf.GetLong(DFSConfigKeys.DfsDfsclientHedgedReadThresholdMillis
				, DFSConfigKeys.DefaultDfsclientHedgedReadThresholdMillis);
			int numThreads = conf.GetInt(DFSConfigKeys.DfsDfsclientHedgedReadThreadpoolSize, 
				DFSConfigKeys.DefaultDfsclientHedgedReadThreadpoolSize);
			if (numThreads > 0)
			{
				this.InitThreadsNumForHedgedReads(numThreads);
			}
			this.saslClient = new SaslDataTransferClient(conf, DataTransferSaslUtil.GetSaslPropertiesResolver
				(conf), TrustedChannelResolver.GetInstance(conf), nnFallbackToSimpleAuth);
		}

		/// <summary>
		/// Return the socket addresses to use with each configured
		/// local interface.
		/// </summary>
		/// <remarks>
		/// Return the socket addresses to use with each configured
		/// local interface. Local interfaces may be specified by IP
		/// address, IP address range using CIDR notation, interface
		/// name (e.g. eth0) or sub-interface name (e.g. eth0:0).
		/// The socket addresses consist of the IPs for the interfaces
		/// and the ephemeral port (port 0). If an IP, IP range, or
		/// interface name matches an interface with sub-interfaces
		/// only the IP of the interface is used. Sub-interfaces can
		/// be used by specifying them explicitly (by IP or name).
		/// </remarks>
		/// <returns>
		/// SocketAddresses for the configured local interfaces,
		/// or an empty array if none are configured
		/// </returns>
		/// <exception cref="Sharpen.UnknownHostException">if a given interface name is invalid
		/// 	</exception>
		private static EndPoint[] GetLocalInterfaceAddrs(string[] interfaceNames)
		{
			IList<EndPoint> localAddrs = new AList<EndPoint>();
			foreach (string interfaceName in interfaceNames)
			{
				if (InetAddresses.IsInetAddress(interfaceName))
				{
					localAddrs.AddItem(new IPEndPoint(interfaceName, 0));
				}
				else
				{
					if (NetUtils.IsValidSubnet(interfaceName))
					{
						foreach (IPAddress addr in NetUtils.GetIPs(interfaceName, false))
						{
							localAddrs.AddItem(new IPEndPoint(addr, 0));
						}
					}
					else
					{
						foreach (string ip in DNS.GetIPs(interfaceName, false))
						{
							localAddrs.AddItem(new IPEndPoint(ip, 0));
						}
					}
				}
			}
			return Sharpen.Collections.ToArray(localAddrs, new EndPoint[localAddrs.Count]);
		}

		/// <summary>Select one of the configured local interfaces at random.</summary>
		/// <remarks>
		/// Select one of the configured local interfaces at random. We use a random
		/// interface because other policies like round-robin are less effective
		/// given that we cache connections to datanodes.
		/// </remarks>
		/// <returns>
		/// one of the local interface addresses at random, or null if no
		/// local interfaces are configured
		/// </returns>
		internal virtual EndPoint GetRandomLocalInterfaceAddr()
		{
			if (localInterfaceAddrs.Length == 0)
			{
				return null;
			}
			int idx = r.Next(localInterfaceAddrs.Length);
			EndPoint addr = localInterfaceAddrs[idx];
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Using local interface " + addr);
			}
			return addr;
		}

		/// <summary>
		/// Return the number of times the client should go back to the namenode
		/// to retrieve block locations when reading.
		/// </summary>
		internal virtual int GetMaxBlockAcquireFailures()
		{
			return dfsClientConf.maxBlockAcquireFailures;
		}

		/// <summary>Return the timeout that clients should use when writing to datanodes.</summary>
		/// <param name="numNodes">the number of nodes in the pipeline.</param>
		internal virtual int GetDatanodeWriteTimeout(int numNodes)
		{
			return (dfsClientConf.confTime > 0) ? (dfsClientConf.confTime + HdfsServerConstants
				.WriteTimeoutExtension * numNodes) : 0;
		}

		internal virtual int GetDatanodeReadTimeout(int numNodes)
		{
			return dfsClientConf.socketTimeout > 0 ? (HdfsServerConstants.ReadTimeoutExtension
				 * numNodes + dfsClientConf.socketTimeout) : 0;
		}

		internal virtual int GetHdfsTimeout()
		{
			return dfsClientConf.hdfsTimeout;
		}

		[VisibleForTesting]
		public virtual string GetClientName()
		{
			return clientName;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckOpen()
		{
			if (!clientRunning)
			{
				IOException result = new IOException("Filesystem closed");
				throw result;
			}
		}

		/// <summary>Return the lease renewer instance.</summary>
		/// <remarks>
		/// Return the lease renewer instance. The renewer thread won't start
		/// until the first output stream is created. The same instance will
		/// be returned until all output streams are closed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual LeaseRenewer GetLeaseRenewer()
		{
			return LeaseRenewer.GetInstance(authority, ugi, this);
		}

		/// <summary>Get a lease and start automatic renewal</summary>
		/// <exception cref="System.IO.IOException"/>
		private void BeginFileLease(long inodeId, DFSOutputStream @out)
		{
			GetLeaseRenewer().Put(inodeId, @out, this);
		}

		/// <summary>Stop renewal of lease for the file.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void EndFileLease(long inodeId)
		{
			GetLeaseRenewer().CloseFile(inodeId, this);
		}

		/// <summary>Put a file.</summary>
		/// <remarks>
		/// Put a file. Only called from LeaseRenewer, where proper locking is
		/// enforced to consistently update its local dfsclients array and
		/// client's filesBeingWritten map.
		/// </remarks>
		internal virtual void PutFileBeingWritten(long inodeId, DFSOutputStream @out)
		{
			lock (filesBeingWritten)
			{
				filesBeingWritten[inodeId] = @out;
				// update the last lease renewal time only when there was no
				// writes. once there is one write stream open, the lease renewer
				// thread keeps it updated well with in anyone's expiration time.
				if (lastLeaseRenewal == 0)
				{
					UpdateLastLeaseRenewal();
				}
			}
		}

		/// <summary>Remove a file.</summary>
		/// <remarks>Remove a file. Only called from LeaseRenewer.</remarks>
		internal virtual void RemoveFileBeingWritten(long inodeId)
		{
			lock (filesBeingWritten)
			{
				Sharpen.Collections.Remove(filesBeingWritten, inodeId);
				if (filesBeingWritten.IsEmpty())
				{
					lastLeaseRenewal = 0;
				}
			}
		}

		/// <summary>Is file-being-written map empty?</summary>
		internal virtual bool IsFilesBeingWrittenEmpty()
		{
			lock (filesBeingWritten)
			{
				return filesBeingWritten.IsEmpty();
			}
		}

		/// <returns>true if the client is running</returns>
		internal virtual bool IsClientRunning()
		{
			return clientRunning;
		}

		internal virtual long GetLastLeaseRenewal()
		{
			return lastLeaseRenewal;
		}

		internal virtual void UpdateLastLeaseRenewal()
		{
			lock (filesBeingWritten)
			{
				if (filesBeingWritten.IsEmpty())
				{
					return;
				}
				lastLeaseRenewal = Time.MonotonicNow();
			}
		}

		/// <summary>Renew leases.</summary>
		/// <returns>
		/// true if lease was renewed. May return false if this
		/// client has been closed or has no files open.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool RenewLease()
		{
			if (clientRunning && !IsFilesBeingWrittenEmpty())
			{
				try
				{
					namenode.RenewLease(clientName);
					UpdateLastLeaseRenewal();
					return true;
				}
				catch (IOException e)
				{
					// Abort if the lease has already expired. 
					long elapsed = Time.MonotonicNow() - GetLastLeaseRenewal();
					if (elapsed > HdfsConstants.LeaseHardlimitPeriod)
					{
						Log.Warn("Failed to renew lease for " + clientName + " for " + (elapsed / 1000) +
							 " seconds (>= hard-limit =" + (HdfsConstants.LeaseHardlimitPeriod / 1000) + " seconds.) "
							 + "Closing all files being written ...", e);
						CloseAllFilesBeingWritten(true);
					}
					else
					{
						// Let the lease renewer handle it and retry.
						throw;
					}
				}
			}
			return false;
		}

		/// <summary>Close connections the Namenode.</summary>
		internal virtual void CloseConnectionToNamenode()
		{
			RPC.StopProxy(namenode);
		}

		/// <summary>Close/abort all files being written.</summary>
		public virtual void CloseAllFilesBeingWritten(bool abort)
		{
			for (; ; )
			{
				long inodeId;
				DFSOutputStream @out;
				lock (filesBeingWritten)
				{
					if (filesBeingWritten.IsEmpty())
					{
						return;
					}
					inodeId = filesBeingWritten.Keys.GetEnumerator().Next();
					@out = Sharpen.Collections.Remove(filesBeingWritten, inodeId);
				}
				if (@out != null)
				{
					try
					{
						if (abort)
						{
							@out.Abort();
						}
						else
						{
							@out.Close();
						}
					}
					catch (IOException ie)
					{
						Log.Error("Failed to " + (abort ? "abort" : "close") + " inode " + inodeId, ie);
					}
				}
			}
		}

		/// <summary>
		/// Close the file system, abandoning all of the leases and files being
		/// created and close connections to the namenode.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				if (clientRunning)
				{
					CloseAllFilesBeingWritten(false);
					clientRunning = false;
					GetLeaseRenewer().CloseClient(this);
					// close connections to the namenode
					CloseConnectionToNamenode();
				}
			}
		}

		/// <summary>
		/// Close all open streams, abandoning all of the leases and files being
		/// created.
		/// </summary>
		/// <param name="abort">whether streams should be gracefully closed</param>
		public virtual void CloseOutputStreams(bool abort)
		{
			if (clientRunning)
			{
				CloseAllFilesBeingWritten(abort);
			}
		}

		/// <summary>Get the default block size for this cluster</summary>
		/// <returns>the default block size in bytes</returns>
		public virtual long GetDefaultBlockSize()
		{
			return dfsClientConf.defaultBlockSize;
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetPreferredBlockSize(string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetBlockSize(string f)
		{
			TraceScope scope = GetPathTraceScope("getBlockSize", f);
			try
			{
				return namenode.GetPreferredBlockSize(f);
			}
			catch (IOException ie)
			{
				Log.Warn("Problem getting block size", ie);
				throw;
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Get server default values for a number of configuration params.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetServerDefaults()
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual FsServerDefaults GetServerDefaults()
		{
			long now = Time.MonotonicNow();
			if ((serverDefaults == null) || (now - serverDefaultsLastUpdate > ServerDefaultsValidityPeriod
				))
			{
				serverDefaults = namenode.GetServerDefaults();
				serverDefaultsLastUpdate = now;
			}
			System.Diagnostics.Debug.Assert(serverDefaults != null);
			return serverDefaults;
		}

		/// <summary>Get a canonical token service name for this client's tokens.</summary>
		/// <remarks>
		/// Get a canonical token service name for this client's tokens.  Null should
		/// be returned if the client is not using tokens.
		/// </remarks>
		/// <returns>the token service for the client</returns>
		public virtual string GetCanonicalServiceName()
		{
			return (dtService != null) ? dtService.ToString() : null;
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetDelegationToken(Org.Apache.Hadoop.IO.Text)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> 
			GetDelegationToken(Text renewer)
		{
			System.Diagnostics.Debug.Assert(dtService != null);
			TraceScope scope = Trace.StartSpan("getDelegationToken", traceSampler);
			try
			{
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = namenode
					.GetDelegationToken(renewer);
				if (token != null)
				{
					token.SetService(this.dtService);
					Log.Info("Created " + DelegationTokenIdentifier.StringifyToken(token));
				}
				else
				{
					Log.Info("Cannot get delegation token from " + renewer);
				}
				return token;
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Renew a delegation token</summary>
		/// <param name="token">the token to renew</param>
		/// <returns>the new expiration time</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Token.renew instead.")]
		public virtual long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
			> token)
		{
			Log.Info("Renewing " + DelegationTokenIdentifier.StringifyToken(token));
			try
			{
				return token.Renew(conf);
			}
			catch (Exception ie)
			{
				throw new RuntimeException("caught interrupted", ie);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(SecretManager.InvalidToken), typeof(AccessControlException
					));
			}
		}

		private static readonly IDictionary<string, bool> localAddrMap = Sharpen.Collections
			.SynchronizedMap(new Dictionary<string, bool>());

		public static bool IsLocalAddress(IPEndPoint targetAddr)
		{
			IPAddress addr = targetAddr.Address;
			bool cached = localAddrMap[addr.GetHostAddress()];
			if (cached != null)
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace("Address " + targetAddr + (cached ? " is local" : " is not local"));
				}
				return cached;
			}
			bool local = NetUtils.IsLocalAddress(addr);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("Address " + targetAddr + (local ? " is local" : " is not local"));
			}
			localAddrMap[addr.GetHostAddress()] = local;
			return local;
		}

		/// <summary>Cancel a delegation token</summary>
		/// <param name="token">the token to cancel</param>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Token.cancel instead.")]
		public virtual void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token<
			DelegationTokenIdentifier> token)
		{
			Log.Info("Cancelling " + DelegationTokenIdentifier.StringifyToken(token));
			try
			{
				token.Cancel(conf);
			}
			catch (Exception ie)
			{
				throw new RuntimeException("caught interrupted", ie);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(SecretManager.InvalidToken), typeof(AccessControlException
					));
			}
		}

		public class Renewer : TokenRenewer
		{
			static Renewer()
			{
				//Ensure that HDFS Configuration files are loaded before trying to use
				// the renewer.
				HdfsConfiguration.Init();
			}

			public override bool HandleKind(Text kind)
			{
				return DelegationTokenIdentifier.HdfsDelegationKind.Equals(kind);
			}

			/// <exception cref="System.IO.IOException"/>
			public override long Renew<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				, Configuration conf)
			{
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> delToken = (Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)token;
				ClientProtocol nn = GetNNProxy(delToken, conf);
				try
				{
					return nn.RenewDelegationToken(delToken);
				}
				catch (RemoteException re)
				{
					throw re.UnwrapRemoteException(typeof(SecretManager.InvalidToken), typeof(AccessControlException
						));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Cancel<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				, Configuration conf)
			{
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> delToken = (Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)token;
				Log.Info("Cancelling " + DelegationTokenIdentifier.StringifyToken(delToken));
				ClientProtocol nn = GetNNProxy(delToken, conf);
				try
				{
					nn.CancelDelegationToken(delToken);
				}
				catch (RemoteException re)
				{
					throw re.UnwrapRemoteException(typeof(SecretManager.InvalidToken), typeof(AccessControlException
						));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private static ClientProtocol GetNNProxy(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token, Configuration conf)
			{
				URI uri = HAUtil.GetServiceUriFromToken(HdfsConstants.HdfsUriScheme, token);
				if (HAUtil.IsTokenForLogicalUri(token) && !HAUtil.IsLogicalUri(conf, uri))
				{
					// If the token is for a logical nameservice, but the configuration
					// we have disagrees about that, we can't actually renew it.
					// This can be the case in MR, for example, if the RM doesn't
					// have all of the HA clusters configured in its configuration.
					throw new IOException("Unable to map logical nameservice URI '" + uri + "' to a NameNode. Local configuration does not have "
						 + "a failover proxy provider configured.");
				}
				NameNodeProxies.ProxyAndInfo<ClientProtocol> info = NameNodeProxies.CreateProxy<ClientProtocol
					>(conf, uri);
				System.Diagnostics.Debug.Assert(info.GetDelegationTokenService().Equals(token.GetService
					()), "Returned service '" + info.GetDelegationTokenService().ToString() + "' doesn't match expected service '"
					 + token.GetService().ToString() + "'");
				return info.GetProxy();
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool IsManaged<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				)
			{
				return true;
			}
		}

		/// <summary>Report corrupt blocks that were discovered by the client.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.ReportBadBlocks(Org.Apache.Hadoop.Hdfs.Protocol.LocatedBlock[])
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReportBadBlocks(LocatedBlock[] blocks)
		{
			namenode.ReportBadBlocks(blocks);
		}

		public virtual short GetDefaultReplication()
		{
			return dfsClientConf.defaultReplication;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual LocatedBlocks GetLocatedBlocks(string src, long start)
		{
			return GetLocatedBlocks(src, start, dfsClientConf.prefetchSize);
		}

		/*
		* This is just a wrapper around callGetBlockLocations, but non-static so that
		* we can stub it out for tests.
		*/
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual LocatedBlocks GetLocatedBlocks(string src, long start, long length
			)
		{
			TraceScope scope = GetPathTraceScope("getBlockLocations", src);
			try
			{
				return CallGetBlockLocations(namenode, src, start, length);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetBlockLocations(string, long, long)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		internal static LocatedBlocks CallGetBlockLocations(ClientProtocol namenode, string
			 src, long start, long length)
		{
			try
			{
				return namenode.GetBlockLocations(src, start, length);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(UnresolvedPathException));
			}
		}

		/// <summary>Recover a file's lease</summary>
		/// <param name="src">a file's path</param>
		/// <returns>true if the file is already closed</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool RecoverLease(string src)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("recoverLease", src);
			try
			{
				return namenode.RecoverLease(src, clientName);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(FileNotFoundException), typeof(AccessControlException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>
		/// Get block location info about file
		/// getBlockLocations() returns a list of hostnames that store
		/// data for a specific file region.
		/// </summary>
		/// <remarks>
		/// Get block location info about file
		/// getBlockLocations() returns a list of hostnames that store
		/// data for a specific file region.  It returns a set of hostnames
		/// for every block within the indicated region.
		/// This function is very useful when writing code that considers
		/// data-placement when performing operations.  For example, the
		/// MapReduce system tries to schedule tasks on the same machines
		/// as the data-block the task processes.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public virtual BlockLocation[] GetBlockLocations(string src, long start, long length
			)
		{
			TraceScope scope = GetPathTraceScope("getBlockLocations", src);
			try
			{
				LocatedBlocks blocks = GetLocatedBlocks(src, start, length);
				BlockLocation[] locations = DFSUtil.LocatedBlocks2Locations(blocks);
				HdfsBlockLocation[] hdfsLocations = new HdfsBlockLocation[locations.Length];
				for (int i = 0; i < locations.Length; i++)
				{
					hdfsLocations[i] = new HdfsBlockLocation(locations[i], blocks.Get(i));
				}
				return hdfsLocations;
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>
		/// Get block location information about a list of
		/// <see cref="Org.Apache.Hadoop.FS.HdfsBlockLocation"/>
		/// .
		/// Used by
		/// <see cref="DistributedFileSystem.GetFileBlockStorageLocations(System.Collections.Generic.IList{E})
		/// 	"/>
		/// to
		/// get
		/// <see cref="Org.Apache.Hadoop.FS.BlockStorageLocation"/>
		/// s for blocks returned by
		/// <see cref="DistributedFileSystem.GetFileBlockLocations(Org.Apache.Hadoop.FS.FileStatus, long, long)
		/// 	"/>
		/// .
		/// This is done by making a round of RPCs to the associated datanodes, asking
		/// the volume of each block replica. The returned array of
		/// <see cref="Org.Apache.Hadoop.FS.BlockStorageLocation"/>
		/// expose this information as a
		/// <see cref="Org.Apache.Hadoop.FS.VolumeId"/>
		/// .
		/// </summary>
		/// <param name="blockLocations">target blocks on which to query volume location information
		/// 	</param>
		/// <returns>
		/// volumeBlockLocations original block array augmented with additional
		/// volume location information for each replica.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Security.Token.Block.InvalidBlockTokenException
		/// 	"/>
		public virtual BlockStorageLocation[] GetBlockStorageLocations(IList<BlockLocation
			> blockLocations)
		{
			if (!GetConf().getHdfsBlocksMetadataEnabled)
			{
				throw new NotSupportedException("Datanode-side support for " + "getVolumeBlockLocations() must also be enabled in the client "
					 + "configuration.");
			}
			// Downcast blockLocations and fetch out required LocatedBlock(s)
			IList<LocatedBlock> blocks = new AList<LocatedBlock>();
			foreach (BlockLocation loc in blockLocations)
			{
				if (!(loc is HdfsBlockLocation))
				{
					throw new InvalidCastException("DFSClient#getVolumeBlockLocations " + "expected to be passed HdfsBlockLocations"
						);
				}
				HdfsBlockLocation hdfsLoc = (HdfsBlockLocation)loc;
				blocks.AddItem(hdfsLoc.GetLocatedBlock());
			}
			// Re-group the LocatedBlocks to be grouped by datanodes, with the values
			// a list of the LocatedBlocks on the datanode.
			IDictionary<DatanodeInfo, IList<LocatedBlock>> datanodeBlocks = new LinkedHashMap
				<DatanodeInfo, IList<LocatedBlock>>();
			foreach (LocatedBlock b in blocks)
			{
				foreach (DatanodeInfo info in b.GetLocations())
				{
					if (!datanodeBlocks.Contains(info))
					{
						datanodeBlocks[info] = new AList<LocatedBlock>();
					}
					IList<LocatedBlock> l = datanodeBlocks[info];
					l.AddItem(b);
				}
			}
			// Make RPCs to the datanodes to get volume locations for its replicas
			TraceScope scope = Trace.StartSpan("getBlockStorageLocations", traceSampler);
			IDictionary<DatanodeInfo, HdfsBlocksMetadata> metadatas;
			try
			{
				metadatas = BlockStorageLocationUtil.QueryDatanodesForHdfsBlocksMetadata(conf, datanodeBlocks
					, GetConf().getFileBlockStorageLocationsNumThreads, GetConf().getFileBlockStorageLocationsTimeoutMs
					, GetConf().connectToDnViaHostname);
				if (Log.IsTraceEnabled())
				{
					Log.Trace("metadata returned: " + Joiner.On("\n").WithKeyValueSeparator("=").Join
						(metadatas));
				}
			}
			finally
			{
				scope.Close();
			}
			// Regroup the returned VolumeId metadata to again be grouped by
			// LocatedBlock rather than by datanode
			IDictionary<LocatedBlock, IList<VolumeId>> blockVolumeIds = BlockStorageLocationUtil
				.AssociateVolumeIdsWithBlocks(blocks, metadatas);
			// Combine original BlockLocations with new VolumeId information
			BlockStorageLocation[] volumeBlockLocations = BlockStorageLocationUtil.ConvertToVolumeBlockLocations
				(blocks, blockVolumeIds);
			return volumeBlockLocations;
		}

		/// <summary>Decrypts a EDEK by consulting the KeyProvider.</summary>
		/// <exception cref="System.IO.IOException"/>
		private KeyProvider.KeyVersion DecryptEncryptedDataEncryptionKey(FileEncryptionInfo
			 feInfo)
		{
			TraceScope scope = Trace.StartSpan("decryptEDEK", traceSampler);
			try
			{
				KeyProvider provider = GetKeyProvider();
				if (provider == null)
				{
					throw new IOException("No KeyProvider is configured, cannot access" + " an encrypted file"
						);
				}
				KeyProviderCryptoExtension.EncryptedKeyVersion ekv = KeyProviderCryptoExtension.EncryptedKeyVersion
					.CreateForDecryption(feInfo.GetKeyName(), feInfo.GetEzKeyVersionName(), feInfo.GetIV
					(), feInfo.GetEncryptedDataEncryptionKey());
				try
				{
					KeyProviderCryptoExtension cryptoProvider = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
						(provider);
					return cryptoProvider.DecryptEncryptedKey(ekv);
				}
				catch (GeneralSecurityException e)
				{
					throw new IOException(e);
				}
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>
		/// Obtain the crypto protocol version from the provided FileEncryptionInfo,
		/// checking to see if this version is supported by.
		/// </summary>
		/// <param name="feInfo">FileEncryptionInfo</param>
		/// <returns>CryptoProtocolVersion from the feInfo</returns>
		/// <exception cref="System.IO.IOException">if the protocol version is unsupported.</exception>
		private static CryptoProtocolVersion GetCryptoProtocolVersion(FileEncryptionInfo 
			feInfo)
		{
			CryptoProtocolVersion version = feInfo.GetCryptoProtocolVersion();
			if (!CryptoProtocolVersion.Supports(version))
			{
				throw new IOException("Client does not support specified " + "CryptoProtocolVersion "
					 + version.GetDescription() + " version " + "number" + version.GetVersion());
			}
			return version;
		}

		/// <summary>
		/// Obtain a CryptoCodec based on the CipherSuite set in a FileEncryptionInfo
		/// and the available CryptoCodecs configured in the Configuration.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <param name="feInfo">FileEncryptionInfo</param>
		/// <returns>CryptoCodec</returns>
		/// <exception cref="System.IO.IOException">
		/// if no suitable CryptoCodec for the CipherSuite is
		/// available.
		/// </exception>
		private static CryptoCodec GetCryptoCodec(Configuration conf, FileEncryptionInfo 
			feInfo)
		{
			CipherSuite suite = feInfo.GetCipherSuite();
			if (suite.Equals(CipherSuite.Unknown))
			{
				throw new IOException("NameNode specified unknown CipherSuite with ID " + suite.GetUnknownValue
					() + ", cannot instantiate CryptoCodec.");
			}
			CryptoCodec codec = CryptoCodec.GetInstance(conf, suite);
			if (codec == null)
			{
				throw new UnknownCipherSuiteException("No configuration found for the cipher suite "
					 + suite.GetConfigSuffix() + " prefixed with " + CommonConfigurationKeysPublic.HadoopSecurityCryptoCodecClassesKeyPrefix
					 + ". Please see the example configuration " + "hadoop.security.crypto.codec.classes.EXAMPLECIPHERSUITE "
					 + "at core-default.xml for details.");
			}
			return codec;
		}

		/// <summary>
		/// Wraps the stream in a CryptoInputStream if the underlying file is
		/// encrypted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsDataInputStream CreateWrappedInputStream(DFSInputStream dfsis)
		{
			FileEncryptionInfo feInfo = dfsis.GetFileEncryptionInfo();
			if (feInfo != null)
			{
				// File is encrypted, wrap the stream in a crypto stream.
				// Currently only one version, so no special logic based on the version #
				GetCryptoProtocolVersion(feInfo);
				CryptoCodec codec = GetCryptoCodec(conf, feInfo);
				KeyProvider.KeyVersion decrypted = DecryptEncryptedDataEncryptionKey(feInfo);
				CryptoInputStream cryptoIn = new CryptoInputStream(dfsis, codec, decrypted.GetMaterial
					(), feInfo.GetIV());
				return new HdfsDataInputStream(cryptoIn);
			}
			else
			{
				// No FileEncryptionInfo so no encryption.
				return new HdfsDataInputStream(dfsis);
			}
		}

		/// <summary>
		/// Wraps the stream in a CryptoOutputStream if the underlying file is
		/// encrypted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsDataOutputStream CreateWrappedOutputStream(DFSOutputStream dfsos
			, FileSystem.Statistics statistics)
		{
			return CreateWrappedOutputStream(dfsos, statistics, 0);
		}

		/// <summary>
		/// Wraps the stream in a CryptoOutputStream if the underlying file is
		/// encrypted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsDataOutputStream CreateWrappedOutputStream(DFSOutputStream dfsos
			, FileSystem.Statistics statistics, long startPos)
		{
			FileEncryptionInfo feInfo = dfsos.GetFileEncryptionInfo();
			if (feInfo != null)
			{
				// File is encrypted, wrap the stream in a crypto stream.
				// Currently only one version, so no special logic based on the version #
				GetCryptoProtocolVersion(feInfo);
				CryptoCodec codec = GetCryptoCodec(conf, feInfo);
				KeyProvider.KeyVersion decrypted = DecryptEncryptedDataEncryptionKey(feInfo);
				CryptoOutputStream cryptoOut = new CryptoOutputStream(dfsos, codec, decrypted.GetMaterial
					(), feInfo.GetIV(), startPos);
				return new HdfsDataOutputStream(cryptoOut, statistics, startPos);
			}
			else
			{
				// No FileEncryptionInfo present so no encryption.
				return new HdfsDataOutputStream(dfsos, statistics, startPos);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public virtual DFSInputStream Open(string src)
		{
			return Open(src, dfsClientConf.ioBufferSize, true, null);
		}

		/// <summary>
		/// Create an input stream that obtains a nodelist from the
		/// namenode, and then reads from all the right places.
		/// </summary>
		/// <remarks>
		/// Create an input stream that obtains a nodelist from the
		/// namenode, and then reads from all the right places.  Creates
		/// inner subclass of InputStream that does the right out-of-band
		/// work.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		[System.ObsoleteAttribute(@"Use Open(string, int, bool) instead.")]
		public virtual DFSInputStream Open(string src, int buffersize, bool verifyChecksum
			, FileSystem.Statistics stats)
		{
			return Open(src, buffersize, verifyChecksum);
		}

		/// <summary>
		/// Create an input stream that obtains a nodelist from the
		/// namenode, and then reads from all the right places.
		/// </summary>
		/// <remarks>
		/// Create an input stream that obtains a nodelist from the
		/// namenode, and then reads from all the right places.  Creates
		/// inner subclass of InputStream that does the right out-of-band
		/// work.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public virtual DFSInputStream Open(string src, int buffersize, bool verifyChecksum
			)
		{
			CheckOpen();
			//    Get block info from namenode
			TraceScope scope = GetPathTraceScope("newDFSInputStream", src);
			try
			{
				return new DFSInputStream(this, src, verifyChecksum);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Get the namenode associated with this DFSClient object</summary>
		/// <returns>the namenode associated with this DFSClient object</returns>
		public virtual ClientProtocol GetNamenode()
		{
			return namenode;
		}

		/// <summary>
		/// Call
		/// <see cref="Create(string, bool, short, long, Org.Apache.Hadoop.Util.Progressable)
		/// 	"/>
		/// with
		/// default <code>replication</code> and <code>blockSize<code> and null <code>
		/// progress</code>.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual OutputStream Create(string src, bool overwrite)
		{
			return Create(src, overwrite, dfsClientConf.defaultReplication, dfsClientConf.defaultBlockSize
				, null);
		}

		/// <summary>
		/// Call
		/// <see cref="Create(string, bool, short, long, Org.Apache.Hadoop.Util.Progressable)
		/// 	"/>
		/// with
		/// default <code>replication</code> and <code>blockSize<code>.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual OutputStream Create(string src, bool overwrite, Progressable progress
			)
		{
			return Create(src, overwrite, dfsClientConf.defaultReplication, dfsClientConf.defaultBlockSize
				, progress);
		}

		/// <summary>
		/// Call
		/// <see cref="Create(string, bool, short, long, Org.Apache.Hadoop.Util.Progressable)
		/// 	"/>
		/// with
		/// null <code>progress</code>.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual OutputStream Create(string src, bool overwrite, short replication, 
			long blockSize)
		{
			return Create(src, overwrite, replication, blockSize, null);
		}

		/// <summary>
		/// Call
		/// <see cref="Create(string, bool, short, long, Org.Apache.Hadoop.Util.Progressable, int)
		/// 	"/>
		/// with default bufferSize.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual OutputStream Create(string src, bool overwrite, short replication, 
			long blockSize, Progressable progress)
		{
			return Create(src, overwrite, replication, blockSize, progress, dfsClientConf.ioBufferSize
				);
		}

		/// <summary>
		/// Call
		/// <see cref="Create(string, Org.Apache.Hadoop.FS.Permission.FsPermission, Sharpen.EnumSet{E}, short, long, Org.Apache.Hadoop.Util.Progressable, int, Org.Apache.Hadoop.FS.Options.ChecksumOpt)
		/// 	"/>
		/// with default <code>permission</code>
		/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission.GetFileDefault()"/>
		/// .
		/// </summary>
		/// <param name="src">File name</param>
		/// <param name="overwrite">overwrite an existing file if true</param>
		/// <param name="replication">replication factor for the file</param>
		/// <param name="blockSize">maximum block size</param>
		/// <param name="progress">interface for reporting client progress</param>
		/// <param name="buffersize">underlying buffersize</param>
		/// <returns>output stream</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual OutputStream Create(string src, bool overwrite, short replication, 
			long blockSize, Progressable progress, int buffersize)
		{
			return Create(src, FsPermission.GetFileDefault(), overwrite ? EnumSet.Of(CreateFlag
				.Create, CreateFlag.Overwrite) : EnumSet.Of(CreateFlag.Create), replication, blockSize
				, progress, buffersize, null);
		}

		/// <summary>
		/// Call
		/// <see cref="Create(string, Org.Apache.Hadoop.FS.Permission.FsPermission, Sharpen.EnumSet{E}, bool, short, long, Org.Apache.Hadoop.Util.Progressable, int, Org.Apache.Hadoop.FS.Options.ChecksumOpt)
		/// 	"/>
		/// with <code>createParent</code>
		/// set to true.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual DFSOutputStream Create(string src, FsPermission permission, EnumSet
			<CreateFlag> flag, short replication, long blockSize, Progressable progress, int
			 buffersize, Options.ChecksumOpt checksumOpt)
		{
			return Create(src, permission, flag, true, replication, blockSize, progress, buffersize
				, checksumOpt, null);
		}

		/// <summary>
		/// Create a new dfs file with the specified block replication
		/// with write-progress reporting and return an output stream for writing
		/// into the file.
		/// </summary>
		/// <param name="src">File name</param>
		/// <param name="permission">
		/// The permission of the directory being created.
		/// If null, use default permission
		/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission.GetFileDefault()"/>
		/// </param>
		/// <param name="flag">
		/// indicates create a new file or create/overwrite an
		/// existing file or append to an existing file
		/// </param>
		/// <param name="createParent">create missing parent directory if true</param>
		/// <param name="replication">block replication</param>
		/// <param name="blockSize">maximum block size</param>
		/// <param name="progress">interface for reporting client progress</param>
		/// <param name="buffersize">underlying buffer size</param>
		/// <param name="checksumOpt">checksum options</param>
		/// <returns>output stream</returns>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Create(string, Org.Apache.Hadoop.FS.Permission.FsPermission, string, Org.Apache.Hadoop.IO.EnumSetWritable{E}, bool, short, long, Org.Apache.Hadoop.Crypto.CryptoProtocolVersion[])
		/// 	">for detailed description of exceptions thrown</seealso>
		/// <exception cref="System.IO.IOException"/>
		public virtual DFSOutputStream Create(string src, FsPermission permission, EnumSet
			<CreateFlag> flag, bool createParent, short replication, long blockSize, Progressable
			 progress, int buffersize, Options.ChecksumOpt checksumOpt)
		{
			return Create(src, permission, flag, createParent, replication, blockSize, progress
				, buffersize, checksumOpt, null);
		}

		/// <summary>
		/// Same as
		/// <see cref="Create(string, Org.Apache.Hadoop.FS.Permission.FsPermission, Sharpen.EnumSet{E}, bool, short, long, Org.Apache.Hadoop.Util.Progressable, int, Org.Apache.Hadoop.FS.Options.ChecksumOpt)
		/// 	"/>
		/// with the addition of favoredNodes that is
		/// a hint to where the namenode should place the file blocks.
		/// The favored nodes hint is not persisted in HDFS. Hence it may be honored
		/// at the creation time only. HDFS could move the blocks during balancing or
		/// replication, to move the blocks from favored nodes. A value of null means
		/// no favored nodes for this create
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual DFSOutputStream Create(string src, FsPermission permission, EnumSet
			<CreateFlag> flag, bool createParent, short replication, long blockSize, Progressable
			 progress, int buffersize, Options.ChecksumOpt checksumOpt, IPEndPoint[] favoredNodes
			)
		{
			CheckOpen();
			if (permission == null)
			{
				permission = FsPermission.GetFileDefault();
			}
			FsPermission masked = permission.ApplyUMask(dfsClientConf.uMask);
			if (Log.IsDebugEnabled())
			{
				Log.Debug(src + ": masked=" + masked);
			}
			DFSOutputStream result = DFSOutputStream.NewStreamForCreate(this, src, masked, flag
				, createParent, replication, blockSize, progress, buffersize, dfsClientConf.CreateChecksum
				(checksumOpt), GetFavoredNodesStr(favoredNodes));
			BeginFileLease(result.GetFileId(), result);
			return result;
		}

		private string[] GetFavoredNodesStr(IPEndPoint[] favoredNodes)
		{
			string[] favoredNodeStrs = null;
			if (favoredNodes != null)
			{
				favoredNodeStrs = new string[favoredNodes.Length];
				for (int i = 0; i < favoredNodes.Length; i++)
				{
					favoredNodeStrs[i] = favoredNodes[i].GetHostName() + ":" + favoredNodes[i].Port;
				}
			}
			return favoredNodeStrs;
		}

		/// <summary>
		/// Append to an existing file if
		/// <see cref="Org.Apache.Hadoop.FS.CreateFlag.Append"/>
		/// is present
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private DFSOutputStream PrimitiveAppend(string src, EnumSet<CreateFlag> flag, int
			 buffersize, Progressable progress)
		{
			if (flag.Contains(CreateFlag.Append))
			{
				HdfsFileStatus stat = GetFileInfo(src);
				if (stat == null)
				{
					// No file to append to
					// New file needs to be created if create option is present
					if (!flag.Contains(CreateFlag.Create))
					{
						throw new FileNotFoundException("failed to append to non-existent file " + src + 
							" on client " + clientName);
					}
					return null;
				}
				return CallAppend(src, buffersize, flag, progress, null);
			}
			return null;
		}

		/// <summary>
		/// Same as {
		/// <see cref="Create(string, Org.Apache.Hadoop.FS.Permission.FsPermission, Sharpen.EnumSet{E}, short, long, Org.Apache.Hadoop.Util.Progressable, int, Org.Apache.Hadoop.FS.Options.ChecksumOpt)
		/// 	"/>
		/// except that the permission
		/// is absolute (ie has already been masked with umask.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public virtual DFSOutputStream PrimitiveCreate(string src, FsPermission absPermission
			, EnumSet<CreateFlag> flag, bool createParent, short replication, long blockSize
			, Progressable progress, int buffersize, Options.ChecksumOpt checksumOpt)
		{
			CheckOpen();
			CreateFlag.Validate(flag);
			DFSOutputStream result = PrimitiveAppend(src, flag, buffersize, progress);
			if (result == null)
			{
				DataChecksum checksum = dfsClientConf.CreateChecksum(checksumOpt);
				result = DFSOutputStream.NewStreamForCreate(this, src, absPermission, flag, createParent
					, replication, blockSize, progress, buffersize, checksum, null);
			}
			BeginFileLease(result.GetFileId(), result);
			return result;
		}

		/// <summary>Creates a symbolic link.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.CreateSymlink(string, string, Org.Apache.Hadoop.FS.Permission.FsPermission, bool)
		/// 	"></seealso>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateSymlink(string target, string link, bool createParent)
		{
			TraceScope scope = GetPathTraceScope("createSymlink", target);
			try
			{
				FsPermission dirPerm = FsPermission.GetDefault().ApplyUMask(dfsClientConf.uMask);
				namenode.CreateSymlink(target, link, dirPerm, createParent);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileAlreadyExistsException
					), typeof(FileNotFoundException), typeof(ParentNotDirectoryException), typeof(NSQuotaExceededException
					), typeof(DSQuotaExceededException), typeof(UnresolvedPathException), typeof(SnapshotAccessControlException
					));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Resolve the *first* symlink, if any, in the path.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetLinkTarget(string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual string GetLinkTarget(string path)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("getLinkTarget", path);
			try
			{
				return namenode.GetLinkTarget(path);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Method to get stream returned by append call</summary>
		/// <exception cref="System.IO.IOException"/>
		private DFSOutputStream CallAppend(string src, int buffersize, EnumSet<CreateFlag
			> flag, Progressable progress, string[] favoredNodes)
		{
			CreateFlag.ValidateForAppend(flag);
			try
			{
				LastBlockWithStatus blkWithStatus = namenode.Append(src, clientName, new EnumSetWritable
					<CreateFlag>(flag, typeof(CreateFlag)));
				HdfsFileStatus status = blkWithStatus.GetFileStatus();
				if (status == null)
				{
					DFSClient.Log.Debug("NameNode is on an older version, request file " + "info with additional RPC call for file: "
						 + src);
					status = GetFileInfo(src);
				}
				return DFSOutputStream.NewStreamForAppend(this, src, flag, buffersize, progress, 
					blkWithStatus.GetLastBlock(), status, dfsClientConf.CreateChecksum(), favoredNodes
					);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(SafeModeException), typeof(DSQuotaExceededException), typeof(NotSupportedException
					), typeof(UnresolvedPathException), typeof(SnapshotAccessControlException));
			}
		}

		/// <summary>Append to an existing HDFS file.</summary>
		/// <param name="src">file name</param>
		/// <param name="buffersize">buffer size</param>
		/// <param name="flag">
		/// indicates whether to append data to a new block instead of
		/// the last block
		/// </param>
		/// <param name="progress">for reporting write-progress; null is acceptable.</param>
		/// <param name="statistics">file system statistics; null is acceptable.</param>
		/// <returns>an output stream for writing into the file</returns>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Append(string, string, Org.Apache.Hadoop.IO.EnumSetWritable{E})
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsDataOutputStream Append(string src, int buffersize, EnumSet<CreateFlag
			> flag, Progressable progress, FileSystem.Statistics statistics)
		{
			DFSOutputStream @out = Append(src, buffersize, flag, null, progress);
			return CreateWrappedOutputStream(@out, statistics, @out.GetInitialLen());
		}

		/// <summary>Append to an existing HDFS file.</summary>
		/// <param name="src">file name</param>
		/// <param name="buffersize">buffer size</param>
		/// <param name="flag">
		/// indicates whether to append data to a new block instead of the
		/// last block
		/// </param>
		/// <param name="progress">for reporting write-progress; null is acceptable.</param>
		/// <param name="statistics">file system statistics; null is acceptable.</param>
		/// <param name="favoredNodes">FavoredNodes for new blocks</param>
		/// <returns>an output stream for writing into the file</returns>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Append(string, string, Org.Apache.Hadoop.IO.EnumSetWritable{E})
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsDataOutputStream Append(string src, int buffersize, EnumSet<CreateFlag
			> flag, Progressable progress, FileSystem.Statistics statistics, IPEndPoint[] favoredNodes
			)
		{
			DFSOutputStream @out = Append(src, buffersize, flag, GetFavoredNodesStr(favoredNodes
				), progress);
			return CreateWrappedOutputStream(@out, statistics, @out.GetInitialLen());
		}

		/// <exception cref="System.IO.IOException"/>
		private DFSOutputStream Append(string src, int buffersize, EnumSet<CreateFlag> flag
			, string[] favoredNodes, Progressable progress)
		{
			CheckOpen();
			DFSOutputStream result = CallAppend(src, buffersize, flag, progress, favoredNodes
				);
			BeginFileLease(result.GetFileId(), result);
			return result;
		}

		/// <summary>Set replication for an existing file.</summary>
		/// <param name="src">file name</param>
		/// <param name="replication">replication to set the file to</param>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetReplication(string, short)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool SetReplication(string src, short replication)
		{
			TraceScope scope = GetPathTraceScope("setReplication", src);
			try
			{
				return namenode.SetReplication(src, replication);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(SafeModeException), typeof(DSQuotaExceededException), typeof(UnresolvedPathException
					), typeof(SnapshotAccessControlException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Set storage policy for an existing file/directory</summary>
		/// <param name="src">file/directory name</param>
		/// <param name="policyName">name of the storage policy</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetStoragePolicy(string src, string policyName)
		{
			TraceScope scope = GetPathTraceScope("setStoragePolicy", src);
			try
			{
				namenode.SetStoragePolicy(src, policyName);
			}
			catch (RemoteException e)
			{
				throw e.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(SafeModeException), typeof(NSQuotaExceededException), typeof(UnresolvedPathException
					), typeof(SnapshotAccessControlException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <returns>All the existing storage policies</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual BlockStoragePolicy[] GetStoragePolicies()
		{
			TraceScope scope = Trace.StartSpan("getStoragePolicies", traceSampler);
			try
			{
				return namenode.GetStoragePolicies();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Rename file or directory.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Rename(string, string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Rename(string, string, Org.Apache.Hadoop.FS.Options.Rename[]) instead."
			)]
		public virtual bool Rename(string src, string dst)
		{
			CheckOpen();
			TraceScope scope = GetSrcDstTraceScope("rename", src, dst);
			try
			{
				return namenode.Rename(src, dst);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(NSQuotaExceededException
					), typeof(DSQuotaExceededException), typeof(UnresolvedPathException), typeof(SnapshotAccessControlException
					));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>
		/// Move blocks from src to trg and delete src
		/// See
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Concat(string, string[])
		/// 	"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Concat(string trg, string[] srcs)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("concat", traceSampler);
			try
			{
				namenode.Concat(trg, srcs);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(UnresolvedPathException
					), typeof(SnapshotAccessControlException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Rename file or directory.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Rename2(string, string, Org.Apache.Hadoop.FS.Options.Rename[])
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Rename(string src, string dst, params Options.Rename[] options
			)
		{
			CheckOpen();
			TraceScope scope = GetSrcDstTraceScope("rename2", src, dst);
			try
			{
				namenode.Rename2(src, dst, options);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(DSQuotaExceededException
					), typeof(FileAlreadyExistsException), typeof(FileNotFoundException), typeof(ParentNotDirectoryException
					), typeof(SafeModeException), typeof(NSQuotaExceededException), typeof(UnresolvedPathException
					), typeof(SnapshotAccessControlException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>
		/// Truncate a file to an indicated size
		/// See
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Truncate(string, long, string)
		/// 	"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Truncate(string src, long newLength)
		{
			CheckOpen();
			if (newLength < 0)
			{
				throw new HadoopIllegalArgumentException("Cannot truncate to a negative file size: "
					 + newLength + ".");
			}
			try
			{
				return namenode.Truncate(src, newLength, clientName);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(UnresolvedPathException
					));
			}
		}

		/// <summary>Delete file or directory.</summary>
		/// <remarks>
		/// Delete file or directory.
		/// See
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Delete(string, bool)"/>
		/// .
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public virtual bool Delete(string src)
		{
			CheckOpen();
			return Delete(src, true);
		}

		/// <summary>delete file or directory.</summary>
		/// <remarks>
		/// delete file or directory.
		/// delete contents of the directory if non empty and recursive
		/// set to true
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Delete(string, bool)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Delete(string src, bool recursive)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("delete", src);
			try
			{
				return namenode.Delete(src, recursive);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(SafeModeException), typeof(UnresolvedPathException), typeof(SnapshotAccessControlException
					));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Implemented using getFileInfo(src)</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Exists(string src)
		{
			CheckOpen();
			return GetFileInfo(src) != null;
		}

		/// <summary>
		/// Get a partial listing of the indicated directory
		/// No block locations need to be fetched
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual DirectoryListing ListPaths(string src, byte[] startAfter)
		{
			return ListPaths(src, startAfter, false);
		}

		/// <summary>
		/// Get a partial listing of the indicated directory
		/// Recommend to use HdfsFileStatus.EMPTY_NAME as startAfter
		/// if the application wants to fetch a listing starting from
		/// the first entry in the directory
		/// </summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetListing(string, byte[], bool)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual DirectoryListing ListPaths(string src, byte[] startAfter, bool needLocation
			)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("listPaths", src);
			try
			{
				return namenode.GetListing(src, startAfter, needLocation);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Get the file info for a specific file or directory.</summary>
		/// <param name="src">The string representation of the path to the file</param>
		/// <returns>
		/// object containing information regarding the file
		/// or null if file not found
		/// </returns>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetFileInfo(string)
		/// 	">for description of exceptions</seealso>
		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsFileStatus GetFileInfo(string src)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("getFileInfo", src);
			try
			{
				return namenode.GetFileInfo(src);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Close status of a file</summary>
		/// <returns>true if file is already closed</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsFileClosed(string src)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("isFileClosed", src);
			try
			{
				return namenode.IsFileClosed(src);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Get the file info for a specific file or directory.</summary>
		/// <remarks>
		/// Get the file info for a specific file or directory. If src
		/// refers to a symlink then the FileStatus of the link is returned.
		/// </remarks>
		/// <param name="src">
		/// path to a file or directory.
		/// For description of exceptions thrown
		/// </param>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetFileLinkInfo(string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual HdfsFileStatus GetFileLinkInfo(string src)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("getFileLinkInfo", src);
			try
			{
				return namenode.GetFileLinkInfo(src);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(UnresolvedPathException
					));
			}
			finally
			{
				scope.Close();
			}
		}

		[InterfaceAudience.Private]
		public virtual void ClearDataEncryptionKey()
		{
			Log.Debug("Clearing encryption key");
			lock (this)
			{
				encryptionKey = null;
			}
		}

		/// <returns>
		/// true if data sent between this client and DNs should be encrypted,
		/// false otherwise.
		/// </returns>
		/// <exception cref="System.IO.IOException">in the event of error communicating with the NN
		/// 	</exception>
		internal virtual bool ShouldEncryptData()
		{
			FsServerDefaults d = GetServerDefaults();
			return d == null ? false : d.GetEncryptDataTransfer();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DataEncryptionKey NewDataEncryptionKey()
		{
			if (ShouldEncryptData())
			{
				lock (this)
				{
					if (encryptionKey == null || encryptionKey.expiryDate < Time.Now())
					{
						Log.Debug("Getting new encryption token from NN");
						encryptionKey = namenode.GetDataEncryptionKey();
					}
					return encryptionKey;
				}
			}
			else
			{
				return null;
			}
		}

		/// <summary>Get the checksum of the whole file of a range of the file.</summary>
		/// <remarks>
		/// Get the checksum of the whole file of a range of the file. Note that the
		/// range always starts from the beginning of the file.
		/// </remarks>
		/// <param name="src">The file path</param>
		/// <param name="length">the length of the range, i.e., the range is [0, length]</param>
		/// <returns>The checksum</returns>
		/// <seealso cref="DistributedFileSystem.GetFileChecksum(Org.Apache.Hadoop.FS.Path)"/
		/// 	>
		/// <exception cref="System.IO.IOException"/>
		public virtual MD5MD5CRC32FileChecksum GetFileChecksum(string src, long length)
		{
			CheckOpen();
			Preconditions.CheckArgument(length >= 0);
			//get block locations for the file range
			LocatedBlocks blockLocations = CallGetBlockLocations(namenode, src, 0, length);
			if (null == blockLocations)
			{
				throw new FileNotFoundException("File does not exist: " + src);
			}
			IList<LocatedBlock> locatedblocks = blockLocations.GetLocatedBlocks();
			DataOutputBuffer md5out = new DataOutputBuffer();
			int bytesPerCRC = -1;
			DataChecksum.Type crcType = DataChecksum.Type.Default;
			long crcPerBlock = 0;
			bool refetchBlocks = false;
			int lastRetriedIndex = -1;
			// get block checksum for each block
			long remaining = length;
			if (src.Contains(HdfsConstants.SeparatorDotSnapshotDirSeparator))
			{
				remaining = Math.Min(length, blockLocations.GetFileLength());
			}
			for (int i = 0; i < locatedblocks.Count && remaining > 0; i++)
			{
				if (refetchBlocks)
				{
					// refetch to get fresh tokens
					blockLocations = CallGetBlockLocations(namenode, src, 0, length);
					if (null == blockLocations)
					{
						throw new FileNotFoundException("File does not exist: " + src);
					}
					locatedblocks = blockLocations.GetLocatedBlocks();
					refetchBlocks = false;
				}
				LocatedBlock lb = locatedblocks[i];
				ExtendedBlock block = lb.GetBlock();
				if (remaining < block.GetNumBytes())
				{
					block.SetNumBytes(remaining);
				}
				remaining -= block.GetNumBytes();
				DatanodeInfo[] datanodes = lb.GetLocations();
				//try each datanode location of the block
				int timeout = 3000 * datanodes.Length + dfsClientConf.socketTimeout;
				bool done = false;
				for (int j = 0; !done && j < datanodes.Length; j++)
				{
					DataOutputStream @out = null;
					DataInputStream @in = null;
					try
					{
						//connect to a datanode
						IOStreamPair pair = ConnectToDN(datanodes[j], timeout, lb);
						@out = new DataOutputStream(new BufferedOutputStream(pair.@out, HdfsConstants.SmallBufferSize
							));
						@in = new DataInputStream(pair.@in);
						if (Log.IsDebugEnabled())
						{
							Log.Debug("write to " + datanodes[j] + ": " + OP.BlockChecksum + ", block=" + block
								);
						}
						// get block MD5
						new Sender(@out).BlockChecksum(block, lb.GetBlockToken());
						DataTransferProtos.BlockOpResponseProto reply = DataTransferProtos.BlockOpResponseProto
							.ParseFrom(PBHelper.VintPrefixed(@in));
						string logInfo = "for block " + block + " from datanode " + datanodes[j];
						DataTransferProtoUtil.CheckBlockOpStatus(reply, logInfo);
						DataTransferProtos.OpBlockChecksumResponseProto checksumData = reply.GetChecksumResponse
							();
						//read byte-per-checksum
						int bpc = checksumData.GetBytesPerCrc();
						if (i == 0)
						{
							//first block
							bytesPerCRC = bpc;
						}
						else
						{
							if (bpc != bytesPerCRC)
							{
								throw new IOException("Byte-per-checksum not matched: bpc=" + bpc + " but bytesPerCRC="
									 + bytesPerCRC);
							}
						}
						//read crc-per-block
						long cpb = checksumData.GetCrcPerBlock();
						if (locatedblocks.Count > 1 && i == 0)
						{
							crcPerBlock = cpb;
						}
						//read md5
						MD5Hash md5 = new MD5Hash(checksumData.GetMd5().ToByteArray());
						md5.Write(md5out);
						// read crc-type
						DataChecksum.Type ct;
						if (checksumData.HasCrcType())
						{
							ct = PBHelper.Convert(checksumData.GetCrcType());
						}
						else
						{
							Log.Debug("Retrieving checksum from an earlier-version DataNode: " + "inferring checksum by reading first byte"
								);
							ct = InferChecksumTypeByReading(lb, datanodes[j]);
						}
						if (i == 0)
						{
							// first block
							crcType = ct;
						}
						else
						{
							if (crcType != DataChecksum.Type.Mixed && crcType != ct)
							{
								// if crc types are mixed in a file
								crcType = DataChecksum.Type.Mixed;
							}
						}
						done = true;
						if (Log.IsDebugEnabled())
						{
							if (i == 0)
							{
								Log.Debug("set bytesPerCRC=" + bytesPerCRC + ", crcPerBlock=" + crcPerBlock);
							}
							Log.Debug("got reply from " + datanodes[j] + ": md5=" + md5);
						}
					}
					catch (InvalidBlockTokenException)
					{
						if (i > lastRetriedIndex)
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("Got access token error in response to OP_BLOCK_CHECKSUM " + "for file "
									 + src + " for block " + block + " from datanode " + datanodes[j] + ". Will retry the block once."
									);
							}
							lastRetriedIndex = i;
							done = true;
							// actually it's not done; but we'll retry
							i--;
							// repeat at i-th block
							refetchBlocks = true;
							break;
						}
					}
					catch (IOException ie)
					{
						Log.Warn("src=" + src + ", datanodes[" + j + "]=" + datanodes[j], ie);
					}
					finally
					{
						IOUtils.CloseStream(@in);
						IOUtils.CloseStream(@out);
					}
				}
				if (!done)
				{
					throw new IOException("Fail to get block MD5 for " + block);
				}
			}
			//compute file MD5
			MD5Hash fileMD5 = MD5Hash.Digest(md5out.GetData());
			switch (crcType)
			{
				case DataChecksum.Type.Crc32:
				{
					return new MD5MD5CRC32GzipFileChecksum(bytesPerCRC, crcPerBlock, fileMD5);
				}

				case DataChecksum.Type.Crc32c:
				{
					return new MD5MD5CRC32CastagnoliFileChecksum(bytesPerCRC, crcPerBlock, fileMD5);
				}

				default:
				{
					// If there is no block allocated for the file,
					// return one with the magic entry that matches what previous
					// hdfs versions return.
					if (locatedblocks.Count == 0)
					{
						return new MD5MD5CRC32GzipFileChecksum(0, 0, fileMD5);
					}
					// we should never get here since the validity was checked
					// when getCrcType() was called above.
					return null;
				}
			}
		}

		/// <summary>
		/// Connect to the given datanode's datantrasfer port, and return
		/// the resulting IOStreamPair.
		/// </summary>
		/// <remarks>
		/// Connect to the given datanode's datantrasfer port, and return
		/// the resulting IOStreamPair. This includes encryption wrapping, etc.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private IOStreamPair ConnectToDN(DatanodeInfo dn, int timeout, LocatedBlock lb)
		{
			bool success = false;
			Socket sock = null;
			try
			{
				sock = socketFactory.CreateSocket();
				string dnAddr = dn.GetXferAddr(GetConf().connectToDnViaHostname);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Connecting to datanode " + dnAddr);
				}
				NetUtils.Connect(sock, NetUtils.CreateSocketAddr(dnAddr), timeout);
				sock.ReceiveTimeout = timeout;
				OutputStream unbufOut = NetUtils.GetOutputStream(sock);
				InputStream unbufIn = NetUtils.GetInputStream(sock);
				IOStreamPair ret = saslClient.NewSocketSend(sock, unbufOut, unbufIn, this, lb.GetBlockToken
					(), dn);
				success = true;
				return ret;
			}
			finally
			{
				if (!success)
				{
					IOUtils.CloseSocket(sock);
				}
			}
		}

		/// <summary>
		/// Infer the checksum type for a replica by sending an OP_READ_BLOCK
		/// for the first byte of that replica.
		/// </summary>
		/// <remarks>
		/// Infer the checksum type for a replica by sending an OP_READ_BLOCK
		/// for the first byte of that replica. This is used for compatibility
		/// with older HDFS versions which did not include the checksum type in
		/// OpBlockChecksumResponseProto.
		/// </remarks>
		/// <param name="lb">the located block</param>
		/// <param name="dn">the connected datanode</param>
		/// <returns>the inferred checksum type</returns>
		/// <exception cref="System.IO.IOException">if an error occurs</exception>
		private DataChecksum.Type InferChecksumTypeByReading(LocatedBlock lb, DatanodeInfo
			 dn)
		{
			IOStreamPair pair = ConnectToDN(dn, dfsClientConf.socketTimeout, lb);
			try
			{
				DataOutputStream @out = new DataOutputStream(new BufferedOutputStream(pair.@out, 
					HdfsConstants.SmallBufferSize));
				DataInputStream @in = new DataInputStream(pair.@in);
				new Sender(@out).ReadBlock(lb.GetBlock(), lb.GetBlockToken(), clientName, 0, 1, true
					, CachingStrategy.NewDefaultStrategy());
				DataTransferProtos.BlockOpResponseProto reply = DataTransferProtos.BlockOpResponseProto
					.ParseFrom(PBHelper.VintPrefixed(@in));
				string logInfo = "trying to read " + lb.GetBlock() + " from datanode " + dn;
				DataTransferProtoUtil.CheckBlockOpStatus(reply, logInfo);
				return PBHelper.Convert(reply.GetReadOpChecksumInfo().GetChecksum().GetType());
			}
			finally
			{
				IOUtils.Cleanup(null, pair.@in, pair.@out);
			}
		}

		/// <summary>Set permissions to a file or directory.</summary>
		/// <param name="src">path name.</param>
		/// <param name="permission">permission to set to</param>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetPermission(string, Org.Apache.Hadoop.FS.Permission.FsPermission)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetPermission(string src, FsPermission permission)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("setPermission", src);
			try
			{
				namenode.SetPermission(src, permission);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(SafeModeException), typeof(UnresolvedPathException), typeof(SnapshotAccessControlException
					));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Set file or directory owner.</summary>
		/// <param name="src">path name.</param>
		/// <param name="username">user id.</param>
		/// <param name="groupname">user group.</param>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetOwner(string, string, string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetOwner(string src, string username, string groupname)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("setOwner", src);
			try
			{
				namenode.SetOwner(src, username, groupname);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(SafeModeException), typeof(UnresolvedPathException), typeof(SnapshotAccessControlException
					));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private long[] CallGetStats()
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("getStats", traceSampler);
			try
			{
				return namenode.GetStats();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetStats()"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual FsStatus GetDiskStatus()
		{
			long[] rawNums = CallGetStats();
			return new FsStatus(rawNums[0], rawNums[1], rawNums[2]);
		}

		/// <summary>Returns count of blocks with no good replicas left.</summary>
		/// <remarks>
		/// Returns count of blocks with no good replicas left. Normally should be
		/// zero.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetMissingBlocksCount()
		{
			return CallGetStats()[ClientProtocol.GetStatsMissingBlocksIdx];
		}

		/// <summary>
		/// Returns count of blocks with replication factor 1 and have
		/// lost the only replica.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetMissingReplOneBlocksCount()
		{
			return CallGetStats()[ClientProtocol.GetStatsMissingReplOneBlocksIdx];
		}

		/// <summary>Returns count of blocks with one of more replica missing.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetUnderReplicatedBlocksCount()
		{
			return CallGetStats()[ClientProtocol.GetStatsUnderReplicatedIdx];
		}

		/// <summary>Returns count of blocks with at least one replica marked corrupt.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetCorruptBlocksCount()
		{
			return CallGetStats()[ClientProtocol.GetStatsCorruptBlocksIdx];
		}

		/// <returns>a list in which each entry describes a corrupt file/block</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual CorruptFileBlocks ListCorruptFileBlocks(string path, string cookie
			)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("listCorruptFileBlocks", path);
			try
			{
				return namenode.ListCorruptFileBlocks(path, cookie);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeInfo[] DatanodeReport(HdfsConstants.DatanodeReportType type
			)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("datanodeReport", traceSampler);
			try
			{
				return namenode.GetDatanodeReport(type);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeStorageReport[] GetDatanodeStorageReport(HdfsConstants.DatanodeReportType
			 type)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("datanodeStorageReport", traceSampler);
			try
			{
				return namenode.GetDatanodeStorageReport(type);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Enter, leave or get safe mode.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetSafeMode(Org.Apache.Hadoop.Hdfs.Protocol.HdfsConstants.SafeModeAction, bool)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool SetSafeMode(HdfsConstants.SafeModeAction action)
		{
			return SetSafeMode(action, false);
		}

		/// <summary>Enter, leave or get safe mode.</summary>
		/// <param name="action">
		/// One of SafeModeAction.GET, SafeModeAction.ENTER and
		/// SafeModeActiob.LEAVE
		/// </param>
		/// <param name="isChecked">
		/// If true, then check only active namenode's safemode status, else
		/// check first namenode's status.
		/// </param>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetSafeMode(Org.Apache.Hadoop.Hdfs.Protocol.HdfsConstants.SafeModeAction, bool)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool SetSafeMode(HdfsConstants.SafeModeAction action, bool isChecked
			)
		{
			TraceScope scope = Trace.StartSpan("setSafeMode", traceSampler);
			try
			{
				return namenode.SetSafeMode(action, isChecked);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Create one snapshot.</summary>
		/// <param name="snapshotRoot">The directory where the snapshot is to be taken</param>
		/// <param name="snapshotName">Name of the snapshot</param>
		/// <returns>the snapshot path.</returns>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.CreateSnapshot(string, string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual string CreateSnapshot(string snapshotRoot, string snapshotName)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("createSnapshot", traceSampler);
			try
			{
				return namenode.CreateSnapshot(snapshotRoot, snapshotName);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Delete a snapshot of a snapshottable directory.</summary>
		/// <param name="snapshotRoot">
		/// The snapshottable directory that the
		/// to-be-deleted snapshot belongs to
		/// </param>
		/// <param name="snapshotName">The name of the to-be-deleted snapshot</param>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.DeleteSnapshot(string, string)
		/// 	"/>
		public virtual void DeleteSnapshot(string snapshotRoot, string snapshotName)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("deleteSnapshot", traceSampler);
			try
			{
				namenode.DeleteSnapshot(snapshotRoot, snapshotName);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Rename a snapshot.</summary>
		/// <param name="snapshotDir">The directory path where the snapshot was taken</param>
		/// <param name="snapshotOldName">Old name of the snapshot</param>
		/// <param name="snapshotNewName">New name of the snapshot</param>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RenameSnapshot(string, string, string)
		/// 	"/>
		public virtual void RenameSnapshot(string snapshotDir, string snapshotOldName, string
			 snapshotNewName)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("renameSnapshot", traceSampler);
			try
			{
				namenode.RenameSnapshot(snapshotDir, snapshotOldName, snapshotNewName);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Get all the current snapshottable directories.</summary>
		/// <returns>All the current snapshottable directories</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetSnapshottableDirListing()
		/// 	"/>
		public virtual SnapshottableDirectoryStatus[] GetSnapshottableDirListing()
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("getSnapshottableDirListing", traceSampler);
			try
			{
				return namenode.GetSnapshottableDirListing();
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Allow snapshot on a directory.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.AllowSnapshot(string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AllowSnapshot(string snapshotRoot)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("allowSnapshot", traceSampler);
			try
			{
				namenode.AllowSnapshot(snapshotRoot);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Disallow snapshot on a directory.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.DisallowSnapshot(string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void DisallowSnapshot(string snapshotRoot)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("disallowSnapshot", traceSampler);
			try
			{
				namenode.DisallowSnapshot(snapshotRoot);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>
		/// Get the difference between two snapshots, or between a snapshot and the
		/// current tree of a directory.
		/// </summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetSnapshotDiffReport(string, string, string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual SnapshotDiffReport GetSnapshotDiffReport(string snapshotDir, string
			 fromSnapshot, string toSnapshot)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("getSnapshotDiffReport", traceSampler);
			try
			{
				return namenode.GetSnapshotDiffReport(snapshotDir, fromSnapshot, toSnapshot);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long AddCacheDirective(CacheDirectiveInfo info, EnumSet<CacheFlag>
			 flags)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("addCacheDirective", traceSampler);
			try
			{
				return namenode.AddCacheDirective(info, flags);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ModifyCacheDirective(CacheDirectiveInfo info, EnumSet<CacheFlag
			> flags)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("modifyCacheDirective", traceSampler);
			try
			{
				namenode.ModifyCacheDirective(info, flags);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveCacheDirective(long id)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("removeCacheDirective", traceSampler);
			try
			{
				namenode.RemoveCacheDirective(id);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<CacheDirectiveEntry> ListCacheDirectives(CacheDirectiveInfo
			 filter)
		{
			return new CacheDirectiveIterator(namenode, filter, traceSampler);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AddCachePool(CachePoolInfo info)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("addCachePool", traceSampler);
			try
			{
				namenode.AddCachePool(info);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ModifyCachePool(CachePoolInfo info)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("modifyCachePool", traceSampler);
			try
			{
				namenode.ModifyCachePool(info);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveCachePool(string poolName)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("removeCachePool", traceSampler);
			try
			{
				namenode.RemoveCachePool(poolName);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<CachePoolEntry> ListCachePools()
		{
			return new CachePoolIterator(namenode, traceSampler);
		}

		/// <summary>Save namespace image.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SaveNamespace()"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SaveNamespace()
		{
			TraceScope scope = Trace.StartSpan("saveNamespace", traceSampler);
			try
			{
				namenode.SaveNamespace();
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Rolls the edit log on the active NameNode.</summary>
		/// <returns>the txid of the new log segment</returns>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RollEdits()"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual long RollEdits()
		{
			TraceScope scope = Trace.StartSpan("rollEdits", traceSampler);
			try
			{
				return namenode.RollEdits();
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException));
			}
			finally
			{
				scope.Close();
			}
		}

		[VisibleForTesting]
		internal virtual ExtendedBlock GetPreviousBlock(long fileId)
		{
			return filesBeingWritten[fileId].GetBlock();
		}

		/// <summary>enable/disable restore failed storage.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RestoreFailedStorage(string)
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool RestoreFailedStorage(string arg)
		{
			TraceScope scope = Trace.StartSpan("restoreFailedStorage", traceSampler);
			try
			{
				return namenode.RestoreFailedStorage(arg);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Refresh the hosts and exclude files.</summary>
		/// <remarks>
		/// Refresh the hosts and exclude files.  (Rereads them.)
		/// See
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RefreshNodes()"/>
		/// 
		/// for more details.
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RefreshNodes()"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshNodes()
		{
			TraceScope scope = Trace.StartSpan("refreshNodes", traceSampler);
			try
			{
				namenode.RefreshNodes();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Dumps DFS data structures into specified file.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.MetaSave(string)"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void MetaSave(string pathname)
		{
			TraceScope scope = Trace.StartSpan("metaSave", traceSampler);
			try
			{
				namenode.MetaSave(pathname);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>
		/// Requests the namenode to tell all datanodes to use a new, non-persistent
		/// bandwidth value for dfs.balance.bandwidthPerSec.
		/// </summary>
		/// <remarks>
		/// Requests the namenode to tell all datanodes to use a new, non-persistent
		/// bandwidth value for dfs.balance.bandwidthPerSec.
		/// See
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetBalancerBandwidth(long)
		/// 	"/>
		/// 
		/// for more details.
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetBalancerBandwidth(long)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetBalancerBandwidth(long bandwidth)
		{
			TraceScope scope = Trace.StartSpan("setBalancerBandwidth", traceSampler);
			try
			{
				namenode.SetBalancerBandwidth(bandwidth);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.FinalizeUpgrade()"/
		/// 	>
		/// <exception cref="System.IO.IOException"/>
		public virtual void FinalizeUpgrade()
		{
			TraceScope scope = Trace.StartSpan("finalizeUpgrade", traceSampler);
			try
			{
				namenode.FinalizeUpgrade();
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual RollingUpgradeInfo RollingUpgrade(HdfsConstants.RollingUpgradeAction
			 action)
		{
			TraceScope scope = Trace.StartSpan("rollingUpgrade", traceSampler);
			try
			{
				return namenode.RollingUpgrade(action);
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public virtual bool Mkdirs(string src)
		{
			return Mkdirs(src, null, true);
		}

		/// <summary>
		/// Create a directory (or hierarchy of directories) with the given
		/// name and permission.
		/// </summary>
		/// <param name="src">The path of the directory being created</param>
		/// <param name="permission">
		/// The permission of the directory being created.
		/// If permission == null, use
		/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission.GetDefault()"/>
		/// .
		/// </param>
		/// <param name="createParent">create missing parent directory if true</param>
		/// <returns>True if the operation success.</returns>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Mkdirs(string, Org.Apache.Hadoop.FS.Permission.FsPermission, bool)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Mkdirs(string src, FsPermission permission, bool createParent
			)
		{
			if (permission == null)
			{
				permission = FsPermission.GetDefault();
			}
			FsPermission masked = permission.ApplyUMask(dfsClientConf.uMask);
			return PrimitiveMkdir(src, masked, createParent);
		}

		/// <summary>
		/// Same {
		/// <see cref="Mkdirs(string, Org.Apache.Hadoop.FS.Permission.FsPermission, bool)"/>
		/// except
		/// that the permissions has already been masked against umask.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool PrimitiveMkdir(string src, FsPermission absPermission)
		{
			return PrimitiveMkdir(src, absPermission, true);
		}

		/// <summary>
		/// Same {
		/// <see cref="Mkdirs(string, Org.Apache.Hadoop.FS.Permission.FsPermission, bool)"/>
		/// except
		/// that the permissions has already been masked against umask.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool PrimitiveMkdir(string src, FsPermission absPermission, bool createParent
			)
		{
			CheckOpen();
			if (absPermission == null)
			{
				absPermission = FsPermission.GetDefault().ApplyUMask(dfsClientConf.uMask);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug(src + ": masked=" + absPermission);
			}
			TraceScope scope = Trace.StartSpan("mkdir", traceSampler);
			try
			{
				return namenode.Mkdirs(src, absPermission, createParent);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(InvalidPathException
					), typeof(FileAlreadyExistsException), typeof(FileNotFoundException), typeof(ParentNotDirectoryException
					), typeof(SafeModeException), typeof(NSQuotaExceededException), typeof(DSQuotaExceededException
					), typeof(UnresolvedPathException), typeof(SnapshotAccessControlException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>
		/// Get
		/// <see cref="Org.Apache.Hadoop.FS.ContentSummary"/>
		/// rooted at the specified directory.
		/// </summary>
		/// <param name="src">The string representation of the path</param>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetContentSummary(string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual ContentSummary GetContentSummary(string src)
		{
			TraceScope scope = GetPathTraceScope("getContentSummary", src);
			try
			{
				return namenode.GetContentSummary(src);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Sets or resets quotas for a directory.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetQuota(string, long, long, Org.Apache.Hadoop.FS.StorageType)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetQuota(string src, long namespaceQuota, long storagespaceQuota
			)
		{
			// sanity check
			if ((namespaceQuota <= 0 && namespaceQuota != HdfsConstants.QuotaDontSet && namespaceQuota
				 != HdfsConstants.QuotaReset) || (storagespaceQuota <= 0 && storagespaceQuota !=
				 HdfsConstants.QuotaDontSet && storagespaceQuota != HdfsConstants.QuotaReset))
			{
				throw new ArgumentException("Invalid values for quota : " + namespaceQuota + " and "
					 + storagespaceQuota);
			}
			TraceScope scope = GetPathTraceScope("setQuota", src);
			try
			{
				// Pass null as storage type for traditional namespace/storagespace quota.
				namenode.SetQuota(src, namespaceQuota, storagespaceQuota, null);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(NSQuotaExceededException), typeof(DSQuotaExceededException), typeof(UnresolvedPathException
					), typeof(SnapshotAccessControlException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>Sets or resets quotas by storage type for a directory.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetQuota(string, long, long, Org.Apache.Hadoop.FS.StorageType)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetQuotaByStorageType(string src, StorageType type, long quota
			)
		{
			if (quota <= 0 && quota != HdfsConstants.QuotaDontSet && quota != HdfsConstants.QuotaReset)
			{
				throw new ArgumentException("Invalid values for quota :" + quota);
			}
			if (type == null)
			{
				throw new ArgumentException("Invalid storage type(null)");
			}
			if (!type.SupportTypeQuota())
			{
				throw new ArgumentException("Don't support Quota for storage type : " + type.ToString
					());
			}
			TraceScope scope = GetPathTraceScope("setQuotaByStorageType", src);
			try
			{
				namenode.SetQuota(src, HdfsConstants.QuotaDontSet, quota, type);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(QuotaByStorageTypeExceededException), typeof(UnresolvedPathException), 
					typeof(SnapshotAccessControlException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>set the modification and access time of a file</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetTimes(string, long, long)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetTimes(string src, long mtime, long atime)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("setTimes", src);
			try
			{
				namenode.SetTimes(src, mtime, atime);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(UnresolvedPathException), typeof(SnapshotAccessControlException));
			}
			finally
			{
				scope.Close();
			}
		}

		[System.ObsoleteAttribute(@"use Org.Apache.Hadoop.Hdfs.Client.HdfsDataInputStream instead."
			)]
		public class DFSDataInputStream : HdfsDataInputStream
		{
			/// <exception cref="System.IO.IOException"/>
			public DFSDataInputStream(DFSInputStream @in)
				: base(@in)
			{
			}
		}

		internal virtual void ReportChecksumFailure(string file, ExtendedBlock blk, DatanodeInfo
			 dn)
		{
			DatanodeInfo[] dnArr = new DatanodeInfo[] { dn };
			LocatedBlock[] lblocks = new LocatedBlock[] { new LocatedBlock(blk, dnArr) };
			ReportChecksumFailure(file, lblocks);
		}

		// just reports checksum failure and ignores any exception during the report.
		internal virtual void ReportChecksumFailure(string file, LocatedBlock[] lblocks)
		{
			try
			{
				ReportBadBlocks(lblocks);
			}
			catch (IOException ie)
			{
				Log.Info("Found corruption while reading " + file + ". Error repairing corrupt blocks. Bad blocks remain."
					, ie);
			}
		}

		public override string ToString()
		{
			return GetType().Name + "[clientName=" + clientName + ", ugi=" + ugi + "]";
		}

		public virtual CachingStrategy GetDefaultReadCachingStrategy()
		{
			return defaultReadCachingStrategy;
		}

		public virtual CachingStrategy GetDefaultWriteCachingStrategy()
		{
			return defaultWriteCachingStrategy;
		}

		public virtual ClientContext GetClientContext()
		{
			return clientContext;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ModifyAclEntries(string src, IList<AclEntry> aclSpec)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("modifyAclEntries", src);
			try
			{
				namenode.ModifyAclEntries(src, aclSpec);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(AclException
					), typeof(FileNotFoundException), typeof(NSQuotaExceededException), typeof(SafeModeException
					), typeof(SnapshotAccessControlException), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveAclEntries(string src, IList<AclEntry> aclSpec)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("removeAclEntries", traceSampler);
			try
			{
				namenode.RemoveAclEntries(src, aclSpec);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(AclException
					), typeof(FileNotFoundException), typeof(NSQuotaExceededException), typeof(SafeModeException
					), typeof(SnapshotAccessControlException), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveDefaultAcl(string src)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("removeDefaultAcl", traceSampler);
			try
			{
				namenode.RemoveDefaultAcl(src);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(AclException
					), typeof(FileNotFoundException), typeof(NSQuotaExceededException), typeof(SafeModeException
					), typeof(SnapshotAccessControlException), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveAcl(string src)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("removeAcl", traceSampler);
			try
			{
				namenode.RemoveAcl(src);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(AclException
					), typeof(FileNotFoundException), typeof(NSQuotaExceededException), typeof(SafeModeException
					), typeof(SnapshotAccessControlException), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetAcl(string src, IList<AclEntry> aclSpec)
		{
			CheckOpen();
			TraceScope scope = Trace.StartSpan("setAcl", traceSampler);
			try
			{
				namenode.SetAcl(src, aclSpec);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(AclException
					), typeof(FileNotFoundException), typeof(NSQuotaExceededException), typeof(SafeModeException
					), typeof(SnapshotAccessControlException), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual AclStatus GetAclStatus(string src)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("getAclStatus", src);
			try
			{
				return namenode.GetAclStatus(src);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(AclException
					), typeof(FileNotFoundException), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CreateEncryptionZone(string src, string keyName)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("createEncryptionZone", src);
			try
			{
				namenode.CreateEncryptionZone(src, keyName);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(SafeModeException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual EncryptionZone GetEZForPath(string src)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("getEZForPath", src);
			try
			{
				return namenode.GetEZForPath(src);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(UnresolvedPathException
					));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<EncryptionZone> ListEncryptionZones()
		{
			CheckOpen();
			return new EncryptionZoneIterator(namenode, traceSampler);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetXAttr(string src, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("setXAttr", src);
			try
			{
				namenode.SetXAttr(src, XAttrHelper.BuildXAttr(name, value), flag);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(NSQuotaExceededException), typeof(SafeModeException), typeof(SnapshotAccessControlException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] GetXAttr(string src, string name)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("getXAttr", src);
			try
			{
				IList<XAttr> xAttrs = XAttrHelper.BuildXAttrAsList(name);
				IList<XAttr> result = namenode.GetXAttrs(src, xAttrs);
				return XAttrHelper.GetFirstXAttrValue(result);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<string, byte[]> GetXAttrs(string src)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("getXAttrs", src);
			try
			{
				return XAttrHelper.BuildXAttrMap(namenode.GetXAttrs(src, null));
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<string, byte[]> GetXAttrs(string src, IList<string> names
			)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("getXAttrs", src);
			try
			{
				return XAttrHelper.BuildXAttrMap(namenode.GetXAttrs(src, XAttrHelper.BuildXAttrs(
					names)));
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IList<string> ListXAttrs(string src)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("listXAttrs", src);
			try
			{
				IDictionary<string, byte[]> xattrs = XAttrHelper.BuildXAttrMap(namenode.ListXAttrs
					(src));
				return Lists.NewArrayList(xattrs.Keys);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveXAttr(string src, string name)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("removeXAttr", src);
			try
			{
				namenode.RemoveXAttr(src, XAttrHelper.BuildXAttr(name));
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(NSQuotaExceededException), typeof(SafeModeException), typeof(SnapshotAccessControlException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CheckAccess(string src, FsAction mode)
		{
			CheckOpen();
			TraceScope scope = GetPathTraceScope("checkAccess", src);
			try
			{
				namenode.CheckAccess(src, mode);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException(typeof(AccessControlException), typeof(FileNotFoundException
					), typeof(UnresolvedPathException));
			}
			finally
			{
				scope.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DFSInotifyEventInputStream GetInotifyEventStream()
		{
			return new DFSInotifyEventInputStream(traceSampler, namenode);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DFSInotifyEventInputStream GetInotifyEventStream(long lastReadTxid
			)
		{
			return new DFSInotifyEventInputStream(traceSampler, namenode, lastReadTxid);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Peer NewConnectedPeer(IPEndPoint addr, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
		{
			// RemotePeerFactory
			Peer peer = null;
			bool success = false;
			Socket sock = null;
			try
			{
				sock = socketFactory.CreateSocket();
				NetUtils.Connect(sock, addr, GetRandomLocalInterfaceAddr(), dfsClientConf.socketTimeout
					);
				peer = TcpPeerServer.PeerFromSocketAndKey(saslClient, sock, this, blockToken, datanodeId
					);
				peer.SetReadTimeout(dfsClientConf.socketTimeout);
				success = true;
				return peer;
			}
			finally
			{
				if (!success)
				{
					IOUtils.Cleanup(Log, peer);
					IOUtils.CloseSocket(sock);
				}
			}
		}

		/// <summary>
		/// Create hedged reads thread pool, HEDGED_READ_THREAD_POOL, if
		/// it does not already exist.
		/// </summary>
		/// <param name="num">
		/// Number of threads for hedged reads thread pool.
		/// If zero, skip hedged reads thread pool creation.
		/// </param>
		private void InitThreadsNumForHedgedReads(int num)
		{
			lock (this)
			{
				if (num <= 0 || HedgedReadThreadPool != null)
				{
					return;
				}
				HedgedReadThreadPool = new ThreadPoolExecutor(1, num, 60, TimeUnit.Seconds, new SynchronousQueue
					<Runnable>(), new _DaemonFactory_3462(), new _CallerRunsPolicy_3473());
				// will run in the current thread
				HedgedReadThreadPool.AllowCoreThreadTimeOut(true);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Using hedged reads; pool threads=" + num);
				}
			}
		}

		private sealed class _DaemonFactory_3462 : Daemon.DaemonFactory
		{
			public _DaemonFactory_3462()
			{
				this.threadIndex = new AtomicInteger(0);
			}

			private readonly AtomicInteger threadIndex;

			public override Sharpen.Thread NewThread(Runnable r)
			{
				Sharpen.Thread t = base.NewThread(r);
				t.SetName("hedgedRead-" + this.threadIndex.GetAndIncrement());
				return t;
			}
		}

		private sealed class _CallerRunsPolicy_3473 : ThreadPoolExecutor.CallerRunsPolicy
		{
			public _CallerRunsPolicy_3473()
			{
			}

			public override void RejectedExecution(Runnable runnable, ThreadPoolExecutor e)
			{
				DFSClient.Log.Info("Execution rejected, Executing in current thread");
				DFSClient.HedgedReadMetric.IncHedgedReadOpsInCurThread();
				base.RejectedExecution(runnable, e);
			}
		}

		internal virtual long GetHedgedReadTimeout()
		{
			return this.hedgedReadThresholdMillis;
		}

		[VisibleForTesting]
		internal virtual void SetHedgedReadTimeout(long timeoutMillis)
		{
			this.hedgedReadThresholdMillis = timeoutMillis;
		}

		internal virtual ThreadPoolExecutor GetHedgedReadsThreadPool()
		{
			return HedgedReadThreadPool;
		}

		internal virtual bool IsHedgedReadsEnabled()
		{
			return (HedgedReadThreadPool != null) && HedgedReadThreadPool.GetMaximumPoolSize(
				) > 0;
		}

		internal virtual DFSHedgedReadMetrics GetHedgedReadMetrics()
		{
			return HedgedReadMetric;
		}

		public virtual KeyProvider GetKeyProvider()
		{
			return clientContext.GetKeyProviderCache().Get(conf);
		}

		[VisibleForTesting]
		public virtual void SetKeyProvider(KeyProvider provider)
		{
			try
			{
				clientContext.GetKeyProviderCache().SetKeyProvider(conf, provider);
			}
			catch (IOException e)
			{
				Log.Error("Could not set KeyProvider !!", e);
			}
		}

		/// <summary>Probe for encryption enabled on this filesystem.</summary>
		/// <remarks>
		/// Probe for encryption enabled on this filesystem.
		/// See
		/// <see cref="DFSUtil.IsHDFSEncryptionEnabled(Org.Apache.Hadoop.Conf.Configuration)"
		/// 	/>
		/// </remarks>
		/// <returns>true if encryption is enabled</returns>
		public virtual bool IsHDFSEncryptionEnabled()
		{
			return DFSUtil.IsHDFSEncryptionEnabled(this.conf);
		}

		/// <summary>Returns the SaslDataTransferClient configured for this DFSClient.</summary>
		/// <returns>SaslDataTransferClient configured for this DFSClient</returns>
		public virtual SaslDataTransferClient GetSaslDataTransferClient()
		{
			return saslClient;
		}

		private static readonly byte[] Path = Sharpen.Runtime.GetBytesForString("path", Sharpen.Extensions.GetEncoding
			("UTF-8"));

		internal virtual TraceScope GetPathTraceScope(string description, string path)
		{
			TraceScope scope = Trace.StartSpan(description, traceSampler);
			Span span = scope.GetSpan();
			if (span != null)
			{
				if (path != null)
				{
					span.AddKVAnnotation(Path, Sharpen.Runtime.GetBytesForString(path, Sharpen.Extensions.GetEncoding
						("UTF-8")));
				}
			}
			return scope;
		}

		private static readonly byte[] Src = Sharpen.Runtime.GetBytesForString("src", Sharpen.Extensions.GetEncoding
			("UTF-8"));

		private static readonly byte[] Dst = Sharpen.Runtime.GetBytesForString("dst", Sharpen.Extensions.GetEncoding
			("UTF-8"));

		internal virtual TraceScope GetSrcDstTraceScope(string description, string src, string
			 dst)
		{
			TraceScope scope = Trace.StartSpan(description, traceSampler);
			Span span = scope.GetSpan();
			if (span != null)
			{
				if (src != null)
				{
					span.AddKVAnnotation(Src, Sharpen.Runtime.GetBytesForString(src, Sharpen.Extensions.GetEncoding
						("UTF-8")));
				}
				if (dst != null)
				{
					span.AddKVAnnotation(Dst, Sharpen.Runtime.GetBytesForString(dst, Sharpen.Extensions.GetEncoding
						("UTF-8")));
				}
			}
			return scope;
		}
	}
}
