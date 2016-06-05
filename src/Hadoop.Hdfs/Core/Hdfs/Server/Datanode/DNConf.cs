using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Simple class encapsulating all of the configuration that the DataNode
	/// loads at startup time.
	/// </summary>
	public class DNConf
	{
		internal readonly Configuration conf;

		internal readonly int socketTimeout;

		internal readonly int socketWriteTimeout;

		internal readonly int socketKeepaliveTimeout;

		internal readonly bool transferToAllowed;

		internal readonly bool dropCacheBehindWrites;

		internal readonly bool syncBehindWrites;

		internal readonly bool syncBehindWritesInBackground;

		internal readonly bool dropCacheBehindReads;

		internal readonly bool syncOnClose;

		internal readonly bool encryptDataTransfer;

		internal readonly bool connectToDnViaHostname;

		internal readonly long readaheadLength;

		internal readonly long heartBeatInterval;

		internal readonly long blockReportInterval;

		internal readonly long blockReportSplitThreshold;

		internal readonly long deleteReportInterval;

		internal readonly long initialBlockReportDelay;

		internal readonly long cacheReportInterval;

		internal readonly long dfsclientSlowIoWarningThresholdMs;

		internal readonly long datanodeSlowIoWarningThresholdMs;

		internal readonly int writePacketSize;

		internal readonly string minimumNameNodeVersion;

		internal readonly string encryptionAlgorithm;

		internal readonly SaslPropertiesResolver saslPropsResolver;

		internal readonly TrustedChannelResolver trustedChannelResolver;

		private readonly bool ignoreSecurePortsForTesting;

		internal readonly long xceiverStopTimeout;

		internal readonly long restartReplicaExpiry;

		internal readonly long maxLockedMemory;

		private readonly long bpReadyTimeout;

		public DNConf(Configuration conf)
		{
			this.conf = conf;
			socketTimeout = conf.GetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, HdfsServerConstants
				.ReadTimeout);
			socketWriteTimeout = conf.GetInt(DFSConfigKeys.DfsDatanodeSocketWriteTimeoutKey, 
				HdfsServerConstants.WriteTimeout);
			socketKeepaliveTimeout = conf.GetInt(DFSConfigKeys.DfsDatanodeSocketReuseKeepaliveKey
				, DFSConfigKeys.DfsDatanodeSocketReuseKeepaliveDefault);
			/* Based on results on different platforms, we might need set the default
			* to false on some of them. */
			transferToAllowed = conf.GetBoolean(DFSConfigKeys.DfsDatanodeTransfertoAllowedKey
				, DFSConfigKeys.DfsDatanodeTransfertoAllowedDefault);
			writePacketSize = conf.GetInt(DFSConfigKeys.DfsClientWritePacketSizeKey, DFSConfigKeys
				.DfsClientWritePacketSizeDefault);
			readaheadLength = conf.GetLong(DFSConfigKeys.DfsDatanodeReadaheadBytesKey, DFSConfigKeys
				.DfsDatanodeReadaheadBytesDefault);
			dropCacheBehindWrites = conf.GetBoolean(DFSConfigKeys.DfsDatanodeDropCacheBehindWritesKey
				, DFSConfigKeys.DfsDatanodeDropCacheBehindWritesDefault);
			syncBehindWrites = conf.GetBoolean(DFSConfigKeys.DfsDatanodeSyncBehindWritesKey, 
				DFSConfigKeys.DfsDatanodeSyncBehindWritesDefault);
			syncBehindWritesInBackground = conf.GetBoolean(DFSConfigKeys.DfsDatanodeSyncBehindWritesInBackgroundKey
				, DFSConfigKeys.DfsDatanodeSyncBehindWritesInBackgroundDefault);
			dropCacheBehindReads = conf.GetBoolean(DFSConfigKeys.DfsDatanodeDropCacheBehindReadsKey
				, DFSConfigKeys.DfsDatanodeDropCacheBehindReadsDefault);
			connectToDnViaHostname = conf.GetBoolean(DFSConfigKeys.DfsDatanodeUseDnHostname, 
				DFSConfigKeys.DfsDatanodeUseDnHostnameDefault);
			this.blockReportInterval = conf.GetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey
				, DFSConfigKeys.DfsBlockreportIntervalMsecDefault);
			this.blockReportSplitThreshold = conf.GetLong(DFSConfigKeys.DfsBlockreportSplitThresholdKey
				, DFSConfigKeys.DfsBlockreportSplitThresholdDefault);
			this.cacheReportInterval = conf.GetLong(DFSConfigKeys.DfsCachereportIntervalMsecKey
				, DFSConfigKeys.DfsCachereportIntervalMsecDefault);
			this.dfsclientSlowIoWarningThresholdMs = conf.GetLong(DFSConfigKeys.DfsClientSlowIoWarningThresholdKey
				, DFSConfigKeys.DfsClientSlowIoWarningThresholdDefault);
			this.datanodeSlowIoWarningThresholdMs = conf.GetLong(DFSConfigKeys.DfsDatanodeSlowIoWarningThresholdKey
				, DFSConfigKeys.DfsDatanodeSlowIoWarningThresholdDefault);
			long initBRDelay = conf.GetLong(DFSConfigKeys.DfsBlockreportInitialDelayKey, DFSConfigKeys
				.DfsBlockreportInitialDelayDefault) * 1000L;
			if (initBRDelay >= blockReportInterval)
			{
				initBRDelay = 0;
				DataNode.Log.Info("dfs.blockreport.initialDelay is greater than " + "dfs.blockreport.intervalMsec."
					 + " Setting initial delay to 0 msec:");
			}
			initialBlockReportDelay = initBRDelay;
			heartBeatInterval = conf.GetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, DFSConfigKeys
				.DfsHeartbeatIntervalDefault) * 1000L;
			this.deleteReportInterval = 100 * heartBeatInterval;
			// do we need to sync block file contents to disk when blockfile is closed?
			this.syncOnClose = conf.GetBoolean(DFSConfigKeys.DfsDatanodeSynconcloseKey, DFSConfigKeys
				.DfsDatanodeSynconcloseDefault);
			this.minimumNameNodeVersion = conf.Get(DFSConfigKeys.DfsDatanodeMinSupportedNamenodeVersionKey
				, DFSConfigKeys.DfsDatanodeMinSupportedNamenodeVersionDefault);
			this.encryptDataTransfer = conf.GetBoolean(DFSConfigKeys.DfsEncryptDataTransferKey
				, DFSConfigKeys.DfsEncryptDataTransferDefault);
			this.encryptionAlgorithm = conf.Get(DFSConfigKeys.DfsDataEncryptionAlgorithmKey);
			this.trustedChannelResolver = TrustedChannelResolver.GetInstance(conf);
			this.saslPropsResolver = DataTransferSaslUtil.GetSaslPropertiesResolver(conf);
			this.ignoreSecurePortsForTesting = conf.GetBoolean(DFSConfigKeys.IgnoreSecurePortsForTestingKey
				, DFSConfigKeys.IgnoreSecurePortsForTestingDefault);
			this.xceiverStopTimeout = conf.GetLong(DFSConfigKeys.DfsDatanodeXceiverStopTimeoutMillisKey
				, DFSConfigKeys.DfsDatanodeXceiverStopTimeoutMillisDefault);
			this.maxLockedMemory = conf.GetLong(DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey, 
				DFSConfigKeys.DfsDatanodeMaxLockedMemoryDefault);
			this.restartReplicaExpiry = conf.GetLong(DFSConfigKeys.DfsDatanodeRestartReplicaExpiryKey
				, DFSConfigKeys.DfsDatanodeRestartReplicaExpiryDefault) * 1000L;
			this.bpReadyTimeout = conf.GetLong(DFSConfigKeys.DfsDatanodeBpReadyTimeoutKey, DFSConfigKeys
				.DfsDatanodeBpReadyTimeoutDefault);
		}

		// We get minimumNameNodeVersion via a method so it can be mocked out in tests.
		internal virtual string GetMinimumNameNodeVersion()
		{
			return this.minimumNameNodeVersion;
		}

		/// <summary>Returns the configuration.</summary>
		/// <returns>Configuration the configuration</returns>
		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>Returns true if encryption enabled for DataTransferProtocol.</summary>
		/// <returns>boolean true if encryption enabled for DataTransferProtocol</returns>
		public virtual bool GetEncryptDataTransfer()
		{
			return encryptDataTransfer;
		}

		/// <summary>
		/// Returns encryption algorithm configured for DataTransferProtocol, or null
		/// if not configured.
		/// </summary>
		/// <returns>encryption algorithm configured for DataTransferProtocol</returns>
		public virtual string GetEncryptionAlgorithm()
		{
			return encryptionAlgorithm;
		}

		public virtual long GetXceiverStopTimeout()
		{
			return xceiverStopTimeout;
		}

		public virtual long GetMaxLockedMemory()
		{
			return maxLockedMemory;
		}

		/// <summary>
		/// Returns the SaslPropertiesResolver configured for use with
		/// DataTransferProtocol, or null if not configured.
		/// </summary>
		/// <returns>SaslPropertiesResolver configured for use with DataTransferProtocol</returns>
		public virtual SaslPropertiesResolver GetSaslPropsResolver()
		{
			return saslPropsResolver;
		}

		/// <summary>
		/// Returns the TrustedChannelResolver configured for use with
		/// DataTransferProtocol, or null if not configured.
		/// </summary>
		/// <returns>TrustedChannelResolver configured for use with DataTransferProtocol</returns>
		public virtual TrustedChannelResolver GetTrustedChannelResolver()
		{
			return trustedChannelResolver;
		}

		/// <summary>
		/// Returns true if configuration is set to skip checking for proper
		/// port configuration in a secured cluster.
		/// </summary>
		/// <remarks>
		/// Returns true if configuration is set to skip checking for proper
		/// port configuration in a secured cluster.  This is only intended for use in
		/// dev testing.
		/// </remarks>
		/// <returns>true if configured to skip checking secured port configuration</returns>
		public virtual bool GetIgnoreSecurePortsForTesting()
		{
			return ignoreSecurePortsForTesting;
		}

		public virtual long GetBpReadyTimeout()
		{
			return bpReadyTimeout;
		}
	}
}
