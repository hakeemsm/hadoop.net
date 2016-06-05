using System;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class contains constants for configuration keys used
	/// in hdfs.
	/// </summary>
	public class DFSConfigKeys : CommonConfigurationKeys
	{
		public const string DfsBlockSizeKey = "dfs.blocksize";

		public const long DfsBlockSizeDefault = 128 * 1024 * 1024;

		public const string DfsReplicationKey = "dfs.replication";

		public const short DfsReplicationDefault = 3;

		public const string DfsStreamBufferSizeKey = "dfs.stream-buffer-size";

		public const int DfsStreamBufferSizeDefault = 4096;

		public const string DfsBytesPerChecksumKey = "dfs.bytes-per-checksum";

		public const int DfsBytesPerChecksumDefault = 512;

		public const string DfsUserHomeDirPrefixKey = "dfs.user.home.dir.prefix";

		public const string DfsUserHomeDirPrefixDefault = "/user";

		public const string DfsClientRetryPolicyEnabledKey = "dfs.client.retry.policy.enabled";

		public const bool DfsClientRetryPolicyEnabledDefault = false;

		public const string DfsClientRetryPolicySpecKey = "dfs.client.retry.policy.spec";

		public const string DfsClientRetryPolicySpecDefault = "10000,6,60000,10";

		public const string DfsChecksumTypeKey = "dfs.checksum.type";

		public const string DfsChecksumTypeDefault = "CRC32C";

		public const string DfsClientWriteMaxPacketsInFlightKey = "dfs.client.write.max-packets-in-flight";

		public const int DfsClientWriteMaxPacketsInFlightDefault = 80;

		public const string DfsClientWritePacketSizeKey = "dfs.client-write-packet-size";

		public const int DfsClientWritePacketSizeDefault = 64 * 1024;

		public const string DfsClientWriteByteArrayManagerEnabledKey = "dfs.client.write.byte-array-manager.enabled";

		public const bool DfsClientWriteByteArrayManagerEnabledDefault = false;

		public const string DfsClientWriteByteArrayManagerCountThresholdKey = "dfs.client.write.byte-array-manager.count-threshold";

		public const int DfsClientWriteByteArrayManagerCountThresholdDefault = 128;

		public const string DfsClientWriteByteArrayManagerCountLimitKey = "dfs.client.write.byte-array-manager.count-limit";

		public const int DfsClientWriteByteArrayManagerCountLimitDefault = 2048;

		public const string DfsClientWriteByteArrayManagerCountResetTimePeriodMsKey = "dfs.client.write.byte-array-manager.count-reset-time-period-ms";

		public const long DfsClientWriteByteArrayManagerCountResetTimePeriodMsDefault = 10L
			 * 1000;

		public const string DfsClientWriteReplaceDatanodeOnFailureEnableKey = "dfs.client.block.write.replace-datanode-on-failure.enable";

		public const bool DfsClientWriteReplaceDatanodeOnFailureEnableDefault = true;

		public const string DfsClientWriteReplaceDatanodeOnFailurePolicyKey = "dfs.client.block.write.replace-datanode-on-failure.policy";

		public const string DfsClientWriteReplaceDatanodeOnFailurePolicyDefault = "DEFAULT";

		public const string DfsClientWriteReplaceDatanodeOnFailureBestEffortKey = "dfs.client.block.write.replace-datanode-on-failure.best-effort";

		public const bool DfsClientWriteReplaceDatanodeOnFailureBestEffortDefault = false;

		public const string DfsClientSocketCacheCapacityKey = "dfs.client.socketcache.capacity";

		public const int DfsClientSocketCacheCapacityDefault = 16;

		public const string DfsClientUseDnHostname = "dfs.client.use.datanode.hostname";

		public const bool DfsClientUseDnHostnameDefault = false;

		public const string DfsClientCacheDropBehindWrites = "dfs.client.cache.drop.behind.writes";

		public const string DfsClientCacheDropBehindReads = "dfs.client.cache.drop.behind.reads";

		public const string DfsClientCacheReadahead = "dfs.client.cache.readahead";

		public const string DfsClientContext = "dfs.client.context";

		public const string DfsClientContextDefault = "default";

		public const string DfsHdfsBlocksMetadataEnabled = "dfs.datanode.hdfs-blocks-metadata.enabled";

		public const bool DfsHdfsBlocksMetadataEnabledDefault = false;

		public const string DfsClientFileBlockStorageLocationsNumThreads = "dfs.client.file-block-storage-locations.num-threads";

		public const int DfsClientFileBlockStorageLocationsNumThreadsDefault = 10;

		public const string DfsClientFileBlockStorageLocationsTimeoutMs = "dfs.client.file-block-storage-locations.timeout.millis";

		public const int DfsClientFileBlockStorageLocationsTimeoutMsDefault = 1000;

		public const string DfsClientRetryTimesGetLastBlockLength = "dfs.client.retry.times.get-last-block-length";

		public const int DfsClientRetryTimesGetLastBlockLengthDefault = 3;

		public const string DfsClientRetryIntervalGetLastBlockLength = "dfs.client.retry.interval-ms.get-last-block-length";

		public const int DfsClientRetryIntervalGetLastBlockLengthDefault = 4000;

		public const string DfsWebhdfsAclPermissionPatternDefault = "^(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?(,(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?)*$";

		public const string DfsServerHtracePrefix = "dfs.htrace.";

		public const string DfsClientHtracePrefix = "dfs.client.htrace.";

		public const string DfsClientFailoverProxyProviderKeyPrefix = "dfs.client.failover.proxy.provider";

		public const string DfsClientFailoverMaxAttemptsKey = "dfs.client.failover.max.attempts";

		public const int DfsClientFailoverMaxAttemptsDefault = 15;

		public const string DfsClientFailoverSleeptimeBaseKey = "dfs.client.failover.sleep.base.millis";

		public const int DfsClientFailoverSleeptimeBaseDefault = 500;

		public const string DfsClientFailoverSleeptimeMaxKey = "dfs.client.failover.sleep.max.millis";

		public const int DfsClientFailoverSleeptimeMaxDefault = 15000;

		public const string DfsClientFailoverConnectionRetriesKey = "dfs.client.failover.connection.retries";

		public const int DfsClientFailoverConnectionRetriesDefault = 0;

		public const string DfsClientFailoverConnectionRetriesOnSocketTimeoutsKey = "dfs.client.failover.connection.retries.on.timeouts";

		public const int DfsClientFailoverConnectionRetriesOnSocketTimeoutsDefault = 0;

		public const string DfsClientRetryMaxAttemptsKey = "dfs.client.retry.max.attempts";

		public const int DfsClientRetryMaxAttemptsDefault = 10;

		public const string DfsClientSocketCacheExpiryMsecKey = "dfs.client.socketcache.expiryMsec";

		public const long DfsClientSocketCacheExpiryMsecDefault = 3000;

		public const string DfsClientWriteExcludeNodesCacheExpiryInterval = "dfs.client.write.exclude.nodes.cache.expiry.interval.millis";

		public const long DfsClientWriteExcludeNodesCacheExpiryIntervalDefault = 10 * 60 
			* 1000;

		public const string DfsClientDatanodeRestartTimeoutKey = "dfs.client.datanode-restart.timeout";

		public const long DfsClientDatanodeRestartTimeoutDefault = 30;

		public const string DfsDatanodeRestartReplicaExpiryKey = "dfs.datanode.restart.replica.expiration";

		public const long DfsDatanodeRestartReplicaExpiryDefault = 50;

		public const string DfsNamenodeBackupAddressKey = "dfs.namenode.backup.address";

		public const string DfsNamenodeBackupAddressDefault = "localhost:50100";

		public const string DfsNamenodeBackupHttpAddressKey = "dfs.namenode.backup.http-address";

		public const string DfsNamenodeBackupHttpAddressDefault = "0.0.0.0:50105";

		public const string DfsNamenodeBackupServiceRpcAddressKey = "dfs.namenode.backup.dnrpc-address";

		public const string DfsDatanodeBalanceBandwidthpersecKey = "dfs.datanode.balance.bandwidthPerSec";

		public const long DfsDatanodeBalanceBandwidthpersecDefault = 1024 * 1024;

		public const string DfsDatanodeBalanceMaxNumConcurrentMovesKey = "dfs.datanode.balance.max.concurrent.moves";

		public const int DfsDatanodeBalanceMaxNumConcurrentMovesDefault = 5;

		public const string DfsDatanodeReadaheadBytesKey = "dfs.datanode.readahead.bytes";

		public const long DfsDatanodeReadaheadBytesDefault = 4 * 1024 * 1024;

		public const string DfsDatanodeDropCacheBehindWritesKey = "dfs.datanode.drop.cache.behind.writes";

		public const bool DfsDatanodeDropCacheBehindWritesDefault = false;

		public const string DfsDatanodeSyncBehindWritesKey = "dfs.datanode.sync.behind.writes";

		public const bool DfsDatanodeSyncBehindWritesDefault = false;

		public const string DfsDatanodeSyncBehindWritesInBackgroundKey = "dfs.datanode.sync.behind.writes.in.background";

		public const bool DfsDatanodeSyncBehindWritesInBackgroundDefault = false;

		public const string DfsDatanodeDropCacheBehindReadsKey = "dfs.datanode.drop.cache.behind.reads";

		public const bool DfsDatanodeDropCacheBehindReadsDefault = false;

		public const string DfsDatanodeUseDnHostname = "dfs.datanode.use.datanode.hostname";

		public const bool DfsDatanodeUseDnHostnameDefault = false;

		public const string DfsDatanodeMaxLockedMemoryKey = "dfs.datanode.max.locked.memory";

		public const long DfsDatanodeMaxLockedMemoryDefault = 0;

		public const string DfsDatanodeFsdatasetcacheMaxThreadsPerVolumeKey = "dfs.datanode.fsdatasetcache.max.threads.per.volume";

		public const int DfsDatanodeFsdatasetcacheMaxThreadsPerVolumeDefault = 4;

		public const string DfsDatanodeLazyWriterIntervalSec = "dfs.datanode.lazywriter.interval.sec";

		public const int DfsDatanodeLazyWriterIntervalDefaultSec = 60;

		public const string DfsDatanodeRamDiskReplicaTrackerKey = "dfs.datanode.ram.disk.replica.tracker";

		public static readonly Type DfsDatanodeRamDiskReplicaTrackerDefault = typeof(RamDiskReplicaLruTracker
			);

		public const string DfsDatanodeRamDiskLowWatermarkPercent = "dfs.datanode.ram.disk.low.watermark.percent";

		public const float DfsDatanodeRamDiskLowWatermarkPercentDefault = 10.0f;

		public const string DfsDatanodeRamDiskLowWatermarkBytes = "dfs.datanode.ram.disk.low.watermark.bytes";

		public const long DfsDatanodeRamDiskLowWatermarkBytesDefault = DfsBlockSizeDefault;

		public const string DfsDatanodeNetworkCountsCacheMaxSizeKey = "dfs.datanode.network.counts.cache.max.size";

		public const int DfsDatanodeNetworkCountsCacheMaxSizeDefault = int.MaxValue;

		public const string DfsDatanodeDuplicateReplicaDeletion = "dfs.datanode.duplicate.replica.deletion";

		public const bool DfsDatanodeDuplicateReplicaDeletionDefault = true;

		public const string DfsNamenodePathBasedCacheBlockMapAllocationPercent = "dfs.namenode.path.based.cache.block.map.allocation.percent";

		public const float DfsNamenodePathBasedCacheBlockMapAllocationPercentDefault = 0.25f;

		public const string DfsNamenodeHttpPortKey = "dfs.http.port";

		public const int DfsNamenodeHttpPortDefault = 50070;

		public const string DfsNamenodeHttpAddressKey = "dfs.namenode.http-address";

		public const string DfsNamenodeHttpAddressDefault = "0.0.0.0:" + DfsNamenodeHttpPortDefault;

		public const string DfsNamenodeHttpBindHostKey = "dfs.namenode.http-bind-host";

		public const string DfsNamenodeRpcAddressKey = "dfs.namenode.rpc-address";

		public const string DfsNamenodeRpcBindHostKey = "dfs.namenode.rpc-bind-host";

		public const string DfsNamenodeServiceRpcAddressKey = "dfs.namenode.servicerpc-address";

		public const string DfsNamenodeServiceRpcBindHostKey = "dfs.namenode.servicerpc-bind-host";

		public const string DfsNamenodeMaxObjectsKey = "dfs.namenode.max.objects";

		public const long DfsNamenodeMaxObjectsDefault = 0;

		public const string DfsNamenodeSafemodeExtensionKey = "dfs.namenode.safemode.extension";

		public const int DfsNamenodeSafemodeExtensionDefault = 30000;

		public const string DfsNamenodeSafemodeThresholdPctKey = "dfs.namenode.safemode.threshold-pct";

		public const float DfsNamenodeSafemodeThresholdPctDefault = 0.999f;

		public const string DfsNamenodeReplQueueThresholdPctKey = "dfs.namenode.replqueue.threshold-pct";

		public const string DfsNamenodeSafemodeMinDatanodesKey = "dfs.namenode.safemode.min.datanodes";

		public const int DfsNamenodeSafemodeMinDatanodesDefault = 0;

		public const string DfsNamenodeSecondaryHttpAddressKey = "dfs.namenode.secondary.http-address";

		public const string DfsNamenodeSecondaryHttpAddressDefault = "0.0.0.0:50090";

		public const string DfsNamenodeSecondaryHttpsAddressKey = "dfs.namenode.secondary.https-address";

		public const string DfsNamenodeSecondaryHttpsAddressDefault = "0.0.0.0:50091";

		public const string DfsNamenodeCheckpointCheckPeriodKey = "dfs.namenode.checkpoint.check.period";

		public const long DfsNamenodeCheckpointCheckPeriodDefault = 60;

		public const string DfsNamenodeCheckpointPeriodKey = "dfs.namenode.checkpoint.period";

		public const long DfsNamenodeCheckpointPeriodDefault = 3600;

		public const string DfsNamenodeCheckpointTxnsKey = "dfs.namenode.checkpoint.txns";

		public const long DfsNamenodeCheckpointTxnsDefault = 1000000;

		public const string DfsNamenodeCheckpointMaxRetriesKey = "dfs.namenode.checkpoint.max-retries";

		public const int DfsNamenodeCheckpointMaxRetriesDefault = 3;

		public const string DfsNamenodeHeartbeatRecheckIntervalKey = "dfs.namenode.heartbeat.recheck-interval";

		public const int DfsNamenodeHeartbeatRecheckIntervalDefault = 5 * 60 * 1000;

		public const string DfsNamenodeTolerateHeartbeatMultiplierKey = "dfs.namenode.tolerate.heartbeat.multiplier";

		public const int DfsNamenodeTolerateHeartbeatMultiplierDefault = 4;

		public const string DfsClientHttpsKeystoreResourceKey = "dfs.client.https.keystore.resource";

		public const string DfsClientHttpsKeystoreResourceDefault = "ssl-client.xml";

		public const string DfsClientHttpsNeedAuthKey = "dfs.client.https.need-auth";

		public const bool DfsClientHttpsNeedAuthDefault = false;

		public const string DfsClientCachedConnRetryKey = "dfs.client.cached.conn.retry";

		public const int DfsClientCachedConnRetryDefault = 3;

		public const string DfsNamenodeAccesstimePrecisionKey = "dfs.namenode.accesstime.precision";

		public const long DfsNamenodeAccesstimePrecisionDefault = 3600000;

		public const string DfsNamenodeReplicationConsiderloadKey = "dfs.namenode.replication.considerLoad";

		public const bool DfsNamenodeReplicationConsiderloadDefault = true;

		public const string DfsNamenodeReplicationIntervalKey = "dfs.namenode.replication.interval";

		public const int DfsNamenodeReplicationIntervalDefault = 3;

		public const string DfsNamenodeReplicationMinKey = "dfs.namenode.replication.min";

		public const int DfsNamenodeReplicationMinDefault = 1;

		public const string DfsNamenodeReplicationPendingTimeoutSecKey = "dfs.namenode.replication.pending.timeout-sec";

		public const int DfsNamenodeReplicationPendingTimeoutSecDefault = -1;

		public const string DfsNamenodeReplicationMaxStreamsKey = "dfs.namenode.replication.max-streams";

		public const int DfsNamenodeReplicationMaxStreamsDefault = 2;

		public const string DfsNamenodeReplicationStreamsHardLimitKey = "dfs.namenode.replication.max-streams-hard-limit";

		public const int DfsNamenodeReplicationStreamsHardLimitDefault = 4;

		public const string DfsWebhdfsAuthenticationFilterKey = "dfs.web.authentication.filter";

		public static readonly string DfsWebhdfsAuthenticationFilterDefault = typeof(AuthFilter
			).FullName;

		public const string DfsWebhdfsEnabledKey = "dfs.webhdfs.enabled";

		public const bool DfsWebhdfsEnabledDefault = true;

		public const string DfsWebhdfsUserPatternKey = "dfs.webhdfs.user.provider.user.pattern";

		public const string DfsWebhdfsUserPatternDefault = "^[A-Za-z_][A-Za-z0-9._-]*[$]?$";

		public const string DfsPermissionsEnabledKey = "dfs.permissions.enabled";

		public const bool DfsPermissionsEnabledDefault = true;

		public const string DfsPermissionsSuperusergroupKey = "dfs.permissions.superusergroup";

		public const string DfsPermissionsSuperusergroupDefault = "supergroup";

		public const string DfsNamenodeAclsEnabledKey = "dfs.namenode.acls.enabled";

		public const bool DfsNamenodeAclsEnabledDefault = false;

		public const string DfsNamenodeXattrsEnabledKey = "dfs.namenode.xattrs.enabled";

		public const bool DfsNamenodeXattrsEnabledDefault = true;

		public const string DfsAdmin = "dfs.cluster.administrators";

		public const string DfsServerHttpsKeystoreResourceKey = "dfs.https.server.keystore.resource";

		public const string DfsServerHttpsKeystoreResourceDefault = "ssl-server.xml";

		public const string DfsServerHttpsKeypasswordKey = "ssl.server.keystore.keypassword";

		public const string DfsServerHttpsKeystorePasswordKey = "ssl.server.keystore.password";

		public const string DfsServerHttpsTruststorePasswordKey = "ssl.server.truststore.password";

		public const string DfsNamenodeNameDirRestoreKey = "dfs.namenode.name.dir.restore";

		public const bool DfsNamenodeNameDirRestoreDefault = false;

		public const string DfsNamenodeSupportAllowFormatKey = "dfs.namenode.support.allow.format";

		public const bool DfsNamenodeSupportAllowFormatDefault = true;

		public const string DfsNamenodeNumCheckpointsRetainedKey = "dfs.namenode.num.checkpoints.retained";

		public const int DfsNamenodeNumCheckpointsRetainedDefault = 2;

		public const string DfsNamenodeNumExtraEditsRetainedKey = "dfs.namenode.num.extra.edits.retained";

		public const int DfsNamenodeNumExtraEditsRetainedDefault = 1000000;

		public const string DfsNamenodeMaxExtraEditsSegmentsRetainedKey = "dfs.namenode.max.extra.edits.segments.retained";

		public const int DfsNamenodeMaxExtraEditsSegmentsRetainedDefault = 10000;

		public const string DfsNamenodeMinSupportedDatanodeVersionKey = "dfs.namenode.min.supported.datanode.version";

		public const string DfsNamenodeMinSupportedDatanodeVersionDefault = "2.1.0-beta";

		public const string DfsNamenodeEditsDirMinimumKey = "dfs.namenode.edits.dir.minimum";

		public const int DfsNamenodeEditsDirMinimumDefault = 1;

		public const string DfsNamenodeEditLogAutorollMultiplierThreshold = "dfs.namenode.edit.log.autoroll.multiplier.threshold";

		public const float DfsNamenodeEditLogAutorollMultiplierThresholdDefault = 2.0f;

		public const string DfsNamenodeEditLogAutorollCheckIntervalMs = "dfs.namenode.edit.log.autoroll.check.interval.ms";

		public const int DfsNamenodeEditLogAutorollCheckIntervalMsDefault = 5 * 60 * 1000;

		public const string DfsNamenodeLazyPersistFileScrubIntervalSec = "dfs.namenode.lazypersist.file.scrub.interval.sec";

		public const int DfsNamenodeLazyPersistFileScrubIntervalSecDefault = 5 * 60;

		public const string DfsNamenodeEditsNoeditlogchannelflush = "dfs.namenode.edits.noeditlogchannelflush";

		public const bool DfsNamenodeEditsNoeditlogchannelflushDefault = false;

		public const string DfsListLimit = "dfs.ls.limit";

		public const int DfsListLimitDefault = 1000;

		public const string DfsContentSummaryLimitKey = "dfs.content-summary.limit";

		public const int DfsContentSummaryLimitDefault = 5000;

		public const string DfsContentSummarySleepMicrosecKey = "dfs.content-summary.sleep-microsec";

		public const long DfsContentSummarySleepMicrosecDefault = 500;

		public const string DfsDatanodeFailedVolumesToleratedKey = "dfs.datanode.failed.volumes.tolerated";

		public const int DfsDatanodeFailedVolumesToleratedDefault = 0;

		public const string DfsDatanodeSynconcloseKey = "dfs.datanode.synconclose";

		public const bool DfsDatanodeSynconcloseDefault = false;

		public const string DfsDatanodeSocketReuseKeepaliveKey = "dfs.datanode.socket.reuse.keepalive";

		public const int DfsDatanodeSocketReuseKeepaliveDefault = 4000;

		public const string DfsDatanodeOobTimeoutKey = "dfs.datanode.oob.timeout-ms";

		public const string DfsDatanodeOobTimeoutDefault = "1500,0,0,0";

		public const string DfsDatanodeCacheRevocationTimeoutMs = "dfs.datanode.cache.revocation.timeout.ms";

		public const long DfsDatanodeCacheRevocationTimeoutMsDefault = 900000L;

		public const string DfsDatanodeCacheRevocationPollingMs = "dfs.datanode.cache.revocation.polling.ms";

		public const long DfsDatanodeCacheRevocationPollingMsDefault = 500L;

		public const string DfsNamenodeDatanodeRegistrationIpHostnameCheckKey = "dfs.namenode.datanode.registration.ip-hostname-check";

		public const bool DfsNamenodeDatanodeRegistrationIpHostnameCheckDefault = true;

		public const string DfsNamenodeListCachePoolsNumResponses = "dfs.namenode.list.cache.pools.num.responses";

		public const int DfsNamenodeListCachePoolsNumResponsesDefault = 100;

		public const string DfsNamenodeListCacheDirectivesNumResponses = "dfs.namenode.list.cache.directives.num.responses";

		public const int DfsNamenodeListCacheDirectivesNumResponsesDefault = 100;

		public const string DfsNamenodePathBasedCacheRefreshIntervalMs = "dfs.namenode.path.based.cache.refresh.interval.ms";

		public const long DfsNamenodePathBasedCacheRefreshIntervalMsDefault = 30000L;

		/// <summary>Pending period of block deletion since NameNode startup</summary>
		public const string DfsNamenodeStartupDelayBlockDeletionSecKey = "dfs.namenode.startup.delay.block.deletion.sec";

		public const long DfsNamenodeStartupDelayBlockDeletionSecDefault = 0L;

		public const string DfsNamenodeAvoidStaleDatanodeForReadKey = "dfs.namenode.avoid.read.stale.datanode";

		public const bool DfsNamenodeAvoidStaleDatanodeForReadDefault = false;

		public const string DfsNamenodeAvoidStaleDatanodeForWriteKey = "dfs.namenode.avoid.write.stale.datanode";

		public const bool DfsNamenodeAvoidStaleDatanodeForWriteDefault = false;

		public const string DfsNamenodeStaleDatanodeIntervalKey = "dfs.namenode.stale.datanode.interval";

		public const long DfsNamenodeStaleDatanodeIntervalDefault = 30 * 1000;

		public const string DfsNamenodeStaleDatanodeMinimumIntervalKey = "dfs.namenode.stale.datanode.minimum.interval";

		public const int DfsNamenodeStaleDatanodeMinimumIntervalDefault = 3;

		public const string DfsNamenodeUseStaleDatanodeForWriteRatioKey = "dfs.namenode.write.stale.datanode.ratio";

		public const float DfsNamenodeUseStaleDatanodeForWriteRatioDefault = 0.5f;

		public const string DfsNamenodeBlocksPerPostponedblocksRescanKey = "dfs.namenode.blocks.per.postponedblocks.rescan";

		public const long DfsNamenodeBlocksPerPostponedblocksRescanKeyDefault = 10000;

		public const string DfsNamenodeInvalidateWorkPctPerIteration = "dfs.namenode.invalidate.work.pct.per.iteration";

		public const float DfsNamenodeInvalidateWorkPctPerIterationDefault = 0.32f;

		public const string DfsNamenodeReplicationWorkMultiplierPerIteration = "dfs.namenode.replication.work.multiplier.per.iteration";

		public const int DfsNamenodeReplicationWorkMultiplierPerIterationDefault = 2;

		public const string DfsNamenodeDelegationKeyUpdateIntervalKey = "dfs.namenode.delegation.key.update-interval";

		public const long DfsNamenodeDelegationKeyUpdateIntervalDefault = 24 * 60 * 60 * 
			1000;

		public const string DfsNamenodeDelegationTokenRenewIntervalKey = "dfs.namenode.delegation.token.renew-interval";

		public const long DfsNamenodeDelegationTokenRenewIntervalDefault = 24 * 60 * 60 *
			 1000;

		public const string DfsNamenodeDelegationTokenMaxLifetimeKey = "dfs.namenode.delegation.token.max-lifetime";

		public const long DfsNamenodeDelegationTokenMaxLifetimeDefault = 7 * 24 * 60 * 60
			 * 1000;

		public const string DfsNamenodeDelegationTokenAlwaysUseKey = "dfs.namenode.delegation.token.always-use";

		public const bool DfsNamenodeDelegationTokenAlwaysUseDefault = false;

		public const string DfsNamenodeMaxComponentLengthKey = "dfs.namenode.fs-limits.max-component-length";

		public const int DfsNamenodeMaxComponentLengthDefault = 255;

		public const string DfsNamenodeMaxDirectoryItemsKey = "dfs.namenode.fs-limits.max-directory-items";

		public const int DfsNamenodeMaxDirectoryItemsDefault = 1024 * 1024;

		public const string DfsNamenodeMinBlockSizeKey = "dfs.namenode.fs-limits.min-block-size";

		public const long DfsNamenodeMinBlockSizeDefault = 1024 * 1024;

		public const string DfsNamenodeMaxBlocksPerFileKey = "dfs.namenode.fs-limits.max-blocks-per-file";

		public const long DfsNamenodeMaxBlocksPerFileDefault = 1024 * 1024;

		public const string DfsNamenodeMaxXattrsPerInodeKey = "dfs.namenode.fs-limits.max-xattrs-per-inode";

		public const int DfsNamenodeMaxXattrsPerInodeDefault = 32;

		public const string DfsNamenodeMaxXattrSizeKey = "dfs.namenode.fs-limits.max-xattr-size";

		public const int DfsNamenodeMaxXattrSizeDefault = 16384;

		public const string DfsDatanodeDataDirKey = "dfs.datanode.data.dir";

		public const string DfsNamenodeHttpsPortKey = "dfs.https.port";

		public const int DfsNamenodeHttpsPortDefault = 50470;

		public const string DfsNamenodeHttpsAddressKey = "dfs.namenode.https-address";

		public const string DfsNamenodeHttpsBindHostKey = "dfs.namenode.https-bind-host";

		public const string DfsNamenodeHttpsAddressDefault = "0.0.0.0:" + DfsNamenodeHttpsPortDefault;

		public const string DfsNamenodeNameDirKey = "dfs.namenode.name.dir";

		public const string DfsNamenodeEditsDirKey = "dfs.namenode.edits.dir";

		public const string DfsNamenodeSharedEditsDirKey = "dfs.namenode.shared.edits.dir";

		public const string DfsNamenodeEditsPluginPrefix = "dfs.namenode.edits.journal-plugin";

		public const string DfsNamenodeEditsDirRequiredKey = "dfs.namenode.edits.dir.required";

		public const string DfsNamenodeEditsDirDefault = "file:///tmp/hadoop/dfs/name";

		public const string DfsClientReadPrefetchSizeKey = "dfs.client.read.prefetch.size";

		public const string DfsClientRetryWindowBase = "dfs.client.retry.window.base";

		public const string DfsMetricsSessionIdKey = "dfs.metrics.session-id";

		public const string DfsMetricsPercentilesIntervalsKey = "dfs.metrics.percentiles.intervals";

		public const string DfsDatanodeHostNameKey = "dfs.datanode.hostname";

		public const string DfsNamenodeHostsKey = "dfs.namenode.hosts";

		public const string DfsNamenodeHostsExcludeKey = "dfs.namenode.hosts.exclude";

		public const string DfsClientSocketTimeoutKey = "dfs.client.socket-timeout";

		public const string DfsNamenodeCheckpointDirKey = "dfs.namenode.checkpoint.dir";

		public const string DfsNamenodeCheckpointEditsDirKey = "dfs.namenode.checkpoint.edits.dir";

		public const string DfsHosts = "dfs.hosts";

		public const string DfsHostsExclude = "dfs.hosts.exclude";

		public const string DfsClientLocalInterfaces = "dfs.client.local.interfaces";

		public const string DfsNamenodeAuditLoggersKey = "dfs.namenode.audit.loggers";

		public const string DfsNamenodeDefaultAuditLoggerName = "default";

		public const string DfsNamenodeAuditLogTokenTrackingIdKey = "dfs.namenode.audit.log.token.tracking.id";

		public const bool DfsNamenodeAuditLogTokenTrackingIdDefault = false;

		public const string DfsNamenodeAuditLogAsyncKey = "dfs.namenode.audit.log.async";

		public const bool DfsNamenodeAuditLogAsyncDefault = false;

		public const string DfsClientBlockWriteLocatefollowingblockRetriesKey = "dfs.client.block.write.locateFollowingBlock.retries";

		public const int DfsClientBlockWriteLocatefollowingblockRetriesDefault = 5;

		public const string DfsClientBlockWriteRetriesKey = "dfs.client.block.write.retries";

		public const int DfsClientBlockWriteRetriesDefault = 3;

		public const string DfsClientMaxBlockAcquireFailuresKey = "dfs.client.max.block.acquire.failures";

		public const int DfsClientMaxBlockAcquireFailuresDefault = 3;

		public const string DfsClientUseLegacyBlockreader = "dfs.client.use.legacy.blockreader";

		public const bool DfsClientUseLegacyBlockreaderDefault = false;

		public const string DfsClientUseLegacyBlockreaderlocal = "dfs.client.use.legacy.blockreader.local";

		public const bool DfsClientUseLegacyBlockreaderlocalDefault = false;

		public const string DfsBalancerMovedwinwidthKey = "dfs.balancer.movedWinWidth";

		public const long DfsBalancerMovedwinwidthDefault = 5400 * 1000L;

		public const string DfsBalancerMoverthreadsKey = "dfs.balancer.moverThreads";

		public const int DfsBalancerMoverthreadsDefault = 1000;

		public const string DfsBalancerDispatcherthreadsKey = "dfs.balancer.dispatcherThreads";

		public const int DfsBalancerDispatcherthreadsDefault = 200;

		public const string DfsMoverMovedwinwidthKey = "dfs.mover.movedWinWidth";

		public const long DfsMoverMovedwinwidthDefault = 5400 * 1000L;

		public const string DfsMoverMoverthreadsKey = "dfs.mover.moverThreads";

		public const int DfsMoverMoverthreadsDefault = 1000;

		public const string DfsMoverRetryMaxAttemptsKey = "dfs.mover.retry.max.attempts";

		public const int DfsMoverRetryMaxAttemptsDefault = 10;

		public const string DfsDatanodeAddressKey = "dfs.datanode.address";

		public const int DfsDatanodeDefaultPort = 50010;

		public const string DfsDatanodeAddressDefault = "0.0.0.0:" + DfsDatanodeDefaultPort;

		public const string DfsDatanodeDataDirPermissionKey = "dfs.datanode.data.dir.perm";

		public const string DfsDatanodeDataDirPermissionDefault = "700";

		public const string DfsDatanodeDirectoryscanIntervalKey = "dfs.datanode.directoryscan.interval";

		public const int DfsDatanodeDirectoryscanIntervalDefault = 21600;

		public const string DfsDatanodeDirectoryscanThreadsKey = "dfs.datanode.directoryscan.threads";

		public const int DfsDatanodeDirectoryscanThreadsDefault = 1;

		public const string DfsDatanodeDnsInterfaceKey = "dfs.datanode.dns.interface";

		public const string DfsDatanodeDnsInterfaceDefault = "default";

		public const string DfsDatanodeDnsNameserverKey = "dfs.datanode.dns.nameserver";

		public const string DfsDatanodeDnsNameserverDefault = "default";

		public const string DfsDatanodeDuReservedKey = "dfs.datanode.du.reserved";

		public const long DfsDatanodeDuReservedDefault = 0;

		public const string DfsDatanodeHandlerCountKey = "dfs.datanode.handler.count";

		public const int DfsDatanodeHandlerCountDefault = 10;

		public const string DfsDatanodeHttpAddressKey = "dfs.datanode.http.address";

		public const int DfsDatanodeHttpDefaultPort = 50075;

		public const string DfsDatanodeHttpAddressDefault = "0.0.0.0:" + DfsDatanodeHttpDefaultPort;

		public const string DfsDatanodeMaxReceiverThreadsKey = "dfs.datanode.max.transfer.threads";

		public const int DfsDatanodeMaxReceiverThreadsDefault = 4096;

		public const string DfsDatanodeScanPeriodHoursKey = "dfs.datanode.scan.period.hours";

		public const int DfsDatanodeScanPeriodHoursDefault = 21 * 24;

		public const string DfsBlockScannerVolumeBytesPerSecond = "dfs.block.scanner.volume.bytes.per.second";

		public const long DfsBlockScannerVolumeBytesPerSecondDefault = 1048576L;

		public const string DfsDatanodeTransfertoAllowedKey = "dfs.datanode.transferTo.allowed";

		public const bool DfsDatanodeTransfertoAllowedDefault = true;

		public const string DfsHeartbeatIntervalKey = "dfs.heartbeat.interval";

		public const long DfsHeartbeatIntervalDefault = 3;

		public const string DfsNamenodePathBasedCacheRetryIntervalMs = "dfs.namenode.path.based.cache.retry.interval.ms";

		public const long DfsNamenodePathBasedCacheRetryIntervalMsDefault = 30000L;

		public const string DfsNamenodeDecommissionIntervalKey = "dfs.namenode.decommission.interval";

		public const int DfsNamenodeDecommissionIntervalDefault = 30;

		public const string DfsNamenodeDecommissionBlocksPerIntervalKey = "dfs.namenode.decommission.blocks.per.interval";

		public const int DfsNamenodeDecommissionBlocksPerIntervalDefault = 500000;

		public const string DfsNamenodeDecommissionMaxConcurrentTrackedNodes = "dfs.namenode.decommission.max.concurrent.tracked.nodes";

		public const int DfsNamenodeDecommissionMaxConcurrentTrackedNodesDefault = 100;

		public const string DfsNamenodeHandlerCountKey = "dfs.namenode.handler.count";

		public const int DfsNamenodeHandlerCountDefault = 10;

		public const string DfsNamenodeServiceHandlerCountKey = "dfs.namenode.service.handler.count";

		public const int DfsNamenodeServiceHandlerCountDefault = 10;

		public const string DfsSupportAppendKey = "dfs.support.append";

		public const bool DfsSupportAppendDefault = true;

		public const string DfsHttpsEnableKey = "dfs.https.enable";

		public const bool DfsHttpsEnableDefault = false;

		public const string DfsHttpPolicyKey = "dfs.http.policy";

		public static readonly string DfsHttpPolicyDefault = HttpConfig.Policy.HttpOnly.ToString
			();

		public const string DfsDefaultChunkViewSizeKey = "dfs.default.chunk.view.size";

		public const int DfsDefaultChunkViewSizeDefault = 32 * 1024;

		public const string DfsDatanodeHttpsAddressKey = "dfs.datanode.https.address";

		public const string DfsDatanodeHttpsPortKey = "datanode.https.port";

		public const int DfsDatanodeHttpsDefaultPort = 50475;

		public const string DfsDatanodeHttpsAddressDefault = "0.0.0.0:" + DfsDatanodeHttpsDefaultPort;

		public const string DfsDatanodeIpcAddressKey = "dfs.datanode.ipc.address";

		public const int DfsDatanodeIpcDefaultPort = 50020;

		public const string DfsDatanodeIpcAddressDefault = "0.0.0.0:" + DfsDatanodeIpcDefaultPort;

		public const string DfsDatanodeMinSupportedNamenodeVersionKey = "dfs.datanode.min.supported.namenode.version";

		public const string DfsDatanodeMinSupportedNamenodeVersionDefault = "2.1.0-beta";

		public const string DfsNamenodeInodeAttributesProviderKey = "dfs.namenode.inode.attributes.provider.class";

		public const string DfsDatanodeBpReadyTimeoutKey = "dfs.datanode.bp-ready.timeout";

		public const long DfsDatanodeBpReadyTimeoutDefault = 20;

		public const string DfsBlockAccessTokenEnableKey = "dfs.block.access.token.enable";

		public const bool DfsBlockAccessTokenEnableDefault = false;

		public const string DfsBlockAccessKeyUpdateIntervalKey = "dfs.block.access.key.update.interval";

		public const long DfsBlockAccessKeyUpdateIntervalDefault = 600L;

		public const string DfsBlockAccessTokenLifetimeKey = "dfs.block.access.token.lifetime";

		public const long DfsBlockAccessTokenLifetimeDefault = 600L;

		public const string DfsBlockReplicatorClassnameKey = "dfs.block.replicator.classname";

		public static readonly Type DfsBlockReplicatorClassnameDefault = typeof(BlockPlacementPolicyDefault
			);

		public const string DfsReplicationMaxKey = "dfs.replication.max";

		public const int DfsReplicationMaxDefault = 512;

		public const string DfsDfIntervalKey = "dfs.df.interval";

		public const int DfsDfIntervalDefault = 60000;

		public const string DfsBlockreportIntervalMsecKey = "dfs.blockreport.intervalMsec";

		public const long DfsBlockreportIntervalMsecDefault = 6 * 60 * 60 * 1000;

		public const string DfsBlockreportInitialDelayKey = "dfs.blockreport.initialDelay";

		public const int DfsBlockreportInitialDelayDefault = 0;

		public const string DfsBlockreportSplitThresholdKey = "dfs.blockreport.split.threshold";

		public const long DfsBlockreportSplitThresholdDefault = 1000 * 1000;

		public const string DfsCachereportIntervalMsecKey = "dfs.cachereport.intervalMsec";

		public const long DfsCachereportIntervalMsecDefault = 10 * 1000;

		public const string DfsBlockInvalidateLimitKey = "dfs.block.invalidate.limit";

		public const int DfsBlockInvalidateLimitDefault = 1000;

		public const string DfsDefaultMaxCorruptFilesReturnedKey = "dfs.corruptfilesreturned.max";

		public const int DfsDefaultMaxCorruptFilesReturned = 500;

		public const string DfsBlockMisreplicationProcessingLimit = "dfs.block.misreplication.processing.limit";

		public const int DfsBlockMisreplicationProcessingLimitDefault = 10000;

		public const string DfsClientReadShortcircuitKey = "dfs.client.read.shortcircuit";

		public const bool DfsClientReadShortcircuitDefault = false;

		public const string DfsClientReadShortcircuitSkipChecksumKey = "dfs.client.read.shortcircuit.skip.checksum";

		public const bool DfsClientReadShortcircuitSkipChecksumDefault = false;

		public const string DfsClientReadShortcircuitBufferSizeKey = "dfs.client.read.shortcircuit.buffer.size";

		public const int DfsClientReadShortcircuitBufferSizeDefault = 1024 * 1024;

		public const string DfsClientReadShortcircuitStreamsCacheSizeKey = "dfs.client.read.shortcircuit.streams.cache.size";

		public const int DfsClientReadShortcircuitStreamsCacheSizeDefault = 256;

		public const string DfsClientReadShortcircuitStreamsCacheExpiryMsKey = "dfs.client.read.shortcircuit.streams.cache.expiry.ms";

		public const long DfsClientReadShortcircuitStreamsCacheExpiryMsDefault = 5 * 60 *
			 1000;

		public const string DfsClientDomainSocketDataTraffic = "dfs.client.domain.socket.data.traffic";

		public const bool DfsClientDomainSocketDataTrafficDefault = false;

		public const string DfsClientMmapEnabled = "dfs.client.mmap.enabled";

		public const bool DfsClientMmapEnabledDefault = true;

		public const string DfsClientMmapCacheSize = "dfs.client.mmap.cache.size";

		public const int DfsClientMmapCacheSizeDefault = 256;

		public const string DfsClientMmapCacheTimeoutMs = "dfs.client.mmap.cache.timeout.ms";

		public const long DfsClientMmapCacheTimeoutMsDefault = 60 * 60 * 1000;

		public const string DfsClientMmapRetryTimeoutMs = "dfs.client.mmap.retry.timeout.ms";

		public const long DfsClientMmapRetryTimeoutMsDefault = 5 * 60 * 1000;

		public const string DfsClientShortCircuitReplicaStaleThresholdMs = "dfs.client.short.circuit.replica.stale.threshold.ms";

		public const long DfsClientShortCircuitReplicaStaleThresholdMsDefault = 30 * 60 *
			 1000;

		public const string DfsImageCompressKey = "dfs.image.compress";

		public const bool DfsImageCompressDefault = false;

		public const string DfsImageCompressionCodecKey = "dfs.image.compression.codec";

		public const string DfsImageCompressionCodecDefault = "org.apache.hadoop.io.compress.DefaultCodec";

		public const string DfsImageTransferRateKey = "dfs.image.transfer.bandwidthPerSec";

		public const long DfsImageTransferRateDefault = 0;

		public const string DfsImageTransferTimeoutKey = "dfs.image.transfer.timeout";

		public const int DfsImageTransferTimeoutDefault = 60 * 1000;

		public const string DfsImageTransferChunksizeKey = "dfs.image.transfer.chunksize";

		public const int DfsImageTransferChunksizeDefault = 64 * 1024;

		public const string DfsDatanodePluginsKey = "dfs.datanode.plugins";

		public const string DfsDatanodeFsdatasetFactoryKey = "dfs.datanode.fsdataset.factory";

		public const string DfsDatanodeFsdatasetVolumeChoosingPolicyKey = "dfs.datanode.fsdataset.volume.choosing.policy";

		public const string DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpaceThresholdKey
			 = "dfs.datanode.available-space-volume-choosing-policy.balanced-space-threshold";

		public const long DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpaceThresholdDefault
			 = 1024L * 1024L * 1024L * 10L;

		public const string DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpacePreferenceFractionKey
			 = "dfs.datanode.available-space-volume-choosing-policy.balanced-space-preference-fraction";

		public const float DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpacePreferenceFractionDefault
			 = 0.75f;

		public const string DfsDatanodeSocketWriteTimeoutKey = "dfs.datanode.socket.write.timeout";

		public const string DfsDatanodeStartupKey = "dfs.datanode.startup";

		public const string DfsNamenodePluginsKey = "dfs.namenode.plugins";

		public const string DfsWebUgiKey = "dfs.web.ugi";

		public const string DfsNamenodeStartupKey = "dfs.namenode.startup";

		public const string DfsDatanodeKeytabFileKey = "dfs.datanode.keytab.file";

		public const string DfsDatanodeKerberosPrincipalKey = "dfs.datanode.kerberos.principal";

		[Obsolete]
		public const string DfsDatanodeUserNameKey = DfsDatanodeKerberosPrincipalKey;

		public const string DfsDatanodeSharedFileDescriptorPaths = "dfs.datanode.shared.file.descriptor.paths";

		public const string DfsDatanodeSharedFileDescriptorPathsDefault = "/dev/shm,/tmp";

		public const string DfsShortCircuitSharedMemoryWatcherInterruptCheckMs = "dfs.short.circuit.shared.memory.watcher.interrupt.check.ms";

		public const int DfsShortCircuitSharedMemoryWatcherInterruptCheckMsDefault = 60000;

		public const string DfsNamenodeKeytabFileKey = "dfs.namenode.keytab.file";

		public const string DfsNamenodeKerberosPrincipalKey = "dfs.namenode.kerberos.principal";

		[Obsolete]
		public const string DfsNamenodeUserNameKey = DfsNamenodeKerberosPrincipalKey;

		public const string DfsNamenodeKerberosInternalSpnegoPrincipalKey = "dfs.namenode.kerberos.internal.spnego.principal";

		[Obsolete]
		public const string DfsNamenodeInternalSpnegoUserNameKey = DfsNamenodeKerberosInternalSpnegoPrincipalKey;

		public const string DfsSecondaryNamenodeKeytabFileKey = "dfs.secondary.namenode.keytab.file";

		public const string DfsSecondaryNamenodeKerberosPrincipalKey = "dfs.secondary.namenode.kerberos.principal";

		[Obsolete]
		public const string DfsSecondaryNamenodeUserNameKey = DfsSecondaryNamenodeKerberosPrincipalKey;

		public const string DfsSecondaryNamenodeKerberosInternalSpnegoPrincipalKey = "dfs.secondary.namenode.kerberos.internal.spnego.principal";

		[Obsolete]
		public const string DfsSecondaryNamenodeInternalSpnegoUserNameKey = DfsSecondaryNamenodeKerberosInternalSpnegoPrincipalKey;

		public const string DfsNamenodeNameCacheThresholdKey = "dfs.namenode.name.cache.threshold";

		public const int DfsNamenodeNameCacheThresholdDefault = 10;

		public const string DfsNamenodeLegacyOivImageDirKey = "dfs.namenode.legacy-oiv-image.dir";

		public const string DfsNameservices = "dfs.nameservices";

		public const string DfsNameserviceId = "dfs.nameservice.id";

		public const string DfsInternalNameservicesKey = "dfs.internal.nameservices";

		public const string DfsNamenodeResourceCheckIntervalKey = "dfs.namenode.resource.check.interval";

		public const int DfsNamenodeResourceCheckIntervalDefault = 5000;

		public const string DfsNamenodeDuReservedKey = "dfs.namenode.resource.du.reserved";

		public const long DfsNamenodeDuReservedDefault = 1024 * 1024 * 100;

		public const string DfsNamenodeCheckedVolumesKey = "dfs.namenode.resource.checked.volumes";

		public const string DfsNamenodeCheckedVolumesMinimumKey = "dfs.namenode.resource.checked.volumes.minimum";

		public const int DfsNamenodeCheckedVolumesMinimumDefault = 1;

		public const string DfsWebAuthenticationSimpleAnonymousAllowed = "dfs.web.authentication.simple.anonymous.allowed";

		public const string DfsWebAuthenticationKerberosPrincipalKey = "dfs.web.authentication.kerberos.principal";

		public const string DfsWebAuthenticationKerberosKeytabKey = "dfs.web.authentication.kerberos.keytab";

		public const string DfsNamenodeMaxOpSizeKey = "dfs.namenode.max.op.size";

		public const int DfsNamenodeMaxOpSizeDefault = 50 * 1024 * 1024;

		public const string DfsBlockLocalPathAccessUserKey = "dfs.block.local-path-access.user";

		public const string DfsDomainSocketPathKey = "dfs.domain.socket.path";

		public const string DfsDomainSocketPathDefault = string.Empty;

		public const string DfsStoragePolicyEnabledKey = "dfs.storage.policy.enabled";

		public const bool DfsStoragePolicyEnabledDefault = true;

		public const string DfsQuotaByStoragetypeEnabledKey = "dfs.quota.by.storage.type.enabled";

		public const bool DfsQuotaByStoragetypeEnabledDefault = true;

		public const string DfsHaNamenodesKeyPrefix = "dfs.ha.namenodes";

		public const string DfsHaNamenodeIdKey = "dfs.ha.namenode.id";

		public const string DfsHaStandbyCheckpointsKey = "dfs.ha.standby.checkpoints";

		public const bool DfsHaStandbyCheckpointsDefault = true;

		public const string DfsHaLogrollPeriodKey = "dfs.ha.log-roll.period";

		public const int DfsHaLogrollPeriodDefault = 2 * 60;

		public const string DfsHaTaileditsPeriodKey = "dfs.ha.tail-edits.period";

		public const int DfsHaTaileditsPeriodDefault = 60;

		public const string DfsHaLogrollRpcTimeoutKey = "dfs.ha.log-roll.rpc.timeout";

		public const int DfsHaLogrollRpcTimeoutDefault = 20000;

		public const string DfsHaFenceMethodsKey = "dfs.ha.fencing.methods";

		public const string DfsHaAutoFailoverEnabledKey = "dfs.ha.automatic-failover.enabled";

		public const bool DfsHaAutoFailoverEnabledDefault = false;

		public const string DfsHaZkfcPortKey = "dfs.ha.zkfc.port";

		public const int DfsHaZkfcPortDefault = 8019;

		public const string DfsEncryptDataTransferKey = "dfs.encrypt.data.transfer";

		public const bool DfsEncryptDataTransferDefault = false;

		public const string DfsEncryptDataTransferCipherKeyBitlengthKey = "dfs.encrypt.data.transfer.cipher.key.bitlength";

		public const int DfsEncryptDataTransferCipherKeyBitlengthDefault = 128;

		public const string DfsEncryptDataTransferCipherSuitesKey = "dfs.encrypt.data.transfer.cipher.suites";

		public const string DfsDataEncryptionAlgorithmKey = "dfs.encrypt.data.transfer.algorithm";

		public const string DfsTrustedchannelResolverClass = "dfs.trustedchannel.resolver.class";

		public const string DfsDataTransferProtectionKey = "dfs.data.transfer.protection";

		public const string DfsDataTransferProtectionDefault = string.Empty;

		public const string DfsDataTransferSaslPropsResolverClassKey = "dfs.data.transfer.saslproperties.resolver.class";

		public const int DfsNamenodeListEncryptionZonesNumResponsesDefault = 100;

		public const string DfsNamenodeListEncryptionZonesNumResponses = "dfs.namenode.list.encryption.zones.num.responses";

		public const string DfsEncryptionKeyProviderUri = "dfs.encryption.key.provider.uri";

		public const string DfsJournalnodeEditsDirKey = "dfs.journalnode.edits.dir";

		public const string DfsJournalnodeEditsDirDefault = "/tmp/hadoop/dfs/journalnode/";

		public const string DfsJournalnodeRpcAddressKey = "dfs.journalnode.rpc-address";

		public const int DfsJournalnodeRpcPortDefault = 8485;

		public const string DfsJournalnodeRpcAddressDefault = "0.0.0.0:" + DfsJournalnodeRpcPortDefault;

		public const string DfsJournalnodeHttpAddressKey = "dfs.journalnode.http-address";

		public const int DfsJournalnodeHttpPortDefault = 8480;

		public const string DfsJournalnodeHttpAddressDefault = "0.0.0.0:" + DfsJournalnodeHttpPortDefault;

		public const string DfsJournalnodeHttpsAddressKey = "dfs.journalnode.https-address";

		public const int DfsJournalnodeHttpsPortDefault = 8481;

		public const string DfsJournalnodeHttpsAddressDefault = "0.0.0.0:" + DfsJournalnodeHttpsPortDefault;

		public const string DfsJournalnodeKeytabFileKey = "dfs.journalnode.keytab.file";

		public const string DfsJournalnodeKerberosPrincipalKey = "dfs.journalnode.kerberos.principal";

		public const string DfsJournalnodeKerberosInternalSpnegoPrincipalKey = "dfs.journalnode.kerberos.internal.spnego.principal";

		public const string DfsQjournalQueueSizeLimitKey = "dfs.qjournal.queued-edits.limit.mb";

		public const int DfsQjournalQueueSizeLimitDefault = 10;

		public const string DfsQjournalStartSegmentTimeoutKey = "dfs.qjournal.start-segment.timeout.ms";

		public const string DfsQjournalPrepareRecoveryTimeoutKey = "dfs.qjournal.prepare-recovery.timeout.ms";

		public const string DfsQjournalAcceptRecoveryTimeoutKey = "dfs.qjournal.accept-recovery.timeout.ms";

		public const string DfsQjournalFinalizeSegmentTimeoutKey = "dfs.qjournal.finalize-segment.timeout.ms";

		public const string DfsQjournalSelectInputStreamsTimeoutKey = "dfs.qjournal.select-input-streams.timeout.ms";

		public const string DfsQjournalGetJournalStateTimeoutKey = "dfs.qjournal.get-journal-state.timeout.ms";

		public const string DfsQjournalNewEpochTimeoutKey = "dfs.qjournal.new-epoch.timeout.ms";

		public const string DfsQjournalWriteTxnsTimeoutKey = "dfs.qjournal.write-txns.timeout.ms";

		public const int DfsQjournalStartSegmentTimeoutDefault = 20000;

		public const int DfsQjournalPrepareRecoveryTimeoutDefault = 120000;

		public const int DfsQjournalAcceptRecoveryTimeoutDefault = 120000;

		public const int DfsQjournalFinalizeSegmentTimeoutDefault = 120000;

		public const int DfsQjournalSelectInputStreamsTimeoutDefault = 20000;

		public const int DfsQjournalGetJournalStateTimeoutDefault = 120000;

		public const int DfsQjournalNewEpochTimeoutDefault = 120000;

		public const int DfsQjournalWriteTxnsTimeoutDefault = 20000;

		public const string DfsMaxNumBlocksToLogKey = "dfs.namenode.max-num-blocks-to-log";

		public const long DfsMaxNumBlocksToLogDefault = 1000l;

		public const string DfsNamenodeEnableRetryCacheKey = "dfs.namenode.enable.retrycache";

		public const bool DfsNamenodeEnableRetryCacheDefault = true;

		public const string DfsNamenodeRetryCacheExpirytimeMillisKey = "dfs.namenode.retrycache.expirytime.millis";

		public const long DfsNamenodeRetryCacheExpirytimeMillisDefault = 600000;

		public const string DfsNamenodeRetryCacheHeapPercentKey = "dfs.namenode.retrycache.heap.percent";

		public const float DfsNamenodeRetryCacheHeapPercentDefault = 0.03f;

		public const string DfsClientTestDropNamenodeResponseNumKey = "dfs.client.test.drop.namenode.response.number";

		public const int DfsClientTestDropNamenodeResponseNumDefault = 0;

		public const string DfsDatanodeXceiverStopTimeoutMillisKey = "dfs.datanode.xceiver.stop.timeout.millis";

		public const long DfsDatanodeXceiverStopTimeoutMillisDefault = 60000;

		public const string DfsHttpClientRetryPolicyEnabledKey = "dfs.http.client.retry.policy.enabled";

		public const bool DfsHttpClientRetryPolicyEnabledDefault = false;

		public const string DfsHttpClientRetryPolicySpecKey = "dfs.http.client.retry.policy.spec";

		public const string DfsHttpClientRetryPolicySpecDefault = "10000,6,60000,10";

		public const string DfsHttpClientFailoverMaxAttemptsKey = "dfs.http.client.failover.max.attempts";

		public const int DfsHttpClientFailoverMaxAttemptsDefault = 15;

		public const string DfsHttpClientRetryMaxAttemptsKey = "dfs.http.client.retry.max.attempts";

		public const int DfsHttpClientRetryMaxAttemptsDefault = 10;

		public const string DfsHttpClientFailoverSleeptimeBaseKey = "dfs.http.client.failover.sleep.base.millis";

		public const int DfsHttpClientFailoverSleeptimeBaseDefault = 500;

		public const string DfsHttpClientFailoverSleeptimeMaxKey = "dfs.http.client.failover.sleep.max.millis";

		public const int DfsHttpClientFailoverSleeptimeMaxDefault = 15000;

		public const string DfsRejectUnresolvedDnTopologyMappingKey = "dfs.namenode.reject-unresolved-dn-topology-mapping";

		public const bool DfsRejectUnresolvedDnTopologyMappingDefault = false;

		public const string DfsDfsclientHedgedReadThresholdMillis = "dfs.client.hedged.read.threshold.millis";

		public const long DefaultDfsclientHedgedReadThresholdMillis = 500;

		public const string DfsDfsclientHedgedReadThreadpoolSize = "dfs.client.hedged.read.threadpool.size";

		public const int DefaultDfsclientHedgedReadThreadpoolSize = 0;

		public const string DfsClientSlowIoWarningThresholdKey = "dfs.client.slow.io.warning.threshold.ms";

		public const long DfsClientSlowIoWarningThresholdDefault = 30000;

		public const string DfsDatanodeSlowIoWarningThresholdKey = "dfs.datanode.slow.io.warning.threshold.ms";

		public const long DfsDatanodeSlowIoWarningThresholdDefault = 300;

		public const string DfsNamenodeInotifyMaxEventsPerRpcKey = "dfs.namenode.inotify.max.events.per.rpc";

		public const int DfsNamenodeInotifyMaxEventsPerRpcDefault = 1000;

		public const string DfsDatanodeBlockIdLayoutUpgradeThreadsKey = "dfs.datanode.block.id.layout.upgrade.threads";

		public const int DfsDatanodeBlockIdLayoutUpgradeThreads = 12;

		public const string IgnoreSecurePortsForTestingKey = "ignore.secure.ports.for.testing";

		public const bool IgnoreSecurePortsForTestingDefault = false;

		public const string NntopEnabledKey = "dfs.namenode.top.enabled";

		public const bool NntopEnabledDefault = true;

		public const string NntopBucketsPerWindowKey = "dfs.namenode.top.window.num.buckets";

		public const int NntopBucketsPerWindowDefault = 10;

		public const string NntopNumUsersKey = "dfs.namenode.top.num.users";

		public const int NntopNumUsersDefault = 10;

		public const string NntopWindowsMinutesKey = "dfs.namenode.top.windows.minutes";

		public static readonly string[] NntopWindowsMinutesDefault = new string[] { "1", 
			"5", "25" };

		public const string DfsPipelineEcnEnabled = "dfs.pipeline.ecn";

		public const bool DfsPipelineEcnEnabledDefault = false;

		public const string DfsClientKeyProviderCacheExpiryMs = "dfs.client.key.provider.cache.expiry";

		public static readonly long DfsClientKeyProviderCacheExpiryDefault = TimeUnit.Days
			.ToMillis(10);

		public const string DfsDatanodeBlockPinningEnabled = "dfs.datanode.block-pinning.enabled";

		public const bool DfsDatanodeBlockPinningEnabledDefault = false;
		//t1,n1,t2,n2,... 
		// HDFS HTrace configuration is controlled by dfs.htrace.spanreceiver.classes,
		// etc.
		// HDFS client HTrace configuration.
		// HA related configuration
		// 10 minutes, in ms
		// 4MB
		// This setting is for testing/internal use only.
		// set this to a slightly smaller value than
		// DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT to populate
		// needed replication queues before exiting safe mode
		//1M
		// 10k
		// OOB_TYPE1, OOB_TYPE2, OOB_TYPE3, OOB_TYPE4
		// Whether to enable datanode's stale state detection and usage for reads
		// Whether to enable datanode's stale state detection and usage for writes
		// The default value of the time interval for marking datanodes as stale
		// 30s
		// The stale interval cannot be too small since otherwise this may cause too frequent churn on stale states. 
		// This value uses the times of heartbeat interval to define the minimum value for stale interval.  
		// i.e. min_interval is 3 * heartbeat_interval = 9s
		// When the percentage of stale datanodes reaches this ratio,
		// allow writing to stale nodes to prevent hotspots.
		// Number of blocks to rescan for each iteration of postponedMisreplicatedBlocks.
		// Replication monitoring related keys
		//Delegation token related keys
		// 1 day
		// 1 day
		// 7 days
		// for tests
		//Filesystem limit keys
		//Following keys have no defaults
		// Much code in hdfs is not yet updated to use these keys.
		// 3 weeks.
		/* Maximum number of blocks to process for initializing replication queues */
		// property for fsimage compression
		//no throttling
		// Image transfer timeout
		// Image transfer chunksize
		//Keys with no defaults
		// 10 GB
		// 100 MB
		// HA related configuration
		// 2m
		// 1m
		// 20s
		// Security-related configs
		// Journal-node related configs. These are read on the JN side.
		// Journal-node related configs for the client side.
		// Quorum-journal timeouts for various operations. Unlikely to need
		// to be tweaked, but configurable just in case.
		// 10 minutes
		// The number of NN response dropped by client proactively in each RPC call.
		// For testing NN retry cache, we can set this property with positive value.
		// Hidden configuration undocumented in hdfs-site. xml
		// Timeout to wait for block receiver and responder thread to stop
		// WebHDFS retry policy
		//t1,n1,t2,n2,...
		// Handling unresolved DN topology mapping
		// hedged read properties
		// Slow io warning log threshold settings for dfsclient and datanode.
		// nntop Configurations
		// comma separated list of nntop reporting periods in minutes
		// Key Provider Cache Expiry
		// 10 days
	}
}
