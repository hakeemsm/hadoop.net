using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Adds deprecated keys into the configuration.</summary>
	public class HdfsConfiguration : Configuration
	{
		static HdfsConfiguration()
		{
			AddDeprecatedKeys();
			// adds the default resources
			Configuration.AddDefaultResource("hdfs-default.xml");
			Configuration.AddDefaultResource("hdfs-site.xml");
		}

		public HdfsConfiguration()
			: base()
		{
		}

		public HdfsConfiguration(bool loadDefaults)
			: base(loadDefaults)
		{
		}

		public HdfsConfiguration(Configuration conf)
			: base(conf)
		{
		}

		/// <summary>
		/// This method is here so that when invoked, HdfsConfiguration is class-loaded if
		/// it hasn't already been previously loaded.
		/// </summary>
		/// <remarks>
		/// This method is here so that when invoked, HdfsConfiguration is class-loaded if
		/// it hasn't already been previously loaded.  Upon loading the class, the static
		/// initializer block above will be executed to add the deprecated keys and to add
		/// the default resources.   It is safe for this method to be called multiple times
		/// as the static initializer block will only get invoked once.
		/// This replaces the previously, dangerous practice of other classes calling
		/// Configuration.addDefaultResource("hdfs-default.xml") directly without loading
		/// HdfsConfiguration class first, thereby skipping the key deprecation
		/// </remarks>
		public static void Init()
		{
		}

		private static void AddDeprecatedKeys()
		{
			Configuration.AddDeprecations(new Configuration.DeprecationDelta[] { new Configuration.DeprecationDelta
				("dfs.backup.address", DFSConfigKeys.DfsNamenodeBackupAddressKey), new Configuration.DeprecationDelta
				("dfs.backup.http.address", DFSConfigKeys.DfsNamenodeBackupHttpAddressKey), new 
				Configuration.DeprecationDelta("dfs.balance.bandwidthPerSec", DFSConfigKeys.DfsDatanodeBalanceBandwidthpersecKey
				), new Configuration.DeprecationDelta("dfs.data.dir", DFSConfigKeys.DfsDatanodeDataDirKey
				), new Configuration.DeprecationDelta("dfs.http.address", DFSConfigKeys.DfsNamenodeHttpAddressKey
				), new Configuration.DeprecationDelta("dfs.https.address", DFSConfigKeys.DfsNamenodeHttpsAddressKey
				), new Configuration.DeprecationDelta("dfs.max.objects", DFSConfigKeys.DfsNamenodeMaxObjectsKey
				), new Configuration.DeprecationDelta("dfs.name.dir", DFSConfigKeys.DfsNamenodeNameDirKey
				), new Configuration.DeprecationDelta("dfs.name.dir.restore", DFSConfigKeys.DfsNamenodeNameDirRestoreKey
				), new Configuration.DeprecationDelta("dfs.name.edits.dir", DFSConfigKeys.DfsNamenodeEditsDirKey
				), new Configuration.DeprecationDelta("dfs.read.prefetch.size", DFSConfigKeys.DfsClientReadPrefetchSizeKey
				), new Configuration.DeprecationDelta("dfs.safemode.extension", DFSConfigKeys.DfsNamenodeSafemodeExtensionKey
				), new Configuration.DeprecationDelta("dfs.safemode.threshold.pct", DFSConfigKeys
				.DfsNamenodeSafemodeThresholdPctKey), new Configuration.DeprecationDelta("dfs.secondary.http.address"
				, DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey), new Configuration.DeprecationDelta
				("dfs.socket.timeout", DFSConfigKeys.DfsClientSocketTimeoutKey), new Configuration.DeprecationDelta
				("fs.checkpoint.dir", DFSConfigKeys.DfsNamenodeCheckpointDirKey), new Configuration.DeprecationDelta
				("fs.checkpoint.edits.dir", DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey), new 
				Configuration.DeprecationDelta("fs.checkpoint.period", DFSConfigKeys.DfsNamenodeCheckpointPeriodKey
				), new Configuration.DeprecationDelta("heartbeat.recheck.interval", DFSConfigKeys
				.DfsNamenodeHeartbeatRecheckIntervalKey), new Configuration.DeprecationDelta("dfs.https.client.keystore.resource"
				, DFSConfigKeys.DfsClientHttpsKeystoreResourceKey), new Configuration.DeprecationDelta
				("dfs.https.need.client.auth", DFSConfigKeys.DfsClientHttpsNeedAuthKey), new Configuration.DeprecationDelta
				("slave.host.name", DFSConfigKeys.DfsDatanodeHostNameKey), new Configuration.DeprecationDelta
				("session.id", DFSConfigKeys.DfsMetricsSessionIdKey), new Configuration.DeprecationDelta
				("dfs.access.time.precision", DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey), 
				new Configuration.DeprecationDelta("dfs.replication.considerLoad", DFSConfigKeys
				.DfsNamenodeReplicationConsiderloadKey), new Configuration.DeprecationDelta("dfs.replication.interval"
				, DFSConfigKeys.DfsNamenodeReplicationIntervalKey), new Configuration.DeprecationDelta
				("dfs.replication.min", DFSConfigKeys.DfsNamenodeReplicationMinKey), new Configuration.DeprecationDelta
				("dfs.replication.pending.timeout.sec", DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey
				), new Configuration.DeprecationDelta("dfs.max-repl-streams", DFSConfigKeys.DfsNamenodeReplicationMaxStreamsKey
				), new Configuration.DeprecationDelta("dfs.permissions", DFSConfigKeys.DfsPermissionsEnabledKey
				), new Configuration.DeprecationDelta("dfs.permissions.supergroup", DFSConfigKeys
				.DfsPermissionsSuperusergroupKey), new Configuration.DeprecationDelta("dfs.write.packet.size"
				, DFSConfigKeys.DfsClientWritePacketSizeKey), new Configuration.DeprecationDelta
				("dfs.block.size", DFSConfigKeys.DfsBlockSizeKey), new Configuration.DeprecationDelta
				("dfs.datanode.max.xcievers", DFSConfigKeys.DfsDatanodeMaxReceiverThreadsKey), new 
				Configuration.DeprecationDelta("io.bytes.per.checksum", DFSConfigKeys.DfsBytesPerChecksumKey
				), new Configuration.DeprecationDelta("dfs.federation.nameservices", DFSConfigKeys
				.DfsNameservices), new Configuration.DeprecationDelta("dfs.federation.nameservice.id"
				, DFSConfigKeys.DfsNameserviceId), new Configuration.DeprecationDelta("dfs.client.file-block-storage-locations.timeout"
				, DFSConfigKeys.DfsClientFileBlockStorageLocationsTimeoutMs) });
		}

		public static void Main(string[] args)
		{
			Init();
			Configuration.DumpDeprecatedKeys();
		}
	}
}
