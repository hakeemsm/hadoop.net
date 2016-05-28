using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics
{
	/// <summary>
	/// This Interface defines the methods to get the status of a the FSNamesystem of
	/// a name node.
	/// </summary>
	/// <remarks>
	/// This Interface defines the methods to get the status of a the FSNamesystem of
	/// a name node.
	/// It is also used for publishing via JMX (hence we follow the JMX naming
	/// convention.)
	/// Note we have not used the MetricsDynamicMBeanBase to implement this
	/// because the interface for the NameNodeStateMBean is stable and should
	/// be published as an interface.
	/// <p>
	/// Name Node runtime activity statistic  info is reported in
	/// </remarks>
	/// <seealso cref="NameNodeMetrics"/>
	public interface FSNamesystemMBean
	{
		/// <summary>The state of the file system: Safemode or Operational</summary>
		/// <returns>the state</returns>
		string GetFSState();

		/// <summary>Number of allocated blocks in the system</summary>
		/// <returns>-  number of allocated blocks</returns>
		long GetBlocksTotal();

		/// <summary>Total storage capacity</summary>
		/// <returns>-  total capacity in bytes</returns>
		long GetCapacityTotal();

		/// <summary>Free (unused) storage capacity</summary>
		/// <returns>-  free capacity in bytes</returns>
		long GetCapacityRemaining();

		/// <summary>Used storage capacity</summary>
		/// <returns>-  used capacity in bytes</returns>
		long GetCapacityUsed();

		/// <summary>Total number of files and directories</summary>
		/// <returns>-  num of files and directories</returns>
		long GetFilesTotal();

		/// <summary>Blocks pending to be replicated</summary>
		/// <returns>-  num of blocks to be replicated</returns>
		long GetPendingReplicationBlocks();

		/// <summary>Blocks under replicated</summary>
		/// <returns>-  num of blocks under replicated</returns>
		long GetUnderReplicatedBlocks();

		/// <summary>Blocks scheduled for replication</summary>
		/// <returns>-  num of blocks scheduled for replication</returns>
		long GetScheduledReplicationBlocks();

		/// <summary>Total Load on the FSNamesystem</summary>
		/// <returns>-  total load of FSNamesystem</returns>
		int GetTotalLoad();

		/// <summary>Number of Live data nodes</summary>
		/// <returns>number of live data nodes</returns>
		int GetNumLiveDataNodes();

		/// <summary>Number of dead data nodes</summary>
		/// <returns>number of dead data nodes</returns>
		int GetNumDeadDataNodes();

		/// <summary>Number of stale data nodes</summary>
		/// <returns>number of stale data nodes</returns>
		int GetNumStaleDataNodes();

		/// <summary>Number of decommissioned Live data nodes</summary>
		/// <returns>number of decommissioned live data nodes</returns>
		int GetNumDecomLiveDataNodes();

		/// <summary>Number of decommissioned dead data nodes</summary>
		/// <returns>number of decommissioned dead data nodes</returns>
		int GetNumDecomDeadDataNodes();

		/// <summary>Number of failed data volumes across all live data nodes.</summary>
		/// <returns>number of failed data volumes across all live data nodes</returns>
		int GetVolumeFailuresTotal();

		/// <summary>
		/// Returns an estimate of total capacity lost due to volume failures in bytes
		/// across all live data nodes.
		/// </summary>
		/// <returns>estimate of total capacity lost in bytes</returns>
		long GetEstimatedCapacityLostTotal();

		/// <summary>Number of data nodes that are in the decommissioning state</summary>
		int GetNumDecommissioningDataNodes();

		/// <summary>The statistics of snapshots</summary>
		string GetSnapshotStats();

		/// <summary>Return the maximum number of inodes in the file system</summary>
		long GetMaxObjects();

		/// <summary>Number of blocks pending deletion</summary>
		/// <returns>number of blocks pending deletion</returns>
		long GetPendingDeletionBlocks();

		/// <summary>Time when block deletions will begin</summary>
		/// <returns>time when block deletions will begin</returns>
		long GetBlockDeletionStartTime();

		/// <summary>Number of content stale storages.</summary>
		/// <returns>number of content stale storages</returns>
		int GetNumStaleStorages();

		/// <summary>
		/// Returns a nested JSON object listing the top users for different RPC
		/// operations over tracked time windows.
		/// </summary>
		/// <returns>JSON string</returns>
		string GetTopUserOpCounts();
	}
}
