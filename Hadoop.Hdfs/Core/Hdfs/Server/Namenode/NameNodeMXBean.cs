using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This is the JMX management interface for namenode information</summary>
	public interface NameNodeMXBean
	{
		/// <summary>Gets the version of Hadoop.</summary>
		/// <returns>the version</returns>
		string GetVersion();

		/// <summary>Get the version of software running on the Namenode</summary>
		/// <returns>a string representing the version</returns>
		string GetSoftwareVersion();

		/// <summary>Gets the used space by data nodes.</summary>
		/// <returns>the used space by data nodes</returns>
		long GetUsed();

		/// <summary>Gets total non-used raw bytes.</summary>
		/// <returns>total non-used raw bytes</returns>
		long GetFree();

		/// <summary>Gets total raw bytes including non-dfs used space.</summary>
		/// <returns>the total raw bytes including non-dfs used space</returns>
		long GetTotal();

		/// <summary>Gets the safemode status</summary>
		/// <returns>the safemode status</returns>
		string GetSafemode();

		/// <summary>Checks if upgrade is finalized.</summary>
		/// <returns>true, if upgrade is finalized</returns>
		bool IsUpgradeFinalized();

		/// <summary>Gets the RollingUpgrade information.</summary>
		/// <returns>
		/// Rolling upgrade information if an upgrade is in progress. Else
		/// (e.g. if there is no upgrade or the upgrade is finalized), returns null.
		/// </returns>
		RollingUpgradeInfo.Bean GetRollingUpgradeStatus();

		/// <summary>
		/// Gets total used space by data nodes for non DFS purposes such as storing
		/// temporary files on the local file system
		/// </summary>
		/// <returns>the non dfs space of the cluster</returns>
		long GetNonDfsUsedSpace();

		/// <summary>Gets the total used space by data nodes as percentage of total capacity</summary>
		/// <returns>the percentage of used space on the cluster.</returns>
		float GetPercentUsed();

		/// <summary>
		/// Gets the total remaining space by data nodes as percentage of total
		/// capacity
		/// </summary>
		/// <returns>the percentage of the remaining space on the cluster</returns>
		float GetPercentRemaining();

		/// <summary>Returns the amount of cache used by the datanode (in bytes).</summary>
		long GetCacheUsed();

		/// <summary>Returns the total cache capacity of the datanode (in bytes).</summary>
		long GetCacheCapacity();

		/// <summary>Get the total space used by the block pools of this namenode</summary>
		long GetBlockPoolUsedSpace();

		/// <summary>Get the total space used by the block pool as percentage of total capacity
		/// 	</summary>
		float GetPercentBlockPoolUsed();

		/// <summary>Gets the total numbers of blocks on the cluster.</summary>
		/// <returns>the total number of blocks of the cluster</returns>
		long GetTotalBlocks();

		/// <summary>Gets the total number of files on the cluster</summary>
		/// <returns>the total number of files on the cluster</returns>
		long GetTotalFiles();

		/// <summary>Gets the total number of missing blocks on the cluster</summary>
		/// <returns>the total number of missing blocks on the cluster</returns>
		long GetNumberOfMissingBlocks();

		/// <summary>
		/// Gets the total number of missing blocks on the cluster with
		/// replication factor 1
		/// </summary>
		/// <returns>
		/// the total number of missing blocks on the cluster with
		/// replication factor 1
		/// </returns>
		long GetNumberOfMissingBlocksWithReplicationFactorOne();

		/// <summary>Gets the number of threads.</summary>
		/// <returns>the number of threads</returns>
		int GetThreads();

		/// <summary>Gets the live node information of the cluster.</summary>
		/// <returns>the live node information</returns>
		string GetLiveNodes();

		/// <summary>Gets the dead node information of the cluster.</summary>
		/// <returns>the dead node information</returns>
		string GetDeadNodes();

		/// <summary>Gets the decommissioning node information of the cluster.</summary>
		/// <returns>the decommissioning node information</returns>
		string GetDecomNodes();

		/// <summary>Gets the cluster id.</summary>
		/// <returns>the cluster id</returns>
		string GetClusterId();

		/// <summary>Gets the block pool id.</summary>
		/// <returns>the block pool id</returns>
		string GetBlockPoolId();

		/// <summary>
		/// Get status information about the directories storing image and edits logs
		/// of the NN.
		/// </summary>
		/// <returns>the name dir status information, as a JSON string.</returns>
		string GetNameDirStatuses();

		/// <summary>Get Max, Median, Min and Standard Deviation of DataNodes usage.</summary>
		/// <returns>the DataNode usage information, as a JSON string.</returns>
		string GetNodeUsage();

		/// <summary>Get status information about the journals of the NN.</summary>
		/// <returns>the name journal status information, as a JSON string.</returns>
		string GetNameJournalStatus();

		/// <summary>
		/// Get information about the transaction ID, including the last applied
		/// transaction ID and the most recent checkpoint's transaction ID
		/// </summary>
		string GetJournalTransactionInfo();

		/// <summary>Gets the NN start time</summary>
		/// <returns>the NN start time</returns>
		string GetNNStarted();

		/// <summary>Get the compilation information which contains date, user and branch</summary>
		/// <returns>the compilation information, as a JSON string.</returns>
		string GetCompileInfo();

		/// <summary>Get the list of corrupt files</summary>
		/// <returns>the list of corrupt files, as a JSON string.</returns>
		string GetCorruptFiles();

		/// <summary>Get the number of distinct versions of live datanodes</summary>
		/// <returns>the number of distinct versions of live datanodes</returns>
		int GetDistinctVersionCount();

		/// <summary>Get the number of live datanodes for each distinct versions</summary>
		/// <returns>the number of live datanodes for each distinct versions</returns>
		IDictionary<string, int> GetDistinctVersions();
	}
}
