using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Datanode statistics</summary>
	public interface DatanodeStatistics
	{
		/// <returns>the total capacity</returns>
		long GetCapacityTotal();

		/// <returns>the used capacity</returns>
		long GetCapacityUsed();

		/// <returns>the percentage of the used capacity over the total capacity.</returns>
		float GetCapacityUsedPercent();

		/// <returns>the remaining capacity</returns>
		long GetCapacityRemaining();

		/// <returns>the percentage of the remaining capacity over the total capacity.</returns>
		float GetCapacityRemainingPercent();

		/// <returns>the block pool used.</returns>
		long GetBlockPoolUsed();

		/// <returns>the percentage of the block pool used space over the total capacity.</returns>
		float GetPercentBlockPoolUsed();

		/// <returns>the total cache capacity of all DataNodes</returns>
		long GetCacheCapacity();

		/// <returns>the total cache used by all DataNodes</returns>
		long GetCacheUsed();

		/// <returns>the xceiver count</returns>
		int GetXceiverCount();

		/// <returns>average xceiver count for non-decommission(ing|ed) nodes</returns>
		int GetInServiceXceiverCount();

		/// <returns>number of non-decommission(ing|ed) nodes</returns>
		int GetNumDatanodesInService();

		/// <returns>
		/// the total used space by data nodes for non-DFS purposes
		/// such as storing temporary files on the local file system
		/// </returns>
		long GetCapacityUsedNonDFS();

		/// <summary>
		/// The same as
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetStats()"/>
		/// .
		/// The block related entries are set to -1.
		/// </summary>
		long[] GetStats();

		/// <returns>the expired heartbeats</returns>
		int GetExpiredHeartbeats();
	}
}
