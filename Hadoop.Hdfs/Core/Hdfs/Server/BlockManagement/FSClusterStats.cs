using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// This interface is used for retrieving the load related statistics of
	/// the cluster.
	/// </summary>
	public interface FSClusterStats
	{
		/// <summary>an indication of the total load of the cluster.</summary>
		/// <returns>
		/// a count of the total number of block transfers and block
		/// writes that are currently occuring on the cluster.
		/// </returns>
		int GetTotalLoad();

		/// <summary>
		/// Indicate whether or not the cluster is now avoiding
		/// to use stale DataNodes for writing.
		/// </summary>
		/// <returns>
		/// True if the cluster is currently avoiding using stale DataNodes
		/// for writing targets, and false otherwise.
		/// </returns>
		bool IsAvoidingStaleDataNodesForWrite();

		/// <summary>Indicates number of datanodes that are in service.</summary>
		/// <returns>Number of datanodes that are both alive and not decommissioned.</returns>
		int GetNumDatanodesInService();

		/// <summary>
		/// an indication of the average load of non-decommission(ing|ed) nodes
		/// eligible for block placement
		/// </summary>
		/// <returns>
		/// average of the in service number of block transfers and block
		/// writes that are currently occurring on the cluster.
		/// </returns>
		double GetInServiceXceiverAverage();
	}
}
