using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Metrics
{
	/// <summary>
	/// This Interface defines the methods to get the status of a the FSDataset of
	/// a data node.
	/// </summary>
	/// <remarks>
	/// This Interface defines the methods to get the status of a the FSDataset of
	/// a data node.
	/// It is also used for publishing via JMX (hence we follow the JMX naming
	/// convention.)
	/// * Note we have not used the MetricsDynamicMBeanBase to implement this
	/// because the interface for the FSDatasetMBean is stable and should
	/// be published as an interface.
	/// <p>
	/// Data Node runtime statistic  info is report in another MBean
	/// </remarks>
	/// <seealso cref="DataNodeMetrics"/>
	public interface FSDatasetMBean
	{
		/// <summary>Returns the total space (in bytes) used by a block pool</summary>
		/// <returns>the total space used by a block pool</returns>
		/// <exception cref="System.IO.IOException"/>
		long GetBlockPoolUsed(string bpid);

		/// <summary>Returns the total space (in bytes) used by dfs datanode</summary>
		/// <returns>the total space used by dfs datanode</returns>
		/// <exception cref="System.IO.IOException"/>
		long GetDfsUsed();

		/// <summary>Returns total capacity (in bytes) of storage (used and unused)</summary>
		/// <returns>total capacity of storage (used and unused)</returns>
		long GetCapacity();

		/// <summary>Returns the amount of free storage space (in bytes)</summary>
		/// <returns>The amount of free storage space</returns>
		/// <exception cref="System.IO.IOException"/>
		long GetRemaining();

		/// <summary>Returns the storage id of the underlying storage</summary>
		string GetStorageInfo();

		/// <summary>Returns the number of failed volumes in the datanode.</summary>
		/// <returns>The number of failed volumes in the datanode.</returns>
		int GetNumFailedVolumes();

		/// <summary>Returns each storage location that has failed, sorted.</summary>
		/// <returns>each storage location that has failed, sorted</returns>
		string[] GetFailedStorageLocations();

		/// <summary>
		/// Returns the date/time of the last volume failure in milliseconds since
		/// epoch.
		/// </summary>
		/// <returns>date/time of last volume failure in milliseconds since epoch</returns>
		long GetLastVolumeFailureDate();

		/// <summary>Returns an estimate of total capacity lost due to volume failures in bytes.
		/// 	</summary>
		/// <returns>estimate of total capacity lost in bytes</returns>
		long GetEstimatedCapacityLostTotal();

		/// <summary>Returns the amount of cache used by the datanode (in bytes).</summary>
		long GetCacheUsed();

		/// <summary>Returns the total cache capacity of the datanode (in bytes).</summary>
		long GetCacheCapacity();

		/// <summary>Returns the number of blocks cached.</summary>
		long GetNumBlocksCached();

		/// <summary>Returns the number of blocks that the datanode was unable to cache</summary>
		long GetNumBlocksFailedToCache();

		/// <summary>Returns the number of blocks that the datanode was unable to uncache</summary>
		long GetNumBlocksFailedToUncache();
	}
}
