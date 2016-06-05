using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Partitions the key space.</summary>
	/// <remarks>
	/// Partitions the key space.
	/// <p><code>Partitioner</code> controls the partitioning of the keys of the
	/// intermediate map-outputs. The key (or a subset of the key) is used to derive
	/// the partition, typically by a hash function. The total number of partitions
	/// is the same as the number of reduce tasks for the job. Hence this controls
	/// which of the <code>m</code> reduce tasks the intermediate key (and hence the
	/// record) is sent for reduction.</p>
	/// </remarks>
	/// <seealso cref="Reducer{K2, V2, K3, V3}"/>
	public interface Partitioner<K2, V2> : JobConfigurable
	{
		/// <summary>
		/// Get the paritition number for a given key (hence record) given the total
		/// number of partitions i.e.
		/// </summary>
		/// <remarks>
		/// Get the paritition number for a given key (hence record) given the total
		/// number of partitions i.e. number of reduce-tasks for the job.
		/// <p>Typically a hash function on a all or a subset of the key.</p>
		/// </remarks>
		/// <param name="key">the key to be paritioned.</param>
		/// <param name="value">the entry value.</param>
		/// <param name="numPartitions">the total number of partitions.</param>
		/// <returns>the partition number for the <code>key</code>.</returns>
		int GetPartition(K2 key, V2 value, int numPartitions);
	}
}
