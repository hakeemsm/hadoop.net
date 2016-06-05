using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
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
	/// Note: If you require your Partitioner class to obtain the Job's configuration
	/// object, implement the
	/// <see cref="Org.Apache.Hadoop.Conf.Configurable"/>
	/// interface.
	/// </remarks>
	/// <seealso cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	public abstract class Partitioner<Key, Value>
	{
		/// <summary>
		/// Get the partition number for a given key (hence record) given the total
		/// number of partitions i.e.
		/// </summary>
		/// <remarks>
		/// Get the partition number for a given key (hence record) given the total
		/// number of partitions i.e. number of reduce-tasks for the job.
		/// <p>Typically a hash function on a all or a subset of the key.</p>
		/// </remarks>
		/// <param name="key">the key to be partioned.</param>
		/// <param name="value">the entry value.</param>
		/// <param name="numPartitions">the total number of partitions.</param>
		/// <returns>the partition number for the <code>key</code>.</returns>
		public abstract int GetPartition(KEY key, VALUE value, int numPartitions);
	}
}
