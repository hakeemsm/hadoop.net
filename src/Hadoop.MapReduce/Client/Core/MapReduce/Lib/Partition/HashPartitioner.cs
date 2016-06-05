using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	/// <summary>
	/// Partition keys by their
	/// <see cref="object.GetHashCode()"/>
	/// .
	/// </summary>
	public class HashPartitioner<K, V> : Partitioner<K, V>
	{
		/// <summary>
		/// Use
		/// <see cref="object.GetHashCode()"/>
		/// to partition.
		/// </summary>
		public override int GetPartition(K key, V value, int numReduceTasks)
		{
			return (key.GetHashCode() & int.MaxValue) % numReduceTasks;
		}
	}
}
