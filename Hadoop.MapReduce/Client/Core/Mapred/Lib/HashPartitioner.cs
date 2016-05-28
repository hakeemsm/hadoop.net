using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// Partition keys by their
	/// <see cref="object.GetHashCode()"/>
	/// .
	/// </summary>
	public class HashPartitioner<K2, V2> : Partitioner<K2, V2>
	{
		public virtual void Configure(JobConf job)
		{
		}

		/// <summary>
		/// Use
		/// <see cref="object.GetHashCode()"/>
		/// to partition.
		/// </summary>
		public virtual int GetPartition(K2 key, V2 value, int numReduceTasks)
		{
			return (key.GetHashCode() & int.MaxValue) % numReduceTasks;
		}
	}
}
