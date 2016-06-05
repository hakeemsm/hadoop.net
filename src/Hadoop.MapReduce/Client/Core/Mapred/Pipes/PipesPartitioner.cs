using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	/// <summary>
	/// This partitioner is one that can either be set manually per a record or it
	/// can fall back onto a Java partitioner that was set by the user.
	/// </summary>
	internal class PipesPartitioner<K, V> : Partitioner<K, V>
		where K : WritableComparable
		where V : Writable
	{
		private static ThreadLocal<int> cache = new ThreadLocal<int>();

		private Partitioner<K, V> part = null;

		public virtual void Configure(JobConf conf)
		{
			part = ReflectionUtils.NewInstance(Submitter.GetJavaPartitioner(conf), conf);
		}

		/// <summary>Set the next key to have the given partition.</summary>
		/// <param name="newValue">the next partition value</param>
		internal static void SetNextPartition(int newValue)
		{
			cache.Set(newValue);
		}

		/// <summary>If a partition result was set manually, return it.</summary>
		/// <remarks>
		/// If a partition result was set manually, return it. Otherwise, we call
		/// the Java partitioner.
		/// </remarks>
		/// <param name="key">the key to partition</param>
		/// <param name="value">the value to partition</param>
		/// <param name="numPartitions">the number of reduces</param>
		public virtual int GetPartition(K key, V value, int numPartitions)
		{
			int result = cache.Get();
			if (result == null)
			{
				return part.GetPartition(key, value, numPartitions);
			}
			else
			{
				return result;
			}
		}
	}
}
