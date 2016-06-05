using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Partition;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// Partition
	/// <see cref="Org.Apache.Hadoop.IO.BinaryComparable"/>
	/// keys using a configurable part of
	/// the bytes array returned by
	/// <see cref="Org.Apache.Hadoop.IO.BinaryComparable.GetBytes()"/>
	/// .
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Lib.Partition.BinaryPartitioner{V}"/>
	public class BinaryPartitioner<V> : BinaryPartitioner<V>, Partitioner<BinaryComparable
		, V>
	{
		public virtual void Configure(JobConf job)
		{
			base.SetConf(job);
		}
	}
}
