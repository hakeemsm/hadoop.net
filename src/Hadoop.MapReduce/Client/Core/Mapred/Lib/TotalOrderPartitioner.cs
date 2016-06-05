using System;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// Partitioner effecting a total order by reading split points from
	/// an externally generated source.
	/// </summary>
	public class TotalOrderPartitioner<K, V> : Org.Apache.Hadoop.Mapreduce.Lib.Partition.TotalOrderPartitioner
		<K, V>, Partitioner<K, V>
		where K : WritableComparable<object>
	{
		public TotalOrderPartitioner()
		{
		}

		public virtual void Configure(JobConf job)
		{
			base.SetConf(job);
		}

		/// <summary>Set the path to the SequenceFile storing the sorted partition keyset.</summary>
		/// <remarks>
		/// Set the path to the SequenceFile storing the sorted partition keyset.
		/// It must be the case that for <tt>R</tt> reduces, there are <tt>R-1</tt>
		/// keys in the SequenceFile.
		/// </remarks>
		[System.ObsoleteAttribute(@"Use #setPartitionFile(Configuration,Path) instead")]
		public static void SetPartitionFile(JobConf job, Path p)
		{
			Org.Apache.Hadoop.Mapreduce.Lib.Partition.TotalOrderPartitioner.SetPartitionFile(
				job, p);
		}

		/// <summary>Get the path to the SequenceFile storing the sorted partition keyset.</summary>
		/// <seealso cref="TotalOrderPartitioner{K, V}.SetPartitionFile(Org.Apache.Hadoop.Mapred.JobConf, Org.Apache.Hadoop.FS.Path)
		/// 	"/>
		[System.ObsoleteAttribute(@"Use #getPartitionFile(Configuration) instead")]
		public static string GetPartitionFile(JobConf job)
		{
			return Org.Apache.Hadoop.Mapreduce.Lib.Partition.TotalOrderPartitioner.GetPartitionFile
				(job);
		}
	}
}
