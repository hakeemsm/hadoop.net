using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>The partitioner partitions the map output according to the operation type.
	/// 	</summary>
	/// <remarks>
	/// The partitioner partitions the map output according to the operation type.
	/// The partition number is the hash of the operation type modular the total
	/// number of the reducers.
	/// </remarks>
	public class SlivePartitioner : Partitioner<Text, Text>
	{
		public virtual void Configure(JobConf conf)
		{
		}

		// JobConfigurable
		public virtual int GetPartition(Text key, Text value, int numPartitions)
		{
			// Partitioner
			OperationOutput oo = new OperationOutput(key, value);
			return (oo.GetOperationType().GetHashCode() & int.MaxValue) % numPartitions;
		}
	}
}
