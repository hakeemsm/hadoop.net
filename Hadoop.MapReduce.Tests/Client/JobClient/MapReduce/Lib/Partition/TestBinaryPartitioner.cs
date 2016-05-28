using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	public class TestBinaryPartitioner : TestCase
	{
		public virtual void TestDefaultOffsets()
		{
			Configuration conf = new Configuration();
			BinaryPartitioner<object> partitioner = ReflectionUtils.NewInstance<BinaryPartitioner
				>(conf);
			BinaryComparable key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 });
			BinaryComparable key2 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 });
			int partition1 = partitioner.GetPartition(key1, null, 10);
			int partition2 = partitioner.GetPartition(key2, null, 10);
			NUnit.Framework.Assert.AreEqual(partition1, partition2);
			key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 });
			key2 = new BytesWritable(new byte[] { 6, 2, 3, 4, 5 });
			partition1 = partitioner.GetPartition(key1, null, 10);
			partition2 = partitioner.GetPartition(key2, null, 10);
			NUnit.Framework.Assert.IsTrue(partition1 != partition2);
			key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 });
			key2 = new BytesWritable(new byte[] { 1, 2, 3, 4, 6 });
			partition1 = partitioner.GetPartition(key1, null, 10);
			partition2 = partitioner.GetPartition(key2, null, 10);
			NUnit.Framework.Assert.IsTrue(partition1 != partition2);
		}

		public virtual void TestCustomOffsets()
		{
			Configuration conf = new Configuration();
			BinaryComparable key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 });
			BinaryComparable key2 = new BytesWritable(new byte[] { 6, 2, 3, 7, 8 });
			BinaryPartitioner.SetOffsets(conf, 1, -3);
			BinaryPartitioner<object> partitioner = ReflectionUtils.NewInstance<BinaryPartitioner
				>(conf);
			int partition1 = partitioner.GetPartition(key1, null, 10);
			int partition2 = partitioner.GetPartition(key2, null, 10);
			NUnit.Framework.Assert.AreEqual(partition1, partition2);
			BinaryPartitioner.SetOffsets(conf, 1, 2);
			partitioner = ReflectionUtils.NewInstance<BinaryPartitioner>(conf);
			partition1 = partitioner.GetPartition(key1, null, 10);
			partition2 = partitioner.GetPartition(key2, null, 10);
			NUnit.Framework.Assert.AreEqual(partition1, partition2);
			BinaryPartitioner.SetOffsets(conf, -4, -3);
			partitioner = ReflectionUtils.NewInstance<BinaryPartitioner>(conf);
			partition1 = partitioner.GetPartition(key1, null, 10);
			partition2 = partitioner.GetPartition(key2, null, 10);
			NUnit.Framework.Assert.AreEqual(partition1, partition2);
		}

		public virtual void TestLowerBound()
		{
			Configuration conf = new Configuration();
			BinaryPartitioner.SetLeftOffset(conf, 0);
			BinaryPartitioner<object> partitioner = ReflectionUtils.NewInstance<BinaryPartitioner
				>(conf);
			BinaryComparable key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 });
			BinaryComparable key2 = new BytesWritable(new byte[] { 6, 2, 3, 4, 5 });
			int partition1 = partitioner.GetPartition(key1, null, 10);
			int partition2 = partitioner.GetPartition(key2, null, 10);
			NUnit.Framework.Assert.IsTrue(partition1 != partition2);
		}

		public virtual void TestUpperBound()
		{
			Configuration conf = new Configuration();
			BinaryPartitioner.SetRightOffset(conf, 4);
			BinaryPartitioner<object> partitioner = ReflectionUtils.NewInstance<BinaryPartitioner
				>(conf);
			BinaryComparable key1 = new BytesWritable(new byte[] { 1, 2, 3, 4, 5 });
			BinaryComparable key2 = new BytesWritable(new byte[] { 1, 2, 3, 4, 6 });
			int partition1 = partitioner.GetPartition(key1, null, 10);
			int partition2 = partitioner.GetPartition(key2, null, 10);
			NUnit.Framework.Assert.IsTrue(partition1 != partition2);
		}
	}
}
