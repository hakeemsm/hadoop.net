using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	public class TestMRKeyFieldBasedPartitioner : TestCase
	{
		/// <summary>Test is key-field-based partitioned works with empty key.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestEmptyKey()
		{
			int numReducers = 10;
			KeyFieldBasedPartitioner<Text, Text> kfbp = new KeyFieldBasedPartitioner<Text, Text
				>();
			Configuration conf = new Configuration();
			conf.SetInt("num.key.fields.for.partition", 10);
			kfbp.SetConf(conf);
			NUnit.Framework.Assert.AreEqual("Empty key should map to 0th partition", 0, kfbp.
				GetPartition(new Text(), new Text(), numReducers));
			// check if the hashcode is correct when no keyspec is specified
			kfbp = new KeyFieldBasedPartitioner<Text, Text>();
			conf = new Configuration();
			kfbp.SetConf(conf);
			string input = "abc\tdef\txyz";
			int hashCode = input.GetHashCode();
			int expectedPartition = kfbp.GetPartition(hashCode, numReducers);
			NUnit.Framework.Assert.AreEqual("Partitioner doesnt work as expected", expectedPartition
				, kfbp.GetPartition(new Text(input), new Text(), numReducers));
			// check if the hashcode is correct with specified keyspec
			kfbp = new KeyFieldBasedPartitioner<Text, Text>();
			conf = new Configuration();
			conf.Set(KeyFieldBasedPartitioner.PartitionerOptions, "-k2,2");
			kfbp.SetConf(conf);
			string expectedOutput = "def";
			byte[] eBytes = Sharpen.Runtime.GetBytesForString(expectedOutput);
			hashCode = kfbp.HashCode(eBytes, 0, eBytes.Length - 1, 0);
			expectedPartition = kfbp.GetPartition(hashCode, numReducers);
			NUnit.Framework.Assert.AreEqual("Partitioner doesnt work as expected", expectedPartition
				, kfbp.GetPartition(new Text(input), new Text(), numReducers));
			// test with invalid end index in keyspecs
			kfbp = new KeyFieldBasedPartitioner<Text, Text>();
			conf = new Configuration();
			conf.Set(KeyFieldBasedPartitioner.PartitionerOptions, "-k2,5");
			kfbp.SetConf(conf);
			expectedOutput = "def\txyz";
			eBytes = Sharpen.Runtime.GetBytesForString(expectedOutput);
			hashCode = kfbp.HashCode(eBytes, 0, eBytes.Length - 1, 0);
			expectedPartition = kfbp.GetPartition(hashCode, numReducers);
			NUnit.Framework.Assert.AreEqual("Partitioner doesnt work as expected", expectedPartition
				, kfbp.GetPartition(new Text(input), new Text(), numReducers));
			// test with 0 end index in keyspecs
			kfbp = new KeyFieldBasedPartitioner<Text, Text>();
			conf = new Configuration();
			conf.Set(KeyFieldBasedPartitioner.PartitionerOptions, "-k2");
			kfbp.SetConf(conf);
			expectedOutput = "def\txyz";
			eBytes = Sharpen.Runtime.GetBytesForString(expectedOutput);
			hashCode = kfbp.HashCode(eBytes, 0, eBytes.Length - 1, 0);
			expectedPartition = kfbp.GetPartition(hashCode, numReducers);
			NUnit.Framework.Assert.AreEqual("Partitioner doesnt work as expected", expectedPartition
				, kfbp.GetPartition(new Text(input), new Text(), numReducers));
			// test with invalid keyspecs
			kfbp = new KeyFieldBasedPartitioner<Text, Text>();
			conf = new Configuration();
			conf.Set(KeyFieldBasedPartitioner.PartitionerOptions, "-k10");
			kfbp.SetConf(conf);
			NUnit.Framework.Assert.AreEqual("Partitioner doesnt work as expected", 0, kfbp.GetPartition
				(new Text(input), new Text(), numReducers));
			// test with multiple keyspecs
			kfbp = new KeyFieldBasedPartitioner<Text, Text>();
			conf = new Configuration();
			conf.Set(KeyFieldBasedPartitioner.PartitionerOptions, "-k2,2 -k4,4");
			kfbp.SetConf(conf);
			input = "abc\tdef\tpqr\txyz";
			expectedOutput = "def";
			eBytes = Sharpen.Runtime.GetBytesForString(expectedOutput);
			hashCode = kfbp.HashCode(eBytes, 0, eBytes.Length - 1, 0);
			expectedOutput = "xyz";
			eBytes = Sharpen.Runtime.GetBytesForString(expectedOutput);
			hashCode = kfbp.HashCode(eBytes, 0, eBytes.Length - 1, hashCode);
			expectedPartition = kfbp.GetPartition(hashCode, numReducers);
			NUnit.Framework.Assert.AreEqual("Partitioner doesnt work as expected", expectedPartition
				, kfbp.GetPartition(new Text(input), new Text(), numReducers));
			// test with invalid start index in keyspecs
			kfbp = new KeyFieldBasedPartitioner<Text, Text>();
			conf = new Configuration();
			conf.Set(KeyFieldBasedPartitioner.PartitionerOptions, "-k2,2 -k30,21 -k4,4 -k5");
			kfbp.SetConf(conf);
			expectedOutput = "def";
			eBytes = Sharpen.Runtime.GetBytesForString(expectedOutput);
			hashCode = kfbp.HashCode(eBytes, 0, eBytes.Length - 1, 0);
			expectedOutput = "xyz";
			eBytes = Sharpen.Runtime.GetBytesForString(expectedOutput);
			hashCode = kfbp.HashCode(eBytes, 0, eBytes.Length - 1, hashCode);
			expectedPartition = kfbp.GetPartition(hashCode, numReducers);
			NUnit.Framework.Assert.AreEqual("Partitioner doesnt work as expected", expectedPartition
				, kfbp.GetPartition(new Text(input), new Text(), numReducers));
		}
	}
}
