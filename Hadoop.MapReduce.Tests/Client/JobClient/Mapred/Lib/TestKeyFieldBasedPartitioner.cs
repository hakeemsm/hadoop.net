using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	public class TestKeyFieldBasedPartitioner
	{
		/// <summary>Test is key-field-based partitioned works with empty key.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyKey()
		{
			KeyFieldBasedPartitioner<Text, Text> kfbp = new KeyFieldBasedPartitioner<Text, Text
				>();
			JobConf conf = new JobConf();
			conf.SetInt("num.key.fields.for.partition", 10);
			kfbp.Configure(conf);
			NUnit.Framework.Assert.AreEqual("Empty key should map to 0th partition", 0, kfbp.
				GetPartition(new Text(), new Text(), 10));
		}

		[NUnit.Framework.Test]
		public virtual void TestMultiConfigure()
		{
			KeyFieldBasedPartitioner<Text, Text> kfbp = new KeyFieldBasedPartitioner<Text, Text
				>();
			JobConf conf = new JobConf();
			conf.Set(KeyFieldBasedPartitioner.PartitionerOptions, "-k1,1");
			kfbp.SetConf(conf);
			Text key = new Text("foo\tbar");
			Text val = new Text("val");
			int partNum = kfbp.GetPartition(key, val, 4096);
			kfbp.Configure(conf);
			NUnit.Framework.Assert.AreEqual(partNum, kfbp.GetPartition(key, val, 4096));
		}
	}
}
