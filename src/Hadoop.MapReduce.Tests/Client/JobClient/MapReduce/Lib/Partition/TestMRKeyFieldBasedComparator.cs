using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	public class TestMRKeyFieldBasedComparator : HadoopTestCase
	{
		internal Configuration conf;

		internal string line1 = "123 -123 005120 123.9 0.01 0.18 010 10.0 4444.1 011 011 234";

		internal string line2 = "134 -12 005100 123.10 -1.01 0.19 02 10.1 4444";

		/// <exception cref="System.IO.IOException"/>
		public TestMRKeyFieldBasedComparator()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 1, 1)
		{
			line1_bytes = Sharpen.Runtime.GetBytesForString(line1);
			line2_bytes = Sharpen.Runtime.GetBytesForString(line2);
			conf = CreateJobConf();
			conf.Set(MRJobConfig.MapOutputKeyFieldSeperator, " ");
		}

		/// <exception cref="System.Exception"/>
		private void TestComparator(string keySpec, int expect)
		{
			string root = Runtime.GetProperty("test.build.data", "/tmp");
			Path inDir = new Path(root, "test_cmp/in");
			Path outDir = new Path(root, "test_cmp/out");
			conf.Set("mapreduce.partition.keycomparator.options", keySpec);
			conf.Set("mapreduce.partition.keypartitioner.options", "-k1.1,1.1");
			conf.Set(MRJobConfig.MapOutputKeyFieldSeperator, " ");
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, 1, 1, line1 + "\n" + line2
				 + "\n");
			job.SetMapperClass(typeof(InverseMapper));
			job.SetReducerClass(typeof(Reducer));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(LongWritable));
			job.SetSortComparatorClass(typeof(KeyFieldBasedComparator));
			job.SetPartitionerClass(typeof(KeyFieldBasedPartitioner));
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(job.IsSuccessful());
			// validate output
			Path[] outputFiles = FileUtil.Stat2Paths(GetFileSystem().ListStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter
				()));
			if (outputFiles.Length > 0)
			{
				InputStream @is = GetFileSystem().Open(outputFiles[0]);
				BufferedReader reader = new BufferedReader(new InputStreamReader(@is));
				string line = reader.ReadLine();
				//make sure we get what we expect as the first line, and also
				//that we have two lines (both the lines must end up in the same
				//reducer since the partitioner takes the same key spec for all
				//lines
				if (expect == 1)
				{
					NUnit.Framework.Assert.IsTrue(line.StartsWith(line1));
				}
				else
				{
					if (expect == 2)
					{
						NUnit.Framework.Assert.IsTrue(line.StartsWith(line2));
					}
				}
				line = reader.ReadLine();
				if (expect == 1)
				{
					NUnit.Framework.Assert.IsTrue(line.StartsWith(line2));
				}
				else
				{
					if (expect == 2)
					{
						NUnit.Framework.Assert.IsTrue(line.StartsWith(line1));
					}
				}
				reader.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBasicUnixComparator()
		{
			TestComparator("-k1,1n", 1);
			TestComparator("-k2,2n", 1);
			TestComparator("-k2.2,2n", 2);
			TestComparator("-k3.4,3n", 2);
			TestComparator("-k3.2,3.3n -k4,4n", 2);
			TestComparator("-k3.2,3.3n -k4,4nr", 1);
			TestComparator("-k2.4,2.4n", 2);
			TestComparator("-k7,7", 1);
			TestComparator("-k7,7n", 2);
			TestComparator("-k8,8n", 1);
			TestComparator("-k9,9", 2);
			TestComparator("-k11,11", 2);
			TestComparator("-k10,10", 2);
			TestWithoutMRJob("-k9,9", 1);
			TestWithoutMRJob("-k9n", 1);
		}

		internal byte[] line1_bytes;

		internal byte[] line2_bytes;

		/// <exception cref="System.Exception"/>
		public virtual void TestWithoutMRJob(string keySpec, int expect)
		{
			KeyFieldBasedComparator<Void, Void> keyFieldCmp = new KeyFieldBasedComparator<Void
				, Void>();
			conf.Set("mapreduce.partition.keycomparator.options", keySpec);
			keyFieldCmp.SetConf(conf);
			int result = keyFieldCmp.Compare(line1_bytes, 0, line1_bytes.Length, line2_bytes, 
				0, line2_bytes.Length);
			if ((expect >= 0 && result < 0) || (expect < 0 && result >= 0))
			{
				Fail();
			}
		}
	}
}
