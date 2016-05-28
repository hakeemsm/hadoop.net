using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	public class TestTotalOrderPartitioner : TestCase
	{
		private static readonly Text[] splitStrings = new Text[] { new Text("aabbb"), new 
			Text("babbb"), new Text("daddd"), new Text("dddee"), new Text("ddhee"), new Text
			("dingo"), new Text("hijjj"), new Text("n"), new Text("yak") };

		internal class Check<T>
		{
			internal T data;

			internal int part;

			internal Check(T data, int part)
			{
				// -inf            // 0
				// 1
				// 2
				// 3
				// 4
				// 5
				// 6
				// 7
				// 8
				// 9
				this.data = data;
				this.part = part;
			}
		}

		private static readonly AList<TestTotalOrderPartitioner.Check<Text>> testStrings = 
			new AList<TestTotalOrderPartitioner.Check<Text>>();

		static TestTotalOrderPartitioner()
		{
			testStrings.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("aaaaa"), 
				0));
			testStrings.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("aaabb"), 
				0));
			testStrings.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("aabbb"), 
				1));
			testStrings.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("aaaaa"), 
				0));
			testStrings.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("babbb"), 
				2));
			testStrings.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("baabb"), 
				1));
			testStrings.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("yai"), 8)
				);
			testStrings.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("yak"), 9)
				);
			testStrings.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("z"), 9));
			testStrings.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("ddngo"), 
				5));
			testStrings.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("hi"), 6));
		}

		/// <exception cref="System.IO.IOException"/>
		private static Path WritePartitionFile<T>(string testname, Configuration conf, T[]
			 splits)
			where T : WritableComparable<object>
		{
			FileSystem fs = FileSystem.GetLocal(conf);
			Path testdir = new Path(Runtime.GetProperty("test.build.data", "/tmp")).MakeQualified
				(fs);
			Path p = new Path(testdir, testname + "/_partition.lst");
			TotalOrderPartitioner.SetPartitionFile(conf, p);
			conf.SetInt(MRJobConfig.NumReduces, splits.Length + 1);
			SequenceFile.Writer w = null;
			try
			{
				w = SequenceFile.CreateWriter(fs, conf, p, splits[0].GetType(), typeof(NullWritable
					), SequenceFile.CompressionType.None);
				for (int i = 0; i < splits.Length; ++i)
				{
					w.Append(splits[i], NullWritable.Get());
				}
			}
			finally
			{
				if (null != w)
				{
					w.Close();
				}
			}
			return p;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTotalOrderMemCmp()
		{
			TotalOrderPartitioner<Text, NullWritable> partitioner = new TotalOrderPartitioner
				<Text, NullWritable>();
			Configuration conf = new Configuration();
			Path p = TestTotalOrderPartitioner.WritePartitionFile<Text>("totalordermemcmp", conf
				, splitStrings);
			conf.SetClass(MRJobConfig.MapOutputKeyClass, typeof(Text), typeof(object));
			try
			{
				partitioner.SetConf(conf);
				NullWritable nw = NullWritable.Get();
				foreach (TestTotalOrderPartitioner.Check<Text> chk in testStrings)
				{
					NUnit.Framework.Assert.AreEqual(chk.data.ToString(), chk.part, partitioner.GetPartition
						(chk.data, nw, splitStrings.Length + 1));
				}
			}
			finally
			{
				p.GetFileSystem(conf).Delete(p, true);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTotalOrderBinarySearch()
		{
			TotalOrderPartitioner<Text, NullWritable> partitioner = new TotalOrderPartitioner
				<Text, NullWritable>();
			Configuration conf = new Configuration();
			Path p = TestTotalOrderPartitioner.WritePartitionFile<Text>("totalorderbinarysearch"
				, conf, splitStrings);
			conf.SetBoolean(TotalOrderPartitioner.NaturalOrder, false);
			conf.SetClass(MRJobConfig.MapOutputKeyClass, typeof(Text), typeof(object));
			try
			{
				partitioner.SetConf(conf);
				NullWritable nw = NullWritable.Get();
				foreach (TestTotalOrderPartitioner.Check<Text> chk in testStrings)
				{
					NUnit.Framework.Assert.AreEqual(chk.data.ToString(), chk.part, partitioner.GetPartition
						(chk.data, nw, splitStrings.Length + 1));
				}
			}
			finally
			{
				p.GetFileSystem(conf).Delete(p, true);
			}
		}

		public class ReverseStringComparator : RawComparator<Text>
		{
			public virtual int Compare(Text a, Text b)
			{
				return -a.CompareTo(b);
			}

			public virtual int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				int n1 = WritableUtils.DecodeVIntSize(b1[s1]);
				int n2 = WritableUtils.DecodeVIntSize(b2[s2]);
				return -1 * WritableComparator.CompareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2
					 - n2);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTotalOrderCustomComparator()
		{
			TotalOrderPartitioner<Text, NullWritable> partitioner = new TotalOrderPartitioner
				<Text, NullWritable>();
			Configuration conf = new Configuration();
			Text[] revSplitStrings = Arrays.CopyOf(splitStrings, splitStrings.Length);
			Arrays.Sort(revSplitStrings, new TestTotalOrderPartitioner.ReverseStringComparator
				());
			Path p = TestTotalOrderPartitioner.WritePartitionFile<Text>("totalordercustomcomparator"
				, conf, revSplitStrings);
			conf.SetBoolean(TotalOrderPartitioner.NaturalOrder, false);
			conf.SetClass(MRJobConfig.MapOutputKeyClass, typeof(Text), typeof(object));
			conf.SetClass(MRJobConfig.KeyComparator, typeof(TestTotalOrderPartitioner.ReverseStringComparator
				), typeof(RawComparator));
			AList<TestTotalOrderPartitioner.Check<Text>> revCheck = new AList<TestTotalOrderPartitioner.Check
				<Text>>();
			revCheck.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("aaaaa"), 9));
			revCheck.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("aaabb"), 9));
			revCheck.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("aabbb"), 9));
			revCheck.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("aaaaa"), 9));
			revCheck.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("babbb"), 8));
			revCheck.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("baabb"), 8));
			revCheck.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("yai"), 1));
			revCheck.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("yak"), 1));
			revCheck.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("z"), 0));
			revCheck.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("ddngo"), 4));
			revCheck.AddItem(new TestTotalOrderPartitioner.Check<Text>(new Text("hi"), 3));
			try
			{
				partitioner.SetConf(conf);
				NullWritable nw = NullWritable.Get();
				foreach (TestTotalOrderPartitioner.Check<Text> chk in revCheck)
				{
					NUnit.Framework.Assert.AreEqual(chk.data.ToString(), chk.part, partitioner.GetPartition
						(chk.data, nw, splitStrings.Length + 1));
				}
			}
			finally
			{
				p.GetFileSystem(conf).Delete(p, true);
			}
		}
	}
}
