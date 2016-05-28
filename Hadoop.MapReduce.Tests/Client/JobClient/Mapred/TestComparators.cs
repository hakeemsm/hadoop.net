using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Two different types of comparators can be used in MapReduce.</summary>
	/// <remarks>
	/// Two different types of comparators can be used in MapReduce. One is used
	/// during the Map and Reduce phases, to sort/merge key-value pairs. Another
	/// is used to group values for a particular key, when calling the user's
	/// reducer. A user can override both of these two.
	/// This class has tests for making sure we use the right comparators at the
	/// right places. See Hadoop issues 485 and 1535. Our tests:
	/// 1. Test that the same comparator is used for all sort/merge operations
	/// during the Map and Reduce phases.
	/// 2. Test the common use case where values are grouped by keys but values
	/// within each key are grouped by a secondary key (a timestamp, for example).
	/// </remarks>
	public class TestComparators
	{
		private static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), "TestComparators-mapred");

		internal JobConf conf = new JobConf(typeof(TestMapOutputType));

		internal JobClient jc;

		internal static Random rng = new Random();

		/// <summary>
		/// RandomGen is a mapper that generates 5 random values for each key
		/// in the input.
		/// </summary>
		/// <remarks>
		/// RandomGen is a mapper that generates 5 random values for each key
		/// in the input. The values are in the range [0-4]. The mapper also
		/// generates a composite key. If the input key is x and the generated
		/// value is y, the composite key is x0y (x-zero-y). Therefore, the inter-
		/// mediate key value pairs are ordered by {input key, value}.
		/// Think of the random value as a timestamp associated with the record.
		/// </remarks>
		internal class RandomGenMapper : Mapper<IntWritable, Writable, IntWritable, IntWritable
			>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(IntWritable key, Writable value, OutputCollector<IntWritable
				, IntWritable> @out, Reporter reporter)
			{
				int num_values = 5;
				for (int i = 0; i < num_values; ++i)
				{
					int val = rng.Next(num_values);
					int compositeKey = key.Get() * 100 + val;
					@out.Collect(new IntWritable(compositeKey), new IntWritable(val));
				}
			}

			public virtual void Close()
			{
			}
		}

		/// <summary>Your basic identity mapper.</summary>
		internal class IdentityMapper : Mapper<WritableComparable, Writable, WritableComparable
			, Writable>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(WritableComparable key, Writable value, OutputCollector<WritableComparable
				, Writable> @out, Reporter reporter)
			{
				@out.Collect(key, value);
			}

			public virtual void Close()
			{
			}
		}

		/// <summary>Checks whether keys are in ascending order.</summary>
		internal class AscendingKeysReducer : Reducer<IntWritable, Writable, IntWritable, 
			Text>
		{
			public virtual void Configure(JobConf job)
			{
			}

			private int lastKey = int.MinValue;

			// keep track of the last key we've seen
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(IntWritable key, IEnumerator<Writable> values, OutputCollector
				<IntWritable, Text> @out, Reporter reporter)
			{
				int currentKey = key.Get();
				// keys should be in ascending order
				if (currentKey < lastKey)
				{
					NUnit.Framework.Assert.Fail("Keys not in sorted ascending order");
				}
				lastKey = currentKey;
				@out.Collect(key, new Text("success"));
			}

			public virtual void Close()
			{
			}
		}

		/// <summary>Checks whether keys are in ascending order.</summary>
		internal class DescendingKeysReducer : Reducer<IntWritable, Writable, IntWritable
			, Text>
		{
			public virtual void Configure(JobConf job)
			{
			}

			private int lastKey = int.MaxValue;

			// keep track of the last key we've seen
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(IntWritable key, IEnumerator<Writable> values, OutputCollector
				<IntWritable, Text> @out, Reporter reporter)
			{
				int currentKey = key.Get();
				// keys should be in descending order
				if (currentKey > lastKey)
				{
					NUnit.Framework.Assert.Fail("Keys not in sorted descending order");
				}
				lastKey = currentKey;
				@out.Collect(key, new Text("success"));
			}

			public virtual void Close()
			{
			}
		}

		/// <summary>
		/// The reducer checks whether the input values are in ascending order and
		/// whether they are correctly grouped by key (i.e.
		/// </summary>
		/// <remarks>
		/// The reducer checks whether the input values are in ascending order and
		/// whether they are correctly grouped by key (i.e. each call to reduce
		/// should have 5 values if the grouping is correct). It also checks whether
		/// the keys themselves are in ascending order.
		/// </remarks>
		internal class AscendingGroupReducer : Reducer<IntWritable, IntWritable, IntWritable
			, Text>
		{
			public virtual void Configure(JobConf job)
			{
			}

			private int lastKey = int.MinValue;

			// keep track of the last key we've seen
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(IntWritable key, IEnumerator<IntWritable> values, OutputCollector
				<IntWritable, Text> @out, Reporter reporter)
			{
				// check key order
				int currentKey = key.Get();
				if (currentKey < lastKey)
				{
					NUnit.Framework.Assert.Fail("Keys not in sorted ascending order");
				}
				lastKey = currentKey;
				// check order of values
				IntWritable previous = new IntWritable(int.MinValue);
				int valueCount = 0;
				while (values.HasNext())
				{
					IntWritable current = values.Next();
					// Check that the values are sorted
					if (current.CompareTo(previous) < 0)
					{
						NUnit.Framework.Assert.Fail("Values generated by Mapper not in order");
					}
					previous = current;
					++valueCount;
				}
				if (valueCount != 5)
				{
					NUnit.Framework.Assert.Fail("Values not grouped by primary key");
				}
				@out.Collect(key, new Text("success"));
			}

			public virtual void Close()
			{
			}
		}

		/// <summary>
		/// The reducer checks whether the input values are in descending order and
		/// whether they are correctly grouped by key (i.e.
		/// </summary>
		/// <remarks>
		/// The reducer checks whether the input values are in descending order and
		/// whether they are correctly grouped by key (i.e. each call to reduce
		/// should have 5 values if the grouping is correct).
		/// </remarks>
		internal class DescendingGroupReducer : Reducer<IntWritable, IntWritable, IntWritable
			, Text>
		{
			public virtual void Configure(JobConf job)
			{
			}

			private int lastKey = int.MaxValue;

			// keep track of the last key we've seen
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(IntWritable key, IEnumerator<IntWritable> values, OutputCollector
				<IntWritable, Text> @out, Reporter reporter)
			{
				// check key order
				int currentKey = key.Get();
				if (currentKey > lastKey)
				{
					NUnit.Framework.Assert.Fail("Keys not in sorted descending order");
				}
				lastKey = currentKey;
				// check order of values
				IntWritable previous = new IntWritable(int.MaxValue);
				int valueCount = 0;
				while (values.HasNext())
				{
					IntWritable current = values.Next();
					// Check that the values are sorted
					if (current.CompareTo(previous) > 0)
					{
						NUnit.Framework.Assert.Fail("Values generated by Mapper not in order");
					}
					previous = current;
					++valueCount;
				}
				if (valueCount != 5)
				{
					NUnit.Framework.Assert.Fail("Values not grouped by primary key");
				}
				@out.Collect(key, new Text("success"));
			}

			public virtual void Close()
			{
			}
		}

		/// <summary>A decreasing Comparator for IntWritable</summary>
		public class DecreasingIntComparator : IntWritable.Comparator
		{
			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return -base.Compare(b1, s1, l1, b2, s2, l2);
			}

			static DecreasingIntComparator()
			{
				// register this comparator
				WritableComparator.Define(typeof(TestComparators.DecreasingIntComparator), new IntWritable.Comparator
					());
			}
		}

		/// <summary>Grouping function for values based on the composite key.</summary>
		/// <remarks>
		/// Grouping function for values based on the composite key. This
		/// comparator strips off the secondary key part from the x0y composite
		/// and only compares the primary key value (x).
		/// </remarks>
		public class CompositeIntGroupFn : WritableComparator
		{
			public CompositeIntGroupFn()
				: base(typeof(IntWritable))
			{
			}

			public override int Compare(WritableComparable v1, WritableComparable v2)
			{
				int val1 = ((IntWritable)(v1)).Get() / 100;
				int val2 = ((IntWritable)(v2)).Get() / 100;
				if (val1 < val2)
				{
					return 1;
				}
				else
				{
					if (val1 > val2)
					{
						return -1;
					}
					else
					{
						return 0;
					}
				}
			}

			public virtual bool Equals(IntWritable v1, IntWritable v2)
			{
				int val1 = v1.Get();
				int val2 = v2.Get();
				return (val1 / 100) == (val2 / 100);
			}

			static CompositeIntGroupFn()
			{
				WritableComparator.Define(typeof(TestComparators.CompositeIntGroupFn), new IntWritable.Comparator
					());
			}
		}

		/// <summary>Reverse grouping function for values based on the composite key.</summary>
		public class CompositeIntReverseGroupFn : TestComparators.CompositeIntGroupFn
		{
			public override int Compare(WritableComparable v1, WritableComparable v2)
			{
				return -base.Compare(v1, v2);
			}

			public override bool Equals(IntWritable v1, IntWritable v2)
			{
				return !(base.Equals(v1, v2));
			}

			static CompositeIntReverseGroupFn()
			{
				WritableComparator.Define(typeof(TestComparators.CompositeIntReverseGroupFn), new 
					IntWritable.Comparator());
			}
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Configure()
		{
			Path testdir = new Path(TestDir.GetAbsolutePath());
			Path inDir = new Path(testdir, "in");
			Path outDir = new Path(testdir, "out");
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(testdir, true);
			conf.SetInputFormat(typeof(SequenceFileInputFormat));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetOutputKeyClass(typeof(IntWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapOutputValueClass(typeof(IntWritable));
			// set up two map jobs, so we can test merge phase in Reduce also
			conf.SetNumMapTasks(2);
			conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
			conf.SetOutputFormat(typeof(SequenceFileOutputFormat));
			if (!fs.Mkdirs(testdir))
			{
				throw new IOException("Mkdirs failed to create " + testdir.ToString());
			}
			if (!fs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
			// set up input data in 2 files 
			Path inFile = new Path(inDir, "part0");
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, inFile, typeof(IntWritable
				), typeof(IntWritable));
			writer.Append(new IntWritable(11), new IntWritable(999));
			writer.Append(new IntWritable(23), new IntWritable(456));
			writer.Append(new IntWritable(10), new IntWritable(780));
			writer.Close();
			inFile = new Path(inDir, "part1");
			writer = SequenceFile.CreateWriter(fs, conf, inFile, typeof(IntWritable), typeof(
				IntWritable));
			writer.Append(new IntWritable(45), new IntWritable(100));
			writer.Append(new IntWritable(18), new IntWritable(200));
			writer.Append(new IntWritable(27), new IntWritable(300));
			writer.Close();
			jc = new JobClient(conf);
		}

		[TearDown]
		public virtual void Cleanup()
		{
			FileUtil.FullyDelete(TestDir);
		}

		/// <summary>Test the default comparator for Map/Reduce.</summary>
		/// <remarks>
		/// Test the default comparator for Map/Reduce.
		/// Use the identity mapper and see if the keys are sorted at the end
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultMRComparator()
		{
			conf.SetMapperClass(typeof(TestComparators.IdentityMapper));
			conf.SetReducerClass(typeof(TestComparators.AscendingKeysReducer));
			RunningJob r_job = jc.SubmitJob(conf);
			while (!r_job.IsComplete())
			{
				Sharpen.Thread.Sleep(1000);
			}
			if (!r_job.IsSuccessful())
			{
				NUnit.Framework.Assert.Fail("Oops! The job broke due to an unexpected error");
			}
		}

		/// <summary>Test user-defined comparator for Map/Reduce.</summary>
		/// <remarks>
		/// Test user-defined comparator for Map/Reduce.
		/// We provide our own comparator that is the reverse of the default int
		/// comparator. Keys should be sorted in reverse order in the reducer.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUserMRComparator()
		{
			conf.SetMapperClass(typeof(TestComparators.IdentityMapper));
			conf.SetReducerClass(typeof(TestComparators.DescendingKeysReducer));
			conf.SetOutputKeyComparatorClass(typeof(TestComparators.DecreasingIntComparator));
			RunningJob r_job = jc.SubmitJob(conf);
			while (!r_job.IsComplete())
			{
				Sharpen.Thread.Sleep(1000);
			}
			if (!r_job.IsSuccessful())
			{
				NUnit.Framework.Assert.Fail("Oops! The job broke due to an unexpected error");
			}
		}

		/// <summary>Test user-defined grouping comparator for grouping values in Reduce.</summary>
		/// <remarks>
		/// Test user-defined grouping comparator for grouping values in Reduce.
		/// We generate composite keys that contain a random number, which acts
		/// as a timestamp associated with the record. In our Reduce function,
		/// values for a key should be sorted by the 'timestamp'.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUserValueGroupingComparator()
		{
			conf.SetMapperClass(typeof(TestComparators.RandomGenMapper));
			conf.SetReducerClass(typeof(TestComparators.AscendingGroupReducer));
			conf.SetOutputValueGroupingComparator(typeof(TestComparators.CompositeIntGroupFn)
				);
			RunningJob r_job = jc.SubmitJob(conf);
			while (!r_job.IsComplete())
			{
				Sharpen.Thread.Sleep(1000);
			}
			if (!r_job.IsSuccessful())
			{
				NUnit.Framework.Assert.Fail("Oops! The job broke due to an unexpected error");
			}
		}

		/// <summary>Test all user comparators.</summary>
		/// <remarks>
		/// Test all user comparators. Super-test of all tests here.
		/// We generate composite keys that contain a random number, which acts
		/// as a timestamp associated with the record. In our Reduce function,
		/// values for a key should be sorted by the 'timestamp'.
		/// We also provide our own comparators that reverse the default sorting
		/// order. This lets us make sure that the right comparators are used.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllUserComparators()
		{
			conf.SetMapperClass(typeof(TestComparators.RandomGenMapper));
			// use a decreasing comparator so keys are sorted in reverse order
			conf.SetOutputKeyComparatorClass(typeof(TestComparators.DecreasingIntComparator));
			conf.SetReducerClass(typeof(TestComparators.DescendingGroupReducer));
			conf.SetOutputValueGroupingComparator(typeof(TestComparators.CompositeIntReverseGroupFn
				));
			RunningJob r_job = jc.SubmitJob(conf);
			while (!r_job.IsComplete())
			{
				Sharpen.Thread.Sleep(1000);
			}
			if (!r_job.IsSuccessful())
			{
				NUnit.Framework.Assert.Fail("Oops! The job broke due to an unexpected error");
			}
		}

		/// <summary>
		/// Test a user comparator that relies on deserializing both arguments
		/// for each compare.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBakedUserComparator()
		{
			TestComparators.MyWritable a = new TestComparators.MyWritable(8, 8);
			TestComparators.MyWritable b = new TestComparators.MyWritable(7, 9);
			NUnit.Framework.Assert.IsTrue(a.CompareTo(b) > 0);
			NUnit.Framework.Assert.IsTrue(WritableComparator.Get(typeof(TestComparators.MyWritable
				)).Compare(a, b) < 0);
		}

		public class MyWritable : WritableComparable<TestComparators.MyWritable>
		{
			internal int i;

			internal int j;

			public MyWritable()
			{
			}

			public MyWritable(int i, int j)
			{
				this.i = i;
				this.j = j;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				i = @in.ReadInt();
				j = @in.ReadInt();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				@out.WriteInt(i);
				@out.WriteInt(j);
			}

			public virtual int CompareTo(TestComparators.MyWritable b)
			{
				return this.i - b.i;
			}

			static MyWritable()
			{
				WritableComparator.Define(typeof(TestComparators.MyWritable), new TestComparators.MyCmp
					());
			}
		}

		public class MyCmp : WritableComparator
		{
			public MyCmp()
				: base(typeof(TestComparators.MyWritable), true)
			{
			}

			public override int Compare(WritableComparable a, WritableComparable b)
			{
				TestComparators.MyWritable aa = (TestComparators.MyWritable)a;
				TestComparators.MyWritable bb = (TestComparators.MyWritable)b;
				return aa.j - bb.j;
			}
		}
	}
}
