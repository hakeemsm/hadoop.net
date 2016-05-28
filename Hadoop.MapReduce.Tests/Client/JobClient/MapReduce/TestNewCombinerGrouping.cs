using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestNewCombinerGrouping
	{
		private static string TestRootDir = new FilePath("build", UUID.RandomUUID().ToString
			()).GetAbsolutePath();

		public class Map : Mapper<LongWritable, Text, Text, LongWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
				string v = value.ToString();
				string k = Sharpen.Runtime.Substring(v, 0, v.IndexOf(","));
				v = Sharpen.Runtime.Substring(v, v.IndexOf(",") + 1);
				context.Write(new Text(k), new LongWritable(long.Parse(v)));
			}
		}

		public class Reduce : Reducer<Text, LongWritable, Text, LongWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(Text key, IEnumerable<LongWritable> values, Reducer.Context
				 context)
			{
				LongWritable maxValue = null;
				foreach (LongWritable value in values)
				{
					if (maxValue == null)
					{
						maxValue = value;
					}
					else
					{
						if (value.CompareTo(maxValue) > 0)
						{
							maxValue = value;
						}
					}
				}
				context.Write(key, maxValue);
			}
		}

		public class Combiner : TestNewCombinerGrouping.Reduce
		{
		}

		public class GroupComparator : RawComparator<Text>
		{
			public virtual int Compare(byte[] bytes, int i, int i2, byte[] bytes2, int i3, int
				 i4)
			{
				byte[] b1 = new byte[i2];
				System.Array.Copy(bytes, i, b1, 0, i2);
				byte[] b2 = new byte[i4];
				System.Array.Copy(bytes2, i3, b2, 0, i4);
				return Compare(new Text(Sharpen.Runtime.GetStringForBytes(b1)), new Text(Sharpen.Runtime.GetStringForBytes
					(b2)));
			}

			public virtual int Compare(Text o1, Text o2)
			{
				string s1 = o1.ToString();
				string s2 = o2.ToString();
				s1 = Sharpen.Runtime.Substring(s1, 0, s1.IndexOf("|"));
				s2 = Sharpen.Runtime.Substring(s2, 0, s2.IndexOf("|"));
				return string.CompareOrdinal(s1, s2);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCombiner()
		{
			if (!new FilePath(TestRootDir).Mkdirs())
			{
				throw new RuntimeException("Could not create test dir: " + TestRootDir);
			}
			FilePath @in = new FilePath(TestRootDir, "input");
			if (!@in.Mkdirs())
			{
				throw new RuntimeException("Could not create test dir: " + @in);
			}
			FilePath @out = new FilePath(TestRootDir, "output");
			PrintWriter pw = new PrintWriter(new FileWriter(new FilePath(@in, "data.txt")));
			pw.WriteLine("A|a,1");
			pw.WriteLine("A|b,2");
			pw.WriteLine("B|a,3");
			pw.WriteLine("B|b,4");
			pw.WriteLine("B|c,5");
			pw.Close();
			JobConf conf = new JobConf();
			conf.Set("mapreduce.framework.name", "local");
			Job job = new Job(conf);
			TextInputFormat.SetInputPaths(job, new Path(@in.GetPath()));
			TextOutputFormat.SetOutputPath(job, new Path(@out.GetPath()));
			job.SetMapperClass(typeof(TestNewCombinerGrouping.Map));
			job.SetReducerClass(typeof(TestNewCombinerGrouping.Reduce));
			job.SetInputFormatClass(typeof(TextInputFormat));
			job.SetMapOutputKeyClass(typeof(Text));
			job.SetMapOutputValueClass(typeof(LongWritable));
			job.SetOutputFormatClass(typeof(TextOutputFormat));
			job.SetGroupingComparatorClass(typeof(TestNewCombinerGrouping.GroupComparator));
			job.SetCombinerKeyGroupingComparatorClass(typeof(TestNewCombinerGrouping.GroupComparator
				));
			job.SetCombinerClass(typeof(TestNewCombinerGrouping.Combiner));
			job.GetConfiguration().SetInt("min.num.spills.for.combine", 0);
			job.Submit();
			job.WaitForCompletion(false);
			if (job.IsSuccessful())
			{
				Counters counters = job.GetCounters();
				long combinerInputRecords = counters.FindCounter("org.apache.hadoop.mapreduce.TaskCounter"
					, "COMBINE_INPUT_RECORDS").GetValue();
				long combinerOutputRecords = counters.FindCounter("org.apache.hadoop.mapreduce.TaskCounter"
					, "COMBINE_OUTPUT_RECORDS").GetValue();
				NUnit.Framework.Assert.IsTrue(combinerInputRecords > 0);
				NUnit.Framework.Assert.IsTrue(combinerInputRecords > combinerOutputRecords);
				BufferedReader br = new BufferedReader(new FileReader(new FilePath(@out, "part-r-00000"
					)));
				ICollection<string> output = new HashSet<string>();
				string line = br.ReadLine();
				NUnit.Framework.Assert.IsNotNull(line);
				output.AddItem(Sharpen.Runtime.Substring(line, 0, 1) + Sharpen.Runtime.Substring(
					line, 4, 5));
				line = br.ReadLine();
				NUnit.Framework.Assert.IsNotNull(line);
				output.AddItem(Sharpen.Runtime.Substring(line, 0, 1) + Sharpen.Runtime.Substring(
					line, 4, 5));
				line = br.ReadLine();
				NUnit.Framework.Assert.IsNull(line);
				br.Close();
				ICollection<string> expected = new HashSet<string>();
				expected.AddItem("A2");
				expected.AddItem("B5");
				NUnit.Framework.Assert.AreEqual(expected, output);
			}
			else
			{
				NUnit.Framework.Assert.Fail("Job failed");
			}
		}
	}
}
