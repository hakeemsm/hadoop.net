using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	public class TestMRMultipleOutputs : HadoopTestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public TestMRMultipleOutputs()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 1, 1)
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWithoutCounters()
		{
			_testMultipleOutputs(false);
			_testMOWithJavaSerialization(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWithCounters()
		{
			_testMultipleOutputs(true);
			_testMOWithJavaSerialization(true);
		}

		private static string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp"
			);

		private static readonly Path RootDir = new Path(localPathRoot, "testing/mo");

		private static readonly Path InDir = new Path(RootDir, "input");

		private static readonly Path OutDir = new Path(RootDir, "output");

		private static string Text = "text";

		private static string Sequence = "sequence";

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			base.SetUp();
			Configuration conf = CreateJobConf();
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(RootDir, true);
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			Configuration conf = CreateJobConf();
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(RootDir, true);
			base.TearDown();
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void _testMOWithJavaSerialization(bool withCounters)
		{
			string input = "a\nb\nc\nd\ne\nc\nd\ne";
			Configuration conf = CreateJobConf();
			conf.Set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
				 + "org.apache.hadoop.io.serializer.WritableSerialization");
			Job job = MapReduceTestUtil.CreateJob(conf, InDir, OutDir, 2, 1, input);
			job.SetJobName("mo");
			MultipleOutputs.AddNamedOutput(job, Text, typeof(TextOutputFormat), typeof(long), 
				typeof(string));
			MultipleOutputs.SetCountersEnabled(job, withCounters);
			job.SetSortComparatorClass(typeof(JavaSerializationComparator));
			job.SetMapOutputKeyClass(typeof(long));
			job.SetMapOutputValueClass(typeof(string));
			job.SetOutputKeyClass(typeof(long));
			job.SetOutputValueClass(typeof(string));
			job.SetMapperClass(typeof(TestMRMultipleOutputs.MOJavaSerDeMap));
			job.SetReducerClass(typeof(TestMRMultipleOutputs.MOJavaSerDeReduce));
			job.WaitForCompletion(true);
			// assert number of named output part files
			int namedOutputCount = 0;
			int valueBasedOutputCount = 0;
			FileSystem fs = OutDir.GetFileSystem(conf);
			FileStatus[] statuses = fs.ListStatus(OutDir);
			foreach (FileStatus status in statuses)
			{
				string fileName = status.GetPath().GetName();
				if (fileName.Equals("text-m-00000") || fileName.Equals("text-m-00001") || fileName
					.Equals("text-r-00000"))
				{
					namedOutputCount++;
				}
				else
				{
					if (fileName.Equals("a-r-00000") || fileName.Equals("b-r-00000") || fileName.Equals
						("c-r-00000") || fileName.Equals("d-r-00000") || fileName.Equals("e-r-00000"))
					{
						valueBasedOutputCount++;
					}
				}
			}
			NUnit.Framework.Assert.AreEqual(3, namedOutputCount);
			NUnit.Framework.Assert.AreEqual(5, valueBasedOutputCount);
			// assert TextOutputFormat files correctness
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.Open(new Path
				(FileOutputFormat.GetOutputPath(job), "text-r-00000"))));
			int count = 0;
			string line = reader.ReadLine();
			while (line != null)
			{
				NUnit.Framework.Assert.IsTrue(line.EndsWith(Text));
				line = reader.ReadLine();
				count++;
			}
			reader.Close();
			NUnit.Framework.Assert.IsFalse(count == 0);
			if (withCounters)
			{
				CounterGroup counters = job.GetCounters().GetGroup(typeof(MultipleOutputs).FullName
					);
				NUnit.Framework.Assert.AreEqual(6, counters.Size());
				NUnit.Framework.Assert.AreEqual(4, counters.FindCounter(Text).GetValue());
				NUnit.Framework.Assert.AreEqual(2, counters.FindCounter("a").GetValue());
				NUnit.Framework.Assert.AreEqual(2, counters.FindCounter("b").GetValue());
				NUnit.Framework.Assert.AreEqual(4, counters.FindCounter("c").GetValue());
				NUnit.Framework.Assert.AreEqual(4, counters.FindCounter("d").GetValue());
				NUnit.Framework.Assert.AreEqual(4, counters.FindCounter("e").GetValue());
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void _testMultipleOutputs(bool withCounters)
		{
			string input = "a\nb\nc\nd\ne\nc\nd\ne";
			Configuration conf = CreateJobConf();
			Job job = MapReduceTestUtil.CreateJob(conf, InDir, OutDir, 2, 1, input);
			job.SetJobName("mo");
			MultipleOutputs.AddNamedOutput(job, Text, typeof(TextOutputFormat), typeof(LongWritable
				), typeof(Text));
			MultipleOutputs.AddNamedOutput(job, Sequence, typeof(SequenceFileOutputFormat), typeof(
				IntWritable), typeof(Text));
			MultipleOutputs.SetCountersEnabled(job, withCounters);
			job.SetMapperClass(typeof(TestMRMultipleOutputs.MOMap));
			job.SetReducerClass(typeof(TestMRMultipleOutputs.MOReduce));
			job.WaitForCompletion(true);
			// assert number of named output part files
			int namedOutputCount = 0;
			int valueBasedOutputCount = 0;
			FileSystem fs = OutDir.GetFileSystem(conf);
			FileStatus[] statuses = fs.ListStatus(OutDir);
			foreach (FileStatus status in statuses)
			{
				string fileName = status.GetPath().GetName();
				if (fileName.Equals("text-m-00000") || fileName.Equals("text-m-00001") || fileName
					.Equals("text-r-00000") || fileName.Equals("sequence_A-m-00000") || fileName.Equals
					("sequence_A-m-00001") || fileName.Equals("sequence_B-m-00000") || fileName.Equals
					("sequence_B-m-00001") || fileName.Equals("sequence_B-r-00000") || fileName.Equals
					("sequence_C-r-00000"))
				{
					namedOutputCount++;
				}
				else
				{
					if (fileName.Equals("a-r-00000") || fileName.Equals("b-r-00000") || fileName.Equals
						("c-r-00000") || fileName.Equals("d-r-00000") || fileName.Equals("e-r-00000"))
					{
						valueBasedOutputCount++;
					}
				}
			}
			NUnit.Framework.Assert.AreEqual(9, namedOutputCount);
			NUnit.Framework.Assert.AreEqual(5, valueBasedOutputCount);
			// assert TextOutputFormat files correctness
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.Open(new Path
				(FileOutputFormat.GetOutputPath(job), "text-r-00000"))));
			int count = 0;
			string line = reader.ReadLine();
			while (line != null)
			{
				NUnit.Framework.Assert.IsTrue(line.EndsWith(Text));
				line = reader.ReadLine();
				count++;
			}
			reader.Close();
			NUnit.Framework.Assert.IsFalse(count == 0);
			// assert SequenceOutputFormat files correctness
			SequenceFile.Reader seqReader = new SequenceFile.Reader(fs, new Path(FileOutputFormat
				.GetOutputPath(job), "sequence_B-r-00000"), conf);
			NUnit.Framework.Assert.AreEqual(typeof(IntWritable), seqReader.GetKeyClass());
			NUnit.Framework.Assert.AreEqual(typeof(Text), seqReader.GetValueClass());
			count = 0;
			IntWritable key = new IntWritable();
			Text value = new Text();
			while (seqReader.Next(key, value))
			{
				NUnit.Framework.Assert.AreEqual(Sequence, value.ToString());
				count++;
			}
			seqReader.Close();
			NUnit.Framework.Assert.IsFalse(count == 0);
			if (withCounters)
			{
				CounterGroup counters = job.GetCounters().GetGroup(typeof(MultipleOutputs).FullName
					);
				NUnit.Framework.Assert.AreEqual(9, counters.Size());
				NUnit.Framework.Assert.AreEqual(4, counters.FindCounter(Text).GetValue());
				NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(Sequence + "_A").GetValue
					());
				NUnit.Framework.Assert.AreEqual(4, counters.FindCounter(Sequence + "_B").GetValue
					());
				NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(Sequence + "_C").GetValue
					());
				NUnit.Framework.Assert.AreEqual(2, counters.FindCounter("a").GetValue());
				NUnit.Framework.Assert.AreEqual(2, counters.FindCounter("b").GetValue());
				NUnit.Framework.Assert.AreEqual(4, counters.FindCounter("c").GetValue());
				NUnit.Framework.Assert.AreEqual(4, counters.FindCounter("d").GetValue());
				NUnit.Framework.Assert.AreEqual(4, counters.FindCounter("e").GetValue());
			}
		}

		public class MOMap : Mapper<LongWritable, Text, LongWritable, Text>
		{
			private MultipleOutputs mos;

			protected override void Setup(Mapper.Context context)
			{
				mos = new MultipleOutputs(context);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
				context.Write(key, value);
				if (value.ToString().Equals("a"))
				{
					mos.Write(Text, key, new Text(Text));
					mos.Write(Sequence, new IntWritable(1), new Text(Sequence), (Sequence + "_A"));
					mos.Write(Sequence, new IntWritable(2), new Text(Sequence), (Sequence + "_B"));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Mapper.Context context)
			{
				mos.Close();
			}
		}

		public class MOReduce : Reducer<LongWritable, Text, LongWritable, Text>
		{
			private MultipleOutputs mos;

			protected override void Setup(Reducer.Context context)
			{
				mos = new MultipleOutputs(context);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(LongWritable key, IEnumerable<Text> values, Reducer.Context
				 context)
			{
				foreach (Text value in values)
				{
					mos.Write(key, value, value.ToString());
					if (!value.ToString().Equals("b"))
					{
						context.Write(key, value);
					}
					else
					{
						mos.Write(Text, key, new Text(Text));
						mos.Write(Sequence, new IntWritable(2), new Text(Sequence), (Sequence + "_B"));
						mos.Write(Sequence, new IntWritable(3), new Text(Sequence), (Sequence + "_C"));
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Reducer.Context context)
			{
				mos.Close();
			}
		}

		public class MOJavaSerDeMap : Mapper<LongWritable, Text, long, string>
		{
			private MultipleOutputs<long, string> mos;

			protected override void Setup(Mapper.Context context)
			{
				mos = new MultipleOutputs<long, string>(context);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
				context.Write(key.Get(), value.ToString());
				if (value.ToString().Equals("a"))
				{
					mos.Write(Text, key.Get(), Text);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Mapper.Context context)
			{
				mos.Close();
			}
		}

		public class MOJavaSerDeReduce : Reducer<long, string, long, string>
		{
			private MultipleOutputs<long, string> mos;

			protected override void Setup(Reducer.Context context)
			{
				mos = new MultipleOutputs<long, string>(context);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(long key, IEnumerable<string> values, Reducer.Context
				 context)
			{
				foreach (string value in values)
				{
					mos.Write(key, value, value.ToString());
					if (!value.ToString().Equals("b"))
					{
						context.Write(key, value);
					}
					else
					{
						mos.Write(Text, key, new Text(Text));
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Reducer.Context context)
			{
				mos.Close();
			}
		}
	}
}
