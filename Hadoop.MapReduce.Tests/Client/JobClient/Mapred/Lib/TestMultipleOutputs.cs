using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	public class TestMultipleOutputs : HadoopTestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public TestMultipleOutputs()
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

		private static readonly Path RootDir = new Path("testing/mo");

		private static readonly Path InDir = new Path(RootDir, "input");

		private static readonly Path OutDir = new Path(RootDir, "output");

		private Path GetDir(Path dir)
		{
			// Hack for local FS that does not have the concept of a 'mounting point'
			if (IsLocalFS())
			{
				string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp").Replace(' '
					, '+');
				dir = new Path(localPathRoot, dir);
			}
			return dir;
		}

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			base.SetUp();
			Path rootDir = GetDir(RootDir);
			Path inDir = GetDir(InDir);
			JobConf conf = CreateJobConf();
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(rootDir, true);
			if (!fs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			Path rootDir = GetDir(RootDir);
			JobConf conf = CreateJobConf();
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(rootDir, true);
			base.TearDown();
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void _testMOWithJavaSerialization(bool withCounters)
		{
			Path inDir = GetDir(InDir);
			Path outDir = GetDir(OutDir);
			JobConf conf = CreateJobConf();
			FileSystem fs = FileSystem.Get(conf);
			DataOutputStream file = fs.Create(new Path(inDir, "part-0"));
			file.WriteBytes("a\nb\n\nc\nd\ne");
			file.Close();
			fs.Delete(inDir, true);
			fs.Delete(outDir, true);
			file = fs.Create(new Path(inDir, "part-1"));
			file.WriteBytes("a\nb\n\nc\nd\ne");
			file.Close();
			conf.SetJobName("mo");
			conf.Set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
				 + "org.apache.hadoop.io.serializer.WritableSerialization");
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetMapOutputKeyClass(typeof(long));
			conf.SetMapOutputValueClass(typeof(string));
			conf.SetOutputKeyComparatorClass(typeof(JavaSerializationComparator));
			conf.SetOutputKeyClass(typeof(long));
			conf.SetOutputValueClass(typeof(string));
			conf.SetOutputFormat(typeof(TextOutputFormat));
			MultipleOutputs.AddNamedOutput(conf, "text", typeof(TextOutputFormat), typeof(long
				), typeof(string));
			MultipleOutputs.SetCountersEnabled(conf, withCounters);
			conf.SetMapperClass(typeof(TestMultipleOutputs.MOJavaSerDeMap));
			conf.SetReducerClass(typeof(TestMultipleOutputs.MOJavaSerDeReduce));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			JobClient jc = new JobClient(conf);
			RunningJob job = jc.SubmitJob(conf);
			while (!job.IsComplete())
			{
				Sharpen.Thread.Sleep(100);
			}
			// assert number of named output part files
			int namedOutputCount = 0;
			FileStatus[] statuses = fs.ListStatus(outDir);
			foreach (FileStatus status in statuses)
			{
				if (status.GetPath().GetName().Equals("text-m-00000") || status.GetPath().GetName
					().Equals("text-r-00000"))
				{
					namedOutputCount++;
				}
			}
			NUnit.Framework.Assert.AreEqual(2, namedOutputCount);
			// assert TextOutputFormat files correctness
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.Open(new Path
				(FileOutputFormat.GetOutputPath(conf), "text-r-00000"))));
			int count = 0;
			string line = reader.ReadLine();
			while (line != null)
			{
				NUnit.Framework.Assert.IsTrue(line.EndsWith("text"));
				line = reader.ReadLine();
				count++;
			}
			reader.Close();
			NUnit.Framework.Assert.IsFalse(count == 0);
			Counters.Group counters = job.GetCounters().GetGroup(typeof(MultipleOutputs).FullName
				);
			if (!withCounters)
			{
				NUnit.Framework.Assert.AreEqual(0, counters.Size());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(1, counters.Size());
				NUnit.Framework.Assert.AreEqual(2, counters.GetCounter("text"));
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void _testMultipleOutputs(bool withCounters)
		{
			Path inDir = GetDir(InDir);
			Path outDir = GetDir(OutDir);
			JobConf conf = CreateJobConf();
			FileSystem fs = FileSystem.Get(conf);
			DataOutputStream file = fs.Create(new Path(inDir, "part-0"));
			file.WriteBytes("a\nb\n\nc\nd\ne");
			file.Close();
			file = fs.Create(new Path(inDir, "part-1"));
			file.WriteBytes("a\nb\n\nc\nd\ne");
			file.Close();
			conf.SetJobName("mo");
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapOutputKeyClass(typeof(LongWritable));
			conf.SetMapOutputValueClass(typeof(Text));
			conf.SetOutputFormat(typeof(TextOutputFormat));
			MultipleOutputs.AddNamedOutput(conf, "text", typeof(TextOutputFormat), typeof(LongWritable
				), typeof(Text));
			MultipleOutputs.AddMultiNamedOutput(conf, "sequence", typeof(SequenceFileOutputFormat
				), typeof(LongWritable), typeof(Text));
			MultipleOutputs.SetCountersEnabled(conf, withCounters);
			conf.SetMapperClass(typeof(TestMultipleOutputs.MOMap));
			conf.SetReducerClass(typeof(TestMultipleOutputs.MOReduce));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			JobClient jc = new JobClient(conf);
			RunningJob job = jc.SubmitJob(conf);
			while (!job.IsComplete())
			{
				Sharpen.Thread.Sleep(100);
			}
			// assert number of named output part files
			int namedOutputCount = 0;
			FileStatus[] statuses = fs.ListStatus(outDir);
			foreach (FileStatus status in statuses)
			{
				if (status.GetPath().GetName().Equals("text-m-00000") || status.GetPath().GetName
					().Equals("text-m-00001") || status.GetPath().GetName().Equals("text-r-00000") ||
					 status.GetPath().GetName().Equals("sequence_A-m-00000") || status.GetPath().GetName
					().Equals("sequence_A-m-00001") || status.GetPath().GetName().Equals("sequence_B-m-00000"
					) || status.GetPath().GetName().Equals("sequence_B-m-00001") || status.GetPath()
					.GetName().Equals("sequence_B-r-00000") || status.GetPath().GetName().Equals("sequence_C-r-00000"
					))
				{
					namedOutputCount++;
				}
			}
			NUnit.Framework.Assert.AreEqual(9, namedOutputCount);
			// assert TextOutputFormat files correctness
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.Open(new Path
				(FileOutputFormat.GetOutputPath(conf), "text-r-00000"))));
			int count = 0;
			string line = reader.ReadLine();
			while (line != null)
			{
				NUnit.Framework.Assert.IsTrue(line.EndsWith("text"));
				line = reader.ReadLine();
				count++;
			}
			reader.Close();
			NUnit.Framework.Assert.IsFalse(count == 0);
			// assert SequenceOutputFormat files correctness
			SequenceFile.Reader seqReader = new SequenceFile.Reader(fs, new Path(FileOutputFormat
				.GetOutputPath(conf), "sequence_B-r-00000"), conf);
			NUnit.Framework.Assert.AreEqual(typeof(LongWritable), seqReader.GetKeyClass());
			NUnit.Framework.Assert.AreEqual(typeof(Text), seqReader.GetValueClass());
			count = 0;
			LongWritable key = new LongWritable();
			Text value = new Text();
			while (seqReader.Next(key, value))
			{
				NUnit.Framework.Assert.AreEqual("sequence", value.ToString());
				count++;
			}
			seqReader.Close();
			NUnit.Framework.Assert.IsFalse(count == 0);
			Counters.Group counters = job.GetCounters().GetGroup(typeof(MultipleOutputs).FullName
				);
			if (!withCounters)
			{
				NUnit.Framework.Assert.AreEqual(0, counters.Size());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(4, counters.Size());
				NUnit.Framework.Assert.AreEqual(4, counters.GetCounter("text"));
				NUnit.Framework.Assert.AreEqual(2, counters.GetCounter("sequence_A"));
				NUnit.Framework.Assert.AreEqual(4, counters.GetCounter("sequence_B"));
				NUnit.Framework.Assert.AreEqual(2, counters.GetCounter("sequence_C"));
			}
		}

		public class MOMap : Mapper<LongWritable, Text, LongWritable, Text>
		{
			private MultipleOutputs mos;

			public virtual void Configure(JobConf conf)
			{
				mos = new MultipleOutputs(conf);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<LongWritable
				, Text> output, Reporter reporter)
			{
				if (!value.ToString().Equals("a"))
				{
					output.Collect(key, value);
				}
				else
				{
					mos.GetCollector("text", reporter).Collect(key, new Text("text"));
					mos.GetCollector("sequence", "A", reporter).Collect(key, new Text("sequence"));
					mos.GetCollector("sequence", "B", reporter).Collect(key, new Text("sequence"));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				mos.Close();
			}
		}

		public class MOReduce : Reducer<LongWritable, Text, LongWritable, Text>
		{
			private MultipleOutputs mos;

			public virtual void Configure(JobConf conf)
			{
				mos = new MultipleOutputs(conf);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(LongWritable key, IEnumerator<Text> values, OutputCollector
				<LongWritable, Text> output, Reporter reporter)
			{
				while (values.HasNext())
				{
					Text value = values.Next();
					if (!value.ToString().Equals("b"))
					{
						output.Collect(key, value);
					}
					else
					{
						mos.GetCollector("text", reporter).Collect(key, new Text("text"));
						mos.GetCollector("sequence", "B", reporter).Collect(key, new Text("sequence"));
						mos.GetCollector("sequence", "C", reporter).Collect(key, new Text("sequence"));
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				mos.Close();
			}
		}

		public class MOJavaSerDeMap : Mapper<LongWritable, Text, long, string>
		{
			private MultipleOutputs mos;

			public virtual void Configure(JobConf conf)
			{
				mos = new MultipleOutputs(conf);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<long, string
				> output, Reporter reporter)
			{
				if (!value.ToString().Equals("a"))
				{
					output.Collect(key.Get(), value.ToString());
				}
				else
				{
					mos.GetCollector("text", reporter).Collect(key, "text");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				mos.Close();
			}
		}

		public class MOJavaSerDeReduce : Reducer<long, string, long, string>
		{
			private MultipleOutputs mos;

			public virtual void Configure(JobConf conf)
			{
				mos = new MultipleOutputs(conf);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(long key, IEnumerator<string> values, OutputCollector<
				long, string> output, Reporter reporter)
			{
				while (values.HasNext())
				{
					string value = values.Next();
					if (!value.Equals("b"))
					{
						output.Collect(key, value);
					}
					else
					{
						mos.GetCollector("text", reporter).Collect(key, "text");
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				mos.Close();
			}
		}
	}
}
