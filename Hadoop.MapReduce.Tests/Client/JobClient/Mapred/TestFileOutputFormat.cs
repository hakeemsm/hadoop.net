using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestFileOutputFormat : HadoopTestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public TestFileOutputFormat()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 1, 1)
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCustomFile()
		{
			Path inDir = new Path("testing/fileoutputformat/input");
			Path outDir = new Path("testing/fileoutputformat/output");
			// Hack for local FS that does not have the concept of a 'mounting point'
			if (IsLocalFS())
			{
				string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp").Replace(' '
					, '+');
				inDir = new Path(localPathRoot, inDir);
				outDir = new Path(localPathRoot, outDir);
			}
			JobConf conf = CreateJobConf();
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(outDir, true);
			if (!fs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
			DataOutputStream file = fs.Create(new Path(inDir, "part-0"));
			file.WriteBytes("a\nb\n\nc\nd\ne");
			file.Close();
			file = fs.Create(new Path(inDir, "part-1"));
			file.WriteBytes("a\nb\n\nc\nd\ne");
			file.Close();
			conf.SetJobName("fof");
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetMapOutputKeyClass(typeof(LongWritable));
			conf.SetMapOutputValueClass(typeof(Text));
			conf.SetOutputFormat(typeof(TextOutputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapperClass(typeof(TestFileOutputFormat.TestMap));
			conf.SetReducerClass(typeof(TestFileOutputFormat.TestReduce));
			conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			JobClient jc = new JobClient(conf);
			RunningJob job = jc.SubmitJob(conf);
			while (!job.IsComplete())
			{
				Sharpen.Thread.Sleep(100);
			}
			NUnit.Framework.Assert.IsTrue(job.IsSuccessful());
			bool map0 = false;
			bool map1 = false;
			bool reduce = false;
			FileStatus[] statuses = fs.ListStatus(outDir);
			foreach (FileStatus status in statuses)
			{
				map0 = map0 || status.GetPath().GetName().Equals("test-m-00000");
				map1 = map1 || status.GetPath().GetName().Equals("test-m-00001");
				reduce = reduce || status.GetPath().GetName().Equals("test-r-00000");
			}
			NUnit.Framework.Assert.IsTrue(map0);
			NUnit.Framework.Assert.IsTrue(map1);
			NUnit.Framework.Assert.IsTrue(reduce);
		}

		public class TestMap : Mapper<LongWritable, Text, LongWritable, Text>
		{
			public virtual void Configure(JobConf conf)
			{
				try
				{
					FileSystem fs = FileSystem.Get(conf);
					OutputStream os = fs.Create(FileOutputFormat.GetPathForCustomFile(conf, "test"));
					os.Write(1);
					os.Close();
				}
				catch (IOException ex)
				{
					throw new RuntimeException(ex);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<LongWritable
				, Text> output, Reporter reporter)
			{
				output.Collect(key, value);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}
		}

		public class TestReduce : Reducer<LongWritable, Text, LongWritable, Text>
		{
			public virtual void Configure(JobConf conf)
			{
				try
				{
					FileSystem fs = FileSystem.Get(conf);
					OutputStream os = fs.Create(FileOutputFormat.GetPathForCustomFile(conf, "test"));
					os.Write(1);
					os.Close();
				}
				catch (IOException ex)
				{
					throw new RuntimeException(ex);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(LongWritable key, IEnumerator<Text> values, OutputCollector
				<LongWritable, Text> output, Reporter reporter)
			{
				while (values.HasNext())
				{
					Text value = values.Next();
					output.Collect(key, value);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}
		}
	}
}
