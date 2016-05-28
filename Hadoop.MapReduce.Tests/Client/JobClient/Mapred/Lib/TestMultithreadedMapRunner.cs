using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	public class TestMultithreadedMapRunner : HadoopTestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public TestMultithreadedMapRunner()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 1, 1)
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestOKRun()
		{
			Run(false, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIOExRun()
		{
			Run(true, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRuntimeExRun()
		{
			Run(false, true);
		}

		/// <exception cref="System.Exception"/>
		private void Run(bool ioEx, bool rtEx)
		{
			Path inDir = new Path("testing/mt/input");
			Path outDir = new Path("testing/mt/output");
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
			{
				DataOutputStream file = fs.Create(new Path(inDir, "part-0"));
				file.WriteBytes("a\nb\n\nc\nd\ne");
				file.Close();
			}
			conf.SetJobName("mt");
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapOutputKeyClass(typeof(LongWritable));
			conf.SetMapOutputValueClass(typeof(Text));
			conf.SetOutputFormat(typeof(TextOutputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapperClass(typeof(TestMultithreadedMapRunner.IDMap));
			conf.SetReducerClass(typeof(TestMultithreadedMapRunner.IDReduce));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetMapRunnerClass(typeof(MultithreadedMapRunner));
			conf.SetInt(MultithreadedMapper.NumThreads, 2);
			if (ioEx)
			{
				conf.SetBoolean("multithreaded.ioException", true);
			}
			if (rtEx)
			{
				conf.SetBoolean("multithreaded.runtimeException", true);
			}
			JobClient jc = new JobClient(conf);
			RunningJob job = jc.SubmitJob(conf);
			while (!job.IsComplete())
			{
				Sharpen.Thread.Sleep(100);
			}
			if (job.IsSuccessful())
			{
				NUnit.Framework.Assert.IsFalse(ioEx || rtEx);
			}
			else
			{
				NUnit.Framework.Assert.IsTrue(ioEx || rtEx);
			}
		}

		public class IDMap : Mapper<LongWritable, Text, LongWritable, Text>
		{
			private bool ioEx = false;

			private bool rtEx = false;

			public virtual void Configure(JobConf job)
			{
				ioEx = job.GetBoolean("multithreaded.ioException", false);
				rtEx = job.GetBoolean("multithreaded.runtimeException", false);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<LongWritable
				, Text> output, Reporter reporter)
			{
				if (ioEx)
				{
					throw new IOException();
				}
				if (rtEx)
				{
					throw new RuntimeException();
				}
				output.Collect(key, value);
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception ex)
				{
					throw new RuntimeException(ex);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}
		}

		public class IDReduce : Reducer<LongWritable, Text, LongWritable, Text>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(LongWritable key, IEnumerator<Text> values, OutputCollector
				<LongWritable, Text> output, Reporter reporter)
			{
				while (values.HasNext())
				{
					output.Collect(key, values.Next());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}
		}
	}
}
