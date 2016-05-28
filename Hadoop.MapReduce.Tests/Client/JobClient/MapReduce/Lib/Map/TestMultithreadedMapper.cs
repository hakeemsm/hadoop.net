using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Map
{
	public class TestMultithreadedMapper : HadoopTestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public TestMultithreadedMapper()
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
			string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp");
			Path inDir = new Path(localPathRoot, "testing/mt/input");
			Path outDir = new Path(localPathRoot, "testing/mt/output");
			Configuration conf = CreateJobConf();
			if (ioEx)
			{
				conf.SetBoolean("multithreaded.ioException", true);
			}
			if (rtEx)
			{
				conf.SetBoolean("multithreaded.runtimeException", true);
			}
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, 1, 1);
			job.SetJobName("mt");
			job.SetMapperClass(typeof(MultithreadedMapper));
			MultithreadedMapper.SetMapperClass(job, typeof(TestMultithreadedMapper.IDMap));
			MultithreadedMapper.SetNumberOfThreads(job, 2);
			job.SetReducerClass(typeof(Reducer));
			job.WaitForCompletion(true);
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

			protected override void Setup(Mapper.Context context)
			{
				ioEx = context.GetConfiguration().GetBoolean("multithreaded.ioException", false);
				rtEx = context.GetConfiguration().GetBoolean("multithreaded.runtimeException", false
					);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
				if (ioEx)
				{
					throw new IOException();
				}
				if (rtEx)
				{
					throw new RuntimeException();
				}
				base.Map(key, value, context);
			}
		}
	}
}
