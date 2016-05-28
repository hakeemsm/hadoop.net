using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// TestMapOutputType checks whether the Map task handles type mismatch
	/// between mapper output and the type specified in
	/// JobConf.MapOutputKeyType and JobConf.MapOutputValueType.
	/// </summary>
	public class TestMapOutputType
	{
		private static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), "TestMapOutputType-mapred");

		internal JobConf conf = new JobConf(typeof(TestMapOutputType));

		internal JobClient jc;

		/// <summary>TextGen is a Mapper that generates a Text key-value pair.</summary>
		/// <remarks>
		/// TextGen is a Mapper that generates a Text key-value pair. The
		/// type specified in conf will be anything but.
		/// </remarks>
		internal class TextGen : Mapper<WritableComparable, Writable, Text, Text>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(WritableComparable key, Writable val, OutputCollector<Text
				, Text> @out, Reporter reporter)
			{
				@out.Collect(new Text("Hello"), new Text("World"));
			}

			public virtual void Close()
			{
			}
		}

		/// <summary>A do-nothing reducer class.</summary>
		/// <remarks>A do-nothing reducer class. We won't get this far, really.</remarks>
		internal class TextReduce : Reducer<Text, Text, Text, Text>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(Text key, IEnumerator<Text> values, OutputCollector<Text
				, Text> @out, Reporter reporter)
			{
				@out.Collect(new Text("Test"), new Text("Me"));
			}

			public virtual void Close()
			{
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
			conf.SetInt(JobContext.IoSortMb, 1);
			conf.SetInputFormat(typeof(SequenceFileInputFormat));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetMapperClass(typeof(TestMapOutputType.TextGen));
			conf.SetReducerClass(typeof(TestMapOutputType.TextReduce));
			conf.SetOutputKeyClass(typeof(Text));
			conf.SetOutputValueClass(typeof(Text));
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
			Path inFile = new Path(inDir, "part0");
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, inFile, typeof(Text
				), typeof(Text));
			writer.Append(new Text("rec: 1"), new Text("Hello"));
			writer.Close();
			jc = new JobClient(conf);
		}

		[TearDown]
		public virtual void Cleanup()
		{
			FileUtil.FullyDelete(TestDir);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKeyMismatch()
		{
			//  Set bad MapOutputKeyClass and MapOutputValueClass
			conf.SetMapOutputKeyClass(typeof(IntWritable));
			conf.SetMapOutputValueClass(typeof(IntWritable));
			RunningJob r_job = jc.SubmitJob(conf);
			while (!r_job.IsComplete())
			{
				Sharpen.Thread.Sleep(1000);
			}
			if (r_job.IsSuccessful())
			{
				NUnit.Framework.Assert.Fail("Oops! The job was supposed to break due to an exception"
					);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestValueMismatch()
		{
			conf.SetMapOutputKeyClass(typeof(Text));
			conf.SetMapOutputValueClass(typeof(IntWritable));
			RunningJob r_job = jc.SubmitJob(conf);
			while (!r_job.IsComplete())
			{
				Sharpen.Thread.Sleep(1000);
			}
			if (r_job.IsSuccessful())
			{
				NUnit.Framework.Assert.Fail("Oops! The job was supposed to break due to an exception"
					);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoMismatch()
		{
			//  Set good MapOutputKeyClass and MapOutputValueClass
			conf.SetMapOutputKeyClass(typeof(Text));
			conf.SetMapOutputValueClass(typeof(Text));
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
	}
}
