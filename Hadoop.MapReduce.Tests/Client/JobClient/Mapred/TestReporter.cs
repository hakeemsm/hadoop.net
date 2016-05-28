using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Tests the old mapred APIs with
	/// <see cref="Reporter.GetProgress()"/>
	/// .
	/// </summary>
	public class TestReporter
	{
		private static readonly Path rootTempDir = new Path(Runtime.GetProperty("test.build.data"
			, "/tmp"));

		private static readonly Path testRootTempDir = new Path(rootTempDir, "TestReporter"
			);

		private static FileSystem fs = null;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			fs = FileSystem.GetLocal(new Configuration());
			fs.Delete(testRootTempDir, true);
			fs.Mkdirs(testRootTempDir);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Cleanup()
		{
			fs.Delete(testRootTempDir, true);
		}

		private const string Input = "Hi\nHi\nHi\nHi\n";

		private static readonly int InputLines = Input.Split("\n").Length;

		internal class ProgressTesterMapper : MapReduceBase, Mapper<LongWritable, Text, Text
			, Text>
		{
			private float progressRange = 0;

			private int numRecords = 0;

			private Reporter reporter = null;

			// an input with 4 lines
			public override void Configure(JobConf job)
			{
				base.Configure(job);
				// set the progress range accordingly
				if (job.GetNumReduceTasks() == 0)
				{
					progressRange = 1f;
				}
				else
				{
					progressRange = 0.667f;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<Text, Text>
				 output, Reporter reporter)
			{
				this.reporter = reporter;
				// calculate the actual map progress
				float mapProgress = ((float)++numRecords) / InputLines;
				// calculate the attempt progress based on the progress range
				float attemptProgress = progressRange * mapProgress;
				NUnit.Framework.Assert.AreEqual("Invalid progress in map", attemptProgress, reporter
					.GetProgress(), 0f);
				output.Collect(new Text(value.ToString() + numRecords), value);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				base.Close();
				NUnit.Framework.Assert.AreEqual("Invalid progress in map cleanup", progressRange, 
					reporter.GetProgress(), 0f);
			}
		}

		internal class StatusLimitMapper : Mapper<LongWritable, Text, Text, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
				StringBuilder sb = new StringBuilder(512);
				for (int i = 0; i < 1000; i++)
				{
					sb.Append("a");
				}
				context.SetStatus(sb.ToString());
				int progressStatusLength = context.GetConfiguration().GetInt(MRConfig.ProgressStatusLenLimitKey
					, MRConfig.ProgressStatusLenLimitDefault);
				if (context.GetStatus().Length > progressStatusLength)
				{
					throw new IOException("Status is not truncated");
				}
			}
		}

		/// <summary>
		/// Test
		/// <see cref="Reporter"/>
		/// 's progress for a map-only job.
		/// This will make sure that only the map phase decides the attempt's progress.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReporterProgressForMapOnlyJob()
		{
			Path test = new Path(testRootTempDir, "testReporterProgressForMapOnlyJob");
			JobConf conf = new JobConf();
			conf.SetMapperClass(typeof(TestReporter.ProgressTesterMapper));
			conf.SetMapOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			// fail early
			conf.SetMaxMapAttempts(1);
			conf.SetMaxReduceAttempts(0);
			RunningJob job = UtilsForTests.RunJob(conf, new Path(test, "in"), new Path(test, 
				"out"), 1, 0, Input);
			job.WaitForCompletion();
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
		}

		/// <summary>
		/// A
		/// <see cref="Reducer{K2, V2, K3, V3}"/>
		/// implementation that checks the progress on every call
		/// to
		/// <see cref="Reducer{K2, V2, K3, V3}.Reduce(object, System.Collections.IEnumerator{E}, OutputCollector{K, V}, Reporter)
		/// 	"/>
		/// .
		/// </summary>
		internal class ProgressTestingReducer : MapReduceBase, Reducer<Org.Apache.Hadoop.IO.Text
			, Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
			>
		{
			private int recordCount = 0;

			private Reporter reporter = null;

			private readonly float ReduceProgressRange = 1.0f / 3;

			private readonly float ShuffleProgressRange = 1 - ReduceProgressRange;

			// reduce task has a fixed split of progress amongst copy, shuffle and 
			// reduce phases.
			public override void Configure(JobConf job)
			{
				base.Configure(job);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(Org.Apache.Hadoop.IO.Text key, IEnumerator<Org.Apache.Hadoop.IO.Text
				> values, OutputCollector<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text> 
				output, Reporter reporter)
			{
				float reducePhaseProgress = ((float)++recordCount) / InputLines;
				float weightedReducePhaseProgress = reducePhaseProgress * ReduceProgressRange;
				NUnit.Framework.Assert.AreEqual("Invalid progress in reduce", ShuffleProgressRange
					 + weightedReducePhaseProgress, reporter.GetProgress(), 0.02f);
				this.reporter = reporter;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				base.Close();
				NUnit.Framework.Assert.AreEqual("Invalid progress in reduce cleanup", 1.0f, reporter
					.GetProgress(), 0f);
			}
		}

		/// <summary>
		/// Test
		/// <see cref="Reporter"/>
		/// 's progress for map-reduce job.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReporterProgressForMRJob()
		{
			Path test = new Path(testRootTempDir, "testReporterProgressForMRJob");
			JobConf conf = new JobConf();
			conf.SetMapperClass(typeof(TestReporter.ProgressTesterMapper));
			conf.SetReducerClass(typeof(TestReporter.ProgressTestingReducer));
			conf.SetMapOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			// fail early
			conf.SetMaxMapAttempts(1);
			conf.SetMaxReduceAttempts(1);
			RunningJob job = UtilsForTests.RunJob(conf, new Path(test, "in"), new Path(test, 
				"out"), 1, 1, Input);
			job.WaitForCompletion();
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		[NUnit.Framework.Test]
		public virtual void TestStatusLimit()
		{
			Path test = new Path(testRootTempDir, "testStatusLimit");
			Configuration conf = new Configuration();
			Path inDir = new Path(test, "in");
			Path outDir = new Path(test, "out");
			FileSystem fs = FileSystem.Get(conf);
			if (fs.Exists(inDir))
			{
				fs.Delete(inDir, true);
			}
			fs.Mkdirs(inDir);
			DataOutputStream file = fs.Create(new Path(inDir, "part-" + 0));
			file.WriteBytes("testStatusLimit");
			file.Close();
			if (fs.Exists(outDir))
			{
				fs.Delete(outDir, true);
			}
			Job job = Job.GetInstance(conf, "testStatusLimit");
			job.SetMapperClass(typeof(TestReporter.StatusLimitMapper));
			job.SetNumReduceTasks(0);
			FileInputFormat.AddInputPath(job, inDir);
			FileOutputFormat.SetOutputPath(job, outDir);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
		}
	}
}
