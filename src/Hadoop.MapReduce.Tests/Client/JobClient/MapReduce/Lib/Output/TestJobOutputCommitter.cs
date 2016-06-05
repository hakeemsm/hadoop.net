using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>A JUnit test to test Map-Reduce job committer.</summary>
	public class TestJobOutputCommitter : HadoopTestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public TestJobOutputCommitter()
			: base(ClusterMr, LocalFs, 1, 1)
		{
		}

		private static string TestRootDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp") + "/" + "test-job-output-committer").ToString();

		private const string CustomCleanupFileName = "_custom_cleanup";

		private const string AbortKilledFileName = "_custom_abort_killed";

		private const string AbortFailedFileName = "_custom_abort_failed";

		private static Path inDir = new Path(TestRootDir, "test-input");

		private static int outDirs = 0;

		private FileSystem fs;

		private Configuration conf = null;

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			base.SetUp();
			conf = CreateJobConf();
			fs = GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			fs.Delete(new Path(TestRootDir), true);
			base.TearDown();
		}

		/// <summary>
		/// Committer with deprecated
		/// <see cref="FileOutputCommitter.CleanupJob(Org.Apache.Hadoop.Mapreduce.JobContext)
		/// 	"/>
		/// making a _failed/_killed in the output folder
		/// </summary>
		internal class CommitterWithCustomDeprecatedCleanup : FileOutputCommitter
		{
			/// <exception cref="System.IO.IOException"/>
			public CommitterWithCustomDeprecatedCleanup(Path outputPath, TaskAttemptContext context
				)
				: base(outputPath, context)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CleanupJob(JobContext context)
			{
				System.Console.Error.WriteLine("---- HERE ----");
				Path outputPath = FileOutputFormat.GetOutputPath(context);
				FileSystem fs = outputPath.GetFileSystem(context.GetConfiguration());
				fs.Create(new Path(outputPath, CustomCleanupFileName)).Close();
			}
		}

		/// <summary>Committer with abort making a _failed/_killed in the output folder</summary>
		internal class CommitterWithCustomAbort : FileOutputCommitter
		{
			/// <exception cref="System.IO.IOException"/>
			public CommitterWithCustomAbort(Path outputPath, TaskAttemptContext context)
				: base(outputPath, context)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void AbortJob(JobContext context, JobStatus.State state)
			{
				Path outputPath = FileOutputFormat.GetOutputPath(context);
				FileSystem fs = outputPath.GetFileSystem(context.GetConfiguration());
				string fileName = (state.Equals(JobStatus.State.Failed)) ? AbortFailedFileName : 
					AbortKilledFileName;
				fs.Create(new Path(outputPath, fileName)).Close();
			}
		}

		private Path GetNewOutputDir()
		{
			return new Path(TestRootDir, "output-" + outDirs++);
		}

		internal class MyOutputFormatWithCustomAbort<K, V> : TextOutputFormat<K, V>
		{
			private OutputCommitter committer = null;

			/// <exception cref="System.IO.IOException"/>
			public override OutputCommitter GetOutputCommitter(TaskAttemptContext context)
			{
				lock (this)
				{
					if (committer == null)
					{
						Path output = GetOutputPath(context);
						committer = new TestJobOutputCommitter.CommitterWithCustomAbort(output, context);
					}
					return committer;
				}
			}
		}

		internal class MyOutputFormatWithCustomCleanup<K, V> : TextOutputFormat<K, V>
		{
			private OutputCommitter committer = null;

			/// <exception cref="System.IO.IOException"/>
			public override OutputCommitter GetOutputCommitter(TaskAttemptContext context)
			{
				lock (this)
				{
					if (committer == null)
					{
						Path output = GetOutputPath(context);
						committer = new TestJobOutputCommitter.CommitterWithCustomDeprecatedCleanup(output
							, context);
					}
					return committer;
				}
			}
		}

		// run a job with 1 map and let it run to completion
		/// <exception cref="System.Exception"/>
		private void TestSuccessfulJob(string filename, Type output, string[] exclude)
		{
			Path outDir = GetNewOutputDir();
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, 1, 0);
			job.SetOutputFormatClass(output);
			NUnit.Framework.Assert.IsTrue("Job failed!", job.WaitForCompletion(true));
			Path testFile = new Path(outDir, filename);
			NUnit.Framework.Assert.IsTrue("Done file missing for job " + job.GetJobID(), fs.Exists
				(testFile));
			// check if the files from the missing set exists
			foreach (string ex in exclude)
			{
				Path file = new Path(outDir, ex);
				NUnit.Framework.Assert.IsFalse("File " + file + " should not be present for successful job "
					 + job.GetJobID(), fs.Exists(file));
			}
		}

		// run a job for which all the attempts simply fail.
		/// <exception cref="System.Exception"/>
		private void TestFailedJob(string fileName, Type output, string[] exclude)
		{
			Path outDir = GetNewOutputDir();
			Job job = MapReduceTestUtil.CreateFailJob(conf, outDir, inDir);
			job.SetOutputFormatClass(output);
			NUnit.Framework.Assert.IsFalse("Job did not fail!", job.WaitForCompletion(true));
			if (fileName != null)
			{
				Path testFile = new Path(outDir, fileName);
				NUnit.Framework.Assert.IsTrue("File " + testFile + " missing for failed job " + job
					.GetJobID(), fs.Exists(testFile));
			}
			// check if the files from the missing set exists
			foreach (string ex in exclude)
			{
				Path file = new Path(outDir, ex);
				NUnit.Framework.Assert.IsFalse("File " + file + " should not be present for failed job "
					 + job.GetJobID(), fs.Exists(file));
			}
		}

		// run a job which gets stuck in mapper and kill it.
		/// <exception cref="System.Exception"/>
		private void TestKilledJob(string fileName, Type output, string[] exclude)
		{
			Path outDir = GetNewOutputDir();
			Job job = MapReduceTestUtil.CreateKillJob(conf, outDir, inDir);
			job.SetOutputFormatClass(output);
			job.Submit();
			// wait for the setup to be completed
			while (job.SetupProgress() != 1.0f)
			{
				UtilsForTests.WaitFor(100);
			}
			job.KillJob();
			// kill the job
			NUnit.Framework.Assert.IsFalse("Job did not get kill", job.WaitForCompletion(true
				));
			if (fileName != null)
			{
				Path testFile = new Path(outDir, fileName);
				NUnit.Framework.Assert.IsTrue("File " + testFile + " missing for job " + job.GetJobID
					(), fs.Exists(testFile));
			}
			// check if the files from the missing set exists
			foreach (string ex in exclude)
			{
				Path file = new Path(outDir, ex);
				NUnit.Framework.Assert.IsFalse("File " + file + " should not be present for killed job "
					 + job.GetJobID(), fs.Exists(file));
			}
		}

		/// <summary>Test default cleanup/abort behavior</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDefaultCleanupAndAbort()
		{
			// check with a successful job
			TestSuccessfulJob(FileOutputCommitter.SucceededFileName, typeof(TextOutputFormat)
				, new string[] {  });
			// check with a failed job
			TestFailedJob(null, typeof(TextOutputFormat), new string[] { FileOutputCommitter.
				SucceededFileName });
			// check default abort job kill
			TestKilledJob(null, typeof(TextOutputFormat), new string[] { FileOutputCommitter.
				SucceededFileName });
		}

		/// <summary>Test if a failed job with custom committer runs the abort code.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCustomAbort()
		{
			// check with a successful job
			TestSuccessfulJob(FileOutputCommitter.SucceededFileName, typeof(TestJobOutputCommitter.MyOutputFormatWithCustomAbort
				), new string[] { AbortFailedFileName, AbortKilledFileName });
			// check with a failed job
			TestFailedJob(AbortFailedFileName, typeof(TestJobOutputCommitter.MyOutputFormatWithCustomAbort
				), new string[] { FileOutputCommitter.SucceededFileName, AbortKilledFileName });
			// check with a killed job
			TestKilledJob(AbortKilledFileName, typeof(TestJobOutputCommitter.MyOutputFormatWithCustomAbort
				), new string[] { FileOutputCommitter.SucceededFileName, AbortFailedFileName });
		}

		/// <summary>
		/// Test if a failed job with custom committer runs the deprecated
		/// <see cref="FileOutputCommitter.CleanupJob(Org.Apache.Hadoop.Mapreduce.JobContext)
		/// 	"/>
		/// code for api
		/// compatibility testing.
		/// </summary>
		/// <exception cref="System.Exception"></exception>
		public virtual void TestCustomCleanup()
		{
			// check with a successful job
			TestSuccessfulJob(CustomCleanupFileName, typeof(TestJobOutputCommitter.MyOutputFormatWithCustomCleanup
				), new string[] {  });
			// check with a failed job
			TestFailedJob(CustomCleanupFileName, typeof(TestJobOutputCommitter.MyOutputFormatWithCustomCleanup
				), new string[] { FileOutputCommitter.SucceededFileName });
			// check with a killed job
			TestKilledJob(CustomCleanupFileName, typeof(TestJobOutputCommitter.MyOutputFormatWithCustomCleanup
				), new string[] { FileOutputCommitter.SucceededFileName });
		}
	}
}
