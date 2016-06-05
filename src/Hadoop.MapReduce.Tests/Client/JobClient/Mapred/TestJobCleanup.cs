using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A JUnit test to test Map-Reduce job cleanup.</summary>
	public class TestJobCleanup
	{
		private static string TestRootDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp") + "/" + "test-job-cleanup").ToString();

		private const string CustomCleanupFileName = "_custom_cleanup";

		private const string AbortKilledFileName = "_custom_abort_killed";

		private const string AbortFailedFileName = "_custom_abort_failed";

		private static FileSystem fileSys = null;

		private static MiniMRCluster mr = null;

		private static Path inDir = null;

		private static Path emptyInDir = null;

		private static int outDirs = 0;

		private static Log Log = LogFactory.GetLog(typeof(TestJobCleanup));

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetUp()
		{
			JobConf conf = new JobConf();
			fileSys = FileSystem.Get(conf);
			fileSys.Delete(new Path(TestRootDir), true);
			conf.Set("mapred.job.tracker.handler.count", "1");
			conf.Set("mapred.job.tracker", "127.0.0.1:0");
			conf.Set("mapred.job.tracker.http.address", "127.0.0.1:0");
			conf.Set("mapred.task.tracker.http.address", "127.0.0.1:0");
			conf.Set(JHAdminConfig.MrHistoryIntermediateDoneDir, TestRootDir + "/intermediate"
				);
			conf.Set(FileOutputCommitter.SuccessfulJobOutputDirMarker, "true");
			mr = new MiniMRCluster(1, "file:///", 1, null, null, conf);
			inDir = new Path(TestRootDir, "test-input");
			string input = "The quick brown fox\n" + "has many silly\n" + "red fox sox\n";
			DataOutputStream file = fileSys.Create(new Path(inDir, "part-" + 0));
			file.WriteBytes(input);
			file.Close();
			emptyInDir = new Path(TestRootDir, "empty-input");
			fileSys.Mkdirs(emptyInDir);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (fileSys != null)
			{
				// fileSys.delete(new Path(TEST_ROOT_DIR), true);
				fileSys.Close();
			}
			if (mr != null)
			{
				mr.Shutdown();
			}
		}

		/// <summary>
		/// Committer with deprecated
		/// <see cref="FileOutputCommitter.CleanupJob(JobContext)"/>
		/// making a _failed/_killed
		/// in the output folder
		/// </summary>
		internal class CommitterWithCustomDeprecatedCleanup : FileOutputCommitter
		{
			/// <exception cref="System.IO.IOException"/>
			public override void CleanupJob(JobContext context)
			{
				System.Console.Error.WriteLine("---- HERE ----");
				JobConf conf = context.GetJobConf();
				Path outputPath = FileOutputFormat.GetOutputPath(conf);
				FileSystem fs = outputPath.GetFileSystem(conf);
				fs.Create(new Path(outputPath, CustomCleanupFileName)).Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CommitJob(JobContext context)
			{
				CleanupJob(context);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void AbortJob(JobContext context, int i)
			{
				CleanupJob(context);
			}
		}

		/// <summary>Committer with abort making a _failed/_killed in the output folder</summary>
		internal class CommitterWithCustomAbort : FileOutputCommitter
		{
			/// <exception cref="System.IO.IOException"/>
			public override void AbortJob(JobContext context, int state)
			{
				JobConf conf = context.GetJobConf();
				Path outputPath = FileOutputFormat.GetOutputPath(conf);
				FileSystem fs = outputPath.GetFileSystem(conf);
				string fileName = (state == JobStatus.Failed) ? TestJobCleanup.AbortFailedFileName
					 : TestJobCleanup.AbortKilledFileName;
				fs.Create(new Path(outputPath, fileName)).Close();
			}
		}

		private Path GetNewOutputDir()
		{
			return new Path(TestRootDir, "output-" + outDirs++);
		}

		private void ConfigureJob(JobConf jc, string jobName, int maps, int reds, Path outDir
			)
		{
			jc.SetJobName(jobName);
			jc.SetInputFormat(typeof(TextInputFormat));
			jc.SetOutputKeyClass(typeof(LongWritable));
			jc.SetOutputValueClass(typeof(Text));
			FileInputFormat.SetInputPaths(jc, inDir);
			FileOutputFormat.SetOutputPath(jc, outDir);
			jc.SetMapperClass(typeof(IdentityMapper));
			jc.SetReducerClass(typeof(IdentityReducer));
			jc.SetNumMapTasks(maps);
			jc.SetNumReduceTasks(reds);
		}

		// run a job with 1 map and let it run to completion
		/// <exception cref="System.IO.IOException"/>
		private void TestSuccessfulJob(string filename, Type committer, string[] exclude)
		{
			JobConf jc = mr.CreateJobConf();
			Path outDir = GetNewOutputDir();
			ConfigureJob(jc, "job with cleanup()", 1, 0, outDir);
			jc.SetOutputCommitter(committer);
			JobClient jobClient = new JobClient(jc);
			RunningJob job = jobClient.SubmitJob(jc);
			JobID id = job.GetID();
			job.WaitForCompletion();
			Log.Info("Job finished : " + job.IsComplete());
			Path testFile = new Path(outDir, filename);
			NUnit.Framework.Assert.IsTrue("Done file \"" + testFile + "\" missing for job " +
				 id, fileSys.Exists(testFile));
			// check if the files from the missing set exists
			foreach (string ex in exclude)
			{
				Path file = new Path(outDir, ex);
				NUnit.Framework.Assert.IsFalse("File " + file + " should not be present for successful job "
					 + id, fileSys.Exists(file));
			}
		}

		// run a job for which all the attempts simply fail.
		/// <exception cref="System.IO.IOException"/>
		private void TestFailedJob(string fileName, Type committer, string[] exclude)
		{
			JobConf jc = mr.CreateJobConf();
			Path outDir = GetNewOutputDir();
			ConfigureJob(jc, "fail job with abort()", 1, 0, outDir);
			jc.SetMaxMapAttempts(1);
			// set the job to fail
			jc.SetMapperClass(typeof(UtilsForTests.FailMapper));
			jc.SetOutputCommitter(committer);
			JobClient jobClient = new JobClient(jc);
			RunningJob job = jobClient.SubmitJob(jc);
			JobID id = job.GetID();
			job.WaitForCompletion();
			NUnit.Framework.Assert.AreEqual("Job did not fail", JobStatus.Failed, job.GetJobState
				());
			if (fileName != null)
			{
				Path testFile = new Path(outDir, fileName);
				NUnit.Framework.Assert.IsTrue("File " + testFile + " missing for failed job " + id
					, fileSys.Exists(testFile));
			}
			// check if the files from the missing set exists
			foreach (string ex in exclude)
			{
				Path file = new Path(outDir, ex);
				NUnit.Framework.Assert.IsFalse("File " + file + " should not be present for failed job "
					 + id, fileSys.Exists(file));
			}
		}

		// run a job which gets stuck in mapper and kill it.
		/// <exception cref="System.IO.IOException"/>
		private void TestKilledJob(string fileName, Type committer, string[] exclude)
		{
			JobConf jc = mr.CreateJobConf();
			Path outDir = GetNewOutputDir();
			ConfigureJob(jc, "kill job with abort()", 1, 0, outDir);
			// set the job to wait for long
			jc.SetMapperClass(typeof(UtilsForTests.KillMapper));
			jc.SetOutputCommitter(committer);
			JobClient jobClient = new JobClient(jc);
			RunningJob job = jobClient.SubmitJob(jc);
			JobID id = job.GetID();
			Counters counters = job.GetCounters();
			// wait for the map to be launched
			while (true)
			{
				if (counters.GetCounter(JobCounter.TotalLaunchedMaps) == 1)
				{
					break;
				}
				Log.Info("Waiting for a map task to be launched");
				UtilsForTests.WaitFor(100);
				counters = job.GetCounters();
			}
			job.KillJob();
			// kill the job
			job.WaitForCompletion();
			// wait for the job to complete
			NUnit.Framework.Assert.AreEqual("Job was not killed", JobStatus.Killed, job.GetJobState
				());
			if (fileName != null)
			{
				Path testFile = new Path(outDir, fileName);
				NUnit.Framework.Assert.IsTrue("File " + testFile + " missing for job " + id, fileSys
					.Exists(testFile));
			}
			// check if the files from the missing set exists
			foreach (string ex in exclude)
			{
				Path file = new Path(outDir, ex);
				NUnit.Framework.Assert.IsFalse("File " + file + " should not be present for killed job "
					 + id, fileSys.Exists(file));
			}
		}

		/// <summary>Test default cleanup/abort behavior</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultCleanupAndAbort()
		{
			// check with a successful job
			TestSuccessfulJob(FileOutputCommitter.SucceededFileName, typeof(FileOutputCommitter
				), new string[] {  });
			// check with a failed job
			TestFailedJob(null, typeof(FileOutputCommitter), new string[] { FileOutputCommitter
				.SucceededFileName });
			// check default abort job kill
			TestKilledJob(null, typeof(FileOutputCommitter), new string[] { FileOutputCommitter
				.SucceededFileName });
		}

		/// <summary>Test if a failed job with custom committer runs the abort code.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCustomAbort()
		{
			// check with a successful job
			TestSuccessfulJob(FileOutputCommitter.SucceededFileName, typeof(TestJobCleanup.CommitterWithCustomAbort
				), new string[] { AbortFailedFileName, AbortKilledFileName });
			// check with a failed job
			TestFailedJob(AbortFailedFileName, typeof(TestJobCleanup.CommitterWithCustomAbort
				), new string[] { FileOutputCommitter.SucceededFileName, AbortKilledFileName });
			// check with a killed job
			TestKilledJob(AbortKilledFileName, typeof(TestJobCleanup.CommitterWithCustomAbort
				), new string[] { FileOutputCommitter.SucceededFileName, AbortFailedFileName });
		}

		/// <summary>
		/// Test if a failed job with custom committer runs the deprecated
		/// <see cref="FileOutputCommitter.CleanupJob(JobContext)"/>
		/// code for api
		/// compatibility testing.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCustomCleanup()
		{
			// check with a successful job
			TestSuccessfulJob(CustomCleanupFileName, typeof(TestJobCleanup.CommitterWithCustomDeprecatedCleanup
				), new string[] {  });
			// check with a failed job
			TestFailedJob(CustomCleanupFileName, typeof(TestJobCleanup.CommitterWithCustomDeprecatedCleanup
				), new string[] { FileOutputCommitter.SucceededFileName });
			// check with a killed job
			TestKilledJob(TestJobCleanup.CustomCleanupFileName, typeof(TestJobCleanup.CommitterWithCustomDeprecatedCleanup
				), new string[] { FileOutputCommitter.SucceededFileName });
		}
	}
}
