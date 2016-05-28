using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestNoJobSetupCleanup : HadoopTestCase
	{
		private static string TestRootDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp")).ToURI().ToString().Replace(' ', '+');

		private readonly Path inDir = new Path(TestRootDir, "./wc/input");

		private readonly Path outDir = new Path(TestRootDir, "./wc/output");

		/// <exception cref="System.IO.IOException"/>
		public TestNoJobSetupCleanup()
			: base(HadoopTestCase.ClusterMr, HadoopTestCase.LocalFs, 2, 2)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		private Job SubmitAndValidateJob(Configuration conf, int numMaps, int numReds)
		{
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, numMaps, numReds);
			job.SetJobSetupCleanupNeeded(false);
			job.SetOutputFormatClass(typeof(TestNoJobSetupCleanup.MyOutputFormat));
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(job.IsSuccessful());
			NUnit.Framework.Assert.IsTrue(job.GetTaskReports(TaskType.JobSetup).Length == 0);
			NUnit.Framework.Assert.IsTrue(job.GetTaskReports(TaskType.JobCleanup).Length == 0
				);
			NUnit.Framework.Assert.IsTrue(job.GetTaskReports(TaskType.Map).Length == numMaps);
			NUnit.Framework.Assert.IsTrue(job.GetTaskReports(TaskType.Reduce).Length == numReds
				);
			FileSystem fs = FileSystem.Get(conf);
			NUnit.Framework.Assert.IsTrue("Job output directory doesn't exit!", fs.Exists(outDir
				));
			// job commit done only in cleanup 
			// therefore output should still be in temp location
			string tempWorkingPathStr = outDir + Path.Separator + "_temporary" + Path.Separator
				 + "0";
			Path tempWorkingPath = new Path(tempWorkingPathStr);
			FileStatus[] list = fs.ListStatus(tempWorkingPath, new TestNoJobSetupCleanup.OutputFilter
				());
			int numPartFiles = numReds == 0 ? numMaps : numReds;
			NUnit.Framework.Assert.IsTrue("Number of part-files is " + list.Length + " and not "
				 + numPartFiles, list.Length == numPartFiles);
			return job;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNoJobSetupCleanup()
		{
			try
			{
				Configuration conf = CreateJobConf();
				// run a job without job-setup and cleanup
				SubmitAndValidateJob(conf, 1, 1);
				// run a map only job.
				SubmitAndValidateJob(conf, 1, 0);
				// run empty job without job setup and cleanup 
				SubmitAndValidateJob(conf, 0, 0);
				// run empty job without job setup and cleanup, with non-zero reduces 
				SubmitAndValidateJob(conf, 0, 1);
			}
			finally
			{
				TearDown();
			}
		}

		public class MyOutputFormat : TextOutputFormat
		{
			/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void CheckOutputSpecs(JobContext job)
			{
				base.CheckOutputSpecs(job);
				// creating dummy TaskAttemptID
				TaskAttemptID tid = new TaskAttemptID("jt", 1, TaskType.JobSetup, 0, 0);
				GetOutputCommitter(new TaskAttemptContextImpl(job.GetConfiguration(), tid)).SetupJob
					(job);
			}
		}

		private class OutputFilter : PathFilter
		{
			public virtual bool Accept(Path path)
			{
				return !(path.GetName().StartsWith("_"));
			}
		}
	}
}
