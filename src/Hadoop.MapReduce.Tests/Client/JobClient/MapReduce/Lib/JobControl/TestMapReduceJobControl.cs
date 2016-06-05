using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol
{
	/// <summary>This class performs unit test for Job/JobControl classes.</summary>
	public class TestMapReduceJobControl : HadoopTestCase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol.TestMapReduceJobControl
			).FullName);

		internal static Path rootDataDir = new Path(Runtime.GetProperty("test.build.data"
			, "."), "TestData");

		internal static Path indir = new Path(rootDataDir, "indir");

		internal static Path outdir_1 = new Path(rootDataDir, "outdir_1");

		internal static Path outdir_2 = new Path(rootDataDir, "outdir_2");

		internal static Path outdir_3 = new Path(rootDataDir, "outdir_3");

		internal static Path outdir_4 = new Path(rootDataDir, "outdir_4");

		internal static ControlledJob cjob1 = null;

		internal static ControlledJob cjob2 = null;

		internal static ControlledJob cjob3 = null;

		internal static ControlledJob cjob4 = null;

		/// <exception cref="System.IO.IOException"/>
		public TestMapReduceJobControl()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 2, 2)
		{
		}

		/// <exception cref="System.Exception"/>
		private void CleanupData(Configuration conf)
		{
			FileSystem fs = FileSystem.Get(conf);
			MapReduceTestUtil.CleanData(fs, indir);
			MapReduceTestUtil.GenerateData(fs, indir);
			MapReduceTestUtil.CleanData(fs, outdir_1);
			MapReduceTestUtil.CleanData(fs, outdir_2);
			MapReduceTestUtil.CleanData(fs, outdir_3);
			MapReduceTestUtil.CleanData(fs, outdir_4);
		}

		/// <summary>This is a main function for testing JobControl class.</summary>
		/// <remarks>
		/// This is a main function for testing JobControl class.
		/// It requires 4 jobs:
		/// Job 1: passed as parameter. input:indir  output:outdir_1
		/// Job 2: copy data from indir to outdir_2
		/// Job 3: copy data from outdir_1 and outdir_2 to outdir_3
		/// Job 4: copy data from outdir to outdir_4
		/// The jobs 1 and 2 have no dependency. The job 3 depends on jobs 1 and 2.
		/// The job 4 depends on job 3.
		/// Then it creates a JobControl object and add the 4 jobs to
		/// the JobControl object.
		/// Finally, it creates a thread to run the JobControl object
		/// </remarks>
		/// <exception cref="System.Exception"/>
		private JobControl CreateDependencies(Configuration conf, Job job1)
		{
			IList<ControlledJob> dependingJobs = null;
			cjob1 = new ControlledJob(job1, dependingJobs);
			Job job2 = MapReduceTestUtil.CreateCopyJob(conf, outdir_2, indir);
			cjob2 = new ControlledJob(job2, dependingJobs);
			Job job3 = MapReduceTestUtil.CreateCopyJob(conf, outdir_3, outdir_1, outdir_2);
			dependingJobs = new AList<ControlledJob>();
			dependingJobs.AddItem(cjob1);
			dependingJobs.AddItem(cjob2);
			cjob3 = new ControlledJob(job3, dependingJobs);
			Job job4 = MapReduceTestUtil.CreateCopyJob(conf, outdir_4, outdir_3);
			dependingJobs = new AList<ControlledJob>();
			dependingJobs.AddItem(cjob3);
			cjob4 = new ControlledJob(job4, dependingJobs);
			JobControl theControl = new JobControl("Test");
			theControl.AddJob(cjob1);
			theControl.AddJob(cjob2);
			theControl.AddJob(cjob3);
			theControl.AddJob(cjob4);
			Sharpen.Thread theController = new Sharpen.Thread(theControl);
			theController.Start();
			return theControl;
		}

		private void WaitTillAllFinished(JobControl theControl)
		{
			while (!theControl.AllFinished())
			{
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception)
				{
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJobControlWithFailJob()
		{
			Log.Info("Starting testJobControlWithFailJob");
			Configuration conf = CreateJobConf();
			CleanupData(conf);
			// create a Fail job
			Job job1 = MapReduceTestUtil.CreateFailJob(conf, outdir_1, indir);
			// create job dependencies
			JobControl theControl = CreateDependencies(conf, job1);
			// wait till all the jobs complete
			WaitTillAllFinished(theControl);
			NUnit.Framework.Assert.IsTrue(cjob1.GetJobState() == ControlledJob.State.Failed);
			NUnit.Framework.Assert.IsTrue(cjob2.GetJobState() == ControlledJob.State.Success);
			NUnit.Framework.Assert.IsTrue(cjob3.GetJobState() == ControlledJob.State.DependentFailed
				);
			NUnit.Framework.Assert.IsTrue(cjob4.GetJobState() == ControlledJob.State.DependentFailed
				);
			theControl.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJobControlWithKillJob()
		{
			Log.Info("Starting testJobControlWithKillJob");
			Configuration conf = CreateJobConf();
			CleanupData(conf);
			Job job1 = MapReduceTestUtil.CreateKillJob(conf, outdir_1, indir);
			JobControl theControl = CreateDependencies(conf, job1);
			while (cjob1.GetJobState() != ControlledJob.State.Running)
			{
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception)
				{
					break;
				}
			}
			// verify adding dependingJo to RUNNING job fails.
			NUnit.Framework.Assert.IsFalse(cjob1.AddDependingJob(cjob2));
			// suspend jobcontrol and resume it again
			theControl.Suspend();
			NUnit.Framework.Assert.IsTrue(theControl.GetThreadState() == JobControl.ThreadState
				.Suspended);
			theControl.Resume();
			// kill the first job.
			cjob1.KillJob();
			// wait till all the jobs complete
			WaitTillAllFinished(theControl);
			NUnit.Framework.Assert.IsTrue(cjob1.GetJobState() == ControlledJob.State.Failed);
			NUnit.Framework.Assert.IsTrue(cjob2.GetJobState() == ControlledJob.State.Success);
			NUnit.Framework.Assert.IsTrue(cjob3.GetJobState() == ControlledJob.State.DependentFailed
				);
			NUnit.Framework.Assert.IsTrue(cjob4.GetJobState() == ControlledJob.State.DependentFailed
				);
			theControl.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJobControl()
		{
			Log.Info("Starting testJobControl");
			Configuration conf = CreateJobConf();
			CleanupData(conf);
			Job job1 = MapReduceTestUtil.CreateCopyJob(conf, outdir_1, indir);
			JobControl theControl = CreateDependencies(conf, job1);
			// wait till all the jobs complete
			WaitTillAllFinished(theControl);
			NUnit.Framework.Assert.AreEqual("Some jobs failed", 0, theControl.GetFailedJobList
				().Count);
			theControl.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestControlledJob()
		{
			Log.Info("Starting testControlledJob");
			Configuration conf = CreateJobConf();
			CleanupData(conf);
			Job job1 = MapReduceTestUtil.CreateCopyJob(conf, outdir_1, indir);
			JobControl theControl = CreateDependencies(conf, job1);
			while (cjob1.GetJobState() != ControlledJob.State.Running)
			{
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception)
				{
					break;
				}
			}
			NUnit.Framework.Assert.IsNotNull(cjob1.GetMapredJobId());
			// wait till all the jobs complete
			WaitTillAllFinished(theControl);
			NUnit.Framework.Assert.AreEqual("Some jobs failed", 0, theControl.GetFailedJobList
				().Count);
			theControl.Stop();
		}
	}
}
