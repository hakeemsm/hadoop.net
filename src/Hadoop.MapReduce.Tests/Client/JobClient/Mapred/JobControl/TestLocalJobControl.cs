using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Jobcontrol
{
	/// <summary>HadoopTestCase that tests the local job runner.</summary>
	public class TestLocalJobControl : HadoopTestCase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.Jobcontrol.TestLocalJobControl
			).FullName);

		/// <summary>
		/// Initialises a new instance of this test case to use a Local MR cluster and
		/// a local filesystem.
		/// </summary>
		/// <exception cref="System.IO.IOException">If an error occurs initialising this object.
		/// 	</exception>
		public TestLocalJobControl()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 2, 2)
		{
		}

		/// <summary>This is a main function for testing JobControl class.</summary>
		/// <remarks>
		/// This is a main function for testing JobControl class. It first cleans all
		/// the dirs it will use. Then it generates some random text data in
		/// TestJobControlData/indir. Then it creates 4 jobs: Job 1: copy data from
		/// indir to outdir_1 Job 2: copy data from indir to outdir_2 Job 3: copy data
		/// from outdir_1 and outdir_2 to outdir_3 Job 4: copy data from outdir to
		/// outdir_4 The jobs 1 and 2 have no dependency. The job 3 depends on jobs 1
		/// and 2. The job 4 depends on job 3.
		/// Then it creates a JobControl object and add the 4 jobs to the JobControl
		/// object. Finally, it creates a thread to run the JobControl object and
		/// monitors/reports the job states.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestLocalJobControlDataCopy()
		{
			FileSystem fs = FileSystem.Get(CreateJobConf());
			Path rootDataDir = new Path(Runtime.GetProperty("test.build.data", "."), "TestLocalJobControlData"
				);
			Path indir = new Path(rootDataDir, "indir");
			Path outdir_1 = new Path(rootDataDir, "outdir_1");
			Path outdir_2 = new Path(rootDataDir, "outdir_2");
			Path outdir_3 = new Path(rootDataDir, "outdir_3");
			Path outdir_4 = new Path(rootDataDir, "outdir_4");
			JobControlTestUtils.CleanData(fs, indir);
			JobControlTestUtils.GenerateData(fs, indir);
			JobControlTestUtils.CleanData(fs, outdir_1);
			JobControlTestUtils.CleanData(fs, outdir_2);
			JobControlTestUtils.CleanData(fs, outdir_3);
			JobControlTestUtils.CleanData(fs, outdir_4);
			AList<Job> dependingJobs = null;
			AList<Path> inPaths_1 = new AList<Path>();
			inPaths_1.AddItem(indir);
			JobConf jobConf_1 = JobControlTestUtils.CreateCopyJob(inPaths_1, outdir_1);
			Job job_1 = new Job(jobConf_1, dependingJobs);
			AList<Path> inPaths_2 = new AList<Path>();
			inPaths_2.AddItem(indir);
			JobConf jobConf_2 = JobControlTestUtils.CreateCopyJob(inPaths_2, outdir_2);
			Job job_2 = new Job(jobConf_2, dependingJobs);
			AList<Path> inPaths_3 = new AList<Path>();
			inPaths_3.AddItem(outdir_1);
			inPaths_3.AddItem(outdir_2);
			JobConf jobConf_3 = JobControlTestUtils.CreateCopyJob(inPaths_3, outdir_3);
			dependingJobs = new AList<Job>();
			dependingJobs.AddItem(job_1);
			dependingJobs.AddItem(job_2);
			Job job_3 = new Job(jobConf_3, dependingJobs);
			AList<Path> inPaths_4 = new AList<Path>();
			inPaths_4.AddItem(outdir_3);
			JobConf jobConf_4 = JobControlTestUtils.CreateCopyJob(inPaths_4, outdir_4);
			dependingJobs = new AList<Job>();
			dependingJobs.AddItem(job_3);
			Job job_4 = new Job(jobConf_4, dependingJobs);
			JobControl theControl = new JobControl("Test");
			theControl.AddJob(job_1);
			theControl.AddJob(job_2);
			theControl.AddJob(job_3);
			theControl.AddJob(job_4);
			Sharpen.Thread theController = new Sharpen.Thread(theControl);
			theController.Start();
			while (!theControl.AllFinished())
			{
				Log.Debug("Jobs in waiting state: " + theControl.GetWaitingJobs().Count);
				Log.Debug("Jobs in ready state: " + theControl.GetReadyJobs().Count);
				Log.Debug("Jobs in running state: " + theControl.GetRunningJobs().Count);
				Log.Debug("Jobs in success state: " + theControl.GetSuccessfulJobs().Count);
				Log.Debug("Jobs in failed state: " + theControl.GetFailedJobs().Count);
				Log.Debug("\n");
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception)
				{
				}
			}
			NUnit.Framework.Assert.AreEqual("Some jobs failed", 0, theControl.GetFailedJobs()
				.Count);
			theControl.Stop();
		}
	}
}
