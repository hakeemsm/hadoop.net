using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Jobcontrol
{
	/// <summary>This class performs unit test for Job/JobControl classes.</summary>
	public class TestJobControl : TestCase
	{
		/// <summary>This is a main function for testing JobControl class.</summary>
		/// <remarks>
		/// This is a main function for testing JobControl class.
		/// It first cleans all the dirs it will use. Then it generates some random text
		/// data in TestJobControlData/indir. Then it creates 4 jobs:
		/// Job 1: copy data from indir to outdir_1
		/// Job 2: copy data from indir to outdir_2
		/// Job 3: copy data from outdir_1 and outdir_2 to outdir_3
		/// Job 4: copy data from outdir to outdir_4
		/// The jobs 1 and 2 have no dependency. The job 3 depends on jobs 1 and 2.
		/// The job 4 depends on job 3.
		/// Then it creates a JobControl object and add the 4 jobs to the JobControl object.
		/// Finally, it creates a thread to run the JobControl object and monitors/reports
		/// the job states.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public static void DoJobControlTest()
		{
			Configuration defaults = new Configuration();
			FileSystem fs = FileSystem.Get(defaults);
			Path rootDataDir = new Path(Runtime.GetProperty("test.build.data", "."), "TestJobControlData"
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
			theControl.AddJob((ControlledJob)job_1);
			theControl.AddJob((ControlledJob)job_2);
			theControl.AddJob(job_3);
			theControl.AddJob(job_4);
			Sharpen.Thread theController = new Sharpen.Thread(theControl);
			theController.Start();
			while (!theControl.AllFinished())
			{
				System.Console.Out.WriteLine("Jobs in waiting state: " + theControl.GetWaitingJobs
					().Count);
				System.Console.Out.WriteLine("Jobs in ready state: " + theControl.GetReadyJobs().
					Count);
				System.Console.Out.WriteLine("Jobs in running state: " + theControl.GetRunningJobs
					().Count);
				System.Console.Out.WriteLine("Jobs in success state: " + theControl.GetSuccessfulJobs
					().Count);
				System.Console.Out.WriteLine("Jobs in failed state: " + theControl.GetFailedJobs(
					).Count);
				System.Console.Out.WriteLine("\n");
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception)
				{
				}
			}
			System.Console.Out.WriteLine("Jobs are all done???");
			System.Console.Out.WriteLine("Jobs in waiting state: " + theControl.GetWaitingJobs
				().Count);
			System.Console.Out.WriteLine("Jobs in ready state: " + theControl.GetReadyJobs().
				Count);
			System.Console.Out.WriteLine("Jobs in running state: " + theControl.GetRunningJobs
				().Count);
			System.Console.Out.WriteLine("Jobs in success state: " + theControl.GetSuccessfulJobs
				().Count);
			System.Console.Out.WriteLine("Jobs in failed state: " + theControl.GetFailedJobs(
				).Count);
			System.Console.Out.WriteLine("\n");
			if (job_1.GetState() != Job.Failed && job_1.GetState() != Job.DependentFailed && 
				job_1.GetState() != Job.Success)
			{
				string states = "job_1:  " + job_1.GetState() + "\n";
				throw new Exception("The state of job_1 is not in a complete state\n" + states);
			}
			if (job_2.GetState() != Job.Failed && job_2.GetState() != Job.DependentFailed && 
				job_2.GetState() != Job.Success)
			{
				string states = "job_2:  " + job_2.GetState() + "\n";
				throw new Exception("The state of job_2 is not in a complete state\n" + states);
			}
			if (job_3.GetState() != Job.Failed && job_3.GetState() != Job.DependentFailed && 
				job_3.GetState() != Job.Success)
			{
				string states = "job_3:  " + job_3.GetState() + "\n";
				throw new Exception("The state of job_3 is not in a complete state\n" + states);
			}
			if (job_4.GetState() != Job.Failed && job_4.GetState() != Job.DependentFailed && 
				job_4.GetState() != Job.Success)
			{
				string states = "job_4:  " + job_4.GetState() + "\n";
				throw new Exception("The state of job_4 is not in a complete state\n" + states);
			}
			if (job_1.GetState() == Job.Failed || job_2.GetState() == Job.Failed || job_1.GetState
				() == Job.DependentFailed || job_2.GetState() == Job.DependentFailed)
			{
				if (job_3.GetState() != Job.DependentFailed)
				{
					string states = "job_1:  " + job_1.GetState() + "\n";
					states = "job_2:  " + job_2.GetState() + "\n";
					states = "job_3:  " + job_3.GetState() + "\n";
					states = "job_4:  " + job_4.GetState() + "\n";
					throw new Exception("The states of jobs 1, 2, 3, 4 are not consistent\n" + states
						);
				}
			}
			if (job_3.GetState() == Job.Failed || job_3.GetState() == Job.DependentFailed)
			{
				if (job_4.GetState() != Job.DependentFailed)
				{
					string states = "job_3:  " + job_3.GetState() + "\n";
					states = "job_4:  " + job_4.GetState() + "\n";
					throw new Exception("The states of jobs 3, 4 are not consistent\n" + states);
				}
			}
			theControl.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJobState()
		{
			Job job_1 = GetCopyJob();
			JobControl jc = new JobControl("Test");
			jc.AddJob(job_1);
			NUnit.Framework.Assert.AreEqual(Job.Waiting, job_1.GetState());
			job_1.SetState(Job.Success);
			NUnit.Framework.Assert.AreEqual(Job.Waiting, job_1.GetState());
			Job mockjob = Org.Mockito.Mockito.Mock<Job>();
			JobID jid = new JobID("test", 0);
			Org.Mockito.Mockito.When(mockjob.GetJobID()).ThenReturn(jid);
			job_1.SetJob(mockjob);
			NUnit.Framework.Assert.AreEqual("job_test_0000", job_1.GetMapredJobID());
			job_1.SetMapredJobID("job_test_0001");
			NUnit.Framework.Assert.AreEqual("job_test_0000", job_1.GetMapredJobID());
			jc.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddingDependingJob()
		{
			Job job_1 = GetCopyJob();
			AList<Job> dependingJobs = new AList<Job>();
			JobControl jc = new JobControl("Test");
			jc.AddJob(job_1);
			NUnit.Framework.Assert.AreEqual(Job.Waiting, job_1.GetState());
			NUnit.Framework.Assert.IsTrue(job_1.AddDependingJob(new Job(job_1.GetJobConf(), dependingJobs
				)));
		}

		/// <exception cref="System.Exception"/>
		public virtual Job GetCopyJob()
		{
			Configuration defaults = new Configuration();
			FileSystem fs = FileSystem.Get(defaults);
			Path rootDataDir = new Path(Runtime.GetProperty("test.build.data", "."), "TestJobControlData"
				);
			Path indir = new Path(rootDataDir, "indir");
			Path outdir_1 = new Path(rootDataDir, "outdir_1");
			JobControlTestUtils.CleanData(fs, indir);
			JobControlTestUtils.GenerateData(fs, indir);
			JobControlTestUtils.CleanData(fs, outdir_1);
			AList<Job> dependingJobs = null;
			AList<Path> inPaths_1 = new AList<Path>();
			inPaths_1.AddItem(indir);
			JobConf jobConf_1 = JobControlTestUtils.CreateCopyJob(inPaths_1, outdir_1);
			Job job_1 = new Job(jobConf_1, dependingJobs);
			return job_1;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJobControl()
		{
			DoJobControlTest();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetAssignedJobId()
		{
			JobConf jc = new JobConf();
			Job j = new Job(jc);
			//Just make sure no exception is thrown
			NUnit.Framework.Assert.IsNull(j.GetAssignedJobID());
			Job mockjob = Org.Mockito.Mockito.Mock<Job>();
			JobID jid = new JobID("test", 0);
			Org.Mockito.Mockito.When(mockjob.GetJobID()).ThenReturn(jid);
			j.SetJob(mockjob);
			JobID expected = new JobID("test", 0);
			NUnit.Framework.Assert.AreEqual(expected, j.GetAssignedJobID());
			Org.Mockito.Mockito.Verify(mockjob).GetJobID();
		}

		public static void Main(string[] args)
		{
			Org.Apache.Hadoop.Mapred.Jobcontrol.TestJobControl test = new Org.Apache.Hadoop.Mapred.Jobcontrol.TestJobControl
				();
			try
			{
				test.TestJobControl();
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
		}
	}
}
