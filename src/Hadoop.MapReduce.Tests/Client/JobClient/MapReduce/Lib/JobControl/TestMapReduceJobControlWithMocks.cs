using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol
{
	/// <summary>Tests the JobControl API using mock and stub Job instances.</summary>
	public class TestMapReduceJobControlWithMocks
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSuccessfulJobs()
		{
			JobControl jobControl = new JobControl("Test");
			ControlledJob job1 = CreateSuccessfulControlledJob(jobControl);
			ControlledJob job2 = CreateSuccessfulControlledJob(jobControl);
			ControlledJob job3 = CreateSuccessfulControlledJob(jobControl, job1, job2);
			ControlledJob job4 = CreateSuccessfulControlledJob(jobControl, job3);
			RunJobControl(jobControl);
			NUnit.Framework.Assert.AreEqual("Success list", 4, jobControl.GetSuccessfulJobList
				().Count);
			NUnit.Framework.Assert.AreEqual("Failed list", 0, jobControl.GetFailedJobList().Count
				);
			NUnit.Framework.Assert.IsTrue(job1.GetJobState() == ControlledJob.State.Success);
			NUnit.Framework.Assert.IsTrue(job2.GetJobState() == ControlledJob.State.Success);
			NUnit.Framework.Assert.IsTrue(job3.GetJobState() == ControlledJob.State.Success);
			NUnit.Framework.Assert.IsTrue(job4.GetJobState() == ControlledJob.State.Success);
			jobControl.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailedJob()
		{
			JobControl jobControl = new JobControl("Test");
			ControlledJob job1 = CreateFailedControlledJob(jobControl);
			ControlledJob job2 = CreateSuccessfulControlledJob(jobControl);
			ControlledJob job3 = CreateSuccessfulControlledJob(jobControl, job1, job2);
			ControlledJob job4 = CreateSuccessfulControlledJob(jobControl, job3);
			RunJobControl(jobControl);
			NUnit.Framework.Assert.AreEqual("Success list", 1, jobControl.GetSuccessfulJobList
				().Count);
			NUnit.Framework.Assert.AreEqual("Failed list", 3, jobControl.GetFailedJobList().Count
				);
			NUnit.Framework.Assert.IsTrue(job1.GetJobState() == ControlledJob.State.Failed);
			NUnit.Framework.Assert.IsTrue(job2.GetJobState() == ControlledJob.State.Success);
			NUnit.Framework.Assert.IsTrue(job3.GetJobState() == ControlledJob.State.DependentFailed
				);
			NUnit.Framework.Assert.IsTrue(job4.GetJobState() == ControlledJob.State.DependentFailed
				);
			jobControl.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestErrorWhileSubmitting()
		{
			JobControl jobControl = new JobControl("Test");
			Job mockJob = Org.Mockito.Mockito.Mock<Job>();
			ControlledJob job1 = new ControlledJob(mockJob, null);
			Org.Mockito.Mockito.When(mockJob.GetConfiguration()).ThenReturn(new Configuration
				());
			Org.Mockito.Mockito.DoThrow(new IncompatibleClassChangeError("This is a test")).When
				(mockJob).Submit();
			jobControl.AddJob(job1);
			RunJobControl(jobControl);
			try
			{
				NUnit.Framework.Assert.AreEqual("Success list", 0, jobControl.GetSuccessfulJobList
					().Count);
				NUnit.Framework.Assert.AreEqual("Failed list", 1, jobControl.GetFailedJobList().Count
					);
				NUnit.Framework.Assert.IsTrue(job1.GetJobState() == ControlledJob.State.Failed);
			}
			finally
			{
				jobControl.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillJob()
		{
			JobControl jobControl = new JobControl("Test");
			ControlledJob job = CreateFailedControlledJob(jobControl);
			job.KillJob();
			// Verify that killJob() was called on the mock Job
			Org.Mockito.Mockito.Verify(job.GetJob()).KillJob();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private Job CreateJob(bool complete, bool successful)
		{
			// Create a stub Job that responds in a controlled way
			Job mockJob = Org.Mockito.Mockito.Mock<Job>();
			Org.Mockito.Mockito.When(mockJob.GetConfiguration()).ThenReturn(new Configuration
				());
			Org.Mockito.Mockito.When(mockJob.IsComplete()).ThenReturn(complete);
			Org.Mockito.Mockito.When(mockJob.IsSuccessful()).ThenReturn(successful);
			return mockJob;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private ControlledJob CreateControlledJob(JobControl jobControl, bool successful, 
			params ControlledJob[] dependingJobs)
		{
			IList<ControlledJob> dependingJobsList = dependingJobs == null ? null : Arrays.AsList
				(dependingJobs);
			ControlledJob job = new ControlledJob(CreateJob(true, successful), dependingJobsList
				);
			jobControl.AddJob(job);
			return job;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private ControlledJob CreateSuccessfulControlledJob(JobControl jobControl, params 
			ControlledJob[] dependingJobs)
		{
			return CreateControlledJob(jobControl, true, dependingJobs);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private ControlledJob CreateFailedControlledJob(JobControl jobControl, params ControlledJob
			[] dependingJobs)
		{
			return CreateControlledJob(jobControl, false, dependingJobs);
		}

		private void RunJobControl(JobControl jobControl)
		{
			Sharpen.Thread controller = new Sharpen.Thread(jobControl);
			controller.Start();
			WaitTillAllFinished(jobControl);
		}

		private void WaitTillAllFinished(JobControl jobControl)
		{
			while (!jobControl.AllFinished())
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
		// ignore
	}
}
