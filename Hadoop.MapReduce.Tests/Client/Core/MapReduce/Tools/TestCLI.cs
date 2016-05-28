using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Tools
{
	public class TestCLI
	{
		private static string jobIdStr = "job_1015298225799_0015";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListAttemptIdsWithValidInput()
		{
			JobID jobId = JobID.ForName(jobIdStr);
			Cluster mockCluster = Org.Mockito.Mockito.Mock<Cluster>();
			Job job = Org.Mockito.Mockito.Mock<Job>();
			CLI cli = Org.Mockito.Mockito.Spy(new CLI());
			Org.Mockito.Mockito.DoReturn(mockCluster).When(cli).CreateCluster();
			Org.Mockito.Mockito.When(job.GetTaskReports(TaskType.Map)).ThenReturn(GetTaskReports
				(jobId, TaskType.Map));
			Org.Mockito.Mockito.When(job.GetTaskReports(TaskType.Reduce)).ThenReturn(GetTaskReports
				(jobId, TaskType.Reduce));
			Org.Mockito.Mockito.When(mockCluster.GetJob(jobId)).ThenReturn(job);
			int retCode_MAP = cli.Run(new string[] { "-list-attempt-ids", jobIdStr, "MAP", "running"
				 });
			// testing case insensitive behavior
			int retCode_map = cli.Run(new string[] { "-list-attempt-ids", jobIdStr, "map", "running"
				 });
			int retCode_REDUCE = cli.Run(new string[] { "-list-attempt-ids", jobIdStr, "REDUCE"
				, "running" });
			int retCode_completed = cli.Run(new string[] { "-list-attempt-ids", jobIdStr, "REDUCE"
				, "completed" });
			NUnit.Framework.Assert.AreEqual("MAP is a valid input,exit code should be 0", 0, 
				retCode_MAP);
			NUnit.Framework.Assert.AreEqual("map is a valid input,exit code should be 0", 0, 
				retCode_map);
			NUnit.Framework.Assert.AreEqual("REDUCE is a valid input,exit code should be 0", 
				0, retCode_REDUCE);
			NUnit.Framework.Assert.AreEqual("REDUCE and completed are a valid inputs to -list-attempt-ids,exit code should be 0"
				, 0, retCode_completed);
			Org.Mockito.Mockito.Verify(job, Org.Mockito.Mockito.Times(2)).GetTaskReports(TaskType
				.Map);
			Org.Mockito.Mockito.Verify(job, Org.Mockito.Mockito.Times(2)).GetTaskReports(TaskType
				.Reduce);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListAttemptIdsWithInvalidInputs()
		{
			JobID jobId = JobID.ForName(jobIdStr);
			Cluster mockCluster = Org.Mockito.Mockito.Mock<Cluster>();
			Job job = Org.Mockito.Mockito.Mock<Job>();
			CLI cli = Org.Mockito.Mockito.Spy(new CLI());
			Org.Mockito.Mockito.DoReturn(mockCluster).When(cli).CreateCluster();
			Org.Mockito.Mockito.When(mockCluster.GetJob(jobId)).ThenReturn(job);
			int retCode_JOB_SETUP = cli.Run(new string[] { "-list-attempt-ids", jobIdStr, "JOB_SETUP"
				, "running" });
			int retCode_JOB_CLEANUP = cli.Run(new string[] { "-list-attempt-ids", jobIdStr, "JOB_CLEANUP"
				, "running" });
			int retCode_invalidTaskState = cli.Run(new string[] { "-list-attempt-ids", jobIdStr
				, "REDUCE", "complete" });
			NUnit.Framework.Assert.AreEqual("JOB_SETUP is an invalid input,exit code should be -1"
				, -1, retCode_JOB_SETUP);
			NUnit.Framework.Assert.AreEqual("JOB_CLEANUP is an invalid input,exit code should be -1"
				, -1, retCode_JOB_CLEANUP);
			NUnit.Framework.Assert.AreEqual("complete is an invalid input,exit code should be -1"
				, -1, retCode_invalidTaskState);
		}

		private TaskReport[] GetTaskReports(JobID jobId, TaskType type)
		{
			return new TaskReport[] { new TaskReport(), new TaskReport() };
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobKIll()
		{
			Cluster mockCluster = Org.Mockito.Mockito.Mock<Cluster>();
			CLI cli = Org.Mockito.Mockito.Spy(new CLI());
			Org.Mockito.Mockito.DoReturn(mockCluster).When(cli).CreateCluster();
			string jobId1 = "job_1234654654_001";
			string jobId2 = "job_1234654654_002";
			string jobId3 = "job_1234654654_003";
			string jobId4 = "job_1234654654_004";
			Job mockJob1 = MockJob(mockCluster, jobId1, JobStatus.State.Running);
			Job mockJob2 = MockJob(mockCluster, jobId2, JobStatus.State.Killed);
			Job mockJob3 = MockJob(mockCluster, jobId3, JobStatus.State.Failed);
			Job mockJob4 = MockJob(mockCluster, jobId4, JobStatus.State.Prep);
			int exitCode1 = cli.Run(new string[] { "-kill", jobId1 });
			NUnit.Framework.Assert.AreEqual(0, exitCode1);
			Org.Mockito.Mockito.Verify(mockJob1, Org.Mockito.Mockito.Times(1)).KillJob();
			int exitCode2 = cli.Run(new string[] { "-kill", jobId2 });
			NUnit.Framework.Assert.AreEqual(-1, exitCode2);
			Org.Mockito.Mockito.Verify(mockJob2, Org.Mockito.Mockito.Times(0)).KillJob();
			int exitCode3 = cli.Run(new string[] { "-kill", jobId3 });
			NUnit.Framework.Assert.AreEqual(-1, exitCode3);
			Org.Mockito.Mockito.Verify(mockJob3, Org.Mockito.Mockito.Times(0)).KillJob();
			int exitCode4 = cli.Run(new string[] { "-kill", jobId4 });
			NUnit.Framework.Assert.AreEqual(0, exitCode4);
			Org.Mockito.Mockito.Verify(mockJob4, Org.Mockito.Mockito.Times(1)).KillJob();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private Job MockJob(Cluster mockCluster, string jobId, JobStatus.State jobState)
		{
			Job mockJob = Org.Mockito.Mockito.Mock<Job>();
			Org.Mockito.Mockito.When(mockCluster.GetJob(JobID.ForName(jobId))).ThenReturn(mockJob
				);
			JobStatus status = new JobStatus(null, 0, 0, 0, 0, jobState, JobPriority.High, null
				, null, null, null);
			Org.Mockito.Mockito.When(mockJob.GetStatus()).ThenReturn(status);
			return mockJob;
		}
	}
}
