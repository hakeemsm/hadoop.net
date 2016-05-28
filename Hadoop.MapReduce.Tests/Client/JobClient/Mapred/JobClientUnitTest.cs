using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Mapreduce;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class JobClientUnitTest
	{
		public class TestJobClient : JobClient
		{
			/// <exception cref="System.IO.IOException"/>
			internal TestJobClient(JobClientUnitTest _enclosing, JobConf jobConf)
				: base(jobConf)
			{
				this._enclosing = _enclosing;
			}

			internal virtual void SetCluster(Cluster cluster)
			{
				this.cluster = cluster;
			}

			private readonly JobClientUnitTest _enclosing;
		}

		public class TestJobClientGetJob : JobClientUnitTest.TestJobClient
		{
			internal int lastGetJobRetriesCounter = 0;

			internal int getJobRetriesCounter = 0;

			internal int getJobRetries = 0;

			internal RunningJob runningJob;

			/// <exception cref="System.IO.IOException"/>
			internal TestJobClientGetJob(JobClientUnitTest _enclosing, JobConf jobConf)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			public virtual int GetLastGetJobRetriesCounter()
			{
				return this.lastGetJobRetriesCounter;
			}

			public virtual void SetGetJobRetries(int getJobRetries)
			{
				this.getJobRetries = getJobRetries;
			}

			public virtual void SetRunningJob(RunningJob runningJob)
			{
				this.runningJob = runningJob;
			}

			/// <exception cref="System.IO.IOException"/>
			protected override RunningJob GetJobInner(JobID jobid)
			{
				if (this.getJobRetriesCounter >= this.getJobRetries)
				{
					this.lastGetJobRetriesCounter = this.getJobRetriesCounter;
					this.getJobRetriesCounter = 0;
					return this.runningJob;
				}
				this.getJobRetriesCounter++;
				return null;
			}

			private readonly JobClientUnitTest _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMapTaskReportsWithNullJob()
		{
			JobClientUnitTest.TestJobClient client = new JobClientUnitTest.TestJobClient(this
				, new JobConf());
			Cluster mockCluster = Org.Mockito.Mockito.Mock<Cluster>();
			client.SetCluster(mockCluster);
			JobID id = new JobID("test", 0);
			Org.Mockito.Mockito.When(mockCluster.GetJob(id)).ThenReturn(null);
			TaskReport[] result = client.GetMapTaskReports(id);
			NUnit.Framework.Assert.AreEqual(0, result.Length);
			Org.Mockito.Mockito.Verify(mockCluster).GetJob(id);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReduceTaskReportsWithNullJob()
		{
			JobClientUnitTest.TestJobClient client = new JobClientUnitTest.TestJobClient(this
				, new JobConf());
			Cluster mockCluster = Org.Mockito.Mockito.Mock<Cluster>();
			client.SetCluster(mockCluster);
			JobID id = new JobID("test", 0);
			Org.Mockito.Mockito.When(mockCluster.GetJob(id)).ThenReturn(null);
			TaskReport[] result = client.GetReduceTaskReports(id);
			NUnit.Framework.Assert.AreEqual(0, result.Length);
			Org.Mockito.Mockito.Verify(mockCluster).GetJob(id);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetupTaskReportsWithNullJob()
		{
			JobClientUnitTest.TestJobClient client = new JobClientUnitTest.TestJobClient(this
				, new JobConf());
			Cluster mockCluster = Org.Mockito.Mockito.Mock<Cluster>();
			client.SetCluster(mockCluster);
			JobID id = new JobID("test", 0);
			Org.Mockito.Mockito.When(mockCluster.GetJob(id)).ThenReturn(null);
			TaskReport[] result = client.GetSetupTaskReports(id);
			NUnit.Framework.Assert.AreEqual(0, result.Length);
			Org.Mockito.Mockito.Verify(mockCluster).GetJob(id);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCleanupTaskReportsWithNullJob()
		{
			JobClientUnitTest.TestJobClient client = new JobClientUnitTest.TestJobClient(this
				, new JobConf());
			Cluster mockCluster = Org.Mockito.Mockito.Mock<Cluster>();
			client.SetCluster(mockCluster);
			JobID id = new JobID("test", 0);
			Org.Mockito.Mockito.When(mockCluster.GetJob(id)).ThenReturn(null);
			TaskReport[] result = client.GetCleanupTaskReports(id);
			NUnit.Framework.Assert.AreEqual(0, result.Length);
			Org.Mockito.Mockito.Verify(mockCluster).GetJob(id);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShowJob()
		{
			JobClientUnitTest.TestJobClient client = new JobClientUnitTest.TestJobClient(this
				, new JobConf());
			long startTime = Runtime.CurrentTimeMillis();
			JobID jobID = new JobID(startTime.ToString(), 12345);
			JobStatus mockJobStatus = Org.Mockito.Mockito.Mock<JobStatus>();
			Org.Mockito.Mockito.When(mockJobStatus.GetJobID()).ThenReturn(jobID);
			Org.Mockito.Mockito.When(mockJobStatus.GetJobName()).ThenReturn(jobID.ToString());
			Org.Mockito.Mockito.When(mockJobStatus.GetState()).ThenReturn(JobStatus.State.Running
				);
			Org.Mockito.Mockito.When(mockJobStatus.GetStartTime()).ThenReturn(startTime);
			Org.Mockito.Mockito.When(mockJobStatus.GetUsername()).ThenReturn("mockuser");
			Org.Mockito.Mockito.When(mockJobStatus.GetQueue()).ThenReturn("mockqueue");
			Org.Mockito.Mockito.When(mockJobStatus.GetPriority()).ThenReturn(JobPriority.Normal
				);
			Org.Mockito.Mockito.When(mockJobStatus.GetNumUsedSlots()).ThenReturn(1);
			Org.Mockito.Mockito.When(mockJobStatus.GetNumReservedSlots()).ThenReturn(1);
			Org.Mockito.Mockito.When(mockJobStatus.GetUsedMem()).ThenReturn(1024);
			Org.Mockito.Mockito.When(mockJobStatus.GetReservedMem()).ThenReturn(512);
			Org.Mockito.Mockito.When(mockJobStatus.GetNeededMem()).ThenReturn(2048);
			Org.Mockito.Mockito.When(mockJobStatus.GetSchedulingInfo()).ThenReturn("NA");
			Job mockJob = Org.Mockito.Mockito.Mock<Job>();
			Org.Mockito.Mockito.When(mockJob.GetTaskReports(Matchers.IsA<TaskType>())).ThenReturn
				(new TaskReport[5]);
			Cluster mockCluster = Org.Mockito.Mockito.Mock<Cluster>();
			Org.Mockito.Mockito.When(mockCluster.GetJob(jobID)).ThenReturn(mockJob);
			client.SetCluster(mockCluster);
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			client.DisplayJobList(new JobStatus[] { mockJobStatus }, new PrintWriter(@out));
			string commandLineOutput = @out.ToString();
			System.Console.Out.WriteLine(commandLineOutput);
			NUnit.Framework.Assert.IsTrue(commandLineOutput.Contains("Total jobs:1"));
			Org.Mockito.Mockito.Verify(mockJobStatus, Org.Mockito.Mockito.AtLeastOnce()).GetJobID
				();
			Org.Mockito.Mockito.Verify(mockJobStatus).GetState();
			Org.Mockito.Mockito.Verify(mockJobStatus).GetStartTime();
			Org.Mockito.Mockito.Verify(mockJobStatus).GetUsername();
			Org.Mockito.Mockito.Verify(mockJobStatus).GetQueue();
			Org.Mockito.Mockito.Verify(mockJobStatus).GetPriority();
			Org.Mockito.Mockito.Verify(mockJobStatus).GetNumUsedSlots();
			Org.Mockito.Mockito.Verify(mockJobStatus).GetNumReservedSlots();
			Org.Mockito.Mockito.Verify(mockJobStatus).GetUsedMem();
			Org.Mockito.Mockito.Verify(mockJobStatus).GetReservedMem();
			Org.Mockito.Mockito.Verify(mockJobStatus).GetNeededMem();
			Org.Mockito.Mockito.Verify(mockJobStatus).GetSchedulingInfo();
			// This call should not go to each AM.
			Org.Mockito.Mockito.Verify(mockCluster, Org.Mockito.Mockito.Never()).GetJob(jobID
				);
			Org.Mockito.Mockito.Verify(mockJob, Org.Mockito.Mockito.Never()).GetTaskReports(Matchers.IsA
				<TaskType>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetJobWithUnknownJob()
		{
			JobClientUnitTest.TestJobClient client = new JobClientUnitTest.TestJobClient(this
				, new JobConf());
			Cluster mockCluster = Org.Mockito.Mockito.Mock<Cluster>();
			client.SetCluster(mockCluster);
			JobID id = new JobID("unknown", 0);
			Org.Mockito.Mockito.When(mockCluster.GetJob(id)).ThenReturn(null);
			NUnit.Framework.Assert.IsNull(client.GetJob(id));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetJobRetry()
		{
			//To prevent the test from running for a very long time, lower the retry
			JobConf conf = new JobConf();
			conf.Set(MRJobConfig.MrClientJobMaxRetries, "3");
			JobClientUnitTest.TestJobClientGetJob client = new JobClientUnitTest.TestJobClientGetJob
				(this, conf);
			JobID id = new JobID("ajob", 1);
			RunningJob rj = Org.Mockito.Mockito.Mock<RunningJob>();
			client.SetRunningJob(rj);
			//no retry
			NUnit.Framework.Assert.IsNotNull(client.GetJob(id));
			NUnit.Framework.Assert.AreEqual(client.GetLastGetJobRetriesCounter(), 0);
			//3 retry
			client.SetGetJobRetries(3);
			NUnit.Framework.Assert.IsNotNull(client.GetJob(id));
			NUnit.Framework.Assert.AreEqual(client.GetLastGetJobRetriesCounter(), 3);
			//beyond MAPREDUCE_JOBCLIENT_GETJOB_MAX_RETRY_KEY, will get null
			client.SetGetJobRetries(5);
			NUnit.Framework.Assert.IsNull(client.GetJob(id));
		}
	}
}
