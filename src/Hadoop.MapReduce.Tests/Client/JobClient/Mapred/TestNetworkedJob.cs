using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token.Delegation;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestNetworkedJob
	{
		private static string TestRootDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp")).ToURI().ToString().Replace(' ', '+');

		private static Path testDir = new Path(TestRootDir + "/test_mini_mr_local");

		private static Path inFile = new Path(testDir, "in");

		private static Path outDir = new Path(testDir, "out");

		/// <exception cref="System.Exception"/>
		public virtual void TestGetNullCounters()
		{
			//mock creation
			Job mockJob = Org.Mockito.Mockito.Mock<Job>();
			RunningJob underTest = new JobClient.NetworkedJob(mockJob);
			Org.Mockito.Mockito.When(mockJob.GetCounters()).ThenReturn(null);
			NUnit.Framework.Assert.IsNull(underTest.GetCounters());
			//verification
			Org.Mockito.Mockito.Verify(mockJob).GetCounters();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void TestGetJobStatus()
		{
			MiniMRClientCluster mr = null;
			FileSystem fileSys = null;
			try
			{
				mr = CreateMiniClusterWithCapacityScheduler();
				JobConf job = new JobConf(mr.GetConfig());
				fileSys = FileSystem.Get(job);
				fileSys.Delete(testDir, true);
				FSDataOutputStream @out = fileSys.Create(inFile, true);
				@out.WriteBytes("This is a test file");
				@out.Close();
				FileInputFormat.SetInputPaths(job, inFile);
				FileOutputFormat.SetOutputPath(job, outDir);
				job.SetInputFormat(typeof(TextInputFormat));
				job.SetOutputFormat(typeof(TextOutputFormat));
				job.SetMapperClass(typeof(IdentityMapper));
				job.SetReducerClass(typeof(IdentityReducer));
				job.SetNumReduceTasks(0);
				JobClient client = new JobClient(mr.GetConfig());
				RunningJob rj = client.SubmitJob(job);
				JobID jobId = rj.GetID();
				// The following asserts read JobStatus twice and ensure the returned
				// JobStatus objects correspond to the same Job.
				NUnit.Framework.Assert.AreEqual("Expected matching JobIDs", jobId, ((JobID)client
					.GetJob(jobId).GetJobStatus().GetJobID()));
				NUnit.Framework.Assert.AreEqual("Expected matching startTimes", rj.GetJobStatus()
					.GetStartTime(), client.GetJob(jobId).GetJobStatus().GetStartTime());
			}
			finally
			{
				if (fileSys != null)
				{
					fileSys.Delete(testDir, true);
				}
				if (mr != null)
				{
					mr.Stop();
				}
			}
		}

		/// <summary>test JobConf</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestNetworkedJob()
		{
			// mock creation
			MiniMRClientCluster mr = null;
			FileSystem fileSys = null;
			try
			{
				mr = CreateMiniClusterWithCapacityScheduler();
				JobConf job = new JobConf(mr.GetConfig());
				fileSys = FileSystem.Get(job);
				fileSys.Delete(testDir, true);
				FSDataOutputStream @out = fileSys.Create(inFile, true);
				@out.WriteBytes("This is a test file");
				@out.Close();
				FileInputFormat.SetInputPaths(job, inFile);
				FileOutputFormat.SetOutputPath(job, outDir);
				job.SetInputFormat(typeof(TextInputFormat));
				job.SetOutputFormat(typeof(TextOutputFormat));
				job.SetMapperClass(typeof(IdentityMapper));
				job.SetReducerClass(typeof(IdentityReducer));
				job.SetNumReduceTasks(0);
				JobClient client = new JobClient(mr.GetConfig());
				RunningJob rj = client.SubmitJob(job);
				JobID jobId = rj.GetID();
				JobClient.NetworkedJob runningJob = (JobClient.NetworkedJob)client.GetJob(jobId);
				runningJob.SetJobPriority(JobPriority.High.ToString());
				// test getters
				NUnit.Framework.Assert.IsTrue(runningJob.GetConfiguration().ToString().EndsWith("0001/job.xml"
					));
				NUnit.Framework.Assert.AreEqual(runningJob.GetID(), jobId);
				NUnit.Framework.Assert.AreEqual(runningJob.GetJobID(), jobId.ToString());
				NUnit.Framework.Assert.AreEqual(runningJob.GetJobName(), "N/A");
				NUnit.Framework.Assert.IsTrue(runningJob.GetJobFile().EndsWith(".staging/" + runningJob
					.GetJobID() + "/job.xml"));
				NUnit.Framework.Assert.IsTrue(runningJob.GetTrackingURL().Length > 0);
				NUnit.Framework.Assert.IsTrue(runningJob.MapProgress() == 0.0f);
				NUnit.Framework.Assert.IsTrue(runningJob.ReduceProgress() == 0.0f);
				NUnit.Framework.Assert.IsTrue(runningJob.CleanupProgress() == 0.0f);
				NUnit.Framework.Assert.IsTrue(runningJob.SetupProgress() == 0.0f);
				TaskCompletionEvent[] tce = runningJob.GetTaskCompletionEvents(0);
				NUnit.Framework.Assert.AreEqual(tce.Length, 0);
				NUnit.Framework.Assert.AreEqual(runningJob.GetHistoryUrl(), string.Empty);
				NUnit.Framework.Assert.IsFalse(runningJob.IsRetired());
				NUnit.Framework.Assert.AreEqual(runningJob.GetFailureInfo(), string.Empty);
				NUnit.Framework.Assert.AreEqual(runningJob.GetJobStatus().GetJobName(), "N/A");
				NUnit.Framework.Assert.AreEqual(client.GetMapTaskReports(jobId).Length, 0);
				try
				{
					client.GetSetupTaskReports(jobId);
				}
				catch (YarnRuntimeException e)
				{
					NUnit.Framework.Assert.AreEqual(e.Message, "Unrecognized task type: JOB_SETUP");
				}
				try
				{
					client.GetCleanupTaskReports(jobId);
				}
				catch (YarnRuntimeException e)
				{
					NUnit.Framework.Assert.AreEqual(e.Message, "Unrecognized task type: JOB_CLEANUP");
				}
				NUnit.Framework.Assert.AreEqual(client.GetReduceTaskReports(jobId).Length, 0);
				// test ClusterStatus
				ClusterStatus status = client.GetClusterStatus(true);
				NUnit.Framework.Assert.AreEqual(status.GetActiveTrackerNames().Count, 2);
				// it method does not implemented and always return empty array or null;
				NUnit.Framework.Assert.AreEqual(status.GetBlacklistedTrackers(), 0);
				NUnit.Framework.Assert.AreEqual(status.GetBlacklistedTrackerNames().Count, 0);
				NUnit.Framework.Assert.AreEqual(status.GetBlackListedTrackersInfo().Count, 0);
				NUnit.Framework.Assert.AreEqual(status.GetJobTrackerStatus(), Cluster.JobTrackerStatus
					.Running);
				NUnit.Framework.Assert.AreEqual(status.GetMapTasks(), 1);
				NUnit.Framework.Assert.AreEqual(status.GetMaxMapTasks(), 20);
				NUnit.Framework.Assert.AreEqual(status.GetMaxReduceTasks(), 4);
				NUnit.Framework.Assert.AreEqual(status.GetNumExcludedNodes(), 0);
				NUnit.Framework.Assert.AreEqual(status.GetReduceTasks(), 1);
				NUnit.Framework.Assert.AreEqual(status.GetTaskTrackers(), 2);
				NUnit.Framework.Assert.AreEqual(status.GetTTExpiryInterval(), 0);
				NUnit.Framework.Assert.AreEqual(status.GetJobTrackerStatus(), Cluster.JobTrackerStatus
					.Running);
				NUnit.Framework.Assert.AreEqual(status.GetGraylistedTrackers(), 0);
				// test read and write
				ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
				status.Write(new DataOutputStream(dataOut));
				ClusterStatus status2 = new ClusterStatus();
				status2.ReadFields(new DataInputStream(new ByteArrayInputStream(dataOut.ToByteArray
					())));
				NUnit.Framework.Assert.AreEqual(status.GetActiveTrackerNames(), status2.GetActiveTrackerNames
					());
				NUnit.Framework.Assert.AreEqual(status.GetBlackListedTrackersInfo(), status2.GetBlackListedTrackersInfo
					());
				NUnit.Framework.Assert.AreEqual(status.GetMapTasks(), status2.GetMapTasks());
				try
				{
				}
				catch (RuntimeException e)
				{
					NUnit.Framework.Assert.IsTrue(e.Message.EndsWith("not found on CLASSPATH"));
				}
				// test taskStatusfilter
				JobClient.SetTaskOutputFilter(job, JobClient.TaskStatusFilter.All);
				NUnit.Framework.Assert.AreEqual(JobClient.GetTaskOutputFilter(job), JobClient.TaskStatusFilter
					.All);
				// runningJob.setJobPriority(JobPriority.HIGH.name());
				// test default map
				NUnit.Framework.Assert.AreEqual(client.GetDefaultMaps(), 20);
				NUnit.Framework.Assert.AreEqual(client.GetDefaultReduces(), 4);
				NUnit.Framework.Assert.AreEqual(client.GetSystemDir().GetName(), "jobSubmitDir");
				// test queue information
				JobQueueInfo[] rootQueueInfo = client.GetRootQueues();
				NUnit.Framework.Assert.AreEqual(rootQueueInfo.Length, 1);
				NUnit.Framework.Assert.AreEqual(rootQueueInfo[0].GetQueueName(), "default");
				JobQueueInfo[] qinfo = client.GetQueues();
				NUnit.Framework.Assert.AreEqual(qinfo.Length, 1);
				NUnit.Framework.Assert.AreEqual(qinfo[0].GetQueueName(), "default");
				NUnit.Framework.Assert.AreEqual(client.GetChildQueues("default").Length, 0);
				NUnit.Framework.Assert.AreEqual(client.GetJobsFromQueue("default").Length, 1);
				NUnit.Framework.Assert.IsTrue(client.GetJobsFromQueue("default")[0].GetJobFile().
					EndsWith("/job.xml"));
				JobQueueInfo qi = client.GetQueueInfo("default");
				NUnit.Framework.Assert.AreEqual(qi.GetQueueName(), "default");
				NUnit.Framework.Assert.AreEqual(qi.GetQueueState(), "running");
				QueueAclsInfo[] aai = client.GetQueueAclsForCurrentUser();
				NUnit.Framework.Assert.AreEqual(aai.Length, 2);
				NUnit.Framework.Assert.AreEqual(aai[0].GetQueueName(), "root");
				NUnit.Framework.Assert.AreEqual(aai[1].GetQueueName(), "default");
				// test token
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = client.
					GetDelegationToken(new Text(UserGroupInformation.GetCurrentUser().GetShortUserName
					()));
				NUnit.Framework.Assert.AreEqual(token.GetKind().ToString(), "RM_DELEGATION_TOKEN"
					);
				// test JobClient
				// The following asserts read JobStatus twice and ensure the returned
				// JobStatus objects correspond to the same Job.
				NUnit.Framework.Assert.AreEqual("Expected matching JobIDs", jobId, ((JobID)client
					.GetJob(jobId).GetJobStatus().GetJobID()));
				NUnit.Framework.Assert.AreEqual("Expected matching startTimes", rj.GetJobStatus()
					.GetStartTime(), client.GetJob(jobId).GetJobStatus().GetStartTime());
			}
			finally
			{
				if (fileSys != null)
				{
					fileSys.Delete(testDir, true);
				}
				if (mr != null)
				{
					mr.Stop();
				}
			}
		}

		/// <summary>test BlackListInfo class</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBlackListInfo()
		{
			ClusterStatus.BlackListInfo info = new ClusterStatus.BlackListInfo();
			info.SetBlackListReport("blackListInfo");
			info.SetReasonForBlackListing("reasonForBlackListing");
			info.SetTrackerName("trackerName");
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			DataOutput @out = new DataOutputStream(byteOut);
			info.Write(@out);
			ClusterStatus.BlackListInfo info2 = new ClusterStatus.BlackListInfo();
			info2.ReadFields(new DataInputStream(new ByteArrayInputStream(byteOut.ToByteArray
				())));
			NUnit.Framework.Assert.AreEqual(info, info);
			NUnit.Framework.Assert.AreEqual(info.ToString(), info.ToString());
			NUnit.Framework.Assert.AreEqual(info.GetTrackerName(), "trackerName");
			NUnit.Framework.Assert.AreEqual(info.GetReasonForBlackListing(), "reasonForBlackListing"
				);
			NUnit.Framework.Assert.AreEqual(info.GetBlackListReport(), "blackListInfo");
		}

		/// <summary>test run from command line JobQueueClient</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestJobQueueClient()
		{
			MiniMRClientCluster mr = null;
			FileSystem fileSys = null;
			TextWriter oldOut = System.Console.Out;
			try
			{
				mr = CreateMiniClusterWithCapacityScheduler();
				JobConf job = new JobConf(mr.GetConfig());
				fileSys = FileSystem.Get(job);
				fileSys.Delete(testDir, true);
				FSDataOutputStream @out = fileSys.Create(inFile, true);
				@out.WriteBytes("This is a test file");
				@out.Close();
				FileInputFormat.SetInputPaths(job, inFile);
				FileOutputFormat.SetOutputPath(job, outDir);
				job.SetInputFormat(typeof(TextInputFormat));
				job.SetOutputFormat(typeof(TextOutputFormat));
				job.SetMapperClass(typeof(IdentityMapper));
				job.SetReducerClass(typeof(IdentityReducer));
				job.SetNumReduceTasks(0);
				JobClient client = new JobClient(mr.GetConfig());
				client.SubmitJob(job);
				JobQueueClient jobClient = new JobQueueClient(job);
				ByteArrayOutputStream bytes = new ByteArrayOutputStream();
				Runtime.SetOut(new TextWriter(bytes));
				string[] arg = new string[] { "-list" };
				jobClient.Run(arg);
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("Queue Name : default"));
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("Queue State : running"));
				bytes = new ByteArrayOutputStream();
				Runtime.SetOut(new TextWriter(bytes));
				string[] arg1 = new string[] { "-showacls" };
				jobClient.Run(arg1);
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("Queue acls for user :"));
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("root  ADMINISTER_QUEUE,SUBMIT_APPLICATIONS"
					));
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("default  ADMINISTER_QUEUE,SUBMIT_APPLICATIONS"
					));
				// test for info and default queue
				bytes = new ByteArrayOutputStream();
				Runtime.SetOut(new TextWriter(bytes));
				string[] arg2 = new string[] { "-info", "default" };
				jobClient.Run(arg2);
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("Queue Name : default"));
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("Queue State : running"));
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("Scheduling Info"));
				// test for info , default queue and jobs
				bytes = new ByteArrayOutputStream();
				Runtime.SetOut(new TextWriter(bytes));
				string[] arg3 = new string[] { "-info", "default", "-showJobs" };
				jobClient.Run(arg3);
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("Queue Name : default"));
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("Queue State : running"));
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("Scheduling Info"));
				NUnit.Framework.Assert.IsTrue(bytes.ToString().Contains("job_1"));
				string[] arg4 = new string[] {  };
				jobClient.Run(arg4);
			}
			finally
			{
				Runtime.SetOut(oldOut);
				if (fileSys != null)
				{
					fileSys.Delete(testDir, true);
				}
				if (mr != null)
				{
					mr.Stop();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private MiniMRClientCluster CreateMiniClusterWithCapacityScheduler()
		{
			Configuration conf = new Configuration();
			// Expected queue names depending on Capacity Scheduler queue naming
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(CapacityScheduler
				));
			return MiniMRClientClusterFactory.Create(this.GetType(), 2, conf);
		}
	}
}
