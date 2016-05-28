using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Commit;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Metrics;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.State;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl
{
	/// <summary>Tests various functions of the JobImpl class</summary>
	public class TestJobImpl
	{
		internal static string stagingDir = "target/test-staging/";

		[BeforeClass]
		public static void Setup()
		{
			FilePath dir = new FilePath(stagingDir);
			stagingDir = dir.GetAbsolutePath();
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Cleanup()
		{
			FilePath dir = new FilePath(stagingDir);
			if (dir.Exists())
			{
				FileUtils.DeleteDirectory(dir);
			}
			dir.Mkdirs();
		}

		[NUnit.Framework.Test]
		public virtual void TestJobNoTasks()
		{
			Configuration conf = new Configuration();
			conf.SetInt(MRJobConfig.NumReduces, 0);
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			conf.Set(MRJobConfig.WorkflowId, "testId");
			conf.Set(MRJobConfig.WorkflowName, "testName");
			conf.Set(MRJobConfig.WorkflowNodeName, "testNodeName");
			conf.Set(MRJobConfig.WorkflowAdjacencyPrefixString + "key1", "value1");
			conf.Set(MRJobConfig.WorkflowAdjacencyPrefixString + "key2", "value2");
			conf.Set(MRJobConfig.WorkflowTags, "tag1,tag2");
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			OutputCommitter committer = Org.Mockito.Mockito.Mock<OutputCommitter>();
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			TestJobImpl.JobSubmittedEventHandler jseHandler = new TestJobImpl.JobSubmittedEventHandler
				("testId", "testName", "testNodeName", "\"key2\"=\"value2\" \"key1\"=\"value1\" "
				, "tag1,tag2");
			dispatcher.Register(typeof(EventType), jseHandler);
			JobImpl job = CreateStubbedJob(conf, dispatcher, 0, null);
			job.Handle(new JobEvent(job.GetID(), JobEventType.JobInit));
			AssertJobState(job, JobStateInternal.Inited);
			job.Handle(new JobStartEvent(job.GetID()));
			AssertJobState(job, JobStateInternal.Succeeded);
			dispatcher.Stop();
			commitHandler.Stop();
			try
			{
				NUnit.Framework.Assert.IsTrue(jseHandler.GetAssertValue());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Workflow related attributes are not tested properly"
					);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommitJobFailsJob()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			CyclicBarrier syncBarrier = new CyclicBarrier(2);
			OutputCommitter committer = new TestJobImpl.TestingOutputCommitter(syncBarrier, false
				);
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			JobImpl job = CreateRunningStubbedJob(conf, dispatcher, 2, null);
			CompleteJobTasks(job);
			AssertJobState(job, JobStateInternal.Committing);
			// let the committer fail and verify the job fails
			syncBarrier.Await();
			AssertJobState(job, JobStateInternal.Failed);
			dispatcher.Stop();
			commitHandler.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckJobCompleteSuccess()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			CyclicBarrier syncBarrier = new CyclicBarrier(2);
			OutputCommitter committer = new TestJobImpl.TestingOutputCommitter(syncBarrier, true
				);
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			JobImpl job = CreateRunningStubbedJob(conf, dispatcher, 2, null);
			CompleteJobTasks(job);
			AssertJobState(job, JobStateInternal.Committing);
			// let the committer complete and verify the job succeeds
			syncBarrier.Await();
			AssertJobState(job, JobStateInternal.Succeeded);
			job.Handle(new JobEvent(job.GetID(), JobEventType.JobTaskAttemptCompleted));
			AssertJobState(job, JobStateInternal.Succeeded);
			job.Handle(new JobEvent(job.GetID(), JobEventType.JobMapTaskRescheduled));
			AssertJobState(job, JobStateInternal.Succeeded);
			dispatcher.Stop();
			commitHandler.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRebootedDuringSetup()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			OutputCommitter committer = new _StubbedOutputCommitter_226();
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Mockito.Mockito.When(mockContext.IsLastAMRetry()).ThenReturn(false);
			JobImpl job = CreateStubbedJob(conf, dispatcher, 2, mockContext);
			JobId jobId = job.GetID();
			job.Handle(new JobEvent(jobId, JobEventType.JobInit));
			AssertJobState(job, JobStateInternal.Inited);
			job.Handle(new JobStartEvent(jobId));
			AssertJobState(job, JobStateInternal.Setup);
			job.Handle(new JobEvent(job.GetID(), JobEventType.JobAmReboot));
			AssertJobState(job, JobStateInternal.Reboot);
			// return the external state as RUNNING since otherwise JobClient will
			// exit when it polls the AM for job state
			NUnit.Framework.Assert.AreEqual(JobState.Running, job.GetState());
			dispatcher.Stop();
			commitHandler.Stop();
		}

		private sealed class _StubbedOutputCommitter_226 : TestJobImpl.StubbedOutputCommitter
		{
			public _StubbedOutputCommitter_226()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetupJob(JobContext jobContext)
			{
				lock (this)
				{
					while (!Sharpen.Thread.Interrupted())
					{
						try
						{
							Sharpen.Runtime.Wait(this);
						}
						catch (Exception)
						{
						}
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRebootedDuringCommit()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			conf.SetInt(MRJobConfig.MrAmMaxAttempts, 2);
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			CyclicBarrier syncBarrier = new CyclicBarrier(2);
			OutputCommitter committer = new TestJobImpl.WaitingOutputCommitter(syncBarrier, true
				);
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Mockito.Mockito.When(mockContext.IsLastAMRetry()).ThenReturn(true);
			Org.Mockito.Mockito.When(mockContext.HasSuccessfullyUnregistered()).ThenReturn(false
				);
			JobImpl job = CreateRunningStubbedJob(conf, dispatcher, 2, mockContext);
			CompleteJobTasks(job);
			AssertJobState(job, JobStateInternal.Committing);
			syncBarrier.Await();
			job.Handle(new JobEvent(job.GetID(), JobEventType.JobAmReboot));
			AssertJobState(job, JobStateInternal.Reboot);
			// return the external state as ERROR since this is last retry.
			NUnit.Framework.Assert.AreEqual(JobState.Running, job.GetState());
			Org.Mockito.Mockito.When(mockContext.HasSuccessfullyUnregistered()).ThenReturn(true
				);
			NUnit.Framework.Assert.AreEqual(JobState.Error, job.GetState());
			dispatcher.Stop();
			commitHandler.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestKilledDuringSetup()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			OutputCommitter committer = new _StubbedOutputCommitter_303();
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			JobImpl job = CreateStubbedJob(conf, dispatcher, 2, null);
			JobId jobId = job.GetID();
			job.Handle(new JobEvent(jobId, JobEventType.JobInit));
			AssertJobState(job, JobStateInternal.Inited);
			job.Handle(new JobStartEvent(jobId));
			AssertJobState(job, JobStateInternal.Setup);
			job.Handle(new JobEvent(job.GetID(), JobEventType.JobKill));
			AssertJobState(job, JobStateInternal.Killed);
			dispatcher.Stop();
			commitHandler.Stop();
		}

		private sealed class _StubbedOutputCommitter_303 : TestJobImpl.StubbedOutputCommitter
		{
			public _StubbedOutputCommitter_303()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetupJob(JobContext jobContext)
			{
				lock (this)
				{
					while (!Sharpen.Thread.Interrupted())
					{
						try
						{
							Sharpen.Runtime.Wait(this);
						}
						catch (Exception)
						{
						}
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestKilledDuringCommit()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			CyclicBarrier syncBarrier = new CyclicBarrier(2);
			OutputCommitter committer = new TestJobImpl.WaitingOutputCommitter(syncBarrier, true
				);
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			JobImpl job = CreateRunningStubbedJob(conf, dispatcher, 2, null);
			CompleteJobTasks(job);
			AssertJobState(job, JobStateInternal.Committing);
			syncBarrier.Await();
			job.Handle(new JobEvent(job.GetID(), JobEventType.JobKill));
			AssertJobState(job, JobStateInternal.Killed);
			dispatcher.Stop();
			commitHandler.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAbortJobCalledAfterKillingTasks()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			conf.Set(MRJobConfig.MrAmCommitterCancelTimeoutMs, "1000");
			InlineDispatcher dispatcher = new InlineDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			OutputCommitter committer = Org.Mockito.Mockito.Mock<OutputCommitter>();
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			JobImpl job = CreateRunningStubbedJob(conf, dispatcher, 2, null);
			//Fail one task. This should land the JobImpl in the FAIL_WAIT state
			job.Handle(new JobTaskEvent(MRBuilderUtils.NewTaskId(job.GetID(), 1, TaskType.Map
				), TaskState.Failed));
			//Verify abort job hasn't been called
			Org.Mockito.Mockito.Verify(committer, Org.Mockito.Mockito.Never()).AbortJob((JobContext
				)Org.Mockito.Mockito.Any(), (JobStatus.State)Org.Mockito.Mockito.Any());
			AssertJobState(job, JobStateInternal.FailWait);
			//Verify abortJob is called once and the job failed
			Org.Mockito.Mockito.Verify(committer, Org.Mockito.Mockito.Timeout(2000).Times(1))
				.AbortJob((JobContext)Org.Mockito.Mockito.Any(), (JobStatus.State)Org.Mockito.Mockito
				.Any());
			AssertJobState(job, JobStateInternal.Failed);
			dispatcher.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailAbortDoesntHang()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			conf.Set(MRJobConfig.MrAmCommitterCancelTimeoutMs, "1000");
			DrainDispatcher dispatcher = new DrainDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			OutputCommitter committer = Org.Mockito.Mockito.Mock<OutputCommitter>();
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			//Job has only 1 mapper task. No reducers
			conf.SetInt(MRJobConfig.NumReduces, 0);
			conf.SetInt(MRJobConfig.MapMaxAttempts, 1);
			JobImpl job = CreateRunningStubbedJob(conf, dispatcher, 1, null);
			//Fail / finish all the tasks. This should land the JobImpl directly in the
			//FAIL_ABORT state
			foreach (Task t in job.tasks.Values)
			{
				TaskImpl task = (TaskImpl)t;
				task.Handle(new TaskEvent(task.GetID(), TaskEventType.TSchedule));
				foreach (TaskAttempt ta in task.GetAttempts().Values)
				{
					task.Handle(new TaskTAttemptEvent(ta.GetID(), TaskEventType.TAttemptFailed));
				}
			}
			dispatcher.Await();
			//Verify abortJob is called once and the job failed
			Org.Mockito.Mockito.Verify(committer, Org.Mockito.Mockito.Timeout(2000).Times(1))
				.AbortJob((JobContext)Org.Mockito.Mockito.Any(), (JobStatus.State)Org.Mockito.Mockito
				.Any());
			AssertJobState(job, JobStateInternal.Failed);
			dispatcher.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestKilledDuringFailAbort()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			OutputCommitter committer = new _StubbedOutputCommitter_436();
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			JobImpl job = CreateStubbedJob(conf, dispatcher, 2, null);
			JobId jobId = job.GetID();
			job.Handle(new JobEvent(jobId, JobEventType.JobInit));
			AssertJobState(job, JobStateInternal.Inited);
			job.Handle(new JobStartEvent(jobId));
			AssertJobState(job, JobStateInternal.FailAbort);
			job.Handle(new JobEvent(jobId, JobEventType.JobKill));
			AssertJobState(job, JobStateInternal.Killed);
			dispatcher.Stop();
			commitHandler.Stop();
		}

		private sealed class _StubbedOutputCommitter_436 : TestJobImpl.StubbedOutputCommitter
		{
			public _StubbedOutputCommitter_436()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetupJob(JobContext jobContext)
			{
				throw new IOException("forced failure");
			}

			/// <exception cref="System.IO.IOException"/>
			public override void AbortJob(JobContext jobContext, JobStatus.State state)
			{
				lock (this)
				{
					while (!Sharpen.Thread.Interrupted())
					{
						try
						{
							Sharpen.Runtime.Wait(this);
						}
						catch (Exception)
						{
						}
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestKilledDuringKillAbort()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			OutputCommitter committer = new _StubbedOutputCommitter_478();
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			JobImpl job = CreateStubbedJob(conf, dispatcher, 2, null);
			JobId jobId = job.GetID();
			job.Handle(new JobEvent(jobId, JobEventType.JobInit));
			AssertJobState(job, JobStateInternal.Inited);
			job.Handle(new JobStartEvent(jobId));
			AssertJobState(job, JobStateInternal.Setup);
			job.Handle(new JobEvent(jobId, JobEventType.JobKill));
			AssertJobState(job, JobStateInternal.KillAbort);
			job.Handle(new JobEvent(jobId, JobEventType.JobKill));
			AssertJobState(job, JobStateInternal.Killed);
			dispatcher.Stop();
			commitHandler.Stop();
		}

		private sealed class _StubbedOutputCommitter_478 : TestJobImpl.StubbedOutputCommitter
		{
			public _StubbedOutputCommitter_478()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void AbortJob(JobContext jobContext, JobStatus.State state)
			{
				lock (this)
				{
					while (!Sharpen.Thread.Interrupted())
					{
						try
						{
							Sharpen.Runtime.Wait(this);
						}
						catch (Exception)
						{
						}
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestJobImpl t = new TestJobImpl();
			t.TestJobNoTasks();
			t.TestCheckJobCompleteSuccess();
			t.TestCheckAccess();
			t.TestReportDiagnostics();
			t.TestUberDecision();
		}

		[NUnit.Framework.Test]
		public virtual void TestCheckAccess()
		{
			// Create two unique users
			string user1 = Runtime.GetProperty("user.name");
			string user2 = user1 + "1234";
			UserGroupInformation ugi1 = UserGroupInformation.CreateRemoteUser(user1);
			UserGroupInformation ugi2 = UserGroupInformation.CreateRemoteUser(user2);
			// Create the job
			JobID jobID = JobID.ForName("job_1234567890000_0001");
			JobId jobId = TypeConverter.ToYarn(jobID);
			// Setup configuration access only to user1 (owner)
			Configuration conf1 = new Configuration();
			conf1.SetBoolean(MRConfig.MrAclsEnabled, true);
			conf1.Set(MRJobConfig.JobAclViewJob, string.Empty);
			// Verify access
			JobImpl job1 = new JobImpl(jobId, null, conf1, null, null, null, null, null, null
				, null, null, true, user1, 0, null, null, null, null);
			NUnit.Framework.Assert.IsTrue(job1.CheckAccess(ugi1, JobACL.ViewJob));
			NUnit.Framework.Assert.IsFalse(job1.CheckAccess(ugi2, JobACL.ViewJob));
			// Setup configuration access to the user1 (owner) and user2
			Configuration conf2 = new Configuration();
			conf2.SetBoolean(MRConfig.MrAclsEnabled, true);
			conf2.Set(MRJobConfig.JobAclViewJob, user2);
			// Verify access
			JobImpl job2 = new JobImpl(jobId, null, conf2, null, null, null, null, null, null
				, null, null, true, user1, 0, null, null, null, null);
			NUnit.Framework.Assert.IsTrue(job2.CheckAccess(ugi1, JobACL.ViewJob));
			NUnit.Framework.Assert.IsTrue(job2.CheckAccess(ugi2, JobACL.ViewJob));
			// Setup configuration access with security enabled and access to all
			Configuration conf3 = new Configuration();
			conf3.SetBoolean(MRConfig.MrAclsEnabled, true);
			conf3.Set(MRJobConfig.JobAclViewJob, "*");
			// Verify access
			JobImpl job3 = new JobImpl(jobId, null, conf3, null, null, null, null, null, null
				, null, null, true, user1, 0, null, null, null, null);
			NUnit.Framework.Assert.IsTrue(job3.CheckAccess(ugi1, JobACL.ViewJob));
			NUnit.Framework.Assert.IsTrue(job3.CheckAccess(ugi2, JobACL.ViewJob));
			// Setup configuration access without security enabled
			Configuration conf4 = new Configuration();
			conf4.SetBoolean(MRConfig.MrAclsEnabled, false);
			conf4.Set(MRJobConfig.JobAclViewJob, string.Empty);
			// Verify access
			JobImpl job4 = new JobImpl(jobId, null, conf4, null, null, null, null, null, null
				, null, null, true, user1, 0, null, null, null, null);
			NUnit.Framework.Assert.IsTrue(job4.CheckAccess(ugi1, JobACL.ViewJob));
			NUnit.Framework.Assert.IsTrue(job4.CheckAccess(ugi2, JobACL.ViewJob));
			// Setup configuration access without security enabled
			Configuration conf5 = new Configuration();
			conf5.SetBoolean(MRConfig.MrAclsEnabled, true);
			conf5.Set(MRJobConfig.JobAclViewJob, string.Empty);
			// Verify access
			JobImpl job5 = new JobImpl(jobId, null, conf5, null, null, null, null, null, null
				, null, null, true, user1, 0, null, null, null, null);
			NUnit.Framework.Assert.IsTrue(job5.CheckAccess(ugi1, null));
			NUnit.Framework.Assert.IsTrue(job5.CheckAccess(ugi2, null));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReportDiagnostics()
		{
			JobID jobID = JobID.ForName("job_1234567890000_0001");
			JobId jobId = TypeConverter.ToYarn(jobID);
			string diagMsg = "some diagnostic message";
			JobDiagnosticsUpdateEvent diagUpdateEvent = new JobDiagnosticsUpdateEvent(jobId, 
				diagMsg);
			MRAppMetrics mrAppMetrics = MRAppMetrics.Create();
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Mockito.Mockito.When(mockContext.HasSuccessfullyUnregistered()).ThenReturn(true
				);
			JobImpl job = new JobImpl(jobId, Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ApplicationAttemptId
				>(), new Configuration(), Org.Mockito.Mockito.Mock<EventHandler>(), null, Org.Mockito.Mockito.Mock
				<JobTokenSecretManager>(), null, new SystemClock(), null, mrAppMetrics, null, true
				, null, 0, null, mockContext, null, null);
			job.Handle(diagUpdateEvent);
			string diagnostics = job.GetReport().GetDiagnostics();
			NUnit.Framework.Assert.IsNotNull(diagnostics);
			NUnit.Framework.Assert.IsTrue(diagnostics.Contains(diagMsg));
			job = new JobImpl(jobId, Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ApplicationAttemptId
				>(), new Configuration(), Org.Mockito.Mockito.Mock<EventHandler>(), null, Org.Mockito.Mockito.Mock
				<JobTokenSecretManager>(), null, new SystemClock(), null, mrAppMetrics, null, true
				, null, 0, null, mockContext, null, null);
			job.Handle(new JobEvent(jobId, JobEventType.JobKill));
			job.Handle(diagUpdateEvent);
			diagnostics = job.GetReport().GetDiagnostics();
			NUnit.Framework.Assert.IsNotNull(diagnostics);
			NUnit.Framework.Assert.IsTrue(diagnostics.Contains(diagMsg));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUberDecision()
		{
			// with default values, no of maps is 2
			Configuration conf = new Configuration();
			bool isUber = TestUberDecision(conf);
			NUnit.Framework.Assert.IsFalse(isUber);
			// enable uber mode, no of maps is 2
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, true);
			isUber = TestUberDecision(conf);
			NUnit.Framework.Assert.IsTrue(isUber);
			// enable uber mode, no of maps is 2, no of reduces is 1 and uber task max
			// reduces is 0
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, true);
			conf.SetInt(MRJobConfig.JobUbertaskMaxreduces, 0);
			conf.SetInt(MRJobConfig.NumReduces, 1);
			isUber = TestUberDecision(conf);
			NUnit.Framework.Assert.IsFalse(isUber);
			// enable uber mode, no of maps is 2, no of reduces is 1 and uber task max
			// reduces is 1
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, true);
			conf.SetInt(MRJobConfig.JobUbertaskMaxreduces, 1);
			conf.SetInt(MRJobConfig.NumReduces, 1);
			isUber = TestUberDecision(conf);
			NUnit.Framework.Assert.IsTrue(isUber);
			// enable uber mode, no of maps is 2 and uber task max maps is 0
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, true);
			conf.SetInt(MRJobConfig.JobUbertaskMaxmaps, 1);
			isUber = TestUberDecision(conf);
			NUnit.Framework.Assert.IsFalse(isUber);
			// enable uber mode of 0 reducer no matter how much memory assigned to reducer
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, true);
			conf.SetInt(MRJobConfig.NumReduces, 0);
			conf.SetInt(MRJobConfig.ReduceMemoryMb, 2048);
			conf.SetInt(MRJobConfig.ReduceCpuVcores, 10);
			isUber = TestUberDecision(conf);
			NUnit.Framework.Assert.IsTrue(isUber);
		}

		private bool TestUberDecision(Configuration conf)
		{
			JobID jobID = JobID.ForName("job_1234567890000_0001");
			JobId jobId = TypeConverter.ToYarn(jobID);
			MRAppMetrics mrAppMetrics = MRAppMetrics.Create();
			JobImpl job = new JobImpl(jobId, ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(0, 0), 0), conf, Org.Mockito.Mockito.Mock<EventHandler>(), null, new JobTokenSecretManager
				(), new Credentials(), null, null, mrAppMetrics, null, true, null, 0, null, null
				, null, null);
			JobImpl.InitTransition initTransition = GetInitTransition(2);
			JobEvent mockJobEvent = Org.Mockito.Mockito.Mock<JobEvent>();
			initTransition.Transition(job, mockJobEvent);
			bool isUber = job.IsUber();
			return isUber;
		}

		private static JobImpl.InitTransition GetInitTransition(int numSplits)
		{
			JobImpl.InitTransition initTransition = new _InitTransition_688(numSplits);
			return initTransition;
		}

		private sealed class _InitTransition_688 : JobImpl.InitTransition
		{
			public _InitTransition_688(int numSplits)
			{
				this.numSplits = numSplits;
			}

			protected internal override JobSplit.TaskSplitMetaInfo[] CreateSplits(JobImpl job
				, JobId jobId)
			{
				JobSplit.TaskSplitMetaInfo[] splits = new JobSplit.TaskSplitMetaInfo[numSplits];
				for (int i = 0; i < numSplits; ++i)
				{
					splits[i] = new JobSplit.TaskSplitMetaInfo();
				}
				return splits;
			}

			private readonly int numSplits;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTransitionsAtFailed()
		{
			Configuration conf = new Configuration();
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			OutputCommitter committer = Org.Mockito.Mockito.Mock<OutputCommitter>();
			Org.Mockito.Mockito.DoThrow(new IOException("forcefail")).When(committer).SetupJob
				(Matchers.Any<JobContext>());
			CommitterEventHandler commitHandler = CreateCommitterEventHandler(dispatcher, committer
				);
			commitHandler.Init(conf);
			commitHandler.Start();
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Mockito.Mockito.When(mockContext.HasSuccessfullyUnregistered()).ThenReturn(false
				);
			JobImpl job = CreateStubbedJob(conf, dispatcher, 2, mockContext);
			JobId jobId = job.GetID();
			job.Handle(new JobEvent(jobId, JobEventType.JobInit));
			AssertJobState(job, JobStateInternal.Inited);
			job.Handle(new JobStartEvent(jobId));
			AssertJobState(job, JobStateInternal.Failed);
			job.Handle(new JobEvent(jobId, JobEventType.JobTaskCompleted));
			AssertJobState(job, JobStateInternal.Failed);
			job.Handle(new JobEvent(jobId, JobEventType.JobTaskAttemptCompleted));
			AssertJobState(job, JobStateInternal.Failed);
			job.Handle(new JobEvent(jobId, JobEventType.JobMapTaskRescheduled));
			AssertJobState(job, JobStateInternal.Failed);
			job.Handle(new JobEvent(jobId, JobEventType.JobTaskAttemptFetchFailure));
			AssertJobState(job, JobStateInternal.Failed);
			NUnit.Framework.Assert.AreEqual(JobState.Running, job.GetState());
			Org.Mockito.Mockito.When(mockContext.HasSuccessfullyUnregistered()).ThenReturn(true
				);
			NUnit.Framework.Assert.AreEqual(JobState.Failed, job.GetState());
			dispatcher.Stop();
			commitHandler.Stop();
		}

		internal const string Exceptionmsg = "Splits max exceeded";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMetaInfoSizeOverMax()
		{
			Configuration conf = new Configuration();
			JobID jobID = JobID.ForName("job_1234567890000_0001");
			JobId jobId = TypeConverter.ToYarn(jobID);
			MRAppMetrics mrAppMetrics = MRAppMetrics.Create();
			JobImpl job = new JobImpl(jobId, ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(0, 0), 0), conf, Org.Mockito.Mockito.Mock<EventHandler>(), null, new JobTokenSecretManager
				(), new Credentials(), null, null, mrAppMetrics, null, true, null, 0, null, null
				, null, null);
			JobImpl.InitTransition initTransition = new _InitTransition_753();
			JobEvent mockJobEvent = Org.Mockito.Mockito.Mock<JobEvent>();
			JobStateInternal jobSI = initTransition.Transition(job, mockJobEvent);
			NUnit.Framework.Assert.IsTrue("When init fails, return value from InitTransition.transition should equal NEW."
				, jobSI.Equals(JobStateInternal.New));
			NUnit.Framework.Assert.IsTrue("Job diagnostics should contain YarnRuntimeException"
				, job.GetDiagnostics().ToString().Contains("YarnRuntimeException"));
			NUnit.Framework.Assert.IsTrue("Job diagnostics should contain " + Exceptionmsg, job
				.GetDiagnostics().ToString().Contains(Exceptionmsg));
		}

		private sealed class _InitTransition_753 : JobImpl.InitTransition
		{
			public _InitTransition_753()
			{
			}

			protected internal override JobSplit.TaskSplitMetaInfo[] CreateSplits(JobImpl job
				, JobId jobId)
			{
				throw new YarnRuntimeException(TestJobImpl.Exceptionmsg);
			}
		}

		private static CommitterEventHandler CreateCommitterEventHandler(Dispatcher dispatcher
			, OutputCommitter committer)
		{
			SystemClock clock = new SystemClock();
			AppContext appContext = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Mockito.Mockito.When(appContext.GetEventHandler()).ThenReturn(dispatcher.GetEventHandler
				());
			Org.Mockito.Mockito.When(appContext.GetClock()).ThenReturn(clock);
			RMHeartbeatHandler heartbeatHandler = new _RMHeartbeatHandler_777(clock);
			ApplicationAttemptId id = ConverterUtils.ToApplicationAttemptId("appattempt_1234567890000_0001_0"
				);
			Org.Mockito.Mockito.When(appContext.GetApplicationID()).ThenReturn(id.GetApplicationId
				());
			Org.Mockito.Mockito.When(appContext.GetApplicationAttemptId()).ThenReturn(id);
			CommitterEventHandler handler = new CommitterEventHandler(appContext, committer, 
				heartbeatHandler);
			dispatcher.Register(typeof(CommitterEventType), handler);
			return handler;
		}

		private sealed class _RMHeartbeatHandler_777 : RMHeartbeatHandler
		{
			public _RMHeartbeatHandler_777(SystemClock clock)
			{
				this.clock = clock;
			}

			public long GetLastHeartbeatTime()
			{
				return clock.GetTime();
			}

			public void RunOnNextHeartbeat(Runnable callback)
			{
				callback.Run();
			}

			private readonly SystemClock clock;
		}

		private static TestJobImpl.StubbedJob CreateStubbedJob(Configuration conf, Dispatcher
			 dispatcher, int numSplits, AppContext appContext)
		{
			JobID jobID = JobID.ForName("job_1234567890000_0001");
			JobId jobId = TypeConverter.ToYarn(jobID);
			if (appContext == null)
			{
				appContext = Org.Mockito.Mockito.Mock<AppContext>();
				Org.Mockito.Mockito.When(appContext.HasSuccessfullyUnregistered()).ThenReturn(true
					);
			}
			TestJobImpl.StubbedJob job = new TestJobImpl.StubbedJob(jobId, ApplicationAttemptId
				.NewInstance(ApplicationId.NewInstance(0, 0), 0), conf, dispatcher.GetEventHandler
				(), true, "somebody", numSplits, appContext);
			dispatcher.Register(typeof(JobEventType), job);
			EventHandler mockHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			dispatcher.Register(typeof(TaskEventType), mockHandler);
			dispatcher.Register(typeof(EventType), mockHandler);
			dispatcher.Register(typeof(JobFinishEvent.Type), mockHandler);
			return job;
		}

		private static TestJobImpl.StubbedJob CreateRunningStubbedJob(Configuration conf, 
			Dispatcher dispatcher, int numSplits, AppContext appContext)
		{
			TestJobImpl.StubbedJob job = CreateStubbedJob(conf, dispatcher, numSplits, appContext
				);
			job.Handle(new JobEvent(job.GetID(), JobEventType.JobInit));
			AssertJobState(job, JobStateInternal.Inited);
			job.Handle(new JobStartEvent(job.GetID()));
			AssertJobState(job, JobStateInternal.Running);
			return job;
		}

		private static void CompleteJobTasks(JobImpl job)
		{
			// complete the map tasks and the reduce tasks so we start committing
			int numMaps = job.GetTotalMaps();
			for (int i = 0; i < numMaps; ++i)
			{
				job.Handle(new JobTaskEvent(MRBuilderUtils.NewTaskId(job.GetID(), 1, TaskType.Map
					), TaskState.Succeeded));
				NUnit.Framework.Assert.AreEqual(JobState.Running, job.GetState());
			}
			int numReduces = job.GetTotalReduces();
			for (int i_1 = 0; i_1 < numReduces; ++i_1)
			{
				job.Handle(new JobTaskEvent(MRBuilderUtils.NewTaskId(job.GetID(), 1, TaskType.Map
					), TaskState.Succeeded));
				NUnit.Framework.Assert.AreEqual(JobState.Running, job.GetState());
			}
		}

		private static void AssertJobState(JobImpl job, JobStateInternal state)
		{
			int timeToWaitMsec = 5 * 1000;
			while (timeToWaitMsec > 0 && job.GetInternalState() != state)
			{
				try
				{
					Sharpen.Thread.Sleep(10);
					timeToWaitMsec -= 10;
				}
				catch (Exception)
				{
					break;
				}
			}
			NUnit.Framework.Assert.AreEqual(state, job.GetInternalState());
		}

		private class JobSubmittedEventHandler : EventHandler<JobHistoryEvent>
		{
			private string workflowId;

			private string workflowName;

			private string workflowNodeName;

			private string workflowAdjacencies;

			private string workflowTags;

			private bool assertBoolean;

			public JobSubmittedEventHandler(string workflowId, string workflowName, string workflowNodeName
				, string workflowAdjacencies, string workflowTags)
			{
				this.workflowId = workflowId;
				this.workflowName = workflowName;
				this.workflowNodeName = workflowNodeName;
				this.workflowAdjacencies = workflowAdjacencies;
				this.workflowTags = workflowTags;
				assertBoolean = null;
			}

			public virtual void Handle(JobHistoryEvent jhEvent)
			{
				if (jhEvent.GetType() != EventType.JobSubmitted)
				{
					return;
				}
				JobSubmittedEvent jsEvent = (JobSubmittedEvent)jhEvent.GetHistoryEvent();
				if (!workflowId.Equals(jsEvent.GetWorkflowId()))
				{
					SetAssertValue(false);
					return;
				}
				if (!workflowName.Equals(jsEvent.GetWorkflowName()))
				{
					SetAssertValue(false);
					return;
				}
				if (!workflowNodeName.Equals(jsEvent.GetWorkflowNodeName()))
				{
					SetAssertValue(false);
					return;
				}
				string[] wrkflowAdj = workflowAdjacencies.Split(" ");
				string[] jswrkflowAdj = jsEvent.GetWorkflowAdjacencies().Split(" ");
				Arrays.Sort(wrkflowAdj);
				Arrays.Sort(jswrkflowAdj);
				if (!Arrays.Equals(wrkflowAdj, jswrkflowAdj))
				{
					SetAssertValue(false);
					return;
				}
				if (!workflowTags.Equals(jsEvent.GetWorkflowTags()))
				{
					SetAssertValue(false);
					return;
				}
				SetAssertValue(true);
			}

			private void SetAssertValue(bool @bool)
			{
				lock (this)
				{
					assertBoolean = @bool;
					Sharpen.Runtime.Notify(this);
				}
			}

			/// <exception cref="System.Exception"/>
			public virtual bool GetAssertValue()
			{
				lock (this)
				{
					while (assertBoolean == null)
					{
						Sharpen.Runtime.Wait(this);
					}
					return assertBoolean;
				}
			}
		}

		private class StubbedJob : JobImpl
		{
			private readonly JobImpl.InitTransition initTransition;

			internal StateMachineFactory<JobImpl, JobStateInternal, JobEventType, JobEvent> localFactory;

			private readonly StateMachine<JobStateInternal, JobEventType, JobEvent> localStateMachine;

			//override the init transition
			protected internal override StateMachine<JobStateInternal, JobEventType, JobEvent
				> GetStateMachine()
			{
				return localStateMachine;
			}

			public StubbedJob(JobId jobId, ApplicationAttemptId applicationAttemptId, Configuration
				 conf, EventHandler eventHandler, bool newApiCommitter, string user, int numSplits
				, AppContext appContext)
				: base(jobId, applicationAttemptId, conf, eventHandler, null, new JobTokenSecretManager
					(), new Credentials(), new SystemClock(), Collections.EmptyMap<TaskId, JobHistoryParser.TaskInfo
					>(), MRAppMetrics.Create(), null, newApiCommitter, user, Runtime.CurrentTimeMillis
					(), null, appContext, null, null)
			{
				initTransition = GetInitTransition(numSplits);
				localFactory = stateMachineFactory.AddTransition(JobStateInternal.New, EnumSet.Of
					(JobStateInternal.Inited, JobStateInternal.Failed), JobEventType.JobInit, initTransition
					);
				// This is abusive.
				// This "this leak" is okay because the retained pointer is in an
				//  instance variable.
				localStateMachine = localFactory.Make(this);
			}
		}

		private class StubbedOutputCommitter : OutputCommitter
		{
			public StubbedOutputCommitter()
				: base()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetupJob(JobContext jobContext)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetupTask(TaskAttemptContext taskContext)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool NeedsTaskCommit(TaskAttemptContext taskContext)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CommitTask(TaskAttemptContext taskContext)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void AbortTask(TaskAttemptContext taskContext)
			{
			}
		}

		private class TestingOutputCommitter : TestJobImpl.StubbedOutputCommitter
		{
			internal CyclicBarrier syncBarrier;

			internal bool shouldSucceed;

			public TestingOutputCommitter(CyclicBarrier syncBarrier, bool shouldSucceed)
				: base()
			{
				this.syncBarrier = syncBarrier;
				this.shouldSucceed = shouldSucceed;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CommitJob(JobContext jobContext)
			{
				try
				{
					syncBarrier.Await();
				}
				catch (BrokenBarrierException)
				{
				}
				catch (Exception)
				{
				}
				if (!shouldSucceed)
				{
					throw new IOException("forced failure");
				}
			}
		}

		private class WaitingOutputCommitter : TestJobImpl.TestingOutputCommitter
		{
			public WaitingOutputCommitter(CyclicBarrier syncBarrier, bool shouldSucceed)
				: base(syncBarrier, shouldSucceed)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CommitJob(JobContext jobContext)
			{
				try
				{
					syncBarrier.Await();
				}
				catch (BrokenBarrierException)
				{
				}
				catch (Exception)
				{
				}
				while (!Sharpen.Thread.Interrupted())
				{
					try
					{
						lock (this)
						{
							Sharpen.Runtime.Wait(this);
						}
					}
					catch (Exception)
					{
						break;
					}
				}
			}
		}
	}
}
