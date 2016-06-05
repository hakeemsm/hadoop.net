using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Commit
{
	public class TestCommitterEventHandler
	{
		public class WaitForItHandler : EventHandler
		{
			private Org.Apache.Hadoop.Yarn.Event.Event @event = null;

			public virtual void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				lock (this)
				{
					this.@event = @event;
					Sharpen.Runtime.NotifyAll(this);
				}
			}

			/// <exception cref="System.Exception"/>
			public virtual Org.Apache.Hadoop.Yarn.Event.Event GetAndClearEvent()
			{
				lock (this)
				{
					if (@event == null)
					{
						long waitTime = 5000;
						long waitStartTime = Time.MonotonicNow();
						while (@event == null && Time.MonotonicNow() - waitStartTime < waitTime)
						{
							//Wait for at most 5 sec
							Sharpen.Runtime.Wait(this, waitTime);
						}
					}
					Org.Apache.Hadoop.Yarn.Event.Event e = @event;
					@event = null;
					return e;
				}
			}
		}

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

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCommitWindow()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			TestCommitterEventHandler.TestingJobEventHandler jeh = new TestCommitterEventHandler.TestingJobEventHandler
				();
			dispatcher.Register(typeof(JobEventType), jeh);
			SystemClock clock = new SystemClock();
			AppContext appContext = Org.Mockito.Mockito.Mock<AppContext>();
			ApplicationAttemptId attemptid = ConverterUtils.ToApplicationAttemptId("appattempt_1234567890000_0001_0"
				);
			Org.Mockito.Mockito.When(appContext.GetApplicationID()).ThenReturn(attemptid.GetApplicationId
				());
			Org.Mockito.Mockito.When(appContext.GetApplicationAttemptId()).ThenReturn(attemptid
				);
			Org.Mockito.Mockito.When(appContext.GetEventHandler()).ThenReturn(dispatcher.GetEventHandler
				());
			Org.Mockito.Mockito.When(appContext.GetClock()).ThenReturn(clock);
			OutputCommitter committer = Org.Mockito.Mockito.Mock<OutputCommitter>();
			TestCommitterEventHandler.TestingRMHeartbeatHandler rmhh = new TestCommitterEventHandler.TestingRMHeartbeatHandler
				();
			CommitterEventHandler ceh = new CommitterEventHandler(appContext, committer, rmhh
				);
			ceh.Init(conf);
			ceh.Start();
			// verify trying to commit when RM heartbeats are stale does not commit
			ceh.Handle(new CommitterJobCommitEvent(null, null));
			long timeToWaitMs = 5000;
			while (rmhh.GetNumCallbacks() != 1 && timeToWaitMs > 0)
			{
				Sharpen.Thread.Sleep(10);
				timeToWaitMs -= 10;
			}
			NUnit.Framework.Assert.AreEqual("committer did not register a heartbeat callback"
				, 1, rmhh.GetNumCallbacks());
			Org.Mockito.Mockito.Verify(committer, Org.Mockito.Mockito.Never()).CommitJob(Matchers.Any
				<JobContext>());
			NUnit.Framework.Assert.AreEqual("committer should not have committed", 0, jeh.numCommitCompletedEvents
				);
			// set a fresh heartbeat and verify commit completes
			rmhh.SetLastHeartbeatTime(clock.GetTime());
			timeToWaitMs = 5000;
			while (jeh.numCommitCompletedEvents != 1 && timeToWaitMs > 0)
			{
				Sharpen.Thread.Sleep(10);
				timeToWaitMs -= 10;
			}
			NUnit.Framework.Assert.AreEqual("committer did not complete commit after RM hearbeat"
				, 1, jeh.numCommitCompletedEvents);
			Org.Mockito.Mockito.Verify(committer, Org.Mockito.Mockito.Times(1)).CommitJob(Matchers.Any
				<JobContext>());
			//Clean up so we can try to commit again (Don't do this at home)
			Cleanup();
			// try to commit again and verify it goes through since the heartbeat
			// is still fresh
			ceh.Handle(new CommitterJobCommitEvent(null, null));
			timeToWaitMs = 5000;
			while (jeh.numCommitCompletedEvents != 2 && timeToWaitMs > 0)
			{
				Sharpen.Thread.Sleep(10);
				timeToWaitMs -= 10;
			}
			NUnit.Framework.Assert.AreEqual("committer did not commit", 2, jeh.numCommitCompletedEvents
				);
			Org.Mockito.Mockito.Verify(committer, Org.Mockito.Mockito.Times(2)).CommitJob(Matchers.Any
				<JobContext>());
			ceh.Stop();
			dispatcher.Stop();
		}

		private class TestingRMHeartbeatHandler : RMHeartbeatHandler
		{
			private long lastHeartbeatTime = 0;

			private ConcurrentLinkedQueue<Runnable> callbacks = new ConcurrentLinkedQueue<Runnable
				>();

			public virtual long GetLastHeartbeatTime()
			{
				return lastHeartbeatTime;
			}

			public virtual void RunOnNextHeartbeat(Runnable callback)
			{
				callbacks.AddItem(callback);
			}

			public virtual void SetLastHeartbeatTime(long timestamp)
			{
				lastHeartbeatTime = timestamp;
				Runnable callback = null;
				while ((callback = callbacks.Poll()) != null)
				{
					callback.Run();
				}
			}

			public virtual int GetNumCallbacks()
			{
				return callbacks.Count;
			}
		}

		private class TestingJobEventHandler : EventHandler<JobEvent>
		{
			internal int numCommitCompletedEvents = 0;

			public virtual void Handle(JobEvent @event)
			{
				if (@event.GetType() == JobEventType.JobCommitCompleted)
				{
					++numCommitCompletedEvents;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBasic()
		{
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			OutputCommitter mockCommitter = Org.Mockito.Mockito.Mock<OutputCommitter>();
			Clock mockClock = Org.Mockito.Mockito.Mock<Clock>();
			CommitterEventHandler handler = new CommitterEventHandler(mockContext, mockCommitter
				, new TestCommitterEventHandler.TestingRMHeartbeatHandler());
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			JobContext mockJobContext = Org.Mockito.Mockito.Mock<JobContext>();
			ApplicationAttemptId attemptid = ConverterUtils.ToApplicationAttemptId("appattempt_1234567890000_0001_0"
				);
			JobId jobId = TypeConverter.ToYarn(TypeConverter.FromYarn(attemptid.GetApplicationId
				()));
			TestCommitterEventHandler.WaitForItHandler waitForItHandler = new TestCommitterEventHandler.WaitForItHandler
				();
			Org.Mockito.Mockito.When(mockContext.GetApplicationID()).ThenReturn(attemptid.GetApplicationId
				());
			Org.Mockito.Mockito.When(mockContext.GetApplicationAttemptId()).ThenReturn(attemptid
				);
			Org.Mockito.Mockito.When(mockContext.GetEventHandler()).ThenReturn(waitForItHandler
				);
			Org.Mockito.Mockito.When(mockContext.GetClock()).ThenReturn(mockClock);
			handler.Init(conf);
			handler.Start();
			try
			{
				handler.Handle(new CommitterJobCommitEvent(jobId, mockJobContext));
				string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
				Path startCommitFile = MRApps.GetStartJobCommitFile(conf, user, jobId);
				Path endCommitSuccessFile = MRApps.GetEndJobCommitSuccessFile(conf, user, jobId);
				Path endCommitFailureFile = MRApps.GetEndJobCommitFailureFile(conf, user, jobId);
				Org.Apache.Hadoop.Yarn.Event.Event e = waitForItHandler.GetAndClearEvent();
				NUnit.Framework.Assert.IsNotNull(e);
				NUnit.Framework.Assert.IsTrue(e is JobCommitCompletedEvent);
				FileSystem fs = FileSystem.Get(conf);
				NUnit.Framework.Assert.IsTrue(startCommitFile.ToString(), fs.Exists(startCommitFile
					));
				NUnit.Framework.Assert.IsTrue(endCommitSuccessFile.ToString(), fs.Exists(endCommitSuccessFile
					));
				NUnit.Framework.Assert.IsFalse(endCommitFailureFile.ToString(), fs.Exists(endCommitFailureFile
					));
				Org.Mockito.Mockito.Verify(mockCommitter).CommitJob(Matchers.Any<JobContext>());
			}
			finally
			{
				handler.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailure()
		{
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			OutputCommitter mockCommitter = Org.Mockito.Mockito.Mock<OutputCommitter>();
			Clock mockClock = Org.Mockito.Mockito.Mock<Clock>();
			CommitterEventHandler handler = new CommitterEventHandler(mockContext, mockCommitter
				, new TestCommitterEventHandler.TestingRMHeartbeatHandler());
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			JobContext mockJobContext = Org.Mockito.Mockito.Mock<JobContext>();
			ApplicationAttemptId attemptid = ConverterUtils.ToApplicationAttemptId("appattempt_1234567890000_0001_0"
				);
			JobId jobId = TypeConverter.ToYarn(TypeConverter.FromYarn(attemptid.GetApplicationId
				()));
			TestCommitterEventHandler.WaitForItHandler waitForItHandler = new TestCommitterEventHandler.WaitForItHandler
				();
			Org.Mockito.Mockito.When(mockContext.GetApplicationID()).ThenReturn(attemptid.GetApplicationId
				());
			Org.Mockito.Mockito.When(mockContext.GetApplicationAttemptId()).ThenReturn(attemptid
				);
			Org.Mockito.Mockito.When(mockContext.GetEventHandler()).ThenReturn(waitForItHandler
				);
			Org.Mockito.Mockito.When(mockContext.GetClock()).ThenReturn(mockClock);
			Org.Mockito.Mockito.DoThrow(new YarnRuntimeException("Intentional Failure")).When
				(mockCommitter).CommitJob(Matchers.Any<JobContext>());
			handler.Init(conf);
			handler.Start();
			try
			{
				handler.Handle(new CommitterJobCommitEvent(jobId, mockJobContext));
				string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
				Path startCommitFile = MRApps.GetStartJobCommitFile(conf, user, jobId);
				Path endCommitSuccessFile = MRApps.GetEndJobCommitSuccessFile(conf, user, jobId);
				Path endCommitFailureFile = MRApps.GetEndJobCommitFailureFile(conf, user, jobId);
				Org.Apache.Hadoop.Yarn.Event.Event e = waitForItHandler.GetAndClearEvent();
				NUnit.Framework.Assert.IsNotNull(e);
				NUnit.Framework.Assert.IsTrue(e is JobCommitFailedEvent);
				FileSystem fs = FileSystem.Get(conf);
				NUnit.Framework.Assert.IsTrue(fs.Exists(startCommitFile));
				NUnit.Framework.Assert.IsFalse(fs.Exists(endCommitSuccessFile));
				NUnit.Framework.Assert.IsTrue(fs.Exists(endCommitFailureFile));
				Org.Mockito.Mockito.Verify(mockCommitter).CommitJob(Matchers.Any<JobContext>());
			}
			finally
			{
				handler.Stop();
			}
		}
	}
}
