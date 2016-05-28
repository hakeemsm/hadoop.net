using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestTaskAttemptListenerImpl
	{
		public class MockTaskAttemptListenerImpl : TaskAttemptListenerImpl
		{
			public MockTaskAttemptListenerImpl(AppContext context, JobTokenSecretManager jobTokenSecretManager
				, RMHeartbeatHandler rmHeartbeatHandler)
				: base(context, jobTokenSecretManager, rmHeartbeatHandler, null)
			{
			}

			public MockTaskAttemptListenerImpl(AppContext context, JobTokenSecretManager jobTokenSecretManager
				, RMHeartbeatHandler rmHeartbeatHandler, TaskHeartbeatHandler hbHandler)
				: base(context, jobTokenSecretManager, rmHeartbeatHandler, null)
			{
				this.taskHeartbeatHandler = hbHandler;
			}

			protected internal override void RegisterHeartbeatHandler(Configuration conf)
			{
			}

			//Empty
			protected internal override void StartRpcServer()
			{
			}

			//Empty
			protected internal override void StopRpcServer()
			{
			}
			//Empty
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetTask()
		{
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			JobTokenSecretManager secret = Org.Mockito.Mockito.Mock<JobTokenSecretManager>();
			RMHeartbeatHandler rmHeartbeatHandler = Org.Mockito.Mockito.Mock<RMHeartbeatHandler
				>();
			TaskHeartbeatHandler hbHandler = Org.Mockito.Mockito.Mock<TaskHeartbeatHandler>();
			TestTaskAttemptListenerImpl.MockTaskAttemptListenerImpl listener = new TestTaskAttemptListenerImpl.MockTaskAttemptListenerImpl
				(appCtx, secret, rmHeartbeatHandler, hbHandler);
			Configuration conf = new Configuration();
			listener.Init(conf);
			listener.Start();
			JVMId id = new JVMId("foo", 1, true, 1);
			WrappedJvmID wid = new WrappedJvmID(id.GetJobId(), id.isMap, id.GetId());
			// Verify ask before registration.
			//The JVM ID has not been registered yet so we should kill it.
			JvmContext context = new JvmContext();
			context.jvmId = id;
			JvmTask result = listener.GetTask(context);
			NUnit.Framework.Assert.IsNotNull(result);
			NUnit.Framework.Assert.IsTrue(result.shouldDie);
			// Verify ask after registration but before launch. 
			// Don't kill, should be null.
			TaskAttemptId attemptID = Org.Mockito.Mockito.Mock<TaskAttemptId>();
			Task task = Org.Mockito.Mockito.Mock<Task>();
			//Now put a task with the ID
			listener.RegisterPendingTask(task, wid);
			result = listener.GetTask(context);
			NUnit.Framework.Assert.IsNull(result);
			// Unregister for more testing.
			listener.Unregister(attemptID, wid);
			// Verify ask after registration and launch
			//Now put a task with the ID
			listener.RegisterPendingTask(task, wid);
			listener.RegisterLaunchedTask(attemptID, wid);
			Org.Mockito.Mockito.Verify(hbHandler).Register(attemptID);
			result = listener.GetTask(context);
			NUnit.Framework.Assert.IsNotNull(result);
			NUnit.Framework.Assert.IsFalse(result.shouldDie);
			// Don't unregister yet for more testing.
			//Verify that if we call it again a second time we are told to die.
			result = listener.GetTask(context);
			NUnit.Framework.Assert.IsNotNull(result);
			NUnit.Framework.Assert.IsTrue(result.shouldDie);
			listener.Unregister(attemptID, wid);
			// Verify after unregistration.
			result = listener.GetTask(context);
			NUnit.Framework.Assert.IsNotNull(result);
			NUnit.Framework.Assert.IsTrue(result.shouldDie);
			listener.Stop();
			// test JVMID
			JVMId jvmid = JVMId.ForName("jvm_001_002_m_004");
			NUnit.Framework.Assert.IsNotNull(jvmid);
			try
			{
				JVMId.ForName("jvm_001_002_m_004_006");
				Assert.Fail();
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.AreEqual(e.Message, "TaskId string : jvm_001_002_m_004_006 is not properly formed"
					);
			}
		}

		public virtual void TestJVMId()
		{
			JVMId jvmid = new JVMId("test", 1, true, 2);
			JVMId jvmid1 = JVMId.ForName("jvm_test_0001_m_000002");
			// test compare methot should be the same
			NUnit.Framework.Assert.AreEqual(0, jvmid.CompareTo(jvmid1));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetMapCompletionEvents()
		{
			TaskAttemptCompletionEvent[] empty = new TaskAttemptCompletionEvent[] {  };
			TaskAttemptCompletionEvent[] taskEvents = new TaskAttemptCompletionEvent[] { CreateTce
				(0, true, TaskAttemptCompletionEventStatus.Obsolete), CreateTce(1, false, TaskAttemptCompletionEventStatus
				.Failed), CreateTce(2, true, TaskAttemptCompletionEventStatus.Succeeded), CreateTce
				(3, false, TaskAttemptCompletionEventStatus.Failed) };
			TaskAttemptCompletionEvent[] mapEvents = new TaskAttemptCompletionEvent[] { taskEvents
				[0], taskEvents[2] };
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetTaskAttemptCompletionEvents(0, 100)).ThenReturn
				(taskEvents);
			Org.Mockito.Mockito.When(mockJob.GetTaskAttemptCompletionEvents(0, 2)).ThenReturn
				(Arrays.CopyOfRange(taskEvents, 0, 2));
			Org.Mockito.Mockito.When(mockJob.GetTaskAttemptCompletionEvents(2, 100)).ThenReturn
				(Arrays.CopyOfRange(taskEvents, 2, 4));
			Org.Mockito.Mockito.When(mockJob.GetMapAttemptCompletionEvents(0, 100)).ThenReturn
				(TypeConverter.FromYarn(mapEvents));
			Org.Mockito.Mockito.When(mockJob.GetMapAttemptCompletionEvents(0, 2)).ThenReturn(
				TypeConverter.FromYarn(mapEvents));
			Org.Mockito.Mockito.When(mockJob.GetMapAttemptCompletionEvents(2, 100)).ThenReturn
				(TypeConverter.FromYarn(empty));
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Mockito.Mockito.When(appCtx.GetJob(Matchers.Any<JobId>())).ThenReturn(mockJob
				);
			JobTokenSecretManager secret = Org.Mockito.Mockito.Mock<JobTokenSecretManager>();
			RMHeartbeatHandler rmHeartbeatHandler = Org.Mockito.Mockito.Mock<RMHeartbeatHandler
				>();
			TaskHeartbeatHandler hbHandler = Org.Mockito.Mockito.Mock<TaskHeartbeatHandler>();
			TaskAttemptListenerImpl listener = new _MockTaskAttemptListenerImpl_200(hbHandler
				, appCtx, secret, rmHeartbeatHandler);
			Configuration conf = new Configuration();
			listener.Init(conf);
			listener.Start();
			JobID jid = new JobID("12345", 1);
			TaskAttemptID tid = new TaskAttemptID("12345", 1, TaskType.Reduce, 1, 0);
			MapTaskCompletionEventsUpdate update = listener.GetMapCompletionEvents(jid, 0, 100
				, tid);
			NUnit.Framework.Assert.AreEqual(2, update.events.Length);
			update = listener.GetMapCompletionEvents(jid, 0, 2, tid);
			NUnit.Framework.Assert.AreEqual(2, update.events.Length);
			update = listener.GetMapCompletionEvents(jid, 2, 100, tid);
			NUnit.Framework.Assert.AreEqual(0, update.events.Length);
		}

		private sealed class _MockTaskAttemptListenerImpl_200 : TestTaskAttemptListenerImpl.MockTaskAttemptListenerImpl
		{
			public _MockTaskAttemptListenerImpl_200(TaskHeartbeatHandler hbHandler, AppContext
				 baseArg1, JobTokenSecretManager baseArg2, RMHeartbeatHandler baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
				this.hbHandler = hbHandler;
			}

			protected internal override void RegisterHeartbeatHandler(Configuration conf)
			{
				this.taskHeartbeatHandler = hbHandler;
			}

			private readonly TaskHeartbeatHandler hbHandler;
		}

		private static TaskAttemptCompletionEvent CreateTce(int eventId, bool isMap, TaskAttemptCompletionEventStatus
			 status)
		{
			JobId jid = MRBuilderUtils.NewJobId(12345, 1, 1);
			TaskId tid = MRBuilderUtils.NewTaskId(jid, 0, isMap ? TaskType.Map : TaskType.Reduce
				);
			TaskAttemptId attemptId = MRBuilderUtils.NewTaskAttemptId(tid, 0);
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
			TaskAttemptCompletionEvent tce = recordFactory.NewRecordInstance<TaskAttemptCompletionEvent
				>();
			tce.SetEventId(eventId);
			tce.SetAttemptId(attemptId);
			tce.SetStatus(status);
			return tce;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCommitWindow()
		{
			SystemClock clock = new SystemClock();
			Task mockTask = Org.Mockito.Mockito.Mock<Task>();
			Org.Mockito.Mockito.When(mockTask.CanCommit(Matchers.Any<TaskAttemptId>())).ThenReturn
				(true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(mockJob.GetTask(Matchers.Any<TaskId>())).ThenReturn(mockTask
				);
			AppContext appCtx = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Mockito.Mockito.When(appCtx.GetJob(Matchers.Any<JobId>())).ThenReturn(mockJob
				);
			Org.Mockito.Mockito.When(appCtx.GetClock()).ThenReturn(clock);
			JobTokenSecretManager secret = Org.Mockito.Mockito.Mock<JobTokenSecretManager>();
			RMHeartbeatHandler rmHeartbeatHandler = Org.Mockito.Mockito.Mock<RMHeartbeatHandler
				>();
			TaskHeartbeatHandler hbHandler = Org.Mockito.Mockito.Mock<TaskHeartbeatHandler>();
			TaskAttemptListenerImpl listener = new _MockTaskAttemptListenerImpl_254(hbHandler
				, appCtx, secret, rmHeartbeatHandler);
			Configuration conf = new Configuration();
			listener.Init(conf);
			listener.Start();
			// verify commit not allowed when RM heartbeat has not occurred recently
			TaskAttemptID tid = new TaskAttemptID("12345", 1, TaskType.Reduce, 1, 0);
			bool canCommit = listener.CanCommit(tid);
			NUnit.Framework.Assert.IsFalse(canCommit);
			Org.Mockito.Mockito.Verify(mockTask, Org.Mockito.Mockito.Never()).CanCommit(Matchers.Any
				<TaskAttemptId>());
			// verify commit allowed when RM heartbeat is recent
			Org.Mockito.Mockito.When(rmHeartbeatHandler.GetLastHeartbeatTime()).ThenReturn(clock
				.GetTime());
			canCommit = listener.CanCommit(tid);
			NUnit.Framework.Assert.IsTrue(canCommit);
			Org.Mockito.Mockito.Verify(mockTask, Org.Mockito.Mockito.Times(1)).CanCommit(Matchers.Any
				<TaskAttemptId>());
			listener.Stop();
		}

		private sealed class _MockTaskAttemptListenerImpl_254 : TestTaskAttemptListenerImpl.MockTaskAttemptListenerImpl
		{
			public _MockTaskAttemptListenerImpl_254(TaskHeartbeatHandler hbHandler, AppContext
				 baseArg1, JobTokenSecretManager baseArg2, RMHeartbeatHandler baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
				this.hbHandler = hbHandler;
			}

			protected internal override void RegisterHeartbeatHandler(Configuration conf)
			{
				this.taskHeartbeatHandler = hbHandler;
			}

			private readonly TaskHeartbeatHandler hbHandler;
		}
	}
}
