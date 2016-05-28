using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Metrics;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl
{
	public class TestTaskImpl
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestTaskImpl));

		private JobConf conf;

		private TaskAttemptListener taskAttemptListener;

		private Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken;

		private JobId jobId;

		private Path remoteJobConfFile;

		private Credentials credentials;

		private Clock clock;

		private MRAppMetrics metrics;

		private TaskImpl mockTask;

		private ApplicationId appId;

		private JobSplit.TaskSplitMetaInfo taskSplitMetaInfo;

		private string[] dataLocations = new string[0];

		private AppContext appContext;

		private int startCount = 0;

		private int taskCounter = 0;

		private readonly int partition = 1;

		private InlineDispatcher dispatcher;

		private IList<TestTaskImpl.MockTaskAttemptImpl> taskAttempts;

		private class MockTaskImpl : TaskImpl
		{
			private int taskAttemptCounter = 0;

			internal TaskType taskType;

			public MockTaskImpl(TestTaskImpl _enclosing, JobId jobId, int partition, EventHandler
				 eventHandler, Path remoteJobConfFile, JobConf conf, TaskAttemptListener taskAttemptListener
				, Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken, Credentials
				 credentials, Clock clock, int startCount, MRAppMetrics metrics, AppContext appContext
				, TaskType taskType)
				: base(jobId, taskType, partition, eventHandler, remoteJobConfFile, conf, taskAttemptListener
					, jobToken, credentials, clock, startCount, metrics, appContext)
			{
				this._enclosing = _enclosing;
				this.taskType = taskType;
			}

			public override TaskType GetType()
			{
				return this.taskType;
			}

			protected internal override TaskAttemptImpl CreateAttempt()
			{
				TestTaskImpl.MockTaskAttemptImpl attempt = new TestTaskImpl.MockTaskAttemptImpl(this
					, this.GetID(), ++this.taskAttemptCounter, this.eventHandler, this.taskAttemptListener
					, this._enclosing.remoteJobConfFile, this.partition, this.conf, this.jobToken, this
					.credentials, this.clock, this.appContext, this.taskType);
				this._enclosing.taskAttempts.AddItem(attempt);
				return attempt;
			}

			protected internal override int GetMaxAttempts()
			{
				return 100;
			}

			protected internal override void InternalError(TaskEventType type)
			{
				base.InternalError(type);
				NUnit.Framework.Assert.Fail("Internal error: " + type);
			}

			private readonly TestTaskImpl _enclosing;
		}

		private class MockTaskAttemptImpl : TaskAttemptImpl
		{
			private float progress = 0;

			private TaskAttemptState state = TaskAttemptState.New;

			private TaskType taskType;

			private Counters attemptCounters = TaskAttemptImpl.EmptyCounters;

			public MockTaskAttemptImpl(TestTaskImpl _enclosing, TaskId taskId, int id, EventHandler
				 eventHandler, TaskAttemptListener taskAttemptListener, Path jobFile, int partition
				, JobConf conf, Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken
				, Credentials credentials, Clock clock, AppContext appContext, TaskType taskType
				)
				: base(taskId, id, eventHandler, taskAttemptListener, jobFile, partition, conf, this
					._enclosing.dataLocations, jobToken, credentials, clock, appContext)
			{
				this._enclosing = _enclosing;
				this.taskType = taskType;
			}

			public virtual TaskAttemptId GetAttemptId()
			{
				return this.GetID();
			}

			protected internal override Task CreateRemoteTask()
			{
				return new TestTaskImpl.MockTask(this, this.taskType);
			}

			public override float GetProgress()
			{
				return this.progress;
			}

			public virtual void SetProgress(float progress)
			{
				this.progress = progress;
			}

			public virtual void SetState(TaskAttemptState state)
			{
				this.state = state;
			}

			public override TaskAttemptState GetState()
			{
				return this.state;
			}

			public override Counters GetCounters()
			{
				return this.attemptCounters;
			}

			public virtual void SetCounters(Counters counters)
			{
				this.attemptCounters = counters;
			}

			private readonly TestTaskImpl _enclosing;
		}

		private class MockTask : Task
		{
			private TaskType taskType;

			internal MockTask(TestTaskImpl _enclosing, TaskType taskType)
			{
				this._enclosing = _enclosing;
				this.taskType = taskType;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.TypeLoadException"/>
			/// <exception cref="System.Exception"/>
			public override void Run(JobConf job, TaskUmbilicalProtocol umbilical)
			{
				return;
			}

			public override bool IsMapTask()
			{
				return (this.taskType == TaskType.Map);
			}

			private readonly TestTaskImpl _enclosing;
		}

		[SetUp]
		public virtual void Setup()
		{
			dispatcher = new InlineDispatcher();
			++startCount;
			conf = new JobConf();
			taskAttemptListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			jobToken = (Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier>)Org.Mockito.Mockito.Mock
				<Org.Apache.Hadoop.Security.Token.Token>();
			remoteJobConfFile = Org.Mockito.Mockito.Mock<Path>();
			credentials = null;
			clock = new SystemClock();
			metrics = Org.Mockito.Mockito.Mock<MRAppMetrics>();
			dataLocations = new string[1];
			appId = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 1);
			jobId = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<JobId>();
			jobId.SetId(1);
			jobId.SetAppId(appId);
			appContext = Org.Mockito.Mockito.Mock<AppContext>();
			taskSplitMetaInfo = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo>();
			Org.Mockito.Mockito.When(taskSplitMetaInfo.GetLocations()).ThenReturn(dataLocations
				);
			taskAttempts = new AList<TestTaskImpl.MockTaskAttemptImpl>();
		}

		private TestTaskImpl.MockTaskImpl CreateMockTask(TaskType taskType)
		{
			return new TestTaskImpl.MockTaskImpl(this, jobId, partition, dispatcher.GetEventHandler
				(), remoteJobConfFile, conf, taskAttemptListener, jobToken, credentials, clock, 
				startCount, metrics, appContext, taskType);
		}

		[TearDown]
		public virtual void Teardown()
		{
			taskAttempts.Clear();
		}

		private TaskId GetNewTaskID()
		{
			TaskId taskId = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<TaskId>();
			taskId.SetId(++taskCounter);
			taskId.SetJobId(jobId);
			taskId.SetTaskType(mockTask.GetType());
			return taskId;
		}

		private void ScheduleTaskAttempt(TaskId taskId)
		{
			mockTask.Handle(new TaskEvent(taskId, TaskEventType.TSchedule));
			AssertTaskScheduledState();
			AssertTaskAttemptAvataar(Avataar.Virgin);
		}

		private void KillTask(TaskId taskId)
		{
			mockTask.Handle(new TaskEvent(taskId, TaskEventType.TKill));
			AssertTaskKillWaitState();
		}

		private void KillScheduledTaskAttempt(TaskAttemptId attemptId)
		{
			mockTask.Handle(new TaskTAttemptEvent(attemptId, TaskEventType.TAttemptKilled));
			AssertTaskScheduledState();
		}

		private void LaunchTaskAttempt(TaskAttemptId attemptId)
		{
			mockTask.Handle(new TaskTAttemptEvent(attemptId, TaskEventType.TAttemptLaunched));
			AssertTaskRunningState();
		}

		private void CommitTaskAttempt(TaskAttemptId attemptId)
		{
			mockTask.Handle(new TaskTAttemptEvent(attemptId, TaskEventType.TAttemptCommitPending
				));
			AssertTaskRunningState();
		}

		private TestTaskImpl.MockTaskAttemptImpl GetLastAttempt()
		{
			return taskAttempts[taskAttempts.Count - 1];
		}

		private void UpdateLastAttemptProgress(float p)
		{
			GetLastAttempt().SetProgress(p);
		}

		private void UpdateLastAttemptState(TaskAttemptState s)
		{
			GetLastAttempt().SetState(s);
		}

		private void KillRunningTaskAttempt(TaskAttemptId attemptId)
		{
			mockTask.Handle(new TaskTAttemptEvent(attemptId, TaskEventType.TAttemptKilled));
			AssertTaskRunningState();
		}

		private void FailRunningTaskAttempt(TaskAttemptId attemptId)
		{
			mockTask.Handle(new TaskTAttemptEvent(attemptId, TaskEventType.TAttemptFailed));
			AssertTaskRunningState();
		}

		/// <summary><see cref="Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskState.New"/></summary>
		private void AssertTaskNewState()
		{
			NUnit.Framework.Assert.AreEqual(TaskState.New, mockTask.GetState());
		}

		/// <summary><see cref="Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskState.Scheduled
		/// 	"/></summary>
		private void AssertTaskScheduledState()
		{
			NUnit.Framework.Assert.AreEqual(TaskState.Scheduled, mockTask.GetState());
		}

		/// <summary><see cref="Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskState.Running"
		/// 	/></summary>
		private void AssertTaskRunningState()
		{
			NUnit.Framework.Assert.AreEqual(TaskState.Running, mockTask.GetState());
		}

		/// <summary><see cref="TaskState#KILL_WAIT"/></summary>
		private void AssertTaskKillWaitState()
		{
			NUnit.Framework.Assert.AreEqual(TaskStateInternal.KillWait, mockTask.GetInternalState
				());
		}

		/// <summary><see cref="Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskState.Succeeded
		/// 	"/></summary>
		private void AssertTaskSucceededState()
		{
			NUnit.Framework.Assert.AreEqual(TaskState.Succeeded, mockTask.GetState());
		}

		/// <summary><see cref="Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Avataar"/></summary>
		private void AssertTaskAttemptAvataar(Avataar avataar)
		{
			foreach (TaskAttempt taskAttempt in mockTask.GetAttempts().Values)
			{
				if (((TaskAttemptImpl)taskAttempt).GetAvataar() == avataar)
				{
					return;
				}
			}
			NUnit.Framework.Assert.Fail("There is no " + (avataar == Avataar.Virgin ? "virgin"
				 : "speculative") + "task attempt");
		}

		[NUnit.Framework.Test]
		public virtual void TestInit()
		{
			Log.Info("--- START: testInit ---");
			mockTask = CreateMockTask(TaskType.Map);
			AssertTaskNewState();
			System.Diagnostics.Debug.Assert((taskAttempts.Count == 0));
		}

		[NUnit.Framework.Test]
		public virtual void TestScheduleTask()
		{
			Log.Info("--- START: testScheduleTask ---");
			mockTask = CreateMockTask(TaskType.Map);
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
		}

		[NUnit.Framework.Test]
		public virtual void TestKillScheduledTask()
		{
			Log.Info("--- START: testKillScheduledTask ---");
			mockTask = CreateMockTask(TaskType.Map);
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
			KillTask(taskId);
		}

		[NUnit.Framework.Test]
		public virtual void TestKillScheduledTaskAttempt()
		{
			Log.Info("--- START: testKillScheduledTaskAttempt ---");
			mockTask = CreateMockTask(TaskType.Map);
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
			KillScheduledTaskAttempt(GetLastAttempt().GetAttemptId());
		}

		[NUnit.Framework.Test]
		public virtual void TestLaunchTaskAttempt()
		{
			Log.Info("--- START: testLaunchTaskAttempt ---");
			mockTask = CreateMockTask(TaskType.Map);
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
		}

		[NUnit.Framework.Test]
		public virtual void TestKillRunningTaskAttempt()
		{
			Log.Info("--- START: testKillRunningTaskAttempt ---");
			mockTask = CreateMockTask(TaskType.Map);
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			KillRunningTaskAttempt(GetLastAttempt().GetAttemptId());
		}

		[NUnit.Framework.Test]
		public virtual void TestKillSuccessfulTask()
		{
			Log.Info("--- START: testKillSuccesfulTask ---");
			mockTask = CreateMockTask(TaskType.Map);
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			CommitTaskAttempt(GetLastAttempt().GetAttemptId());
			mockTask.Handle(new TaskTAttemptEvent(GetLastAttempt().GetAttemptId(), TaskEventType
				.TAttemptSucceeded));
			AssertTaskSucceededState();
			mockTask.Handle(new TaskEvent(taskId, TaskEventType.TKill));
			AssertTaskSucceededState();
		}

		[NUnit.Framework.Test]
		public virtual void TestTaskProgress()
		{
			Log.Info("--- START: testTaskProgress ---");
			mockTask = CreateMockTask(TaskType.Map);
			// launch task
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
			float progress = 0f;
			System.Diagnostics.Debug.Assert((mockTask.GetProgress() == progress));
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			// update attempt1 
			progress = 50f;
			UpdateLastAttemptProgress(progress);
			System.Diagnostics.Debug.Assert((mockTask.GetProgress() == progress));
			progress = 100f;
			UpdateLastAttemptProgress(progress);
			System.Diagnostics.Debug.Assert((mockTask.GetProgress() == progress));
			progress = 0f;
			// mark first attempt as killed
			UpdateLastAttemptState(TaskAttemptState.Killed);
			System.Diagnostics.Debug.Assert((mockTask.GetProgress() == progress));
			// kill first attempt 
			// should trigger a new attempt
			// as no successful attempts 
			KillRunningTaskAttempt(GetLastAttempt().GetAttemptId());
			System.Diagnostics.Debug.Assert((taskAttempts.Count == 2));
			System.Diagnostics.Debug.Assert((mockTask.GetProgress() == 0f));
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			progress = 50f;
			UpdateLastAttemptProgress(progress);
			System.Diagnostics.Debug.Assert((mockTask.GetProgress() == progress));
		}

		[NUnit.Framework.Test]
		public virtual void TestKillDuringTaskAttemptCommit()
		{
			mockTask = CreateMockTask(TaskType.Reduce);
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			UpdateLastAttemptState(TaskAttemptState.CommitPending);
			CommitTaskAttempt(GetLastAttempt().GetAttemptId());
			TaskAttemptId commitAttempt = GetLastAttempt().GetAttemptId();
			UpdateLastAttemptState(TaskAttemptState.Killed);
			KillRunningTaskAttempt(commitAttempt);
			NUnit.Framework.Assert.IsFalse(mockTask.CanCommit(commitAttempt));
		}

		[NUnit.Framework.Test]
		public virtual void TestFailureDuringTaskAttemptCommit()
		{
			mockTask = CreateMockTask(TaskType.Map);
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			UpdateLastAttemptState(TaskAttemptState.CommitPending);
			CommitTaskAttempt(GetLastAttempt().GetAttemptId());
			// During the task attempt commit there is an exception which causes
			// the attempt to fail
			UpdateLastAttemptState(TaskAttemptState.Failed);
			FailRunningTaskAttempt(GetLastAttempt().GetAttemptId());
			NUnit.Framework.Assert.AreEqual(2, taskAttempts.Count);
			UpdateLastAttemptState(TaskAttemptState.Succeeded);
			CommitTaskAttempt(GetLastAttempt().GetAttemptId());
			mockTask.Handle(new TaskTAttemptEvent(GetLastAttempt().GetAttemptId(), TaskEventType
				.TAttemptSucceeded));
			NUnit.Framework.Assert.IsFalse("First attempt should not commit", mockTask.CanCommit
				(taskAttempts[0].GetAttemptId()));
			NUnit.Framework.Assert.IsTrue("Second attempt should commit", mockTask.CanCommit(
				GetLastAttempt().GetAttemptId()));
			AssertTaskSucceededState();
		}

		private void RunSpeculativeTaskAttemptSucceeds(TaskEventType firstAttemptFinishEvent
			)
		{
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			UpdateLastAttemptState(TaskAttemptState.Running);
			// Add a speculative task attempt that succeeds
			mockTask.Handle(new TaskTAttemptEvent(GetLastAttempt().GetAttemptId(), TaskEventType
				.TAddSpecAttempt));
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			CommitTaskAttempt(GetLastAttempt().GetAttemptId());
			mockTask.Handle(new TaskTAttemptEvent(GetLastAttempt().GetAttemptId(), TaskEventType
				.TAttemptSucceeded));
			// The task should now have succeeded
			AssertTaskSucceededState();
			// Now complete the first task attempt, after the second has succeeded
			mockTask.Handle(new TaskTAttemptEvent(taskAttempts[0].GetAttemptId(), firstAttemptFinishEvent
				));
			// The task should still be in the succeeded state
			AssertTaskSucceededState();
			// The task should contain speculative a task attempt
			AssertTaskAttemptAvataar(Avataar.Speculative);
		}

		[NUnit.Framework.Test]
		public virtual void TestMapSpeculativeTaskAttemptSucceedsEvenIfFirstFails()
		{
			mockTask = CreateMockTask(TaskType.Map);
			RunSpeculativeTaskAttemptSucceeds(TaskEventType.TAttemptFailed);
		}

		[NUnit.Framework.Test]
		public virtual void TestReduceSpeculativeTaskAttemptSucceedsEvenIfFirstFails()
		{
			mockTask = CreateMockTask(TaskType.Reduce);
			RunSpeculativeTaskAttemptSucceeds(TaskEventType.TAttemptFailed);
		}

		[NUnit.Framework.Test]
		public virtual void TestMapSpeculativeTaskAttemptSucceedsEvenIfFirstIsKilled()
		{
			mockTask = CreateMockTask(TaskType.Map);
			RunSpeculativeTaskAttemptSucceeds(TaskEventType.TAttemptKilled);
		}

		[NUnit.Framework.Test]
		public virtual void TestReduceSpeculativeTaskAttemptSucceedsEvenIfFirstIsKilled()
		{
			mockTask = CreateMockTask(TaskType.Reduce);
			RunSpeculativeTaskAttemptSucceeds(TaskEventType.TAttemptKilled);
		}

		[NUnit.Framework.Test]
		public virtual void TestMultipleTaskAttemptsSucceed()
		{
			mockTask = CreateMockTask(TaskType.Map);
			RunSpeculativeTaskAttemptSucceeds(TaskEventType.TAttemptSucceeded);
		}

		[NUnit.Framework.Test]
		public virtual void TestCommitAfterSucceeds()
		{
			mockTask = CreateMockTask(TaskType.Reduce);
			RunSpeculativeTaskAttemptSucceeds(TaskEventType.TAttemptCommitPending);
		}

		[NUnit.Framework.Test]
		public virtual void TestSpeculativeMapFetchFailure()
		{
			// Setup a scenario where speculative task wins, first attempt killed
			mockTask = CreateMockTask(TaskType.Map);
			RunSpeculativeTaskAttemptSucceeds(TaskEventType.TAttemptKilled);
			NUnit.Framework.Assert.AreEqual(2, taskAttempts.Count);
			// speculative attempt retroactively fails from fetch failures
			mockTask.Handle(new TaskTAttemptEvent(taskAttempts[1].GetAttemptId(), TaskEventType
				.TAttemptFailed));
			AssertTaskScheduledState();
			NUnit.Framework.Assert.AreEqual(3, taskAttempts.Count);
		}

		[NUnit.Framework.Test]
		public virtual void TestSpeculativeMapMultipleSucceedFetchFailure()
		{
			// Setup a scenario where speculative task wins, first attempt succeeds
			mockTask = CreateMockTask(TaskType.Map);
			RunSpeculativeTaskAttemptSucceeds(TaskEventType.TAttemptSucceeded);
			NUnit.Framework.Assert.AreEqual(2, taskAttempts.Count);
			// speculative attempt retroactively fails from fetch failures
			mockTask.Handle(new TaskTAttemptEvent(taskAttempts[1].GetAttemptId(), TaskEventType
				.TAttemptFailed));
			AssertTaskScheduledState();
			NUnit.Framework.Assert.AreEqual(3, taskAttempts.Count);
		}

		[NUnit.Framework.Test]
		public virtual void TestSpeculativeMapFailedFetchFailure()
		{
			// Setup a scenario where speculative task wins, first attempt succeeds
			mockTask = CreateMockTask(TaskType.Map);
			RunSpeculativeTaskAttemptSucceeds(TaskEventType.TAttemptFailed);
			NUnit.Framework.Assert.AreEqual(2, taskAttempts.Count);
			// speculative attempt retroactively fails from fetch failures
			mockTask.Handle(new TaskTAttemptEvent(taskAttempts[1].GetAttemptId(), TaskEventType
				.TAttemptFailed));
			AssertTaskScheduledState();
			NUnit.Framework.Assert.AreEqual(3, taskAttempts.Count);
		}

		[NUnit.Framework.Test]
		public virtual void TestFailedTransitions()
		{
			mockTask = new _MockTaskImpl_648(jobId, partition, dispatcher.GetEventHandler(), 
				remoteJobConfFile, conf, taskAttemptListener, jobToken, credentials, clock, startCount
				, metrics, appContext, TaskType.Map);
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			// add three more speculative attempts
			mockTask.Handle(new TaskTAttemptEvent(GetLastAttempt().GetAttemptId(), TaskEventType
				.TAddSpecAttempt));
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			mockTask.Handle(new TaskTAttemptEvent(GetLastAttempt().GetAttemptId(), TaskEventType
				.TAddSpecAttempt));
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			mockTask.Handle(new TaskTAttemptEvent(GetLastAttempt().GetAttemptId(), TaskEventType
				.TAddSpecAttempt));
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			NUnit.Framework.Assert.AreEqual(4, taskAttempts.Count);
			// have the first attempt fail, verify task failed due to no retries
			TestTaskImpl.MockTaskAttemptImpl taskAttempt = taskAttempts[0];
			taskAttempt.SetState(TaskAttemptState.Failed);
			mockTask.Handle(new TaskTAttemptEvent(taskAttempt.GetAttemptId(), TaskEventType.TAttemptFailed
				));
			NUnit.Framework.Assert.AreEqual(TaskState.Failed, mockTask.GetState());
			// verify task can no longer be killed
			mockTask.Handle(new TaskEvent(taskId, TaskEventType.TKill));
			NUnit.Framework.Assert.AreEqual(TaskState.Failed, mockTask.GetState());
			// verify speculative doesn't launch new tasks
			mockTask.Handle(new TaskTAttemptEvent(GetLastAttempt().GetAttemptId(), TaskEventType
				.TAddSpecAttempt));
			mockTask.Handle(new TaskTAttemptEvent(GetLastAttempt().GetAttemptId(), TaskEventType
				.TAttemptLaunched));
			NUnit.Framework.Assert.AreEqual(TaskState.Failed, mockTask.GetState());
			NUnit.Framework.Assert.AreEqual(4, taskAttempts.Count);
			// verify attempt events from active tasks don't knock task out of FAILED
			taskAttempt = taskAttempts[1];
			taskAttempt.SetState(TaskAttemptState.CommitPending);
			mockTask.Handle(new TaskTAttemptEvent(taskAttempt.GetAttemptId(), TaskEventType.TAttemptCommitPending
				));
			NUnit.Framework.Assert.AreEqual(TaskState.Failed, mockTask.GetState());
			taskAttempt.SetState(TaskAttemptState.Failed);
			mockTask.Handle(new TaskTAttemptEvent(taskAttempt.GetAttemptId(), TaskEventType.TAttemptFailed
				));
			NUnit.Framework.Assert.AreEqual(TaskState.Failed, mockTask.GetState());
			taskAttempt = taskAttempts[2];
			taskAttempt.SetState(TaskAttemptState.Succeeded);
			mockTask.Handle(new TaskTAttemptEvent(taskAttempt.GetAttemptId(), TaskEventType.TAttemptSucceeded
				));
			NUnit.Framework.Assert.AreEqual(TaskState.Failed, mockTask.GetState());
			taskAttempt = taskAttempts[3];
			taskAttempt.SetState(TaskAttemptState.Killed);
			mockTask.Handle(new TaskTAttemptEvent(taskAttempt.GetAttemptId(), TaskEventType.TAttemptKilled
				));
			NUnit.Framework.Assert.AreEqual(TaskState.Failed, mockTask.GetState());
		}

		private sealed class _MockTaskImpl_648 : TestTaskImpl.MockTaskImpl
		{
			public _MockTaskImpl_648(JobId baseArg1, int baseArg2, EventHandler baseArg3, Path
				 baseArg4, JobConf baseArg5, TaskAttemptListener baseArg6, Org.Apache.Hadoop.Security.Token.Token
				<JobTokenIdentifier> baseArg7, Credentials baseArg8, Clock baseArg9, int baseArg10
				, MRAppMetrics baseArg11, AppContext baseArg12, TaskType baseArg13)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7, baseArg8
					, baseArg9, baseArg10, baseArg11, baseArg12, baseArg13)
			{
			}

			protected internal override int GetMaxAttempts()
			{
				return 1;
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestCountersWithSpeculation()
		{
			mockTask = new _MockTaskImpl_715(jobId, partition, dispatcher.GetEventHandler(), 
				remoteJobConfFile, conf, taskAttemptListener, jobToken, credentials, clock, startCount
				, metrics, appContext, TaskType.Map);
			TaskId taskId = GetNewTaskID();
			ScheduleTaskAttempt(taskId);
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			UpdateLastAttemptState(TaskAttemptState.Running);
			TestTaskImpl.MockTaskAttemptImpl baseAttempt = GetLastAttempt();
			// add a speculative attempt
			mockTask.Handle(new TaskTAttemptEvent(GetLastAttempt().GetAttemptId(), TaskEventType
				.TAddSpecAttempt));
			LaunchTaskAttempt(GetLastAttempt().GetAttemptId());
			UpdateLastAttemptState(TaskAttemptState.Running);
			TestTaskImpl.MockTaskAttemptImpl specAttempt = GetLastAttempt();
			NUnit.Framework.Assert.AreEqual(2, taskAttempts.Count);
			Counters specAttemptCounters = new Counters();
			Counter cpuCounter = specAttemptCounters.FindCounter(TaskCounter.CpuMilliseconds);
			cpuCounter.SetValue(1000);
			specAttempt.SetCounters(specAttemptCounters);
			// have the spec attempt succeed but second attempt at 1.0 progress as well
			CommitTaskAttempt(specAttempt.GetAttemptId());
			specAttempt.SetProgress(1.0f);
			specAttempt.SetState(TaskAttemptState.Succeeded);
			mockTask.Handle(new TaskTAttemptEvent(specAttempt.GetAttemptId(), TaskEventType.TAttemptSucceeded
				));
			NUnit.Framework.Assert.AreEqual(TaskState.Succeeded, mockTask.GetState());
			baseAttempt.SetProgress(1.0f);
			Counters taskCounters = mockTask.GetCounters();
			NUnit.Framework.Assert.AreEqual("wrong counters for task", specAttemptCounters, taskCounters
				);
		}

		private sealed class _MockTaskImpl_715 : TestTaskImpl.MockTaskImpl
		{
			public _MockTaskImpl_715(JobId baseArg1, int baseArg2, EventHandler baseArg3, Path
				 baseArg4, JobConf baseArg5, TaskAttemptListener baseArg6, Org.Apache.Hadoop.Security.Token.Token
				<JobTokenIdentifier> baseArg7, Credentials baseArg8, Clock baseArg9, int baseArg10
				, MRAppMetrics baseArg11, AppContext baseArg12, TaskType baseArg13)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7, baseArg8
					, baseArg9, baseArg10, baseArg11, baseArg12, baseArg13)
			{
			}

			protected internal override int GetMaxAttempts()
			{
				return 1;
			}
		}
	}
}
