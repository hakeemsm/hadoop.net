using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Speculate;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class TestRuntimeEstimators
	{
		private static int InitialNumberFreeSlots = 600;

		private static int MapSlotRequirement = 3;

		private static int ReduceSlotRequirement = 4;

		private static int MapTasks = 200;

		private static int ReduceTasks = 150;

		internal TestRuntimeEstimators.MockClock clock;

		internal Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job myJob;

		internal AppContext myAppContext;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestRuntimeEstimators)
			);

		private readonly AtomicInteger slotsInUse = new AtomicInteger(0);

		internal AsyncDispatcher dispatcher;

		internal DefaultSpeculator speculator;

		internal TaskRuntimeEstimator estimator;

		private readonly AtomicInteger completedMaps = new AtomicInteger(0);

		private readonly AtomicInteger completedReduces = new AtomicInteger(0);

		private readonly AtomicInteger successfulSpeculations = new AtomicInteger(0);

		private readonly AtomicLong taskTimeSavedBySpeculation = new AtomicLong(0L);

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		// this has to be at least as much as map slot requirement
		// This is a huge kluge.  The real implementations have a decent approach
		private void CoreTestEstimator(TaskRuntimeEstimator testedEstimator, int expectedSpeculations
			)
		{
			estimator = testedEstimator;
			clock = new TestRuntimeEstimators.MockClock();
			dispatcher = new AsyncDispatcher();
			myJob = null;
			slotsInUse.Set(0);
			completedMaps.Set(0);
			completedReduces.Set(0);
			successfulSpeculations.Set(0);
			taskTimeSavedBySpeculation.Set(0);
			clock.AdvanceTime(1000);
			Configuration conf = new Configuration();
			myAppContext = new TestRuntimeEstimators.MyAppContext(this, MapTasks, ReduceTasks
				);
			myJob = myAppContext.GetAllJobs().Values.GetEnumerator().Next();
			estimator.Contextualize(conf, myAppContext);
			conf.SetLong(MRJobConfig.SpeculativeRetryAfterNoSpeculate, 500L);
			conf.SetLong(MRJobConfig.SpeculativeRetryAfterSpeculate, 5000L);
			conf.SetDouble(MRJobConfig.SpeculativecapRunningTasks, 0.1);
			conf.SetDouble(MRJobConfig.SpeculativecapTotalTasks, 0.001);
			conf.SetInt(MRJobConfig.SpeculativeMinimumAllowedTasks, 5);
			speculator = new DefaultSpeculator(conf, myAppContext, estimator, clock);
			NUnit.Framework.Assert.AreEqual("wrong SPECULATIVE_RETRY_AFTER_NO_SPECULATE value"
				, 500L, speculator.GetSoonestRetryAfterNoSpeculate());
			NUnit.Framework.Assert.AreEqual("wrong SPECULATIVE_RETRY_AFTER_SPECULATE value", 
				5000L, speculator.GetSoonestRetryAfterSpeculate());
			NUnit.Framework.Assert.AreEqual(speculator.GetProportionRunningTasksSpeculatable(
				), 0.1, 0.00001);
			NUnit.Framework.Assert.AreEqual(speculator.GetProportionTotalTasksSpeculatable(), 
				0.001, 0.00001);
			NUnit.Framework.Assert.AreEqual("wrong SPECULATIVE_MINIMUM_ALLOWED_TASKS value", 
				5, speculator.GetMinimumAllowedSpeculativeTasks());
			dispatcher.Register(typeof(Speculator.EventType), speculator);
			dispatcher.Register(typeof(TaskEventType), new TestRuntimeEstimators.SpeculationRequestEventHandler
				(this));
			dispatcher.Init(conf);
			dispatcher.Start();
			speculator.Init(conf);
			speculator.Start();
			// Now that the plumbing is hooked up, we do the following:
			//  do until all tasks are finished, ...
			//  1: If we have spare capacity, assign as many map tasks as we can, then
			//     assign as many reduce tasks as we can.  Note that an odd reduce
			//     task might be started while there are still map tasks, because
			//     map tasks take 3 slots and reduce tasks 2 slots.
			//  2: Send a speculation event for every task attempt that's running
			//  note that new attempts might get started by the speculator
			// discover undone tasks
			int undoneMaps = MapTasks;
			int undoneReduces = ReduceTasks;
			// build a task sequence where all the maps precede any of the reduces
			IList<Task> allTasksSequence = new List<Task>();
			Sharpen.Collections.AddAll(allTasksSequence, myJob.GetTasks(TaskType.Map).Values);
			Sharpen.Collections.AddAll(allTasksSequence, myJob.GetTasks(TaskType.Reduce).Values
				);
			while (undoneMaps + undoneReduces > 0)
			{
				undoneMaps = 0;
				undoneReduces = 0;
				// start all attempts which are new but for which there is enough slots
				foreach (Task task in allTasksSequence)
				{
					if (!task.IsFinished())
					{
						if (task.GetType() == TaskType.Map)
						{
							++undoneMaps;
						}
						else
						{
							++undoneReduces;
						}
					}
					foreach (TaskAttempt attempt in task.GetAttempts().Values)
					{
						if (attempt.GetState() == TaskAttemptState.New && InitialNumberFreeSlots - slotsInUse
							.Get() >= TaskTypeSlots(task.GetType()))
						{
							TestRuntimeEstimators.MyTaskAttemptImpl attemptImpl = (TestRuntimeEstimators.MyTaskAttemptImpl
								)attempt;
							SpeculatorEvent @event = new SpeculatorEvent(attempt.GetID(), false, clock.GetTime
								());
							speculator.Handle(@event);
							attemptImpl.StartUp();
						}
						else
						{
							// If a task attempt is in progress we should send the news to
							// the Speculator.
							TaskAttemptStatusUpdateEvent.TaskAttemptStatus status = new TaskAttemptStatusUpdateEvent.TaskAttemptStatus
								();
							status.id = attempt.GetID();
							status.progress = attempt.GetProgress();
							status.stateString = attempt.GetState().ToString();
							status.taskState = attempt.GetState();
							SpeculatorEvent @event = new SpeculatorEvent(status, clock.GetTime());
							speculator.Handle(@event);
						}
					}
				}
				long startTime = Runtime.CurrentTimeMillis();
				// drain the speculator event queue
				while (!speculator.EventQueueEmpty())
				{
					Sharpen.Thread.Yield();
					if (Runtime.CurrentTimeMillis() > startTime + 130000)
					{
						return;
					}
				}
				clock.AdvanceTime(1000L);
				if (clock.GetTime() % 10000L == 0L)
				{
					speculator.ScanForSpeculations();
				}
			}
			NUnit.Framework.Assert.AreEqual("We got the wrong number of successful speculations."
				, expectedSpeculations, successfulSpeculations.Get());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLegacyEstimator()
		{
			TaskRuntimeEstimator specificEstimator = new LegacyTaskRuntimeEstimator();
			CoreTestEstimator(specificEstimator, 3);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExponentialEstimator()
		{
			TaskRuntimeEstimator specificEstimator = new ExponentiallySmoothedTaskRuntimeEstimator
				();
			CoreTestEstimator(specificEstimator, 3);
		}

		internal virtual int TaskTypeSlots(TaskType type)
		{
			return type == TaskType.Map ? MapSlotRequirement : ReduceSlotRequirement;
		}

		internal class SpeculationRequestEventHandler : EventHandler<TaskEvent>
		{
			public virtual void Handle(TaskEvent @event)
			{
				TaskId taskID = @event.GetTaskID();
				Task task = this._enclosing.myJob.GetTask(taskID);
				NUnit.Framework.Assert.AreEqual("Wrong type event", TaskEventType.TAddSpecAttempt
					, @event.GetType());
				System.Console.Out.WriteLine("SpeculationRequestEventHandler.handle adds a speculation task for "
					 + taskID);
				this._enclosing.AddAttempt(task);
			}

			internal SpeculationRequestEventHandler(TestRuntimeEstimators _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRuntimeEstimators _enclosing;
		}

		internal virtual void AddAttempt(Task task)
		{
			TestRuntimeEstimators.MyTaskImpl myTask = (TestRuntimeEstimators.MyTaskImpl)task;
			myTask.AddAttempt();
		}

		internal class MyTaskImpl : Task
		{
			private readonly TaskId taskID;

			private readonly IDictionary<TaskAttemptId, TaskAttempt> attempts = new ConcurrentHashMap
				<TaskAttemptId, TaskAttempt>(4);

			internal MyTaskImpl(TestRuntimeEstimators _enclosing, JobId jobID, int index, TaskType
				 type)
			{
				this._enclosing = _enclosing;
				this.taskID = this._enclosing.recordFactory.NewRecordInstance<TaskId>();
				this.taskID.SetId(index);
				this.taskID.SetTaskType(type);
				this.taskID.SetJobId(jobID);
			}

			internal virtual void AddAttempt()
			{
				TaskAttempt taskAttempt = new TestRuntimeEstimators.MyTaskAttemptImpl(this, this.
					taskID, this.attempts.Count, this._enclosing.clock);
				TaskAttemptId taskAttemptID = taskAttempt.GetID();
				this.attempts[taskAttemptID] = taskAttempt;
				System.Console.Out.WriteLine("TLTRE.MyTaskImpl.addAttempt " + this.GetID());
				SpeculatorEvent @event = new SpeculatorEvent(this.taskID, +1);
				this._enclosing.dispatcher.GetEventHandler().Handle(@event);
			}

			public virtual TaskId GetID()
			{
				return this.taskID;
			}

			public virtual TaskReport GetReport()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual Counters GetCounters()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual float GetProgress()
			{
				float result = 0.0F;
				foreach (TaskAttempt attempt in this.attempts.Values)
				{
					result = Math.Max(result, attempt.GetProgress());
				}
				return result;
			}

			public virtual TaskType GetType()
			{
				return this.taskID.GetTaskType();
			}

			public virtual IDictionary<TaskAttemptId, TaskAttempt> GetAttempts()
			{
				IDictionary<TaskAttemptId, TaskAttempt> result = new Dictionary<TaskAttemptId, TaskAttempt
					>(this.attempts.Count);
				result.PutAll(this.attempts);
				return result;
			}

			public virtual TaskAttempt GetAttempt(TaskAttemptId attemptID)
			{
				return this.attempts[attemptID];
			}

			public virtual bool IsFinished()
			{
				foreach (TaskAttempt attempt in this.attempts.Values)
				{
					if (attempt.GetState() == TaskAttemptState.Succeeded)
					{
						return true;
					}
				}
				return false;
			}

			public virtual bool CanCommit(TaskAttemptId taskAttemptID)
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual TaskState GetState()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			private readonly TestRuntimeEstimators _enclosing;
		}

		internal class MyJobImpl : Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
		{
			private readonly JobId jobID;

			private readonly IDictionary<TaskId, Task> allTasks = new Dictionary<TaskId, Task
				>();

			private readonly IDictionary<TaskId, Task> mapTasks = new Dictionary<TaskId, Task
				>();

			private readonly IDictionary<TaskId, Task> reduceTasks = new Dictionary<TaskId, Task
				>();

			internal MyJobImpl(TestRuntimeEstimators _enclosing, JobId jobID, int numMaps, int
				 numReduces)
			{
				this._enclosing = _enclosing;
				this.jobID = jobID;
				for (int i = 0; i < numMaps; ++i)
				{
					Task newTask = new TestRuntimeEstimators.MyTaskImpl(this, jobID, i, TaskType.Map);
					this.mapTasks[newTask.GetID()] = newTask;
					this.allTasks[newTask.GetID()] = newTask;
				}
				for (int i_1 = 0; i_1 < numReduces; ++i_1)
				{
					Task newTask = new TestRuntimeEstimators.MyTaskImpl(this, jobID, i_1, TaskType.Reduce
						);
					this.reduceTasks[newTask.GetID()] = newTask;
					this.allTasks[newTask.GetID()] = newTask;
				}
				// give every task an attempt
				foreach (Task task in this.allTasks.Values)
				{
					TestRuntimeEstimators.MyTaskImpl myTaskImpl = (TestRuntimeEstimators.MyTaskImpl)task;
					myTaskImpl.AddAttempt();
				}
			}

			public virtual JobId GetID()
			{
				return this.jobID;
			}

			public virtual JobState GetState()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual JobReport GetReport()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual float GetProgress()
			{
				return 0;
			}

			public virtual Counters GetAllCounters()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual IDictionary<TaskId, Task> GetTasks()
			{
				return this.allTasks;
			}

			public virtual IDictionary<TaskId, Task> GetTasks(TaskType taskType)
			{
				return taskType == TaskType.Map ? this.mapTasks : this.reduceTasks;
			}

			public virtual Task GetTask(TaskId taskID)
			{
				return this.allTasks[taskID];
			}

			public virtual IList<string> GetDiagnostics()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual int GetCompletedMaps()
			{
				return this._enclosing.completedMaps.Get();
			}

			public virtual int GetCompletedReduces()
			{
				return this._enclosing.completedReduces.Get();
			}

			public virtual TaskAttemptCompletionEvent[] GetTaskAttemptCompletionEvents(int fromEventId
				, int maxEvents)
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual TaskCompletionEvent[] GetMapAttemptCompletionEvents(int startIndex
				, int maxEvents)
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual string GetName()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual string GetQueueName()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual int GetTotalMaps()
			{
				return this.mapTasks.Count;
			}

			public virtual int GetTotalReduces()
			{
				return this.reduceTasks.Count;
			}

			public virtual bool IsUber()
			{
				return false;
			}

			public virtual bool CheckAccess(UserGroupInformation callerUGI, JobACL jobOperation
				)
			{
				return true;
			}

			public virtual string GetUserName()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual Path GetConfFile()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual IDictionary<JobACL, AccessControlList> GetJobACLs()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual IList<AMInfo> GetAMInfos()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual Configuration LoadConfFile()
			{
				throw new NotSupportedException();
			}

			public virtual void SetQueueName(string queueName)
			{
			}

			private readonly TestRuntimeEstimators _enclosing;
			// do nothing
		}

		internal class MyTaskAttemptImpl : TaskAttempt
		{
			private readonly TaskAttemptId myAttemptID;

			internal long startMockTime = long.MinValue;

			internal long shuffleCompletedTime = long.MaxValue;

			internal TaskAttemptState overridingState = TaskAttemptState.New;

			internal MyTaskAttemptImpl(TestRuntimeEstimators _enclosing, TaskId taskID, int index
				, Clock clock)
			{
				this._enclosing = _enclosing;
				/*
				* We follow the pattern of the real XxxImpl .  We create a job and initialize
				* it with a full suite of tasks which in turn have one attempt each in the
				* NEW state.  Attempts transition only from NEW to RUNNING to SUCCEEDED .
				*/
				this.myAttemptID = this._enclosing.recordFactory.NewRecordInstance<TaskAttemptId>
					();
				this.myAttemptID.SetId(index);
				this.myAttemptID.SetTaskId(taskID);
			}

			internal virtual void StartUp()
			{
				this.startMockTime = this._enclosing.clock.GetTime();
				this.overridingState = null;
				this._enclosing.slotsInUse.AddAndGet(this._enclosing.TaskTypeSlots(this.myAttemptID
					.GetTaskId().GetTaskType()));
				System.Console.Out.WriteLine("TLTRE.MyTaskAttemptImpl.startUp starting " + this.GetID
					());
				SpeculatorEvent @event = new SpeculatorEvent(this.GetID().GetTaskId(), -1);
				this._enclosing.dispatcher.GetEventHandler().Handle(@event);
			}

			/// <exception cref="System.NotSupportedException"/>
			public virtual NodeId GetNodeId()
			{
				throw new NotSupportedException();
			}

			public virtual TaskAttemptId GetID()
			{
				return this.myAttemptID;
			}

			public virtual TaskAttemptReport GetReport()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual IList<string> GetDiagnostics()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual Counters GetCounters()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual int GetShufflePort()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			private float GetCodeRuntime()
			{
				int taskIndex = this.myAttemptID.GetTaskId().GetId();
				int attemptIndex = this.myAttemptID.GetId();
				float result = 200.0F;
				switch (taskIndex % 4)
				{
					case 0:
					{
						if (taskIndex % 40 == 0 && attemptIndex == 0)
						{
							result = 600.0F;
							break;
						}
						break;
					}

					case 2:
					{
						break;
					}

					case 1:
					{
						result = 150.0F;
						break;
					}

					case 3:
					{
						result = 250.0F;
						break;
					}
				}
				return result;
			}

			private float GetMapProgress()
			{
				float runtime = this.GetCodeRuntime();
				return Math.Min((float)(this._enclosing.clock.GetTime() - this.startMockTime) / (
					runtime * 1000.0F), 1.0F);
			}

			private float GetReduceProgress()
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this._enclosing.myAppContext.GetJob
					(this.myAttemptID.GetTaskId().GetJobId());
				float runtime = this.GetCodeRuntime();
				ICollection<Task> allMapTasks = job.GetTasks(TaskType.Map).Values;
				int numberMaps = allMapTasks.Count;
				int numberDoneMaps = 0;
				foreach (Task mapTask in allMapTasks)
				{
					if (mapTask.IsFinished())
					{
						++numberDoneMaps;
					}
				}
				if (numberMaps == numberDoneMaps)
				{
					this.shuffleCompletedTime = Math.Min(this.shuffleCompletedTime, this._enclosing.clock
						.GetTime());
					return Math.Min((float)(this._enclosing.clock.GetTime() - this.shuffleCompletedTime
						) / (runtime * 2000.0F) + 0.5F, 1.0F);
				}
				else
				{
					return ((float)numberDoneMaps) / numberMaps * 0.5F;
				}
			}

			// we compute progress from time and an algorithm now
			public virtual float GetProgress()
			{
				if (this.overridingState == TaskAttemptState.New)
				{
					return 0.0F;
				}
				return this.myAttemptID.GetTaskId().GetTaskType() == TaskType.Map ? this.GetMapProgress
					() : this.GetReduceProgress();
			}

			public virtual Phase GetPhase()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual TaskAttemptState GetState()
			{
				if (this.overridingState != null)
				{
					return this.overridingState;
				}
				TaskAttemptState result = this.GetProgress() < 1.0F ? TaskAttemptState.Running : 
					TaskAttemptState.Succeeded;
				if (result == TaskAttemptState.Succeeded)
				{
					this.overridingState = TaskAttemptState.Succeeded;
					System.Console.Out.WriteLine("MyTaskAttemptImpl.getState() -- attempt " + this.myAttemptID
						 + " finished.");
					this._enclosing.slotsInUse.AddAndGet(-this._enclosing.TaskTypeSlots(this.myAttemptID
						.GetTaskId().GetTaskType()));
					(this.myAttemptID.GetTaskId().GetTaskType() == TaskType.Map ? this._enclosing.completedMaps
						 : this._enclosing.completedReduces).GetAndIncrement();
					// check for a spectacularly successful speculation
					TaskId taskID = this.myAttemptID.GetTaskId();
					Task task = this._enclosing.myJob.GetTask(taskID);
					foreach (TaskAttempt otherAttempt in task.GetAttempts().Values)
					{
						if (otherAttempt != this && otherAttempt.GetState() == TaskAttemptState.Running)
						{
							// we had two instances running.  Try to determine how much
							//  we might have saved by speculation
							if (this.GetID().GetId() > otherAttempt.GetID().GetId())
							{
								// the speculation won
								this._enclosing.successfulSpeculations.GetAndIncrement();
								float hisProgress = otherAttempt.GetProgress();
								long hisStartTime = ((TestRuntimeEstimators.MyTaskAttemptImpl)otherAttempt).startMockTime;
								System.Console.Out.WriteLine("TLTRE:A speculation finished at time " + this._enclosing
									.clock.GetTime() + ".  The stalled attempt is at " + (hisProgress * 100.0) + "% progress, and it started at "
									 + hisStartTime + ", which is " + (this._enclosing.clock.GetTime() - hisStartTime
									) + " ago.");
								long originalTaskEndEstimate = (hisStartTime + this._enclosing.estimator.EstimatedRuntime
									(otherAttempt.GetID()));
								System.Console.Out.WriteLine("TLTRE: We would have expected the original attempt to take "
									 + this._enclosing.estimator.EstimatedRuntime(otherAttempt.GetID()) + ", finishing at "
									 + originalTaskEndEstimate);
								long estimatedSavings = originalTaskEndEstimate - this._enclosing.clock.GetTime();
								this._enclosing.taskTimeSavedBySpeculation.AddAndGet(estimatedSavings);
								System.Console.Out.WriteLine("TLTRE: The task is " + task.GetID());
								this._enclosing.slotsInUse.AddAndGet(-this._enclosing.TaskTypeSlots(this.myAttemptID
									.GetTaskId().GetTaskType()));
								((TestRuntimeEstimators.MyTaskAttemptImpl)otherAttempt).overridingState = TaskAttemptState
									.Killed;
							}
							else
							{
								System.Console.Out.WriteLine("TLTRE: The normal attempt beat the speculation in "
									 + task.GetID());
							}
						}
					}
				}
				return result;
			}

			public virtual bool IsFinished()
			{
				return this.GetProgress() == 1.0F;
			}

			public virtual ContainerId GetAssignedContainerID()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual string GetNodeHttpAddress()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual string GetNodeRackName()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual long GetLaunchTime()
			{
				return this.startMockTime;
			}

			public virtual long GetFinishTime()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual long GetShuffleFinishTime()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual long GetSortFinishTime()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual string GetAssignedContainerMgrAddress()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			private readonly TestRuntimeEstimators _enclosing;
		}

		internal class MockClock : Clock
		{
			private long currentTime = 0;

			public virtual long GetTime()
			{
				return currentTime;
			}

			internal virtual void SetMeasuredTime(long newTime)
			{
				currentTime = newTime;
			}

			internal virtual void AdvanceTime(long increment)
			{
				currentTime += increment;
			}
		}

		internal class MyAppMaster : CompositeService
		{
			internal readonly Clock clock;

			public MyAppMaster(TestRuntimeEstimators _enclosing, Clock clock)
				: base(typeof(TestRuntimeEstimators.MyAppMaster).FullName)
			{
				this._enclosing = _enclosing;
				if (clock == null)
				{
					clock = new SystemClock();
				}
				this.clock = clock;
				TestRuntimeEstimators.Log.Info("Created MyAppMaster");
			}

			private readonly TestRuntimeEstimators _enclosing;
		}

		internal class MyAppContext : AppContext
		{
			private readonly ApplicationAttemptId myAppAttemptID;

			private readonly ApplicationId myApplicationID;

			private readonly JobId myJobID;

			private readonly IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> allJobs;

			internal MyAppContext(TestRuntimeEstimators _enclosing, int numberMaps, int numberReduces
				)
			{
				this._enclosing = _enclosing;
				this.myApplicationID = ApplicationId.NewInstance(this._enclosing.clock.GetTime(), 
					1);
				this.myAppAttemptID = ApplicationAttemptId.NewInstance(this.myApplicationID, 0);
				this.myJobID = this._enclosing.recordFactory.NewRecordInstance<JobId>();
				this.myJobID.SetAppId(this.myApplicationID);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job myJob = new TestRuntimeEstimators.MyJobImpl
					(this, this.myJobID, numberMaps, numberReduces);
				this.allJobs = Sharpen.Collections.SingletonMap(this.myJobID, myJob);
			}

			public virtual ApplicationAttemptId GetApplicationAttemptId()
			{
				return this.myAppAttemptID;
			}

			public virtual ApplicationId GetApplicationID()
			{
				return this.myApplicationID;
			}

			public virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetJob(JobId jobID)
			{
				return this.allJobs[jobID];
			}

			public virtual IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllJobs
				()
			{
				return this.allJobs;
			}

			public virtual EventHandler GetEventHandler()
			{
				return this._enclosing.dispatcher.GetEventHandler();
			}

			public virtual CharSequence GetUser()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public virtual Clock GetClock()
			{
				return this._enclosing.clock;
			}

			public virtual string GetApplicationName()
			{
				return null;
			}

			public virtual long GetStartTime()
			{
				return 0;
			}

			public virtual ClusterInfo GetClusterInfo()
			{
				return new ClusterInfo();
			}

			public virtual ICollection<string> GetBlacklistedNodes()
			{
				return null;
			}

			public virtual ClientToAMTokenSecretManager GetClientToAMTokenSecretManager()
			{
				return null;
			}

			public virtual bool IsLastAMRetry()
			{
				return false;
			}

			public virtual bool HasSuccessfullyUnregistered()
			{
				// bogus - Not Required
				return true;
			}

			public virtual string GetNMHostname()
			{
				// bogus - Not Required
				return null;
			}

			private readonly TestRuntimeEstimators _enclosing;
		}
	}
}
