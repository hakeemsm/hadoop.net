using System;
using System.Collections.Generic;
using System.Reflection;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Speculate
{
	public class DefaultSpeculator : AbstractService, Speculator
	{
		private const long OnSchedule = long.MinValue;

		private const long AlreadySpeculating = long.MinValue + 1;

		private const long TooNew = long.MinValue + 2;

		private const long ProgressIsGood = long.MinValue + 3;

		private const long NotRunning = long.MinValue + 4;

		private const long TooLateToSpeculate = long.MinValue + 5;

		private long soonestRetryAfterNoSpeculate;

		private long soonestRetryAfterSpeculate;

		private double proportionRunningTasksSpeculatable;

		private double proportionTotalTasksSpeculatable;

		private int minimumAllowedSpeculativeTasks;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Speculate.DefaultSpeculator
			));

		private readonly ConcurrentMap<TaskId, bool> runningTasks = new ConcurrentHashMap
			<TaskId, bool>();

		private readonly ConcurrentMap<TaskAttemptId, DefaultSpeculator.TaskAttemptHistoryStatistics
			> runningTaskAttemptStatistics = new ConcurrentHashMap<TaskAttemptId, DefaultSpeculator.TaskAttemptHistoryStatistics
			>();

		private const long MaxWaittingTimeForHeartbeat = 9 * 1000;

		private readonly ConcurrentMap<JobId, AtomicInteger> mapContainerNeeds = new ConcurrentHashMap
			<JobId, AtomicInteger>();

		private readonly ConcurrentMap<JobId, AtomicInteger> reduceContainerNeeds = new ConcurrentHashMap
			<JobId, AtomicInteger>();

		private readonly ICollection<TaskId> mayHaveSpeculated = new HashSet<TaskId>();

		private readonly Configuration conf;

		private AppContext context;

		private Sharpen.Thread speculationBackgroundThread = null;

		private volatile bool stopped = false;

		private BlockingQueue<SpeculatorEvent> eventQueue = new LinkedBlockingQueue<SpeculatorEvent
			>();

		private TaskRuntimeEstimator estimator;

		private BlockingQueue<object> scanControl = new LinkedBlockingQueue<object>();

		private readonly Clock clock;

		private readonly EventHandler<TaskEvent> eventHandler;

		public DefaultSpeculator(Configuration conf, AppContext context)
			: this(conf, context, context.GetClock())
		{
		}

		public DefaultSpeculator(Configuration conf, AppContext context, Clock clock)
			: this(conf, context, GetEstimator(conf, context), clock)
		{
		}

		// Used to track any TaskAttempts that aren't heart-beating for a while, so
		// that we can aggressively speculate instead of waiting for task-timeout.
		// Regular heartbeat from tasks is every 3 secs. So if we don't get a
		// heartbeat in 9 secs (3 heartbeats), we simulate a heartbeat with no change
		// in progress.
		// These are the current needs, not the initial needs.  For each job, these
		//  record the number of attempts that exist and that are actively
		//  waiting for a container [as opposed to running or finished]
		private static TaskRuntimeEstimator GetEstimator(Configuration conf, AppContext context
			)
		{
			TaskRuntimeEstimator estimator;
			try
			{
				// "yarn.mapreduce.job.task.runtime.estimator.class"
				Type estimatorClass = conf.GetClass<TaskRuntimeEstimator>(MRJobConfig.MrAmTaskEstimator
					, typeof(LegacyTaskRuntimeEstimator));
				Constructor<TaskRuntimeEstimator> estimatorConstructor = estimatorClass.GetConstructor
					();
				estimator = estimatorConstructor.NewInstance();
				estimator.Contextualize(conf, context);
			}
			catch (InstantiationException ex)
			{
				Log.Error("Can't make a speculation runtime estimator", ex);
				throw new YarnRuntimeException(ex);
			}
			catch (MemberAccessException ex)
			{
				Log.Error("Can't make a speculation runtime estimator", ex);
				throw new YarnRuntimeException(ex);
			}
			catch (TargetInvocationException ex)
			{
				Log.Error("Can't make a speculation runtime estimator", ex);
				throw new YarnRuntimeException(ex);
			}
			catch (MissingMethodException ex)
			{
				Log.Error("Can't make a speculation runtime estimator", ex);
				throw new YarnRuntimeException(ex);
			}
			return estimator;
		}

		public DefaultSpeculator(Configuration conf, AppContext context, TaskRuntimeEstimator
			 estimator, Clock clock)
			: base(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Speculate.DefaultSpeculator).FullName
				)
		{
			// This constructor is designed to be called by other constructors.
			//  However, it's public because we do use it in the test cases.
			// Normally we figure out our own estimator.
			this.conf = conf;
			this.context = context;
			this.estimator = estimator;
			this.clock = clock;
			this.eventHandler = context.GetEventHandler();
			this.soonestRetryAfterNoSpeculate = conf.GetLong(MRJobConfig.SpeculativeRetryAfterNoSpeculate
				, MRJobConfig.DefaultSpeculativeRetryAfterNoSpeculate);
			this.soonestRetryAfterSpeculate = conf.GetLong(MRJobConfig.SpeculativeRetryAfterSpeculate
				, MRJobConfig.DefaultSpeculativeRetryAfterSpeculate);
			this.proportionRunningTasksSpeculatable = conf.GetDouble(MRJobConfig.SpeculativecapRunningTasks
				, MRJobConfig.DefaultSpeculativecapRunningTasks);
			this.proportionTotalTasksSpeculatable = conf.GetDouble(MRJobConfig.SpeculativecapTotalTasks
				, MRJobConfig.DefaultSpeculativecapTotalTasks);
			this.minimumAllowedSpeculativeTasks = conf.GetInt(MRJobConfig.SpeculativeMinimumAllowedTasks
				, MRJobConfig.DefaultSpeculativeMinimumAllowedTasks);
		}

		/*   *************************************************************    */
		// This is the task-mongering that creates the two new threads -- one for
		//  processing events from the event queue and one for periodically
		//  looking for speculation opportunities
		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Runnable speculationBackgroundCore = new _Runnable_192(this);
			speculationBackgroundThread = new Sharpen.Thread(speculationBackgroundCore, "DefaultSpeculator background processing"
				);
			speculationBackgroundThread.Start();
			base.ServiceStart();
		}

		private sealed class _Runnable_192 : Runnable
		{
			public _Runnable_192(DefaultSpeculator _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				while (!this._enclosing.stopped && !Sharpen.Thread.CurrentThread().IsInterrupted(
					))
				{
					long backgroundRunStartTime = this._enclosing.clock.GetTime();
					try
					{
						int speculations = this._enclosing.ComputeSpeculations();
						long mininumRecomp = speculations > 0 ? this._enclosing.soonestRetryAfterSpeculate
							 : this._enclosing.soonestRetryAfterNoSpeculate;
						long wait = Math.Max(mininumRecomp, this._enclosing.clock.GetTime() - backgroundRunStartTime
							);
						if (speculations > 0)
						{
							Org.Apache.Hadoop.Mapreduce.V2.App.Speculate.DefaultSpeculator.Log.Info("We launched "
								 + speculations + " speculations.  Sleeping " + wait + " milliseconds.");
						}
						object pollResult = this._enclosing.scanControl.Poll(wait, TimeUnit.Milliseconds);
					}
					catch (Exception e)
					{
						if (!this._enclosing.stopped)
						{
							Org.Apache.Hadoop.Mapreduce.V2.App.Speculate.DefaultSpeculator.Log.Error("Background thread returning, interrupted"
								, e);
						}
						return;
					}
				}
			}

			private readonly DefaultSpeculator _enclosing;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			stopped = true;
			// this could be called before background thread is established
			if (speculationBackgroundThread != null)
			{
				speculationBackgroundThread.Interrupt();
			}
			base.ServiceStop();
		}

		public override void HandleAttempt(TaskAttemptStatusUpdateEvent.TaskAttemptStatus
			 status)
		{
			long timestamp = clock.GetTime();
			StatusUpdate(status, timestamp);
		}

		// This section is not part of the Speculator interface; it's used only for
		//  testing
		public virtual bool EventQueueEmpty()
		{
			return eventQueue.IsEmpty();
		}

		// This interface is intended to be used only for test cases.
		public virtual void ScanForSpeculations()
		{
			Log.Info("We got asked to run a debug speculation scan.");
			// debug
			System.Console.Out.WriteLine("We got asked to run a debug speculation scan.");
			System.Console.Out.WriteLine("There are " + scanControl.Count + " events stacked already."
				);
			scanControl.AddItem(new object());
			Sharpen.Thread.Yield();
		}

		/*   *************************************************************    */
		// This section contains the code that gets run for a SpeculatorEvent
		private AtomicInteger ContainerNeed(TaskId taskID)
		{
			JobId jobID = taskID.GetJobId();
			TaskType taskType = taskID.GetTaskType();
			ConcurrentMap<JobId, AtomicInteger> relevantMap = taskType == TaskType.Map ? mapContainerNeeds
				 : reduceContainerNeeds;
			AtomicInteger result = relevantMap[jobID];
			if (result == null)
			{
				relevantMap.PutIfAbsent(jobID, new AtomicInteger(0));
				result = relevantMap[jobID];
			}
			return result;
		}

		private void ProcessSpeculatorEvent(SpeculatorEvent @event)
		{
			lock (this)
			{
				switch (@event.GetType())
				{
					case Speculator.EventType.AttemptStatusUpdate:
					{
						StatusUpdate(@event.GetReportedStatus(), @event.GetTimestamp());
						break;
					}

					case Speculator.EventType.TaskContainerNeedUpdate:
					{
						AtomicInteger need = ContainerNeed(@event.GetTaskID());
						need.AddAndGet(@event.ContainersNeededChange());
						break;
					}

					case Speculator.EventType.AttemptStart:
					{
						Log.Info("ATTEMPT_START " + @event.GetTaskID());
						estimator.EnrollAttempt(@event.GetReportedStatus(), @event.GetTimestamp());
						break;
					}

					case Speculator.EventType.JobCreate:
					{
						Log.Info("JOB_CREATE " + @event.GetJobID());
						estimator.Contextualize(GetConfig(), context);
						break;
					}
				}
			}
		}

		/// <summary>Absorbs one TaskAttemptStatus</summary>
		/// <param name="reportedStatus">
		/// the status report that we got from a task attempt
		/// that we want to fold into the speculation data for this job
		/// </param>
		/// <param name="timestamp">
		/// the time this status corresponds to.  This matters
		/// because statuses contain progress.
		/// </param>
		protected internal virtual void StatusUpdate(TaskAttemptStatusUpdateEvent.TaskAttemptStatus
			 reportedStatus, long timestamp)
		{
			string stateString = reportedStatus.taskState.ToString();
			TaskAttemptId attemptID = reportedStatus.id;
			TaskId taskID = attemptID.GetTaskId();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = context.GetJob(taskID.GetJobId()
				);
			if (job == null)
			{
				return;
			}
			Task task = job.GetTask(taskID);
			if (task == null)
			{
				return;
			}
			estimator.UpdateAttempt(reportedStatus, timestamp);
			if (stateString.Equals(TaskAttemptState.Running.ToString()))
			{
				runningTasks.PutIfAbsent(taskID, true);
			}
			else
			{
				runningTasks.Remove(taskID, true);
				if (!stateString.Equals(TaskAttemptState.Starting.ToString()))
				{
					Sharpen.Collections.Remove(runningTaskAttemptStatistics, attemptID);
				}
			}
		}

		/*   *************************************************************    */
		// This is the code section that runs periodically and adds speculations for
		//  those jobs that need them.
		// This can return a few magic values for tasks that shouldn't speculate:
		//  returns ON_SCHEDULE if thresholdRuntime(taskID) says that we should not
		//     considering speculating this task
		//  returns ALREADY_SPECULATING if that is true.  This has priority.
		//  returns TOO_NEW if our companion task hasn't gotten any information
		//  returns PROGRESS_IS_GOOD if the task is sailing through
		//  returns NOT_RUNNING if the task is not running
		//
		// All of these values are negative.  Any value that should be allowed to
		//  speculate is 0 or positive.
		private long SpeculationValue(TaskId taskID, long now)
		{
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = context.GetJob(taskID.GetJobId()
				);
			Task task = job.GetTask(taskID);
			IDictionary<TaskAttemptId, TaskAttempt> attempts = task.GetAttempts();
			long acceptableRuntime = long.MinValue;
			long result = long.MinValue;
			if (!mayHaveSpeculated.Contains(taskID))
			{
				acceptableRuntime = estimator.ThresholdRuntime(taskID);
				if (acceptableRuntime == long.MaxValue)
				{
					return OnSchedule;
				}
			}
			TaskAttemptId runningTaskAttemptID = null;
			int numberRunningAttempts = 0;
			foreach (TaskAttempt taskAttempt in attempts.Values)
			{
				if (taskAttempt.GetState() == TaskAttemptState.Running || taskAttempt.GetState() 
					== TaskAttemptState.Starting)
				{
					if (++numberRunningAttempts > 1)
					{
						return AlreadySpeculating;
					}
					runningTaskAttemptID = taskAttempt.GetID();
					long estimatedRunTime = estimator.EstimatedRuntime(runningTaskAttemptID);
					long taskAttemptStartTime = estimator.AttemptEnrolledTime(runningTaskAttemptID);
					if (taskAttemptStartTime > now)
					{
						// This background process ran before we could process the task
						//  attempt status change that chronicles the attempt start
						return TooNew;
					}
					long estimatedEndTime = estimatedRunTime + taskAttemptStartTime;
					long estimatedReplacementEndTime = now + estimator.EstimatedNewAttemptRuntime(taskID
						);
					float progress = taskAttempt.GetProgress();
					DefaultSpeculator.TaskAttemptHistoryStatistics data = runningTaskAttemptStatistics
						[runningTaskAttemptID];
					if (data == null)
					{
						runningTaskAttemptStatistics[runningTaskAttemptID] = new DefaultSpeculator.TaskAttemptHistoryStatistics
							(estimatedRunTime, progress, now);
					}
					else
					{
						if (estimatedRunTime == data.GetEstimatedRunTime() && progress == data.GetProgress
							())
						{
							// Previous stats are same as same stats
							if (data.NotHeartbeatedInAWhile(now))
							{
								// Stats have stagnated for a while, simulate heart-beat.
								TaskAttemptStatusUpdateEvent.TaskAttemptStatus taskAttemptStatus = new TaskAttemptStatusUpdateEvent.TaskAttemptStatus
									();
								taskAttemptStatus.id = runningTaskAttemptID;
								taskAttemptStatus.progress = progress;
								taskAttemptStatus.taskState = taskAttempt.GetState();
								// Now simulate the heart-beat
								HandleAttempt(taskAttemptStatus);
							}
						}
						else
						{
							// Stats have changed - update our data structure
							data.SetEstimatedRunTime(estimatedRunTime);
							data.SetProgress(progress);
							data.ResetHeartBeatTime(now);
						}
					}
					if (estimatedEndTime < now)
					{
						return ProgressIsGood;
					}
					if (estimatedReplacementEndTime >= estimatedEndTime)
					{
						return TooLateToSpeculate;
					}
					result = estimatedEndTime - estimatedReplacementEndTime;
				}
			}
			// If we are here, there's at most one task attempt.
			if (numberRunningAttempts == 0)
			{
				return NotRunning;
			}
			if (acceptableRuntime == long.MinValue)
			{
				acceptableRuntime = estimator.ThresholdRuntime(taskID);
				if (acceptableRuntime == long.MaxValue)
				{
					return OnSchedule;
				}
			}
			return result;
		}

		//Add attempt to a given Task.
		protected internal virtual void AddSpeculativeAttempt(TaskId taskID)
		{
			Log.Info("DefaultSpeculator.addSpeculativeAttempt -- we are speculating " + taskID
				);
			eventHandler.Handle(new TaskEvent(taskID, TaskEventType.TAddSpecAttempt));
			mayHaveSpeculated.AddItem(taskID);
		}

		public virtual void Handle(SpeculatorEvent @event)
		{
			ProcessSpeculatorEvent(@event);
		}

		private int MaybeScheduleAMapSpeculation()
		{
			return MaybeScheduleASpeculation(TaskType.Map);
		}

		private int MaybeScheduleAReduceSpeculation()
		{
			return MaybeScheduleASpeculation(TaskType.Reduce);
		}

		private int MaybeScheduleASpeculation(TaskType type)
		{
			int successes = 0;
			long now = clock.GetTime();
			ConcurrentMap<JobId, AtomicInteger> containerNeeds = type == TaskType.Map ? mapContainerNeeds
				 : reduceContainerNeeds;
			foreach (KeyValuePair<JobId, AtomicInteger> jobEntry in containerNeeds)
			{
				// This race conditon is okay.  If we skip a speculation attempt we
				//  should have tried because the event that lowers the number of
				//  containers needed to zero hasn't come through, it will next time.
				// Also, if we miss the fact that the number of containers needed was
				//  zero but increased due to a failure it's not too bad to launch one
				//  container prematurely.
				if (jobEntry.Value.Get() > 0)
				{
					continue;
				}
				int numberSpeculationsAlready = 0;
				int numberRunningTasks = 0;
				// loop through the tasks of the kind
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = context.GetJob(jobEntry.Key);
				IDictionary<TaskId, Task> tasks = job.GetTasks(type);
				int numberAllowedSpeculativeTasks = (int)Math.Max(minimumAllowedSpeculativeTasks, 
					proportionTotalTasksSpeculatable * tasks.Count);
				TaskId bestTaskID = null;
				long bestSpeculationValue = -1L;
				// this loop is potentially pricey.
				// TODO track the tasks that are potentially worth looking at
				foreach (KeyValuePair<TaskId, Task> taskEntry in tasks)
				{
					long mySpeculationValue = SpeculationValue(taskEntry.Key, now);
					if (mySpeculationValue == AlreadySpeculating)
					{
						++numberSpeculationsAlready;
					}
					if (mySpeculationValue != NotRunning)
					{
						++numberRunningTasks;
					}
					if (mySpeculationValue > bestSpeculationValue)
					{
						bestTaskID = taskEntry.Key;
						bestSpeculationValue = mySpeculationValue;
					}
				}
				numberAllowedSpeculativeTasks = (int)Math.Max(numberAllowedSpeculativeTasks, proportionRunningTasksSpeculatable
					 * numberRunningTasks);
				// If we found a speculation target, fire it off
				if (bestTaskID != null && numberAllowedSpeculativeTasks > numberSpeculationsAlready)
				{
					AddSpeculativeAttempt(bestTaskID);
					++successes;
				}
			}
			return successes;
		}

		private int ComputeSpeculations()
		{
			// We'll try to issue one map and one reduce speculation per job per run
			return MaybeScheduleAMapSpeculation() + MaybeScheduleAReduceSpeculation();
		}

		internal class TaskAttemptHistoryStatistics
		{
			private long estimatedRunTime;

			private float progress;

			private long lastHeartBeatTime;

			public TaskAttemptHistoryStatistics(long estimatedRunTime, float progress, long nonProgressStartTime
				)
			{
				this.estimatedRunTime = estimatedRunTime;
				this.progress = progress;
				ResetHeartBeatTime(nonProgressStartTime);
			}

			public virtual long GetEstimatedRunTime()
			{
				return this.estimatedRunTime;
			}

			public virtual float GetProgress()
			{
				return this.progress;
			}

			public virtual void SetEstimatedRunTime(long estimatedRunTime)
			{
				this.estimatedRunTime = estimatedRunTime;
			}

			public virtual void SetProgress(float progress)
			{
				this.progress = progress;
			}

			public virtual bool NotHeartbeatedInAWhile(long now)
			{
				if (now - lastHeartBeatTime <= MaxWaittingTimeForHeartbeat)
				{
					return false;
				}
				else
				{
					ResetHeartBeatTime(now);
					return true;
				}
			}

			public virtual void ResetHeartBeatTime(long lastHeartBeatTime)
			{
				this.lastHeartBeatTime = lastHeartBeatTime;
			}
		}

		[VisibleForTesting]
		public virtual long GetSoonestRetryAfterNoSpeculate()
		{
			return soonestRetryAfterNoSpeculate;
		}

		[VisibleForTesting]
		public virtual long GetSoonestRetryAfterSpeculate()
		{
			return soonestRetryAfterSpeculate;
		}

		[VisibleForTesting]
		public virtual double GetProportionRunningTasksSpeculatable()
		{
			return proportionRunningTasksSpeculatable;
		}

		[VisibleForTesting]
		public virtual double GetProportionTotalTasksSpeculatable()
		{
			return proportionTotalTasksSpeculatable;
		}

		[VisibleForTesting]
		public virtual int GetMinimumAllowedSpeculativeTasks()
		{
			return minimumAllowedSpeculativeTasks;
		}
	}
}
