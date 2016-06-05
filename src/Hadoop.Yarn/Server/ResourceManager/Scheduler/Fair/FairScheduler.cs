using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>A scheduler that schedules resources between a set of queues.</summary>
	/// <remarks>
	/// A scheduler that schedules resources between a set of queues. The scheduler
	/// keeps track of the resources used by each queue, and attempts to maintain
	/// fairness by scheduling tasks at queues whose allocations are farthest below
	/// an ideal fair distribution.
	/// The fair scheduler supports hierarchical queues. All queues descend from a
	/// queue named "root". Available resources are distributed among the children
	/// of the root queue in the typical fair scheduling fashion. Then, the children
	/// distribute the resources assigned to them to their children in the same
	/// fashion.  Applications may only be scheduled on leaf queues. Queues can be
	/// specified as children of other queues by placing them as sub-elements of their
	/// parents in the fair scheduler configuration file.
	/// A queue's name starts with the names of its parents, with periods as
	/// separators.  So a queue named "queue1" under the root named, would be
	/// referred to as "root.queue1", and a queue named "queue2" under a queue
	/// named "parent1" would be referred to as "root.parent1.queue2".
	/// </remarks>
	public class FairScheduler : AbstractYarnScheduler<FSAppAttempt, FSSchedulerNode>
	{
		private FairSchedulerConfiguration conf;

		private Resource incrAllocation;

		private QueueManager queueMgr;

		private volatile Clock clock;

		private bool usePortForNodeName;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FairScheduler
			));

		private static readonly ResourceCalculator ResourceCalculator = new DefaultResourceCalculator
			();

		private static readonly ResourceCalculator DominantResourceCalculator = new DominantResourceCalculator
			();

		public static readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource ContainerReserved
			 = Resources.CreateResource(-1);

		protected internal long updateInterval;

		private readonly int UpdateDebugFrequency = 5;

		private int updatesToSkipForDebug;

		[VisibleForTesting]
		internal Sharpen.Thread updateThread;

		[VisibleForTesting]
		internal Sharpen.Thread schedulingThread;

		protected internal readonly long ThreadJoinTimeoutMs = 1000;

		internal FSQueueMetrics rootMetrics;

		internal FSOpDurations fsOpDurations;

		protected internal long lastPreemptionUpdateTime;

		private long lastPreemptCheckTime;

		protected internal bool preemptionEnabled;

		protected internal float preemptionUtilizationThreshold;

		protected internal long preemptionInterval;

		protected internal long waitTimeBeforeKill;

		private IList<RMContainer> warnedContainers = new AList<RMContainer>();

		protected internal bool sizeBasedWeight;

		protected internal WeightAdjuster weightAdjuster;

		protected internal bool continuousSchedulingEnabled;

		protected internal int continuousSchedulingSleepMs;

		private IComparer<NodeId> nodeAvailableResourceComparator;

		protected internal double nodeLocalityThreshold;

		protected internal double rackLocalityThreshold;

		protected internal long nodeLocalityDelayMs;

		protected internal long rackLocalityDelayMs;

		private FairSchedulerEventLog eventLog;

		protected internal bool assignMultiple;

		protected internal int maxAssign;

		[VisibleForTesting]
		internal readonly MaxRunningAppsEnforcer maxRunningEnforcer;

		private AllocationFileLoaderService allocsLoader;

		[VisibleForTesting]
		internal AllocationConfiguration allocConf;

		public FairScheduler()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FairScheduler
				).FullName)
		{
			updatesToSkipForDebug = UpdateDebugFrequency;
			nodeAvailableResourceComparator = new FairScheduler.NodeAvailableResourceComparator
				(this);
			// Value that container assignment methods return when a container is
			// reserved
			// How often fair shares are re-calculated (ms)
			// timeout to join when we stop this service
			// Aggregate metrics
			// Time when we last updated preemption vars
			// Time we last ran preemptTasksIfNecessary
			// Preemption related variables
			// How often tasks are preempted
			// ms to wait before force killing stuff (must be longer than a couple
			// of heartbeats to give task-kill commands a chance to act).
			// Containers whose AMs have been warned that they will be preempted soon.
			// Give larger weights to larger jobs
			// Can be null for no weight adjuster
			// Continuous Scheduling enabled or not
			// Sleep time for each pass in continuous scheduling
			// Node available resource comparator
			// Cluster threshold for node locality
			// Cluster threshold for rack locality
			// Delay for node locality
			// Delay for rack locality
			// Machine-readable event log
			// Allocate multiple containers per
			// heartbeat
			// Max containers to assign per heartbeat
			clock = new SystemClock();
			allocsLoader = new AllocationFileLoaderService();
			queueMgr = new QueueManager(this);
			maxRunningEnforcer = new MaxRunningAppsEnforcer(this);
		}

		private void ValidateConf(Configuration conf)
		{
			// validate scheduler memory allocation setting
			int minMem = conf.GetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationMb);
			int maxMem = conf.GetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMaximumAllocationMb);
			if (minMem < 0 || minMem > maxMem)
			{
				throw new YarnRuntimeException("Invalid resource scheduler memory" + " allocation configuration"
					 + ", " + YarnConfiguration.RmSchedulerMinimumAllocationMb + "=" + minMem + ", "
					 + YarnConfiguration.RmSchedulerMaximumAllocationMb + "=" + maxMem + ", min should equal greater than 0"
					 + ", max should be no smaller than min.");
			}
			// validate scheduler vcores allocation setting
			int minVcores = conf.GetInt(YarnConfiguration.RmSchedulerMinimumAllocationVcores, 
				YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
			int maxVcores = conf.GetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores, 
				YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores);
			if (minVcores < 0 || minVcores > maxVcores)
			{
				throw new YarnRuntimeException("Invalid resource scheduler vcores" + " allocation configuration"
					 + ", " + YarnConfiguration.RmSchedulerMinimumAllocationVcores + "=" + minVcores
					 + ", " + YarnConfiguration.RmSchedulerMaximumAllocationVcores + "=" + maxVcores
					 + ", min should equal greater than 0" + ", max should be no smaller than min.");
			}
		}

		public virtual FairSchedulerConfiguration GetConf()
		{
			return conf;
		}

		public virtual QueueManager GetQueueManager()
		{
			return queueMgr;
		}

		/// <summary>
		/// Thread which calls
		/// <see cref="FairScheduler.Update()"/>
		/// every
		/// <code>updateInterval</code> milliseconds.
		/// </summary>
		private class UpdateThread : Sharpen.Thread
		{
			public override void Run()
			{
				while (!Sharpen.Thread.CurrentThread().IsInterrupted())
				{
					try
					{
						Sharpen.Thread.Sleep(this._enclosing.updateInterval);
						long start = this._enclosing.GetClock().GetTime();
						this._enclosing.Update();
						this._enclosing.PreemptTasksIfNecessary();
						long duration = this._enclosing.GetClock().GetTime() - start;
						this._enclosing.fsOpDurations.AddUpdateThreadRunDuration(duration);
					}
					catch (Exception)
					{
						FairScheduler.Log.Warn("Update thread interrupted. Exiting.");
						return;
					}
					catch (Exception e)
					{
						FairScheduler.Log.Error("Exception in fair scheduler UpdateThread", e);
					}
				}
			}

			internal UpdateThread(FairScheduler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FairScheduler _enclosing;
		}

		/// <summary>
		/// Thread which attempts scheduling resources continuously,
		/// asynchronous to the node heartbeats.
		/// </summary>
		private class ContinuousSchedulingThread : Sharpen.Thread
		{
			public override void Run()
			{
				while (!Sharpen.Thread.CurrentThread().IsInterrupted())
				{
					try
					{
						this._enclosing.ContinuousSchedulingAttempt();
						Sharpen.Thread.Sleep(this._enclosing.GetContinuousSchedulingSleepMs());
					}
					catch (Exception e)
					{
						FairScheduler.Log.Warn("Continuous scheduling thread interrupted. Exiting.", e);
						return;
					}
				}
			}

			internal ContinuousSchedulingThread(FairScheduler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FairScheduler _enclosing;
		}

		/// <summary>
		/// Recompute the internal variables used by the scheduler - per-job weights,
		/// fair shares, deficits, minimum slot allocations, and amount of used and
		/// required resources per job.
		/// </summary>
		protected internal virtual void Update()
		{
			lock (this)
			{
				long start = GetClock().GetTime();
				UpdateStarvationStats();
				// Determine if any queues merit preemption
				FSQueue rootQueue = queueMgr.GetRootQueue();
				// Recursively update demands for all queues
				rootQueue.UpdateDemand();
				rootQueue.SetFairShare(clusterResource);
				// Recursively compute fair shares for all queues
				// and update metrics
				rootQueue.RecomputeShares();
				UpdateRootQueueMetrics();
				if (Log.IsDebugEnabled())
				{
					if (--updatesToSkipForDebug < 0)
					{
						updatesToSkipForDebug = UpdateDebugFrequency;
						Log.Debug("Cluster Capacity: " + clusterResource + "  Allocations: " + rootMetrics
							.GetAllocatedResources() + "  Availability: " + Org.Apache.Hadoop.Yarn.Api.Records.Resource
							.NewInstance(rootMetrics.GetAvailableMB(), rootMetrics.GetAvailableVirtualCores(
							)) + "  Demand: " + rootQueue.GetDemand());
					}
				}
				long duration = GetClock().GetTime() - start;
				fsOpDurations.AddUpdateCallDuration(duration);
			}
		}

		/// <summary>Update the preemption fields for all QueueScheduables, i.e.</summary>
		/// <remarks>
		/// Update the preemption fields for all QueueScheduables, i.e. the times since
		/// each queue last was at its guaranteed share and over its fair share
		/// threshold for each type of task.
		/// </remarks>
		private void UpdateStarvationStats()
		{
			lastPreemptionUpdateTime = clock.GetTime();
			foreach (FSLeafQueue sched in queueMgr.GetLeafQueues())
			{
				sched.UpdateStarvationStats();
			}
		}

		/// <summary>
		/// Check for queues that need tasks preempted, either because they have been
		/// below their guaranteed share for minSharePreemptionTimeout or they have
		/// been below their fair share threshold for the fairSharePreemptionTimeout.
		/// </summary>
		/// <remarks>
		/// Check for queues that need tasks preempted, either because they have been
		/// below their guaranteed share for minSharePreemptionTimeout or they have
		/// been below their fair share threshold for the fairSharePreemptionTimeout. If
		/// such queues exist, compute how many tasks of each type need to be preempted
		/// and then select the right ones using preemptTasks.
		/// </remarks>
		protected internal virtual void PreemptTasksIfNecessary()
		{
			lock (this)
			{
				if (!ShouldAttemptPreemption())
				{
					return;
				}
				long curTime = GetClock().GetTime();
				if (curTime - lastPreemptCheckTime < preemptionInterval)
				{
					return;
				}
				lastPreemptCheckTime = curTime;
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resToPreempt = Resources.Clone(Resources
					.None());
				foreach (FSLeafQueue sched in queueMgr.GetLeafQueues())
				{
					Resources.AddTo(resToPreempt, ResToPreempt(sched, curTime));
				}
				if (Resources.GreaterThan(ResourceCalculator, clusterResource, resToPreempt, Resources
					.None()))
				{
					PreemptResources(resToPreempt);
				}
			}
		}

		/// <summary>Preempt a quantity of resources.</summary>
		/// <remarks>
		/// Preempt a quantity of resources. Each round, we start from the root queue,
		/// level-by-level, until choosing a candidate application.
		/// The policy for prioritizing preemption for each queue depends on its
		/// SchedulingPolicy: (1) fairshare/DRF, choose the ChildSchedulable that is
		/// most over its fair share; (2) FIFO, choose the childSchedulable that is
		/// latest launched.
		/// Inside each application, we further prioritize preemption by choosing
		/// containers with lowest priority to preempt.
		/// We make sure that no queue is placed below its fair share in the process.
		/// </remarks>
		protected internal virtual void PreemptResources(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 toPreempt)
		{
			long start = GetClock().GetTime();
			if (Resources.Equals(toPreempt, Resources.None()))
			{
				return;
			}
			// Scan down the list of containers we've already warned and kill them
			// if we need to.  Remove any containers from the list that we don't need
			// or that are no longer running.
			IEnumerator<RMContainer> warnedIter = warnedContainers.GetEnumerator();
			while (warnedIter.HasNext())
			{
				RMContainer container = warnedIter.Next();
				if ((container.GetState() == RMContainerState.Running || container.GetState() == 
					RMContainerState.Allocated) && Resources.GreaterThan(ResourceCalculator, clusterResource
					, toPreempt, Resources.None()))
				{
					WarnOrKillContainer(container);
					Resources.SubtractFrom(toPreempt, container.GetContainer().GetResource());
				}
				else
				{
					warnedIter.Remove();
				}
			}
			try
			{
				// Reset preemptedResource for each app
				foreach (FSLeafQueue queue in GetQueueManager().GetLeafQueues())
				{
					queue.ResetPreemptedResources();
				}
				while (Resources.GreaterThan(ResourceCalculator, clusterResource, toPreempt, Resources
					.None()))
				{
					RMContainer container = GetQueueManager().GetRootQueue().PreemptContainer();
					if (container == null)
					{
						break;
					}
					else
					{
						WarnOrKillContainer(container);
						warnedContainers.AddItem(container);
						Resources.SubtractFrom(toPreempt, container.GetContainer().GetResource());
					}
				}
			}
			finally
			{
				// Clear preemptedResources for each app
				foreach (FSLeafQueue queue in GetQueueManager().GetLeafQueues())
				{
					queue.ClearPreemptedResources();
				}
			}
			long duration = GetClock().GetTime() - start;
			fsOpDurations.AddPreemptCallDuration(duration);
		}

		protected internal virtual void WarnOrKillContainer(RMContainer container)
		{
			ApplicationAttemptId appAttemptId = container.GetApplicationAttemptId();
			FSAppAttempt app = GetSchedulerApp(appAttemptId);
			FSLeafQueue queue = ((FSLeafQueue)app.GetQueue());
			Log.Info("Preempting container (prio=" + container.GetContainer().GetPriority() +
				 "res=" + container.GetContainer().GetResource() + ") from queue " + queue.GetName
				());
			long time = app.GetContainerPreemptionTime(container);
			if (time != null)
			{
				// if we asked for preemption more than maxWaitTimeBeforeKill ms ago,
				// proceed with kill
				if (time + waitTimeBeforeKill < GetClock().GetTime())
				{
					ContainerStatus status = SchedulerUtils.CreatePreemptedContainerStatus(container.
						GetContainerId(), SchedulerUtils.PreemptedContainer);
					// TODO: Not sure if this ever actually adds this to the list of cleanup
					// containers on the RMNode (see SchedulerNode.releaseContainer()).
					CompletedContainer(container, status, RMContainerEventType.Kill);
					Log.Info("Killing container" + container + " (after waiting for premption for " +
						 (GetClock().GetTime() - time) + "ms)");
				}
			}
			else
			{
				// track the request in the FSAppAttempt itself
				app.AddPreemption(container, GetClock().GetTime());
			}
		}

		/// <summary>Return the resource amount that this queue is allowed to preempt, if any.
		/// 	</summary>
		/// <remarks>
		/// Return the resource amount that this queue is allowed to preempt, if any.
		/// If the queue has been below its min share for at least its preemption
		/// timeout, it should preempt the difference between its current share and
		/// this min share. If it has been below its fair share preemption threshold
		/// for at least the fairSharePreemptionTimeout, it should preempt enough tasks
		/// to get up to its full fair share. If both conditions hold, we preempt the
		/// max of the two amounts (this shouldn't happen unless someone sets the
		/// timeouts to be identical for some reason).
		/// </remarks>
		protected internal virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource ResToPreempt
			(FSLeafQueue sched, long curTime)
		{
			long minShareTimeout = sched.GetMinSharePreemptionTimeout();
			long fairShareTimeout = sched.GetFairSharePreemptionTimeout();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource resDueToMinShare = Resources.None();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource resDueToFairShare = Resources.None();
			if (curTime - sched.GetLastTimeAtMinShare() > minShareTimeout)
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource target = Resources.Min(ResourceCalculator
					, clusterResource, sched.GetMinShare(), sched.GetDemand());
				resDueToMinShare = Resources.Max(ResourceCalculator, clusterResource, Resources.None
					(), Resources.Subtract(target, sched.GetResourceUsage()));
			}
			if (curTime - sched.GetLastTimeAtFairShareThreshold() > fairShareTimeout)
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource target = Resources.Min(ResourceCalculator
					, clusterResource, sched.GetFairShare(), sched.GetDemand());
				resDueToFairShare = Resources.Max(ResourceCalculator, clusterResource, Resources.
					None(), Resources.Subtract(target, sched.GetResourceUsage()));
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Resource resToPreempt = Resources.Max(ResourceCalculator
				, clusterResource, resDueToMinShare, resDueToFairShare);
			if (Resources.GreaterThan(ResourceCalculator, clusterResource, resToPreempt, Resources
				.None()))
			{
				string message = "Should preempt " + resToPreempt + " res for queue " + sched.GetName
					() + ": resDueToMinShare = " + resDueToMinShare + ", resDueToFairShare = " + resDueToFairShare;
				Log.Info(message);
			}
			return resToPreempt;
		}

		public virtual RMContainerTokenSecretManager GetContainerTokenSecretManager()
		{
			lock (this)
			{
				return rmContext.GetContainerTokenSecretManager();
			}
		}

		// synchronized for sizeBasedWeight
		public virtual ResourceWeights GetAppWeight(FSAppAttempt app)
		{
			lock (this)
			{
				double weight = 1.0;
				if (sizeBasedWeight)
				{
					// Set weight based on current memory demand
					weight = Math.Log1p(app.GetDemand().GetMemory()) / Math.Log(2);
				}
				weight *= app.GetPriority().GetPriority();
				if (weightAdjuster != null)
				{
					// Run weight through the user-supplied weightAdjuster
					weight = weightAdjuster.AdjustWeight(app, weight);
				}
				ResourceWeights resourceWeights = app.GetResourceWeights();
				resourceWeights.SetWeight((float)weight);
				return resourceWeights;
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetIncrementResourceCapability
			()
		{
			return incrAllocation;
		}

		private FSSchedulerNode GetFSSchedulerNode(NodeId nodeId)
		{
			return nodes[nodeId];
		}

		public virtual double GetNodeLocalityThreshold()
		{
			return nodeLocalityThreshold;
		}

		public virtual double GetRackLocalityThreshold()
		{
			return rackLocalityThreshold;
		}

		public virtual long GetNodeLocalityDelayMs()
		{
			return nodeLocalityDelayMs;
		}

		public virtual long GetRackLocalityDelayMs()
		{
			return rackLocalityDelayMs;
		}

		public virtual bool IsContinuousSchedulingEnabled()
		{
			return continuousSchedulingEnabled;
		}

		public virtual int GetContinuousSchedulingSleepMs()
		{
			lock (this)
			{
				return continuousSchedulingSleepMs;
			}
		}

		public virtual Clock GetClock()
		{
			return clock;
		}

		[VisibleForTesting]
		internal virtual void SetClock(Clock clock)
		{
			this.clock = clock;
		}

		public virtual FairSchedulerEventLog GetEventLog()
		{
			return eventLog;
		}

		/// <summary>
		/// Add a new application to the scheduler, with a given id, queue name, and
		/// user.
		/// </summary>
		/// <remarks>
		/// Add a new application to the scheduler, with a given id, queue name, and
		/// user. This will accept a new app even if the user or queue is above
		/// configured limits, but the app will not be marked as runnable.
		/// </remarks>
		protected internal virtual void AddApplication(ApplicationId applicationId, string
			 queueName, string user, bool isAppRecovering)
		{
			lock (this)
			{
				if (queueName == null || queueName.IsEmpty())
				{
					string message = "Reject application " + applicationId + " submitted by user " + 
						user + " with an empty queue name.";
					Log.Info(message);
					rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId, 
						RMAppEventType.AppRejected, message));
					return;
				}
				if (queueName.StartsWith(".") || queueName.EndsWith("."))
				{
					string message = "Reject application " + applicationId + " submitted by user " + 
						user + " with an illegal queue name " + queueName + ". " + "The queue name cannot start/end with period.";
					Log.Info(message);
					rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId, 
						RMAppEventType.AppRejected, message));
					return;
				}
				RMApp rmApp = rmContext.GetRMApps()[applicationId];
				FSLeafQueue queue = AssignToQueue(rmApp, queueName, user);
				if (queue == null)
				{
					return;
				}
				// Enforce ACLs
				UserGroupInformation userUgi = UserGroupInformation.CreateRemoteUser(user);
				if (!queue.HasAccess(QueueACL.SubmitApplications, userUgi) && !queue.HasAccess(QueueACL
					.AdministerQueue, userUgi))
				{
					string msg = "User " + userUgi.GetUserName() + " cannot submit applications to queue "
						 + queue.GetName();
					Log.Info(msg);
					rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId, 
						RMAppEventType.AppRejected, msg));
					return;
				}
				SchedulerApplication<FSAppAttempt> application = new SchedulerApplication<FSAppAttempt
					>(queue, user);
				applications[applicationId] = application;
				queue.GetMetrics().SubmitApp(user);
				Log.Info("Accepted application " + applicationId + " from user: " + user + ", in queue: "
					 + queueName + ", currently num of applications: " + applications.Count);
				if (isAppRecovering)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
					}
				}
				else
				{
					rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId, 
						RMAppEventType.AppAccepted));
				}
			}
		}

		/// <summary>Add a new application attempt to the scheduler.</summary>
		protected internal virtual void AddApplicationAttempt(ApplicationAttemptId applicationAttemptId
			, bool transferStateFromPreviousAttempt, bool isAttemptRecovering)
		{
			lock (this)
			{
				SchedulerApplication<FSAppAttempt> application = applications[applicationAttemptId
					.GetApplicationId()];
				string user = application.GetUser();
				FSLeafQueue queue = (FSLeafQueue)application.GetQueue();
				FSAppAttempt attempt = new FSAppAttempt(this, applicationAttemptId, user, queue, 
					new ActiveUsersManager(GetRootQueueMetrics()), rmContext);
				if (transferStateFromPreviousAttempt)
				{
					attempt.TransferStateFromPreviousAttempt(application.GetCurrentAppAttempt());
				}
				application.SetCurrentAppAttempt(attempt);
				bool runnable = maxRunningEnforcer.CanAppBeRunnable(queue, user);
				queue.AddApp(attempt, runnable);
				if (runnable)
				{
					maxRunningEnforcer.TrackRunnableApp(attempt);
				}
				else
				{
					maxRunningEnforcer.TrackNonRunnableApp(attempt);
				}
				queue.GetMetrics().SubmitAppAttempt(user);
				Log.Info("Added Application Attempt " + applicationAttemptId + " to scheduler from user: "
					 + user);
				if (isAttemptRecovering)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(applicationAttemptId + " is recovering. Skipping notifying ATTEMPT_ADDED"
							);
					}
				}
				else
				{
					rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppAttemptEvent(applicationAttemptId
						, RMAppAttemptEventType.AttemptAdded));
				}
			}
		}

		/// <summary>Helper method that attempts to assign the app to a queue.</summary>
		/// <remarks>
		/// Helper method that attempts to assign the app to a queue. The method is
		/// responsible to call the appropriate event-handler if the app is rejected.
		/// </remarks>
		[VisibleForTesting]
		internal virtual FSLeafQueue AssignToQueue(RMApp rmApp, string queueName, string 
			user)
		{
			FSLeafQueue queue = null;
			string appRejectMsg = null;
			try
			{
				QueuePlacementPolicy placementPolicy = allocConf.GetPlacementPolicy();
				queueName = placementPolicy.AssignAppToQueue(queueName, user);
				if (queueName == null)
				{
					appRejectMsg = "Application rejected by queue placement policy";
				}
				else
				{
					queue = queueMgr.GetLeafQueue(queueName, true);
					if (queue == null)
					{
						appRejectMsg = queueName + " is not a leaf queue";
					}
				}
			}
			catch (IOException)
			{
				appRejectMsg = "Error assigning app to queue " + queueName;
			}
			if (appRejectMsg != null && rmApp != null)
			{
				Log.Error(appRejectMsg);
				rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(rmApp.GetApplicationId
					(), RMAppEventType.AppRejected, appRejectMsg));
				return null;
			}
			if (rmApp != null)
			{
				rmApp.SetQueue(queue.GetName());
			}
			else
			{
				Log.Error("Couldn't find RM app to set queue name on");
			}
			return queue;
		}

		private void RemoveApplication(ApplicationId applicationId, RMAppState finalState
			)
		{
			lock (this)
			{
				SchedulerApplication<FSAppAttempt> application = applications[applicationId];
				if (application == null)
				{
					Log.Warn("Couldn't find application " + applicationId);
					return;
				}
				application.Stop(finalState);
				Sharpen.Collections.Remove(applications, applicationId);
			}
		}

		private void RemoveApplicationAttempt(ApplicationAttemptId applicationAttemptId, 
			RMAppAttemptState rmAppAttemptFinalState, bool keepContainers)
		{
			lock (this)
			{
				Log.Info("Application " + applicationAttemptId + " is done." + " finalState=" + rmAppAttemptFinalState
					);
				SchedulerApplication<FSAppAttempt> application = applications[applicationAttemptId
					.GetApplicationId()];
				FSAppAttempt attempt = GetSchedulerApp(applicationAttemptId);
				if (attempt == null || application == null)
				{
					Log.Info("Unknown application " + applicationAttemptId + " has completed!");
					return;
				}
				// Release all the running containers
				foreach (RMContainer rmContainer in attempt.GetLiveContainers())
				{
					if (keepContainers && rmContainer.GetState().Equals(RMContainerState.Running))
					{
						// do not kill the running container in the case of work-preserving AM
						// restart.
						Log.Info("Skip killing " + rmContainer.GetContainerId());
						continue;
					}
					CompletedContainer(rmContainer, SchedulerUtils.CreateAbnormalContainerStatus(rmContainer
						.GetContainerId(), SchedulerUtils.CompletedApplication), RMContainerEventType.Kill
						);
				}
				// Release all reserved containers
				foreach (RMContainer rmContainer_1 in attempt.GetReservedContainers())
				{
					CompletedContainer(rmContainer_1, SchedulerUtils.CreateAbnormalContainerStatus(rmContainer_1
						.GetContainerId(), "Application Complete"), RMContainerEventType.Kill);
				}
				// Clean up pending requests, metrics etc.
				attempt.Stop(rmAppAttemptFinalState);
				// Inform the queue
				FSLeafQueue queue = queueMgr.GetLeafQueue(((FSLeafQueue)attempt.GetQueue()).GetQueueName
					(), false);
				bool wasRunnable = queue.RemoveApp(attempt);
				if (wasRunnable)
				{
					maxRunningEnforcer.UntrackRunnableApp(attempt);
					maxRunningEnforcer.UpdateRunnabilityOnAppRemoval(attempt, ((FSLeafQueue)attempt.GetQueue
						()));
				}
				else
				{
					maxRunningEnforcer.UntrackNonRunnableApp(attempt);
				}
			}
		}

		/// <summary>Clean up a completed container.</summary>
		protected internal override void CompletedContainer(RMContainer rmContainer, ContainerStatus
			 containerStatus, RMContainerEventType @event)
		{
			lock (this)
			{
				if (rmContainer == null)
				{
					Log.Info("Null container completed...");
					return;
				}
				Container container = rmContainer.GetContainer();
				// Get the application for the finished container
				FSAppAttempt application = GetCurrentAttemptForContainer(container.GetId());
				ApplicationId appId = container.GetId().GetApplicationAttemptId().GetApplicationId
					();
				if (application == null)
				{
					Log.Info("Container " + container + " of" + " unknown application attempt " + appId
						 + " completed with event " + @event);
					return;
				}
				// Get the node on which the container was allocated
				FSSchedulerNode node = GetFSSchedulerNode(container.GetNodeId());
				if (rmContainer.GetState() == RMContainerState.Reserved)
				{
					application.Unreserve(rmContainer.GetReservedPriority(), node);
				}
				else
				{
					application.ContainerCompleted(rmContainer, containerStatus, @event);
					node.ReleaseContainer(container);
					UpdateRootQueueMetrics();
				}
				Log.Info("Application attempt " + application.GetApplicationAttemptId() + " released container "
					 + container.GetId() + " on node: " + node + " with event: " + @event);
			}
		}

		private void AddNode(RMNode node)
		{
			lock (this)
			{
				FSSchedulerNode schedulerNode = new FSSchedulerNode(node, usePortForNodeName);
				nodes[node.GetNodeID()] = schedulerNode;
				Resources.AddTo(clusterResource, node.GetTotalCapability());
				UpdateRootQueueMetrics();
				UpdateMaximumAllocation(schedulerNode, true);
				queueMgr.GetRootQueue().SetSteadyFairShare(clusterResource);
				queueMgr.GetRootQueue().RecomputeSteadyShares();
				Log.Info("Added node " + node.GetNodeAddress() + " cluster capacity: " + clusterResource
					);
			}
		}

		private void RemoveNode(RMNode rmNode)
		{
			lock (this)
			{
				FSSchedulerNode node = GetFSSchedulerNode(rmNode.GetNodeID());
				// This can occur when an UNHEALTHY node reconnects
				if (node == null)
				{
					return;
				}
				Resources.SubtractFrom(clusterResource, rmNode.GetTotalCapability());
				UpdateRootQueueMetrics();
				// Remove running containers
				IList<RMContainer> runningContainers = node.GetRunningContainers();
				foreach (RMContainer container in runningContainers)
				{
					CompletedContainer(container, SchedulerUtils.CreateAbnormalContainerStatus(container
						.GetContainerId(), SchedulerUtils.LostContainer), RMContainerEventType.Kill);
				}
				// Remove reservations, if any
				RMContainer reservedContainer = node.GetReservedContainer();
				if (reservedContainer != null)
				{
					CompletedContainer(reservedContainer, SchedulerUtils.CreateAbnormalContainerStatus
						(reservedContainer.GetContainerId(), SchedulerUtils.LostContainer), RMContainerEventType
						.Kill);
				}
				Sharpen.Collections.Remove(nodes, rmNode.GetNodeID());
				queueMgr.GetRootQueue().SetSteadyFairShare(clusterResource);
				queueMgr.GetRootQueue().RecomputeSteadyShares();
				UpdateMaximumAllocation(node, false);
				Log.Info("Removed node " + rmNode.GetNodeAddress() + " cluster capacity: " + clusterResource
					);
			}
		}

		public override Allocation Allocate(ApplicationAttemptId appAttemptId, IList<ResourceRequest
			> ask, IList<ContainerId> release, IList<string> blacklistAdditions, IList<string
			> blacklistRemovals)
		{
			// Make sure this application exists
			FSAppAttempt application = GetSchedulerApp(appAttemptId);
			if (application == null)
			{
				Log.Info("Calling allocate on removed " + "or non existant application " + appAttemptId
					);
				return EmptyAllocation;
			}
			// Sanity check
			SchedulerUtils.NormalizeRequests(ask, DominantResourceCalculator, clusterResource
				, minimumAllocation, GetMaximumResourceCapability(), incrAllocation);
			// Set amResource for this app
			if (!application.GetUnmanagedAM() && ask.Count == 1 && application.GetLiveContainers
				().IsEmpty())
			{
				application.SetAMResource(ask[0].GetCapability());
			}
			// Release containers
			ReleaseContainers(release, application);
			lock (application)
			{
				if (!ask.IsEmpty())
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("allocate: pre-update" + " applicationAttemptId=" + appAttemptId + " application="
							 + application.GetApplicationId());
					}
					application.ShowRequests();
					// Update application requests
					application.UpdateResourceRequests(ask);
					application.ShowRequests();
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("allocate: post-update" + " applicationAttemptId=" + appAttemptId + " #ask="
						 + ask.Count + " reservation= " + application.GetCurrentReservation());
					Log.Debug("Preempting " + application.GetPreemptionContainers().Count + " container(s)"
						);
				}
				ICollection<ContainerId> preemptionContainerIds = new HashSet<ContainerId>();
				foreach (RMContainer container in application.GetPreemptionContainers())
				{
					preemptionContainerIds.AddItem(container.GetContainerId());
				}
				application.UpdateBlacklist(blacklistAdditions, blacklistRemovals);
				SchedulerApplicationAttempt.ContainersAndNMTokensAllocation allocation = application
					.PullNewlyAllocatedContainersAndNMTokens();
				Org.Apache.Hadoop.Yarn.Api.Records.Resource headroom = application.GetHeadroom();
				application.SetApplicationHeadroomForMetrics(headroom);
				return new Allocation(allocation.GetContainerList(), headroom, preemptionContainerIds
					, null, null, allocation.GetNMTokenList());
			}
		}

		/// <summary>Process a heartbeat update from a node.</summary>
		private void NodeUpdate(RMNode nm)
		{
			lock (this)
			{
				long start = GetClock().GetTime();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("nodeUpdate: " + nm + " cluster capacity: " + clusterResource);
				}
				eventLog.Log("HEARTBEAT", nm.GetHostName());
				FSSchedulerNode node = GetFSSchedulerNode(nm.GetNodeID());
				IList<UpdatedContainerInfo> containerInfoList = nm.PullContainerUpdates();
				IList<ContainerStatus> newlyLaunchedContainers = new AList<ContainerStatus>();
				IList<ContainerStatus> completedContainers = new AList<ContainerStatus>();
				foreach (UpdatedContainerInfo containerInfo in containerInfoList)
				{
					Sharpen.Collections.AddAll(newlyLaunchedContainers, containerInfo.GetNewlyLaunchedContainers
						());
					Sharpen.Collections.AddAll(completedContainers, containerInfo.GetCompletedContainers
						());
				}
				// Processing the newly launched containers
				foreach (ContainerStatus launchedContainer in newlyLaunchedContainers)
				{
					ContainerLaunchedOnNode(launchedContainer.GetContainerId(), node);
				}
				// Process completed containers
				foreach (ContainerStatus completedContainer in completedContainers)
				{
					ContainerId containerId = completedContainer.GetContainerId();
					Log.Debug("Container FINISHED: " + containerId);
					CompletedContainer(GetRMContainer(containerId), completedContainer, RMContainerEventType
						.Finished);
				}
				if (continuousSchedulingEnabled)
				{
					if (!completedContainers.IsEmpty())
					{
						AttemptScheduling(node);
					}
				}
				else
				{
					AttemptScheduling(node);
				}
				long duration = GetClock().GetTime() - start;
				fsOpDurations.AddNodeUpdateDuration(duration);
			}
		}

		/// <exception cref="System.Exception"/>
		internal virtual void ContinuousSchedulingAttempt()
		{
			long start = GetClock().GetTime();
			IList<NodeId> nodeIdList = new AList<NodeId>(nodes.Keys);
			// Sort the nodes by space available on them, so that we offer
			// containers on emptier nodes first, facilitating an even spread. This
			// requires holding the scheduler lock, so that the space available on a
			// node doesn't change during the sort.
			lock (this)
			{
				nodeIdList.Sort(nodeAvailableResourceComparator);
			}
			// iterate all nodes
			foreach (NodeId nodeId in nodeIdList)
			{
				FSSchedulerNode node = GetFSSchedulerNode(nodeId);
				try
				{
					if (node != null && Resources.FitsIn(minimumAllocation, node.GetAvailableResource
						()))
					{
						AttemptScheduling(node);
					}
				}
				catch (Exception ex)
				{
					Log.Error("Error while attempting scheduling for node " + node + ": " + ex.ToString
						(), ex);
					if ((ex is YarnRuntimeException) && (ex.InnerException is Exception))
					{
						// AsyncDispatcher translates InterruptedException to
						// YarnRuntimeException with cause InterruptedException.
						// Need to throw InterruptedException to stop schedulingThread.
						throw (Exception)ex.InnerException;
					}
				}
			}
			long duration = GetClock().GetTime() - start;
			fsOpDurations.AddContinuousSchedulingRunDuration(duration);
		}

		/// <summary>Sort nodes by available resource</summary>
		private class NodeAvailableResourceComparator : IComparer<NodeId>
		{
			public virtual int Compare(NodeId n1, NodeId n2)
			{
				if (!this._enclosing.nodes.Contains(n1))
				{
					return 1;
				}
				if (!this._enclosing.nodes.Contains(n2))
				{
					return -1;
				}
				return FairScheduler.ResourceCalculator.Compare(this._enclosing.clusterResource, 
					this._enclosing.nodes[n2].GetAvailableResource(), this._enclosing.nodes[n1].GetAvailableResource
					());
			}

			internal NodeAvailableResourceComparator(FairScheduler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FairScheduler _enclosing;
		}

		[VisibleForTesting]
		internal virtual void AttemptScheduling(FSSchedulerNode node)
		{
			lock (this)
			{
				if (rmContext.IsWorkPreservingRecoveryEnabled() && !rmContext.IsSchedulerReadyForAllocatingContainers
					())
				{
					return;
				}
				NodeId nodeID = node.GetNodeID();
				if (!nodes.Contains(nodeID))
				{
					// The node might have just been removed while this thread was waiting
					// on the synchronized lock before it entered this synchronized method
					Log.Info("Skipping scheduling as the node " + nodeID + " has been removed");
					return;
				}
				// Assign new containers...
				// 1. Check for reserved applications
				// 2. Schedule if there are no reservations
				FSAppAttempt reservedAppSchedulable = node.GetReservedAppSchedulable();
				if (reservedAppSchedulable != null)
				{
					Priority reservedPriority = node.GetReservedContainer().GetReservedPriority();
					FSQueue queue = ((FSLeafQueue)reservedAppSchedulable.GetQueue());
					if (!reservedAppSchedulable.HasContainerForNode(reservedPriority, node) || !FitsInMaxShare
						(queue, node.GetReservedContainer().GetReservedResource()))
					{
						// Don't hold the reservation if app can no longer use it
						Log.Info("Releasing reservation that cannot be satisfied for application " + reservedAppSchedulable
							.GetApplicationAttemptId() + " on node " + node);
						reservedAppSchedulable.Unreserve(reservedPriority, node);
						reservedAppSchedulable = null;
					}
					else
					{
						// Reservation exists; try to fulfill the reservation
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Trying to fulfill reservation for application " + reservedAppSchedulable
								.GetApplicationAttemptId() + " on node: " + node);
						}
						node.GetReservedAppSchedulable().AssignReservedContainer(node);
					}
				}
				if (reservedAppSchedulable == null)
				{
					// No reservation, schedule at queue which is farthest below fair share
					int assignedContainers = 0;
					while (node.GetReservedContainer() == null)
					{
						bool assignedContainer = false;
						if (!queueMgr.GetRootQueue().AssignContainer(node).Equals(Resources.None()))
						{
							assignedContainers++;
							assignedContainer = true;
						}
						if (!assignedContainer)
						{
							break;
						}
						if (!assignMultiple)
						{
							break;
						}
						if ((assignedContainers >= maxAssign) && (maxAssign > 0))
						{
							break;
						}
					}
				}
				UpdateRootQueueMetrics();
			}
		}

		internal static bool FitsInMaxShare(FSQueue queue, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 additionalResource)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource usagePlusAddition = Resources.Add(queue
				.GetResourceUsage(), additionalResource);
			if (!Resources.FitsIn(usagePlusAddition, queue.GetMaxShare()))
			{
				return false;
			}
			FSQueue parentQueue = queue.GetParent();
			if (parentQueue != null)
			{
				return FitsInMaxShare(parentQueue, additionalResource);
			}
			return true;
		}

		public virtual FSAppAttempt GetSchedulerApp(ApplicationAttemptId appAttemptId)
		{
			return base.GetApplicationAttempt(appAttemptId);
		}

		public override ResourceCalculator GetResourceCalculator()
		{
			return ResourceCalculator;
		}

		/// <summary>
		/// Subqueue metrics might be a little out of date because fair shares are
		/// recalculated at the update interval, but the root queue metrics needs to
		/// be updated synchronously with allocations and completions so that cluster
		/// metrics will be consistent.
		/// </summary>
		private void UpdateRootQueueMetrics()
		{
			rootMetrics.SetAvailableResourcesToQueue(Resources.Subtract(clusterResource, rootMetrics
				.GetAllocatedResources()));
		}

		/// <summary>
		/// Check if preemption is enabled and the utilization threshold for
		/// preemption is met.
		/// </summary>
		/// <returns>true if preemption should be attempted, false otherwise.</returns>
		private bool ShouldAttemptPreemption()
		{
			if (preemptionEnabled)
			{
				return (preemptionUtilizationThreshold < Math.Max((float)rootMetrics.GetAllocatedMB
					() / clusterResource.GetMemory(), (float)rootMetrics.GetAllocatedVirtualCores() 
					/ clusterResource.GetVirtualCores()));
			}
			return false;
		}

		public override QueueMetrics GetRootQueueMetrics()
		{
			return rootMetrics;
		}

		public override void Handle(SchedulerEvent @event)
		{
			switch (@event.GetType())
			{
				case SchedulerEventType.NodeAdded:
				{
					if (!(@event is NodeAddedSchedulerEvent))
					{
						throw new RuntimeException("Unexpected event type: " + @event);
					}
					NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)@event;
					AddNode(nodeAddedEvent.GetAddedRMNode());
					RecoverContainersOnNode(nodeAddedEvent.GetContainerReports(), nodeAddedEvent.GetAddedRMNode
						());
					break;
				}

				case SchedulerEventType.NodeRemoved:
				{
					if (!(@event is NodeRemovedSchedulerEvent))
					{
						throw new RuntimeException("Unexpected event type: " + @event);
					}
					NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)@event;
					RemoveNode(nodeRemovedEvent.GetRemovedRMNode());
					break;
				}

				case SchedulerEventType.NodeUpdate:
				{
					if (!(@event is NodeUpdateSchedulerEvent))
					{
						throw new RuntimeException("Unexpected event type: " + @event);
					}
					NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)@event;
					NodeUpdate(nodeUpdatedEvent.GetRMNode());
					break;
				}

				case SchedulerEventType.AppAdded:
				{
					if (!(@event is AppAddedSchedulerEvent))
					{
						throw new RuntimeException("Unexpected event type: " + @event);
					}
					AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent)@event;
					string queueName = ResolveReservationQueueName(appAddedEvent.GetQueue(), appAddedEvent
						.GetApplicationId(), appAddedEvent.GetReservationID());
					if (queueName != null)
					{
						AddApplication(appAddedEvent.GetApplicationId(), queueName, appAddedEvent.GetUser
							(), appAddedEvent.GetIsAppRecovering());
					}
					break;
				}

				case SchedulerEventType.AppRemoved:
				{
					if (!(@event is AppRemovedSchedulerEvent))
					{
						throw new RuntimeException("Unexpected event type: " + @event);
					}
					AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)@event;
					RemoveApplication(appRemovedEvent.GetApplicationID(), appRemovedEvent.GetFinalState
						());
					break;
				}

				case SchedulerEventType.NodeResourceUpdate:
				{
					if (!(@event is NodeResourceUpdateSchedulerEvent))
					{
						throw new RuntimeException("Unexpected event type: " + @event);
					}
					NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent = (NodeResourceUpdateSchedulerEvent
						)@event;
					UpdateNodeResource(nodeResourceUpdatedEvent.GetRMNode(), nodeResourceUpdatedEvent
						.GetResourceOption());
					break;
				}

				case SchedulerEventType.AppAttemptAdded:
				{
					if (!(@event is AppAttemptAddedSchedulerEvent))
					{
						throw new RuntimeException("Unexpected event type: " + @event);
					}
					AppAttemptAddedSchedulerEvent appAttemptAddedEvent = (AppAttemptAddedSchedulerEvent
						)@event;
					AddApplicationAttempt(appAttemptAddedEvent.GetApplicationAttemptId(), appAttemptAddedEvent
						.GetTransferStateFromPreviousAttempt(), appAttemptAddedEvent.GetIsAttemptRecovering
						());
					break;
				}

				case SchedulerEventType.AppAttemptRemoved:
				{
					if (!(@event is AppAttemptRemovedSchedulerEvent))
					{
						throw new RuntimeException("Unexpected event type: " + @event);
					}
					AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent = (AppAttemptRemovedSchedulerEvent
						)@event;
					RemoveApplicationAttempt(appAttemptRemovedEvent.GetApplicationAttemptID(), appAttemptRemovedEvent
						.GetFinalAttemptState(), appAttemptRemovedEvent.GetKeepContainersAcrossAppAttempts
						());
					break;
				}

				case SchedulerEventType.ContainerExpired:
				{
					if (!(@event is ContainerExpiredSchedulerEvent))
					{
						throw new RuntimeException("Unexpected event type: " + @event);
					}
					ContainerExpiredSchedulerEvent containerExpiredEvent = (ContainerExpiredSchedulerEvent
						)@event;
					ContainerId containerId = containerExpiredEvent.GetContainerId();
					CompletedContainer(GetRMContainer(containerId), SchedulerUtils.CreateAbnormalContainerStatus
						(containerId, SchedulerUtils.ExpiredContainer), RMContainerEventType.Expire);
					break;
				}

				case SchedulerEventType.ContainerRescheduled:
				{
					if (!(@event is ContainerRescheduledEvent))
					{
						throw new RuntimeException("Unexpected event type: " + @event);
					}
					ContainerRescheduledEvent containerRescheduledEvent = (ContainerRescheduledEvent)
						@event;
					RMContainer container = containerRescheduledEvent.GetContainer();
					RecoverResourceRequestForContainer(container);
					break;
				}

				default:
				{
					Log.Error("Unknown event arrived at FairScheduler: " + @event.ToString());
					break;
				}
			}
		}

		private string ResolveReservationQueueName(string queueName, ApplicationId applicationId
			, ReservationId reservationID)
		{
			lock (this)
			{
				FSQueue queue = queueMgr.GetQueue(queueName);
				if ((queue == null) || !allocConf.IsReservable(queue.GetQueueName()))
				{
					return queueName;
				}
				// Use fully specified name from now on (including root. prefix)
				queueName = queue.GetQueueName();
				if (reservationID != null)
				{
					string resQName = queueName + "." + reservationID.ToString();
					queue = queueMgr.GetQueue(resQName);
					if (queue == null)
					{
						string message = "Application " + applicationId + " submitted to a reservation which is not yet currently active: "
							 + resQName;
						this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
							, RMAppEventType.AppRejected, message));
						return null;
					}
					if (!queue.GetParent().GetQueueName().Equals(queueName))
					{
						string message = "Application: " + applicationId + " submitted to a reservation "
							 + resQName + " which does not belong to the specified queue: " + queueName;
						this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
							, RMAppEventType.AppRejected, message));
						return null;
					}
					// use the reservation queue to run the app
					queueName = resQName;
				}
				else
				{
					// use the default child queue of the plan for unreserved apps
					queueName = GetDefaultQueueForPlanQueue(queueName);
				}
				return queueName;
			}
		}

		private string GetDefaultQueueForPlanQueue(string queueName)
		{
			string planName = Sharpen.Runtime.Substring(queueName, queueName.LastIndexOf(".")
				 + 1);
			queueName = queueName + "." + planName + ReservationConstants.DefaultQueueSuffix;
			return queueName;
		}

		/// <exception cref="System.Exception"/>
		public override void Recover(RMStateStore.RMState state)
		{
		}

		// NOT IMPLEMENTED
		public override void SetRMContext(RMContext rmContext)
		{
			lock (this)
			{
				this.rmContext = rmContext;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitScheduler(Configuration conf)
		{
			lock (this)
			{
				this.conf = new FairSchedulerConfiguration(conf);
				ValidateConf(this.conf);
				minimumAllocation = this.conf.GetMinimumAllocation();
				InitMaximumResourceCapability(this.conf.GetMaximumAllocation());
				incrAllocation = this.conf.GetIncrementAllocation();
				continuousSchedulingEnabled = this.conf.IsContinuousSchedulingEnabled();
				continuousSchedulingSleepMs = this.conf.GetContinuousSchedulingSleepMs();
				nodeLocalityThreshold = this.conf.GetLocalityThresholdNode();
				rackLocalityThreshold = this.conf.GetLocalityThresholdRack();
				nodeLocalityDelayMs = this.conf.GetLocalityDelayNodeMs();
				rackLocalityDelayMs = this.conf.GetLocalityDelayRackMs();
				preemptionEnabled = this.conf.GetPreemptionEnabled();
				preemptionUtilizationThreshold = this.conf.GetPreemptionUtilizationThreshold();
				assignMultiple = this.conf.GetAssignMultiple();
				maxAssign = this.conf.GetMaxAssign();
				sizeBasedWeight = this.conf.GetSizeBasedWeight();
				preemptionInterval = this.conf.GetPreemptionInterval();
				waitTimeBeforeKill = this.conf.GetWaitTimeBeforeKill();
				usePortForNodeName = this.conf.GetUsePortForNodeName();
				updateInterval = this.conf.GetUpdateInterval();
				if (updateInterval < 0)
				{
					updateInterval = FairSchedulerConfiguration.DefaultUpdateIntervalMs;
					Log.Warn(FairSchedulerConfiguration.UpdateIntervalMs + " is invalid, so using default value "
						 + +FairSchedulerConfiguration.DefaultUpdateIntervalMs + " ms instead");
				}
				rootMetrics = ((FSQueueMetrics)FSQueueMetrics.ForQueue("root", null, true, conf));
				fsOpDurations = FSOpDurations.GetInstance(true);
				// This stores per-application scheduling information
				this.applications = new ConcurrentHashMap<ApplicationId, SchedulerApplication<FSAppAttempt
					>>();
				this.eventLog = new FairSchedulerEventLog();
				eventLog.Init(this.conf);
				allocConf = new AllocationConfiguration(conf);
				try
				{
					queueMgr.Initialize(conf);
				}
				catch (Exception e)
				{
					throw new IOException("Failed to start FairScheduler", e);
				}
				updateThread = new FairScheduler.UpdateThread(this);
				updateThread.SetName("FairSchedulerUpdateThread");
				updateThread.SetDaemon(true);
				if (continuousSchedulingEnabled)
				{
					// start continuous scheduling thread
					schedulingThread = new FairScheduler.ContinuousSchedulingThread(this);
					schedulingThread.SetName("FairSchedulerContinuousScheduling");
					schedulingThread.SetDaemon(true);
				}
			}
			allocsLoader.Init(conf);
			allocsLoader.SetReloadListener(new FairScheduler.AllocationReloadListener(this));
			// If we fail to load allocations file on initialize, we want to fail
			// immediately.  After a successful load, exceptions on future reloads
			// will just result in leaving things as they are.
			try
			{
				allocsLoader.ReloadAllocations();
			}
			catch (Exception e)
			{
				throw new IOException("Failed to initialize FairScheduler", e);
			}
		}

		private void StartSchedulerThreads()
		{
			lock (this)
			{
				Preconditions.CheckNotNull(updateThread, "updateThread is null");
				Preconditions.CheckNotNull(allocsLoader, "allocsLoader is null");
				updateThread.Start();
				if (continuousSchedulingEnabled)
				{
					Preconditions.CheckNotNull(schedulingThread, "schedulingThread is null");
					schedulingThread.Start();
				}
				allocsLoader.Start();
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			InitScheduler(conf);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			StartSchedulerThreads();
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			lock (this)
			{
				if (updateThread != null)
				{
					updateThread.Interrupt();
					updateThread.Join(ThreadJoinTimeoutMs);
				}
				if (continuousSchedulingEnabled)
				{
					if (schedulingThread != null)
					{
						schedulingThread.Interrupt();
						schedulingThread.Join(ThreadJoinTimeoutMs);
					}
				}
				if (allocsLoader != null)
				{
					allocsLoader.Stop();
				}
			}
			base.ServiceStop();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Reinitialize(Configuration conf, RMContext rmContext)
		{
			try
			{
				allocsLoader.ReloadAllocations();
			}
			catch (Exception e)
			{
				Log.Error("Failed to reload allocations file", e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override QueueInfo GetQueueInfo(string queueName, bool includeChildQueues, 
			bool recursive)
		{
			if (!queueMgr.Exists(queueName))
			{
				throw new IOException("queue " + queueName + " does not exist");
			}
			return queueMgr.GetQueue(queueName).GetQueueInfo(includeChildQueues, recursive);
		}

		public override IList<QueueUserACLInfo> GetQueueUserAclInfo()
		{
			UserGroupInformation user;
			try
			{
				user = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException)
			{
				return new AList<QueueUserACLInfo>();
			}
			return queueMgr.GetRootQueue().GetQueueUserAclInfo(user);
		}

		public override int GetNumClusterNodes()
		{
			return nodes.Count;
		}

		public override bool CheckAccess(UserGroupInformation callerUGI, QueueACL acl, string
			 queueName)
		{
			lock (this)
			{
				FSQueue queue = GetQueueManager().GetQueue(queueName);
				if (queue == null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("ACL not found for queue access-type " + acl + " for queue " + queueName
							);
					}
					return false;
				}
				return queue.HasAccess(acl, callerUGI);
			}
		}

		public virtual AllocationConfiguration GetAllocationConfiguration()
		{
			return allocConf;
		}

		private class AllocationReloadListener : AllocationFileLoaderService.Listener
		{
			public virtual void OnReload(AllocationConfiguration queueInfo)
			{
				// Commit the reload; also create any queue defined in the alloc file
				// if it does not already exist, so it can be displayed on the web UI.
				lock (this._enclosing)
				{
					this._enclosing.allocConf = queueInfo;
					this._enclosing.allocConf.GetDefaultSchedulingPolicy().Initialize(this._enclosing
						.clusterResource);
					this._enclosing.queueMgr.UpdateAllocationConfiguration(this._enclosing.allocConf);
					this._enclosing.maxRunningEnforcer.UpdateRunnabilityOnReload();
				}
			}

			internal AllocationReloadListener(FairScheduler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FairScheduler _enclosing;
		}

		public override IList<ApplicationAttemptId> GetAppsInQueue(string queueName)
		{
			FSQueue queue = queueMgr.GetQueue(queueName);
			if (queue == null)
			{
				return null;
			}
			IList<ApplicationAttemptId> apps = new AList<ApplicationAttemptId>();
			queue.CollectSchedulerApplications(apps);
			return apps;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override string MoveApplication(ApplicationId appId, string queueName)
		{
			lock (this)
			{
				SchedulerApplication<FSAppAttempt> app = applications[appId];
				if (app == null)
				{
					throw new YarnException("App to be moved " + appId + " not found.");
				}
				FSAppAttempt attempt = (FSAppAttempt)app.GetCurrentAppAttempt();
				// To serialize with FairScheduler#allocate, synchronize on app attempt
				lock (attempt)
				{
					FSLeafQueue oldQueue = (FSLeafQueue)app.GetQueue();
					string destQueueName = HandleMoveToPlanQueue(queueName);
					FSLeafQueue targetQueue = queueMgr.GetLeafQueue(destQueueName, false);
					if (targetQueue == null)
					{
						throw new YarnException("Target queue " + queueName + " not found or is not a leaf queue."
							);
					}
					if (targetQueue == oldQueue)
					{
						return oldQueue.GetQueueName();
					}
					if (oldQueue.IsRunnableApp(attempt))
					{
						VerifyMoveDoesNotViolateConstraints(attempt, oldQueue, targetQueue);
					}
					ExecuteMove(app, attempt, oldQueue, targetQueue);
					return targetQueue.GetQueueName();
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void VerifyMoveDoesNotViolateConstraints(FSAppAttempt app, FSLeafQueue oldQueue
			, FSLeafQueue targetQueue)
		{
			string queueName = targetQueue.GetQueueName();
			ApplicationAttemptId appAttId = app.GetApplicationAttemptId();
			// When checking maxResources and maxRunningApps, only need to consider
			// queues before the lowest common ancestor of the two queues because the
			// total running apps in queues above will not be changed.
			FSQueue lowestCommonAncestor = FindLowestCommonAncestorQueue(oldQueue, targetQueue
				);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource consumption = app.GetCurrentConsumption
				();
			// Check whether the move would go over maxRunningApps or maxShare
			FSQueue cur = targetQueue;
			while (cur != lowestCommonAncestor)
			{
				// maxRunningApps
				if (cur.GetNumRunnableApps() == allocConf.GetQueueMaxApps(cur.GetQueueName()))
				{
					throw new YarnException("Moving app attempt " + appAttId + " to queue " + queueName
						 + " would violate queue maxRunningApps constraints on" + " queue " + cur.GetQueueName
						());
				}
				// maxShare
				if (!Resources.FitsIn(Resources.Add(cur.GetResourceUsage(), consumption), cur.GetMaxShare
					()))
				{
					throw new YarnException("Moving app attempt " + appAttId + " to queue " + queueName
						 + " would violate queue maxShare constraints on" + " queue " + cur.GetQueueName
						());
				}
				cur = cur.GetParent();
			}
		}

		/// <summary>
		/// Helper for moveApplication, which has appropriate synchronization, so all
		/// operations will be atomic.
		/// </summary>
		private void ExecuteMove(SchedulerApplication<FSAppAttempt> app, FSAppAttempt attempt
			, FSLeafQueue oldQueue, FSLeafQueue newQueue)
		{
			bool wasRunnable = oldQueue.RemoveApp(attempt);
			// if app was not runnable before, it may be runnable now
			bool nowRunnable = maxRunningEnforcer.CanAppBeRunnable(newQueue, attempt.GetUser(
				));
			if (wasRunnable && !nowRunnable)
			{
				throw new InvalidOperationException("Should have already verified that app " + attempt
					.GetApplicationId() + " would be runnable in new queue");
			}
			if (wasRunnable)
			{
				maxRunningEnforcer.UntrackRunnableApp(attempt);
			}
			else
			{
				if (nowRunnable)
				{
					// App has changed from non-runnable to runnable
					maxRunningEnforcer.UntrackNonRunnableApp(attempt);
				}
			}
			attempt.Move(newQueue);
			// This updates all the metrics
			app.SetQueue(newQueue);
			newQueue.AddApp(attempt, nowRunnable);
			if (nowRunnable)
			{
				maxRunningEnforcer.TrackRunnableApp(attempt);
			}
			if (wasRunnable)
			{
				maxRunningEnforcer.UpdateRunnabilityOnAppRemoval(attempt, oldQueue);
			}
		}

		[VisibleForTesting]
		internal virtual FSQueue FindLowestCommonAncestorQueue(FSQueue queue1, FSQueue queue2
			)
		{
			// Because queue names include ancestors, separated by periods, we can find
			// the lowest common ancestors by going from the start of the names until
			// there's a character that doesn't match.
			string name1 = queue1.GetName();
			string name2 = queue2.GetName();
			// We keep track of the last period we encounter to avoid returning root.apple
			// when the queues are root.applepie and root.appletart
			int lastPeriodIndex = -1;
			for (int i = 0; i < Math.Max(name1.Length, name2.Length); i++)
			{
				if (name1.Length <= i || name2.Length <= i || name1[i] != name2[i])
				{
					return queueMgr.GetQueue(Sharpen.Runtime.Substring(name1, 0, lastPeriodIndex));
				}
				else
				{
					if (name1[i] == '.')
					{
						lastPeriodIndex = i;
					}
				}
			}
			return queue1;
		}

		// names are identical
		/// <summary>Process resource update on a node and update Queue.</summary>
		public override void UpdateNodeResource(RMNode nm, ResourceOption resourceOption)
		{
			lock (this)
			{
				base.UpdateNodeResource(nm, resourceOption);
				UpdateRootQueueMetrics();
				queueMgr.GetRootQueue().SetSteadyFairShare(clusterResource);
				queueMgr.GetRootQueue().RecomputeSteadyShares();
			}
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override EnumSet<YarnServiceProtos.SchedulerResourceTypes> GetSchedulingResourceTypes
			()
		{
			return EnumSet.Of(YarnServiceProtos.SchedulerResourceTypes.Memory, YarnServiceProtos.SchedulerResourceTypes
				.Cpu);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override ICollection<string> GetPlanQueues()
		{
			ICollection<string> planQueues = new HashSet<string>();
			foreach (FSQueue fsQueue in queueMgr.GetQueues())
			{
				string queueName = fsQueue.GetName();
				if (allocConf.IsReservable(queueName))
				{
					planQueues.AddItem(queueName);
				}
			}
			return planQueues;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void SetEntitlement(string queueName, QueueEntitlement entitlement
			)
		{
			FSLeafQueue reservationQueue = queueMgr.GetLeafQueue(queueName, false);
			if (reservationQueue == null)
			{
				throw new YarnException("Target queue " + queueName + " not found or is not a leaf queue."
					);
			}
			reservationQueue.SetWeights(entitlement.GetCapacity());
		}

		// TODO Does MaxCapacity need to be set for fairScheduler ?
		/// <summary>Only supports removing empty leaf queues</summary>
		/// <param name="queueName">name of queue to remove</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException">
		/// if queue to remove is either not a leaf or if its
		/// not empty
		/// </exception>
		public override void RemoveQueue(string queueName)
		{
			FSLeafQueue reservationQueue = queueMgr.GetLeafQueue(queueName, false);
			if (reservationQueue != null)
			{
				if (!queueMgr.RemoveLeafQueue(queueName))
				{
					throw new YarnException("Could not remove queue " + queueName + " as " + "its either not a leaf queue or its not empty"
						);
				}
			}
		}

		private string HandleMoveToPlanQueue(string targetQueueName)
		{
			FSQueue dest = queueMgr.GetQueue(targetQueueName);
			if (dest != null && allocConf.IsReservable(dest.GetQueueName()))
			{
				// use the default child reservation queue of the plan
				targetQueueName = GetDefaultQueueForPlanQueue(targetQueueName);
			}
			return targetQueueName;
		}
	}
}
