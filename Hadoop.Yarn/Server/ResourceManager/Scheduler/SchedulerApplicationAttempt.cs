using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Lang.Time;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>Represents an application attempt from the viewpoint of the scheduler.</summary>
	/// <remarks>
	/// Represents an application attempt from the viewpoint of the scheduler.
	/// Each running app attempt in the RM corresponds to one instance
	/// of this class.
	/// </remarks>
	public class SchedulerApplicationAttempt
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.SchedulerApplicationAttempt
			));

		private const long MemAggregateAllocationCacheMsecs = 3000;

		protected internal long lastMemoryAggregateAllocationUpdateTime = 0;

		private long lastMemorySeconds = 0;

		private long lastVcoreSeconds = 0;

		protected internal readonly AppSchedulingInfo appSchedulingInfo;

		protected internal ApplicationAttemptId attemptId;

		protected internal IDictionary<ContainerId, RMContainer> liveContainers = new Dictionary
			<ContainerId, RMContainer>();

		protected internal readonly IDictionary<Priority, IDictionary<NodeId, RMContainer
			>> reservedContainers = new Dictionary<Priority, IDictionary<NodeId, RMContainer
			>>();

		private readonly Multiset<Priority> reReservations = HashMultiset.Create();

		protected internal readonly Resource currentReservation = Resource.NewInstance(0, 
			0);

		private Resource resourceLimit = Resource.NewInstance(0, 0);

		protected internal Resource currentConsumption = Resource.NewInstance(0, 0);

		private Resource amResource = Resources.None();

		private bool unmanagedAM = true;

		private bool amRunning = false;

		private LogAggregationContext logAggregationContext;

		protected internal IList<RMContainer> newlyAllocatedContainers = new AList<RMContainer
			>();

		private ICollection<ContainerId> pendingRelease = null;

		/// <summary>
		/// Count how many times the application has been given an opportunity
		/// to schedule a task at each priority.
		/// </summary>
		/// <remarks>
		/// Count how many times the application has been given an opportunity
		/// to schedule a task at each priority. Each time the scheduler
		/// asks the application for a task at this priority, it is incremented,
		/// and each time the application successfully schedules a task, it
		/// is reset to 0.
		/// </remarks>
		internal Multiset<Priority> schedulingOpportunities = HashMultiset.Create();

		protected internal IDictionary<Priority, long> lastScheduledContainer = new Dictionary
			<Priority, long>();

		protected internal Queue queue;

		protected internal bool isStopped = false;

		protected internal readonly RMContext rmContext;

		public SchedulerApplicationAttempt(ApplicationAttemptId applicationAttemptId, string
			 user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext)
		{
			// This pendingRelease is used in work-preserving recovery scenario to keep
			// track of the AM's outstanding release requests. RM on recovery could
			// receive the release request form AM before it receives the container status
			// from NM for recovery. In this case, the to-be-recovered containers reported
			// by NM should not be recovered.
			// Time of the last container scheduled at the current allowed level
			Preconditions.CheckNotNull(rmContext, "RMContext should not be null");
			this.rmContext = rmContext;
			this.appSchedulingInfo = new AppSchedulingInfo(applicationAttemptId, user, queue, 
				activeUsersManager, rmContext.GetEpoch());
			this.queue = queue;
			this.pendingRelease = new HashSet<ContainerId>();
			this.attemptId = applicationAttemptId;
			if (rmContext.GetRMApps() != null && rmContext.GetRMApps().Contains(applicationAttemptId
				.GetApplicationId()))
			{
				ApplicationSubmissionContext appSubmissionContext = rmContext.GetRMApps()[applicationAttemptId
					.GetApplicationId()].GetApplicationSubmissionContext();
				if (appSubmissionContext != null)
				{
					unmanagedAM = appSubmissionContext.GetUnmanagedAM();
					this.logAggregationContext = appSubmissionContext.GetLogAggregationContext();
				}
			}
		}

		/// <summary>Get the live containers of the application.</summary>
		/// <returns>live containers of the application</returns>
		public virtual ICollection<RMContainer> GetLiveContainers()
		{
			lock (this)
			{
				return new AList<RMContainer>(liveContainers.Values);
			}
		}

		public virtual AppSchedulingInfo GetAppSchedulingInfo()
		{
			return this.appSchedulingInfo;
		}

		/// <summary>Is this application pending?</summary>
		/// <returns>true if it is else false.</returns>
		public virtual bool IsPending()
		{
			return appSchedulingInfo.IsPending();
		}

		/// <summary>
		/// Get
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// of the application master.
		/// </summary>
		/// <returns><code>ApplicationAttemptId</code> of the application master</returns>
		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return appSchedulingInfo.GetApplicationAttemptId();
		}

		public virtual ApplicationId GetApplicationId()
		{
			return appSchedulingInfo.GetApplicationId();
		}

		public virtual string GetUser()
		{
			return appSchedulingInfo.GetUser();
		}

		public virtual IDictionary<string, ResourceRequest> GetResourceRequests(Priority 
			priority)
		{
			return appSchedulingInfo.GetResourceRequests(priority);
		}

		public virtual ICollection<ContainerId> GetPendingRelease()
		{
			return this.pendingRelease;
		}

		public virtual long GetNewContainerId()
		{
			return appSchedulingInfo.GetNewContainerId();
		}

		public virtual ICollection<Priority> GetPriorities()
		{
			return appSchedulingInfo.GetPriorities();
		}

		public virtual ResourceRequest GetResourceRequest(Priority priority, string resourceName
			)
		{
			lock (this)
			{
				return this.appSchedulingInfo.GetResourceRequest(priority, resourceName);
			}
		}

		public virtual int GetTotalRequiredResources(Priority priority)
		{
			lock (this)
			{
				return GetResourceRequest(priority, ResourceRequest.Any).GetNumContainers();
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResource(Priority priority
			)
		{
			lock (this)
			{
				return appSchedulingInfo.GetResource(priority);
			}
		}

		public virtual string GetQueueName()
		{
			return appSchedulingInfo.GetQueueName();
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetAMResource()
		{
			return amResource;
		}

		public virtual void SetAMResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource amResource
			)
		{
			this.amResource = amResource;
		}

		public virtual bool IsAmRunning()
		{
			return amRunning;
		}

		public virtual void SetAmRunning(bool @bool)
		{
			amRunning = @bool;
		}

		public virtual bool GetUnmanagedAM()
		{
			return unmanagedAM;
		}

		public virtual RMContainer GetRMContainer(ContainerId id)
		{
			lock (this)
			{
				return liveContainers[id];
			}
		}

		protected internal virtual void ResetReReservations(Priority priority)
		{
			lock (this)
			{
				reReservations.SetCount(priority, 0);
			}
		}

		protected internal virtual void AddReReservation(Priority priority)
		{
			lock (this)
			{
				reReservations.AddItem(priority);
			}
		}

		public virtual int GetReReservations(Priority priority)
		{
			lock (this)
			{
				return reReservations.Count(priority);
			}
		}

		/// <summary>Get total current reservations.</summary>
		/// <remarks>
		/// Get total current reservations.
		/// Used only by unit tests
		/// </remarks>
		/// <returns>total current reservations</returns>
		[InterfaceStability.Stable]
		[InterfaceAudience.Private]
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetCurrentReservation(
			)
		{
			lock (this)
			{
				return currentReservation;
			}
		}

		public virtual Queue GetQueue()
		{
			return queue;
		}

		public virtual void UpdateResourceRequests(IList<ResourceRequest> requests)
		{
			lock (this)
			{
				if (!isStopped)
				{
					appSchedulingInfo.UpdateResourceRequests(requests, false);
				}
			}
		}

		public virtual void RecoverResourceRequests(IList<ResourceRequest> requests)
		{
			lock (this)
			{
				if (!isStopped)
				{
					appSchedulingInfo.UpdateResourceRequests(requests, true);
				}
			}
		}

		public virtual void Stop(RMAppAttemptState rmAppAttemptFinalState)
		{
			lock (this)
			{
				// Cleanup all scheduling information
				isStopped = true;
				appSchedulingInfo.Stop(rmAppAttemptFinalState);
			}
		}

		public virtual bool IsStopped()
		{
			lock (this)
			{
				return isStopped;
			}
		}

		/// <summary>Get the list of reserved containers</summary>
		/// <returns>All of the reserved containers.</returns>
		public virtual IList<RMContainer> GetReservedContainers()
		{
			lock (this)
			{
				IList<RMContainer> reservedContainers = new AList<RMContainer>();
				foreach (KeyValuePair<Priority, IDictionary<NodeId, RMContainer>> e in this.reservedContainers)
				{
					Sharpen.Collections.AddAll(reservedContainers, e.Value.Values);
				}
				return reservedContainers;
			}
		}

		public virtual RMContainer Reserve(SchedulerNode node, Priority priority, RMContainer
			 rmContainer, Container container)
		{
			lock (this)
			{
				// Create RMContainer if necessary
				if (rmContainer == null)
				{
					rmContainer = new RMContainerImpl(container, GetApplicationAttemptId(), node.GetNodeID
						(), appSchedulingInfo.GetUser(), rmContext);
					Resources.AddTo(currentReservation, container.GetResource());
					// Reset the re-reservation count
					ResetReReservations(priority);
				}
				else
				{
					// Note down the re-reservation
					AddReReservation(priority);
				}
				rmContainer.Handle(new RMContainerReservedEvent(container.GetId(), container.GetResource
					(), node.GetNodeID(), priority));
				IDictionary<NodeId, RMContainer> reservedContainers = this.reservedContainers[priority
					];
				if (reservedContainers == null)
				{
					reservedContainers = new Dictionary<NodeId, RMContainer>();
					this.reservedContainers[priority] = reservedContainers;
				}
				reservedContainers[node.GetNodeID()] = rmContainer;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Application attempt " + GetApplicationAttemptId() + " reserved container "
						 + rmContainer + " on node " + node + ". This attempt currently has " + reservedContainers
						.Count + " reserved containers at priority " + priority + "; currentReservation "
						 + currentReservation.GetMemory());
				}
				return rmContainer;
			}
		}

		/// <summary>
		/// Has the application reserved the given <code>node</code> at the
		/// given <code>priority</code>?
		/// </summary>
		/// <param name="node">node to be checked</param>
		/// <param name="priority">priority of reserved container</param>
		/// <returns>true is reserved, false if not</returns>
		public virtual bool IsReserved(SchedulerNode node, Priority priority)
		{
			lock (this)
			{
				IDictionary<NodeId, RMContainer> reservedContainers = this.reservedContainers[priority
					];
				if (reservedContainers != null)
				{
					return reservedContainers.Contains(node.GetNodeID());
				}
				return false;
			}
		}

		public virtual void SetHeadroom(Org.Apache.Hadoop.Yarn.Api.Records.Resource globalLimit
			)
		{
			lock (this)
			{
				this.resourceLimit = globalLimit;
			}
		}

		/// <summary>Get available headroom in terms of resources for the application's user.
		/// 	</summary>
		/// <returns>available resource headroom</returns>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetHeadroom()
		{
			lock (this)
			{
				// Corner case to deal with applications being slightly over-limit
				if (resourceLimit.GetMemory() < 0)
				{
					resourceLimit.SetMemory(0);
				}
				return resourceLimit;
			}
		}

		public virtual int GetNumReservedContainers(Priority priority)
		{
			lock (this)
			{
				IDictionary<NodeId, RMContainer> reservedContainers = this.reservedContainers[priority
					];
				return (reservedContainers == null) ? 0 : reservedContainers.Count;
			}
		}

		public virtual void ContainerLaunchedOnNode(ContainerId containerId, NodeId nodeId
			)
		{
			lock (this)
			{
				// Inform the container
				RMContainer rmContainer = GetRMContainer(containerId);
				if (rmContainer == null)
				{
					// Some unknown container sneaked into the system. Kill it.
					rmContext.GetDispatcher().GetEventHandler().Handle(new RMNodeCleanContainerEvent(
						nodeId, containerId));
					return;
				}
				rmContainer.Handle(new RMContainerEvent(containerId, RMContainerEventType.Launched
					));
			}
		}

		public virtual void ShowRequests()
		{
			lock (this)
			{
				if (Log.IsDebugEnabled())
				{
					foreach (Priority priority in GetPriorities())
					{
						IDictionary<string, ResourceRequest> requests = GetResourceRequests(priority);
						if (requests != null)
						{
							Log.Debug("showRequests:" + " application=" + GetApplicationId() + " headRoom=" +
								 GetHeadroom() + " currentConsumption=" + currentConsumption.GetMemory());
							foreach (ResourceRequest request in requests.Values)
							{
								Log.Debug("showRequests:" + " application=" + GetApplicationId() + " request=" + 
									request);
							}
						}
					}
				}
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetCurrentConsumption(
			)
		{
			return currentConsumption;
		}

		public class ContainersAndNMTokensAllocation
		{
			internal IList<Container> containerList;

			internal IList<NMToken> nmTokenList;

			public ContainersAndNMTokensAllocation(IList<Container> containerList, IList<NMToken
				> nmTokenList)
			{
				this.containerList = containerList;
				this.nmTokenList = nmTokenList;
			}

			public virtual IList<Container> GetContainerList()
			{
				return containerList;
			}

			public virtual IList<NMToken> GetNMTokenList()
			{
				return nmTokenList;
			}
		}

		// Create container token and NMToken altogether, if either of them fails for
		// some reason like DNS unavailable, do not return this container and keep it
		// in the newlyAllocatedContainers waiting to be refetched.
		public virtual SchedulerApplicationAttempt.ContainersAndNMTokensAllocation PullNewlyAllocatedContainersAndNMTokens
			()
		{
			lock (this)
			{
				IList<Container> returnContainerList = new AList<Container>(newlyAllocatedContainers
					.Count);
				IList<NMToken> nmTokens = new AList<NMToken>();
				for (IEnumerator<RMContainer> i = newlyAllocatedContainers.GetEnumerator(); i.HasNext
					(); )
				{
					RMContainer rmContainer = i.Next();
					Container container = rmContainer.GetContainer();
					try
					{
						// create container token and NMToken altogether.
						container.SetContainerToken(rmContext.GetContainerTokenSecretManager().CreateContainerToken
							(container.GetId(), container.GetNodeId(), GetUser(), container.GetResource(), container
							.GetPriority(), rmContainer.GetCreationTime(), this.logAggregationContext));
						NMToken nmToken = rmContext.GetNMTokenSecretManager().CreateAndGetNMToken(GetUser
							(), GetApplicationAttemptId(), container);
						if (nmToken != null)
						{
							nmTokens.AddItem(nmToken);
						}
					}
					catch (ArgumentException e)
					{
						// DNS might be down, skip returning this container.
						Log.Error("Error trying to assign container token and NM token to" + " an allocated container "
							 + container.GetId(), e);
						continue;
					}
					returnContainerList.AddItem(container);
					i.Remove();
					rmContainer.Handle(new RMContainerEvent(rmContainer.GetContainerId(), RMContainerEventType
						.Acquired));
				}
				return new SchedulerApplicationAttempt.ContainersAndNMTokensAllocation(returnContainerList
					, nmTokens);
			}
		}

		public virtual void UpdateBlacklist(IList<string> blacklistAdditions, IList<string
			> blacklistRemovals)
		{
			lock (this)
			{
				if (!isStopped)
				{
					this.appSchedulingInfo.UpdateBlacklist(blacklistAdditions, blacklistRemovals);
				}
			}
		}

		public virtual bool IsBlacklisted(string resourceName)
		{
			return this.appSchedulingInfo.IsBlacklisted(resourceName);
		}

		public virtual void AddSchedulingOpportunity(Priority priority)
		{
			lock (this)
			{
				schedulingOpportunities.SetCount(priority, schedulingOpportunities.Count(priority
					) + 1);
			}
		}

		public virtual void SubtractSchedulingOpportunity(Priority priority)
		{
			lock (this)
			{
				int count = schedulingOpportunities.Count(priority) - 1;
				this.schedulingOpportunities.SetCount(priority, Math.Max(count, 0));
			}
		}

		/// <summary>
		/// Return the number of times the application has been given an opportunity
		/// to schedule a task at the given priority since the last time it
		/// successfully did so.
		/// </summary>
		public virtual int GetSchedulingOpportunities(Priority priority)
		{
			lock (this)
			{
				return schedulingOpportunities.Count(priority);
			}
		}

		/// <summary>
		/// Should be called when an application has successfully scheduled a container,
		/// or when the scheduling locality threshold is relaxed.
		/// </summary>
		/// <remarks>
		/// Should be called when an application has successfully scheduled a container,
		/// or when the scheduling locality threshold is relaxed.
		/// Reset various internal counters which affect delay scheduling
		/// </remarks>
		/// <param name="priority">The priority of the container scheduled.</param>
		public virtual void ResetSchedulingOpportunities(Priority priority)
		{
			lock (this)
			{
				ResetSchedulingOpportunities(priority, Runtime.CurrentTimeMillis());
			}
		}

		// used for continuous scheduling
		public virtual void ResetSchedulingOpportunities(Priority priority, long currentTimeMs
			)
		{
			lock (this)
			{
				lastScheduledContainer[priority] = currentTimeMs;
				schedulingOpportunities.SetCount(priority, 0);
			}
		}

		internal virtual AggregateAppResourceUsage GetRunningAggregateAppResourceUsage()
		{
			lock (this)
			{
				long currentTimeMillis = Runtime.CurrentTimeMillis();
				// Don't walk the whole container list if the resources were computed
				// recently.
				if ((currentTimeMillis - lastMemoryAggregateAllocationUpdateTime) > MemAggregateAllocationCacheMsecs)
				{
					long memorySeconds = 0;
					long vcoreSeconds = 0;
					foreach (RMContainer rmContainer in this.liveContainers.Values)
					{
						long usedMillis = currentTimeMillis - rmContainer.GetCreationTime();
						Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = rmContainer.GetContainer()
							.GetResource();
						memorySeconds += resource.GetMemory() * usedMillis / DateUtils.MillisPerSecond;
						vcoreSeconds += resource.GetVirtualCores() * usedMillis / DateUtils.MillisPerSecond;
					}
					lastMemoryAggregateAllocationUpdateTime = currentTimeMillis;
					lastMemorySeconds = memorySeconds;
					lastVcoreSeconds = vcoreSeconds;
				}
				return new AggregateAppResourceUsage(lastMemorySeconds, lastVcoreSeconds);
			}
		}

		public virtual ApplicationResourceUsageReport GetResourceUsageReport()
		{
			lock (this)
			{
				AggregateAppResourceUsage resUsage = GetRunningAggregateAppResourceUsage();
				return ApplicationResourceUsageReport.NewInstance(liveContainers.Count, reservedContainers
					.Count, Resources.Clone(currentConsumption), Resources.Clone(currentReservation)
					, Resources.Add(currentConsumption, currentReservation), resUsage.GetMemorySeconds
					(), resUsage.GetVcoreSeconds());
			}
		}

		public virtual IDictionary<ContainerId, RMContainer> GetLiveContainersMap()
		{
			lock (this)
			{
				return this.liveContainers;
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResourceLimit()
		{
			lock (this)
			{
				return this.resourceLimit;
			}
		}

		public virtual IDictionary<Priority, long> GetLastScheduledContainer()
		{
			lock (this)
			{
				return this.lastScheduledContainer;
			}
		}

		public virtual void TransferStateFromPreviousAttempt(SchedulerApplicationAttempt 
			appAttempt)
		{
			lock (this)
			{
				this.liveContainers = appAttempt.GetLiveContainersMap();
				// this.reReservations = appAttempt.reReservations;
				this.currentConsumption = appAttempt.GetCurrentConsumption();
				this.resourceLimit = appAttempt.GetResourceLimit();
				// this.currentReservation = appAttempt.currentReservation;
				// this.newlyAllocatedContainers = appAttempt.newlyAllocatedContainers;
				// this.schedulingOpportunities = appAttempt.schedulingOpportunities;
				this.lastScheduledContainer = appAttempt.GetLastScheduledContainer();
				this.appSchedulingInfo.TransferStateFromPreviousAppSchedulingInfo(appAttempt.appSchedulingInfo
					);
			}
		}

		public virtual void Move(Queue newQueue)
		{
			lock (this)
			{
				QueueMetrics oldMetrics = queue.GetMetrics();
				QueueMetrics newMetrics = newQueue.GetMetrics();
				string user = GetUser();
				foreach (RMContainer liveContainer in liveContainers.Values)
				{
					Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = liveContainer.GetContainer
						().GetResource();
					oldMetrics.ReleaseResources(user, 1, resource);
					newMetrics.AllocateResources(user, 1, resource, false);
				}
				foreach (IDictionary<NodeId, RMContainer> map in reservedContainers.Values)
				{
					foreach (RMContainer reservedContainer in map.Values)
					{
						Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = reservedContainer.GetReservedResource
							();
						oldMetrics.UnreserveResource(user, resource);
						newMetrics.ReserveResource(user, resource);
					}
				}
				appSchedulingInfo.Move(newQueue);
				this.queue = newQueue;
			}
		}

		public virtual void RecoverContainer(RMContainer rmContainer)
		{
			lock (this)
			{
				// recover app scheduling info
				appSchedulingInfo.RecoverContainer(rmContainer);
				if (rmContainer.GetState().Equals(RMContainerState.Completed))
				{
					return;
				}
				Log.Info("SchedulerAttempt " + GetApplicationAttemptId() + " is recovering container "
					 + rmContainer.GetContainerId());
				liveContainers[rmContainer.GetContainerId()] = rmContainer;
				Resources.AddTo(currentConsumption, rmContainer.GetContainer().GetResource());
			}
		}

		// resourceLimit: updated when LeafQueue#recoverContainer#allocateResource
		// is called.
		// newlyAllocatedContainers.add(rmContainer);
		// schedulingOpportunities
		// lastScheduledContainer
		public virtual void IncNumAllocatedContainers(NodeType containerType, NodeType requestType
			)
		{
			RMAppAttempt attempt = rmContext.GetRMApps()[attemptId.GetApplicationId()].GetCurrentAppAttempt
				();
			if (attempt != null)
			{
				attempt.GetRMAppAttemptMetrics().IncNumAllocatedContainers(containerType, requestType
					);
			}
		}

		public virtual void SetApplicationHeadroomForMetrics(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 headroom)
		{
			RMAppAttempt attempt = rmContext.GetRMApps()[attemptId.GetApplicationId()].GetCurrentAppAttempt
				();
			if (attempt != null)
			{
				attempt.GetRMAppAttemptMetrics().SetApplicationAttemptHeadRoom(Resources.Clone(headroom
					));
			}
		}

		public virtual ICollection<string> GetBlacklistedNodes()
		{
			return this.appSchedulingInfo.GetBlackListCopy();
		}
	}
}
