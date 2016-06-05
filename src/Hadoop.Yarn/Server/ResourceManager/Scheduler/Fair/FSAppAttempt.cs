using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>Represents an application attempt from the viewpoint of the Fair Scheduler.
	/// 	</summary>
	public class FSAppAttempt : SchedulerApplicationAttempt, Schedulable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSAppAttempt
			));

		private static readonly DefaultResourceCalculator ResourceCalculator = new DefaultResourceCalculator
			();

		private long startTime;

		private Priority priority;

		private ResourceWeights resourceWeights;

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource demand = Resources.CreateResource
			(0);

		private FairScheduler scheduler;

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource fairShare = Resources.CreateResource
			(0, 0);

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource preemptedResources = Resources
			.CreateResource(0);

		private FSAppAttempt.RMContainerComparator comparator = new FSAppAttempt.RMContainerComparator
			();

		private readonly IDictionary<RMContainer, long> preemptionMap = new Dictionary<RMContainer
			, long>();

		/// <summary>
		/// Delay scheduling: We often want to prioritize scheduling of node-local
		/// containers over rack-local or off-switch containers.
		/// </summary>
		/// <remarks>
		/// Delay scheduling: We often want to prioritize scheduling of node-local
		/// containers over rack-local or off-switch containers. To achieve this
		/// we first only allow node-local assignments for a given priority level,
		/// then relax the locality threshold once we've had a long enough period
		/// without successfully scheduling. We measure both the number of "missed"
		/// scheduling opportunities since the last container was scheduled
		/// at the current allowed level and the time since the last container
		/// was scheduled. Currently we use only the former.
		/// </remarks>
		private readonly IDictionary<Priority, NodeType> allowedLocalityLevel = new Dictionary
			<Priority, NodeType>();

		public FSAppAttempt(FairScheduler scheduler, ApplicationAttemptId applicationAttemptId
			, string user, FSLeafQueue queue, ActiveUsersManager activeUsersManager, RMContext
			 rmContext)
			: base(applicationAttemptId, user, queue, activeUsersManager, rmContext)
		{
			this.scheduler = scheduler;
			this.startTime = scheduler.GetClock().GetTime();
			this.priority = Priority.NewInstance(1);
			this.resourceWeights = new ResourceWeights();
		}

		public virtual ResourceWeights GetResourceWeights()
		{
			return resourceWeights;
		}

		/// <summary>Get metrics reference from containing queue.</summary>
		public virtual QueueMetrics GetMetrics()
		{
			return queue.GetMetrics();
		}

		public virtual void ContainerCompleted(RMContainer rmContainer, ContainerStatus containerStatus
			, RMContainerEventType @event)
		{
			lock (this)
			{
				Container container = rmContainer.GetContainer();
				ContainerId containerId = container.GetId();
				// Remove from the list of newly allocated containers if found
				newlyAllocatedContainers.Remove(rmContainer);
				// Inform the container
				rmContainer.Handle(new RMContainerFinishedEvent(containerId, containerStatus, @event
					));
				Log.Info("Completed container: " + rmContainer.GetContainerId() + " in state: " +
					 rmContainer.GetState() + " event:" + @event);
				// Remove from the list of containers
				Sharpen.Collections.Remove(liveContainers, rmContainer.GetContainerId());
				RMAuditLogger.LogSuccess(GetUser(), RMAuditLogger.AuditConstants.ReleaseContainer
					, "SchedulerApp", GetApplicationId(), containerId);
				// Update usage metrics 
				Org.Apache.Hadoop.Yarn.Api.Records.Resource containerResource = rmContainer.GetContainer
					().GetResource();
				queue.GetMetrics().ReleaseResources(GetUser(), 1, containerResource);
				Resources.SubtractFrom(currentConsumption, containerResource);
				// remove from preemption map if it is completed
				Sharpen.Collections.Remove(preemptionMap, rmContainer);
				// Clear resource utilization metrics cache.
				lastMemoryAggregateAllocationUpdateTime = -1;
			}
		}

		private void UnreserveInternal(Priority priority, FSSchedulerNode node)
		{
			lock (this)
			{
				IDictionary<NodeId, RMContainer> reservedContainers = this.reservedContainers[priority
					];
				RMContainer reservedContainer = Sharpen.Collections.Remove(reservedContainers, node
					.GetNodeID());
				if (reservedContainers.IsEmpty())
				{
					Sharpen.Collections.Remove(this.reservedContainers, priority);
				}
				// Reset the re-reservation count
				ResetReReservations(priority);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = reservedContainer.GetContainer
					().GetResource();
				Resources.SubtractFrom(currentReservation, resource);
				Log.Info("Application " + GetApplicationId() + " unreserved " + " on node " + node
					 + ", currently has " + reservedContainers.Count + " at priority " + priority + 
					"; currentReservation " + currentReservation);
			}
		}

		/// <summary>
		/// Headroom depends on resources in the cluster, current usage of the
		/// queue, queue's fair-share and queue's max-resources.
		/// </summary>
		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetHeadroom()
		{
			FSQueue queue = (FSQueue)this.queue;
			SchedulingPolicy policy = queue.GetPolicy();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource queueFairShare = queue.GetFairShare();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource queueUsage = queue.GetResourceUsage();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = this.scheduler.GetClusterResource
				();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterUsage = this.scheduler.GetRootQueueMetrics
				().GetAllocatedResources();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterAvailableResources = Resources
				.Subtract(clusterResource, clusterUsage);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource queueMaxAvailableResources = Resources
				.Subtract(queue.GetMaxShare(), queueUsage);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxAvailableResource = Resources.ComponentwiseMin
				(clusterAvailableResources, queueMaxAvailableResources);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource headroom = policy.GetHeadroom(queueFairShare
				, queueUsage, maxAvailableResource);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Headroom calculation for " + this.GetName() + ":" + "Min(" + "(queueFairShare="
					 + queueFairShare + " - queueUsage=" + queueUsage + ")," + " maxAvailableResource="
					 + maxAvailableResource + "Headroom=" + headroom);
			}
			return headroom;
		}

		public virtual float GetLocalityWaitFactor(Priority priority, int clusterNodes)
		{
			lock (this)
			{
				// Estimate: Required unique resources (i.e. hosts + racks)
				int requiredResources = Math.Max(this.GetResourceRequests(priority).Count - 1, 0);
				// waitFactor can't be more than '1' 
				// i.e. no point skipping more than clustersize opportunities
				return Math.Min(((float)requiredResources / clusterNodes), 1.0f);
			}
		}

		/// <summary>
		/// Return the level at which we are allowed to schedule containers, given the
		/// current size of the cluster and thresholds indicating how many nodes to
		/// fail at (as a fraction of cluster size) before relaxing scheduling
		/// constraints.
		/// </summary>
		public virtual NodeType GetAllowedLocalityLevel(Priority priority, int numNodes, 
			double nodeLocalityThreshold, double rackLocalityThreshold)
		{
			lock (this)
			{
				// upper limit on threshold
				if (nodeLocalityThreshold > 1.0)
				{
					nodeLocalityThreshold = 1.0;
				}
				if (rackLocalityThreshold > 1.0)
				{
					rackLocalityThreshold = 1.0;
				}
				// If delay scheduling is not being used, can schedule anywhere
				if (nodeLocalityThreshold < 0.0 || rackLocalityThreshold < 0.0)
				{
					return NodeType.OffSwitch;
				}
				// Default level is NODE_LOCAL
				if (!allowedLocalityLevel.Contains(priority))
				{
					allowedLocalityLevel[priority] = NodeType.NodeLocal;
					return NodeType.NodeLocal;
				}
				NodeType allowed = allowedLocalityLevel[priority];
				// If level is already most liberal, we're done
				if (allowed.Equals(NodeType.OffSwitch))
				{
					return NodeType.OffSwitch;
				}
				double threshold = allowed.Equals(NodeType.NodeLocal) ? nodeLocalityThreshold : rackLocalityThreshold;
				// Relax locality constraints once we've surpassed threshold.
				if (GetSchedulingOpportunities(priority) > (numNodes * threshold))
				{
					if (allowed.Equals(NodeType.NodeLocal))
					{
						allowedLocalityLevel[priority] = NodeType.RackLocal;
						ResetSchedulingOpportunities(priority);
					}
					else
					{
						if (allowed.Equals(NodeType.RackLocal))
						{
							allowedLocalityLevel[priority] = NodeType.OffSwitch;
							ResetSchedulingOpportunities(priority);
						}
					}
				}
				return allowedLocalityLevel[priority];
			}
		}

		/// <summary>Return the level at which we are allowed to schedule containers.</summary>
		/// <remarks>
		/// Return the level at which we are allowed to schedule containers.
		/// Given the thresholds indicating how much time passed before relaxing
		/// scheduling constraints.
		/// </remarks>
		public virtual NodeType GetAllowedLocalityLevelByTime(Priority priority, long nodeLocalityDelayMs
			, long rackLocalityDelayMs, long currentTimeMs)
		{
			lock (this)
			{
				// if not being used, can schedule anywhere
				if (nodeLocalityDelayMs < 0 || rackLocalityDelayMs < 0)
				{
					return NodeType.OffSwitch;
				}
				// default level is NODE_LOCAL
				if (!allowedLocalityLevel.Contains(priority))
				{
					allowedLocalityLevel[priority] = NodeType.NodeLocal;
					return NodeType.NodeLocal;
				}
				NodeType allowed = allowedLocalityLevel[priority];
				// if level is already most liberal, we're done
				if (allowed.Equals(NodeType.OffSwitch))
				{
					return NodeType.OffSwitch;
				}
				// check waiting time
				long waitTime = currentTimeMs;
				if (lastScheduledContainer.Contains(priority))
				{
					waitTime -= lastScheduledContainer[priority];
				}
				else
				{
					waitTime -= GetStartTime();
				}
				long thresholdTime = allowed.Equals(NodeType.NodeLocal) ? nodeLocalityDelayMs : rackLocalityDelayMs;
				if (waitTime > thresholdTime)
				{
					if (allowed.Equals(NodeType.NodeLocal))
					{
						allowedLocalityLevel[priority] = NodeType.RackLocal;
						ResetSchedulingOpportunities(priority, currentTimeMs);
					}
					else
					{
						if (allowed.Equals(NodeType.RackLocal))
						{
							allowedLocalityLevel[priority] = NodeType.OffSwitch;
							ResetSchedulingOpportunities(priority, currentTimeMs);
						}
					}
				}
				return allowedLocalityLevel[priority];
			}
		}

		public virtual RMContainer Allocate(NodeType type, FSSchedulerNode node, Priority
			 priority, ResourceRequest request, Container container)
		{
			lock (this)
			{
				// Update allowed locality level
				NodeType allowed = allowedLocalityLevel[priority];
				if (allowed != null)
				{
					if (allowed.Equals(NodeType.OffSwitch) && (type.Equals(NodeType.NodeLocal) || type
						.Equals(NodeType.RackLocal)))
					{
						this.ResetAllowedLocalityLevel(priority, type);
					}
					else
					{
						if (allowed.Equals(NodeType.RackLocal) && type.Equals(NodeType.NodeLocal))
						{
							this.ResetAllowedLocalityLevel(priority, type);
						}
					}
				}
				// Required sanity check - AM can call 'allocate' to update resource 
				// request without locking the scheduler, hence we need to check
				if (GetTotalRequiredResources(priority) <= 0)
				{
					return null;
				}
				// Create RMContainer
				RMContainer rmContainer = new RMContainerImpl(container, GetApplicationAttemptId(
					), node.GetNodeID(), appSchedulingInfo.GetUser(), rmContext);
				// Add it to allContainers list.
				newlyAllocatedContainers.AddItem(rmContainer);
				liveContainers[container.GetId()] = rmContainer;
				// Update consumption and track allocations
				IList<ResourceRequest> resourceRequestList = appSchedulingInfo.Allocate(type, node
					, priority, request, container);
				Resources.AddTo(currentConsumption, container.GetResource());
				// Update resource requests related to "request" and store in RMContainer
				((RMContainerImpl)rmContainer).SetResourceRequests(resourceRequestList);
				// Inform the container
				rmContainer.Handle(new RMContainerEvent(container.GetId(), RMContainerEventType.Start
					));
				if (Log.IsDebugEnabled())
				{
					Log.Debug("allocate: applicationAttemptId=" + container.GetId().GetApplicationAttemptId
						() + " container=" + container.GetId() + " host=" + container.GetNodeId().GetHost
						() + " type=" + type);
				}
				RMAuditLogger.LogSuccess(GetUser(), RMAuditLogger.AuditConstants.AllocContainer, 
					"SchedulerApp", GetApplicationId(), container.GetId());
				return rmContainer;
			}
		}

		/// <summary>
		/// Should be called when the scheduler assigns a container at a higher
		/// degree of locality than the current threshold.
		/// </summary>
		/// <remarks>
		/// Should be called when the scheduler assigns a container at a higher
		/// degree of locality than the current threshold. Reset the allowed locality
		/// level to a higher degree of locality.
		/// </remarks>
		public virtual void ResetAllowedLocalityLevel(Priority priority, NodeType level)
		{
			lock (this)
			{
				NodeType old = allowedLocalityLevel[priority];
				Log.Info("Raising locality level from " + old + " to " + level + " at " + " priority "
					 + priority);
				allowedLocalityLevel[priority] = level;
			}
		}

		// related methods
		public virtual void AddPreemption(RMContainer container, long time)
		{
			System.Diagnostics.Debug.Assert(preemptionMap[container] == null);
			preemptionMap[container] = time;
			Resources.AddTo(preemptedResources, container.GetAllocatedResource());
		}

		public virtual long GetContainerPreemptionTime(RMContainer container)
		{
			return preemptionMap[container];
		}

		public virtual ICollection<RMContainer> GetPreemptionContainers()
		{
			return preemptionMap.Keys;
		}

		public override Queue GetQueue()
		{
			return (FSLeafQueue)base.GetQueue();
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetPreemptedResources(
			)
		{
			return preemptedResources;
		}

		public virtual void ResetPreemptedResources()
		{
			preemptedResources = Resources.CreateResource(0);
			foreach (RMContainer container in GetPreemptionContainers())
			{
				Resources.AddTo(preemptedResources, container.GetAllocatedResource());
			}
		}

		public virtual void ClearPreemptedResources()
		{
			preemptedResources.SetMemory(0);
			preemptedResources.SetVirtualCores(0);
		}

		/// <summary>
		/// Create and return a container object reflecting an allocation for the
		/// given appliction on the given node with the given capability and
		/// priority.
		/// </summary>
		public virtual Container CreateContainer(FSSchedulerNode node, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 capability, Priority priority)
		{
			NodeId nodeId = node.GetRMNode().GetNodeID();
			ContainerId containerId = BuilderUtils.NewContainerId(GetApplicationAttemptId(), 
				GetNewContainerId());
			// Create the container
			Container container = BuilderUtils.NewContainer(containerId, nodeId, node.GetRMNode
				().GetHttpAddress(), capability, priority, null);
			return container;
		}

		/// <summary>
		/// Reserve a spot for
		/// <paramref name="container"/>
		/// on this
		/// <paramref name="node"/>
		/// . If
		/// the container is
		/// <paramref name="alreadyReserved"/>
		/// on the node, simply
		/// update relevant bookeeping. This dispatches ro relevant handlers
		/// in
		/// <see cref="FSSchedulerNode"/>
		/// ..
		/// </summary>
		private void Reserve(Priority priority, FSSchedulerNode node, Container container
			, bool alreadyReserved)
		{
			Log.Info("Making reservation: node=" + node.GetNodeName() + " app_id=" + GetApplicationId
				());
			if (!alreadyReserved)
			{
				GetMetrics().ReserveResource(GetUser(), container.GetResource());
				RMContainer rmContainer = base.Reserve(node, priority, null, container);
				node.ReserveResource(this, priority, rmContainer);
			}
			else
			{
				RMContainer rmContainer = node.GetReservedContainer();
				base.Reserve(node, priority, rmContainer, container);
				node.ReserveResource(this, priority, rmContainer);
			}
		}

		/// <summary>
		/// Remove the reservation on
		/// <paramref name="node"/>
		/// at the given
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Priority"/>
		/// .
		/// This dispatches SchedulerNode handlers as well.
		/// </summary>
		public virtual void Unreserve(Priority priority, FSSchedulerNode node)
		{
			RMContainer rmContainer = node.GetReservedContainer();
			UnreserveInternal(priority, node);
			node.UnreserveResource(this);
			GetMetrics().UnreserveResource(GetUser(), rmContainer.GetContainer().GetResource(
				));
		}

		/// <summary>
		/// Assign a container to this node to facilitate
		/// <paramref name="request"/>
		/// . If node does
		/// not have enough memory, create a reservation. This is called once we are
		/// sure the particular request should be facilitated by this node.
		/// </summary>
		/// <param name="node">The node to try placing the container on.</param>
		/// <param name="request">The ResourceRequest we're trying to satisfy.</param>
		/// <param name="type">The locality of the assignment.</param>
		/// <param name="reserved">Whether there's already a container reserved for this app on the node.
		/// 	</param>
		/// <returns>
		/// If an assignment was made, returns the resources allocated to the
		/// container.  If a reservation was made, returns
		/// FairScheduler.CONTAINER_RESERVED.  If no assignment or reservation was
		/// made, returns an empty resource.
		/// </returns>
		private Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignContainer(FSSchedulerNode
			 node, ResourceRequest request, NodeType type, bool reserved)
		{
			// How much does this request need?
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability = request.GetCapability();
			// How much does the node have?
			Org.Apache.Hadoop.Yarn.Api.Records.Resource available = node.GetAvailableResource
				();
			Container container = null;
			if (reserved)
			{
				container = node.GetReservedContainer().GetContainer();
			}
			else
			{
				container = CreateContainer(node, capability, request.GetPriority());
			}
			// Can we allocate a container on this node?
			if (Resources.FitsIn(capability, available))
			{
				// Inform the application of the new container for this request
				RMContainer allocatedContainer = Allocate(type, node, request.GetPriority(), request
					, container);
				if (allocatedContainer == null)
				{
					// Did the application need this resource?
					if (reserved)
					{
						Unreserve(request.GetPriority(), node);
					}
					return Resources.None();
				}
				// If we had previously made a reservation, delete it
				if (reserved)
				{
					Unreserve(request.GetPriority(), node);
				}
				// Inform the node
				node.AllocateContainer(allocatedContainer);
				// If this container is used to run AM, update the leaf queue's AM usage
				if (GetLiveContainers().Count == 1 && !GetUnmanagedAM())
				{
					((FSLeafQueue)GetQueue()).AddAMResourceUsage(container.GetResource());
					SetAmRunning(true);
				}
				return container.GetResource();
			}
			else
			{
				if (!FairScheduler.FitsInMaxShare(((FSLeafQueue)GetQueue()), capability))
				{
					return Resources.None();
				}
				// The desired container won't fit here, so reserve
				Reserve(request.GetPriority(), node, container, reserved);
				return FairScheduler.ContainerReserved;
			}
		}

		private bool HasNodeOrRackLocalRequests(Priority priority)
		{
			return GetResourceRequests(priority).Count > 1;
		}

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignContainer(FSSchedulerNode
			 node, bool reserved)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Node offered to app: " + GetName() + " reserved: " + reserved);
			}
			ICollection<Priority> prioritiesToTry = (reserved) ? Arrays.AsList(node.GetReservedContainer
				().GetReservedPriority()) : GetPriorities();
			// For each priority, see if we can schedule a node local, rack local
			// or off-switch request. Rack of off-switch requests may be delayed
			// (not scheduled) in order to promote better locality.
			lock (this)
			{
				foreach (Priority priority in prioritiesToTry)
				{
					if (GetTotalRequiredResources(priority) <= 0 || !HasContainerForNode(priority, node
						))
					{
						continue;
					}
					AddSchedulingOpportunity(priority);
					// Check the AM resource usage for the leaf queue
					if (GetLiveContainers().Count == 0 && !GetUnmanagedAM())
					{
						if (!((FSLeafQueue)GetQueue()).CanRunAppAM(GetAMResource()))
						{
							return Resources.None();
						}
					}
					ResourceRequest rackLocalRequest = GetResourceRequest(priority, node.GetRackName(
						));
					ResourceRequest localRequest = GetResourceRequest(priority, node.GetNodeName());
					if (localRequest != null && !localRequest.GetRelaxLocality())
					{
						Log.Warn("Relax locality off is not supported on local request: " + localRequest);
					}
					NodeType allowedLocality;
					if (scheduler.IsContinuousSchedulingEnabled())
					{
						allowedLocality = GetAllowedLocalityLevelByTime(priority, scheduler.GetNodeLocalityDelayMs
							(), scheduler.GetRackLocalityDelayMs(), scheduler.GetClock().GetTime());
					}
					else
					{
						allowedLocality = GetAllowedLocalityLevel(priority, scheduler.GetNumClusterNodes(
							), scheduler.GetNodeLocalityThreshold(), scheduler.GetRackLocalityThreshold());
					}
					if (rackLocalRequest != null && rackLocalRequest.GetNumContainers() != 0 && localRequest
						 != null && localRequest.GetNumContainers() != 0)
					{
						return AssignContainer(node, localRequest, NodeType.NodeLocal, reserved);
					}
					if (rackLocalRequest != null && !rackLocalRequest.GetRelaxLocality())
					{
						continue;
					}
					if (rackLocalRequest != null && rackLocalRequest.GetNumContainers() != 0 && (allowedLocality
						.Equals(NodeType.RackLocal) || allowedLocality.Equals(NodeType.OffSwitch)))
					{
						return AssignContainer(node, rackLocalRequest, NodeType.RackLocal, reserved);
					}
					ResourceRequest offSwitchRequest = GetResourceRequest(priority, ResourceRequest.Any
						);
					if (offSwitchRequest != null && !offSwitchRequest.GetRelaxLocality())
					{
						continue;
					}
					if (offSwitchRequest != null && offSwitchRequest.GetNumContainers() != 0)
					{
						if (!HasNodeOrRackLocalRequests(priority) || allowedLocality.Equals(NodeType.OffSwitch
							))
						{
							return AssignContainer(node, offSwitchRequest, NodeType.OffSwitch, reserved);
						}
					}
				}
			}
			return Resources.None();
		}

		/// <summary>
		/// Called when this application already has an existing reservation on the
		/// given node.
		/// </summary>
		/// <remarks>
		/// Called when this application already has an existing reservation on the
		/// given node.  Sees whether we can turn the reservation into an allocation.
		/// Also checks whether the application needs the reservation anymore, and
		/// releases it if not.
		/// </remarks>
		/// <param name="node">Node that the application has an existing reservation on</param>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignReservedContainer
			(FSSchedulerNode node)
		{
			RMContainer rmContainer = node.GetReservedContainer();
			Priority priority = rmContainer.GetReservedPriority();
			// Make sure the application still needs requests at this priority
			if (GetTotalRequiredResources(priority) == 0)
			{
				Unreserve(priority, node);
				return Resources.None();
			}
			// Fail early if the reserved container won't fit.
			// Note that we have an assumption here that there's only one container size
			// per priority.
			if (!Resources.FitsIn(node.GetReservedContainer().GetReservedResource(), node.GetAvailableResource
				()))
			{
				return Resources.None();
			}
			return AssignContainer(node, true);
		}

		/// <summary>
		/// Whether this app has containers requests that could be satisfied on the
		/// given node, if the node had full space.
		/// </summary>
		public virtual bool HasContainerForNode(Priority prio, FSSchedulerNode node)
		{
			ResourceRequest anyRequest = GetResourceRequest(prio, ResourceRequest.Any);
			ResourceRequest rackRequest = GetResourceRequest(prio, node.GetRackName());
			ResourceRequest nodeRequest = GetResourceRequest(prio, node.GetNodeName());
			return anyRequest != null && anyRequest.GetNumContainers() > 0 && (anyRequest.GetRelaxLocality
				() || (rackRequest != null && rackRequest.GetNumContainers() > 0)) && (rackRequest
				 == null || rackRequest.GetRelaxLocality() || (nodeRequest != null && nodeRequest
				.GetNumContainers() > 0)) && Resources.LessThanOrEqual(ResourceCalculator, null, 
				anyRequest.GetCapability(), node.GetRMNode().GetTotalCapability());
		}

		[System.Serializable]
		internal class RMContainerComparator : IComparer<RMContainer>
		{
			// There must be outstanding requests at the given priority:
			// If locality relaxation is turned off at *-level, there must be a
			// non-zero request for the node's rack:
			// If locality relaxation is turned off at rack-level, there must be a
			// non-zero request at the node:
			// The requested container must be able to fit on the node:
			public virtual int Compare(RMContainer c1, RMContainer c2)
			{
				int ret = c1.GetContainer().GetPriority().CompareTo(c2.GetContainer().GetPriority
					());
				if (ret == 0)
				{
					return c2.GetContainerId().CompareTo(c1.GetContainerId());
				}
				return ret;
			}
		}

		/* Schedulable methods implementation */
		public virtual string GetName()
		{
			return GetApplicationId().ToString();
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetDemand()
		{
			return demand;
		}

		public virtual long GetStartTime()
		{
			return startTime;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMinShare()
		{
			return Resources.None();
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaxShare()
		{
			return Resources.Unbounded();
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResourceUsage()
		{
			// Here the getPreemptedResources() always return zero, except in
			// a preemption round
			return Resources.Subtract(GetCurrentConsumption(), GetPreemptedResources());
		}

		public virtual ResourceWeights GetWeights()
		{
			return scheduler.GetAppWeight(this);
		}

		public virtual Priority GetPriority()
		{
			// Right now per-app priorities are not passed to scheduler,
			// so everyone has the same priority.
			return priority;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetFairShare()
		{
			return this.fairShare;
		}

		public virtual void SetFairShare(Org.Apache.Hadoop.Yarn.Api.Records.Resource fairShare
			)
		{
			this.fairShare = fairShare;
		}

		public virtual void UpdateDemand()
		{
			demand = Resources.CreateResource(0);
			// Demand is current consumption plus outstanding requests
			Resources.AddTo(demand, GetCurrentConsumption());
			// Add up outstanding resource requests
			lock (this)
			{
				foreach (Priority p in GetPriorities())
				{
					foreach (ResourceRequest r in GetResourceRequests(p).Values)
					{
						Org.Apache.Hadoop.Yarn.Api.Records.Resource total = Resources.Multiply(r.GetCapability
							(), r.GetNumContainers());
						Resources.AddTo(demand, total);
					}
				}
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignContainer(FSSchedulerNode
			 node)
		{
			return AssignContainer(node, false);
		}

		/// <summary>Preempt a running container according to the priority</summary>
		public virtual RMContainer PreemptContainer()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("App " + GetName() + " is going to preempt a running " + "container");
			}
			RMContainer toBePreempted = null;
			foreach (RMContainer container in GetLiveContainers())
			{
				if (!GetPreemptionContainers().Contains(container) && (toBePreempted == null || comparator
					.Compare(toBePreempted, container) > 0))
				{
					toBePreempted = container;
				}
			}
			return toBePreempted;
		}
	}
}
