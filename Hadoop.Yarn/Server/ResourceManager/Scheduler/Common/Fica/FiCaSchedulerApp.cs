using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica
{
	/// <summary>
	/// Represents an application attempt from the viewpoint of the FIFO or Capacity
	/// scheduler.
	/// </summary>
	public class FiCaSchedulerApp : SchedulerApplicationAttempt
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica.FiCaSchedulerApp
			));

		private readonly ICollection<ContainerId> containersToPreempt = new HashSet<ContainerId
			>();

		private CapacityHeadroomProvider headroomProvider;

		public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId, string user, Queue
			 queue, ActiveUsersManager activeUsersManager, RMContext rmContext)
			: base(applicationAttemptId, user, queue, activeUsersManager, rmContext)
		{
			RMApp rmApp = rmContext.GetRMApps()[GetApplicationId()];
			Resource amResource;
			if (rmApp == null || rmApp.GetAMResourceRequest() == null)
			{
				//the rmApp may be undefined (the resource manager checks for this too)
				//and unmanaged applications do not provide an amResource request
				//in these cases, provide a default using the scheduler
				amResource = rmContext.GetScheduler().GetMinimumResourceCapability();
			}
			else
			{
				amResource = rmApp.GetAMResourceRequest().GetCapability();
			}
			SetAMResource(amResource);
		}

		public virtual bool ContainerCompleted(RMContainer rmContainer, ContainerStatus containerStatus
			, RMContainerEventType @event)
		{
			lock (this)
			{
				// Remove from the list of containers
				if (null == Sharpen.Collections.Remove(liveContainers, rmContainer.GetContainerId
					()))
				{
					return false;
				}
				// Remove from the list of newly allocated containers if found
				newlyAllocatedContainers.Remove(rmContainer);
				Container container = rmContainer.GetContainer();
				ContainerId containerId = container.GetId();
				// Inform the container
				rmContainer.Handle(new RMContainerFinishedEvent(containerId, containerStatus, @event
					));
				Log.Info("Completed container: " + rmContainer.GetContainerId() + " in state: " +
					 rmContainer.GetState() + " event:" + @event);
				containersToPreempt.Remove(rmContainer.GetContainerId());
				RMAuditLogger.LogSuccess(GetUser(), RMAuditLogger.AuditConstants.ReleaseContainer
					, "SchedulerApp", GetApplicationId(), containerId);
				// Update usage metrics 
				Resource containerResource = rmContainer.GetContainer().GetResource();
				queue.GetMetrics().ReleaseResources(GetUser(), 1, containerResource);
				Resources.SubtractFrom(currentConsumption, containerResource);
				// Clear resource utilization metrics cache.
				lastMemoryAggregateAllocationUpdateTime = -1;
				return true;
			}
		}

		public virtual RMContainer Allocate(NodeType type, FiCaSchedulerNode node, Priority
			 priority, ResourceRequest request, Container container)
		{
			lock (this)
			{
				if (isStopped)
				{
					return null;
				}
				// Required sanity check - AM can call 'allocate' to update resource 
				// request without locking the scheduler, hence we need to check
				if (GetTotalRequiredResources(priority) <= 0)
				{
					return null;
				}
				// Create RMContainer
				RMContainer rmContainer = new RMContainerImpl(container, this.GetApplicationAttemptId
					(), node.GetNodeID(), appSchedulingInfo.GetUser(), this.rmContext);
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

		public virtual bool Unreserve(FiCaSchedulerNode node, Priority priority)
		{
			lock (this)
			{
				IDictionary<NodeId, RMContainer> reservedContainers = this.reservedContainers[priority
					];
				if (reservedContainers != null)
				{
					RMContainer reservedContainer = Sharpen.Collections.Remove(reservedContainers, node
						.GetNodeID());
					// unreserve is now triggered in new scenarios (preemption)
					// as a consequence reservedcontainer might be null, adding NP-checks
					if (reservedContainer != null && reservedContainer.GetContainer() != null && reservedContainer
						.GetContainer().GetResource() != null)
					{
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
						return true;
					}
				}
				return false;
			}
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

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetTotalPendingRequests
			()
		{
			lock (this)
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource ret = Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(0, 0);
				foreach (ResourceRequest rr in appSchedulingInfo.GetAllResourceRequests())
				{
					// to avoid double counting we count only "ANY" resource requests
					if (ResourceRequest.IsAnyLocation(rr.GetResourceName()))
					{
						Resources.AddTo(ret, Resources.Multiply(rr.GetCapability(), rr.GetNumContainers()
							));
					}
				}
				return ret;
			}
		}

		public virtual void AddPreemptContainer(ContainerId cont)
		{
			lock (this)
			{
				// ignore already completed containers
				if (liveContainers.Contains(cont))
				{
					containersToPreempt.AddItem(cont);
				}
			}
		}

		/// <summary>
		/// This method produces an Allocation that includes the current view
		/// of the resources that will be allocated to and preempted from this
		/// application.
		/// </summary>
		/// <param name="rc"/>
		/// <param name="clusterResource"/>
		/// <param name="minimumAllocation"/>
		/// <returns>an allocation</returns>
		public virtual Allocation GetAllocation(ResourceCalculator rc, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource minimumAllocation)
		{
			lock (this)
			{
				ICollection<ContainerId> currentContPreemption = Sharpen.Collections.UnmodifiableSet
					(new HashSet<ContainerId>(containersToPreempt));
				containersToPreempt.Clear();
				Org.Apache.Hadoop.Yarn.Api.Records.Resource tot = Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(0, 0);
				foreach (ContainerId c in currentContPreemption)
				{
					Resources.AddTo(tot, liveContainers[c].GetContainer().GetResource());
				}
				int numCont = (int)Math.Ceil(Resources.Divide(rc, clusterResource, tot, minimumAllocation
					));
				ResourceRequest rr = ResourceRequest.NewInstance(Priority.Undefined, ResourceRequest
					.Any, minimumAllocation, numCont);
				SchedulerApplicationAttempt.ContainersAndNMTokensAllocation allocation = PullNewlyAllocatedContainersAndNMTokens
					();
				Org.Apache.Hadoop.Yarn.Api.Records.Resource headroom = GetHeadroom();
				SetApplicationHeadroomForMetrics(headroom);
				return new Allocation(allocation.GetContainerList(), headroom, null, currentContPreemption
					, Sharpen.Collections.SingletonList(rr), allocation.GetNMTokenList());
			}
		}

		public virtual NodeId GetNodeIdToUnreserve(Priority priority, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 resourceNeedUnreserve, ResourceCalculator rc, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			lock (this)
			{
				// first go around make this algorithm simple and just grab first
				// reservation that has enough resources
				IDictionary<NodeId, RMContainer> reservedContainers = this.reservedContainers[priority
					];
				if ((reservedContainers != null) && (!reservedContainers.IsEmpty()))
				{
					foreach (KeyValuePair<NodeId, RMContainer> entry in reservedContainers)
					{
						NodeId nodeId = entry.Key;
						Org.Apache.Hadoop.Yarn.Api.Records.Resource containerResource = entry.Value.GetContainer
							().GetResource();
						// make sure we unreserve one with at least the same amount of
						// resources, otherwise could affect capacity limits
						if (Resources.LessThanOrEqual(rc, clusterResource, resourceNeedUnreserve, containerResource
							))
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("unreserving node with reservation size: " + containerResource + " in order to allocate container with size: "
									 + resourceNeedUnreserve);
							}
							return nodeId;
						}
					}
				}
				return null;
			}
		}

		public virtual void SetHeadroomProvider(CapacityHeadroomProvider headroomProvider
			)
		{
			lock (this)
			{
				this.headroomProvider = headroomProvider;
			}
		}

		public virtual CapacityHeadroomProvider GetHeadroomProvider()
		{
			lock (this)
			{
				return headroomProvider;
			}
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetHeadroom()
		{
			lock (this)
			{
				if (headroomProvider != null)
				{
					return headroomProvider.GetHeadroom();
				}
				return base.GetHeadroom();
			}
		}

		public override void TransferStateFromPreviousAttempt(SchedulerApplicationAttempt
			 appAttempt)
		{
			lock (this)
			{
				base.TransferStateFromPreviousAttempt(appAttempt);
				this.headroomProvider = ((Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica.FiCaSchedulerApp
					)appAttempt).GetHeadroomProvider();
			}
		}
	}
}
