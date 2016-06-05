using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>This class keeps track of all the consumption of an application.</summary>
	/// <remarks>
	/// This class keeps track of all the consumption of an application. This also
	/// keeps track of current running/completed containers for the application.
	/// </remarks>
	public class AppSchedulingInfo
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.AppSchedulingInfo
			));

		private readonly ApplicationAttemptId applicationAttemptId;

		internal readonly ApplicationId applicationId;

		private string queueName;

		internal Queue queue;

		internal readonly string user;

		private readonly AtomicLong containerIdCounter;

		private readonly int EpochBitShift = 40;

		internal readonly ICollection<Priority> priorities = new TreeSet<Priority>(new Priority.Comparator
			());

		internal readonly IDictionary<Priority, IDictionary<string, ResourceRequest>> requests
			 = new ConcurrentHashMap<Priority, IDictionary<string, ResourceRequest>>();

		private ICollection<string> blacklist = new HashSet<string>();

		private ActiveUsersManager activeUsersManager;

		internal bool pending = true;

		public AppSchedulingInfo(ApplicationAttemptId appAttemptId, string user, Queue queue
			, ActiveUsersManager activeUsersManager, long epoch)
		{
			// TODO making containerIdCounter long
			//private final ApplicationStore store;
			/* Allocated by scheduler */
			// for app metrics
			this.applicationAttemptId = appAttemptId;
			this.applicationId = appAttemptId.GetApplicationId();
			this.queue = queue;
			this.queueName = queue.GetQueueName();
			this.user = user;
			this.activeUsersManager = activeUsersManager;
			this.containerIdCounter = new AtomicLong(epoch << EpochBitShift);
		}

		public virtual ApplicationId GetApplicationId()
		{
			return applicationId;
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return applicationAttemptId;
		}

		public virtual string GetQueueName()
		{
			return queueName;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual bool IsPending()
		{
			lock (this)
			{
				return pending;
			}
		}

		/// <summary>Clear any pending requests from this application.</summary>
		private void ClearRequests()
		{
			lock (this)
			{
				priorities.Clear();
				requests.Clear();
				Log.Info("Application " + applicationId + " requests cleared");
			}
		}

		public virtual long GetNewContainerId()
		{
			return this.containerIdCounter.IncrementAndGet();
		}

		/// <summary>
		/// The ApplicationMaster is updating resource requirements for the
		/// application, by asking for more resources and releasing resources acquired
		/// by the application.
		/// </summary>
		/// <param name="requests">resources to be acquired</param>
		/// <param name="recoverPreemptedRequest">recover Resource Request on preemption</param>
		public virtual void UpdateResourceRequests(IList<ResourceRequest> requests, bool 
			recoverPreemptedRequest)
		{
			lock (this)
			{
				QueueMetrics metrics = queue.GetMetrics();
				// Update resource requests
				foreach (ResourceRequest request in requests)
				{
					Priority priority = request.GetPriority();
					string resourceName = request.GetResourceName();
					bool updatePendingResources = false;
					ResourceRequest lastRequest = null;
					if (resourceName.Equals(ResourceRequest.Any))
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("update:" + " application=" + applicationId + " request=" + request);
						}
						updatePendingResources = true;
						// Premature optimization?
						// Assumes that we won't see more than one priority request updated
						// in one call, reasonable assumption... however, it's totally safe
						// to activate same application more than once.
						// Thus we don't need another loop ala the one in decrementOutstanding()  
						// which is needed during deactivate.
						if (request.GetNumContainers() > 0)
						{
							activeUsersManager.ActivateApplication(user, applicationId);
						}
					}
					IDictionary<string, ResourceRequest> asks = this.requests[priority];
					if (asks == null)
					{
						asks = new ConcurrentHashMap<string, ResourceRequest>();
						this.requests[priority] = asks;
						this.priorities.AddItem(priority);
					}
					lastRequest = asks[resourceName];
					if (recoverPreemptedRequest && lastRequest != null)
					{
						// Increment the number of containers to 1, as it is recovering a
						// single container.
						request.SetNumContainers(lastRequest.GetNumContainers() + 1);
					}
					asks[resourceName] = request;
					if (updatePendingResources)
					{
						// Similarly, deactivate application?
						if (request.GetNumContainers() <= 0)
						{
							Log.Info("checking for deactivate of application :" + this.applicationId);
							CheckForDeactivation();
						}
						int lastRequestContainers = lastRequest != null ? lastRequest.GetNumContainers() : 
							0;
						Org.Apache.Hadoop.Yarn.Api.Records.Resource lastRequestCapability = lastRequest !=
							 null ? lastRequest.GetCapability() : Resources.None();
						metrics.IncrPendingResources(user, request.GetNumContainers(), request.GetCapability
							());
						metrics.DecrPendingResources(user, lastRequestContainers, lastRequestCapability);
					}
				}
			}
		}

		/// <summary>The ApplicationMaster is updating the blacklist</summary>
		/// <param name="blacklistAdditions">resources to be added to the blacklist</param>
		/// <param name="blacklistRemovals">resources to be removed from the blacklist</param>
		public virtual void UpdateBlacklist(IList<string> blacklistAdditions, IList<string
			> blacklistRemovals)
		{
			lock (this)
			{
				// Add to blacklist
				if (blacklistAdditions != null)
				{
					Sharpen.Collections.AddAll(blacklist, blacklistAdditions);
				}
				// Remove from blacklist
				if (blacklistRemovals != null)
				{
					blacklist.RemoveAll(blacklistRemovals);
				}
			}
		}

		public virtual ICollection<Priority> GetPriorities()
		{
			lock (this)
			{
				return priorities;
			}
		}

		public virtual IDictionary<string, ResourceRequest> GetResourceRequests(Priority 
			priority)
		{
			lock (this)
			{
				return requests[priority];
			}
		}

		public virtual IList<ResourceRequest> GetAllResourceRequests()
		{
			IList<ResourceRequest> ret = new AList<ResourceRequest>();
			foreach (IDictionary<string, ResourceRequest> r in requests.Values)
			{
				Sharpen.Collections.AddAll(ret, r.Values);
			}
			return ret;
		}

		public virtual ResourceRequest GetResourceRequest(Priority priority, string resourceName
			)
		{
			lock (this)
			{
				IDictionary<string, ResourceRequest> nodeRequests = requests[priority];
				return (nodeRequests == null) ? null : nodeRequests[resourceName];
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResource(Priority priority
			)
		{
			lock (this)
			{
				ResourceRequest request = GetResourceRequest(priority, ResourceRequest.Any);
				return (request == null) ? null : request.GetCapability();
			}
		}

		public virtual bool IsBlacklisted(string resourceName)
		{
			lock (this)
			{
				return blacklist.Contains(resourceName);
			}
		}

		/// <summary>
		/// Resources have been allocated to this application by the resource
		/// scheduler.
		/// </summary>
		/// <remarks>
		/// Resources have been allocated to this application by the resource
		/// scheduler. Track them.
		/// </remarks>
		/// <param name="type">the type of the node</param>
		/// <param name="node">the nodeinfo of the node</param>
		/// <param name="priority">the priority of the request.</param>
		/// <param name="request">the request</param>
		/// <param name="container">the containers allocated.</param>
		public virtual IList<ResourceRequest> Allocate(NodeType type, SchedulerNode node, 
			Priority priority, ResourceRequest request, Container container)
		{
			lock (this)
			{
				IList<ResourceRequest> resourceRequests = new AList<ResourceRequest>();
				if (type == NodeType.NodeLocal)
				{
					AllocateNodeLocal(node, priority, request, container, resourceRequests);
				}
				else
				{
					if (type == NodeType.RackLocal)
					{
						AllocateRackLocal(node, priority, request, container, resourceRequests);
					}
					else
					{
						AllocateOffSwitch(node, priority, request, container, resourceRequests);
					}
				}
				QueueMetrics metrics = queue.GetMetrics();
				if (pending)
				{
					// once an allocation is done we assume the application is
					// running from scheduler's POV.
					pending = false;
					metrics.RunAppAttempt(applicationId, user);
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("allocate: applicationId=" + applicationId + " container=" + container.
						GetId() + " host=" + container.GetNodeId().ToString() + " user=" + user + " resource="
						 + request.GetCapability());
				}
				metrics.AllocateResources(user, 1, request.GetCapability(), true);
				return resourceRequests;
			}
		}

		/// <summary>
		/// The
		/// <see cref="ResourceScheduler"/>
		/// is allocating data-local resources to the
		/// application.
		/// </summary>
		/// <param name="allocatedContainers">resources allocated to the application</param>
		private void AllocateNodeLocal(SchedulerNode node, Priority priority, ResourceRequest
			 nodeLocalRequest, Container container, IList<ResourceRequest> resourceRequests)
		{
			lock (this)
			{
				// Update future requirements
				DecResourceRequest(node.GetNodeName(), priority, nodeLocalRequest);
				ResourceRequest rackLocalRequest = requests[priority][node.GetRackName()];
				DecResourceRequest(node.GetRackName(), priority, rackLocalRequest);
				ResourceRequest offRackRequest = requests[priority][ResourceRequest.Any];
				DecrementOutstanding(offRackRequest);
				// Update cloned NodeLocal, RackLocal and OffRack requests for recovery
				resourceRequests.AddItem(CloneResourceRequest(nodeLocalRequest));
				resourceRequests.AddItem(CloneResourceRequest(rackLocalRequest));
				resourceRequests.AddItem(CloneResourceRequest(offRackRequest));
			}
		}

		private void DecResourceRequest(string resourceName, Priority priority, ResourceRequest
			 request)
		{
			request.SetNumContainers(request.GetNumContainers() - 1);
			if (request.GetNumContainers() == 0)
			{
				Sharpen.Collections.Remove(requests[priority], resourceName);
			}
		}

		/// <summary>
		/// The
		/// <see cref="ResourceScheduler"/>
		/// is allocating data-local resources to the
		/// application.
		/// </summary>
		/// <param name="allocatedContainers">resources allocated to the application</param>
		private void AllocateRackLocal(SchedulerNode node, Priority priority, ResourceRequest
			 rackLocalRequest, Container container, IList<ResourceRequest> resourceRequests)
		{
			lock (this)
			{
				// Update future requirements
				DecResourceRequest(node.GetRackName(), priority, rackLocalRequest);
				ResourceRequest offRackRequest = requests[priority][ResourceRequest.Any];
				DecrementOutstanding(offRackRequest);
				// Update cloned RackLocal and OffRack requests for recovery
				resourceRequests.AddItem(CloneResourceRequest(rackLocalRequest));
				resourceRequests.AddItem(CloneResourceRequest(offRackRequest));
			}
		}

		/// <summary>
		/// The
		/// <see cref="ResourceScheduler"/>
		/// is allocating data-local resources to the
		/// application.
		/// </summary>
		/// <param name="allocatedContainers">resources allocated to the application</param>
		private void AllocateOffSwitch(SchedulerNode node, Priority priority, ResourceRequest
			 offSwitchRequest, Container container, IList<ResourceRequest> resourceRequests)
		{
			lock (this)
			{
				// Update future requirements
				DecrementOutstanding(offSwitchRequest);
				// Update cloned OffRack requests for recovery
				resourceRequests.AddItem(CloneResourceRequest(offSwitchRequest));
			}
		}

		private void DecrementOutstanding(ResourceRequest offSwitchRequest)
		{
			lock (this)
			{
				int numOffSwitchContainers = offSwitchRequest.GetNumContainers() - 1;
				// Do not remove ANY
				offSwitchRequest.SetNumContainers(numOffSwitchContainers);
				// Do we have any outstanding requests?
				// If there is nothing, we need to deactivate this application
				if (numOffSwitchContainers == 0)
				{
					CheckForDeactivation();
				}
			}
		}

		private void CheckForDeactivation()
		{
			lock (this)
			{
				bool deactivate = true;
				foreach (Priority priority in GetPriorities())
				{
					ResourceRequest request = GetResourceRequest(priority, ResourceRequest.Any);
					if (request != null)
					{
						if (request.GetNumContainers() > 0)
						{
							deactivate = false;
							break;
						}
					}
				}
				if (deactivate)
				{
					activeUsersManager.DeactivateApplication(user, applicationId);
				}
			}
		}

		public virtual void Move(Queue newQueue)
		{
			lock (this)
			{
				QueueMetrics oldMetrics = queue.GetMetrics();
				QueueMetrics newMetrics = newQueue.GetMetrics();
				foreach (IDictionary<string, ResourceRequest> asks in requests.Values)
				{
					ResourceRequest request = asks[ResourceRequest.Any];
					if (request != null)
					{
						oldMetrics.DecrPendingResources(user, request.GetNumContainers(), request.GetCapability
							());
						newMetrics.IncrPendingResources(user, request.GetNumContainers(), request.GetCapability
							());
					}
				}
				oldMetrics.MoveAppFrom(this);
				newMetrics.MoveAppTo(this);
				activeUsersManager.DeactivateApplication(user, applicationId);
				activeUsersManager = newQueue.GetActiveUsersManager();
				activeUsersManager.ActivateApplication(user, applicationId);
				this.queue = newQueue;
				this.queueName = newQueue.GetQueueName();
			}
		}

		public virtual void Stop(RMAppAttemptState rmAppAttemptFinalState)
		{
			lock (this)
			{
				// clear pending resources metrics for the application
				QueueMetrics metrics = queue.GetMetrics();
				foreach (IDictionary<string, ResourceRequest> asks in requests.Values)
				{
					ResourceRequest request = asks[ResourceRequest.Any];
					if (request != null)
					{
						metrics.DecrPendingResources(user, request.GetNumContainers(), request.GetCapability
							());
					}
				}
				metrics.FinishAppAttempt(applicationId, pending, user);
				// Clear requests themselves
				ClearRequests();
			}
		}

		public virtual void SetQueue(Queue queue)
		{
			lock (this)
			{
				this.queue = queue;
			}
		}

		public virtual ICollection<string> GetBlackList()
		{
			lock (this)
			{
				return this.blacklist;
			}
		}

		public virtual ICollection<string> GetBlackListCopy()
		{
			lock (this)
			{
				return new HashSet<string>(this.blacklist);
			}
		}

		public virtual void TransferStateFromPreviousAppSchedulingInfo(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.AppSchedulingInfo
			 appInfo)
		{
			lock (this)
			{
				//    this.priorities = appInfo.getPriorities();
				//    this.requests = appInfo.getRequests();
				this.blacklist = appInfo.GetBlackList();
			}
		}

		public virtual void RecoverContainer(RMContainer rmContainer)
		{
			lock (this)
			{
				QueueMetrics metrics = queue.GetMetrics();
				if (pending)
				{
					// If there was any container to recover, the application was
					// running from scheduler's POV.
					pending = false;
					metrics.RunAppAttempt(applicationId, user);
				}
				// Container is completed. Skip recovering resources.
				if (rmContainer.GetState().Equals(RMContainerState.Completed))
				{
					return;
				}
				metrics.AllocateResources(user, 1, rmContainer.GetAllocatedResource(), false);
			}
		}

		public virtual ResourceRequest CloneResourceRequest(ResourceRequest request)
		{
			ResourceRequest newRequest = ResourceRequest.NewInstance(request.GetPriority(), request
				.GetResourceName(), request.GetCapability(), 1, request.GetRelaxLocality());
			return newRequest;
		}
	}
}
