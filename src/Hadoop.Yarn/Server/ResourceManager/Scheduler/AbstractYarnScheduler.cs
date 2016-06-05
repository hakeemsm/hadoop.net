using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	public abstract class AbstractYarnScheduler<T, N> : AbstractService, ResourceScheduler
		where T : SchedulerApplicationAttempt
		where N : SchedulerNode
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.AbstractYarnScheduler
			));

		protected internal IDictionary<NodeId, N> nodes = new ConcurrentHashMap<NodeId, N
			>();

		protected internal Resource clusterResource = Resource.NewInstance(0, 0);

		protected internal Resource minimumAllocation;

		private Resource maximumAllocation;

		private Resource configuredMaximumAllocation;

		private int maxNodeMemory = -1;

		private int maxNodeVCores = -1;

		private readonly ReentrantReadWriteLock.ReadLock maxAllocReadLock;

		private readonly ReentrantReadWriteLock.WriteLock maxAllocWriteLock;

		private bool useConfiguredMaximumAllocationOnly = true;

		private long configuredMaximumAllocationWaitTime;

		protected internal RMContext rmContext;

		protected internal ConcurrentMap<ApplicationId, SchedulerApplication<T>> applications;

		protected internal int nmExpireInterval;

		protected internal static readonly IList<Container> EmptyContainerList = new AList
			<Container>();

		protected internal static readonly Allocation EmptyAllocation = new Allocation(EmptyContainerList
			, Resources.CreateResource(0), null, null, null);

		/// <summary>Construct the service.</summary>
		/// <param name="name">service name</param>
		public AbstractYarnScheduler(string name)
			: base(name)
		{
			// Nodes in the cluster, indexed by NodeId
			// Whole capacity of the cluster
			/*
			* All schedulers which are inheriting AbstractYarnScheduler should use
			* concurrent version of 'applications' map.
			*/
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			this.maxAllocReadLock = Lock.ReadLock();
			this.maxAllocWriteLock = Lock.WriteLock();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			nmExpireInterval = conf.GetInt(YarnConfiguration.RmNmExpiryIntervalMs, YarnConfiguration
				.DefaultRmNmExpiryIntervalMs);
			configuredMaximumAllocationWaitTime = conf.GetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs
				, YarnConfiguration.DefaultRmWorkPreservingRecoverySchedulingWaitMs);
			CreateReleaseCache();
			base.ServiceInit(conf);
		}

		public virtual IList<Container> GetTransferredContainers(ApplicationAttemptId currentAttempt
			)
		{
			ApplicationId appId = currentAttempt.GetApplicationId();
			SchedulerApplication<T> app = applications[appId];
			IList<Container> containerList = new AList<Container>();
			RMApp appImpl = this.rmContext.GetRMApps()[appId];
			if (appImpl.GetApplicationSubmissionContext().GetUnmanagedAM())
			{
				return containerList;
			}
			if (app == null)
			{
				return containerList;
			}
			ICollection<RMContainer> liveContainers = app.GetCurrentAppAttempt().GetLiveContainers
				();
			ContainerId amContainerId = rmContext.GetRMApps()[appId].GetCurrentAppAttempt().GetMasterContainer
				().GetId();
			foreach (RMContainer rmContainer in liveContainers)
			{
				if (!rmContainer.GetContainerId().Equals(amContainerId))
				{
					containerList.AddItem(rmContainer.GetContainer());
				}
			}
			return containerList;
		}

		public virtual IDictionary<ApplicationId, SchedulerApplication<T>> GetSchedulerApplications
			()
		{
			return applications;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetClusterResource()
		{
			return clusterResource;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMinimumResourceCapability
			()
		{
			return minimumAllocation;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaximumResourceCapability
			()
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxResource;
			maxAllocReadLock.Lock();
			try
			{
				if (useConfiguredMaximumAllocationOnly)
				{
					if (Runtime.CurrentTimeMillis() - ResourceManager.GetClusterTimeStamp() > configuredMaximumAllocationWaitTime)
					{
						useConfiguredMaximumAllocationOnly = false;
					}
					maxResource = Resources.Clone(configuredMaximumAllocation);
				}
				else
				{
					maxResource = Resources.Clone(maximumAllocation);
				}
			}
			finally
			{
				maxAllocReadLock.Unlock();
			}
			return maxResource;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaximumResourceCapability
			(string queueName)
		{
			return GetMaximumResourceCapability();
		}

		protected internal virtual void InitMaximumResourceCapability(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumAllocation)
		{
			maxAllocWriteLock.Lock();
			try
			{
				if (this.configuredMaximumAllocation == null)
				{
					this.configuredMaximumAllocation = Resources.Clone(maximumAllocation);
					this.maximumAllocation = Resources.Clone(maximumAllocation);
				}
			}
			finally
			{
				maxAllocWriteLock.Unlock();
			}
		}

		protected internal virtual void ContainerLaunchedOnNode(ContainerId containerId, 
			SchedulerNode node)
		{
			lock (this)
			{
				// Get the application for the finished container
				SchedulerApplicationAttempt application = GetCurrentAttemptForContainer(containerId
					);
				if (application == null)
				{
					Log.Info("Unknown application " + containerId.GetApplicationAttemptId().GetApplicationId
						() + " launched container " + containerId + " on node: " + node);
					this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMNodeCleanContainerEvent
						(node.GetNodeID(), containerId));
					return;
				}
				application.ContainerLaunchedOnNode(containerId, node.GetNodeID());
			}
		}

		public virtual T GetApplicationAttempt(ApplicationAttemptId applicationAttemptId)
		{
			SchedulerApplication<T> app = applications[applicationAttemptId.GetApplicationId(
				)];
			return app == null ? null : app.GetCurrentAppAttempt();
		}

		public virtual SchedulerAppReport GetSchedulerAppInfo(ApplicationAttemptId appAttemptId
			)
		{
			SchedulerApplicationAttempt attempt = GetApplicationAttempt(appAttemptId);
			if (attempt == null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Request for appInfo of unknown attempt " + appAttemptId);
				}
				return null;
			}
			return new SchedulerAppReport(attempt);
		}

		public virtual ApplicationResourceUsageReport GetAppResourceUsageReport(ApplicationAttemptId
			 appAttemptId)
		{
			SchedulerApplicationAttempt attempt = GetApplicationAttempt(appAttemptId);
			if (attempt == null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Request for appInfo of unknown attempt " + appAttemptId);
				}
				return null;
			}
			return attempt.GetResourceUsageReport();
		}

		public virtual T GetCurrentAttemptForContainer(ContainerId containerId)
		{
			return GetApplicationAttempt(containerId.GetApplicationAttemptId());
		}

		public virtual RMContainer GetRMContainer(ContainerId containerId)
		{
			SchedulerApplicationAttempt attempt = GetCurrentAttemptForContainer(containerId);
			return (attempt == null) ? null : attempt.GetRMContainer(containerId);
		}

		public virtual SchedulerNodeReport GetNodeReport(NodeId nodeId)
		{
			N node = nodes[nodeId];
			return node == null ? null : new SchedulerNodeReport(node);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual string MoveApplication(ApplicationId appId, string newQueue)
		{
			throw new YarnException(GetType().Name + " does not support moving apps between queues"
				);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void RemoveQueue(string queueName)
		{
			throw new YarnException(GetType().Name + " does not support removing queues");
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void AddQueue(Queue newQueue)
		{
			throw new YarnException(GetType().Name + " does not support this operation");
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void SetEntitlement(string queue, QueueEntitlement entitlement)
		{
			throw new YarnException(GetType().Name + " does not support this operation");
		}

		private void KillOrphanContainerOnNode(RMNode node, NMContainerStatus container)
		{
			if (!container.GetContainerState().Equals(ContainerState.Complete))
			{
				this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMNodeCleanContainerEvent
					(node.GetNodeID(), container.GetContainerId()));
			}
		}

		public virtual void RecoverContainersOnNode(IList<NMContainerStatus> containerReports
			, RMNode nm)
		{
			lock (this)
			{
				if (!rmContext.IsWorkPreservingRecoveryEnabled() || containerReports == null || (
					containerReports != null && containerReports.IsEmpty()))
				{
					return;
				}
				foreach (NMContainerStatus container in containerReports)
				{
					ApplicationId appId = container.GetContainerId().GetApplicationAttemptId().GetApplicationId
						();
					RMApp rmApp = rmContext.GetRMApps()[appId];
					if (rmApp == null)
					{
						Log.Error("Skip recovering container " + container + " for unknown application.");
						KillOrphanContainerOnNode(nm, container);
						continue;
					}
					// Unmanaged AM recovery is addressed in YARN-1815
					if (rmApp.GetApplicationSubmissionContext().GetUnmanagedAM())
					{
						Log.Info("Skip recovering container " + container + " for unmanaged AM." + rmApp.
							GetApplicationId());
						KillOrphanContainerOnNode(nm, container);
						continue;
					}
					SchedulerApplication<T> schedulerApp = applications[appId];
					if (schedulerApp == null)
					{
						Log.Info("Skip recovering container  " + container + " for unknown SchedulerApplication. Application current state is "
							 + rmApp.GetState());
						KillOrphanContainerOnNode(nm, container);
						continue;
					}
					Log.Info("Recovering container " + container);
					SchedulerApplicationAttempt schedulerAttempt = schedulerApp.GetCurrentAppAttempt(
						);
					if (!rmApp.GetApplicationSubmissionContext().GetKeepContainersAcrossApplicationAttempts
						())
					{
						// Do not recover containers for stopped attempt or previous attempt.
						if (schedulerAttempt.IsStopped() || !schedulerAttempt.GetApplicationAttemptId().Equals
							(container.GetContainerId().GetApplicationAttemptId()))
						{
							Log.Info("Skip recovering container " + container + " for already stopped attempt."
								);
							KillOrphanContainerOnNode(nm, container);
							continue;
						}
					}
					// create container
					RMContainer rmContainer = RecoverAndCreateContainer(container, nm);
					// recover RMContainer
					rmContainer.Handle(new RMContainerRecoverEvent(container.GetContainerId(), container
						));
					// recover scheduler node
					nodes[nm.GetNodeID()].RecoverContainer(rmContainer);
					// recover queue: update headroom etc.
					Queue queue = schedulerAttempt.GetQueue();
					queue.RecoverContainer(clusterResource, schedulerAttempt, rmContainer);
					// recover scheduler attempt
					schedulerAttempt.RecoverContainer(rmContainer);
					// set master container for the current running AMContainer for this
					// attempt.
					RMAppAttempt appAttempt = rmApp.GetCurrentAppAttempt();
					if (appAttempt != null)
					{
						Container masterContainer = appAttempt.GetMasterContainer();
						// Mark current running AMContainer's RMContainer based on the master
						// container ID stored in AppAttempt.
						if (masterContainer != null && masterContainer.GetId().Equals(rmContainer.GetContainerId
							()))
						{
							((RMContainerImpl)rmContainer).SetAMContainer(true);
						}
					}
					lock (schedulerAttempt)
					{
						ICollection<ContainerId> releases = schedulerAttempt.GetPendingRelease();
						if (releases.Contains(container.GetContainerId()))
						{
							// release the container
							rmContainer.Handle(new RMContainerFinishedEvent(container.GetContainerId(), SchedulerUtils
								.CreateAbnormalContainerStatus(container.GetContainerId(), SchedulerUtils.ReleasedContainer
								), RMContainerEventType.Released));
							releases.Remove(container.GetContainerId());
							Log.Info(container.GetContainerId() + " is released by application.");
						}
					}
				}
			}
		}

		private RMContainer RecoverAndCreateContainer(NMContainerStatus status, RMNode node
			)
		{
			Container container = Container.NewInstance(status.GetContainerId(), node.GetNodeID
				(), node.GetHttpAddress(), status.GetAllocatedResource(), status.GetPriority(), 
				null);
			ApplicationAttemptId attemptId = container.GetId().GetApplicationAttemptId();
			RMContainer rmContainer = new RMContainerImpl(container, attemptId, node.GetNodeID
				(), applications[attemptId.GetApplicationId()].GetUser(), rmContext, status.GetCreationTime
				());
			return rmContainer;
		}

		/// <summary>
		/// Recover resource request back from RMContainer when a container is
		/// preempted before AM pulled the same.
		/// </summary>
		/// <remarks>
		/// Recover resource request back from RMContainer when a container is
		/// preempted before AM pulled the same. If container is pulled by
		/// AM, then RMContainer will not have resource request to recover.
		/// </remarks>
		/// <param name="rmContainer"/>
		protected internal virtual void RecoverResourceRequestForContainer(RMContainer rmContainer
			)
		{
			IList<ResourceRequest> requests = rmContainer.GetResourceRequests();
			// If container state is moved to ACQUIRED, request will be empty.
			if (requests == null)
			{
				return;
			}
			// Add resource request back to Scheduler.
			SchedulerApplicationAttempt schedulerAttempt = GetCurrentAttemptForContainer(rmContainer
				.GetContainerId());
			if (schedulerAttempt != null)
			{
				schedulerAttempt.RecoverResourceRequests(requests);
			}
		}

		protected internal virtual void CreateReleaseCache()
		{
			// Cleanup the cache after nm expire interval.
			new Timer().Schedule(new _TimerTask_450(this), nmExpireInterval);
		}

		private sealed class _TimerTask_450 : TimerTask
		{
			public _TimerTask_450(AbstractYarnScheduler<T, N> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				foreach (SchedulerApplication<T> app in this._enclosing.applications.Values)
				{
					T attempt = app.GetCurrentAppAttempt();
					lock (attempt)
					{
						foreach (ContainerId containerId in attempt.GetPendingRelease())
						{
							RMAuditLogger.LogFailure(app.GetUser(), RMAuditLogger.AuditConstants.ReleaseContainer
								, "Unauthorized access or invalid container", "Scheduler", "Trying to release container not owned by app or with invalid id."
								, attempt.GetApplicationId(), containerId);
						}
						attempt.GetPendingRelease().Clear();
					}
				}
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.AbstractYarnScheduler.Log
					.Info("Release request cache is cleaned up");
			}

			private readonly AbstractYarnScheduler<T, N> _enclosing;
		}

		// clean up a completed container
		protected internal abstract void CompletedContainer(RMContainer rmContainer, ContainerStatus
			 containerStatus, RMContainerEventType @event);

		protected internal virtual void ReleaseContainers(IList<ContainerId> containers, 
			SchedulerApplicationAttempt attempt)
		{
			foreach (ContainerId containerId in containers)
			{
				RMContainer rmContainer = GetRMContainer(containerId);
				if (rmContainer == null)
				{
					if (Runtime.CurrentTimeMillis() - ResourceManager.GetClusterTimeStamp() < nmExpireInterval)
					{
						Log.Info(containerId + " doesn't exist. Add the container" + " to the release request cache as it maybe on recovery."
							);
						lock (attempt)
						{
							attempt.GetPendingRelease().AddItem(containerId);
						}
					}
					else
					{
						RMAuditLogger.LogFailure(attempt.GetUser(), RMAuditLogger.AuditConstants.ReleaseContainer
							, "Unauthorized access or invalid container", "Scheduler", "Trying to release container not owned by app or with invalid id."
							, attempt.GetApplicationId(), containerId);
					}
				}
				CompletedContainer(rmContainer, SchedulerUtils.CreateAbnormalContainerStatus(containerId
					, SchedulerUtils.ReleasedContainer), RMContainerEventType.Released);
			}
		}

		public virtual SchedulerNode GetSchedulerNode(NodeId nodeId)
		{
			return nodes[nodeId];
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void MoveAllApps(string sourceQueue, string destQueue)
		{
			lock (this)
			{
				// check if destination queue is a valid leaf queue
				try
				{
					GetQueueInfo(destQueue, false, false);
				}
				catch (IOException e)
				{
					Log.Warn(e);
					throw new YarnException(e);
				}
				// check if source queue is a valid
				IList<ApplicationAttemptId> apps = GetAppsInQueue(sourceQueue);
				if (apps == null)
				{
					string errMsg = "The specified Queue: " + sourceQueue + " doesn't exist";
					Log.Warn(errMsg);
					throw new YarnException(errMsg);
				}
				// generate move events for each pending/running app
				foreach (ApplicationAttemptId app in apps)
				{
					SettableFuture<object> future = SettableFuture.Create();
					this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppMoveEvent(app.GetApplicationId
						(), destQueue, future));
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void KillAllAppsInQueue(string queueName)
		{
			lock (this)
			{
				// check if queue is a valid
				IList<ApplicationAttemptId> apps = GetAppsInQueue(queueName);
				if (apps == null)
				{
					string errMsg = "The specified Queue: " + queueName + " doesn't exist";
					Log.Warn(errMsg);
					throw new YarnException(errMsg);
				}
				// generate kill events for each pending/running app
				foreach (ApplicationAttemptId app in apps)
				{
					this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(app.GetApplicationId
						(), RMAppEventType.Kill, "Application killed due to expiry of reservation queue "
						 + queueName + "."));
				}
			}
		}

		/// <summary>Process resource update on a node.</summary>
		public virtual void UpdateNodeResource(RMNode nm, ResourceOption resourceOption)
		{
			lock (this)
			{
				SchedulerNode node = GetSchedulerNode(nm.GetNodeID());
				Org.Apache.Hadoop.Yarn.Api.Records.Resource newResource = resourceOption.GetResource
					();
				Org.Apache.Hadoop.Yarn.Api.Records.Resource oldResource = node.GetTotalResource();
				if (!oldResource.Equals(newResource))
				{
					// Log resource change
					Log.Info("Update resource on node: " + node.GetNodeName() + " from: " + oldResource
						 + ", to: " + newResource);
					Sharpen.Collections.Remove(nodes, nm.GetNodeID());
					UpdateMaximumAllocation(node, false);
					// update resource to node
					node.SetTotalResource(newResource);
					nodes[nm.GetNodeID()] = (N)node;
					UpdateMaximumAllocation(node, true);
					// update resource to clusterResource
					Resources.SubtractFrom(clusterResource, oldResource);
					Resources.AddTo(clusterResource, newResource);
				}
				else
				{
					// Log resource change
					Log.Warn("Update resource on node: " + node.GetNodeName() + " with the same resource: "
						 + newResource);
				}
			}
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual EnumSet<YarnServiceProtos.SchedulerResourceTypes> GetSchedulingResourceTypes
			()
		{
			return EnumSet.Of(YarnServiceProtos.SchedulerResourceTypes.Memory);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual ICollection<string> GetPlanQueues()
		{
			throw new YarnException(GetType().Name + " does not support reservations");
		}

		protected internal virtual void UpdateMaximumAllocation(SchedulerNode node, bool 
			add)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totalResource = node.GetTotalResource
				();
			maxAllocWriteLock.Lock();
			try
			{
				if (add)
				{
					// added node
					int nodeMemory = totalResource.GetMemory();
					if (nodeMemory > maxNodeMemory)
					{
						maxNodeMemory = nodeMemory;
						maximumAllocation.SetMemory(Math.Min(configuredMaximumAllocation.GetMemory(), maxNodeMemory
							));
					}
					int nodeVCores = totalResource.GetVirtualCores();
					if (nodeVCores > maxNodeVCores)
					{
						maxNodeVCores = nodeVCores;
						maximumAllocation.SetVirtualCores(Math.Min(configuredMaximumAllocation.GetVirtualCores
							(), maxNodeVCores));
					}
				}
				else
				{
					// removed node
					if (maxNodeMemory == totalResource.GetMemory())
					{
						maxNodeMemory = -1;
					}
					if (maxNodeVCores == totalResource.GetVirtualCores())
					{
						maxNodeVCores = -1;
					}
					// We only have to iterate through the nodes if the current max memory
					// or vcores was equal to the removed node's
					if (maxNodeMemory == -1 || maxNodeVCores == -1)
					{
						foreach (KeyValuePair<NodeId, N> nodeEntry in nodes)
						{
							int nodeMemory = nodeEntry.Value.GetTotalResource().GetMemory();
							if (nodeMemory > maxNodeMemory)
							{
								maxNodeMemory = nodeMemory;
							}
							int nodeVCores = nodeEntry.Value.GetTotalResource().GetVirtualCores();
							if (nodeVCores > maxNodeVCores)
							{
								maxNodeVCores = nodeVCores;
							}
						}
						if (maxNodeMemory == -1)
						{
							// no nodes
							maximumAllocation.SetMemory(configuredMaximumAllocation.GetMemory());
						}
						else
						{
							maximumAllocation.SetMemory(Math.Min(configuredMaximumAllocation.GetMemory(), maxNodeMemory
								));
						}
						if (maxNodeVCores == -1)
						{
							// no nodes
							maximumAllocation.SetVirtualCores(configuredMaximumAllocation.GetVirtualCores());
						}
						else
						{
							maximumAllocation.SetVirtualCores(Math.Min(configuredMaximumAllocation.GetVirtualCores
								(), maxNodeVCores));
						}
					}
				}
			}
			finally
			{
				maxAllocWriteLock.Unlock();
			}
		}

		protected internal virtual void RefreshMaximumAllocation(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 newMaxAlloc)
		{
			maxAllocWriteLock.Lock();
			try
			{
				configuredMaximumAllocation = Resources.Clone(newMaxAlloc);
				int maxMemory = newMaxAlloc.GetMemory();
				if (maxNodeMemory != -1)
				{
					maxMemory = Math.Min(maxMemory, maxNodeMemory);
				}
				int maxVcores = newMaxAlloc.GetVirtualCores();
				if (maxNodeVCores != -1)
				{
					maxVcores = Math.Min(maxVcores, maxNodeVCores);
				}
				maximumAllocation = Resources.CreateResource(maxMemory, maxVcores);
			}
			finally
			{
				maxAllocWriteLock.Unlock();
			}
		}

		public virtual IList<ResourceRequest> GetPendingResourceRequestsForAttempt(ApplicationAttemptId
			 attemptId)
		{
			SchedulerApplicationAttempt attempt = GetApplicationAttempt(attemptId);
			if (attempt != null)
			{
				return attempt.GetAppSchedulingInfo().GetAllResourceRequests();
			}
			return null;
		}

		public abstract void Handle(SchedulerEvent arg1);

		public abstract Allocation Allocate(ApplicationAttemptId arg1, IList<ResourceRequest
			> arg2, IList<ContainerId> arg3, IList<string> arg4, IList<string> arg5);

		public abstract bool CheckAccess(UserGroupInformation arg1, QueueACL arg2, string
			 arg3);

		public abstract IList<ApplicationAttemptId> GetAppsInQueue(string arg1);

		public abstract int GetNumClusterNodes();

		public abstract QueueInfo GetQueueInfo(string arg1, bool arg2, bool arg3);

		public abstract IList<QueueUserACLInfo> GetQueueUserAclInfo();

		public abstract ResourceCalculator GetResourceCalculator();

		public abstract QueueMetrics GetRootQueueMetrics();

		public abstract void Recover(RMStateStore.RMState arg1);

		public abstract void Reinitialize(Configuration arg1, RMContext arg2);

		public abstract void SetRMContext(RMContext arg1);
	}
}
