using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo
{
	public class FifoScheduler : AbstractYarnScheduler<FiCaSchedulerApp, FiCaSchedulerNode
		>, Configurable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo.FifoScheduler
			));

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		internal Configuration conf;

		private bool usePortForNodeName;

		private ActiveUsersManager activeUsersManager;

		private const string DefaultQueueName = "default";

		private QueueMetrics metrics;

		private readonly ResourceCalculator resourceCalculator = new DefaultResourceCalculator
			();

		private sealed class _Queue_124 : Queue
		{
			public _Queue_124(FifoScheduler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public string GetQueueName()
			{
				return Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo.FifoScheduler
					.DefaultQueueName;
			}

			public QueueMetrics GetMetrics()
			{
				return this._enclosing.metrics;
			}

			public QueueInfo GetQueueInfo(bool includeChildQueues, bool recursive)
			{
				QueueInfo queueInfo = Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo.FifoScheduler
					.recordFactory.NewRecordInstance<QueueInfo>();
				queueInfo.SetQueueName(this._enclosing.DefaultQueue.GetQueueName());
				queueInfo.SetCapacity(1.0f);
				if (this._enclosing.clusterResource.GetMemory() == 0)
				{
					queueInfo.SetCurrentCapacity(0.0f);
				}
				else
				{
					queueInfo.SetCurrentCapacity((float)this._enclosing.usedResource.GetMemory() / this
						._enclosing.clusterResource.GetMemory());
				}
				queueInfo.SetMaximumCapacity(1.0f);
				queueInfo.SetChildQueues(new AList<QueueInfo>());
				queueInfo.SetQueueState(QueueState.Running);
				return queueInfo;
			}

			public IDictionary<QueueACL, AccessControlList> GetQueueAcls()
			{
				IDictionary<QueueACL, AccessControlList> acls = new Dictionary<QueueACL, AccessControlList
					>();
				foreach (QueueACL acl in QueueACL.Values())
				{
					acls[acl] = new AccessControlList("*");
				}
				return acls;
			}

			public IList<QueueUserACLInfo> GetQueueUserAclInfo(UserGroupInformation unused)
			{
				QueueUserACLInfo queueUserAclInfo = Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo.FifoScheduler
					.recordFactory.NewRecordInstance<QueueUserACLInfo>();
				queueUserAclInfo.SetQueueName(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo.FifoScheduler
					.DefaultQueueName);
				queueUserAclInfo.SetUserAcls(Arrays.AsList(QueueACL.Values()));
				return Sharpen.Collections.SingletonList(queueUserAclInfo);
			}

			public bool HasAccess(QueueACL acl, UserGroupInformation user)
			{
				return this.GetQueueAcls()[acl].IsUserAllowed(user);
			}

			public ActiveUsersManager GetActiveUsersManager()
			{
				return this._enclosing.activeUsersManager;
			}

			public void RecoverContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
				, SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer)
			{
				if (rmContainer.GetState().Equals(RMContainerState.Completed))
				{
					return;
				}
				this._enclosing.IncreaseUsedResources(rmContainer);
				this._enclosing.UpdateAppHeadRoom(schedulerAttempt);
				this._enclosing.UpdateAvailableResourcesMetrics();
			}

			public ICollection<string> GetAccessibleNodeLabels()
			{
				// TODO add implementation for FIFO scheduler
				return null;
			}

			public string GetDefaultNodeLabelExpression()
			{
				// TODO add implementation for FIFO scheduler
				return null;
			}

			private readonly FifoScheduler _enclosing;
		}

		private readonly Queue DefaultQueue;

		public FifoScheduler()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo.FifoScheduler
				).FullName)
		{
			DefaultQueue = new _Queue_124(this);
		}

		private void InitScheduler(Configuration conf)
		{
			lock (this)
			{
				ValidateConf(conf);
				//Use ConcurrentSkipListMap because applications need to be ordered
				this.applications = new ConcurrentSkipListMap<ApplicationId, SchedulerApplication
					<FiCaSchedulerApp>>();
				this.minimumAllocation = Resources.CreateResource(conf.GetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb
					, YarnConfiguration.DefaultRmSchedulerMinimumAllocationMb));
				InitMaximumResourceCapability(Resources.CreateResource(conf.GetInt(YarnConfiguration
					.RmSchedulerMaximumAllocationMb, YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb
					), conf.GetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores, YarnConfiguration
					.DefaultRmSchedulerMaximumAllocationVcores)));
				this.usePortForNodeName = conf.GetBoolean(YarnConfiguration.RmSchedulerIncludePortInNodeName
					, YarnConfiguration.DefaultRmSchedulerUsePortForNodeName);
				this.metrics = QueueMetrics.ForQueue(DefaultQueueName, null, false, conf);
				this.activeUsersManager = new ActiveUsersManager(metrics);
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
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			base.ServiceStop();
		}

		public virtual void SetConf(Configuration conf)
		{
			lock (this)
			{
				this.conf = conf;
			}
		}

		private void ValidateConf(Configuration conf)
		{
			// validate scheduler memory allocation setting
			int minMem = conf.GetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationMb);
			int maxMem = conf.GetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMaximumAllocationMb);
			if (minMem <= 0 || minMem > maxMem)
			{
				throw new YarnRuntimeException("Invalid resource scheduler memory" + " allocation configuration"
					 + ", " + YarnConfiguration.RmSchedulerMinimumAllocationMb + "=" + minMem + ", "
					 + YarnConfiguration.RmSchedulerMaximumAllocationMb + "=" + maxMem + ", min and max should be greater than 0"
					 + ", max should be no smaller than min.");
			}
		}

		public virtual Configuration GetConf()
		{
			lock (this)
			{
				return conf;
			}
		}

		public override int GetNumClusterNodes()
		{
			return nodes.Count;
		}

		public override void SetRMContext(RMContext rmContext)
		{
			lock (this)
			{
				this.rmContext = rmContext;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Reinitialize(Configuration conf, RMContext rmContext)
		{
			lock (this)
			{
				SetConf(conf);
			}
		}

		public override Allocation Allocate(ApplicationAttemptId applicationAttemptId, IList
			<ResourceRequest> ask, IList<ContainerId> release, IList<string> blacklistAdditions
			, IList<string> blacklistRemovals)
		{
			FiCaSchedulerApp application = GetApplicationAttempt(applicationAttemptId);
			if (application == null)
			{
				Log.Error("Calling allocate on removed " + "or non existant application " + applicationAttemptId
					);
				return EmptyAllocation;
			}
			// Sanity check
			SchedulerUtils.NormalizeRequests(ask, resourceCalculator, clusterResource, minimumAllocation
				, GetMaximumResourceCapability());
			// Release containers
			ReleaseContainers(release, application);
			lock (application)
			{
				// make sure we aren't stopping/removing the application
				// when the allocate comes in
				if (application.IsStopped())
				{
					Log.Info("Calling allocate on a stopped " + "application " + applicationAttemptId
						);
					return EmptyAllocation;
				}
				if (!ask.IsEmpty())
				{
					Log.Debug("allocate: pre-update" + " applicationId=" + applicationAttemptId + " application="
						 + application);
					application.ShowRequests();
					// Update application requests
					application.UpdateResourceRequests(ask);
					Log.Debug("allocate: post-update" + " applicationId=" + applicationAttemptId + " application="
						 + application);
					application.ShowRequests();
					Log.Debug("allocate:" + " applicationId=" + applicationAttemptId + " #ask=" + ask
						.Count);
				}
				application.UpdateBlacklist(blacklistAdditions, blacklistRemovals);
				SchedulerApplicationAttempt.ContainersAndNMTokensAllocation allocation = application
					.PullNewlyAllocatedContainersAndNMTokens();
				Org.Apache.Hadoop.Yarn.Api.Records.Resource headroom = application.GetHeadroom();
				application.SetApplicationHeadroomForMetrics(headroom);
				return new Allocation(allocation.GetContainerList(), headroom, null, null, null, 
					allocation.GetNMTokenList());
			}
		}

		private FiCaSchedulerNode GetNode(NodeId nodeId)
		{
			return nodes[nodeId];
		}

		[VisibleForTesting]
		public virtual void AddApplication(ApplicationId applicationId, string queue, string
			 user, bool isAppRecovering)
		{
			lock (this)
			{
				SchedulerApplication<FiCaSchedulerApp> application = new SchedulerApplication<FiCaSchedulerApp
					>(DefaultQueue, user);
				applications[applicationId] = application;
				metrics.SubmitApp(user);
				Log.Info("Accepted application " + applicationId + " from user: " + user + ", currently num of applications: "
					 + applications.Count);
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

		[VisibleForTesting]
		public virtual void AddApplicationAttempt(ApplicationAttemptId appAttemptId, bool
			 transferStateFromPreviousAttempt, bool isAttemptRecovering)
		{
			lock (this)
			{
				SchedulerApplication<FiCaSchedulerApp> application = applications[appAttemptId.GetApplicationId
					()];
				string user = application.GetUser();
				// TODO: Fix store
				FiCaSchedulerApp schedulerApp = new FiCaSchedulerApp(appAttemptId, user, DefaultQueue
					, activeUsersManager, this.rmContext);
				if (transferStateFromPreviousAttempt)
				{
					schedulerApp.TransferStateFromPreviousAttempt(application.GetCurrentAppAttempt());
				}
				application.SetCurrentAppAttempt(schedulerApp);
				metrics.SubmitAppAttempt(user);
				Log.Info("Added Application Attempt " + appAttemptId + " to scheduler from user "
					 + application.GetUser());
				if (isAttemptRecovering)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(appAttemptId + " is recovering. Skipping notifying ATTEMPT_ADDED");
					}
				}
				else
				{
					rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppAttemptEvent(appAttemptId
						, RMAppAttemptEventType.AttemptAdded));
				}
			}
		}

		private void DoneApplication(ApplicationId applicationId, RMAppState finalState)
		{
			lock (this)
			{
				SchedulerApplication<FiCaSchedulerApp> application = applications[applicationId];
				if (application == null)
				{
					Log.Warn("Couldn't find application " + applicationId);
					return;
				}
				// Inform the activeUsersManager
				activeUsersManager.DeactivateApplication(application.GetUser(), applicationId);
				application.Stop(finalState);
				Sharpen.Collections.Remove(applications, applicationId);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoneApplicationAttempt(ApplicationAttemptId applicationAttemptId, RMAppAttemptState
			 rmAppAttemptFinalState, bool keepContainers)
		{
			lock (this)
			{
				FiCaSchedulerApp attempt = GetApplicationAttempt(applicationAttemptId);
				SchedulerApplication<FiCaSchedulerApp> application = applications[applicationAttemptId
					.GetApplicationId()];
				if (application == null || attempt == null)
				{
					throw new IOException("Unknown application " + applicationAttemptId + " has completed!"
						);
				}
				// Kill all 'live' containers
				foreach (RMContainer container in attempt.GetLiveContainers())
				{
					if (keepContainers && container.GetState().Equals(RMContainerState.Running))
					{
						// do not kill the running container in the case of work-preserving AM
						// restart.
						Log.Info("Skip killing " + container.GetContainerId());
						continue;
					}
					CompletedContainer(container, SchedulerUtils.CreateAbnormalContainerStatus(container
						.GetContainerId(), SchedulerUtils.CompletedApplication), RMContainerEventType.Kill
						);
				}
				// Clean up pending requests, metrics etc.
				attempt.Stop(rmAppAttemptFinalState);
			}
		}

		/// <summary>Heart of the scheduler...</summary>
		/// <param name="node">node on which resources are available to be allocated</param>
		private void AssignContainers(FiCaSchedulerNode node)
		{
			Log.Debug("assignContainers:" + " node=" + node.GetRMNode().GetNodeAddress() + " #applications="
				 + applications.Count);
			// Try to assign containers to applications in fifo order
			foreach (KeyValuePair<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> e in 
				applications)
			{
				FiCaSchedulerApp application = e.Value.GetCurrentAppAttempt();
				if (application == null)
				{
					continue;
				}
				Log.Debug("pre-assignContainers");
				application.ShowRequests();
				lock (application)
				{
					// Check if this resource is on the blacklist
					if (SchedulerAppUtils.IsBlacklisted(application, node, Log))
					{
						continue;
					}
					foreach (Priority priority in application.GetPriorities())
					{
						int maxContainers = GetMaxAllocatableContainers(application, priority, node, NodeType
							.OffSwitch);
						// Ensure the application needs containers of this priority
						if (maxContainers > 0)
						{
							int assignedContainers = AssignContainersOnNode(node, application, priority);
							// Do not assign out of order w.r.t priorities
							if (assignedContainers == 0)
							{
								break;
							}
						}
					}
				}
				Log.Debug("post-assignContainers");
				application.ShowRequests();
				// Done
				if (Resources.LessThan(resourceCalculator, clusterResource, node.GetAvailableResource
					(), minimumAllocation))
				{
					break;
				}
			}
			// Update the applications' headroom to correctly take into
			// account the containers assigned in this update.
			foreach (SchedulerApplication<FiCaSchedulerApp> application_1 in applications.Values)
			{
				FiCaSchedulerApp attempt = (FiCaSchedulerApp)application_1.GetCurrentAppAttempt();
				if (attempt == null)
				{
					continue;
				}
				UpdateAppHeadRoom(attempt);
			}
		}

		private int GetMaxAllocatableContainers(FiCaSchedulerApp application, Priority priority
			, FiCaSchedulerNode node, NodeType type)
		{
			int maxContainers = 0;
			ResourceRequest offSwitchRequest = application.GetResourceRequest(priority, ResourceRequest
				.Any);
			if (offSwitchRequest != null)
			{
				maxContainers = offSwitchRequest.GetNumContainers();
			}
			if (type == NodeType.OffSwitch)
			{
				return maxContainers;
			}
			if (type == NodeType.RackLocal)
			{
				ResourceRequest rackLocalRequest = application.GetResourceRequest(priority, node.
					GetRMNode().GetRackName());
				if (rackLocalRequest == null)
				{
					return maxContainers;
				}
				maxContainers = Math.Min(maxContainers, rackLocalRequest.GetNumContainers());
			}
			if (type == NodeType.NodeLocal)
			{
				ResourceRequest nodeLocalRequest = application.GetResourceRequest(priority, node.
					GetRMNode().GetNodeAddress());
				if (nodeLocalRequest != null)
				{
					maxContainers = Math.Min(maxContainers, nodeLocalRequest.GetNumContainers());
				}
			}
			return maxContainers;
		}

		private int AssignContainersOnNode(FiCaSchedulerNode node, FiCaSchedulerApp application
			, Priority priority)
		{
			// Data-local
			int nodeLocalContainers = AssignNodeLocalContainers(node, application, priority);
			// Rack-local
			int rackLocalContainers = AssignRackLocalContainers(node, application, priority);
			// Off-switch
			int offSwitchContainers = AssignOffSwitchContainers(node, application, priority);
			Log.Debug("assignContainersOnNode:" + " node=" + node.GetRMNode().GetNodeAddress(
				) + " application=" + application.GetApplicationId().GetId() + " priority=" + priority
				.GetPriority() + " #assigned=" + (nodeLocalContainers + rackLocalContainers + offSwitchContainers
				));
			return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
		}

		private int AssignNodeLocalContainers(FiCaSchedulerNode node, FiCaSchedulerApp application
			, Priority priority)
		{
			int assignedContainers = 0;
			ResourceRequest request = application.GetResourceRequest(priority, node.GetNodeName
				());
			if (request != null)
			{
				// Don't allocate on this node if we don't need containers on this rack
				ResourceRequest rackRequest = application.GetResourceRequest(priority, node.GetRMNode
					().GetRackName());
				if (rackRequest == null || rackRequest.GetNumContainers() <= 0)
				{
					return 0;
				}
				int assignableContainers = Math.Min(GetMaxAllocatableContainers(application, priority
					, node, NodeType.NodeLocal), request.GetNumContainers());
				assignedContainers = AssignContainer(node, application, priority, assignableContainers
					, request, NodeType.NodeLocal);
			}
			return assignedContainers;
		}

		private int AssignRackLocalContainers(FiCaSchedulerNode node, FiCaSchedulerApp application
			, Priority priority)
		{
			int assignedContainers = 0;
			ResourceRequest request = application.GetResourceRequest(priority, node.GetRMNode
				().GetRackName());
			if (request != null)
			{
				// Don't allocate on this rack if the application doens't need containers
				ResourceRequest offSwitchRequest = application.GetResourceRequest(priority, ResourceRequest
					.Any);
				if (offSwitchRequest.GetNumContainers() <= 0)
				{
					return 0;
				}
				int assignableContainers = Math.Min(GetMaxAllocatableContainers(application, priority
					, node, NodeType.RackLocal), request.GetNumContainers());
				assignedContainers = AssignContainer(node, application, priority, assignableContainers
					, request, NodeType.RackLocal);
			}
			return assignedContainers;
		}

		private int AssignOffSwitchContainers(FiCaSchedulerNode node, FiCaSchedulerApp application
			, Priority priority)
		{
			int assignedContainers = 0;
			ResourceRequest request = application.GetResourceRequest(priority, ResourceRequest
				.Any);
			if (request != null)
			{
				assignedContainers = AssignContainer(node, application, priority, request.GetNumContainers
					(), request, NodeType.OffSwitch);
			}
			return assignedContainers;
		}

		private int AssignContainer(FiCaSchedulerNode node, FiCaSchedulerApp application, 
			Priority priority, int assignableContainers, ResourceRequest request, NodeType type
			)
		{
			Log.Debug("assignContainers:" + " node=" + node.GetRMNode().GetNodeAddress() + " application="
				 + application.GetApplicationId().GetId() + " priority=" + priority.GetPriority(
				) + " assignableContainers=" + assignableContainers + " request=" + request + " type="
				 + type);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability = request.GetCapability();
			int availableContainers = node.GetAvailableResource().GetMemory() / capability.GetMemory
				();
			// TODO: A buggy
			// application
			// with this
			// zero would
			// crash the
			// scheduler.
			int assignedContainers = Math.Min(assignableContainers, availableContainers);
			if (assignedContainers > 0)
			{
				for (int i = 0; i < assignedContainers; ++i)
				{
					NodeId nodeId = node.GetRMNode().GetNodeID();
					ContainerId containerId = BuilderUtils.NewContainerId(application.GetApplicationAttemptId
						(), application.GetNewContainerId());
					// Create the container
					Container container = BuilderUtils.NewContainer(containerId, nodeId, node.GetRMNode
						().GetHttpAddress(), capability, priority, null);
					// Allocate!
					// Inform the application
					RMContainer rmContainer = application.Allocate(type, node, priority, request, container
						);
					// Inform the node
					node.AllocateContainer(rmContainer);
					// Update usage for this container
					IncreaseUsedResources(rmContainer);
				}
			}
			return assignedContainers;
		}

		private void NodeUpdate(RMNode rmNode)
		{
			lock (this)
			{
				FiCaSchedulerNode node = GetNode(rmNode.GetNodeID());
				IList<UpdatedContainerInfo> containerInfoList = rmNode.PullContainerUpdates();
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
				if (rmContext.IsWorkPreservingRecoveryEnabled() && !rmContext.IsSchedulerReadyForAllocatingContainers
					())
				{
					return;
				}
				if (Resources.GreaterThanOrEqual(resourceCalculator, clusterResource, node.GetAvailableResource
					(), minimumAllocation))
				{
					Log.Debug("Node heartbeat " + rmNode.GetNodeID() + " available resource = " + node
						.GetAvailableResource());
					AssignContainers(node);
					Log.Debug("Node after allocation " + rmNode.GetNodeID() + " resource = " + node.GetAvailableResource
						());
				}
				UpdateAvailableResourcesMetrics();
			}
		}

		private void IncreaseUsedResources(RMContainer rmContainer)
		{
			Resources.AddTo(usedResource, rmContainer.GetAllocatedResource());
		}

		private void UpdateAppHeadRoom(SchedulerApplicationAttempt schedulerAttempt)
		{
			schedulerAttempt.SetHeadroom(Resources.Subtract(clusterResource, usedResource));
		}

		private void UpdateAvailableResourcesMetrics()
		{
			metrics.SetAvailableResourcesToQueue(Resources.Subtract(clusterResource, usedResource
				));
		}

		public override void Handle(SchedulerEvent @event)
		{
			switch (@event.GetType())
			{
				case SchedulerEventType.NodeAdded:
				{
					NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)@event;
					AddNode(nodeAddedEvent.GetAddedRMNode());
					RecoverContainersOnNode(nodeAddedEvent.GetContainerReports(), nodeAddedEvent.GetAddedRMNode
						());
					break;
				}

				case SchedulerEventType.NodeRemoved:
				{
					NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)@event;
					RemoveNode(nodeRemovedEvent.GetRemovedRMNode());
					break;
				}

				case SchedulerEventType.NodeResourceUpdate:
				{
					NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent = (NodeResourceUpdateSchedulerEvent
						)@event;
					UpdateNodeResource(nodeResourceUpdatedEvent.GetRMNode(), nodeResourceUpdatedEvent
						.GetResourceOption());
					break;
				}

				case SchedulerEventType.NodeUpdate:
				{
					NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)@event;
					NodeUpdate(nodeUpdatedEvent.GetRMNode());
					break;
				}

				case SchedulerEventType.AppAdded:
				{
					AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent)@event;
					AddApplication(appAddedEvent.GetApplicationId(), appAddedEvent.GetQueue(), appAddedEvent
						.GetUser(), appAddedEvent.GetIsAppRecovering());
					break;
				}

				case SchedulerEventType.AppRemoved:
				{
					AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)@event;
					DoneApplication(appRemovedEvent.GetApplicationID(), appRemovedEvent.GetFinalState
						());
					break;
				}

				case SchedulerEventType.AppAttemptAdded:
				{
					AppAttemptAddedSchedulerEvent appAttemptAddedEvent = (AppAttemptAddedSchedulerEvent
						)@event;
					AddApplicationAttempt(appAttemptAddedEvent.GetApplicationAttemptId(), appAttemptAddedEvent
						.GetTransferStateFromPreviousAttempt(), appAttemptAddedEvent.GetIsAttemptRecovering
						());
					break;
				}

				case SchedulerEventType.AppAttemptRemoved:
				{
					AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent = (AppAttemptRemovedSchedulerEvent
						)@event;
					try
					{
						DoneApplicationAttempt(appAttemptRemovedEvent.GetApplicationAttemptID(), appAttemptRemovedEvent
							.GetFinalAttemptState(), appAttemptRemovedEvent.GetKeepContainersAcrossAppAttempts
							());
					}
					catch (IOException ie)
					{
						Log.Error("Unable to remove application " + appAttemptRemovedEvent.GetApplicationAttemptID
							(), ie);
					}
					break;
				}

				case SchedulerEventType.ContainerExpired:
				{
					ContainerExpiredSchedulerEvent containerExpiredEvent = (ContainerExpiredSchedulerEvent
						)@event;
					ContainerId containerid = containerExpiredEvent.GetContainerId();
					CompletedContainer(GetRMContainer(containerid), SchedulerUtils.CreateAbnormalContainerStatus
						(containerid, SchedulerUtils.ExpiredContainer), RMContainerEventType.Expire);
					break;
				}

				case SchedulerEventType.ContainerRescheduled:
				{
					ContainerRescheduledEvent containerRescheduledEvent = (ContainerRescheduledEvent)
						@event;
					RMContainer container = containerRescheduledEvent.GetContainer();
					RecoverResourceRequestForContainer(container);
					break;
				}

				default:
				{
					Log.Error("Invalid eventtype " + @event.GetType() + ". Ignoring!");
					break;
				}
			}
		}

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
				// Get the application for the finished container
				Container container = rmContainer.GetContainer();
				FiCaSchedulerApp application = GetCurrentAttemptForContainer(container.GetId());
				ApplicationId appId = container.GetId().GetApplicationAttemptId().GetApplicationId
					();
				// Get the node on which the container was allocated
				FiCaSchedulerNode node = GetNode(container.GetNodeId());
				if (application == null)
				{
					Log.Info("Unknown application: " + appId + " released container " + container.GetId
						() + " on node: " + node + " with event: " + @event);
					return;
				}
				// Inform the application
				application.ContainerCompleted(rmContainer, containerStatus, @event);
				// Inform the node
				node.ReleaseContainer(container);
				// Update total usage
				Resources.SubtractFrom(usedResource, container.GetResource());
				Log.Info("Application attempt " + application.GetApplicationAttemptId() + " released container "
					 + container.GetId() + " on node: " + node + " with event: " + @event);
			}
		}

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource usedResource = recordFactory.
			NewRecordInstance<Org.Apache.Hadoop.Yarn.Api.Records.Resource>();

		private void RemoveNode(RMNode nodeInfo)
		{
			lock (this)
			{
				FiCaSchedulerNode node = GetNode(nodeInfo.GetNodeID());
				if (node == null)
				{
					return;
				}
				// Kill running containers
				foreach (RMContainer container in node.GetRunningContainers())
				{
					CompletedContainer(container, SchedulerUtils.CreateAbnormalContainerStatus(container
						.GetContainerId(), SchedulerUtils.LostContainer), RMContainerEventType.Kill);
				}
				//Remove the node
				Sharpen.Collections.Remove(this.nodes, nodeInfo.GetNodeID());
				UpdateMaximumAllocation(node, false);
				// Update cluster metrics
				Resources.SubtractFrom(clusterResource, node.GetTotalResource());
			}
		}

		public override QueueInfo GetQueueInfo(string queueName, bool includeChildQueues, 
			bool recursive)
		{
			return DefaultQueue.GetQueueInfo(false, false);
		}

		public override IList<QueueUserACLInfo> GetQueueUserAclInfo()
		{
			return DefaultQueue.GetQueueUserAclInfo(null);
		}

		public override ResourceCalculator GetResourceCalculator()
		{
			return resourceCalculator;
		}

		private void AddNode(RMNode nodeManager)
		{
			lock (this)
			{
				FiCaSchedulerNode schedulerNode = new FiCaSchedulerNode(nodeManager, usePortForNodeName
					);
				this.nodes[nodeManager.GetNodeID()] = schedulerNode;
				Resources.AddTo(clusterResource, schedulerNode.GetTotalResource());
				UpdateMaximumAllocation(schedulerNode, true);
			}
		}

		public override void Recover(RMStateStore.RMState state)
		{
		}

		// NOT IMPLEMENTED
		public override RMContainer GetRMContainer(ContainerId containerId)
		{
			FiCaSchedulerApp attempt = GetCurrentAttemptForContainer(containerId);
			return (attempt == null) ? null : attempt.GetRMContainer(containerId);
		}

		public override QueueMetrics GetRootQueueMetrics()
		{
			return DefaultQueue.GetMetrics();
		}

		public override bool CheckAccess(UserGroupInformation callerUGI, QueueACL acl, string
			 queueName)
		{
			lock (this)
			{
				return DefaultQueue.HasAccess(acl, callerUGI);
			}
		}

		public override IList<ApplicationAttemptId> GetAppsInQueue(string queueName)
		{
			lock (this)
			{
				if (queueName.Equals(DefaultQueue.GetQueueName()))
				{
					IList<ApplicationAttemptId> attempts = new AList<ApplicationAttemptId>(applications
						.Count);
					foreach (SchedulerApplication<FiCaSchedulerApp> app in applications.Values)
					{
						attempts.AddItem(app.GetCurrentAppAttempt().GetApplicationAttemptId());
					}
					return attempts;
				}
				else
				{
					return null;
				}
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetUsedResource()
		{
			return usedResource;
		}
	}
}
