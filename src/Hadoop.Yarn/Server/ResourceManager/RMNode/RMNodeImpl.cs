using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.State;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	/// <summary>
	/// This class is used to keep track of all the applications/containers
	/// running on a node.
	/// </summary>
	public class RMNodeImpl : RMNode, EventHandler<RMNodeEvent>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode.RMNodeImpl
			));

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private readonly ReentrantReadWriteLock.ReadLock readLock;

		private readonly ReentrantReadWriteLock.WriteLock writeLock;

		private readonly ConcurrentLinkedQueue<UpdatedContainerInfo> nodeUpdateQueue;

		private volatile bool nextHeartBeat = true;

		private readonly NodeId nodeId;

		private readonly RMContext context;

		private readonly string hostName;

		private readonly int commandPort;

		private int httpPort;

		private readonly string nodeAddress;

		private string httpAddress;

		private volatile Resource totalCapability;

		private readonly Node node;

		private string healthReport;

		private long lastHealthReportTime;

		private string nodeManagerVersion;

		private readonly ICollection<ContainerId> launchedContainers = new HashSet<ContainerId
			>();

		private readonly ICollection<ContainerId> containersToClean = new TreeSet<ContainerId
			>(new BuilderUtils.ContainerIdComparator());

		private readonly ICollection<ContainerId> containersToBeRemovedFromNM = new HashSet
			<ContainerId>();

		private readonly IList<ApplicationId> finishedApplications = new AList<ApplicationId
			>();

		private NodeHeartbeatResponse latestNodeHeartBeatResponse = recordFactory.NewRecordInstance
			<NodeHeartbeatResponse>();

		private static readonly StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode.RMNodeImpl
			, NodeState, RMNodeEventType, RMNodeEvent> stateMachineFactory = new StateMachineFactory
			<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode.RMNodeImpl, NodeState, RMNodeEventType
			, RMNodeEvent>(NodeState.New).AddTransition(NodeState.New, NodeState.Running, RMNodeEventType
			.Started, new RMNodeImpl.AddNodeTransition()).AddTransition(NodeState.New, NodeState
			.New, RMNodeEventType.ResourceUpdate, new RMNodeImpl.UpdateNodeResourceWhenUnusableTransition
			()).AddTransition(NodeState.Running, EnumSet.Of(NodeState.Running, NodeState.Unhealthy
			), RMNodeEventType.StatusUpdate, new RMNodeImpl.StatusUpdateWhenHealthyTransition
			()).AddTransition(NodeState.Running, NodeState.Decommissioned, RMNodeEventType.Decommission
			, new RMNodeImpl.DeactivateNodeTransition(NodeState.Decommissioned)).AddTransition
			(NodeState.Running, NodeState.Lost, RMNodeEventType.Expire, new RMNodeImpl.DeactivateNodeTransition
			(NodeState.Lost)).AddTransition(NodeState.Running, NodeState.Rebooted, RMNodeEventType
			.Rebooting, new RMNodeImpl.DeactivateNodeTransition(NodeState.Rebooted)).AddTransition
			(NodeState.Running, NodeState.Running, RMNodeEventType.CleanupApp, new RMNodeImpl.CleanUpAppTransition
			()).AddTransition(NodeState.Running, NodeState.Running, RMNodeEventType.CleanupContainer
			, new RMNodeImpl.CleanUpContainerTransition()).AddTransition(NodeState.Running, 
			NodeState.Running, RMNodeEventType.FinishedContainersPulledByAm, new RMNodeImpl.AddContainersToBeRemovedFromNMTransition
			()).AddTransition(NodeState.Running, NodeState.Running, RMNodeEventType.Reconnected
			, new RMNodeImpl.ReconnectNodeTransition()).AddTransition(NodeState.Running, NodeState
			.Running, RMNodeEventType.ResourceUpdate, new RMNodeImpl.UpdateNodeResourceWhenRunningTransition
			()).AddTransition(NodeState.Rebooted, NodeState.Rebooted, RMNodeEventType.ResourceUpdate
			, new RMNodeImpl.UpdateNodeResourceWhenUnusableTransition()).AddTransition(NodeState
			.Decommissioned, NodeState.Decommissioned, RMNodeEventType.ResourceUpdate, new RMNodeImpl.UpdateNodeResourceWhenUnusableTransition
			()).AddTransition(NodeState.Decommissioned, NodeState.Decommissioned, RMNodeEventType
			.FinishedContainersPulledByAm, new RMNodeImpl.AddContainersToBeRemovedFromNMTransition
			()).AddTransition(NodeState.Lost, NodeState.Lost, RMNodeEventType.ResourceUpdate
			, new RMNodeImpl.UpdateNodeResourceWhenUnusableTransition()).AddTransition(NodeState
			.Lost, NodeState.Lost, RMNodeEventType.FinishedContainersPulledByAm, new RMNodeImpl.AddContainersToBeRemovedFromNMTransition
			()).AddTransition(NodeState.Unhealthy, EnumSet.Of(NodeState.Unhealthy, NodeState
			.Running), RMNodeEventType.StatusUpdate, new RMNodeImpl.StatusUpdateWhenUnHealthyTransition
			()).AddTransition(NodeState.Unhealthy, NodeState.Decommissioned, RMNodeEventType
			.Decommission, new RMNodeImpl.DeactivateNodeTransition(NodeState.Decommissioned)
			).AddTransition(NodeState.Unhealthy, NodeState.Lost, RMNodeEventType.Expire, new 
			RMNodeImpl.DeactivateNodeTransition(NodeState.Lost)).AddTransition(NodeState.Unhealthy
			, NodeState.Rebooted, RMNodeEventType.Rebooting, new RMNodeImpl.DeactivateNodeTransition
			(NodeState.Rebooted)).AddTransition(NodeState.Unhealthy, NodeState.Unhealthy, RMNodeEventType
			.Reconnected, new RMNodeImpl.ReconnectNodeTransition()).AddTransition(NodeState.
			Unhealthy, NodeState.Unhealthy, RMNodeEventType.CleanupApp, new RMNodeImpl.CleanUpAppTransition
			()).AddTransition(NodeState.Unhealthy, NodeState.Unhealthy, RMNodeEventType.CleanupContainer
			, new RMNodeImpl.CleanUpContainerTransition()).AddTransition(NodeState.Unhealthy
			, NodeState.Unhealthy, RMNodeEventType.ResourceUpdate, new RMNodeImpl.UpdateNodeResourceWhenUnusableTransition
			()).AddTransition(NodeState.Unhealthy, NodeState.Unhealthy, RMNodeEventType.FinishedContainersPulledByAm
			, new RMNodeImpl.AddContainersToBeRemovedFromNMTransition()).InstallTopology();

		private readonly StateMachine<NodeState, RMNodeEventType, RMNodeEvent> stateMachine;

		public RMNodeImpl(NodeId nodeId, RMContext context, string hostName, int cmPort, 
			int httpPort, Node node, Resource capability, string nodeManagerVersion)
		{
			// The containerManager address
			/* set of containers that have just launched */
			/* set of containers that need to be cleaned */
			/*
			* set of containers to notify NM to remove them from its context. Currently,
			* this includes containers that were notified to AM about their completion
			*/
			/* the list of applications that have finished and need to be purged */
			//Transitions from NEW state
			//Transitions from RUNNING state
			//Transitions from REBOOTED state
			//Transitions from DECOMMISSIONED state
			//Transitions from LOST state
			//Transitions from UNHEALTHY state
			// create the topology tables
			this.nodeId = nodeId;
			this.context = context;
			this.hostName = hostName;
			this.commandPort = cmPort;
			this.httpPort = httpPort;
			this.totalCapability = capability;
			this.nodeAddress = hostName + ":" + cmPort;
			this.httpAddress = hostName + ":" + httpPort;
			this.node = node;
			this.healthReport = "Healthy";
			this.lastHealthReportTime = Runtime.CurrentTimeMillis();
			this.nodeManagerVersion = nodeManagerVersion;
			this.latestNodeHeartBeatResponse.SetResponseId(0);
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			this.readLock = Lock.ReadLock();
			this.writeLock = Lock.WriteLock();
			this.stateMachine = stateMachineFactory.Make(this);
			this.nodeUpdateQueue = new ConcurrentLinkedQueue<UpdatedContainerInfo>();
		}

		public override string ToString()
		{
			return this.nodeId.ToString();
		}

		public virtual string GetHostName()
		{
			return hostName;
		}

		public virtual int GetCommandPort()
		{
			return commandPort;
		}

		public virtual int GetHttpPort()
		{
			return httpPort;
		}

		public virtual NodeId GetNodeID()
		{
			return this.nodeId;
		}

		public virtual string GetNodeAddress()
		{
			return this.nodeAddress;
		}

		public virtual string GetHttpAddress()
		{
			return this.httpAddress;
		}

		public virtual Resource GetTotalCapability()
		{
			return this.totalCapability;
		}

		public virtual string GetRackName()
		{
			return node.GetNetworkLocation();
		}

		public virtual Node GetNode()
		{
			return this.node;
		}

		public virtual string GetHealthReport()
		{
			this.readLock.Lock();
			try
			{
				return this.healthReport;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual void SetHealthReport(string healthReport)
		{
			this.writeLock.Lock();
			try
			{
				this.healthReport = healthReport;
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		public virtual void SetLastHealthReportTime(long lastHealthReportTime)
		{
			this.writeLock.Lock();
			try
			{
				this.lastHealthReportTime = lastHealthReportTime;
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		public virtual long GetLastHealthReportTime()
		{
			this.readLock.Lock();
			try
			{
				return this.lastHealthReportTime;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual string GetNodeManagerVersion()
		{
			return nodeManagerVersion;
		}

		public virtual NodeState GetState()
		{
			this.readLock.Lock();
			try
			{
				return this.stateMachine.GetCurrentState();
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual IList<ApplicationId> GetAppsToCleanup()
		{
			this.readLock.Lock();
			try
			{
				return new AList<ApplicationId>(this.finishedApplications);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual IList<ContainerId> GetContainersToCleanUp()
		{
			this.readLock.Lock();
			try
			{
				return new AList<ContainerId>(this.containersToClean);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual void UpdateNodeHeartbeatResponseForCleanup(NodeHeartbeatResponse response
			)
		{
			this.writeLock.Lock();
			try
			{
				response.AddAllContainersToCleanup(new AList<ContainerId>(this.containersToClean)
					);
				response.AddAllApplicationsToCleanup(this.finishedApplications);
				response.AddContainersToBeRemovedFromNM(new AList<ContainerId>(this.containersToBeRemovedFromNM
					));
				this.containersToClean.Clear();
				this.finishedApplications.Clear();
				this.containersToBeRemovedFromNM.Clear();
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		public virtual NodeHeartbeatResponse GetLastNodeHeartBeatResponse()
		{
			this.readLock.Lock();
			try
			{
				return this.latestNodeHeartBeatResponse;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual void ResetLastNodeHeartBeatResponse()
		{
			this.writeLock.Lock();
			try
			{
				latestNodeHeartBeatResponse.SetResponseId(0);
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		public virtual void Handle(RMNodeEvent @event)
		{
			Log.Debug("Processing " + @event.GetNodeId() + " of type " + @event.GetType());
			try
			{
				writeLock.Lock();
				NodeState oldState = GetState();
				try
				{
					stateMachine.DoTransition(@event.GetType(), @event);
				}
				catch (InvalidStateTransitonException e)
				{
					Log.Error("Can't handle this event at current state", e);
					Log.Error("Invalid event " + @event.GetType() + " on Node  " + this.nodeId);
				}
				if (oldState != GetState())
				{
					Log.Info(nodeId + " Node Transitioned from " + oldState + " to " + GetState());
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		private void UpdateMetricsForRejoinedNode(NodeState previousNodeState)
		{
			ClusterMetrics metrics = ClusterMetrics.GetMetrics();
			metrics.IncrNumActiveNodes();
			switch (previousNodeState)
			{
				case NodeState.Lost:
				{
					metrics.DecrNumLostNMs();
					break;
				}

				case NodeState.Rebooted:
				{
					metrics.DecrNumRebootedNMs();
					break;
				}

				case NodeState.Decommissioned:
				{
					metrics.DecrDecommisionedNMs();
					break;
				}

				case NodeState.Unhealthy:
				{
					metrics.DecrNumUnhealthyNMs();
					break;
				}

				default:
				{
					Log.Debug("Unexpected previous node state");
					break;
				}
			}
		}

		private void UpdateMetricsForDeactivatedNode(NodeState initialState, NodeState finalState
			)
		{
			ClusterMetrics metrics = ClusterMetrics.GetMetrics();
			switch (initialState)
			{
				case NodeState.Running:
				{
					metrics.DecrNumActiveNodes();
					break;
				}

				case NodeState.Unhealthy:
				{
					metrics.DecrNumUnhealthyNMs();
					break;
				}

				default:
				{
					Log.Debug("Unexpected inital state");
					break;
				}
			}
			switch (finalState)
			{
				case NodeState.Decommissioned:
				{
					metrics.IncrDecommisionedNMs();
					break;
				}

				case NodeState.Lost:
				{
					metrics.IncrNumLostNMs();
					break;
				}

				case NodeState.Rebooted:
				{
					metrics.IncrNumRebootedNMs();
					break;
				}

				case NodeState.Unhealthy:
				{
					metrics.IncrNumUnhealthyNMs();
					break;
				}

				default:
				{
					Log.Debug("Unexpected final state");
					break;
				}
			}
		}

		private static void HandleRunningAppOnNode(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode.RMNodeImpl
			 rmNode, RMContext context, ApplicationId appId, NodeId nodeId)
		{
			RMApp app = context.GetRMApps()[appId];
			// if we failed getting app by appId, maybe something wrong happened, just
			// add the app to the finishedApplications list so that the app can be
			// cleaned up on the NM
			if (null == app)
			{
				Log.Warn("Cannot get RMApp by appId=" + appId + ", just added it to finishedApplications list for cleanup"
					);
				rmNode.finishedApplications.AddItem(appId);
				return;
			}
			context.GetDispatcher().GetEventHandler().Handle(new RMAppRunningOnNodeEvent(appId
				, nodeId));
		}

		private static void UpdateNodeResourceFromEvent(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode.RMNodeImpl
			 rmNode, RMNodeResourceUpdateEvent @event)
		{
			ResourceOption resourceOption = @event.GetResourceOption();
			// Set resource on RMNode
			rmNode.totalCapability = resourceOption.GetResource();
		}

		public class AddNodeTransition : SingleArcTransition<RMNodeImpl, RMNodeEvent>
		{
			public virtual void Transition(RMNodeImpl rmNode, RMNodeEvent @event)
			{
				// Inform the scheduler
				RMNodeStartedEvent startEvent = (RMNodeStartedEvent)@event;
				IList<NMContainerStatus> containers = null;
				string host = rmNode.nodeId.GetHost();
				if (rmNode.context.GetInactiveRMNodes().Contains(host))
				{
					// Old node rejoining
					RMNode previouRMNode = rmNode.context.GetInactiveRMNodes()[host];
					Sharpen.Collections.Remove(rmNode.context.GetInactiveRMNodes(), host);
					rmNode.UpdateMetricsForRejoinedNode(previouRMNode.GetState());
				}
				else
				{
					// Increment activeNodes explicitly because this is a new node.
					ClusterMetrics.GetMetrics().IncrNumActiveNodes();
					containers = startEvent.GetNMContainerStatuses();
					if (containers != null && !containers.IsEmpty())
					{
						foreach (NMContainerStatus container in containers)
						{
							if (container.GetContainerState() == ContainerState.Running)
							{
								rmNode.launchedContainers.AddItem(container.GetContainerId());
							}
						}
					}
				}
				if (null != startEvent.GetRunningApplications())
				{
					foreach (ApplicationId appId in startEvent.GetRunningApplications())
					{
						HandleRunningAppOnNode(rmNode, rmNode.context, appId, rmNode.nodeId);
					}
				}
				rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodeAddedSchedulerEvent
					(rmNode, containers));
				rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodesListManagerEvent
					(NodesListManagerEventType.NodeUsable, rmNode));
			}
		}

		public class ReconnectNodeTransition : SingleArcTransition<RMNodeImpl, RMNodeEvent
			>
		{
			public virtual void Transition(RMNodeImpl rmNode, RMNodeEvent @event)
			{
				RMNodeReconnectEvent reconnectEvent = (RMNodeReconnectEvent)@event;
				RMNode newNode = reconnectEvent.GetReconnectedNode();
				rmNode.nodeManagerVersion = newNode.GetNodeManagerVersion();
				IList<ApplicationId> runningApps = reconnectEvent.GetRunningApplications();
				bool noRunningApps = (runningApps == null) || (runningApps.Count == 0);
				// No application running on the node, so send node-removal event with 
				// cleaning up old container info.
				if (noRunningApps)
				{
					rmNode.nodeUpdateQueue.Clear();
					rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodeRemovedSchedulerEvent
						(rmNode));
					if (rmNode.GetHttpPort() == newNode.GetHttpPort())
					{
						if (!rmNode.GetTotalCapability().Equals(newNode.GetTotalCapability()))
						{
							rmNode.totalCapability = newNode.GetTotalCapability();
						}
						if (rmNode.GetState().Equals(NodeState.Running))
						{
							// Only add old node if old state is RUNNING
							rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodeAddedSchedulerEvent
								(rmNode));
						}
					}
					else
					{
						switch (rmNode.GetState())
						{
							case NodeState.Running:
							{
								// Reconnected node differs, so replace old node and start new node
								ClusterMetrics.GetMetrics().DecrNumActiveNodes();
								break;
							}

							case NodeState.Unhealthy:
							{
								ClusterMetrics.GetMetrics().DecrNumUnhealthyNMs();
								break;
							}

							default:
							{
								Log.Debug("Unexpected Rmnode state");
								break;
							}
						}
						rmNode.context.GetRMNodes()[newNode.GetNodeID()] = newNode;
						rmNode.context.GetDispatcher().GetEventHandler().Handle(new RMNodeStartedEvent(newNode
							.GetNodeID(), null, null));
					}
				}
				else
				{
					rmNode.httpPort = newNode.GetHttpPort();
					rmNode.httpAddress = newNode.GetHttpAddress();
					bool isCapabilityChanged = false;
					if (!rmNode.GetTotalCapability().Equals(newNode.GetTotalCapability()))
					{
						rmNode.totalCapability = newNode.GetTotalCapability();
						isCapabilityChanged = true;
					}
					HandleNMContainerStatus(reconnectEvent.GetNMContainerStatuses(), rmNode);
					foreach (ApplicationId appId in reconnectEvent.GetRunningApplications())
					{
						HandleRunningAppOnNode(rmNode, rmNode.context, appId, rmNode.nodeId);
					}
					if (isCapabilityChanged && rmNode.GetState().Equals(NodeState.Running))
					{
						// Update scheduler node's capacity for reconnect node.
						rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodeResourceUpdateSchedulerEvent
							(rmNode, ResourceOption.NewInstance(newNode.GetTotalCapability(), -1)));
					}
				}
			}

			private void HandleNMContainerStatus(IList<NMContainerStatus> nmContainerStatuses
				, RMNodeImpl rmnode)
			{
				IList<ContainerStatus> containerStatuses = new AList<ContainerStatus>();
				foreach (NMContainerStatus nmContainerStatus in nmContainerStatuses)
				{
					containerStatuses.AddItem(CreateContainerStatus(nmContainerStatus));
				}
				rmnode.HandleContainerStatus(containerStatuses);
			}

			private ContainerStatus CreateContainerStatus(NMContainerStatus remoteContainer)
			{
				ContainerStatus cStatus = ContainerStatus.NewInstance(remoteContainer.GetContainerId
					(), remoteContainer.GetContainerState(), remoteContainer.GetDiagnostics(), remoteContainer
					.GetContainerExitStatus());
				return cStatus;
			}
		}

		public class UpdateNodeResourceWhenRunningTransition : SingleArcTransition<RMNodeImpl
			, RMNodeEvent>
		{
			public virtual void Transition(RMNodeImpl rmNode, RMNodeEvent @event)
			{
				RMNodeResourceUpdateEvent updateEvent = (RMNodeResourceUpdateEvent)@event;
				UpdateNodeResourceFromEvent(rmNode, updateEvent);
				// Notify new resourceOption to scheduler
				rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodeResourceUpdateSchedulerEvent
					(rmNode, updateEvent.GetResourceOption()));
			}
		}

		public class UpdateNodeResourceWhenUnusableTransition : SingleArcTransition<RMNodeImpl
			, RMNodeEvent>
		{
			public virtual void Transition(RMNodeImpl rmNode, RMNodeEvent @event)
			{
				// The node is not usable, only log a warn message
				Log.Warn("Try to update resource on a " + rmNode.GetState().ToString() + " node: "
					 + rmNode.ToString());
				UpdateNodeResourceFromEvent(rmNode, (RMNodeResourceUpdateEvent)@event);
			}
			// No need to notify scheduler as schedulerNode is not function now
			// and can sync later from RMnode.
		}

		public class CleanUpAppTransition : SingleArcTransition<RMNodeImpl, RMNodeEvent>
		{
			public virtual void Transition(RMNodeImpl rmNode, RMNodeEvent @event)
			{
				rmNode.finishedApplications.AddItem(((RMNodeCleanAppEvent)@event).GetAppId());
			}
		}

		public class CleanUpContainerTransition : SingleArcTransition<RMNodeImpl, RMNodeEvent
			>
		{
			public virtual void Transition(RMNodeImpl rmNode, RMNodeEvent @event)
			{
				rmNode.containersToClean.AddItem(((RMNodeCleanContainerEvent)@event).GetContainerId
					());
			}
		}

		public class AddContainersToBeRemovedFromNMTransition : SingleArcTransition<RMNodeImpl
			, RMNodeEvent>
		{
			public virtual void Transition(RMNodeImpl rmNode, RMNodeEvent @event)
			{
				Sharpen.Collections.AddAll(rmNode.containersToBeRemovedFromNM, ((RMNodeFinishedContainersPulledByAMEvent
					)@event).GetContainers());
			}
		}

		public class DeactivateNodeTransition : SingleArcTransition<RMNodeImpl, RMNodeEvent
			>
		{
			private readonly NodeState finalState;

			public DeactivateNodeTransition(NodeState finalState)
			{
				this.finalState = finalState;
			}

			public virtual void Transition(RMNodeImpl rmNode, RMNodeEvent @event)
			{
				// Inform the scheduler
				rmNode.nodeUpdateQueue.Clear();
				// If the current state is NodeState.UNHEALTHY
				// Then node is already been removed from the
				// Scheduler
				NodeState initialState = rmNode.GetState();
				if (!initialState.Equals(NodeState.Unhealthy))
				{
					rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodeRemovedSchedulerEvent
						(rmNode));
				}
				rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodesListManagerEvent
					(NodesListManagerEventType.NodeUnusable, rmNode));
				// Deactivate the node
				Sharpen.Collections.Remove(rmNode.context.GetRMNodes(), rmNode.nodeId);
				Log.Info("Deactivating Node " + rmNode.nodeId + " as it is now " + finalState);
				rmNode.context.GetInactiveRMNodes()[rmNode.nodeId.GetHost()] = rmNode;
				//Update the metrics
				rmNode.UpdateMetricsForDeactivatedNode(initialState, finalState);
			}
		}

		public class StatusUpdateWhenHealthyTransition : MultipleArcTransition<RMNodeImpl
			, RMNodeEvent, NodeState>
		{
			public virtual NodeState Transition(RMNodeImpl rmNode, RMNodeEvent @event)
			{
				RMNodeStatusEvent statusEvent = (RMNodeStatusEvent)@event;
				// Switch the last heartbeatresponse.
				rmNode.latestNodeHeartBeatResponse = statusEvent.GetLatestResponse();
				NodeHealthStatus remoteNodeHealthStatus = statusEvent.GetNodeHealthStatus();
				rmNode.SetHealthReport(remoteNodeHealthStatus.GetHealthReport());
				rmNode.SetLastHealthReportTime(remoteNodeHealthStatus.GetLastHealthReportTime());
				if (!remoteNodeHealthStatus.GetIsNodeHealthy())
				{
					Log.Info("Node " + rmNode.nodeId + " reported UNHEALTHY with details: " + remoteNodeHealthStatus
						.GetHealthReport());
					rmNode.nodeUpdateQueue.Clear();
					// Inform the scheduler
					rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodeRemovedSchedulerEvent
						(rmNode));
					rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodesListManagerEvent
						(NodesListManagerEventType.NodeUnusable, rmNode));
					// Update metrics
					rmNode.UpdateMetricsForDeactivatedNode(rmNode.GetState(), NodeState.Unhealthy);
					return NodeState.Unhealthy;
				}
				rmNode.HandleContainerStatus(statusEvent.GetContainers());
				if (rmNode.nextHeartBeat)
				{
					rmNode.nextHeartBeat = false;
					rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodeUpdateSchedulerEvent
						(rmNode));
				}
				// Update DTRenewer in secure mode to keep these apps alive. Today this is
				// needed for log-aggregation to finish long after the apps are gone.
				if (UserGroupInformation.IsSecurityEnabled())
				{
					rmNode.context.GetDelegationTokenRenewer().UpdateKeepAliveApplications(statusEvent
						.GetKeepAliveAppIds());
				}
				return NodeState.Running;
			}
		}

		public class StatusUpdateWhenUnHealthyTransition : MultipleArcTransition<RMNodeImpl
			, RMNodeEvent, NodeState>
		{
			public virtual NodeState Transition(RMNodeImpl rmNode, RMNodeEvent @event)
			{
				RMNodeStatusEvent statusEvent = (RMNodeStatusEvent)@event;
				// Switch the last heartbeatresponse.
				rmNode.latestNodeHeartBeatResponse = statusEvent.GetLatestResponse();
				NodeHealthStatus remoteNodeHealthStatus = statusEvent.GetNodeHealthStatus();
				rmNode.SetHealthReport(remoteNodeHealthStatus.GetHealthReport());
				rmNode.SetLastHealthReportTime(remoteNodeHealthStatus.GetLastHealthReportTime());
				if (remoteNodeHealthStatus.GetIsNodeHealthy())
				{
					rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodeAddedSchedulerEvent
						(rmNode));
					rmNode.context.GetDispatcher().GetEventHandler().Handle(new NodesListManagerEvent
						(NodesListManagerEventType.NodeUsable, rmNode));
					// ??? how about updating metrics before notifying to ensure that
					// notifiers get update metadata because they will very likely query it
					// upon notification
					// Update metrics
					rmNode.UpdateMetricsForRejoinedNode(NodeState.Unhealthy);
					return NodeState.Running;
				}
				return NodeState.Unhealthy;
			}
		}

		public virtual IList<UpdatedContainerInfo> PullContainerUpdates()
		{
			IList<UpdatedContainerInfo> latestContainerInfoList = new AList<UpdatedContainerInfo
				>();
			UpdatedContainerInfo containerInfo;
			while ((containerInfo = nodeUpdateQueue.Poll()) != null)
			{
				latestContainerInfoList.AddItem(containerInfo);
			}
			this.nextHeartBeat = true;
			return latestContainerInfoList;
		}

		[VisibleForTesting]
		public virtual void SetNextHeartBeat(bool nextHeartBeat)
		{
			this.nextHeartBeat = nextHeartBeat;
		}

		[VisibleForTesting]
		public virtual int GetQueueSize()
		{
			return nodeUpdateQueue.Count;
		}

		// For test only.
		[VisibleForTesting]
		public virtual ICollection<ContainerId> GetLaunchedContainers()
		{
			return this.launchedContainers;
		}

		public virtual ICollection<string> GetNodeLabels()
		{
			RMNodeLabelsManager nlm = context.GetNodeLabelManager();
			if (nlm == null || nlm.GetLabelsOnNode(nodeId) == null)
			{
				return CommonNodeLabelsManager.EmptyStringSet;
			}
			return nlm.GetLabelsOnNode(nodeId);
		}

		private void HandleContainerStatus(IList<ContainerStatus> containerStatuses)
		{
			// Filter the map to only obtain just launched containers and finished
			// containers.
			IList<ContainerStatus> newlyLaunchedContainers = new AList<ContainerStatus>();
			IList<ContainerStatus> completedContainers = new AList<ContainerStatus>();
			foreach (ContainerStatus remoteContainer in containerStatuses)
			{
				ContainerId containerId = remoteContainer.GetContainerId();
				// Don't bother with containers already scheduled for cleanup, or for
				// applications already killed. The scheduler doens't need to know any
				// more about this container
				if (containersToClean.Contains(containerId))
				{
					Log.Info("Container " + containerId + " already scheduled for " + "cleanup, no further processing"
						);
					continue;
				}
				if (finishedApplications.Contains(containerId.GetApplicationAttemptId().GetApplicationId
					()))
				{
					Log.Info("Container " + containerId + " belongs to an application that is already killed,"
						 + " no further processing");
					continue;
				}
				// Process running containers
				if (remoteContainer.GetState() == ContainerState.Running)
				{
					if (!launchedContainers.Contains(containerId))
					{
						// Just launched container. RM knows about it the first time.
						launchedContainers.AddItem(containerId);
						newlyLaunchedContainers.AddItem(remoteContainer);
					}
				}
				else
				{
					// A finished container
					launchedContainers.Remove(containerId);
					completedContainers.AddItem(remoteContainer);
				}
			}
			if (newlyLaunchedContainers.Count != 0 || completedContainers.Count != 0)
			{
				nodeUpdateQueue.AddItem(new UpdatedContainerInfo(newlyLaunchedContainers, completedContainers
					));
			}
		}
	}
}
