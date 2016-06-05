using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Lang.Time;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.State;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer
{
	public class RMContainerImpl : RMContainer
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer.RMContainerImpl
			));

		private static readonly StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer.RMContainerImpl
			, RMContainerState, RMContainerEventType, RMContainerEvent> stateMachineFactory = 
			new StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer.RMContainerImpl
			, RMContainerState, RMContainerEventType, RMContainerEvent>(RMContainerState.New
			).AddTransition(RMContainerState.New, RMContainerState.Allocated, RMContainerEventType
			.Start, new RMContainerImpl.ContainerStartedTransition()).AddTransition(RMContainerState
			.New, RMContainerState.Killed, RMContainerEventType.Kill).AddTransition(RMContainerState
			.New, RMContainerState.Reserved, RMContainerEventType.Reserved, new RMContainerImpl.ContainerReservedTransition
			()).AddTransition(RMContainerState.New, EnumSet.Of(RMContainerState.Running, RMContainerState
			.Completed), RMContainerEventType.Recover, new RMContainerImpl.ContainerRecoveredTransition
			()).AddTransition(RMContainerState.Reserved, RMContainerState.Reserved, RMContainerEventType
			.Reserved, new RMContainerImpl.ContainerReservedTransition()).AddTransition(RMContainerState
			.Reserved, RMContainerState.Allocated, RMContainerEventType.Start, new RMContainerImpl.ContainerStartedTransition
			()).AddTransition(RMContainerState.Reserved, RMContainerState.Killed, RMContainerEventType
			.Kill).AddTransition(RMContainerState.Reserved, RMContainerState.Released, RMContainerEventType
			.Released).AddTransition(RMContainerState.Allocated, RMContainerState.Acquired, 
			RMContainerEventType.Acquired, new RMContainerImpl.AcquiredTransition()).AddTransition
			(RMContainerState.Allocated, RMContainerState.Expired, RMContainerEventType.Expire
			, new RMContainerImpl.FinishedTransition()).AddTransition(RMContainerState.Allocated
			, RMContainerState.Killed, RMContainerEventType.Kill, new RMContainerImpl.ContainerRescheduledTransition
			()).AddTransition(RMContainerState.Acquired, RMContainerState.Running, RMContainerEventType
			.Launched, new RMContainerImpl.LaunchedTransition()).AddTransition(RMContainerState
			.Acquired, RMContainerState.Completed, RMContainerEventType.Finished, new RMContainerImpl.ContainerFinishedAtAcquiredState
			()).AddTransition(RMContainerState.Acquired, RMContainerState.Released, RMContainerEventType
			.Released, new RMContainerImpl.KillTransition()).AddTransition(RMContainerState.
			Acquired, RMContainerState.Expired, RMContainerEventType.Expire, new RMContainerImpl.KillTransition
			()).AddTransition(RMContainerState.Acquired, RMContainerState.Killed, RMContainerEventType
			.Kill, new RMContainerImpl.KillTransition()).AddTransition(RMContainerState.Running
			, RMContainerState.Completed, RMContainerEventType.Finished, new RMContainerImpl.FinishedTransition
			()).AddTransition(RMContainerState.Running, RMContainerState.Killed, RMContainerEventType
			.Kill, new RMContainerImpl.KillTransition()).AddTransition(RMContainerState.Running
			, RMContainerState.Released, RMContainerEventType.Released, new RMContainerImpl.KillTransition
			()).AddTransition(RMContainerState.Running, RMContainerState.Running, RMContainerEventType
			.Expire).AddTransition(RMContainerState.Completed, RMContainerState.Completed, EnumSet
			.Of(RMContainerEventType.Expire, RMContainerEventType.Released, RMContainerEventType
			.Kill)).AddTransition(RMContainerState.Expired, RMContainerState.Expired, EnumSet
			.Of(RMContainerEventType.Released, RMContainerEventType.Kill)).AddTransition(RMContainerState
			.Released, RMContainerState.Released, EnumSet.Of(RMContainerEventType.Expire, RMContainerEventType
			.Released, RMContainerEventType.Kill, RMContainerEventType.Finished)).AddTransition
			(RMContainerState.Killed, RMContainerState.Killed, EnumSet.Of(RMContainerEventType
			.Expire, RMContainerEventType.Released, RMContainerEventType.Kill, RMContainerEventType
			.Finished)).InstallTopology();

		private readonly StateMachine<RMContainerState, RMContainerEventType, RMContainerEvent
			> stateMachine;

		private readonly ReentrantReadWriteLock.ReadLock readLock;

		private readonly ReentrantReadWriteLock.WriteLock writeLock;

		private readonly ContainerId containerId;

		private readonly ApplicationAttemptId appAttemptId;

		private readonly NodeId nodeId;

		private readonly Container container;

		private readonly RMContext rmContext;

		private readonly EventHandler eventHandler;

		private readonly ContainerAllocationExpirer containerAllocationExpirer;

		private readonly string user;

		private Resource reservedResource;

		private NodeId reservedNode;

		private Priority reservedPriority;

		private long creationTime;

		private long finishTime;

		private ContainerStatus finishedStatus;

		private bool isAMContainer;

		private IList<ResourceRequest> resourceRequests;

		private bool saveNonAMContainerMetaInfo;

		public RMContainerImpl(Container container, ApplicationAttemptId appAttemptId, NodeId
			 nodeId, string user, RMContext rmContext)
			: this(container, appAttemptId, nodeId, user, rmContext, Runtime.CurrentTimeMillis
				())
		{
		}

		public RMContainerImpl(Container container, ApplicationAttemptId appAttemptId, NodeId
			 nodeId, string user, RMContext rmContext, long creationTime)
		{
			// Transitions from NEW state
			// Transitions from RESERVED state
			// nothing to do
			// nothing to do
			// Transitions from ALLOCATED state
			// Transitions from ACQUIRED state
			// Transitions from RUNNING state
			// Transitions from COMPLETED state
			// Transitions from EXPIRED state
			// Transitions from RELEASED state
			// Transitions from KILLED state
			// create the topology tables
			this.stateMachine = stateMachineFactory.Make(this);
			this.containerId = container.GetId();
			this.nodeId = nodeId;
			this.container = container;
			this.appAttemptId = appAttemptId;
			this.user = user;
			this.creationTime = creationTime;
			this.rmContext = rmContext;
			this.eventHandler = rmContext.GetDispatcher().GetEventHandler();
			this.containerAllocationExpirer = rmContext.GetContainerAllocationExpirer();
			this.isAMContainer = false;
			this.resourceRequests = null;
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			this.readLock = Lock.ReadLock();
			this.writeLock = Lock.WriteLock();
			saveNonAMContainerMetaInfo = rmContext.GetYarnConfiguration().GetBoolean(YarnConfiguration
				.ApplicationHistorySaveNonAmContainerMetaInfo, YarnConfiguration.DefaultApplicationHistorySaveNonAmContainerMetaInfo
				);
			rmContext.GetRMApplicationHistoryWriter().ContainerStarted(this);
			// If saveNonAMContainerMetaInfo is true, store system metrics for all
			// containers. If false, and if this container is marked as the AM, metrics
			// will still be published for this container, but that calculation happens
			// later.
			if (saveNonAMContainerMetaInfo)
			{
				rmContext.GetSystemMetricsPublisher().ContainerCreated(this, this.creationTime);
			}
		}

		public virtual ContainerId GetContainerId()
		{
			return this.containerId;
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return this.appAttemptId;
		}

		public virtual Container GetContainer()
		{
			return this.container;
		}

		public virtual RMContainerState GetState()
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

		public virtual Resource GetReservedResource()
		{
			return reservedResource;
		}

		public virtual NodeId GetReservedNode()
		{
			return reservedNode;
		}

		public virtual Priority GetReservedPriority()
		{
			return reservedPriority;
		}

		public virtual Resource GetAllocatedResource()
		{
			return container.GetResource();
		}

		public virtual NodeId GetAllocatedNode()
		{
			return container.GetNodeId();
		}

		public virtual Priority GetAllocatedPriority()
		{
			return container.GetPriority();
		}

		public virtual long GetCreationTime()
		{
			return creationTime;
		}

		public virtual long GetFinishTime()
		{
			try
			{
				readLock.Lock();
				return finishTime;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual string GetDiagnosticsInfo()
		{
			try
			{
				readLock.Lock();
				if (GetFinishedStatus() != null)
				{
					return GetFinishedStatus().GetDiagnostics();
				}
				else
				{
					return null;
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual string GetLogURL()
		{
			try
			{
				readLock.Lock();
				StringBuilder logURL = new StringBuilder();
				logURL.Append(WebAppUtils.GetHttpSchemePrefix(rmContext.GetYarnConfiguration()));
				logURL.Append(WebAppUtils.GetRunningLogURL(container.GetNodeHttpAddress(), ConverterUtils
					.ToString(containerId), user));
				return logURL.ToString();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual int GetContainerExitStatus()
		{
			try
			{
				readLock.Lock();
				if (GetFinishedStatus() != null)
				{
					return GetFinishedStatus().GetExitStatus();
				}
				else
				{
					return 0;
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual ContainerState GetContainerState()
		{
			try
			{
				readLock.Lock();
				if (GetFinishedStatus() != null)
				{
					return GetFinishedStatus().GetState();
				}
				else
				{
					return ContainerState.Running;
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual IList<ResourceRequest> GetResourceRequests()
		{
			try
			{
				readLock.Lock();
				return resourceRequests;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual void SetResourceRequests(IList<ResourceRequest> requests)
		{
			try
			{
				writeLock.Lock();
				this.resourceRequests = requests;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public override string ToString()
		{
			return containerId.ToString();
		}

		public virtual bool IsAMContainer()
		{
			try
			{
				readLock.Lock();
				return isAMContainer;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual void SetAMContainer(bool isAMContainer)
		{
			try
			{
				writeLock.Lock();
				this.isAMContainer = isAMContainer;
			}
			finally
			{
				writeLock.Unlock();
			}
			// Even if saveNonAMContainerMetaInfo is not true, the AM container's system
			// metrics still need to be saved so that the AM's logs can be accessed.
			// This call to getSystemMetricsPublisher().containerCreated() is mutually
			// exclusive with the one in the RMContainerImpl constructor.
			if (!saveNonAMContainerMetaInfo && this.isAMContainer)
			{
				rmContext.GetSystemMetricsPublisher().ContainerCreated(this, this.creationTime);
			}
		}

		public virtual void Handle(RMContainerEvent @event)
		{
			Log.Debug("Processing " + @event.GetContainerId() + " of type " + @event.GetType(
				));
			try
			{
				writeLock.Lock();
				RMContainerState oldState = GetState();
				try
				{
					stateMachine.DoTransition(@event.GetType(), @event);
				}
				catch (InvalidStateTransitonException e)
				{
					Log.Error("Can't handle this event at current state", e);
					Log.Error("Invalid event " + @event.GetType() + " on container " + this.containerId
						);
				}
				if (oldState != GetState())
				{
					Log.Info(@event.GetContainerId() + " Container Transitioned from " + oldState + " to "
						 + GetState());
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public virtual ContainerStatus GetFinishedStatus()
		{
			return finishedStatus;
		}

		private class BaseTransition : SingleArcTransition<RMContainerImpl, RMContainerEvent
			>
		{
			public virtual void Transition(RMContainerImpl cont, RMContainerEvent @event)
			{
			}
		}

		private sealed class ContainerRecoveredTransition : MultipleArcTransition<RMContainerImpl
			, RMContainerEvent, RMContainerState>
		{
			public RMContainerState Transition(RMContainerImpl container, RMContainerEvent @event
				)
			{
				NMContainerStatus report = ((RMContainerRecoverEvent)@event).GetContainerReport();
				if (report.GetContainerState().Equals(ContainerState.Complete))
				{
					ContainerStatus status = ContainerStatus.NewInstance(report.GetContainerId(), report
						.GetContainerState(), report.GetDiagnostics(), report.GetContainerExitStatus());
					new RMContainerImpl.FinishedTransition().Transition(container, new RMContainerFinishedEvent
						(container.containerId, status, RMContainerEventType.Finished));
					return RMContainerState.Completed;
				}
				else
				{
					if (report.GetContainerState().Equals(ContainerState.Running))
					{
						// Tell the app
						container.eventHandler.Handle(new RMAppRunningOnNodeEvent(container.GetApplicationAttemptId
							().GetApplicationId(), container.nodeId));
						return RMContainerState.Running;
					}
					else
					{
						// This can never happen.
						Log.Warn("RMContainer received unexpected recover event with container" + " state "
							 + report.GetContainerState() + " while recovering.");
						return RMContainerState.Running;
					}
				}
			}
		}

		private sealed class ContainerReservedTransition : RMContainerImpl.BaseTransition
		{
			public override void Transition(RMContainerImpl container, RMContainerEvent @event
				)
			{
				RMContainerReservedEvent e = (RMContainerReservedEvent)@event;
				container.reservedResource = e.GetReservedResource();
				container.reservedNode = e.GetReservedNode();
				container.reservedPriority = e.GetReservedPriority();
			}
		}

		private sealed class ContainerStartedTransition : RMContainerImpl.BaseTransition
		{
			public override void Transition(RMContainerImpl container, RMContainerEvent @event
				)
			{
				container.eventHandler.Handle(new RMAppAttemptEvent(container.appAttemptId, RMAppAttemptEventType
					.ContainerAllocated));
			}
		}

		private sealed class AcquiredTransition : RMContainerImpl.BaseTransition
		{
			public override void Transition(RMContainerImpl container, RMContainerEvent @event
				)
			{
				// Clear ResourceRequest stored in RMContainer
				container.SetResourceRequests(null);
				// Register with containerAllocationExpirer.
				container.containerAllocationExpirer.Register(container.GetContainerId());
				// Tell the app
				container.eventHandler.Handle(new RMAppRunningOnNodeEvent(container.GetApplicationAttemptId
					().GetApplicationId(), container.nodeId));
			}
		}

		private sealed class LaunchedTransition : RMContainerImpl.BaseTransition
		{
			public override void Transition(RMContainerImpl container, RMContainerEvent @event
				)
			{
				// Unregister from containerAllocationExpirer.
				container.containerAllocationExpirer.Unregister(container.GetContainerId());
			}
		}

		private sealed class ContainerRescheduledTransition : RMContainerImpl.FinishedTransition
		{
			public override void Transition(RMContainerImpl container, RMContainerEvent @event
				)
			{
				// Tell scheduler to recover request of this container to app
				container.eventHandler.Handle(new ContainerRescheduledEvent(container));
				base.Transition(container, @event);
			}
		}

		private class FinishedTransition : RMContainerImpl.BaseTransition
		{
			public override void Transition(RMContainerImpl container, RMContainerEvent @event
				)
			{
				RMContainerFinishedEvent finishedEvent = (RMContainerFinishedEvent)@event;
				container.finishTime = Runtime.CurrentTimeMillis();
				container.finishedStatus = finishedEvent.GetRemoteContainerStatus();
				// Inform AppAttempt
				// container.getContainer() can return null when a RMContainer is a
				// reserved container
				UpdateAttemptMetrics(container);
				container.eventHandler.Handle(new RMAppAttemptContainerFinishedEvent(container.appAttemptId
					, finishedEvent.GetRemoteContainerStatus(), container.GetAllocatedNode()));
				container.rmContext.GetRMApplicationHistoryWriter().ContainerFinished(container);
				bool saveNonAMContainerMetaInfo = container.rmContext.GetYarnConfiguration().GetBoolean
					(YarnConfiguration.ApplicationHistorySaveNonAmContainerMetaInfo, YarnConfiguration
					.DefaultApplicationHistorySaveNonAmContainerMetaInfo);
				if (saveNonAMContainerMetaInfo || container.IsAMContainer())
				{
					container.rmContext.GetSystemMetricsPublisher().ContainerFinished(container, container
						.finishTime);
				}
			}

			private static void UpdateAttemptMetrics(RMContainerImpl container)
			{
				// If this is a preempted container, update preemption metrics
				Resource resource = container.GetContainer().GetResource();
				RMAppAttempt rmAttempt = container.rmContext.GetRMApps()[container.GetApplicationAttemptId
					().GetApplicationId()].GetCurrentAppAttempt();
				if (ContainerExitStatus.Preempted == container.finishedStatus.GetExitStatus())
				{
					rmAttempt.GetRMAppAttemptMetrics().UpdatePreemptionInfo(resource, container);
				}
				if (rmAttempt != null)
				{
					long usedMillis = container.finishTime - container.creationTime;
					long memorySeconds = resource.GetMemory() * usedMillis / DateUtils.MillisPerSecond;
					long vcoreSeconds = resource.GetVirtualCores() * usedMillis / DateUtils.MillisPerSecond;
					rmAttempt.GetRMAppAttemptMetrics().UpdateAggregateAppResourceUsage(memorySeconds, 
						vcoreSeconds);
				}
			}
		}

		private sealed class ContainerFinishedAtAcquiredState : RMContainerImpl.FinishedTransition
		{
			public override void Transition(RMContainerImpl container, RMContainerEvent @event
				)
			{
				// Unregister from containerAllocationExpirer.
				container.containerAllocationExpirer.Unregister(container.GetContainerId());
				// Inform AppAttempt
				base.Transition(container, @event);
			}
		}

		private sealed class KillTransition : RMContainerImpl.FinishedTransition
		{
			public override void Transition(RMContainerImpl container, RMContainerEvent @event
				)
			{
				// Unregister from containerAllocationExpirer.
				container.containerAllocationExpirer.Unregister(container.GetContainerId());
				// Inform node
				container.eventHandler.Handle(new RMNodeCleanContainerEvent(container.nodeId, container
					.containerId));
				// Inform appAttempt
				base.Transition(container, @event);
			}
		}

		public virtual ContainerReport CreateContainerReport()
		{
			this.readLock.Lock();
			ContainerReport containerReport = null;
			try
			{
				containerReport = ContainerReport.NewInstance(this.GetContainerId(), this.GetAllocatedResource
					(), this.GetAllocatedNode(), this.GetAllocatedPriority(), this.GetCreationTime()
					, this.GetFinishTime(), this.GetDiagnosticsInfo(), this.GetLogURL(), this.GetContainerExitStatus
					(), this.GetContainerState(), this.GetNodeHttpAddress());
			}
			finally
			{
				this.readLock.Unlock();
			}
			return containerReport;
		}

		public virtual string GetNodeHttpAddress()
		{
			try
			{
				readLock.Lock();
				if (container.GetNodeHttpAddress() != null)
				{
					StringBuilder httpAddress = new StringBuilder();
					httpAddress.Append(WebAppUtils.GetHttpSchemePrefix(rmContext.GetYarnConfiguration
						()));
					httpAddress.Append(container.GetNodeHttpAddress());
					return httpAddress.ToString();
				}
				else
				{
					return null;
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}
	}
}
