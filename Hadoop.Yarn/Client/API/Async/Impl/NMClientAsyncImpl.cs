using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Client.Api.Async;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.State;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl
{
	public class NMClientAsyncImpl : NMClientAsync
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl.NMClientAsyncImpl
			));

		protected internal const int InitialThreadPoolSize = 10;

		protected internal ThreadPoolExecutor threadPool;

		protected internal int maxThreadPoolSize;

		protected internal Sharpen.Thread eventDispatcherThread;

		protected internal AtomicBoolean stopped = new AtomicBoolean(false);

		protected internal BlockingQueue<NMClientAsyncImpl.ContainerEvent> events = new LinkedBlockingQueue
			<NMClientAsyncImpl.ContainerEvent>();

		protected internal ConcurrentMap<ContainerId, NMClientAsyncImpl.StatefulContainer
			> containers = new ConcurrentHashMap<ContainerId, NMClientAsyncImpl.StatefulContainer
			>();

		public NMClientAsyncImpl(NMClientAsync.CallbackHandler callbackHandler)
			: this(typeof(NMClientAsync).FullName, callbackHandler)
		{
		}

		public NMClientAsyncImpl(string name, NMClientAsync.CallbackHandler callbackHandler
			)
			: this(name, new NMClientImpl(), callbackHandler)
		{
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		protected internal NMClientAsyncImpl(string name, NMClient client, NMClientAsync.CallbackHandler
			 callbackHandler)
			: base(name, client, callbackHandler)
		{
			this.client = client;
			this.callbackHandler = callbackHandler;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.maxThreadPoolSize = conf.GetInt(YarnConfiguration.NmClientAsyncThreadPoolMaxSize
				, YarnConfiguration.DefaultNmClientAsyncThreadPoolMaxSize);
			Log.Info("Upper bound of the thread pool size is " + maxThreadPoolSize);
			client.Init(conf);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			client.Start();
			ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat(this.GetType().FullName
				 + " #%d").SetDaemon(true).Build();
			// Start with a default core-pool size and change it dynamically.
			int initSize = Math.Min(InitialThreadPoolSize, maxThreadPoolSize);
			threadPool = new ThreadPoolExecutor(initSize, int.MaxValue, 1, TimeUnit.Hours, new 
				LinkedBlockingQueue<Runnable>(), tf);
			eventDispatcherThread = new _Thread_125(this);
			// We can increase the pool size only if haven't reached the maximum
			// limit yet.
			// nodes where containers will run at *this* point of time. This is
			// *not* the cluster size and doesn't need to be.
			// Bump up the pool size to idealThreadPoolSize +
			// INITIAL_POOL_SIZE, the later is just a buffer so we are not
			// always increasing the pool-size
			// the events from the queue are handled in parallel with a thread
			// pool
			// TODO: Group launching of multiple containers to a single
			// NodeManager into a single connection
			eventDispatcherThread.SetName("Container  Event Dispatcher");
			eventDispatcherThread.SetDaemon(false);
			eventDispatcherThread.Start();
			base.ServiceStart();
		}

		private sealed class _Thread_125 : Sharpen.Thread
		{
			public _Thread_125(NMClientAsyncImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				NMClientAsyncImpl.ContainerEvent @event = null;
				ICollection<string> allNodes = new HashSet<string>();
				while (!this._enclosing.stopped.Get() && !Sharpen.Thread.CurrentThread().IsInterrupted
					())
				{
					try
					{
						@event = this._enclosing.events.Take();
					}
					catch (Exception e)
					{
						if (!this._enclosing.stopped.Get())
						{
							Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl.NMClientAsyncImpl.Log.Error("Returning, thread interrupted"
								, e);
						}
						return;
					}
					allNodes.AddItem(@event.GetNodeId().ToString());
					int threadPoolSize = this._enclosing.threadPool.GetCorePoolSize();
					if (threadPoolSize != this._enclosing.maxThreadPoolSize)
					{
						int nodeNum = allNodes.Count;
						int idealThreadPoolSize = Math.Min(this._enclosing.maxThreadPoolSize, nodeNum);
						if (threadPoolSize < idealThreadPoolSize)
						{
							int newThreadPoolSize = Math.Min(this._enclosing.maxThreadPoolSize, idealThreadPoolSize
								 + Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl.NMClientAsyncImpl.InitialThreadPoolSize
								);
							Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl.NMClientAsyncImpl.Log.Info("Set NMClientAsync thread pool size to "
								 + newThreadPoolSize + " as the number of nodes to talk to is " + nodeNum);
							this._enclosing.threadPool.SetCorePoolSize(newThreadPoolSize);
						}
					}
					this._enclosing.threadPool.Execute(this._enclosing.GetContainerEventProcessor(@event
						));
				}
			}

			private readonly NMClientAsyncImpl _enclosing;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (stopped.GetAndSet(true))
			{
				// return if already stopped
				return;
			}
			if (eventDispatcherThread != null)
			{
				eventDispatcherThread.Interrupt();
				try
				{
					eventDispatcherThread.Join();
				}
				catch (Exception e)
				{
					Log.Error("The thread of " + eventDispatcherThread.GetName() + " didn't finish normally."
						, e);
				}
			}
			if (threadPool != null)
			{
				threadPool.ShutdownNow();
			}
			if (client != null)
			{
				// If NMClientImpl doesn't stop running containers, the states doesn't
				// need to be cleared.
				if (!(client is NMClientImpl) || ((NMClientImpl)client).GetCleanupRunningContainers
					().Get())
				{
					if (containers != null)
					{
						containers.Clear();
					}
				}
				client.Stop();
			}
			base.ServiceStop();
		}

		public override void StartContainerAsync(Container container, ContainerLaunchContext
			 containerLaunchContext)
		{
			if (containers.PutIfAbsent(container.GetId(), new NMClientAsyncImpl.StatefulContainer
				(this, container.GetId())) != null)
			{
				callbackHandler.OnStartContainerError(container.GetId(), RPCUtil.GetRemoteException
					("Container " + container.GetId() + " is already started or scheduled to start")
					);
			}
			try
			{
				events.Put(new NMClientAsyncImpl.StartContainerEvent(container, containerLaunchContext
					));
			}
			catch (Exception e)
			{
				Log.Warn("Exception when scheduling the event of starting Container " + container
					.GetId());
				callbackHandler.OnStartContainerError(container.GetId(), e);
			}
		}

		public override void StopContainerAsync(ContainerId containerId, NodeId nodeId)
		{
			if (containers[containerId] == null)
			{
				callbackHandler.OnStopContainerError(containerId, RPCUtil.GetRemoteException("Container "
					 + containerId + " is neither started nor scheduled to start"));
			}
			try
			{
				events.Put(new NMClientAsyncImpl.ContainerEvent(containerId, nodeId, null, NMClientAsyncImpl.ContainerEventType
					.StopContainer));
			}
			catch (Exception e)
			{
				Log.Warn("Exception when scheduling the event of stopping Container " + containerId
					);
				callbackHandler.OnStopContainerError(containerId, e);
			}
		}

		public override void GetContainerStatusAsync(ContainerId containerId, NodeId nodeId
			)
		{
			try
			{
				events.Put(new NMClientAsyncImpl.ContainerEvent(containerId, nodeId, null, NMClientAsyncImpl.ContainerEventType
					.QueryContainer));
			}
			catch (Exception e)
			{
				Log.Warn("Exception when scheduling the event of querying the status" + " of Container "
					 + containerId);
				callbackHandler.OnGetContainerStatusError(containerId, e);
			}
		}

		protected internal enum ContainerState
		{
			Prep,
			Failed,
			Running,
			Done
		}

		protected internal virtual bool IsCompletelyDone(NMClientAsyncImpl.StatefulContainer
			 container)
		{
			return container.GetState() == NMClientAsyncImpl.ContainerState.Done || container
				.GetState() == NMClientAsyncImpl.ContainerState.Failed;
		}

		protected internal virtual NMClientAsyncImpl.ContainerEventProcessor GetContainerEventProcessor
			(NMClientAsyncImpl.ContainerEvent @event)
		{
			return new NMClientAsyncImpl.ContainerEventProcessor(this, @event);
		}

		/// <summary>The type of the event of interacting with a container</summary>
		protected internal enum ContainerEventType
		{
			StartContainer,
			StopContainer,
			QueryContainer
		}

		protected internal class ContainerEvent : AbstractEvent<NMClientAsyncImpl.ContainerEventType
			>
		{
			private ContainerId containerId;

			private NodeId nodeId;

			private Token containerToken;

			public ContainerEvent(ContainerId containerId, NodeId nodeId, Token containerToken
				, NMClientAsyncImpl.ContainerEventType type)
				: base(type)
			{
				this.containerId = containerId;
				this.nodeId = nodeId;
				this.containerToken = containerToken;
			}

			public virtual ContainerId GetContainerId()
			{
				return containerId;
			}

			public virtual NodeId GetNodeId()
			{
				return nodeId;
			}

			public virtual Token GetContainerToken()
			{
				return containerToken;
			}
		}

		protected internal class StartContainerEvent : NMClientAsyncImpl.ContainerEvent
		{
			private Container container;

			private ContainerLaunchContext containerLaunchContext;

			public StartContainerEvent(Container container, ContainerLaunchContext containerLaunchContext
				)
				: base(container.GetId(), container.GetNodeId(), container.GetContainerToken(), NMClientAsyncImpl.ContainerEventType
					.StartContainer)
			{
				this.container = container;
				this.containerLaunchContext = containerLaunchContext;
			}

			public virtual Container GetContainer()
			{
				return container;
			}

			public virtual ContainerLaunchContext GetContainerLaunchContext()
			{
				return containerLaunchContext;
			}
		}

		protected internal class StatefulContainer : EventHandler<NMClientAsyncImpl.ContainerEvent
			>
		{
			protected internal static readonly StateMachineFactory<NMClientAsyncImpl.StatefulContainer
				, NMClientAsyncImpl.ContainerState, NMClientAsyncImpl.ContainerEventType, NMClientAsyncImpl.ContainerEvent
				> stateMachineFactory = new StateMachineFactory<NMClientAsyncImpl.StatefulContainer
				, NMClientAsyncImpl.ContainerState, NMClientAsyncImpl.ContainerEventType, NMClientAsyncImpl.ContainerEvent
				>(NMClientAsyncImpl.ContainerState.Prep).AddTransition(NMClientAsyncImpl.ContainerState
				.Prep, EnumSet.Of(NMClientAsyncImpl.ContainerState.Running, NMClientAsyncImpl.ContainerState
				.Failed), NMClientAsyncImpl.ContainerEventType.StartContainer, new NMClientAsyncImpl.StatefulContainer.StartContainerTransition
				()).AddTransition(NMClientAsyncImpl.ContainerState.Prep, NMClientAsyncImpl.ContainerState
				.Done, NMClientAsyncImpl.ContainerEventType.StopContainer, new NMClientAsyncImpl.StatefulContainer.OutOfOrderTransition
				()).AddTransition(NMClientAsyncImpl.ContainerState.Running, EnumSet.Of(NMClientAsyncImpl.ContainerState
				.Done, NMClientAsyncImpl.ContainerState.Failed), NMClientAsyncImpl.ContainerEventType
				.StopContainer, new NMClientAsyncImpl.StatefulContainer.StopContainerTransition(
				)).AddTransition(NMClientAsyncImpl.ContainerState.Done, NMClientAsyncImpl.ContainerState
				.Done, EnumSet.Of(NMClientAsyncImpl.ContainerEventType.StartContainer, NMClientAsyncImpl.ContainerEventType
				.StopContainer)).AddTransition(NMClientAsyncImpl.ContainerState.Failed, NMClientAsyncImpl.ContainerState
				.Failed, EnumSet.Of(NMClientAsyncImpl.ContainerEventType.StartContainer, NMClientAsyncImpl.ContainerEventType
				.StopContainer));

			protected internal class StartContainerTransition : MultipleArcTransition<NMClientAsyncImpl.StatefulContainer
				, NMClientAsyncImpl.ContainerEvent, NMClientAsyncImpl.ContainerState>
			{
				// Transitions from PREP state
				// Transitions from RUNNING state
				// RUNNING -> RUNNING should be the invalid transition
				// Transition from DONE state
				// Transition from FAILED state
				public virtual NMClientAsyncImpl.ContainerState Transition(NMClientAsyncImpl.StatefulContainer
					 container, NMClientAsyncImpl.ContainerEvent @event)
				{
					ContainerId containerId = @event.GetContainerId();
					try
					{
						NMClientAsyncImpl.StartContainerEvent scEvent = null;
						if (@event is NMClientAsyncImpl.StartContainerEvent)
						{
							scEvent = (NMClientAsyncImpl.StartContainerEvent)@event;
						}
						System.Diagnostics.Debug.Assert(scEvent != null);
						IDictionary<string, ByteBuffer> allServiceResponse = container.nmClientAsync.GetClient
							().StartContainer(scEvent.GetContainer(), scEvent.GetContainerLaunchContext());
						try
						{
							container.nmClientAsync.GetCallbackHandler().OnContainerStarted(containerId, allServiceResponse
								);
						}
						catch (Exception thr)
						{
							// Don't process user created unchecked exception
							Log.Info("Unchecked exception is thrown from onContainerStarted for " + "Container "
								 + containerId, thr);
						}
						return NMClientAsyncImpl.ContainerState.Running;
					}
					catch (YarnException e)
					{
						return OnExceptionRaised(container, @event, e);
					}
					catch (IOException e)
					{
						return OnExceptionRaised(container, @event, e);
					}
					catch (Exception t)
					{
						return OnExceptionRaised(container, @event, t);
					}
				}

				private NMClientAsyncImpl.ContainerState OnExceptionRaised(NMClientAsyncImpl.StatefulContainer
					 container, NMClientAsyncImpl.ContainerEvent @event, Exception t)
				{
					try
					{
						container.nmClientAsync.GetCallbackHandler().OnStartContainerError(@event.GetContainerId
							(), t);
					}
					catch (Exception thr)
					{
						// Don't process user created unchecked exception
						Log.Info("Unchecked exception is thrown from onStartContainerError for " + "Container "
							 + @event.GetContainerId(), thr);
					}
					return NMClientAsyncImpl.ContainerState.Failed;
				}
			}

			protected internal class StopContainerTransition : MultipleArcTransition<NMClientAsyncImpl.StatefulContainer
				, NMClientAsyncImpl.ContainerEvent, NMClientAsyncImpl.ContainerState>
			{
				public virtual NMClientAsyncImpl.ContainerState Transition(NMClientAsyncImpl.StatefulContainer
					 container, NMClientAsyncImpl.ContainerEvent @event)
				{
					ContainerId containerId = @event.GetContainerId();
					try
					{
						container.nmClientAsync.GetClient().StopContainer(containerId, @event.GetNodeId()
							);
						try
						{
							container.nmClientAsync.GetCallbackHandler().OnContainerStopped(@event.GetContainerId
								());
						}
						catch (Exception thr)
						{
							// Don't process user created unchecked exception
							Log.Info("Unchecked exception is thrown from onContainerStopped for " + "Container "
								 + @event.GetContainerId(), thr);
						}
						return NMClientAsyncImpl.ContainerState.Done;
					}
					catch (YarnException e)
					{
						return OnExceptionRaised(container, @event, e);
					}
					catch (IOException e)
					{
						return OnExceptionRaised(container, @event, e);
					}
					catch (Exception t)
					{
						return OnExceptionRaised(container, @event, t);
					}
				}

				private NMClientAsyncImpl.ContainerState OnExceptionRaised(NMClientAsyncImpl.StatefulContainer
					 container, NMClientAsyncImpl.ContainerEvent @event, Exception t)
				{
					try
					{
						container.nmClientAsync.GetCallbackHandler().OnStopContainerError(@event.GetContainerId
							(), t);
					}
					catch (Exception thr)
					{
						// Don't process user created unchecked exception
						Log.Info("Unchecked exception is thrown from onStopContainerError for " + "Container "
							 + @event.GetContainerId(), thr);
					}
					return NMClientAsyncImpl.ContainerState.Failed;
				}
			}

			protected internal class OutOfOrderTransition : SingleArcTransition<NMClientAsyncImpl.StatefulContainer
				, NMClientAsyncImpl.ContainerEvent>
			{
				protected internal const string StopBeforeStartErrorMsg = "Container was killed before it was launched";

				public virtual void Transition(NMClientAsyncImpl.StatefulContainer container, NMClientAsyncImpl.ContainerEvent
					 @event)
				{
					try
					{
						container.nmClientAsync.GetCallbackHandler().OnStartContainerError(@event.GetContainerId
							(), RPCUtil.GetRemoteException(StopBeforeStartErrorMsg));
					}
					catch (Exception thr)
					{
						// Don't process user created unchecked exception
						Log.Info("Unchecked exception is thrown from onStartContainerError for " + "Container "
							 + @event.GetContainerId(), thr);
					}
				}
			}

			private readonly NMClientAsync nmClientAsync;

			private readonly ContainerId containerId;

			private readonly StateMachine<NMClientAsyncImpl.ContainerState, NMClientAsyncImpl.ContainerEventType
				, NMClientAsyncImpl.ContainerEvent> stateMachine;

			private readonly ReentrantReadWriteLock.ReadLock readLock;

			private readonly ReentrantReadWriteLock.WriteLock writeLock;

			public StatefulContainer(NMClientAsync client, ContainerId containerId)
			{
				this.nmClientAsync = client;
				this.containerId = containerId;
				stateMachine = stateMachineFactory.Make(this);
				ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
				readLock = Lock.ReadLock();
				writeLock = Lock.WriteLock();
			}

			public virtual void Handle(NMClientAsyncImpl.ContainerEvent @event)
			{
				writeLock.Lock();
				try
				{
					try
					{
						this.stateMachine.DoTransition(@event.GetType(), @event);
					}
					catch (InvalidStateTransitonException e)
					{
						Log.Error("Can't handle this event at current state", e);
					}
				}
				finally
				{
					writeLock.Unlock();
				}
			}

			public virtual ContainerId GetContainerId()
			{
				return containerId;
			}

			public virtual NMClientAsyncImpl.ContainerState GetState()
			{
				readLock.Lock();
				try
				{
					return stateMachine.GetCurrentState();
				}
				finally
				{
					readLock.Unlock();
				}
			}
		}

		protected internal class ContainerEventProcessor : Runnable
		{
			protected internal NMClientAsyncImpl.ContainerEvent @event;

			public ContainerEventProcessor(NMClientAsyncImpl _enclosing, NMClientAsyncImpl.ContainerEvent
				 @event)
			{
				this._enclosing = _enclosing;
				this.@event = @event;
			}

			public virtual void Run()
			{
				ContainerId containerId = this.@event.GetContainerId();
				NMClientAsyncImpl.Log.Info("Processing Event " + this.@event + " for Container " 
					+ containerId);
				if (this.@event.GetType() == NMClientAsyncImpl.ContainerEventType.QueryContainer)
				{
					try
					{
						ContainerStatus containerStatus = this._enclosing.client.GetContainerStatus(containerId
							, this.@event.GetNodeId());
						try
						{
							this._enclosing.callbackHandler.OnContainerStatusReceived(containerId, containerStatus
								);
						}
						catch (Exception thr)
						{
							// Don't process user created unchecked exception
							NMClientAsyncImpl.Log.Info("Unchecked exception is thrown from onContainerStatusReceived"
								 + " for Container " + this.@event.GetContainerId(), thr);
						}
					}
					catch (YarnException e)
					{
						this.OnExceptionRaised(containerId, e);
					}
					catch (IOException e)
					{
						this.OnExceptionRaised(containerId, e);
					}
					catch (Exception t)
					{
						this.OnExceptionRaised(containerId, t);
					}
				}
				else
				{
					NMClientAsyncImpl.StatefulContainer container = this._enclosing.containers[containerId
						];
					if (container == null)
					{
						NMClientAsyncImpl.Log.Info("Container " + containerId + " is already stopped or failed"
							);
					}
					else
					{
						container.Handle(this.@event);
						if (this._enclosing.IsCompletelyDone(container))
						{
							Sharpen.Collections.Remove(this._enclosing.containers, containerId);
						}
					}
				}
			}

			private void OnExceptionRaised(ContainerId containerId, Exception t)
			{
				try
				{
					this._enclosing.callbackHandler.OnGetContainerStatusError(containerId, t);
				}
				catch (Exception thr)
				{
					// Don't process user created unchecked exception
					NMClientAsyncImpl.Log.Info("Unchecked exception is thrown from onGetContainerStatusError"
						 + " for Container " + containerId, thr);
				}
			}

			private readonly NMClientAsyncImpl _enclosing;
		}
	}
}
