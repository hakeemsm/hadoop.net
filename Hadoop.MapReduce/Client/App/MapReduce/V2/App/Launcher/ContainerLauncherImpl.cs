using System;
using System.Collections.Generic;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Launcher
{
	/// <summary>This class is responsible for launching of containers.</summary>
	public class ContainerLauncherImpl : AbstractService, ContainerLauncher
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Launcher.ContainerLauncherImpl
			));

		private ConcurrentHashMap<ContainerId, ContainerLauncherImpl.Container> containers
			 = new ConcurrentHashMap<ContainerId, ContainerLauncherImpl.Container>();

		private readonly AppContext context;

		protected internal ThreadPoolExecutor launcherPool;

		protected internal int initialPoolSize;

		private int limitOnPoolSize;

		private Sharpen.Thread eventHandlingThread;

		protected internal BlockingQueue<ContainerLauncherEvent> eventQueue = new LinkedBlockingQueue
			<ContainerLauncherEvent>();

		private readonly AtomicBoolean stopped;

		private ContainerManagementProtocolProxy cmProxy;

		private ContainerLauncherImpl.Container GetContainer(ContainerLauncherEvent @event
			)
		{
			ContainerId id = @event.GetContainerID();
			ContainerLauncherImpl.Container c = containers[id];
			if (c == null)
			{
				c = new ContainerLauncherImpl.Container(this, @event.GetTaskAttemptID(), @event.GetContainerID
					(), @event.GetContainerMgrAddress());
				ContainerLauncherImpl.Container old = containers.PutIfAbsent(id, c);
				if (old != null)
				{
					c = old;
				}
			}
			return c;
		}

		private void RemoveContainerIfDone(ContainerId id)
		{
			ContainerLauncherImpl.Container c = containers[id];
			if (c != null && c.IsCompletelyDone())
			{
				Sharpen.Collections.Remove(containers, id);
			}
		}

		private enum ContainerState
		{
			Prep,
			Failed,
			Running,
			Done,
			KilledBeforeLaunch
		}

		private class Container
		{
			private ContainerLauncherImpl.ContainerState state;

			private TaskAttemptId taskAttemptID;

			private ContainerId containerID;

			private readonly string containerMgrAddress;

			public Container(ContainerLauncherImpl _enclosing, TaskAttemptId taId, ContainerId
				 containerID, string containerMgrAddress)
			{
				this._enclosing = _enclosing;
				// store enough information to be able to cleanup the container
				this.state = ContainerLauncherImpl.ContainerState.Prep;
				this.taskAttemptID = taId;
				this.containerMgrAddress = containerMgrAddress;
				this.containerID = containerID;
			}

			public virtual bool IsCompletelyDone()
			{
				lock (this)
				{
					return this.state == ContainerLauncherImpl.ContainerState.Done || this.state == ContainerLauncherImpl.ContainerState
						.Failed;
				}
			}

			public virtual void Launch(ContainerRemoteLaunchEvent @event)
			{
				lock (this)
				{
					ContainerLauncherImpl.Log.Info("Launching " + this.taskAttemptID);
					if (this.state == ContainerLauncherImpl.ContainerState.KilledBeforeLaunch)
					{
						this.state = ContainerLauncherImpl.ContainerState.Done;
						this._enclosing.SendContainerLaunchFailedMsg(this.taskAttemptID, "Container was killed before it was launched"
							);
						return;
					}
					ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData proxy = null;
					try
					{
						proxy = this._enclosing.GetCMProxy(this.containerMgrAddress, this.containerID);
						// Construct the actual Container
						ContainerLaunchContext containerLaunchContext = @event.GetContainerLaunchContext(
							);
						// Now launch the actual container
						StartContainerRequest startRequest = StartContainerRequest.NewInstance(containerLaunchContext
							, @event.GetContainerToken());
						IList<StartContainerRequest> list = new AList<StartContainerRequest>();
						list.AddItem(startRequest);
						StartContainersRequest requestList = StartContainersRequest.NewInstance(list);
						StartContainersResponse response = proxy.GetContainerManagementProtocol().StartContainers
							(requestList);
						if (response.GetFailedRequests() != null && response.GetFailedRequests().Contains
							(this.containerID))
						{
							throw response.GetFailedRequests()[this.containerID].DeSerialize();
						}
						ByteBuffer portInfo = response.GetAllServicesMetaData()[ShuffleHandler.MapreduceShuffleServiceid
							];
						int port = -1;
						if (portInfo != null)
						{
							port = ShuffleHandler.DeserializeMetaData(portInfo);
						}
						ContainerLauncherImpl.Log.Info("Shuffle port returned by ContainerManager for " +
							 this.taskAttemptID + " : " + port);
						if (port < 0)
						{
							this.state = ContainerLauncherImpl.ContainerState.Failed;
							throw new InvalidOperationException("Invalid shuffle port number " + port + " returned for "
								 + this.taskAttemptID);
						}
						// after launching, send launched event to task attempt to move
						// it from ASSIGNED to RUNNING state
						this._enclosing.context.GetEventHandler().Handle(new TaskAttemptContainerLaunchedEvent
							(this.taskAttemptID, port));
						this.state = ContainerLauncherImpl.ContainerState.Running;
					}
					catch (Exception t)
					{
						string message = "Container launch failed for " + this.containerID + " : " + StringUtils
							.StringifyException(t);
						this.state = ContainerLauncherImpl.ContainerState.Failed;
						this._enclosing.SendContainerLaunchFailedMsg(this.taskAttemptID, message);
					}
					finally
					{
						if (proxy != null)
						{
							this._enclosing.cmProxy.MayBeCloseProxy(proxy);
						}
					}
				}
			}

			public virtual void Kill()
			{
				lock (this)
				{
					if (this.state == ContainerLauncherImpl.ContainerState.Prep)
					{
						this.state = ContainerLauncherImpl.ContainerState.KilledBeforeLaunch;
					}
					else
					{
						if (!this.IsCompletelyDone())
						{
							ContainerLauncherImpl.Log.Info("KILLING " + this.taskAttemptID);
							ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData proxy = null;
							try
							{
								proxy = this._enclosing.GetCMProxy(this.containerMgrAddress, this.containerID);
								// kill the remote container if already launched
								IList<ContainerId> ids = new AList<ContainerId>();
								ids.AddItem(this.containerID);
								StopContainersRequest request = StopContainersRequest.NewInstance(ids);
								StopContainersResponse response = proxy.GetContainerManagementProtocol().StopContainers
									(request);
								if (response.GetFailedRequests() != null && response.GetFailedRequests().Contains
									(this.containerID))
								{
									throw response.GetFailedRequests()[this.containerID].DeSerialize();
								}
							}
							catch (Exception t)
							{
								// ignore the cleanup failure
								string message = "cleanup failed for container " + this.containerID + " : " + StringUtils
									.StringifyException(t);
								this._enclosing.context.GetEventHandler().Handle(new TaskAttemptDiagnosticsUpdateEvent
									(this.taskAttemptID, message));
								ContainerLauncherImpl.Log.Warn(message);
							}
							finally
							{
								if (proxy != null)
								{
									this._enclosing.cmProxy.MayBeCloseProxy(proxy);
								}
							}
							this.state = ContainerLauncherImpl.ContainerState.Done;
						}
					}
					// after killing, send killed event to task attempt
					this._enclosing.context.GetEventHandler().Handle(new TaskAttemptEvent(this.taskAttemptID
						, TaskAttemptEventType.TaContainerCleaned));
				}
			}

			private readonly ContainerLauncherImpl _enclosing;
		}

		public ContainerLauncherImpl(AppContext context)
			: base(typeof(ContainerLauncherImpl).FullName)
		{
			this.context = context;
			this.stopped = new AtomicBoolean(false);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.limitOnPoolSize = conf.GetInt(MRJobConfig.MrAmContainerlauncherThreadCountLimit
				, MRJobConfig.DefaultMrAmContainerlauncherThreadCountLimit);
			Log.Info("Upper limit on the thread pool size is " + this.limitOnPoolSize);
			this.initialPoolSize = conf.GetInt(MRJobConfig.MrAmContainerlauncherThreadpoolInitialSize
				, MRJobConfig.DefaultMrAmContainerlauncherThreadpoolInitialSize);
			Log.Info("The thread pool initial size is " + this.initialPoolSize);
			base.ServiceInit(conf);
			cmProxy = new ContainerManagementProtocolProxy(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat("ContainerLauncher #%d"
				).SetDaemon(true).Build();
			// Start with a default core-pool size of 10 and change it dynamically.
			launcherPool = new ThreadPoolExecutor(initialPoolSize, int.MaxValue, 1, TimeUnit.
				Hours, new LinkedBlockingQueue<Runnable>(), tf);
			eventHandlingThread = new _Thread_269(this);
			// See if we need up the pool size only if haven't reached the
			// maximum limit yet.
			// nodes where containers will run at *this* point of time. This is
			// *not* the cluster size and doesn't need to be.
			// Bump up the pool size to idealPoolSize+initialPoolSize, the
			// later is just a buffer so we are not always increasing the
			// pool-size
			// the events from the queue are handled in parallel
			// using a thread pool
			// TODO: Group launching of multiple containers to a single
			// NodeManager into a single connection
			eventHandlingThread.SetName("ContainerLauncher Event Handler");
			eventHandlingThread.Start();
			base.ServiceStart();
		}

		private sealed class _Thread_269 : Sharpen.Thread
		{
			public _Thread_269(ContainerLauncherImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				ContainerLauncherEvent @event = null;
				ICollection<string> allNodes = new HashSet<string>();
				while (!this._enclosing.stopped.Get() && !Sharpen.Thread.CurrentThread().IsInterrupted
					())
				{
					try
					{
						@event = this._enclosing.eventQueue.Take();
					}
					catch (Exception e)
					{
						if (!this._enclosing.stopped.Get())
						{
							ContainerLauncherImpl.Log.Error("Returning, interrupted : " + e);
						}
						return;
					}
					allNodes.AddItem(@event.GetContainerMgrAddress());
					int poolSize = this._enclosing.launcherPool.GetCorePoolSize();
					if (poolSize != this._enclosing.limitOnPoolSize)
					{
						int numNodes = allNodes.Count;
						int idealPoolSize = Math.Min(this._enclosing.limitOnPoolSize, numNodes);
						if (poolSize < idealPoolSize)
						{
							int newPoolSize = Math.Min(this._enclosing.limitOnPoolSize, idealPoolSize + this.
								_enclosing.initialPoolSize);
							ContainerLauncherImpl.Log.Info("Setting ContainerLauncher pool size to " + newPoolSize
								 + " as number-of-nodes to talk to is " + numNodes);
							this._enclosing.launcherPool.SetCorePoolSize(newPoolSize);
						}
					}
					this._enclosing.launcherPool.Execute(this._enclosing.CreateEventProcessor(@event)
						);
				}
			}

			private readonly ContainerLauncherImpl _enclosing;
		}

		private void ShutdownAllContainers()
		{
			foreach (ContainerLauncherImpl.Container ct in this.containers.Values)
			{
				if (ct != null)
				{
					ct.Kill();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (stopped.GetAndSet(true))
			{
				// return if already stopped
				return;
			}
			// shutdown any containers that might be left running
			ShutdownAllContainers();
			if (eventHandlingThread != null)
			{
				eventHandlingThread.Interrupt();
			}
			if (launcherPool != null)
			{
				launcherPool.ShutdownNow();
			}
			base.ServiceStop();
		}

		protected internal virtual ContainerLauncherImpl.EventProcessor CreateEventProcessor
			(ContainerLauncherEvent @event)
		{
			return new ContainerLauncherImpl.EventProcessor(this, @event);
		}

		/// <summary>Setup and start the container on remote nodemanager.</summary>
		internal class EventProcessor : Runnable
		{
			private ContainerLauncherEvent @event;

			internal EventProcessor(ContainerLauncherImpl _enclosing, ContainerLauncherEvent 
				@event)
			{
				this._enclosing = _enclosing;
				this.@event = @event;
			}

			public virtual void Run()
			{
				ContainerLauncherImpl.Log.Info("Processing the event " + this.@event.ToString());
				// Load ContainerManager tokens before creating a connection.
				// TODO: Do it only once per NodeManager.
				ContainerId containerID = this.@event.GetContainerID();
				ContainerLauncherImpl.Container c = this._enclosing.GetContainer(this.@event);
				switch (this.@event.GetType())
				{
					case ContainerLauncher.EventType.ContainerRemoteLaunch:
					{
						ContainerRemoteLaunchEvent launchEvent = (ContainerRemoteLaunchEvent)this.@event;
						c.Launch(launchEvent);
						break;
					}

					case ContainerLauncher.EventType.ContainerRemoteCleanup:
					{
						c.Kill();
						break;
					}
				}
				this._enclosing.RemoveContainerIfDone(containerID);
			}

			private readonly ContainerLauncherImpl _enclosing;
		}

		internal virtual void SendContainerLaunchFailedMsg(TaskAttemptId taskAttemptID, string
			 message)
		{
			Log.Error(message);
			context.GetEventHandler().Handle(new TaskAttemptDiagnosticsUpdateEvent(taskAttemptID
				, message));
			context.GetEventHandler().Handle(new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType
				.TaContainerLaunchFailed));
		}

		public virtual void Handle(ContainerLauncherEvent @event)
		{
			try
			{
				eventQueue.Put(@event);
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
			 GetCMProxy(string containerMgrBindAddr, ContainerId containerId)
		{
			return cmProxy.GetProxy(containerMgrBindAddr, containerId);
		}
	}
}
