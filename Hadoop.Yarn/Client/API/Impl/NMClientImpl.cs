using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	/// <summary>
	/// <p>
	/// This class implements
	/// <see cref="Org.Apache.Hadoop.Yarn.Client.Api.NMClient"/>
	/// . All the APIs are blocking.
	/// </p>
	/// <p>
	/// By default, this client stops all the running containers that are started by
	/// it when it stops. It can be disabled via
	/// <see cref="CleanupRunningContainersOnStop(bool)"/>
	/// , in which case containers will
	/// continue to run even after this client is stopped and till the application
	/// runs at which point ResourceManager will forcefully kill them.
	/// </p>
	/// <p>
	/// Note that the blocking APIs ensure the RPC calls to <code>NodeManager</code>
	/// are executed immediately, and the responses are received before these APIs
	/// return. However, when
	/// <see cref="StartContainer(Org.Apache.Hadoop.Yarn.Api.Records.Container, Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext)
	/// 	"/>
	/// or
	/// <see cref="StopContainer(Org.Apache.Hadoop.Yarn.Api.Records.ContainerId, Org.Apache.Hadoop.Yarn.Api.Records.NodeId)
	/// 	"/>
	/// returns, <code>NodeManager</code> may still need some time to either start
	/// or stop the container because of its asynchronous implementation. Therefore,
	/// <see cref="GetContainerStatus(Org.Apache.Hadoop.Yarn.Api.Records.ContainerId, Org.Apache.Hadoop.Yarn.Api.Records.NodeId)
	/// 	"/>
	/// is likely to return a transit container status
	/// if it is executed immediately after
	/// <see cref="StartContainer(Org.Apache.Hadoop.Yarn.Api.Records.Container, Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext)
	/// 	"/>
	/// or
	/// <see cref="StopContainer(Org.Apache.Hadoop.Yarn.Api.Records.ContainerId, Org.Apache.Hadoop.Yarn.Api.Records.NodeId)
	/// 	"/>
	/// .
	/// </p>
	/// </summary>
	public class NMClientImpl : NMClient
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.NMClientImpl
			));

		protected internal ConcurrentMap<ContainerId, NMClientImpl.StartedContainer> startedContainers
			 = new ConcurrentHashMap<ContainerId, NMClientImpl.StartedContainer>();

		private readonly AtomicBoolean cleanupRunningContainers = new AtomicBoolean(true);

		private ContainerManagementProtocolProxy cmProxy;

		public NMClientImpl()
			: base(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.NMClientImpl).FullName)
		{
		}

		public NMClientImpl(string name)
			: base(name)
		{
		}

		// The logically coherent operations on startedContainers is synchronized to
		// ensure they are atomic
		//enabled by default
		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			// Usually, started-containers are stopped when this client stops. Unless
			// the flag cleanupRunningContainers is set to false.
			if (GetCleanupRunningContainers().Get())
			{
				CleanupRunningContainers();
			}
			cmProxy.StopAllProxies();
			base.ServiceStop();
		}

		protected internal virtual void CleanupRunningContainers()
		{
			lock (this)
			{
				foreach (NMClientImpl.StartedContainer startedContainer in startedContainers.Values)
				{
					try
					{
						StopContainer(startedContainer.GetContainerId(), startedContainer.GetNodeId());
					}
					catch (YarnException)
					{
						Log.Error("Failed to stop Container " + startedContainer.GetContainerId() + "when stopping NMClientImpl"
							);
					}
					catch (IOException)
					{
						Log.Error("Failed to stop Container " + startedContainer.GetContainerId() + "when stopping NMClientImpl"
							);
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			if (GetNMTokenCache() == null)
			{
				throw new InvalidOperationException("NMTokenCache has not been set");
			}
			cmProxy = new ContainerManagementProtocolProxy(conf, GetNMTokenCache());
		}

		public override void CleanupRunningContainersOnStop(bool enabled)
		{
			GetCleanupRunningContainers().Set(enabled);
		}

		protected internal class StartedContainer
		{
			private ContainerId containerId;

			private NodeId nodeId;

			private ContainerState state;

			public StartedContainer(ContainerId containerId, NodeId nodeId, Token containerToken
				)
			{
				this.containerId = containerId;
				this.nodeId = nodeId;
				state = ContainerState.New;
			}

			public virtual ContainerId GetContainerId()
			{
				return containerId;
			}

			public virtual NodeId GetNodeId()
			{
				return nodeId;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void AddStartingContainer(NMClientImpl.StartedContainer startedContainer)
		{
			if (startedContainers.PutIfAbsent(startedContainer.containerId, startedContainer)
				 != null)
			{
				throw RPCUtil.GetRemoteException("Container " + startedContainer.containerId.ToString
					() + " is already started");
			}
			startedContainers[startedContainer.GetContainerId()] = startedContainer;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, ByteBuffer> StartContainer(Container container
			, ContainerLaunchContext containerLaunchContext)
		{
			// Do synchronization on StartedContainer to prevent race condition
			// between startContainer and stopContainer only when startContainer is
			// in progress for a given container.
			NMClientImpl.StartedContainer startingContainer = CreateStartedContainer(container
				);
			lock (startingContainer)
			{
				AddStartingContainer(startingContainer);
				IDictionary<string, ByteBuffer> allServiceResponse;
				ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData proxy = null;
				try
				{
					proxy = cmProxy.GetProxy(container.GetNodeId().ToString(), container.GetId());
					StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
						, container.GetContainerToken());
					IList<StartContainerRequest> list = new AList<StartContainerRequest>();
					list.AddItem(scRequest);
					StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
					StartContainersResponse response = proxy.GetContainerManagementProtocol().StartContainers
						(allRequests);
					if (response.GetFailedRequests() != null && response.GetFailedRequests().Contains
						(container.GetId()))
					{
						Exception t = response.GetFailedRequests()[container.GetId()].DeSerialize();
						ParseAndThrowException(t);
					}
					allServiceResponse = response.GetAllServicesMetaData();
					startingContainer.state = ContainerState.Running;
				}
				catch (YarnException e)
				{
					startingContainer.state = ContainerState.Complete;
					// Remove the started container if it failed to start
					RemoveStartedContainer(startingContainer);
					throw;
				}
				catch (IOException e)
				{
					startingContainer.state = ContainerState.Complete;
					RemoveStartedContainer(startingContainer);
					throw;
				}
				catch (Exception t)
				{
					startingContainer.state = ContainerState.Complete;
					RemoveStartedContainer(startingContainer);
					throw RPCUtil.GetRemoteException(t);
				}
				finally
				{
					if (proxy != null)
					{
						cmProxy.MayBeCloseProxy(proxy);
					}
				}
				return allServiceResponse;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void StopContainer(ContainerId containerId, NodeId nodeId)
		{
			NMClientImpl.StartedContainer startedContainer = GetStartedContainer(containerId);
			// Only allow one request of stopping the container to move forward
			// When entering the block, check whether the precursor has already stopped
			// the container
			if (startedContainer != null)
			{
				lock (startedContainer)
				{
					if (startedContainer.state != ContainerState.Running)
					{
						return;
					}
					StopContainerInternal(containerId, nodeId);
					// Only after successful
					startedContainer.state = ContainerState.Complete;
					RemoveStartedContainer(startedContainer);
				}
			}
			else
			{
				StopContainerInternal(containerId, nodeId);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ContainerStatus GetContainerStatus(ContainerId containerId, NodeId
			 nodeId)
		{
			ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData proxy = null;
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(containerId);
			try
			{
				proxy = cmProxy.GetProxy(nodeId.ToString(), containerId);
				GetContainerStatusesResponse response = proxy.GetContainerManagementProtocol().GetContainerStatuses
					(GetContainerStatusesRequest.NewInstance(containerIds));
				if (response.GetFailedRequests() != null && response.GetFailedRequests().Contains
					(containerId))
				{
					Exception t = response.GetFailedRequests()[containerId].DeSerialize();
					ParseAndThrowException(t);
				}
				ContainerStatus containerStatus = response.GetContainerStatuses()[0];
				return containerStatus;
			}
			finally
			{
				if (proxy != null)
				{
					cmProxy.MayBeCloseProxy(proxy);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void StopContainerInternal(ContainerId containerId, NodeId nodeId)
		{
			ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData proxy = null;
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(containerId);
			try
			{
				proxy = cmProxy.GetProxy(nodeId.ToString(), containerId);
				StopContainersResponse response = proxy.GetContainerManagementProtocol().StopContainers
					(StopContainersRequest.NewInstance(containerIds));
				if (response.GetFailedRequests() != null && response.GetFailedRequests().Contains
					(containerId))
				{
					Exception t = response.GetFailedRequests()[containerId].DeSerialize();
					ParseAndThrowException(t);
				}
			}
			finally
			{
				if (proxy != null)
				{
					cmProxy.MayBeCloseProxy(proxy);
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual NMClientImpl.StartedContainer CreateStartedContainer(Container
			 container)
		{
			lock (this)
			{
				NMClientImpl.StartedContainer startedContainer = new NMClientImpl.StartedContainer
					(container.GetId(), container.GetNodeId(), container.GetContainerToken());
				return startedContainer;
			}
		}

		protected internal virtual void RemoveStartedContainer(NMClientImpl.StartedContainer
			 container)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(startedContainers, container.containerId);
			}
		}

		protected internal virtual NMClientImpl.StartedContainer GetStartedContainer(ContainerId
			 containerId)
		{
			lock (this)
			{
				return startedContainers[containerId];
			}
		}

		public virtual AtomicBoolean GetCleanupRunningContainers()
		{
			return cleanupRunningContainers;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void ParseAndThrowException(Exception t)
		{
			if (t is YarnException)
			{
				throw (YarnException)t;
			}
			else
			{
				if (t is SecretManager.InvalidToken)
				{
					throw (SecretManager.InvalidToken)t;
				}
				else
				{
					throw (IOException)t;
				}
			}
		}
	}
}
