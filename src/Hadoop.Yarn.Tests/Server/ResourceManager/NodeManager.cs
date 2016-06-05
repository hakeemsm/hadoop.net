using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class NodeManager : ContainerManagementProtocol
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.NodeManager
			));

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private readonly string containerManagerAddress;

		private readonly string nodeHttpAddress;

		private readonly string rackName;

		private readonly NodeId nodeId;

		private readonly Resource capability;

		private readonly ResourceManager resourceManager;

		internal Resource available = recordFactory.NewRecordInstance<Resource>();

		internal Resource used = recordFactory.NewRecordInstance<Resource>();

		internal readonly ResourceTrackerService resourceTrackerService;

		internal readonly IDictionary<ApplicationId, IList<Container>> containers = new Dictionary
			<ApplicationId, IList<Container>>();

		internal readonly IDictionary<Container, ContainerStatus> containerStatusMap = new 
			Dictionary<Container, ContainerStatus>();

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public NodeManager(string hostName, int containerManagerPort, int httpPort, string
			 rackName, Resource capability, ResourceManager resourceManager)
		{
			this.containerManagerAddress = hostName + ":" + containerManagerPort;
			this.nodeHttpAddress = hostName + ":" + httpPort;
			this.rackName = rackName;
			this.resourceTrackerService = resourceManager.GetResourceTrackerService();
			this.capability = capability;
			Resources.AddTo(available, capability);
			this.nodeId = NodeId.NewInstance(hostName, containerManagerPort);
			RegisterNodeManagerRequest request = recordFactory.NewRecordInstance<RegisterNodeManagerRequest
				>();
			request.SetHttpPort(httpPort);
			request.SetResource(capability);
			request.SetNodeId(this.nodeId);
			request.SetNMVersion(YarnVersionInfo.GetVersion());
			resourceTrackerService.RegisterNodeManager(request);
			this.resourceManager = resourceManager;
			resourceManager.GetResourceScheduler().GetNodeReport(this.nodeId);
		}

		public virtual string GetHostName()
		{
			return containerManagerAddress;
		}

		public virtual string GetRackName()
		{
			return rackName;
		}

		public virtual NodeId GetNodeId()
		{
			return nodeId;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetCapability()
		{
			return capability;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetAvailable()
		{
			return available;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetUsed()
		{
			return used;
		}

		internal int responseID = 0;

		private IList<ContainerStatus> GetContainerStatuses(IDictionary<ApplicationId, IList
			<Container>> containers)
		{
			IList<ContainerStatus> containerStatuses = new AList<ContainerStatus>();
			foreach (IList<Container> appContainers in containers.Values)
			{
				foreach (Container container in appContainers)
				{
					containerStatuses.AddItem(containerStatusMap[container]);
				}
			}
			return containerStatuses;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void Heartbeat()
		{
			NodeStatus nodeStatus = Org.Apache.Hadoop.Yarn.Server.Resourcemanager.NodeManager
				.CreateNodeStatus(nodeId, GetContainerStatuses(containers));
			nodeStatus.SetResponseId(responseID);
			NodeHeartbeatRequest request = recordFactory.NewRecordInstance<NodeHeartbeatRequest
				>();
			request.SetNodeStatus(nodeStatus);
			NodeHeartbeatResponse response = resourceTrackerService.NodeHeartbeat(request);
			responseID = response.GetResponseId();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual StartContainersResponse StartContainers(StartContainersRequest requests
			)
		{
			lock (this)
			{
				foreach (StartContainerRequest request in requests.GetStartContainerRequests())
				{
					Token containerToken = request.GetContainerToken();
					ContainerTokenIdentifier tokenId = null;
					try
					{
						tokenId = BuilderUtils.NewContainerTokenIdentifier(containerToken);
					}
					catch (IOException e)
					{
						throw RPCUtil.GetRemoteException(e);
					}
					ContainerId containerID = tokenId.GetContainerID();
					ApplicationId applicationId = containerID.GetApplicationAttemptId().GetApplicationId
						();
					IList<Container> applicationContainers = containers[applicationId];
					if (applicationContainers == null)
					{
						applicationContainers = new AList<Container>();
						containers[applicationId] = applicationContainers;
					}
					// Sanity check
					foreach (Container container in applicationContainers)
					{
						if (container.GetId().CompareTo(containerID) == 0)
						{
							throw new InvalidOperationException("Container " + containerID + " already setup on node "
								 + containerManagerAddress);
						}
					}
					Container container_1 = BuilderUtils.NewContainer(containerID, this.nodeId, nodeHttpAddress
						, tokenId.GetResource(), null, null);
					// DKDC - Doesn't matter
					ContainerStatus containerStatus = BuilderUtils.NewContainerStatus(container_1.GetId
						(), ContainerState.New, string.Empty, -1000);
					applicationContainers.AddItem(container_1);
					containerStatusMap[container_1] = containerStatus;
					Resources.SubtractFrom(available, tokenId.GetResource());
					Resources.AddTo(used, tokenId.GetResource());
					if (Log.IsDebugEnabled())
					{
						Log.Debug("startContainer:" + " node=" + containerManagerAddress + " application="
							 + applicationId + " container=" + container_1 + " available=" + available + " used="
							 + used);
					}
				}
				StartContainersResponse response = StartContainersResponse.NewInstance(null, null
					, null);
				return response;
			}
		}

		public virtual void CheckResourceUsage()
		{
			lock (this)
			{
				Log.Info("Checking resource usage for " + containerManagerAddress);
				NUnit.Framework.Assert.AreEqual(available.GetMemory(), resourceManager.GetResourceScheduler
					().GetNodeReport(this.nodeId).GetAvailableResource().GetMemory());
				NUnit.Framework.Assert.AreEqual(used.GetMemory(), resourceManager.GetResourceScheduler
					().GetNodeReport(this.nodeId).GetUsedResource().GetMemory());
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual StopContainersResponse StopContainers(StopContainersRequest request
			)
		{
			lock (this)
			{
				foreach (ContainerId containerID in request.GetContainerIds())
				{
					string applicationId = containerID.GetApplicationAttemptId().GetApplicationId().GetId
						().ToString();
					// Mark the container as COMPLETE
					IList<Container> applicationContainers = containers[containerID.GetApplicationAttemptId
						().GetApplicationId()];
					foreach (Container c in applicationContainers)
					{
						if (c.GetId().CompareTo(containerID) == 0)
						{
							ContainerStatus containerStatus = containerStatusMap[c];
							containerStatus.SetState(ContainerState.Complete);
							containerStatusMap[c] = containerStatus;
						}
					}
					// Send a heartbeat
					try
					{
						Heartbeat();
					}
					catch (IOException ioe)
					{
						throw RPCUtil.GetRemoteException(ioe);
					}
					// Remove container and update status
					int ctr = 0;
					Container container = null;
					for (IEnumerator<Container> i = applicationContainers.GetEnumerator(); i.HasNext(
						); )
					{
						container = i.Next();
						if (container.GetId().CompareTo(containerID) == 0)
						{
							i.Remove();
							++ctr;
						}
					}
					if (ctr != 1)
					{
						throw new InvalidOperationException("Container " + containerID + " stopped " + ctr
							 + " times!");
					}
					Resources.AddTo(available, container.GetResource());
					Resources.SubtractFrom(used, container.GetResource());
					if (Log.IsDebugEnabled())
					{
						Log.Debug("stopContainer:" + " node=" + containerManagerAddress + " application="
							 + applicationId + " container=" + containerID + " available=" + available + " used="
							 + used);
					}
				}
				return StopContainersResponse.NewInstance(null, null);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual GetContainerStatusesResponse GetContainerStatuses(GetContainerStatusesRequest
			 request)
		{
			lock (this)
			{
				IList<ContainerStatus> statuses = new AList<ContainerStatus>();
				foreach (ContainerId containerId in request.GetContainerIds())
				{
					IList<Container> appContainers = containers[containerId.GetApplicationAttemptId()
						.GetApplicationId()];
					Container container = null;
					foreach (Container c in appContainers)
					{
						if (c.GetId().Equals(containerId))
						{
							container = c;
						}
					}
					if (container != null && containerStatusMap[container].GetState() != null)
					{
						statuses.AddItem(containerStatusMap[container]);
					}
				}
				return GetContainerStatusesResponse.NewInstance(statuses, null);
			}
		}

		public static NodeStatus CreateNodeStatus(NodeId nodeId, IList<ContainerStatus> containers
			)
		{
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
			NodeStatus nodeStatus = recordFactory.NewRecordInstance<NodeStatus>();
			nodeStatus.SetNodeId(nodeId);
			nodeStatus.SetContainersStatuses(containers);
			NodeHealthStatus nodeHealthStatus = recordFactory.NewRecordInstance<NodeHealthStatus
				>();
			nodeHealthStatus.SetIsNodeHealthy(true);
			nodeStatus.SetNodeHealthStatus(nodeHealthStatus);
			return nodeStatus;
		}
	}
}
