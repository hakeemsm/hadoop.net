using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class MockNM
	{
		private int responseId;

		private NodeId nodeId;

		private readonly int memory;

		private readonly int vCores;

		private ResourceTrackerService resourceTracker;

		private int httpPort = 2;

		private MasterKey currentContainerTokenMasterKey;

		private MasterKey currentNMTokenMasterKey;

		private string version;

		public MockNM(string nodeIdStr, int memory, ResourceTrackerService resourceTracker
			)
			: this(nodeIdStr, memory, Math.Max(1, (memory * YarnConfiguration.DefaultNmVcores
				) / YarnConfiguration.DefaultNmPmemMb), resourceTracker)
		{
		}

		public MockNM(string nodeIdStr, int memory, int vcores, ResourceTrackerService resourceTracker
			)
			: this(nodeIdStr, memory, vcores, resourceTracker, YarnVersionInfo.GetVersion())
		{
		}

		public MockNM(string nodeIdStr, int memory, int vcores, ResourceTrackerService resourceTracker
			, string version)
		{
			// scale vcores based on the requested memory
			this.memory = memory;
			this.vCores = vcores;
			this.resourceTracker = resourceTracker;
			this.version = version;
			string[] splits = nodeIdStr.Split(":");
			nodeId = BuilderUtils.NewNodeId(splits[0], System.Convert.ToInt32(splits[1]));
		}

		public virtual NodeId GetNodeId()
		{
			return nodeId;
		}

		public virtual int GetHttpPort()
		{
			return httpPort;
		}

		public virtual void SetHttpPort(int port)
		{
			httpPort = port;
		}

		public virtual void SetResourceTrackerService(ResourceTrackerService resourceTracker
			)
		{
			this.resourceTracker = resourceTracker;
		}

		/// <exception cref="System.Exception"/>
		public virtual void ContainerStatus(Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus
			 containerStatus)
		{
			IDictionary<ApplicationId, IList<Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus
				>> conts = new Dictionary<ApplicationId, IList<Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus
				>>();
			conts[containerStatus.GetContainerId().GetApplicationAttemptId().GetApplicationId
				()] = Arrays.AsList(new Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus[] { containerStatus
				 });
			NodeHeartbeat(conts, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual RegisterNodeManagerResponse RegisterNode()
		{
			return RegisterNode(null, null);
		}

		/// <exception cref="System.Exception"/>
		public virtual RegisterNodeManagerResponse RegisterNode(IList<ApplicationId> runningApplications
			)
		{
			return RegisterNode(null, runningApplications);
		}

		/// <exception cref="System.Exception"/>
		public virtual RegisterNodeManagerResponse RegisterNode(IList<NMContainerStatus> 
			containerReports, IList<ApplicationId> runningApplications)
		{
			RegisterNodeManagerRequest req = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<RegisterNodeManagerRequest
				>();
			req.SetNodeId(nodeId);
			req.SetHttpPort(httpPort);
			Resource resource = BuilderUtils.NewResource(memory, vCores);
			req.SetResource(resource);
			req.SetContainerStatuses(containerReports);
			req.SetNMVersion(version);
			req.SetRunningApplications(runningApplications);
			RegisterNodeManagerResponse registrationResponse = resourceTracker.RegisterNodeManager
				(req);
			this.currentContainerTokenMasterKey = registrationResponse.GetContainerTokenMasterKey
				();
			this.currentNMTokenMasterKey = registrationResponse.GetNMTokenMasterKey();
			return registrationResponse;
		}

		/// <exception cref="System.Exception"/>
		public virtual NodeHeartbeatResponse NodeHeartbeat(bool isHealthy)
		{
			return NodeHeartbeat(new Dictionary<ApplicationId, IList<Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus
				>>(), isHealthy, ++responseId);
		}

		/// <exception cref="System.Exception"/>
		public virtual NodeHeartbeatResponse NodeHeartbeat(ApplicationAttemptId attemptId
			, long containerId, ContainerState containerState)
		{
			Dictionary<ApplicationId, IList<Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus
				>> nodeUpdate = new Dictionary<ApplicationId, IList<Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus
				>>(1);
			Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus containerStatus = BuilderUtils
				.NewContainerStatus(BuilderUtils.NewContainerId(attemptId, containerId), containerState
				, "Success", 0);
			AList<Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus> containerStatusList = new 
				AList<Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus>(1);
			containerStatusList.AddItem(containerStatus);
			Org.Mortbay.Log.Log.Info("ContainerStatus: " + containerStatus);
			nodeUpdate[attemptId.GetApplicationId()] = containerStatusList;
			return NodeHeartbeat(nodeUpdate, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual NodeHeartbeatResponse NodeHeartbeat(IDictionary<ApplicationId, IList
			<Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus>> conts, bool isHealthy)
		{
			return NodeHeartbeat(conts, isHealthy, ++responseId);
		}

		/// <exception cref="System.Exception"/>
		public virtual NodeHeartbeatResponse NodeHeartbeat(IDictionary<ApplicationId, IList
			<Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus>> conts, bool isHealthy, int
			 resId)
		{
			NodeHeartbeatRequest req = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NodeHeartbeatRequest
				>();
			NodeStatus status = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NodeStatus>();
			status.SetResponseId(resId);
			status.SetNodeId(nodeId);
			foreach (KeyValuePair<ApplicationId, IList<Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus
				>> entry in conts)
			{
				Org.Mortbay.Log.Log.Info("entry.getValue() " + entry.Value);
				status.SetContainersStatuses(entry.Value);
			}
			NodeHealthStatus healthStatus = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NodeHealthStatus
				>();
			healthStatus.SetHealthReport(string.Empty);
			healthStatus.SetIsNodeHealthy(isHealthy);
			healthStatus.SetLastHealthReportTime(1);
			status.SetNodeHealthStatus(healthStatus);
			req.SetNodeStatus(status);
			req.SetLastKnownContainerTokenMasterKey(this.currentContainerTokenMasterKey);
			req.SetLastKnownNMTokenMasterKey(this.currentNMTokenMasterKey);
			NodeHeartbeatResponse heartbeatResponse = resourceTracker.NodeHeartbeat(req);
			MasterKey masterKeyFromRM = heartbeatResponse.GetContainerTokenMasterKey();
			if (masterKeyFromRM != null && masterKeyFromRM.GetKeyId() != this.currentContainerTokenMasterKey
				.GetKeyId())
			{
				this.currentContainerTokenMasterKey = masterKeyFromRM;
			}
			masterKeyFromRM = heartbeatResponse.GetNMTokenMasterKey();
			if (masterKeyFromRM != null && masterKeyFromRM.GetKeyId() != this.currentNMTokenMasterKey
				.GetKeyId())
			{
				this.currentNMTokenMasterKey = masterKeyFromRM;
			}
			return heartbeatResponse;
		}

		public virtual int GetMemory()
		{
			return memory;
		}

		public virtual int GetvCores()
		{
			return vCores;
		}
	}
}
