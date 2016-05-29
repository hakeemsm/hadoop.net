using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Records
{
	public abstract class NodeStatus
	{
		public static NodeStatus NewInstance(NodeId nodeId, int responseId, IList<ContainerStatus
			> containerStatuses, IList<ApplicationId> keepAliveApplications, NodeHealthStatus
			 nodeHealthStatus)
		{
			NodeStatus nodeStatus = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NodeStatus>
				();
			nodeStatus.SetResponseId(responseId);
			nodeStatus.SetNodeId(nodeId);
			nodeStatus.SetContainersStatuses(containerStatuses);
			nodeStatus.SetKeepAliveApplications(keepAliveApplications);
			nodeStatus.SetNodeHealthStatus(nodeHealthStatus);
			return nodeStatus;
		}

		public abstract NodeId GetNodeId();

		public abstract int GetResponseId();

		public abstract IList<ContainerStatus> GetContainersStatuses();

		public abstract void SetContainersStatuses(IList<ContainerStatus> containersStatuses
			);

		public abstract IList<ApplicationId> GetKeepAliveApplications();

		public abstract void SetKeepAliveApplications(IList<ApplicationId> appIds);

		public abstract NodeHealthStatus GetNodeHealthStatus();

		public abstract void SetNodeHealthStatus(NodeHealthStatus healthStatus);

		public abstract void SetNodeId(NodeId nodeId);

		public abstract void SetResponseId(int responseId);
	}
}
