using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	public class RMNodeStatusEvent : RMNodeEvent
	{
		private readonly NodeHealthStatus nodeHealthStatus;

		private readonly IList<ContainerStatus> containersCollection;

		private readonly NodeHeartbeatResponse latestResponse;

		private readonly IList<ApplicationId> keepAliveAppIds;

		public RMNodeStatusEvent(NodeId nodeId, NodeHealthStatus nodeHealthStatus, IList<
			ContainerStatus> collection, IList<ApplicationId> keepAliveAppIds, NodeHeartbeatResponse
			 latestResponse)
			: base(nodeId, RMNodeEventType.StatusUpdate)
		{
			this.nodeHealthStatus = nodeHealthStatus;
			this.containersCollection = collection;
			this.keepAliveAppIds = keepAliveAppIds;
			this.latestResponse = latestResponse;
		}

		public virtual NodeHealthStatus GetNodeHealthStatus()
		{
			return this.nodeHealthStatus;
		}

		public virtual IList<ContainerStatus> GetContainers()
		{
			return this.containersCollection;
		}

		public virtual NodeHeartbeatResponse GetLatestResponse()
		{
			return this.latestResponse;
		}

		public virtual IList<ApplicationId> GetKeepAliveAppIds()
		{
			return this.keepAliveAppIds;
		}
	}
}
