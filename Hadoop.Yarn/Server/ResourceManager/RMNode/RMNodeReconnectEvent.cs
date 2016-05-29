using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	public class RMNodeReconnectEvent : RMNodeEvent
	{
		private RMNode reconnectedNode;

		private IList<ApplicationId> runningApplications;

		private IList<NMContainerStatus> containerStatuses;

		public RMNodeReconnectEvent(NodeId nodeId, RMNode newNode, IList<ApplicationId> runningApps
			, IList<NMContainerStatus> containerReports)
			: base(nodeId, RMNodeEventType.Reconnected)
		{
			reconnectedNode = newNode;
			runningApplications = runningApps;
			containerStatuses = containerReports;
		}

		public virtual RMNode GetReconnectedNode()
		{
			return reconnectedNode;
		}

		public virtual IList<ApplicationId> GetRunningApplications()
		{
			return runningApplications;
		}

		public virtual IList<NMContainerStatus> GetNMContainerStatuses()
		{
			return containerStatuses;
		}
	}
}
