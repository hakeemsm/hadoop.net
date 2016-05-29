using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	public class RMNodeStartedEvent : RMNodeEvent
	{
		private IList<NMContainerStatus> containerStatuses;

		private IList<ApplicationId> runningApplications;

		public RMNodeStartedEvent(NodeId nodeId, IList<NMContainerStatus> containerReports
			, IList<ApplicationId> runningApplications)
			: base(nodeId, RMNodeEventType.Started)
		{
			this.containerStatuses = containerReports;
			this.runningApplications = runningApplications;
		}

		public virtual IList<NMContainerStatus> GetNMContainerStatuses()
		{
			return this.containerStatuses;
		}

		public virtual IList<ApplicationId> GetRunningApplications()
		{
			return runningApplications;
		}
	}
}
