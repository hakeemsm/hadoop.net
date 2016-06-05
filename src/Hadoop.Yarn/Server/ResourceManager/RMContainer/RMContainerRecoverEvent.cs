using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer
{
	public class RMContainerRecoverEvent : RMContainerEvent
	{
		private readonly NMContainerStatus containerReport;

		public RMContainerRecoverEvent(ContainerId containerId, NMContainerStatus containerReport
			)
			: base(containerId, RMContainerEventType.Recover)
		{
			this.containerReport = containerReport;
		}

		public virtual NMContainerStatus GetContainerReport()
		{
			return containerReport;
		}
	}
}
