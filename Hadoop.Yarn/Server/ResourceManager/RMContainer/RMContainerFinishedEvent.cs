using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer
{
	public class RMContainerFinishedEvent : RMContainerEvent
	{
		private readonly ContainerStatus remoteContainerStatus;

		public RMContainerFinishedEvent(ContainerId containerId, ContainerStatus containerStatus
			, RMContainerEventType @event)
			: base(containerId, @event)
		{
			this.remoteContainerStatus = containerStatus;
		}

		public virtual ContainerStatus GetRemoteContainerStatus()
		{
			return this.remoteContainerStatus;
		}
	}
}
