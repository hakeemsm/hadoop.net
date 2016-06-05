using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs
{
	public class WritingContainerStartEvent : WritingApplicationHistoryEvent
	{
		private ContainerId containerId;

		private ContainerStartData containerStart;

		public WritingContainerStartEvent(ContainerId containerId, ContainerStartData containerStart
			)
			: base(WritingHistoryEventType.ContainerStart)
		{
			this.containerId = containerId;
			this.containerStart = containerStart;
		}

		public override int GetHashCode()
		{
			return containerId.GetApplicationAttemptId().GetApplicationId().GetHashCode();
		}

		public virtual ContainerId GetContainerId()
		{
			return containerId;
		}

		public virtual ContainerStartData GetContainerStartData()
		{
			return containerStart;
		}
	}
}
