using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs
{
	public class WritingContainerFinishEvent : WritingApplicationHistoryEvent
	{
		private ContainerId containerId;

		private ContainerFinishData containerFinish;

		public WritingContainerFinishEvent(ContainerId containerId, ContainerFinishData containerFinish
			)
			: base(WritingHistoryEventType.ContainerFinish)
		{
			this.containerId = containerId;
			this.containerFinish = containerFinish;
		}

		public override int GetHashCode()
		{
			return containerId.GetApplicationAttemptId().GetApplicationId().GetHashCode();
		}

		public virtual ContainerId GetContainerId()
		{
			return containerId;
		}

		public virtual ContainerFinishData GetContainerFinishData()
		{
			return containerFinish;
		}
	}
}
