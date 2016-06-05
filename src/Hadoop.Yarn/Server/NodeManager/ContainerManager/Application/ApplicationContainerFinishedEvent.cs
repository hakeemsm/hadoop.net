using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application
{
	public class ApplicationContainerFinishedEvent : ApplicationEvent
	{
		private ContainerId containerID;

		public ApplicationContainerFinishedEvent(ContainerId containerID)
			: base(containerID.GetApplicationAttemptId().GetApplicationId(), ApplicationEventType
				.ApplicationContainerFinished)
		{
			this.containerID = containerID;
		}

		public virtual ContainerId GetContainerID()
		{
			return this.containerID;
		}
	}
}
