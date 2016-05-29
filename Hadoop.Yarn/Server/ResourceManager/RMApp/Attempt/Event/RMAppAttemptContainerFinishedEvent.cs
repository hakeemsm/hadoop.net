using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event
{
	public class RMAppAttemptContainerFinishedEvent : RMAppAttemptEvent
	{
		private readonly ContainerStatus containerStatus;

		private readonly NodeId nodeId;

		public RMAppAttemptContainerFinishedEvent(ApplicationAttemptId appAttemptId, ContainerStatus
			 containerStatus, NodeId nodeId)
			: base(appAttemptId, RMAppAttemptEventType.ContainerFinished)
		{
			this.containerStatus = containerStatus;
			this.nodeId = nodeId;
		}

		public virtual ContainerStatus GetContainerStatus()
		{
			return this.containerStatus;
		}

		public virtual NodeId GetNodeId()
		{
			return this.nodeId;
		}
	}
}
