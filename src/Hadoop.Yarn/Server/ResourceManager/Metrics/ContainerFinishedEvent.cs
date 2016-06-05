using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics
{
	public class ContainerFinishedEvent : SystemMetricsEvent
	{
		private ContainerId containerId;

		private string diagnosticsInfo;

		private int containerExitStatus;

		private ContainerState state;

		public ContainerFinishedEvent(ContainerId containerId, string diagnosticsInfo, int
			 containerExitStatus, ContainerState state, long finishedTime)
			: base(SystemMetricsEventType.ContainerFinished, finishedTime)
		{
			this.containerId = containerId;
			this.diagnosticsInfo = diagnosticsInfo;
			this.containerExitStatus = containerExitStatus;
			this.state = state;
		}

		public override int GetHashCode()
		{
			return containerId.GetApplicationAttemptId().GetApplicationId().GetHashCode();
		}

		public virtual ContainerId GetContainerId()
		{
			return containerId;
		}

		public virtual string GetDiagnosticsInfo()
		{
			return diagnosticsInfo;
		}

		public virtual int GetContainerExitStatus()
		{
			return containerExitStatus;
		}

		public virtual ContainerState GetContainerState()
		{
			return state;
		}
	}
}
