using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor
{
	public class ContainerStopMonitoringEvent : ContainersMonitorEvent
	{
		public ContainerStopMonitoringEvent(ContainerId containerId)
			: base(containerId, ContainersMonitorEventType.StopMonitoringContainer)
		{
		}
	}
}
