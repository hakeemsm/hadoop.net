using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor
{
	public class ContainersMonitorEvent : AbstractEvent<ContainersMonitorEventType>
	{
		private readonly ContainerId containerId;

		public ContainersMonitorEvent(ContainerId containerId, ContainersMonitorEventType
			 eventType)
			: base(eventType)
		{
			this.containerId = containerId;
		}

		public virtual ContainerId GetContainerId()
		{
			return this.containerId;
		}
	}
}
