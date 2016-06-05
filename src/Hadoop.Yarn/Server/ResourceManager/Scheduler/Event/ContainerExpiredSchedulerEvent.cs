using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	/// <summary>
	/// The
	/// <see cref="SchedulerEvent"/>
	/// which notifies that a
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
	/// has expired, sent by
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer.ContainerAllocationExpirer
	/// 	"/>
	/// 
	/// </summary>
	public class ContainerExpiredSchedulerEvent : SchedulerEvent
	{
		private readonly ContainerId containerId;

		public ContainerExpiredSchedulerEvent(ContainerId containerId)
			: base(SchedulerEventType.ContainerExpired)
		{
			this.containerId = containerId;
		}

		public virtual ContainerId GetContainerId()
		{
			return containerId;
		}
	}
}
