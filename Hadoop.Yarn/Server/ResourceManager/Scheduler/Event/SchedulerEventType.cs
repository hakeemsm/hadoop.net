using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public enum SchedulerEventType
	{
		NodeAdded,
		NodeRemoved,
		NodeUpdate,
		NodeResourceUpdate,
		NodeLabelsUpdate,
		AppAdded,
		AppRemoved,
		AppAttemptAdded,
		AppAttemptRemoved,
		ContainerExpired,
		ContainerRescheduled,
		DropReservation,
		PreemptContainer,
		KillContainer
	}
}
