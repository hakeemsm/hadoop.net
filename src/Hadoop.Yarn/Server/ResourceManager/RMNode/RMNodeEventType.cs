using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	public enum RMNodeEventType
	{
		Started,
		Decommission,
		ResourceUpdate,
		StatusUpdate,
		Rebooting,
		Reconnected,
		CleanupApp,
		ContainerAllocated,
		CleanupContainer,
		FinishedContainersPulledByAm,
		Expire
	}
}
