using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	public abstract class ContainerAllocator : EventHandler<ContainerAllocatorEvent>
	{
		public enum EventType
		{
			ContainerReq,
			ContainerDeallocate,
			ContainerFailed
		}
	}

	public static class ContainerAllocatorConstants
	{
	}
}
