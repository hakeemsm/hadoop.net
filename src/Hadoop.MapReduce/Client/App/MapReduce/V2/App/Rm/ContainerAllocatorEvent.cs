using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	public class ContainerAllocatorEvent : AbstractEvent<ContainerAllocator.EventType
		>
	{
		private TaskAttemptId attemptID;

		public ContainerAllocatorEvent(TaskAttemptId attemptID, ContainerAllocator.EventType
			 type)
			: base(type)
		{
			this.attemptID = attemptID;
		}

		public virtual TaskAttemptId GetAttemptID()
		{
			return attemptID;
		}
	}
}
