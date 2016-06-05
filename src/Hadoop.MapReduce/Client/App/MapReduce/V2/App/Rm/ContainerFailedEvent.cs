using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	public class ContainerFailedEvent : ContainerAllocatorEvent
	{
		private readonly string contMgrAddress;

		public ContainerFailedEvent(TaskAttemptId attemptID, string contMgrAddr)
			: base(attemptID, ContainerAllocator.EventType.ContainerFailed)
		{
			this.contMgrAddress = contMgrAddr;
		}

		public virtual string GetContMgrAddress()
		{
			return contMgrAddress;
		}
	}
}
