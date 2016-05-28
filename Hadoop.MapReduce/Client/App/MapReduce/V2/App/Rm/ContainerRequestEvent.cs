using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	public class ContainerRequestEvent : ContainerAllocatorEvent
	{
		private readonly Resource capability;

		private readonly string[] hosts;

		private readonly string[] racks;

		private bool earlierAttemptFailed = false;

		public ContainerRequestEvent(TaskAttemptId attemptID, Resource capability, string
			[] hosts, string[] racks)
			: base(attemptID, ContainerAllocator.EventType.ContainerReq)
		{
			this.capability = capability;
			this.hosts = hosts;
			this.racks = racks;
		}

		internal ContainerRequestEvent(TaskAttemptId attemptID, Resource capability)
			: this(attemptID, capability, new string[0], new string[0])
		{
			this.earlierAttemptFailed = true;
		}

		public static Org.Apache.Hadoop.Mapreduce.V2.App.RM.ContainerRequestEvent CreateContainerRequestEventForFailedContainer
			(TaskAttemptId attemptID, Resource capability)
		{
			//ContainerRequest for failed events does not consider rack / node locality?
			return new Org.Apache.Hadoop.Mapreduce.V2.App.RM.ContainerRequestEvent(attemptID, 
				capability);
		}

		public virtual Resource GetCapability()
		{
			return capability;
		}

		public virtual string[] GetHosts()
		{
			return hosts;
		}

		public virtual string[] GetRacks()
		{
			return racks;
		}

		public virtual bool GetEarlierAttemptFailed()
		{
			return earlierAttemptFailed;
		}
	}
}
