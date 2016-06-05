using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class CMgrCompletedContainersEvent : ContainerManagerEvent
	{
		private readonly IList<ContainerId> containerToCleanup;

		private readonly CMgrCompletedContainersEvent.Reason reason;

		public CMgrCompletedContainersEvent(IList<ContainerId> containersToCleanup, CMgrCompletedContainersEvent.Reason
			 reason)
			: base(ContainerManagerEventType.FinishContainers)
		{
			this.containerToCleanup = containersToCleanup;
			this.reason = reason;
		}

		public virtual IList<ContainerId> GetContainersToCleanup()
		{
			return this.containerToCleanup;
		}

		public virtual CMgrCompletedContainersEvent.Reason GetReason()
		{
			return reason;
		}

		public enum Reason
		{
			OnShutdown,
			OnNodemanagerResync,
			ByResourcemanager
		}
	}
}
