using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	public class RMNodeResourceUpdateEvent : RMNodeEvent
	{
		private readonly ResourceOption resourceOption;

		public RMNodeResourceUpdateEvent(NodeId nodeId, ResourceOption resourceOption)
			: base(nodeId, RMNodeEventType.ResourceUpdate)
		{
			this.resourceOption = resourceOption;
		}

		public virtual ResourceOption GetResourceOption()
		{
			return resourceOption;
		}
	}
}
