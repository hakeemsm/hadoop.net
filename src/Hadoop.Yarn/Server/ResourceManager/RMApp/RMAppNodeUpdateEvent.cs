using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public class RMAppNodeUpdateEvent : RMAppEvent
	{
		public enum RMAppNodeUpdateType
		{
			NodeUsable,
			NodeUnusable
		}

		private readonly RMNode node;

		private readonly RMAppNodeUpdateEvent.RMAppNodeUpdateType updateType;

		public RMAppNodeUpdateEvent(ApplicationId appId, RMNode node, RMAppNodeUpdateEvent.RMAppNodeUpdateType
			 updateType)
			: base(appId, RMAppEventType.NodeUpdate)
		{
			this.node = node;
			this.updateType = updateType;
		}

		public virtual RMNode GetNode()
		{
			return node;
		}

		public virtual RMAppNodeUpdateEvent.RMAppNodeUpdateType GetUpdateType()
		{
			return updateType;
		}
	}
}
