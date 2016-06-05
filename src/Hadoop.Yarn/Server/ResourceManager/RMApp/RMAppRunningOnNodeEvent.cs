using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public class RMAppRunningOnNodeEvent : RMAppEvent
	{
		private readonly NodeId node;

		public RMAppRunningOnNodeEvent(ApplicationId appId, NodeId node)
			: base(appId, RMAppEventType.AppRunningOnNode)
		{
			this.node = node;
		}

		public virtual NodeId GetNodeId()
		{
			return node;
		}
	}
}
