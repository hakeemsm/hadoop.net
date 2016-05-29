using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	public class RMNodeCleanAppEvent : RMNodeEvent
	{
		private ApplicationId appId;

		public RMNodeCleanAppEvent(NodeId nodeId, ApplicationId appId)
			: base(nodeId, RMNodeEventType.CleanupApp)
		{
			this.appId = appId;
		}

		public virtual ApplicationId GetAppId()
		{
			return this.appId;
		}
	}
}
