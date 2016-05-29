using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class NodeHeartbeatRequest
	{
		public static NodeHeartbeatRequest NewInstance(NodeStatus nodeStatus, MasterKey lastKnownContainerTokenMasterKey
			, MasterKey lastKnownNMTokenMasterKey)
		{
			NodeHeartbeatRequest nodeHeartbeatRequest = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<NodeHeartbeatRequest>();
			nodeHeartbeatRequest.SetNodeStatus(nodeStatus);
			nodeHeartbeatRequest.SetLastKnownContainerTokenMasterKey(lastKnownContainerTokenMasterKey
				);
			nodeHeartbeatRequest.SetLastKnownNMTokenMasterKey(lastKnownNMTokenMasterKey);
			return nodeHeartbeatRequest;
		}

		public abstract NodeStatus GetNodeStatus();

		public abstract void SetNodeStatus(NodeStatus status);

		public abstract MasterKey GetLastKnownContainerTokenMasterKey();

		public abstract void SetLastKnownContainerTokenMasterKey(MasterKey secretKey);

		public abstract MasterKey GetLastKnownNMTokenMasterKey();

		public abstract void SetLastKnownNMTokenMasterKey(MasterKey secretKey);
	}
}
