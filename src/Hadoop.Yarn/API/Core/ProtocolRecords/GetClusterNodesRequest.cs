using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request from clients to get a report of all nodes
	/// in the cluster from the <code>ResourceManager</code>.</p>
	/// The request will ask for all nodes in the given
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.NodeState"/>
	/// s.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetClusterNodes(GetClusterNodesRequest)
	/// 	"></seealso>
	public abstract class GetClusterNodesRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetClusterNodesRequest NewInstance(EnumSet<NodeState> states)
		{
			GetClusterNodesRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetClusterNodesRequest
				>();
			request.SetNodeStates(states);
			return request;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetClusterNodesRequest NewInstance()
		{
			GetClusterNodesRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetClusterNodesRequest
				>();
			return request;
		}

		/// <summary>The state to filter the cluster nodes with.</summary>
		public abstract EnumSet<NodeState> GetNodeStates();

		/// <summary>The state to filter the cluster nodes with.</summary>
		public abstract void SetNodeStates(EnumSet<NodeState> states);
	}
}
