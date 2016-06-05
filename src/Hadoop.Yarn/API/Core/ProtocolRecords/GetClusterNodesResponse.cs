using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The response sent by the <code>ResourceManager</code> to a client
	/// requesting a
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.NodeReport"/>
	/// for all nodes.</p>
	/// <p>The <code>NodeReport</code> contains per-node information such as
	/// available resources, number of containers, tracking url, rack name, health
	/// status etc.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.NodeReport"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetClusterNodes(GetClusterNodesRequest)
	/// 	"/>
	public abstract class GetClusterNodesResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static GetClusterNodesResponse NewInstance(IList<NodeReport> nodeReports)
		{
			GetClusterNodesResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				GetClusterNodesResponse>();
			response.SetNodeReports(nodeReports);
			return response;
		}

		/// <summary>Get <code>NodeReport</code> for all nodes in the cluster.</summary>
		/// <returns><code>NodeReport</code> for all nodes in the cluster</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<NodeReport> GetNodeReports();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNodeReports(IList<NodeReport> nodeReports);
	}
}
