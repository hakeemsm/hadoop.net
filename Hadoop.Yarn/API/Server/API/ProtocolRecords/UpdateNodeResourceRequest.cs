using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request sent by admin to change a list of nodes' resource to the
	/// <code>ResourceManager</code>.</p>
	/// <p>The request contains details such as a map from
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.NodeId"/>
	/// to
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceOption"/>
	/// for updating the RMNodes' resources in
	/// <code>ResourceManager</code>.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Server.Api.ResourceManagerAdministrationProtocol.UpdateNodeResource(UpdateNodeResourceRequest)
	/// 	"/>
	public abstract class UpdateNodeResourceRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static UpdateNodeResourceRequest NewInstance(IDictionary<NodeId, ResourceOption
			> nodeResourceMap)
		{
			UpdateNodeResourceRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<UpdateNodeResourceRequest>();
			request.SetNodeResourceMap(nodeResourceMap);
			return request;
		}

		/// <summary>Get the map from <code>NodeId</code> to <code>ResourceOption</code>.</summary>
		/// <returns>
		/// the map of
		/// <c>&lt;NodeId, ResourceOption&gt;</c>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract IDictionary<NodeId, ResourceOption> GetNodeResourceMap();

		/// <summary>Set the map from <code>NodeId</code> to <code>ResourceOption</code>.</summary>
		/// <param name="nodeResourceMap">
		/// the map of
		/// <c>&lt;NodeId, ResourceOption&gt;</c>
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract void SetNodeResourceMap(IDictionary<NodeId, ResourceOption> nodeResourceMap
			);
	}
}
