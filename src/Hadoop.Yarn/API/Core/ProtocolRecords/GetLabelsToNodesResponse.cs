using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	public abstract class GetLabelsToNodesResponse
	{
		public static GetLabelsToNodesResponse NewInstance(IDictionary<string, ICollection
			<NodeId>> map)
		{
			GetLabelsToNodesResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetLabelsToNodesResponse>();
			response.SetLabelsToNodes(map);
			return response;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract void SetLabelsToNodes(IDictionary<string, ICollection<NodeId>> map
			);

		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract IDictionary<string, ICollection<NodeId>> GetLabelsToNodes();
	}
}
