using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	public abstract class GetNodesToLabelsResponse
	{
		public static GetNodesToLabelsResponse NewInstance(IDictionary<NodeId, ICollection
			<string>> map)
		{
			GetNodesToLabelsResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetNodesToLabelsResponse>();
			response.SetNodeToLabels(map);
			return response;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract void SetNodeToLabels(IDictionary<NodeId, ICollection<string>> map
			);

		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract IDictionary<NodeId, ICollection<string>> GetNodeToLabels();
	}
}
