using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class ReplaceLabelsOnNodeRequest
	{
		public static ReplaceLabelsOnNodeRequest NewInstance(IDictionary<NodeId, ICollection
			<string>> map)
		{
			ReplaceLabelsOnNodeRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ReplaceLabelsOnNodeRequest>();
			request.SetNodeToLabels(map);
			return request;
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
