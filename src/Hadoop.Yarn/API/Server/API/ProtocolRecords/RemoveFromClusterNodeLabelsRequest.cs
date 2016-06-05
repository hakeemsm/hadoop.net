using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RemoveFromClusterNodeLabelsRequest
	{
		public static RemoveFromClusterNodeLabelsRequest NewInstance(ICollection<string> 
			labels)
		{
			RemoveFromClusterNodeLabelsRequest request = Records.NewRecord<RemoveFromClusterNodeLabelsRequest
				>();
			request.SetNodeLabels(labels);
			return request;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract void SetNodeLabels(ICollection<string> labels);

		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract ICollection<string> GetNodeLabels();
	}
}
