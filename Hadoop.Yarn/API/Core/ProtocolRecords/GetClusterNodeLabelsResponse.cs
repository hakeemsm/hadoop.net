using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	public abstract class GetClusterNodeLabelsResponse
	{
		public static GetClusterNodeLabelsResponse NewInstance(ICollection<string> labels
			)
		{
			GetClusterNodeLabelsResponse request = Records.NewRecord<GetClusterNodeLabelsResponse
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
