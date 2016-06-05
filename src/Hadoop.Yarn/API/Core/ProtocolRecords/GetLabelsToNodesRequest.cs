using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	public abstract class GetLabelsToNodesRequest
	{
		public static GetLabelsToNodesRequest NewInstance()
		{
			return Records.NewRecord<GetLabelsToNodesRequest>();
		}

		public static GetLabelsToNodesRequest NewInstance(ICollection<string> nodeLabels)
		{
			GetLabelsToNodesRequest request = Records.NewRecord<GetLabelsToNodesRequest>();
			request.SetNodeLabels(nodeLabels);
			return request;
		}

		public abstract void SetNodeLabels(ICollection<string> nodeLabels);

		public abstract ICollection<string> GetNodeLabels();
	}
}
