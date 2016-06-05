using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshNodesRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static RefreshNodesRequest NewInstance()
		{
			RefreshNodesRequest request = Records.NewRecord<RefreshNodesRequest>();
			return request;
		}
	}
}
