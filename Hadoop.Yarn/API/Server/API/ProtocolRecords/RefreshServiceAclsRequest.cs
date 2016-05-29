using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshServiceAclsRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static RefreshServiceAclsRequest NewInstance()
		{
			RefreshServiceAclsRequest request = Records.NewRecord<RefreshServiceAclsRequest>(
				);
			return request;
		}
	}
}
