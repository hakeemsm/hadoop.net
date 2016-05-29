using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshAdminAclsRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static RefreshAdminAclsRequest NewInstance()
		{
			RefreshAdminAclsRequest request = Records.NewRecord<RefreshAdminAclsRequest>();
			return request;
		}
	}
}
