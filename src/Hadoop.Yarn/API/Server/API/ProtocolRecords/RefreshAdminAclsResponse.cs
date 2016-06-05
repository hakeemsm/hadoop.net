using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshAdminAclsResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static RefreshAdminAclsResponse NewInstance()
		{
			RefreshAdminAclsResponse response = Records.NewRecord<RefreshAdminAclsResponse>();
			return response;
		}
	}
}
