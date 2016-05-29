using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshServiceAclsResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static RefreshServiceAclsResponse NewInstance()
		{
			RefreshServiceAclsResponse response = Records.NewRecord<RefreshServiceAclsResponse
				>();
			return response;
		}
	}
}
