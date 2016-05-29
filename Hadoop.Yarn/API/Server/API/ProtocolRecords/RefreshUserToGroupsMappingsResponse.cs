using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshUserToGroupsMappingsResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static RefreshUserToGroupsMappingsResponse NewInstance()
		{
			RefreshUserToGroupsMappingsResponse response = Records.NewRecord<RefreshUserToGroupsMappingsResponse
				>();
			return response;
		}
	}
}
