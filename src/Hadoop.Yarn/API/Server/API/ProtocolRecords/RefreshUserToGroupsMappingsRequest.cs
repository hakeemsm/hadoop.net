using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshUserToGroupsMappingsRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static RefreshUserToGroupsMappingsRequest NewInstance()
		{
			RefreshUserToGroupsMappingsRequest request = Records.NewRecord<RefreshUserToGroupsMappingsRequest
				>();
			return request;
		}
	}
}
