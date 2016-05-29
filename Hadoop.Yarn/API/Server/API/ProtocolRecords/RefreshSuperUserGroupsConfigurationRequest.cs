using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshSuperUserGroupsConfigurationRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static RefreshSuperUserGroupsConfigurationRequest NewInstance()
		{
			RefreshSuperUserGroupsConfigurationRequest request = Records.NewRecord<RefreshSuperUserGroupsConfigurationRequest
				>();
			return request;
		}
	}
}
