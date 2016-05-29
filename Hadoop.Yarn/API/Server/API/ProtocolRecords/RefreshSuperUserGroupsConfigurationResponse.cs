using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshSuperUserGroupsConfigurationResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static RefreshSuperUserGroupsConfigurationResponse NewInstance()
		{
			RefreshSuperUserGroupsConfigurationResponse response = Records.NewRecord<RefreshSuperUserGroupsConfigurationResponse
				>();
			return response;
		}
	}
}
