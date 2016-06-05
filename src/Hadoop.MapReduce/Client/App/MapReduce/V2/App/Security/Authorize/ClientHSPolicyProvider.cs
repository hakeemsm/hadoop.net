using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Tools;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Security.Authorize
{
	/// <summary>
	/// <see cref="Org.Apache.Hadoop.Security.Authorize.PolicyProvider"/>
	/// for MapReduce history server protocols.
	/// </summary>
	public class ClientHSPolicyProvider : PolicyProvider
	{
		private static readonly Service[] mrHSServices = new Service[] { new Service(JHAdminConfig
			.MrHsSecurityServiceAuthorization, typeof(HSClientProtocolPB)), new Service(CommonConfigurationKeys
			.HadoopSecurityServiceAuthorizationGetUserMappings, typeof(GetUserMappingsProtocol
			)), new Service(CommonConfigurationKeys.HadoopSecurityServiceAuthorizationRefreshUserMappings
			, typeof(RefreshUserMappingsProtocol)), new Service(JHAdminConfig.MrHsSecurityServiceAuthorizationAdminRefresh
			, typeof(HSAdminRefreshProtocol)) };

		public override Service[] GetServices()
		{
			return mrHSServices;
		}
	}
}
