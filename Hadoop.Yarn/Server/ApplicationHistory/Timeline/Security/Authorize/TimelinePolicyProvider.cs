using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Security.Authorize
{
	/// <summary>
	/// <see cref="Org.Apache.Hadoop.Security.Authorize.PolicyProvider"/>
	/// for YARN timeline server protocols.
	/// </summary>
	public class TimelinePolicyProvider : PolicyProvider
	{
		public override Service[] GetServices()
		{
			return new Service[] { new Service(YarnConfiguration.YarnSecurityServiceAuthorizationApplicationhistoryProtocol
				, typeof(ApplicationHistoryProtocolPB)) };
		}
	}
}
