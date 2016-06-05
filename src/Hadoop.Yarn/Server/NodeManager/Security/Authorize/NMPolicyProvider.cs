using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security.Authorize
{
	/// <summary>
	/// <see cref="Org.Apache.Hadoop.Security.Authorize.PolicyProvider"/>
	/// for YARN NodeManager protocols.
	/// </summary>
	public class NMPolicyProvider : PolicyProvider
	{
		private static readonly Service[] nodeManagerServices = new Service[] { new Service
			(YarnConfiguration.YarnSecurityServiceAuthorizationContainerManagementProtocol, 
			typeof(ContainerManagementProtocolPB)), new Service(YarnConfiguration.YarnSecurityServiceAuthorizationResourceLocalizer
			, typeof(LocalizationProtocolPB)) };

		public override Service[] GetServices()
		{
			return nodeManagerServices;
		}
	}
}
