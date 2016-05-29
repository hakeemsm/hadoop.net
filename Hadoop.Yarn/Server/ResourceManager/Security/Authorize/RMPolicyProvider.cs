using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.Authorize
{
	/// <summary>
	/// <see cref="Org.Apache.Hadoop.Security.Authorize.PolicyProvider"/>
	/// for YARN ResourceManager protocols.
	/// </summary>
	public class RMPolicyProvider : PolicyProvider
	{
		private static Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.Authorize.RMPolicyProvider
			 rmPolicyProvider = null;

		private RMPolicyProvider()
		{
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.Authorize.RMPolicyProvider
			 GetInstance()
		{
			if (rmPolicyProvider == null)
			{
				lock (typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.Authorize.RMPolicyProvider
					))
				{
					if (rmPolicyProvider == null)
					{
						rmPolicyProvider = new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.Authorize.RMPolicyProvider
							();
					}
				}
			}
			return rmPolicyProvider;
		}

		private static readonly Service[] resourceManagerServices = new Service[] { new Service
			(YarnConfiguration.YarnSecurityServiceAuthorizationResourcetrackerProtocol, typeof(
			ResourceTrackerPB)), new Service(YarnConfiguration.YarnSecurityServiceAuthorizationApplicationclientProtocol
			, typeof(ApplicationClientProtocolPB)), new Service(YarnConfiguration.YarnSecurityServiceAuthorizationApplicationmasterProtocol
			, typeof(ApplicationMasterProtocolPB)), new Service(YarnConfiguration.YarnSecurityServiceAuthorizationResourcemanagerAdministrationProtocol
			, typeof(ResourceManagerAdministrationProtocolPB)), new Service(YarnConfiguration
			.YarnSecurityServiceAuthorizationContainerManagementProtocol, typeof(ContainerManagementProtocolPB
			)), new Service(CommonConfigurationKeys.SecurityHaServiceProtocolAcl, typeof(HAServiceProtocol
			)) };

		public override Service[] GetServices()
		{
			return resourceManagerServices;
		}
	}
}
