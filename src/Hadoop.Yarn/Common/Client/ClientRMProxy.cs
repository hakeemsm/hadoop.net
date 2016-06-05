using System;
using System.Net;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class ClientRMProxy<T> : RMProxy<T>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.ClientRMProxy
			));

		private static readonly Org.Apache.Hadoop.Yarn.Client.ClientRMProxy Instance = new 
			Org.Apache.Hadoop.Yarn.Client.ClientRMProxy();

		private interface ClientRMProtocols : ApplicationClientProtocol, ApplicationMasterProtocol
			, ResourceManagerAdministrationProtocol
		{
			// Add nothing
		}

		private ClientRMProxy()
			: base()
		{
		}

		/// <summary>Create a proxy to the ResourceManager for the specified protocol.</summary>
		/// <param name="configuration">Configuration with all the required information.</param>
		/// <param name="protocol">Client protocol for which proxy is being requested.</param>
		/// <?/>
		/// <returns>Proxy to the ResourceManager for the specified client protocol.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static T CreateRMProxy<T>(Configuration configuration)
		{
			System.Type protocol = typeof(T);
			return CreateRMProxy(configuration, protocol, Instance);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void SetAMRMTokenService(Configuration conf)
		{
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in UserGroupInformation
				.GetCurrentUser().GetTokens())
			{
				if (token.GetKind().Equals(AMRMTokenIdentifier.KindName))
				{
					token.SetService(GetAMRMTokenService(conf));
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		protected internal override IPEndPoint GetRMAddress(YarnConfiguration conf, Type 
			protocol)
		{
			if (protocol == typeof(ApplicationClientProtocol))
			{
				return conf.GetSocketAddr(YarnConfiguration.RmAddress, YarnConfiguration.DefaultRmAddress
					, YarnConfiguration.DefaultRmPort);
			}
			else
			{
				if (protocol == typeof(ResourceManagerAdministrationProtocol))
				{
					return conf.GetSocketAddr(YarnConfiguration.RmAdminAddress, YarnConfiguration.DefaultRmAdminAddress
						, YarnConfiguration.DefaultRmAdminPort);
				}
				else
				{
					if (protocol == typeof(ApplicationMasterProtocol))
					{
						SetAMRMTokenService(conf);
						return conf.GetSocketAddr(YarnConfiguration.RmSchedulerAddress, YarnConfiguration
							.DefaultRmSchedulerAddress, YarnConfiguration.DefaultRmSchedulerPort);
					}
					else
					{
						string message = "Unsupported protocol found when creating the proxy " + "connection to ResourceManager: "
							 + ((protocol != null) ? protocol.GetType().FullName : "null");
						Log.Error(message);
						throw new InvalidOperationException(message);
					}
				}
			}
		}

		[InterfaceAudience.Private]
		protected internal override void CheckAllowedProtocols(Type protocol)
		{
			Preconditions.CheckArgument(protocol.IsAssignableFrom(typeof(ClientRMProxy.ClientRMProtocols
				)), "RM does not support this client protocol");
		}

		/// <summary>Get the token service name to be used for RMDelegationToken.</summary>
		/// <remarks>
		/// Get the token service name to be used for RMDelegationToken. Depending
		/// on whether HA is enabled or not, this method generates the appropriate
		/// service name as a comma-separated list of service addresses.
		/// </remarks>
		/// <param name="conf">
		/// Configuration corresponding to the cluster we need the
		/// RMDelegationToken for
		/// </param>
		/// <returns>- Service name for RMDelegationToken</returns>
		[InterfaceStability.Unstable]
		public static Text GetRMDelegationTokenService(Configuration conf)
		{
			return GetTokenService(conf, YarnConfiguration.RmAddress, YarnConfiguration.DefaultRmAddress
				, YarnConfiguration.DefaultRmPort);
		}

		[InterfaceStability.Unstable]
		public static Text GetAMRMTokenService(Configuration conf)
		{
			return GetTokenService(conf, YarnConfiguration.RmSchedulerAddress, YarnConfiguration
				.DefaultRmSchedulerAddress, YarnConfiguration.DefaultRmSchedulerPort);
		}

		[InterfaceStability.Unstable]
		public static Text GetTokenService(Configuration conf, string address, string defaultAddr
			, int defaultPort)
		{
			if (HAUtil.IsHAEnabled(conf))
			{
				// Build a list of service addresses to form the service name
				AList<string> services = new AList<string>();
				YarnConfiguration yarnConf = new YarnConfiguration(conf);
				foreach (string rmId in HAUtil.GetRMHAIds(conf))
				{
					// Set RM_ID to get the corresponding RM_ADDRESS
					yarnConf.Set(YarnConfiguration.RmHaId, rmId);
					services.AddItem(SecurityUtil.BuildTokenService(yarnConf.GetSocketAddr(address, defaultAddr
						, defaultPort)).ToString());
				}
				return new Text(Joiner.On(',').Join(services));
			}
			// Non-HA case - no need to set RM_ID
			return SecurityUtil.BuildTokenService(conf.GetSocketAddr(address, defaultAddr, defaultPort
				));
		}
	}
}
