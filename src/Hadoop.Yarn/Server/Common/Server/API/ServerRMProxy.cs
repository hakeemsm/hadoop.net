using System;
using System.Net;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	public class ServerRMProxy<T> : RMProxy<T>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Api.ServerRMProxy
			));

		private static readonly Org.Apache.Hadoop.Yarn.Server.Api.ServerRMProxy Instance = 
			new Org.Apache.Hadoop.Yarn.Server.Api.ServerRMProxy();

		private ServerRMProxy()
			: base()
		{
		}

		/// <summary>Create a proxy to the ResourceManager for the specified protocol.</summary>
		/// <param name="configuration">Configuration with all the required information.</param>
		/// <param name="protocol">Server protocol for which proxy is being requested.</param>
		/// <?/>
		/// <returns>Proxy to the ResourceManager for the specified server protocol.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static T CreateRMProxy<T>(Configuration configuration)
		{
			System.Type protocol = typeof(T);
			return CreateRMProxy(configuration, protocol, Instance);
		}

		[InterfaceAudience.Private]
		protected override IPEndPoint GetRMAddress(YarnConfiguration conf, Type protocol)
		{
			if (protocol == typeof(ResourceTracker))
			{
				return conf.GetSocketAddr(YarnConfiguration.RmResourceTrackerAddress, YarnConfiguration
					.DefaultRmResourceTrackerAddress, YarnConfiguration.DefaultRmResourceTrackerPort
					);
			}
			else
			{
				string message = "Unsupported protocol found when creating the proxy " + "connection to ResourceManager: "
					 + ((protocol != null) ? protocol.GetType().FullName : "null");
				Log.Error(message);
				throw new InvalidOperationException(message);
			}
		}

		[InterfaceAudience.Private]
		protected override void CheckAllowedProtocols(Type protocol)
		{
			Preconditions.CheckArgument(protocol.IsAssignableFrom(typeof(ResourceTracker)), "ResourceManager does not support this protocol"
				);
		}
	}
}
