using System;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authorize
{
	public class ProxyUsers
	{
		public const string ConfHadoopProxyuser = "hadoop.proxyuser";

		private static volatile ImpersonationProvider sip;

		/// <summary>Returns an instance of ImpersonationProvider.</summary>
		/// <remarks>
		/// Returns an instance of ImpersonationProvider.
		/// Looks up the configuration to see if there is custom class specified.
		/// </remarks>
		/// <param name="conf"/>
		/// <returns>ImpersonationProvider</returns>
		private static ImpersonationProvider GetInstance(Configuration conf)
		{
			Type clazz = conf.GetClass<ImpersonationProvider>(CommonConfigurationKeysPublic.HadoopSecurityImpersonationProviderClass
				, typeof(DefaultImpersonationProvider));
			return ReflectionUtils.NewInstance(clazz, conf);
		}

		/// <summary>refresh Impersonation rules</summary>
		public static void RefreshSuperUserGroupsConfiguration()
		{
			//load server side configuration;
			RefreshSuperUserGroupsConfiguration(new Configuration());
		}

		/// <summary>
		/// Refreshes configuration using the specified Proxy user prefix for
		/// properties.
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <param name="proxyUserPrefix">proxy user configuration prefix</param>
		public static void RefreshSuperUserGroupsConfiguration(Configuration conf, string
			 proxyUserPrefix)
		{
			Preconditions.CheckArgument(proxyUserPrefix != null && !proxyUserPrefix.IsEmpty()
				, "prefix cannot be NULL or empty");
			// sip is volatile. Any assignment to it as well as the object's state
			// will be visible to all the other threads. 
			ImpersonationProvider ip = GetInstance(conf);
			ip.Init(proxyUserPrefix);
			sip = ip;
			ProxyServers.Refresh(conf);
		}

		/// <summary>Refreshes configuration using the default Proxy user prefix for properties.
		/// 	</summary>
		/// <param name="conf">configuration</param>
		public static void RefreshSuperUserGroupsConfiguration(Configuration conf)
		{
			RefreshSuperUserGroupsConfiguration(conf, ConfHadoopProxyuser);
		}

		/// <summary>Authorize the superuser which is doing doAs</summary>
		/// <param name="user">ugi of the effective or proxy user which contains a real user</param>
		/// <param name="remoteAddress">the ip address of client</param>
		/// <exception cref="AuthorizationException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		public static void Authorize(UserGroupInformation user, string remoteAddress)
		{
			if (sip == null)
			{
				// In a race situation, It is possible for multiple threads to satisfy this condition.
				// The last assignment will prevail.
				RefreshSuperUserGroupsConfiguration();
			}
			sip.Authorize(user, remoteAddress);
		}

		/// <summary>This function is kept to provide backward compatibility.</summary>
		/// <param name="user"/>
		/// <param name="remoteAddress"/>
		/// <param name="conf"/>
		/// <exception cref="AuthorizationException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		[System.ObsoleteAttribute(@"use Authorize(Org.Apache.Hadoop.Security.UserGroupInformation, string) instead."
			)]
		public static void Authorize(UserGroupInformation user, string remoteAddress, Configuration
			 conf)
		{
			Authorize(user, remoteAddress);
		}

		[VisibleForTesting]
		public static DefaultImpersonationProvider GetDefaultImpersonationProvider()
		{
			return ((DefaultImpersonationProvider)sip);
		}
	}
}
