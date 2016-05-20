using Sharpen;

namespace org.apache.hadoop.security.authorize
{
	public class ProxyUsers
	{
		public const string CONF_HADOOP_PROXYUSER = "hadoop.proxyuser";

		private static volatile org.apache.hadoop.security.authorize.ImpersonationProvider
			 sip;

		/// <summary>Returns an instance of ImpersonationProvider.</summary>
		/// <remarks>
		/// Returns an instance of ImpersonationProvider.
		/// Looks up the configuration to see if there is custom class specified.
		/// </remarks>
		/// <param name="conf"/>
		/// <returns>ImpersonationProvider</returns>
		private static org.apache.hadoop.security.authorize.ImpersonationProvider getInstance
			(org.apache.hadoop.conf.Configuration conf)
		{
			java.lang.Class clazz = conf.getClass<org.apache.hadoop.security.authorize.ImpersonationProvider
				>(org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS
				, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authorize.DefaultImpersonationProvider
				)));
			return org.apache.hadoop.util.ReflectionUtils.newInstance(clazz, conf);
		}

		/// <summary>refresh Impersonation rules</summary>
		public static void refreshSuperUserGroupsConfiguration()
		{
			//load server side configuration;
			refreshSuperUserGroupsConfiguration(new org.apache.hadoop.conf.Configuration());
		}

		/// <summary>
		/// Refreshes configuration using the specified Proxy user prefix for
		/// properties.
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <param name="proxyUserPrefix">proxy user configuration prefix</param>
		public static void refreshSuperUserGroupsConfiguration(org.apache.hadoop.conf.Configuration
			 conf, string proxyUserPrefix)
		{
			com.google.common.@base.Preconditions.checkArgument(proxyUserPrefix != null && !proxyUserPrefix
				.isEmpty(), "prefix cannot be NULL or empty");
			// sip is volatile. Any assignment to it as well as the object's state
			// will be visible to all the other threads. 
			org.apache.hadoop.security.authorize.ImpersonationProvider ip = getInstance(conf);
			ip.init(proxyUserPrefix);
			sip = ip;
			org.apache.hadoop.security.authorize.ProxyServers.refresh(conf);
		}

		/// <summary>Refreshes configuration using the default Proxy user prefix for properties.
		/// 	</summary>
		/// <param name="conf">configuration</param>
		public static void refreshSuperUserGroupsConfiguration(org.apache.hadoop.conf.Configuration
			 conf)
		{
			refreshSuperUserGroupsConfiguration(conf, CONF_HADOOP_PROXYUSER);
		}

		/// <summary>Authorize the superuser which is doing doAs</summary>
		/// <param name="user">ugi of the effective or proxy user which contains a real user</param>
		/// <param name="remoteAddress">the ip address of client</param>
		/// <exception cref="AuthorizationException"/>
		/// <exception cref="org.apache.hadoop.security.authorize.AuthorizationException"/>
		public static void authorize(org.apache.hadoop.security.UserGroupInformation user
			, string remoteAddress)
		{
			if (sip == null)
			{
				// In a race situation, It is possible for multiple threads to satisfy this condition.
				// The last assignment will prevail.
				refreshSuperUserGroupsConfiguration();
			}
			sip.authorize(user, remoteAddress);
		}

		/// <summary>This function is kept to provide backward compatibility.</summary>
		/// <param name="user"/>
		/// <param name="remoteAddress"/>
		/// <param name="conf"/>
		/// <exception cref="AuthorizationException"/>
		/// <exception cref="org.apache.hadoop.security.authorize.AuthorizationException"/>
		[System.ObsoleteAttribute(@"use authorize(org.apache.hadoop.security.UserGroupInformation, string) instead."
			)]
		public static void authorize(org.apache.hadoop.security.UserGroupInformation user
			, string remoteAddress, org.apache.hadoop.conf.Configuration conf)
		{
			authorize(user, remoteAddress);
		}

		[com.google.common.annotations.VisibleForTesting]
		public static org.apache.hadoop.security.authorize.DefaultImpersonationProvider getDefaultImpersonationProvider
			()
		{
			return ((org.apache.hadoop.security.authorize.DefaultImpersonationProvider)sip);
		}
	}
}
