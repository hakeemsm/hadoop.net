using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Security.Http
{
	public class RMAuthenticationFilterInitializer : FilterInitializer
	{
		internal string configPrefix;

		internal string kerberosPrincipalProperty;

		internal string cookiePath;

		public RMAuthenticationFilterInitializer()
		{
			this.configPrefix = "hadoop.http.authentication.";
			this.kerberosPrincipalProperty = KerberosAuthenticationHandler.Principal;
			this.cookiePath = "/";
		}

		protected internal virtual IDictionary<string, string> CreateFilterConfig(Configuration
			 conf)
		{
			IDictionary<string, string> filterConfig = new Dictionary<string, string>();
			// setting the cookie path to root '/' so it is used for all resources.
			filterConfig[AuthenticationFilter.CookiePath] = cookiePath;
			// Before conf object is passed in, RM has already processed it and used RM
			// specific configs to overwrite hadoop common ones. Hence we just need to
			// source hadoop.proxyuser configs here.
			foreach (KeyValuePair<string, string> entry in conf)
			{
				string propName = entry.Key;
				if (propName.StartsWith(configPrefix))
				{
					string value = conf.Get(propName);
					string name = Sharpen.Runtime.Substring(propName, configPrefix.Length);
					filterConfig[name] = value;
				}
				else
				{
					if (propName.StartsWith(ProxyUsers.ConfHadoopProxyuser))
					{
						string value = conf.Get(propName);
						string name = Sharpen.Runtime.Substring(propName, "hadoop.".Length);
						filterConfig[name] = value;
					}
				}
			}
			// Resolve _HOST into bind address
			string bindAddress = conf.Get(HttpServer2.BindAddress);
			string principal = filterConfig[kerberosPrincipalProperty];
			if (principal != null)
			{
				try
				{
					principal = SecurityUtil.GetServerPrincipal(principal, bindAddress);
				}
				catch (IOException ex)
				{
					throw new RuntimeException("Could not resolve Kerberos principal name: " + ex.ToString
						(), ex);
				}
				filterConfig[KerberosAuthenticationHandler.Principal] = principal;
			}
			filterConfig[DelegationTokenAuthenticationHandler.TokenKind] = RMDelegationTokenIdentifier
				.KindName.ToString();
			return filterConfig;
		}

		public override void InitFilter(FilterContainer container, Configuration conf)
		{
			IDictionary<string, string> filterConfig = CreateFilterConfig(conf);
			container.AddFilter("RMAuthenticationFilter", typeof(RMAuthenticationFilter).FullName
				, filterConfig);
		}
	}
}
