using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Security
{
	/// <summary>
	/// Initializes
	/// <see cref="TimelineAuthenticationFilter"/>
	/// which provides support for
	/// Kerberos HTTP SPNEGO authentication.
	/// <p>
	/// It enables Kerberos HTTP SPNEGO plus delegation token authentication for the
	/// timeline server.
	/// <p>
	/// Refer to the
	/// <c>core-default.xml</c>
	/// file, after the comment 'HTTP
	/// Authentication' for details on the configuration options. All related
	/// configuration properties have
	/// <c>hadoop.http.authentication.</c>
	/// as prefix.
	/// </summary>
	public class TimelineAuthenticationFilterInitializer : FilterInitializer
	{
		/// <summary>The configuration prefix of timeline HTTP authentication</summary>
		public const string Prefix = "yarn.timeline-service.http-authentication.";

		[VisibleForTesting]
		internal IDictionary<string, string> filterConfig;

		/// <summary>
		/// Initializes
		/// <see cref="TimelineAuthenticationFilter"/>
		/// <p>
		/// Propagates to
		/// <see cref="TimelineAuthenticationFilter"/>
		/// configuration all YARN
		/// configuration properties prefixed with
		/// <c>yarn.timeline-service.authentication.</c>
		/// </summary>
		/// <param name="container">The filter container</param>
		/// <param name="conf">Configuration for run-time parameters</param>
		public override void InitFilter(FilterContainer container, Configuration conf)
		{
			filterConfig = new Dictionary<string, string>();
			// setting the cookie path to root '/' so it is used for all resources.
			filterConfig[TimelineAuthenticationFilter.CookiePath] = "/";
			foreach (KeyValuePair<string, string> entry in conf)
			{
				string name = entry.Key;
				if (name.StartsWith(ProxyUsers.ConfHadoopProxyuser))
				{
					string value = conf.Get(name);
					name = Sharpen.Runtime.Substring(name, "hadoop.".Length);
					filterConfig[name] = value;
				}
			}
			foreach (KeyValuePair<string, string> entry_1 in conf)
			{
				string name = entry_1.Key;
				if (name.StartsWith(Prefix))
				{
					// yarn.timeline-service.http-authentication.proxyuser will override
					// hadoop.proxyuser
					string value = conf.Get(name);
					name = Sharpen.Runtime.Substring(name, Prefix.Length);
					filterConfig[name] = value;
				}
			}
			string authType = filterConfig[AuthenticationFilter.AuthType];
			if (authType.Equals(PseudoAuthenticationHandler.Type))
			{
				filterConfig[AuthenticationFilter.AuthType] = typeof(PseudoDelegationTokenAuthenticationHandler
					).FullName;
			}
			else
			{
				if (authType.Equals(KerberosAuthenticationHandler.Type))
				{
					filterConfig[AuthenticationFilter.AuthType] = typeof(KerberosDelegationTokenAuthenticationHandler
						).FullName;
					// Resolve _HOST into bind address
					string bindAddress = conf.Get(HttpServer2.BindAddress);
					string principal = filterConfig[KerberosAuthenticationHandler.Principal];
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
				}
			}
			filterConfig[DelegationTokenAuthenticationHandler.TokenKind] = TimelineDelegationTokenIdentifier
				.KindName.ToString();
			container.AddGlobalFilter("Timeline Authentication Filter", typeof(TimelineAuthenticationFilter
				).FullName, filterConfig);
		}
	}
}
