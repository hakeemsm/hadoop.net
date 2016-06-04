using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// Initializes hadoop-auth AuthenticationFilter which provides support for
	/// Kerberos HTTP SPNEGO authentication.
	/// </summary>
	/// <remarks>
	/// Initializes hadoop-auth AuthenticationFilter which provides support for
	/// Kerberos HTTP SPNEGO authentication.
	/// <p/>
	/// It enables anonymous access, simple/speudo and Kerberos HTTP SPNEGO
	/// authentication  for Hadoop JobTracker, NameNode, DataNodes and
	/// TaskTrackers.
	/// <p/>
	/// Refer to the <code>core-default.xml</code> file, after the comment
	/// 'HTTP Authentication' for details on the configuration options.
	/// All related configuration properties have 'hadoop.http.authentication.'
	/// as prefix.
	/// </remarks>
	public class AuthenticationFilterInitializer : FilterInitializer
	{
		internal const string Prefix = "hadoop.http.authentication.";

		/// <summary>Initializes hadoop-auth AuthenticationFilter.</summary>
		/// <remarks>
		/// Initializes hadoop-auth AuthenticationFilter.
		/// <p/>
		/// Propagates to hadoop-auth AuthenticationFilter configuration all Hadoop
		/// configuration properties prefixed with "hadoop.http.authentication."
		/// </remarks>
		/// <param name="container">The filter container</param>
		/// <param name="conf">Configuration for run-time parameters</param>
		public override void InitFilter(FilterContainer container, Configuration conf)
		{
			IDictionary<string, string> filterConfig = GetFilterConfigMap(conf, Prefix);
			container.AddFilter("authentication", typeof(AuthenticationFilter).FullName, filterConfig
				);
		}

		public static IDictionary<string, string> GetFilterConfigMap(Configuration conf, 
			string prefix)
		{
			IDictionary<string, string> filterConfig = new Dictionary<string, string>();
			//setting the cookie path to root '/' so it is used for all resources.
			filterConfig[AuthenticationFilter.CookiePath] = "/";
			foreach (KeyValuePair<string, string> entry in conf)
			{
				string name = entry.Key;
				if (name.StartsWith(prefix))
				{
					string value = conf.Get(name);
					name = Sharpen.Runtime.Substring(name, prefix.Length);
					filterConfig[name] = value;
				}
			}
			//Resolve _HOST into bind address
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
			return filterConfig;
		}
	}
}
