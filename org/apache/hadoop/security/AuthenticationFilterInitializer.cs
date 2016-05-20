using Sharpen;

namespace org.apache.hadoop.security
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
	public class AuthenticationFilterInitializer : org.apache.hadoop.http.FilterInitializer
	{
		internal const string PREFIX = "hadoop.http.authentication.";

		/// <summary>Initializes hadoop-auth AuthenticationFilter.</summary>
		/// <remarks>
		/// Initializes hadoop-auth AuthenticationFilter.
		/// <p/>
		/// Propagates to hadoop-auth AuthenticationFilter configuration all Hadoop
		/// configuration properties prefixed with "hadoop.http.authentication."
		/// </remarks>
		/// <param name="container">The filter container</param>
		/// <param name="conf">Configuration for run-time parameters</param>
		public override void initFilter(org.apache.hadoop.http.FilterContainer container, 
			org.apache.hadoop.conf.Configuration conf)
		{
			System.Collections.Generic.IDictionary<string, string> filterConfig = getFilterConfigMap
				(conf, PREFIX);
			container.addFilter("authentication", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				)).getName(), filterConfig);
		}

		public static System.Collections.Generic.IDictionary<string, string> getFilterConfigMap
			(org.apache.hadoop.conf.Configuration conf, string prefix)
		{
			System.Collections.Generic.IDictionary<string, string> filterConfig = new System.Collections.Generic.Dictionary
				<string, string>();
			//setting the cookie path to root '/' so it is used for all resources.
			filterConfig[org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.COOKIE_PATH] = "/";
			foreach (System.Collections.Generic.KeyValuePair<string, string> entry in conf)
			{
				string name = entry.Key;
				if (name.StartsWith(prefix))
				{
					string value = conf.get(name);
					name = Sharpen.Runtime.substring(name, prefix.Length);
					filterConfig[name] = value;
				}
			}
			//Resolve _HOST into bind address
			string bindAddress = conf.get(org.apache.hadoop.http.HttpServer2.BIND_ADDRESS);
			string principal = filterConfig[org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.PRINCIPAL];
			if (principal != null)
			{
				try
				{
					principal = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(principal, 
						bindAddress);
				}
				catch (System.IO.IOException ex)
				{
					throw new System.Exception("Could not resolve Kerberos principal name: " + ex.ToString
						(), ex);
				}
				filterConfig[org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
					.PRINCIPAL] = principal;
			}
			return filterConfig;
		}
	}
}
