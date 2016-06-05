using System.Collections.Generic;
using System.IO;
using System.Text;
using Javax.Servlet;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	/// <summary>
	/// Subclass of hadoop-auth <code>AuthenticationFilter</code> that obtains its configuration
	/// from HttpFSServer's server configuration.
	/// </summary>
	public class HttpFSAuthenticationFilter : DelegationTokenAuthenticationFilter
	{
		private const string ConfPrefix = "httpfs.authentication.";

		private const string SignatureSecretFile = SignatureSecret + ".file";

		/// <summary>Returns the hadoop-auth configuration from HttpFSServer's configuration.
		/// 	</summary>
		/// <remarks>
		/// Returns the hadoop-auth configuration from HttpFSServer's configuration.
		/// <p>
		/// It returns all HttpFSServer's configuration properties prefixed with
		/// <code>httpfs.authentication</code>. The <code>httpfs.authentication</code>
		/// prefix is removed from the returned property names.
		/// </remarks>
		/// <param name="configPrefix">parameter not used.</param>
		/// <param name="filterConfig">parameter not used.</param>
		/// <returns>hadoop-auth configuration read from HttpFSServer's configuration.</returns>
		/// <exception cref="Javax.Servlet.ServletException"/>
		protected override Properties GetConfiguration(string configPrefix, FilterConfig 
			filterConfig)
		{
			Properties props = new Properties();
			Configuration conf = HttpFSServerWebApp.Get().GetConfig();
			props.SetProperty(AuthenticationFilter.CookiePath, "/");
			foreach (KeyValuePair<string, string> entry in conf)
			{
				string name = entry.Key;
				if (name.StartsWith(ConfPrefix))
				{
					string value = conf.Get(name);
					name = Sharpen.Runtime.Substring(name, ConfPrefix.Length);
					props.SetProperty(name, value);
				}
			}
			string signatureSecretFile = props.GetProperty(SignatureSecretFile, null);
			if (signatureSecretFile == null)
			{
				throw new RuntimeException("Undefined property: " + SignatureSecretFile);
			}
			try
			{
				StringBuilder secret = new StringBuilder();
				StreamReader reader = new InputStreamReader(new FileInputStream(signatureSecretFile
					), Charsets.Utf8);
				int c = reader.Read();
				while (c > -1)
				{
					secret.Append((char)c);
					c = reader.Read();
				}
				reader.Close();
				props.SetProperty(AuthenticationFilter.SignatureSecret, secret.ToString());
			}
			catch (IOException)
			{
				throw new RuntimeException("Could not read HttpFS signature secret file: " + signatureSecretFile
					);
			}
			return props;
		}

		protected override Configuration GetProxyuserConfiguration(FilterConfig filterConfig
			)
		{
			IDictionary<string, string> proxyuserConf = HttpFSServerWebApp.Get().GetConfig().
				GetValByRegex("httpfs\\.proxyuser\\.");
			Configuration conf = new Configuration(false);
			foreach (KeyValuePair<string, string> entry in proxyuserConf)
			{
				conf.Set(Sharpen.Runtime.Substring(entry.Key, "httpfs.".Length), entry.Value);
			}
			return conf;
		}
	}
}
