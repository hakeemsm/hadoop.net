using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Util
{
	public class WebAppUtils
	{
		public const string WebAppTruststorePasswordKey = "ssl.server.truststore.password";

		public const string WebAppKeystorePasswordKey = "ssl.server.keystore.password";

		public const string WebAppKeyPasswordKey = "ssl.server.keystore.keypassword";

		public const string HttpsPrefix = "https://";

		public const string HttpPrefix = "http://";

		public static void SetRMWebAppPort(Configuration conf, int port)
		{
			string hostname = GetRMWebAppURLWithoutScheme(conf);
			hostname = (hostname.Contains(":")) ? Sharpen.Runtime.Substring(hostname, 0, hostname
				.IndexOf(":")) : hostname;
			SetRMWebAppHostnameAndPort(conf, hostname, port);
		}

		public static void SetRMWebAppHostnameAndPort(Configuration conf, string hostname
			, int port)
		{
			string resolvedAddress = hostname + ":" + port;
			if (YarnConfiguration.UseHttps(conf))
			{
				conf.Set(YarnConfiguration.RmWebappHttpsAddress, resolvedAddress);
			}
			else
			{
				conf.Set(YarnConfiguration.RmWebappAddress, resolvedAddress);
			}
		}

		public static void SetNMWebAppHostNameAndPort(Configuration conf, string hostName
			, int port)
		{
			if (YarnConfiguration.UseHttps(conf))
			{
				conf.Set(YarnConfiguration.NmWebappHttpsAddress, hostName + ":" + port);
			}
			else
			{
				conf.Set(YarnConfiguration.NmWebappAddress, hostName + ":" + port);
			}
		}

		public static string GetRMWebAppURLWithScheme(Configuration conf)
		{
			return GetHttpSchemePrefix(conf) + GetRMWebAppURLWithoutScheme(conf);
		}

		public static string GetRMWebAppURLWithoutScheme(Configuration conf)
		{
			if (YarnConfiguration.UseHttps(conf))
			{
				return conf.Get(YarnConfiguration.RmWebappHttpsAddress, YarnConfiguration.DefaultRmWebappHttpsAddress
					);
			}
			else
			{
				return conf.Get(YarnConfiguration.RmWebappAddress, YarnConfiguration.DefaultRmWebappAddress
					);
			}
		}

		public static IList<string> GetProxyHostsAndPortsForAmFilter(Configuration conf)
		{
			IList<string> addrs = new AList<string>();
			string proxyAddr = conf.Get(YarnConfiguration.ProxyAddress);
			// If PROXY_ADDRESS isn't set, fallback to RM_WEBAPP(_HTTPS)_ADDRESS
			// There could be multiple if using RM HA
			if (proxyAddr == null || proxyAddr.IsEmpty())
			{
				// If RM HA is enabled, try getting those addresses
				if (HAUtil.IsHAEnabled(conf))
				{
					IList<string> haAddrs = RMHAUtils.GetRMHAWebappAddresses(new YarnConfiguration(conf
						));
					foreach (string addr in haAddrs)
					{
						try
						{
							IPEndPoint socketAddr = NetUtils.CreateSocketAddr(addr);
							addrs.AddItem(GetResolvedAddress(socketAddr));
						}
						catch (ArgumentException)
						{
						}
					}
				}
				// skip if can't resolve
				// If couldn't resolve any of the addresses or not using RM HA, fallback
				if (addrs.IsEmpty())
				{
					addrs.AddItem(GetResolvedRMWebAppURLWithoutScheme(conf));
				}
			}
			else
			{
				addrs.AddItem(proxyAddr);
			}
			return addrs;
		}

		public static string GetProxyHostAndPort(Configuration conf)
		{
			string addr = conf.Get(YarnConfiguration.ProxyAddress);
			if (addr == null || addr.IsEmpty())
			{
				addr = GetResolvedRMWebAppURLWithoutScheme(conf);
			}
			return addr;
		}

		public static string GetResolvedRemoteRMWebAppURLWithScheme(Configuration conf)
		{
			return GetHttpSchemePrefix(conf) + GetResolvedRemoteRMWebAppURLWithoutScheme(conf
				);
		}

		public static string GetResolvedRMWebAppURLWithScheme(Configuration conf)
		{
			return GetHttpSchemePrefix(conf) + GetResolvedRMWebAppURLWithoutScheme(conf);
		}

		public static string GetResolvedRemoteRMWebAppURLWithoutScheme(Configuration conf
			)
		{
			return GetResolvedRemoteRMWebAppURLWithoutScheme(conf, YarnConfiguration.UseHttps
				(conf) ? HttpConfig.Policy.HttpsOnly : HttpConfig.Policy.HttpOnly);
		}

		public static string GetResolvedRMWebAppURLWithoutScheme(Configuration conf)
		{
			return GetResolvedRMWebAppURLWithoutScheme(conf, YarnConfiguration.UseHttps(conf)
				 ? HttpConfig.Policy.HttpsOnly : HttpConfig.Policy.HttpOnly);
		}

		public static string GetResolvedRMWebAppURLWithoutScheme(Configuration conf, HttpConfig.Policy
			 httpPolicy)
		{
			IPEndPoint address = null;
			if (httpPolicy == HttpConfig.Policy.HttpsOnly)
			{
				address = conf.GetSocketAddr(YarnConfiguration.RmWebappHttpsAddress, YarnConfiguration
					.DefaultRmWebappHttpsAddress, YarnConfiguration.DefaultRmWebappHttpsPort);
			}
			else
			{
				address = conf.GetSocketAddr(YarnConfiguration.RmWebappAddress, YarnConfiguration
					.DefaultRmWebappAddress, YarnConfiguration.DefaultRmWebappPort);
			}
			return GetResolvedAddress(address);
		}

		public static string GetResolvedRemoteRMWebAppURLWithoutScheme(Configuration conf
			, HttpConfig.Policy httpPolicy)
		{
			IPEndPoint address = null;
			string rmId = null;
			if (HAUtil.IsHAEnabled(conf))
			{
				// If HA enabled, pick one of the RM-IDs and rely on redirect to go to
				// the Active RM
				rmId = (string)Sharpen.Collections.ToArray(HAUtil.GetRMHAIds(conf))[0];
			}
			if (httpPolicy == HttpConfig.Policy.HttpsOnly)
			{
				address = conf.GetSocketAddr(rmId == null ? YarnConfiguration.RmWebappHttpsAddress
					 : HAUtil.AddSuffix(YarnConfiguration.RmWebappHttpsAddress, rmId), YarnConfiguration
					.DefaultRmWebappHttpsAddress, YarnConfiguration.DefaultRmWebappHttpsPort);
			}
			else
			{
				address = conf.GetSocketAddr(rmId == null ? YarnConfiguration.RmWebappAddress : HAUtil
					.AddSuffix(YarnConfiguration.RmWebappAddress, rmId), YarnConfiguration.DefaultRmWebappAddress
					, YarnConfiguration.DefaultRmWebappPort);
			}
			return GetResolvedAddress(address);
		}

		private static string GetResolvedAddress(IPEndPoint address)
		{
			address = NetUtils.GetConnectAddress(address);
			StringBuilder sb = new StringBuilder();
			IPAddress resolved = address.Address;
			if (resolved == null || resolved.IsAnyLocalAddress() || resolved.IsLoopbackAddress
				())
			{
				string lh = address.GetHostName();
				try
				{
					lh = Sharpen.Runtime.GetLocalHost().ToString();
				}
				catch (UnknownHostException)
				{
				}
				//Ignore and fallback.
				sb.Append(lh);
			}
			else
			{
				sb.Append(address.GetHostName());
			}
			sb.Append(":").Append(address.Port);
			return sb.ToString();
		}

		/// <summary>
		/// Get the URL to use for binding where bind hostname can be specified
		/// to override the hostname in the webAppURLWithoutScheme.
		/// </summary>
		/// <remarks>
		/// Get the URL to use for binding where bind hostname can be specified
		/// to override the hostname in the webAppURLWithoutScheme. Port specified in the
		/// webAppURLWithoutScheme will be used.
		/// </remarks>
		/// <param name="conf">the configuration</param>
		/// <param name="hostProperty">bind host property name</param>
		/// <param name="webAppURLWithoutScheme">web app URL without scheme String</param>
		/// <returns>String representing bind URL</returns>
		public static string GetWebAppBindURL(Configuration conf, string hostProperty, string
			 webAppURLWithoutScheme)
		{
			// If the bind-host setting exists then it overrides the hostname
			// portion of the corresponding webAppURLWithoutScheme
			string host = conf.GetTrimmed(hostProperty);
			if (host != null && !host.IsEmpty())
			{
				if (webAppURLWithoutScheme.Contains(":"))
				{
					webAppURLWithoutScheme = host + ":" + webAppURLWithoutScheme.Split(":")[1];
				}
				else
				{
					throw new YarnRuntimeException("webAppURLWithoutScheme must include port specification but doesn't: "
						 + webAppURLWithoutScheme);
				}
			}
			return webAppURLWithoutScheme;
		}

		public static string GetNMWebAppURLWithoutScheme(Configuration conf)
		{
			if (YarnConfiguration.UseHttps(conf))
			{
				return conf.Get(YarnConfiguration.NmWebappHttpsAddress, YarnConfiguration.DefaultNmWebappHttpsAddress
					);
			}
			else
			{
				return conf.Get(YarnConfiguration.NmWebappAddress, YarnConfiguration.DefaultNmWebappAddress
					);
			}
		}

		public static string GetAHSWebAppURLWithoutScheme(Configuration conf)
		{
			if (YarnConfiguration.UseHttps(conf))
			{
				return conf.Get(YarnConfiguration.TimelineServiceWebappHttpsAddress, YarnConfiguration
					.DefaultTimelineServiceWebappHttpsAddress);
			}
			else
			{
				return conf.Get(YarnConfiguration.TimelineServiceWebappAddress, YarnConfiguration
					.DefaultTimelineServiceWebappAddress);
			}
		}

		/// <summary>
		/// if url has scheme then it will be returned as it is else it will return
		/// url with scheme.
		/// </summary>
		/// <param name="schemePrefix">eg. http:// or https://</param>
		/// <param name="url"/>
		/// <returns>url with scheme</returns>
		public static string GetURLWithScheme(string schemePrefix, string url)
		{
			// If scheme is provided then it will be returned as it is
			if (url.IndexOf("://") > 0)
			{
				return url;
			}
			else
			{
				return schemePrefix + url;
			}
		}

		public static string GetRunningLogURL(string nodeHttpAddress, string containerId, 
			string user)
		{
			if (nodeHttpAddress == null || nodeHttpAddress.IsEmpty() || containerId == null ||
				 containerId.IsEmpty() || user == null || user.IsEmpty())
			{
				return null;
			}
			return StringHelper.PathJoiner.Join(nodeHttpAddress, "node", "containerlogs", containerId
				, user);
		}

		public static string GetAggregatedLogURL(string serverHttpAddress, string allocatedNode
			, string containerId, string entity, string user)
		{
			if (serverHttpAddress == null || serverHttpAddress.IsEmpty() || allocatedNode == 
				null || allocatedNode.IsEmpty() || containerId == null || containerId.IsEmpty() 
				|| entity == null || entity.IsEmpty() || user == null || user.IsEmpty())
			{
				return null;
			}
			return StringHelper.PathJoiner.Join(serverHttpAddress, "applicationhistory", "logs"
				, allocatedNode, containerId, entity, user);
		}

		/// <summary>
		/// Choose which scheme (HTTP or HTTPS) to use when generating a URL based on
		/// the configuration.
		/// </summary>
		/// <returns>the scheme (HTTP / HTTPS)</returns>
		public static string GetHttpSchemePrefix(Configuration conf)
		{
			return YarnConfiguration.UseHttps(conf) ? HttpsPrefix : HttpPrefix;
		}

		/// <summary>Load the SSL keystore / truststore into the HttpServer builder.</summary>
		/// <param name="builder">the HttpServer2.Builder to populate with ssl config</param>
		public static HttpServer2.Builder LoadSslConfiguration(HttpServer2.Builder builder
			)
		{
			return LoadSslConfiguration(builder, null);
		}

		/// <summary>Load the SSL keystore / truststore into the HttpServer builder.</summary>
		/// <param name="builder">the HttpServer2.Builder to populate with ssl config</param>
		/// <param name="sslConf">the Configuration instance to use during loading of SSL conf
		/// 	</param>
		public static HttpServer2.Builder LoadSslConfiguration(HttpServer2.Builder builder
			, Configuration sslConf)
		{
			if (sslConf == null)
			{
				sslConf = new Configuration(false);
			}
			bool needsClientAuth = YarnConfiguration.YarnSslClientHttpsNeedAuthDefault;
			sslConf.AddResource(YarnConfiguration.YarnSslServerResourceDefault);
			return builder.NeedsClientAuth(needsClientAuth).KeyPassword(GetPassword(sslConf, 
				WebAppKeyPasswordKey)).KeyStore(sslConf.Get("ssl.server.keystore.location"), GetPassword
				(sslConf, WebAppKeystorePasswordKey), sslConf.Get("ssl.server.keystore.type", "jks"
				)).TrustStore(sslConf.Get("ssl.server.truststore.location"), GetPassword(sslConf
				, WebAppTruststorePasswordKey), sslConf.Get("ssl.server.truststore.type", "jks")
				);
		}

		/// <summary>
		/// Leverages the Configuration.getPassword method to attempt to get
		/// passwords from the CredentialProvider API before falling back to
		/// clear text in config - if falling back is allowed.
		/// </summary>
		/// <param name="conf">Configuration instance</param>
		/// <param name="alias">name of the credential to retreive</param>
		/// <returns>String credential value or null</returns>
		internal static string GetPassword(Configuration conf, string alias)
		{
			string password = null;
			try
			{
				char[] passchars = conf.GetPassword(alias);
				if (passchars != null)
				{
					password = new string(passchars);
				}
			}
			catch (IOException)
			{
				password = null;
			}
			return password;
		}
	}
}
