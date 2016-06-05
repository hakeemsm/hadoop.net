using System;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy
{
	public class WebAppProxy : AbstractService
	{
		public const string FetcherAttribute = "AppUrlFetcher";

		public const string IsSecurityEnabledAttribute = "IsSecurityEnabled";

		public const string ProxyHostAttribute = "proxyHost";

		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Server.Webproxy.WebAppProxy
			));

		private HttpServer2 proxyServer = null;

		private string bindAddress = null;

		private int port = 0;

		private AccessControlList acl = null;

		private AppReportFetcher fetcher = null;

		private bool isSecurityEnabled = false;

		private string proxyHost = null;

		public WebAppProxy()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Webproxy.WebAppProxy).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			string auth = conf.Get(CommonConfigurationKeys.HadoopSecurityAuthentication);
			if (auth == null || "simple".Equals(auth))
			{
				isSecurityEnabled = false;
			}
			else
			{
				if ("kerberos".Equals(auth))
				{
					isSecurityEnabled = true;
				}
				else
				{
					Log.Warn("Unrecongized attribute value for " + CommonConfigurationKeys.HadoopSecurityAuthentication
						 + " of " + auth);
				}
			}
			string proxy = WebAppUtils.GetProxyHostAndPort(conf);
			string[] proxyParts = proxy.Split(":");
			proxyHost = proxyParts[0];
			fetcher = new AppReportFetcher(conf);
			bindAddress = conf.Get(YarnConfiguration.ProxyAddress);
			if (bindAddress == null || bindAddress.IsEmpty())
			{
				throw new YarnRuntimeException(YarnConfiguration.ProxyAddress + " is not set so the proxy will not run."
					);
			}
			Log.Info("Instantiating Proxy at " + bindAddress);
			string[] parts = StringUtils.Split(bindAddress, ':');
			port = 0;
			if (parts.Length == 2)
			{
				bindAddress = parts[0];
				port = System.Convert.ToInt32(parts[1]);
			}
			acl = new AccessControlList(conf.Get(YarnConfiguration.YarnAdminAcl, YarnConfiguration
				.DefaultYarnAdminAcl));
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			try
			{
				Configuration conf = GetConfig();
				HttpServer2.Builder b = new HttpServer2.Builder().SetName("proxy").AddEndpoint(URI
					.Create(WebAppUtils.GetHttpSchemePrefix(conf) + bindAddress + ":" + port)).SetFindPort
					(port == 0).SetConf(GetConfig()).SetACL(acl);
				if (YarnConfiguration.UseHttps(conf))
				{
					WebAppUtils.LoadSslConfiguration(b);
				}
				proxyServer = b.Build();
				proxyServer.AddServlet(ProxyUriUtils.ProxyServletName, ProxyUriUtils.ProxyPathSpec
					, typeof(WebAppProxyServlet));
				proxyServer.SetAttribute(FetcherAttribute, fetcher);
				proxyServer.SetAttribute(IsSecurityEnabledAttribute, isSecurityEnabled);
				proxyServer.SetAttribute(ProxyHostAttribute, proxyHost);
				proxyServer.Start();
			}
			catch (IOException e)
			{
				Log.Error("Could not start proxy web server", e);
				throw;
			}
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (proxyServer != null)
			{
				try
				{
					proxyServer.Stop();
				}
				catch (Exception e)
				{
					Log.Error("Error stopping proxy web server", e);
					throw new YarnRuntimeException("Error stopping proxy web server", e);
				}
			}
			if (this.fetcher != null)
			{
				this.fetcher.Stop();
			}
			base.ServiceStop();
		}

		public virtual void Join()
		{
			if (proxyServer != null)
			{
				try
				{
					proxyServer.Join();
				}
				catch (Exception)
				{
				}
			}
		}

		// ignored
		[VisibleForTesting]
		internal virtual string GetBindAddress()
		{
			return bindAddress + ":" + port;
		}
	}
}
