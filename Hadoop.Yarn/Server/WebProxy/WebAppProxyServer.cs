using System;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy
{
	/// <summary>
	/// ProxyServer will sit in between the end user and AppMaster
	/// web interfaces.
	/// </summary>
	public class WebAppProxyServer : CompositeService
	{
		/// <summary>Priority of the ResourceManager shutdown hook.</summary>
		public const int ShutdownHookPriority = 30;

		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Server.Webproxy.WebAppProxyServer
			));

		private WebAppProxy proxy = null;

		public WebAppProxyServer()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Webproxy.WebAppProxyServer).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			Configuration config = new YarnConfiguration(conf);
			DoSecureLogin(conf);
			proxy = new WebAppProxy();
			AddService(proxy);
			base.ServiceInit(config);
		}

		/// <summary>Log in as the Kerberose principal designated for the proxy</summary>
		/// <param name="conf">the configuration holding this information in it.</param>
		/// <exception cref="System.IO.IOException">on any error.</exception>
		protected internal virtual void DoSecureLogin(Configuration conf)
		{
			IPEndPoint socAddr = GetBindAddress(conf);
			SecurityUtil.Login(conf, YarnConfiguration.ProxyKeytab, YarnConfiguration.ProxyPrincipal
				, socAddr.GetHostName());
		}

		/// <summary>Retrieve PROXY bind address from configuration</summary>
		/// <param name="conf"/>
		/// <returns>InetSocketAddress</returns>
		public static IPEndPoint GetBindAddress(Configuration conf)
		{
			return conf.GetSocketAddr(YarnConfiguration.ProxyAddress, YarnConfiguration.DefaultProxyAddress
				, YarnConfiguration.DefaultProxyPort);
		}

		public static void Main(string[] args)
		{
			Sharpen.Thread.SetDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler
				());
			StringUtils.StartupShutdownMessage(typeof(Org.Apache.Hadoop.Yarn.Server.Webproxy.WebAppProxyServer
				), args, Log);
			try
			{
				YarnConfiguration configuration = new YarnConfiguration();
				new GenericOptionsParser(configuration, args);
				Org.Apache.Hadoop.Yarn.Server.Webproxy.WebAppProxyServer proxyServer = StartServer
					(configuration);
				proxyServer.proxy.Join();
			}
			catch (Exception t)
			{
				ExitUtil.Terminate(-1, t);
			}
		}

		/// <summary>Start proxy server.</summary>
		/// <returns>proxy server instance.</returns>
		/// <exception cref="System.Exception"/>
		protected internal static Org.Apache.Hadoop.Yarn.Server.Webproxy.WebAppProxyServer
			 StartServer(Configuration configuration)
		{
			Org.Apache.Hadoop.Yarn.Server.Webproxy.WebAppProxyServer proxy = new Org.Apache.Hadoop.Yarn.Server.Webproxy.WebAppProxyServer
				();
			ShutdownHookManager.Get().AddShutdownHook(new CompositeService.CompositeServiceShutdownHook
				(proxy), ShutdownHookPriority);
			proxy.Init(configuration);
			proxy.Start();
			return proxy;
		}
	}
}
