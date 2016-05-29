using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy
{
	public class TestWebAppProxyServer
	{
		private WebAppProxyServer webAppProxy = null;

		private readonly string proxyAddress = "0.0.0.0:8888";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.ProxyAddress, proxyAddress);
			webAppProxy = new WebAppProxyServer();
			webAppProxy.Init(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			webAppProxy.Stop();
		}

		[NUnit.Framework.Test]
		public virtual void TestStart()
		{
			NUnit.Framework.Assert.AreEqual(Service.STATE.Inited, webAppProxy.GetServiceState
				());
			webAppProxy.Start();
			foreach (Org.Apache.Hadoop.Service.Service service in webAppProxy.GetServices())
			{
				if (service is WebAppProxy)
				{
					NUnit.Framework.Assert.AreEqual(((WebAppProxy)service).GetBindAddress(), proxyAddress
						);
				}
			}
			NUnit.Framework.Assert.AreEqual(Service.STATE.Started, webAppProxy.GetServiceState
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestBindAddress()
		{
			YarnConfiguration conf = new YarnConfiguration();
			IPEndPoint defaultBindAddress = WebAppProxyServer.GetBindAddress(conf);
			NUnit.Framework.Assert.AreEqual("Web Proxy default bind address port is incorrect"
				, YarnConfiguration.DefaultProxyPort, defaultBindAddress.Port);
		}
	}
}
