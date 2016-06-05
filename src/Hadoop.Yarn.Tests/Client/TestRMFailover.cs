using System;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Webproxy;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class TestRMFailover : ClientBaseWithFixes
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestRMFailover).FullName
			);

		private static readonly HAServiceProtocol.StateChangeRequestInfo req = new HAServiceProtocol.StateChangeRequestInfo
			(HAServiceProtocol.RequestSource.RequestByUser);

		private const string Rm1NodeId = "rm1";

		private const int Rm1PortBase = 10000;

		private const string Rm2NodeId = "rm2";

		private const int Rm2PortBase = 20000;

		private Configuration conf;

		private MiniYARNCluster cluster;

		private ApplicationId fakeAppId;

		private void SetConfForRM(string rmId, string prefix, string value)
		{
			conf.Set(HAUtil.AddSuffix(prefix, rmId), value);
		}

		private void SetRpcAddressForRM(string rmId, int @base)
		{
			SetConfForRM(rmId, YarnConfiguration.RmAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmPort));
			SetConfForRM(rmId, YarnConfiguration.RmSchedulerAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmSchedulerPort));
			SetConfForRM(rmId, YarnConfiguration.RmAdminAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmAdminPort));
			SetConfForRM(rmId, YarnConfiguration.RmResourceTrackerAddress, "0.0.0.0:" + (@base
				 + YarnConfiguration.DefaultRmResourceTrackerPort));
			SetConfForRM(rmId, YarnConfiguration.RmWebappAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmWebappPort));
			SetConfForRM(rmId, YarnConfiguration.RmWebappHttpsAddress, "0.0.0.0:" + (@base + 
				YarnConfiguration.DefaultRmWebappHttpsPort));
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			fakeAppId = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 0);
			conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.Set(YarnConfiguration.RmHaIds, Rm1NodeId + "," + Rm2NodeId);
			SetRpcAddressForRM(Rm1NodeId, Rm1PortBase);
			SetRpcAddressForRM(Rm2NodeId, Rm2PortBase);
			conf.SetLong(YarnConfiguration.ClientFailoverSleeptimeBaseMs, 100L);
			conf.SetBoolean(YarnConfiguration.YarnMiniclusterFixedPorts, true);
			conf.SetBoolean(YarnConfiguration.YarnMiniclusterUseRpc, true);
			cluster = new MiniYARNCluster(typeof(TestRMFailover).FullName, 2, 1, 1, 1);
		}

		[TearDown]
		public virtual void Teardown()
		{
			cluster.Stop();
		}

		private void VerifyClientConnection()
		{
			int numRetries = 3;
			while (numRetries-- > 0)
			{
				Configuration conf = new YarnConfiguration(this.conf);
				YarnClient client = YarnClient.CreateYarnClient();
				client.Init(conf);
				client.Start();
				try
				{
					client.GetApplications();
					return;
				}
				catch (Exception e)
				{
					Log.Error(e);
				}
				finally
				{
					client.Stop();
				}
			}
			NUnit.Framework.Assert.Fail("Client couldn't connect to the Active RM");
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void VerifyConnections()
		{
			NUnit.Framework.Assert.IsTrue("NMs failed to connect to the RM", cluster.WaitForNodeManagersToConnect
				(20000));
			VerifyClientConnection();
		}

		private AdminService GetAdminService(int index)
		{
			return cluster.GetResourceManager(index).GetRMContext().GetRMAdminService();
		}

		/// <exception cref="System.IO.IOException"/>
		private void ExplicitFailover()
		{
			int activeRMIndex = cluster.GetActiveRMIndex();
			int newActiveRMIndex = (activeRMIndex + 1) % 2;
			GetAdminService(activeRMIndex).TransitionToStandby(req);
			GetAdminService(newActiveRMIndex).TransitionToActive(req);
			NUnit.Framework.Assert.AreEqual("Failover failed", newActiveRMIndex, cluster.GetActiveRMIndex
				());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void Failover()
		{
			int activeRMIndex = cluster.GetActiveRMIndex();
			cluster.StopResourceManager(activeRMIndex);
			NUnit.Framework.Assert.AreEqual("Failover failed", (activeRMIndex + 1) % 2, cluster
				.GetActiveRMIndex());
			cluster.RestartResourceManager(activeRMIndex);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestExplicitFailover()
		{
			conf.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			cluster.Init(conf);
			cluster.Start();
			GetAdminService(0).TransitionToActive(req);
			NUnit.Framework.Assert.IsFalse("RM never turned active", -1 == cluster.GetActiveRMIndex
				());
			VerifyConnections();
			ExplicitFailover();
			VerifyConnections();
			ExplicitFailover();
			VerifyConnections();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAutomaticFailover()
		{
			conf.Set(YarnConfiguration.RmClusterId, "yarn-test-cluster");
			conf.Set(YarnConfiguration.RmZkAddress, hostPort);
			conf.SetInt(YarnConfiguration.RmZkTimeoutMs, 2000);
			cluster.Init(conf);
			cluster.Start();
			NUnit.Framework.Assert.IsFalse("RM never turned active", -1 == cluster.GetActiveRMIndex
				());
			VerifyConnections();
			Failover();
			VerifyConnections();
			Failover();
			VerifyConnections();
			// Make the current Active handle an RMFatalEvent,
			// so it transitions to standby.
			ResourceManager rm = cluster.GetResourceManager(cluster.GetActiveRMIndex());
			rm.HandleTransitionToStandBy();
			int maxWaitingAttempts = 2000;
			while (maxWaitingAttempts-- > 0)
			{
				if (rm.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState.Standby)
				{
					break;
				}
				Sharpen.Thread.Sleep(1);
			}
			NUnit.Framework.Assert.IsFalse("RM didn't transition to Standby ", maxWaitingAttempts
				 == 0);
			VerifyConnections();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWebAppProxyInStandAloneMode()
		{
			conf.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			WebAppProxyServer webAppProxyServer = new WebAppProxyServer();
			try
			{
				conf.Set(YarnConfiguration.ProxyAddress, "0.0.0.0:9099");
				cluster.Init(conf);
				cluster.Start();
				GetAdminService(0).TransitionToActive(req);
				NUnit.Framework.Assert.IsFalse("RM never turned active", -1 == cluster.GetActiveRMIndex
					());
				VerifyConnections();
				webAppProxyServer.Init(conf);
				// Start webAppProxyServer
				NUnit.Framework.Assert.AreEqual(Service.STATE.Inited, webAppProxyServer.GetServiceState
					());
				webAppProxyServer.Start();
				NUnit.Framework.Assert.AreEqual(Service.STATE.Started, webAppProxyServer.GetServiceState
					());
				// send httpRequest with fakeApplicationId
				// expect to get "Not Found" response and 404 response code
				Uri wrongUrl = new Uri("http://0.0.0.0:9099/proxy/" + fakeAppId);
				HttpURLConnection proxyConn = (HttpURLConnection)wrongUrl.OpenConnection();
				proxyConn.Connect();
				VerifyResponse(proxyConn);
				ExplicitFailover();
				VerifyConnections();
				proxyConn.Connect();
				VerifyResponse(proxyConn);
			}
			finally
			{
				webAppProxyServer.Stop();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEmbeddedWebAppProxy()
		{
			conf.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			cluster.Init(conf);
			cluster.Start();
			GetAdminService(0).TransitionToActive(req);
			NUnit.Framework.Assert.IsFalse("RM never turned active", -1 == cluster.GetActiveRMIndex
				());
			VerifyConnections();
			// send httpRequest with fakeApplicationId
			// expect to get "Not Found" response and 404 response code
			Uri wrongUrl = new Uri("http://0.0.0.0:18088/proxy/" + fakeAppId);
			HttpURLConnection proxyConn = (HttpURLConnection)wrongUrl.OpenConnection();
			proxyConn.Connect();
			VerifyResponse(proxyConn);
			ExplicitFailover();
			VerifyConnections();
			proxyConn.Connect();
			VerifyResponse(proxyConn);
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyResponse(HttpURLConnection response)
		{
			NUnit.Framework.Assert.AreEqual("Not Found", response.GetResponseMessage());
			NUnit.Framework.Assert.AreEqual(404, response.GetResponseCode());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRMWebAppRedirect()
		{
			cluster = new MiniYARNCluster(typeof(TestRMFailover).FullName, 2, 0, 1, 1);
			conf.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			cluster.Init(conf);
			cluster.Start();
			GetAdminService(0).TransitionToActive(req);
			string rm1Url = "http://0.0.0.0:18088";
			string rm2Url = "http://0.0.0.0:28088";
			string redirectURL = GetRedirectURL(rm2Url);
			// if uri is null, RMWebAppFilter will append a slash at the trail of the redirection url
			NUnit.Framework.Assert.AreEqual(redirectURL, rm1Url + "/");
			redirectURL = GetRedirectURL(rm2Url + "/metrics");
			NUnit.Framework.Assert.AreEqual(redirectURL, rm1Url + "/metrics");
			redirectURL = GetRedirectURL(rm2Url + "/jmx");
			NUnit.Framework.Assert.AreEqual(redirectURL, rm1Url + "/jmx");
			// standby RM links /conf, /stacks, /logLevel, /static, /logs,
			// /cluster/cluster as well as webService
			// /ws/v1/cluster/info should not be redirected to active RM
			redirectURL = GetRedirectURL(rm2Url + "/cluster/cluster");
			NUnit.Framework.Assert.IsNull(redirectURL);
			redirectURL = GetRedirectURL(rm2Url + "/conf");
			NUnit.Framework.Assert.IsNull(redirectURL);
			redirectURL = GetRedirectURL(rm2Url + "/stacks");
			NUnit.Framework.Assert.IsNull(redirectURL);
			redirectURL = GetRedirectURL(rm2Url + "/logLevel");
			NUnit.Framework.Assert.IsNull(redirectURL);
			redirectURL = GetRedirectURL(rm2Url + "/static");
			NUnit.Framework.Assert.IsNull(redirectURL);
			redirectURL = GetRedirectURL(rm2Url + "/logs");
			NUnit.Framework.Assert.IsNull(redirectURL);
			redirectURL = GetRedirectURL(rm2Url + "/ws/v1/cluster/info");
			NUnit.Framework.Assert.IsNull(redirectURL);
			redirectURL = GetRedirectURL(rm2Url + "/ws/v1/cluster/apps");
			NUnit.Framework.Assert.AreEqual(redirectURL, rm1Url + "/ws/v1/cluster/apps");
			redirectURL = GetRedirectURL(rm2Url + "/proxy/" + fakeAppId);
			NUnit.Framework.Assert.IsNull(redirectURL);
			// transit the active RM to standby
			// Both of RMs are in standby mode
			GetAdminService(0).TransitionToStandby(req);
			// RM2 is expected to send the httpRequest to itself.
			// The Header Field: Refresh is expected to be set.
			redirectURL = GetRefreshURL(rm2Url);
			NUnit.Framework.Assert.IsTrue(redirectURL != null && redirectURL.Contains(YarnWebParams
				.NextRefreshInterval) && redirectURL.Contains(rm2Url));
		}

		// set up http connection with the given url and get the redirection url from the response
		// return null if the url is not redirected
		internal static string GetRedirectURL(string url)
		{
			string redirectUrl = null;
			try
			{
				HttpURLConnection conn = (HttpURLConnection)new Uri(url).OpenConnection();
				// do not automatically follow the redirection
				// otherwise we get too many redirections exception
				conn.SetInstanceFollowRedirects(false);
				if (conn.GetResponseCode() == HttpServletResponse.ScTemporaryRedirect)
				{
					redirectUrl = conn.GetHeaderField("Location");
				}
			}
			catch (Exception)
			{
			}
			// throw new RuntimeException(e);
			return redirectUrl;
		}

		internal static string GetRefreshURL(string url)
		{
			string redirectUrl = null;
			try
			{
				HttpURLConnection conn = (HttpURLConnection)new Uri(url).OpenConnection();
				// do not automatically follow the redirection
				// otherwise we get too many redirections exception
				conn.SetInstanceFollowRedirects(false);
				redirectUrl = conn.GetHeaderField("Refresh");
			}
			catch (Exception)
			{
			}
			// throw new RuntimeException(e);
			return redirectUrl;
		}
	}
}
