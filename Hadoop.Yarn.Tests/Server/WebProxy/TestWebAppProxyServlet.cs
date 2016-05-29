using System;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Mortbay.Jetty.Servlet;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy
{
	/// <summary>Test the WebAppProxyServlet and WebAppProxy.</summary>
	/// <remarks>
	/// Test the WebAppProxyServlet and WebAppProxy. For back end use simple web
	/// server.
	/// </remarks>
	public class TestWebAppProxyServlet
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Server.Webproxy.TestWebAppProxyServlet
			));

		private static Org.Mortbay.Jetty.Server server;

		private static int originalPort = 0;

		internal Configuration configuration = new Configuration();

		/// <summary>Simple http server.</summary>
		/// <remarks>Simple http server. Server should send answer with status 200</remarks>
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Start()
		{
			server = new Org.Mortbay.Jetty.Server(0);
			Context context = new Context();
			context.SetContextPath("/foo");
			server.SetHandler(context);
			context.AddServlet(new ServletHolder(typeof(TestWebAppProxyServlet.TestServlet)), 
				"/bar");
			server.GetConnectors()[0].SetHost("localhost");
			server.Start();
			originalPort = server.GetConnectors()[0].GetLocalPort();
			Log.Info("Running embedded servlet container at: http://localhost:" + originalPort
				);
		}

		[System.Serializable]
		public class TestServlet : HttpServlet
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
			{
				resp.SetStatus(HttpServletResponse.ScOk);
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoPost(HttpServletRequest req, HttpServletResponse resp)
			{
				InputStream @is = req.GetInputStream();
				OutputStream os = resp.GetOutputStream();
				int c = @is.Read();
				while (c > -1)
				{
					os.Write(c);
					c = @is.Read();
				}
				@is.Close();
				os.Close();
				resp.SetStatus(HttpServletResponse.ScOk);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWebAppProxyServlet()
		{
			configuration.Set(YarnConfiguration.ProxyAddress, "localhost:9090");
			// overriding num of web server threads, see HttpServer.HTTP_MAXTHREADS 
			configuration.SetInt("hadoop.http.max.threads", 5);
			TestWebAppProxyServlet.WebAppProxyServerForTest proxy = new TestWebAppProxyServlet.WebAppProxyServerForTest
				(this);
			proxy.Init(configuration);
			proxy.Start();
			int proxyPort = proxy.proxy.proxyServer.GetConnectorAddress(0).Port;
			TestWebAppProxyServlet.AppReportFetcherForTest appReportFetcher = proxy.proxy.appReportFetcher;
			// wrong url
			try
			{
				// wrong url. Set wrong app ID
				Uri wrongUrl = new Uri("http://localhost:" + proxyPort + "/proxy/app");
				HttpURLConnection proxyConn = (HttpURLConnection)wrongUrl.OpenConnection();
				proxyConn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpInternalError, proxyConn.GetResponseCode
					());
				// set true Application ID in url
				Uri url = new Uri("http://localhost:" + proxyPort + "/proxy/application_00_0");
				proxyConn = (HttpURLConnection)url.OpenConnection();
				// set cookie
				proxyConn.SetRequestProperty("Cookie", "checked_application_0_0000=true");
				proxyConn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, proxyConn.GetResponseCode
					());
				NUnit.Framework.Assert.IsTrue(IsResponseCookiePresent(proxyConn, "checked_application_0_0000"
					, "true"));
				// cannot found application 1: null
				appReportFetcher.answer = 1;
				proxyConn = (HttpURLConnection)url.OpenConnection();
				proxyConn.SetRequestProperty("Cookie", "checked_application_0_0000=true");
				proxyConn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpNotFound, proxyConn.GetResponseCode
					());
				NUnit.Framework.Assert.IsFalse(IsResponseCookiePresent(proxyConn, "checked_application_0_0000"
					, "true"));
				// cannot found application 2: ApplicationNotFoundException
				appReportFetcher.answer = 4;
				proxyConn = (HttpURLConnection)url.OpenConnection();
				proxyConn.SetRequestProperty("Cookie", "checked_application_0_0000=true");
				proxyConn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpNotFound, proxyConn.GetResponseCode
					());
				NUnit.Framework.Assert.IsFalse(IsResponseCookiePresent(proxyConn, "checked_application_0_0000"
					, "true"));
				// wrong user
				appReportFetcher.answer = 2;
				proxyConn = (HttpURLConnection)url.OpenConnection();
				proxyConn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, proxyConn.GetResponseCode
					());
				string s = ReadInputStream(proxyConn.GetInputStream());
				NUnit.Framework.Assert.IsTrue(s.Contains("to continue to an Application Master web interface owned by"
					));
				NUnit.Framework.Assert.IsTrue(s.Contains("WARNING: The following page may not be safe!"
					));
				//case if task has a not running status
				appReportFetcher.answer = 3;
				proxyConn = (HttpURLConnection)url.OpenConnection();
				proxyConn.SetRequestProperty("Cookie", "checked_application_0_0000=true");
				proxyConn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, proxyConn.GetResponseCode
					());
				// test user-provided path and query parameter can be appended to the
				// original tracking url
				appReportFetcher.answer = 5;
				Uri clientUrl = new Uri("http://localhost:" + proxyPort + "/proxy/application_00_0/test/tez?x=y&h=p"
					);
				proxyConn = (HttpURLConnection)clientUrl.OpenConnection();
				proxyConn.Connect();
				Log.Info(string.Empty + proxyConn.GetURL());
				Log.Info("ProxyConn.getHeaderField(): " + proxyConn.GetHeaderField(ProxyUtils.Location
					));
				NUnit.Framework.Assert.AreEqual("http://localhost:" + originalPort + "/foo/bar/test/tez?a=b&x=y&h=p#main"
					, proxyConn.GetURL().ToString());
			}
			finally
			{
				proxy.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppReportForEmptyTrackingUrl()
		{
			configuration.Set(YarnConfiguration.ProxyAddress, "localhost:9090");
			// overriding num of web server threads, see HttpServer.HTTP_MAXTHREADS
			configuration.SetInt("hadoop.http.max.threads", 5);
			TestWebAppProxyServlet.WebAppProxyServerForTest proxy = new TestWebAppProxyServlet.WebAppProxyServerForTest
				(this);
			proxy.Init(configuration);
			proxy.Start();
			int proxyPort = proxy.proxy.proxyServer.GetConnectorAddress(0).Port;
			TestWebAppProxyServlet.AppReportFetcherForTest appReportFetcher = proxy.proxy.appReportFetcher;
			try
			{
				//set AHS_ENBALED = false to simulate getting the app report from RM
				configuration.SetBoolean(YarnConfiguration.ApplicationHistoryEnabled, false);
				ApplicationId app = ApplicationId.NewInstance(0, 0);
				appReportFetcher.answer = 6;
				Uri url = new Uri("http://localhost:" + proxyPort + "/proxy/" + app.ToString());
				HttpURLConnection proxyConn = (HttpURLConnection)url.OpenConnection();
				proxyConn.Connect();
				try
				{
					proxyConn.GetResponseCode();
				}
				catch (ConnectException)
				{
				}
				// Connection Exception is expected as we have set
				// appReportFetcher.answer = 6, which does not set anything for
				// original tracking url field in the app report.
				string appAddressInRm = WebAppUtils.GetResolvedRMWebAppURLWithScheme(configuration
					) + "/cluster" + "/app/" + app.ToString();
				NUnit.Framework.Assert.IsTrue("Webapp proxy servlet should have redirected to RM"
					, proxyConn.GetURL().ToString().Equals(appAddressInRm));
				//set AHS_ENBALED = true to simulate getting the app report from AHS
				configuration.SetBoolean(YarnConfiguration.ApplicationHistoryEnabled, true);
				proxyConn = (HttpURLConnection)url.OpenConnection();
				proxyConn.Connect();
				try
				{
					proxyConn.GetResponseCode();
				}
				catch (ConnectException)
				{
				}
				// Connection Exception is expected as we have set
				// appReportFetcher.answer = 6, which does not set anything for
				// original tracking url field in the app report.
				string appAddressInAhs = WebAppUtils.GetHttpSchemePrefix(configuration) + WebAppUtils
					.GetAHSWebAppURLWithoutScheme(configuration) + "/applicationhistory" + "/apps/" 
					+ app.ToString();
				NUnit.Framework.Assert.IsTrue("Webapp proxy servlet should have redirected to AHS"
					, proxyConn.GetURL().ToString().Equals(appAddressInAhs));
			}
			finally
			{
				proxy.Close();
			}
		}

		/// <summary>Test main method of WebAppProxyServer</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestWebAppProxyServerMainMethod()
		{
			WebAppProxyServer mainServer = null;
			Configuration conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.ProxyAddress, "localhost:9099");
			try
			{
				mainServer = WebAppProxyServer.StartServer(conf);
				int counter = 20;
				Uri wrongUrl = new Uri("http://localhost:9099/proxy/app");
				HttpURLConnection proxyConn = null;
				while (counter > 0)
				{
					counter--;
					try
					{
						proxyConn = (HttpURLConnection)wrongUrl.OpenConnection();
						proxyConn.Connect();
						proxyConn.GetResponseCode();
						// server started ok
						counter = 0;
					}
					catch (Exception)
					{
						Sharpen.Thread.Sleep(100);
					}
				}
				NUnit.Framework.Assert.IsNotNull(proxyConn);
				// wrong application Id
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpInternalError, proxyConn.GetResponseCode
					());
			}
			finally
			{
				if (mainServer != null)
				{
					mainServer.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private string ReadInputStream(InputStream input)
		{
			ByteArrayOutputStream data = new ByteArrayOutputStream();
			byte[] buffer = new byte[512];
			int read;
			while ((read = input.Read(buffer)) >= 0)
			{
				data.Write(buffer, 0, read);
			}
			return Sharpen.Runtime.GetStringForBytes(data.ToByteArray(), "UTF-8");
		}

		private bool IsResponseCookiePresent(HttpURLConnection proxyConn, string expectedName
			, string expectedValue)
		{
			IDictionary<string, IList<string>> headerFields = proxyConn.GetHeaderFields();
			IList<string> cookiesHeader = headerFields["Set-Cookie"];
			if (cookiesHeader != null)
			{
				foreach (string cookie in cookiesHeader)
				{
					HttpCookie c = HttpCookie.Parse(cookie)[0];
					if (c.GetName().Equals(expectedName) && c.GetValue().Equals(expectedValue))
					{
						return true;
					}
				}
			}
			return false;
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Stop()
		{
			try
			{
				server.Stop();
			}
			catch (Exception)
			{
			}
			try
			{
				server.Destroy();
			}
			catch (Exception)
			{
			}
		}

		private class WebAppProxyServerForTest : CompositeService
		{
			private TestWebAppProxyServlet.WebAppProxyForTest proxy = null;

			public WebAppProxyServerForTest(TestWebAppProxyServlet _enclosing)
				: base(typeof(WebAppProxyServer).FullName)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
				lock (this)
				{
					this.proxy = new TestWebAppProxyServlet.WebAppProxyForTest(this);
					this.AddService(this.proxy);
					base.ServiceInit(conf);
				}
			}

			private readonly TestWebAppProxyServlet _enclosing;
		}

		private class WebAppProxyForTest : WebAppProxy
		{
			internal HttpServer2 proxyServer;

			internal TestWebAppProxyServlet.AppReportFetcherForTest appReportFetcher;

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				Configuration conf = this.GetConfig();
				string bindAddress = conf.Get(YarnConfiguration.ProxyAddress);
				bindAddress = StringUtils.Split(bindAddress, ':')[0];
				AccessControlList acl = new AccessControlList(conf.Get(YarnConfiguration.YarnAdminAcl
					, YarnConfiguration.DefaultYarnAdminAcl));
				this.proxyServer = new HttpServer2.Builder().SetName("proxy").AddEndpoint(URI.Create
					(WebAppUtils.GetHttpSchemePrefix(conf) + bindAddress + ":0")).SetFindPort(true).
					SetConf(conf).SetACL(acl).Build();
				this.proxyServer.AddServlet(ProxyUriUtils.ProxyServletName, ProxyUriUtils.ProxyPathSpec
					, typeof(WebAppProxyServlet));
				this.appReportFetcher = new TestWebAppProxyServlet.AppReportFetcherForTest(this, 
					conf);
				this.proxyServer.SetAttribute(WebAppProxy.FetcherAttribute, this.appReportFetcher
					);
				this.proxyServer.SetAttribute(WebAppProxy.IsSecurityEnabledAttribute, true);
				string proxy = WebAppUtils.GetProxyHostAndPort(conf);
				string[] proxyParts = proxy.Split(":");
				string proxyHost = proxyParts[0];
				this.proxyServer.SetAttribute(WebAppProxy.ProxyHostAttribute, proxyHost);
				this.proxyServer.Start();
				TestWebAppProxyServlet.Log.Info("Proxy server is started at port {}", this.proxyServer
					.GetConnectorAddress(0).Port);
			}

			internal WebAppProxyForTest(TestWebAppProxyServlet _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestWebAppProxyServlet _enclosing;
		}

		private class AppReportFetcherForTest : AppReportFetcher
		{
			internal int answer = 0;

			public AppReportFetcherForTest(TestWebAppProxyServlet _enclosing, Configuration conf
				)
				: base(conf)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public override AppReportFetcher.FetchedAppReport GetApplicationReport(ApplicationId
				 appId)
			{
				if (this.answer == 0)
				{
					return this.GetDefaultApplicationReport(appId);
				}
				else
				{
					if (this.answer == 1)
					{
						return null;
					}
					else
					{
						if (this.answer == 2)
						{
							AppReportFetcher.FetchedAppReport result = this.GetDefaultApplicationReport(appId
								);
							result.GetApplicationReport().SetUser("user");
							return result;
						}
						else
						{
							if (this.answer == 3)
							{
								AppReportFetcher.FetchedAppReport result = this.GetDefaultApplicationReport(appId
									);
								result.GetApplicationReport().SetYarnApplicationState(YarnApplicationState.Killed
									);
								return result;
							}
							else
							{
								if (this.answer == 4)
								{
									throw new ApplicationNotFoundException("Application is not found");
								}
								else
								{
									if (this.answer == 5)
									{
										// test user-provided path and query parameter can be appended to the
										// original tracking url
										AppReportFetcher.FetchedAppReport result = this.GetDefaultApplicationReport(appId
											);
										result.GetApplicationReport().SetOriginalTrackingUrl("localhost:" + TestWebAppProxyServlet
											.originalPort + "/foo/bar?a=b#main");
										result.GetApplicationReport().SetYarnApplicationState(YarnApplicationState.Finished
											);
										return result;
									}
									else
									{
										if (this.answer == 6)
										{
											return this.GetDefaultApplicationReport(appId, false);
										}
									}
								}
							}
						}
					}
				}
				return null;
			}

			/*
			* If this method is called with isTrackingUrl=false, no tracking url
			* will set in the app report. Hence, there will be a connection exception
			* when the prxyCon tries to connect.
			*/
			private AppReportFetcher.FetchedAppReport GetDefaultApplicationReport(ApplicationId
				 appId, bool isTrackingUrl)
			{
				AppReportFetcher.FetchedAppReport fetchedReport;
				ApplicationReport result = new ApplicationReportPBImpl();
				result.SetApplicationId(appId);
				result.SetYarnApplicationState(YarnApplicationState.Running);
				result.SetUser(CommonConfigurationKeys.DefaultHadoopHttpStaticUser);
				if (isTrackingUrl)
				{
					result.SetOriginalTrackingUrl("localhost:" + TestWebAppProxyServlet.originalPort 
						+ "/foo/bar");
				}
				if (this._enclosing.configuration.GetBoolean(YarnConfiguration.ApplicationHistoryEnabled
					, false))
				{
					fetchedReport = new AppReportFetcher.FetchedAppReport(result, AppReportFetcher.AppReportSource
						.Ahs);
				}
				else
				{
					fetchedReport = new AppReportFetcher.FetchedAppReport(result, AppReportFetcher.AppReportSource
						.Rm);
				}
				return fetchedReport;
			}

			private AppReportFetcher.FetchedAppReport GetDefaultApplicationReport(ApplicationId
				 appId)
			{
				return this.GetDefaultApplicationReport(appId, true);
			}

			private readonly TestWebAppProxyServlet _enclosing;
		}
	}
}
