using System;
using System.Collections;
using System.IO;
using Com.Sun.Jersey.Api.Client;
using Javax.WS.RS.Core;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebappAuthentication
	{
		private static MockRM rm;

		private static Configuration simpleConf;

		private static Configuration kerberosConf;

		private static readonly FilePath testRootDir = new FilePath("target", typeof(TestRMWebServicesDelegationTokenAuthentication
			).FullName + "-root");

		private static FilePath httpSpnegoKeytabFile = new FilePath(KerberosTestUtils.GetKeytabFile
			());

		private static bool miniKDCStarted = false;

		private static MiniKdc testMiniKDC;

		static TestRMWebappAuthentication()
		{
			/* Just a simple test class to ensure that the RM handles the static web user
			* correctly for secure and un-secure modes
			*
			*/
			simpleConf = new Configuration();
			simpleConf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
			simpleConf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(
				ResourceScheduler));
			simpleConf.SetBoolean("mockrm.webapp.enabled", true);
			kerberosConf = new Configuration();
			kerberosConf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
			kerberosConf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(
				ResourceScheduler));
			kerberosConf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			kerberosConf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos"
				);
			kerberosConf.Set(YarnConfiguration.RmKeytab, httpSpnegoKeytabFile.GetAbsolutePath
				());
			kerberosConf.SetBoolean("mockrm.webapp.enabled", true);
		}

		[Parameterized.Parameters]
		public static ICollection Params()
		{
			return Arrays.AsList(new object[][] { new object[] { 1, simpleConf }, new object[
				] { 2, kerberosConf } });
		}

		public TestRMWebappAuthentication(int run, Configuration conf)
			: base()
		{
			SetupAndStartRM(conf);
		}

		[BeforeClass]
		public static void SetUp()
		{
			try
			{
				testMiniKDC = new MiniKdc(MiniKdc.CreateConf(), testRootDir);
				SetupKDC();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.IsTrue("Couldn't create MiniKDC", false);
			}
		}

		[AfterClass]
		public static void TearDown()
		{
			if (testMiniKDC != null)
			{
				testMiniKDC.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		private static void SetupKDC()
		{
			if (!miniKDCStarted)
			{
				testMiniKDC.Start();
				GetKdc().CreatePrincipal(httpSpnegoKeytabFile, "HTTP/localhost", "client", UserGroupInformation
					.GetLoginUser().GetShortUserName());
				miniKDCStarted = true;
			}
		}

		private static MiniKdc GetKdc()
		{
			return testMiniKDC;
		}

		private static void SetupAndStartRM(Configuration conf)
		{
			UserGroupInformation.SetConfiguration(conf);
			rm = new MockRM(conf);
		}

		// ensure that in a non-secure cluster users can access
		// the web pages as earlier and submit apps as anonymous
		// user or by identifying themselves
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleAuth()
		{
			rm.Start();
			// ensure users can access web pages
			// this should work for secure and non-secure clusters
			Uri url = new Uri("http://localhost:8088/cluster");
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			try
			{
				conn.GetInputStream();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok.GetStatusCode(), conn.GetResponseCode
					());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Fetching url failed");
			}
			if (UserGroupInformation.IsSecurityEnabled())
			{
				TestAnonymousKerberosUser();
			}
			else
			{
				TestAnonymousSimpleUser();
			}
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		private void TestAnonymousKerberosUser()
		{
			ApplicationSubmissionContextInfo app = new ApplicationSubmissionContextInfo();
			string appid = "application_123_0";
			app.SetApplicationId(appid);
			string requestBody = TestRMWebServicesDelegationTokenAuthentication.GetMarshalledAppInfo
				(app);
			Uri url = new Uri("http://localhost:8088/ws/v1/cluster/apps/new-application");
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			TestRMWebServicesDelegationTokenAuthentication.SetupConn(conn, "POST", "application/xml"
				, requestBody);
			try
			{
				conn.GetInputStream();
				NUnit.Framework.Assert.Fail("Anonymous users should not be allowed to get new application ids in secure mode."
					);
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden.GetStatusCode(), 
					conn.GetResponseCode());
			}
			url = new Uri("http://localhost:8088/ws/v1/cluster/apps");
			conn = (HttpURLConnection)url.OpenConnection();
			TestRMWebServicesDelegationTokenAuthentication.SetupConn(conn, "POST", "application/xml"
				, requestBody);
			try
			{
				conn.GetInputStream();
				NUnit.Framework.Assert.Fail("Anonymous users should not be allowed to submit apps in secure mode."
					);
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden.GetStatusCode(), 
					conn.GetResponseCode());
			}
			requestBody = "{ \"state\": \"KILLED\"}";
			url = new Uri("http://localhost:8088/ws/v1/cluster/apps/application_123_0/state");
			conn = (HttpURLConnection)url.OpenConnection();
			TestRMWebServicesDelegationTokenAuthentication.SetupConn(conn, "PUT", "application/json"
				, requestBody);
			try
			{
				conn.GetInputStream();
				NUnit.Framework.Assert.Fail("Anonymous users should not be allowed to kill apps in secure mode."
					);
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden.GetStatusCode(), 
					conn.GetResponseCode());
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestAnonymousSimpleUser()
		{
			ApplicationSubmissionContextInfo app = new ApplicationSubmissionContextInfo();
			string appid = "application_123_0";
			app.SetApplicationId(appid);
			string requestBody = TestRMWebServicesDelegationTokenAuthentication.GetMarshalledAppInfo
				(app);
			Uri url = new Uri("http://localhost:8088/ws/v1/cluster/apps");
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			TestRMWebServicesDelegationTokenAuthentication.SetupConn(conn, "POST", "application/xml"
				, requestBody);
			conn.GetInputStream();
			NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Accepted.GetStatusCode(), conn
				.GetResponseCode());
			bool appExists = rm.GetRMContext().GetRMApps().Contains(ConverterUtils.ToApplicationId
				(appid));
			NUnit.Framework.Assert.IsTrue(appExists);
			RMApp actualApp = rm.GetRMContext().GetRMApps()[ConverterUtils.ToApplicationId(appid
				)];
			string owner = actualApp.GetUser();
			NUnit.Framework.Assert.AreEqual(rm.GetConfig().Get(CommonConfigurationKeys.HadoopHttpStaticUser
				, CommonConfigurationKeys.DefaultHadoopHttpStaticUser), owner);
			appid = "application_123_1";
			app.SetApplicationId(appid);
			requestBody = TestRMWebServicesDelegationTokenAuthentication.GetMarshalledAppInfo
				(app);
			url = new Uri("http://localhost:8088/ws/v1/cluster/apps?user.name=client");
			conn = (HttpURLConnection)url.OpenConnection();
			TestRMWebServicesDelegationTokenAuthentication.SetupConn(conn, "POST", MediaType.
				ApplicationXml, requestBody);
			conn.GetInputStream();
			appExists = rm.GetRMContext().GetRMApps().Contains(ConverterUtils.ToApplicationId
				(appid));
			NUnit.Framework.Assert.IsTrue(appExists);
			actualApp = rm.GetRMContext().GetRMApps()[ConverterUtils.ToApplicationId(appid)];
			owner = actualApp.GetUser();
			NUnit.Framework.Assert.AreEqual("client", owner);
		}
	}
}
