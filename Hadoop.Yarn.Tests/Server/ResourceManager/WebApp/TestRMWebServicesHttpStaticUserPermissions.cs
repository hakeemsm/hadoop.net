using System;
using System.Collections.Generic;
using System.IO;
using Com.Sun.Jersey.Api.Client;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebServicesHttpStaticUserPermissions
	{
		private static readonly FilePath testRootDir = new FilePath("target", typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesHttpStaticUserPermissions
			).FullName + "-root");

		private static FilePath spnegoKeytabFile = new FilePath(KerberosTestUtils.GetKeytabFile
			());

		private static string spnegoPrincipal = KerberosTestUtils.GetServerPrincipal();

		private static MiniKdc testMiniKDC;

		private static MockRM rm;

		internal class Helper
		{
			internal string method;

			internal string requestBody;

			internal Helper(string method, string requestBody)
			{
				this.method = method;
				this.requestBody = requestBody;
			}
		}

		[BeforeClass]
		public static void SetUp()
		{
			try
			{
				testMiniKDC = new MiniKdc(MiniKdc.CreateConf(), testRootDir);
				SetupKDC();
				SetupAndStartRM();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Couldn't create MiniKDC");
			}
		}

		[AfterClass]
		public static void TearDown()
		{
			if (testMiniKDC != null)
			{
				testMiniKDC.Stop();
			}
			if (rm != null)
			{
				rm.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public TestRMWebServicesHttpStaticUserPermissions()
			: base()
		{
		}

		/// <exception cref="System.Exception"/>
		private static void SetupAndStartRM()
		{
			Configuration rmconf = new Configuration();
			rmconf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
			rmconf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
				));
			rmconf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			rmconf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos"
				);
			rmconf.Set("yarn.resourcemanager.principal", spnegoPrincipal);
			rmconf.Set("yarn.resourcemanager.keytab", spnegoKeytabFile.GetAbsolutePath());
			rmconf.SetBoolean("mockrm.webapp.enabled", true);
			UserGroupInformation.SetConfiguration(rmconf);
			rm = new MockRM(rmconf);
			rm.Start();
		}

		/// <exception cref="System.Exception"/>
		private static void SetupKDC()
		{
			testMiniKDC.Start();
			testMiniKDC.CreatePrincipal(spnegoKeytabFile, "HTTP/localhost", "client", UserGroupInformation
				.GetLoginUser().GetShortUserName(), "client2");
		}

		// Test that the http static user can't submit or kill apps
		// when secure mode is turned on
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWebServiceAccess()
		{
			ApplicationSubmissionContextInfo app = new ApplicationSubmissionContextInfo();
			string appid = "application_123_0";
			app.SetApplicationId(appid);
			string submitAppRequestBody = TestRMWebServicesDelegationTokenAuthentication.GetMarshalledAppInfo
				(app);
			Uri url = new Uri("http://localhost:8088/ws/v1/cluster/apps");
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			// we should be access the apps page with the static user
			TestRMWebServicesDelegationTokenAuthentication.SetupConn(conn, "GET", string.Empty
				, string.Empty);
			try
			{
				conn.GetInputStream();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok.GetStatusCode(), conn.GetResponseCode
					());
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.Fail("Got " + conn.GetResponseCode() + " instead of 200 accessing "
					 + url.ToString());
			}
			conn.Disconnect();
			// new-application, submit app and kill should fail with
			// forbidden
			IDictionary<string, TestRMWebServicesHttpStaticUserPermissions.Helper> urlRequestMap
				 = new Dictionary<string, TestRMWebServicesHttpStaticUserPermissions.Helper>();
			string killAppRequestBody = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
				 + "<appstate>\n" + "  <state>KILLED</state>\n" + "</appstate>";
			urlRequestMap["http://localhost:8088/ws/v1/cluster/apps"] = new TestRMWebServicesHttpStaticUserPermissions.Helper
				("POST", submitAppRequestBody);
			urlRequestMap["http://localhost:8088/ws/v1/cluster/apps/new-application"] = new TestRMWebServicesHttpStaticUserPermissions.Helper
				("POST", string.Empty);
			urlRequestMap["http://localhost:8088/ws/v1/cluster/apps/app_123_1/state"] = new TestRMWebServicesHttpStaticUserPermissions.Helper
				("PUT", killAppRequestBody);
			foreach (KeyValuePair<string, TestRMWebServicesHttpStaticUserPermissions.Helper> 
				entry in urlRequestMap)
			{
				Uri reqURL = new Uri(entry.Key);
				conn = (HttpURLConnection)reqURL.OpenConnection();
				string method = entry.Value.method;
				string body = entry.Value.requestBody;
				TestRMWebServicesDelegationTokenAuthentication.SetupConn(conn, method, "application/xml"
					, body);
				try
				{
					conn.GetInputStream();
					NUnit.Framework.Assert.Fail("Request " + entry.Key + "succeeded but should have failed"
						);
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden.GetStatusCode(), 
						conn.GetResponseCode());
					InputStream errorStream = conn.GetErrorStream();
					string error = string.Empty;
					BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream, "UTF8"
						));
					for (string line; (line = reader.ReadLine()) != null; )
					{
						error += line;
					}
					reader.Close();
					errorStream.Close();
					NUnit.Framework.Assert.AreEqual("The default static user cannot carry out this operation."
						, error);
				}
				conn.Disconnect();
			}
		}
	}
}
