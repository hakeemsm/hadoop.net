using System;
using System.Collections.Generic;
using System.IO;
using Com.Sun.Jersey.Api.Client;
using Javax.WS.RS.Core;
using Javax.Xml.Bind;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Codehaus.Jettison.Json;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebServicesDelegationTokenAuthentication
	{
		private static readonly FilePath testRootDir = new FilePath("target", typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesDelegationTokenAuthentication
			).FullName + "-root");

		private static FilePath httpSpnegoKeytabFile = new FilePath(KerberosTestUtils.GetKeytabFile
			());

		private static string httpSpnegoPrincipal = KerberosTestUtils.GetServerPrincipal(
			);

		private static bool miniKDCStarted = false;

		private static MiniKdc testMiniKDC;

		private static MockRM rm;

		internal string delegationTokenHeader;

		internal const string OldDelegationTokenHeader = "Hadoop-YARN-Auth-Delegation-Token";

		internal const string NewDelegationTokenHeader = DelegationTokenAuthenticator.DelegationTokenHeader;

		// use published header name
		// alternate header name
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
			if (rm != null)
			{
				rm.Stop();
			}
		}

		[Parameterized.Parameters]
		public static ICollection<object[]> Headers()
		{
			return Arrays.AsList(new object[][] { new object[] { OldDelegationTokenHeader }, 
				new object[] { NewDelegationTokenHeader } });
		}

		/// <exception cref="System.Exception"/>
		public TestRMWebServicesDelegationTokenAuthentication(string header)
			: base()
		{
			this.delegationTokenHeader = header;
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
			string httpPrefix = "hadoop.http.authentication.";
			rmconf.SetStrings(httpPrefix + "type", "kerberos");
			rmconf.Set(httpPrefix + KerberosAuthenticationHandler.Principal, httpSpnegoPrincipal
				);
			rmconf.Set(httpPrefix + KerberosAuthenticationHandler.Keytab, httpSpnegoKeytabFile
				.GetAbsolutePath());
			// use any file for signature secret
			rmconf.Set(httpPrefix + AuthenticationFilter.SignatureSecret + ".file", httpSpnegoKeytabFile
				.GetAbsolutePath());
			rmconf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos"
				);
			rmconf.SetBoolean(YarnConfiguration.RmWebappDelegationTokenAuthFilter, true);
			rmconf.Set("hadoop.http.filter.initializers", typeof(AuthenticationFilterInitializer
				).FullName);
			rmconf.Set(YarnConfiguration.RmWebappSpnegoUserNameKey, httpSpnegoPrincipal);
			rmconf.Set(YarnConfiguration.RmKeytab, httpSpnegoKeytabFile.GetAbsolutePath());
			rmconf.Set(YarnConfiguration.RmWebappSpnegoKeytabFileKey, httpSpnegoKeytabFile.GetAbsolutePath
				());
			rmconf.Set(YarnConfiguration.NmWebappSpnegoUserNameKey, httpSpnegoPrincipal);
			rmconf.Set(YarnConfiguration.NmWebappSpnegoKeytabFileKey, httpSpnegoKeytabFile.GetAbsolutePath
				());
			rmconf.SetBoolean("mockrm.webapp.enabled", true);
			rmconf.Set("yarn.resourcemanager.proxyuser.client.hosts", "*");
			rmconf.Set("yarn.resourcemanager.proxyuser.client.groups", "*");
			UserGroupInformation.SetConfiguration(rmconf);
			rm = new MockRM(rmconf);
			rm.Start();
		}

		/// <exception cref="System.Exception"/>
		private static void SetupKDC()
		{
			if (!miniKDCStarted)
			{
				testMiniKDC.Start();
				GetKdc().CreatePrincipal(httpSpnegoKeytabFile, "HTTP/localhost", "client", UserGroupInformation
					.GetLoginUser().GetShortUserName(), "client2");
				miniKDCStarted = true;
			}
		}

		private static MiniKdc GetKdc()
		{
			return testMiniKDC;
		}

		// Test that you can authenticate with only delegation tokens
		// 1. Get a delegation token using Kerberos auth(this ends up
		// testing the fallback authenticator)
		// 2. Submit an app without kerberos or delegation-token
		// - we should get an UNAUTHORIZED response
		// 3. Submit same app with delegation-token
		// - we should get OK response
		// - confirm owner of the app is the user whose
		// delegation-token we used
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenAuth()
		{
			string token = GetDelegationToken("test");
			ApplicationSubmissionContextInfo app = new ApplicationSubmissionContextInfo();
			string appid = "application_123_0";
			app.SetApplicationId(appid);
			string requestBody = GetMarshalledAppInfo(app);
			Uri url = new Uri("http://localhost:8088/ws/v1/cluster/apps");
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			SetupConn(conn, "POST", "application/xml", requestBody);
			// this should fail with unauthorized because only
			// auth is kerberos or delegation token
			try
			{
				conn.GetInputStream();
				NUnit.Framework.Assert.Fail("we should not be here");
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Unauthorized.GetStatusCode(
					), conn.GetResponseCode());
			}
			conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestProperty(delegationTokenHeader, token);
			SetupConn(conn, "POST", MediaType.ApplicationXml, requestBody);
			// this should not fail
			try
			{
				conn.GetInputStream();
			}
			catch (IOException)
			{
				InputStream errorStream = conn.GetErrorStream();
				string error = string.Empty;
				BufferedReader reader = null;
				reader = new BufferedReader(new InputStreamReader(errorStream, "UTF8"));
				for (string line; (line = reader.ReadLine()) != null; )
				{
					error += line;
				}
				reader.Close();
				errorStream.Close();
				NUnit.Framework.Assert.Fail("Response " + conn.GetResponseCode() + "; " + error);
			}
			bool appExists = rm.GetRMContext().GetRMApps().Contains(ConverterUtils.ToApplicationId
				(appid));
			NUnit.Framework.Assert.IsTrue(appExists);
			RMApp actualApp = rm.GetRMContext().GetRMApps()[ConverterUtils.ToApplicationId(appid
				)];
			string owner = actualApp.GetUser();
			NUnit.Framework.Assert.AreEqual("client", owner);
		}

		// Test to make sure that cancelled delegation tokens
		// are rejected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCancelledDelegationToken()
		{
			string token = GetDelegationToken("client");
			CancelDelegationToken(token);
			ApplicationSubmissionContextInfo app = new ApplicationSubmissionContextInfo();
			string appid = "application_123_0";
			app.SetApplicationId(appid);
			string requestBody = GetMarshalledAppInfo(app);
			Uri url = new Uri("http://localhost:8088/ws/v1/cluster/apps");
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestProperty(delegationTokenHeader, token);
			SetupConn(conn, "POST", MediaType.ApplicationXml, requestBody);
			// this should fail with unauthorized because only
			// auth is kerberos or delegation token
			try
			{
				conn.GetInputStream();
				NUnit.Framework.Assert.Fail("Authentication should fail with expired delegation tokens"
					);
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden.GetStatusCode(), 
					conn.GetResponseCode());
			}
		}

		// Test to make sure that we can't do delegation token
		// functions using just delegation token auth
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenOps()
		{
			string token = GetDelegationToken("client");
			string createRequest = "{\"renewer\":\"test\"}";
			string renewRequest = "{\"token\": \"" + token + "\"}";
			// first test create and renew
			string[] requests = new string[] { createRequest, renewRequest };
			foreach (string requestBody in requests)
			{
				Uri url = new Uri("http://localhost:8088/ws/v1/cluster/delegation-token");
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestProperty(delegationTokenHeader, token);
				SetupConn(conn, "POST", MediaType.ApplicationJson, requestBody);
				try
				{
					conn.GetInputStream();
					NUnit.Framework.Assert.Fail("Creation/Renewing delegation tokens should not be " 
						+ "allowed with token auth");
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden.GetStatusCode(), 
						conn.GetResponseCode());
				}
			}
			// test cancel
			Uri url_1 = new Uri("http://localhost:8088/ws/v1/cluster/delegation-token");
			HttpURLConnection conn_1 = (HttpURLConnection)url_1.OpenConnection();
			conn_1.SetRequestProperty(delegationTokenHeader, token);
			conn_1.SetRequestProperty(RMWebServices.DelegationTokenHeader, token);
			SetupConn(conn_1, "DELETE", null, null);
			try
			{
				conn_1.GetInputStream();
				NUnit.Framework.Assert.Fail("Cancelling delegation tokens should not be allowed with token auth"
					);
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden.GetStatusCode(), 
					conn_1.GetResponseCode());
			}
		}

		// Superuser "client" should be able to get a delegation token
		// for user "client2" when authenticated using Kerberos
		// The request shouldn't work when authenticated using DelegationTokens
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoAs()
		{
			KerberosTestUtils.DoAsClient(new _Callable_319());
			// this should not work
			string token = GetDelegationToken("client");
			string renewer = "renewer";
			string body = "{\"renewer\":\"" + renewer + "\"}";
			Uri url = new Uri("http://localhost:8088/ws/v1/cluster/delegation-token?doAs=client2"
				);
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestProperty(delegationTokenHeader, token);
			SetupConn(conn, "POST", MediaType.ApplicationJson, body);
			try
			{
				conn.GetInputStream();
				NUnit.Framework.Assert.Fail("Client should not be allowed to impersonate using delegation tokens"
					);
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden.GetStatusCode(), 
					conn.GetResponseCode());
			}
			// this should also fail due to client2 not being a super user
			KerberosTestUtils.DoAs("client2@EXAMPLE.COM", new _Callable_374());
		}

		private sealed class _Callable_319 : Callable<Void>
		{
			public _Callable_319()
			{
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				string token = string.Empty;
				string owner = string.Empty;
				string renewer = "renewer";
				string body = "{\"renewer\":\"" + renewer + "\"}";
				Uri url = new Uri("http://localhost:8088/ws/v1/cluster/delegation-token?doAs=client2"
					);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesDelegationTokenAuthentication
					.SetupConn(conn, "POST", MediaType.ApplicationJson, body);
				InputStream response = conn.GetInputStream();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok.GetStatusCode(), conn.GetResponseCode
					());
				BufferedReader reader = null;
				try
				{
					reader = new BufferedReader(new InputStreamReader(response, "UTF8"));
					for (string line; (line = reader.ReadLine()) != null; )
					{
						JSONObject obj = new JSONObject(line);
						if (obj.Has("token"))
						{
							token = obj.GetString("token");
						}
						if (obj.Has("owner"))
						{
							owner = obj.GetString("owner");
						}
					}
				}
				finally
				{
					IOUtils.CloseQuietly(reader);
					IOUtils.CloseQuietly(response);
				}
				NUnit.Framework.Assert.AreEqual("client2", owner);
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> realToken = new 
					Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier>();
				realToken.DecodeFromUrlString(token);
				NUnit.Framework.Assert.AreEqual("client2", realToken.DecodeIdentifier().GetOwner(
					).ToString());
				return null;
			}
		}

		private sealed class _Callable_374 : Callable<Void>
		{
			public _Callable_374()
			{
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				string renewer = "renewer";
				string body = "{\"renewer\":\"" + renewer + "\"}";
				Uri url = new Uri("http://localhost:8088/ws/v1/cluster/delegation-token?doAs=client"
					);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesDelegationTokenAuthentication
					.SetupConn(conn, "POST", MediaType.ApplicationJson, body);
				try
				{
					conn.GetInputStream();
					NUnit.Framework.Assert.Fail("Non superuser client should not be allowed to carry out doAs"
						);
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden.GetStatusCode(), 
						conn.GetResponseCode());
				}
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		private string GetDelegationToken(string renewer)
		{
			return KerberosTestUtils.DoAsClient(new _Callable_398(renewer));
		}

		private sealed class _Callable_398 : Callable<string>
		{
			public _Callable_398(string renewer)
			{
				this.renewer = renewer;
			}

			/// <exception cref="System.Exception"/>
			public string Call()
			{
				string ret = null;
				string body = "{\"renewer\":\"" + renewer + "\"}";
				Uri url = new Uri("http://localhost:8088/ws/v1/cluster/delegation-token");
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesDelegationTokenAuthentication
					.SetupConn(conn, "POST", MediaType.ApplicationJson, body);
				InputStream response = conn.GetInputStream();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok.GetStatusCode(), conn.GetResponseCode
					());
				BufferedReader reader = null;
				try
				{
					reader = new BufferedReader(new InputStreamReader(response, "UTF8"));
					for (string line; (line = reader.ReadLine()) != null; )
					{
						JSONObject obj = new JSONObject(line);
						if (obj.Has("token"))
						{
							reader.Close();
							response.Close();
							ret = obj.GetString("token");
							break;
						}
					}
				}
				finally
				{
					IOUtils.CloseQuietly(reader);
					IOUtils.CloseQuietly(response);
				}
				return ret;
			}

			private readonly string renewer;
		}

		/// <exception cref="System.Exception"/>
		private void CancelDelegationToken(string tokenString)
		{
			KerberosTestUtils.DoAsClient(new _Callable_432(tokenString));
		}

		private sealed class _Callable_432 : Callable<Void>
		{
			public _Callable_432(string tokenString)
			{
				this.tokenString = tokenString;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				Uri url = new Uri("http://localhost:8088/ws/v1/cluster/delegation-token");
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestProperty(RMWebServices.DelegationTokenHeader, tokenString);
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesDelegationTokenAuthentication
					.SetupConn(conn, "DELETE", null, null);
				InputStream response = conn.GetInputStream();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok.GetStatusCode(), conn.GetResponseCode
					());
				response.Close();
				return null;
			}

			private readonly string tokenString;
		}

		/// <exception cref="System.Exception"/>
		internal static string GetMarshalledAppInfo(ApplicationSubmissionContextInfo appInfo
			)
		{
			StringWriter writer = new StringWriter();
			JAXBContext context = JAXBContext.NewInstance(typeof(ApplicationSubmissionContextInfo
				));
			Marshaller m = context.CreateMarshaller();
			m.Marshal(appInfo, writer);
			return writer.ToString();
		}

		/// <exception cref="System.Exception"/>
		internal static void SetupConn(HttpURLConnection conn, string method, string contentType
			, string body)
		{
			conn.SetRequestMethod(method);
			conn.SetDoOutput(true);
			conn.SetRequestProperty("Accept-Charset", "UTF8");
			if (contentType != null && !contentType.IsEmpty())
			{
				conn.SetRequestProperty("Content-Type", contentType + ";charset=UTF8");
				if (body != null && !body.IsEmpty())
				{
					OutputStream stream = conn.GetOutputStream();
					stream.Write(Sharpen.Runtime.GetBytesForString(body, "UTF8"));
					stream.Close();
				}
			}
		}
	}
}
