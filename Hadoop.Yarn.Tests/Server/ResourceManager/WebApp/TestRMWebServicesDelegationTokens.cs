using System.Collections.Generic;
using System.IO;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Api.Client.Filter;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.Servlet;
using Javax.WS.RS.Core;
using Javax.Xml.Parsers;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Security.Authentication;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebServicesDelegationTokens : JerseyTestBase
	{
		private static FilePath testRootDir;

		private static FilePath httpSpnegoKeytabFile = new FilePath(KerberosTestUtils.GetKeytabFile
			());

		private static string httpSpnegoPrincipal = KerberosTestUtils.GetServerPrincipal(
			);

		private static MiniKdc testMiniKDC;

		private static MockRM rm;

		private Injector injector;

		private bool isKerberosAuth = false;

		internal readonly string yarnTokenHeader = "Hadoop-YARN-RM-Delegation-Token";

		public class TestKerberosAuthFilter : AuthenticationFilter
		{
			// Make sure the test uses the published header string
			/// <exception cref="Javax.Servlet.ServletException"/>
			protected override Properties GetConfiguration(string configPrefix, FilterConfig 
				filterConfig)
			{
				Properties properties = base.GetConfiguration(configPrefix, filterConfig);
				properties[KerberosAuthenticationHandler.Principal] = httpSpnegoPrincipal;
				properties[KerberosAuthenticationHandler.Keytab] = httpSpnegoKeytabFile.GetAbsolutePath
					();
				properties[AuthenticationFilter.AuthType] = "kerberos";
				return properties;
			}
		}

		public class TestSimpleAuthFilter : AuthenticationFilter
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			protected override Properties GetConfiguration(string configPrefix, FilterConfig 
				filterConfig)
			{
				Properties properties = base.GetConfiguration(configPrefix, filterConfig);
				properties[KerberosAuthenticationHandler.Principal] = httpSpnegoPrincipal;
				properties[KerberosAuthenticationHandler.Keytab] = httpSpnegoKeytabFile.GetAbsolutePath
					();
				properties[AuthenticationFilter.AuthType] = "simple";
				properties[PseudoAuthenticationHandler.AnonymousAllowed] = "false";
				return properties;
			}
		}

		private class TestServletModule : ServletModule
		{
			public Configuration rmconf = new Configuration();

			protected override void ConfigureServlets()
			{
				this.Bind<JAXBContextResolver>();
				this.Bind<RMWebServices>();
				this.Bind<GenericExceptionHandler>();
				Configuration rmconf = new Configuration();
				rmconf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
					);
				rmconf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
					));
				rmconf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
				TestRMWebServicesDelegationTokens.rm = new MockRM(rmconf);
				this.Bind<ResourceManager>().ToInstance(TestRMWebServicesDelegationTokens.rm);
				if (this._enclosing.isKerberosAuth == true)
				{
					this.Filter("/*").Through(typeof(TestRMWebServicesDelegationTokens.TestKerberosAuthFilter
						));
				}
				else
				{
					this.Filter("/*").Through(typeof(TestRMWebServicesDelegationTokens.TestSimpleAuthFilter
						));
				}
				this.Serve("/*").With(typeof(GuiceContainer));
			}

			internal TestServletModule(TestRMWebServicesDelegationTokens _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;
		}

		private Injector GetSimpleAuthInjector()
		{
			return Com.Google.Inject.Guice.CreateInjector(new _TestServletModule_169(this));
		}

		private sealed class _TestServletModule_169 : TestRMWebServicesDelegationTokens.TestServletModule
		{
			public _TestServletModule_169(TestRMWebServicesDelegationTokens _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected override void ConfigureServlets()
			{
				this._enclosing.isKerberosAuth = false;
				this.rmconf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "simple"
					);
				base.ConfigureServlets();
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;
		}

		private Injector GetKerberosAuthInjector()
		{
			return Com.Google.Inject.Guice.CreateInjector(new _TestServletModule_182(this));
		}

		private sealed class _TestServletModule_182 : TestRMWebServicesDelegationTokens.TestServletModule
		{
			public _TestServletModule_182(TestRMWebServicesDelegationTokens _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected override void ConfigureServlets()
			{
				this._enclosing.isKerberosAuth = true;
				this.rmconf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos"
					);
				this.rmconf.Set(YarnConfiguration.RmWebappSpnegoUserNameKey, TestRMWebServicesDelegationTokens
					.httpSpnegoPrincipal);
				this.rmconf.Set(YarnConfiguration.RmWebappSpnegoKeytabFileKey, TestRMWebServicesDelegationTokens
					.httpSpnegoKeytabFile.GetAbsolutePath());
				this.rmconf.Set(YarnConfiguration.NmWebappSpnegoUserNameKey, TestRMWebServicesDelegationTokens
					.httpSpnegoPrincipal);
				this.rmconf.Set(YarnConfiguration.NmWebappSpnegoKeytabFileKey, TestRMWebServicesDelegationTokens
					.httpSpnegoKeytabFile.GetAbsolutePath());
				base.ConfigureServlets();
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;
		}

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestRMWebServicesDelegationTokens _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;
		}

		[Parameterized.Parameters]
		public static ICollection<object[]> GuiceConfigs()
		{
			return Arrays.AsList(new object[][] { new object[] { 0 }, new object[] { 1 } });
		}

		/// <exception cref="System.Exception"/>
		public TestRMWebServicesDelegationTokens(int run)
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.resourcemanager.webapp"
				).ContextListenerClass(typeof(TestRMWebServicesDelegationTokens.GuiceServletConfig
				)).FilterClass(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath
				("/").Build())
		{
			switch (run)
			{
				case 0:
				default:
				{
					injector = GetKerberosAuthInjector();
					break;
				}

				case 1:
				{
					injector = GetSimpleAuthInjector();
					break;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupKDC()
		{
			testRootDir = new FilePath("target", typeof(TestRMWebServicesDelegationTokens).FullName
				 + "-root");
			testMiniKDC = new MiniKdc(MiniKdc.CreateConf(), testRootDir);
			testMiniKDC.Start();
			testMiniKDC.CreatePrincipal(httpSpnegoKeytabFile, "HTTP/localhost", "client", "client2"
				, "client3");
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
			httpSpnegoKeytabFile.DeleteOnExit();
			testRootDir.DeleteOnExit();
		}

		[AfterClass]
		public static void ShutdownKdc()
		{
			if (testMiniKDC != null)
			{
				testMiniKDC.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public override void TearDown()
		{
			rm.Stop();
			base.TearDown();
		}

		// Simple test - try to create a delegation token via web services and check
		// to make sure we get back a valid token. Validate token using RM function
		// calls. It should only succeed with the kerberos filter
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateDelegationToken()
		{
			rm.Start();
			this.Client().AddFilter(new LoggingFilter(System.Console.Out));
			string renewer = "test-renewer";
			string jsonBody = "{ \"renewer\" : \"" + renewer + "\" }";
			string xmlBody = "<delegation-token><renewer>" + renewer + "</renewer></delegation-token>";
			string[] mediaTypes = new string[] { MediaType.ApplicationJson, MediaType.ApplicationXml
				 };
			IDictionary<string, string> bodyMap = new Dictionary<string, string>();
			bodyMap[MediaType.ApplicationJson] = jsonBody;
			bodyMap[MediaType.ApplicationXml] = xmlBody;
			foreach (string mediaType in mediaTypes)
			{
				string body = bodyMap[mediaType];
				foreach (string contentType in mediaTypes)
				{
					if (isKerberosAuth == true)
					{
						VerifyKerberosAuthCreate(mediaType, contentType, body, renewer);
					}
					else
					{
						VerifySimpleAuthCreate(mediaType, contentType, body);
					}
				}
			}
			rm.Stop();
			return;
		}

		private void VerifySimpleAuthCreate(string mediaType, string contentType, string 
			body)
		{
			ClientResponse response = Resource().Path("ws").Path("v1").Path("cluster").Path("delegation-token"
				).QueryParam("user.name", "testuser").Accept(contentType).Entity(body, mediaType
				).Post<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
				());
		}

		/// <exception cref="System.Exception"/>
		private void VerifyKerberosAuthCreate(string mType, string cType, string reqBody, 
			string renUser)
		{
			string mediaType = mType;
			string contentType = cType;
			string body = reqBody;
			string renewer = renUser;
			KerberosTestUtils.DoAsClient(new _Callable_313(this, contentType, body, mediaType
				, renewer));
		}

		private sealed class _Callable_313 : Callable<Void>
		{
			public _Callable_313(TestRMWebServicesDelegationTokens _enclosing, string contentType
				, string body, string mediaType, string renewer)
			{
				this._enclosing = _enclosing;
				this.contentType = contentType;
				this.body = body;
				this.mediaType = mediaType;
				this.renewer = renewer;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Accept(contentType).Entity(body, mediaType).Post<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				DelegationToken tok = this._enclosing.GetDelegationTokenFromResponse(response);
				NUnit.Framework.Assert.IsFalse(tok.GetToken().IsEmpty());
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
					<RMDelegationTokenIdentifier>();
				token.DecodeFromUrlString(tok.GetToken());
				NUnit.Framework.Assert.AreEqual(renewer, token.DecodeIdentifier().GetRenewer().ToString
					());
				this._enclosing.AssertValidRMToken(tok.GetToken());
				DelegationToken dtoken = new DelegationToken();
				response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster").Path(
					"delegation-token").Accept(contentType).Entity(dtoken, mediaType).Post<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				tok = this._enclosing.GetDelegationTokenFromResponse(response);
				NUnit.Framework.Assert.IsFalse(tok.GetToken().IsEmpty());
				token = new Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier>();
				token.DecodeFromUrlString(tok.GetToken());
				NUnit.Framework.Assert.AreEqual(string.Empty, token.DecodeIdentifier().GetRenewer
					().ToString());
				this._enclosing.AssertValidRMToken(tok.GetToken());
				return null;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly string contentType;

			private readonly string body;

			private readonly string mediaType;

			private readonly string renewer;
		}

		// Test to verify renew functionality - create a token and then try to renew
		// it. The renewer should succeed; owner and third user should fail
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenewDelegationToken()
		{
			Client().AddFilter(new LoggingFilter(System.Console.Out));
			rm.Start();
			string renewer = "client2";
			this.Client().AddFilter(new LoggingFilter(System.Console.Out));
			DelegationToken dummyToken = new DelegationToken();
			dummyToken.SetRenewer(renewer);
			string[] mediaTypes = new string[] { MediaType.ApplicationJson, MediaType.ApplicationXml
				 };
			foreach (string mediaType in mediaTypes)
			{
				foreach (string contentType in mediaTypes)
				{
					if (isKerberosAuth == false)
					{
						VerifySimpleAuthRenew(mediaType, contentType);
						continue;
					}
					// test "client" and client2" trying to renew "client" token
					DelegationToken responseToken = KerberosTestUtils.DoAsClient(new _Callable_367(this
						, contentType, dummyToken, mediaType));
					KerberosTestUtils.DoAs(renewer, new _Callable_390(this, responseToken, mediaType, 
						contentType));
					// renew twice so that we can confirm that the
					// expiration time actually changes
					// artificial sleep to ensure we get a different expiration time
					// test unauthorized user renew attempt
					KerberosTestUtils.DoAs("client3", new _Callable_431(this, mediaType, responseToken
						, contentType));
					// test bad request - incorrect format, empty token string and random
					// token string
					KerberosTestUtils.DoAsClient(new _Callable_449(this, mediaType, contentType));
				}
			}
			// missing token header
			rm.Stop();
			return;
		}

		private sealed class _Callable_367 : Callable<DelegationToken>
		{
			public _Callable_367(TestRMWebServicesDelegationTokens _enclosing, string contentType
				, DelegationToken dummyToken, string mediaType)
			{
				this._enclosing = _enclosing;
				this.contentType = contentType;
				this.dummyToken = dummyToken;
				this.mediaType = mediaType;
			}

			/// <exception cref="System.Exception"/>
			public DelegationToken Call()
			{
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Accept(contentType).Entity(dummyToken, mediaType).Post
					<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				DelegationToken tok = this._enclosing.GetDelegationTokenFromResponse(response);
				NUnit.Framework.Assert.IsFalse(tok.GetToken().IsEmpty());
				string body = TestRMWebServicesDelegationTokens.GenerateRenewTokenBody(mediaType, 
					tok.GetToken());
				response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster").Path(
					"delegation-token").Path("expiration").Header(this._enclosing.yarnTokenHeader, tok
					.GetToken()).Accept(contentType).Entity(body, mediaType).Post<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
					());
				return tok;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly string contentType;

			private readonly DelegationToken dummyToken;

			private readonly string mediaType;
		}

		private sealed class _Callable_390 : Callable<DelegationToken>
		{
			public _Callable_390(TestRMWebServicesDelegationTokens _enclosing, DelegationToken
				 responseToken, string mediaType, string contentType)
			{
				this._enclosing = _enclosing;
				this.responseToken = responseToken;
				this.mediaType = mediaType;
				this.contentType = contentType;
			}

			/// <exception cref="System.Exception"/>
			public DelegationToken Call()
			{
				long oldExpirationTime = Time.Now();
				this._enclosing.AssertValidRMToken(responseToken.GetToken());
				string body = TestRMWebServicesDelegationTokens.GenerateRenewTokenBody(mediaType, 
					responseToken.GetToken());
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Path("expiration").Header(this._enclosing.yarnTokenHeader
					, responseToken.GetToken()).Accept(contentType).Entity(body, mediaType).Post<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				DelegationToken tok = this._enclosing.GetDelegationTokenFromResponse(response);
				string message = "Expiration time not as expected: old = " + oldExpirationTime + 
					"; new = " + tok.GetNextExpirationTime();
				NUnit.Framework.Assert.IsTrue(message, tok.GetNextExpirationTime() > oldExpirationTime
					);
				oldExpirationTime = tok.GetNextExpirationTime();
				Sharpen.Thread.Sleep(1000);
				response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster").Path(
					"delegation-token").Path("expiration").Header(this._enclosing.yarnTokenHeader, responseToken
					.GetToken()).Accept(contentType).Entity(body, mediaType).Post<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				tok = this._enclosing.GetDelegationTokenFromResponse(response);
				message = "Expiration time not as expected: old = " + oldExpirationTime + "; new = "
					 + tok.GetNextExpirationTime();
				NUnit.Framework.Assert.IsTrue(message, tok.GetNextExpirationTime() > oldExpirationTime
					);
				return tok;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly DelegationToken responseToken;

			private readonly string mediaType;

			private readonly string contentType;
		}

		private sealed class _Callable_431 : Callable<DelegationToken>
		{
			public _Callable_431(TestRMWebServicesDelegationTokens _enclosing, string mediaType
				, DelegationToken responseToken, string contentType)
			{
				this._enclosing = _enclosing;
				this.mediaType = mediaType;
				this.responseToken = responseToken;
				this.contentType = contentType;
			}

			/// <exception cref="System.Exception"/>
			public DelegationToken Call()
			{
				string body = TestRMWebServicesDelegationTokens.GenerateRenewTokenBody(mediaType, 
					responseToken.GetToken());
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Path("expiration").Header(this._enclosing.yarnTokenHeader
					, responseToken.GetToken()).Accept(contentType).Entity(body, mediaType).Post<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
					());
				return null;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly string mediaType;

			private readonly DelegationToken responseToken;

			private readonly string contentType;
		}

		private sealed class _Callable_449 : Callable<Void>
		{
			public _Callable_449(TestRMWebServicesDelegationTokens _enclosing, string mediaType
				, string contentType)
			{
				this._enclosing = _enclosing;
				this.mediaType = mediaType;
				this.contentType = contentType;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				string token = "TEST_TOKEN_STRING";
				string body = string.Empty;
				if (mediaType.Equals(MediaType.ApplicationJson))
				{
					body = "{\"token\": \"" + token + "\" }";
				}
				else
				{
					body = "<delegation-token><token>" + token + "</token></delegation-token>";
				}
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Path("expiration").Accept(contentType).Entity(body, mediaType
					).Post<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.BadRequest, response.GetClientResponseStatus
					());
				return null;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly string mediaType;

			private readonly string contentType;
		}

		private void VerifySimpleAuthRenew(string mediaType, string contentType)
		{
			string token = "TEST_TOKEN_STRING";
			string body = string.Empty;
			// contents of body don't matter because the request processing shouldn't
			// get that far
			if (mediaType.Equals(MediaType.ApplicationJson))
			{
				body = "{\"token\": \"" + token + "\" }";
				body = "{\"abcd\": \"test-123\" }";
			}
			else
			{
				body = "<delegation-token><token>" + token + "</token></delegation-token>";
				body = "<delegation-token><xml>abcd</xml></delegation-token>";
			}
			ClientResponse response = Resource().Path("ws").Path("v1").Path("cluster").Path("delegation-token"
				).QueryParam("user.name", "testuser").Accept(contentType).Entity(body, mediaType
				).Post<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
				());
		}

		// Test to verify cancel functionality - create a token and then try to cancel
		// it. The owner and renewer should succeed; third user should fail
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCancelDelegationToken()
		{
			rm.Start();
			this.Client().AddFilter(new LoggingFilter(System.Console.Out));
			if (isKerberosAuth == false)
			{
				VerifySimpleAuthCancel();
				return;
			}
			DelegationToken dtoken = new DelegationToken();
			string renewer = "client2";
			dtoken.SetRenewer(renewer);
			string[] mediaTypes = new string[] { MediaType.ApplicationJson, MediaType.ApplicationXml
				 };
			foreach (string mediaType in mediaTypes)
			{
				foreach (string contentType in mediaTypes)
				{
					// owner should be able to cancel delegation token
					KerberosTestUtils.DoAsClient(new _Callable_520(this, contentType, dtoken, mediaType
						));
					// renewer should be able to cancel token
					DelegationToken tmpToken = KerberosTestUtils.DoAsClient(new _Callable_542(this, contentType
						, dtoken, mediaType));
					KerberosTestUtils.DoAs(renewer, new _Callable_555(this, tmpToken, contentType));
					// third user should not be able to cancel token
					DelegationToken tmpToken2 = KerberosTestUtils.DoAsClient(new _Callable_571(this, 
						contentType, dtoken, mediaType));
					KerberosTestUtils.DoAs("client3", new _Callable_584(this, tmpToken2, contentType)
						);
					TestCancelTokenBadRequests(mediaType, contentType);
				}
			}
			rm.Stop();
			return;
		}

		private sealed class _Callable_520 : Callable<Void>
		{
			public _Callable_520(TestRMWebServicesDelegationTokens _enclosing, string contentType
				, DelegationToken dtoken, string mediaType)
			{
				this._enclosing = _enclosing;
				this.contentType = contentType;
				this.dtoken = dtoken;
				this.mediaType = mediaType;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Accept(contentType).Entity(dtoken, mediaType).Post<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				DelegationToken tok = this._enclosing.GetDelegationTokenFromResponse(response);
				response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster").Path(
					"delegation-token").Header(this._enclosing.yarnTokenHeader, tok.GetToken()).Accept
					(contentType).Delete<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				this._enclosing.AssertTokenCancelled(tok.GetToken());
				return null;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly string contentType;

			private readonly DelegationToken dtoken;

			private readonly string mediaType;
		}

		private sealed class _Callable_542 : Callable<DelegationToken>
		{
			public _Callable_542(TestRMWebServicesDelegationTokens _enclosing, string contentType
				, DelegationToken dtoken, string mediaType)
			{
				this._enclosing = _enclosing;
				this.contentType = contentType;
				this.dtoken = dtoken;
				this.mediaType = mediaType;
			}

			/// <exception cref="System.Exception"/>
			public DelegationToken Call()
			{
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Accept(contentType).Entity(dtoken, mediaType).Post<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				DelegationToken tok = this._enclosing.GetDelegationTokenFromResponse(response);
				return tok;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly string contentType;

			private readonly DelegationToken dtoken;

			private readonly string mediaType;
		}

		private sealed class _Callable_555 : Callable<Void>
		{
			public _Callable_555(TestRMWebServicesDelegationTokens _enclosing, DelegationToken
				 tmpToken, string contentType)
			{
				this._enclosing = _enclosing;
				this.tmpToken = tmpToken;
				this.contentType = contentType;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Header(this._enclosing.yarnTokenHeader, tmpToken.GetToken
					()).Accept(contentType).Delete<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				this._enclosing.AssertTokenCancelled(tmpToken.GetToken());
				return null;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly DelegationToken tmpToken;

			private readonly string contentType;
		}

		private sealed class _Callable_571 : Callable<DelegationToken>
		{
			public _Callable_571(TestRMWebServicesDelegationTokens _enclosing, string contentType
				, DelegationToken dtoken, string mediaType)
			{
				this._enclosing = _enclosing;
				this.contentType = contentType;
				this.dtoken = dtoken;
				this.mediaType = mediaType;
			}

			/// <exception cref="System.Exception"/>
			public DelegationToken Call()
			{
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Accept(contentType).Entity(dtoken, mediaType).Post<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				DelegationToken tok = this._enclosing.GetDelegationTokenFromResponse(response);
				return tok;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly string contentType;

			private readonly DelegationToken dtoken;

			private readonly string mediaType;
		}

		private sealed class _Callable_584 : Callable<Void>
		{
			public _Callable_584(TestRMWebServicesDelegationTokens _enclosing, DelegationToken
				 tmpToken2, string contentType)
			{
				this._enclosing = _enclosing;
				this.tmpToken2 = tmpToken2;
				this.contentType = contentType;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Header(this._enclosing.yarnTokenHeader, tmpToken2.GetToken
					()).Accept(contentType).Delete<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
					());
				this._enclosing.AssertValidRMToken(tmpToken2.GetToken());
				return null;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly DelegationToken tmpToken2;

			private readonly string contentType;
		}

		/// <exception cref="System.Exception"/>
		private void TestCancelTokenBadRequests(string mType, string cType)
		{
			string mediaType = mType;
			string contentType = cType;
			DelegationToken dtoken = new DelegationToken();
			string renewer = "client2";
			dtoken.SetRenewer(renewer);
			// bad request(invalid header value)
			KerberosTestUtils.DoAsClient(new _Callable_616(this, contentType));
			// bad request(missing header)
			KerberosTestUtils.DoAsClient(new _Callable_630(this, contentType));
			// bad request(cancelled token)
			DelegationToken tmpToken = KerberosTestUtils.DoAsClient(new _Callable_645(this, contentType
				, dtoken, mediaType));
			KerberosTestUtils.DoAs(renewer, new _Callable_658(this, tmpToken, contentType));
		}

		private sealed class _Callable_616 : Callable<Void>
		{
			public _Callable_616(TestRMWebServicesDelegationTokens _enclosing, string contentType
				)
			{
				this._enclosing = _enclosing;
				this.contentType = contentType;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Header(this._enclosing.yarnTokenHeader, "random-string"
					).Accept(contentType).Delete<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.BadRequest, response.GetClientResponseStatus
					());
				return null;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly string contentType;
		}

		private sealed class _Callable_630 : Callable<Void>
		{
			public _Callable_630(TestRMWebServicesDelegationTokens _enclosing, string contentType
				)
			{
				this._enclosing = _enclosing;
				this.contentType = contentType;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Accept(contentType).Delete<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.BadRequest, response.GetClientResponseStatus
					());
				return null;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly string contentType;
		}

		private sealed class _Callable_645 : Callable<DelegationToken>
		{
			public _Callable_645(TestRMWebServicesDelegationTokens _enclosing, string contentType
				, DelegationToken dtoken, string mediaType)
			{
				this._enclosing = _enclosing;
				this.contentType = contentType;
				this.dtoken = dtoken;
				this.mediaType = mediaType;
			}

			/// <exception cref="System.Exception"/>
			public DelegationToken Call()
			{
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Accept(contentType).Entity(dtoken, mediaType).Post<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				DelegationToken tok = this._enclosing.GetDelegationTokenFromResponse(response);
				return tok;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly string contentType;

			private readonly DelegationToken dtoken;

			private readonly string mediaType;
		}

		private sealed class _Callable_658 : Callable<Void>
		{
			public _Callable_658(TestRMWebServicesDelegationTokens _enclosing, DelegationToken
				 tmpToken, string contentType)
			{
				this._enclosing = _enclosing;
				this.tmpToken = tmpToken;
				this.contentType = contentType;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				ClientResponse response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster"
					).Path("delegation-token").Header(this._enclosing.yarnTokenHeader, tmpToken.GetToken
					()).Accept(contentType).Delete<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				response = this._enclosing.Resource().Path("ws").Path("v1").Path("cluster").Path(
					"delegation-token").Header(this._enclosing.yarnTokenHeader, tmpToken.GetToken())
					.Accept(contentType).Delete<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.BadRequest, response.GetClientResponseStatus
					());
				return null;
			}

			private readonly TestRMWebServicesDelegationTokens _enclosing;

			private readonly DelegationToken tmpToken;

			private readonly string contentType;
		}

		private void VerifySimpleAuthCancel()
		{
			// contents of header don't matter; request should never get that far
			ClientResponse response = Resource().Path("ws").Path("v1").Path("cluster").Path("delegation-token"
				).QueryParam("user.name", "testuser").Header(RMWebServices.DelegationTokenHeader
				, "random").Delete<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
				());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		private DelegationToken GetDelegationTokenFromResponse(ClientResponse response)
		{
			if (response.GetType().ToString().Equals(MediaType.ApplicationJson))
			{
				return GetDelegationTokenFromJson(response.GetEntity<JSONObject>());
			}
			return GetDelegationTokenFromXML(response.GetEntity<string>());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public static DelegationToken GetDelegationTokenFromXML(string tokenXML)
		{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(tokenXML));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("delegation-token");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			Element element = (Element)nodes.Item(0);
			DelegationToken ret = new DelegationToken();
			string token = WebServicesTestUtils.GetXmlString(element, "token");
			if (token != null)
			{
				ret.SetToken(token);
			}
			else
			{
				long expiration = WebServicesTestUtils.GetXmlLong(element, "expiration-time");
				ret.SetNextExpirationTime(expiration);
			}
			return ret;
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public static DelegationToken GetDelegationTokenFromJson(JSONObject json)
		{
			DelegationToken ret = new DelegationToken();
			if (json.Has("token"))
			{
				ret.SetToken(json.GetString("token"));
			}
			else
			{
				if (json.Has("expiration-time"))
				{
					ret.SetNextExpirationTime(json.GetLong("expiration-time"));
				}
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertValidRMToken(string encodedToken)
		{
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> realToken = new 
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier>();
			realToken.DecodeFromUrlString(encodedToken);
			RMDelegationTokenIdentifier ident = rm.GetRMContext().GetRMDelegationTokenSecretManager
				().DecodeTokenIdentifier(realToken);
			rm.GetRMContext().GetRMDelegationTokenSecretManager().VerifyToken(ident, realToken
				.GetPassword());
			NUnit.Framework.Assert.IsTrue(rm.GetRMContext().GetRMDelegationTokenSecretManager
				().GetAllTokens().Contains(ident));
		}

		/// <exception cref="System.Exception"/>
		private void AssertTokenCancelled(string encodedToken)
		{
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> realToken = new 
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier>();
			realToken.DecodeFromUrlString(encodedToken);
			RMDelegationTokenIdentifier ident = rm.GetRMContext().GetRMDelegationTokenSecretManager
				().DecodeTokenIdentifier(realToken);
			bool exceptionCaught = false;
			try
			{
				rm.GetRMContext().GetRMDelegationTokenSecretManager().VerifyToken(ident, realToken
					.GetPassword());
			}
			catch (SecretManager.InvalidToken)
			{
				exceptionCaught = true;
			}
			NUnit.Framework.Assert.IsTrue("InvalidToken exception not thrown", exceptionCaught
				);
			NUnit.Framework.Assert.IsFalse(rm.GetRMContext().GetRMDelegationTokenSecretManager
				().GetAllTokens().Contains(ident));
		}

		private static string GenerateRenewTokenBody(string mediaType, string token)
		{
			string body = string.Empty;
			if (mediaType.Equals(MediaType.ApplicationJson))
			{
				body = "{\"token\": \"" + token + "\" }";
			}
			else
			{
				body = "<delegation-token><token>" + token + "</token></delegation-token>";
			}
			return body;
		}
	}
}
