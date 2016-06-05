using System;
using System.Collections.Generic;
using Javax.Security.Auth.Kerberos;
using Javax.Servlet;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Security.Authentication;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Authentication.Util;


namespace Org.Apache.Hadoop.Security.Authentication.Server
{
	public class TestKerberosAuthenticationHandler : KerberosSecurityTestcase
	{
		protected internal KerberosAuthenticationHandler handler;

		protected internal virtual KerberosAuthenticationHandler GetNewAuthenticationHandler
			()
		{
			return new KerberosAuthenticationHandler();
		}

		protected internal virtual string GetExpectedType()
		{
			return KerberosAuthenticationHandler.Type;
		}

		protected internal virtual Properties GetDefaultProperties()
		{
			Properties props = new Properties();
			props.SetProperty(KerberosAuthenticationHandler.Principal, KerberosTestUtils.GetServerPrincipal
				());
			props.SetProperty(KerberosAuthenticationHandler.Keytab, KerberosTestUtils.GetKeytabFile
				());
			props.SetProperty(KerberosAuthenticationHandler.NameRules, "RULE:[1:$1@$0](.*@" +
				 KerberosTestUtils.GetRealm() + ")s/@.*//\n");
			return props;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			// create keytab
			FilePath keytabFile = new FilePath(KerberosTestUtils.GetKeytabFile());
			string clientPrincipal = KerberosTestUtils.GetClientPrincipal();
			string serverPrincipal = KerberosTestUtils.GetServerPrincipal();
			clientPrincipal = Runtime.Substring(clientPrincipal, 0, clientPrincipal.LastIndexOf
				("@"));
			serverPrincipal = Runtime.Substring(serverPrincipal, 0, serverPrincipal.LastIndexOf
				("@"));
			GetKdc().CreatePrincipal(keytabFile, clientPrincipal, serverPrincipal);
			// handler
			handler = GetNewAuthenticationHandler();
			Properties props = GetDefaultProperties();
			try
			{
				handler.Init(props);
			}
			catch (Exception ex)
			{
				handler = null;
				throw;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNameRules()
		{
			KerberosName kn = new KerberosName(KerberosTestUtils.GetServerPrincipal());
			Assert.Equal(KerberosTestUtils.GetRealm(), kn.GetRealm());
			//destroy handler created in setUp()
			handler.Destroy();
			KerberosName.SetRules("RULE:[1:$1@$0](.*@FOO)s/@.*//\nDEFAULT");
			handler = GetNewAuthenticationHandler();
			Properties props = GetDefaultProperties();
			props.SetProperty(KerberosAuthenticationHandler.NameRules, "RULE:[1:$1@$0](.*@BAR)s/@.*//\nDEFAULT"
				);
			try
			{
				handler.Init(props);
			}
			catch (Exception)
			{
			}
			kn = new KerberosName("bar@BAR");
			Assert.Equal("bar", kn.GetShortName());
			kn = new KerberosName("bar@FOO");
			try
			{
				kn.GetShortName();
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInit()
		{
			Assert.Equal(KerberosTestUtils.GetKeytabFile(), handler.GetKeytab
				());
			ICollection<KerberosPrincipal> principals = handler.GetPrincipals();
			Principal expectedPrincipal = new KerberosPrincipal(KerberosTestUtils.GetServerPrincipal
				());
			Assert.True(principals.Contains(expectedPrincipal));
			Assert.Equal(1, principals.Count);
		}

		// dynamic configuration of HTTP principals
		/// <exception cref="System.Exception"/>
		public virtual void TestDynamicPrincipalDiscovery()
		{
			string[] keytabUsers = new string[] { "HTTP/host1", "HTTP/host2", "HTTP2/host1", 
				"XHTTP/host" };
			string keytab = KerberosTestUtils.GetKeytabFile();
			GetKdc().CreatePrincipal(new FilePath(keytab), keytabUsers);
			// destroy handler created in setUp()
			handler.Destroy();
			Properties props = new Properties();
			props.SetProperty(KerberosAuthenticationHandler.Keytab, keytab);
			props.SetProperty(KerberosAuthenticationHandler.Principal, "*");
			handler = GetNewAuthenticationHandler();
			handler.Init(props);
			Assert.Equal(KerberosTestUtils.GetKeytabFile(), handler.GetKeytab
				());
			ICollection<KerberosPrincipal> loginPrincipals = handler.GetPrincipals();
			foreach (string user in keytabUsers)
			{
				Principal principal = new KerberosPrincipal(user + "@" + KerberosTestUtils.GetRealm
					());
				bool expected = user.StartsWith("HTTP/");
				Assert.Equal("checking for " + user, expected, loginPrincipals
					.Contains(principal));
			}
		}

		// dynamic configuration of HTTP principals
		/// <exception cref="System.Exception"/>
		public virtual void TestDynamicPrincipalDiscoveryMissingPrincipals()
		{
			string[] keytabUsers = new string[] { "hdfs/localhost" };
			string keytab = KerberosTestUtils.GetKeytabFile();
			GetKdc().CreatePrincipal(new FilePath(keytab), keytabUsers);
			// destroy handler created in setUp()
			handler.Destroy();
			Properties props = new Properties();
			props.SetProperty(KerberosAuthenticationHandler.Keytab, keytab);
			props.SetProperty(KerberosAuthenticationHandler.Principal, "*");
			handler = GetNewAuthenticationHandler();
			try
			{
				handler.Init(props);
				NUnit.Framework.Assert.Fail("init should have failed");
			}
			catch (ServletException ex)
			{
				Assert.Equal("Principals do not exist in the keytab", ex.InnerException
					.Message);
			}
			catch (Exception t)
			{
				NUnit.Framework.Assert.Fail("wrong exception: " + t);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestType()
		{
			Assert.Equal(GetExpectedType(), handler.GetType());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRequestWithoutAuthorization()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			NUnit.Framework.Assert.IsNull(handler.Authenticate(request, response));
			Org.Mockito.Mockito.Verify(response).SetHeader(KerberosAuthenticator.WwwAuthenticate
				, KerberosAuthenticator.Negotiate);
			Org.Mockito.Mockito.Verify(response).SetStatus(HttpServletResponse.ScUnauthorized
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRequestWithInvalidAuthorization()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(request.GetHeader(KerberosAuthenticator.Authorization)).
				ThenReturn("invalid");
			NUnit.Framework.Assert.IsNull(handler.Authenticate(request, response));
			Org.Mockito.Mockito.Verify(response).SetHeader(KerberosAuthenticator.WwwAuthenticate
				, KerberosAuthenticator.Negotiate);
			Org.Mockito.Mockito.Verify(response).SetStatus(HttpServletResponse.ScUnauthorized
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRequestWithIncompleteAuthorization()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(request.GetHeader(KerberosAuthenticator.Authorization)).
				ThenReturn(KerberosAuthenticator.Negotiate);
			try
			{
				handler.Authenticate(request, response);
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthenticationException)
			{
			}
			catch (Exception)
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRequestWithAuthorization()
		{
			string token = KerberosTestUtils.DoAsClient(new _Callable_225());
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(request.GetHeader(KerberosAuthenticator.Authorization)).
				ThenReturn(KerberosAuthenticator.Negotiate + " " + token);
			Org.Mockito.Mockito.When(request.GetServerName()).ThenReturn("localhost");
			AuthenticationToken authToken = handler.Authenticate(request, response);
			if (authToken != null)
			{
				Org.Mockito.Mockito.Verify(response).SetHeader(Org.Mockito.Mockito.Eq(KerberosAuthenticator
					.WwwAuthenticate), Org.Mockito.Mockito.Matches(KerberosAuthenticator.Negotiate +
					 " .*"));
				Org.Mockito.Mockito.Verify(response).SetStatus(HttpServletResponse.ScOk);
				Assert.Equal(KerberosTestUtils.GetClientPrincipal(), authToken
					.GetName());
				Assert.True(KerberosTestUtils.GetClientPrincipal().StartsWith(authToken
					.GetUserName()));
				Assert.Equal(GetExpectedType(), authToken.GetType());
			}
			else
			{
				Org.Mockito.Mockito.Verify(response).SetHeader(Org.Mockito.Mockito.Eq(KerberosAuthenticator
					.WwwAuthenticate), Org.Mockito.Mockito.Matches(KerberosAuthenticator.Negotiate +
					 " .*"));
				Org.Mockito.Mockito.Verify(response).SetStatus(HttpServletResponse.ScUnauthorized
					);
			}
		}

		private sealed class _Callable_225 : Callable<string>
		{
			public _Callable_225()
			{
			}

			/// <exception cref="System.Exception"/>
			public string Call()
			{
				GSSManager gssManager = GSSManager.GetInstance();
				GSSContext gssContext = null;
				try
				{
					string servicePrincipal = KerberosTestUtils.GetServerPrincipal();
					Oid oid = KerberosUtil.GetOidInstance("NT_GSS_KRB5_PRINCIPAL");
					GSSName serviceName = gssManager.CreateName(servicePrincipal, oid);
					oid = KerberosUtil.GetOidInstance("GSS_KRB5_MECH_OID");
					gssContext = gssManager.CreateContext(serviceName, oid, null, GSSContext.
						DefaultLifetime);
					gssContext.RequestCredDeleg(true);
					gssContext.RequestMutualAuth(true);
					byte[] inToken = new byte[0];
					byte[] outToken = gssContext.InitSecContext(inToken, 0, inToken.Length);
					Base64 base64 = new Base64(0);
					return base64.EncodeToString(outToken);
				}
				finally
				{
					if (gssContext != null)
					{
						gssContext.Dispose();
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRequestWithInvalidKerberosAuthorization()
		{
			string token = new Base64(0).EncodeToString(new byte[] { 0, 1, 2 });
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(request.GetHeader(KerberosAuthenticator.Authorization)).
				ThenReturn(KerberosAuthenticator.Negotiate + token);
			try
			{
				handler.Authenticate(request, response);
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthenticationException)
			{
			}
			catch (Exception)
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (handler != null)
			{
				handler.Destroy();
				handler = null;
			}
		}
	}
}
