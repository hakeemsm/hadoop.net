using System;
using System.Collections;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Security.Authentication;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Client
{
	public class TestKerberosAuthenticator : KerberosSecurityTestcase
	{
		private bool useTomcat = false;

		public TestKerberosAuthenticator(bool useTomcat)
		{
			this.useTomcat = useTomcat;
		}

		[Parameterized.Parameters]
		public static ICollection Booleans()
		{
			return Arrays.AsList(new object[][] { new object[] { false }, new object[] { true
				 } });
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			// create keytab
			FilePath keytabFile = new FilePath(KerberosTestUtils.GetKeytabFile());
			string clientPrincipal = KerberosTestUtils.GetClientPrincipal();
			string serverPrincipal = KerberosTestUtils.GetServerPrincipal();
			clientPrincipal = Sharpen.Runtime.Substring(clientPrincipal, 0, clientPrincipal.LastIndexOf
				("@"));
			serverPrincipal = Sharpen.Runtime.Substring(serverPrincipal, 0, serverPrincipal.LastIndexOf
				("@"));
			GetKdc().CreatePrincipal(keytabFile, clientPrincipal, serverPrincipal);
		}

		private Properties GetAuthenticationHandlerConfiguration()
		{
			Properties props = new Properties();
			props.SetProperty(AuthenticationFilter.AuthType, "kerberos");
			props.SetProperty(KerberosAuthenticationHandler.Principal, KerberosTestUtils.GetServerPrincipal
				());
			props.SetProperty(KerberosAuthenticationHandler.Keytab, KerberosTestUtils.GetKeytabFile
				());
			props.SetProperty(KerberosAuthenticationHandler.NameRules, "RULE:[1:$1@$0](.*@" +
				 KerberosTestUtils.GetRealm() + ")s/@.*//\n");
			return props;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFallbacktoPseudoAuthenticator()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
			Properties props = new Properties();
			props.SetProperty(AuthenticationFilter.AuthType, "simple");
			props.SetProperty(PseudoAuthenticationHandler.AnonymousAllowed, "false");
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(props);
			auth._testAuthentication(new KerberosAuthenticator(), false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFallbacktoPseudoAuthenticatorAnonymous()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
			Properties props = new Properties();
			props.SetProperty(AuthenticationFilter.AuthType, "simple");
			props.SetProperty(PseudoAuthenticationHandler.AnonymousAllowed, "true");
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(props);
			auth._testAuthentication(new KerberosAuthenticator(), false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNotAuthenticated()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				());
			auth.Start();
			try
			{
				Uri url = new Uri(auth.GetBaseURL());
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.Connect();
				Assert.Equal(HttpURLConnection.HttpUnauthorized, conn.GetResponseCode
					());
				Assert.True(conn.GetHeaderField(KerberosAuthenticator.WwwAuthenticate
					) != null);
			}
			finally
			{
				auth.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAuthentication()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				());
			KerberosTestUtils.DoAsClient(new _Callable_115(auth));
		}

		private sealed class _Callable_115 : Callable<Void>
		{
			public _Callable_115(AuthenticatorTestCase auth)
			{
				this.auth = auth;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				auth._testAuthentication(new KerberosAuthenticator(), false);
				return null;
			}

			private readonly AuthenticatorTestCase auth;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAuthenticationPost()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				());
			KerberosTestUtils.DoAsClient(new _Callable_129(auth));
		}

		private sealed class _Callable_129 : Callable<Void>
		{
			public _Callable_129(AuthenticatorTestCase auth)
			{
				this.auth = auth;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				auth._testAuthentication(new KerberosAuthenticator(), true);
				return null;
			}

			private readonly AuthenticatorTestCase auth;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAuthenticationHttpClient()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				());
			KerberosTestUtils.DoAsClient(new _Callable_143(auth));
		}

		private sealed class _Callable_143 : Callable<Void>
		{
			public _Callable_143(AuthenticatorTestCase auth)
			{
				this.auth = auth;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				auth._testAuthenticationHttpClient(new KerberosAuthenticator(), false);
				return null;
			}

			private readonly AuthenticatorTestCase auth;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAuthenticationHttpClientPost()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				());
			KerberosTestUtils.DoAsClient(new _Callable_157(auth));
		}

		private sealed class _Callable_157 : Callable<Void>
		{
			public _Callable_157(AuthenticatorTestCase auth)
			{
				this.auth = auth;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				auth._testAuthenticationHttpClient(new KerberosAuthenticator(), true);
				return null;
			}

			private readonly AuthenticatorTestCase auth;
		}
	}
}
