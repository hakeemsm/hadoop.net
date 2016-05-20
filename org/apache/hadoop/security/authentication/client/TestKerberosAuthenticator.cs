using Sharpen;

namespace org.apache.hadoop.security.authentication.client
{
	public class TestKerberosAuthenticator : org.apache.hadoop.minikdc.KerberosSecurityTestcase
	{
		private bool useTomcat = false;

		public TestKerberosAuthenticator(bool useTomcat)
		{
			this.useTomcat = useTomcat;
		}

		[NUnit.Framework.runners.Parameterized.Parameters]
		public static System.Collections.ICollection booleans()
		{
			return java.util.Arrays.asList(new object[][] { new object[] { false }, new object
				[] { true } });
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			// create keytab
			java.io.File keytabFile = new java.io.File(org.apache.hadoop.security.authentication.KerberosTestUtils
				.getKeytabFile());
			string clientPrincipal = org.apache.hadoop.security.authentication.KerberosTestUtils
				.getClientPrincipal();
			string serverPrincipal = org.apache.hadoop.security.authentication.KerberosTestUtils
				.getServerPrincipal();
			clientPrincipal = Sharpen.Runtime.substring(clientPrincipal, 0, clientPrincipal.LastIndexOf
				("@"));
			serverPrincipal = Sharpen.Runtime.substring(serverPrincipal, 0, serverPrincipal.LastIndexOf
				("@"));
			getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
		}

		private java.util.Properties getAuthenticationHandlerConfiguration()
		{
			java.util.Properties props = new java.util.Properties();
			props.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.AUTH_TYPE, "kerberos");
			props.setProperty(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.PRINCIPAL, org.apache.hadoop.security.authentication.KerberosTestUtils.getServerPrincipal
				());
			props.setProperty(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.KEYTAB, org.apache.hadoop.security.authentication.KerberosTestUtils.getKeytabFile
				());
			props.setProperty(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				.NAME_RULES, "RULE:[1:$1@$0](.*@" + org.apache.hadoop.security.authentication.KerberosTestUtils
				.getRealm() + ")s/@.*//\n");
			return props;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testFallbacktoPseudoAuthenticator()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase(useTomcat
				);
			java.util.Properties props = new java.util.Properties();
			props.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.AUTH_TYPE, "simple");
			props.setProperty(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				.ANONYMOUS_ALLOWED, "false");
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(props);
			auth._testAuthentication(new org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				(), false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testFallbacktoPseudoAuthenticatorAnonymous()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase(useTomcat
				);
			java.util.Properties props = new java.util.Properties();
			props.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.AUTH_TYPE, "simple");
			props.setProperty(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				.ANONYMOUS_ALLOWED, "true");
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(props);
			auth._testAuthentication(new org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				(), false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testNotAuthenticated()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase(useTomcat
				);
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(getAuthenticationHandlerConfiguration());
			auth.start();
			try
			{
				java.net.URL url = new java.net.URL(auth.getBaseURL());
				java.net.HttpURLConnection conn = (java.net.HttpURLConnection)url.openConnection(
					);
				conn.connect();
				NUnit.Framework.Assert.AreEqual(java.net.HttpURLConnection.HTTP_UNAUTHORIZED, conn
					.getResponseCode());
				NUnit.Framework.Assert.IsTrue(conn.getHeaderField(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
					.WWW_AUTHENTICATE) != null);
			}
			finally
			{
				auth.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAuthentication()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase(useTomcat
				);
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(getAuthenticationHandlerConfiguration());
			org.apache.hadoop.security.authentication.KerberosTestUtils.doAsClient(new _Callable_115
				(auth));
		}

		private sealed class _Callable_115 : java.util.concurrent.Callable<java.lang.Void
			>
		{
			public _Callable_115(org.apache.hadoop.security.authentication.client.AuthenticatorTestCase
				 auth)
			{
				this.auth = auth;
			}

			/// <exception cref="System.Exception"/>
			public java.lang.Void call()
			{
				auth._testAuthentication(new org.apache.hadoop.security.authentication.client.KerberosAuthenticator
					(), false);
				return null;
			}

			private readonly org.apache.hadoop.security.authentication.client.AuthenticatorTestCase
				 auth;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAuthenticationPost()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase(useTomcat
				);
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(getAuthenticationHandlerConfiguration());
			org.apache.hadoop.security.authentication.KerberosTestUtils.doAsClient(new _Callable_129
				(auth));
		}

		private sealed class _Callable_129 : java.util.concurrent.Callable<java.lang.Void
			>
		{
			public _Callable_129(org.apache.hadoop.security.authentication.client.AuthenticatorTestCase
				 auth)
			{
				this.auth = auth;
			}

			/// <exception cref="System.Exception"/>
			public java.lang.Void call()
			{
				auth._testAuthentication(new org.apache.hadoop.security.authentication.client.KerberosAuthenticator
					(), true);
				return null;
			}

			private readonly org.apache.hadoop.security.authentication.client.AuthenticatorTestCase
				 auth;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAuthenticationHttpClient()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase(useTomcat
				);
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(getAuthenticationHandlerConfiguration());
			org.apache.hadoop.security.authentication.KerberosTestUtils.doAsClient(new _Callable_143
				(auth));
		}

		private sealed class _Callable_143 : java.util.concurrent.Callable<java.lang.Void
			>
		{
			public _Callable_143(org.apache.hadoop.security.authentication.client.AuthenticatorTestCase
				 auth)
			{
				this.auth = auth;
			}

			/// <exception cref="System.Exception"/>
			public java.lang.Void call()
			{
				auth._testAuthenticationHttpClient(new org.apache.hadoop.security.authentication.client.KerberosAuthenticator
					(), false);
				return null;
			}

			private readonly org.apache.hadoop.security.authentication.client.AuthenticatorTestCase
				 auth;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAuthenticationHttpClientPost()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase(useTomcat
				);
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(getAuthenticationHandlerConfiguration());
			org.apache.hadoop.security.authentication.KerberosTestUtils.doAsClient(new _Callable_157
				(auth));
		}

		private sealed class _Callable_157 : java.util.concurrent.Callable<java.lang.Void
			>
		{
			public _Callable_157(org.apache.hadoop.security.authentication.client.AuthenticatorTestCase
				 auth)
			{
				this.auth = auth;
			}

			/// <exception cref="System.Exception"/>
			public java.lang.Void call()
			{
				auth._testAuthenticationHttpClient(new org.apache.hadoop.security.authentication.client.KerberosAuthenticator
					(), true);
				return null;
			}

			private readonly org.apache.hadoop.security.authentication.client.AuthenticatorTestCase
				 auth;
		}
	}
}
