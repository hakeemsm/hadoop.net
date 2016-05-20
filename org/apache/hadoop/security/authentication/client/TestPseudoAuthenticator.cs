using Sharpen;

namespace org.apache.hadoop.security.authentication.client
{
	public class TestPseudoAuthenticator
	{
		private java.util.Properties getAuthenticationHandlerConfiguration(bool anonymousAllowed
			)
		{
			java.util.Properties props = new java.util.Properties();
			props.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.AUTH_TYPE, "simple");
			props.setProperty(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				.ANONYMOUS_ALLOWED, bool.toString(anonymousAllowed));
			return props;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetUserName()
		{
			org.apache.hadoop.security.authentication.client.PseudoAuthenticator authenticator
				 = new org.apache.hadoop.security.authentication.client.PseudoAuthenticator();
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getProperty("user.name"), authenticator
				.getUserName());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAnonymousAllowed()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase();
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(getAuthenticationHandlerConfiguration(true));
			auth.start();
			try
			{
				java.net.URL url = new java.net.URL(auth.getBaseURL());
				java.net.HttpURLConnection conn = (java.net.HttpURLConnection)url.openConnection(
					);
				conn.connect();
				NUnit.Framework.Assert.AreEqual(java.net.HttpURLConnection.HTTP_OK, conn.getResponseCode
					());
			}
			finally
			{
				auth.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAnonymousDisallowed()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase();
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(getAuthenticationHandlerConfiguration(false));
			auth.start();
			try
			{
				java.net.URL url = new java.net.URL(auth.getBaseURL());
				java.net.HttpURLConnection conn = (java.net.HttpURLConnection)url.openConnection(
					);
				conn.connect();
				NUnit.Framework.Assert.AreEqual(java.net.HttpURLConnection.HTTP_UNAUTHORIZED, conn
					.getResponseCode());
				NUnit.Framework.Assert.IsTrue(conn.getHeaderFields().Contains("WWW-Authenticate")
					);
				NUnit.Framework.Assert.AreEqual("Authentication required", conn.getResponseMessage
					());
			}
			finally
			{
				auth.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAuthenticationAnonymousAllowed()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase();
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(getAuthenticationHandlerConfiguration(true));
			auth._testAuthentication(new org.apache.hadoop.security.authentication.client.PseudoAuthenticator
				(), false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAuthenticationAnonymousDisallowed()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase();
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(getAuthenticationHandlerConfiguration(false));
			auth._testAuthentication(new org.apache.hadoop.security.authentication.client.PseudoAuthenticator
				(), false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAuthenticationAnonymousAllowedWithPost()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase();
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(getAuthenticationHandlerConfiguration(true));
			auth._testAuthentication(new org.apache.hadoop.security.authentication.client.PseudoAuthenticator
				(), true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAuthenticationAnonymousDisallowedWithPost()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase auth = new 
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase();
			org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.setAuthenticationHandlerConfig
				(getAuthenticationHandlerConfiguration(false));
			auth._testAuthentication(new org.apache.hadoop.security.authentication.client.PseudoAuthenticator
				(), true);
		}
	}
}
