using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Client
{
	public class TestPseudoAuthenticator
	{
		private Properties GetAuthenticationHandlerConfiguration(bool anonymousAllowed)
		{
			Properties props = new Properties();
			props.SetProperty(AuthenticationFilter.AuthType, "simple");
			props.SetProperty(PseudoAuthenticationHandler.AnonymousAllowed, bool.ToString(anonymousAllowed
				));
			return props;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetUserName()
		{
			PseudoAuthenticator authenticator = new PseudoAuthenticator();
			NUnit.Framework.Assert.AreEqual(Runtime.GetProperty("user.name"), authenticator.GetUserName
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAnonymousAllowed()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase();
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				(true));
			auth.Start();
			try
			{
				Uri url = new Uri(auth.GetBaseURL());
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			}
			finally
			{
				auth.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAnonymousDisallowed()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase();
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				(false));
			auth.Start();
			try
			{
				Uri url = new Uri(auth.GetBaseURL());
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpUnauthorized, conn.GetResponseCode
					());
				NUnit.Framework.Assert.IsTrue(conn.GetHeaderFields().Contains("WWW-Authenticate")
					);
				NUnit.Framework.Assert.AreEqual("Authentication required", conn.GetResponseMessage
					());
			}
			finally
			{
				auth.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuthenticationAnonymousAllowed()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase();
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				(true));
			auth._testAuthentication(new PseudoAuthenticator(), false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuthenticationAnonymousDisallowed()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase();
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				(false));
			auth._testAuthentication(new PseudoAuthenticator(), false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuthenticationAnonymousAllowedWithPost()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase();
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				(true));
			auth._testAuthentication(new PseudoAuthenticator(), true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuthenticationAnonymousDisallowedWithPost()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase();
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				(false));
			auth._testAuthentication(new PseudoAuthenticator(), true);
		}
	}
}
