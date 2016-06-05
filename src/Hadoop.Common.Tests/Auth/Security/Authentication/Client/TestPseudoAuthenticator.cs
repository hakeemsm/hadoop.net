using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Security.Authentication.Server;


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
		[Fact]
		public virtual void TestGetUserName()
		{
			PseudoAuthenticator authenticator = new PseudoAuthenticator();
			Assert.Equal(Runtime.GetProperty("user.name"), authenticator.GetUserName
				());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
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
				Assert.Equal(HttpURLConnection.HttpOk, conn.GetResponseCode());
			}
			finally
			{
				auth.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
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
				Assert.Equal(HttpURLConnection.HttpUnauthorized, conn.GetResponseCode
					());
				Assert.True(conn.GetHeaderFields().Contains("WWW-Authenticate")
					);
				Assert.Equal("Authentication required", conn.GetResponseMessage
					());
			}
			finally
			{
				auth.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAuthenticationAnonymousAllowed()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase();
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				(true));
			auth._testAuthentication(new PseudoAuthenticator(), false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAuthenticationAnonymousDisallowed()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase();
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				(false));
			auth._testAuthentication(new PseudoAuthenticator(), false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAuthenticationAnonymousAllowedWithPost()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase();
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				(true));
			auth._testAuthentication(new PseudoAuthenticator(), true);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAuthenticationAnonymousDisallowedWithPost()
		{
			AuthenticatorTestCase auth = new AuthenticatorTestCase();
			AuthenticatorTestCase.SetAuthenticationHandlerConfig(GetAuthenticationHandlerConfiguration
				(false));
			auth._testAuthentication(new PseudoAuthenticator(), true);
		}
	}
}
