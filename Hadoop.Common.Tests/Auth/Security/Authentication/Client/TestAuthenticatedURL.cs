using System;
using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Client
{
	public class TestAuthenticatedURL
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestToken()
		{
			AuthenticatedURL.Token token = new AuthenticatedURL.Token();
			NUnit.Framework.Assert.IsFalse(token.IsSet());
			token = new AuthenticatedURL.Token("foo");
			NUnit.Framework.Assert.IsTrue(token.IsSet());
			NUnit.Framework.Assert.AreEqual("foo", token.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInjectToken()
		{
			HttpURLConnection conn = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			AuthenticatedURL.Token token = new AuthenticatedURL.Token();
			token.Set("foo");
			AuthenticatedURL.InjectToken(conn, token);
			Org.Mockito.Mockito.Verify(conn).AddRequestProperty(Org.Mockito.Mockito.Eq("Cookie"
				), Org.Mockito.Mockito.AnyString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExtractTokenOK()
		{
			HttpURLConnection conn = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			Org.Mockito.Mockito.When(conn.GetResponseCode()).ThenReturn(HttpURLConnection.HttpOk
				);
			string tokenStr = "foo";
			IDictionary<string, IList<string>> headers = new Dictionary<string, IList<string>
				>();
			IList<string> cookies = new AList<string>();
			cookies.AddItem(AuthenticatedURL.AuthCookie + "=" + tokenStr);
			headers["Set-Cookie"] = cookies;
			Org.Mockito.Mockito.When(conn.GetHeaderFields()).ThenReturn(headers);
			AuthenticatedURL.Token token = new AuthenticatedURL.Token();
			AuthenticatedURL.ExtractToken(conn, token);
			NUnit.Framework.Assert.AreEqual(tokenStr, token.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExtractTokenFail()
		{
			HttpURLConnection conn = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			Org.Mockito.Mockito.When(conn.GetResponseCode()).ThenReturn(HttpURLConnection.HttpUnauthorized
				);
			string tokenStr = "foo";
			IDictionary<string, IList<string>> headers = new Dictionary<string, IList<string>
				>();
			IList<string> cookies = new AList<string>();
			cookies.AddItem(AuthenticatedURL.AuthCookie + "=" + tokenStr);
			headers["Set-Cookie"] = cookies;
			Org.Mockito.Mockito.When(conn.GetHeaderFields()).ThenReturn(headers);
			AuthenticatedURL.Token token = new AuthenticatedURL.Token();
			token.Set("bar");
			try
			{
				AuthenticatedURL.ExtractToken(conn, token);
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthenticationException)
			{
				// Expected
				NUnit.Framework.Assert.IsFalse(token.IsSet());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConnectionConfigurator()
		{
			HttpURLConnection conn = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			Org.Mockito.Mockito.When(conn.GetResponseCode()).ThenReturn(HttpURLConnection.HttpUnauthorized
				);
			ConnectionConfigurator connConf = Org.Mockito.Mockito.Mock<ConnectionConfigurator
				>();
			Org.Mockito.Mockito.When(connConf.Configure(Org.Mockito.Mockito.Any<HttpURLConnection
				>())).ThenReturn(conn);
			Authenticator authenticator = Org.Mockito.Mockito.Mock<Authenticator>();
			AuthenticatedURL aURL = new AuthenticatedURL(authenticator, connConf);
			aURL.OpenConnection(new Uri("http://foo"), new AuthenticatedURL.Token());
			Org.Mockito.Mockito.Verify(connConf).Configure(Org.Mockito.Mockito.Any<HttpURLConnection
				>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAuthenticator()
		{
			Authenticator authenticator = Org.Mockito.Mockito.Mock<Authenticator>();
			AuthenticatedURL aURL = new AuthenticatedURL(authenticator);
			NUnit.Framework.Assert.AreEqual(authenticator, aURL.GetAuthenticator());
		}
	}
}
