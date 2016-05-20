using Sharpen;

namespace org.apache.hadoop.security.authentication.client
{
	public class TestAuthenticatedURL
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testToken()
		{
			org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token token = new 
				org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token();
			NUnit.Framework.Assert.IsFalse(token.isSet());
			token = new org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
				("foo");
			NUnit.Framework.Assert.IsTrue(token.isSet());
			NUnit.Framework.Assert.AreEqual("foo", token.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInjectToken()
		{
			java.net.HttpURLConnection conn = org.mockito.Mockito.mock<java.net.HttpURLConnection
				>();
			org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token token = new 
				org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token();
			token.set("foo");
			org.apache.hadoop.security.authentication.client.AuthenticatedURL.injectToken(conn
				, token);
			org.mockito.Mockito.verify(conn).addRequestProperty(org.mockito.Mockito.eq("Cookie"
				), org.mockito.Mockito.anyString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testExtractTokenOK()
		{
			java.net.HttpURLConnection conn = org.mockito.Mockito.mock<java.net.HttpURLConnection
				>();
			org.mockito.Mockito.when(conn.getResponseCode()).thenReturn(java.net.HttpURLConnection
				.HTTP_OK);
			string tokenStr = "foo";
			System.Collections.Generic.IDictionary<string, System.Collections.Generic.IList<string
				>> headers = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.IList
				<string>>();
			System.Collections.Generic.IList<string> cookies = new System.Collections.Generic.List
				<string>();
			cookies.add(org.apache.hadoop.security.authentication.client.AuthenticatedURL.AUTH_COOKIE
				 + "=" + tokenStr);
			headers["Set-Cookie"] = cookies;
			org.mockito.Mockito.when(conn.getHeaderFields()).thenReturn(headers);
			org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token token = new 
				org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token();
			org.apache.hadoop.security.authentication.client.AuthenticatedURL.extractToken(conn
				, token);
			NUnit.Framework.Assert.AreEqual(tokenStr, token.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testExtractTokenFail()
		{
			java.net.HttpURLConnection conn = org.mockito.Mockito.mock<java.net.HttpURLConnection
				>();
			org.mockito.Mockito.when(conn.getResponseCode()).thenReturn(java.net.HttpURLConnection
				.HTTP_UNAUTHORIZED);
			string tokenStr = "foo";
			System.Collections.Generic.IDictionary<string, System.Collections.Generic.IList<string
				>> headers = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.IList
				<string>>();
			System.Collections.Generic.IList<string> cookies = new System.Collections.Generic.List
				<string>();
			cookies.add(org.apache.hadoop.security.authentication.client.AuthenticatedURL.AUTH_COOKIE
				 + "=" + tokenStr);
			headers["Set-Cookie"] = cookies;
			org.mockito.Mockito.when(conn.getHeaderFields()).thenReturn(headers);
			org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token token = new 
				org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token();
			token.set("bar");
			try
			{
				org.apache.hadoop.security.authentication.client.AuthenticatedURL.extractToken(conn
					, token);
				NUnit.Framework.Assert.Fail();
			}
			catch (org.apache.hadoop.security.authentication.client.AuthenticationException)
			{
				// Expected
				NUnit.Framework.Assert.IsFalse(token.isSet());
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testConnectionConfigurator()
		{
			java.net.HttpURLConnection conn = org.mockito.Mockito.mock<java.net.HttpURLConnection
				>();
			org.mockito.Mockito.when(conn.getResponseCode()).thenReturn(java.net.HttpURLConnection
				.HTTP_UNAUTHORIZED);
			org.apache.hadoop.security.authentication.client.ConnectionConfigurator connConf = 
				org.mockito.Mockito.mock<org.apache.hadoop.security.authentication.client.ConnectionConfigurator
				>();
			org.mockito.Mockito.when(connConf.configure(org.mockito.Mockito.any<java.net.HttpURLConnection
				>())).thenReturn(conn);
			org.apache.hadoop.security.authentication.client.Authenticator authenticator = org.mockito.Mockito
				.mock<org.apache.hadoop.security.authentication.client.Authenticator>();
			org.apache.hadoop.security.authentication.client.AuthenticatedURL aURL = new org.apache.hadoop.security.authentication.client.AuthenticatedURL
				(authenticator, connConf);
			aURL.openConnection(new java.net.URL("http://foo"), new org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
				());
			org.mockito.Mockito.verify(connConf).configure(org.mockito.Mockito.any<java.net.HttpURLConnection
				>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetAuthenticator()
		{
			org.apache.hadoop.security.authentication.client.Authenticator authenticator = org.mockito.Mockito
				.mock<org.apache.hadoop.security.authentication.client.Authenticator>();
			org.apache.hadoop.security.authentication.client.AuthenticatedURL aURL = new org.apache.hadoop.security.authentication.client.AuthenticatedURL
				(authenticator);
			NUnit.Framework.Assert.AreEqual(authenticator, aURL.getAuthenticator());
		}
	}
}
