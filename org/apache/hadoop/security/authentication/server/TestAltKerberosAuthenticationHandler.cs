using Sharpen;

namespace org.apache.hadoop.security.authentication.server
{
	public class TestAltKerberosAuthenticationHandler : org.apache.hadoop.security.authentication.server.TestKerberosAuthenticationHandler
	{
		protected internal override org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
			 getNewAuthenticationHandler()
		{
			// AltKerberosAuthenticationHandler is abstract; a subclass would normally
			// perform some other authentication when alternateAuthenticate() is called.
			// For the test, we'll just return an AuthenticationToken as the other
			// authentication is left up to the developer of the subclass
			return new _AltKerberosAuthenticationHandler_34();
		}

		private sealed class _AltKerberosAuthenticationHandler_34 : org.apache.hadoop.security.authentication.server.AltKerberosAuthenticationHandler
		{
			public _AltKerberosAuthenticationHandler_34()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
			/// 	"/>
			public override org.apache.hadoop.security.authentication.server.AuthenticationToken
				 alternateAuthenticate(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
				 response)
			{
				return new org.apache.hadoop.security.authentication.server.AuthenticationToken("A"
					, "B", this.getType());
			}
		}

		protected internal override string getExpectedType()
		{
			return org.apache.hadoop.security.authentication.server.AltKerberosAuthenticationHandler
				.TYPE;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAlternateAuthenticationAsBrowser()
		{
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
				>();
			// By default, a User-Agent without "java", "curl", "wget", or "perl" in it
			// is considered a browser
			org.mockito.Mockito.when(request.getHeader("User-Agent")).thenReturn("Some Browser"
				);
			org.apache.hadoop.security.authentication.server.AuthenticationToken token = handler
				.authenticate(request, response);
			NUnit.Framework.Assert.AreEqual("A", token.getUserName());
			NUnit.Framework.Assert.AreEqual("B", token.getName());
			NUnit.Framework.Assert.AreEqual(getExpectedType(), token.getType());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testNonDefaultNonBrowserUserAgentAsBrowser()
		{
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
				>();
			if (handler != null)
			{
				handler.destroy();
				handler = null;
			}
			handler = getNewAuthenticationHandler();
			java.util.Properties props = getDefaultProperties();
			props.setProperty("alt-kerberos.non-browser.user-agents", "foo, bar");
			try
			{
				handler.init(props);
			}
			catch (System.Exception ex)
			{
				handler = null;
				throw;
			}
			// Pretend we're something that will not match with "foo" (or "bar")
			org.mockito.Mockito.when(request.getHeader("User-Agent")).thenReturn("blah");
			// Should use alt authentication
			org.apache.hadoop.security.authentication.server.AuthenticationToken token = handler
				.authenticate(request, response);
			NUnit.Framework.Assert.AreEqual("A", token.getUserName());
			NUnit.Framework.Assert.AreEqual("B", token.getName());
			NUnit.Framework.Assert.AreEqual(getExpectedType(), token.getType());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testNonDefaultNonBrowserUserAgentAsNonBrowser()
		{
			if (handler != null)
			{
				handler.destroy();
				handler = null;
			}
			handler = getNewAuthenticationHandler();
			java.util.Properties props = getDefaultProperties();
			props.setProperty("alt-kerberos.non-browser.user-agents", "foo, bar");
			try
			{
				handler.init(props);
			}
			catch (System.Exception ex)
			{
				handler = null;
				throw;
			}
			// Run the kerberos tests again
			testRequestWithoutAuthorization();
			testRequestWithInvalidAuthorization();
			testRequestWithAuthorization();
			testRequestWithInvalidKerberosAuthorization();
		}
	}
}
