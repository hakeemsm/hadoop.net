using System;
using Javax.Servlet.Http;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Server
{
	public class TestAltKerberosAuthenticationHandler : TestKerberosAuthenticationHandler
	{
		protected internal override KerberosAuthenticationHandler GetNewAuthenticationHandler
			()
		{
			// AltKerberosAuthenticationHandler is abstract; a subclass would normally
			// perform some other authentication when alternateAuthenticate() is called.
			// For the test, we'll just return an AuthenticationToken as the other
			// authentication is left up to the developer of the subclass
			return new _AltKerberosAuthenticationHandler_34();
		}

		private sealed class _AltKerberosAuthenticationHandler_34 : AltKerberosAuthenticationHandler
		{
			public _AltKerberosAuthenticationHandler_34()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
			/// 	"/>
			public override AuthenticationToken AlternateAuthenticate(HttpServletRequest request
				, HttpServletResponse response)
			{
				return new AuthenticationToken("A", "B", this.GetType());
			}
		}

		protected internal override string GetExpectedType()
		{
			return AltKerberosAuthenticationHandler.Type;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAlternateAuthenticationAsBrowser()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			// By default, a User-Agent without "java", "curl", "wget", or "perl" in it
			// is considered a browser
			Org.Mockito.Mockito.When(request.GetHeader("User-Agent")).ThenReturn("Some Browser"
				);
			AuthenticationToken token = handler.Authenticate(request, response);
			Assert.Equal("A", token.GetUserName());
			Assert.Equal("B", token.GetName());
			Assert.Equal(GetExpectedType(), token.GetType());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNonDefaultNonBrowserUserAgentAsBrowser()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			if (handler != null)
			{
				handler.Destroy();
				handler = null;
			}
			handler = GetNewAuthenticationHandler();
			Properties props = GetDefaultProperties();
			props.SetProperty("alt-kerberos.non-browser.user-agents", "foo, bar");
			try
			{
				handler.Init(props);
			}
			catch (Exception ex)
			{
				handler = null;
				throw;
			}
			// Pretend we're something that will not match with "foo" (or "bar")
			Org.Mockito.Mockito.When(request.GetHeader("User-Agent")).ThenReturn("blah");
			// Should use alt authentication
			AuthenticationToken token = handler.Authenticate(request, response);
			Assert.Equal("A", token.GetUserName());
			Assert.Equal("B", token.GetName());
			Assert.Equal(GetExpectedType(), token.GetType());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNonDefaultNonBrowserUserAgentAsNonBrowser()
		{
			if (handler != null)
			{
				handler.Destroy();
				handler = null;
			}
			handler = GetNewAuthenticationHandler();
			Properties props = GetDefaultProperties();
			props.SetProperty("alt-kerberos.non-browser.user-agents", "foo, bar");
			try
			{
				handler.Init(props);
			}
			catch (Exception ex)
			{
				handler = null;
				throw;
			}
			// Run the kerberos tests again
			TestRequestWithoutAuthorization();
			TestRequestWithInvalidAuthorization();
			TestRequestWithAuthorization();
			TestRequestWithInvalidKerberosAuthorization();
		}
	}
}
