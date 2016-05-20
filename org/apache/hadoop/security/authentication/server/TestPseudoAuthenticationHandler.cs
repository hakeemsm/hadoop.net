using Sharpen;

namespace org.apache.hadoop.security.authentication.server
{
	public class TestPseudoAuthenticationHandler
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInit()
		{
			org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler handler
				 = new org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				();
			try
			{
				java.util.Properties props = new java.util.Properties();
				props.setProperty(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
					.ANONYMOUS_ALLOWED, "false");
				handler.init(props);
				NUnit.Framework.Assert.AreEqual(false, handler.getAcceptAnonymous());
			}
			finally
			{
				handler.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testType()
		{
			org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler handler
				 = new org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				();
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				.TYPE, handler.getType());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAnonymousOn()
		{
			org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler handler
				 = new org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				();
			try
			{
				java.util.Properties props = new java.util.Properties();
				props.setProperty(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
					.ANONYMOUS_ALLOWED, "true");
				handler.init(props);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
					>();
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = handler
					.authenticate(request, response);
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.security.authentication.server.AuthenticationToken
					.ANONYMOUS, token);
			}
			finally
			{
				handler.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAnonymousOff()
		{
			org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler handler
				 = new org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				();
			try
			{
				java.util.Properties props = new java.util.Properties();
				props.setProperty(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
					.ANONYMOUS_ALLOWED, "false");
				handler.init(props);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
					>();
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = handler
					.authenticate(request, response);
				NUnit.Framework.Assert.IsNull(token);
			}
			finally
			{
				handler.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		private void _testUserName(bool anonymous)
		{
			org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler handler
				 = new org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				();
			try
			{
				java.util.Properties props = new java.util.Properties();
				props.setProperty(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
					.ANONYMOUS_ALLOWED, bool.toString(anonymous));
				handler.init(props);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
					>();
				org.mockito.Mockito.when(request.getQueryString()).thenReturn(org.apache.hadoop.security.authentication.client.PseudoAuthenticator
					.USER_NAME + "=" + "user");
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = handler
					.authenticate(request, response);
				NUnit.Framework.Assert.IsNotNull(token);
				NUnit.Framework.Assert.AreEqual("user", token.getUserName());
				NUnit.Framework.Assert.AreEqual("user", token.getName());
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
					.TYPE, token.getType());
			}
			finally
			{
				handler.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testUserNameAnonymousOff()
		{
			_testUserName(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testUserNameAnonymousOn()
		{
			_testUserName(true);
		}
	}
}
