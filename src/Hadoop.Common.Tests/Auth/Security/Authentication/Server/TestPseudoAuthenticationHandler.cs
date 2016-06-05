using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.Security.Authentication.Client;


namespace Org.Apache.Hadoop.Security.Authentication.Server
{
	public class TestPseudoAuthenticationHandler
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInit()
		{
			PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
			try
			{
				Properties props = new Properties();
				props.SetProperty(PseudoAuthenticationHandler.AnonymousAllowed, "false");
				handler.Init(props);
				Assert.Equal(false, handler.GetAcceptAnonymous());
			}
			finally
			{
				handler.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestType()
		{
			PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
			Assert.Equal(PseudoAuthenticationHandler.Type, handler.GetType
				());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAnonymousOn()
		{
			PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
			try
			{
				Properties props = new Properties();
				props.SetProperty(PseudoAuthenticationHandler.AnonymousAllowed, "true");
				handler.Init(props);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				AuthenticationToken token = handler.Authenticate(request, response);
				Assert.Equal(AuthenticationToken.Anonymous, token);
			}
			finally
			{
				handler.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAnonymousOff()
		{
			PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
			try
			{
				Properties props = new Properties();
				props.SetProperty(PseudoAuthenticationHandler.AnonymousAllowed, "false");
				handler.Init(props);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				AuthenticationToken token = handler.Authenticate(request, response);
				NUnit.Framework.Assert.IsNull(token);
			}
			finally
			{
				handler.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		private void _testUserName(bool anonymous)
		{
			PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
			try
			{
				Properties props = new Properties();
				props.SetProperty(PseudoAuthenticationHandler.AnonymousAllowed, bool.ToString(anonymous
					));
				handler.Init(props);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn(PseudoAuthenticator
					.UserName + "=" + "user");
				AuthenticationToken token = handler.Authenticate(request, response);
				NUnit.Framework.Assert.IsNotNull(token);
				Assert.Equal("user", token.GetUserName());
				Assert.Equal("user", token.GetName());
				Assert.Equal(PseudoAuthenticationHandler.Type, token.GetType()
					);
			}
			finally
			{
				handler.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUserNameAnonymousOff()
		{
			_testUserName(false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUserNameAnonymousOn()
		{
			_testUserName(true);
		}
	}
}
