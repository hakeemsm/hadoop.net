using System.Collections;
using System.IO;
using Javax.Servlet.Http;
using Javax.WS.RS.Core;
using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Token;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	public class TestDelegationTokenAuthenticationHandlerWithMocks
	{
		public class MockDelegationTokenAuthenticationHandler : DelegationTokenAuthenticationHandler
		{
			public MockDelegationTokenAuthenticationHandler()
				: base(new _AuthenticationHandler_52())
			{
			}

			private sealed class _AuthenticationHandler_52 : AuthenticationHandler
			{
				public _AuthenticationHandler_52()
				{
				}

				public override string GetType()
				{
					return "T";
				}

				/// <exception cref="Javax.Servlet.ServletException"/>
				public override void Init(Properties config)
				{
				}

				public override void Destroy()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
				/// 	"/>
				public override bool ManagementOperation(AuthenticationToken token, HttpServletRequest
					 request, HttpServletResponse response)
				{
					return false;
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
				/// 	"/>
				public override AuthenticationToken Authenticate(HttpServletRequest request, HttpServletResponse
					 response)
				{
					response.SetStatus(HttpServletResponse.ScUnauthorized);
					response.SetHeader(KerberosAuthenticator.WwwAuthenticate, "mock");
					return null;
				}
			}
		}

		private DelegationTokenAuthenticationHandler handler;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			Properties conf = new Properties();
			conf[KerberosDelegationTokenAuthenticationHandler.TokenKind] = "foo";
			handler = new TestDelegationTokenAuthenticationHandlerWithMocks.MockDelegationTokenAuthenticationHandler
				();
			handler.InitTokenManager(conf);
		}

		[TearDown]
		public virtual void CleanUp()
		{
			handler.Destroy();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestManagementOperations()
		{
			TestNonManagementOperation();
			TestManagementOperationErrors();
			TestGetToken(null, new Text("foo"));
			TestGetToken("bar", new Text("foo"));
			TestCancelToken();
			TestRenewToken();
		}

		/// <exception cref="System.Exception"/>
		private void TestNonManagementOperation()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(request.GetParameter(DelegationTokenAuthenticator.OpParam
				)).ThenReturn(null);
			NUnit.Framework.Assert.IsTrue(handler.ManagementOperation(null, request, null));
			Org.Mockito.Mockito.When(request.GetParameter(DelegationTokenAuthenticator.OpParam
				)).ThenReturn("CREATE");
			NUnit.Framework.Assert.IsTrue(handler.ManagementOperation(null, request, null));
		}

		/// <exception cref="System.Exception"/>
		private void TestManagementOperationErrors()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn(DelegationTokenAuthenticator
				.OpParam + "=" + DelegationTokenAuthenticator.DelegationTokenOperation.Getdelegationtoken
				.ToString());
			Org.Mockito.Mockito.When(request.GetMethod()).ThenReturn("FOO");
			NUnit.Framework.Assert.IsFalse(handler.ManagementOperation(null, request, response
				));
			Org.Mockito.Mockito.Verify(response).SendError(Org.Mockito.Mockito.Eq(HttpServletResponse
				.ScBadRequest), Org.Mockito.Mockito.StartsWith("Wrong HTTP method"));
			Org.Mockito.Mockito.Reset(response);
			Org.Mockito.Mockito.When(request.GetMethod()).ThenReturn(DelegationTokenAuthenticator.DelegationTokenOperation
				.Getdelegationtoken.GetHttpMethod());
			NUnit.Framework.Assert.IsFalse(handler.ManagementOperation(null, request, response
				));
			Org.Mockito.Mockito.Verify(response).SetStatus(Org.Mockito.Mockito.Eq(HttpServletResponse
				.ScUnauthorized));
			Org.Mockito.Mockito.Verify(response).SetHeader(Org.Mockito.Mockito.Eq(KerberosAuthenticator
				.WwwAuthenticate), Org.Mockito.Mockito.Eq("mock"));
		}

		/// <exception cref="System.Exception"/>
		private void TestGetToken(string renewer, Text expectedTokenKind)
		{
			DelegationTokenAuthenticator.DelegationTokenOperation op = DelegationTokenAuthenticator.DelegationTokenOperation
				.Getdelegationtoken;
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn(DelegationTokenAuthenticator
				.OpParam + "=" + op.ToString());
			Org.Mockito.Mockito.When(request.GetMethod()).ThenReturn(op.GetHttpMethod());
			AuthenticationToken token = Org.Mockito.Mockito.Mock<AuthenticationToken>();
			Org.Mockito.Mockito.When(token.GetUserName()).ThenReturn("user");
			Org.Mockito.Mockito.When(response.GetWriter()).ThenReturn(new PrintWriter(new StringWriter
				()));
			NUnit.Framework.Assert.IsFalse(handler.ManagementOperation(token, request, response
				));
			Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn(DelegationTokenAuthenticator
				.OpParam + "=" + op.ToString() + "&" + DelegationTokenAuthenticator.RenewerParam
				 + "=" + renewer);
			Org.Mockito.Mockito.Reset(response);
			Org.Mockito.Mockito.Reset(token);
			Org.Mockito.Mockito.When(token.GetUserName()).ThenReturn("user");
			StringWriter writer = new StringWriter();
			PrintWriter pwriter = new PrintWriter(writer);
			Org.Mockito.Mockito.When(response.GetWriter()).ThenReturn(pwriter);
			NUnit.Framework.Assert.IsFalse(handler.ManagementOperation(token, request, response
				));
			if (renewer == null)
			{
				Org.Mockito.Mockito.Verify(token).GetUserName();
			}
			else
			{
				Org.Mockito.Mockito.Verify(token).GetUserName();
			}
			Org.Mockito.Mockito.Verify(response).SetStatus(HttpServletResponse.ScOk);
			Org.Mockito.Mockito.Verify(response).SetContentType(MediaType.ApplicationJson);
			pwriter.Close();
			string responseOutput = writer.ToString();
			string tokenLabel = DelegationTokenAuthenticator.DelegationTokenJson;
			NUnit.Framework.Assert.IsTrue(responseOutput.Contains(tokenLabel));
			NUnit.Framework.Assert.IsTrue(responseOutput.Contains(DelegationTokenAuthenticator
				.DelegationTokenUrlStringJson));
			ObjectMapper jsonMapper = new ObjectMapper();
			IDictionary json = jsonMapper.ReadValue<IDictionary>(responseOutput);
			json = (IDictionary)json[tokenLabel];
			string tokenStr;
			tokenStr = (string)json[DelegationTokenAuthenticator.DelegationTokenUrlStringJson
				];
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> dt = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>();
			dt.DecodeFromUrlString(tokenStr);
			handler.GetTokenManager().VerifyToken(dt);
			NUnit.Framework.Assert.AreEqual(expectedTokenKind, dt.GetKind());
		}

		/// <exception cref="System.Exception"/>
		private void TestCancelToken()
		{
			DelegationTokenAuthenticator.DelegationTokenOperation op = DelegationTokenAuthenticator.DelegationTokenOperation
				.Canceldelegationtoken;
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn(DelegationTokenAuthenticator
				.OpParam + "=" + op.ToString());
			Org.Mockito.Mockito.When(request.GetMethod()).ThenReturn(op.GetHttpMethod());
			NUnit.Framework.Assert.IsFalse(handler.ManagementOperation(null, request, response
				));
			Org.Mockito.Mockito.Verify(response).SendError(Org.Mockito.Mockito.Eq(HttpServletResponse
				.ScBadRequest), Org.Mockito.Mockito.Contains("requires the parameter [token]"));
			Org.Mockito.Mockito.Reset(response);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = (Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>)handler.GetTokenManager().CreateToken(UserGroupInformation
				.GetCurrentUser(), "foo");
			Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn(DelegationTokenAuthenticator
				.OpParam + "=" + op.ToString() + "&" + DelegationTokenAuthenticator.TokenParam +
				 "=" + token.EncodeToUrlString());
			NUnit.Framework.Assert.IsFalse(handler.ManagementOperation(null, request, response
				));
			Org.Mockito.Mockito.Verify(response).SetStatus(HttpServletResponse.ScOk);
			try
			{
				handler.GetTokenManager().VerifyToken(token);
				NUnit.Framework.Assert.Fail();
			}
			catch (SecretManager.InvalidToken)
			{
			}
			catch
			{
				//NOP
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestRenewToken()
		{
			DelegationTokenAuthenticator.DelegationTokenOperation op = DelegationTokenAuthenticator.DelegationTokenOperation
				.Renewdelegationtoken;
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn(DelegationTokenAuthenticator
				.OpParam + "=" + op.ToString());
			Org.Mockito.Mockito.When(request.GetMethod()).ThenReturn(op.GetHttpMethod());
			NUnit.Framework.Assert.IsFalse(handler.ManagementOperation(null, request, response
				));
			Org.Mockito.Mockito.Verify(response).SetStatus(Org.Mockito.Mockito.Eq(HttpServletResponse
				.ScUnauthorized));
			Org.Mockito.Mockito.Verify(response).SetHeader(Org.Mockito.Mockito.Eq(KerberosAuthenticator
				.WwwAuthenticate), Org.Mockito.Mockito.Eq("mock"));
			Org.Mockito.Mockito.Reset(response);
			AuthenticationToken token = Org.Mockito.Mockito.Mock<AuthenticationToken>();
			Org.Mockito.Mockito.When(token.GetUserName()).ThenReturn("user");
			NUnit.Framework.Assert.IsFalse(handler.ManagementOperation(token, request, response
				));
			Org.Mockito.Mockito.Verify(response).SendError(Org.Mockito.Mockito.Eq(HttpServletResponse
				.ScBadRequest), Org.Mockito.Mockito.Contains("requires the parameter [token]"));
			Org.Mockito.Mockito.Reset(response);
			StringWriter writer = new StringWriter();
			PrintWriter pwriter = new PrintWriter(writer);
			Org.Mockito.Mockito.When(response.GetWriter()).ThenReturn(pwriter);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> dToken = (Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>)handler.GetTokenManager().CreateToken(UserGroupInformation
				.GetCurrentUser(), "user");
			Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn(DelegationTokenAuthenticator
				.OpParam + "=" + op.ToString() + "&" + DelegationTokenAuthenticator.TokenParam +
				 "=" + dToken.EncodeToUrlString());
			NUnit.Framework.Assert.IsFalse(handler.ManagementOperation(token, request, response
				));
			Org.Mockito.Mockito.Verify(response).SetStatus(HttpServletResponse.ScOk);
			pwriter.Close();
			NUnit.Framework.Assert.IsTrue(writer.ToString().Contains("long"));
			handler.GetTokenManager().VerifyToken(dToken);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuthenticate()
		{
			TestValidDelegationTokenQueryString();
			TestValidDelegationTokenHeader();
			TestInvalidDelegationTokenQueryString();
			TestInvalidDelegationTokenHeader();
		}

		/// <exception cref="System.Exception"/>
		private void TestValidDelegationTokenQueryString()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> dToken = (Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>)handler.GetTokenManager().CreateToken(UserGroupInformation
				.GetCurrentUser(), "user");
			Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn(DelegationTokenAuthenticator
				.DelegationParam + "=" + dToken.EncodeToUrlString());
			AuthenticationToken token = handler.Authenticate(request, response);
			NUnit.Framework.Assert.AreEqual(UserGroupInformation.GetCurrentUser().GetShortUserName
				(), token.GetUserName());
			NUnit.Framework.Assert.AreEqual(0, token.GetExpires());
			NUnit.Framework.Assert.AreEqual(handler.GetType(), token.GetType());
			NUnit.Framework.Assert.IsTrue(token.IsExpired());
		}

		/// <exception cref="System.Exception"/>
		private void TestValidDelegationTokenHeader()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> dToken = (Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>)handler.GetTokenManager().CreateToken(UserGroupInformation
				.GetCurrentUser(), "user");
			Org.Mockito.Mockito.When(request.GetHeader(Org.Mockito.Mockito.Eq(DelegationTokenAuthenticator
				.DelegationTokenHeader))).ThenReturn(dToken.EncodeToUrlString());
			AuthenticationToken token = handler.Authenticate(request, response);
			NUnit.Framework.Assert.AreEqual(UserGroupInformation.GetCurrentUser().GetShortUserName
				(), token.GetUserName());
			NUnit.Framework.Assert.AreEqual(0, token.GetExpires());
			NUnit.Framework.Assert.AreEqual(handler.GetType(), token.GetType());
			NUnit.Framework.Assert.IsTrue(token.IsExpired());
		}

		/// <exception cref="System.Exception"/>
		private void TestInvalidDelegationTokenQueryString()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn(DelegationTokenAuthenticator
				.DelegationParam + "=invalid");
			StringWriter writer = new StringWriter();
			Org.Mockito.Mockito.When(response.GetWriter()).ThenReturn(new PrintWriter(writer)
				);
			NUnit.Framework.Assert.IsNull(handler.Authenticate(request, response));
			Org.Mockito.Mockito.Verify(response).SetStatus(HttpServletResponse.ScForbidden);
			NUnit.Framework.Assert.IsTrue(writer.ToString().Contains("AuthenticationException"
				));
		}

		/// <exception cref="System.Exception"/>
		private void TestInvalidDelegationTokenHeader()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(request.GetHeader(Org.Mockito.Mockito.Eq(DelegationTokenAuthenticator
				.DelegationTokenHeader))).ThenReturn("invalid");
			StringWriter writer = new StringWriter();
			Org.Mockito.Mockito.When(response.GetWriter()).ThenReturn(new PrintWriter(writer)
				);
			NUnit.Framework.Assert.IsNull(handler.Authenticate(request, response));
			NUnit.Framework.Assert.IsTrue(writer.ToString().Contains("AuthenticationException"
				));
		}
	}
}
