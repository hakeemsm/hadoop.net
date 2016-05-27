using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Javax.Servlet;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Hamcrest;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Server
{
	public class TestAuthenticationFilter
	{
		private const long TokenValiditySec = 1000;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetConfiguration()
		{
			AuthenticationFilter filter = new AuthenticationFilter();
			FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
			Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.ConfigPrefix
				)).ThenReturn(string.Empty);
			Org.Mockito.Mockito.When(config.GetInitParameter("a")).ThenReturn("A");
			Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
				>(Arrays.AsList("a")).GetEnumerator());
			Properties props = filter.GetConfiguration(string.Empty, config);
			NUnit.Framework.Assert.AreEqual("A", props.GetProperty("a"));
			config = Org.Mockito.Mockito.Mock<FilterConfig>();
			Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.ConfigPrefix
				)).ThenReturn("foo");
			Org.Mockito.Mockito.When(config.GetInitParameter("foo.a")).ThenReturn("A");
			Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
				>(Arrays.AsList("foo.a")).GetEnumerator());
			props = filter.GetConfiguration("foo.", config);
			NUnit.Framework.Assert.AreEqual("A", props.GetProperty("a"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInitEmpty()
		{
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>().GetEnumerator());
				filter.Init(config);
				NUnit.Framework.Assert.Fail();
			}
			catch (ServletException ex)
			{
				// Expected
				NUnit.Framework.Assert.AreEqual("Authentication type must be specified: simple|kerberos|<class>"
					, ex.Message);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			finally
			{
				filter.Destroy();
			}
		}

		public class DummyAuthenticationHandler : AuthenticationHandler
		{
			public static bool init;

			public static bool managementOperationReturn;

			public static bool destroy;

			public static bool expired;

			public const string Type = "dummy";

			public static void Reset()
			{
				init = false;
				destroy = false;
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			public override void Init(Properties config)
			{
				init = true;
				managementOperationReturn = config.GetProperty("management.operation.return", "true"
					).Equals("true");
				expired = config.GetProperty("expired.token", "false").Equals("true");
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
			/// 	"/>
			public override bool ManagementOperation(AuthenticationToken token, HttpServletRequest
				 request, HttpServletResponse response)
			{
				if (!managementOperationReturn)
				{
					response.SetStatus(HttpServletResponse.ScAccepted);
				}
				return managementOperationReturn;
			}

			public override void Destroy()
			{
				destroy = true;
			}

			public override string GetType()
			{
				return Type;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
			/// 	"/>
			public override AuthenticationToken Authenticate(HttpServletRequest request, HttpServletResponse
				 response)
			{
				AuthenticationToken token = null;
				string param = request.GetParameter("authenticated");
				if (param != null && param.Equals("true"))
				{
					token = new AuthenticationToken("u", "p", "t");
					token.SetExpires((expired) ? 0 : Runtime.CurrentTimeMillis() + TokenValiditySec);
				}
				else
				{
					if (request.GetHeader("WWW-Authenticate") == null)
					{
						response.SetHeader("WWW-Authenticate", "dummyauth");
					}
					else
					{
						throw new AuthenticationException("AUTH FAILED");
					}
				}
				return token;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFallbackToRandomSecretProvider()
		{
			// minimal configuration & simple auth handler (Pseudo)
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn("simple");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthTokenValidity
					)).ThenReturn((TokenValiditySec).ToString());
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, AuthenticationFilter.AuthTokenValidity
					)).GetEnumerator());
				ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
				Org.Mockito.Mockito.When(context.GetAttribute(AuthenticationFilter.SignerSecretProviderAttribute
					)).ThenReturn(null);
				Org.Mockito.Mockito.When(config.GetServletContext()).ThenReturn(context);
				filter.Init(config);
				NUnit.Framework.Assert.AreEqual(typeof(PseudoAuthenticationHandler), filter.GetAuthenticationHandler
					().GetType());
				NUnit.Framework.Assert.IsTrue(filter.IsRandomSecret());
				NUnit.Framework.Assert.IsFalse(filter.IsCustomSignerSecretProvider());
				NUnit.Framework.Assert.IsNull(filter.GetCookieDomain());
				NUnit.Framework.Assert.IsNull(filter.GetCookiePath());
				NUnit.Framework.Assert.AreEqual(TokenValiditySec, filter.GetValidity());
			}
			finally
			{
				filter.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInit()
		{
			// custom secret as inline
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn("simple");
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType)).GetEnumerator());
				ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
				Org.Mockito.Mockito.When(context.GetAttribute(AuthenticationFilter.SignerSecretProviderAttribute
					)).ThenReturn(new _SignerSecretProvider_196());
				Org.Mockito.Mockito.When(config.GetServletContext()).ThenReturn(context);
				filter.Init(config);
				NUnit.Framework.Assert.IsFalse(filter.IsRandomSecret());
				NUnit.Framework.Assert.IsTrue(filter.IsCustomSignerSecretProvider());
			}
			finally
			{
				filter.Destroy();
			}
			// custom secret by file
			FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data", "target/test-dir"
				));
			testDir.Mkdirs();
			string secretValue = "hadoop";
			FilePath secretFile = new FilePath(testDir, "http-secret.txt");
			TextWriter writer = new FileWriter(secretFile);
			writer.Write(secretValue);
			writer.Close();
			filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn("simple");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.SignatureSecretFile
					)).ThenReturn(secretFile.GetAbsolutePath());
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, AuthenticationFilter.SignatureSecretFile
					)).GetEnumerator());
				ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
				Org.Mockito.Mockito.When(context.GetAttribute(AuthenticationFilter.SignerSecretProviderAttribute
					)).ThenReturn(null);
				Org.Mockito.Mockito.When(config.GetServletContext()).ThenReturn(context);
				filter.Init(config);
				NUnit.Framework.Assert.IsFalse(filter.IsRandomSecret());
				NUnit.Framework.Assert.IsFalse(filter.IsCustomSignerSecretProvider());
			}
			finally
			{
				filter.Destroy();
			}
			// custom cookie domain and cookie path
			filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn("simple");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.CookieDomain
					)).ThenReturn(".foo.com");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.CookiePath)
					).ThenReturn("/bar");
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, AuthenticationFilter.CookieDomain
					, AuthenticationFilter.CookiePath)).GetEnumerator());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				NUnit.Framework.Assert.AreEqual(".foo.com", filter.GetCookieDomain());
				NUnit.Framework.Assert.AreEqual("/bar", filter.GetCookiePath());
			}
			finally
			{
				filter.Destroy();
			}
			// authentication handler lifecycle, and custom impl
			TestAuthenticationFilter.DummyAuthenticationHandler.Reset();
			filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
					ThenReturn("true");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, "management.operation.return")).GetEnumerator
					());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				NUnit.Framework.Assert.IsTrue(TestAuthenticationFilter.DummyAuthenticationHandler
					.init);
			}
			finally
			{
				filter.Destroy();
				NUnit.Framework.Assert.IsTrue(TestAuthenticationFilter.DummyAuthenticationHandler
					.destroy);
			}
			// kerberos auth handler
			filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				ServletContext sc = Org.Mockito.Mockito.Mock<ServletContext>();
				Org.Mockito.Mockito.When(config.GetServletContext()).ThenReturn(sc);
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn("kerberos");
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType)).GetEnumerator());
				filter.Init(config);
			}
			catch (ServletException)
			{
			}
			finally
			{
				// Expected
				NUnit.Framework.Assert.AreEqual(typeof(KerberosAuthenticationHandler), filter.GetAuthenticationHandler
					().GetType());
				filter.Destroy();
			}
		}

		private sealed class _SignerSecretProvider_196 : SignerSecretProvider
		{
			public _SignerSecretProvider_196()
			{
			}

			public override void Init(Properties config, ServletContext servletContext, long 
				tokenValidity)
			{
			}

			public override byte[] GetCurrentSecret()
			{
				return null;
			}

			public override byte[][] GetAllSecrets()
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInitCaseSensitivity()
		{
			// minimal configuration & simple auth handler (Pseudo)
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn("SimPle");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthTokenValidity
					)).ThenReturn((TokenValiditySec).ToString());
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, AuthenticationFilter.AuthTokenValidity
					)).GetEnumerator());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				NUnit.Framework.Assert.AreEqual(typeof(PseudoAuthenticationHandler), filter.GetAuthenticationHandler
					().GetType());
			}
			finally
			{
				filter.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetRequestURL()
		{
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
					ThenReturn("true");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, "management.operation.return")).GetEnumerator
					());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				Org.Mockito.Mockito.When(request.GetRequestURL()).ThenReturn(new StringBuilder("http://foo:8080/bar"
					));
				Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn("a=A&b=B");
				NUnit.Framework.Assert.AreEqual("http://foo:8080/bar?a=A&b=B", filter.GetRequestURL
					(request));
			}
			finally
			{
				filter.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetToken()
		{
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
					ThenReturn("true");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.SignatureSecret
					)).ThenReturn("secret");
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, AuthenticationFilter.SignatureSecret
					, "management.operation.return")).GetEnumerator());
				SignerSecretProvider secretProvider = GetMockedServletContextWithStringSigner(config
					);
				filter.Init(config);
				AuthenticationToken token = new AuthenticationToken("u", "p", TestAuthenticationFilter.DummyAuthenticationHandler
					.Type);
				token.SetExpires(Runtime.CurrentTimeMillis() + TokenValiditySec);
				Signer signer = new Signer(secretProvider);
				string tokenSigned = signer.Sign(token.ToString());
				Cookie cookie = new Cookie(AuthenticatedURL.AuthCookie, tokenSigned);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				Org.Mockito.Mockito.When(request.GetCookies()).ThenReturn(new Cookie[] { cookie }
					);
				AuthenticationToken newToken = filter.GetToken(request);
				NUnit.Framework.Assert.AreEqual(token.ToString(), newToken.ToString());
			}
			finally
			{
				filter.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetTokenExpired()
		{
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
					ThenReturn("true");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.SignatureSecret
					)).ThenReturn("secret");
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, AuthenticationFilter.SignatureSecret
					, "management.operation.return")).GetEnumerator());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				AuthenticationToken token = new AuthenticationToken("u", "p", TestAuthenticationFilter.DummyAuthenticationHandler
					.Type);
				token.SetExpires(Runtime.CurrentTimeMillis() - TokenValiditySec);
				SignerSecretProvider secretProvider = StringSignerSecretProviderCreator.NewStringSignerSecretProvider
					();
				Properties secretProviderProps = new Properties();
				secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecret, "secret");
				secretProvider.Init(secretProviderProps, null, TokenValiditySec);
				Signer signer = new Signer(secretProvider);
				string tokenSigned = signer.Sign(token.ToString());
				Cookie cookie = new Cookie(AuthenticatedURL.AuthCookie, tokenSigned);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				Org.Mockito.Mockito.When(request.GetCookies()).ThenReturn(new Cookie[] { cookie }
					);
				bool failed = false;
				try
				{
					filter.GetToken(request);
				}
				catch (AuthenticationException ex)
				{
					NUnit.Framework.Assert.AreEqual("AuthenticationToken expired", ex.Message);
					failed = true;
				}
				finally
				{
					NUnit.Framework.Assert.IsTrue("token not expired", failed);
				}
			}
			finally
			{
				filter.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetTokenInvalidType()
		{
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
					ThenReturn("true");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.SignatureSecret
					)).ThenReturn("secret");
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, AuthenticationFilter.SignatureSecret
					, "management.operation.return")).GetEnumerator());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				AuthenticationToken token = new AuthenticationToken("u", "p", "invalidtype");
				token.SetExpires(Runtime.CurrentTimeMillis() + TokenValiditySec);
				SignerSecretProvider secretProvider = StringSignerSecretProviderCreator.NewStringSignerSecretProvider
					();
				Properties secretProviderProps = new Properties();
				secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecret, "secret");
				secretProvider.Init(secretProviderProps, null, TokenValiditySec);
				Signer signer = new Signer(secretProvider);
				string tokenSigned = signer.Sign(token.ToString());
				Cookie cookie = new Cookie(AuthenticatedURL.AuthCookie, tokenSigned);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				Org.Mockito.Mockito.When(request.GetCookies()).ThenReturn(new Cookie[] { cookie }
					);
				bool failed = false;
				try
				{
					filter.GetToken(request);
				}
				catch (AuthenticationException ex)
				{
					NUnit.Framework.Assert.AreEqual("Invalid AuthenticationToken type", ex.Message);
					failed = true;
				}
				finally
				{
					NUnit.Framework.Assert.IsTrue("token not invalid type", failed);
				}
			}
			finally
			{
				filter.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		private static SignerSecretProvider GetMockedServletContextWithStringSigner(FilterConfig
			 config)
		{
			Properties secretProviderProps = new Properties();
			secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecret, "secret");
			SignerSecretProvider secretProvider = StringSignerSecretProviderCreator.NewStringSignerSecretProvider
				();
			secretProvider.Init(secretProviderProps, null, TokenValiditySec);
			ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
			Org.Mockito.Mockito.When(context.GetAttribute(AuthenticationFilter.SignerSecretProviderAttribute
				)).ThenReturn(secretProvider);
			Org.Mockito.Mockito.When(config.GetServletContext()).ThenReturn(context);
			return secretProvider;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoFilterNotAuthenticated()
		{
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
					ThenReturn("true");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, "management.operation.return")).GetEnumerator
					());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				Org.Mockito.Mockito.When(request.GetRequestURL()).ThenReturn(new StringBuilder("http://foo:8080/bar"
					));
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				FilterChain chain = Org.Mockito.Mockito.Mock<FilterChain>();
				Org.Mockito.Mockito.DoAnswer(new _Answer_530()).When(chain).DoFilter(Org.Mockito.Mockito
					.AnyObject<ServletRequest>(), Org.Mockito.Mockito.AnyObject<ServletResponse>());
				Org.Mockito.Mockito.When(response.ContainsHeader("WWW-Authenticate")).ThenReturn(
					true);
				filter.DoFilter(request, response, chain);
				Org.Mockito.Mockito.Verify(response).SendError(HttpServletResponse.ScUnauthorized
					, "Authentication required");
			}
			finally
			{
				filter.Destroy();
			}
		}

		private sealed class _Answer_530 : Answer<object>
		{
			public _Answer_530()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				NUnit.Framework.Assert.Fail();
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		private void _testDoFilterAuthentication(bool withDomainPath, bool invalidToken, 
			bool expired)
		{
			AuthenticationFilter filter = new AuthenticationFilter();
			FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
			Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
				ThenReturn("true");
			Org.Mockito.Mockito.When(config.GetInitParameter("expired.token")).ThenReturn(bool
				.ToString(expired));
			Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
				ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
			Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthTokenValidity
				)).ThenReturn(TokenValiditySec.ToString());
			Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.SignatureSecret
				)).ThenReturn("secret");
			Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
				>(Arrays.AsList(AuthenticationFilter.AuthType, AuthenticationFilter.AuthTokenValidity
				, AuthenticationFilter.SignatureSecret, "management.operation" + ".return", "expired.token"
				)).GetEnumerator());
			GetMockedServletContextWithStringSigner(config);
			if (withDomainPath)
			{
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.CookieDomain
					)).ThenReturn(".foo.com");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.CookiePath)
					).ThenReturn("/bar");
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, AuthenticationFilter.AuthTokenValidity
					, AuthenticationFilter.SignatureSecret, AuthenticationFilter.CookieDomain, AuthenticationFilter
					.CookiePath, "management.operation.return")).GetEnumerator());
			}
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(request.GetParameter("authenticated")).ThenReturn("true"
				);
			Org.Mockito.Mockito.When(request.GetRequestURL()).ThenReturn(new StringBuilder("http://foo:8080/bar"
				));
			Org.Mockito.Mockito.When(request.GetQueryString()).ThenReturn("authenticated=true"
				);
			if (invalidToken)
			{
				Org.Mockito.Mockito.When(request.GetCookies()).ThenReturn(new Cookie[] { new Cookie
					(AuthenticatedURL.AuthCookie, "foo") });
			}
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			FilterChain chain = Org.Mockito.Mockito.Mock<FilterChain>();
			Dictionary<string, string> cookieMap = new Dictionary<string, string>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_599(cookieMap)).When(response).AddHeader
				(Org.Mockito.Mockito.Eq("Set-Cookie"), Org.Mockito.Mockito.AnyString());
			try
			{
				filter.Init(config);
				filter.DoFilter(request, response, chain);
				if (expired)
				{
					Org.Mockito.Mockito.Verify(response, Org.Mockito.Mockito.Never()).AddHeader(Org.Mockito.Mockito
						.Eq("Set-Cookie"), Org.Mockito.Mockito.AnyString());
				}
				else
				{
					string v = cookieMap[AuthenticatedURL.AuthCookie];
					NUnit.Framework.Assert.IsNotNull("cookie missing", v);
					NUnit.Framework.Assert.IsTrue(v.Contains("u=") && v.Contains("p=") && v.Contains(
						"t=") && v.Contains("e=") && v.Contains("s="));
					Org.Mockito.Mockito.Verify(chain).DoFilter(Org.Mockito.Mockito.Any<ServletRequest
						>(), Org.Mockito.Mockito.Any<ServletResponse>());
					SignerSecretProvider secretProvider = StringSignerSecretProviderCreator.NewStringSignerSecretProvider
						();
					Properties secretProviderProps = new Properties();
					secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecret, "secret");
					secretProvider.Init(secretProviderProps, null, TokenValiditySec);
					Signer signer = new Signer(secretProvider);
					string value = signer.VerifyAndExtract(v);
					AuthenticationToken token = ((AuthenticationToken)AuthenticationToken.Parse(value
						));
					Assert.AssertThat(token.GetExpires(), CoreMatchers.Not(0L));
					if (withDomainPath)
					{
						NUnit.Framework.Assert.AreEqual(".foo.com", cookieMap["Domain"]);
						NUnit.Framework.Assert.AreEqual("/bar", cookieMap["Path"]);
					}
					else
					{
						NUnit.Framework.Assert.IsFalse(cookieMap.Contains("Domain"));
						NUnit.Framework.Assert.IsFalse(cookieMap.Contains("Path"));
					}
				}
			}
			finally
			{
				filter.Destroy();
			}
		}

		private sealed class _Answer_599 : Answer<object>
		{
			public _Answer_599(Dictionary<string, string> cookieMap)
			{
				this.cookieMap = cookieMap;
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				string cookieHeader = (string)invocation.GetArguments()[1];
				TestAuthenticationFilter.ParseCookieMap(cookieHeader, cookieMap);
				return null;
			}

			private readonly Dictionary<string, string> cookieMap;
		}

		private static void ParseCookieMap(string cookieHeader, Dictionary<string, string
			> cookieMap)
		{
			IList<HttpCookie> cookies = HttpCookie.Parse(cookieHeader);
			foreach (HttpCookie cookie in cookies)
			{
				if (AuthenticatedURL.AuthCookie.Equals(cookie.GetName()))
				{
					cookieMap[cookie.GetName()] = cookie.GetValue();
					if (cookie.GetPath() != null)
					{
						cookieMap["Path"] = cookie.GetPath();
					}
					if (cookie.GetDomain() != null)
					{
						cookieMap["Domain"] = cookie.GetDomain();
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoFilterAuthentication()
		{
			_testDoFilterAuthentication(false, false, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoFilterAuthenticationImmediateExpiration()
		{
			_testDoFilterAuthentication(false, false, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoFilterAuthenticationWithInvalidToken()
		{
			_testDoFilterAuthentication(false, true, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoFilterAuthenticationWithDomainPath()
		{
			_testDoFilterAuthentication(true, false, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoFilterAuthenticated()
		{
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
					ThenReturn("true");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, "management.operation.return")).GetEnumerator
					());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				Org.Mockito.Mockito.When(request.GetRequestURL()).ThenReturn(new StringBuilder("http://foo:8080/bar"
					));
				AuthenticationToken token = new AuthenticationToken("u", "p", "t");
				token.SetExpires(Runtime.CurrentTimeMillis() + TokenValiditySec);
				SignerSecretProvider secretProvider = StringSignerSecretProviderCreator.NewStringSignerSecretProvider
					();
				Properties secretProviderProps = new Properties();
				secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecret, "secret");
				secretProvider.Init(secretProviderProps, null, TokenValiditySec);
				Signer signer = new Signer(secretProvider);
				string tokenSigned = signer.Sign(token.ToString());
				Cookie cookie = new Cookie(AuthenticatedURL.AuthCookie, tokenSigned);
				Org.Mockito.Mockito.When(request.GetCookies()).ThenReturn(new Cookie[] { cookie }
					);
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				FilterChain chain = Org.Mockito.Mockito.Mock<FilterChain>();
				Org.Mockito.Mockito.DoAnswer(new _Answer_721()).When(chain).DoFilter(Org.Mockito.Mockito
					.AnyObject<ServletRequest>(), Org.Mockito.Mockito.AnyObject<ServletResponse>());
				filter.DoFilter(request, response, chain);
			}
			finally
			{
				filter.Destroy();
			}
		}

		private sealed class _Answer_721 : Answer<object>
		{
			public _Answer_721()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				HttpServletRequest request = (HttpServletRequest)args[0];
				NUnit.Framework.Assert.AreEqual("u", request.GetRemoteUser());
				NUnit.Framework.Assert.AreEqual("p", request.GetUserPrincipal().GetName());
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoFilterAuthenticationFailure()
		{
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
					ThenReturn("true");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, "management.operation.return")).GetEnumerator
					());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				Org.Mockito.Mockito.When(request.GetRequestURL()).ThenReturn(new StringBuilder("http://foo:8080/bar"
					));
				Org.Mockito.Mockito.When(request.GetCookies()).ThenReturn(new Cookie[] {  });
				Org.Mockito.Mockito.When(request.GetHeader("WWW-Authenticate")).ThenReturn("dummyauth"
					);
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				FilterChain chain = Org.Mockito.Mockito.Mock<FilterChain>();
				Dictionary<string, string> cookieMap = new Dictionary<string, string>();
				Org.Mockito.Mockito.DoAnswer(new _Answer_766(cookieMap)).When(response).AddHeader
					(Org.Mockito.Mockito.Eq("Set-Cookie"), Org.Mockito.Mockito.AnyString());
				Org.Mockito.Mockito.DoAnswer(new _Answer_777()).When(chain).DoFilter(Org.Mockito.Mockito
					.AnyObject<ServletRequest>(), Org.Mockito.Mockito.AnyObject<ServletResponse>());
				filter.DoFilter(request, response, chain);
				Org.Mockito.Mockito.Verify(response).SendError(HttpServletResponse.ScForbidden, "AUTH FAILED"
					);
				Org.Mockito.Mockito.Verify(response, Org.Mockito.Mockito.Never()).SetHeader(Org.Mockito.Mockito
					.Eq("WWW-Authenticate"), Org.Mockito.Mockito.AnyString());
				string value = cookieMap[AuthenticatedURL.AuthCookie];
				NUnit.Framework.Assert.IsNotNull("cookie missing", value);
				NUnit.Framework.Assert.AreEqual(string.Empty, value);
			}
			finally
			{
				filter.Destroy();
			}
		}

		private sealed class _Answer_766 : Answer<object>
		{
			public _Answer_766(Dictionary<string, string> cookieMap)
			{
				this.cookieMap = cookieMap;
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				TestAuthenticationFilter.ParseCookieMap((string)args[1], cookieMap);
				return null;
			}

			private readonly Dictionary<string, string> cookieMap;
		}

		private sealed class _Answer_777 : Answer<object>
		{
			public _Answer_777()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				NUnit.Framework.Assert.Fail("shouldn't get here");
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoFilterAuthenticatedExpired()
		{
			string secret = "secret";
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
					ThenReturn("true");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.SignatureSecret
					)).ThenReturn(secret);
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, AuthenticationFilter.SignatureSecret
					, "management.operation.return")).GetEnumerator());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				Org.Mockito.Mockito.When(request.GetRequestURL()).ThenReturn(new StringBuilder("http://foo:8080/bar"
					));
				AuthenticationToken token = new AuthenticationToken("u", "p", TestAuthenticationFilter.DummyAuthenticationHandler
					.Type);
				token.SetExpires(Runtime.CurrentTimeMillis() - TokenValiditySec);
				SignerSecretProvider secretProvider = StringSignerSecretProviderCreator.NewStringSignerSecretProvider
					();
				Properties secretProviderProps = new Properties();
				secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecret, secret);
				secretProvider.Init(secretProviderProps, null, TokenValiditySec);
				Signer signer = new Signer(secretProvider);
				string tokenSigned = signer.Sign(token.ToString());
				Cookie cookie = new Cookie(AuthenticatedURL.AuthCookie, tokenSigned);
				Org.Mockito.Mockito.When(request.GetCookies()).ThenReturn(new Cookie[] { cookie }
					);
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				Org.Mockito.Mockito.When(response.ContainsHeader("WWW-Authenticate")).ThenReturn(
					true);
				FilterChain chain = Org.Mockito.Mockito.Mock<FilterChain>();
				VerifyUnauthorized(filter, request, response, chain);
			}
			finally
			{
				filter.Destroy();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		private static void VerifyUnauthorized(AuthenticationFilter filter, HttpServletRequest
			 request, HttpServletResponse response, FilterChain chain)
		{
			Dictionary<string, string> cookieMap = new Dictionary<string, string>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_854(cookieMap)).When(response).AddHeader
				(Org.Mockito.Mockito.Eq("Set-Cookie"), Org.Mockito.Mockito.AnyString());
			filter.DoFilter(request, response, chain);
			Org.Mockito.Mockito.Verify(response).SendError(Org.Mockito.Mockito.Eq(HttpServletResponse
				.ScUnauthorized), Org.Mockito.Mockito.AnyString());
			Org.Mockito.Mockito.Verify(chain, Org.Mockito.Mockito.Never()).DoFilter(Org.Mockito.Mockito
				.Any<ServletRequest>(), Org.Mockito.Mockito.Any<ServletResponse>());
			NUnit.Framework.Assert.IsTrue("cookie is missing", cookieMap.Contains(AuthenticatedURL
				.AuthCookie));
			NUnit.Framework.Assert.AreEqual(string.Empty, cookieMap[AuthenticatedURL.AuthCookie
				]);
		}

		private sealed class _Answer_854 : Answer<object>
		{
			public _Answer_854(Dictionary<string, string> cookieMap)
			{
				this.cookieMap = cookieMap;
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				string cookieHeader = (string)invocation.GetArguments()[1];
				TestAuthenticationFilter.ParseCookieMap(cookieHeader, cookieMap);
				return null;
			}

			private readonly Dictionary<string, string> cookieMap;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoFilterAuthenticatedInvalidType()
		{
			string secret = "secret";
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
					ThenReturn("true");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.SignatureSecret
					)).ThenReturn(secret);
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, AuthenticationFilter.SignatureSecret
					, "management.operation.return")).GetEnumerator());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				Org.Mockito.Mockito.When(request.GetRequestURL()).ThenReturn(new StringBuilder("http://foo:8080/bar"
					));
				AuthenticationToken token = new AuthenticationToken("u", "p", "invalidtype");
				token.SetExpires(Runtime.CurrentTimeMillis() + TokenValiditySec);
				SignerSecretProvider secretProvider = StringSignerSecretProviderCreator.NewStringSignerSecretProvider
					();
				Properties secretProviderProps = new Properties();
				secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecret, secret);
				secretProvider.Init(secretProviderProps, null, TokenValiditySec);
				Signer signer = new Signer(secretProvider);
				string tokenSigned = signer.Sign(token.ToString());
				Cookie cookie = new Cookie(AuthenticatedURL.AuthCookie, tokenSigned);
				Org.Mockito.Mockito.When(request.GetCookies()).ThenReturn(new Cookie[] { cookie }
					);
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				Org.Mockito.Mockito.When(response.ContainsHeader("WWW-Authenticate")).ThenReturn(
					true);
				FilterChain chain = Org.Mockito.Mockito.Mock<FilterChain>();
				VerifyUnauthorized(filter, request, response, chain);
			}
			finally
			{
				filter.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestManagementOperation()
		{
			AuthenticationFilter filter = new AuthenticationFilter();
			try
			{
				FilterConfig config = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(config.GetInitParameter("management.operation.return")).
					ThenReturn("false");
				Org.Mockito.Mockito.When(config.GetInitParameter(AuthenticationFilter.AuthType)).
					ThenReturn(typeof(TestAuthenticationFilter.DummyAuthenticationHandler).FullName);
				Org.Mockito.Mockito.When(config.GetInitParameterNames()).ThenReturn(new Vector<string
					>(Arrays.AsList(AuthenticationFilter.AuthType, "management.operation.return")).GetEnumerator
					());
				GetMockedServletContextWithStringSigner(config);
				filter.Init(config);
				HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				Org.Mockito.Mockito.When(request.GetRequestURL()).ThenReturn(new StringBuilder("http://foo:8080/bar"
					));
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				FilterChain chain = Org.Mockito.Mockito.Mock<FilterChain>();
				filter.DoFilter(request, response, chain);
				Org.Mockito.Mockito.Verify(response).SetStatus(HttpServletResponse.ScAccepted);
				Org.Mockito.Mockito.VerifyNoMoreInteractions(response);
				Org.Mockito.Mockito.Reset(request);
				Org.Mockito.Mockito.Reset(response);
				AuthenticationToken token = new AuthenticationToken("u", "p", "t");
				token.SetExpires(Runtime.CurrentTimeMillis() + TokenValiditySec);
				SignerSecretProvider secretProvider = StringSignerSecretProviderCreator.NewStringSignerSecretProvider
					();
				Properties secretProviderProps = new Properties();
				secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecret, "secret");
				secretProvider.Init(secretProviderProps, null, TokenValiditySec);
				Signer signer = new Signer(secretProvider);
				string tokenSigned = signer.Sign(token.ToString());
				Cookie cookie = new Cookie(AuthenticatedURL.AuthCookie, tokenSigned);
				Org.Mockito.Mockito.When(request.GetCookies()).ThenReturn(new Cookie[] { cookie }
					);
				filter.DoFilter(request, response, chain);
				Org.Mockito.Mockito.Verify(response).SetStatus(HttpServletResponse.ScAccepted);
				Org.Mockito.Mockito.VerifyNoMoreInteractions(response);
			}
			finally
			{
				filter.Destroy();
			}
		}
	}
}
