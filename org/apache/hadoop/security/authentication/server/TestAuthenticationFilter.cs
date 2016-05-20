using Sharpen;

namespace org.apache.hadoop.security.authentication.server
{
	public class TestAuthenticationFilter
	{
		private const long TOKEN_VALIDITY_SEC = 1000;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetConfiguration()
		{
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
				>();
			org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.CONFIG_PREFIX)).thenReturn(string.Empty);
			org.mockito.Mockito.when(config.getInitParameter("a")).thenReturn("A");
			org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
				<string>(java.util.Arrays.asList("a")).GetEnumerator());
			java.util.Properties props = filter.getConfiguration(string.Empty, config);
			NUnit.Framework.Assert.AreEqual("A", props.getProperty("a"));
			config = org.mockito.Mockito.mock<javax.servlet.FilterConfig>();
			org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.CONFIG_PREFIX)).thenReturn("foo");
			org.mockito.Mockito.when(config.getInitParameter("foo.a")).thenReturn("A");
			org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
				<string>(java.util.Arrays.asList("foo.a")).GetEnumerator());
			props = filter.getConfiguration("foo.", config);
			NUnit.Framework.Assert.AreEqual("A", props.getProperty("a"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInitEmpty()
		{
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>().GetEnumerator());
				filter.init(config);
				NUnit.Framework.Assert.Fail();
			}
			catch (javax.servlet.ServletException ex)
			{
				// Expected
				NUnit.Framework.Assert.AreEqual("Authentication type must be specified: simple|kerberos|<class>"
					, ex.Message);
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			finally
			{
				filter.destroy();
			}
		}

		public class DummyAuthenticationHandler : org.apache.hadoop.security.authentication.server.AuthenticationHandler
		{
			public static bool init;

			public static bool managementOperationReturn;

			public static bool destroy;

			public static bool expired;

			public const string TYPE = "dummy";

			public static void reset()
			{
				init = false;
				destroy = false;
			}

			/// <exception cref="javax.servlet.ServletException"/>
			public override void init(java.util.Properties config)
			{
				init = true;
				managementOperationReturn = config.getProperty("management.operation.return", "true"
					).Equals("true");
				expired = config.getProperty("expired.token", "false").Equals("true");
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
			/// 	"/>
			public override bool managementOperation(org.apache.hadoop.security.authentication.server.AuthenticationToken
				 token, javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
				 response)
			{
				if (!managementOperationReturn)
				{
					response.setStatus(javax.servlet.http.HttpServletResponse.SC_ACCEPTED);
				}
				return managementOperationReturn;
			}

			public override void destroy()
			{
				destroy = true;
			}

			public override string getType()
			{
				return TYPE;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
			/// 	"/>
			public override org.apache.hadoop.security.authentication.server.AuthenticationToken
				 authenticate(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
				 response)
			{
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = null;
				string param = request.getParameter("authenticated");
				if (param != null && param.Equals("true"))
				{
					token = new org.apache.hadoop.security.authentication.server.AuthenticationToken(
						"u", "p", "t");
					token.setExpires((expired) ? 0 : Sharpen.Runtime.currentTimeMillis() + TOKEN_VALIDITY_SEC
						);
				}
				else
				{
					if (request.getHeader("WWW-Authenticate") == null)
					{
						response.setHeader("WWW-Authenticate", "dummyauth");
					}
					else
					{
						throw new org.apache.hadoop.security.authentication.client.AuthenticationException
							("AUTH FAILED");
					}
				}
				return token;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFallbackToRandomSecretProvider()
		{
			// minimal configuration & simple auth handler (Pseudo)
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn("simple");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TOKEN_VALIDITY)).thenReturn((TOKEN_VALIDITY_SEC).ToString());
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TOKEN_VALIDITY)).GetEnumerator());
				javax.servlet.ServletContext context = org.mockito.Mockito.mock<javax.servlet.ServletContext
					>();
				org.mockito.Mockito.when(context.getAttribute(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNER_SECRET_PROVIDER_ATTRIBUTE)).thenReturn(null);
				org.mockito.Mockito.when(config.getServletContext()).thenReturn(context);
				filter.init(config);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
					)), Sharpen.Runtime.getClassForObject(filter.getAuthenticationHandler()));
				NUnit.Framework.Assert.IsTrue(filter.isRandomSecret());
				NUnit.Framework.Assert.IsFalse(filter.isCustomSignerSecretProvider());
				NUnit.Framework.Assert.IsNull(filter.getCookieDomain());
				NUnit.Framework.Assert.IsNull(filter.getCookiePath());
				NUnit.Framework.Assert.AreEqual(TOKEN_VALIDITY_SEC, filter.getValidity());
			}
			finally
			{
				filter.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInit()
		{
			// custom secret as inline
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn("simple");
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).GetEnumerator());
				javax.servlet.ServletContext context = org.mockito.Mockito.mock<javax.servlet.ServletContext
					>();
				org.mockito.Mockito.when(context.getAttribute(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNER_SECRET_PROVIDER_ATTRIBUTE)).thenReturn(new _SignerSecretProvider_196());
				org.mockito.Mockito.when(config.getServletContext()).thenReturn(context);
				filter.init(config);
				NUnit.Framework.Assert.IsFalse(filter.isRandomSecret());
				NUnit.Framework.Assert.IsTrue(filter.isCustomSignerSecretProvider());
			}
			finally
			{
				filter.destroy();
			}
			// custom secret by file
			java.io.File testDir = new java.io.File(Sharpen.Runtime.getProperty("test.build.data"
				, "target/test-dir"));
			testDir.mkdirs();
			string secretValue = "hadoop";
			java.io.File secretFile = new java.io.File(testDir, "http-secret.txt");
			System.IO.TextWriter writer = new java.io.FileWriter(secretFile);
			writer.write(secretValue);
			writer.close();
			filter = new org.apache.hadoop.security.authentication.server.AuthenticationFilter
				();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn("simple");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET_FILE)).thenReturn(secretFile.getAbsolutePath());
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET_FILE)).GetEnumerator());
				javax.servlet.ServletContext context = org.mockito.Mockito.mock<javax.servlet.ServletContext
					>();
				org.mockito.Mockito.when(context.getAttribute(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNER_SECRET_PROVIDER_ATTRIBUTE)).thenReturn(null);
				org.mockito.Mockito.when(config.getServletContext()).thenReturn(context);
				filter.init(config);
				NUnit.Framework.Assert.IsFalse(filter.isRandomSecret());
				NUnit.Framework.Assert.IsFalse(filter.isCustomSignerSecretProvider());
			}
			finally
			{
				filter.destroy();
			}
			// custom cookie domain and cookie path
			filter = new org.apache.hadoop.security.authentication.server.AuthenticationFilter
				();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn("simple");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.COOKIE_DOMAIN)).thenReturn(".foo.com");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.COOKIE_PATH)).thenReturn("/bar");
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.COOKIE_DOMAIN, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.COOKIE_PATH)).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				NUnit.Framework.Assert.AreEqual(".foo.com", filter.getCookieDomain());
				NUnit.Framework.Assert.AreEqual("/bar", filter.getCookiePath());
			}
			finally
			{
				filter.destroy();
			}
			// authentication handler lifecycle, and custom impl
			org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
				.reset();
			filter = new org.apache.hadoop.security.authentication.server.AuthenticationFilter
				();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
					thenReturn("true");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					)).getName());
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, "management.operation.return")).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				NUnit.Framework.Assert.IsTrue(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					.init);
			}
			finally
			{
				filter.destroy();
				NUnit.Framework.Assert.IsTrue(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					.destroy);
			}
			// kerberos auth handler
			filter = new org.apache.hadoop.security.authentication.server.AuthenticationFilter
				();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				javax.servlet.ServletContext sc = org.mockito.Mockito.mock<javax.servlet.ServletContext
					>();
				org.mockito.Mockito.when(config.getServletContext()).thenReturn(sc);
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn("kerberos");
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).GetEnumerator());
				filter.init(config);
			}
			catch (javax.servlet.ServletException)
			{
			}
			finally
			{
				// Expected
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
					)), Sharpen.Runtime.getClassForObject(filter.getAuthenticationHandler()));
				filter.destroy();
			}
		}

		private sealed class _SignerSecretProvider_196 : org.apache.hadoop.security.authentication.util.SignerSecretProvider
		{
			public _SignerSecretProvider_196()
			{
			}

			public override void init(java.util.Properties config, javax.servlet.ServletContext
				 servletContext, long tokenValidity)
			{
			}

			public override byte[] getCurrentSecret()
			{
				return null;
			}

			public override byte[][] getAllSecrets()
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInitCaseSensitivity()
		{
			// minimal configuration & simple auth handler (Pseudo)
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn("SimPle");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TOKEN_VALIDITY)).thenReturn((TOKEN_VALIDITY_SEC).ToString());
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TOKEN_VALIDITY)).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
					)), Sharpen.Runtime.getClassForObject(filter.getAuthenticationHandler()));
			}
			finally
			{
				filter.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetRequestURL()
		{
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
					thenReturn("true");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					)).getName());
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, "management.operation.return")).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				org.mockito.Mockito.when(request.getRequestURL()).thenReturn(new System.Text.StringBuilder
					("http://foo:8080/bar"));
				org.mockito.Mockito.when(request.getQueryString()).thenReturn("a=A&b=B");
				NUnit.Framework.Assert.AreEqual("http://foo:8080/bar?a=A&b=B", filter.getRequestURL
					(request));
			}
			finally
			{
				filter.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetToken()
		{
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
					thenReturn("true");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					)).getName());
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET)).thenReturn("secret");
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, "management.operation.return")).GetEnumerator());
				org.apache.hadoop.security.authentication.util.SignerSecretProvider secretProvider
					 = getMockedServletContextWithStringSigner(config);
				filter.init(config);
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = new 
					org.apache.hadoop.security.authentication.server.AuthenticationToken("u", "p", org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					.TYPE);
				token.setExpires(Sharpen.Runtime.currentTimeMillis() + TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
					(secretProvider);
				string tokenSigned = signer.sign(token.ToString());
				javax.servlet.http.Cookie cookie = new javax.servlet.http.Cookie(org.apache.hadoop.security.authentication.client.AuthenticatedURL
					.AUTH_COOKIE, tokenSigned);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				org.mockito.Mockito.when(request.getCookies()).thenReturn(new javax.servlet.http.Cookie
					[] { cookie });
				org.apache.hadoop.security.authentication.server.AuthenticationToken newToken = filter
					.getToken(request);
				NUnit.Framework.Assert.AreEqual(token.ToString(), newToken.ToString());
			}
			finally
			{
				filter.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetTokenExpired()
		{
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
					thenReturn("true");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					)).getName());
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET)).thenReturn("secret");
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, "management.operation.return")).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = new 
					org.apache.hadoop.security.authentication.server.AuthenticationToken("u", "p", org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					.TYPE);
				token.setExpires(Sharpen.Runtime.currentTimeMillis() - TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.SignerSecretProvider secretProvider
					 = org.apache.hadoop.security.authentication.util.StringSignerSecretProviderCreator
					.newStringSignerSecretProvider();
				java.util.Properties secretProviderProps = new java.util.Properties();
				secretProviderProps.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, "secret");
				secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
					(secretProvider);
				string tokenSigned = signer.sign(token.ToString());
				javax.servlet.http.Cookie cookie = new javax.servlet.http.Cookie(org.apache.hadoop.security.authentication.client.AuthenticatedURL
					.AUTH_COOKIE, tokenSigned);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				org.mockito.Mockito.when(request.getCookies()).thenReturn(new javax.servlet.http.Cookie
					[] { cookie });
				bool failed = false;
				try
				{
					filter.getToken(request);
				}
				catch (org.apache.hadoop.security.authentication.client.AuthenticationException ex
					)
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
				filter.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetTokenInvalidType()
		{
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
					thenReturn("true");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					)).getName());
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET)).thenReturn("secret");
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, "management.operation.return")).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = new 
					org.apache.hadoop.security.authentication.server.AuthenticationToken("u", "p", "invalidtype"
					);
				token.setExpires(Sharpen.Runtime.currentTimeMillis() + TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.SignerSecretProvider secretProvider
					 = org.apache.hadoop.security.authentication.util.StringSignerSecretProviderCreator
					.newStringSignerSecretProvider();
				java.util.Properties secretProviderProps = new java.util.Properties();
				secretProviderProps.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, "secret");
				secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
					(secretProvider);
				string tokenSigned = signer.sign(token.ToString());
				javax.servlet.http.Cookie cookie = new javax.servlet.http.Cookie(org.apache.hadoop.security.authentication.client.AuthenticatedURL
					.AUTH_COOKIE, tokenSigned);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				org.mockito.Mockito.when(request.getCookies()).thenReturn(new javax.servlet.http.Cookie
					[] { cookie });
				bool failed = false;
				try
				{
					filter.getToken(request);
				}
				catch (org.apache.hadoop.security.authentication.client.AuthenticationException ex
					)
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
				filter.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		private static org.apache.hadoop.security.authentication.util.SignerSecretProvider
			 getMockedServletContextWithStringSigner(javax.servlet.FilterConfig config)
		{
			java.util.Properties secretProviderProps = new java.util.Properties();
			secretProviderProps.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.SIGNATURE_SECRET, "secret");
			org.apache.hadoop.security.authentication.util.SignerSecretProvider secretProvider
				 = org.apache.hadoop.security.authentication.util.StringSignerSecretProviderCreator
				.newStringSignerSecretProvider();
			secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
			javax.servlet.ServletContext context = org.mockito.Mockito.mock<javax.servlet.ServletContext
				>();
			org.mockito.Mockito.when(context.getAttribute(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.SIGNER_SECRET_PROVIDER_ATTRIBUTE)).thenReturn(secretProvider);
			org.mockito.Mockito.when(config.getServletContext()).thenReturn(context);
			return secretProvider;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDoFilterNotAuthenticated()
		{
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
					thenReturn("true");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					)).getName());
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, "management.operation.return")).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				org.mockito.Mockito.when(request.getRequestURL()).thenReturn(new System.Text.StringBuilder
					("http://foo:8080/bar"));
				javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
					>();
				javax.servlet.FilterChain chain = org.mockito.Mockito.mock<javax.servlet.FilterChain
					>();
				org.mockito.Mockito.doAnswer(new _Answer_530()).when(chain).doFilter(org.mockito.Mockito
					.anyObject<javax.servlet.ServletRequest>(), org.mockito.Mockito.anyObject<javax.servlet.ServletResponse
					>());
				org.mockito.Mockito.when(response.containsHeader("WWW-Authenticate")).thenReturn(
					true);
				filter.doFilter(request, response, chain);
				org.mockito.Mockito.verify(response).sendError(javax.servlet.http.HttpServletResponse
					.SC_UNAUTHORIZED, "Authentication required");
			}
			finally
			{
				filter.destroy();
			}
		}

		private sealed class _Answer_530 : org.mockito.stubbing.Answer<object>
		{
			public _Answer_530()
			{
			}

			/// <exception cref="System.Exception"/>
			public object answer(org.mockito.invocation.InvocationOnMock invocation)
			{
				NUnit.Framework.Assert.Fail();
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		private void _testDoFilterAuthentication(bool withDomainPath, bool invalidToken, 
			bool expired)
		{
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
				>();
			org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
				thenReturn("true");
			org.mockito.Mockito.when(config.getInitParameter("expired.token")).thenReturn(bool
				.toString(expired));
			org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
				)).getName());
			org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.AUTH_TOKEN_VALIDITY)).thenReturn(TOKEN_VALIDITY_SEC.ToString());
			org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.SIGNATURE_SECRET)).thenReturn("secret");
			org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
				<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.AUTH_TYPE, org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.AUTH_TOKEN_VALIDITY, org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.SIGNATURE_SECRET, "management.operation" + ".return", "expired.token")).GetEnumerator
				());
			getMockedServletContextWithStringSigner(config);
			if (withDomainPath)
			{
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.COOKIE_DOMAIN)).thenReturn(".foo.com");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.COOKIE_PATH)).thenReturn("/bar");
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TOKEN_VALIDITY, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.COOKIE_DOMAIN, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.COOKIE_PATH, "management.operation.return")).GetEnumerator());
			}
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			org.mockito.Mockito.when(request.getParameter("authenticated")).thenReturn("true"
				);
			org.mockito.Mockito.when(request.getRequestURL()).thenReturn(new System.Text.StringBuilder
				("http://foo:8080/bar"));
			org.mockito.Mockito.when(request.getQueryString()).thenReturn("authenticated=true"
				);
			if (invalidToken)
			{
				org.mockito.Mockito.when(request.getCookies()).thenReturn(new javax.servlet.http.Cookie
					[] { new javax.servlet.http.Cookie(org.apache.hadoop.security.authentication.client.AuthenticatedURL
					.AUTH_COOKIE, "foo") });
			}
			javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
				>();
			javax.servlet.FilterChain chain = org.mockito.Mockito.mock<javax.servlet.FilterChain
				>();
			System.Collections.Generic.Dictionary<string, string> cookieMap = new System.Collections.Generic.Dictionary
				<string, string>();
			org.mockito.Mockito.doAnswer(new _Answer_599(cookieMap)).when(response).addHeader
				(org.mockito.Mockito.eq("Set-Cookie"), org.mockito.Mockito.anyString());
			try
			{
				filter.init(config);
				filter.doFilter(request, response, chain);
				if (expired)
				{
					org.mockito.Mockito.verify(response, org.mockito.Mockito.never()).addHeader(org.mockito.Mockito
						.eq("Set-Cookie"), org.mockito.Mockito.anyString());
				}
				else
				{
					string v = cookieMap[org.apache.hadoop.security.authentication.client.AuthenticatedURL
						.AUTH_COOKIE];
					NUnit.Framework.Assert.IsNotNull("cookie missing", v);
					NUnit.Framework.Assert.IsTrue(v.contains("u=") && v.contains("p=") && v.contains(
						"t=") && v.contains("e=") && v.contains("s="));
					org.mockito.Mockito.verify(chain).doFilter(org.mockito.Mockito.any<javax.servlet.ServletRequest
						>(), org.mockito.Mockito.any<javax.servlet.ServletResponse>());
					org.apache.hadoop.security.authentication.util.SignerSecretProvider secretProvider
						 = org.apache.hadoop.security.authentication.util.StringSignerSecretProviderCreator
						.newStringSignerSecretProvider();
					java.util.Properties secretProviderProps = new java.util.Properties();
					secretProviderProps.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
						.SIGNATURE_SECRET, "secret");
					secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
					org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
						(secretProvider);
					string value = signer.verifyAndExtract(v);
					org.apache.hadoop.security.authentication.server.AuthenticationToken token = ((org.apache.hadoop.security.authentication.server.AuthenticationToken
						)org.apache.hadoop.security.authentication.server.AuthenticationToken.parse(value
						));
					NUnit.Framework.Assert.assertThat(token.getExpires(), org.hamcrest.CoreMatchers.not
						(0L));
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
				filter.destroy();
			}
		}

		private sealed class _Answer_599 : org.mockito.stubbing.Answer<object>
		{
			public _Answer_599(System.Collections.Generic.Dictionary<string, string> cookieMap
				)
			{
				this.cookieMap = cookieMap;
			}

			/// <exception cref="System.Exception"/>
			public object answer(org.mockito.invocation.InvocationOnMock invocation)
			{
				string cookieHeader = (string)invocation.getArguments()[1];
				org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.parseCookieMap
					(cookieHeader, cookieMap);
				return null;
			}

			private readonly System.Collections.Generic.Dictionary<string, string> cookieMap;
		}

		private static void parseCookieMap(string cookieHeader, System.Collections.Generic.Dictionary
			<string, string> cookieMap)
		{
			System.Collections.Generic.IList<java.net.HttpCookie> cookies = java.net.HttpCookie
				.parse(cookieHeader);
			foreach (java.net.HttpCookie cookie in cookies)
			{
				if (org.apache.hadoop.security.authentication.client.AuthenticatedURL.AUTH_COOKIE
					.Equals(cookie.getName()))
				{
					cookieMap[cookie.getName()] = cookie.getValue();
					if (cookie.getPath() != null)
					{
						cookieMap["Path"] = cookie.getPath();
					}
					if (cookie.getDomain() != null)
					{
						cookieMap["Domain"] = cookie.getDomain();
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDoFilterAuthentication()
		{
			_testDoFilterAuthentication(false, false, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDoFilterAuthenticationImmediateExpiration()
		{
			_testDoFilterAuthentication(false, false, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDoFilterAuthenticationWithInvalidToken()
		{
			_testDoFilterAuthentication(false, true, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDoFilterAuthenticationWithDomainPath()
		{
			_testDoFilterAuthentication(true, false, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDoFilterAuthenticated()
		{
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
					thenReturn("true");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					)).getName());
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, "management.operation.return")).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				org.mockito.Mockito.when(request.getRequestURL()).thenReturn(new System.Text.StringBuilder
					("http://foo:8080/bar"));
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = new 
					org.apache.hadoop.security.authentication.server.AuthenticationToken("u", "p", "t"
					);
				token.setExpires(Sharpen.Runtime.currentTimeMillis() + TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.SignerSecretProvider secretProvider
					 = org.apache.hadoop.security.authentication.util.StringSignerSecretProviderCreator
					.newStringSignerSecretProvider();
				java.util.Properties secretProviderProps = new java.util.Properties();
				secretProviderProps.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, "secret");
				secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
					(secretProvider);
				string tokenSigned = signer.sign(token.ToString());
				javax.servlet.http.Cookie cookie = new javax.servlet.http.Cookie(org.apache.hadoop.security.authentication.client.AuthenticatedURL
					.AUTH_COOKIE, tokenSigned);
				org.mockito.Mockito.when(request.getCookies()).thenReturn(new javax.servlet.http.Cookie
					[] { cookie });
				javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
					>();
				javax.servlet.FilterChain chain = org.mockito.Mockito.mock<javax.servlet.FilterChain
					>();
				org.mockito.Mockito.doAnswer(new _Answer_721()).when(chain).doFilter(org.mockito.Mockito
					.anyObject<javax.servlet.ServletRequest>(), org.mockito.Mockito.anyObject<javax.servlet.ServletResponse
					>());
				filter.doFilter(request, response, chain);
			}
			finally
			{
				filter.destroy();
			}
		}

		private sealed class _Answer_721 : org.mockito.stubbing.Answer<object>
		{
			public _Answer_721()
			{
			}

			/// <exception cref="System.Exception"/>
			public object answer(org.mockito.invocation.InvocationOnMock invocation)
			{
				object[] args = invocation.getArguments();
				javax.servlet.http.HttpServletRequest request = (javax.servlet.http.HttpServletRequest
					)args[0];
				NUnit.Framework.Assert.AreEqual("u", request.getRemoteUser());
				NUnit.Framework.Assert.AreEqual("p", request.getUserPrincipal().getName());
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDoFilterAuthenticationFailure()
		{
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
					thenReturn("true");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					)).getName());
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, "management.operation.return")).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				org.mockito.Mockito.when(request.getRequestURL()).thenReturn(new System.Text.StringBuilder
					("http://foo:8080/bar"));
				org.mockito.Mockito.when(request.getCookies()).thenReturn(new javax.servlet.http.Cookie
					[] {  });
				org.mockito.Mockito.when(request.getHeader("WWW-Authenticate")).thenReturn("dummyauth"
					);
				javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
					>();
				javax.servlet.FilterChain chain = org.mockito.Mockito.mock<javax.servlet.FilterChain
					>();
				System.Collections.Generic.Dictionary<string, string> cookieMap = new System.Collections.Generic.Dictionary
					<string, string>();
				org.mockito.Mockito.doAnswer(new _Answer_766(cookieMap)).when(response).addHeader
					(org.mockito.Mockito.eq("Set-Cookie"), org.mockito.Mockito.anyString());
				org.mockito.Mockito.doAnswer(new _Answer_777()).when(chain).doFilter(org.mockito.Mockito
					.anyObject<javax.servlet.ServletRequest>(), org.mockito.Mockito.anyObject<javax.servlet.ServletResponse
					>());
				filter.doFilter(request, response, chain);
				org.mockito.Mockito.verify(response).sendError(javax.servlet.http.HttpServletResponse
					.SC_FORBIDDEN, "AUTH FAILED");
				org.mockito.Mockito.verify(response, org.mockito.Mockito.never()).setHeader(org.mockito.Mockito
					.eq("WWW-Authenticate"), org.mockito.Mockito.anyString());
				string value = cookieMap[org.apache.hadoop.security.authentication.client.AuthenticatedURL
					.AUTH_COOKIE];
				NUnit.Framework.Assert.IsNotNull("cookie missing", value);
				NUnit.Framework.Assert.AreEqual(string.Empty, value);
			}
			finally
			{
				filter.destroy();
			}
		}

		private sealed class _Answer_766 : org.mockito.stubbing.Answer<object>
		{
			public _Answer_766(System.Collections.Generic.Dictionary<string, string> cookieMap
				)
			{
				this.cookieMap = cookieMap;
			}

			/// <exception cref="System.Exception"/>
			public object answer(org.mockito.invocation.InvocationOnMock invocation)
			{
				object[] args = invocation.getArguments();
				org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.parseCookieMap
					((string)args[1], cookieMap);
				return null;
			}

			private readonly System.Collections.Generic.Dictionary<string, string> cookieMap;
		}

		private sealed class _Answer_777 : org.mockito.stubbing.Answer<object>
		{
			public _Answer_777()
			{
			}

			/// <exception cref="System.Exception"/>
			public object answer(org.mockito.invocation.InvocationOnMock invocation)
			{
				NUnit.Framework.Assert.Fail("shouldn't get here");
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDoFilterAuthenticatedExpired()
		{
			string secret = "secret";
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
					thenReturn("true");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					)).getName());
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET)).thenReturn(secret);
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, "management.operation.return")).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				org.mockito.Mockito.when(request.getRequestURL()).thenReturn(new System.Text.StringBuilder
					("http://foo:8080/bar"));
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = new 
					org.apache.hadoop.security.authentication.server.AuthenticationToken("u", "p", org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					.TYPE);
				token.setExpires(Sharpen.Runtime.currentTimeMillis() - TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.SignerSecretProvider secretProvider
					 = org.apache.hadoop.security.authentication.util.StringSignerSecretProviderCreator
					.newStringSignerSecretProvider();
				java.util.Properties secretProviderProps = new java.util.Properties();
				secretProviderProps.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, secret);
				secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
					(secretProvider);
				string tokenSigned = signer.sign(token.ToString());
				javax.servlet.http.Cookie cookie = new javax.servlet.http.Cookie(org.apache.hadoop.security.authentication.client.AuthenticatedURL
					.AUTH_COOKIE, tokenSigned);
				org.mockito.Mockito.when(request.getCookies()).thenReturn(new javax.servlet.http.Cookie
					[] { cookie });
				javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
					>();
				org.mockito.Mockito.when(response.containsHeader("WWW-Authenticate")).thenReturn(
					true);
				javax.servlet.FilterChain chain = org.mockito.Mockito.mock<javax.servlet.FilterChain
					>();
				verifyUnauthorized(filter, request, response, chain);
			}
			finally
			{
				filter.destroy();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="javax.servlet.ServletException"/>
		private static void verifyUnauthorized(org.apache.hadoop.security.authentication.server.AuthenticationFilter
			 filter, javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response, javax.servlet.FilterChain chain)
		{
			System.Collections.Generic.Dictionary<string, string> cookieMap = new System.Collections.Generic.Dictionary
				<string, string>();
			org.mockito.Mockito.doAnswer(new _Answer_854(cookieMap)).when(response).addHeader
				(org.mockito.Mockito.eq("Set-Cookie"), org.mockito.Mockito.anyString());
			filter.doFilter(request, response, chain);
			org.mockito.Mockito.verify(response).sendError(org.mockito.Mockito.eq(javax.servlet.http.HttpServletResponse
				.SC_UNAUTHORIZED), org.mockito.Mockito.anyString());
			org.mockito.Mockito.verify(chain, org.mockito.Mockito.never()).doFilter(org.mockito.Mockito
				.any<javax.servlet.ServletRequest>(), org.mockito.Mockito.any<javax.servlet.ServletResponse
				>());
			NUnit.Framework.Assert.IsTrue("cookie is missing", cookieMap.Contains(org.apache.hadoop.security.authentication.client.AuthenticatedURL
				.AUTH_COOKIE));
			NUnit.Framework.Assert.AreEqual(string.Empty, cookieMap[org.apache.hadoop.security.authentication.client.AuthenticatedURL
				.AUTH_COOKIE]);
		}

		private sealed class _Answer_854 : org.mockito.stubbing.Answer<object>
		{
			public _Answer_854(System.Collections.Generic.Dictionary<string, string> cookieMap
				)
			{
				this.cookieMap = cookieMap;
			}

			/// <exception cref="System.Exception"/>
			public object answer(org.mockito.invocation.InvocationOnMock invocation)
			{
				string cookieHeader = (string)invocation.getArguments()[1];
				org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.parseCookieMap
					(cookieHeader, cookieMap);
				return null;
			}

			private readonly System.Collections.Generic.Dictionary<string, string> cookieMap;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDoFilterAuthenticatedInvalidType()
		{
			string secret = "secret";
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
					thenReturn("true");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					)).getName());
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET)).thenReturn(secret);
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, "management.operation.return")).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				org.mockito.Mockito.when(request.getRequestURL()).thenReturn(new System.Text.StringBuilder
					("http://foo:8080/bar"));
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = new 
					org.apache.hadoop.security.authentication.server.AuthenticationToken("u", "p", "invalidtype"
					);
				token.setExpires(Sharpen.Runtime.currentTimeMillis() + TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.SignerSecretProvider secretProvider
					 = org.apache.hadoop.security.authentication.util.StringSignerSecretProviderCreator
					.newStringSignerSecretProvider();
				java.util.Properties secretProviderProps = new java.util.Properties();
				secretProviderProps.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, secret);
				secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
					(secretProvider);
				string tokenSigned = signer.sign(token.ToString());
				javax.servlet.http.Cookie cookie = new javax.servlet.http.Cookie(org.apache.hadoop.security.authentication.client.AuthenticatedURL
					.AUTH_COOKIE, tokenSigned);
				org.mockito.Mockito.when(request.getCookies()).thenReturn(new javax.servlet.http.Cookie
					[] { cookie });
				javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
					>();
				org.mockito.Mockito.when(response.containsHeader("WWW-Authenticate")).thenReturn(
					true);
				javax.servlet.FilterChain chain = org.mockito.Mockito.mock<javax.servlet.FilterChain
					>();
				verifyUnauthorized(filter, request, response, chain);
			}
			finally
			{
				filter.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testManagementOperation()
		{
			org.apache.hadoop.security.authentication.server.AuthenticationFilter filter = new 
				org.apache.hadoop.security.authentication.server.AuthenticationFilter();
			try
			{
				javax.servlet.FilterConfig config = org.mockito.Mockito.mock<javax.servlet.FilterConfig
					>();
				org.mockito.Mockito.when(config.getInitParameter("management.operation.return")).
					thenReturn("false");
				org.mockito.Mockito.when(config.getInitParameter(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE)).thenReturn(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.TestAuthenticationFilter.DummyAuthenticationHandler
					)).getName());
				org.mockito.Mockito.when(config.getInitParameterNames()).thenReturn(new java.util.Vector
					<string>(java.util.Arrays.asList(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.AUTH_TYPE, "management.operation.return")).GetEnumerator());
				getMockedServletContextWithStringSigner(config);
				filter.init(config);
				javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
					>();
				org.mockito.Mockito.when(request.getRequestURL()).thenReturn(new System.Text.StringBuilder
					("http://foo:8080/bar"));
				javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
					>();
				javax.servlet.FilterChain chain = org.mockito.Mockito.mock<javax.servlet.FilterChain
					>();
				filter.doFilter(request, response, chain);
				org.mockito.Mockito.verify(response).setStatus(javax.servlet.http.HttpServletResponse
					.SC_ACCEPTED);
				org.mockito.Mockito.verifyNoMoreInteractions(response);
				org.mockito.Mockito.reset(request);
				org.mockito.Mockito.reset(response);
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = new 
					org.apache.hadoop.security.authentication.server.AuthenticationToken("u", "p", "t"
					);
				token.setExpires(Sharpen.Runtime.currentTimeMillis() + TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.SignerSecretProvider secretProvider
					 = org.apache.hadoop.security.authentication.util.StringSignerSecretProviderCreator
					.newStringSignerSecretProvider();
				java.util.Properties secretProviderProps = new java.util.Properties();
				secretProviderProps.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNATURE_SECRET, "secret");
				secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
				org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
					(secretProvider);
				string tokenSigned = signer.sign(token.ToString());
				javax.servlet.http.Cookie cookie = new javax.servlet.http.Cookie(org.apache.hadoop.security.authentication.client.AuthenticatedURL
					.AUTH_COOKIE, tokenSigned);
				org.mockito.Mockito.when(request.getCookies()).thenReturn(new javax.servlet.http.Cookie
					[] { cookie });
				filter.doFilter(request, response, chain);
				org.mockito.Mockito.verify(response).setStatus(javax.servlet.http.HttpServletResponse
					.SC_ACCEPTED);
				org.mockito.Mockito.verifyNoMoreInteractions(response);
			}
			finally
			{
				filter.destroy();
			}
		}
	}
}
