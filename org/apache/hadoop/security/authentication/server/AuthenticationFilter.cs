using Sharpen;

namespace org.apache.hadoop.security.authentication.server
{
	/// <summary>
	/// <p>The
	/// <see cref="AuthenticationFilter"/>
	/// enables protecting web application
	/// resources with different (pluggable)
	/// authentication mechanisms and signer secret providers.
	/// </p>
	/// <p>
	/// Out of the box it provides 2 authentication mechanisms: Pseudo and Kerberos SPNEGO.
	/// </p>
	/// Additional authentication mechanisms are supported via the
	/// <see cref="AuthenticationHandler"/>
	/// interface.
	/// <p>
	/// This filter delegates to the configured authentication handler for authentication and once it obtains an
	/// <see cref="AuthenticationToken"/>
	/// from it, sets a signed HTTP cookie with the token. For client requests
	/// that provide the signed HTTP cookie, it verifies the validity of the cookie, extracts the user information
	/// and lets the request proceed to the target resource.
	/// </p>
	/// The supported configuration properties are:
	/// <ul>
	/// <li>config.prefix: indicates the prefix to be used by all other configuration properties, the default value
	/// is no prefix. See below for details on how/why this prefix is used.</li>
	/// <li>[#PREFIX#.]type: simple|kerberos|#CLASS#, 'simple' is short for the
	/// <see cref="PseudoAuthenticationHandler"/>
	/// , 'kerberos' is short for
	/// <see cref="KerberosAuthenticationHandler"/>
	/// , otherwise
	/// the full class name of the
	/// <see cref="AuthenticationHandler"/>
	/// must be specified.</li>
	/// <li>[#PREFIX#.]signature.secret: when signer.secret.provider is set to
	/// "string" or not specified, this is the value for the secret used to sign the
	/// HTTP cookie.</li>
	/// <li>[#PREFIX#.]token.validity: time -in seconds- that the generated token is
	/// valid before a new authentication is triggered, default value is
	/// <code>3600</code> seconds. This is also used for the rollover interval for
	/// the "random" and "zookeeper" SignerSecretProviders.</li>
	/// <li>[#PREFIX#.]cookie.domain: domain to use for the HTTP cookie that stores the authentication token.</li>
	/// <li>[#PREFIX#.]cookie.path: path to use for the HTTP cookie that stores the authentication token.</li>
	/// </ul>
	/// <p>
	/// The rest of the configuration properties are specific to the
	/// <see cref="AuthenticationHandler"/>
	/// implementation and the
	/// <see cref="AuthenticationFilter"/>
	/// will take all the properties that start with the prefix #PREFIX#, it will remove
	/// the prefix from it and it will pass them to the the authentication handler for initialization. Properties that do
	/// not start with the prefix will not be passed to the authentication handler initialization.
	/// </p>
	/// <p>
	/// Out of the box it provides 3 signer secret provider implementations:
	/// "string", "random", and "zookeeper"
	/// </p>
	/// Additional signer secret providers are supported via the
	/// <see cref="org.apache.hadoop.security.authentication.util.SignerSecretProvider"/>
	/// class.
	/// <p>
	/// For the HTTP cookies mentioned above, the SignerSecretProvider is used to
	/// determine the secret to use for signing the cookies. Different
	/// implementations can have different behaviors.  The "string" implementation
	/// simply uses the string set in the [#PREFIX#.]signature.secret property
	/// mentioned above.  The "random" implementation uses a randomly generated
	/// secret that rolls over at the interval specified by the
	/// [#PREFIX#.]token.validity mentioned above.  The "zookeeper" implementation
	/// is like the "random" one, except that it synchronizes the random secret
	/// and rollovers between multiple servers; it's meant for HA services.
	/// </p>
	/// The relevant configuration properties are:
	/// <ul>
	/// <li>signer.secret.provider: indicates the name of the SignerSecretProvider
	/// class to use. Possible values are: "string", "random", "zookeeper", or a
	/// classname. If not specified, the "string" implementation will be used with
	/// [#PREFIX#.]signature.secret; and if that's not specified, the "random"
	/// implementation will be used.</li>
	/// <li>[#PREFIX#.]signature.secret: When the "string" implementation is
	/// specified, this value is used as the secret.</li>
	/// <li>[#PREFIX#.]token.validity: When the "random" or "zookeeper"
	/// implementations are specified, this value is used as the rollover
	/// interval.</li>
	/// </ul>
	/// <p>
	/// The "zookeeper" implementation has additional configuration properties that
	/// must be specified; see
	/// <see cref="org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider"
	/// 	/>
	/// for details.
	/// </p>
	/// For subclasses of AuthenticationFilter that want additional control over the
	/// SignerSecretProvider, they can use the following attribute set in the
	/// ServletContext:
	/// <ul>
	/// <li>signer.secret.provider.object: A SignerSecretProvider implementation can
	/// be passed here that will be used instead of the signer.secret.provider
	/// configuration property. Note that the class should already be
	/// initialized.</li>
	/// </ul>
	/// </summary>
	public class AuthenticationFilter : javax.servlet.Filter
	{
		private static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.security.authentication.server.AuthenticationFilter)));

		/// <summary>Constant for the property that specifies the configuration prefix.</summary>
		public const string CONFIG_PREFIX = "config.prefix";

		/// <summary>Constant for the property that specifies the authentication handler to use.
		/// 	</summary>
		public const string AUTH_TYPE = "type";

		/// <summary>Constant for the property that specifies the secret to use for signing the HTTP Cookies.
		/// 	</summary>
		public const string SIGNATURE_SECRET = "signature.secret";

		public const string SIGNATURE_SECRET_FILE = SIGNATURE_SECRET + ".file";

		/// <summary>Constant for the configuration property that indicates the validity of the generated token.
		/// 	</summary>
		public const string AUTH_TOKEN_VALIDITY = "token.validity";

		/// <summary>Constant for the configuration property that indicates the domain to use in the HTTP cookie.
		/// 	</summary>
		public const string COOKIE_DOMAIN = "cookie.domain";

		/// <summary>Constant for the configuration property that indicates the path to use in the HTTP cookie.
		/// 	</summary>
		public const string COOKIE_PATH = "cookie.path";

		/// <summary>
		/// Constant for the configuration property that indicates the name of the
		/// SignerSecretProvider class to use.
		/// </summary>
		/// <remarks>
		/// Constant for the configuration property that indicates the name of the
		/// SignerSecretProvider class to use.
		/// Possible values are: "string", "random", "zookeeper", or a classname.
		/// If not specified, the "string" implementation will be used with
		/// SIGNATURE_SECRET; and if that's not specified, the "random" implementation
		/// will be used.
		/// </remarks>
		public const string SIGNER_SECRET_PROVIDER = "signer.secret.provider";

		/// <summary>
		/// Constant for the ServletContext attribute that can be used for providing a
		/// custom implementation of the SignerSecretProvider.
		/// </summary>
		/// <remarks>
		/// Constant for the ServletContext attribute that can be used for providing a
		/// custom implementation of the SignerSecretProvider. Note that the class
		/// should already be initialized. If not specified, SIGNER_SECRET_PROVIDER
		/// will be used.
		/// </remarks>
		public const string SIGNER_SECRET_PROVIDER_ATTRIBUTE = "signer.secret.provider.object";

		private java.util.Properties config;

		private org.apache.hadoop.security.authentication.util.Signer signer;

		private org.apache.hadoop.security.authentication.util.SignerSecretProvider secretProvider;

		private org.apache.hadoop.security.authentication.server.AuthenticationHandler authHandler;

		private long validity;

		private string cookieDomain;

		private string cookiePath;

		private bool isInitializedByTomcat;

		/// <summary>
		/// <p>Initializes the authentication filter and signer secret provider.</p>
		/// It instantiates and initializes the specified
		/// <see cref="AuthenticationHandler"/>
		/// .
		/// </summary>
		/// <param name="filterConfig">filter configuration.</param>
		/// <exception cref="javax.servlet.ServletException">thrown if the filter or the authentication handler could not be initialized properly.
		/// 	</exception>
		public virtual void init(javax.servlet.FilterConfig filterConfig)
		{
			string configPrefix = filterConfig.getInitParameter(CONFIG_PREFIX);
			configPrefix = (configPrefix != null) ? configPrefix + "." : string.Empty;
			config = getConfiguration(configPrefix, filterConfig);
			string authHandlerName = config.getProperty(AUTH_TYPE, null);
			string authHandlerClassName;
			if (authHandlerName == null)
			{
				throw new javax.servlet.ServletException("Authentication type must be specified: "
					 + org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler.
					TYPE + "|" + org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
					.TYPE + "|<class>");
			}
			if (authHandlerName.ToLower(java.util.Locale.ENGLISH).Equals(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				.TYPE))
			{
				authHandlerClassName = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
					)).getName();
			}
			else
			{
				if (authHandlerName.ToLower(java.util.Locale.ENGLISH).Equals(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
					.TYPE))
				{
					authHandlerClassName = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
						)).getName();
				}
				else
				{
					authHandlerClassName = authHandlerName;
				}
			}
			validity = long.Parse(config.getProperty(AUTH_TOKEN_VALIDITY, "36000")) * 1000;
			//10 hours
			initializeSecretProvider(filterConfig);
			initializeAuthHandler(authHandlerClassName, filterConfig);
			cookieDomain = config.getProperty(COOKIE_DOMAIN, null);
			cookiePath = config.getProperty(COOKIE_PATH, null);
		}

		/// <exception cref="javax.servlet.ServletException"/>
		protected internal virtual void initializeAuthHandler(string authHandlerClassName
			, javax.servlet.FilterConfig filterConfig)
		{
			try
			{
				java.lang.Class klass = java.lang.Thread.currentThread().getContextClassLoader().
					loadClass(authHandlerClassName);
				authHandler = (org.apache.hadoop.security.authentication.server.AuthenticationHandler
					)klass.newInstance();
				authHandler.init(config);
			}
			catch (java.lang.ReflectiveOperationException ex)
			{
				throw new javax.servlet.ServletException(ex);
			}
		}

		/// <exception cref="javax.servlet.ServletException"/>
		protected internal virtual void initializeSecretProvider(javax.servlet.FilterConfig
			 filterConfig)
		{
			secretProvider = (org.apache.hadoop.security.authentication.util.SignerSecretProvider
				)filterConfig.getServletContext().getAttribute(SIGNER_SECRET_PROVIDER_ATTRIBUTE);
			if (secretProvider == null)
			{
				// As tomcat cannot specify the provider object in the configuration.
				// It'll go into this path
				try
				{
					secretProvider = constructSecretProvider(filterConfig.getServletContext(), config
						, false);
					isInitializedByTomcat = true;
				}
				catch (System.Exception ex)
				{
					throw new javax.servlet.ServletException(ex);
				}
			}
			signer = new org.apache.hadoop.security.authentication.util.Signer(secretProvider
				);
		}

		/// <exception cref="System.Exception"/>
		public static org.apache.hadoop.security.authentication.util.SignerSecretProvider
			 constructSecretProvider(javax.servlet.ServletContext ctx, java.util.Properties 
			config, bool disallowFallbackToRandomSecretProvider)
		{
			string name = config.getProperty(SIGNER_SECRET_PROVIDER, "file");
			long validity = long.Parse(config.getProperty(AUTH_TOKEN_VALIDITY, "36000")) * 1000;
			if (!disallowFallbackToRandomSecretProvider && "file".Equals(name) && config.getProperty
				(SIGNATURE_SECRET_FILE) == null)
			{
				name = "random";
			}
			org.apache.hadoop.security.authentication.util.SignerSecretProvider provider;
			if ("file".Equals(name))
			{
				provider = new org.apache.hadoop.security.authentication.util.FileSignerSecretProvider
					();
				try
				{
					provider.init(config, ctx, validity);
				}
				catch (System.Exception e)
				{
					if (!disallowFallbackToRandomSecretProvider)
					{
						LOG.info("Unable to initialize FileSignerSecretProvider, " + "falling back to use random secrets."
							);
						provider = new org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider
							();
						provider.init(config, ctx, validity);
					}
					else
					{
						throw;
					}
				}
			}
			else
			{
				if ("random".Equals(name))
				{
					provider = new org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider
						();
					provider.init(config, ctx, validity);
				}
				else
				{
					if ("zookeeper".Equals(name))
					{
						provider = new org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider
							();
						provider.init(config, ctx, validity);
					}
					else
					{
						provider = (org.apache.hadoop.security.authentication.util.SignerSecretProvider)java.lang.Thread
							.currentThread().getContextClassLoader().loadClass(name).newInstance();
						provider.init(config, ctx, validity);
					}
				}
			}
			return provider;
		}

		/// <summary>
		/// Returns the configuration properties of the
		/// <see cref="AuthenticationFilter"/>
		/// without the prefix. The returned properties are the same that the
		/// <see cref="getConfiguration(string, javax.servlet.FilterConfig)"/>
		/// method returned.
		/// </summary>
		/// <returns>the configuration properties.</returns>
		protected internal virtual java.util.Properties getConfiguration()
		{
			return config;
		}

		/// <summary>Returns the authentication handler being used.</summary>
		/// <returns>the authentication handler being used.</returns>
		protected internal virtual org.apache.hadoop.security.authentication.server.AuthenticationHandler
			 getAuthenticationHandler()
		{
			return authHandler;
		}

		/// <summary>Returns if a random secret is being used.</summary>
		/// <returns>if a random secret is being used.</returns>
		protected internal virtual bool isRandomSecret()
		{
			return Sharpen.Runtime.getClassForObject(secretProvider) == Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider
				));
		}

		/// <summary>Returns if a custom implementation of a SignerSecretProvider is being used.
		/// 	</summary>
		/// <returns>if a custom implementation of a SignerSecretProvider is being used.</returns>
		protected internal virtual bool isCustomSignerSecretProvider()
		{
			java.lang.Class clazz = Sharpen.Runtime.getClassForObject(secretProvider);
			return clazz != Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.util.FileSignerSecretProvider
				)) && clazz != Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider
				)) && clazz != Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider
				));
		}

		/// <summary>Returns the validity time of the generated tokens.</summary>
		/// <returns>the validity time of the generated tokens, in seconds.</returns>
		protected internal virtual long getValidity()
		{
			return validity / 1000;
		}

		/// <summary>Returns the cookie domain to use for the HTTP cookie.</summary>
		/// <returns>the cookie domain to use for the HTTP cookie.</returns>
		protected internal virtual string getCookieDomain()
		{
			return cookieDomain;
		}

		/// <summary>Returns the cookie path to use for the HTTP cookie.</summary>
		/// <returns>the cookie path to use for the HTTP cookie.</returns>
		protected internal virtual string getCookiePath()
		{
			return cookiePath;
		}

		/// <summary>Destroys the filter.</summary>
		/// <remarks>
		/// Destroys the filter.
		/// <p>
		/// It invokes the
		/// <see cref="AuthenticationHandler.destroy()"/>
		/// method to release any resources it may hold.
		/// </remarks>
		public virtual void destroy()
		{
			if (authHandler != null)
			{
				authHandler.destroy();
				authHandler = null;
			}
			if (secretProvider != null && isInitializedByTomcat)
			{
				secretProvider.destroy();
				secretProvider = null;
			}
		}

		/// <summary>Returns the filtered configuration (only properties starting with the specified prefix).
		/// 	</summary>
		/// <remarks>
		/// Returns the filtered configuration (only properties starting with the specified prefix). The property keys
		/// are also trimmed from the prefix. The returned
		/// <see cref="java.util.Properties"/>
		/// object is used to initialized the
		/// <see cref="AuthenticationHandler"/>
		/// .
		/// <p>
		/// This method can be overriden by subclasses to obtain the configuration from other configuration source than
		/// the web.xml file.
		/// </remarks>
		/// <param name="configPrefix">configuration prefix to use for extracting configuration properties.
		/// 	</param>
		/// <param name="filterConfig">filter configuration object</param>
		/// <returns>
		/// the configuration to be used with the
		/// <see cref="AuthenticationHandler"/>
		/// instance.
		/// </returns>
		/// <exception cref="javax.servlet.ServletException">thrown if the configuration could not be created.
		/// 	</exception>
		protected internal virtual java.util.Properties getConfiguration(string configPrefix
			, javax.servlet.FilterConfig filterConfig)
		{
			java.util.Properties props = new java.util.Properties();
			java.util.Enumeration<object> names = filterConfig.getInitParameterNames();
			while (names.MoveNext())
			{
				string name = (string)names.Current;
				if (name.StartsWith(configPrefix))
				{
					string value = filterConfig.getInitParameter(name);
					props[Sharpen.Runtime.substring(name, configPrefix.Length)] = value;
				}
			}
			return props;
		}

		/// <summary>Returns the full URL of the request including the query string.</summary>
		/// <remarks>
		/// Returns the full URL of the request including the query string.
		/// <p>
		/// Used as a convenience method for logging purposes.
		/// </remarks>
		/// <param name="request">the request object.</param>
		/// <returns>the full URL of the request including the query string.</returns>
		protected internal virtual string getRequestURL(javax.servlet.http.HttpServletRequest
			 request)
		{
			System.Text.StringBuilder sb = request.getRequestURL();
			if (request.getQueryString() != null)
			{
				sb.Append("?").Append(request.getQueryString());
			}
			return sb.ToString();
		}

		/// <summary>
		/// Returns the
		/// <see cref="AuthenticationToken"/>
		/// for the request.
		/// <p>
		/// It looks at the received HTTP cookies and extracts the value of the
		/// <see cref="org.apache.hadoop.security.authentication.client.AuthenticatedURL.AUTH_COOKIE
		/// 	"/>
		/// if present. It verifies the signature and if correct it creates the
		/// <see cref="AuthenticationToken"/>
		/// and returns
		/// it.
		/// <p>
		/// If this method returns <code>null</code> the filter will invoke the configured
		/// <see cref="AuthenticationHandler"/>
		/// to perform user authentication.
		/// </summary>
		/// <param name="request">request object.</param>
		/// <returns>the Authentication token if the request is authenticated, <code>null</code> otherwise.
		/// 	</returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">thrown if the token is invalid or if it has expired.</exception>
		protected internal virtual org.apache.hadoop.security.authentication.server.AuthenticationToken
			 getToken(javax.servlet.http.HttpServletRequest request)
		{
			org.apache.hadoop.security.authentication.server.AuthenticationToken token = null;
			string tokenStr = null;
			javax.servlet.http.Cookie[] cookies = request.getCookies();
			if (cookies != null)
			{
				foreach (javax.servlet.http.Cookie cookie in cookies)
				{
					if (cookie.getName().Equals(org.apache.hadoop.security.authentication.client.AuthenticatedURL
						.AUTH_COOKIE))
					{
						tokenStr = cookie.getValue();
						try
						{
							tokenStr = signer.verifyAndExtract(tokenStr);
						}
						catch (org.apache.hadoop.security.authentication.util.SignerException ex)
						{
							throw new org.apache.hadoop.security.authentication.client.AuthenticationException
								(ex);
						}
						break;
					}
				}
			}
			if (tokenStr != null)
			{
				token = ((org.apache.hadoop.security.authentication.server.AuthenticationToken)org.apache.hadoop.security.authentication.server.AuthenticationToken
					.parse(tokenStr));
				if (!token.getType().Equals(authHandler.getType()))
				{
					throw new org.apache.hadoop.security.authentication.client.AuthenticationException
						("Invalid AuthenticationToken type");
				}
				if (token.isExpired())
				{
					throw new org.apache.hadoop.security.authentication.client.AuthenticationException
						("AuthenticationToken expired");
				}
			}
			return token;
		}

		/// <summary>
		/// If the request has a valid authentication token it allows the request to continue to the target resource,
		/// otherwise it triggers an authentication sequence using the configured
		/// <see cref="AuthenticationHandler"/>
		/// .
		/// </summary>
		/// <param name="request">the request object.</param>
		/// <param name="response">the response object.</param>
		/// <param name="filterChain">the filter chain object.</param>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
		/// <exception cref="javax.servlet.ServletException">thrown if a processing error occurred.
		/// 	</exception>
		public virtual void doFilter(javax.servlet.ServletRequest request, javax.servlet.ServletResponse
			 response, javax.servlet.FilterChain filterChain)
		{
			bool unauthorizedResponse = true;
			int errCode = javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
			org.apache.hadoop.security.authentication.client.AuthenticationException authenticationEx
				 = null;
			javax.servlet.http.HttpServletRequest httpRequest = (javax.servlet.http.HttpServletRequest
				)request;
			javax.servlet.http.HttpServletResponse httpResponse = (javax.servlet.http.HttpServletResponse
				)response;
			bool isHttps = "https".Equals(httpRequest.getScheme());
			try
			{
				bool newToken = false;
				org.apache.hadoop.security.authentication.server.AuthenticationToken token;
				try
				{
					token = getToken(httpRequest);
				}
				catch (org.apache.hadoop.security.authentication.client.AuthenticationException ex
					)
				{
					LOG.warn("AuthenticationToken ignored: " + ex.Message);
					// will be sent back in a 401 unless filter authenticates
					authenticationEx = ex;
					token = null;
				}
				if (authHandler.managementOperation(token, httpRequest, httpResponse))
				{
					if (token == null)
					{
						if (LOG.isDebugEnabled())
						{
							LOG.debug("Request [{}] triggering authentication", getRequestURL(httpRequest));
						}
						token = authHandler.authenticate(httpRequest, httpResponse);
						if (token != null && token.getExpires() != 0 && token != org.apache.hadoop.security.authentication.server.AuthenticationToken
							.ANONYMOUS)
						{
							token.setExpires(Sharpen.Runtime.currentTimeMillis() + getValidity() * 1000);
						}
						newToken = true;
					}
					if (token != null)
					{
						unauthorizedResponse = false;
						if (LOG.isDebugEnabled())
						{
							LOG.debug("Request [{}] user [{}] authenticated", getRequestURL(httpRequest), token
								.getUserName());
						}
						org.apache.hadoop.security.authentication.server.AuthenticationToken authToken = 
							token;
						httpRequest = new _HttpServletRequestWrapper_532(authToken, httpRequest);
						if (newToken && !token.isExpired() && token != org.apache.hadoop.security.authentication.server.AuthenticationToken
							.ANONYMOUS)
						{
							string signedToken = signer.sign(token.ToString());
							createAuthCookie(httpResponse, signedToken, getCookieDomain(), getCookiePath(), token
								.getExpires(), isHttps);
						}
						doFilter(filterChain, httpRequest, httpResponse);
					}
				}
				else
				{
					unauthorizedResponse = false;
				}
			}
			catch (org.apache.hadoop.security.authentication.client.AuthenticationException ex
				)
			{
				// exception from the filter itself is fatal
				errCode = javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
				authenticationEx = ex;
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Authentication exception: " + ex.Message, ex);
				}
				else
				{
					LOG.warn("Authentication exception: " + ex.Message);
				}
			}
			if (unauthorizedResponse)
			{
				if (!httpResponse.isCommitted())
				{
					createAuthCookie(httpResponse, string.Empty, getCookieDomain(), getCookiePath(), 
						0, isHttps);
					// If response code is 401. Then WWW-Authenticate Header should be
					// present.. reset to 403 if not found..
					if ((errCode == javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED) && (!httpResponse
						.containsHeader(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
						.WWW_AUTHENTICATE)))
					{
						errCode = javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
					}
					if (authenticationEx == null)
					{
						httpResponse.sendError(errCode, "Authentication required");
					}
					else
					{
						httpResponse.sendError(errCode, authenticationEx.Message);
					}
				}
			}
		}

		private sealed class _HttpServletRequestWrapper_532 : javax.servlet.http.HttpServletRequestWrapper
		{
			public _HttpServletRequestWrapper_532(org.apache.hadoop.security.authentication.server.AuthenticationToken
				 authToken, javax.servlet.http.HttpServletRequest baseArg1)
				: base(baseArg1)
			{
				this.authToken = authToken;
			}

			public override string getAuthType()
			{
				return authToken.getType();
			}

			public override string getRemoteUser()
			{
				return authToken.getUserName();
			}

			public override java.security.Principal getUserPrincipal()
			{
				return (authToken != org.apache.hadoop.security.authentication.server.AuthenticationToken
					.ANONYMOUS) ? authToken : null;
			}

			private readonly org.apache.hadoop.security.authentication.server.AuthenticationToken
				 authToken;
		}

		/// <summary>Delegates call to the servlet filter chain.</summary>
		/// <remarks>
		/// Delegates call to the servlet filter chain. Sub-classes my override this
		/// method to perform pre and post tasks.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="javax.servlet.ServletException"/>
		protected internal virtual void doFilter(javax.servlet.FilterChain filterChain, javax.servlet.http.HttpServletRequest
			 request, javax.servlet.http.HttpServletResponse response)
		{
			filterChain.doFilter(request, response);
		}

		/// <summary>Creates the Hadoop authentication HTTP cookie.</summary>
		/// <param name="token">authentication token for the cookie.</param>
		/// <param name="expires">
		/// UNIX timestamp that indicates the expire date of the
		/// cookie. It has no effect if its value &lt; 0.
		/// XXX the following code duplicate some logic in Jetty / Servlet API,
		/// because of the fact that Hadoop is stuck at servlet 2.5 and jetty 6
		/// right now.
		/// </param>
		public static void createAuthCookie(javax.servlet.http.HttpServletResponse resp, 
			string token, string domain, string path, long expires, bool isSecure)
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder(org.apache.hadoop.security.authentication.client.AuthenticatedURL
				.AUTH_COOKIE).Append("=");
			if (token != null && token.Length > 0)
			{
				sb.Append("\"").Append(token).Append("\"");
			}
			if (path != null)
			{
				sb.Append("; Path=").Append(path);
			}
			if (domain != null)
			{
				sb.Append("; Domain=").Append(domain);
			}
			if (expires >= 0)
			{
				System.DateTime date = new System.DateTime(expires);
				java.text.SimpleDateFormat df = new java.text.SimpleDateFormat("EEE, " + "dd-MMM-yyyy HH:mm:ss zzz"
					);
				df.setTimeZone(java.util.TimeZone.getTimeZone("GMT"));
				sb.Append("; Expires=").Append(df.format(date));
			}
			if (isSecure)
			{
				sb.Append("; Secure");
			}
			sb.Append("; HttpOnly");
			resp.addHeader("Set-Cookie", sb.ToString());
		}
	}
}
