using System;
using System.Text;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Server
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
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Util.SignerSecretProvider"/>
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
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Util.ZKSignerSecretProvider"
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
	public class AuthenticationFilter : Filter
	{
		private static Logger Log = LoggerFactory.GetLogger(typeof(AuthenticationFilter));

		/// <summary>Constant for the property that specifies the configuration prefix.</summary>
		public const string ConfigPrefix = "config.prefix";

		/// <summary>Constant for the property that specifies the authentication handler to use.
		/// 	</summary>
		public const string AuthType = "type";

		/// <summary>Constant for the property that specifies the secret to use for signing the HTTP Cookies.
		/// 	</summary>
		public const string SignatureSecret = "signature.secret";

		public const string SignatureSecretFile = SignatureSecret + ".file";

		/// <summary>Constant for the configuration property that indicates the validity of the generated token.
		/// 	</summary>
		public const string AuthTokenValidity = "token.validity";

		/// <summary>Constant for the configuration property that indicates the domain to use in the HTTP cookie.
		/// 	</summary>
		public const string CookieDomain = "cookie.domain";

		/// <summary>Constant for the configuration property that indicates the path to use in the HTTP cookie.
		/// 	</summary>
		public const string CookiePath = "cookie.path";

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
		public const string SignerSecretProvider = "signer.secret.provider";

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
		public const string SignerSecretProviderAttribute = "signer.secret.provider.object";

		private Properties config;

		private Signer signer;

		private SignerSecretProvider secretProvider;

		private AuthenticationHandler authHandler;

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
		/// <exception cref="Javax.Servlet.ServletException">thrown if the filter or the authentication handler could not be initialized properly.
		/// 	</exception>
		public virtual void Init(FilterConfig filterConfig)
		{
			string configPrefix = filterConfig.GetInitParameter(ConfigPrefix);
			configPrefix = (configPrefix != null) ? configPrefix + "." : string.Empty;
			config = GetConfiguration(configPrefix, filterConfig);
			string authHandlerName = config.GetProperty(AuthType, null);
			string authHandlerClassName;
			if (authHandlerName == null)
			{
				throw new ServletException("Authentication type must be specified: " + PseudoAuthenticationHandler
					.Type + "|" + KerberosAuthenticationHandler.Type + "|<class>");
			}
			if (authHandlerName.ToLower(Sharpen.Extensions.GetEnglishCulture()).Equals(PseudoAuthenticationHandler
				.Type))
			{
				authHandlerClassName = typeof(PseudoAuthenticationHandler).FullName;
			}
			else
			{
				if (authHandlerName.ToLower(Sharpen.Extensions.GetEnglishCulture()).Equals(KerberosAuthenticationHandler
					.Type))
				{
					authHandlerClassName = typeof(KerberosAuthenticationHandler).FullName;
				}
				else
				{
					authHandlerClassName = authHandlerName;
				}
			}
			validity = long.Parse(config.GetProperty(AuthTokenValidity, "36000")) * 1000;
			//10 hours
			InitializeSecretProvider(filterConfig);
			InitializeAuthHandler(authHandlerClassName, filterConfig);
			cookieDomain = config.GetProperty(CookieDomain, null);
			cookiePath = config.GetProperty(CookiePath, null);
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		protected internal virtual void InitializeAuthHandler(string authHandlerClassName
			, FilterConfig filterConfig)
		{
			try
			{
				Type klass = Sharpen.Thread.CurrentThread().GetContextClassLoader().LoadClass(authHandlerClassName
					);
				authHandler = (AuthenticationHandler)System.Activator.CreateInstance(klass);
				authHandler.Init(config);
			}
			catch (ReflectiveOperationException ex)
			{
				throw new ServletException(ex);
			}
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		protected internal virtual void InitializeSecretProvider(FilterConfig filterConfig
			)
		{
			secretProvider = (SignerSecretProvider)filterConfig.GetServletContext().GetAttribute
				(SignerSecretProviderAttribute);
			if (secretProvider == null)
			{
				// As tomcat cannot specify the provider object in the configuration.
				// It'll go into this path
				try
				{
					secretProvider = ConstructSecretProvider(filterConfig.GetServletContext(), config
						, false);
					isInitializedByTomcat = true;
				}
				catch (Exception ex)
				{
					throw new ServletException(ex);
				}
			}
			signer = new Signer(secretProvider);
		}

		/// <exception cref="System.Exception"/>
		public static SignerSecretProvider ConstructSecretProvider(ServletContext ctx, Properties
			 config, bool disallowFallbackToRandomSecretProvider)
		{
			string name = config.GetProperty(SignerSecretProvider, "file");
			long validity = long.Parse(config.GetProperty(AuthTokenValidity, "36000")) * 1000;
			if (!disallowFallbackToRandomSecretProvider && "file".Equals(name) && config.GetProperty
				(SignatureSecretFile) == null)
			{
				name = "random";
			}
			SignerSecretProvider provider;
			if ("file".Equals(name))
			{
				provider = new FileSignerSecretProvider();
				try
				{
					provider.Init(config, ctx, validity);
				}
				catch (Exception e)
				{
					if (!disallowFallbackToRandomSecretProvider)
					{
						Log.Info("Unable to initialize FileSignerSecretProvider, " + "falling back to use random secrets."
							);
						provider = new RandomSignerSecretProvider();
						provider.Init(config, ctx, validity);
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
					provider = new RandomSignerSecretProvider();
					provider.Init(config, ctx, validity);
				}
				else
				{
					if ("zookeeper".Equals(name))
					{
						provider = new ZKSignerSecretProvider();
						provider.Init(config, ctx, validity);
					}
					else
					{
						provider = (SignerSecretProvider)System.Activator.CreateInstance(Sharpen.Thread.CurrentThread
							().GetContextClassLoader().LoadClass(name));
						provider.Init(config, ctx, validity);
					}
				}
			}
			return provider;
		}

		/// <summary>
		/// Returns the configuration properties of the
		/// <see cref="AuthenticationFilter"/>
		/// without the prefix. The returned properties are the same that the
		/// <see cref="GetConfiguration(string, Javax.Servlet.FilterConfig)"/>
		/// method returned.
		/// </summary>
		/// <returns>the configuration properties.</returns>
		protected internal virtual Properties GetConfiguration()
		{
			return config;
		}

		/// <summary>Returns the authentication handler being used.</summary>
		/// <returns>the authentication handler being used.</returns>
		protected internal virtual AuthenticationHandler GetAuthenticationHandler()
		{
			return authHandler;
		}

		/// <summary>Returns if a random secret is being used.</summary>
		/// <returns>if a random secret is being used.</returns>
		protected internal virtual bool IsRandomSecret()
		{
			return secretProvider.GetType() == typeof(RandomSignerSecretProvider);
		}

		/// <summary>Returns if a custom implementation of a SignerSecretProvider is being used.
		/// 	</summary>
		/// <returns>if a custom implementation of a SignerSecretProvider is being used.</returns>
		protected internal virtual bool IsCustomSignerSecretProvider()
		{
			Type clazz = secretProvider.GetType();
			return clazz != typeof(FileSignerSecretProvider) && clazz != typeof(RandomSignerSecretProvider
				) && clazz != typeof(ZKSignerSecretProvider);
		}

		/// <summary>Returns the validity time of the generated tokens.</summary>
		/// <returns>the validity time of the generated tokens, in seconds.</returns>
		protected internal virtual long GetValidity()
		{
			return validity / 1000;
		}

		/// <summary>Returns the cookie domain to use for the HTTP cookie.</summary>
		/// <returns>the cookie domain to use for the HTTP cookie.</returns>
		protected internal virtual string GetCookieDomain()
		{
			return cookieDomain;
		}

		/// <summary>Returns the cookie path to use for the HTTP cookie.</summary>
		/// <returns>the cookie path to use for the HTTP cookie.</returns>
		protected internal virtual string GetCookiePath()
		{
			return cookiePath;
		}

		/// <summary>Destroys the filter.</summary>
		/// <remarks>
		/// Destroys the filter.
		/// <p>
		/// It invokes the
		/// <see cref="AuthenticationHandler.Destroy()"/>
		/// method to release any resources it may hold.
		/// </remarks>
		public virtual void Destroy()
		{
			if (authHandler != null)
			{
				authHandler.Destroy();
				authHandler = null;
			}
			if (secretProvider != null && isInitializedByTomcat)
			{
				secretProvider.Destroy();
				secretProvider = null;
			}
		}

		/// <summary>Returns the filtered configuration (only properties starting with the specified prefix).
		/// 	</summary>
		/// <remarks>
		/// Returns the filtered configuration (only properties starting with the specified prefix). The property keys
		/// are also trimmed from the prefix. The returned
		/// <see cref="Sharpen.Properties"/>
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
		/// <exception cref="Javax.Servlet.ServletException">thrown if the configuration could not be created.
		/// 	</exception>
		protected internal virtual Properties GetConfiguration(string configPrefix, FilterConfig
			 filterConfig)
		{
			Properties props = new Properties();
			Enumeration<object> names = filterConfig.GetInitParameterNames();
			while (names.MoveNext())
			{
				string name = (string)names.Current;
				if (name.StartsWith(configPrefix))
				{
					string value = filterConfig.GetInitParameter(name);
					props[Sharpen.Runtime.Substring(name, configPrefix.Length)] = value;
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
		protected internal virtual string GetRequestURL(HttpServletRequest request)
		{
			StringBuilder sb = request.GetRequestURL();
			if (request.GetQueryString() != null)
			{
				sb.Append("?").Append(request.GetQueryString());
			}
			return sb.ToString();
		}

		/// <summary>
		/// Returns the
		/// <see cref="AuthenticationToken"/>
		/// for the request.
		/// <p>
		/// It looks at the received HTTP cookies and extracts the value of the
		/// <see cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticatedURL.AuthCookie
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">thrown if the token is invalid or if it has expired.</exception>
		protected internal virtual AuthenticationToken GetToken(HttpServletRequest request
			)
		{
			AuthenticationToken token = null;
			string tokenStr = null;
			Cookie[] cookies = request.GetCookies();
			if (cookies != null)
			{
				foreach (Cookie cookie in cookies)
				{
					if (cookie.GetName().Equals(AuthenticatedURL.AuthCookie))
					{
						tokenStr = cookie.GetValue();
						try
						{
							tokenStr = signer.VerifyAndExtract(tokenStr);
						}
						catch (SignerException ex)
						{
							throw new AuthenticationException(ex);
						}
						break;
					}
				}
			}
			if (tokenStr != null)
			{
				token = ((AuthenticationToken)AuthenticationToken.Parse(tokenStr));
				if (!token.GetType().Equals(authHandler.GetType()))
				{
					throw new AuthenticationException("Invalid AuthenticationToken type");
				}
				if (token.IsExpired())
				{
					throw new AuthenticationException("AuthenticationToken expired");
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
		/// <exception cref="Javax.Servlet.ServletException">thrown if a processing error occurred.
		/// 	</exception>
		public virtual void DoFilter(ServletRequest request, ServletResponse response, FilterChain
			 filterChain)
		{
			bool unauthorizedResponse = true;
			int errCode = HttpServletResponse.ScUnauthorized;
			AuthenticationException authenticationEx = null;
			HttpServletRequest httpRequest = (HttpServletRequest)request;
			HttpServletResponse httpResponse = (HttpServletResponse)response;
			bool isHttps = "https".Equals(httpRequest.GetScheme());
			try
			{
				bool newToken = false;
				AuthenticationToken token;
				try
				{
					token = GetToken(httpRequest);
				}
				catch (AuthenticationException ex)
				{
					Log.Warn("AuthenticationToken ignored: " + ex.Message);
					// will be sent back in a 401 unless filter authenticates
					authenticationEx = ex;
					token = null;
				}
				if (authHandler.ManagementOperation(token, httpRequest, httpResponse))
				{
					if (token == null)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Request [{}] triggering authentication", GetRequestURL(httpRequest));
						}
						token = authHandler.Authenticate(httpRequest, httpResponse);
						if (token != null && token.GetExpires() != 0 && token != AuthenticationToken.Anonymous)
						{
							token.SetExpires(Runtime.CurrentTimeMillis() + GetValidity() * 1000);
						}
						newToken = true;
					}
					if (token != null)
					{
						unauthorizedResponse = false;
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Request [{}] user [{}] authenticated", GetRequestURL(httpRequest), token
								.GetUserName());
						}
						AuthenticationToken authToken = token;
						httpRequest = new _HttpServletRequestWrapper_532(authToken, httpRequest);
						if (newToken && !token.IsExpired() && token != AuthenticationToken.Anonymous)
						{
							string signedToken = signer.Sign(token.ToString());
							CreateAuthCookie(httpResponse, signedToken, GetCookieDomain(), GetCookiePath(), token
								.GetExpires(), isHttps);
						}
						DoFilter(filterChain, httpRequest, httpResponse);
					}
				}
				else
				{
					unauthorizedResponse = false;
				}
			}
			catch (AuthenticationException ex)
			{
				// exception from the filter itself is fatal
				errCode = HttpServletResponse.ScForbidden;
				authenticationEx = ex;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Authentication exception: " + ex.Message, ex);
				}
				else
				{
					Log.Warn("Authentication exception: " + ex.Message);
				}
			}
			if (unauthorizedResponse)
			{
				if (!httpResponse.IsCommitted())
				{
					CreateAuthCookie(httpResponse, string.Empty, GetCookieDomain(), GetCookiePath(), 
						0, isHttps);
					// If response code is 401. Then WWW-Authenticate Header should be
					// present.. reset to 403 if not found..
					if ((errCode == HttpServletResponse.ScUnauthorized) && (!httpResponse.ContainsHeader
						(KerberosAuthenticator.WwwAuthenticate)))
					{
						errCode = HttpServletResponse.ScForbidden;
					}
					if (authenticationEx == null)
					{
						httpResponse.SendError(errCode, "Authentication required");
					}
					else
					{
						httpResponse.SendError(errCode, authenticationEx.Message);
					}
				}
			}
		}

		private sealed class _HttpServletRequestWrapper_532 : HttpServletRequestWrapper
		{
			public _HttpServletRequestWrapper_532(AuthenticationToken authToken, HttpServletRequest
				 baseArg1)
				: base(baseArg1)
			{
				this.authToken = authToken;
			}

			public override string GetAuthType()
			{
				return authToken.GetType();
			}

			public override string GetRemoteUser()
			{
				return authToken.GetUserName();
			}

			public override Principal GetUserPrincipal()
			{
				return (authToken != AuthenticationToken.Anonymous) ? authToken : null;
			}

			private readonly AuthenticationToken authToken;
		}

		/// <summary>Delegates call to the servlet filter chain.</summary>
		/// <remarks>
		/// Delegates call to the servlet filter chain. Sub-classes my override this
		/// method to perform pre and post tasks.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		protected internal virtual void DoFilter(FilterChain filterChain, HttpServletRequest
			 request, HttpServletResponse response)
		{
			filterChain.DoFilter(request, response);
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
		public static void CreateAuthCookie(HttpServletResponse resp, string token, string
			 domain, string path, long expires, bool isSecure)
		{
			StringBuilder sb = new StringBuilder(AuthenticatedURL.AuthCookie).Append("=");
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
				DateTime date = Sharpen.Extensions.CreateDate(expires);
				SimpleDateFormat df = new SimpleDateFormat("EEE, " + "dd-MMM-yyyy HH:mm:ss zzz");
				df.SetTimeZone(Sharpen.Extensions.GetTimeZone("GMT"));
				sb.Append("; Expires=").Append(df.Format(date));
			}
			if (isSecure)
			{
				sb.Append("; Secure");
			}
			sb.Append("; HttpOnly");
			resp.AddHeader("Set-Cookie", sb.ToString());
		}
	}
}
