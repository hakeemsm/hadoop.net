using Sharpen;

namespace org.apache.hadoop.security.authentication.server
{
	/// <summary>
	/// The <code>PseudoAuthenticationHandler</code> provides a pseudo authentication mechanism that accepts
	/// the user name specified as a query string parameter.
	/// </summary>
	/// <remarks>
	/// The <code>PseudoAuthenticationHandler</code> provides a pseudo authentication mechanism that accepts
	/// the user name specified as a query string parameter.
	/// <p>
	/// This mimics the model of Hadoop Simple authentication which trust the 'user.name' property provided in
	/// the configuration object.
	/// <p>
	/// This handler can be configured to support anonymous users.
	/// <p>
	/// The only supported configuration property is:
	/// <ul>
	/// <li>simple.anonymous.allowed: <code>true|false</code>, default value is <code>false</code></li>
	/// </ul>
	/// </remarks>
	public class PseudoAuthenticationHandler : org.apache.hadoop.security.authentication.server.AuthenticationHandler
	{
		/// <summary>Constant that identifies the authentication mechanism.</summary>
		public const string TYPE = "simple";

		/// <summary>Constant for the configuration property that indicates if anonymous users are allowed.
		/// 	</summary>
		public const string ANONYMOUS_ALLOWED = TYPE + ".anonymous.allowed";

		private static readonly java.nio.charset.Charset UTF8_CHARSET = java.nio.charset.Charset
			.forName("UTF-8");

		private const string PSEUDO_AUTH = "PseudoAuth";

		private bool acceptAnonymous;

		private string type;

		/// <summary>
		/// Creates a Hadoop pseudo authentication handler with the default auth-token
		/// type, <code>simple</code>.
		/// </summary>
		public PseudoAuthenticationHandler()
			: this(TYPE)
		{
		}

		/// <summary>
		/// Creates a Hadoop pseudo authentication handler with a custom auth-token
		/// type.
		/// </summary>
		/// <param name="type">auth-token type.</param>
		public PseudoAuthenticationHandler(string type)
		{
			this.type = type;
		}

		/// <summary>Initializes the authentication handler instance.</summary>
		/// <remarks>
		/// Initializes the authentication handler instance.
		/// <p>
		/// This method is invoked by the
		/// <see cref="AuthenticationFilter.init(javax.servlet.FilterConfig)"/>
		/// method.
		/// </remarks>
		/// <param name="config">configuration properties to initialize the handler.</param>
		/// <exception cref="javax.servlet.ServletException">thrown if the handler could not be initialized.
		/// 	</exception>
		public override void init(java.util.Properties config)
		{
			acceptAnonymous = bool.parseBoolean(config.getProperty(ANONYMOUS_ALLOWED, "false"
				));
		}

		/// <summary>Returns if the handler is configured to support anonymous users.</summary>
		/// <returns>if the handler is configured to support anonymous users.</returns>
		protected internal virtual bool getAcceptAnonymous()
		{
			return acceptAnonymous;
		}

		/// <summary>Releases any resources initialized by the authentication handler.</summary>
		/// <remarks>
		/// Releases any resources initialized by the authentication handler.
		/// <p>
		/// This implementation does a NOP.
		/// </remarks>
		public override void destroy()
		{
		}

		/// <summary>Returns the authentication type of the authentication handler, 'simple'.
		/// 	</summary>
		/// <returns>the authentication type of the authentication handler, 'simple'.</returns>
		public override string getType()
		{
			return type;
		}

		/// <summary>This is an empty implementation, it always returns <code>TRUE</code>.</summary>
		/// <param name="token">the authentication token if any, otherwise <code>NULL</code>.
		/// 	</param>
		/// <param name="request">the HTTP client request.</param>
		/// <param name="response">the HTTP client response.</param>
		/// <returns><code>TRUE</code></returns>
		/// <exception cref="System.IO.IOException">it is never thrown.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">it is never thrown.</exception>
		public override bool managementOperation(org.apache.hadoop.security.authentication.server.AuthenticationToken
			 token, javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			return true;
		}

		private string getUserName(javax.servlet.http.HttpServletRequest request)
		{
			System.Collections.Generic.IList<org.apache.http.NameValuePair> list = org.apache.http.client.utils.URLEncodedUtils
				.parse(request.getQueryString(), UTF8_CHARSET);
			if (list != null)
			{
				foreach (org.apache.http.NameValuePair nv in list)
				{
					if (org.apache.hadoop.security.authentication.client.PseudoAuthenticator.USER_NAME
						.Equals(nv.getName()))
					{
						return nv.getValue();
					}
				}
			}
			return null;
		}

		/// <summary>Authenticates an HTTP client request.</summary>
		/// <remarks>
		/// Authenticates an HTTP client request.
		/// <p>
		/// It extracts the
		/// <see cref="org.apache.hadoop.security.authentication.client.PseudoAuthenticator.USER_NAME
		/// 	"/>
		/// parameter from the query string and creates
		/// an
		/// <see cref="AuthenticationToken"/>
		/// with it.
		/// <p>
		/// If the HTTP client request does not contain the
		/// <see cref="org.apache.hadoop.security.authentication.client.PseudoAuthenticator.USER_NAME
		/// 	"/>
		/// parameter and
		/// the handler is configured to allow anonymous users it returns the
		/// <see cref="AuthenticationToken.ANONYMOUS"/>
		/// token.
		/// <p>
		/// If the HTTP client request does not contain the
		/// <see cref="org.apache.hadoop.security.authentication.client.PseudoAuthenticator.USER_NAME
		/// 	"/>
		/// parameter and
		/// the handler is configured to disallow anonymous users it throws an
		/// <see cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		/// .
		/// </remarks>
		/// <param name="request">the HTTP client request.</param>
		/// <param name="response">the HTTP client response.</param>
		/// <returns>an authentication token if the HTTP client request is accepted and credentials are valid.
		/// 	</returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">thrown if HTTP client request was not accepted as an authentication request.</exception>
		public override org.apache.hadoop.security.authentication.server.AuthenticationToken
			 authenticate(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			org.apache.hadoop.security.authentication.server.AuthenticationToken token;
			string userName = getUserName(request);
			if (userName == null)
			{
				if (getAcceptAnonymous())
				{
					token = org.apache.hadoop.security.authentication.server.AuthenticationToken.ANONYMOUS;
				}
				else
				{
					response.setStatus(javax.servlet.http.HttpServletResponse.SC_FORBIDDEN);
					response.setHeader(WWW_AUTHENTICATE, PSEUDO_AUTH);
					token = null;
				}
			}
			else
			{
				token = new org.apache.hadoop.security.authentication.server.AuthenticationToken(
					userName, userName, getType());
			}
			return token;
		}
	}
}
