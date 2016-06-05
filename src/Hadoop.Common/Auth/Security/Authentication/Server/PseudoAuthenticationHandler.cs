using System.Collections.Generic;
using System.Text;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Http;
using Org.Apache.Http.Client.Utils;


namespace Org.Apache.Hadoop.Security.Authentication.Server
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
	public class PseudoAuthenticationHandler : AuthenticationHandler
	{
		/// <summary>Constant that identifies the authentication mechanism.</summary>
		public const string Type = "simple";

		/// <summary>Constant for the configuration property that indicates if anonymous users are allowed.
		/// 	</summary>
		public const string AnonymousAllowed = Type + ".anonymous.allowed";

		private static readonly Encoding Utf8Charset = Extensions.GetEncoding("UTF-8"
			);

		private const string PseudoAuth = "PseudoAuth";

		private bool acceptAnonymous;

		private string type;

		/// <summary>
		/// Creates a Hadoop pseudo authentication handler with the default auth-token
		/// type, <code>simple</code>.
		/// </summary>
		public PseudoAuthenticationHandler()
			: this(Type)
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
		/// <see cref="AuthenticationFilter.Init(Javax.Servlet.FilterConfig)"/>
		/// method.
		/// </remarks>
		/// <param name="config">configuration properties to initialize the handler.</param>
		/// <exception cref="Javax.Servlet.ServletException">thrown if the handler could not be initialized.
		/// 	</exception>
		public override void Init(Properties config)
		{
			acceptAnonymous = System.Boolean.Parse(config.GetProperty(AnonymousAllowed, "false"
				));
		}

		/// <summary>Returns if the handler is configured to support anonymous users.</summary>
		/// <returns>if the handler is configured to support anonymous users.</returns>
		protected internal virtual bool GetAcceptAnonymous()
		{
			return acceptAnonymous;
		}

		/// <summary>Releases any resources initialized by the authentication handler.</summary>
		/// <remarks>
		/// Releases any resources initialized by the authentication handler.
		/// <p>
		/// This implementation does a NOP.
		/// </remarks>
		public override void Destroy()
		{
		}

		/// <summary>Returns the authentication type of the authentication handler, 'simple'.
		/// 	</summary>
		/// <returns>the authentication type of the authentication handler, 'simple'.</returns>
		public override string GetType()
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">it is never thrown.</exception>
		public override bool ManagementOperation(AuthenticationToken token, HttpServletRequest
			 request, HttpServletResponse response)
		{
			return true;
		}

		private string GetUserName(HttpServletRequest request)
		{
			IList<NameValuePair> list = URLEncodedUtils.Parse(request.GetQueryString(), Utf8Charset
				);
			if (list != null)
			{
				foreach (NameValuePair nv in list)
				{
					if (PseudoAuthenticator.UserName.Equals(nv.GetName()))
					{
						return nv.GetValue();
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
		/// <see cref="Org.Apache.Hadoop.Security.Authentication.Client.PseudoAuthenticator.UserName
		/// 	"/>
		/// parameter from the query string and creates
		/// an
		/// <see cref="AuthenticationToken"/>
		/// with it.
		/// <p>
		/// If the HTTP client request does not contain the
		/// <see cref="Org.Apache.Hadoop.Security.Authentication.Client.PseudoAuthenticator.UserName
		/// 	"/>
		/// parameter and
		/// the handler is configured to allow anonymous users it returns the
		/// <see cref="AuthenticationToken.Anonymous"/>
		/// token.
		/// <p>
		/// If the HTTP client request does not contain the
		/// <see cref="Org.Apache.Hadoop.Security.Authentication.Client.PseudoAuthenticator.UserName
		/// 	"/>
		/// parameter and
		/// the handler is configured to disallow anonymous users it throws an
		/// <see cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		/// .
		/// </remarks>
		/// <param name="request">the HTTP client request.</param>
		/// <param name="response">the HTTP client response.</param>
		/// <returns>an authentication token if the HTTP client request is accepted and credentials are valid.
		/// 	</returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">thrown if HTTP client request was not accepted as an authentication request.</exception>
		public override AuthenticationToken Authenticate(HttpServletRequest request, HttpServletResponse
			 response)
		{
			AuthenticationToken token;
			string userName = GetUserName(request);
			if (userName == null)
			{
				if (GetAcceptAnonymous())
				{
					token = AuthenticationToken.Anonymous;
				}
				else
				{
					response.SetStatus(HttpServletResponse.ScForbidden);
					response.SetHeader(WwwAuthenticate, PseudoAuth);
					token = null;
				}
			}
			else
			{
				token = new AuthenticationToken(userName, userName, GetType());
			}
			return token;
		}
	}
}
