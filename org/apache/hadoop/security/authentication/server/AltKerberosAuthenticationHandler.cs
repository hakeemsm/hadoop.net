using Sharpen;

namespace org.apache.hadoop.security.authentication.server
{
	/// <summary>
	/// The
	/// <see cref="AltKerberosAuthenticationHandler"/>
	/// behaves exactly the same way as
	/// the
	/// <see cref="KerberosAuthenticationHandler"/>
	/// , except that it allows for an
	/// alternative form of authentication for browsers while still using Kerberos
	/// for Java access.  This is an abstract class that should be subclassed
	/// to allow a developer to implement their own custom authentication for browser
	/// access.  The alternateAuthenticate method will be called whenever a request
	/// comes from a browser.
	/// </summary>
	public abstract class AltKerberosAuthenticationHandler : org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
	{
		/// <summary>Constant that identifies the authentication mechanism.</summary>
		public const string TYPE = "alt-kerberos";

		/// <summary>
		/// Constant for the configuration property that indicates which user agents
		/// are not considered browsers (comma separated)
		/// </summary>
		public const string NON_BROWSER_USER_AGENTS = TYPE + ".non-browser.user-agents";

		private const string NON_BROWSER_USER_AGENTS_DEFAULT = "java,curl,wget,perl";

		private string[] nonBrowserUserAgents;

		/// <summary>
		/// Returns the authentication type of the authentication handler,
		/// 'alt-kerberos'.
		/// </summary>
		/// <returns>
		/// the authentication type of the authentication handler,
		/// 'alt-kerberos'.
		/// </returns>
		public override string getType()
		{
			return TYPE;
		}

		/// <exception cref="javax.servlet.ServletException"/>
		public override void init(java.util.Properties config)
		{
			base.init(config);
			nonBrowserUserAgents = config.getProperty(NON_BROWSER_USER_AGENTS, NON_BROWSER_USER_AGENTS_DEFAULT
				).split("\\W*,\\W*");
			for (int i = 0; i < nonBrowserUserAgents.Length; i++)
			{
				nonBrowserUserAgents[i] = nonBrowserUserAgents[i].ToLower(java.util.Locale.ENGLISH
					);
			}
		}

		/// <summary>
		/// It enforces the the Kerberos SPNEGO authentication sequence returning an
		/// <see cref="AuthenticationToken"/>
		/// only after the Kerberos SPNEGO sequence has
		/// completed successfully (in the case of Java access) and only after the
		/// custom authentication implemented by the subclass in alternateAuthenticate
		/// has completed successfully (in the case of browser access).
		/// </summary>
		/// <param name="request">the HTTP client request.</param>
		/// <param name="response">the HTTP client response.</param>
		/// <returns>an authentication token if the request is authorized or null</returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurred</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">thrown if an authentication error occurred</exception>
		public override org.apache.hadoop.security.authentication.server.AuthenticationToken
			 authenticate(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			org.apache.hadoop.security.authentication.server.AuthenticationToken token;
			if (isBrowser(request.getHeader("User-Agent")))
			{
				token = alternateAuthenticate(request, response);
			}
			else
			{
				token = base.authenticate(request, response);
			}
			return token;
		}

		/// <summary>
		/// This method parses the User-Agent String and returns whether or not it
		/// refers to a browser.
		/// </summary>
		/// <remarks>
		/// This method parses the User-Agent String and returns whether or not it
		/// refers to a browser.  If its not a browser, then Kerberos authentication
		/// will be used; if it is a browser, alternateAuthenticate from the subclass
		/// will be used.
		/// <p>
		/// A User-Agent String is considered to be a browser if it does not contain
		/// any of the values from alt-kerberos.non-browser.user-agents; the default
		/// behavior is to consider everything a browser unless it contains one of:
		/// "java", "curl", "wget", or "perl".  Subclasses can optionally override
		/// this method to use different behavior.
		/// </remarks>
		/// <param name="userAgent">The User-Agent String, or null if there isn't one</param>
		/// <returns>true if the User-Agent String refers to a browser, false if not</returns>
		protected internal virtual bool isBrowser(string userAgent)
		{
			if (userAgent == null)
			{
				return false;
			}
			userAgent = userAgent.ToLower(java.util.Locale.ENGLISH);
			bool isBrowser = true;
			foreach (string nonBrowserUserAgent in nonBrowserUserAgents)
			{
				if (userAgent.contains(nonBrowserUserAgent))
				{
					isBrowser = false;
					break;
				}
			}
			return isBrowser;
		}

		/// <summary>
		/// Subclasses should implement this method to provide the custom
		/// authentication to be used for browsers.
		/// </summary>
		/// <param name="request">the HTTP client request.</param>
		/// <param name="response">the HTTP client response.</param>
		/// <returns>an authentication token if the request is authorized, or null</returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurs</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">thrown if an authentication error occurs</exception>
		public abstract org.apache.hadoop.security.authentication.server.AuthenticationToken
			 alternateAuthenticate(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response);
	}
}
