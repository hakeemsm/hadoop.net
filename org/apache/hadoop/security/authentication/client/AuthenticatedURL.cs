using Sharpen;

namespace org.apache.hadoop.security.authentication.client
{
	/// <summary>
	/// The
	/// <see cref="AuthenticatedURL"/>
	/// class enables the use of the JDK
	/// <see cref="java.net.URL"/>
	/// class
	/// against HTTP endpoints protected with the
	/// <see cref="org.apache.hadoop.security.authentication.server.AuthenticationFilter"
	/// 	/>
	/// .
	/// <p>
	/// The authentication mechanisms supported by default are Hadoop Simple  authentication
	/// (also known as pseudo authentication) and Kerberos SPNEGO authentication.
	/// <p>
	/// Additional authentication mechanisms can be supported via
	/// <see cref="Authenticator"/>
	/// implementations.
	/// <p>
	/// The default
	/// <see cref="Authenticator"/>
	/// is the
	/// <see cref="KerberosAuthenticator"/>
	/// class which supports
	/// automatic fallback from Kerberos SPNEGO to Hadoop Simple authentication.
	/// <p>
	/// <code>AuthenticatedURL</code> instances are not thread-safe.
	/// <p>
	/// The usage pattern of the
	/// <see cref="AuthenticatedURL"/>
	/// is:
	/// <pre>
	/// // establishing an initial connection
	/// URL url = new URL("http://foo:8080/bar");
	/// AuthenticatedURL.Token token = new AuthenticatedURL.Token();
	/// AuthenticatedURL aUrl = new AuthenticatedURL();
	/// HttpURLConnection conn = new AuthenticatedURL(url, token).openConnection();
	/// ....
	/// // use the 'conn' instance
	/// ....
	/// // establishing a follow up connection using a token from the previous connection
	/// HttpURLConnection conn = new AuthenticatedURL(url, token).openConnection();
	/// ....
	/// // use the 'conn' instance
	/// ....
	/// </pre>
	/// </summary>
	public class AuthenticatedURL
	{
		/// <summary>Name of the HTTP cookie used for the authentication token between the client and the server.
		/// 	</summary>
		public const string AUTH_COOKIE = "hadoop.auth";

		private const string AUTH_COOKIE_EQ = AUTH_COOKIE + "=";

		/// <summary>Client side authentication token.</summary>
		public class Token
		{
			private string token;

			/// <summary>Creates a token.</summary>
			public Token()
			{
			}

			/// <summary>Creates a token using an existing string representation of the token.</summary>
			/// <param name="tokenStr">string representation of the tokenStr.</param>
			public Token(string tokenStr)
			{
				if (tokenStr == null)
				{
					throw new System.ArgumentException("tokenStr cannot be null");
				}
				set(tokenStr);
			}

			/// <summary>Returns if a token from the server has been set.</summary>
			/// <returns>if a token from the server has been set.</returns>
			public virtual bool isSet()
			{
				return token != null;
			}

			/// <summary>Sets a token.</summary>
			/// <param name="tokenStr">string representation of the tokenStr.</param>
			internal virtual void set(string tokenStr)
			{
				token = tokenStr;
			}

			/// <summary>Returns the string representation of the token.</summary>
			/// <returns>the string representation of the token.</returns>
			public override string ToString()
			{
				return token;
			}
		}

		private static java.lang.Class DEFAULT_AUTHENTICATOR = Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.security.authentication.client.KerberosAuthenticator));

		/// <summary>
		/// Sets the default
		/// <see cref="Authenticator"/>
		/// class to use when an
		/// <see cref="AuthenticatedURL"/>
		/// instance
		/// is created without specifying an authenticator.
		/// </summary>
		/// <param name="authenticator">the authenticator class to use as default.</param>
		public static void setDefaultAuthenticator(java.lang.Class authenticator)
		{
			DEFAULT_AUTHENTICATOR = authenticator;
		}

		/// <summary>
		/// Returns the default
		/// <see cref="Authenticator"/>
		/// class to use when an
		/// <see cref="AuthenticatedURL"/>
		/// instance
		/// is created without specifying an authenticator.
		/// </summary>
		/// <returns>the authenticator class to use as default.</returns>
		public static java.lang.Class getDefaultAuthenticator()
		{
			return DEFAULT_AUTHENTICATOR;
		}

		private org.apache.hadoop.security.authentication.client.Authenticator authenticator;

		private org.apache.hadoop.security.authentication.client.ConnectionConfigurator connConfigurator;

		/// <summary>
		/// Creates an
		/// <see cref="AuthenticatedURL"/>
		/// .
		/// </summary>
		public AuthenticatedURL()
			: this(null)
		{
		}

		/// <summary>Creates an <code>AuthenticatedURL</code>.</summary>
		/// <param name="authenticator">
		/// the
		/// <see cref="Authenticator"/>
		/// instance to use, if <code>null</code> a
		/// <see cref="KerberosAuthenticator"/>
		/// is used.
		/// </param>
		public AuthenticatedURL(org.apache.hadoop.security.authentication.client.Authenticator
			 authenticator)
			: this(authenticator, null)
		{
		}

		/// <summary>Creates an <code>AuthenticatedURL</code>.</summary>
		/// <param name="authenticator">
		/// the
		/// <see cref="Authenticator"/>
		/// instance to use, if <code>null</code> a
		/// <see cref="KerberosAuthenticator"/>
		/// is used.
		/// </param>
		/// <param name="connConfigurator">a connection configurator.</param>
		public AuthenticatedURL(org.apache.hadoop.security.authentication.client.Authenticator
			 authenticator, org.apache.hadoop.security.authentication.client.ConnectionConfigurator
			 connConfigurator)
		{
			try
			{
				this.authenticator = (authenticator != null) ? authenticator : DEFAULT_AUTHENTICATOR
					.newInstance();
			}
			catch (System.Exception ex)
			{
				throw new System.Exception(ex);
			}
			this.connConfigurator = connConfigurator;
			this.authenticator.setConnectionConfigurator(connConfigurator);
		}

		/// <summary>
		/// Returns the
		/// <see cref="Authenticator"/>
		/// instance used by the
		/// <code>AuthenticatedURL</code>.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Authenticator"/>
		/// instance
		/// </returns>
		protected internal virtual org.apache.hadoop.security.authentication.client.Authenticator
			 getAuthenticator()
		{
			return authenticator;
		}

		/// <summary>
		/// Returns an authenticated
		/// <see cref="java.net.HttpURLConnection"/>
		/// .
		/// </summary>
		/// <param name="url">the URL to connect to. Only HTTP/S URLs are supported.</param>
		/// <param name="token">the authentication token being used for the user.</param>
		/// <returns>
		/// an authenticated
		/// <see cref="java.net.HttpURLConnection"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="AuthenticationException">if an authentication exception occurred.
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		public virtual java.net.HttpURLConnection openConnection(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token)
		{
			if (url == null)
			{
				throw new System.ArgumentException("url cannot be NULL");
			}
			if (!Sharpen.Runtime.equalsIgnoreCase(url.getProtocol(), "http") && !Sharpen.Runtime.equalsIgnoreCase
				(url.getProtocol(), "https"))
			{
				throw new System.ArgumentException("url must be for a HTTP or HTTPS resource");
			}
			if (token == null)
			{
				throw new System.ArgumentException("token cannot be NULL");
			}
			authenticator.authenticate(url, token);
			java.net.HttpURLConnection conn = (java.net.HttpURLConnection)url.openConnection(
				);
			if (connConfigurator != null)
			{
				conn = connConfigurator.configure(conn);
			}
			injectToken(conn, token);
			return conn;
		}

		/// <summary>Helper method that injects an authentication token to send with a connection.
		/// 	</summary>
		/// <param name="conn">connection to inject the authentication token into.</param>
		/// <param name="token">authentication token to inject.</param>
		public static void injectToken(java.net.HttpURLConnection conn, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token)
		{
			string t = token.token;
			if (t != null)
			{
				if (!t.StartsWith("\""))
				{
					t = "\"" + t + "\"";
				}
				conn.addRequestProperty("Cookie", AUTH_COOKIE_EQ + t);
			}
		}

		/// <summary>Helper method that extracts an authentication token received from a connection.
		/// 	</summary>
		/// <remarks>
		/// Helper method that extracts an authentication token received from a connection.
		/// <p>
		/// This method is used by
		/// <see cref="Authenticator"/>
		/// implementations.
		/// </remarks>
		/// <param name="conn">connection to extract the authentication token from.</param>
		/// <param name="token">the authentication token.</param>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="AuthenticationException">if an authentication exception occurred.
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		public static void extractToken(java.net.HttpURLConnection conn, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token)
		{
			int respCode = conn.getResponseCode();
			if (respCode == java.net.HttpURLConnection.HTTP_OK || respCode == java.net.HttpURLConnection
				.HTTP_CREATED || respCode == java.net.HttpURLConnection.HTTP_ACCEPTED)
			{
				System.Collections.Generic.IDictionary<string, System.Collections.Generic.IList<string
					>> headers = conn.getHeaderFields();
				System.Collections.Generic.IList<string> cookies = headers["Set-Cookie"];
				if (cookies != null)
				{
					foreach (string cookie in cookies)
					{
						if (cookie.StartsWith(AUTH_COOKIE_EQ))
						{
							string value = Sharpen.Runtime.substring(cookie, AUTH_COOKIE_EQ.Length);
							int separator = value.IndexOf(";");
							if (separator > -1)
							{
								value = Sharpen.Runtime.substring(value, 0, separator);
							}
							if (value.Length > 0)
							{
								token.set(value);
							}
						}
					}
				}
			}
			else
			{
				token.set(null);
				throw new org.apache.hadoop.security.authentication.client.AuthenticationException
					("Authentication failed, status: " + conn.getResponseCode() + ", message: " + conn
					.getResponseMessage());
			}
		}
	}
}
