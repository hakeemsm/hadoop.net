using System;
using System.Collections.Generic;


namespace Org.Apache.Hadoop.Security.Authentication.Client
{
	/// <summary>
	/// The
	/// <see cref="AuthenticatedURL"/>
	/// class enables the use of the JDK
	/// <see cref="System.Uri"/>
	/// class
	/// against HTTP endpoints protected with the
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationFilter"
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
		public const string AuthCookie = "hadoop.auth";

		private const string AuthCookieEq = AuthCookie + "=";

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
					throw new ArgumentException("tokenStr cannot be null");
				}
				Set(tokenStr);
			}

			/// <summary>Returns if a token from the server has been set.</summary>
			/// <returns>if a token from the server has been set.</returns>
			public virtual bool IsSet()
			{
				return token != null;
			}

			/// <summary>Sets a token.</summary>
			/// <param name="tokenStr">string representation of the tokenStr.</param>
			internal virtual void Set(string tokenStr)
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

		private static Type DefaultAuthenticator = typeof(KerberosAuthenticator);

		/// <summary>
		/// Sets the default
		/// <see cref="Authenticator"/>
		/// class to use when an
		/// <see cref="AuthenticatedURL"/>
		/// instance
		/// is created without specifying an authenticator.
		/// </summary>
		/// <param name="authenticator">the authenticator class to use as default.</param>
		public static void SetDefaultAuthenticator(Type authenticator)
		{
			DefaultAuthenticator = authenticator;
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
		public static Type GetDefaultAuthenticator()
		{
			return DefaultAuthenticator;
		}

		private Authenticator authenticator;

		private ConnectionConfigurator connConfigurator;

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
		public AuthenticatedURL(Authenticator authenticator)
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
		public AuthenticatedURL(Authenticator authenticator, ConnectionConfigurator connConfigurator
			)
		{
			try
			{
				this.authenticator = (authenticator != null) ? authenticator : System.Activator.CreateInstance
					(DefaultAuthenticator);
			}
			catch (Exception ex)
			{
				throw new RuntimeException(ex);
			}
			this.connConfigurator = connConfigurator;
			this.authenticator.SetConnectionConfigurator(connConfigurator);
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
		protected internal virtual Authenticator GetAuthenticator()
		{
			return authenticator;
		}

		/// <summary>
		/// Returns an authenticated
		/// <see cref="HttpURLConnection"/>
		/// .
		/// </summary>
		/// <param name="url">the URL to connect to. Only HTTP/S URLs are supported.</param>
		/// <param name="token">the authentication token being used for the user.</param>
		/// <returns>
		/// an authenticated
		/// <see cref="HttpURLConnection"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="AuthenticationException">if an authentication exception occurred.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		public virtual HttpURLConnection OpenConnection(Uri url, AuthenticatedURL.Token token
			)
		{
			if (url == null)
			{
				throw new ArgumentException("url cannot be NULL");
			}
			if (!Runtime.EqualsIgnoreCase(url.Scheme, "http") && !Runtime.EqualsIgnoreCase
				(url.Scheme, "https"))
			{
				throw new ArgumentException("url must be for a HTTP or HTTPS resource");
			}
			if (token == null)
			{
				throw new ArgumentException("token cannot be NULL");
			}
			authenticator.Authenticate(url, token);
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			if (connConfigurator != null)
			{
				conn = connConfigurator.Configure(conn);
			}
			InjectToken(conn, token);
			return conn;
		}

		/// <summary>Helper method that injects an authentication token to send with a connection.
		/// 	</summary>
		/// <param name="conn">connection to inject the authentication token into.</param>
		/// <param name="token">authentication token to inject.</param>
		public static void InjectToken(HttpURLConnection conn, AuthenticatedURL.Token token
			)
		{
			string t = token.token;
			if (t != null)
			{
				if (!t.StartsWith("\""))
				{
					t = "\"" + t + "\"";
				}
				conn.AddRequestProperty("Cookie", AuthCookieEq + t);
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		public static void ExtractToken(HttpURLConnection conn, AuthenticatedURL.Token token
			)
		{
			int respCode = conn.GetResponseCode();
			if (respCode == HttpURLConnection.HttpOk || respCode == HttpURLConnection.HttpCreated
				 || respCode == HttpURLConnection.HttpAccepted)
			{
				IDictionary<string, IList<string>> headers = conn.GetHeaderFields();
				IList<string> cookies = headers["Set-Cookie"];
				if (cookies != null)
				{
					foreach (string cookie in cookies)
					{
						if (cookie.StartsWith(AuthCookieEq))
						{
							string value = Runtime.Substring(cookie, AuthCookieEq.Length);
							int separator = value.IndexOf(";");
							if (separator > -1)
							{
								value = Runtime.Substring(value, 0, separator);
							}
							if (value.Length > 0)
							{
								token.Set(value);
							}
						}
					}
				}
			}
			else
			{
				token.Set(null);
				throw new AuthenticationException("Authentication failed, status: " + conn.GetResponseCode
					() + ", message: " + conn.GetResponseMessage());
			}
		}
	}
}
