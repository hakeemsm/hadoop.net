using System;


namespace Org.Apache.Hadoop.Security.Authentication.Client
{
	/// <summary>
	/// The
	/// <see cref="PseudoAuthenticator"/>
	/// implementation provides an authentication equivalent to Hadoop's
	/// Simple authentication, it trusts the value of the 'user.name' Java System property.
	/// <p>
	/// The 'user.name' value is propagated using an additional query string parameter
	/// <see cref="UserName"/>
	/// ('user.name').
	/// </summary>
	public class PseudoAuthenticator : Authenticator
	{
		/// <summary>Name of the additional parameter that carries the 'user.name' value.</summary>
		public const string UserName = "user.name";

		private const string UserNameEq = UserName + "=";

		private ConnectionConfigurator connConfigurator;

		/// <summary>
		/// Sets a
		/// <see cref="ConnectionConfigurator"/>
		/// instance to use for
		/// configuring connections.
		/// </summary>
		/// <param name="configurator">
		/// the
		/// <see cref="ConnectionConfigurator"/>
		/// instance.
		/// </param>
		public virtual void SetConnectionConfigurator(ConnectionConfigurator configurator
			)
		{
			connConfigurator = configurator;
		}

		/// <summary>Performs simple authentication against the specified URL.</summary>
		/// <remarks>
		/// Performs simple authentication against the specified URL.
		/// <p>
		/// If a token is given it does a NOP and returns the given token.
		/// <p>
		/// If no token is given, it will perform an HTTP <code>OPTIONS</code> request injecting an additional
		/// parameter
		/// <see cref="UserName"/>
		/// in the query string with the value returned by the
		/// <see cref="GetUserName()"/>
		/// method.
		/// <p>
		/// If the response is successful it will update the authentication token.
		/// </remarks>
		/// <param name="url">the URl to authenticate against.</param>
		/// <param name="token">the authencation token being used for the user.</param>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="AuthenticationException">if an authentication error occurred.</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		public virtual void Authenticate(Uri url, AuthenticatedURL.Token token)
		{
			string strUrl = url.ToString();
			string paramSeparator = (strUrl.Contains("?")) ? "&" : "?";
			strUrl += paramSeparator + UserNameEq + GetUserName();
			url = new Uri(strUrl);
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			if (connConfigurator != null)
			{
				conn = connConfigurator.Configure(conn);
			}
			conn.SetRequestMethod("OPTIONS");
			conn.Connect();
			AuthenticatedURL.ExtractToken(conn, token);
		}

		/// <summary>Returns the current user name.</summary>
		/// <remarks>
		/// Returns the current user name.
		/// <p>
		/// This implementation returns the value of the Java system property 'user.name'
		/// </remarks>
		/// <returns>the current user name.</returns>
		protected internal virtual string GetUserName()
		{
			return Runtime.GetProperty("user.name");
		}
	}
}
