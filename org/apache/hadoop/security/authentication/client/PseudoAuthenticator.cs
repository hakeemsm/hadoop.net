using Sharpen;

namespace org.apache.hadoop.security.authentication.client
{
	/// <summary>
	/// The
	/// <see cref="PseudoAuthenticator"/>
	/// implementation provides an authentication equivalent to Hadoop's
	/// Simple authentication, it trusts the value of the 'user.name' Java System property.
	/// <p>
	/// The 'user.name' value is propagated using an additional query string parameter
	/// <see cref="USER_NAME"/>
	/// ('user.name').
	/// </summary>
	public class PseudoAuthenticator : org.apache.hadoop.security.authentication.client.Authenticator
	{
		/// <summary>Name of the additional parameter that carries the 'user.name' value.</summary>
		public const string USER_NAME = "user.name";

		private const string USER_NAME_EQ = USER_NAME + "=";

		private org.apache.hadoop.security.authentication.client.ConnectionConfigurator connConfigurator;

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
		public virtual void setConnectionConfigurator(org.apache.hadoop.security.authentication.client.ConnectionConfigurator
			 configurator)
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
		/// <see cref="USER_NAME"/>
		/// in the query string with the value returned by the
		/// <see cref="getUserName()"/>
		/// method.
		/// <p>
		/// If the response is successful it will update the authentication token.
		/// </remarks>
		/// <param name="url">the URl to authenticate against.</param>
		/// <param name="token">the authencation token being used for the user.</param>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="AuthenticationException">if an authentication error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		public virtual void authenticate(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token)
		{
			string strUrl = url.ToString();
			string paramSeparator = (strUrl.contains("?")) ? "&" : "?";
			strUrl += paramSeparator + USER_NAME_EQ + getUserName();
			url = new java.net.URL(strUrl);
			java.net.HttpURLConnection conn = (java.net.HttpURLConnection)url.openConnection(
				);
			if (connConfigurator != null)
			{
				conn = connConfigurator.configure(conn);
			}
			conn.setRequestMethod("OPTIONS");
			conn.connect();
			org.apache.hadoop.security.authentication.client.AuthenticatedURL.extractToken(conn
				, token);
		}

		/// <summary>Returns the current user name.</summary>
		/// <remarks>
		/// Returns the current user name.
		/// <p>
		/// This implementation returns the value of the Java system property 'user.name'
		/// </remarks>
		/// <returns>the current user name.</returns>
		protected internal virtual string getUserName()
		{
			return Sharpen.Runtime.getProperty("user.name");
		}
	}
}
