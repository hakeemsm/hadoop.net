using System;


namespace Org.Apache.Hadoop.Security.Authentication.Client
{
	/// <summary>Interface for client authentication mechanisms.</summary>
	/// <remarks>
	/// Interface for client authentication mechanisms.
	/// <p>
	/// Implementations are use-once instances, they don't need to be thread safe.
	/// </remarks>
	public interface Authenticator
	{
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
		void SetConnectionConfigurator(ConnectionConfigurator configurator);

		/// <summary>
		/// Authenticates against a URL and returns a
		/// <see cref="Token"/>
		/// to be
		/// used by subsequent requests.
		/// </summary>
		/// <param name="url">the URl to authenticate against.</param>
		/// <param name="token">the authentication token being used for the user.</param>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="AuthenticationException">if an authentication error occurred.</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		void Authenticate(Uri url, AuthenticatedURL.Token token);
	}
}
