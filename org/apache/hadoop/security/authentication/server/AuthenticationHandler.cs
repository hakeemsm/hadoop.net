using Sharpen;

namespace org.apache.hadoop.security.authentication.server
{
	/// <summary>Interface for server authentication mechanisms.</summary>
	/// <remarks>
	/// Interface for server authentication mechanisms.
	/// The
	/// <see cref="AuthenticationFilter"/>
	/// manages the lifecycle of the authentication handler.
	/// Implementations must be thread-safe as one instance is initialized and used for all requests.
	/// </remarks>
	public abstract class AuthenticationHandler
	{
		public const string WWW_AUTHENTICATE = "WWW-Authenticate";

		/// <summary>Returns the authentication type of the authentication handler.</summary>
		/// <remarks>
		/// Returns the authentication type of the authentication handler.
		/// This should be a name that uniquely identifies the authentication type.
		/// For example 'simple' or 'kerberos'.
		/// </remarks>
		/// <returns>the authentication type of the authentication handler.</returns>
		public abstract string getType();

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
		public abstract void init(java.util.Properties config);

		/// <summary>Destroys the authentication handler instance.</summary>
		/// <remarks>
		/// Destroys the authentication handler instance.
		/// <p>
		/// This method is invoked by the
		/// <see cref="AuthenticationFilter.destroy()"/>
		/// method.
		/// </remarks>
		public abstract void destroy();

		/// <summary>Performs an authentication management operation.</summary>
		/// <remarks>
		/// Performs an authentication management operation.
		/// <p>
		/// This is useful for handling operations like get/renew/cancel
		/// delegation tokens which are being handled as operations of the
		/// service end-point.
		/// <p>
		/// If the method returns <code>TRUE</code> the request will continue normal
		/// processing, this means the method has not produced any HTTP response.
		/// <p>
		/// If the method returns <code>FALSE</code> the request will end, this means
		/// the method has produced the corresponding HTTP response.
		/// </remarks>
		/// <param name="token">the authentication token if any, otherwise <code>NULL</code>.
		/// 	</param>
		/// <param name="request">the HTTP client request.</param>
		/// <param name="response">the HTTP client response.</param>
		/// <returns>
		/// <code>TRUE</code> if the request should be processed as a regular
		/// request,
		/// <code>FALSE</code> otherwise.
		/// </returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">thrown if an Authentication error occurred.</exception>
		public abstract bool managementOperation(org.apache.hadoop.security.authentication.server.AuthenticationToken
			 token, javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response);

		/// <summary>Performs an authentication step for the given HTTP client request.</summary>
		/// <remarks>
		/// Performs an authentication step for the given HTTP client request.
		/// <p>
		/// This method is invoked by the
		/// <see cref="AuthenticationFilter"/>
		/// only if the HTTP client request is
		/// not yet authenticated.
		/// <p>
		/// Depending upon the authentication mechanism being implemented, a particular HTTP client may
		/// end up making a sequence of invocations before authentication is successfully established (this is
		/// the case of Kerberos SPNEGO).
		/// <p>
		/// This method must return an
		/// <see cref="AuthenticationToken"/>
		/// only if the the HTTP client request has
		/// been successfully and fully authenticated.
		/// <p>
		/// If the HTTP client request has not been completely authenticated, this method must take over
		/// the corresponding HTTP response and it must return <code>null</code>.
		/// </remarks>
		/// <param name="request">the HTTP client request.</param>
		/// <param name="response">the HTTP client response.</param>
		/// <returns>
		/// an
		/// <see cref="AuthenticationToken"/>
		/// if the HTTP client request has been authenticated,
		/// <code>null</code> otherwise (in this case it must take care of the response).
		/// </returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">thrown if an Authentication error occurred.</exception>
		public abstract org.apache.hadoop.security.authentication.server.AuthenticationToken
			 authenticate(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response);
	}

	public static class AuthenticationHandlerConstants
	{
	}
}
