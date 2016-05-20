using Sharpen;

namespace org.apache.hadoop.security.token.delegation.web
{
	/// <summary>
	/// The <code>DelegationTokenAuthenticatedURL</code> is a
	/// <see cref="org.apache.hadoop.security.authentication.client.AuthenticatedURL"/>
	/// sub-class with built-in Hadoop Delegation Token
	/// functionality.
	/// <p/>
	/// The authentication mechanisms supported by default are Hadoop Simple
	/// authentication (also known as pseudo authentication) and Kerberos SPNEGO
	/// authentication.
	/// <p/>
	/// Additional authentication mechanisms can be supported via
	/// <see cref="DelegationTokenAuthenticator"/>
	/// implementations.
	/// <p/>
	/// The default
	/// <see cref="DelegationTokenAuthenticator"/>
	/// is the
	/// <see cref="KerberosDelegationTokenAuthenticator"/>
	/// class which supports
	/// automatic fallback from Kerberos SPNEGO to Hadoop Simple authentication via
	/// the
	/// <see cref="PseudoDelegationTokenAuthenticator"/>
	/// class.
	/// <p/>
	/// <code>AuthenticatedURL</code> instances are not thread-safe.
	/// </summary>
	public class DelegationTokenAuthenticatedURL : org.apache.hadoop.security.authentication.client.AuthenticatedURL
	{
		/// <summary>
		/// Constant used in URL's query string to perform a proxy user request, the
		/// value of the <code>DO_AS</code> parameter is the user the request will be
		/// done on behalf of.
		/// </summary>
		internal const string DO_AS = "doAs";

		/// <summary>Client side authentication token that handles Delegation Tokens.</summary>
		public class Token : org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
		{
			private org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
				> delegationToken;

			public virtual org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
				> getDelegationToken()
			{
				return delegationToken;
			}

			public virtual void setDelegationToken(org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
				> delegationToken)
			{
				this.delegationToken = delegationToken;
			}
		}

		private static java.lang.Class DEFAULT_AUTHENTICATOR = Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
			));

		/// <summary>
		/// Sets the default
		/// <see cref="DelegationTokenAuthenticator"/>
		/// class to use when an
		/// <see cref="DelegationTokenAuthenticatedURL"/>
		/// instance is created without
		/// specifying one.
		/// The default class is
		/// <see cref="KerberosDelegationTokenAuthenticator"/>
		/// </summary>
		/// <param name="authenticator">the authenticator class to use as default.</param>
		public static void setDefaultDelegationTokenAuthenticator(java.lang.Class authenticator
			)
		{
			DEFAULT_AUTHENTICATOR = authenticator;
		}

		/// <summary>
		/// Returns the default
		/// <see cref="DelegationTokenAuthenticator"/>
		/// class to use when
		/// an
		/// <see cref="DelegationTokenAuthenticatedURL"/>
		/// instance is created without
		/// specifying one.
		/// <p/>
		/// The default class is
		/// <see cref="KerberosDelegationTokenAuthenticator"/>
		/// </summary>
		/// <returns>the delegation token authenticator class to use as default.</returns>
		public static java.lang.Class getDefaultDelegationTokenAuthenticator()
		{
			return DEFAULT_AUTHENTICATOR;
		}

		private static org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator
			 obtainDelegationTokenAuthenticator(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator
			 dta, org.apache.hadoop.security.authentication.client.ConnectionConfigurator connConfigurator
			)
		{
			try
			{
				if (dta == null)
				{
					dta = DEFAULT_AUTHENTICATOR.newInstance();
					dta.setConnectionConfigurator(connConfigurator);
				}
				return dta;
			}
			catch (System.Exception ex)
			{
				throw new System.ArgumentException(ex);
			}
		}

		private bool useQueryStringforDelegationToken = false;

		/// <summary>Creates an <code>DelegationTokenAuthenticatedURL</code>.</summary>
		/// <remarks>
		/// Creates an <code>DelegationTokenAuthenticatedURL</code>.
		/// <p/>
		/// An instance of the default
		/// <see cref="DelegationTokenAuthenticator"/>
		/// will be
		/// used.
		/// </remarks>
		public DelegationTokenAuthenticatedURL()
			: this(null, null)
		{
		}

		/// <summary>Creates an <code>DelegationTokenAuthenticatedURL</code>.</summary>
		/// <param name="authenticator">
		/// the
		/// <see cref="DelegationTokenAuthenticator"/>
		/// instance to
		/// use, if <code>null</code> the default one will be used.
		/// </param>
		public DelegationTokenAuthenticatedURL(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator
			 authenticator)
			: this(authenticator, null)
		{
		}

		/// <summary>
		/// Creates an <code>DelegationTokenAuthenticatedURL</code> using the default
		/// <see cref="DelegationTokenAuthenticator"/>
		/// class.
		/// </summary>
		/// <param name="connConfigurator">a connection configurator.</param>
		public DelegationTokenAuthenticatedURL(org.apache.hadoop.security.authentication.client.ConnectionConfigurator
			 connConfigurator)
			: this(null, connConfigurator)
		{
		}

		/// <summary>Creates an <code>DelegationTokenAuthenticatedURL</code>.</summary>
		/// <param name="authenticator">
		/// the
		/// <see cref="DelegationTokenAuthenticator"/>
		/// instance to
		/// use, if <code>null</code> the default one will be used.
		/// </param>
		/// <param name="connConfigurator">a connection configurator.</param>
		public DelegationTokenAuthenticatedURL(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator
			 authenticator, org.apache.hadoop.security.authentication.client.ConnectionConfigurator
			 connConfigurator)
			: base(obtainDelegationTokenAuthenticator(authenticator, connConfigurator), connConfigurator
				)
		{
		}

		/// <summary>Sets if delegation token should be transmitted in the URL query string.</summary>
		/// <remarks>
		/// Sets if delegation token should be transmitted in the URL query string.
		/// By default it is transmitted using the
		/// <see cref="DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER"/>
		/// HTTP header.
		/// <p/>
		/// This method is provided to enable WebHDFS backwards compatibility.
		/// </remarks>
		/// <param name="useQueryString">
		/// <code>TRUE</code> if the token is transmitted in the
		/// URL query string, <code>FALSE</code> if the delegation token is transmitted
		/// using the
		/// <see cref="DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER"/>
		/// HTTP
		/// header.
		/// </param>
		[System.Obsolete]
		protected internal virtual void setUseQueryStringForDelegationToken(bool useQueryString
			)
		{
			useQueryStringforDelegationToken = useQueryString;
		}

		/// <summary>Returns if delegation token is transmitted as a HTTP header.</summary>
		/// <returns>
		/// <code>TRUE</code> if the token is transmitted in the URL query
		/// string, <code>FALSE</code> if the delegation token is transmitted using the
		/// <see cref="DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER"/>
		/// HTTP header.
		/// </returns>
		public virtual bool useQueryStringForDelegationToken()
		{
			return useQueryStringforDelegationToken;
		}

		/// <summary>
		/// Returns an authenticated
		/// <see cref="java.net.HttpURLConnection"/>
		/// , it uses a Delegation
		/// Token only if the given auth token is an instance of
		/// <see cref="Token"/>
		/// and
		/// it contains a Delegation Token, otherwise use the configured
		/// <see cref="DelegationTokenAuthenticator"/>
		/// to authenticate the connection.
		/// </summary>
		/// <param name="url">the URL to connect to. Only HTTP/S URLs are supported.</param>
		/// <param name="token">the authentication token being used for the user.</param>
		/// <returns>
		/// an authenticated
		/// <see cref="java.net.HttpURLConnection"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public override java.net.HttpURLConnection openConnection(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token)
		{
			return (token is org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
				) ? openConnection(url, (org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
				)token) : base.openConnection(url, token);
		}

		/// <summary>
		/// Returns an authenticated
		/// <see cref="java.net.HttpURLConnection"/>
		/// . If the Delegation
		/// Token is present, it will be used taking precedence over the configured
		/// <code>Authenticator</code>.
		/// </summary>
		/// <param name="url">the URL to connect to. Only HTTP/S URLs are supported.</param>
		/// <param name="token">the authentication token being used for the user.</param>
		/// <returns>
		/// an authenticated
		/// <see cref="java.net.HttpURLConnection"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual java.net.HttpURLConnection openConnection(java.net.URL url, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
			 token)
		{
			return openConnection(url, token, null);
		}

		/// <exception cref="System.IO.IOException"/>
		private java.net.URL augmentURL(java.net.URL url, System.Collections.Generic.IDictionary
			<string, string> @params)
		{
			if (@params != null && @params.Count > 0)
			{
				string urlStr = url.toExternalForm();
				java.lang.StringBuilder sb = new java.lang.StringBuilder(urlStr);
				string separator = (urlStr.contains("?")) ? "&" : "?";
				foreach (System.Collections.Generic.KeyValuePair<string, string> param in @params)
				{
					sb.Append(separator).Append(param.Key).Append("=").Append(param.Value);
					separator = "&";
				}
				url = new java.net.URL(sb.ToString());
			}
			return url;
		}

		/// <summary>
		/// Returns an authenticated
		/// <see cref="java.net.HttpURLConnection"/>
		/// . If the Delegation
		/// Token is present, it will be used taking precedence over the configured
		/// <code>Authenticator</code>. If the <code>doAs</code> parameter is not NULL,
		/// the request will be done on behalf of the specified <code>doAs</code> user.
		/// </summary>
		/// <param name="url">the URL to connect to. Only HTTP/S URLs are supported.</param>
		/// <param name="token">the authentication token being used for the user.</param>
		/// <param name="doAs">
		/// user to do the the request on behalf of, if NULL the request is
		/// as self.
		/// </param>
		/// <returns>
		/// an authenticated
		/// <see cref="java.net.HttpURLConnection"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual java.net.HttpURLConnection openConnection(java.net.URL url, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
			 token, string doAs)
		{
			com.google.common.@base.Preconditions.checkNotNull(url, "url");
			com.google.common.@base.Preconditions.checkNotNull(token, "token");
			System.Collections.Generic.IDictionary<string, string> extraParams = new System.Collections.Generic.Dictionary
				<string, string>();
			org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.TokenIdentifier
				> dToken = null;
			// if we have valid auth token, it takes precedence over a delegation token
			// and we don't even look for one.
			if (!token.isSet())
			{
				// delegation token
				org.apache.hadoop.security.Credentials creds = org.apache.hadoop.security.UserGroupInformation
					.getCurrentUser().getCredentials();
				if (!creds.getAllTokens().isEmpty())
				{
					java.net.InetSocketAddress serviceAddr = new java.net.InetSocketAddress(url.getHost
						(), url.getPort());
					org.apache.hadoop.io.Text service = org.apache.hadoop.security.SecurityUtil.buildTokenService
						(serviceAddr);
					dToken = creds.getToken(service);
					if (dToken != null)
					{
						if (useQueryStringForDelegationToken())
						{
							// delegation token will go in the query string, injecting it
							extraParams[org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
								.DELEGATION_PARAM] = dToken.encodeToUrlString();
						}
						else
						{
							// delegation token will go as request header, setting it in the
							// auth-token to ensure no authentication handshake is triggered
							// (if we have a delegation token, we are authenticated)
							// the delegation token header is injected in the connection request
							// at the end of this method.
							token.delegationToken = (org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
								>)dToken;
						}
					}
				}
			}
			// proxyuser
			if (doAs != null)
			{
				extraParams[DO_AS] = java.net.URLEncoder.encode(doAs, "UTF-8");
			}
			url = augmentURL(url, extraParams);
			java.net.HttpURLConnection conn = base.openConnection(url, token);
			if (!token.isSet() && !useQueryStringForDelegationToken() && dToken != null)
			{
				// injecting the delegation token header in the connection request
				conn.setRequestProperty(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator
					.DELEGATION_TOKEN_HEADER, dToken.encodeToUrlString());
			}
			return conn;
		}

		/// <summary>
		/// Requests a delegation token using the configured <code>Authenticator</code>
		/// for authentication.
		/// </summary>
		/// <param name="url">
		/// the URL to get the delegation token from. Only HTTP/S URLs are
		/// supported.
		/// </param>
		/// <param name="token">
		/// the authentication token being used for the user where the
		/// Delegation token will be stored.
		/// </param>
		/// <param name="renewer">the renewer user.</param>
		/// <returns>a delegation token.</returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
			> getDelegationToken(java.net.URL url, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
			 token, string renewer)
		{
			return getDelegationToken(url, token, renewer, null);
		}

		/// <summary>
		/// Requests a delegation token using the configured <code>Authenticator</code>
		/// for authentication.
		/// </summary>
		/// <param name="url">
		/// the URL to get the delegation token from. Only HTTP/S URLs are
		/// supported.
		/// </param>
		/// <param name="token">
		/// the authentication token being used for the user where the
		/// Delegation token will be stored.
		/// </param>
		/// <param name="renewer">the renewer user.</param>
		/// <param name="doAsUser">the user to do as, which will be the token owner.</param>
		/// <returns>a delegation token.</returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
			> getDelegationToken(java.net.URL url, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
			 token, string renewer, string doAsUser)
		{
			com.google.common.@base.Preconditions.checkNotNull(url, "url");
			com.google.common.@base.Preconditions.checkNotNull(token, "token");
			try
			{
				token.delegationToken = ((org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
					)getAuthenticator()).getDelegationToken(url, token, renewer, doAsUser);
				return token.delegationToken;
			}
			catch (System.IO.IOException ex)
			{
				token.delegationToken = null;
				throw;
			}
		}

		/// <summary>
		/// Renews a delegation token from the server end-point using the
		/// configured <code>Authenticator</code> for authentication.
		/// </summary>
		/// <param name="url">
		/// the URL to renew the delegation token from. Only HTTP/S URLs are
		/// supported.
		/// </param>
		/// <param name="token">the authentication token with the Delegation Token to renew.</param>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual long renewDelegationToken(java.net.URL url, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
			 token)
		{
			return renewDelegationToken(url, token, null);
		}

		/// <summary>
		/// Renews a delegation token from the server end-point using the
		/// configured <code>Authenticator</code> for authentication.
		/// </summary>
		/// <param name="url">
		/// the URL to renew the delegation token from. Only HTTP/S URLs are
		/// supported.
		/// </param>
		/// <param name="token">the authentication token with the Delegation Token to renew.</param>
		/// <param name="doAsUser">the user to do as, which will be the token owner.</param>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual long renewDelegationToken(java.net.URL url, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
			 token, string doAsUser)
		{
			com.google.common.@base.Preconditions.checkNotNull(url, "url");
			com.google.common.@base.Preconditions.checkNotNull(token, "token");
			com.google.common.@base.Preconditions.checkNotNull(token.delegationToken, "No delegation token available"
				);
			try
			{
				return ((org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
					)getAuthenticator()).renewDelegationToken(url, token, token.delegationToken, doAsUser
					);
			}
			catch (System.IO.IOException ex)
			{
				token.delegationToken = null;
				throw;
			}
		}

		/// <summary>Cancels a delegation token from the server end-point.</summary>
		/// <remarks>
		/// Cancels a delegation token from the server end-point. It does not require
		/// being authenticated by the configured <code>Authenticator</code>.
		/// </remarks>
		/// <param name="url">
		/// the URL to cancel the delegation token from. Only HTTP/S URLs
		/// are supported.
		/// </param>
		/// <param name="token">the authentication token with the Delegation Token to cancel.
		/// 	</param>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		public virtual void cancelDelegationToken(java.net.URL url, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
			 token)
		{
			cancelDelegationToken(url, token, null);
		}

		/// <summary>Cancels a delegation token from the server end-point.</summary>
		/// <remarks>
		/// Cancels a delegation token from the server end-point. It does not require
		/// being authenticated by the configured <code>Authenticator</code>.
		/// </remarks>
		/// <param name="url">
		/// the URL to cancel the delegation token from. Only HTTP/S URLs
		/// are supported.
		/// </param>
		/// <param name="token">the authentication token with the Delegation Token to cancel.
		/// 	</param>
		/// <param name="doAsUser">the user to do as, which will be the token owner.</param>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		public virtual void cancelDelegationToken(java.net.URL url, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
			 token, string doAsUser)
		{
			com.google.common.@base.Preconditions.checkNotNull(url, "url");
			com.google.common.@base.Preconditions.checkNotNull(token, "token");
			com.google.common.@base.Preconditions.checkNotNull(token.delegationToken, "No delegation token available"
				);
			try
			{
				((org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
					)getAuthenticator()).cancelDelegationToken(url, token, token.delegationToken, doAsUser
					);
			}
			finally
			{
				token.delegationToken = null;
			}
		}
	}
}
