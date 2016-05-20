using Sharpen;

namespace org.apache.hadoop.security.token.delegation.web
{
	/// <summary>
	/// <see cref="org.apache.hadoop.security.authentication.client.Authenticator"/>
	/// wrapper that enhances an
	/// <see cref="org.apache.hadoop.security.authentication.client.Authenticator"/>
	/// with
	/// Delegation Token support.
	/// </summary>
	public abstract class DelegationTokenAuthenticator : org.apache.hadoop.security.authentication.client.Authenticator
	{
		private static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator
			)));

		private const string CONTENT_TYPE = "Content-Type";

		private const string APPLICATION_JSON_MIME = "application/json";

		private const string HTTP_GET = "GET";

		private const string HTTP_PUT = "PUT";

		public const string OP_PARAM = "op";

		public const string DELEGATION_TOKEN_HEADER = "X-Hadoop-Delegation-Token";

		public const string DELEGATION_PARAM = "delegation";

		public const string TOKEN_PARAM = "token";

		public const string RENEWER_PARAM = "renewer";

		public const string DELEGATION_TOKEN_JSON = "Token";

		public const string DELEGATION_TOKEN_URL_STRING_JSON = "urlString";

		public const string RENEW_DELEGATION_TOKEN_JSON = "long";

		/// <summary>DelegationToken operations.</summary>
		[System.Serializable]
		public sealed class DelegationTokenOperation
		{
			public static readonly org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
				 GETDELEGATIONTOKEN = new org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
				(HTTP_GET, true);

			public static readonly org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
				 RENEWDELEGATIONTOKEN = new org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
				(HTTP_PUT, true);

			public static readonly org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
				 CANCELDELEGATIONTOKEN = new org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
				(HTTP_PUT, false);

			private string httpMethod;

			private bool requiresKerberosCredentials;

			private DelegationTokenOperation(string httpMethod, bool requiresKerberosCredentials
				)
			{
				this.httpMethod = httpMethod;
				this.requiresKerberosCredentials = requiresKerberosCredentials;
			}

			public string getHttpMethod()
			{
				return org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
					.httpMethod;
			}

			public bool requiresKerberosCredentials()
			{
				return org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
					.requiresKerberosCredentials;
			}
		}

		private org.apache.hadoop.security.authentication.client.Authenticator authenticator;

		private org.apache.hadoop.security.authentication.client.ConnectionConfigurator connConfigurator;

		public DelegationTokenAuthenticator(org.apache.hadoop.security.authentication.client.Authenticator
			 authenticator)
		{
			this.authenticator = authenticator;
		}

		public virtual void setConnectionConfigurator(org.apache.hadoop.security.authentication.client.ConnectionConfigurator
			 configurator)
		{
			authenticator.setConnectionConfigurator(configurator);
			connConfigurator = configurator;
		}

		private bool hasDelegationToken(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token)
		{
			bool hasDt = false;
			if (token is org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token)
			{
				hasDt = ((org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
					)token).getDelegationToken() != null;
			}
			if (!hasDt)
			{
				string queryStr = url.getQuery();
				hasDt = (queryStr != null) && queryStr.contains(DELEGATION_PARAM + "=");
			}
			return hasDt;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		public virtual void authenticate(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token)
		{
			if (!hasDelegationToken(url, token))
			{
				authenticator.authenticate(url, token);
			}
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
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
			> getDelegationToken(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
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
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
			> getDelegationToken(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token, string renewer, string doAsUser)
		{
			System.Collections.IDictionary json = doDelegationTokenOperation(url, token, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
				.GETDELEGATIONTOKEN, renewer, null, true, doAsUser);
			json = (System.Collections.IDictionary)json[DELEGATION_TOKEN_JSON];
			string tokenStr = (string)json[DELEGATION_TOKEN_URL_STRING_JSON];
			org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
				> dToken = new org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
				>();
			dToken.decodeFromUrlString(tokenStr);
			java.net.InetSocketAddress service = new java.net.InetSocketAddress(url.getHost()
				, url.getPort());
			org.apache.hadoop.security.SecurityUtil.setTokenService(dToken, service);
			return dToken;
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
		public virtual long renewDelegationToken(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token, org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
			> dToken)
		{
			return renewDelegationToken(url, token, dToken, null);
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
		public virtual long renewDelegationToken(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token, org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
			> dToken, string doAsUser)
		{
			System.Collections.IDictionary json = doDelegationTokenOperation(url, token, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
				.RENEWDELEGATIONTOKEN, null, dToken, true, doAsUser);
			return (long)json[RENEW_DELEGATION_TOKEN_JSON];
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
		public virtual void cancelDelegationToken(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token, org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
			> dToken)
		{
			cancelDelegationToken(url, token, dToken, null);
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
		public virtual void cancelDelegationToken(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token, org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
			> dToken, string doAsUser)
		{
			try
			{
				doDelegationTokenOperation(url, token, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
					.CANCELDELEGATIONTOKEN, null, dToken, false, doAsUser);
			}
			catch (org.apache.hadoop.security.authentication.client.AuthenticationException ex
				)
			{
				throw new System.IO.IOException("This should not happen: " + ex.Message, ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		private System.Collections.IDictionary doDelegationTokenOperation<_T0>(java.net.URL
			 url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token token
			, org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
			 operation, string renewer, org.apache.hadoop.security.token.Token<_T0> dToken, 
			bool hasResponse, string doAsUser)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier
		{
			System.Collections.IDictionary ret = null;
			System.Collections.Generic.IDictionary<string, string> @params = new System.Collections.Generic.Dictionary
				<string, string>();
			@params[OP_PARAM] = operation.ToString();
			if (renewer != null)
			{
				@params[RENEWER_PARAM] = renewer;
			}
			if (dToken != null)
			{
				@params[TOKEN_PARAM] = dToken.encodeToUrlString();
			}
			// proxyuser
			if (doAsUser != null)
			{
				@params[org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL
					.DO_AS] = java.net.URLEncoder.encode(doAsUser, "UTF-8");
			}
			string urlStr = url.toExternalForm();
			java.lang.StringBuilder sb = new java.lang.StringBuilder(urlStr);
			string separator = (urlStr.contains("?")) ? "&" : "?";
			foreach (System.Collections.Generic.KeyValuePair<string, string> entry in @params)
			{
				sb.Append(separator).Append(entry.Key).Append("=").Append(java.net.URLEncoder.encode
					(entry.Value, "UTF8"));
				separator = "&";
			}
			url = new java.net.URL(sb.ToString());
			org.apache.hadoop.security.authentication.client.AuthenticatedURL aUrl = new org.apache.hadoop.security.authentication.client.AuthenticatedURL
				(this, connConfigurator);
			java.net.HttpURLConnection conn = aUrl.openConnection(url, token);
			conn.setRequestMethod(operation.getHttpMethod());
			org.apache.hadoop.util.HttpExceptionUtils.validateResponse(conn, java.net.HttpURLConnection
				.HTTP_OK);
			if (hasResponse)
			{
				string contentType = conn.getHeaderField(CONTENT_TYPE);
				contentType = (contentType != null) ? org.apache.hadoop.util.StringUtils.toLowerCase
					(contentType) : null;
				if (contentType != null && contentType.contains(APPLICATION_JSON_MIME))
				{
					try
					{
						org.codehaus.jackson.map.ObjectMapper mapper = new org.codehaus.jackson.map.ObjectMapper
							();
						ret = mapper.readValue<System.Collections.IDictionary>(conn.getInputStream());
					}
					catch (System.Exception ex)
					{
						throw new org.apache.hadoop.security.authentication.client.AuthenticationException
							(string.format("'%s' did not handle the '%s' delegation token operation: %s", url
							.getAuthority(), operation, ex.Message), ex);
					}
				}
				else
				{
					throw new org.apache.hadoop.security.authentication.client.AuthenticationException
						(string.format("'%s' did not " + "respond with JSON to the '%s' delegation token operation"
						, url.getAuthority(), operation));
				}
			}
			return ret;
		}
	}
}
