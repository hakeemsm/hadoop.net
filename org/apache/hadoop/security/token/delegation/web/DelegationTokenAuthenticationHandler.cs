using Sharpen;

namespace org.apache.hadoop.security.token.delegation.web
{
	/// <summary>
	/// An
	/// <see cref="org.apache.hadoop.security.authentication.server.AuthenticationHandler
	/// 	"/>
	/// that implements Kerberos SPNEGO mechanism
	/// for HTTP and supports Delegation Token functionality.
	/// <p/>
	/// In addition to the wrapped
	/// <see cref="org.apache.hadoop.security.authentication.server.AuthenticationHandler
	/// 	"/>
	/// configuration
	/// properties, this handler supports the following properties prefixed
	/// with the type of the wrapped <code>AuthenticationHandler</code>:
	/// <ul>
	/// <li>delegation-token.token-kind: the token kind for generated tokens
	/// (no default, required property).</li>
	/// <li>delegation-token.update-interval.sec: secret manager master key
	/// update interval in seconds (default 1 day).</li>
	/// <li>delegation-token.max-lifetime.sec: maximum life of a delegation
	/// token in seconds (default 7 days).</li>
	/// <li>delegation-token.renewal-interval.sec: renewal interval for
	/// delegation tokens in seconds (default 1 day).</li>
	/// <li>delegation-token.removal-scan-interval.sec: delegation tokens
	/// removal scan interval in seconds (default 1 hour).</li>
	/// </ul>
	/// </summary>
	public abstract class DelegationTokenAuthenticationHandler : org.apache.hadoop.security.authentication.server.AuthenticationHandler
	{
		protected internal const string TYPE_POSTFIX = "-dt";

		public const string PREFIX = "delegation-token.";

		public const string TOKEN_KIND = PREFIX + "token-kind";

		private static readonly System.Collections.Generic.ICollection<string> DELEGATION_TOKEN_OPS
			 = new java.util.HashSet<string>();

		public const string DELEGATION_TOKEN_UGI_ATTRIBUTE = "hadoop.security.delegation-token.ugi";

		static DelegationTokenAuthenticationHandler()
		{
			DELEGATION_TOKEN_OPS.add(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
				.GETDELEGATIONTOKEN.ToString());
			DELEGATION_TOKEN_OPS.add(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
				.RENEWDELEGATIONTOKEN.ToString());
			DELEGATION_TOKEN_OPS.add(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
				.CANCELDELEGATIONTOKEN.ToString());
		}

		private org.apache.hadoop.security.authentication.server.AuthenticationHandler authHandler;

		private org.apache.hadoop.security.token.delegation.web.DelegationTokenManager tokenManager;

		private string authType;

		public DelegationTokenAuthenticationHandler(org.apache.hadoop.security.authentication.server.AuthenticationHandler
			 handler)
		{
			authHandler = handler;
			authType = handler.getType();
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual org.apache.hadoop.security.token.delegation.web.DelegationTokenManager
			 getTokenManager()
		{
			return tokenManager;
		}

		/// <exception cref="javax.servlet.ServletException"/>
		public override void init(java.util.Properties config)
		{
			authHandler.init(config);
			initTokenManager(config);
		}

		/// <summary>
		/// Sets an external <code>DelegationTokenSecretManager</code> instance to
		/// manage creation and verification of Delegation Tokens.
		/// </summary>
		/// <remarks>
		/// Sets an external <code>DelegationTokenSecretManager</code> instance to
		/// manage creation and verification of Delegation Tokens.
		/// <p/>
		/// This is useful for use cases where secrets must be shared across multiple
		/// services.
		/// </remarks>
		/// <param name="secretManager">a <code>DelegationTokenSecretManager</code> instance</param>
		public virtual void setExternalDelegationTokenSecretManager(org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager
			 secretManager)
		{
			tokenManager.setExternalDelegationTokenSecretManager(secretManager);
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual void initTokenManager(java.util.Properties config)
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(false);
			foreach (System.Collections.DictionaryEntry entry in config)
			{
				conf.set((string)entry.Key, (string)entry.Value);
			}
			string tokenKind = conf.get(TOKEN_KIND);
			if (tokenKind == null)
			{
				throw new System.ArgumentException("The configuration does not define the token kind"
					);
			}
			tokenKind = tokenKind.Trim();
			tokenManager = new org.apache.hadoop.security.token.delegation.web.DelegationTokenManager
				(conf, new org.apache.hadoop.io.Text(tokenKind));
			tokenManager.init();
		}

		public override void destroy()
		{
			tokenManager.destroy();
			authHandler.destroy();
		}

		public override string getType()
		{
			return authType;
		}

		private static readonly string ENTER = Sharpen.Runtime.getProperty("line.separator"
			);

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		public override bool managementOperation(org.apache.hadoop.security.authentication.server.AuthenticationToken
			 token, javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			bool requestContinues = true;
			string op = org.apache.hadoop.security.token.delegation.web.ServletUtils.getParameter
				(request, org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
				.OP_PARAM);
			op = (op != null) ? org.apache.hadoop.util.StringUtils.toUpperCase(op) : null;
			if (DELEGATION_TOKEN_OPS.contains(op) && !request.getMethod().Equals("OPTIONS"))
			{
				org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
					 dtOp = org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
					.valueOf(op);
				if (dtOp.getHttpMethod().Equals(request.getMethod()))
				{
					bool doManagement;
					if (dtOp.requiresKerberosCredentials() && token == null)
					{
						token = authenticate(request, response);
						if (token == null)
						{
							requestContinues = false;
							doManagement = false;
						}
						else
						{
							doManagement = true;
						}
					}
					else
					{
						doManagement = true;
					}
					if (doManagement)
					{
						org.apache.hadoop.security.UserGroupInformation requestUgi = (token != null) ? org.apache.hadoop.security.UserGroupInformation
							.createRemoteUser(token.getUserName()) : null;
						// Create the proxy user if doAsUser exists
						string doAsUser = org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter
							.getDoAs(request);
						if (requestUgi != null && doAsUser != null)
						{
							requestUgi = org.apache.hadoop.security.UserGroupInformation.createProxyUser(doAsUser
								, requestUgi);
							try
							{
								org.apache.hadoop.security.authorize.ProxyUsers.authorize(requestUgi, request.getRemoteHost
									());
							}
							catch (org.apache.hadoop.security.authorize.AuthorizationException ex)
							{
								org.apache.hadoop.util.HttpExceptionUtils.createServletExceptionResponse(response
									, javax.servlet.http.HttpServletResponse.SC_FORBIDDEN, ex);
								return false;
							}
						}
						System.Collections.IDictionary map = null;
						switch (dtOp)
						{
							case org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
								.GETDELEGATIONTOKEN:
							{
								if (requestUgi == null)
								{
									throw new System.InvalidOperationException("request UGI cannot be NULL");
								}
								string renewer = org.apache.hadoop.security.token.delegation.web.ServletUtils.getParameter
									(request, org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
									.RENEWER_PARAM);
								try
								{
									org.apache.hadoop.security.token.Token<object> dToken = tokenManager.createToken(
										requestUgi, renewer);
									map = delegationTokenToJSON(dToken);
								}
								catch (System.IO.IOException ex)
								{
									throw new org.apache.hadoop.security.authentication.client.AuthenticationException
										(ex.ToString(), ex);
								}
								break;
							}

							case org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
								.RENEWDELEGATIONTOKEN:
							{
								if (requestUgi == null)
								{
									throw new System.InvalidOperationException("request UGI cannot be NULL");
								}
								string tokenToRenew = org.apache.hadoop.security.token.delegation.web.ServletUtils
									.getParameter(request, org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
									.TOKEN_PARAM);
								if (tokenToRenew == null)
								{
									response.sendError(javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST, java.text.MessageFormat
										.format("Operation [{0}] requires the parameter [{1}]", dtOp, org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
										.TOKEN_PARAM));
									requestContinues = false;
								}
								else
								{
									org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
										> dt = new org.apache.hadoop.security.token.Token();
									try
									{
										dt.decodeFromUrlString(tokenToRenew);
										long expirationTime = tokenManager.renewToken(dt, requestUgi.getShortUserName());
										map = new System.Collections.Hashtable();
										map["long"] = expirationTime;
									}
									catch (System.IO.IOException ex)
									{
										throw new org.apache.hadoop.security.authentication.client.AuthenticationException
											(ex.ToString(), ex);
									}
								}
								break;
							}

							case org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation
								.CANCELDELEGATIONTOKEN:
							{
								string tokenToCancel = org.apache.hadoop.security.token.delegation.web.ServletUtils
									.getParameter(request, org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
									.TOKEN_PARAM);
								if (tokenToCancel == null)
								{
									response.sendError(javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST, java.text.MessageFormat
										.format("Operation [{0}] requires the parameter [{1}]", dtOp, org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
										.TOKEN_PARAM));
									requestContinues = false;
								}
								else
								{
									org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
										> dt = new org.apache.hadoop.security.token.Token();
									try
									{
										dt.decodeFromUrlString(tokenToCancel);
										tokenManager.cancelToken(dt, (requestUgi != null) ? requestUgi.getShortUserName()
											 : null);
									}
									catch (System.IO.IOException)
									{
										response.sendError(javax.servlet.http.HttpServletResponse.SC_NOT_FOUND, "Invalid delegation token, cannot cancel"
											);
										requestContinues = false;
									}
								}
								break;
							}
						}
						if (requestContinues)
						{
							response.setStatus(javax.servlet.http.HttpServletResponse.SC_OK);
							if (map != null)
							{
								response.setContentType(javax.ws.rs.core.MediaType.APPLICATION_JSON);
								System.IO.TextWriter writer = response.getWriter();
								org.codehaus.jackson.map.ObjectMapper jsonMapper = new org.codehaus.jackson.map.ObjectMapper
									();
								jsonMapper.writeValue(writer, map);
								writer.write(ENTER);
								writer.flush();
							}
							requestContinues = false;
						}
					}
				}
				else
				{
					response.sendError(javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST, java.text.MessageFormat
						.format("Wrong HTTP method [{0}] for operation [{1}], it should be " + "[{2}]", 
						request.getMethod(), dtOp, dtOp.getHttpMethod()));
					requestContinues = false;
				}
			}
			return requestContinues;
		}

		/// <exception cref="System.IO.IOException"/>
		private static System.Collections.IDictionary delegationTokenToJSON(org.apache.hadoop.security.token.Token
			 token)
		{
			System.Collections.IDictionary json = new java.util.LinkedHashMap();
			json[org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
				.DELEGATION_TOKEN_URL_STRING_JSON] = token.encodeToUrlString();
			System.Collections.IDictionary response = new java.util.LinkedHashMap();
			response[org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
				.DELEGATION_TOKEN_JSON] = json;
			return response;
		}

		/// <summary>
		/// Authenticates a request looking for the <code>delegation</code>
		/// query-string parameter and verifying it is a valid token.
		/// </summary>
		/// <remarks>
		/// Authenticates a request looking for the <code>delegation</code>
		/// query-string parameter and verifying it is a valid token. If there is not
		/// <code>delegation</code> query-string parameter, it delegates the
		/// authentication to the
		/// <see cref="org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
		/// 	"/>
		/// unless it is
		/// disabled.
		/// </remarks>
		/// <param name="request">the HTTP client request.</param>
		/// <param name="response">the HTTP client response.</param>
		/// <returns>the authentication token for the authenticated request.</returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">thrown if the authentication failed.</exception>
		public override org.apache.hadoop.security.authentication.server.AuthenticationToken
			 authenticate(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			org.apache.hadoop.security.authentication.server.AuthenticationToken token;
			string delegationParam = getDelegationToken(request);
			if (delegationParam != null)
			{
				try
				{
					org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
						> dt = new org.apache.hadoop.security.token.Token();
					dt.decodeFromUrlString(delegationParam);
					org.apache.hadoop.security.UserGroupInformation ugi = tokenManager.verifyToken(dt
						);
					string shortName = ugi.getShortUserName();
					// creating a ephemeral token
					token = new org.apache.hadoop.security.authentication.server.AuthenticationToken(
						shortName, ugi.getUserName(), getType());
					token.setExpires(0);
					request.setAttribute(DELEGATION_TOKEN_UGI_ATTRIBUTE, ugi);
				}
				catch (System.Exception ex)
				{
					token = null;
					org.apache.hadoop.util.HttpExceptionUtils.createServletExceptionResponse(response
						, javax.servlet.http.HttpServletResponse.SC_FORBIDDEN, new org.apache.hadoop.security.authentication.client.AuthenticationException
						(ex));
				}
			}
			else
			{
				token = authHandler.authenticate(request, response);
			}
			return token;
		}

		/// <exception cref="System.IO.IOException"/>
		private string getDelegationToken(javax.servlet.http.HttpServletRequest request)
		{
			string dToken = request.getHeader(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator
				.DELEGATION_TOKEN_HEADER);
			if (dToken == null)
			{
				dToken = org.apache.hadoop.security.token.delegation.web.ServletUtils.getParameter
					(request, org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator
					.DELEGATION_PARAM);
			}
			return dToken;
		}
	}
}
