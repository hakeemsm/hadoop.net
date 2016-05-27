using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Javax.Servlet.Http;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationHandler
	/// 	"/>
	/// that implements Kerberos SPNEGO mechanism
	/// for HTTP and supports Delegation Token functionality.
	/// <p/>
	/// In addition to the wrapped
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationHandler
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
	public abstract class DelegationTokenAuthenticationHandler : AuthenticationHandler
	{
		protected internal const string TypePostfix = "-dt";

		public const string Prefix = "delegation-token.";

		public const string TokenKind = Prefix + "token-kind";

		private static readonly ICollection<string> DelegationTokenOps = new HashSet<string
			>();

		public const string DelegationTokenUgiAttribute = "hadoop.security.delegation-token.ugi";

		static DelegationTokenAuthenticationHandler()
		{
			DelegationTokenOps.AddItem(DelegationTokenAuthenticator.DelegationTokenOperation.
				Getdelegationtoken.ToString());
			DelegationTokenOps.AddItem(DelegationTokenAuthenticator.DelegationTokenOperation.
				Renewdelegationtoken.ToString());
			DelegationTokenOps.AddItem(DelegationTokenAuthenticator.DelegationTokenOperation.
				Canceldelegationtoken.ToString());
		}

		private AuthenticationHandler authHandler;

		private DelegationTokenManager tokenManager;

		private string authType;

		public DelegationTokenAuthenticationHandler(AuthenticationHandler handler)
		{
			authHandler = handler;
			authType = handler.GetType();
		}

		[VisibleForTesting]
		internal virtual DelegationTokenManager GetTokenManager()
		{
			return tokenManager;
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		public override void Init(Properties config)
		{
			authHandler.Init(config);
			InitTokenManager(config);
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
		public virtual void SetExternalDelegationTokenSecretManager(AbstractDelegationTokenSecretManager
			 secretManager)
		{
			tokenManager.SetExternalDelegationTokenSecretManager(secretManager);
		}

		[VisibleForTesting]
		public virtual void InitTokenManager(Properties config)
		{
			Configuration conf = new Configuration(false);
			foreach (DictionaryEntry entry in config)
			{
				conf.Set((string)entry.Key, (string)entry.Value);
			}
			string tokenKind = conf.Get(TokenKind);
			if (tokenKind == null)
			{
				throw new ArgumentException("The configuration does not define the token kind");
			}
			tokenKind = tokenKind.Trim();
			tokenManager = new DelegationTokenManager(conf, new Text(tokenKind));
			tokenManager.Init();
		}

		public override void Destroy()
		{
			tokenManager.Destroy();
			authHandler.Destroy();
		}

		public override string GetType()
		{
			return authType;
		}

		private static readonly string Enter = Runtime.GetProperty("line.separator");

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		public override bool ManagementOperation(AuthenticationToken token, HttpServletRequest
			 request, HttpServletResponse response)
		{
			bool requestContinues = true;
			string op = ServletUtils.GetParameter(request, KerberosDelegationTokenAuthenticator
				.OpParam);
			op = (op != null) ? StringUtils.ToUpperCase(op) : null;
			if (DelegationTokenOps.Contains(op) && !request.GetMethod().Equals("OPTIONS"))
			{
				DelegationTokenAuthenticator.DelegationTokenOperation dtOp = DelegationTokenAuthenticator.DelegationTokenOperation
					.ValueOf(op);
				if (dtOp.GetHttpMethod().Equals(request.GetMethod()))
				{
					bool doManagement;
					if (dtOp.RequiresKerberosCredentials() && token == null)
					{
						token = Authenticate(request, response);
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
						UserGroupInformation requestUgi = (token != null) ? UserGroupInformation.CreateRemoteUser
							(token.GetUserName()) : null;
						// Create the proxy user if doAsUser exists
						string doAsUser = DelegationTokenAuthenticationFilter.GetDoAs(request);
						if (requestUgi != null && doAsUser != null)
						{
							requestUgi = UserGroupInformation.CreateProxyUser(doAsUser, requestUgi);
							try
							{
								ProxyUsers.Authorize(requestUgi, request.GetRemoteHost());
							}
							catch (AuthorizationException ex)
							{
								HttpExceptionUtils.CreateServletExceptionResponse(response, HttpServletResponse.ScForbidden
									, ex);
								return false;
							}
						}
						IDictionary map = null;
						switch (dtOp)
						{
							case DelegationTokenAuthenticator.DelegationTokenOperation.Getdelegationtoken:
							{
								if (requestUgi == null)
								{
									throw new InvalidOperationException("request UGI cannot be NULL");
								}
								string renewer = ServletUtils.GetParameter(request, KerberosDelegationTokenAuthenticator
									.RenewerParam);
								try
								{
									Org.Apache.Hadoop.Security.Token.Token<object> dToken = tokenManager.CreateToken(
										requestUgi, renewer);
									map = DelegationTokenToJSON(dToken);
								}
								catch (IOException ex)
								{
									throw new AuthenticationException(ex.ToString(), ex);
								}
								break;
							}

							case DelegationTokenAuthenticator.DelegationTokenOperation.Renewdelegationtoken:
							{
								if (requestUgi == null)
								{
									throw new InvalidOperationException("request UGI cannot be NULL");
								}
								string tokenToRenew = ServletUtils.GetParameter(request, KerberosDelegationTokenAuthenticator
									.TokenParam);
								if (tokenToRenew == null)
								{
									response.SendError(HttpServletResponse.ScBadRequest, MessageFormat.Format("Operation [{0}] requires the parameter [{1}]"
										, dtOp, KerberosDelegationTokenAuthenticator.TokenParam));
									requestContinues = false;
								}
								else
								{
									Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier> dt = new 
										Org.Apache.Hadoop.Security.Token.Token();
									try
									{
										dt.DecodeFromUrlString(tokenToRenew);
										long expirationTime = tokenManager.RenewToken(dt, requestUgi.GetShortUserName());
										map = new Hashtable();
										map["long"] = expirationTime;
									}
									catch (IOException ex)
									{
										throw new AuthenticationException(ex.ToString(), ex);
									}
								}
								break;
							}

							case DelegationTokenAuthenticator.DelegationTokenOperation.Canceldelegationtoken:
							{
								string tokenToCancel = ServletUtils.GetParameter(request, KerberosDelegationTokenAuthenticator
									.TokenParam);
								if (tokenToCancel == null)
								{
									response.SendError(HttpServletResponse.ScBadRequest, MessageFormat.Format("Operation [{0}] requires the parameter [{1}]"
										, dtOp, KerberosDelegationTokenAuthenticator.TokenParam));
									requestContinues = false;
								}
								else
								{
									Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier> dt = new 
										Org.Apache.Hadoop.Security.Token.Token();
									try
									{
										dt.DecodeFromUrlString(tokenToCancel);
										tokenManager.CancelToken(dt, (requestUgi != null) ? requestUgi.GetShortUserName()
											 : null);
									}
									catch (IOException)
									{
										response.SendError(HttpServletResponse.ScNotFound, "Invalid delegation token, cannot cancel"
											);
										requestContinues = false;
									}
								}
								break;
							}
						}
						if (requestContinues)
						{
							response.SetStatus(HttpServletResponse.ScOk);
							if (map != null)
							{
								response.SetContentType(MediaType.ApplicationJson);
								TextWriter writer = response.GetWriter();
								ObjectMapper jsonMapper = new ObjectMapper();
								jsonMapper.WriteValue(writer, map);
								writer.Write(Enter);
								writer.Flush();
							}
							requestContinues = false;
						}
					}
				}
				else
				{
					response.SendError(HttpServletResponse.ScBadRequest, MessageFormat.Format("Wrong HTTP method [{0}] for operation [{1}], it should be "
						 + "[{2}]", request.GetMethod(), dtOp, dtOp.GetHttpMethod()));
					requestContinues = false;
				}
			}
			return requestContinues;
		}

		/// <exception cref="System.IO.IOException"/>
		private static IDictionary DelegationTokenToJSON(Org.Apache.Hadoop.Security.Token.Token
			 token)
		{
			IDictionary json = new LinkedHashMap();
			json[KerberosDelegationTokenAuthenticator.DelegationTokenUrlStringJson] = token.EncodeToUrlString
				();
			IDictionary response = new LinkedHashMap();
			response[KerberosDelegationTokenAuthenticator.DelegationTokenJson] = json;
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
		/// <see cref="Org.Apache.Hadoop.Security.Authentication.Server.KerberosAuthenticationHandler
		/// 	"/>
		/// unless it is
		/// disabled.
		/// </remarks>
		/// <param name="request">the HTTP client request.</param>
		/// <param name="response">the HTTP client response.</param>
		/// <returns>the authentication token for the authenticated request.</returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">thrown if the authentication failed.</exception>
		public override AuthenticationToken Authenticate(HttpServletRequest request, HttpServletResponse
			 response)
		{
			AuthenticationToken token;
			string delegationParam = GetDelegationToken(request);
			if (delegationParam != null)
			{
				try
				{
					Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier> dt = new 
						Org.Apache.Hadoop.Security.Token.Token();
					dt.DecodeFromUrlString(delegationParam);
					UserGroupInformation ugi = tokenManager.VerifyToken(dt);
					string shortName = ugi.GetShortUserName();
					// creating a ephemeral token
					token = new AuthenticationToken(shortName, ugi.GetUserName(), GetType());
					token.SetExpires(0);
					request.SetAttribute(DelegationTokenUgiAttribute, ugi);
				}
				catch (Exception ex)
				{
					token = null;
					HttpExceptionUtils.CreateServletExceptionResponse(response, HttpServletResponse.ScForbidden
						, new AuthenticationException(ex));
				}
			}
			else
			{
				token = authHandler.Authenticate(request, response);
			}
			return token;
		}

		/// <exception cref="System.IO.IOException"/>
		private string GetDelegationToken(HttpServletRequest request)
		{
			string dToken = request.GetHeader(DelegationTokenAuthenticator.DelegationTokenHeader
				);
			if (dToken == null)
			{
				dToken = ServletUtils.GetParameter(request, KerberosDelegationTokenAuthenticator.
					DelegationParam);
			}
			return dToken;
		}
	}
}
