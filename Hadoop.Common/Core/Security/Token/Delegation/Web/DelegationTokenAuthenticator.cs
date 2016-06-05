using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Org.Codehaus.Jackson.Map;
using Org.Slf4j;


namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	/// <summary>
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Client.Authenticator"/>
	/// wrapper that enhances an
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Client.Authenticator"/>
	/// with
	/// Delegation Token support.
	/// </summary>
	public abstract class DelegationTokenAuthenticator : Authenticator
	{
		private static Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Security.Token.Delegation.Web.DelegationTokenAuthenticator
			));

		private const string ContentType = "Content-Type";

		private const string ApplicationJsonMime = "application/json";

		private const string HttpGet = "GET";

		private const string HttpPut = "PUT";

		public const string OpParam = "op";

		public const string DelegationTokenHeader = "X-Hadoop-Delegation-Token";

		public const string DelegationParam = "delegation";

		public const string TokenParam = "token";

		public const string RenewerParam = "renewer";

		public const string DelegationTokenJson = "Token";

		public const string DelegationTokenUrlStringJson = "urlString";

		public const string RenewDelegationTokenJson = "long";

		/// <summary>DelegationToken operations.</summary>
		[System.Serializable]
		public sealed class DelegationTokenOperation
		{
			public static readonly DelegationTokenAuthenticator.DelegationTokenOperation Getdelegationtoken
				 = new DelegationTokenAuthenticator.DelegationTokenOperation(HttpGet, true);

			public static readonly DelegationTokenAuthenticator.DelegationTokenOperation Renewdelegationtoken
				 = new DelegationTokenAuthenticator.DelegationTokenOperation(HttpPut, true);

			public static readonly DelegationTokenAuthenticator.DelegationTokenOperation Canceldelegationtoken
				 = new DelegationTokenAuthenticator.DelegationTokenOperation(HttpPut, false);

			private string httpMethod;

			private bool requiresKerberosCredentials;

			private DelegationTokenOperation(string httpMethod, bool requiresKerberosCredentials
				)
			{
				this.httpMethod = httpMethod;
				this.requiresKerberosCredentials = requiresKerberosCredentials;
			}

			public string GetHttpMethod()
			{
				return DelegationTokenAuthenticator.DelegationTokenOperation.httpMethod;
			}

			public bool RequiresKerberosCredentials()
			{
				return DelegationTokenAuthenticator.DelegationTokenOperation.requiresKerberosCredentials;
			}
		}

		private Authenticator authenticator;

		private ConnectionConfigurator connConfigurator;

		public DelegationTokenAuthenticator(Authenticator authenticator)
		{
			this.authenticator = authenticator;
		}

		public virtual void SetConnectionConfigurator(ConnectionConfigurator configurator
			)
		{
			authenticator.SetConnectionConfigurator(configurator);
			connConfigurator = configurator;
		}

		private bool HasDelegationToken(Uri url, AuthenticatedURL.Token token)
		{
			bool hasDt = false;
			if (token is DelegationTokenAuthenticatedURL.Token)
			{
				hasDt = ((DelegationTokenAuthenticatedURL.Token)token).GetDelegationToken() != null;
			}
			if (!hasDt)
			{
				string queryStr = url.GetQuery();
				hasDt = (queryStr != null) && queryStr.Contains(DelegationParam + "=");
			}
			return hasDt;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		public virtual void Authenticate(Uri url, AuthenticatedURL.Token token)
		{
			if (!HasDelegationToken(url, token))
			{
				authenticator.Authenticate(url, token);
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier
			> GetDelegationToken(Uri url, AuthenticatedURL.Token token, string renewer)
		{
			return GetDelegationToken(url, token, renewer, null);
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier
			> GetDelegationToken(Uri url, AuthenticatedURL.Token token, string renewer, string
			 doAsUser)
		{
			IDictionary json = DoDelegationTokenOperation(url, token, DelegationTokenAuthenticator.DelegationTokenOperation
				.Getdelegationtoken, renewer, null, true, doAsUser);
			json = (IDictionary)json[DelegationTokenJson];
			string tokenStr = (string)json[DelegationTokenUrlStringJson];
			Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier> dToken = 
				new Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier>();
			dToken.DecodeFromUrlString(tokenStr);
			IPEndPoint service = new IPEndPoint(url.GetHost(), url.Port);
			SecurityUtil.SetTokenService(dToken, service);
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual long RenewDelegationToken(Uri url, AuthenticatedURL.Token token, Org.Apache.Hadoop.Security.Token.Token
			<AbstractDelegationTokenIdentifier> dToken)
		{
			return RenewDelegationToken(url, token, dToken, null);
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual long RenewDelegationToken(Uri url, AuthenticatedURL.Token token, Org.Apache.Hadoop.Security.Token.Token
			<AbstractDelegationTokenIdentifier> dToken, string doAsUser)
		{
			IDictionary json = DoDelegationTokenOperation(url, token, DelegationTokenAuthenticator.DelegationTokenOperation
				.Renewdelegationtoken, null, dToken, true, doAsUser);
			return (long)json[RenewDelegationTokenJson];
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
		public virtual void CancelDelegationToken(Uri url, AuthenticatedURL.Token token, 
			Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier> dToken
			)
		{
			CancelDelegationToken(url, token, dToken, null);
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
		public virtual void CancelDelegationToken(Uri url, AuthenticatedURL.Token token, 
			Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier> dToken
			, string doAsUser)
		{
			try
			{
				DoDelegationTokenOperation(url, token, DelegationTokenAuthenticator.DelegationTokenOperation
					.Canceldelegationtoken, null, dToken, false, doAsUser);
			}
			catch (AuthenticationException ex)
			{
				throw new IOException("This should not happen: " + ex.Message, ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		private IDictionary DoDelegationTokenOperation<_T0>(Uri url, AuthenticatedURL.Token
			 token, DelegationTokenAuthenticator.DelegationTokenOperation operation, string 
			renewer, Org.Apache.Hadoop.Security.Token.Token<_T0> dToken, bool hasResponse, string
			 doAsUser)
			where _T0 : TokenIdentifier
		{
			IDictionary ret = null;
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = operation.ToString();
			if (renewer != null)
			{
				@params[RenewerParam] = renewer;
			}
			if (dToken != null)
			{
				@params[TokenParam] = dToken.EncodeToUrlString();
			}
			// proxyuser
			if (doAsUser != null)
			{
				@params[DelegationTokenAuthenticatedURL.DoAs] = URLEncoder.Encode(doAsUser, "UTF-8"
					);
			}
			string urlStr = url.ToExternalForm();
			StringBuilder sb = new StringBuilder(urlStr);
			string separator = (urlStr.Contains("?")) ? "&" : "?";
			foreach (KeyValuePair<string, string> entry in @params)
			{
				sb.Append(separator).Append(entry.Key).Append("=").Append(URLEncoder.Encode(entry
					.Value, "UTF8"));
				separator = "&";
			}
			url = new Uri(sb.ToString());
			AuthenticatedURL aUrl = new AuthenticatedURL(this, connConfigurator);
			HttpURLConnection conn = aUrl.OpenConnection(url, token);
			conn.SetRequestMethod(operation.GetHttpMethod());
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			if (hasResponse)
			{
				string contentType = conn.GetHeaderField(ContentType);
				contentType = (contentType != null) ? StringUtils.ToLowerCase(contentType) : null;
				if (contentType != null && contentType.Contains(ApplicationJsonMime))
				{
					try
					{
						ObjectMapper mapper = new ObjectMapper();
						ret = mapper.ReadValue<IDictionary>(conn.GetInputStream());
					}
					catch (Exception ex)
					{
						throw new AuthenticationException(string.Format("'%s' did not handle the '%s' delegation token operation: %s"
							, url.GetAuthority(), operation, ex.Message), ex);
					}
				}
				else
				{
					throw new AuthenticationException(string.Format("'%s' did not " + "respond with JSON to the '%s' delegation token operation"
						, url.GetAuthority(), operation));
				}
			}
			return ret;
		}
	}
}
