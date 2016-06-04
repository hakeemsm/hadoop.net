using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Base;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	/// <summary>
	/// The <code>DelegationTokenAuthenticatedURL</code> is a
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticatedURL"/>
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
	public class DelegationTokenAuthenticatedURL : AuthenticatedURL
	{
		/// <summary>
		/// Constant used in URL's query string to perform a proxy user request, the
		/// value of the <code>DO_AS</code> parameter is the user the request will be
		/// done on behalf of.
		/// </summary>
		internal const string DoAs = "doAs";

		/// <summary>Client side authentication token that handles Delegation Tokens.</summary>
		public class Token : AuthenticatedURL.Token
		{
			private Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier>
				 delegationToken;

			public virtual Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier
				> GetDelegationToken()
			{
				return delegationToken;
			}

			public virtual void SetDelegationToken(Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier
				> delegationToken)
			{
				this.delegationToken = delegationToken;
			}
		}

		private static Type DefaultAuthenticator = typeof(KerberosDelegationTokenAuthenticator
			);

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
		public static void SetDefaultDelegationTokenAuthenticator(Type authenticator)
		{
			DefaultAuthenticator = authenticator;
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
		public static Type GetDefaultDelegationTokenAuthenticator()
		{
			return DefaultAuthenticator;
		}

		private static DelegationTokenAuthenticator ObtainDelegationTokenAuthenticator(DelegationTokenAuthenticator
			 dta, ConnectionConfigurator connConfigurator)
		{
			try
			{
				if (dta == null)
				{
					dta = System.Activator.CreateInstance(DefaultAuthenticator);
					dta.SetConnectionConfigurator(connConfigurator);
				}
				return dta;
			}
			catch (Exception ex)
			{
				throw new ArgumentException(ex);
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
		public DelegationTokenAuthenticatedURL(DelegationTokenAuthenticator authenticator
			)
			: this(authenticator, null)
		{
		}

		/// <summary>
		/// Creates an <code>DelegationTokenAuthenticatedURL</code> using the default
		/// <see cref="DelegationTokenAuthenticator"/>
		/// class.
		/// </summary>
		/// <param name="connConfigurator">a connection configurator.</param>
		public DelegationTokenAuthenticatedURL(ConnectionConfigurator connConfigurator)
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
		public DelegationTokenAuthenticatedURL(DelegationTokenAuthenticator authenticator
			, ConnectionConfigurator connConfigurator)
			: base(ObtainDelegationTokenAuthenticator(authenticator, connConfigurator), connConfigurator
				)
		{
		}

		/// <summary>Sets if delegation token should be transmitted in the URL query string.</summary>
		/// <remarks>
		/// Sets if delegation token should be transmitted in the URL query string.
		/// By default it is transmitted using the
		/// <see cref="DelegationTokenAuthenticator.DelegationTokenHeader"/>
		/// HTTP header.
		/// <p/>
		/// This method is provided to enable WebHDFS backwards compatibility.
		/// </remarks>
		/// <param name="useQueryString">
		/// <code>TRUE</code> if the token is transmitted in the
		/// URL query string, <code>FALSE</code> if the delegation token is transmitted
		/// using the
		/// <see cref="DelegationTokenAuthenticator.DelegationTokenHeader"/>
		/// HTTP
		/// header.
		/// </param>
		[Obsolete]
		protected internal virtual void SetUseQueryStringForDelegationToken(bool useQueryString
			)
		{
			useQueryStringforDelegationToken = useQueryString;
		}

		/// <summary>Returns if delegation token is transmitted as a HTTP header.</summary>
		/// <returns>
		/// <code>TRUE</code> if the token is transmitted in the URL query
		/// string, <code>FALSE</code> if the delegation token is transmitted using the
		/// <see cref="DelegationTokenAuthenticator.DelegationTokenHeader"/>
		/// HTTP header.
		/// </returns>
		public virtual bool UseQueryStringForDelegationToken()
		{
			return useQueryStringforDelegationToken;
		}

		/// <summary>
		/// Returns an authenticated
		/// <see cref="Sharpen.HttpURLConnection"/>
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
		/// <see cref="Sharpen.HttpURLConnection"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public override HttpURLConnection OpenConnection(Uri url, AuthenticatedURL.Token 
			token)
		{
			return (token is DelegationTokenAuthenticatedURL.Token) ? OpenConnection(url, (DelegationTokenAuthenticatedURL.Token
				)token) : base.OpenConnection(url, token);
		}

		/// <summary>
		/// Returns an authenticated
		/// <see cref="Sharpen.HttpURLConnection"/>
		/// . If the Delegation
		/// Token is present, it will be used taking precedence over the configured
		/// <code>Authenticator</code>.
		/// </summary>
		/// <param name="url">the URL to connect to. Only HTTP/S URLs are supported.</param>
		/// <param name="token">the authentication token being used for the user.</param>
		/// <returns>
		/// an authenticated
		/// <see cref="Sharpen.HttpURLConnection"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual HttpURLConnection OpenConnection(Uri url, DelegationTokenAuthenticatedURL.Token
			 token)
		{
			return OpenConnection(url, token, null);
		}

		/// <exception cref="System.IO.IOException"/>
		private Uri AugmentURL(Uri url, IDictionary<string, string> @params)
		{
			if (@params != null && @params.Count > 0)
			{
				string urlStr = url.ToExternalForm();
				StringBuilder sb = new StringBuilder(urlStr);
				string separator = (urlStr.Contains("?")) ? "&" : "?";
				foreach (KeyValuePair<string, string> param in @params)
				{
					sb.Append(separator).Append(param.Key).Append("=").Append(param.Value);
					separator = "&";
				}
				url = new Uri(sb.ToString());
			}
			return url;
		}

		/// <summary>
		/// Returns an authenticated
		/// <see cref="Sharpen.HttpURLConnection"/>
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
		/// <see cref="Sharpen.HttpURLConnection"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual HttpURLConnection OpenConnection(Uri url, DelegationTokenAuthenticatedURL.Token
			 token, string doAs)
		{
			Preconditions.CheckNotNull(url, "url");
			Preconditions.CheckNotNull(token, "token");
			IDictionary<string, string> extraParams = new Dictionary<string, string>();
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> dToken = null;
			// if we have valid auth token, it takes precedence over a delegation token
			// and we don't even look for one.
			if (!token.IsSet())
			{
				// delegation token
				Credentials creds = UserGroupInformation.GetCurrentUser().GetCredentials();
				if (!creds.GetAllTokens().IsEmpty())
				{
					IPEndPoint serviceAddr = new IPEndPoint(url.GetHost(), url.Port);
					Text service = SecurityUtil.BuildTokenService(serviceAddr);
					dToken = creds.GetToken(service);
					if (dToken != null)
					{
						if (UseQueryStringForDelegationToken())
						{
							// delegation token will go in the query string, injecting it
							extraParams[KerberosDelegationTokenAuthenticator.DelegationParam] = dToken.EncodeToUrlString
								();
						}
						else
						{
							// delegation token will go as request header, setting it in the
							// auth-token to ensure no authentication handshake is triggered
							// (if we have a delegation token, we are authenticated)
							// the delegation token header is injected in the connection request
							// at the end of this method.
							token.delegationToken = (Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier
								>)dToken;
						}
					}
				}
			}
			// proxyuser
			if (doAs != null)
			{
				extraParams[DoAs] = URLEncoder.Encode(doAs, "UTF-8");
			}
			url = AugmentURL(url, extraParams);
			HttpURLConnection conn = base.OpenConnection(url, token);
			if (!token.IsSet() && !UseQueryStringForDelegationToken() && dToken != null)
			{
				// injecting the delegation token header in the connection request
				conn.SetRequestProperty(DelegationTokenAuthenticator.DelegationTokenHeader, dToken
					.EncodeToUrlString());
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier
			> GetDelegationToken(Uri url, DelegationTokenAuthenticatedURL.Token token, string
			 renewer)
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
		/// <returns>a delegation token.</returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier
			> GetDelegationToken(Uri url, DelegationTokenAuthenticatedURL.Token token, string
			 renewer, string doAsUser)
		{
			Preconditions.CheckNotNull(url, "url");
			Preconditions.CheckNotNull(token, "token");
			try
			{
				token.delegationToken = ((KerberosDelegationTokenAuthenticator)GetAuthenticator()
					).GetDelegationToken(url, token, renewer, doAsUser);
				return token.delegationToken;
			}
			catch (IOException ex)
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">if an authentication exception occurred.</exception>
		public virtual long RenewDelegationToken(Uri url, DelegationTokenAuthenticatedURL.Token
			 token)
		{
			return RenewDelegationToken(url, token, null);
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
		public virtual long RenewDelegationToken(Uri url, DelegationTokenAuthenticatedURL.Token
			 token, string doAsUser)
		{
			Preconditions.CheckNotNull(url, "url");
			Preconditions.CheckNotNull(token, "token");
			Preconditions.CheckNotNull(token.delegationToken, "No delegation token available"
				);
			try
			{
				return ((KerberosDelegationTokenAuthenticator)GetAuthenticator()).RenewDelegationToken
					(url, token, token.delegationToken, doAsUser);
			}
			catch (IOException ex)
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
		public virtual void CancelDelegationToken(Uri url, DelegationTokenAuthenticatedURL.Token
			 token)
		{
			CancelDelegationToken(url, token, null);
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
		public virtual void CancelDelegationToken(Uri url, DelegationTokenAuthenticatedURL.Token
			 token, string doAsUser)
		{
			Preconditions.CheckNotNull(url, "url");
			Preconditions.CheckNotNull(token, "token");
			Preconditions.CheckNotNull(token.delegationToken, "No delegation token available"
				);
			try
			{
				((KerberosDelegationTokenAuthenticator)GetAuthenticator()).CancelDelegationToken(
					url, token, token.delegationToken, doAsUser);
			}
			finally
			{
				token.delegationToken = null;
			}
		}
	}
}
