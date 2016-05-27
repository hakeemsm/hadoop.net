using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Curator.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Org.Apache.Http;
using Org.Apache.Http.Client.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	/// <summary>
	/// The <code>DelegationTokenAuthenticationFilter</code> filter is a
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationFilter"
	/// 	/>
	/// with Hadoop Delegation Token support.
	/// <p/>
	/// By default it uses it own instance of the
	/// <see cref="Org.Apache.Hadoop.Security.Token.Delegation.AbstractDelegationTokenSecretManager{TokenIdent}
	/// 	"/>
	/// . For situations where an external
	/// <code>AbstractDelegationTokenSecretManager</code> is required (i.e. one that
	/// shares the secret with <code>AbstractDelegationTokenSecretManager</code>
	/// instance running in other services), the external
	/// <code>AbstractDelegationTokenSecretManager</code> must be set as an
	/// attribute in the
	/// <see cref="Javax.Servlet.ServletContext"/>
	/// of the web application using the
	/// <see cref="DelegationTokenSecretManagerAttr"/>
	/// attribute name (
	/// 'hadoop.http.delegation-token-secret-manager').
	/// </summary>
	public class DelegationTokenAuthenticationFilter : AuthenticationFilter
	{
		private const string ApplicationJsonMime = "application/json";

		private const string ErrorExceptionJson = "exception";

		private const string ErrorMessageJson = "message";

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
		public const string DelegationTokenSecretManagerAttr = "hadoop.http.delegation-token-secret-manager";

		private static readonly Encoding Utf8Charset = Sharpen.Extensions.GetEncoding("UTF-8"
			);

		private static readonly ThreadLocal<UserGroupInformation> UgiTl = new ThreadLocal
			<UserGroupInformation>();

		public const string ProxyuserPrefix = "proxyuser";

		private SaslRpcServer.AuthMethod handlerAuthMethod;

		/// <summary>
		/// It delegates to
		/// <see cref="Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationFilter.GetConfiguration(string, Javax.Servlet.FilterConfig)
		/// 	"/>
		/// and
		/// then overrides the
		/// <see cref="Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationHandler
		/// 	"/>
		/// to use if authentication
		/// type is set to <code>simple</code> or <code>kerberos</code> in order to use
		/// the corresponding implementation with delegation token support.
		/// </summary>
		/// <param name="configPrefix">parameter not used.</param>
		/// <param name="filterConfig">parameter not used.</param>
		/// <returns>hadoop-auth de-prefixed configuration for the filter and handler.</returns>
		/// <exception cref="Javax.Servlet.ServletException"/>
		protected override Properties GetConfiguration(string configPrefix, FilterConfig 
			filterConfig)
		{
			Properties props = base.GetConfiguration(configPrefix, filterConfig);
			SetAuthHandlerClass(props);
			return props;
		}

		/// <summary>
		/// Set AUTH_TYPE property to the name of the corresponding authentication
		/// handler class based on the input properties.
		/// </summary>
		/// <param name="props">input properties.</param>
		/// <exception cref="Javax.Servlet.ServletException"/>
		protected internal virtual void SetAuthHandlerClass(Properties props)
		{
			string authType = props.GetProperty(AuthType);
			if (authType == null)
			{
				throw new ServletException("Config property " + AuthType + " doesn't exist");
			}
			if (authType.Equals(PseudoAuthenticationHandler.Type))
			{
				props.SetProperty(AuthType, typeof(PseudoDelegationTokenAuthenticationHandler).FullName
					);
			}
			else
			{
				if (authType.Equals(KerberosAuthenticationHandler.Type))
				{
					props.SetProperty(AuthType, typeof(KerberosDelegationTokenAuthenticationHandler).
						FullName);
				}
			}
		}

		/// <summary>Returns the proxyuser configuration.</summary>
		/// <remarks>
		/// Returns the proxyuser configuration. All returned properties must start
		/// with <code>proxyuser.</code>'
		/// <p/>
		/// Subclasses may override this method if the proxyuser configuration is
		/// read from other place than the filter init parameters.
		/// </remarks>
		/// <param name="filterConfig">filter configuration object</param>
		/// <returns>the proxyuser configuration properties.</returns>
		/// <exception cref="Javax.Servlet.ServletException">thrown if the configuration could not be created.
		/// 	</exception>
		protected internal virtual Configuration GetProxyuserConfiguration(FilterConfig filterConfig
			)
		{
			// this filter class gets the configuration from the filter configs, we are
			// creating an empty configuration and injecting the proxyuser settings in
			// it. In the initialization of the filter, the returned configuration is
			// passed to the ProxyUsers which only looks for 'proxyusers.' properties.
			Configuration conf = new Configuration(false);
			Enumeration<object> names = filterConfig.GetInitParameterNames();
			while (names.MoveNext())
			{
				string name = (string)names.Current;
				if (name.StartsWith(ProxyuserPrefix + "."))
				{
					string value = filterConfig.GetInitParameter(name);
					conf.Set(name, value);
				}
			}
			return conf;
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		public override void Init(FilterConfig filterConfig)
		{
			base.Init(filterConfig);
			AuthenticationHandler handler = GetAuthenticationHandler();
			AbstractDelegationTokenSecretManager dtSecretManager = (AbstractDelegationTokenSecretManager
				)filterConfig.GetServletContext().GetAttribute(DelegationTokenSecretManagerAttr);
			if (dtSecretManager != null && handler is DelegationTokenAuthenticationHandler)
			{
				DelegationTokenAuthenticationHandler dtHandler = (DelegationTokenAuthenticationHandler
					)GetAuthenticationHandler();
				dtHandler.SetExternalDelegationTokenSecretManager(dtSecretManager);
			}
			if (handler is PseudoAuthenticationHandler || handler is PseudoDelegationTokenAuthenticationHandler)
			{
				SetHandlerAuthMethod(SaslRpcServer.AuthMethod.Simple);
			}
			if (handler is KerberosAuthenticationHandler || handler is KerberosDelegationTokenAuthenticationHandler)
			{
				SetHandlerAuthMethod(SaslRpcServer.AuthMethod.Kerberos);
			}
			// proxyuser configuration
			Configuration conf = GetProxyuserConfiguration(filterConfig);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf, ProxyuserPrefix);
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		protected override void InitializeAuthHandler(string authHandlerClassName, FilterConfig
			 filterConfig)
		{
			// A single CuratorFramework should be used for a ZK cluster.
			// If the ZKSignerSecretProvider has already created it, it has to
			// be set here... to be used by the ZKDelegationTokenSecretManager
			ZKDelegationTokenSecretManager.SetCurator((CuratorFramework)filterConfig.GetServletContext
				().GetAttribute(ZKSignerSecretProvider.ZookeeperSignerSecretProviderCuratorClientAttribute
				));
			base.InitializeAuthHandler(authHandlerClassName, filterConfig);
			ZKDelegationTokenSecretManager.SetCurator(null);
		}

		protected internal virtual void SetHandlerAuthMethod(SaslRpcServer.AuthMethod authMethod
			)
		{
			this.handlerAuthMethod = authMethod;
		}

		[VisibleForTesting]
		internal static string GetDoAs(HttpServletRequest request)
		{
			IList<NameValuePair> list = URLEncodedUtils.Parse(request.GetQueryString(), Utf8Charset
				);
			if (list != null)
			{
				foreach (NameValuePair nv in list)
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(DelegationTokenAuthenticatedURL.DoAs, nv.GetName
						()))
					{
						return nv.GetValue();
					}
				}
			}
			return null;
		}

		internal static UserGroupInformation GetHttpUserGroupInformationInContext()
		{
			return UgiTl.Get();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		protected override void DoFilter(FilterChain filterChain, HttpServletRequest request
			, HttpServletResponse response)
		{
			bool requestCompleted = false;
			UserGroupInformation ugi = null;
			AuthenticationToken authToken = (AuthenticationToken)request.GetUserPrincipal();
			if (authToken != null && authToken != AuthenticationToken.Anonymous)
			{
				// if the request was authenticated because of a delegation token,
				// then we ignore proxyuser (this is the same as the RPC behavior).
				ugi = (UserGroupInformation)request.GetAttribute(DelegationTokenAuthenticationHandler
					.DelegationTokenUgiAttribute);
				if (ugi == null)
				{
					string realUser = request.GetUserPrincipal().GetName();
					ugi = UserGroupInformation.CreateRemoteUser(realUser, handlerAuthMethod);
					string doAsUser = GetDoAs(request);
					if (doAsUser != null)
					{
						ugi = UserGroupInformation.CreateProxyUser(doAsUser, ugi);
						try
						{
							ProxyUsers.Authorize(ugi, request.GetRemoteHost());
						}
						catch (AuthorizationException ex)
						{
							HttpExceptionUtils.CreateServletExceptionResponse(response, HttpServletResponse.ScForbidden
								, ex);
							requestCompleted = true;
						}
					}
				}
				UgiTl.Set(ugi);
			}
			if (!requestCompleted)
			{
				UserGroupInformation ugiF = ugi;
				try
				{
					request = new _HttpServletRequestWrapper_269(this, ugiF, request);
					base.DoFilter(filterChain, request, response);
				}
				finally
				{
					UgiTl.Remove();
				}
			}
		}

		private sealed class _HttpServletRequestWrapper_269 : HttpServletRequestWrapper
		{
			public _HttpServletRequestWrapper_269(DelegationTokenAuthenticationFilter _enclosing
				, UserGroupInformation ugiF, HttpServletRequest baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.ugiF = ugiF;
			}

			public override string GetAuthType()
			{
				return (ugiF != null) ? this._enclosing.handlerAuthMethod.ToString() : null;
			}

			public override string GetRemoteUser()
			{
				return (ugiF != null) ? ugiF.GetShortUserName() : null;
			}

			public override Principal GetUserPrincipal()
			{
				return (ugiF != null) ? new _Principal_283(ugiF) : null;
			}

			private sealed class _Principal_283 : Principal
			{
				public _Principal_283(UserGroupInformation ugiF)
				{
					this.ugiF = ugiF;
				}

				public string GetName()
				{
					return ugiF.GetUserName();
				}

				private readonly UserGroupInformation ugiF;
			}

			private readonly DelegationTokenAuthenticationFilter _enclosing;

			private readonly UserGroupInformation ugiF;
		}
	}
}
