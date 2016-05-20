using Sharpen;

namespace org.apache.hadoop.security.token.delegation.web
{
	/// <summary>
	/// The <code>DelegationTokenAuthenticationFilter</code> filter is a
	/// <see cref="org.apache.hadoop.security.authentication.server.AuthenticationFilter"
	/// 	/>
	/// with Hadoop Delegation Token support.
	/// <p/>
	/// By default it uses it own instance of the
	/// <see cref="org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager{TokenIdent}
	/// 	"/>
	/// . For situations where an external
	/// <code>AbstractDelegationTokenSecretManager</code> is required (i.e. one that
	/// shares the secret with <code>AbstractDelegationTokenSecretManager</code>
	/// instance running in other services), the external
	/// <code>AbstractDelegationTokenSecretManager</code> must be set as an
	/// attribute in the
	/// <see cref="javax.servlet.ServletContext"/>
	/// of the web application using the
	/// <see cref="DELEGATION_TOKEN_SECRET_MANAGER_ATTR"/>
	/// attribute name (
	/// 'hadoop.http.delegation-token-secret-manager').
	/// </summary>
	public class DelegationTokenAuthenticationFilter : org.apache.hadoop.security.authentication.server.AuthenticationFilter
	{
		private const string APPLICATION_JSON_MIME = "application/json";

		private const string ERROR_EXCEPTION_JSON = "exception";

		private const string ERROR_MESSAGE_JSON = "message";

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
		public const string DELEGATION_TOKEN_SECRET_MANAGER_ATTR = "hadoop.http.delegation-token-secret-manager";

		private static readonly java.nio.charset.Charset UTF8_CHARSET = java.nio.charset.Charset
			.forName("UTF-8");

		private static readonly java.lang.ThreadLocal<org.apache.hadoop.security.UserGroupInformation
			> UGI_TL = new java.lang.ThreadLocal<org.apache.hadoop.security.UserGroupInformation
			>();

		public const string PROXYUSER_PREFIX = "proxyuser";

		private org.apache.hadoop.security.SaslRpcServer.AuthMethod handlerAuthMethod;

		/// <summary>
		/// It delegates to
		/// <see cref="org.apache.hadoop.security.authentication.server.AuthenticationFilter.getConfiguration(string, javax.servlet.FilterConfig)
		/// 	"/>
		/// and
		/// then overrides the
		/// <see cref="org.apache.hadoop.security.authentication.server.AuthenticationHandler
		/// 	"/>
		/// to use if authentication
		/// type is set to <code>simple</code> or <code>kerberos</code> in order to use
		/// the corresponding implementation with delegation token support.
		/// </summary>
		/// <param name="configPrefix">parameter not used.</param>
		/// <param name="filterConfig">parameter not used.</param>
		/// <returns>hadoop-auth de-prefixed configuration for the filter and handler.</returns>
		/// <exception cref="javax.servlet.ServletException"/>
		protected internal override java.util.Properties getConfiguration(string configPrefix
			, javax.servlet.FilterConfig filterConfig)
		{
			java.util.Properties props = base.getConfiguration(configPrefix, filterConfig);
			setAuthHandlerClass(props);
			return props;
		}

		/// <summary>
		/// Set AUTH_TYPE property to the name of the corresponding authentication
		/// handler class based on the input properties.
		/// </summary>
		/// <param name="props">input properties.</param>
		/// <exception cref="javax.servlet.ServletException"/>
		protected internal virtual void setAuthHandlerClass(java.util.Properties props)
		{
			string authType = props.getProperty(AUTH_TYPE);
			if (authType == null)
			{
				throw new javax.servlet.ServletException("Config property " + AUTH_TYPE + " doesn't exist"
					);
			}
			if (authType.Equals(org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				.TYPE))
			{
				props.setProperty(AUTH_TYPE, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticationHandler
					)).getName());
			}
			else
			{
				if (authType.Equals(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
					.TYPE))
				{
					props.setProperty(AUTH_TYPE, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticationHandler
						)).getName());
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
		/// <exception cref="javax.servlet.ServletException">thrown if the configuration could not be created.
		/// 	</exception>
		protected internal virtual org.apache.hadoop.conf.Configuration getProxyuserConfiguration
			(javax.servlet.FilterConfig filterConfig)
		{
			// this filter class gets the configuration from the filter configs, we are
			// creating an empty configuration and injecting the proxyuser settings in
			// it. In the initialization of the filter, the returned configuration is
			// passed to the ProxyUsers which only looks for 'proxyusers.' properties.
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(false);
			java.util.Enumeration<object> names = filterConfig.getInitParameterNames();
			while (names.MoveNext())
			{
				string name = (string)names.Current;
				if (name.StartsWith(PROXYUSER_PREFIX + "."))
				{
					string value = filterConfig.getInitParameter(name);
					conf.set(name, value);
				}
			}
			return conf;
		}

		/// <exception cref="javax.servlet.ServletException"/>
		public override void init(javax.servlet.FilterConfig filterConfig)
		{
			base.init(filterConfig);
			org.apache.hadoop.security.authentication.server.AuthenticationHandler handler = 
				getAuthenticationHandler();
			org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager 
				dtSecretManager = (org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager
				)filterConfig.getServletContext().getAttribute(DELEGATION_TOKEN_SECRET_MANAGER_ATTR
				);
			if (dtSecretManager != null && handler is org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler)
			{
				org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler
					 dtHandler = (org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler
					)getAuthenticationHandler();
				dtHandler.setExternalDelegationTokenSecretManager(dtSecretManager);
			}
			if (handler is org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler
				 || handler is org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticationHandler)
			{
				setHandlerAuthMethod(org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE);
			}
			if (handler is org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				 || handler is org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticationHandler)
			{
				setHandlerAuthMethod(org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS
					);
			}
			// proxyuser configuration
			org.apache.hadoop.conf.Configuration conf = getProxyuserConfiguration(filterConfig
				);
			org.apache.hadoop.security.authorize.ProxyUsers.refreshSuperUserGroupsConfiguration
				(conf, PROXYUSER_PREFIX);
		}

		/// <exception cref="javax.servlet.ServletException"/>
		protected internal override void initializeAuthHandler(string authHandlerClassName
			, javax.servlet.FilterConfig filterConfig)
		{
			// A single CuratorFramework should be used for a ZK cluster.
			// If the ZKSignerSecretProvider has already created it, it has to
			// be set here... to be used by the ZKDelegationTokenSecretManager
			org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.setCurator
				((org.apache.curator.framework.CuratorFramework)filterConfig.getServletContext()
				.getAttribute(org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider
				.ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE));
			base.initializeAuthHandler(authHandlerClassName, filterConfig);
			org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.setCurator
				(null);
		}

		protected internal virtual void setHandlerAuthMethod(org.apache.hadoop.security.SaslRpcServer.AuthMethod
			 authMethod)
		{
			this.handlerAuthMethod = authMethod;
		}

		[com.google.common.annotations.VisibleForTesting]
		internal static string getDoAs(javax.servlet.http.HttpServletRequest request)
		{
			System.Collections.Generic.IList<org.apache.http.NameValuePair> list = org.apache.http.client.utils.URLEncodedUtils
				.parse(request.getQueryString(), UTF8_CHARSET);
			if (list != null)
			{
				foreach (org.apache.http.NameValuePair nv in list)
				{
					if (Sharpen.Runtime.equalsIgnoreCase(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL
						.DO_AS, nv.getName()))
					{
						return nv.getValue();
					}
				}
			}
			return null;
		}

		internal static org.apache.hadoop.security.UserGroupInformation getHttpUserGroupInformationInContext
			()
		{
			return UGI_TL.get();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="javax.servlet.ServletException"/>
		protected internal override void doFilter(javax.servlet.FilterChain filterChain, 
			javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			bool requestCompleted = false;
			org.apache.hadoop.security.UserGroupInformation ugi = null;
			org.apache.hadoop.security.authentication.server.AuthenticationToken authToken = 
				(org.apache.hadoop.security.authentication.server.AuthenticationToken)request.getUserPrincipal
				();
			if (authToken != null && authToken != org.apache.hadoop.security.authentication.server.AuthenticationToken
				.ANONYMOUS)
			{
				// if the request was authenticated because of a delegation token,
				// then we ignore proxyuser (this is the same as the RPC behavior).
				ugi = (org.apache.hadoop.security.UserGroupInformation)request.getAttribute(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler
					.DELEGATION_TOKEN_UGI_ATTRIBUTE);
				if (ugi == null)
				{
					string realUser = request.getUserPrincipal().getName();
					ugi = org.apache.hadoop.security.UserGroupInformation.createRemoteUser(realUser, 
						handlerAuthMethod);
					string doAsUser = getDoAs(request);
					if (doAsUser != null)
					{
						ugi = org.apache.hadoop.security.UserGroupInformation.createProxyUser(doAsUser, ugi
							);
						try
						{
							org.apache.hadoop.security.authorize.ProxyUsers.authorize(ugi, request.getRemoteHost
								());
						}
						catch (org.apache.hadoop.security.authorize.AuthorizationException ex)
						{
							org.apache.hadoop.util.HttpExceptionUtils.createServletExceptionResponse(response
								, javax.servlet.http.HttpServletResponse.SC_FORBIDDEN, ex);
							requestCompleted = true;
						}
					}
				}
				UGI_TL.set(ugi);
			}
			if (!requestCompleted)
			{
				org.apache.hadoop.security.UserGroupInformation ugiF = ugi;
				try
				{
					request = new _HttpServletRequestWrapper_269(this, ugiF, request);
					base.doFilter(filterChain, request, response);
				}
				finally
				{
					UGI_TL.remove();
				}
			}
		}

		private sealed class _HttpServletRequestWrapper_269 : javax.servlet.http.HttpServletRequestWrapper
		{
			public _HttpServletRequestWrapper_269(DelegationTokenAuthenticationFilter _enclosing
				, org.apache.hadoop.security.UserGroupInformation ugiF, javax.servlet.http.HttpServletRequest
				 baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.ugiF = ugiF;
			}

			public override string getAuthType()
			{
				return (ugiF != null) ? this._enclosing.handlerAuthMethod.ToString() : null;
			}

			public override string getRemoteUser()
			{
				return (ugiF != null) ? ugiF.getShortUserName() : null;
			}

			public override java.security.Principal getUserPrincipal()
			{
				return (ugiF != null) ? new _Principal_283(ugiF) : null;
			}

			private sealed class _Principal_283 : java.security.Principal
			{
				public _Principal_283(org.apache.hadoop.security.UserGroupInformation ugiF)
				{
					this.ugiF = ugiF;
				}

				public string getName()
				{
					return ugiF.getUserName();
				}

				private readonly org.apache.hadoop.security.UserGroupInformation ugiF;
			}

			private readonly DelegationTokenAuthenticationFilter _enclosing;

			private readonly org.apache.hadoop.security.UserGroupInformation ugiF;
		}
	}
}
