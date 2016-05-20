using Sharpen;

namespace org.apache.hadoop.security.authentication.server
{
	/// <summary>
	/// The
	/// <see cref="KerberosAuthenticationHandler"/>
	/// implements the Kerberos SPNEGO authentication mechanism for HTTP.
	/// <p>
	/// The supported configuration properties are:
	/// <ul>
	/// <li>kerberos.principal: the Kerberos principal to used by the server. As stated by the Kerberos SPNEGO
	/// specification, it should be <code>HTTP/${HOSTNAME}@{REALM}</code>. The realm can be omitted from the
	/// principal as the JDK GSS libraries will use the realm name of the configured default realm.
	/// It does not have a default value.</li>
	/// <li>kerberos.keytab: the keytab file containing the credentials for the Kerberos principal.
	/// It does not have a default value.</li>
	/// <li>kerberos.name.rules: kerberos names rules to resolve principal names, see
	/// <see cref="org.apache.hadoop.security.authentication.util.KerberosName.setRules(string)
	/// 	"/>
	/// </li>
	/// </ul>
	/// </summary>
	public class KerberosAuthenticationHandler : org.apache.hadoop.security.authentication.server.AuthenticationHandler
	{
		private static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
			)));

		/// <summary>Kerberos context configuration for the JDK GSS library.</summary>
		private class KerberosConfiguration : javax.security.auth.login.Configuration
		{
			private string keytab;

			private string principal;

			public KerberosConfiguration(string keytab, string principal)
			{
				this.keytab = keytab;
				this.principal = principal;
			}

			public override javax.security.auth.login.AppConfigurationEntry[] getAppConfigurationEntry
				(string name)
			{
				System.Collections.Generic.IDictionary<string, string> options = new System.Collections.Generic.Dictionary
					<string, string>();
				if (org.apache.hadoop.util.PlatformName.IBM_JAVA)
				{
					options["useKeytab"] = keytab.StartsWith("file://") ? keytab : "file://" + keytab;
					options["principal"] = principal;
					options["credsType"] = "acceptor";
				}
				else
				{
					options["keyTab"] = keytab;
					options["principal"] = principal;
					options["useKeyTab"] = "true";
					options["storeKey"] = "true";
					options["doNotPrompt"] = "true";
					options["useTicketCache"] = "true";
					options["renewTGT"] = "true";
					options["isInitiator"] = "false";
				}
				options["refreshKrb5Config"] = "true";
				string ticketCache = Sharpen.Runtime.getenv("KRB5CCNAME");
				if (ticketCache != null)
				{
					if (org.apache.hadoop.util.PlatformName.IBM_JAVA)
					{
						options["useDefaultCcache"] = "true";
						// The first value searched when "useDefaultCcache" is used.
						Sharpen.Runtime.setProperty("KRB5CCNAME", ticketCache);
						options["renewTGT"] = "true";
						options["credsType"] = "both";
					}
					else
					{
						options["ticketCache"] = ticketCache;
					}
				}
				if (LOG.isDebugEnabled())
				{
					options["debug"] = "true";
				}
				return new javax.security.auth.login.AppConfigurationEntry[] { new javax.security.auth.login.AppConfigurationEntry
					(org.apache.hadoop.security.authentication.util.KerberosUtil.getKrb5LoginModuleName
					(), javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED
					, options) };
			}
		}

		/// <summary>Constant that identifies the authentication mechanism.</summary>
		public const string TYPE = "kerberos";

		/// <summary>Constant for the configuration property that indicates the kerberos principal.
		/// 	</summary>
		public const string PRINCIPAL = TYPE + ".principal";

		/// <summary>Constant for the configuration property that indicates the keytab file path.
		/// 	</summary>
		public const string KEYTAB = TYPE + ".keytab";

		/// <summary>
		/// Constant for the configuration property that indicates the Kerberos name
		/// rules for the Kerberos principals.
		/// </summary>
		public const string NAME_RULES = TYPE + ".name.rules";

		private string type;

		private string keytab;

		private org.ietf.jgss.GSSManager gssManager;

		private javax.security.auth.Subject serverSubject = new javax.security.auth.Subject
			();

		private System.Collections.Generic.IList<javax.security.auth.login.LoginContext> 
			loginContexts = new System.Collections.Generic.List<javax.security.auth.login.LoginContext
			>();

		/// <summary>
		/// Creates a Kerberos SPNEGO authentication handler with the default
		/// auth-token type, <code>kerberos</code>.
		/// </summary>
		public KerberosAuthenticationHandler()
			: this(TYPE)
		{
		}

		/// <summary>
		/// Creates a Kerberos SPNEGO authentication handler with a custom auth-token
		/// type.
		/// </summary>
		/// <param name="type">auth-token type.</param>
		public KerberosAuthenticationHandler(string type)
		{
			this.type = type;
		}

		/// <summary>Initializes the authentication handler instance.</summary>
		/// <remarks>
		/// Initializes the authentication handler instance.
		/// <p>
		/// It creates a Kerberos context using the principal and keytab specified in the configuration.
		/// <p>
		/// This method is invoked by the
		/// <see cref="AuthenticationFilter.init(javax.servlet.FilterConfig)"/>
		/// method.
		/// </remarks>
		/// <param name="config">configuration properties to initialize the handler.</param>
		/// <exception cref="javax.servlet.ServletException">thrown if the handler could not be initialized.
		/// 	</exception>
		public override void init(java.util.Properties config)
		{
			try
			{
				string principal = config.getProperty(PRINCIPAL);
				if (principal == null || principal.Trim().Length == 0)
				{
					throw new javax.servlet.ServletException("Principal not defined in configuration"
						);
				}
				keytab = config.getProperty(KEYTAB, keytab);
				if (keytab == null || keytab.Trim().Length == 0)
				{
					throw new javax.servlet.ServletException("Keytab not defined in configuration");
				}
				if (!new java.io.File(keytab).exists())
				{
					throw new javax.servlet.ServletException("Keytab does not exist: " + keytab);
				}
				// use all SPNEGO principals in the keytab if a principal isn't
				// specifically configured
				string[] spnegoPrincipals;
				if (principal.Equals("*"))
				{
					spnegoPrincipals = org.apache.hadoop.security.authentication.util.KerberosUtil.getPrincipalNames
						(keytab, java.util.regex.Pattern.compile("HTTP/.*"));
					if (spnegoPrincipals.Length == 0)
					{
						throw new javax.servlet.ServletException("Principals do not exist in the keytab");
					}
				}
				else
				{
					spnegoPrincipals = new string[] { principal };
				}
				string nameRules = config.getProperty(NAME_RULES, null);
				if (nameRules != null)
				{
					org.apache.hadoop.security.authentication.util.KerberosName.setRules(nameRules);
				}
				foreach (string spnegoPrincipal in spnegoPrincipals)
				{
					LOG.info("Login using keytab {}, for principal {}", keytab, spnegoPrincipal);
					org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.KerberosConfiguration
						 kerberosConfiguration = new org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.KerberosConfiguration
						(keytab, spnegoPrincipal);
					javax.security.auth.login.LoginContext loginContext = new javax.security.auth.login.LoginContext
						(string.Empty, serverSubject, null, kerberosConfiguration);
					try
					{
						loginContext.login();
					}
					catch (javax.security.auth.login.LoginException le)
					{
						LOG.warn("Failed to login as [{}]", spnegoPrincipal, le);
						throw new org.apache.hadoop.security.authentication.client.AuthenticationException
							(le);
					}
					loginContexts.add(loginContext);
				}
				try
				{
					gssManager = javax.security.auth.Subject.doAs(serverSubject, new _PrivilegedExceptionAction_229
						());
				}
				catch (java.security.PrivilegedActionException ex)
				{
					throw ex.getException();
				}
			}
			catch (System.Exception ex)
			{
				throw new javax.servlet.ServletException(ex);
			}
		}

		private sealed class _PrivilegedExceptionAction_229 : java.security.PrivilegedExceptionAction
			<org.ietf.jgss.GSSManager>
		{
			public _PrivilegedExceptionAction_229()
			{
			}

			/// <exception cref="System.Exception"/>
			public org.ietf.jgss.GSSManager run()
			{
				return org.ietf.jgss.GSSManager.getInstance();
			}
		}

		/// <summary>Releases any resources initialized by the authentication handler.</summary>
		/// <remarks>
		/// Releases any resources initialized by the authentication handler.
		/// <p>
		/// It destroys the Kerberos context.
		/// </remarks>
		public override void destroy()
		{
			keytab = null;
			serverSubject = null;
			foreach (javax.security.auth.login.LoginContext loginContext in loginContexts)
			{
				try
				{
					loginContext.logout();
				}
				catch (javax.security.auth.login.LoginException ex)
				{
					LOG.warn(ex.Message, ex);
				}
			}
			loginContexts.clear();
		}

		/// <summary>Returns the authentication type of the authentication handler, 'kerberos'.
		/// 	</summary>
		/// <remarks>
		/// Returns the authentication type of the authentication handler, 'kerberos'.
		/// <p>
		/// </remarks>
		/// <returns>the authentication type of the authentication handler, 'kerberos'.</returns>
		public override string getType()
		{
			return type;
		}

		/// <summary>Returns the Kerberos principals used by the authentication handler.</summary>
		/// <returns>the Kerberos principals used by the authentication handler.</returns>
		protected internal virtual System.Collections.Generic.ICollection<javax.security.auth.kerberos.KerberosPrincipal
			> getPrincipals()
		{
			return serverSubject.getPrincipals<javax.security.auth.kerberos.KerberosPrincipal
				>();
		}

		/// <summary>Returns the keytab used by the authentication handler.</summary>
		/// <returns>the keytab used by the authentication handler.</returns>
		protected internal virtual string getKeytab()
		{
			return keytab;
		}

		/// <summary>This is an empty implementation, it always returns <code>TRUE</code>.</summary>
		/// <param name="token">the authentication token if any, otherwise <code>NULL</code>.
		/// 	</param>
		/// <param name="request">the HTTP client request.</param>
		/// <param name="response">the HTTP client response.</param>
		/// <returns><code>TRUE</code></returns>
		/// <exception cref="System.IO.IOException">it is never thrown.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">it is never thrown.</exception>
		public override bool managementOperation(org.apache.hadoop.security.authentication.server.AuthenticationToken
			 token, javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			return true;
		}

		/// <summary>
		/// It enforces the the Kerberos SPNEGO authentication sequence returning an
		/// <see cref="AuthenticationToken"/>
		/// only
		/// after the Kerberos SPNEGO sequence has completed successfully.
		/// </summary>
		/// <param name="request">the HTTP client request.</param>
		/// <param name="response">the HTTP client response.</param>
		/// <returns>
		/// an authentication token if the Kerberos SPNEGO sequence is complete and valid,
		/// <code>null</code> if it is in progress (in this case the handler handles the response to the client).
		/// </returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">thrown if Kerberos SPNEGO sequence failed.</exception>
		public override org.apache.hadoop.security.authentication.server.AuthenticationToken
			 authenticate(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			org.apache.hadoop.security.authentication.server.AuthenticationToken token = null;
			string authorization = request.getHeader(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.AUTHORIZATION);
			if (authorization == null || !authorization.StartsWith(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
				.NEGOTIATE))
			{
				response.setHeader(WWW_AUTHENTICATE, org.apache.hadoop.security.authentication.client.KerberosAuthenticator
					.NEGOTIATE);
				response.setStatus(javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED);
				if (authorization == null)
				{
					LOG.trace("SPNEGO starting");
				}
				else
				{
					LOG.warn("'" + org.apache.hadoop.security.authentication.client.KerberosAuthenticator
						.AUTHORIZATION + "' does not start with '" + org.apache.hadoop.security.authentication.client.KerberosAuthenticator
						.NEGOTIATE + "' :  {}", authorization);
				}
			}
			else
			{
				authorization = Sharpen.Runtime.substring(authorization, org.apache.hadoop.security.authentication.client.KerberosAuthenticator
					.NEGOTIATE.Length).Trim();
				org.apache.commons.codec.binary.Base64 base64 = new org.apache.commons.codec.binary.Base64
					(0);
				byte[] clientToken = base64.decode(authorization);
				string serverName = request.getServerName();
				try
				{
					token = javax.security.auth.Subject.doAs(serverSubject, new _PrivilegedExceptionAction_347
						(this, serverName, clientToken, base64, response));
				}
				catch (java.security.PrivilegedActionException ex)
				{
					if (ex.getException() is System.IO.IOException)
					{
						throw (System.IO.IOException)ex.getException();
					}
					else
					{
						throw new org.apache.hadoop.security.authentication.client.AuthenticationException
							(ex.getException());
					}
				}
			}
			return token;
		}

		private sealed class _PrivilegedExceptionAction_347 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.security.authentication.server.AuthenticationToken>
		{
			public _PrivilegedExceptionAction_347(KerberosAuthenticationHandler _enclosing, string
				 serverName, byte[] clientToken, org.apache.commons.codec.binary.Base64 base64, 
				javax.servlet.http.HttpServletResponse response)
			{
				this._enclosing = _enclosing;
				this.serverName = serverName;
				this.clientToken = clientToken;
				this.base64 = base64;
				this.response = response;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.security.authentication.server.AuthenticationToken run()
			{
				org.apache.hadoop.security.authentication.server.AuthenticationToken token = null;
				org.ietf.jgss.GSSContext gssContext = null;
				org.ietf.jgss.GSSCredential gssCreds = null;
				try
				{
					gssCreds = this._enclosing.gssManager.createCredential(this._enclosing.gssManager
						.createName(org.apache.hadoop.security.authentication.util.KerberosUtil.getServicePrincipal
						("HTTP", serverName), org.apache.hadoop.security.authentication.util.KerberosUtil
						.getOidInstance("NT_GSS_KRB5_PRINCIPAL")), org.ietf.jgss.GSSCredential.INDEFINITE_LIFETIME
						, new org.ietf.jgss.Oid[] { org.apache.hadoop.security.authentication.util.KerberosUtil
						.getOidInstance("GSS_SPNEGO_MECH_OID"), org.apache.hadoop.security.authentication.util.KerberosUtil
						.getOidInstance("GSS_KRB5_MECH_OID") }, org.ietf.jgss.GSSCredential.ACCEPT_ONLY);
					gssContext = this._enclosing.gssManager.createContext(gssCreds);
					byte[] serverToken = gssContext.acceptSecContext(clientToken, 0, clientToken.Length
						);
					if (serverToken != null && serverToken.Length > 0)
					{
						string authenticate = base64.encodeToString(serverToken);
						response.setHeader(org.apache.hadoop.security.authentication.client.KerberosAuthenticator
							.WWW_AUTHENTICATE, org.apache.hadoop.security.authentication.client.KerberosAuthenticator
							.NEGOTIATE + " " + authenticate);
					}
					if (!gssContext.isEstablished())
					{
						response.setStatus(javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED);
						org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.LOG
							.trace("SPNEGO in progress");
					}
					else
					{
						string clientPrincipal = gssContext.getSrcName().ToString();
						org.apache.hadoop.security.authentication.util.KerberosName kerberosName = new org.apache.hadoop.security.authentication.util.KerberosName
							(clientPrincipal);
						string userName = kerberosName.getShortName();
						token = new org.apache.hadoop.security.authentication.server.AuthenticationToken(
							userName, clientPrincipal, this._enclosing.getType());
						response.setStatus(javax.servlet.http.HttpServletResponse.SC_OK);
						org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.LOG
							.trace("SPNEGO completed for principal [{}]", clientPrincipal);
					}
				}
				finally
				{
					if (gssContext != null)
					{
						gssContext.dispose();
					}
					if (gssCreds != null)
					{
						gssCreds.dispose();
					}
				}
				return token;
			}

			private readonly KerberosAuthenticationHandler _enclosing;

			private readonly string serverName;

			private readonly byte[] clientToken;

			private readonly org.apache.commons.codec.binary.Base64 base64;

			private readonly javax.servlet.http.HttpServletResponse response;
		}
	}
}
