using System;
using System.Collections.Generic;
using System.IO;
using Javax.Security.Auth;
using Javax.Security.Auth.Kerberos;
using Javax.Security.Auth.Login;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Server
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
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Util.KerberosName.SetRules(string)
	/// 	"/>
	/// </li>
	/// </ul>
	/// </summary>
	public class KerberosAuthenticationHandler : AuthenticationHandler
	{
		private static Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Security.Authentication.Server.KerberosAuthenticationHandler
			));

		/// <summary>Kerberos context configuration for the JDK GSS library.</summary>
		private class KerberosConfiguration : Configuration
		{
			private string keytab;

			private string principal;

			public KerberosConfiguration(string keytab, string principal)
			{
				this.keytab = keytab;
				this.principal = principal;
			}

			public override AppConfigurationEntry[] GetAppConfigurationEntry(string name)
			{
				IDictionary<string, string> options = new Dictionary<string, string>();
				if (PlatformName.IbmJava)
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
				string ticketCache = Runtime.Getenv("KRB5CCNAME");
				if (ticketCache != null)
				{
					if (PlatformName.IbmJava)
					{
						options["useDefaultCcache"] = "true";
						// The first value searched when "useDefaultCcache" is used.
						Runtime.SetProperty("KRB5CCNAME", ticketCache);
						options["renewTGT"] = "true";
						options["credsType"] = "both";
					}
					else
					{
						options["ticketCache"] = ticketCache;
					}
				}
				if (Log.IsDebugEnabled())
				{
					options["debug"] = "true";
				}
				return new AppConfigurationEntry[] { new AppConfigurationEntry(KerberosUtil.GetKrb5LoginModuleName
					(), AppConfigurationEntry.LoginModuleControlFlag.Required, options) };
			}
		}

		/// <summary>Constant that identifies the authentication mechanism.</summary>
		public const string Type = "kerberos";

		/// <summary>Constant for the configuration property that indicates the kerberos principal.
		/// 	</summary>
		public const string Principal = Type + ".principal";

		/// <summary>Constant for the configuration property that indicates the keytab file path.
		/// 	</summary>
		public const string Keytab = Type + ".keytab";

		/// <summary>
		/// Constant for the configuration property that indicates the Kerberos name
		/// rules for the Kerberos principals.
		/// </summary>
		public const string NameRules = Type + ".name.rules";

		private string type;

		private string keytab;

		private GSSManager gssManager;

		private Subject serverSubject = new Subject();

		private IList<LoginContext> loginContexts = new AList<LoginContext>();

		/// <summary>
		/// Creates a Kerberos SPNEGO authentication handler with the default
		/// auth-token type, <code>kerberos</code>.
		/// </summary>
		public KerberosAuthenticationHandler()
			: this(Type)
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
		/// <see cref="AuthenticationFilter.Init(Javax.Servlet.FilterConfig)"/>
		/// method.
		/// </remarks>
		/// <param name="config">configuration properties to initialize the handler.</param>
		/// <exception cref="Javax.Servlet.ServletException">thrown if the handler could not be initialized.
		/// 	</exception>
		public override void Init(Properties config)
		{
			try
			{
				string principal = config.GetProperty(Principal);
				if (principal == null || principal.Trim().Length == 0)
				{
					throw new ServletException("Principal not defined in configuration");
				}
				keytab = config.GetProperty(Keytab, keytab);
				if (keytab == null || keytab.Trim().Length == 0)
				{
					throw new ServletException("Keytab not defined in configuration");
				}
				if (!new FilePath(keytab).Exists())
				{
					throw new ServletException("Keytab does not exist: " + keytab);
				}
				// use all SPNEGO principals in the keytab if a principal isn't
				// specifically configured
				string[] spnegoPrincipals;
				if (principal.Equals("*"))
				{
					spnegoPrincipals = KerberosUtil.GetPrincipalNames(keytab, Sharpen.Pattern.Compile
						("HTTP/.*"));
					if (spnegoPrincipals.Length == 0)
					{
						throw new ServletException("Principals do not exist in the keytab");
					}
				}
				else
				{
					spnegoPrincipals = new string[] { principal };
				}
				string nameRules = config.GetProperty(NameRules, null);
				if (nameRules != null)
				{
					KerberosName.SetRules(nameRules);
				}
				foreach (string spnegoPrincipal in spnegoPrincipals)
				{
					Log.Info("Login using keytab {}, for principal {}", keytab, spnegoPrincipal);
					KerberosAuthenticationHandler.KerberosConfiguration kerberosConfiguration = new KerberosAuthenticationHandler.KerberosConfiguration
						(keytab, spnegoPrincipal);
					LoginContext loginContext = new LoginContext(string.Empty, serverSubject, null, kerberosConfiguration
						);
					try
					{
						loginContext.Login();
					}
					catch (LoginException le)
					{
						Log.Warn("Failed to login as [{}]", spnegoPrincipal, le);
						throw new AuthenticationException(le);
					}
					loginContexts.AddItem(loginContext);
				}
				try
				{
					gssManager = Subject.DoAs(serverSubject, new _PrivilegedExceptionAction_229());
				}
				catch (PrivilegedActionException ex)
				{
					throw ex.GetException();
				}
			}
			catch (Exception ex)
			{
				throw new ServletException(ex);
			}
		}

		private sealed class _PrivilegedExceptionAction_229 : PrivilegedExceptionAction<GSSManager
			>
		{
			public _PrivilegedExceptionAction_229()
			{
			}

			/// <exception cref="System.Exception"/>
			public GSSManager Run()
			{
				return GSSManager.GetInstance();
			}
		}

		/// <summary>Releases any resources initialized by the authentication handler.</summary>
		/// <remarks>
		/// Releases any resources initialized by the authentication handler.
		/// <p>
		/// It destroys the Kerberos context.
		/// </remarks>
		public override void Destroy()
		{
			keytab = null;
			serverSubject = null;
			foreach (LoginContext loginContext in loginContexts)
			{
				try
				{
					loginContext.Logout();
				}
				catch (LoginException ex)
				{
					Log.Warn(ex.Message, ex);
				}
			}
			loginContexts.Clear();
		}

		/// <summary>Returns the authentication type of the authentication handler, 'kerberos'.
		/// 	</summary>
		/// <remarks>
		/// Returns the authentication type of the authentication handler, 'kerberos'.
		/// <p>
		/// </remarks>
		/// <returns>the authentication type of the authentication handler, 'kerberos'.</returns>
		public override string GetType()
		{
			return type;
		}

		/// <summary>Returns the Kerberos principals used by the authentication handler.</summary>
		/// <returns>the Kerberos principals used by the authentication handler.</returns>
		protected internal virtual ICollection<KerberosPrincipal> GetPrincipals()
		{
			return serverSubject.GetPrincipals<KerberosPrincipal>();
		}

		/// <summary>Returns the keytab used by the authentication handler.</summary>
		/// <returns>the keytab used by the authentication handler.</returns>
		protected internal virtual string GetKeytab()
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">it is never thrown.</exception>
		public override bool ManagementOperation(AuthenticationToken token, HttpServletRequest
			 request, HttpServletResponse response)
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">thrown if Kerberos SPNEGO sequence failed.</exception>
		public override AuthenticationToken Authenticate(HttpServletRequest request, HttpServletResponse
			 response)
		{
			AuthenticationToken token = null;
			string authorization = request.GetHeader(KerberosAuthenticator.Authorization);
			if (authorization == null || !authorization.StartsWith(KerberosAuthenticator.Negotiate
				))
			{
				response.SetHeader(WwwAuthenticate, KerberosAuthenticator.Negotiate);
				response.SetStatus(HttpServletResponse.ScUnauthorized);
				if (authorization == null)
				{
					Log.Trace("SPNEGO starting");
				}
				else
				{
					Log.Warn("'" + KerberosAuthenticator.Authorization + "' does not start with '" + 
						KerberosAuthenticator.Negotiate + "' :  {}", authorization);
				}
			}
			else
			{
				authorization = Sharpen.Runtime.Substring(authorization, KerberosAuthenticator.Negotiate
					.Length).Trim();
				Base64 base64 = new Base64(0);
				byte[] clientToken = base64.Decode(authorization);
				string serverName = request.GetServerName();
				try
				{
					token = Subject.DoAs(serverSubject, new _PrivilegedExceptionAction_347(this, serverName
						, clientToken, base64, response));
				}
				catch (PrivilegedActionException ex)
				{
					if (ex.GetException() is IOException)
					{
						throw (IOException)ex.GetException();
					}
					else
					{
						throw new AuthenticationException(ex.GetException());
					}
				}
			}
			return token;
		}

		private sealed class _PrivilegedExceptionAction_347 : PrivilegedExceptionAction<AuthenticationToken
			>
		{
			public _PrivilegedExceptionAction_347(KerberosAuthenticationHandler _enclosing, string
				 serverName, byte[] clientToken, Base64 base64, HttpServletResponse response)
			{
				this._enclosing = _enclosing;
				this.serverName = serverName;
				this.clientToken = clientToken;
				this.base64 = base64;
				this.response = response;
			}

			/// <exception cref="System.Exception"/>
			public AuthenticationToken Run()
			{
				AuthenticationToken token = null;
				Sharpen.GSSContext gssContext = null;
				GSSCredential gssCreds = null;
				try
				{
					gssCreds = this._enclosing.gssManager.CreateCredential(this._enclosing.gssManager
						.CreateName(KerberosUtil.GetServicePrincipal("HTTP", serverName), KerberosUtil.GetOidInstance
						("NT_GSS_KRB5_PRINCIPAL")), GSSCredential.IndefiniteLifetime, new Oid[] { KerberosUtil
						.GetOidInstance("GSS_SPNEGO_MECH_OID"), KerberosUtil.GetOidInstance("GSS_KRB5_MECH_OID"
						) }, GSSCredential.AcceptOnly);
					gssContext = this._enclosing.gssManager.CreateContext(gssCreds);
					byte[] serverToken = gssContext.AcceptSecContext(clientToken, 0, clientToken.Length
						);
					if (serverToken != null && serverToken.Length > 0)
					{
						string authenticate = base64.EncodeToString(serverToken);
						response.SetHeader(KerberosAuthenticator.WwwAuthenticate, KerberosAuthenticator.Negotiate
							 + " " + authenticate);
					}
					if (!gssContext.IsEstablished())
					{
						response.SetStatus(HttpServletResponse.ScUnauthorized);
						KerberosAuthenticationHandler.Log.Trace("SPNEGO in progress");
					}
					else
					{
						string clientPrincipal = gssContext.GetSrcName().ToString();
						KerberosName kerberosName = new KerberosName(clientPrincipal);
						string userName = kerberosName.GetShortName();
						token = new AuthenticationToken(userName, clientPrincipal, this._enclosing.GetType
							());
						response.SetStatus(HttpServletResponse.ScOk);
						KerberosAuthenticationHandler.Log.Trace("SPNEGO completed for principal [{}]", clientPrincipal
							);
					}
				}
				finally
				{
					if (gssContext != null)
					{
						gssContext.Dispose();
					}
					if (gssCreds != null)
					{
						gssCreds.Dispose();
					}
				}
				return token;
			}

			private readonly KerberosAuthenticationHandler _enclosing;

			private readonly string serverName;

			private readonly byte[] clientToken;

			private readonly Base64 base64;

			private readonly HttpServletResponse response;
		}
	}
}
