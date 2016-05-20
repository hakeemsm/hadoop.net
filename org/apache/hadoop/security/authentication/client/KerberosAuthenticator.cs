using Sharpen;

namespace org.apache.hadoop.security.authentication.client
{
	/// <summary>
	/// The
	/// <see cref="KerberosAuthenticator"/>
	/// implements the Kerberos SPNEGO authentication sequence.
	/// <p>
	/// It uses the default principal for the Kerberos cache (normally set via kinit).
	/// <p>
	/// It falls back to the
	/// <see cref="PseudoAuthenticator"/>
	/// if the HTTP endpoint does not trigger an SPNEGO authentication
	/// sequence.
	/// </summary>
	public class KerberosAuthenticator : org.apache.hadoop.security.authentication.client.Authenticator
	{
		private static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.security.authentication.client.KerberosAuthenticator))
			);

		/// <summary>HTTP header used by the SPNEGO server endpoint during an authentication sequence.
		/// 	</summary>
		public const string WWW_AUTHENTICATE = "WWW-Authenticate";

		/// <summary>HTTP header used by the SPNEGO client endpoint during an authentication sequence.
		/// 	</summary>
		public const string AUTHORIZATION = "Authorization";

		/// <summary>HTTP header prefix used by the SPNEGO client/server endpoints during an authentication sequence.
		/// 	</summary>
		public const string NEGOTIATE = "Negotiate";

		private const string AUTH_HTTP_METHOD = "OPTIONS";

		private class KerberosConfiguration : javax.security.auth.login.Configuration
		{
			private static readonly string OS_LOGIN_MODULE_NAME;

			private static readonly bool windows = Sharpen.Runtime.getProperty("os.name").StartsWith
				("Windows");

			private static readonly bool is64Bit = Sharpen.Runtime.getProperty("os.arch").contains
				("64");

			private static readonly bool aix = Sharpen.Runtime.getProperty("os.name").Equals(
				"AIX");

			/*
			* Defines the Kerberos configuration that will be used to obtain the Kerberos principal from the
			* Kerberos cache.
			*/
			/* Return the OS login module class name */
			private static string getOSLoginModuleName()
			{
				if (org.apache.hadoop.util.PlatformName.IBM_JAVA)
				{
					if (windows)
					{
						return is64Bit ? "com.ibm.security.auth.module.Win64LoginModule" : "com.ibm.security.auth.module.NTLoginModule";
					}
					else
					{
						if (aix)
						{
							return is64Bit ? "com.ibm.security.auth.module.AIX64LoginModule" : "com.ibm.security.auth.module.AIXLoginModule";
						}
						else
						{
							return "com.ibm.security.auth.module.LinuxLoginModule";
						}
					}
				}
				else
				{
					return windows ? "com.sun.security.auth.module.NTLoginModule" : "com.sun.security.auth.module.UnixLoginModule";
				}
			}

			static KerberosConfiguration()
			{
				OS_LOGIN_MODULE_NAME = getOSLoginModuleName();
			}

			private static readonly javax.security.auth.login.AppConfigurationEntry OS_SPECIFIC_LOGIN
				 = new javax.security.auth.login.AppConfigurationEntry(OS_LOGIN_MODULE_NAME, javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag
				.REQUIRED, new System.Collections.Generic.Dictionary<string, string>());

			private static readonly System.Collections.Generic.IDictionary<string, string> USER_KERBEROS_OPTIONS
				 = new System.Collections.Generic.Dictionary<string, string>();

			static KerberosConfiguration()
			{
				string ticketCache = Sharpen.Runtime.getenv("KRB5CCNAME");
				if (org.apache.hadoop.util.PlatformName.IBM_JAVA)
				{
					USER_KERBEROS_OPTIONS["useDefaultCcache"] = "true";
				}
				else
				{
					USER_KERBEROS_OPTIONS["doNotPrompt"] = "true";
					USER_KERBEROS_OPTIONS["useTicketCache"] = "true";
				}
				if (ticketCache != null)
				{
					if (org.apache.hadoop.util.PlatformName.IBM_JAVA)
					{
						// The first value searched when "useDefaultCcache" is used.
						Sharpen.Runtime.setProperty("KRB5CCNAME", ticketCache);
					}
					else
					{
						USER_KERBEROS_OPTIONS["ticketCache"] = ticketCache;
					}
				}
				USER_KERBEROS_OPTIONS["renewTGT"] = "true";
			}

			private static readonly javax.security.auth.login.AppConfigurationEntry USER_KERBEROS_LOGIN
				 = new javax.security.auth.login.AppConfigurationEntry(org.apache.hadoop.security.authentication.util.KerberosUtil
				.getKrb5LoginModuleName(), javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag
				.OPTIONAL, USER_KERBEROS_OPTIONS);

			private static readonly javax.security.auth.login.AppConfigurationEntry[] USER_KERBEROS_CONF
				 = new javax.security.auth.login.AppConfigurationEntry[] { OS_SPECIFIC_LOGIN, USER_KERBEROS_LOGIN
				 };

			public override javax.security.auth.login.AppConfigurationEntry[] getAppConfigurationEntry
				(string appName)
			{
				return USER_KERBEROS_CONF;
			}
		}

		private java.net.URL url;

		private java.net.HttpURLConnection conn;

		private org.apache.commons.codec.binary.Base64 base64;

		private org.apache.hadoop.security.authentication.client.ConnectionConfigurator connConfigurator;

		/// <summary>
		/// Sets a
		/// <see cref="ConnectionConfigurator"/>
		/// instance to use for
		/// configuring connections.
		/// </summary>
		/// <param name="configurator">
		/// the
		/// <see cref="ConnectionConfigurator"/>
		/// instance.
		/// </param>
		public virtual void setConnectionConfigurator(org.apache.hadoop.security.authentication.client.ConnectionConfigurator
			 configurator)
		{
			connConfigurator = configurator;
		}

		/// <summary>Performs SPNEGO authentication against the specified URL.</summary>
		/// <remarks>
		/// Performs SPNEGO authentication against the specified URL.
		/// <p>
		/// If a token is given it does a NOP and returns the given token.
		/// <p>
		/// If no token is given, it will perform the SPNEGO authentication sequence using an
		/// HTTP <code>OPTIONS</code> request.
		/// </remarks>
		/// <param name="url">the URl to authenticate against.</param>
		/// <param name="token">the authentication token being used for the user.</param>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="AuthenticationException">if an authentication error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		public virtual void authenticate(java.net.URL url, org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token)
		{
			if (!token.isSet())
			{
				this.url = url;
				base64 = new org.apache.commons.codec.binary.Base64(0);
				conn = (java.net.HttpURLConnection)url.openConnection();
				if (connConfigurator != null)
				{
					conn = connConfigurator.configure(conn);
				}
				conn.setRequestMethod(AUTH_HTTP_METHOD);
				conn.connect();
				bool needFallback = false;
				if (conn.getResponseCode() == java.net.HttpURLConnection.HTTP_OK)
				{
					LOG.debug("JDK performed authentication on our behalf.");
					// If the JDK already did the SPNEGO back-and-forth for
					// us, just pull out the token.
					org.apache.hadoop.security.authentication.client.AuthenticatedURL.extractToken(conn
						, token);
					if (isTokenKerberos(token))
					{
						return;
					}
					needFallback = true;
				}
				if (!needFallback && isNegotiate())
				{
					LOG.debug("Performing our own SPNEGO sequence.");
					doSpnegoSequence(token);
				}
				else
				{
					LOG.debug("Using fallback authenticator sequence.");
					org.apache.hadoop.security.authentication.client.Authenticator auth = getFallBackAuthenticator
						();
					// Make sure that the fall back authenticator have the same
					// ConnectionConfigurator, since the method might be overridden.
					// Otherwise the fall back authenticator might not have the information
					// to make the connection (e.g., SSL certificates)
					auth.setConnectionConfigurator(connConfigurator);
					auth.authenticate(url, token);
				}
			}
		}

		/// <summary>
		/// If the specified URL does not support SPNEGO authentication, a fallback
		/// <see cref="Authenticator"/>
		/// will be used.
		/// <p>
		/// This implementation returns a
		/// <see cref="PseudoAuthenticator"/>
		/// .
		/// </summary>
		/// <returns>
		/// the fallback
		/// <see cref="Authenticator"/>
		/// .
		/// </returns>
		protected internal virtual org.apache.hadoop.security.authentication.client.Authenticator
			 getFallBackAuthenticator()
		{
			org.apache.hadoop.security.authentication.client.Authenticator auth = new org.apache.hadoop.security.authentication.client.PseudoAuthenticator
				();
			if (connConfigurator != null)
			{
				auth.setConnectionConfigurator(connConfigurator);
			}
			return auth;
		}

		/*
		* Check if the passed token is of type "kerberos" or "kerberos-dt"
		*/
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		private bool isTokenKerberos(org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token)
		{
			if (token.isSet())
			{
				org.apache.hadoop.security.authentication.util.AuthToken aToken = org.apache.hadoop.security.authentication.util.AuthToken
					.parse(token.ToString());
				if (aToken.getType().Equals("kerberos") || aToken.getType().Equals("kerberos-dt"))
				{
					return true;
				}
			}
			return false;
		}

		/*
		* Indicates if the response is starting a SPNEGO negotiation.
		*/
		/// <exception cref="System.IO.IOException"/>
		private bool isNegotiate()
		{
			bool negotiate = false;
			if (conn.getResponseCode() == java.net.HttpURLConnection.HTTP_UNAUTHORIZED)
			{
				string authHeader = conn.getHeaderField(WWW_AUTHENTICATE);
				negotiate = authHeader != null && authHeader.Trim().StartsWith(NEGOTIATE);
			}
			return negotiate;
		}

		/// <summary>
		/// Implements the SPNEGO authentication sequence interaction using the current default principal
		/// in the Kerberos cache (normally set via kinit).
		/// </summary>
		/// <param name="token">the authentication token being used for the user.</param>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		/// <exception cref="AuthenticationException">if an authentication error occurred.</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		private void doSpnegoSequence(org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token
			 token)
		{
			try
			{
				java.security.AccessControlContext context = java.security.AccessController.getContext
					();
				javax.security.auth.Subject subject = javax.security.auth.Subject.getSubject(context
					);
				if (subject == null || (subject.getPrivateCredentials<javax.security.auth.kerberos.KerberosKey
					>().isEmpty() && subject.getPrivateCredentials<javax.security.auth.kerberos.KerberosTicket
					>().isEmpty()))
				{
					LOG.debug("No subject in context, logging in");
					subject = new javax.security.auth.Subject();
					javax.security.auth.login.LoginContext login = new javax.security.auth.login.LoginContext
						(string.Empty, subject, null, new org.apache.hadoop.security.authentication.client.KerberosAuthenticator.KerberosConfiguration
						());
					login.login();
				}
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Using subject: " + subject);
				}
				javax.security.auth.Subject.doAs(subject, new _PrivilegedExceptionAction_287(this
					));
			}
			catch (java.security.PrivilegedActionException ex)
			{
				// Loop while the context is still not established
				throw new org.apache.hadoop.security.authentication.client.AuthenticationException
					(ex.getException());
			}
			catch (javax.security.auth.login.LoginException ex)
			{
				throw new org.apache.hadoop.security.authentication.client.AuthenticationException
					(ex);
			}
			org.apache.hadoop.security.authentication.client.AuthenticatedURL.extractToken(conn
				, token);
		}

		private sealed class _PrivilegedExceptionAction_287 : java.security.PrivilegedExceptionAction
			<java.lang.Void>
		{
			public _PrivilegedExceptionAction_287(KerberosAuthenticator _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public java.lang.Void run()
			{
				org.ietf.jgss.GSSContext gssContext = null;
				try
				{
					org.ietf.jgss.GSSManager gssManager = org.ietf.jgss.GSSManager.getInstance();
					string servicePrincipal = org.apache.hadoop.security.authentication.util.KerberosUtil
						.getServicePrincipal("HTTP", this._enclosing.url.getHost());
					org.ietf.jgss.Oid oid = org.apache.hadoop.security.authentication.util.KerberosUtil
						.getOidInstance("NT_GSS_KRB5_PRINCIPAL");
					org.ietf.jgss.GSSName serviceName = gssManager.createName(servicePrincipal, oid);
					oid = org.apache.hadoop.security.authentication.util.KerberosUtil.getOidInstance(
						"GSS_KRB5_MECH_OID");
					gssContext = gssManager.createContext(serviceName, oid, null, org.ietf.jgss.GSSContext
						.DEFAULT_LIFETIME);
					gssContext.requestCredDeleg(true);
					gssContext.requestMutualAuth(true);
					byte[] inToken = new byte[0];
					byte[] outToken;
					bool established = false;
					while (!established)
					{
						outToken = gssContext.initSecContext(inToken, 0, inToken.Length);
						if (outToken != null)
						{
							this._enclosing.sendToken(outToken);
						}
						if (!gssContext.isEstablished())
						{
							inToken = this._enclosing.readToken();
						}
						else
						{
							established = true;
						}
					}
				}
				finally
				{
					if (gssContext != null)
					{
						gssContext.dispose();
						gssContext = null;
					}
				}
				return null;
			}

			private readonly KerberosAuthenticator _enclosing;
		}

		/*
		* Sends the Kerberos token to the server.
		*/
		/// <exception cref="System.IO.IOException"/>
		private void sendToken(byte[] outToken)
		{
			string token = base64.encodeToString(outToken);
			conn = (java.net.HttpURLConnection)url.openConnection();
			if (connConfigurator != null)
			{
				conn = connConfigurator.configure(conn);
			}
			conn.setRequestMethod(AUTH_HTTP_METHOD);
			conn.setRequestProperty(AUTHORIZATION, NEGOTIATE + " " + token);
			conn.connect();
		}

		/*
		* Retrieves the Kerberos token returned by the server.
		*/
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		private byte[] readToken()
		{
			int status = conn.getResponseCode();
			if (status == java.net.HttpURLConnection.HTTP_OK || status == java.net.HttpURLConnection
				.HTTP_UNAUTHORIZED)
			{
				string authHeader = conn.getHeaderField(WWW_AUTHENTICATE);
				if (authHeader == null || !authHeader.Trim().StartsWith(NEGOTIATE))
				{
					throw new org.apache.hadoop.security.authentication.client.AuthenticationException
						("Invalid SPNEGO sequence, '" + WWW_AUTHENTICATE + "' header incorrect: " + authHeader
						);
				}
				string negotiation = Sharpen.Runtime.substring(authHeader.Trim(), (NEGOTIATE + " "
					).Length).Trim();
				return base64.decode(negotiation);
			}
			throw new org.apache.hadoop.security.authentication.client.AuthenticationException
				("Invalid SPNEGO sequence, status code: " + status);
		}
	}
}
