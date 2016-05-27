using System;
using System.Collections.Generic;
using Javax.Security.Auth;
using Javax.Security.Auth.Kerberos;
using Javax.Security.Auth.Login;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Client
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
	public class KerberosAuthenticator : Authenticator
	{
		private static Logger Log = LoggerFactory.GetLogger(typeof(KerberosAuthenticator)
			);

		/// <summary>HTTP header used by the SPNEGO server endpoint during an authentication sequence.
		/// 	</summary>
		public const string WwwAuthenticate = "WWW-Authenticate";

		/// <summary>HTTP header used by the SPNEGO client endpoint during an authentication sequence.
		/// 	</summary>
		public const string Authorization = "Authorization";

		/// <summary>HTTP header prefix used by the SPNEGO client/server endpoints during an authentication sequence.
		/// 	</summary>
		public const string Negotiate = "Negotiate";

		private const string AuthHttpMethod = "OPTIONS";

		private class KerberosConfiguration : Configuration
		{
			private static readonly string OsLoginModuleName;

			private static readonly bool windows = Runtime.GetProperty("os.name").StartsWith(
				"Windows");

			private static readonly bool is64Bit = Runtime.GetProperty("os.arch").Contains("64"
				);

			private static readonly bool aix = Runtime.GetProperty("os.name").Equals("AIX");

			/*
			* Defines the Kerberos configuration that will be used to obtain the Kerberos principal from the
			* Kerberos cache.
			*/
			/* Return the OS login module class name */
			private static string GetOSLoginModuleName()
			{
				if (PlatformName.IbmJava)
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
				OsLoginModuleName = GetOSLoginModuleName();
			}

			private static readonly AppConfigurationEntry OsSpecificLogin = new AppConfigurationEntry
				(OsLoginModuleName, AppConfigurationEntry.LoginModuleControlFlag.Required, new Dictionary
				<string, string>());

			private static readonly IDictionary<string, string> UserKerberosOptions = new Dictionary
				<string, string>();

			static KerberosConfiguration()
			{
				string ticketCache = Runtime.Getenv("KRB5CCNAME");
				if (PlatformName.IbmJava)
				{
					UserKerberosOptions["useDefaultCcache"] = "true";
				}
				else
				{
					UserKerberosOptions["doNotPrompt"] = "true";
					UserKerberosOptions["useTicketCache"] = "true";
				}
				if (ticketCache != null)
				{
					if (PlatformName.IbmJava)
					{
						// The first value searched when "useDefaultCcache" is used.
						Runtime.SetProperty("KRB5CCNAME", ticketCache);
					}
					else
					{
						UserKerberosOptions["ticketCache"] = ticketCache;
					}
				}
				UserKerberosOptions["renewTGT"] = "true";
			}

			private static readonly AppConfigurationEntry UserKerberosLogin = new AppConfigurationEntry
				(KerberosUtil.GetKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag
				.Optional, UserKerberosOptions);

			private static readonly AppConfigurationEntry[] UserKerberosConf = new AppConfigurationEntry
				[] { OsSpecificLogin, UserKerberosLogin };

			public override AppConfigurationEntry[] GetAppConfigurationEntry(string appName)
			{
				return UserKerberosConf;
			}
		}

		private Uri url;

		private HttpURLConnection conn;

		private Base64 base64;

		private ConnectionConfigurator connConfigurator;

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
		public virtual void SetConnectionConfigurator(ConnectionConfigurator configurator
			)
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		public virtual void Authenticate(Uri url, AuthenticatedURL.Token token)
		{
			if (!token.IsSet())
			{
				this.url = url;
				base64 = new Base64(0);
				conn = (HttpURLConnection)url.OpenConnection();
				if (connConfigurator != null)
				{
					conn = connConfigurator.Configure(conn);
				}
				conn.SetRequestMethod(AuthHttpMethod);
				conn.Connect();
				bool needFallback = false;
				if (conn.GetResponseCode() == HttpURLConnection.HttpOk)
				{
					Log.Debug("JDK performed authentication on our behalf.");
					// If the JDK already did the SPNEGO back-and-forth for
					// us, just pull out the token.
					AuthenticatedURL.ExtractToken(conn, token);
					if (IsTokenKerberos(token))
					{
						return;
					}
					needFallback = true;
				}
				if (!needFallback && IsNegotiate())
				{
					Log.Debug("Performing our own SPNEGO sequence.");
					DoSpnegoSequence(token);
				}
				else
				{
					Log.Debug("Using fallback authenticator sequence.");
					Authenticator auth = GetFallBackAuthenticator();
					// Make sure that the fall back authenticator have the same
					// ConnectionConfigurator, since the method might be overridden.
					// Otherwise the fall back authenticator might not have the information
					// to make the connection (e.g., SSL certificates)
					auth.SetConnectionConfigurator(connConfigurator);
					auth.Authenticate(url, token);
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
		protected internal virtual Authenticator GetFallBackAuthenticator()
		{
			Authenticator auth = new PseudoAuthenticator();
			if (connConfigurator != null)
			{
				auth.SetConnectionConfigurator(connConfigurator);
			}
			return auth;
		}

		/*
		* Check if the passed token is of type "kerberos" or "kerberos-dt"
		*/
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		private bool IsTokenKerberos(AuthenticatedURL.Token token)
		{
			if (token.IsSet())
			{
				AuthToken aToken = AuthToken.Parse(token.ToString());
				if (aToken.GetType().Equals("kerberos") || aToken.GetType().Equals("kerberos-dt"))
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
		private bool IsNegotiate()
		{
			bool negotiate = false;
			if (conn.GetResponseCode() == HttpURLConnection.HttpUnauthorized)
			{
				string authHeader = conn.GetHeaderField(WwwAuthenticate);
				negotiate = authHeader != null && authHeader.Trim().StartsWith(Negotiate);
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
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		private void DoSpnegoSequence(AuthenticatedURL.Token token)
		{
			try
			{
				AccessControlContext context = AccessController.GetContext();
				Subject subject = Subject.GetSubject(context);
				if (subject == null || (subject.GetPrivateCredentials<KerberosKey>().IsEmpty() &&
					 subject.GetPrivateCredentials<KerberosTicket>().IsEmpty()))
				{
					Log.Debug("No subject in context, logging in");
					subject = new Subject();
					LoginContext login = new LoginContext(string.Empty, subject, null, new KerberosAuthenticator.KerberosConfiguration
						());
					login.Login();
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Using subject: " + subject);
				}
				Subject.DoAs(subject, new _PrivilegedExceptionAction_287(this));
			}
			catch (PrivilegedActionException ex)
			{
				// Loop while the context is still not established
				throw new AuthenticationException(ex.GetException());
			}
			catch (LoginException ex)
			{
				throw new AuthenticationException(ex);
			}
			AuthenticatedURL.ExtractToken(conn, token);
		}

		private sealed class _PrivilegedExceptionAction_287 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_287(KerberosAuthenticator _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				Sharpen.GSSContext gssContext = null;
				try
				{
					GSSManager gssManager = GSSManager.GetInstance();
					string servicePrincipal = KerberosUtil.GetServicePrincipal("HTTP", this._enclosing
						.url.GetHost());
					Oid oid = KerberosUtil.GetOidInstance("NT_GSS_KRB5_PRINCIPAL");
					GSSName serviceName = gssManager.CreateName(servicePrincipal, oid);
					oid = KerberosUtil.GetOidInstance("GSS_KRB5_MECH_OID");
					gssContext = gssManager.CreateContext(serviceName, oid, null, Sharpen.GSSContext.
						DefaultLifetime);
					gssContext.RequestCredDeleg(true);
					gssContext.RequestMutualAuth(true);
					byte[] inToken = new byte[0];
					byte[] outToken;
					bool established = false;
					while (!established)
					{
						outToken = gssContext.InitSecContext(inToken, 0, inToken.Length);
						if (outToken != null)
						{
							this._enclosing.SendToken(outToken);
						}
						if (!gssContext.IsEstablished())
						{
							inToken = this._enclosing.ReadToken();
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
						gssContext.Dispose();
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
		private void SendToken(byte[] outToken)
		{
			string token = base64.EncodeToString(outToken);
			conn = (HttpURLConnection)url.OpenConnection();
			if (connConfigurator != null)
			{
				conn = connConfigurator.Configure(conn);
			}
			conn.SetRequestMethod(AuthHttpMethod);
			conn.SetRequestProperty(Authorization, Negotiate + " " + token);
			conn.Connect();
		}

		/*
		* Retrieves the Kerberos token returned by the server.
		*/
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		private byte[] ReadToken()
		{
			int status = conn.GetResponseCode();
			if (status == HttpURLConnection.HttpOk || status == HttpURLConnection.HttpUnauthorized)
			{
				string authHeader = conn.GetHeaderField(WwwAuthenticate);
				if (authHeader == null || !authHeader.Trim().StartsWith(Negotiate))
				{
					throw new AuthenticationException("Invalid SPNEGO sequence, '" + WwwAuthenticate 
						+ "' header incorrect: " + authHeader);
				}
				string negotiation = Sharpen.Runtime.Substring(authHeader.Trim(), (Negotiate + " "
					).Length).Trim();
				return base64.Decode(negotiation);
			}
			throw new AuthenticationException("Invalid SPNEGO sequence, status code: " + status
				);
		}
	}
}
