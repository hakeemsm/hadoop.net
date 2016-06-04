using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Javax.Security.Auth;
using Javax.Security.Auth.Callback;
using Javax.Security.Auth.Kerberos;
using Javax.Security.Auth.Login;
using Javax.Security.Auth.Spi;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>User and group information for Hadoop.</summary>
	/// <remarks>
	/// User and group information for Hadoop.
	/// This class wraps around a JAAS Subject and provides methods to determine the
	/// user's username and groups. It supports both the Windows, Unix and Kerberos
	/// login modules.
	/// </remarks>
	public class UserGroupInformation
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.UserGroupInformation
			));

		/// <summary>Percentage of the ticket window to use before we renew ticket.</summary>
		private const float TicketRenewWindow = 0.80f;

		private static bool shouldRenewImmediatelyForTests = false;

		internal const string HadoopUserName = "HADOOP_USER_NAME";

		internal const string HadoopProxyUser = "HADOOP_PROXY_USER";

		/// <summary>
		/// For the purposes of unit tests, we want to test login
		/// from keytab and don't want to wait until the renew
		/// window (controlled by TICKET_RENEW_WINDOW).
		/// </summary>
		/// <param name="immediate">true if we should login without waiting for ticket window
		/// 	</param>
		[VisibleForTesting]
		internal static void SetShouldRenewImmediatelyForTests(bool immediate)
		{
			shouldRenewImmediatelyForTests = immediate;
		}

		/// <summary>
		/// UgiMetrics maintains UGI activity statistics
		/// and publishes them through the metrics interfaces.
		/// </summary>
		internal class UgiMetrics
		{
			internal readonly MetricsRegistry registry = new MetricsRegistry("UgiMetrics");

			internal MutableRate loginSuccess;

			internal MutableRate loginFailure;

			internal MutableRate getGroups;

			internal MutableQuantiles[] getGroupsQuantiles;

			internal static UserGroupInformation.UgiMetrics Create()
			{
				return DefaultMetricsSystem.Instance().Register(new UserGroupInformation.UgiMetrics
					());
			}

			internal virtual void AddGetGroups(long latency)
			{
				getGroups.Add(latency);
				if (getGroupsQuantiles != null)
				{
					foreach (MutableQuantiles q in getGroupsQuantiles)
					{
						q.Add(latency);
					}
				}
			}
		}

		/// <summary>
		/// A login module that looks at the Kerberos, Unix, or Windows principal and
		/// adds the corresponding UserName.
		/// </summary>
		public class HadoopLoginModule : LoginModule
		{
			private Subject subject;

			/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
			public virtual bool Abort()
			{
				return true;
			}

			private T GetCanonicalUser<T>()
				where T : Principal
			{
				System.Type cls = typeof(T);
				foreach (T user in subject.GetPrincipals(cls))
				{
					return user;
				}
				return null;
			}

			/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
			public virtual bool Commit()
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("hadoop login commit");
				}
				// if we already have a user, we are done.
				if (!subject.GetPrincipals<User>().IsEmpty())
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("using existing subject:" + subject.GetPrincipals());
					}
					return true;
				}
				Principal user = null;
				// if we are using kerberos, try it out
				if (IsAuthenticationMethodEnabled(UserGroupInformation.AuthenticationMethod.Kerberos
					))
				{
					user = GetCanonicalUser<KerberosPrincipal>();
					if (Log.IsDebugEnabled())
					{
						Log.Debug("using kerberos user:" + user);
					}
				}
				//If we don't have a kerberos user and security is disabled, check
				//if user is specified in the environment or properties
				if (!IsSecurityEnabled() && (user == null))
				{
					string envUser = Runtime.Getenv(HadoopUserName);
					if (envUser == null)
					{
						envUser = Runtime.GetProperty(HadoopUserName);
					}
					user = envUser == null ? null : new User(envUser);
				}
				// use the OS user
				if (user == null)
				{
					user = GetCanonicalUser(OsPrincipalClass);
					if (Log.IsDebugEnabled())
					{
						Log.Debug("using local user:" + user);
					}
				}
				// if we found the user, add our principal
				if (user != null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Using user: \"" + user + "\" with name " + user.GetName());
					}
					User userEntry = null;
					try
					{
						userEntry = new User(user.GetName());
					}
					catch (Exception e)
					{
						throw (LoginException)(Sharpen.Extensions.InitCause(new LoginException(e.ToString
							()), e));
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("User entry: \"" + userEntry.ToString() + "\"");
					}
					subject.GetPrincipals().AddItem(userEntry);
					return true;
				}
				Log.Error("Can't find user in " + subject);
				throw new LoginException("Can't find user name");
			}

			public virtual void Initialize(Subject subject, CallbackHandler callbackHandler, 
				IDictionary<string, object> sharedState, IDictionary<string, object> options)
			{
				this.subject = subject;
			}

			/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
			public virtual bool Login()
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("hadoop login");
				}
				return true;
			}

			/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
			public virtual bool Logout()
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("hadoop logout");
				}
				return true;
			}
		}

		/// <summary>Metrics to track UGI activity</summary>
		internal static UserGroupInformation.UgiMetrics metrics = UserGroupInformation.UgiMetrics
			.Create();

		/// <summary>The auth method to use</summary>
		private static UserGroupInformation.AuthenticationMethod authenticationMethod;

		/// <summary>Server-side groups fetching service</summary>
		private static Groups groups;

		/// <summary>The configuration to use</summary>
		private static Configuration conf;

		/// <summary>Leave 10 minutes between relogin attempts.</summary>
		private const long MinTimeBeforeRelogin = 10 * 60 * 1000L;

		/// <summary>Environment variable pointing to the token cache file</summary>
		public const string HadoopTokenFileLocation = "HADOOP_TOKEN_FILE_LOCATION";

		/// <summary>A method to initialize the fields that depend on a configuration.</summary>
		/// <remarks>
		/// A method to initialize the fields that depend on a configuration.
		/// Must be called before useKerberos or groups is used.
		/// </remarks>
		private static void EnsureInitialized()
		{
			if (conf == null)
			{
				lock (typeof(UserGroupInformation))
				{
					if (conf == null)
					{
						// someone might have beat us
						Initialize(new Configuration(), false);
					}
				}
			}
		}

		/// <summary>Initialize UGI and related classes.</summary>
		/// <param name="conf">the configuration to use</param>
		private static void Initialize(Configuration conf, bool overrideNameRules)
		{
			lock (typeof(UserGroupInformation))
			{
				authenticationMethod = SecurityUtil.GetAuthenticationMethod(conf);
				if (overrideNameRules || !HadoopKerberosName.HasRulesBeenSet())
				{
					try
					{
						HadoopKerberosName.SetConfiguration(conf);
					}
					catch (IOException ioe)
					{
						throw new RuntimeException("Problem with Kerberos auth_to_local name configuration"
							, ioe);
					}
				}
				// If we haven't set up testing groups, use the configuration to find it
				if (!(groups is UserGroupInformation.TestingGroups))
				{
					groups = Groups.GetUserToGroupsMappingService(conf);
				}
				UserGroupInformation.conf = conf;
				if (metrics.getGroupsQuantiles == null)
				{
					int[] intervals = conf.GetInts(CommonConfigurationKeys.HadoopUserGroupMetricsPercentilesIntervals
						);
					if (intervals != null && intervals.Length > 0)
					{
						int length = intervals.Length;
						MutableQuantiles[] getGroupsQuantiles = new MutableQuantiles[length];
						for (int i = 0; i < length; i++)
						{
							getGroupsQuantiles[i] = metrics.registry.NewQuantiles("getGroups" + intervals[i] 
								+ "s", "Get groups", "ops", "latency", intervals[i]);
						}
						metrics.getGroupsQuantiles = getGroupsQuantiles;
					}
				}
			}
		}

		/// <summary>Set the static configuration for UGI.</summary>
		/// <remarks>
		/// Set the static configuration for UGI.
		/// In particular, set the security authentication mechanism and the
		/// group look up service.
		/// </remarks>
		/// <param name="conf">the configuration to use</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static void SetConfiguration(Configuration conf)
		{
			Initialize(conf, true);
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal static void Reset()
		{
			authenticationMethod = null;
			conf = null;
			groups = null;
			SetLoginUser(null);
			HadoopKerberosName.SetRules(null);
		}

		/// <summary>
		/// Determine if UserGroupInformation is using Kerberos to determine
		/// user identities or is relying on simple authentication
		/// </summary>
		/// <returns>true if UGI is working in a secure environment</returns>
		public static bool IsSecurityEnabled()
		{
			return !IsAuthenticationMethodEnabled(UserGroupInformation.AuthenticationMethod.Simple
				);
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Evolving]
		private static bool IsAuthenticationMethodEnabled(UserGroupInformation.AuthenticationMethod
			 method)
		{
			EnsureInitialized();
			return (authenticationMethod == method);
		}

		/// <summary>Information about the logged in user.</summary>
		private static UserGroupInformation loginUser = null;

		private static string keytabPrincipal = null;

		private static string keytabFile = null;

		private readonly Subject subject;

		private readonly User user;

		private readonly bool isKeytab;

		private readonly bool isKrbTkt;

		private static string OsLoginModuleName;

		private static Type OsPrincipalClass;

		private static readonly bool windows = Runtime.GetProperty("os.name").StartsWith(
			"Windows");

		private static readonly bool is64Bit = Runtime.GetProperty("os.arch").Contains("64"
			);

		private static readonly bool aix = Runtime.GetProperty("os.name").Equals("AIX");

		// All non-static fields must be read-only caches that come from the subject.
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

		/* Return the OS principal class */
		private static Type GetOsPrincipalClass()
		{
			ClassLoader cl = ClassLoader.GetSystemClassLoader();
			try
			{
				string principalClass = null;
				if (PlatformName.IbmJava)
				{
					if (is64Bit)
					{
						principalClass = "com.ibm.security.auth.UsernamePrincipal";
					}
					else
					{
						if (windows)
						{
							principalClass = "com.ibm.security.auth.NTUserPrincipal";
						}
						else
						{
							if (aix)
							{
								principalClass = "com.ibm.security.auth.AIXPrincipal";
							}
							else
							{
								principalClass = "com.ibm.security.auth.LinuxPrincipal";
							}
						}
					}
				}
				else
				{
					principalClass = windows ? "com.sun.security.auth.NTUserPrincipal" : "com.sun.security.auth.UnixPrincipal";
				}
				return (Type)cl.LoadClass(principalClass);
			}
			catch (TypeLoadException e)
			{
				Log.Error("Unable to find JAAS classes:" + e.Message);
			}
			return null;
		}

		static UserGroupInformation()
		{
			OsLoginModuleName = GetOSLoginModuleName();
			OsPrincipalClass = GetOsPrincipalClass();
		}

		private class RealUser : Principal
		{
			private readonly UserGroupInformation realUser;

			internal RealUser(UserGroupInformation realUser)
			{
				this.realUser = realUser;
			}

			public virtual string GetName()
			{
				return realUser.GetUserName();
			}

			public virtual UserGroupInformation GetRealUser()
			{
				return realUser;
			}

			public override bool Equals(object o)
			{
				if (this == o)
				{
					return true;
				}
				else
				{
					if (o == null || GetType() != o.GetType())
					{
						return false;
					}
					else
					{
						return realUser.Equals(((UserGroupInformation.RealUser)o).realUser);
					}
				}
			}

			public override int GetHashCode()
			{
				return realUser.GetHashCode();
			}

			public override string ToString()
			{
				return realUser.ToString();
			}
		}

		/// <summary>
		/// A JAAS configuration that defines the login modules that we want
		/// to use for login.
		/// </summary>
		private class HadoopConfiguration : Configuration
		{
			private const string SimpleConfigName = "hadoop-simple";

			private const string UserKerberosConfigName = "hadoop-user-kerberos";

			private const string KeytabKerberosConfigName = "hadoop-keytab-kerberos";

			private static readonly IDictionary<string, string> BasicJaasOptions = new Dictionary
				<string, string>();

			static HadoopConfiguration()
			{
				string jaasEnvVar = Runtime.Getenv("HADOOP_JAAS_DEBUG");
				if (jaasEnvVar != null && Sharpen.Runtime.EqualsIgnoreCase("true", jaasEnvVar))
				{
					BasicJaasOptions["debug"] = "true";
				}
			}

			private static readonly AppConfigurationEntry OsSpecificLogin = new AppConfigurationEntry
				(OsLoginModuleName, AppConfigurationEntry.LoginModuleControlFlag.Required, BasicJaasOptions
				);

			private static readonly AppConfigurationEntry HadoopLogin = new AppConfigurationEntry
				(typeof(UserGroupInformation.HadoopLoginModule).FullName, AppConfigurationEntry.LoginModuleControlFlag
				.Required, BasicJaasOptions);

			private static readonly IDictionary<string, string> UserKerberosOptions = new Dictionary
				<string, string>();

			static HadoopConfiguration()
			{
				if (PlatformName.IbmJava)
				{
					UserKerberosOptions["useDefaultCcache"] = "true";
				}
				else
				{
					UserKerberosOptions["doNotPrompt"] = "true";
					UserKerberosOptions["useTicketCache"] = "true";
				}
				string ticketCache = Runtime.Getenv("KRB5CCNAME");
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
				UserKerberosOptions.PutAll(BasicJaasOptions);
			}

			private static readonly AppConfigurationEntry UserKerberosLogin = new AppConfigurationEntry
				(KerberosUtil.GetKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag
				.Optional, UserKerberosOptions);

			private static readonly IDictionary<string, string> KeytabKerberosOptions = new Dictionary
				<string, string>();

			static HadoopConfiguration()
			{
				if (PlatformName.IbmJava)
				{
					KeytabKerberosOptions["credsType"] = "both";
				}
				else
				{
					KeytabKerberosOptions["doNotPrompt"] = "true";
					KeytabKerberosOptions["useKeyTab"] = "true";
					KeytabKerberosOptions["storeKey"] = "true";
				}
				KeytabKerberosOptions["refreshKrb5Config"] = "true";
				KeytabKerberosOptions.PutAll(BasicJaasOptions);
			}

			private static readonly AppConfigurationEntry KeytabKerberosLogin = new AppConfigurationEntry
				(KerberosUtil.GetKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag
				.Required, KeytabKerberosOptions);

			private static readonly AppConfigurationEntry[] SimpleConf = new AppConfigurationEntry
				[] { OsSpecificLogin, HadoopLogin };

			private static readonly AppConfigurationEntry[] UserKerberosConf = new AppConfigurationEntry
				[] { OsSpecificLogin, UserKerberosLogin, HadoopLogin };

			private static readonly AppConfigurationEntry[] KeytabKerberosConf = new AppConfigurationEntry
				[] { KeytabKerberosLogin, HadoopLogin };

			public override AppConfigurationEntry[] GetAppConfigurationEntry(string appName)
			{
				if (SimpleConfigName.Equals(appName))
				{
					return SimpleConf;
				}
				else
				{
					if (UserKerberosConfigName.Equals(appName))
					{
						return UserKerberosConf;
					}
					else
					{
						if (KeytabKerberosConfigName.Equals(appName))
						{
							if (PlatformName.IbmJava)
							{
								KeytabKerberosOptions["useKeytab"] = PrependFileAuthority(keytabFile);
							}
							else
							{
								KeytabKerberosOptions["keyTab"] = keytabFile;
							}
							KeytabKerberosOptions["principal"] = keytabPrincipal;
							return KeytabKerberosConf;
						}
					}
				}
				return null;
			}
		}

		private static string PrependFileAuthority(string keytabPath)
		{
			return keytabPath.StartsWith("file://") ? keytabPath : "file://" + keytabPath;
		}

		/// <summary>Represents a javax.security configuration that is created at runtime.</summary>
		private class DynamicConfiguration : Configuration
		{
			private AppConfigurationEntry[] ace;

			internal DynamicConfiguration(AppConfigurationEntry[] ace)
			{
				this.ace = ace;
			}

			public override AppConfigurationEntry[] GetAppConfigurationEntry(string appName)
			{
				return ace;
			}
		}

		/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
		private static LoginContext NewLoginContext(string appName, Subject subject, Configuration
			 loginConf)
		{
			// Temporarily switch the thread's ContextClassLoader to match this
			// class's classloader, so that we can properly load HadoopLoginModule
			// from the JAAS libraries.
			Sharpen.Thread t = Sharpen.Thread.CurrentThread();
			ClassLoader oldCCL = t.GetContextClassLoader();
			t.SetContextClassLoader(typeof(UserGroupInformation.HadoopLoginModule).GetClassLoader
				());
			try
			{
				return new LoginContext(appName, subject, null, loginConf);
			}
			finally
			{
				t.SetContextClassLoader(oldCCL);
			}
		}

		private LoginContext GetLogin()
		{
			return user.GetLogin();
		}

		private void SetLogin(LoginContext login)
		{
			user.SetLogin(login);
		}

		/// <summary>Create a UserGroupInformation for the given subject.</summary>
		/// <remarks>
		/// Create a UserGroupInformation for the given subject.
		/// This does not change the subject or acquire new credentials.
		/// </remarks>
		/// <param name="subject">the user's subject</param>
		internal UserGroupInformation(Subject subject)
		{
			this.subject = subject;
			this.user = subject.GetPrincipals<User>().GetEnumerator().Next();
			this.isKeytab = !subject.GetPrivateCredentials<KeyTab>().IsEmpty();
			this.isKrbTkt = !subject.GetPrivateCredentials<KerberosTicket>().IsEmpty();
		}

		/// <summary>checks if logged in using kerberos</summary>
		/// <returns>true if the subject logged via keytab or has a Kerberos TGT</returns>
		public virtual bool HasKerberosCredentials()
		{
			return isKeytab || isKrbTkt;
		}

		/// <summary>Return the current user, including any doAs in the current stack.</summary>
		/// <returns>the current user</returns>
		/// <exception cref="System.IO.IOException">if login fails</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static UserGroupInformation GetCurrentUser()
		{
			lock (typeof(UserGroupInformation))
			{
				AccessControlContext context = AccessController.GetContext();
				Subject subject = Subject.GetSubject(context);
				if (subject == null || subject.GetPrincipals<User>().IsEmpty())
				{
					return GetLoginUser();
				}
				else
				{
					return new UserGroupInformation(subject);
				}
			}
		}

		/// <summary>Find the most appropriate UserGroupInformation to use</summary>
		/// <param name="ticketCachePath">
		/// The Kerberos ticket cache path, or NULL
		/// if none is specfied
		/// </param>
		/// <param name="user">The user name, or NULL if none is specified.</param>
		/// <returns>The most appropriate UserGroupInformation</returns>
		/// <exception cref="System.IO.IOException"/>
		public static UserGroupInformation GetBestUGI(string ticketCachePath, string user
			)
		{
			if (ticketCachePath != null)
			{
				return GetUGIFromTicketCache(ticketCachePath, user);
			}
			else
			{
				if (user == null)
				{
					return GetCurrentUser();
				}
				else
				{
					return CreateRemoteUser(user);
				}
			}
		}

		/// <summary>Create a UserGroupInformation from a Kerberos ticket cache.</summary>
		/// <param name="user">
		/// The principal name to load from the ticket
		/// cache
		/// </param>
		/// <param name="ticketCachePath">the path to the ticket cache file</param>
		/// <exception cref="System.IO.IOException">if the kerberos login fails</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static UserGroupInformation GetUGIFromTicketCache(string ticketCache, string
			 user)
		{
			if (!IsAuthenticationMethodEnabled(UserGroupInformation.AuthenticationMethod.Kerberos
				))
			{
				return GetBestUGI(null, user);
			}
			try
			{
				IDictionary<string, string> krbOptions = new Dictionary<string, string>();
				if (PlatformName.IbmJava)
				{
					krbOptions["useDefaultCcache"] = "true";
					// The first value searched when "useDefaultCcache" is used.
					Runtime.SetProperty("KRB5CCNAME", ticketCache);
				}
				else
				{
					krbOptions["doNotPrompt"] = "true";
					krbOptions["useTicketCache"] = "true";
					krbOptions["useKeyTab"] = "false";
					krbOptions["ticketCache"] = ticketCache;
				}
				krbOptions["renewTGT"] = "false";
				krbOptions.PutAll(UserGroupInformation.HadoopConfiguration.BasicJaasOptions);
				AppConfigurationEntry ace = new AppConfigurationEntry(KerberosUtil.GetKrb5LoginModuleName
					(), AppConfigurationEntry.LoginModuleControlFlag.Required, krbOptions);
				UserGroupInformation.DynamicConfiguration dynConf = new UserGroupInformation.DynamicConfiguration
					(new AppConfigurationEntry[] { ace });
				LoginContext login = NewLoginContext(UserGroupInformation.HadoopConfiguration.UserKerberosConfigName
					, null, dynConf);
				login.Login();
				Subject loginSubject = login.GetSubject();
				ICollection<Principal> loginPrincipals = loginSubject.GetPrincipals();
				if (loginPrincipals.IsEmpty())
				{
					throw new RuntimeException("No login principals found!");
				}
				if (loginPrincipals.Count != 1)
				{
					Log.Warn("found more than one principal in the ticket cache file " + ticketCache);
				}
				User ugiUser = new User(loginPrincipals.GetEnumerator().Next().GetName(), UserGroupInformation.AuthenticationMethod
					.Kerberos, login);
				loginSubject.GetPrincipals().AddItem(ugiUser);
				UserGroupInformation ugi = new UserGroupInformation(loginSubject);
				ugi.SetLogin(login);
				ugi.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos);
				return ugi;
			}
			catch (LoginException le)
			{
				throw new IOException("failure to login using ticket cache file " + ticketCache, 
					le);
			}
		}

		/// <summary>Create a UserGroupInformation from a Subject with Kerberos principal.</summary>
		/// <param name="user">The KerberosPrincipal to use in UGI</param>
		/// <exception cref="System.IO.IOException">if the kerberos login fails</exception>
		public static UserGroupInformation GetUGIFromSubject(Subject subject)
		{
			if (subject == null)
			{
				throw new IOException("Subject must not be null");
			}
			if (subject.GetPrincipals<KerberosPrincipal>().IsEmpty())
			{
				throw new IOException("Provided Subject must contain a KerberosPrincipal");
			}
			KerberosPrincipal principal = subject.GetPrincipals<KerberosPrincipal>().GetEnumerator
				().Next();
			User ugiUser = new User(principal.GetName(), UserGroupInformation.AuthenticationMethod
				.Kerberos, null);
			subject.GetPrincipals().AddItem(ugiUser);
			UserGroupInformation ugi = new UserGroupInformation(subject);
			ugi.SetLogin(null);
			ugi.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos);
			return ugi;
		}

		/// <summary>Get the currently logged in user.</summary>
		/// <returns>the logged in user</returns>
		/// <exception cref="System.IO.IOException">if login fails</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static UserGroupInformation GetLoginUser()
		{
			lock (typeof(UserGroupInformation))
			{
				if (loginUser == null)
				{
					LoginUserFromSubject(null);
				}
				return loginUser;
			}
		}

		/// <summary>
		/// remove the login method that is followed by a space from the username
		/// e.g.
		/// </summary>
		/// <remarks>
		/// remove the login method that is followed by a space from the username
		/// e.g. "jack (auth:SIMPLE)" -&gt; "jack"
		/// </remarks>
		/// <param name="userName"/>
		/// <returns>userName without login method</returns>
		public static string TrimLoginMethod(string userName)
		{
			int spaceIndex = userName.IndexOf(' ');
			if (spaceIndex >= 0)
			{
				userName = Sharpen.Runtime.Substring(userName, 0, spaceIndex);
			}
			return userName;
		}

		/// <summary>Log in a user using the given subject</summary>
		/// <parma>
		/// subject the subject to use when logging in a user, or null to
		/// create a new subject.
		/// </parma>
		/// <exception cref="System.IO.IOException">if login fails</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static void LoginUserFromSubject(Subject subject)
		{
			lock (typeof(UserGroupInformation))
			{
				EnsureInitialized();
				try
				{
					if (subject == null)
					{
						subject = new Subject();
					}
					LoginContext login = NewLoginContext(authenticationMethod.GetLoginAppName(), subject
						, new UserGroupInformation.HadoopConfiguration());
					login.Login();
					UserGroupInformation realUser = new UserGroupInformation(subject);
					realUser.SetLogin(login);
					realUser.SetAuthenticationMethod(authenticationMethod);
					realUser = new UserGroupInformation(login.GetSubject());
					// If the HADOOP_PROXY_USER environment variable or property
					// is specified, create a proxy user as the logged in user.
					string proxyUser = Runtime.Getenv(HadoopProxyUser);
					if (proxyUser == null)
					{
						proxyUser = Runtime.GetProperty(HadoopProxyUser);
					}
					loginUser = proxyUser == null ? realUser : CreateProxyUser(proxyUser, realUser);
					string fileLocation = Runtime.Getenv(HadoopTokenFileLocation);
					if (fileLocation != null)
					{
						// Load the token storage file and put all of the tokens into the
						// user. Don't use the FileSystem API for reading since it has a lock
						// cycle (HADOOP-9212).
						Credentials cred = Credentials.ReadTokenStorageFile(new FilePath(fileLocation), conf
							);
						loginUser.AddCredentials(cred);
					}
					loginUser.SpawnAutoRenewalThreadForUserCreds();
				}
				catch (LoginException le)
				{
					Log.Debug("failure to login", le);
					throw new IOException("failure to login", le);
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("UGI loginUser:" + loginUser);
				}
			}
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		[VisibleForTesting]
		public static void SetLoginUser(UserGroupInformation ugi)
		{
			lock (typeof(UserGroupInformation))
			{
				// if this is to become stable, should probably logout the currently
				// logged in ugi if it's different
				loginUser = ugi;
			}
		}

		/// <summary>Is this user logged in from a keytab file?</summary>
		/// <returns>true if the credentials are from a keytab file.</returns>
		public virtual bool IsFromKeytab()
		{
			return isKeytab;
		}

		/// <summary>Get the Kerberos TGT</summary>
		/// <returns>the user's TGT or null if none was found</returns>
		private KerberosTicket GetTGT()
		{
			lock (this)
			{
				ICollection<KerberosTicket> tickets = subject.GetPrivateCredentials<KerberosTicket
					>();
				foreach (KerberosTicket ticket in tickets)
				{
					if (SecurityUtil.IsOriginalTGT(ticket))
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Found tgt " + ticket);
						}
						return ticket;
					}
				}
				return null;
			}
		}

		private long GetRefreshTime(KerberosTicket tgt)
		{
			long start = tgt.GetStartTime().GetTime();
			long end = tgt.GetEndTime().GetTime();
			return start + (long)((end - start) * TicketRenewWindow);
		}

		/// <summary>Spawn a thread to do periodic renewals of kerberos credentials</summary>
		private void SpawnAutoRenewalThreadForUserCreds()
		{
			if (IsSecurityEnabled())
			{
				//spawn thread only if we have kerb credentials
				if (user.GetAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.Kerberos
					 && !isKeytab)
				{
					Sharpen.Thread t = new Sharpen.Thread(new _Runnable_877(this));
					t.SetDaemon(true);
					t.SetName("TGT Renewer for " + GetUserName());
					t.Start();
				}
			}
		}

		private sealed class _Runnable_877 : Runnable
		{
			public _Runnable_877(UserGroupInformation _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				string cmd = UserGroupInformation.conf.Get("hadoop.kerberos.kinit.command", "kinit"
					);
				KerberosTicket tgt = this._enclosing.GetTGT();
				if (tgt == null)
				{
					return;
				}
				long nextRefresh = this._enclosing.GetRefreshTime(tgt);
				while (true)
				{
					try
					{
						long now = Time.Now();
						if (UserGroupInformation.Log.IsDebugEnabled())
						{
							UserGroupInformation.Log.Debug("Current time is " + now);
							UserGroupInformation.Log.Debug("Next refresh is " + nextRefresh);
						}
						if (now < nextRefresh)
						{
							Sharpen.Thread.Sleep(nextRefresh - now);
						}
						Shell.ExecCommand(cmd, "-R");
						if (UserGroupInformation.Log.IsDebugEnabled())
						{
							UserGroupInformation.Log.Debug("renewed ticket");
						}
						this._enclosing.ReloginFromTicketCache();
						tgt = this._enclosing.GetTGT();
						if (tgt == null)
						{
							UserGroupInformation.Log.Warn("No TGT after renewal. Aborting renew thread for " 
								+ this._enclosing.GetUserName());
							return;
						}
						nextRefresh = Math.Max(this._enclosing.GetRefreshTime(tgt), now + UserGroupInformation
							.MinTimeBeforeRelogin);
					}
					catch (Exception)
					{
						UserGroupInformation.Log.Warn("Terminating renewal thread");
						return;
					}
					catch (IOException ie)
					{
						UserGroupInformation.Log.Warn("Exception encountered while running the" + " renewal command. Aborting renew thread. "
							 + ie);
						return;
					}
				}
			}

			private readonly UserGroupInformation _enclosing;
		}

		/// <summary>Log a user in from a keytab file.</summary>
		/// <remarks>
		/// Log a user in from a keytab file. Loads a user identity from a keytab
		/// file and logs them in. They become the currently logged-in user.
		/// </remarks>
		/// <param name="user">the principal name to load from the keytab</param>
		/// <param name="path">the path to the keytab file</param>
		/// <exception cref="System.IO.IOException">if the keytab file can't be read</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static void LoginUserFromKeytab(string user, string path)
		{
			lock (typeof(UserGroupInformation))
			{
				if (!IsSecurityEnabled())
				{
					return;
				}
				keytabFile = path;
				keytabPrincipal = user;
				Subject subject = new Subject();
				LoginContext login;
				long start = 0;
				try
				{
					login = NewLoginContext(UserGroupInformation.HadoopConfiguration.KeytabKerberosConfigName
						, subject, new UserGroupInformation.HadoopConfiguration());
					start = Time.Now();
					login.Login();
					metrics.loginSuccess.Add(Time.Now() - start);
					loginUser = new UserGroupInformation(subject);
					loginUser.SetLogin(login);
					loginUser.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
						);
				}
				catch (LoginException le)
				{
					if (start > 0)
					{
						metrics.loginFailure.Add(Time.Now() - start);
					}
					throw new IOException("Login failure for " + user + " from keytab " + path + ": "
						 + le, le);
				}
				Log.Info("Login successful for user " + keytabPrincipal + " using keytab file " +
					 keytabFile);
			}
		}

		/// <summary>Re-login a user from keytab if TGT is expired or is close to expiry.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CheckTGTAndReloginFromKeytab()
		{
			lock (this)
			{
				if (!IsSecurityEnabled() || user.GetAuthenticationMethod() != UserGroupInformation.AuthenticationMethod
					.Kerberos || !isKeytab)
				{
					return;
				}
				KerberosTicket tgt = GetTGT();
				if (tgt != null && !shouldRenewImmediatelyForTests && Time.Now() < GetRefreshTime
					(tgt))
				{
					return;
				}
				ReloginFromKeytab();
			}
		}

		/// <summary>Re-Login a user in from a keytab file.</summary>
		/// <remarks>
		/// Re-Login a user in from a keytab file. Loads a user identity from a keytab
		/// file and logs them in. They become the currently logged-in user. This
		/// method assumes that
		/// <see cref="LoginUserFromKeytab(string, string)"/>
		/// had
		/// happened already.
		/// The Subject field of this UserGroupInformation object is updated to have
		/// the new credentials.
		/// </remarks>
		/// <exception cref="System.IO.IOException">on a failure</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public virtual void ReloginFromKeytab()
		{
			lock (this)
			{
				if (!IsSecurityEnabled() || user.GetAuthenticationMethod() != UserGroupInformation.AuthenticationMethod
					.Kerberos || !isKeytab)
				{
					return;
				}
				long now = Time.Now();
				if (!shouldRenewImmediatelyForTests && !HasSufficientTimeElapsed(now))
				{
					return;
				}
				KerberosTicket tgt = GetTGT();
				//Return if TGT is valid and is not going to expire soon.
				if (tgt != null && !shouldRenewImmediatelyForTests && now < GetRefreshTime(tgt))
				{
					return;
				}
				LoginContext login = GetLogin();
				if (login == null || keytabFile == null)
				{
					throw new IOException("loginUserFromKeyTab must be done first");
				}
				long start = 0;
				// register most recent relogin attempt
				user.SetLastLogin(now);
				try
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Initiating logout for " + GetUserName());
					}
					lock (typeof(UserGroupInformation))
					{
						// clear up the kerberos state. But the tokens are not cleared! As per
						// the Java kerberos login module code, only the kerberos credentials
						// are cleared
						login.Logout();
						// login and also update the subject field of this instance to
						// have the new credentials (pass it to the LoginContext constructor)
						login = NewLoginContext(UserGroupInformation.HadoopConfiguration.KeytabKerberosConfigName
							, GetSubject(), new UserGroupInformation.HadoopConfiguration());
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Initiating re-login for " + keytabPrincipal);
						}
						start = Time.Now();
						login.Login();
						metrics.loginSuccess.Add(Time.Now() - start);
						SetLogin(login);
					}
				}
				catch (LoginException le)
				{
					if (start > 0)
					{
						metrics.loginFailure.Add(Time.Now() - start);
					}
					throw new IOException("Login failure for " + keytabPrincipal + " from keytab " + 
						keytabFile, le);
				}
			}
		}

		/// <summary>Re-Login a user in from the ticket cache.</summary>
		/// <remarks>
		/// Re-Login a user in from the ticket cache.  This
		/// method assumes that login had happened already.
		/// The Subject field of this UserGroupInformation object is updated to have
		/// the new credentials.
		/// </remarks>
		/// <exception cref="System.IO.IOException">on a failure</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public virtual void ReloginFromTicketCache()
		{
			lock (this)
			{
				if (!IsSecurityEnabled() || user.GetAuthenticationMethod() != UserGroupInformation.AuthenticationMethod
					.Kerberos || !isKrbTkt)
				{
					return;
				}
				LoginContext login = GetLogin();
				if (login == null)
				{
					throw new IOException("login must be done first");
				}
				long now = Time.Now();
				if (!HasSufficientTimeElapsed(now))
				{
					return;
				}
				// register most recent relogin attempt
				user.SetLastLogin(now);
				try
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Initiating logout for " + GetUserName());
					}
					//clear up the kerberos state. But the tokens are not cleared! As per 
					//the Java kerberos login module code, only the kerberos credentials
					//are cleared
					login.Logout();
					//login and also update the subject field of this instance to 
					//have the new credentials (pass it to the LoginContext constructor)
					login = NewLoginContext(UserGroupInformation.HadoopConfiguration.UserKerberosConfigName
						, GetSubject(), new UserGroupInformation.HadoopConfiguration());
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Initiating re-login for " + GetUserName());
					}
					login.Login();
					SetLogin(login);
				}
				catch (LoginException le)
				{
					throw new IOException("Login failure for " + GetUserName(), le);
				}
			}
		}

		/// <summary>Log a user in from a keytab file.</summary>
		/// <remarks>
		/// Log a user in from a keytab file. Loads a user identity from a keytab
		/// file and login them in. This new user does not affect the currently
		/// logged-in user.
		/// </remarks>
		/// <param name="user">the principal name to load from the keytab</param>
		/// <param name="path">the path to the keytab file</param>
		/// <exception cref="System.IO.IOException">if the keytab file can't be read</exception>
		public static UserGroupInformation LoginUserFromKeytabAndReturnUGI(string user, string
			 path)
		{
			lock (typeof(UserGroupInformation))
			{
				if (!IsSecurityEnabled())
				{
					return UserGroupInformation.GetCurrentUser();
				}
				string oldKeytabFile = null;
				string oldKeytabPrincipal = null;
				long start = 0;
				try
				{
					oldKeytabFile = keytabFile;
					oldKeytabPrincipal = keytabPrincipal;
					keytabFile = path;
					keytabPrincipal = user;
					Subject subject = new Subject();
					LoginContext login = NewLoginContext(UserGroupInformation.HadoopConfiguration.KeytabKerberosConfigName
						, subject, new UserGroupInformation.HadoopConfiguration());
					start = Time.Now();
					login.Login();
					metrics.loginSuccess.Add(Time.Now() - start);
					UserGroupInformation newLoginUser = new UserGroupInformation(subject);
					newLoginUser.SetLogin(login);
					newLoginUser.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
						);
					return newLoginUser;
				}
				catch (LoginException le)
				{
					if (start > 0)
					{
						metrics.loginFailure.Add(Time.Now() - start);
					}
					throw new IOException("Login failure for " + user + " from keytab " + path, le);
				}
				finally
				{
					if (oldKeytabFile != null)
					{
						keytabFile = oldKeytabFile;
					}
					if (oldKeytabPrincipal != null)
					{
						keytabPrincipal = oldKeytabPrincipal;
					}
				}
			}
		}

		private bool HasSufficientTimeElapsed(long now)
		{
			if (now - user.GetLastLogin() < MinTimeBeforeRelogin)
			{
				Log.Warn("Not attempting to re-login since the last re-login was " + "attempted less than "
					 + (MinTimeBeforeRelogin / 1000) + " seconds" + " before.");
				return false;
			}
			return true;
		}

		/// <summary>Did the login happen via keytab</summary>
		/// <returns>true or false</returns>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static bool IsLoginKeytabBased()
		{
			lock (typeof(UserGroupInformation))
			{
				return GetLoginUser().isKeytab;
			}
		}

		/// <summary>Did the login happen via ticket cache</summary>
		/// <returns>true or false</returns>
		/// <exception cref="System.IO.IOException"/>
		public static bool IsLoginTicketBased()
		{
			return GetLoginUser().isKrbTkt;
		}

		/// <summary>Create a user from a login name.</summary>
		/// <remarks>
		/// Create a user from a login name. It is intended to be used for remote
		/// users in RPC, since it won't have any credentials.
		/// </remarks>
		/// <param name="user">the full user principal name, must not be empty or null</param>
		/// <returns>the UserGroupInformation for the remote user.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static UserGroupInformation CreateRemoteUser(string user)
		{
			return CreateRemoteUser(user, SaslRpcServer.AuthMethod.Simple);
		}

		/// <summary>Create a user from a login name.</summary>
		/// <remarks>
		/// Create a user from a login name. It is intended to be used for remote
		/// users in RPC, since it won't have any credentials.
		/// </remarks>
		/// <param name="user">the full user principal name, must not be empty or null</param>
		/// <returns>the UserGroupInformation for the remote user.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static UserGroupInformation CreateRemoteUser(string user, SaslRpcServer.AuthMethod
			 authMethod)
		{
			if (user == null || user.IsEmpty())
			{
				throw new ArgumentException("Null user");
			}
			Subject subject = new Subject();
			subject.GetPrincipals().AddItem(new User(user));
			UserGroupInformation result = new UserGroupInformation(subject);
			result.SetAuthenticationMethod(authMethod);
			return result;
		}

		/// <summary>existing types of authentications' methods</summary>
		[System.Serializable]
		public sealed class AuthenticationMethod
		{
			public static readonly UserGroupInformation.AuthenticationMethod Simple = new UserGroupInformation.AuthenticationMethod
				(SaslRpcServer.AuthMethod.Simple, UserGroupInformation.HadoopConfiguration.SimpleConfigName
				);

			public static readonly UserGroupInformation.AuthenticationMethod Kerberos = new UserGroupInformation.AuthenticationMethod
				(SaslRpcServer.AuthMethod.Kerberos, UserGroupInformation.HadoopConfiguration.UserKerberosConfigName
				);

			public static readonly UserGroupInformation.AuthenticationMethod Token = new UserGroupInformation.AuthenticationMethod
				(SaslRpcServer.AuthMethod.Token);

			public static readonly UserGroupInformation.AuthenticationMethod Certificate = new 
				UserGroupInformation.AuthenticationMethod(null);

			public static readonly UserGroupInformation.AuthenticationMethod KerberosSsl = new 
				UserGroupInformation.AuthenticationMethod(null);

			public static readonly UserGroupInformation.AuthenticationMethod Proxy = new UserGroupInformation.AuthenticationMethod
				(null);

			private readonly SaslRpcServer.AuthMethod authMethod;

			private readonly string loginAppName;

			private AuthenticationMethod(SaslRpcServer.AuthMethod authMethod)
				: this(authMethod, null)
			{
			}

			private AuthenticationMethod(SaslRpcServer.AuthMethod authMethod, string loginAppName
				)
			{
				// currently we support only one auth per method, but eventually a 
				// subtype is needed to differentiate, ex. if digest is token or ldap
				this.authMethod = authMethod;
				this.loginAppName = loginAppName;
			}

			public SaslRpcServer.AuthMethod GetAuthMethod()
			{
				return UserGroupInformation.AuthenticationMethod.authMethod;
			}

			internal string GetLoginAppName()
			{
				if (UserGroupInformation.AuthenticationMethod.loginAppName == null)
				{
					throw new NotSupportedException(this + " login authentication is not supported");
				}
				return UserGroupInformation.AuthenticationMethod.loginAppName;
			}

			public static UserGroupInformation.AuthenticationMethod ValueOf(SaslRpcServer.AuthMethod
				 authMethod)
			{
				foreach (UserGroupInformation.AuthenticationMethod value in Values())
				{
					if (value.GetAuthMethod() == authMethod)
					{
						return value;
					}
				}
				throw new ArgumentException("no authentication method for " + authMethod);
			}
		}

		/// <summary>
		/// Create a proxy user using username of the effective user and the ugi of the
		/// real user.
		/// </summary>
		/// <param name="user"/>
		/// <param name="realUser"/>
		/// <returns>proxyUser ugi</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static UserGroupInformation CreateProxyUser(string user, UserGroupInformation
			 realUser)
		{
			if (user == null || user.IsEmpty())
			{
				throw new ArgumentException("Null user");
			}
			if (realUser == null)
			{
				throw new ArgumentException("Null real user");
			}
			Subject subject = new Subject();
			ICollection<Principal> principals = subject.GetPrincipals();
			principals.AddItem(new User(user));
			principals.AddItem(new UserGroupInformation.RealUser(realUser));
			UserGroupInformation result = new UserGroupInformation(subject);
			result.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Proxy);
			return result;
		}

		/// <summary>get RealUser (vs.</summary>
		/// <remarks>get RealUser (vs. EffectiveUser)</remarks>
		/// <returns>realUser running over proxy user</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public virtual UserGroupInformation GetRealUser()
		{
			foreach (UserGroupInformation.RealUser p in subject.GetPrincipals<UserGroupInformation.RealUser
				>())
			{
				return p.GetRealUser();
			}
			return null;
		}

		/// <summary>This class is used for storing the groups for testing.</summary>
		/// <remarks>
		/// This class is used for storing the groups for testing. It stores a local
		/// map that has the translation of usernames to groups.
		/// </remarks>
		private class TestingGroups : Groups
		{
			private readonly IDictionary<string, IList<string>> userToGroupsMapping = new Dictionary
				<string, IList<string>>();

			private Groups underlyingImplementation;

			private TestingGroups(Groups underlyingImplementation)
				: base(new Configuration())
			{
				this.underlyingImplementation = underlyingImplementation;
			}

			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetGroups(string user)
			{
				IList<string> result = userToGroupsMapping[user];
				if (result == null)
				{
					result = underlyingImplementation.GetGroups(user);
				}
				return result;
			}

			private void SetUserGroups(string user, string[] groups)
			{
				userToGroupsMapping[user] = Arrays.AsList(groups);
			}
		}

		/// <summary>Create a UGI for testing HDFS and MapReduce</summary>
		/// <param name="user">the full user principal name</param>
		/// <param name="userGroups">the names of the groups that the user belongs to</param>
		/// <returns>a fake user for running unit tests</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public static UserGroupInformation CreateUserForTesting(string user, string[] userGroups
			)
		{
			EnsureInitialized();
			UserGroupInformation ugi = CreateRemoteUser(user);
			// make sure that the testing object is setup
			if (!(groups is UserGroupInformation.TestingGroups))
			{
				groups = new UserGroupInformation.TestingGroups(groups);
			}
			// add the user groups
			((UserGroupInformation.TestingGroups)groups).SetUserGroups(ugi.GetShortUserName()
				, userGroups);
			return ugi;
		}

		/// <summary>Create a proxy user UGI for testing HDFS and MapReduce</summary>
		/// <param name="user">the full user principal name for effective user</param>
		/// <param name="realUser">UGI of the real user</param>
		/// <param name="userGroups">the names of the groups that the user belongs to</param>
		/// <returns>a fake user for running unit tests</returns>
		public static UserGroupInformation CreateProxyUserForTesting(string user, UserGroupInformation
			 realUser, string[] userGroups)
		{
			EnsureInitialized();
			UserGroupInformation ugi = CreateProxyUser(user, realUser);
			// make sure that the testing object is setup
			if (!(groups is UserGroupInformation.TestingGroups))
			{
				groups = new UserGroupInformation.TestingGroups(groups);
			}
			// add the user groups
			((UserGroupInformation.TestingGroups)groups).SetUserGroups(ugi.GetShortUserName()
				, userGroups);
			return ugi;
		}

		/// <summary>Get the user's login name.</summary>
		/// <returns>the user's name up to the first '/' or '@'.</returns>
		public virtual string GetShortUserName()
		{
			foreach (User p in subject.GetPrincipals<User>())
			{
				return p.GetShortName();
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string GetPrimaryGroupName()
		{
			string[] groups = GetGroupNames();
			if (groups.Length == 0)
			{
				throw new IOException("There is no primary group for UGI " + this);
			}
			return groups[0];
		}

		/// <summary>Get the user's full principal name.</summary>
		/// <returns>the user's full principal name.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public virtual string GetUserName()
		{
			return user.GetName();
		}

		/// <summary>Add a TokenIdentifier to this UGI.</summary>
		/// <remarks>
		/// Add a TokenIdentifier to this UGI. The TokenIdentifier has typically been
		/// authenticated by the RPC layer as belonging to the user represented by this
		/// UGI.
		/// </remarks>
		/// <param name="tokenId">tokenIdentifier to be added</param>
		/// <returns>true on successful add of new tokenIdentifier</returns>
		public virtual bool AddTokenIdentifier(TokenIdentifier tokenId)
		{
			lock (this)
			{
				return subject.GetPublicCredentials().AddItem(tokenId);
			}
		}

		/// <summary>Get the set of TokenIdentifiers belonging to this UGI</summary>
		/// <returns>the set of TokenIdentifiers belonging to this UGI</returns>
		public virtual ICollection<TokenIdentifier> GetTokenIdentifiers()
		{
			lock (this)
			{
				return subject.GetPublicCredentials<TokenIdentifier>();
			}
		}

		/// <summary>Add a token to this UGI</summary>
		/// <param name="token">Token to be added</param>
		/// <returns>true on successful add of new token</returns>
		public virtual bool AddToken<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			)
			where _T0 : TokenIdentifier
		{
			return (token != null) ? AddToken(token.GetService(), token) : false;
		}

		/// <summary>Add a named token to this UGI</summary>
		/// <param name="alias">Name of the token</param>
		/// <param name="token">Token to be added</param>
		/// <returns>true on successful add of new token</returns>
		public virtual bool AddToken<_T0>(Text alias, Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			lock (subject)
			{
				GetCredentialsInternal().AddToken(alias, token);
				return true;
			}
		}

		/// <summary>Obtain the collection of tokens associated with this user.</summary>
		/// <returns>an unmodifiable collection of tokens associated with user</returns>
		public virtual ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
			>> GetTokens()
		{
			lock (subject)
			{
				return Sharpen.Collections.UnmodifiableCollection(new AList<Org.Apache.Hadoop.Security.Token.Token
					<object>>(GetCredentialsInternal().GetAllTokens()));
			}
		}

		/// <summary>Obtain the tokens in credentials form associated with this user.</summary>
		/// <returns>Credentials of tokens associated with this user</returns>
		public virtual Credentials GetCredentials()
		{
			lock (subject)
			{
				Credentials creds = new Credentials(GetCredentialsInternal());
				IEnumerator<Org.Apache.Hadoop.Security.Token.Token<object>> iter = creds.GetAllTokens
					().GetEnumerator();
				while (iter.HasNext())
				{
					if (iter.Next() is Token.PrivateToken)
					{
						iter.Remove();
					}
				}
				return creds;
			}
		}

		/// <summary>Add the given Credentials to this user.</summary>
		/// <param name="credentials">of tokens and secrets</param>
		public virtual void AddCredentials(Credentials credentials)
		{
			lock (subject)
			{
				GetCredentialsInternal().AddAll(credentials);
			}
		}

		private Credentials GetCredentialsInternal()
		{
			lock (this)
			{
				Credentials credentials;
				ICollection<Credentials> credentialsSet = subject.GetPrivateCredentials<Credentials
					>();
				if (!credentialsSet.IsEmpty())
				{
					credentials = credentialsSet.GetEnumerator().Next();
				}
				else
				{
					credentials = new Credentials();
					subject.GetPrivateCredentials().AddItem(credentials);
				}
				return credentials;
			}
		}

		/// <summary>Get the group names for this user.</summary>
		/// <returns>
		/// the list of users with the primary group first. If the command
		/// fails, it returns an empty list.
		/// </returns>
		public virtual string[] GetGroupNames()
		{
			lock (this)
			{
				EnsureInitialized();
				try
				{
					ICollection<string> result = new LinkedHashSet<string>(groups.GetGroups(GetShortUserName
						()));
					return Sharpen.Collections.ToArray(result, new string[result.Count]);
				}
				catch (IOException)
				{
					Log.Warn("No groups available for user " + GetShortUserName());
					return new string[0];
				}
			}
		}

		/// <summary>Return the username.</summary>
		public override string ToString()
		{
			StringBuilder sb = new StringBuilder(GetUserName());
			sb.Append(" (auth:" + GetAuthenticationMethod() + ")");
			if (GetRealUser() != null)
			{
				sb.Append(" via ").Append(GetRealUser().ToString());
			}
			return sb.ToString();
		}

		/// <summary>Sets the authentication method in the subject</summary>
		/// <param name="authMethod"/>
		public virtual void SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod
			 authMethod)
		{
			lock (this)
			{
				user.SetAuthenticationMethod(authMethod);
			}
		}

		/// <summary>Sets the authentication method in the subject</summary>
		/// <param name="authMethod"/>
		public virtual void SetAuthenticationMethod(SaslRpcServer.AuthMethod authMethod)
		{
			user.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.ValueOf(authMethod
				));
		}

		/// <summary>Get the authentication method from the subject</summary>
		/// <returns>AuthenticationMethod in the subject, null if not present.</returns>
		public virtual UserGroupInformation.AuthenticationMethod GetAuthenticationMethod(
			)
		{
			lock (this)
			{
				return user.GetAuthenticationMethod();
			}
		}

		/// <summary>Get the authentication method from the real user's subject.</summary>
		/// <remarks>
		/// Get the authentication method from the real user's subject.  If there
		/// is no real user, return the given user's authentication method.
		/// </remarks>
		/// <returns>AuthenticationMethod in the subject, null if not present.</returns>
		public virtual UserGroupInformation.AuthenticationMethod GetRealAuthenticationMethod
			()
		{
			lock (this)
			{
				UserGroupInformation ugi = GetRealUser();
				if (ugi == null)
				{
					ugi = this;
				}
				return ugi.GetAuthenticationMethod();
			}
		}

		/// <summary>Returns the authentication method of a ugi.</summary>
		/// <remarks>
		/// Returns the authentication method of a ugi. If the authentication method is
		/// PROXY, returns the authentication method of the real user.
		/// </remarks>
		/// <param name="ugi"/>
		/// <returns>AuthenticationMethod</returns>
		public static UserGroupInformation.AuthenticationMethod GetRealAuthenticationMethod
			(UserGroupInformation ugi)
		{
			UserGroupInformation.AuthenticationMethod authMethod = ugi.GetAuthenticationMethod
				();
			if (authMethod == UserGroupInformation.AuthenticationMethod.Proxy)
			{
				authMethod = ugi.GetRealUser().GetAuthenticationMethod();
			}
			return authMethod;
		}

		/// <summary>Compare the subjects to see if they are equal to each other.</summary>
		public override bool Equals(object o)
		{
			if (o == this)
			{
				return true;
			}
			else
			{
				if (o == null || GetType() != o.GetType())
				{
					return false;
				}
				else
				{
					return subject == ((UserGroupInformation)o).subject;
				}
			}
		}

		/// <summary>Return the hash of the subject.</summary>
		public override int GetHashCode()
		{
			return Runtime.IdentityHashCode(subject);
		}

		/// <summary>Get the underlying subject from this ugi.</summary>
		/// <returns>the subject that represents this user.</returns>
		protected internal virtual Subject GetSubject()
		{
			return subject;
		}

		/// <summary>Run the given action as the user.</summary>
		/// <?/>
		/// <param name="action">the method to execute</param>
		/// <returns>the value from the run method</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public virtual T DoAs<T>(PrivilegedAction<T> action)
		{
			LogPrivilegedAction(subject, action);
			return Subject.DoAs(subject, action);
		}

		/// <summary>Run the given action as the user, potentially throwing an exception.</summary>
		/// <?/>
		/// <param name="action">the method to execute</param>
		/// <returns>the value from the run method</returns>
		/// <exception cref="System.IO.IOException">if the action throws an IOException</exception>
		/// <exception cref="Sharpen.Error">if the action throws an Error</exception>
		/// <exception cref="Sharpen.RuntimeException">if the action throws a RuntimeException
		/// 	</exception>
		/// <exception cref="System.Exception">if the action throws an InterruptedException</exception>
		/// <exception cref="Sharpen.Reflect.UndeclaredThrowableException">if the action throws something else
		/// 	</exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public virtual T DoAs<T>(PrivilegedExceptionAction<T> action)
		{
			try
			{
				LogPrivilegedAction(subject, action);
				return Subject.DoAs(subject, action);
			}
			catch (PrivilegedActionException pae)
			{
				Exception cause = pae.InnerException;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("PrivilegedActionException as:" + this + " cause:" + cause);
				}
				if (cause is IOException)
				{
					throw (IOException)cause;
				}
				else
				{
					if (cause is Error)
					{
						throw (Error)cause;
					}
					else
					{
						if (cause is RuntimeException)
						{
							throw (RuntimeException)cause;
						}
						else
						{
							if (cause is Exception)
							{
								throw (Exception)cause;
							}
							else
							{
								throw new UndeclaredThrowableException(cause);
							}
						}
					}
				}
			}
		}

		private void LogPrivilegedAction(Subject subject, object action)
		{
			if (Log.IsDebugEnabled())
			{
				// would be nice if action included a descriptive toString()
				string where = new Exception().GetStackTrace()[2].ToString();
				Log.Debug("PrivilegedAction as:" + this + " from:" + where);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void Print()
		{
			System.Console.Out.WriteLine("User: " + GetUserName());
			System.Console.Out.Write("Group Ids: ");
			System.Console.Out.WriteLine();
			string[] groups = GetGroupNames();
			System.Console.Out.Write("Groups: ");
			for (int i = 0; i < groups.Length; i++)
			{
				System.Console.Out.Write(groups[i] + " ");
			}
			System.Console.Out.WriteLine();
		}

		/// <summary>A test method to print out the current user's UGI.</summary>
		/// <param name="args">
		/// if there are two arguments, read the user from the keytab
		/// and print it out.
		/// </param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			System.Console.Out.WriteLine("Getting UGI for current user");
			UserGroupInformation ugi = GetCurrentUser();
			ugi.Print();
			System.Console.Out.WriteLine("UGI: " + ugi);
			System.Console.Out.WriteLine("Auth method " + ugi.user.GetAuthenticationMethod());
			System.Console.Out.WriteLine("Keytab " + ugi.isKeytab);
			System.Console.Out.WriteLine("============================================================"
				);
			if (args.Length == 2)
			{
				System.Console.Out.WriteLine("Getting UGI from keytab....");
				LoginUserFromKeytab(args[0], args[1]);
				GetCurrentUser().Print();
				System.Console.Out.WriteLine("Keytab: " + ugi);
				System.Console.Out.WriteLine("Auth method " + loginUser.user.GetAuthenticationMethod
					());
				System.Console.Out.WriteLine("Keytab " + loginUser.isKeytab);
			}
		}
	}
}
