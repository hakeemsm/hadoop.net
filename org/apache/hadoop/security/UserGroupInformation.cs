using Sharpen;

namespace org.apache.hadoop.security
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
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.UserGroupInformation
			)));

		/// <summary>Percentage of the ticket window to use before we renew ticket.</summary>
		private const float TICKET_RENEW_WINDOW = 0.80f;

		private static bool shouldRenewImmediatelyForTests = false;

		internal const string HADOOP_USER_NAME = "HADOOP_USER_NAME";

		internal const string HADOOP_PROXY_USER = "HADOOP_PROXY_USER";

		/// <summary>
		/// For the purposes of unit tests, we want to test login
		/// from keytab and don't want to wait until the renew
		/// window (controlled by TICKET_RENEW_WINDOW).
		/// </summary>
		/// <param name="immediate">true if we should login without waiting for ticket window
		/// 	</param>
		[com.google.common.annotations.VisibleForTesting]
		internal static void setShouldRenewImmediatelyForTests(bool immediate)
		{
			shouldRenewImmediatelyForTests = immediate;
		}

		/// <summary>
		/// UgiMetrics maintains UGI activity statistics
		/// and publishes them through the metrics interfaces.
		/// </summary>
		internal class UgiMetrics
		{
			internal readonly org.apache.hadoop.metrics2.lib.MetricsRegistry registry = new org.apache.hadoop.metrics2.lib.MetricsRegistry
				("UgiMetrics");

			internal org.apache.hadoop.metrics2.lib.MutableRate loginSuccess;

			internal org.apache.hadoop.metrics2.lib.MutableRate loginFailure;

			internal org.apache.hadoop.metrics2.lib.MutableRate getGroups;

			internal org.apache.hadoop.metrics2.lib.MutableQuantiles[] getGroupsQuantiles;

			internal static org.apache.hadoop.security.UserGroupInformation.UgiMetrics create
				()
			{
				return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.instance().register(new 
					org.apache.hadoop.security.UserGroupInformation.UgiMetrics());
			}

			internal virtual void addGetGroups(long latency)
			{
				getGroups.add(latency);
				if (getGroupsQuantiles != null)
				{
					foreach (org.apache.hadoop.metrics2.lib.MutableQuantiles q in getGroupsQuantiles)
					{
						q.add(latency);
					}
				}
			}
		}

		/// <summary>
		/// A login module that looks at the Kerberos, Unix, or Windows principal and
		/// adds the corresponding UserName.
		/// </summary>
		public class HadoopLoginModule : javax.security.auth.spi.LoginModule
		{
			private javax.security.auth.Subject subject;

			/// <exception cref="javax.security.auth.login.LoginException"/>
			public virtual bool abort()
			{
				return true;
			}

			private T getCanonicalUser<T>()
				where T : java.security.Principal
			{
				System.Type cls = typeof(T);
				foreach (T user in subject.getPrincipals(cls))
				{
					return user;
				}
				return null;
			}

			/// <exception cref="javax.security.auth.login.LoginException"/>
			public virtual bool commit()
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("hadoop login commit");
				}
				// if we already have a user, we are done.
				if (!subject.getPrincipals<org.apache.hadoop.security.User>().isEmpty())
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("using existing subject:" + subject.getPrincipals());
					}
					return true;
				}
				java.security.Principal user = null;
				// if we are using kerberos, try it out
				if (isAuthenticationMethodEnabled(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
					.KERBEROS))
				{
					user = getCanonicalUser<javax.security.auth.kerberos.KerberosPrincipal>();
					if (LOG.isDebugEnabled())
					{
						LOG.debug("using kerberos user:" + user);
					}
				}
				//If we don't have a kerberos user and security is disabled, check
				//if user is specified in the environment or properties
				if (!isSecurityEnabled() && (user == null))
				{
					string envUser = Sharpen.Runtime.getenv(HADOOP_USER_NAME);
					if (envUser == null)
					{
						envUser = Sharpen.Runtime.getProperty(HADOOP_USER_NAME);
					}
					user = envUser == null ? null : new org.apache.hadoop.security.User(envUser);
				}
				// use the OS user
				if (user == null)
				{
					user = getCanonicalUser(OS_PRINCIPAL_CLASS);
					if (LOG.isDebugEnabled())
					{
						LOG.debug("using local user:" + user);
					}
				}
				// if we found the user, add our principal
				if (user != null)
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Using user: \"" + user + "\" with name " + user.getName());
					}
					org.apache.hadoop.security.User userEntry = null;
					try
					{
						userEntry = new org.apache.hadoop.security.User(user.getName());
					}
					catch (System.Exception e)
					{
						throw (javax.security.auth.login.LoginException)(new javax.security.auth.login.LoginException
							(e.ToString()).initCause(e));
					}
					if (LOG.isDebugEnabled())
					{
						LOG.debug("User entry: \"" + userEntry.ToString() + "\"");
					}
					subject.getPrincipals().add(userEntry);
					return true;
				}
				LOG.error("Can't find user in " + subject);
				throw new javax.security.auth.login.LoginException("Can't find user name");
			}

			public virtual void initialize(javax.security.auth.Subject subject, javax.security.auth.callback.CallbackHandler
				 callbackHandler, System.Collections.Generic.IDictionary<string, object> sharedState
				, System.Collections.Generic.IDictionary<string, object> options)
			{
				this.subject = subject;
			}

			/// <exception cref="javax.security.auth.login.LoginException"/>
			public virtual bool login()
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("hadoop login");
				}
				return true;
			}

			/// <exception cref="javax.security.auth.login.LoginException"/>
			public virtual bool logout()
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("hadoop logout");
				}
				return true;
			}
		}

		/// <summary>Metrics to track UGI activity</summary>
		internal static org.apache.hadoop.security.UserGroupInformation.UgiMetrics metrics
			 = org.apache.hadoop.security.UserGroupInformation.UgiMetrics.create();

		/// <summary>The auth method to use</summary>
		private static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 authenticationMethod;

		/// <summary>Server-side groups fetching service</summary>
		private static org.apache.hadoop.security.Groups groups;

		/// <summary>The configuration to use</summary>
		private static org.apache.hadoop.conf.Configuration conf;

		/// <summary>Leave 10 minutes between relogin attempts.</summary>
		private const long MIN_TIME_BEFORE_RELOGIN = 10 * 60 * 1000L;

		/// <summary>Environment variable pointing to the token cache file</summary>
		public const string HADOOP_TOKEN_FILE_LOCATION = "HADOOP_TOKEN_FILE_LOCATION";

		/// <summary>A method to initialize the fields that depend on a configuration.</summary>
		/// <remarks>
		/// A method to initialize the fields that depend on a configuration.
		/// Must be called before useKerberos or groups is used.
		/// </remarks>
		private static void ensureInitialized()
		{
			if (conf == null)
			{
				lock (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.UserGroupInformation
					)))
				{
					if (conf == null)
					{
						// someone might have beat us
						initialize(new org.apache.hadoop.conf.Configuration(), false);
					}
				}
			}
		}

		/// <summary>Initialize UGI and related classes.</summary>
		/// <param name="conf">the configuration to use</param>
		private static void initialize(org.apache.hadoop.conf.Configuration conf, bool overrideNameRules
			)
		{
			lock (typeof(UserGroupInformation))
			{
				authenticationMethod = org.apache.hadoop.security.SecurityUtil.getAuthenticationMethod
					(conf);
				if (overrideNameRules || !org.apache.hadoop.security.HadoopKerberosName.hasRulesBeenSet
					())
				{
					try
					{
						org.apache.hadoop.security.HadoopKerberosName.setConfiguration(conf);
					}
					catch (System.IO.IOException ioe)
					{
						throw new System.Exception("Problem with Kerberos auth_to_local name configuration"
							, ioe);
					}
				}
				// If we haven't set up testing groups, use the configuration to find it
				if (!(groups is org.apache.hadoop.security.UserGroupInformation.TestingGroups))
				{
					groups = org.apache.hadoop.security.Groups.getUserToGroupsMappingService(conf);
				}
				org.apache.hadoop.security.UserGroupInformation.conf = conf;
				if (metrics.getGroupsQuantiles == null)
				{
					int[] intervals = conf.getInts(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS
						);
					if (intervals != null && intervals.Length > 0)
					{
						int length = intervals.Length;
						org.apache.hadoop.metrics2.lib.MutableQuantiles[] getGroupsQuantiles = new org.apache.hadoop.metrics2.lib.MutableQuantiles
							[length];
						for (int i = 0; i < length; i++)
						{
							getGroupsQuantiles[i] = metrics.registry.newQuantiles("getGroups" + intervals[i] 
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
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static void setConfiguration(org.apache.hadoop.conf.Configuration conf)
		{
			initialize(conf, true);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[com.google.common.annotations.VisibleForTesting]
		internal static void reset()
		{
			authenticationMethod = null;
			conf = null;
			groups = null;
			setLoginUser(null);
			org.apache.hadoop.security.HadoopKerberosName.setRules(null);
		}

		/// <summary>
		/// Determine if UserGroupInformation is using Kerberos to determine
		/// user identities or is relying on simple authentication
		/// </summary>
		/// <returns>true if UGI is working in a secure environment</returns>
		public static bool isSecurityEnabled()
		{
			return !isAuthenticationMethodEnabled(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				.SIMPLE);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		private static bool isAuthenticationMethodEnabled(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 method)
		{
			ensureInitialized();
			return (authenticationMethod == method);
		}

		/// <summary>Information about the logged in user.</summary>
		private static org.apache.hadoop.security.UserGroupInformation loginUser = null;

		private static string keytabPrincipal = null;

		private static string keytabFile = null;

		private readonly javax.security.auth.Subject subject;

		private readonly org.apache.hadoop.security.User user;

		private readonly bool isKeytab;

		private readonly bool isKrbTkt;

		private static string OS_LOGIN_MODULE_NAME;

		private static java.lang.Class OS_PRINCIPAL_CLASS;

		private static readonly bool windows = Sharpen.Runtime.getProperty("os.name").StartsWith
			("Windows");

		private static readonly bool is64Bit = Sharpen.Runtime.getProperty("os.arch").contains
			("64");

		private static readonly bool aix = Sharpen.Runtime.getProperty("os.name").Equals(
			"AIX");

		// All non-static fields must be read-only caches that come from the subject.
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

		/* Return the OS principal class */
		private static java.lang.Class getOsPrincipalClass()
		{
			java.lang.ClassLoader cl = java.lang.ClassLoader.getSystemClassLoader();
			try
			{
				string principalClass = null;
				if (org.apache.hadoop.util.PlatformName.IBM_JAVA)
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
				return (java.lang.Class)cl.loadClass(principalClass);
			}
			catch (java.lang.ClassNotFoundException e)
			{
				LOG.error("Unable to find JAAS classes:" + e.Message);
			}
			return null;
		}

		static UserGroupInformation()
		{
			OS_LOGIN_MODULE_NAME = getOSLoginModuleName();
			OS_PRINCIPAL_CLASS = getOsPrincipalClass();
		}

		private class RealUser : java.security.Principal
		{
			private readonly org.apache.hadoop.security.UserGroupInformation realUser;

			internal RealUser(org.apache.hadoop.security.UserGroupInformation realUser)
			{
				this.realUser = realUser;
			}

			public virtual string getName()
			{
				return realUser.getUserName();
			}

			public virtual org.apache.hadoop.security.UserGroupInformation getRealUser()
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
					if (o == null || Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject
						(o))
					{
						return false;
					}
					else
					{
						return realUser.Equals(((org.apache.hadoop.security.UserGroupInformation.RealUser
							)o).realUser);
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
		private class HadoopConfiguration : javax.security.auth.login.Configuration
		{
			private const string SIMPLE_CONFIG_NAME = "hadoop-simple";

			private const string USER_KERBEROS_CONFIG_NAME = "hadoop-user-kerberos";

			private const string KEYTAB_KERBEROS_CONFIG_NAME = "hadoop-keytab-kerberos";

			private static readonly System.Collections.Generic.IDictionary<string, string> BASIC_JAAS_OPTIONS
				 = new System.Collections.Generic.Dictionary<string, string>();

			static HadoopConfiguration()
			{
				string jaasEnvVar = Sharpen.Runtime.getenv("HADOOP_JAAS_DEBUG");
				if (jaasEnvVar != null && Sharpen.Runtime.equalsIgnoreCase("true", jaasEnvVar))
				{
					BASIC_JAAS_OPTIONS["debug"] = "true";
				}
			}

			private static readonly javax.security.auth.login.AppConfigurationEntry OS_SPECIFIC_LOGIN
				 = new javax.security.auth.login.AppConfigurationEntry(OS_LOGIN_MODULE_NAME, javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag
				.REQUIRED, BASIC_JAAS_OPTIONS);

			private static readonly javax.security.auth.login.AppConfigurationEntry HADOOP_LOGIN
				 = new javax.security.auth.login.AppConfigurationEntry(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.security.UserGroupInformation.HadoopLoginModule)).getName
				(), javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED
				, BASIC_JAAS_OPTIONS);

			private static readonly System.Collections.Generic.IDictionary<string, string> USER_KERBEROS_OPTIONS
				 = new System.Collections.Generic.Dictionary<string, string>();

			static HadoopConfiguration()
			{
				if (org.apache.hadoop.util.PlatformName.IBM_JAVA)
				{
					USER_KERBEROS_OPTIONS["useDefaultCcache"] = "true";
				}
				else
				{
					USER_KERBEROS_OPTIONS["doNotPrompt"] = "true";
					USER_KERBEROS_OPTIONS["useTicketCache"] = "true";
				}
				string ticketCache = Sharpen.Runtime.getenv("KRB5CCNAME");
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
				USER_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
			}

			private static readonly javax.security.auth.login.AppConfigurationEntry USER_KERBEROS_LOGIN
				 = new javax.security.auth.login.AppConfigurationEntry(org.apache.hadoop.security.authentication.util.KerberosUtil
				.getKrb5LoginModuleName(), javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag
				.OPTIONAL, USER_KERBEROS_OPTIONS);

			private static readonly System.Collections.Generic.IDictionary<string, string> KEYTAB_KERBEROS_OPTIONS
				 = new System.Collections.Generic.Dictionary<string, string>();

			static HadoopConfiguration()
			{
				if (org.apache.hadoop.util.PlatformName.IBM_JAVA)
				{
					KEYTAB_KERBEROS_OPTIONS["credsType"] = "both";
				}
				else
				{
					KEYTAB_KERBEROS_OPTIONS["doNotPrompt"] = "true";
					KEYTAB_KERBEROS_OPTIONS["useKeyTab"] = "true";
					KEYTAB_KERBEROS_OPTIONS["storeKey"] = "true";
				}
				KEYTAB_KERBEROS_OPTIONS["refreshKrb5Config"] = "true";
				KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
			}

			private static readonly javax.security.auth.login.AppConfigurationEntry KEYTAB_KERBEROS_LOGIN
				 = new javax.security.auth.login.AppConfigurationEntry(org.apache.hadoop.security.authentication.util.KerberosUtil
				.getKrb5LoginModuleName(), javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag
				.REQUIRED, KEYTAB_KERBEROS_OPTIONS);

			private static readonly javax.security.auth.login.AppConfigurationEntry[] SIMPLE_CONF
				 = new javax.security.auth.login.AppConfigurationEntry[] { OS_SPECIFIC_LOGIN, HADOOP_LOGIN
				 };

			private static readonly javax.security.auth.login.AppConfigurationEntry[] USER_KERBEROS_CONF
				 = new javax.security.auth.login.AppConfigurationEntry[] { OS_SPECIFIC_LOGIN, USER_KERBEROS_LOGIN
				, HADOOP_LOGIN };

			private static readonly javax.security.auth.login.AppConfigurationEntry[] KEYTAB_KERBEROS_CONF
				 = new javax.security.auth.login.AppConfigurationEntry[] { KEYTAB_KERBEROS_LOGIN
				, HADOOP_LOGIN };

			public override javax.security.auth.login.AppConfigurationEntry[] getAppConfigurationEntry
				(string appName)
			{
				if (SIMPLE_CONFIG_NAME.Equals(appName))
				{
					return SIMPLE_CONF;
				}
				else
				{
					if (USER_KERBEROS_CONFIG_NAME.Equals(appName))
					{
						return USER_KERBEROS_CONF;
					}
					else
					{
						if (KEYTAB_KERBEROS_CONFIG_NAME.Equals(appName))
						{
							if (org.apache.hadoop.util.PlatformName.IBM_JAVA)
							{
								KEYTAB_KERBEROS_OPTIONS["useKeytab"] = prependFileAuthority(keytabFile);
							}
							else
							{
								KEYTAB_KERBEROS_OPTIONS["keyTab"] = keytabFile;
							}
							KEYTAB_KERBEROS_OPTIONS["principal"] = keytabPrincipal;
							return KEYTAB_KERBEROS_CONF;
						}
					}
				}
				return null;
			}
		}

		private static string prependFileAuthority(string keytabPath)
		{
			return keytabPath.StartsWith("file://") ? keytabPath : "file://" + keytabPath;
		}

		/// <summary>Represents a javax.security configuration that is created at runtime.</summary>
		private class DynamicConfiguration : javax.security.auth.login.Configuration
		{
			private javax.security.auth.login.AppConfigurationEntry[] ace;

			internal DynamicConfiguration(javax.security.auth.login.AppConfigurationEntry[] ace
				)
			{
				this.ace = ace;
			}

			public override javax.security.auth.login.AppConfigurationEntry[] getAppConfigurationEntry
				(string appName)
			{
				return ace;
			}
		}

		/// <exception cref="javax.security.auth.login.LoginException"/>
		private static javax.security.auth.login.LoginContext newLoginContext(string appName
			, javax.security.auth.Subject subject, javax.security.auth.login.Configuration loginConf
			)
		{
			// Temporarily switch the thread's ContextClassLoader to match this
			// class's classloader, so that we can properly load HadoopLoginModule
			// from the JAAS libraries.
			java.lang.Thread t = java.lang.Thread.currentThread();
			java.lang.ClassLoader oldCCL = t.getContextClassLoader();
			t.setContextClassLoader(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.UserGroupInformation.HadoopLoginModule
				)).getClassLoader());
			try
			{
				return new javax.security.auth.login.LoginContext(appName, subject, null, loginConf
					);
			}
			finally
			{
				t.setContextClassLoader(oldCCL);
			}
		}

		private javax.security.auth.login.LoginContext getLogin()
		{
			return user.getLogin();
		}

		private void setLogin(javax.security.auth.login.LoginContext login)
		{
			user.setLogin(login);
		}

		/// <summary>Create a UserGroupInformation for the given subject.</summary>
		/// <remarks>
		/// Create a UserGroupInformation for the given subject.
		/// This does not change the subject or acquire new credentials.
		/// </remarks>
		/// <param name="subject">the user's subject</param>
		internal UserGroupInformation(javax.security.auth.Subject subject)
		{
			this.subject = subject;
			this.user = subject.getPrincipals<org.apache.hadoop.security.User>().GetEnumerator
				().Current;
			this.isKeytab = !subject.getPrivateCredentials<javax.security.auth.kerberos.KeyTab
				>().isEmpty();
			this.isKrbTkt = !subject.getPrivateCredentials<javax.security.auth.kerberos.KerberosTicket
				>().isEmpty();
		}

		/// <summary>checks if logged in using kerberos</summary>
		/// <returns>true if the subject logged via keytab or has a Kerberos TGT</returns>
		public virtual bool hasKerberosCredentials()
		{
			return isKeytab || isKrbTkt;
		}

		/// <summary>Return the current user, including any doAs in the current stack.</summary>
		/// <returns>the current user</returns>
		/// <exception cref="System.IO.IOException">if login fails</exception>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static org.apache.hadoop.security.UserGroupInformation getCurrentUser()
		{
			lock (typeof(UserGroupInformation))
			{
				java.security.AccessControlContext context = java.security.AccessController.getContext
					();
				javax.security.auth.Subject subject = javax.security.auth.Subject.getSubject(context
					);
				if (subject == null || subject.getPrincipals<org.apache.hadoop.security.User>().isEmpty
					())
				{
					return getLoginUser();
				}
				else
				{
					return new org.apache.hadoop.security.UserGroupInformation(subject);
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
		public static org.apache.hadoop.security.UserGroupInformation getBestUGI(string ticketCachePath
			, string user)
		{
			if (ticketCachePath != null)
			{
				return getUGIFromTicketCache(ticketCachePath, user);
			}
			else
			{
				if (user == null)
				{
					return getCurrentUser();
				}
				else
				{
					return createRemoteUser(user);
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
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static org.apache.hadoop.security.UserGroupInformation getUGIFromTicketCache
			(string ticketCache, string user)
		{
			if (!isAuthenticationMethodEnabled(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				.KERBEROS))
			{
				return getBestUGI(null, user);
			}
			try
			{
				System.Collections.Generic.IDictionary<string, string> krbOptions = new System.Collections.Generic.Dictionary
					<string, string>();
				if (org.apache.hadoop.util.PlatformName.IBM_JAVA)
				{
					krbOptions["useDefaultCcache"] = "true";
					// The first value searched when "useDefaultCcache" is used.
					Sharpen.Runtime.setProperty("KRB5CCNAME", ticketCache);
				}
				else
				{
					krbOptions["doNotPrompt"] = "true";
					krbOptions["useTicketCache"] = "true";
					krbOptions["useKeyTab"] = "false";
					krbOptions["ticketCache"] = ticketCache;
				}
				krbOptions["renewTGT"] = "false";
				krbOptions.putAll(org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
					.BASIC_JAAS_OPTIONS);
				javax.security.auth.login.AppConfigurationEntry ace = new javax.security.auth.login.AppConfigurationEntry
					(org.apache.hadoop.security.authentication.util.KerberosUtil.getKrb5LoginModuleName
					(), javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED
					, krbOptions);
				org.apache.hadoop.security.UserGroupInformation.DynamicConfiguration dynConf = new 
					org.apache.hadoop.security.UserGroupInformation.DynamicConfiguration(new javax.security.auth.login.AppConfigurationEntry
					[] { ace });
				javax.security.auth.login.LoginContext login = newLoginContext(org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
					.USER_KERBEROS_CONFIG_NAME, null, dynConf);
				login.login();
				javax.security.auth.Subject loginSubject = login.getSubject();
				System.Collections.Generic.ICollection<java.security.Principal> loginPrincipals = 
					loginSubject.getPrincipals();
				if (loginPrincipals.isEmpty())
				{
					throw new System.Exception("No login principals found!");
				}
				if (loginPrincipals.Count != 1)
				{
					LOG.warn("found more than one principal in the ticket cache file " + ticketCache);
				}
				org.apache.hadoop.security.User ugiUser = new org.apache.hadoop.security.User(loginPrincipals
					.GetEnumerator().Current.getName(), org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
					.KERBEROS, login);
				loginSubject.getPrincipals().add(ugiUser);
				org.apache.hadoop.security.UserGroupInformation ugi = new org.apache.hadoop.security.UserGroupInformation
					(loginSubject);
				ugi.setLogin(login);
				ugi.setAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
					.KERBEROS);
				return ugi;
			}
			catch (javax.security.auth.login.LoginException le)
			{
				throw new System.IO.IOException("failure to login using ticket cache file " + ticketCache
					, le);
			}
		}

		/// <summary>Create a UserGroupInformation from a Subject with Kerberos principal.</summary>
		/// <param name="user">The KerberosPrincipal to use in UGI</param>
		/// <exception cref="System.IO.IOException">if the kerberos login fails</exception>
		public static org.apache.hadoop.security.UserGroupInformation getUGIFromSubject(javax.security.auth.Subject
			 subject)
		{
			if (subject == null)
			{
				throw new System.IO.IOException("Subject must not be null");
			}
			if (subject.getPrincipals<javax.security.auth.kerberos.KerberosPrincipal>().isEmpty
				())
			{
				throw new System.IO.IOException("Provided Subject must contain a KerberosPrincipal"
					);
			}
			javax.security.auth.kerberos.KerberosPrincipal principal = subject.getPrincipals<
				javax.security.auth.kerberos.KerberosPrincipal>().GetEnumerator().Current;
			org.apache.hadoop.security.User ugiUser = new org.apache.hadoop.security.User(principal
				.getName(), org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				.KERBEROS, null);
			subject.getPrincipals().add(ugiUser);
			org.apache.hadoop.security.UserGroupInformation ugi = new org.apache.hadoop.security.UserGroupInformation
				(subject);
			ugi.setLogin(null);
			ugi.setAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				.KERBEROS);
			return ugi;
		}

		/// <summary>Get the currently logged in user.</summary>
		/// <returns>the logged in user</returns>
		/// <exception cref="System.IO.IOException">if login fails</exception>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static org.apache.hadoop.security.UserGroupInformation getLoginUser()
		{
			lock (typeof(UserGroupInformation))
			{
				if (loginUser == null)
				{
					loginUserFromSubject(null);
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
		public static string trimLoginMethod(string userName)
		{
			int spaceIndex = userName.IndexOf(' ');
			if (spaceIndex >= 0)
			{
				userName = Sharpen.Runtime.substring(userName, 0, spaceIndex);
			}
			return userName;
		}

		/// <summary>Log in a user using the given subject</summary>
		/// <parma>
		/// subject the subject to use when logging in a user, or null to
		/// create a new subject.
		/// </parma>
		/// <exception cref="System.IO.IOException">if login fails</exception>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static void loginUserFromSubject(javax.security.auth.Subject subject)
		{
			lock (typeof(UserGroupInformation))
			{
				ensureInitialized();
				try
				{
					if (subject == null)
					{
						subject = new javax.security.auth.Subject();
					}
					javax.security.auth.login.LoginContext login = newLoginContext(authenticationMethod
						.getLoginAppName(), subject, new org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
						());
					login.login();
					org.apache.hadoop.security.UserGroupInformation realUser = new org.apache.hadoop.security.UserGroupInformation
						(subject);
					realUser.setLogin(login);
					realUser.setAuthenticationMethod(authenticationMethod);
					realUser = new org.apache.hadoop.security.UserGroupInformation(login.getSubject()
						);
					// If the HADOOP_PROXY_USER environment variable or property
					// is specified, create a proxy user as the logged in user.
					string proxyUser = Sharpen.Runtime.getenv(HADOOP_PROXY_USER);
					if (proxyUser == null)
					{
						proxyUser = Sharpen.Runtime.getProperty(HADOOP_PROXY_USER);
					}
					loginUser = proxyUser == null ? realUser : createProxyUser(proxyUser, realUser);
					string fileLocation = Sharpen.Runtime.getenv(HADOOP_TOKEN_FILE_LOCATION);
					if (fileLocation != null)
					{
						// Load the token storage file and put all of the tokens into the
						// user. Don't use the FileSystem API for reading since it has a lock
						// cycle (HADOOP-9212).
						org.apache.hadoop.security.Credentials cred = org.apache.hadoop.security.Credentials
							.readTokenStorageFile(new java.io.File(fileLocation), conf);
						loginUser.addCredentials(cred);
					}
					loginUser.spawnAutoRenewalThreadForUserCreds();
				}
				catch (javax.security.auth.login.LoginException le)
				{
					LOG.debug("failure to login", le);
					throw new System.IO.IOException("failure to login", le);
				}
				if (LOG.isDebugEnabled())
				{
					LOG.debug("UGI loginUser:" + loginUser);
				}
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[org.apache.hadoop.classification.InterfaceStability.Unstable]
		[com.google.common.annotations.VisibleForTesting]
		public static void setLoginUser(org.apache.hadoop.security.UserGroupInformation ugi
			)
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
		public virtual bool isFromKeytab()
		{
			return isKeytab;
		}

		/// <summary>Get the Kerberos TGT</summary>
		/// <returns>the user's TGT or null if none was found</returns>
		private javax.security.auth.kerberos.KerberosTicket getTGT()
		{
			lock (this)
			{
				System.Collections.Generic.ICollection<javax.security.auth.kerberos.KerberosTicket
					> tickets = subject.getPrivateCredentials<javax.security.auth.kerberos.KerberosTicket
					>();
				foreach (javax.security.auth.kerberos.KerberosTicket ticket in tickets)
				{
					if (org.apache.hadoop.security.SecurityUtil.isOriginalTGT(ticket))
					{
						if (LOG.isDebugEnabled())
						{
							LOG.debug("Found tgt " + ticket);
						}
						return ticket;
					}
				}
				return null;
			}
		}

		private long getRefreshTime(javax.security.auth.kerberos.KerberosTicket tgt)
		{
			long start = tgt.getStartTime().getTime();
			long end = tgt.getEndTime().getTime();
			return start + (long)((end - start) * TICKET_RENEW_WINDOW);
		}

		/// <summary>Spawn a thread to do periodic renewals of kerberos credentials</summary>
		private void spawnAutoRenewalThreadForUserCreds()
		{
			if (isSecurityEnabled())
			{
				//spawn thread only if we have kerb credentials
				if (user.getAuthenticationMethod() == org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
					.KERBEROS && !isKeytab)
				{
					java.lang.Thread t = new java.lang.Thread(new _Runnable_877(this));
					t.setDaemon(true);
					t.setName("TGT Renewer for " + getUserName());
					t.start();
				}
			}
		}

		private sealed class _Runnable_877 : java.lang.Runnable
		{
			public _Runnable_877(UserGroupInformation _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void run()
			{
				string cmd = org.apache.hadoop.security.UserGroupInformation.conf.get("hadoop.kerberos.kinit.command"
					, "kinit");
				javax.security.auth.kerberos.KerberosTicket tgt = this._enclosing.getTGT();
				if (tgt == null)
				{
					return;
				}
				long nextRefresh = this._enclosing.getRefreshTime(tgt);
				while (true)
				{
					try
					{
						long now = org.apache.hadoop.util.Time.now();
						if (org.apache.hadoop.security.UserGroupInformation.LOG.isDebugEnabled())
						{
							org.apache.hadoop.security.UserGroupInformation.LOG.debug("Current time is " + now
								);
							org.apache.hadoop.security.UserGroupInformation.LOG.debug("Next refresh is " + nextRefresh
								);
						}
						if (now < nextRefresh)
						{
							java.lang.Thread.sleep(nextRefresh - now);
						}
						org.apache.hadoop.util.Shell.execCommand(cmd, "-R");
						if (org.apache.hadoop.security.UserGroupInformation.LOG.isDebugEnabled())
						{
							org.apache.hadoop.security.UserGroupInformation.LOG.debug("renewed ticket");
						}
						this._enclosing.reloginFromTicketCache();
						tgt = this._enclosing.getTGT();
						if (tgt == null)
						{
							org.apache.hadoop.security.UserGroupInformation.LOG.warn("No TGT after renewal. Aborting renew thread for "
								 + this._enclosing.getUserName());
							return;
						}
						nextRefresh = System.Math.max(this._enclosing.getRefreshTime(tgt), now + org.apache.hadoop.security.UserGroupInformation
							.MIN_TIME_BEFORE_RELOGIN);
					}
					catch (System.Exception)
					{
						org.apache.hadoop.security.UserGroupInformation.LOG.warn("Terminating renewal thread"
							);
						return;
					}
					catch (System.IO.IOException ie)
					{
						org.apache.hadoop.security.UserGroupInformation.LOG.warn("Exception encountered while running the"
							 + " renewal command. Aborting renew thread. " + ie);
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
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static void loginUserFromKeytab(string user, string path)
		{
			lock (typeof(UserGroupInformation))
			{
				if (!isSecurityEnabled())
				{
					return;
				}
				keytabFile = path;
				keytabPrincipal = user;
				javax.security.auth.Subject subject = new javax.security.auth.Subject();
				javax.security.auth.login.LoginContext login;
				long start = 0;
				try
				{
					login = newLoginContext(org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
						.KEYTAB_KERBEROS_CONFIG_NAME, subject, new org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
						());
					start = org.apache.hadoop.util.Time.now();
					login.login();
					metrics.loginSuccess.add(org.apache.hadoop.util.Time.now() - start);
					loginUser = new org.apache.hadoop.security.UserGroupInformation(subject);
					loginUser.setLogin(login);
					loginUser.setAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
						.KERBEROS);
				}
				catch (javax.security.auth.login.LoginException le)
				{
					if (start > 0)
					{
						metrics.loginFailure.add(org.apache.hadoop.util.Time.now() - start);
					}
					throw new System.IO.IOException("Login failure for " + user + " from keytab " + path
						 + ": " + le, le);
				}
				LOG.info("Login successful for user " + keytabPrincipal + " using keytab file " +
					 keytabFile);
			}
		}

		/// <summary>Re-login a user from keytab if TGT is expired or is close to expiry.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void checkTGTAndReloginFromKeytab()
		{
			lock (this)
			{
				if (!isSecurityEnabled() || user.getAuthenticationMethod() != org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
					.KERBEROS || !isKeytab)
				{
					return;
				}
				javax.security.auth.kerberos.KerberosTicket tgt = getTGT();
				if (tgt != null && !shouldRenewImmediatelyForTests && org.apache.hadoop.util.Time
					.now() < getRefreshTime(tgt))
				{
					return;
				}
				reloginFromKeytab();
			}
		}

		/// <summary>Re-Login a user in from a keytab file.</summary>
		/// <remarks>
		/// Re-Login a user in from a keytab file. Loads a user identity from a keytab
		/// file and logs them in. They become the currently logged-in user. This
		/// method assumes that
		/// <see cref="loginUserFromKeytab(string, string)"/>
		/// had
		/// happened already.
		/// The Subject field of this UserGroupInformation object is updated to have
		/// the new credentials.
		/// </remarks>
		/// <exception cref="System.IO.IOException">on a failure</exception>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public virtual void reloginFromKeytab()
		{
			lock (this)
			{
				if (!isSecurityEnabled() || user.getAuthenticationMethod() != org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
					.KERBEROS || !isKeytab)
				{
					return;
				}
				long now = org.apache.hadoop.util.Time.now();
				if (!shouldRenewImmediatelyForTests && !hasSufficientTimeElapsed(now))
				{
					return;
				}
				javax.security.auth.kerberos.KerberosTicket tgt = getTGT();
				//Return if TGT is valid and is not going to expire soon.
				if (tgt != null && !shouldRenewImmediatelyForTests && now < getRefreshTime(tgt))
				{
					return;
				}
				javax.security.auth.login.LoginContext login = getLogin();
				if (login == null || keytabFile == null)
				{
					throw new System.IO.IOException("loginUserFromKeyTab must be done first");
				}
				long start = 0;
				// register most recent relogin attempt
				user.setLastLogin(now);
				try
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Initiating logout for " + getUserName());
					}
					lock (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.UserGroupInformation
						)))
					{
						// clear up the kerberos state. But the tokens are not cleared! As per
						// the Java kerberos login module code, only the kerberos credentials
						// are cleared
						login.logout();
						// login and also update the subject field of this instance to
						// have the new credentials (pass it to the LoginContext constructor)
						login = newLoginContext(org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
							.KEYTAB_KERBEROS_CONFIG_NAME, getSubject(), new org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
							());
						if (LOG.isDebugEnabled())
						{
							LOG.debug("Initiating re-login for " + keytabPrincipal);
						}
						start = org.apache.hadoop.util.Time.now();
						login.login();
						metrics.loginSuccess.add(org.apache.hadoop.util.Time.now() - start);
						setLogin(login);
					}
				}
				catch (javax.security.auth.login.LoginException le)
				{
					if (start > 0)
					{
						metrics.loginFailure.add(org.apache.hadoop.util.Time.now() - start);
					}
					throw new System.IO.IOException("Login failure for " + keytabPrincipal + " from keytab "
						 + keytabFile, le);
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
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public virtual void reloginFromTicketCache()
		{
			lock (this)
			{
				if (!isSecurityEnabled() || user.getAuthenticationMethod() != org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
					.KERBEROS || !isKrbTkt)
				{
					return;
				}
				javax.security.auth.login.LoginContext login = getLogin();
				if (login == null)
				{
					throw new System.IO.IOException("login must be done first");
				}
				long now = org.apache.hadoop.util.Time.now();
				if (!hasSufficientTimeElapsed(now))
				{
					return;
				}
				// register most recent relogin attempt
				user.setLastLogin(now);
				try
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Initiating logout for " + getUserName());
					}
					//clear up the kerberos state. But the tokens are not cleared! As per 
					//the Java kerberos login module code, only the kerberos credentials
					//are cleared
					login.logout();
					//login and also update the subject field of this instance to 
					//have the new credentials (pass it to the LoginContext constructor)
					login = newLoginContext(org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
						.USER_KERBEROS_CONFIG_NAME, getSubject(), new org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
						());
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Initiating re-login for " + getUserName());
					}
					login.login();
					setLogin(login);
				}
				catch (javax.security.auth.login.LoginException le)
				{
					throw new System.IO.IOException("Login failure for " + getUserName(), le);
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
		public static org.apache.hadoop.security.UserGroupInformation loginUserFromKeytabAndReturnUGI
			(string user, string path)
		{
			lock (typeof(UserGroupInformation))
			{
				if (!isSecurityEnabled())
				{
					return org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
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
					javax.security.auth.Subject subject = new javax.security.auth.Subject();
					javax.security.auth.login.LoginContext login = newLoginContext(org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
						.KEYTAB_KERBEROS_CONFIG_NAME, subject, new org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
						());
					start = org.apache.hadoop.util.Time.now();
					login.login();
					metrics.loginSuccess.add(org.apache.hadoop.util.Time.now() - start);
					org.apache.hadoop.security.UserGroupInformation newLoginUser = new org.apache.hadoop.security.UserGroupInformation
						(subject);
					newLoginUser.setLogin(login);
					newLoginUser.setAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
						.KERBEROS);
					return newLoginUser;
				}
				catch (javax.security.auth.login.LoginException le)
				{
					if (start > 0)
					{
						metrics.loginFailure.add(org.apache.hadoop.util.Time.now() - start);
					}
					throw new System.IO.IOException("Login failure for " + user + " from keytab " + path
						, le);
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

		private bool hasSufficientTimeElapsed(long now)
		{
			if (now - user.getLastLogin() < MIN_TIME_BEFORE_RELOGIN)
			{
				LOG.warn("Not attempting to re-login since the last re-login was " + "attempted less than "
					 + (MIN_TIME_BEFORE_RELOGIN / 1000) + " seconds" + " before.");
				return false;
			}
			return true;
		}

		/// <summary>Did the login happen via keytab</summary>
		/// <returns>true or false</returns>
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static bool isLoginKeytabBased()
		{
			lock (typeof(UserGroupInformation))
			{
				return getLoginUser().isKeytab;
			}
		}

		/// <summary>Did the login happen via ticket cache</summary>
		/// <returns>true or false</returns>
		/// <exception cref="System.IO.IOException"/>
		public static bool isLoginTicketBased()
		{
			return getLoginUser().isKrbTkt;
		}

		/// <summary>Create a user from a login name.</summary>
		/// <remarks>
		/// Create a user from a login name. It is intended to be used for remote
		/// users in RPC, since it won't have any credentials.
		/// </remarks>
		/// <param name="user">the full user principal name, must not be empty or null</param>
		/// <returns>the UserGroupInformation for the remote user.</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static org.apache.hadoop.security.UserGroupInformation createRemoteUser(string
			 user)
		{
			return createRemoteUser(user, org.apache.hadoop.security.SaslRpcServer.AuthMethod
				.SIMPLE);
		}

		/// <summary>Create a user from a login name.</summary>
		/// <remarks>
		/// Create a user from a login name. It is intended to be used for remote
		/// users in RPC, since it won't have any credentials.
		/// </remarks>
		/// <param name="user">the full user principal name, must not be empty or null</param>
		/// <returns>the UserGroupInformation for the remote user.</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static org.apache.hadoop.security.UserGroupInformation createRemoteUser(string
			 user, org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod)
		{
			if (user == null || user.isEmpty())
			{
				throw new System.ArgumentException("Null user");
			}
			javax.security.auth.Subject subject = new javax.security.auth.Subject();
			subject.getPrincipals().add(new org.apache.hadoop.security.User(user));
			org.apache.hadoop.security.UserGroupInformation result = new org.apache.hadoop.security.UserGroupInformation
				(subject);
			result.setAuthenticationMethod(authMethod);
			return result;
		}

		/// <summary>existing types of authentications' methods</summary>
		[System.Serializable]
		public sealed class AuthenticationMethod
		{
			public static readonly org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				 SIMPLE = new org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				(org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE, org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
				.SIMPLE_CONFIG_NAME);

			public static readonly org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				 KERBEROS = new org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				(org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS, org.apache.hadoop.security.UserGroupInformation.HadoopConfiguration
				.USER_KERBEROS_CONFIG_NAME);

			public static readonly org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				 TOKEN = new org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				(org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN);

			public static readonly org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				 CERTIFICATE = new org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				(null);

			public static readonly org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				 KERBEROS_SSL = new org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				(null);

			public static readonly org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				 PROXY = new org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				(null);

			private readonly org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod;

			private readonly string loginAppName;

			private AuthenticationMethod(org.apache.hadoop.security.SaslRpcServer.AuthMethod 
				authMethod)
				: this(authMethod, null)
			{
			}

			private AuthenticationMethod(org.apache.hadoop.security.SaslRpcServer.AuthMethod 
				authMethod, string loginAppName)
			{
				// currently we support only one auth per method, but eventually a 
				// subtype is needed to differentiate, ex. if digest is token or ldap
				this.authMethod = authMethod;
				this.loginAppName = loginAppName;
			}

			public org.apache.hadoop.security.SaslRpcServer.AuthMethod getAuthMethod()
			{
				return org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.authMethod;
			}

			internal string getLoginAppName()
			{
				if (org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.loginAppName
					 == null)
				{
					throw new System.NotSupportedException(this + " login authentication is not supported"
						);
				}
				return org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.loginAppName;
			}

			public static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				 valueOf(org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod)
			{
				foreach (org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod value
					 in values())
				{
					if (value.getAuthMethod() == authMethod)
					{
						return value;
					}
				}
				throw new System.ArgumentException("no authentication method for " + authMethod);
			}
		}

		/// <summary>
		/// Create a proxy user using username of the effective user and the ugi of the
		/// real user.
		/// </summary>
		/// <param name="user"/>
		/// <param name="realUser"/>
		/// <returns>proxyUser ugi</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static org.apache.hadoop.security.UserGroupInformation createProxyUser(string
			 user, org.apache.hadoop.security.UserGroupInformation realUser)
		{
			if (user == null || user.isEmpty())
			{
				throw new System.ArgumentException("Null user");
			}
			if (realUser == null)
			{
				throw new System.ArgumentException("Null real user");
			}
			javax.security.auth.Subject subject = new javax.security.auth.Subject();
			System.Collections.Generic.ICollection<java.security.Principal> principals = subject
				.getPrincipals();
			principals.add(new org.apache.hadoop.security.User(user));
			principals.add(new org.apache.hadoop.security.UserGroupInformation.RealUser(realUser
				));
			org.apache.hadoop.security.UserGroupInformation result = new org.apache.hadoop.security.UserGroupInformation
				(subject);
			result.setAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				.PROXY);
			return result;
		}

		/// <summary>get RealUser (vs.</summary>
		/// <remarks>get RealUser (vs. EffectiveUser)</remarks>
		/// <returns>realUser running over proxy user</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public virtual org.apache.hadoop.security.UserGroupInformation getRealUser()
		{
			foreach (org.apache.hadoop.security.UserGroupInformation.RealUser p in subject.getPrincipals
				<org.apache.hadoop.security.UserGroupInformation.RealUser>())
			{
				return p.getRealUser();
			}
			return null;
		}

		/// <summary>This class is used for storing the groups for testing.</summary>
		/// <remarks>
		/// This class is used for storing the groups for testing. It stores a local
		/// map that has the translation of usernames to groups.
		/// </remarks>
		private class TestingGroups : org.apache.hadoop.security.Groups
		{
			private readonly System.Collections.Generic.IDictionary<string, System.Collections.Generic.IList
				<string>> userToGroupsMapping = new System.Collections.Generic.Dictionary<string
				, System.Collections.Generic.IList<string>>();

			private org.apache.hadoop.security.Groups underlyingImplementation;

			private TestingGroups(org.apache.hadoop.security.Groups underlyingImplementation)
				: base(new org.apache.hadoop.conf.Configuration())
			{
				this.underlyingImplementation = underlyingImplementation;
			}

			/// <exception cref="System.IO.IOException"/>
			public override System.Collections.Generic.IList<string> getGroups(string user)
			{
				System.Collections.Generic.IList<string> result = userToGroupsMapping[user];
				if (result == null)
				{
					result = underlyingImplementation.getGroups(user);
				}
				return result;
			}

			private void setUserGroups(string user, string[] groups)
			{
				userToGroupsMapping[user] = java.util.Arrays.asList(groups);
			}
		}

		/// <summary>Create a UGI for testing HDFS and MapReduce</summary>
		/// <param name="user">the full user principal name</param>
		/// <param name="userGroups">the names of the groups that the user belongs to</param>
		/// <returns>a fake user for running unit tests</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public static org.apache.hadoop.security.UserGroupInformation createUserForTesting
			(string user, string[] userGroups)
		{
			ensureInitialized();
			org.apache.hadoop.security.UserGroupInformation ugi = createRemoteUser(user);
			// make sure that the testing object is setup
			if (!(groups is org.apache.hadoop.security.UserGroupInformation.TestingGroups))
			{
				groups = new org.apache.hadoop.security.UserGroupInformation.TestingGroups(groups
					);
			}
			// add the user groups
			((org.apache.hadoop.security.UserGroupInformation.TestingGroups)groups).setUserGroups
				(ugi.getShortUserName(), userGroups);
			return ugi;
		}

		/// <summary>Create a proxy user UGI for testing HDFS and MapReduce</summary>
		/// <param name="user">the full user principal name for effective user</param>
		/// <param name="realUser">UGI of the real user</param>
		/// <param name="userGroups">the names of the groups that the user belongs to</param>
		/// <returns>a fake user for running unit tests</returns>
		public static org.apache.hadoop.security.UserGroupInformation createProxyUserForTesting
			(string user, org.apache.hadoop.security.UserGroupInformation realUser, string[]
			 userGroups)
		{
			ensureInitialized();
			org.apache.hadoop.security.UserGroupInformation ugi = createProxyUser(user, realUser
				);
			// make sure that the testing object is setup
			if (!(groups is org.apache.hadoop.security.UserGroupInformation.TestingGroups))
			{
				groups = new org.apache.hadoop.security.UserGroupInformation.TestingGroups(groups
					);
			}
			// add the user groups
			((org.apache.hadoop.security.UserGroupInformation.TestingGroups)groups).setUserGroups
				(ugi.getShortUserName(), userGroups);
			return ugi;
		}

		/// <summary>Get the user's login name.</summary>
		/// <returns>the user's name up to the first '/' or '@'.</returns>
		public virtual string getShortUserName()
		{
			foreach (org.apache.hadoop.security.User p in subject.getPrincipals<org.apache.hadoop.security.User
				>())
			{
				return p.getShortName();
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string getPrimaryGroupName()
		{
			string[] groups = getGroupNames();
			if (groups.Length == 0)
			{
				throw new System.IO.IOException("There is no primary group for UGI " + this);
			}
			return groups[0];
		}

		/// <summary>Get the user's full principal name.</summary>
		/// <returns>the user's full principal name.</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public virtual string getUserName()
		{
			return user.getName();
		}

		/// <summary>Add a TokenIdentifier to this UGI.</summary>
		/// <remarks>
		/// Add a TokenIdentifier to this UGI. The TokenIdentifier has typically been
		/// authenticated by the RPC layer as belonging to the user represented by this
		/// UGI.
		/// </remarks>
		/// <param name="tokenId">tokenIdentifier to be added</param>
		/// <returns>true on successful add of new tokenIdentifier</returns>
		public virtual bool addTokenIdentifier(org.apache.hadoop.security.token.TokenIdentifier
			 tokenId)
		{
			lock (this)
			{
				return subject.getPublicCredentials().add(tokenId);
			}
		}

		/// <summary>Get the set of TokenIdentifiers belonging to this UGI</summary>
		/// <returns>the set of TokenIdentifiers belonging to this UGI</returns>
		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.security.token.TokenIdentifier
			> getTokenIdentifiers()
		{
			lock (this)
			{
				return subject.getPublicCredentials<org.apache.hadoop.security.token.TokenIdentifier
					>();
			}
		}

		/// <summary>Add a token to this UGI</summary>
		/// <param name="token">Token to be added</param>
		/// <returns>true on successful add of new token</returns>
		public virtual bool addToken<_T0>(org.apache.hadoop.security.token.Token<_T0> token
			)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier
		{
			return (token != null) ? addToken(token.getService(), token) : false;
		}

		/// <summary>Add a named token to this UGI</summary>
		/// <param name="alias">Name of the token</param>
		/// <param name="token">Token to be added</param>
		/// <returns>true on successful add of new token</returns>
		public virtual bool addToken<_T0>(org.apache.hadoop.io.Text alias, org.apache.hadoop.security.token.Token
			<_T0> token)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier
		{
			lock (subject)
			{
				getCredentialsInternal().addToken(alias, token);
				return true;
			}
		}

		/// <summary>Obtain the collection of tokens associated with this user.</summary>
		/// <returns>an unmodifiable collection of tokens associated with user</returns>
		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.security.token.Token
			<org.apache.hadoop.security.token.TokenIdentifier>> getTokens()
		{
			lock (subject)
			{
				return java.util.Collections.unmodifiableCollection(new System.Collections.Generic.List
					<org.apache.hadoop.security.token.Token<object>>(getCredentialsInternal().getAllTokens
					()));
			}
		}

		/// <summary>Obtain the tokens in credentials form associated with this user.</summary>
		/// <returns>Credentials of tokens associated with this user</returns>
		public virtual org.apache.hadoop.security.Credentials getCredentials()
		{
			lock (subject)
			{
				org.apache.hadoop.security.Credentials creds = new org.apache.hadoop.security.Credentials
					(getCredentialsInternal());
				System.Collections.Generic.IEnumerator<org.apache.hadoop.security.token.Token<object
					>> iter = creds.getAllTokens().GetEnumerator();
				while (iter.MoveNext())
				{
					if (iter.Current is org.apache.hadoop.security.token.Token.PrivateToken)
					{
						iter.remove();
					}
				}
				return creds;
			}
		}

		/// <summary>Add the given Credentials to this user.</summary>
		/// <param name="credentials">of tokens and secrets</param>
		public virtual void addCredentials(org.apache.hadoop.security.Credentials credentials
			)
		{
			lock (subject)
			{
				getCredentialsInternal().addAll(credentials);
			}
		}

		private org.apache.hadoop.security.Credentials getCredentialsInternal()
		{
			lock (this)
			{
				org.apache.hadoop.security.Credentials credentials;
				System.Collections.Generic.ICollection<org.apache.hadoop.security.Credentials> credentialsSet
					 = subject.getPrivateCredentials<org.apache.hadoop.security.Credentials>();
				if (!credentialsSet.isEmpty())
				{
					credentials = credentialsSet.GetEnumerator().Current;
				}
				else
				{
					credentials = new org.apache.hadoop.security.Credentials();
					subject.getPrivateCredentials().add(credentials);
				}
				return credentials;
			}
		}

		/// <summary>Get the group names for this user.</summary>
		/// <returns>
		/// the list of users with the primary group first. If the command
		/// fails, it returns an empty list.
		/// </returns>
		public virtual string[] getGroupNames()
		{
			lock (this)
			{
				ensureInitialized();
				try
				{
					System.Collections.Generic.ICollection<string> result = new java.util.LinkedHashSet
						<string>(groups.getGroups(getShortUserName()));
					return Sharpen.Collections.ToArray(result, new string[result.Count]);
				}
				catch (System.IO.IOException)
				{
					LOG.warn("No groups available for user " + getShortUserName());
					return new string[0];
				}
			}
		}

		/// <summary>Return the username.</summary>
		public override string ToString()
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder(getUserName());
			sb.Append(" (auth:" + getAuthenticationMethod() + ")");
			if (getRealUser() != null)
			{
				sb.Append(" via ").Append(getRealUser().ToString());
			}
			return sb.ToString();
		}

		/// <summary>Sets the authentication method in the subject</summary>
		/// <param name="authMethod"/>
		public virtual void setAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 authMethod)
		{
			lock (this)
			{
				user.setAuthenticationMethod(authMethod);
			}
		}

		/// <summary>Sets the authentication method in the subject</summary>
		/// <param name="authMethod"/>
		public virtual void setAuthenticationMethod(org.apache.hadoop.security.SaslRpcServer.AuthMethod
			 authMethod)
		{
			user.setAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				.valueOf(authMethod));
		}

		/// <summary>Get the authentication method from the subject</summary>
		/// <returns>AuthenticationMethod in the subject, null if not present.</returns>
		public virtual org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 getAuthenticationMethod()
		{
			lock (this)
			{
				return user.getAuthenticationMethod();
			}
		}

		/// <summary>Get the authentication method from the real user's subject.</summary>
		/// <remarks>
		/// Get the authentication method from the real user's subject.  If there
		/// is no real user, return the given user's authentication method.
		/// </remarks>
		/// <returns>AuthenticationMethod in the subject, null if not present.</returns>
		public virtual org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 getRealAuthenticationMethod()
		{
			lock (this)
			{
				org.apache.hadoop.security.UserGroupInformation ugi = getRealUser();
				if (ugi == null)
				{
					ugi = this;
				}
				return ugi.getAuthenticationMethod();
			}
		}

		/// <summary>Returns the authentication method of a ugi.</summary>
		/// <remarks>
		/// Returns the authentication method of a ugi. If the authentication method is
		/// PROXY, returns the authentication method of the real user.
		/// </remarks>
		/// <param name="ugi"/>
		/// <returns>AuthenticationMethod</returns>
		public static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
			 getRealAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation ugi
			)
		{
			org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod authMethod = 
				ugi.getAuthenticationMethod();
			if (authMethod == org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				.PROXY)
			{
				authMethod = ugi.getRealUser().getAuthenticationMethod();
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
				if (o == null || Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject
					(o))
				{
					return false;
				}
				else
				{
					return subject == ((org.apache.hadoop.security.UserGroupInformation)o).subject;
				}
			}
		}

		/// <summary>Return the hash of the subject.</summary>
		public override int GetHashCode()
		{
			return Sharpen.Runtime.identityHashCode(subject);
		}

		/// <summary>Get the underlying subject from this ugi.</summary>
		/// <returns>the subject that represents this user.</returns>
		protected internal virtual javax.security.auth.Subject getSubject()
		{
			return subject;
		}

		/// <summary>Run the given action as the user.</summary>
		/// <?/>
		/// <param name="action">the method to execute</param>
		/// <returns>the value from the run method</returns>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public virtual T doAs<T>(java.security.PrivilegedAction<T> action)
		{
			logPrivilegedAction(subject, action);
			return javax.security.auth.Subject.doAs(subject, action);
		}

		/// <summary>Run the given action as the user, potentially throwing an exception.</summary>
		/// <?/>
		/// <param name="action">the method to execute</param>
		/// <returns>the value from the run method</returns>
		/// <exception cref="System.IO.IOException">if the action throws an IOException</exception>
		/// <exception cref="System.Exception">if the action throws an Error</exception>
		/// <exception cref="System.Exception">if the action throws a RuntimeException</exception>
		/// <exception cref="System.Exception">if the action throws an InterruptedException</exception>
		/// <exception cref="java.lang.reflect.UndeclaredThrowableException">if the action throws something else
		/// 	</exception>
		[org.apache.hadoop.classification.InterfaceAudience.Public]
		[org.apache.hadoop.classification.InterfaceStability.Evolving]
		public virtual T doAs<T>(java.security.PrivilegedExceptionAction<T> action)
		{
			try
			{
				logPrivilegedAction(subject, action);
				return javax.security.auth.Subject.doAs(subject, action);
			}
			catch (java.security.PrivilegedActionException pae)
			{
				System.Exception cause = pae.InnerException;
				if (LOG.isDebugEnabled())
				{
					LOG.debug("PrivilegedActionException as:" + this + " cause:" + cause);
				}
				if (cause is System.IO.IOException)
				{
					throw (System.IO.IOException)cause;
				}
				else
				{
					if (cause is System.Exception)
					{
						throw (System.Exception)cause;
					}
					else
					{
						if (cause is System.Exception)
						{
							throw (System.Exception)cause;
						}
						else
						{
							if (cause is System.Exception)
							{
								throw (System.Exception)cause;
							}
							else
							{
								throw new java.lang.reflect.UndeclaredThrowableException(cause);
							}
						}
					}
				}
			}
		}

		private void logPrivilegedAction(javax.security.auth.Subject subject, object action
			)
		{
			if (LOG.isDebugEnabled())
			{
				// would be nice if action included a descriptive toString()
				string where = new System.Exception().getStackTrace()[2].ToString();
				LOG.debug("PrivilegedAction as:" + this + " from:" + where);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void print()
		{
			System.Console.Out.WriteLine("User: " + getUserName());
			System.Console.Out.Write("Group Ids: ");
			System.Console.Out.WriteLine();
			string[] groups = getGroupNames();
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
			org.apache.hadoop.security.UserGroupInformation ugi = getCurrentUser();
			ugi.print();
			System.Console.Out.WriteLine("UGI: " + ugi);
			System.Console.Out.WriteLine("Auth method " + ugi.user.getAuthenticationMethod());
			System.Console.Out.WriteLine("Keytab " + ugi.isKeytab);
			System.Console.Out.WriteLine("============================================================"
				);
			if (args.Length == 2)
			{
				System.Console.Out.WriteLine("Getting UGI from keytab....");
				loginUserFromKeytab(args[0], args[1]);
				getCurrentUser().print();
				System.Console.Out.WriteLine("Keytab: " + ugi);
				System.Console.Out.WriteLine("Auth method " + loginUser.user.getAuthenticationMethod
					());
				System.Console.Out.WriteLine("Keytab " + loginUser.isKeytab);
			}
		}
	}
}
