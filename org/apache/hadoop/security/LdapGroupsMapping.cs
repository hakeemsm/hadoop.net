using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>
	/// An implementation of
	/// <see cref="GroupMappingServiceProvider"/>
	/// which
	/// connects directly to an LDAP server for determining group membership.
	/// This provider should be used only if it is necessary to map users to
	/// groups that reside exclusively in an Active Directory or LDAP installation.
	/// The common case for a Hadoop installation will be that LDAP users and groups
	/// materialized on the Unix servers, and for an installation like that,
	/// ShellBasedUnixGroupsMapping is preferred. However, in cases where
	/// those users and groups aren't materialized in Unix, but need to be used for
	/// access control, this class may be used to communicate directly with the LDAP
	/// server.
	/// It is important to note that resolving group mappings will incur network
	/// traffic, and may cause degraded performance, although user-group mappings
	/// will be cached via the infrastructure provided by
	/// <see cref="Groups"/>
	/// .
	/// This implementation does not support configurable search limits. If a filter
	/// is used for searching users or groups which returns more results than are
	/// allowed by the server, an exception will be thrown.
	/// The implementation also does not attempt to resolve group hierarchies. In
	/// order to be considered a member of a group, the user must be an explicit
	/// member in LDAP.
	/// </summary>
	public class LdapGroupsMapping : org.apache.hadoop.security.GroupMappingServiceProvider
		, org.apache.hadoop.conf.Configurable
	{
		public const string LDAP_CONFIG_PREFIX = "hadoop.security.group.mapping.ldap";

		public const string LDAP_URL_KEY = LDAP_CONFIG_PREFIX + ".url";

		public const string LDAP_URL_DEFAULT = string.Empty;

		public const string LDAP_USE_SSL_KEY = LDAP_CONFIG_PREFIX + ".ssl";

		public static readonly bool LDAP_USE_SSL_DEFAULT = false;

		public const string LDAP_KEYSTORE_KEY = LDAP_CONFIG_PREFIX + ".ssl.keystore";

		public const string LDAP_KEYSTORE_DEFAULT = string.Empty;

		public const string LDAP_KEYSTORE_PASSWORD_KEY = LDAP_CONFIG_PREFIX + ".ssl.keystore.password";

		public const string LDAP_KEYSTORE_PASSWORD_DEFAULT = string.Empty;

		public const string LDAP_KEYSTORE_PASSWORD_FILE_KEY = LDAP_KEYSTORE_PASSWORD_KEY 
			+ ".file";

		public const string LDAP_KEYSTORE_PASSWORD_FILE_DEFAULT = string.Empty;

		public const string BIND_USER_KEY = LDAP_CONFIG_PREFIX + ".bind.user";

		public const string BIND_USER_DEFAULT = string.Empty;

		public const string BIND_PASSWORD_KEY = LDAP_CONFIG_PREFIX + ".bind.password";

		public const string BIND_PASSWORD_DEFAULT = string.Empty;

		public const string BIND_PASSWORD_FILE_KEY = BIND_PASSWORD_KEY + ".file";

		public const string BIND_PASSWORD_FILE_DEFAULT = string.Empty;

		public const string BASE_DN_KEY = LDAP_CONFIG_PREFIX + ".base";

		public const string BASE_DN_DEFAULT = string.Empty;

		public const string USER_SEARCH_FILTER_KEY = LDAP_CONFIG_PREFIX + ".search.filter.user";

		public const string USER_SEARCH_FILTER_DEFAULT = "(&(objectClass=user)(sAMAccountName={0}))";

		public const string GROUP_SEARCH_FILTER_KEY = LDAP_CONFIG_PREFIX + ".search.filter.group";

		public const string GROUP_SEARCH_FILTER_DEFAULT = "(objectClass=group)";

		public const string GROUP_MEMBERSHIP_ATTR_KEY = LDAP_CONFIG_PREFIX + ".search.attr.member";

		public const string GROUP_MEMBERSHIP_ATTR_DEFAULT = "member";

		public const string GROUP_NAME_ATTR_KEY = LDAP_CONFIG_PREFIX + ".search.attr.group.name";

		public const string GROUP_NAME_ATTR_DEFAULT = "cn";

		public const string DIRECTORY_SEARCH_TIMEOUT = LDAP_CONFIG_PREFIX + ".directory.search.timeout";

		public const int DIRECTORY_SEARCH_TIMEOUT_DEFAULT = 10000;

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.LdapGroupsMapping
			)));

		private static readonly javax.naming.directory.SearchControls SEARCH_CONTROLS = new 
			javax.naming.directory.SearchControls();

		static LdapGroupsMapping()
		{
			/*
			* URL of the LDAP server
			*/
			/*
			* Should SSL be used to connect to the server
			*/
			/*
			* File path to the location of the SSL keystore to use
			*/
			/*
			* Password for the keystore
			*/
			/*
			* User to bind to the LDAP server with
			*/
			/*
			* Password for the bind user
			*/
			/*
			* Base distinguished name to use for searches
			*/
			/*
			* Any additional filters to apply when searching for users
			*/
			/*
			* Any additional filters to apply when finding relevant groups
			*/
			/*
			* LDAP attribute to use for determining group membership
			*/
			/*
			* LDAP attribute to use for identifying a group's name
			*/
			/*
			* LDAP {@link SearchControls} attribute to set the time limit
			* for an invoked directory search. Prevents infinite wait cases.
			*/
			// 10s
			SEARCH_CONTROLS.setSearchScope(javax.naming.directory.SearchControls.SUBTREE_SCOPE
				);
		}

		private javax.naming.directory.DirContext ctx;

		private org.apache.hadoop.conf.Configuration conf;

		private string ldapUrl;

		private bool useSsl;

		private string keystore;

		private string keystorePass;

		private string bindUser;

		private string bindPassword;

		private string baseDN;

		private string groupSearchFilter;

		private string userSearchFilter;

		private string groupMemberAttr;

		private string groupNameAttr;

		public static int RECONNECT_RETRY_COUNT = 3;

		/// <summary>Returns list of groups for a user.</summary>
		/// <remarks>
		/// Returns list of groups for a user.
		/// The LdapCtx which underlies the DirContext object is not thread-safe, so
		/// we need to block around this whole method. The caching infrastructure will
		/// ensure that performance stays in an acceptable range.
		/// </remarks>
		/// <param name="user">get groups for this user</param>
		/// <returns>list of groups for a given user</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual System.Collections.Generic.IList<string> getGroups(string user)
		{
			lock (this)
			{
				System.Collections.Generic.IList<string> emptyResults = new System.Collections.Generic.List
					<string>();
				/*
				* Normal garbage collection takes care of removing Context instances when they are no longer in use.
				* Connections used by Context instances being garbage collected will be closed automatically.
				* So in case connection is closed and gets CommunicationException, retry some times with new new DirContext/connection.
				*/
				try
				{
					return doGetGroups(user);
				}
				catch (javax.naming.CommunicationException)
				{
					LOG.warn("Connection is closed, will try to reconnect");
				}
				catch (javax.naming.NamingException e)
				{
					LOG.warn("Exception trying to get groups for user " + user + ": " + e.Message);
					return emptyResults;
				}
				int retryCount = 0;
				while (retryCount++ < RECONNECT_RETRY_COUNT)
				{
					//reset ctx so that new DirContext can be created with new connection
					this.ctx = null;
					try
					{
						return doGetGroups(user);
					}
					catch (javax.naming.CommunicationException)
					{
						LOG.warn("Connection being closed, reconnecting failed, retryCount = " + retryCount
							);
					}
					catch (javax.naming.NamingException e)
					{
						LOG.warn("Exception trying to get groups for user " + user + ":" + e.Message);
						return emptyResults;
					}
				}
				return emptyResults;
			}
		}

		/// <exception cref="javax.naming.NamingException"/>
		internal virtual System.Collections.Generic.IList<string> doGetGroups(string user
			)
		{
			System.Collections.Generic.IList<string> groups = new System.Collections.Generic.List
				<string>();
			javax.naming.directory.DirContext ctx = getDirContext();
			// Search for the user. We'll only ever need to look at the first result
			javax.naming.NamingEnumeration<javax.naming.directory.SearchResult> results = ctx
				.search(baseDN, userSearchFilter, new object[] { user }, SEARCH_CONTROLS);
			if (results.MoveNext())
			{
				javax.naming.directory.SearchResult result = results.Current;
				string userDn = result.getNameInNamespace();
				javax.naming.NamingEnumeration<javax.naming.directory.SearchResult> groupResults = 
					ctx.search(baseDN, "(&" + groupSearchFilter + "(" + groupMemberAttr + "={0}))", 
					new object[] { userDn }, SEARCH_CONTROLS);
				while (groupResults.MoveNext())
				{
					javax.naming.directory.SearchResult groupResult = groupResults.Current;
					javax.naming.directory.Attribute groupName = groupResult.getAttributes().get(groupNameAttr
						);
					groups.add(groupName.get().ToString());
				}
			}
			return groups;
		}

		/// <exception cref="javax.naming.NamingException"/>
		internal virtual javax.naming.directory.DirContext getDirContext()
		{
			if (ctx == null)
			{
				// Set up the initial environment for LDAP connectivity
				java.util.Hashtable<string, string> env = new java.util.Hashtable<string, string>
					();
				env[javax.naming.Context.INITIAL_CONTEXT_FACTORY] = Sharpen.Runtime.getClassForType
					(typeof(com.sun.jndi.ldap.LdapCtxFactory)).getName();
				env[javax.naming.Context.PROVIDER_URL] = ldapUrl;
				env[javax.naming.Context.SECURITY_AUTHENTICATION] = "simple";
				// Set up SSL security, if necessary
				if (useSsl)
				{
					env[javax.naming.Context.SECURITY_PROTOCOL] = "ssl";
					Sharpen.Runtime.setProperty("javax.net.ssl.keyStore", keystore);
					Sharpen.Runtime.setProperty("javax.net.ssl.keyStorePassword", keystorePass);
				}
				env[javax.naming.Context.SECURITY_PRINCIPAL] = bindUser;
				env[javax.naming.Context.SECURITY_CREDENTIALS] = bindPassword;
				ctx = new javax.naming.directory.InitialDirContext(env);
			}
			return ctx;
		}

		/// <summary>Caches groups, no need to do that for this provider</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void cacheGroupsRefresh()
		{
		}

		// does nothing in this provider of user to groups mapping
		/// <summary>Adds groups to cache, no need to do that for this provider</summary>
		/// <param name="groups">unused</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void cacheGroupsAdd(System.Collections.Generic.IList<string> groups
			)
		{
		}

		// does nothing in this provider of user to groups mapping
		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			lock (this)
			{
				return conf;
			}
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			lock (this)
			{
				ldapUrl = conf.get(LDAP_URL_KEY, LDAP_URL_DEFAULT);
				if (ldapUrl == null || ldapUrl.isEmpty())
				{
					throw new System.Exception("LDAP URL is not configured");
				}
				useSsl = conf.getBoolean(LDAP_USE_SSL_KEY, LDAP_USE_SSL_DEFAULT);
				keystore = conf.get(LDAP_KEYSTORE_KEY, LDAP_KEYSTORE_DEFAULT);
				keystorePass = getPassword(conf, LDAP_KEYSTORE_PASSWORD_KEY, LDAP_KEYSTORE_PASSWORD_DEFAULT
					);
				if (keystorePass.isEmpty())
				{
					keystorePass = extractPassword(conf.get(LDAP_KEYSTORE_PASSWORD_FILE_KEY, LDAP_KEYSTORE_PASSWORD_FILE_DEFAULT
						));
				}
				bindUser = conf.get(BIND_USER_KEY, BIND_USER_DEFAULT);
				bindPassword = getPassword(conf, BIND_PASSWORD_KEY, BIND_PASSWORD_DEFAULT);
				if (bindPassword.isEmpty())
				{
					bindPassword = extractPassword(conf.get(BIND_PASSWORD_FILE_KEY, BIND_PASSWORD_FILE_DEFAULT
						));
				}
				baseDN = conf.get(BASE_DN_KEY, BASE_DN_DEFAULT);
				groupSearchFilter = conf.get(GROUP_SEARCH_FILTER_KEY, GROUP_SEARCH_FILTER_DEFAULT
					);
				userSearchFilter = conf.get(USER_SEARCH_FILTER_KEY, USER_SEARCH_FILTER_DEFAULT);
				groupMemberAttr = conf.get(GROUP_MEMBERSHIP_ATTR_KEY, GROUP_MEMBERSHIP_ATTR_DEFAULT
					);
				groupNameAttr = conf.get(GROUP_NAME_ATTR_KEY, GROUP_NAME_ATTR_DEFAULT);
				int dirSearchTimeout = conf.getInt(DIRECTORY_SEARCH_TIMEOUT, DIRECTORY_SEARCH_TIMEOUT_DEFAULT
					);
				SEARCH_CONTROLS.setTimeLimit(dirSearchTimeout);
				// Limit the attributes returned to only those required to speed up the search. See HADOOP-10626 for more details.
				SEARCH_CONTROLS.setReturningAttributes(new string[] { groupNameAttr });
				this.conf = conf;
			}
		}

		internal virtual string getPassword(org.apache.hadoop.conf.Configuration conf, string
			 alias, string defaultPass)
		{
			string password = null;
			try
			{
				char[] passchars = conf.getPassword(alias);
				if (passchars != null)
				{
					password = new string(passchars);
				}
				else
				{
					password = defaultPass;
				}
			}
			catch (System.IO.IOException ioe)
			{
				LOG.warn("Exception while trying to password for alias " + alias + ": " + ioe.Message
					);
			}
			return password;
		}

		internal virtual string extractPassword(string pwFile)
		{
			if (pwFile.isEmpty())
			{
				// If there is no password file defined, we'll assume that we should do
				// an anonymous bind
				return string.Empty;
			}
			java.lang.StringBuilder password = new java.lang.StringBuilder();
			try
			{
				using (java.io.Reader reader = new java.io.InputStreamReader(new java.io.FileInputStream
					(pwFile), org.apache.commons.io.Charsets.UTF_8))
				{
					int c = reader.read();
					while (c > -1)
					{
						password.Append((char)c);
						c = reader.read();
					}
					return password.ToString().Trim();
				}
			}
			catch (System.IO.IOException ioe)
			{
				throw new System.Exception("Could not read password file: " + pwFile, ioe);
			}
		}
	}
}
