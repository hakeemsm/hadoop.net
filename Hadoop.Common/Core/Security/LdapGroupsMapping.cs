using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Sun.Jndi.Ldap;
using Hadoop.Common.Core.Conf;
using Javax.Naming;
using Javax.Naming.Directory;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Security
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
	public class LdapGroupsMapping : GroupMappingServiceProvider, Configurable
	{
		public const string LdapConfigPrefix = "hadoop.security.group.mapping.ldap";

		public const string LdapUrlKey = LdapConfigPrefix + ".url";

		public const string LdapUrlDefault = string.Empty;

		public const string LdapUseSslKey = LdapConfigPrefix + ".ssl";

		public static readonly bool LdapUseSslDefault = false;

		public const string LdapKeystoreKey = LdapConfigPrefix + ".ssl.keystore";

		public const string LdapKeystoreDefault = string.Empty;

		public const string LdapKeystorePasswordKey = LdapConfigPrefix + ".ssl.keystore.password";

		public const string LdapKeystorePasswordDefault = string.Empty;

		public const string LdapKeystorePasswordFileKey = LdapKeystorePasswordKey + ".file";

		public const string LdapKeystorePasswordFileDefault = string.Empty;

		public const string BindUserKey = LdapConfigPrefix + ".bind.user";

		public const string BindUserDefault = string.Empty;

		public const string BindPasswordKey = LdapConfigPrefix + ".bind.password";

		public const string BindPasswordDefault = string.Empty;

		public const string BindPasswordFileKey = BindPasswordKey + ".file";

		public const string BindPasswordFileDefault = string.Empty;

		public const string BaseDnKey = LdapConfigPrefix + ".base";

		public const string BaseDnDefault = string.Empty;

		public const string UserSearchFilterKey = LdapConfigPrefix + ".search.filter.user";

		public const string UserSearchFilterDefault = "(&(objectClass=user)(sAMAccountName={0}))";

		public const string GroupSearchFilterKey = LdapConfigPrefix + ".search.filter.group";

		public const string GroupSearchFilterDefault = "(objectClass=group)";

		public const string GroupMembershipAttrKey = LdapConfigPrefix + ".search.attr.member";

		public const string GroupMembershipAttrDefault = "member";

		public const string GroupNameAttrKey = LdapConfigPrefix + ".search.attr.group.name";

		public const string GroupNameAttrDefault = "cn";

		public const string DirectorySearchTimeout = LdapConfigPrefix + ".directory.search.timeout";

		public const int DirectorySearchTimeoutDefault = 10000;

		private static readonly Log Log = LogFactory.GetLog(typeof(LdapGroupsMapping));

		private static readonly SearchControls SearchControls = new SearchControls();

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
			SearchControls.SetSearchScope(SearchControls.SubtreeScope);
		}

		private DirContext ctx;

		private Configuration conf;

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

		public static int ReconnectRetryCount = 3;

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
		public virtual IList<string> GetGroups(string user)
		{
			lock (this)
			{
				IList<string> emptyResults = new AList<string>();
				/*
				* Normal garbage collection takes care of removing Context instances when they are no longer in use.
				* Connections used by Context instances being garbage collected will be closed automatically.
				* So in case connection is closed and gets CommunicationException, retry some times with new new DirContext/connection.
				*/
				try
				{
					return DoGetGroups(user);
				}
				catch (CommunicationException)
				{
					Log.Warn("Connection is closed, will try to reconnect");
				}
				catch (NamingException e)
				{
					Log.Warn("Exception trying to get groups for user " + user + ": " + e.Message);
					return emptyResults;
				}
				int retryCount = 0;
				while (retryCount++ < ReconnectRetryCount)
				{
					//reset ctx so that new DirContext can be created with new connection
					this.ctx = null;
					try
					{
						return DoGetGroups(user);
					}
					catch (CommunicationException)
					{
						Log.Warn("Connection being closed, reconnecting failed, retryCount = " + retryCount
							);
					}
					catch (NamingException e)
					{
						Log.Warn("Exception trying to get groups for user " + user + ":" + e.Message);
						return emptyResults;
					}
				}
				return emptyResults;
			}
		}

		/// <exception cref="Javax.Naming.NamingException"/>
		internal virtual IList<string> DoGetGroups(string user)
		{
			IList<string> groups = new AList<string>();
			DirContext ctx = GetDirContext();
			// Search for the user. We'll only ever need to look at the first result
			NamingEnumeration<SearchResult> results = ctx.Search(baseDN, userSearchFilter, new 
				object[] { user }, SearchControls);
			if (results.MoveNext())
			{
				SearchResult result = results.Current;
				string userDn = result.GetNameInNamespace();
				NamingEnumeration<SearchResult> groupResults = ctx.Search(baseDN, "(&" + groupSearchFilter
					 + "(" + groupMemberAttr + "={0}))", new object[] { userDn }, SearchControls);
				while (groupResults.MoveNext())
				{
					SearchResult groupResult = groupResults.Current;
					Attribute groupName = groupResult.GetAttributes().Get(groupNameAttr);
					groups.AddItem(groupName.Get().ToString());
				}
			}
			return groups;
		}

		/// <exception cref="Javax.Naming.NamingException"/>
		internal virtual DirContext GetDirContext()
		{
			if (ctx == null)
			{
				// Set up the initial environment for LDAP connectivity
				Hashtable<string, string> env = new Hashtable<string, string>();
				env[Context.InitialContextFactory] = typeof(LdapCtxFactory).FullName;
				env[Context.ProviderUrl] = ldapUrl;
				env[Context.SecurityAuthentication] = "simple";
				// Set up SSL security, if necessary
				if (useSsl)
				{
					env[Context.SecurityProtocol] = "ssl";
					Runtime.SetProperty("javax.net.ssl.keyStore", keystore);
					Runtime.SetProperty("javax.net.ssl.keyStorePassword", keystorePass);
				}
				env[Context.SecurityPrincipal] = bindUser;
				env[Context.SecurityCredentials] = bindPassword;
				ctx = new InitialDirContext(env);
			}
			return ctx;
		}

		/// <summary>Caches groups, no need to do that for this provider</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CacheGroupsRefresh()
		{
		}

		// does nothing in this provider of user to groups mapping
		/// <summary>Adds groups to cache, no need to do that for this provider</summary>
		/// <param name="groups">unused</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CacheGroupsAdd(IList<string> groups)
		{
		}

		// does nothing in this provider of user to groups mapping
		public virtual Configuration GetConf()
		{
			lock (this)
			{
				return conf;
			}
		}

		public virtual void SetConf(Configuration conf)
		{
			lock (this)
			{
				ldapUrl = conf.Get(LdapUrlKey, LdapUrlDefault);
				if (ldapUrl == null || ldapUrl.IsEmpty())
				{
					throw new RuntimeException("LDAP URL is not configured");
				}
				useSsl = conf.GetBoolean(LdapUseSslKey, LdapUseSslDefault);
				keystore = conf.Get(LdapKeystoreKey, LdapKeystoreDefault);
				keystorePass = GetPassword(conf, LdapKeystorePasswordKey, LdapKeystorePasswordDefault
					);
				if (keystorePass.IsEmpty())
				{
					keystorePass = ExtractPassword(conf.Get(LdapKeystorePasswordFileKey, LdapKeystorePasswordFileDefault
						));
				}
				bindUser = conf.Get(BindUserKey, BindUserDefault);
				bindPassword = GetPassword(conf, BindPasswordKey, BindPasswordDefault);
				if (bindPassword.IsEmpty())
				{
					bindPassword = ExtractPassword(conf.Get(BindPasswordFileKey, BindPasswordFileDefault
						));
				}
				baseDN = conf.Get(BaseDnKey, BaseDnDefault);
				groupSearchFilter = conf.Get(GroupSearchFilterKey, GroupSearchFilterDefault);
				userSearchFilter = conf.Get(UserSearchFilterKey, UserSearchFilterDefault);
				groupMemberAttr = conf.Get(GroupMembershipAttrKey, GroupMembershipAttrDefault);
				groupNameAttr = conf.Get(GroupNameAttrKey, GroupNameAttrDefault);
				int dirSearchTimeout = conf.GetInt(DirectorySearchTimeout, DirectorySearchTimeoutDefault
					);
				SearchControls.SetTimeLimit(dirSearchTimeout);
				// Limit the attributes returned to only those required to speed up the search. See HADOOP-10626 for more details.
				SearchControls.SetReturningAttributes(new string[] { groupNameAttr });
				this.conf = conf;
			}
		}

		internal virtual string GetPassword(Configuration conf, string alias, string defaultPass
			)
		{
			string password = null;
			try
			{
				char[] passchars = conf.GetPassword(alias);
				if (passchars != null)
				{
					password = new string(passchars);
				}
				else
				{
					password = defaultPass;
				}
			}
			catch (IOException ioe)
			{
				Log.Warn("Exception while trying to password for alias " + alias + ": " + ioe.Message
					);
			}
			return password;
		}

		internal virtual string ExtractPassword(string pwFile)
		{
			if (pwFile.IsEmpty())
			{
				// If there is no password file defined, we'll assume that we should do
				// an anonymous bind
				return string.Empty;
			}
			StringBuilder password = new StringBuilder();
			try
			{
				using (StreamReader reader = new InputStreamReader(new FileInputStream(pwFile), Charsets
					.Utf8))
				{
					int c = reader.Read();
					while (c > -1)
					{
						password.Append((char)c);
						c = reader.Read();
					}
					return password.ToString().Trim();
				}
			}
			catch (IOException ioe)
			{
				throw new RuntimeException("Could not read password file: " + pwFile, ioe);
			}
		}
	}
}
