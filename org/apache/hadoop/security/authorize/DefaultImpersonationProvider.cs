using Sharpen;

namespace org.apache.hadoop.security.authorize
{
	public class DefaultImpersonationProvider : org.apache.hadoop.security.authorize.ImpersonationProvider
	{
		private const string CONF_HOSTS = ".hosts";

		private const string CONF_USERS = ".users";

		private const string CONF_GROUPS = ".groups";

		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.security.authorize.AccessControlList
			> proxyUserAcl = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.security.authorize.AccessControlList
			>();

		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.util.MachineList
			> proxyHosts = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.util.MachineList
			>();

		private org.apache.hadoop.conf.Configuration conf;

		private static org.apache.hadoop.security.authorize.DefaultImpersonationProvider 
			testProvider;

		// acl and list of hosts per proxyuser
		public static org.apache.hadoop.security.authorize.DefaultImpersonationProvider getTestProvider
			()
		{
			lock (typeof(DefaultImpersonationProvider))
			{
				if (testProvider == null)
				{
					testProvider = new org.apache.hadoop.security.authorize.DefaultImpersonationProvider
						();
					testProvider.setConf(new org.apache.hadoop.conf.Configuration());
					testProvider.init(org.apache.hadoop.security.authorize.ProxyUsers.CONF_HADOOP_PROXYUSER
						);
				}
				return testProvider;
			}
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}

		private string configPrefix;

		public virtual void init(string configurationPrefix)
		{
			configPrefix = configurationPrefix + (configurationPrefix.EndsWith(".") ? string.Empty
				 : ".");
			// constructing regex to match the following patterns:
			//   $configPrefix.[ANY].users
			//   $configPrefix.[ANY].groups
			//   $configPrefix.[ANY].hosts
			//
			string prefixRegEx = configPrefix.Replace(".", "\\.");
			string usersGroupsRegEx = prefixRegEx + "[^.]*(" + java.util.regex.Pattern.quote(
				CONF_USERS) + "|" + java.util.regex.Pattern.quote(CONF_GROUPS) + ")";
			string hostsRegEx = prefixRegEx + "[^.]*" + java.util.regex.Pattern.quote(CONF_HOSTS
				);
			// get list of users and groups per proxyuser
			System.Collections.Generic.IDictionary<string, string> allMatchKeys = conf.getValByRegex
				(usersGroupsRegEx);
			foreach (System.Collections.Generic.KeyValuePair<string, string> entry in allMatchKeys)
			{
				string aclKey = getAclKey(entry.Key);
				if (!proxyUserAcl.Contains(aclKey))
				{
					proxyUserAcl[aclKey] = new org.apache.hadoop.security.authorize.AccessControlList
						(allMatchKeys[aclKey + CONF_USERS], allMatchKeys[aclKey + CONF_GROUPS]);
				}
			}
			// get hosts per proxyuser
			allMatchKeys = conf.getValByRegex(hostsRegEx);
			foreach (System.Collections.Generic.KeyValuePair<string, string> entry_1 in allMatchKeys)
			{
				proxyHosts[entry_1.Key] = new org.apache.hadoop.util.MachineList(entry_1.Value);
			}
		}

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}

		/// <exception cref="org.apache.hadoop.security.authorize.AuthorizationException"/>
		public virtual void authorize(org.apache.hadoop.security.UserGroupInformation user
			, string remoteAddress)
		{
			org.apache.hadoop.security.UserGroupInformation realUser = user.getRealUser();
			if (realUser == null)
			{
				return;
			}
			org.apache.hadoop.security.authorize.AccessControlList acl = proxyUserAcl[configPrefix
				 + realUser.getShortUserName()];
			if (acl == null || !acl.isUserAllowed(user))
			{
				throw new org.apache.hadoop.security.authorize.AuthorizationException("User: " + 
					realUser.getUserName() + " is not allowed to impersonate " + user.getUserName());
			}
			org.apache.hadoop.util.MachineList MachineList = proxyHosts[getProxySuperuserIpConfKey
				(realUser.getShortUserName())];
			if (MachineList == null || !MachineList.includes(remoteAddress))
			{
				throw new org.apache.hadoop.security.authorize.AuthorizationException("Unauthorized connection for super-user: "
					 + realUser.getUserName() + " from IP " + remoteAddress);
			}
		}

		private string getAclKey(string key)
		{
			int endIndex = key.LastIndexOf(".");
			if (endIndex != -1)
			{
				return Sharpen.Runtime.substring(key, 0, endIndex);
			}
			return key;
		}

		/// <summary>Returns configuration key for effective usergroups allowed for a superuser
		/// 	</summary>
		/// <param name="userName">name of the superuser</param>
		/// <returns>configuration key for superuser usergroups</returns>
		public virtual string getProxySuperuserUserConfKey(string userName)
		{
			return configPrefix + userName + CONF_USERS;
		}

		/// <summary>Returns configuration key for effective groups allowed for a superuser</summary>
		/// <param name="userName">name of the superuser</param>
		/// <returns>configuration key for superuser groups</returns>
		public virtual string getProxySuperuserGroupConfKey(string userName)
		{
			return configPrefix + userName + CONF_GROUPS;
		}

		/// <summary>Return configuration key for superuser ip addresses</summary>
		/// <param name="userName">name of the superuser</param>
		/// <returns>configuration key for superuser ip-addresses</returns>
		public virtual string getProxySuperuserIpConfKey(string userName)
		{
			return configPrefix + userName + CONF_HOSTS;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual System.Collections.Generic.IDictionary<string, System.Collections.Generic.ICollection
			<string>> getProxyGroups()
		{
			System.Collections.Generic.IDictionary<string, System.Collections.Generic.ICollection
				<string>> proxyGroups = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.ICollection
				<string>>();
			foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.security.authorize.AccessControlList
				> entry in proxyUserAcl)
			{
				proxyGroups[entry.Key + CONF_GROUPS] = entry.Value.getGroups();
			}
			return proxyGroups;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual System.Collections.Generic.IDictionary<string, System.Collections.Generic.ICollection
			<string>> getProxyHosts()
		{
			System.Collections.Generic.IDictionary<string, System.Collections.Generic.ICollection
				<string>> tmpProxyHosts = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.ICollection
				<string>>();
			foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.util.MachineList
				> proxyHostEntry in proxyHosts)
			{
				tmpProxyHosts[proxyHostEntry.Key] = proxyHostEntry.Value.getCollection();
			}
			return tmpProxyHosts;
		}
	}
}
