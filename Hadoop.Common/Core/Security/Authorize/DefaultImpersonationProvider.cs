using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authorize
{
	public class DefaultImpersonationProvider : ImpersonationProvider
	{
		private const string ConfHosts = ".hosts";

		private const string ConfUsers = ".users";

		private const string ConfGroups = ".groups";

		private IDictionary<string, AccessControlList> proxyUserAcl = new Dictionary<string
			, AccessControlList>();

		private IDictionary<string, MachineList> proxyHosts = new Dictionary<string, MachineList
			>();

		private Configuration conf;

		private static DefaultImpersonationProvider testProvider;

		// acl and list of hosts per proxyuser
		public static DefaultImpersonationProvider GetTestProvider()
		{
			lock (typeof(DefaultImpersonationProvider))
			{
				if (testProvider == null)
				{
					testProvider = new DefaultImpersonationProvider();
					testProvider.SetConf(new Configuration());
					testProvider.Init(ProxyUsers.ConfHadoopProxyuser);
				}
				return testProvider;
			}
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		private string configPrefix;

		public virtual void Init(string configurationPrefix)
		{
			configPrefix = configurationPrefix + (configurationPrefix.EndsWith(".") ? string.Empty
				 : ".");
			// constructing regex to match the following patterns:
			//   $configPrefix.[ANY].users
			//   $configPrefix.[ANY].groups
			//   $configPrefix.[ANY].hosts
			//
			string prefixRegEx = configPrefix.Replace(".", "\\.");
			string usersGroupsRegEx = prefixRegEx + "[^.]*(" + Sharpen.Pattern.Quote(ConfUsers
				) + "|" + Sharpen.Pattern.Quote(ConfGroups) + ")";
			string hostsRegEx = prefixRegEx + "[^.]*" + Sharpen.Pattern.Quote(ConfHosts);
			// get list of users and groups per proxyuser
			IDictionary<string, string> allMatchKeys = conf.GetValByRegex(usersGroupsRegEx);
			foreach (KeyValuePair<string, string> entry in allMatchKeys)
			{
				string aclKey = GetAclKey(entry.Key);
				if (!proxyUserAcl.Contains(aclKey))
				{
					proxyUserAcl[aclKey] = new AccessControlList(allMatchKeys[aclKey + ConfUsers], allMatchKeys
						[aclKey + ConfGroups]);
				}
			}
			// get hosts per proxyuser
			allMatchKeys = conf.GetValByRegex(hostsRegEx);
			foreach (KeyValuePair<string, string> entry_1 in allMatchKeys)
			{
				proxyHosts[entry_1.Key] = new MachineList(entry_1.Value);
			}
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		public virtual void Authorize(UserGroupInformation user, string remoteAddress)
		{
			UserGroupInformation realUser = user.GetRealUser();
			if (realUser == null)
			{
				return;
			}
			AccessControlList acl = proxyUserAcl[configPrefix + realUser.GetShortUserName()];
			if (acl == null || !acl.IsUserAllowed(user))
			{
				throw new AuthorizationException("User: " + realUser.GetUserName() + " is not allowed to impersonate "
					 + user.GetUserName());
			}
			MachineList MachineList = proxyHosts[GetProxySuperuserIpConfKey(realUser.GetShortUserName
				())];
			if (MachineList == null || !MachineList.Includes(remoteAddress))
			{
				throw new AuthorizationException("Unauthorized connection for super-user: " + realUser
					.GetUserName() + " from IP " + remoteAddress);
			}
		}

		private string GetAclKey(string key)
		{
			int endIndex = key.LastIndexOf(".");
			if (endIndex != -1)
			{
				return Sharpen.Runtime.Substring(key, 0, endIndex);
			}
			return key;
		}

		/// <summary>Returns configuration key for effective usergroups allowed for a superuser
		/// 	</summary>
		/// <param name="userName">name of the superuser</param>
		/// <returns>configuration key for superuser usergroups</returns>
		public virtual string GetProxySuperuserUserConfKey(string userName)
		{
			return configPrefix + userName + ConfUsers;
		}

		/// <summary>Returns configuration key for effective groups allowed for a superuser</summary>
		/// <param name="userName">name of the superuser</param>
		/// <returns>configuration key for superuser groups</returns>
		public virtual string GetProxySuperuserGroupConfKey(string userName)
		{
			return configPrefix + userName + ConfGroups;
		}

		/// <summary>Return configuration key for superuser ip addresses</summary>
		/// <param name="userName">name of the superuser</param>
		/// <returns>configuration key for superuser ip-addresses</returns>
		public virtual string GetProxySuperuserIpConfKey(string userName)
		{
			return configPrefix + userName + ConfHosts;
		}

		[VisibleForTesting]
		public virtual IDictionary<string, ICollection<string>> GetProxyGroups()
		{
			IDictionary<string, ICollection<string>> proxyGroups = new Dictionary<string, ICollection
				<string>>();
			foreach (KeyValuePair<string, AccessControlList> entry in proxyUserAcl)
			{
				proxyGroups[entry.Key + ConfGroups] = entry.Value.GetGroups();
			}
			return proxyGroups;
		}

		[VisibleForTesting]
		public virtual IDictionary<string, ICollection<string>> GetProxyHosts()
		{
			IDictionary<string, ICollection<string>> tmpProxyHosts = new Dictionary<string, ICollection
				<string>>();
			foreach (KeyValuePair<string, MachineList> proxyHostEntry in proxyHosts)
			{
				tmpProxyHosts[proxyHostEntry.Key] = proxyHostEntry.Value.GetCollection();
			}
			return tmpProxyHosts;
		}
	}
}
