using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// An implementation of
	/// <see cref="GroupMappingServiceProvider"/>
	/// which
	/// composites other group mapping providers for determining group membership.
	/// This allows to combine existing provider implementations and composite
	/// a virtually new provider without customized development to deal with complex situation.
	/// </summary>
	public class CompositeGroupsMapping : GroupMappingServiceProvider, Configurable
	{
		public const string MappingProvidersConfigKey = GroupMappingConfigPrefix + ".providers";

		public const string MappingProvidersCombinedConfigKey = MappingProvidersConfigKey
			 + ".combined";

		public const string MappingProviderConfigPrefix = GroupMappingConfigPrefix + ".provider";

		private static readonly Log Log = LogFactory.GetLog(typeof(CompositeGroupsMapping
			));

		private IList<GroupMappingServiceProvider> providersList = new AList<GroupMappingServiceProvider
			>();

		private Configuration conf;

		private bool combined;

		/// <summary>Returns list of groups for a user.</summary>
		/// <param name="user">get groups for this user</param>
		/// <returns>list of groups for a given user</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual IList<string> GetGroups(string user)
		{
			lock (this)
			{
				ICollection<string> groupSet = new TreeSet<string>();
				IList<string> groups = null;
				foreach (GroupMappingServiceProvider provider in providersList)
				{
					try
					{
						groups = provider.GetGroups(user);
					}
					catch (Exception)
					{
					}
					//LOG.warn("Exception trying to get groups for user " + user, e);      
					if (groups != null && !groups.IsEmpty())
					{
						Sharpen.Collections.AddAll(groupSet, groups);
						if (!combined)
						{
							break;
						}
					}
				}
				IList<string> results = new AList<string>(groupSet.Count);
				Sharpen.Collections.AddAll(results, groupSet);
				return results;
			}
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
				this.conf = conf;
				this.combined = conf.GetBoolean(MappingProvidersCombinedConfigKey, true);
				LoadMappingProviders();
			}
		}

		private void LoadMappingProviders()
		{
			string[] providerNames = conf.GetStrings(MappingProvidersConfigKey, new string[] 
				{  });
			string providerKey;
			foreach (string name in providerNames)
			{
				providerKey = MappingProviderConfigPrefix + "." + name;
				Type providerClass = conf.GetClass(providerKey, null);
				if (providerClass == null)
				{
					Log.Error("The mapping provider, " + name + " does not have a valid class");
				}
				else
				{
					AddMappingProvider(name, providerClass);
				}
			}
		}

		private void AddMappingProvider(string providerName, Type providerClass)
		{
			Configuration newConf = PrepareConf(providerName);
			GroupMappingServiceProvider provider = (GroupMappingServiceProvider)ReflectionUtils
				.NewInstance(providerClass, newConf);
			providersList.AddItem(provider);
		}

		/*
		* For any provider specific configuration properties, such as "hadoop.security.group.mapping.ldap.url"
		* and the like, allow them to be configured as "hadoop.security.group.mapping.provider.PROVIDER-X.ldap.url",
		* so that a provider such as LdapGroupsMapping can be used to composite a complex one with other providers.
		*/
		private Configuration PrepareConf(string providerName)
		{
			Configuration newConf = new Configuration();
			IEnumerator<KeyValuePair<string, string>> entries = conf.GetEnumerator();
			string providerKey = MappingProviderConfigPrefix + "." + providerName;
			while (entries.HasNext())
			{
				KeyValuePair<string, string> entry = entries.Next();
				string key = entry.Key;
				// get a property like "hadoop.security.group.mapping.provider.PROVIDER-X.ldap.url"
				if (key.StartsWith(providerKey) && !key.Equals(providerKey))
				{
					// restore to be the one like "hadoop.security.group.mapping.ldap.url" 
					// so that can be used by original provider.
					key = key.Replace(".provider." + providerName, string.Empty);
					newConf.Set(key, entry.Value);
				}
			}
			return newConf;
		}
	}
}
