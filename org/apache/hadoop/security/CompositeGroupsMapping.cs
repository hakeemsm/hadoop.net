using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>
	/// An implementation of
	/// <see cref="GroupMappingServiceProvider"/>
	/// which
	/// composites other group mapping providers for determining group membership.
	/// This allows to combine existing provider implementations and composite
	/// a virtually new provider without customized development to deal with complex situation.
	/// </summary>
	public class CompositeGroupsMapping : org.apache.hadoop.security.GroupMappingServiceProvider
		, org.apache.hadoop.conf.Configurable
	{
		public const string MAPPING_PROVIDERS_CONFIG_KEY = GROUP_MAPPING_CONFIG_PREFIX + 
			".providers";

		public const string MAPPING_PROVIDERS_COMBINED_CONFIG_KEY = MAPPING_PROVIDERS_CONFIG_KEY
			 + ".combined";

		public const string MAPPING_PROVIDER_CONFIG_PREFIX = GROUP_MAPPING_CONFIG_PREFIX 
			+ ".provider";

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.CompositeGroupsMapping
			)));

		private System.Collections.Generic.IList<org.apache.hadoop.security.GroupMappingServiceProvider
			> providersList = new System.Collections.Generic.List<org.apache.hadoop.security.GroupMappingServiceProvider
			>();

		private org.apache.hadoop.conf.Configuration conf;

		private bool combined;

		/// <summary>Returns list of groups for a user.</summary>
		/// <param name="user">get groups for this user</param>
		/// <returns>list of groups for a given user</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual System.Collections.Generic.IList<string> getGroups(string user)
		{
			lock (this)
			{
				System.Collections.Generic.ICollection<string> groupSet = new java.util.TreeSet<string
					>();
				System.Collections.Generic.IList<string> groups = null;
				foreach (org.apache.hadoop.security.GroupMappingServiceProvider provider in providersList)
				{
					try
					{
						groups = provider.getGroups(user);
					}
					catch (System.Exception)
					{
					}
					//LOG.warn("Exception trying to get groups for user " + user, e);      
					if (groups != null && !groups.isEmpty())
					{
						Sharpen.Collections.AddAll(groupSet, groups);
						if (!combined)
						{
							break;
						}
					}
				}
				System.Collections.Generic.IList<string> results = new System.Collections.Generic.List
					<string>(groupSet.Count);
				Sharpen.Collections.AddAll(results, groupSet);
				return results;
			}
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
				this.conf = conf;
				this.combined = conf.getBoolean(MAPPING_PROVIDERS_COMBINED_CONFIG_KEY, true);
				loadMappingProviders();
			}
		}

		private void loadMappingProviders()
		{
			string[] providerNames = conf.getStrings(MAPPING_PROVIDERS_CONFIG_KEY, new string
				[] {  });
			string providerKey;
			foreach (string name in providerNames)
			{
				providerKey = MAPPING_PROVIDER_CONFIG_PREFIX + "." + name;
				java.lang.Class providerClass = conf.getClass(providerKey, null);
				if (providerClass == null)
				{
					LOG.error("The mapping provider, " + name + " does not have a valid class");
				}
				else
				{
					addMappingProvider(name, providerClass);
				}
			}
		}

		private void addMappingProvider(string providerName, java.lang.Class providerClass
			)
		{
			org.apache.hadoop.conf.Configuration newConf = prepareConf(providerName);
			org.apache.hadoop.security.GroupMappingServiceProvider provider = (org.apache.hadoop.security.GroupMappingServiceProvider
				)org.apache.hadoop.util.ReflectionUtils.newInstance(providerClass, newConf);
			providersList.add(provider);
		}

		/*
		* For any provider specific configuration properties, such as "hadoop.security.group.mapping.ldap.url"
		* and the like, allow them to be configured as "hadoop.security.group.mapping.provider.PROVIDER-X.ldap.url",
		* so that a provider such as LdapGroupsMapping can be used to composite a complex one with other providers.
		*/
		private org.apache.hadoop.conf.Configuration prepareConf(string providerName)
		{
			org.apache.hadoop.conf.Configuration newConf = new org.apache.hadoop.conf.Configuration
				();
			System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<string
				, string>> entries = conf.GetEnumerator();
			string providerKey = MAPPING_PROVIDER_CONFIG_PREFIX + "." + providerName;
			while (entries.MoveNext())
			{
				System.Collections.Generic.KeyValuePair<string, string> entry = entries.Current;
				string key = entry.Key;
				// get a property like "hadoop.security.group.mapping.provider.PROVIDER-X.ldap.url"
				if (key.StartsWith(providerKey) && !key.Equals(providerKey))
				{
					// restore to be the one like "hadoop.security.group.mapping.ldap.url" 
					// so that can be used by original provider.
					key = key.Replace(".provider." + providerName, string.Empty);
					newConf.set(key, entry.Value);
				}
			}
			return newConf;
		}
	}
}
