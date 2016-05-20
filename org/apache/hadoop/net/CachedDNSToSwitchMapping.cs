using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>
	/// A cached implementation of DNSToSwitchMapping that takes an
	/// raw DNSToSwitchMapping and stores the resolved network location in
	/// a cache.
	/// </summary>
	/// <remarks>
	/// A cached implementation of DNSToSwitchMapping that takes an
	/// raw DNSToSwitchMapping and stores the resolved network location in
	/// a cache. The following calls to a resolved network location
	/// will get its location from the cache.
	/// </remarks>
	public class CachedDNSToSwitchMapping : org.apache.hadoop.net.AbstractDNSToSwitchMapping
	{
		private System.Collections.Generic.IDictionary<string, string> cache = new java.util.concurrent.ConcurrentHashMap
			<string, string>();

		/// <summary>The uncached mapping</summary>
		protected internal readonly org.apache.hadoop.net.DNSToSwitchMapping rawMapping;

		/// <summary>cache a raw DNS mapping</summary>
		/// <param name="rawMapping">the raw mapping to cache</param>
		public CachedDNSToSwitchMapping(org.apache.hadoop.net.DNSToSwitchMapping rawMapping
			)
		{
			this.rawMapping = rawMapping;
		}

		/// <param name="names">a list of hostnames to probe for being cached</param>
		/// <returns>the hosts from 'names' that have not been cached previously</returns>
		private System.Collections.Generic.IList<string> getUncachedHosts(System.Collections.Generic.IList
			<string> names)
		{
			// find out all names without cached resolved location
			System.Collections.Generic.IList<string> unCachedHosts = new System.Collections.Generic.List
				<string>(names.Count);
			foreach (string name in names)
			{
				if (cache[name] == null)
				{
					unCachedHosts.add(name);
				}
			}
			return unCachedHosts;
		}

		/// <summary>Caches the resolved host:rack mappings.</summary>
		/// <remarks>
		/// Caches the resolved host:rack mappings. The two list
		/// parameters must be of equal size.
		/// </remarks>
		/// <param name="uncachedHosts">a list of hosts that were uncached</param>
		/// <param name="resolvedHosts">
		/// a list of resolved host entries where the element
		/// at index(i) is the resolved value for the entry in uncachedHosts[i]
		/// </param>
		private void cacheResolvedHosts(System.Collections.Generic.IList<string> uncachedHosts
			, System.Collections.Generic.IList<string> resolvedHosts)
		{
			// Cache the result
			if (resolvedHosts != null)
			{
				for (int i = 0; i < uncachedHosts.Count; i++)
				{
					cache[uncachedHosts[i]] = resolvedHosts[i];
				}
			}
		}

		/// <param name="names">a list of hostnames to look up (can be be empty)</param>
		/// <returns>
		/// the cached resolution of the list of hostnames/addresses.
		/// or null if any of the names are not currently in the cache
		/// </returns>
		private System.Collections.Generic.IList<string> getCachedHosts(System.Collections.Generic.IList
			<string> names)
		{
			System.Collections.Generic.IList<string> result = new System.Collections.Generic.List
				<string>(names.Count);
			// Construct the result
			foreach (string name in names)
			{
				string networkLocation = cache[name];
				if (networkLocation != null)
				{
					result.add(networkLocation);
				}
				else
				{
					return null;
				}
			}
			return result;
		}

		public override System.Collections.Generic.IList<string> resolve(System.Collections.Generic.IList
			<string> names)
		{
			// normalize all input names to be in the form of IP addresses
			names = org.apache.hadoop.net.NetUtils.normalizeHostNames(names);
			System.Collections.Generic.IList<string> result = new System.Collections.Generic.List
				<string>(names.Count);
			if (names.isEmpty())
			{
				return result;
			}
			System.Collections.Generic.IList<string> uncachedHosts = getUncachedHosts(names);
			// Resolve the uncached hosts
			System.Collections.Generic.IList<string> resolvedHosts = rawMapping.resolve(uncachedHosts
				);
			//cache them
			cacheResolvedHosts(uncachedHosts, resolvedHosts);
			//now look up the entire list in the cache
			return getCachedHosts(names);
		}

		/// <summary>Get the (host x switch) map.</summary>
		/// <returns>a copy of the cached map of hosts to rack</returns>
		public override System.Collections.Generic.IDictionary<string, string> getSwitchMap
			()
		{
			System.Collections.Generic.IDictionary<string, string> switchMap = new System.Collections.Generic.Dictionary
				<string, string>(cache);
			return switchMap;
		}

		public override string ToString()
		{
			return "cached switch mapping relaying to " + rawMapping;
		}

		/// <summary>
		/// Delegate the switch topology query to the raw mapping, via
		/// <see cref="AbstractDNSToSwitchMapping.isMappingSingleSwitch(DNSToSwitchMapping)"/
		/// 	>
		/// </summary>
		/// <returns>true iff the raw mapper is considered single-switch.</returns>
		public override bool isSingleSwitch()
		{
			return isMappingSingleSwitch(rawMapping);
		}

		public override void reloadCachedMappings()
		{
			cache.clear();
		}

		public override void reloadCachedMappings(System.Collections.Generic.IList<string
			> names)
		{
			foreach (string name in names)
			{
				Sharpen.Collections.Remove(cache, name);
			}
		}
	}
}
