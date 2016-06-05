using System.Collections.Generic;


namespace Org.Apache.Hadoop.Net
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
	public class CachedDNSToSwitchMapping : AbstractDNSToSwitchMapping
	{
		private IDictionary<string, string> cache = new ConcurrentHashMap<string, string>
			();

		/// <summary>The uncached mapping</summary>
		protected internal readonly DNSToSwitchMapping rawMapping;

		/// <summary>cache a raw DNS mapping</summary>
		/// <param name="rawMapping">the raw mapping to cache</param>
		public CachedDNSToSwitchMapping(DNSToSwitchMapping rawMapping)
		{
			this.rawMapping = rawMapping;
		}

		/// <param name="names">a list of hostnames to probe for being cached</param>
		/// <returns>the hosts from 'names' that have not been cached previously</returns>
		private IList<string> GetUncachedHosts(IList<string> names)
		{
			// find out all names without cached resolved location
			IList<string> unCachedHosts = new AList<string>(names.Count);
			foreach (string name in names)
			{
				if (cache[name] == null)
				{
					unCachedHosts.AddItem(name);
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
		private void CacheResolvedHosts(IList<string> uncachedHosts, IList<string> resolvedHosts
			)
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
		private IList<string> GetCachedHosts(IList<string> names)
		{
			IList<string> result = new AList<string>(names.Count);
			// Construct the result
			foreach (string name in names)
			{
				string networkLocation = cache[name];
				if (networkLocation != null)
				{
					result.AddItem(networkLocation);
				}
				else
				{
					return null;
				}
			}
			return result;
		}

		public override IList<string> Resolve(IList<string> names)
		{
			// normalize all input names to be in the form of IP addresses
			names = NetUtils.NormalizeHostNames(names);
			IList<string> result = new AList<string>(names.Count);
			if (names.IsEmpty())
			{
				return result;
			}
			IList<string> uncachedHosts = GetUncachedHosts(names);
			// Resolve the uncached hosts
			IList<string> resolvedHosts = rawMapping.Resolve(uncachedHosts);
			//cache them
			CacheResolvedHosts(uncachedHosts, resolvedHosts);
			//now look up the entire list in the cache
			return GetCachedHosts(names);
		}

		/// <summary>Get the (host x switch) map.</summary>
		/// <returns>a copy of the cached map of hosts to rack</returns>
		public override IDictionary<string, string> GetSwitchMap()
		{
			IDictionary<string, string> switchMap = new Dictionary<string, string>(cache);
			return switchMap;
		}

		public override string ToString()
		{
			return "cached switch mapping relaying to " + rawMapping;
		}

		/// <summary>
		/// Delegate the switch topology query to the raw mapping, via
		/// <see cref="AbstractDNSToSwitchMapping.IsMappingSingleSwitch(DNSToSwitchMapping)"/
		/// 	>
		/// </summary>
		/// <returns>true iff the raw mapper is considered single-switch.</returns>
		public override bool IsSingleSwitch()
		{
			return IsMappingSingleSwitch(rawMapping);
		}

		public override void ReloadCachedMappings()
		{
			cache.Clear();
		}

		public override void ReloadCachedMappings(IList<string> names)
		{
			foreach (string name in names)
			{
				Collections.Remove(cache, name);
			}
		}
	}
}
