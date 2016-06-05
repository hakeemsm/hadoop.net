using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Caches frequently used names to facilitate reuse.</summary>
	/// <remarks>
	/// Caches frequently used names to facilitate reuse.
	/// (example: byte[] representation of the file name in
	/// <see cref="INode"/>
	/// ).
	/// This class is used by initially adding all the file names. Cache
	/// tracks the number of times a name is used in a transient map. It promotes
	/// a name used more than
	/// <c>useThreshold</c>
	/// to the cache.
	/// One all the names are added,
	/// <see cref="NameCache{K}.Initialized()"/>
	/// should be called to
	/// finish initialization. The transient map where use count is tracked is
	/// discarded and cache is ready for use.
	/// <p>
	/// This class must be synchronized externally.
	/// </remarks>
	/// <?/>
	internal class NameCache<K>
	{
		/// <summary>Class for tracking use count of a name</summary>
		private class UseCount
		{
			internal int count;

			internal readonly K value;

			internal UseCount(NameCache<K> _enclosing, K value)
			{
				this._enclosing = _enclosing;
				// Internal value for the name
				this.count = 1;
				this.value = value;
			}

			internal virtual void Increment()
			{
				this.count++;
			}

			internal virtual int Get()
			{
				return this.count;
			}

			private readonly NameCache<K> _enclosing;
		}

		internal static readonly Log Log = LogFactory.GetLog(typeof(NameCache).FullName);

		/// <summary>indicates initialization is in progress</summary>
		private bool initialized = false;

		/// <summary>
		/// names used more than
		/// <c>useThreshold</c>
		/// is added to the cache
		/// </summary>
		private readonly int useThreshold;

		/// <summary>of times a cache look up was successful</summary>
		private int lookups = 0;

		/// <summary>Cached names</summary>
		internal readonly Dictionary<K, K> cache = new Dictionary<K, K>();

		/// <summary>Names and with number of occurrences tracked during initialization</summary>
		internal IDictionary<K, NameCache.UseCount> transientMap = new Dictionary<K, NameCache.UseCount
			>();

		/// <summary>Constructor</summary>
		/// <param name="useThreshold">
		/// names occurring more than this is promoted to the
		/// cache
		/// </param>
		internal NameCache(int useThreshold)
		{
			this.useThreshold = useThreshold;
		}

		/// <summary>Add a given name to the cache or track use count.</summary>
		/// <remarks>
		/// Add a given name to the cache or track use count.
		/// exist. If the name already exists, then the internal value is returned.
		/// </remarks>
		/// <param name="name">name to be looked up</param>
		/// <returns>internal value for the name if found; otherwise null</returns>
		internal virtual K Put(K name)
		{
			K @internal = cache[name];
			if (@internal != null)
			{
				lookups++;
				return @internal;
			}
			// Track the usage count only during initialization
			if (!initialized)
			{
				NameCache.UseCount useCount = transientMap[name];
				if (useCount != null)
				{
					useCount.Increment();
					if (useCount.Get() >= useThreshold)
					{
						Promote(name);
					}
					return useCount.value;
				}
				useCount = new NameCache.UseCount(this, name);
				transientMap[name] = useCount;
			}
			return null;
		}

		/// <summary>Lookup count when a lookup for a name returned cached object</summary>
		/// <returns>number of successful lookups</returns>
		internal virtual int GetLookupCount()
		{
			return lookups;
		}

		/// <summary>Size of the cache</summary>
		/// <returns>Number of names stored in the cache</returns>
		internal virtual int Size()
		{
			return cache.Count;
		}

		/// <summary>Mark the name cache as initialized.</summary>
		/// <remarks>
		/// Mark the name cache as initialized. The use count is no longer tracked
		/// and the transient map used for initializing the cache is discarded to
		/// save heap space.
		/// </remarks>
		internal virtual void Initialized()
		{
			Log.Info("initialized with " + Size() + " entries " + lookups + " lookups");
			this.initialized = true;
			transientMap.Clear();
			transientMap = null;
		}

		/// <summary>Promote a frequently used name to the cache</summary>
		private void Promote(K name)
		{
			Sharpen.Collections.Remove(transientMap, name);
			cache[name] = name;
			lookups += useThreshold;
		}

		public virtual void Reset()
		{
			initialized = false;
			cache.Clear();
			if (transientMap == null)
			{
				transientMap = new Dictionary<K, NameCache.UseCount>();
			}
			else
			{
				transientMap.Clear();
			}
		}
	}
}
