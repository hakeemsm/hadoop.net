using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>CacheableIPList loads a list of subnets from a file.</summary>
	/// <remarks>
	/// CacheableIPList loads a list of subnets from a file.
	/// The list is cached and the cache can be refreshed by specifying cache timeout.
	/// A negative value of cache timeout disables any caching.
	/// Thread safe.
	/// </remarks>
	public class CacheableIPList : IPList
	{
		private readonly long cacheTimeout;

		private volatile long cacheExpiryTimeStamp;

		private volatile FileBasedIPList ipList;

		public CacheableIPList(FileBasedIPList ipList, long cacheTimeout)
		{
			this.cacheTimeout = cacheTimeout;
			this.ipList = ipList;
			UpdateCacheExpiryTime();
		}

		/// <summary>Reloads the ip list</summary>
		private void Reset()
		{
			ipList = ipList.Reload();
			UpdateCacheExpiryTime();
		}

		private void UpdateCacheExpiryTime()
		{
			if (cacheTimeout < 0)
			{
				cacheExpiryTimeStamp = -1;
			}
			else
			{
				// no automatic cache expiry.
				cacheExpiryTimeStamp = Runtime.CurrentTimeMillis() + cacheTimeout;
			}
		}

		/// <summary>Refreshes the ip list</summary>
		public virtual void Refresh()
		{
			cacheExpiryTimeStamp = 0;
		}

		public virtual bool IsIn(string ipAddress)
		{
			//is cache expired
			//Uses Double Checked Locking using volatile
			if (cacheExpiryTimeStamp >= 0 && cacheExpiryTimeStamp < Runtime.CurrentTimeMillis
				())
			{
				lock (this)
				{
					//check if cache expired again
					if (cacheExpiryTimeStamp < Runtime.CurrentTimeMillis())
					{
						Reset();
					}
				}
			}
			return ipList.IsIn(ipAddress);
		}
	}
}
