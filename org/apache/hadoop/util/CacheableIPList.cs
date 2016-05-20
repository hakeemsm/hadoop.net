using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>CacheableIPList loads a list of subnets from a file.</summary>
	/// <remarks>
	/// CacheableIPList loads a list of subnets from a file.
	/// The list is cached and the cache can be refreshed by specifying cache timeout.
	/// A negative value of cache timeout disables any caching.
	/// Thread safe.
	/// </remarks>
	public class CacheableIPList : org.apache.hadoop.util.IPList
	{
		private readonly long cacheTimeout;

		private volatile long cacheExpiryTimeStamp;

		private volatile org.apache.hadoop.util.FileBasedIPList ipList;

		public CacheableIPList(org.apache.hadoop.util.FileBasedIPList ipList, long cacheTimeout
			)
		{
			this.cacheTimeout = cacheTimeout;
			this.ipList = ipList;
			updateCacheExpiryTime();
		}

		/// <summary>Reloads the ip list</summary>
		private void reset()
		{
			ipList = ipList.reload();
			updateCacheExpiryTime();
		}

		private void updateCacheExpiryTime()
		{
			if (cacheTimeout < 0)
			{
				cacheExpiryTimeStamp = -1;
			}
			else
			{
				// no automatic cache expiry.
				cacheExpiryTimeStamp = Sharpen.Runtime.currentTimeMillis() + cacheTimeout;
			}
		}

		/// <summary>Refreshes the ip list</summary>
		public virtual void refresh()
		{
			cacheExpiryTimeStamp = 0;
		}

		public virtual bool isIn(string ipAddress)
		{
			//is cache expired
			//Uses Double Checked Locking using volatile
			if (cacheExpiryTimeStamp >= 0 && cacheExpiryTimeStamp < Sharpen.Runtime.currentTimeMillis
				())
			{
				lock (this)
				{
					//check if cache expired again
					if (cacheExpiryTimeStamp < Sharpen.Runtime.currentTimeMillis())
					{
						reset();
					}
				}
			}
			return ipList.isIn(ipAddress);
		}
	}
}
