using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Sharedcache
{
	public class SharedCacheChecksumFactory
	{
		private static readonly ConcurrentMap<Type, SharedCacheChecksum> instances = new 
			ConcurrentHashMap<Type, SharedCacheChecksum>();

		private static readonly Type defaultAlgorithm;

		static SharedCacheChecksumFactory()
		{
			try
			{
				defaultAlgorithm = (Type)Sharpen.Runtime.GetType(YarnConfiguration.DefaultSharedCacheChecksumAlgoImpl
					);
			}
			catch (Exception e)
			{
				// cannot happen
				throw new ExceptionInInitializerError(e);
			}
		}

		/// <summary>
		/// Get a new <code>SharedCacheChecksum</code> object based on the configurable
		/// algorithm implementation
		/// (see <code>yarn.sharedcache.checksum.algo.impl</code>)
		/// </summary>
		/// <returns><code>SharedCacheChecksum</code> object</returns>
		public static SharedCacheChecksum GetChecksum(Configuration conf)
		{
			Type clazz = conf.GetClass<SharedCacheChecksum>(YarnConfiguration.SharedCacheChecksumAlgoImpl
				, defaultAlgorithm);
			SharedCacheChecksum checksum = instances[clazz];
			if (checksum == null)
			{
				try
				{
					checksum = ReflectionUtils.NewInstance(clazz, conf);
					SharedCacheChecksum old = instances.PutIfAbsent(clazz, checksum);
					if (old != null)
					{
						checksum = old;
					}
				}
				catch (Exception e)
				{
					throw new YarnRuntimeException(e);
				}
			}
			return checksum;
		}
	}
}
