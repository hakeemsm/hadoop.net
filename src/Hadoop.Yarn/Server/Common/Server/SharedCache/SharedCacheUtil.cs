using System;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcache
{
	/// <summary>
	/// A utility class that contains helper methods for dealing with the internal
	/// shared cache structure.
	/// </summary>
	public class SharedCacheUtil
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(SharedCacheUtil));

		[InterfaceAudience.Private]
		public static int GetCacheDepth(Configuration conf)
		{
			int cacheDepth = conf.GetInt(YarnConfiguration.SharedCacheNestedLevel, YarnConfiguration
				.DefaultSharedCacheNestedLevel);
			if (cacheDepth <= 0)
			{
				Log.Warn("Specified cache depth was less than or equal to zero." + " Using default value instead. Default: "
					 + YarnConfiguration.DefaultSharedCacheNestedLevel + ", Specified: " + cacheDepth
					);
				cacheDepth = YarnConfiguration.DefaultSharedCacheNestedLevel;
			}
			return cacheDepth;
		}

		[InterfaceAudience.Private]
		public static string GetCacheEntryPath(int cacheDepth, string cacheRoot, string checksum
			)
		{
			if (cacheDepth <= 0)
			{
				throw new ArgumentException("The cache depth must be greater than 0. Passed value: "
					 + cacheDepth);
			}
			if (checksum.Length < cacheDepth)
			{
				throw new ArgumentException("The checksum passed was too short: " + checksum);
			}
			// Build the cache entry path to the specified depth. For example, if the
			// depth is 3 and the checksum is 3c4f, the path would be:
			// SHARED_CACHE_ROOT/3/c/4/3c4f
			StringBuilder sb = new StringBuilder(cacheRoot);
			for (int i = 0; i < cacheDepth; i++)
			{
				sb.Append(Path.SeparatorChar);
				sb.Append(checksum[i]);
			}
			sb.Append(Path.SeparatorChar).Append(checksum);
			return sb.ToString();
		}

		[InterfaceAudience.Private]
		public static string GetCacheEntryGlobPattern(int depth)
		{
			StringBuilder pattern = new StringBuilder();
			for (int i = 0; i < depth; i++)
			{
				pattern.Append("*/");
			}
			pattern.Append("*");
			return pattern.ToString();
		}
	}
}
