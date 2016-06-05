using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security
{
	public class JniBasedUnixGroupsMappingWithFallback : GroupMappingServiceProvider
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.JniBasedUnixGroupsMappingWithFallback
			));

		private GroupMappingServiceProvider impl;

		public JniBasedUnixGroupsMappingWithFallback()
		{
			if (NativeCodeLoader.IsNativeCodeLoaded())
			{
				this.impl = new JniBasedUnixGroupsMapping();
			}
			else
			{
				PerformanceAdvisory.Log.Debug("Falling back to shell based");
				this.impl = new ShellBasedUnixGroupsMapping();
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Group mapping impl=" + impl.GetType().FullName);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetGroups(string user)
		{
			return impl.GetGroups(user);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsRefresh()
		{
			impl.CacheGroupsRefresh();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsAdd(IList<string> groups)
		{
			impl.CacheGroupsAdd(groups);
		}
	}
}
