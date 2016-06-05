using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security
{
	public class JniBasedUnixGroupsNetgroupMappingWithFallback : GroupMappingServiceProvider
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.JniBasedUnixGroupsNetgroupMappingWithFallback
			));

		private GroupMappingServiceProvider impl;

		public JniBasedUnixGroupsNetgroupMappingWithFallback()
		{
			if (NativeCodeLoader.IsNativeCodeLoaded())
			{
				this.impl = new JniBasedUnixGroupsNetgroupMapping();
			}
			else
			{
				Log.Info("Falling back to shell based");
				this.impl = new ShellBasedUnixGroupsNetgroupMapping();
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
