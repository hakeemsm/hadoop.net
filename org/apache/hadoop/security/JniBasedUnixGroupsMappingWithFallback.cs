using Sharpen;

namespace org.apache.hadoop.security
{
	public class JniBasedUnixGroupsMappingWithFallback : org.apache.hadoop.security.GroupMappingServiceProvider
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback
			)));

		private org.apache.hadoop.security.GroupMappingServiceProvider impl;

		public JniBasedUnixGroupsMappingWithFallback()
		{
			if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
			{
				this.impl = new org.apache.hadoop.security.JniBasedUnixGroupsMapping();
			}
			else
			{
				org.apache.hadoop.util.PerformanceAdvisory.LOG.debug("Falling back to shell based"
					);
				this.impl = new org.apache.hadoop.security.ShellBasedUnixGroupsMapping();
			}
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Group mapping impl=" + Sharpen.Runtime.getClassForObject(impl).getName
					());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> getGroups(string user)
		{
			return impl.getGroups(user);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void cacheGroupsRefresh()
		{
			impl.cacheGroupsRefresh();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void cacheGroupsAdd(System.Collections.Generic.IList<string> groups
			)
		{
			impl.cacheGroupsAdd(groups);
		}
	}
}
