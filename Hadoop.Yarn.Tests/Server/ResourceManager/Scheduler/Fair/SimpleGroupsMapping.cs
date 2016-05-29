using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class SimpleGroupsMapping : GroupMappingServiceProvider
	{
		public override IList<string> GetGroups(string user)
		{
			return Arrays.AsList(user + "group", user + "subgroup1", user + "subgroup2");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsRefresh()
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsAdd(IList<string> groups)
		{
			throw new NotSupportedException();
		}
	}
}
