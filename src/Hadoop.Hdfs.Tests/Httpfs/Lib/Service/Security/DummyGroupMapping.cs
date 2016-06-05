using System.Collections.Generic;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service.Security
{
	public class DummyGroupMapping : GroupMappingServiceProvider
	{
		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetGroups(string user)
		{
			if (user.Equals("root"))
			{
				return Arrays.AsList("admin");
			}
			else
			{
				if (user.Equals("nobody"))
				{
					return Arrays.AsList("nobody");
				}
				else
				{
					string[] groups = HadoopUsersConfTestHelper.GetHadoopUserGroups(user);
					return (groups != null) ? Arrays.AsList(groups) : Sharpen.Collections.EmptyList;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsRefresh()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CacheGroupsAdd(IList<string> groups)
		{
		}
	}
}
