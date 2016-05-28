using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service.Security
{
	public class TestGroupsService : HTestCase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void Service()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", StringUtils.Join(",", Arrays.AsList(typeof(GroupsService
				).FullName)));
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			Groups groups = server.Get<Groups>();
			NUnit.Framework.Assert.IsNotNull(groups);
			IList<string> g = groups.GetGroups(Runtime.GetProperty("user.name"));
			NUnit.Framework.Assert.AreNotSame(g.Count, 0);
			server.Destroy();
		}

		/// <exception cref="System.Exception"/>
		[TestDir]
		public virtual void InvalidGroupsMapping()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", StringUtils.Join(",", Arrays.AsList(typeof(GroupsService
				).FullName)));
			conf.Set("server.groups.hadoop.security.group.mapping", typeof(string).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
		}
	}
}
