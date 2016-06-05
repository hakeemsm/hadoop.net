using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestRMProxyUsersConf
	{
		private static readonly UserGroupInformation FooUser = UserGroupInformation.CreateUserForTesting
			("foo", new string[] { "foo_group" });

		private static readonly UserGroupInformation BarUser = UserGroupInformation.CreateUserForTesting
			("bar", new string[] { "bar_group" });

		private readonly string ipAddress = "127.0.0.1";

		[Parameterized.Parameters]
		public static ICollection<object[]> Headers()
		{
			return Arrays.AsList(new object[][] { new object[] { 0 }, new object[] { 1 }, new 
				object[] { 2 } });
		}

		private Configuration conf;

		public TestRMProxyUsersConf(int round)
		{
			conf = new YarnConfiguration();
			switch (round)
			{
				case 0:
				{
					// hadoop.proxyuser prefix
					conf.Set("hadoop.proxyuser.foo.hosts", ipAddress);
					conf.Set("hadoop.proxyuser.foo.users", "bar");
					conf.Set("hadoop.proxyuser.foo.groups", "bar_group");
					break;
				}

				case 1:
				{
					// yarn.resourcemanager.proxyuser prefix
					conf.Set("yarn.resourcemanager.proxyuser.foo.hosts", ipAddress);
					conf.Set("yarn.resourcemanager.proxyuser.foo.users", "bar");
					conf.Set("yarn.resourcemanager.proxyuser.foo.groups", "bar_group");
					break;
				}

				case 2:
				{
					// hadoop.proxyuser prefix has been overwritten by
					// yarn.resourcemanager.proxyuser prefix
					conf.Set("hadoop.proxyuser.foo.hosts", "xyz");
					conf.Set("hadoop.proxyuser.foo.users", "xyz");
					conf.Set("hadoop.proxyuser.foo.groups", "xyz");
					conf.Set("yarn.resourcemanager.proxyuser.foo.hosts", ipAddress);
					conf.Set("yarn.resourcemanager.proxyuser.foo.users", "bar");
					conf.Set("yarn.resourcemanager.proxyuser.foo.groups", "bar_group");
					break;
				}

				default:
				{
					break;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProxyUserConfiguration()
		{
			MockRM rm = null;
			try
			{
				rm = new MockRM(conf);
				rm.Start();
				// wait for web server starting
				Sharpen.Thread.Sleep(10000);
				UserGroupInformation proxyUser = UserGroupInformation.CreateProxyUser(BarUser.GetShortUserName
					(), FooUser);
				try
				{
					ProxyUsers.GetDefaultImpersonationProvider().Authorize(proxyUser, ipAddress);
				}
				catch (AuthorizationException)
				{
					// Exception is not expected
					NUnit.Framework.Assert.Fail();
				}
			}
			finally
			{
				if (rm != null)
				{
					rm.Stop();
					rm.Close();
				}
			}
		}
	}
}
