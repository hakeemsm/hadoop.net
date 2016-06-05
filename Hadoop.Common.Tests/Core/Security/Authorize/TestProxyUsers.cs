using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authorize
{
	public class TestProxyUsers
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.Authorize.TestProxyUsers
			));

		private const string RealUserName = "proxier";

		private const string ProxyUserName = "proxied_user";

		private const string AuthorizedProxyUserName = "authorized_proxied_user";

		private static readonly string[] GroupNames = new string[] { "foo_group" };

		private static readonly string[] NetgroupNames = new string[] { "@foo_group" };

		private static readonly string[] OtherGroupNames = new string[] { "bar_group" };

		private static readonly string[] SudoGroupNames = new string[] { "sudo_proxied_user"
			 };

		private const string ProxyIp = "1.2.3.4";

		private const string ProxyIpRange = "10.222.0.0/16,10.113.221.221";

		/// <summary>
		/// Test the netgroups (groups in ACL rules that start with @)
		/// This is a  manual test because it requires:
		/// - host setup
		/// - native code compiled
		/// - specify the group mapping class
		/// Host setup:
		/// /etc/nsswitch.conf should have a line like this:
		/// netgroup: files
		/// /etc/netgroup should be (the whole file):
		/// foo_group (,proxied_user,)
		/// To run this test:
		/// export JAVA_HOME='path/to/java'
		/// mvn test \
		/// -Dtest=TestProxyUsers \
		/// -DTestProxyUsersGroupMapping=$className \
		/// where $className is one of the classes that provide group
		/// mapping services, i.e.
		/// </summary>
		/// <remarks>
		/// Test the netgroups (groups in ACL rules that start with @)
		/// This is a  manual test because it requires:
		/// - host setup
		/// - native code compiled
		/// - specify the group mapping class
		/// Host setup:
		/// /etc/nsswitch.conf should have a line like this:
		/// netgroup: files
		/// /etc/netgroup should be (the whole file):
		/// foo_group (,proxied_user,)
		/// To run this test:
		/// export JAVA_HOME='path/to/java'
		/// mvn test \
		/// -Dtest=TestProxyUsers \
		/// -DTestProxyUsersGroupMapping=$className \
		/// where $className is one of the classes that provide group
		/// mapping services, i.e. classes that implement
		/// GroupMappingServiceProvider interface, at this time:
		/// - org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMapping
		/// - org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestNetgroups()
		{
			if (!NativeCodeLoader.IsNativeCodeLoaded())
			{
				Log.Info("Not testing netgroups, " + "this test only runs when native code is compiled"
					);
				return;
			}
			string groupMappingClassName = Runtime.GetProperty("TestProxyUsersGroupMapping");
			if (groupMappingClassName == null)
			{
				Log.Info("Not testing netgroups, no group mapping class specified, " + "use -DTestProxyUsersGroupMapping=$className to specify "
					 + "group mapping class (must implement GroupMappingServiceProvider " + "interface and support netgroups)"
					);
				return;
			}
			Log.Info("Testing netgroups using: " + groupMappingClassName);
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityGroupMapping, groupMappingClassName
				);
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserName), StringUtils.Join(",", Arrays.AsList(NetgroupNames)));
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserName), ProxyIp);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			Groups groups = Groups.GetUserToGroupsMappingService(conf);
			// try proxying a group that's allowed
			UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(ProxyUserName, realUserUgi, Sharpen.Collections.ToArray(groups.GetGroups(ProxyUserName
				), new string[groups.GetGroups(ProxyUserName).Count]));
			AssertAuthorized(proxyUserUgi, ProxyIp);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestProxyUsers()
		{
			Configuration conf = new Configuration();
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserName), StringUtils.Join(",", Arrays.AsList(GroupNames)));
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserName), ProxyIp);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			// First try proxying a group that's allowed
			UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(ProxyUserName, realUserUgi, GroupNames);
			// From good IP
			AssertAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
			// Now try proxying a group that's not allowed
			realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName);
			proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting(ProxyUserName, realUserUgi
				, OtherGroupNames);
			// From good IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestProxyUsersWithUserConf()
		{
			Configuration conf = new Configuration();
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserUserConfKey
				(RealUserName), StringUtils.Join(",", Arrays.AsList(AuthorizedProxyUserName)));
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserName), ProxyIp);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			// First try proxying a user that's allowed
			UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(AuthorizedProxyUserName, realUserUgi, GroupNames);
			// From good IP
			AssertAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
			// Now try proxying a user that's not allowed
			realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName);
			proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting(ProxyUserName, realUserUgi
				, GroupNames);
			// From good IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
		}

		[Fact]
		public virtual void TestWildcardGroup()
		{
			Configuration conf = new Configuration();
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserName), "*");
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserName), ProxyIp);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			// First try proxying a group that's allowed
			UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(ProxyUserName, realUserUgi, GroupNames);
			// From good IP
			AssertAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
			// Now try proxying a different group (just to make sure we aren't getting spill over
			// from the other test case!)
			realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName);
			proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting(ProxyUserName, realUserUgi
				, OtherGroupNames);
			// From good IP
			AssertAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
		}

		[Fact]
		public virtual void TestWildcardUser()
		{
			Configuration conf = new Configuration();
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserUserConfKey
				(RealUserName), "*");
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserName), ProxyIp);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			// First try proxying a user that's allowed
			UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(AuthorizedProxyUserName, realUserUgi, GroupNames);
			// From good IP
			AssertAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
			// Now try proxying a different user (just to make sure we aren't getting spill over
			// from the other test case!)
			realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName);
			proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting(ProxyUserName, realUserUgi
				, OtherGroupNames);
			// From good IP
			AssertAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
		}

		[Fact]
		public virtual void TestWildcardIP()
		{
			Configuration conf = new Configuration();
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserName), StringUtils.Join(",", Arrays.AsList(GroupNames)));
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserName), "*");
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			// First try proxying a group that's allowed
			UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(ProxyUserName, realUserUgi, GroupNames);
			// From either IP should be fine
			AssertAuthorized(proxyUserUgi, "1.2.3.4");
			AssertAuthorized(proxyUserUgi, "1.2.3.5");
			// Now set up an unallowed group
			realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName);
			proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting(ProxyUserName, realUserUgi
				, OtherGroupNames);
			// Neither IP should be OK
			AssertNotAuthorized(proxyUserUgi, "1.2.3.4");
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
		}

		[Fact]
		public virtual void TestIPRange()
		{
			Configuration conf = new Configuration();
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserName), "*");
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserName), ProxyIpRange);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			// First try proxying a group that's allowed
			UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(ProxyUserName, realUserUgi, GroupNames);
			// From good IP
			AssertAuthorized(proxyUserUgi, "10.222.0.0");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "10.221.0.0");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWithDuplicateProxyGroups()
		{
			Configuration conf = new Configuration();
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserName), StringUtils.Join(",", Arrays.AsList(GroupNames, GroupNames)));
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserName), ProxyIp);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			ICollection<string> groupsToBeProxied = ProxyUsers.GetDefaultImpersonationProvider
				().GetProxyGroups()[DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserName)];
			Assert.Equal(1, groupsToBeProxied.Count);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWithDuplicateProxyHosts()
		{
			Configuration conf = new Configuration();
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserName), StringUtils.Join(",", Arrays.AsList(GroupNames)));
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserName), StringUtils.Join(",", Arrays.AsList(ProxyIp, ProxyIp)));
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			ICollection<string> hosts = ProxyUsers.GetDefaultImpersonationProvider().GetProxyHosts
				()[DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey(RealUserName
				)];
			Assert.Equal(1, hosts.Count);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestProxyUsersWithProviderOverride()
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityImpersonationProviderClass, 
				"org.apache.hadoop.security.authorize.TestProxyUsers$TestDummyImpersonationProvider"
				);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			// First try proxying a group that's allowed
			UserGroupInformation realUserUgi = UserGroupInformation.CreateUserForTesting(RealUserName
				, SudoGroupNames);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(ProxyUserName, realUserUgi, GroupNames);
			// From good IP
			AssertAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertAuthorized(proxyUserUgi, "1.2.3.5");
			// Now try proxying a group that's not allowed
			realUserUgi = UserGroupInformation.CreateUserForTesting(RealUserName, GroupNames);
			proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting(ProxyUserName, realUserUgi
				, GroupNames);
			// From good IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWithProxyGroupsAndUsersWithSpaces()
		{
			Configuration conf = new Configuration();
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserUserConfKey
				(RealUserName), StringUtils.Join(",", Arrays.AsList(ProxyUserName + " ", AuthorizedProxyUserName
				, "ONEMORE")));
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserName), StringUtils.Join(",", Arrays.AsList(GroupNames)));
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserName), ProxyIp);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			ICollection<string> groupsToBeProxied = ProxyUsers.GetDefaultImpersonationProvider
				().GetProxyGroups()[DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserName)];
			Assert.Equal(GroupNames.Length, groupsToBeProxied.Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestProxyUsersWithNullPrefix()
		{
			ProxyUsers.RefreshSuperUserGroupsConfiguration(new Configuration(false), null);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestProxyUsersWithEmptyPrefix()
		{
			ProxyUsers.RefreshSuperUserGroupsConfiguration(new Configuration(false), string.Empty
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestProxyUsersWithCustomPrefix()
		{
			Configuration conf = new Configuration(false);
			conf.Set("x." + RealUserName + ".users", StringUtils.Join(",", Arrays.AsList(AuthorizedProxyUserName
				)));
			conf.Set("x." + RealUserName + ".hosts", ProxyIp);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf, "x");
			// First try proxying a user that's allowed
			UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(AuthorizedProxyUserName, realUserUgi, GroupNames);
			// From good IP
			AssertAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
			// Now try proxying a user that's not allowed
			realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName);
			proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting(ProxyUserName, realUserUgi
				, GroupNames);
			// From good IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.4");
			// From bad IP
			AssertNotAuthorized(proxyUserUgi, "1.2.3.5");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNoHostsForUsers()
		{
			Configuration conf = new Configuration(false);
			conf.Set("y." + RealUserName + ".users", StringUtils.Join(",", Arrays.AsList(AuthorizedProxyUserName
				)));
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf, "y");
			UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(AuthorizedProxyUserName, realUserUgi, GroupNames);
			// IP doesn't matter
			AssertNotAuthorized(proxyUserUgi, "1.2.3.4");
		}

		private void AssertNotAuthorized(UserGroupInformation proxyUgi, string host)
		{
			try
			{
				ProxyUsers.Authorize(proxyUgi, host);
				NUnit.Framework.Assert.Fail("Allowed authorization of " + proxyUgi + " from " + host
					);
			}
			catch (AuthorizationException)
			{
			}
		}

		// Expected
		private void AssertAuthorized(UserGroupInformation proxyUgi, string host)
		{
			try
			{
				ProxyUsers.Authorize(proxyUgi, host);
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail("Did not allow authorization of " + proxyUgi + " from "
					 + host);
			}
		}

		internal class TestDummyImpersonationProvider : ImpersonationProvider
		{
			public virtual void Init(string configurationPrefix)
			{
			}

			/// <summary>
			/// Authorize a user (superuser) to impersonate another user (user1) if the
			/// superuser belongs to the group "sudo_user1" .
			/// </summary>
			/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
			public virtual void Authorize(UserGroupInformation user, string remoteAddress)
			{
				UserGroupInformation superUser = user.GetRealUser();
				string sudoGroupName = "sudo_" + user.GetShortUserName();
				if (!Arrays.AsList(superUser.GetGroupNames()).Contains(sudoGroupName))
				{
					throw new AuthorizationException("User: " + superUser.GetUserName() + " is not allowed to impersonate "
						 + user.GetUserName());
				}
			}

			public virtual void SetConf(Configuration conf)
			{
			}

			public virtual Configuration GetConf()
			{
				return null;
			}
		}

		public static void LoadTest(string ipString, int testRange)
		{
			Configuration conf = new Configuration();
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserName), StringUtils.Join(",", Arrays.AsList(GroupNames)));
			conf.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserName), ipString);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			// First try proxying a group that's allowed
			UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(ProxyUserName, realUserUgi, GroupNames);
			long startTime = Runtime.NanoTime();
			SecureRandom sr = new SecureRandom();
			for (int i = 1; i < 1000000; i++)
			{
				try
				{
					ProxyUsers.Authorize(proxyUserUgi, "1.2.3." + sr.Next(testRange));
				}
				catch (AuthorizationException)
				{
				}
			}
			long stopTime = Runtime.NanoTime();
			long elapsedTime = stopTime - startTime;
			System.Console.Out.WriteLine(elapsedTime / 1000000 + " ms");
		}

		/// <summary>
		/// invokes the load Test
		/// A few sample invocations  are as below
		/// TestProxyUsers ip 128 256
		/// TestProxyUsers range 1.2.3.0/25 256
		/// TestProxyUsers ip 4 8
		/// TestProxyUsers range 1.2.3.0/30 8
		/// </summary>
		/// <param name="args"/>
		public static void Main(string[] args)
		{
			string ipValues = null;
			if (args.Length != 3 || (!args[0].Equals("ip") && !args[0].Equals("range")))
			{
				System.Console.Out.WriteLine("Invalid invocation. The right syntax is ip/range <numberofIps/cidr> <testRange>"
					);
			}
			else
			{
				if (args[0].Equals("ip"))
				{
					int numberOfIps = System.Convert.ToInt32(args[1]);
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < numberOfIps; i++)
					{
						sb.Append("1.2.3." + i + ",");
					}
					ipValues = sb.ToString();
				}
				else
				{
					if (args[0].Equals("range"))
					{
						ipValues = args[1];
					}
				}
				int testRange = System.Convert.ToInt32(args[2]);
				LoadTest(ipValues, testRange);
			}
		}
	}
}
