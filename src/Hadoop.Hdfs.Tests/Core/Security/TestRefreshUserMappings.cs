using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class TestRefreshUserMappings
	{
		private MiniDFSCluster cluster;

		internal Configuration config;

		private const long groupRefreshTimeoutSec = 1;

		private string tempResource = null;

		public class MockUnixGroupsMapping : GroupMappingServiceProvider
		{
			private int i = 0;

			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetGroups(string user)
			{
				System.Console.Out.WriteLine("Getting groups in MockUnixGroupsMapping");
				string g1 = user + (10 * i + 1);
				string g2 = user + (10 * i + 2);
				IList<string> l = new AList<string>(2);
				l.AddItem(g1);
				l.AddItem(g2);
				i++;
				return l;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CacheGroupsRefresh()
			{
				System.Console.Out.WriteLine("Refreshing groups in MockUnixGroupsMapping");
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CacheGroupsAdd(IList<string> groups)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			config = new Configuration();
			config.SetClass("hadoop.security.group.mapping", typeof(TestRefreshUserMappings.MockUnixGroupsMapping
				), typeof(GroupMappingServiceProvider));
			config.SetLong("hadoop.security.groups.cache.secs", groupRefreshTimeoutSec);
			Groups.GetUserToGroupsMappingService(config);
			FileSystem.SetDefaultUri(config, "hdfs://localhost:" + "0");
			cluster = new MiniDFSCluster.Builder(config).Build();
			cluster.WaitActive();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			if (tempResource != null)
			{
				FilePath f = new FilePath(tempResource);
				f.Delete();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGroupMappingRefresh()
		{
			DFSAdmin admin = new DFSAdmin(config);
			string[] args = new string[] { "-refreshUserToGroupsMappings" };
			Groups groups = Groups.GetUserToGroupsMappingService(config);
			string user = UserGroupInformation.GetCurrentUser().GetUserName();
			System.Console.Out.WriteLine("first attempt:");
			IList<string> g1 = groups.GetGroups(user);
			string[] str_groups = new string[g1.Count];
			Sharpen.Collections.ToArray(g1, str_groups);
			System.Console.Out.WriteLine(Arrays.ToString(str_groups));
			System.Console.Out.WriteLine("second attempt, should be same:");
			IList<string> g2 = groups.GetGroups(user);
			Sharpen.Collections.ToArray(g2, str_groups);
			System.Console.Out.WriteLine(Arrays.ToString(str_groups));
			for (int i = 0; i < g2.Count; i++)
			{
				NUnit.Framework.Assert.AreEqual("Should be same group ", g1[i], g2[i]);
			}
			admin.Run(args);
			System.Console.Out.WriteLine("third attempt(after refresh command), should be different:"
				);
			IList<string> g3 = groups.GetGroups(user);
			Sharpen.Collections.ToArray(g3, str_groups);
			System.Console.Out.WriteLine(Arrays.ToString(str_groups));
			for (int i_1 = 0; i_1 < g3.Count; i_1++)
			{
				NUnit.Framework.Assert.IsFalse("Should be different group: " + g1[i_1] + " and " 
					+ g3[i_1], g1[i_1].Equals(g3[i_1]));
			}
			// test time out
			Sharpen.Thread.Sleep(groupRefreshTimeoutSec * 1100);
			System.Console.Out.WriteLine("fourth attempt(after timeout), should be different:"
				);
			IList<string> g4 = groups.GetGroups(user);
			Sharpen.Collections.ToArray(g4, str_groups);
			System.Console.Out.WriteLine(Arrays.ToString(str_groups));
			for (int i_2 = 0; i_2 < g4.Count; i_2++)
			{
				NUnit.Framework.Assert.IsFalse("Should be different group ", g3[i_2].Equals(g4[i_2
					]));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshSuperUserGroupsConfiguration()
		{
			string SuperUser = "super_user";
			string[] GroupNames1 = new string[] { "gr1", "gr2" };
			string[] GroupNames2 = new string[] { "gr3", "gr4" };
			//keys in conf
			string userKeyGroups = DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(SuperUser);
			string userKeyHosts = DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(SuperUser);
			config.Set(userKeyGroups, "gr3,gr4,gr5");
			// superuser can proxy for this group
			config.Set(userKeyHosts, "127.0.0.1");
			ProxyUsers.RefreshSuperUserGroupsConfiguration(config);
			UserGroupInformation ugi1 = Org.Mockito.Mockito.Mock<UserGroupInformation>();
			UserGroupInformation ugi2 = Org.Mockito.Mockito.Mock<UserGroupInformation>();
			UserGroupInformation suUgi = Org.Mockito.Mockito.Mock<UserGroupInformation>();
			Org.Mockito.Mockito.When(ugi1.GetRealUser()).ThenReturn(suUgi);
			Org.Mockito.Mockito.When(ugi2.GetRealUser()).ThenReturn(suUgi);
			Org.Mockito.Mockito.When(suUgi.GetShortUserName()).ThenReturn(SuperUser);
			// super user
			Org.Mockito.Mockito.When(suUgi.GetUserName()).ThenReturn(SuperUser + "L");
			// super user
			Org.Mockito.Mockito.When(ugi1.GetShortUserName()).ThenReturn("user1");
			Org.Mockito.Mockito.When(ugi2.GetShortUserName()).ThenReturn("user2");
			Org.Mockito.Mockito.When(ugi1.GetUserName()).ThenReturn("userL1");
			Org.Mockito.Mockito.When(ugi2.GetUserName()).ThenReturn("userL2");
			// set groups for users
			Org.Mockito.Mockito.When(ugi1.GetGroupNames()).ThenReturn(GroupNames1);
			Org.Mockito.Mockito.When(ugi2.GetGroupNames()).ThenReturn(GroupNames2);
			// check before
			try
			{
				ProxyUsers.Authorize(ugi1, "127.0.0.1");
				NUnit.Framework.Assert.Fail("first auth for " + ugi1.GetShortUserName() + " should've failed "
					);
			}
			catch (AuthorizationException)
			{
				// expected
				System.Console.Error.WriteLine("auth for " + ugi1.GetUserName() + " failed");
			}
			try
			{
				ProxyUsers.Authorize(ugi2, "127.0.0.1");
				System.Console.Error.WriteLine("auth for " + ugi2.GetUserName() + " succeeded");
			}
			catch (AuthorizationException e)
			{
				// expected
				NUnit.Framework.Assert.Fail("first auth for " + ugi2.GetShortUserName() + " should've succeeded: "
					 + e.GetLocalizedMessage());
			}
			// refresh will look at configuration on the server side
			// add additional resource with the new value
			// so the server side will pick it up
			string rsrc = "testGroupMappingRefresh_rsrc.xml";
			AddNewConfigResource(rsrc, userKeyGroups, "gr2", userKeyHosts, "127.0.0.1");
			DFSAdmin admin = new DFSAdmin(config);
			string[] args = new string[] { "-refreshSuperUserGroupsConfiguration" };
			admin.Run(args);
			try
			{
				ProxyUsers.Authorize(ugi2, "127.0.0.1");
				NUnit.Framework.Assert.Fail("second auth for " + ugi2.GetShortUserName() + " should've failed "
					);
			}
			catch (AuthorizationException)
			{
				// expected
				System.Console.Error.WriteLine("auth for " + ugi2.GetUserName() + " failed");
			}
			try
			{
				ProxyUsers.Authorize(ugi1, "127.0.0.1");
				System.Console.Error.WriteLine("auth for " + ugi1.GetUserName() + " succeeded");
			}
			catch (AuthorizationException e)
			{
				// expected
				NUnit.Framework.Assert.Fail("second auth for " + ugi1.GetShortUserName() + " should've succeeded: "
					 + e.GetLocalizedMessage());
			}
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.UnsupportedEncodingException"/>
		private void AddNewConfigResource(string rsrcName, string keyGroup, string groups
			, string keyHosts, string hosts)
		{
			// location for temp resource should be in CLASSPATH
			Configuration conf = new Configuration();
			Uri url = conf.GetResource("hdfs-site.xml");
			string urlPath = URLDecoder.Decode(url.AbsolutePath.ToString(), "UTF-8");
			Path p = new Path(urlPath);
			Path dir = p.GetParent();
			tempResource = dir.ToString() + "/" + rsrcName;
			string newResource = "<configuration>" + "<property><name>" + keyGroup + "</name><value>"
				 + groups + "</value></property>" + "<property><name>" + keyHosts + "</name><value>"
				 + hosts + "</value></property>" + "</configuration>";
			PrintWriter writer = new PrintWriter(new FileOutputStream(tempResource));
			writer.WriteLine(newResource);
			writer.Close();
			Configuration.AddDefaultResource(rsrcName);
		}
	}
}
