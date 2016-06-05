using System;
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Client;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Server
{
	public class TestHSAdminServer
	{
		private bool securityEnabled = true;

		private HSAdminServer hsAdminServer = null;

		private HSAdmin hsAdminClient = null;

		internal JobConf conf = null;

		private static long groupRefreshTimeoutSec = 1;

		internal JobHistory jobHistoryService = null;

		internal AggregatedLogDeletionService alds = null;

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

		[Parameterized.Parameters]
		public static ICollection<object[]> TestParameters()
		{
			return Arrays.AsList(new object[][] { new object[] { false }, new object[] { true
				 } });
		}

		public TestHSAdminServer(bool enableSecurity)
		{
			securityEnabled = enableSecurity;
		}

		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException"/>
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Init()
		{
			conf = new JobConf();
			conf.Set(JHAdminConfig.JhsAdminAddress, "0.0.0.0:0");
			conf.SetClass("hadoop.security.group.mapping", typeof(TestHSAdminServer.MockUnixGroupsMapping
				), typeof(GroupMappingServiceProvider));
			conf.SetLong("hadoop.security.groups.cache.secs", groupRefreshTimeoutSec);
			conf.SetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, securityEnabled
				);
			Groups.GetUserToGroupsMappingService(conf);
			jobHistoryService = Org.Mockito.Mockito.Mock<JobHistory>();
			alds = Org.Mockito.Mockito.Mock<AggregatedLogDeletionService>();
			hsAdminServer = new _HSAdminServer_119(this, alds, jobHistoryService);
			hsAdminServer.Init(conf);
			hsAdminServer.Start();
			conf.SetSocketAddr(JHAdminConfig.JhsAdminAddress, hsAdminServer.clientRpcServer.GetListenerAddress
				());
			hsAdminClient = new HSAdmin(conf);
		}

		private sealed class _HSAdminServer_119 : HSAdminServer
		{
			public _HSAdminServer_119(TestHSAdminServer _enclosing, AggregatedLogDeletionService
				 baseArg1, JobHistory baseArg2)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
			}

			protected internal override Configuration CreateConf()
			{
				return this._enclosing.conf;
			}

			private readonly TestHSAdminServer _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetGroups()
		{
			// Get the current user
			string user = UserGroupInformation.GetCurrentUser().GetUserName();
			string[] args = new string[2];
			args[0] = "-getGroups";
			args[1] = user;
			// Run the getGroups command
			int exitCode = hsAdminClient.Run(args);
			NUnit.Framework.Assert.AreEqual("Exit code should be 0 but was: " + exitCode, 0, 
				exitCode);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshUserToGroupsMappings()
		{
			string[] args = new string[] { "-refreshUserToGroupsMappings" };
			Groups groups = Groups.GetUserToGroupsMappingService(conf);
			string user = UserGroupInformation.GetCurrentUser().GetUserName();
			System.Console.Out.WriteLine("first attempt:");
			IList<string> g1 = groups.GetGroups(user);
			string[] str_groups = new string[g1.Count];
			Sharpen.Collections.ToArray(g1, str_groups);
			System.Console.Out.WriteLine(Arrays.ToString(str_groups));
			// Now groups of this user has changed but getGroups returns from the
			// cache,so we would see same groups as before
			System.Console.Out.WriteLine("second attempt, should be same:");
			IList<string> g2 = groups.GetGroups(user);
			Sharpen.Collections.ToArray(g2, str_groups);
			System.Console.Out.WriteLine(Arrays.ToString(str_groups));
			for (int i = 0; i < g2.Count; i++)
			{
				NUnit.Framework.Assert.AreEqual("Should be same group ", g1[i], g2[i]);
			}
			// run the command,which clears the cache
			hsAdminClient.Run(args);
			System.Console.Out.WriteLine("third attempt(after refresh command), should be different:"
				);
			// Now get groups should return new groups
			IList<string> g3 = groups.GetGroups(user);
			Sharpen.Collections.ToArray(g3, str_groups);
			System.Console.Out.WriteLine(Arrays.ToString(str_groups));
			for (int i_1 = 0; i_1 < g3.Count; i_1++)
			{
				NUnit.Framework.Assert.IsFalse("Should be different group: " + g1[i_1] + " and " 
					+ g3[i_1], g1[i_1].Equals(g3[i_1]));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshSuperUserGroups()
		{
			UserGroupInformation ugi = Org.Mockito.Mockito.Mock<UserGroupInformation>();
			UserGroupInformation superUser = Org.Mockito.Mockito.Mock<UserGroupInformation>();
			Org.Mockito.Mockito.When(ugi.GetRealUser()).ThenReturn(superUser);
			Org.Mockito.Mockito.When(superUser.GetShortUserName()).ThenReturn("superuser");
			Org.Mockito.Mockito.When(superUser.GetUserName()).ThenReturn("superuser");
			Org.Mockito.Mockito.When(ugi.GetGroupNames()).ThenReturn(new string[] { "group3" }
				);
			Org.Mockito.Mockito.When(ugi.GetUserName()).ThenReturn("regularUser");
			// Set super user groups not to include groups of regularUser
			conf.Set("hadoop.proxyuser.superuser.groups", "group1,group2");
			conf.Set("hadoop.proxyuser.superuser.hosts", "127.0.0.1");
			string[] args = new string[1];
			args[0] = "-refreshSuperUserGroupsConfiguration";
			hsAdminClient.Run(args);
			Exception th = null;
			try
			{
				ProxyUsers.Authorize(ugi, "127.0.0.1");
			}
			catch (Exception e)
			{
				th = e;
			}
			// Exception should be thrown
			NUnit.Framework.Assert.IsTrue(th is AuthorizationException);
			// Now add regularUser group to superuser group but not execute
			// refreshSuperUserGroupMapping
			conf.Set("hadoop.proxyuser.superuser.groups", "group1,group2,group3");
			// Again,lets run ProxyUsers.authorize and see if regularUser can be
			// impersonated
			// resetting th
			th = null;
			try
			{
				ProxyUsers.Authorize(ugi, "127.0.0.1");
			}
			catch (Exception e)
			{
				th = e;
			}
			// Exception should be thrown again since we didn't refresh the configs
			NUnit.Framework.Assert.IsTrue(th is AuthorizationException);
			// Lets refresh the config by running refreshSuperUserGroupsConfiguration
			hsAdminClient.Run(args);
			th = null;
			try
			{
				ProxyUsers.Authorize(ugi, "127.0.0.1");
			}
			catch (Exception e)
			{
				th = e;
			}
			// No exception thrown since regularUser can be impersonated.
			NUnit.Framework.Assert.IsNull("Unexpected exception thrown: " + th, th);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshAdminAcls()
		{
			// Setting current user to admin acl
			conf.Set(JHAdminConfig.JhsAdminAcl, UserGroupInformation.GetCurrentUser().GetUserName
				());
			string[] args = new string[1];
			args[0] = "-refreshAdminAcls";
			hsAdminClient.Run(args);
			// Now I should be able to run any hsadmin command without any exception
			// being thrown
			args[0] = "-refreshSuperUserGroupsConfiguration";
			hsAdminClient.Run(args);
			// Lets remove current user from admin acl
			conf.Set(JHAdminConfig.JhsAdminAcl, "notCurrentUser");
			args[0] = "-refreshAdminAcls";
			hsAdminClient.Run(args);
			// Now I should get an exception if i run any hsadmin command
			Exception th = null;
			args[0] = "-refreshSuperUserGroupsConfiguration";
			try
			{
				hsAdminClient.Run(args);
			}
			catch (Exception e)
			{
				th = e;
			}
			NUnit.Framework.Assert.IsTrue(th is RemoteException);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshLoadedJobCache()
		{
			string[] args = new string[1];
			args[0] = "-refreshLoadedJobCache";
			hsAdminClient.Run(args);
			Org.Mockito.Mockito.Verify(jobHistoryService).RefreshLoadedJobCache();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshLogRetentionSettings()
		{
			string[] args = new string[1];
			args[0] = "-refreshLogRetentionSettings";
			hsAdminClient.Run(args);
			Org.Mockito.Mockito.Verify(alds).RefreshLogRetentionSettings();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshJobRetentionSettings()
		{
			string[] args = new string[1];
			args[0] = "-refreshJobRetentionSettings";
			hsAdminClient.Run(args);
			Org.Mockito.Mockito.Verify(jobHistoryService).RefreshJobRetentionSettings();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUGIForLogAndJobRefresh()
		{
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting("test", new 
				string[] { "grp" });
			UserGroupInformation loginUGI = Org.Mockito.Mockito.Spy(hsAdminServer.GetLoginUGI
				());
			hsAdminServer.SetLoginUGI(loginUGI);
			// Run refresh log retention settings with test user
			ugi.DoAs(new _PrivilegedAction_303(this));
			// Verify if AggregatedLogDeletionService#refreshLogRetentionSettings was
			// called with login UGI, instead of the UGI command was run with.
			Org.Mockito.Mockito.Verify(loginUGI).DoAs(Matchers.Any<PrivilegedExceptionAction>
				());
			Org.Mockito.Mockito.Verify(alds).RefreshLogRetentionSettings();
			// Reset for refresh job retention settings
			Org.Mockito.Mockito.Reset(loginUGI);
			// Run refresh job retention settings with test user
			ugi.DoAs(new _PrivilegedAction_325(this));
			// Verify if JobHistory#refreshJobRetentionSettings was called with
			// login UGI, instead of the UGI command was run with.
			Org.Mockito.Mockito.Verify(loginUGI).DoAs(Matchers.Any<PrivilegedExceptionAction>
				());
			Org.Mockito.Mockito.Verify(jobHistoryService).RefreshJobRetentionSettings();
		}

		private sealed class _PrivilegedAction_303 : PrivilegedAction<Void>
		{
			public _PrivilegedAction_303(TestHSAdminServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public Void Run()
			{
				string[] args = new string[1];
				args[0] = "-refreshLogRetentionSettings";
				try
				{
					this._enclosing.hsAdminClient.Run(args);
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail("refreshLogRetentionSettings should have been successful"
						);
				}
				return null;
			}

			private readonly TestHSAdminServer _enclosing;
		}

		private sealed class _PrivilegedAction_325 : PrivilegedAction<Void>
		{
			public _PrivilegedAction_325(TestHSAdminServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public Void Run()
			{
				string[] args = new string[1];
				args[0] = "-refreshJobRetentionSettings";
				try
				{
					this._enclosing.hsAdminClient.Run(args);
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail("refreshJobRetentionSettings should have been successful"
						);
				}
				return null;
			}

			private readonly TestHSAdminServer _enclosing;
		}

		[TearDown]
		public virtual void CleanUp()
		{
			if (hsAdminServer != null)
			{
				hsAdminServer.Stop();
			}
		}
	}
}
