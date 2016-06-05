using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security
{
	public class TestGroupsCaching
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.TestGroupsCaching
			));

		private static string[] myGroups = new string[] { "grp1", "grp2" };

		private Configuration conf;

		[SetUp]
		public virtual void Setup()
		{
			TestGroupsCaching.FakeGroupMapping.ResetRequestCount();
			TestGroupsCaching.ExceptionalGroupMapping.ResetRequestCount();
			conf = new Configuration();
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(TestGroupsCaching.FakeGroupMapping
				), typeof(ShellBasedUnixGroupsMapping));
		}

		public class FakeGroupMapping : ShellBasedUnixGroupsMapping
		{
			private static ICollection<string> allGroups = new HashSet<string>();

			private static ICollection<string> blackList = new HashSet<string>();

			private static int requestCount = 0;

			private static long getGroupsDelayMs = 0;

			// any to n mapping
			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetGroups(string user)
			{
				Log.Info("Getting groups for " + user);
				requestCount++;
				DelayIfNecessary();
				if (blackList.Contains(user))
				{
					return new List<string>();
				}
				return new List<string>(allGroups);
			}

			private void DelayIfNecessary()
			{
				if (getGroupsDelayMs > 0)
				{
					try
					{
						Thread.Sleep(getGroupsDelayMs);
					}
					catch (Exception e)
					{
						throw new RuntimeException(e);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CacheGroupsRefresh()
			{
				Log.Info("Cache is being refreshed.");
				ClearBlackList();
				return;
			}

			/// <exception cref="System.IO.IOException"/>
			public static void ClearBlackList()
			{
				Log.Info("Clearing the blacklist");
				blackList.Clear();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CacheGroupsAdd(IList<string> groups)
			{
				Log.Info("Adding " + groups + " to groups.");
				Collections.AddAll(allGroups, groups);
			}

			/// <exception cref="System.IO.IOException"/>
			public static void AddToBlackList(string user)
			{
				Log.Info("Adding " + user + " to the blacklist");
				blackList.AddItem(user);
			}

			public static int GetRequestCount()
			{
				return requestCount;
			}

			public static void ResetRequestCount()
			{
				requestCount = 0;
			}

			public static void SetGetGroupsDelayMs(long delayMs)
			{
				getGroupsDelayMs = delayMs;
			}
		}

		public class ExceptionalGroupMapping : ShellBasedUnixGroupsMapping
		{
			private static int requestCount = 0;

			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetGroups(string user)
			{
				requestCount++;
				throw new IOException("For test");
			}

			public static int GetRequestCount()
			{
				return requestCount;
			}

			public static void ResetRequestCount()
			{
				requestCount = 0;
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGroupsCaching()
		{
			// Disable negative cache.
			conf.SetLong(CommonConfigurationKeys.HadoopSecurityGroupsNegativeCacheSecs, 0);
			Groups groups = new Groups(conf);
			groups.CacheGroupsAdd(Arrays.AsList(myGroups));
			groups.Refresh();
			TestGroupsCaching.FakeGroupMapping.ClearBlackList();
			TestGroupsCaching.FakeGroupMapping.AddToBlackList("user1");
			// regular entry
			Assert.True(groups.GetGroups("me").Count == 2);
			// this must be cached. blacklisting should have no effect.
			TestGroupsCaching.FakeGroupMapping.AddToBlackList("me");
			Assert.True(groups.GetGroups("me").Count == 2);
			// ask for a negative entry
			try
			{
				Log.Error("We are not supposed to get here." + groups.GetGroups("user1").ToString
					());
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException ioe)
			{
				if (!ioe.Message.StartsWith("No groups found"))
				{
					Log.Error("Got unexpected exception: " + ioe.Message);
					NUnit.Framework.Assert.Fail();
				}
			}
			// this shouldn't be cached. remove from the black list and retry.
			TestGroupsCaching.FakeGroupMapping.ClearBlackList();
			Assert.True(groups.GetGroups("user1").Count == 2);
		}

		public class FakeunPrivilegedGroupMapping : TestGroupsCaching.FakeGroupMapping
		{
			private static bool invoked = false;

			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetGroups(string user)
			{
				invoked = true;
				return base.GetGroups(user);
			}
		}

		/*
		* Group lookup should not happen for static users
		*/
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGroupLookupForStaticUsers()
		{
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(TestGroupsCaching.FakeunPrivilegedGroupMapping
				), typeof(ShellBasedUnixGroupsMapping));
			conf.Set(CommonConfigurationKeys.HadoopUserGroupStaticOverrides, "me=;user1=group1;user2=group1,group2"
				);
			Groups groups = new Groups(conf);
			IList<string> userGroups = groups.GetGroups("me");
			Assert.True("non-empty groups for static user", userGroups.IsEmpty
				());
			NUnit.Framework.Assert.IsFalse("group lookup done for static user", TestGroupsCaching.FakeunPrivilegedGroupMapping
				.invoked);
			IList<string> expected = new AList<string>();
			expected.AddItem("group1");
			TestGroupsCaching.FakeunPrivilegedGroupMapping.invoked = false;
			userGroups = groups.GetGroups("user1");
			Assert.True("groups not correct", expected.Equals(userGroups));
			NUnit.Framework.Assert.IsFalse("group lookup done for unprivileged user", TestGroupsCaching.FakeunPrivilegedGroupMapping
				.invoked);
			expected.AddItem("group2");
			TestGroupsCaching.FakeunPrivilegedGroupMapping.invoked = false;
			userGroups = groups.GetGroups("user2");
			Assert.True("groups not correct", expected.Equals(userGroups));
			NUnit.Framework.Assert.IsFalse("group lookup done for unprivileged user", TestGroupsCaching.FakeunPrivilegedGroupMapping
				.invoked);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNegativeGroupCaching()
		{
			string user = "negcache";
			string failMessage = "Did not throw IOException: ";
			conf.SetLong(CommonConfigurationKeys.HadoopSecurityGroupsNegativeCacheSecs, 2);
			FakeTimer timer = new FakeTimer();
			Groups groups = new Groups(conf, timer);
			groups.CacheGroupsAdd(Arrays.AsList(myGroups));
			groups.Refresh();
			TestGroupsCaching.FakeGroupMapping.AddToBlackList(user);
			// In the first attempt, the user will be put in the negative cache.
			try
			{
				groups.GetGroups(user);
				NUnit.Framework.Assert.Fail(failMessage + "Failed to obtain groups from FakeGroupMapping."
					);
			}
			catch (IOException e)
			{
				// Expects to raise exception for the first time. But the user will be
				// put into the negative cache
				GenericTestUtils.AssertExceptionContains("No groups found for user", e);
			}
			// The second time, the user is in the negative cache.
			try
			{
				groups.GetGroups(user);
				NUnit.Framework.Assert.Fail(failMessage + "The user is in the negative cache.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("No groups found for user", e);
			}
			// Brings back the backend user-group mapping service.
			TestGroupsCaching.FakeGroupMapping.ClearBlackList();
			// It should still get groups from the negative cache.
			try
			{
				groups.GetGroups(user);
				NUnit.Framework.Assert.Fail(failMessage + "The user is still in the negative cache, even "
					 + "FakeGroupMapping has resumed.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("No groups found for user", e);
			}
			// Let the elements in the negative cache expire.
			timer.Advance(4 * 1000);
			// The groups for the user is expired in the negative cache, a new copy of
			// groups for the user is fetched.
			Assert.Equal(Arrays.AsList(myGroups), groups.GetGroups(user));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCachePreventsImplRequest()
		{
			// Disable negative cache.
			conf.SetLong(CommonConfigurationKeys.HadoopSecurityGroupsNegativeCacheSecs, 0);
			Groups groups = new Groups(conf);
			groups.CacheGroupsAdd(Arrays.AsList(myGroups));
			groups.Refresh();
			TestGroupsCaching.FakeGroupMapping.ClearBlackList();
			Assert.Equal(0, TestGroupsCaching.FakeGroupMapping.GetRequestCount
				());
			// First call hits the wire
			Assert.True(groups.GetGroups("me").Count == 2);
			Assert.Equal(1, TestGroupsCaching.FakeGroupMapping.GetRequestCount
				());
			// Second count hits cache
			Assert.True(groups.GetGroups("me").Count == 2);
			Assert.Equal(1, TestGroupsCaching.FakeGroupMapping.GetRequestCount
				());
		}

		[Fact]
		public virtual void TestExceptionsFromImplNotCachedInNegativeCache()
		{
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(TestGroupsCaching.ExceptionalGroupMapping
				), typeof(ShellBasedUnixGroupsMapping));
			conf.SetLong(CommonConfigurationKeys.HadoopSecurityGroupsNegativeCacheSecs, 10000
				);
			Groups groups = new Groups(conf);
			groups.CacheGroupsAdd(Arrays.AsList(myGroups));
			groups.Refresh();
			Assert.Equal(0, TestGroupsCaching.ExceptionalGroupMapping.GetRequestCount
				());
			// First call should hit the wire
			try
			{
				groups.GetGroups("anything");
				NUnit.Framework.Assert.Fail("Should have thrown");
			}
			catch (IOException)
			{
			}
			// okay
			Assert.Equal(1, TestGroupsCaching.ExceptionalGroupMapping.GetRequestCount
				());
			// Second call should hit the wire (no negative caching)
			try
			{
				groups.GetGroups("anything");
				NUnit.Framework.Assert.Fail("Should have thrown");
			}
			catch (IOException)
			{
			}
			// okay
			Assert.Equal(2, TestGroupsCaching.ExceptionalGroupMapping.GetRequestCount
				());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestOnlyOneRequestWhenNoEntryIsCached()
		{
			// Disable negative cache.
			conf.SetLong(CommonConfigurationKeys.HadoopSecurityGroupsNegativeCacheSecs, 0);
			Groups groups = new Groups(conf);
			groups.CacheGroupsAdd(Arrays.AsList(myGroups));
			groups.Refresh();
			TestGroupsCaching.FakeGroupMapping.ClearBlackList();
			TestGroupsCaching.FakeGroupMapping.SetGetGroupsDelayMs(100);
			AList<Thread> threads = new AList<Thread>();
			for (int i = 0; i < 10; i++)
			{
				threads.AddItem(new _Thread_337(groups));
			}
			// We start a bunch of threads who all see no cached value
			foreach (Thread t in threads)
			{
				t.Start();
			}
			foreach (Thread t_1 in threads)
			{
				t_1.Join();
			}
			// But only one thread should have made the request
			Assert.Equal(1, TestGroupsCaching.FakeGroupMapping.GetRequestCount
				());
		}

		private sealed class _Thread_337 : Thread
		{
			public _Thread_337(Groups groups)
			{
				this.groups = groups;
			}

			public override void Run()
			{
				try
				{
					Assert.Equal(2, groups.GetGroups("me").Count);
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("Should not happen");
				}
			}

			private readonly Groups groups;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestOnlyOneRequestWhenExpiredEntryExists()
		{
			conf.SetLong(CommonConfigurationKeys.HadoopSecurityGroupsCacheSecs, 1);
			FakeTimer timer = new FakeTimer();
			Groups groups = new Groups(conf, timer);
			groups.CacheGroupsAdd(Arrays.AsList(myGroups));
			groups.Refresh();
			TestGroupsCaching.FakeGroupMapping.ClearBlackList();
			TestGroupsCaching.FakeGroupMapping.SetGetGroupsDelayMs(100);
			// We make an initial request to populate the cache
			groups.GetGroups("me");
			int startingRequestCount = TestGroupsCaching.FakeGroupMapping.GetRequestCount();
			// Then expire that entry
			timer.Advance(400 * 1000);
			Thread.Sleep(100);
			AList<Thread> threads = new AList<Thread>();
			for (int i = 0; i < 10; i++)
			{
				threads.AddItem(new _Thread_382(groups));
			}
			// We start a bunch of threads who all see the cached value
			foreach (Thread t in threads)
			{
				t.Start();
			}
			foreach (Thread t_1 in threads)
			{
				t_1.Join();
			}
			// Only one extra request is made
			Assert.Equal(startingRequestCount + 1, TestGroupsCaching.FakeGroupMapping
				.GetRequestCount());
		}

		private sealed class _Thread_382 : Thread
		{
			public _Thread_382(Groups groups)
			{
				this.groups = groups;
			}

			public override void Run()
			{
				try
				{
					Assert.Equal(2, groups.GetGroups("me").Count);
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("Should not happen");
				}
			}

			private readonly Groups groups;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCacheEntriesExpire()
		{
			conf.SetLong(CommonConfigurationKeys.HadoopSecurityGroupsCacheSecs, 1);
			FakeTimer timer = new FakeTimer();
			Groups groups = new Groups(conf, timer);
			groups.CacheGroupsAdd(Arrays.AsList(myGroups));
			groups.Refresh();
			TestGroupsCaching.FakeGroupMapping.ClearBlackList();
			// We make an entry
			groups.GetGroups("me");
			int startingRequestCount = TestGroupsCaching.FakeGroupMapping.GetRequestCount();
			timer.Advance(20 * 1000);
			// Cache entry has expired so it results in a new fetch
			groups.GetGroups("me");
			Assert.Equal(startingRequestCount + 1, TestGroupsCaching.FakeGroupMapping
				.GetRequestCount());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNegativeCacheClearedOnRefresh()
		{
			conf.SetLong(CommonConfigurationKeys.HadoopSecurityGroupsNegativeCacheSecs, 100);
			Groups groups = new Groups(conf);
			groups.CacheGroupsAdd(Arrays.AsList(myGroups));
			groups.Refresh();
			TestGroupsCaching.FakeGroupMapping.ClearBlackList();
			TestGroupsCaching.FakeGroupMapping.AddToBlackList("dne");
			try
			{
				groups.GetGroups("dne");
				NUnit.Framework.Assert.Fail("Should have failed to find this group");
			}
			catch (IOException)
			{
			}
			// pass
			int startingRequestCount = TestGroupsCaching.FakeGroupMapping.GetRequestCount();
			groups.Refresh();
			TestGroupsCaching.FakeGroupMapping.AddToBlackList("dne");
			try
			{
				IList<string> g = groups.GetGroups("dne");
				NUnit.Framework.Assert.Fail("Should have failed to find this group");
			}
			catch (IOException)
			{
			}
			// pass
			Assert.Equal(startingRequestCount + 1, TestGroupsCaching.FakeGroupMapping
				.GetRequestCount());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNegativeCacheEntriesExpire()
		{
			conf.SetLong(CommonConfigurationKeys.HadoopSecurityGroupsNegativeCacheSecs, 2);
			FakeTimer timer = new FakeTimer();
			// Ensure that stale entries are removed from negative cache every 2 seconds
			Groups groups = new Groups(conf, timer);
			groups.CacheGroupsAdd(Arrays.AsList(myGroups));
			groups.Refresh();
			// Add both these users to blacklist so that they
			// can be added to negative cache
			TestGroupsCaching.FakeGroupMapping.AddToBlackList("user1");
			TestGroupsCaching.FakeGroupMapping.AddToBlackList("user2");
			// Put user1 in negative cache.
			try
			{
				groups.GetGroups("user1");
				NUnit.Framework.Assert.Fail("Did not throw IOException : Failed to obtain groups"
					 + " from FakeGroupMapping.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("No groups found for user", e);
			}
			// Check if user1 exists in negative cache
			Assert.True(groups.GetNegativeCache().Contains("user1"));
			// Advance fake timer
			timer.Advance(1000);
			// Put user2 in negative cache
			try
			{
				groups.GetGroups("user2");
				NUnit.Framework.Assert.Fail("Did not throw IOException : Failed to obtain groups"
					 + " from FakeGroupMapping.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("No groups found for user", e);
			}
			// Check if user2 exists in negative cache
			Assert.True(groups.GetNegativeCache().Contains("user2"));
			// Advance timer. Only user2 should be present in negative cache.
			timer.Advance(1100);
			NUnit.Framework.Assert.IsFalse(groups.GetNegativeCache().Contains("user1"));
			Assert.True(groups.GetNegativeCache().Contains("user2"));
			// Advance timer. Even user2 should not be present in negative cache.
			timer.Advance(1000);
			NUnit.Framework.Assert.IsFalse(groups.GetNegativeCache().Contains("user2"));
		}
	}
}
