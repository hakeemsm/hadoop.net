using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authorize
{
	public class TestAccessControlList
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.Authorize.TestAccessControlList
			));

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
		/// lasVegas (,elvis,)
		/// memphis (,elvis,) (,jerryLeeLewis,)
		/// To run this test:
		/// export JAVA_HOME='path/to/java'
		/// ant \
		/// -Dtestcase=TestAccessControlList \
		/// -Dtest.output=yes \
		/// -DTestAccessControlListGroupMapping=$className \
		/// compile-native test
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
		/// lasVegas (,elvis,)
		/// memphis (,elvis,) (,jerryLeeLewis,)
		/// To run this test:
		/// export JAVA_HOME='path/to/java'
		/// ant \
		/// -Dtestcase=TestAccessControlList \
		/// -Dtest.output=yes \
		/// -DTestAccessControlListGroupMapping=$className \
		/// compile-native test
		/// where $className is one of the classes that provide group
		/// mapping services, i.e. classes that implement
		/// GroupMappingServiceProvider interface, at this time:
		/// - org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMapping
		/// - org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNetgroups()
		{
			if (!NativeCodeLoader.IsNativeCodeLoaded())
			{
				Log.Info("Not testing netgroups, " + "this test only runs when native code is compiled"
					);
				return;
			}
			string groupMappingClassName = Runtime.GetProperty("TestAccessControlListGroupMapping"
				);
			if (groupMappingClassName == null)
			{
				Log.Info("Not testing netgroups, no group mapping class specified, " + "use -DTestAccessControlListGroupMapping=$className to specify "
					 + "group mapping class (must implement GroupMappingServiceProvider " + "interface and support netgroups)"
					);
				return;
			}
			Log.Info("Testing netgroups using: " + groupMappingClassName);
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityGroupMapping, groupMappingClassName
				);
			Groups groups = Groups.GetUserToGroupsMappingService(conf);
			AccessControlList acl;
			// create these ACLs to populate groups cache
			acl = new AccessControlList("ja my");
			// plain
			acl = new AccessControlList("sinatra ratpack,@lasVegas");
			// netgroup
			acl = new AccessControlList(" somegroup,@someNetgroup");
			// no user
			// this ACL will be used for testing ACLs
			acl = new AccessControlList("carlPerkins ratpack,@lasVegas");
			acl.AddGroup("@memphis");
			// validate the netgroups before and after rehresh to make
			// sure refresh works correctly
			ValidateNetgroups(groups, acl);
			groups.Refresh();
			ValidateNetgroups(groups, acl);
		}

		/// <summary>
		/// Validate the netgroups, both group membership and ACL
		/// functionality
		/// Note: assumes a specific acl setup done by testNetgroups
		/// </summary>
		/// <param name="groups">group to user mapping service</param>
		/// <param name="acl">ACL set up in a specific way, see testNetgroups</param>
		/// <exception cref="System.Exception"/>
		private void ValidateNetgroups(Groups groups, AccessControlList acl)
		{
			// check that the netgroups are working
			IList<string> elvisGroups = groups.GetGroups("elvis");
			Assert.True(elvisGroups.Contains("@lasVegas"));
			Assert.True(elvisGroups.Contains("@memphis"));
			IList<string> jerryLeeLewisGroups = groups.GetGroups("jerryLeeLewis");
			Assert.True(jerryLeeLewisGroups.Contains("@memphis"));
			// allowed becuase his netgroup is in ACL
			UserGroupInformation elvis = UserGroupInformation.CreateRemoteUser("elvis");
			AssertUserAllowed(elvis, acl);
			// allowed because he's in ACL
			UserGroupInformation carlPerkins = UserGroupInformation.CreateRemoteUser("carlPerkins"
				);
			AssertUserAllowed(carlPerkins, acl);
			// not allowed because he's not in ACL and has no netgroups
			UserGroupInformation littleRichard = UserGroupInformation.CreateRemoteUser("littleRichard"
				);
			AssertUserNotAllowed(littleRichard, acl);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWildCardAccessControlList()
		{
			AccessControlList acl;
			acl = new AccessControlList("*");
			Assert.True(acl.IsAllAllowed());
			acl = new AccessControlList("  * ");
			Assert.True(acl.IsAllAllowed());
			acl = new AccessControlList(" *");
			Assert.True(acl.IsAllAllowed());
			acl = new AccessControlList("*  ");
			Assert.True(acl.IsAllAllowed());
		}

		// Check if AccessControlList.toString() works as expected.
		// Also validate if getAclString() for various cases.
		[Fact]
		public virtual void TestAclString()
		{
			AccessControlList acl;
			acl = new AccessControlList("*");
			Assert.True(acl.ToString().Equals("All users are allowed"));
			ValidateGetAclString(acl);
			acl = new AccessControlList(" ");
			Assert.True(acl.ToString().Equals("No users are allowed"));
			acl = new AccessControlList("user1,user2");
			Assert.True(acl.ToString().Equals("Users [user1, user2] are allowed"
				));
			ValidateGetAclString(acl);
			acl = new AccessControlList("user1,user2 ");
			// with space
			Assert.True(acl.ToString().Equals("Users [user1, user2] are allowed"
				));
			ValidateGetAclString(acl);
			acl = new AccessControlList(" group1,group2");
			Assert.True(acl.ToString().Equals("Members of the groups [group1, group2] are allowed"
				));
			ValidateGetAclString(acl);
			acl = new AccessControlList("user1,user2 group1,group2");
			Assert.True(acl.ToString().Equals("Users [user1, user2] and " +
				 "members of the groups [group1, group2] are allowed"));
			ValidateGetAclString(acl);
		}

		// Validates if getAclString() is working as expected. i.e. if we can build
		// a new ACL instance from the value returned by getAclString().
		private void ValidateGetAclString(AccessControlList acl)
		{
			Assert.True(acl.ToString().Equals(new AccessControlList(acl.GetAclString
				()).ToString()));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAccessControlList()
		{
			AccessControlList acl;
			ICollection<string> users;
			ICollection<string> groups;
			acl = new AccessControlList("drwho tardis");
			users = acl.GetUsers();
			Assert.Equal(users.Count, 1);
			Assert.Equal(users.GetEnumerator().Next(), "drwho");
			groups = acl.GetGroups();
			Assert.Equal(groups.Count, 1);
			Assert.Equal(groups.GetEnumerator().Next(), "tardis");
			acl = new AccessControlList("drwho");
			users = acl.GetUsers();
			Assert.Equal(users.Count, 1);
			Assert.Equal(users.GetEnumerator().Next(), "drwho");
			groups = acl.GetGroups();
			Assert.Equal(groups.Count, 0);
			acl = new AccessControlList("drwho ");
			users = acl.GetUsers();
			Assert.Equal(users.Count, 1);
			Assert.Equal(users.GetEnumerator().Next(), "drwho");
			groups = acl.GetGroups();
			Assert.Equal(groups.Count, 0);
			acl = new AccessControlList(" tardis");
			users = acl.GetUsers();
			Assert.Equal(users.Count, 0);
			groups = acl.GetGroups();
			Assert.Equal(groups.Count, 1);
			Assert.Equal(groups.GetEnumerator().Next(), "tardis");
			IEnumerator<string> iter;
			acl = new AccessControlList("drwho,joe tardis, users");
			users = acl.GetUsers();
			Assert.Equal(users.Count, 2);
			iter = users.GetEnumerator();
			Assert.Equal(iter.Next(), "drwho");
			Assert.Equal(iter.Next(), "joe");
			groups = acl.GetGroups();
			Assert.Equal(groups.Count, 2);
			iter = groups.GetEnumerator();
			Assert.Equal(iter.Next(), "tardis");
			Assert.Equal(iter.Next(), "users");
		}

		/// <summary>Test addUser/Group and removeUser/Group api.</summary>
		[Fact]
		public virtual void TestAddRemoveAPI()
		{
			AccessControlList acl;
			ICollection<string> users;
			ICollection<string> groups;
			acl = new AccessControlList(" ");
			Assert.Equal(0, acl.GetUsers().Count);
			Assert.Equal(0, acl.GetGroups().Count);
			Assert.Equal(" ", acl.GetAclString());
			acl.AddUser("drwho");
			users = acl.GetUsers();
			Assert.Equal(users.Count, 1);
			Assert.Equal(users.GetEnumerator().Next(), "drwho");
			Assert.Equal("drwho ", acl.GetAclString());
			acl.AddGroup("tardis");
			groups = acl.GetGroups();
			Assert.Equal(groups.Count, 1);
			Assert.Equal(groups.GetEnumerator().Next(), "tardis");
			Assert.Equal("drwho tardis", acl.GetAclString());
			acl.AddUser("joe");
			acl.AddGroup("users");
			users = acl.GetUsers();
			Assert.Equal(users.Count, 2);
			IEnumerator<string> iter = users.GetEnumerator();
			Assert.Equal(iter.Next(), "drwho");
			Assert.Equal(iter.Next(), "joe");
			groups = acl.GetGroups();
			Assert.Equal(groups.Count, 2);
			iter = groups.GetEnumerator();
			Assert.Equal(iter.Next(), "tardis");
			Assert.Equal(iter.Next(), "users");
			Assert.Equal("drwho,joe tardis,users", acl.GetAclString());
			acl.RemoveUser("joe");
			acl.RemoveGroup("users");
			users = acl.GetUsers();
			Assert.Equal(users.Count, 1);
			NUnit.Framework.Assert.IsFalse(users.Contains("joe"));
			groups = acl.GetGroups();
			Assert.Equal(groups.Count, 1);
			NUnit.Framework.Assert.IsFalse(groups.Contains("users"));
			Assert.Equal("drwho tardis", acl.GetAclString());
			acl.RemoveGroup("tardis");
			groups = acl.GetGroups();
			Assert.Equal(0, groups.Count);
			NUnit.Framework.Assert.IsFalse(groups.Contains("tardis"));
			Assert.Equal("drwho ", acl.GetAclString());
			acl.RemoveUser("drwho");
			Assert.Equal(0, users.Count);
			NUnit.Framework.Assert.IsFalse(users.Contains("drwho"));
			Assert.Equal(0, acl.GetGroups().Count);
			Assert.Equal(0, acl.GetUsers().Count);
			Assert.Equal(" ", acl.GetAclString());
		}

		/// <summary>Tests adding/removing wild card as the user/group.</summary>
		[Fact]
		public virtual void TestAddRemoveWildCard()
		{
			AccessControlList acl = new AccessControlList("drwho tardis");
			Exception th = null;
			try
			{
				acl.AddUser(" * ");
			}
			catch (Exception t)
			{
				th = t;
			}
			NUnit.Framework.Assert.IsNotNull(th);
			Assert.True(th is ArgumentException);
			th = null;
			try
			{
				acl.AddGroup(" * ");
			}
			catch (Exception t)
			{
				th = t;
			}
			NUnit.Framework.Assert.IsNotNull(th);
			Assert.True(th is ArgumentException);
			th = null;
			try
			{
				acl.RemoveUser(" * ");
			}
			catch (Exception t)
			{
				th = t;
			}
			NUnit.Framework.Assert.IsNotNull(th);
			Assert.True(th is ArgumentException);
			th = null;
			try
			{
				acl.RemoveGroup(" * ");
			}
			catch (Exception t)
			{
				th = t;
			}
			NUnit.Framework.Assert.IsNotNull(th);
			Assert.True(th is ArgumentException);
		}

		/// <summary>Tests adding user/group to an wild card acl.</summary>
		[Fact]
		public virtual void TestAddRemoveToWildCardACL()
		{
			AccessControlList acl = new AccessControlList(" * ");
			Assert.True(acl.IsAllAllowed());
			UserGroupInformation drwho = UserGroupInformation.CreateUserForTesting("drwho@APACHE.ORG"
				, new string[] { "aliens" });
			UserGroupInformation drwho2 = UserGroupInformation.CreateUserForTesting("drwho2@APACHE.ORG"
				, new string[] { "tardis" });
			acl.AddUser("drwho");
			Assert.True(acl.IsAllAllowed());
			NUnit.Framework.Assert.IsFalse(acl.GetAclString().Contains("drwho"));
			acl.AddGroup("tardis");
			Assert.True(acl.IsAllAllowed());
			NUnit.Framework.Assert.IsFalse(acl.GetAclString().Contains("tardis"));
			acl.RemoveUser("drwho");
			Assert.True(acl.IsAllAllowed());
			AssertUserAllowed(drwho, acl);
			acl.RemoveGroup("tardis");
			Assert.True(acl.IsAllAllowed());
			AssertUserAllowed(drwho2, acl);
		}

		/// <summary>Verify the method isUserAllowed()</summary>
		[Fact]
		public virtual void TestIsUserAllowed()
		{
			AccessControlList acl;
			UserGroupInformation drwho = UserGroupInformation.CreateUserForTesting("drwho@APACHE.ORG"
				, new string[] { "aliens", "humanoids", "timelord" });
			UserGroupInformation susan = UserGroupInformation.CreateUserForTesting("susan@APACHE.ORG"
				, new string[] { "aliens", "humanoids", "timelord" });
			UserGroupInformation barbara = UserGroupInformation.CreateUserForTesting("barbara@APACHE.ORG"
				, new string[] { "humans", "teachers" });
			UserGroupInformation ian = UserGroupInformation.CreateUserForTesting("ian@APACHE.ORG"
				, new string[] { "humans", "teachers" });
			acl = new AccessControlList("drwho humanoids");
			AssertUserAllowed(drwho, acl);
			AssertUserAllowed(susan, acl);
			AssertUserNotAllowed(barbara, acl);
			AssertUserNotAllowed(ian, acl);
			acl = new AccessControlList("drwho");
			AssertUserAllowed(drwho, acl);
			AssertUserNotAllowed(susan, acl);
			AssertUserNotAllowed(barbara, acl);
			AssertUserNotAllowed(ian, acl);
			acl = new AccessControlList("drwho ");
			AssertUserAllowed(drwho, acl);
			AssertUserNotAllowed(susan, acl);
			AssertUserNotAllowed(barbara, acl);
			AssertUserNotAllowed(ian, acl);
			acl = new AccessControlList(" humanoids");
			AssertUserAllowed(drwho, acl);
			AssertUserAllowed(susan, acl);
			AssertUserNotAllowed(barbara, acl);
			AssertUserNotAllowed(ian, acl);
			acl = new AccessControlList("drwho,ian aliens,teachers");
			AssertUserAllowed(drwho, acl);
			AssertUserAllowed(susan, acl);
			AssertUserAllowed(barbara, acl);
			AssertUserAllowed(ian, acl);
			acl = new AccessControlList(string.Empty);
			UserGroupInformation spyUser = Org.Mockito.Mockito.Spy(drwho);
			acl.IsUserAllowed(spyUser);
			Org.Mockito.Mockito.Verify(spyUser, Org.Mockito.Mockito.Never()).GetGroupNames();
		}

		private void AssertUserAllowed(UserGroupInformation ugi, AccessControlList acl)
		{
			Assert.True("User " + ugi + " is not granted the access-control!!"
				, acl.IsUserAllowed(ugi));
		}

		private void AssertUserNotAllowed(UserGroupInformation ugi, AccessControlList acl
			)
		{
			NUnit.Framework.Assert.IsFalse("User " + ugi + " is incorrectly granted the access-control!!"
				, acl.IsUserAllowed(ugi));
		}
	}
}
