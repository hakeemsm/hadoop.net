using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class TestNetgroupCache
	{
		private const string User1 = "user1";

		private const string User2 = "user2";

		private const string User3 = "user3";

		private const string Group1 = "group1";

		private const string Group2 = "group2";

		[TearDown]
		public virtual void Teardown()
		{
			NetgroupCache.Clear();
		}

		/// <summary>Cache two groups with a set of users.</summary>
		/// <remarks>
		/// Cache two groups with a set of users.
		/// Test membership correctness.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestMembership()
		{
			IList<string> users = new AList<string>();
			users.AddItem(User1);
			users.AddItem(User2);
			NetgroupCache.Add(Group1, users);
			users = new AList<string>();
			users.AddItem(User1);
			users.AddItem(User3);
			NetgroupCache.Add(Group2, users);
			VerifyGroupMembership(User1, 2, Group1);
			VerifyGroupMembership(User1, 2, Group2);
			VerifyGroupMembership(User2, 1, Group1);
			VerifyGroupMembership(User3, 1, Group2);
		}

		/// <summary>Cache a group with a set of users.</summary>
		/// <remarks>
		/// Cache a group with a set of users.
		/// Test membership correctness.
		/// Clear cache, remove a user from the group and cache the group
		/// Test membership correctness.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestUserRemoval()
		{
			IList<string> users = new AList<string>();
			users.AddItem(User1);
			users.AddItem(User2);
			NetgroupCache.Add(Group1, users);
			VerifyGroupMembership(User1, 1, Group1);
			VerifyGroupMembership(User2, 1, Group1);
			users.Remove(User2);
			NetgroupCache.Clear();
			NetgroupCache.Add(Group1, users);
			VerifyGroupMembership(User1, 1, Group1);
			VerifyGroupMembership(User2, 0, null);
		}

		/// <summary>Cache two groups with a set of users.</summary>
		/// <remarks>
		/// Cache two groups with a set of users.
		/// Test membership correctness.
		/// Clear cache, cache only one group.
		/// Test membership correctness.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestGroupRemoval()
		{
			IList<string> users = new AList<string>();
			users.AddItem(User1);
			users.AddItem(User2);
			NetgroupCache.Add(Group1, users);
			users = new AList<string>();
			users.AddItem(User1);
			users.AddItem(User3);
			NetgroupCache.Add(Group2, users);
			VerifyGroupMembership(User1, 2, Group1);
			VerifyGroupMembership(User1, 2, Group2);
			VerifyGroupMembership(User2, 1, Group1);
			VerifyGroupMembership(User3, 1, Group2);
			NetgroupCache.Clear();
			users = new AList<string>();
			users.AddItem(User1);
			users.AddItem(User2);
			NetgroupCache.Add(Group1, users);
			VerifyGroupMembership(User1, 1, Group1);
			VerifyGroupMembership(User2, 1, Group1);
			VerifyGroupMembership(User3, 0, null);
		}

		private void VerifyGroupMembership(string user, int size, string group)
		{
			IList<string> groups = new AList<string>();
			NetgroupCache.GetNetgroups(user, groups);
			NUnit.Framework.Assert.AreEqual(size, groups.Count);
			if (size > 0)
			{
				bool present = false;
				foreach (string groupEntry in groups)
				{
					if (groupEntry.Equals(group))
					{
						present = true;
						break;
					}
				}
				NUnit.Framework.Assert.IsTrue(present);
			}
		}
	}
}
