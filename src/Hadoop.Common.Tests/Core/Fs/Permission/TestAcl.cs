using NUnit.Framework;


namespace Org.Apache.Hadoop.FS.Permission
{
	/// <summary>Tests covering basic functionality of the ACL objects.</summary>
	public class TestAcl
	{
		private static AclEntry Entry1;

		private static AclEntry Entry2;

		private static AclEntry Entry3;

		private static AclEntry Entry4;

		private static AclEntry Entry5;

		private static AclEntry Entry6;

		private static AclEntry Entry7;

		private static AclEntry Entry8;

		private static AclEntry Entry9;

		private static AclEntry Entry10;

		private static AclEntry Entry11;

		private static AclEntry Entry12;

		private static AclEntry Entry13;

		private static AclStatus Status1;

		private static AclStatus Status2;

		private static AclStatus Status3;

		private static AclStatus Status4;

		[BeforeClass]
		public static void SetUp()
		{
			// named user
			AclEntry.Builder aclEntryBuilder = new AclEntry.Builder().SetType(AclEntryType.User
				).SetName("user1").SetPermission(FsAction.All);
			Entry1 = aclEntryBuilder.Build();
			Entry2 = aclEntryBuilder.Build();
			// named group
			Entry3 = new AclEntry.Builder().SetType(AclEntryType.Group).SetName("group2").SetPermission
				(FsAction.ReadWrite).Build();
			// default other
			Entry4 = new AclEntry.Builder().SetType(AclEntryType.Other).SetPermission(FsAction
				.None).SetScope(AclEntryScope.Default).Build();
			// owner
			Entry5 = new AclEntry.Builder().SetType(AclEntryType.User).SetPermission(FsAction
				.All).Build();
			// default named group
			Entry6 = new AclEntry.Builder().SetType(AclEntryType.Group).SetName("group3").SetPermission
				(FsAction.ReadWrite).SetScope(AclEntryScope.Default).Build();
			// other
			Entry7 = new AclEntry.Builder().SetType(AclEntryType.Other).SetPermission(FsAction
				.None).Build();
			// default named user
			Entry8 = new AclEntry.Builder().SetType(AclEntryType.User).SetName("user3").SetPermission
				(FsAction.All).SetScope(AclEntryScope.Default).Build();
			// mask
			Entry9 = new AclEntry.Builder().SetType(AclEntryType.Mask).SetPermission(FsAction
				.Read).Build();
			// default mask
			Entry10 = new AclEntry.Builder().SetType(AclEntryType.Mask).SetPermission(FsAction
				.ReadExecute).SetScope(AclEntryScope.Default).Build();
			// group
			Entry11 = new AclEntry.Builder().SetType(AclEntryType.Group).SetPermission(FsAction
				.Read).Build();
			// default group
			Entry12 = new AclEntry.Builder().SetType(AclEntryType.Group).SetPermission(FsAction
				.Read).SetScope(AclEntryScope.Default).Build();
			// default owner
			Entry13 = new AclEntry.Builder().SetType(AclEntryType.User).SetPermission(FsAction
				.All).SetScope(AclEntryScope.Default).Build();
			AclStatus.Builder aclStatusBuilder = new AclStatus.Builder().Owner("owner1").Group
				("group1").AddEntry(Entry1).AddEntry(Entry3).AddEntry(Entry4);
			Status1 = aclStatusBuilder.Build();
			Status2 = aclStatusBuilder.Build();
			Status3 = new AclStatus.Builder().Owner("owner2").Group("group2").StickyBit(true)
				.Build();
			Status4 = new AclStatus.Builder().AddEntry(Entry1).AddEntry(Entry3).AddEntry(Entry4
				).AddEntry(Entry5).AddEntry(Entry6).AddEntry(Entry7).AddEntry(Entry8).AddEntry(Entry9
				).AddEntry(Entry10).AddEntry(Entry11).AddEntry(Entry12).AddEntry(Entry13).Build(
				);
		}

		[Fact]
		public virtual void TestEntryEquals()
		{
			NUnit.Framework.Assert.AreNotSame(Entry1, Entry2);
			NUnit.Framework.Assert.AreNotSame(Entry1, Entry3);
			NUnit.Framework.Assert.AreNotSame(Entry1, Entry4);
			NUnit.Framework.Assert.AreNotSame(Entry2, Entry3);
			NUnit.Framework.Assert.AreNotSame(Entry2, Entry4);
			NUnit.Framework.Assert.AreNotSame(Entry3, Entry4);
			Assert.Equal(Entry1, Entry1);
			Assert.Equal(Entry2, Entry2);
			Assert.Equal(Entry1, Entry2);
			Assert.Equal(Entry2, Entry1);
			NUnit.Framework.Assert.IsFalse(Entry1.Equals(Entry3));
			NUnit.Framework.Assert.IsFalse(Entry1.Equals(Entry4));
			NUnit.Framework.Assert.IsFalse(Entry3.Equals(Entry4));
			NUnit.Framework.Assert.IsFalse(Entry1.Equals(null));
			NUnit.Framework.Assert.IsFalse(Entry1.Equals(new object()));
		}

		[Fact]
		public virtual void TestEntryHashCode()
		{
			Assert.Equal(Entry1.GetHashCode(), Entry2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(Entry1.GetHashCode() == Entry3.GetHashCode());
			NUnit.Framework.Assert.IsFalse(Entry1.GetHashCode() == Entry4.GetHashCode());
			NUnit.Framework.Assert.IsFalse(Entry3.GetHashCode() == Entry4.GetHashCode());
		}

		[Fact]
		public virtual void TestEntryScopeIsAccessIfUnspecified()
		{
			Assert.Equal(AclEntryScope.Access, Entry1.GetScope());
			Assert.Equal(AclEntryScope.Access, Entry2.GetScope());
			Assert.Equal(AclEntryScope.Access, Entry3.GetScope());
			Assert.Equal(AclEntryScope.Default, Entry4.GetScope());
		}

		[Fact]
		public virtual void TestStatusEquals()
		{
			NUnit.Framework.Assert.AreNotSame(Status1, Status2);
			NUnit.Framework.Assert.AreNotSame(Status1, Status3);
			NUnit.Framework.Assert.AreNotSame(Status2, Status3);
			Assert.Equal(Status1, Status1);
			Assert.Equal(Status2, Status2);
			Assert.Equal(Status1, Status2);
			Assert.Equal(Status2, Status1);
			NUnit.Framework.Assert.IsFalse(Status1.Equals(Status3));
			NUnit.Framework.Assert.IsFalse(Status2.Equals(Status3));
			NUnit.Framework.Assert.IsFalse(Status1.Equals(null));
			NUnit.Framework.Assert.IsFalse(Status1.Equals(new object()));
		}

		[Fact]
		public virtual void TestStatusHashCode()
		{
			Assert.Equal(Status1.GetHashCode(), Status2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(Status1.GetHashCode() == Status3.GetHashCode());
		}

		[Fact]
		public virtual void TestToString()
		{
			Assert.Equal("user:user1:rwx", Entry1.ToString());
			Assert.Equal("user:user1:rwx", Entry2.ToString());
			Assert.Equal("group:group2:rw-", Entry3.ToString());
			Assert.Equal("default:other::---", Entry4.ToString());
			Assert.Equal("owner: owner1, group: group1, acl: {entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false}"
				, Status1.ToString());
			Assert.Equal("owner: owner1, group: group1, acl: {entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false}"
				, Status2.ToString());
			Assert.Equal("owner: owner2, group: group2, acl: {entries: [], stickyBit: true}"
				, Status3.ToString());
		}
	}
}
