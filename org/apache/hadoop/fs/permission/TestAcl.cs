using Sharpen;

namespace org.apache.hadoop.fs.permission
{
	/// <summary>Tests covering basic functionality of the ACL objects.</summary>
	public class TestAcl
	{
		private static org.apache.hadoop.fs.permission.AclEntry ENTRY1;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY2;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY3;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY4;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY5;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY6;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY7;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY8;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY9;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY10;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY11;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY12;

		private static org.apache.hadoop.fs.permission.AclEntry ENTRY13;

		private static org.apache.hadoop.fs.permission.AclStatus STATUS1;

		private static org.apache.hadoop.fs.permission.AclStatus STATUS2;

		private static org.apache.hadoop.fs.permission.AclStatus STATUS3;

		private static org.apache.hadoop.fs.permission.AclStatus STATUS4;

		[NUnit.Framework.BeforeClass]
		public static void setUp()
		{
			// named user
			org.apache.hadoop.fs.permission.AclEntry.Builder aclEntryBuilder = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.USER).setName("user1").setPermission
				(org.apache.hadoop.fs.permission.FsAction.ALL);
			ENTRY1 = aclEntryBuilder.build();
			ENTRY2 = aclEntryBuilder.build();
			// named group
			ENTRY3 = new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(org.apache.hadoop.fs.permission.AclEntryType
				.GROUP).setName("group2").setPermission(org.apache.hadoop.fs.permission.FsAction
				.READ_WRITE).build();
			// default other
			ENTRY4 = new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(org.apache.hadoop.fs.permission.AclEntryType
				.OTHER).setPermission(org.apache.hadoop.fs.permission.FsAction.NONE).setScope(org.apache.hadoop.fs.permission.AclEntryScope
				.DEFAULT).build();
			// owner
			ENTRY5 = new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(org.apache.hadoop.fs.permission.AclEntryType
				.USER).setPermission(org.apache.hadoop.fs.permission.FsAction.ALL).build();
			// default named group
			ENTRY6 = new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(org.apache.hadoop.fs.permission.AclEntryType
				.GROUP).setName("group3").setPermission(org.apache.hadoop.fs.permission.FsAction
				.READ_WRITE).setScope(org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT).build
				();
			// other
			ENTRY7 = new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(org.apache.hadoop.fs.permission.AclEntryType
				.OTHER).setPermission(org.apache.hadoop.fs.permission.FsAction.NONE).build();
			// default named user
			ENTRY8 = new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(org.apache.hadoop.fs.permission.AclEntryType
				.USER).setName("user3").setPermission(org.apache.hadoop.fs.permission.FsAction.ALL
				).setScope(org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT).build();
			// mask
			ENTRY9 = new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(org.apache.hadoop.fs.permission.AclEntryType
				.MASK).setPermission(org.apache.hadoop.fs.permission.FsAction.READ).build();
			// default mask
			ENTRY10 = new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(org.apache.hadoop.fs.permission.AclEntryType
				.MASK).setPermission(org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE).setScope
				(org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT).build();
			// group
			ENTRY11 = new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(org.apache.hadoop.fs.permission.AclEntryType
				.GROUP).setPermission(org.apache.hadoop.fs.permission.FsAction.READ).build();
			// default group
			ENTRY12 = new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(org.apache.hadoop.fs.permission.AclEntryType
				.GROUP).setPermission(org.apache.hadoop.fs.permission.FsAction.READ).setScope(org.apache.hadoop.fs.permission.AclEntryScope
				.DEFAULT).build();
			// default owner
			ENTRY13 = new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(org.apache.hadoop.fs.permission.AclEntryType
				.USER).setPermission(org.apache.hadoop.fs.permission.FsAction.ALL).setScope(org.apache.hadoop.fs.permission.AclEntryScope
				.DEFAULT).build();
			org.apache.hadoop.fs.permission.AclStatus.Builder aclStatusBuilder = new org.apache.hadoop.fs.permission.AclStatus.Builder
				().owner("owner1").group("group1").addEntry(ENTRY1).addEntry(ENTRY3).addEntry(ENTRY4
				);
			STATUS1 = aclStatusBuilder.build();
			STATUS2 = aclStatusBuilder.build();
			STATUS3 = new org.apache.hadoop.fs.permission.AclStatus.Builder().owner("owner2")
				.group("group2").stickyBit(true).build();
			STATUS4 = new org.apache.hadoop.fs.permission.AclStatus.Builder().addEntry(ENTRY1
				).addEntry(ENTRY3).addEntry(ENTRY4).addEntry(ENTRY5).addEntry(ENTRY6).addEntry(ENTRY7
				).addEntry(ENTRY8).addEntry(ENTRY9).addEntry(ENTRY10).addEntry(ENTRY11).addEntry
				(ENTRY12).addEntry(ENTRY13).build();
		}

		[NUnit.Framework.Test]
		public virtual void testEntryEquals()
		{
			NUnit.Framework.Assert.AreNotSame(ENTRY1, ENTRY2);
			NUnit.Framework.Assert.AreNotSame(ENTRY1, ENTRY3);
			NUnit.Framework.Assert.AreNotSame(ENTRY1, ENTRY4);
			NUnit.Framework.Assert.AreNotSame(ENTRY2, ENTRY3);
			NUnit.Framework.Assert.AreNotSame(ENTRY2, ENTRY4);
			NUnit.Framework.Assert.AreNotSame(ENTRY3, ENTRY4);
			NUnit.Framework.Assert.AreEqual(ENTRY1, ENTRY1);
			NUnit.Framework.Assert.AreEqual(ENTRY2, ENTRY2);
			NUnit.Framework.Assert.AreEqual(ENTRY1, ENTRY2);
			NUnit.Framework.Assert.AreEqual(ENTRY2, ENTRY1);
			NUnit.Framework.Assert.IsFalse(ENTRY1.Equals(ENTRY3));
			NUnit.Framework.Assert.IsFalse(ENTRY1.Equals(ENTRY4));
			NUnit.Framework.Assert.IsFalse(ENTRY3.Equals(ENTRY4));
			NUnit.Framework.Assert.IsFalse(ENTRY1.Equals(null));
			NUnit.Framework.Assert.IsFalse(ENTRY1.Equals(new object()));
		}

		[NUnit.Framework.Test]
		public virtual void testEntryHashCode()
		{
			NUnit.Framework.Assert.AreEqual(ENTRY1.GetHashCode(), ENTRY2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(ENTRY1.GetHashCode() == ENTRY3.GetHashCode());
			NUnit.Framework.Assert.IsFalse(ENTRY1.GetHashCode() == ENTRY4.GetHashCode());
			NUnit.Framework.Assert.IsFalse(ENTRY3.GetHashCode() == ENTRY4.GetHashCode());
		}

		[NUnit.Framework.Test]
		public virtual void testEntryScopeIsAccessIfUnspecified()
		{
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.permission.AclEntryScope.ACCESS
				, ENTRY1.getScope());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.permission.AclEntryScope.ACCESS
				, ENTRY2.getScope());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.permission.AclEntryScope.ACCESS
				, ENTRY3.getScope());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT
				, ENTRY4.getScope());
		}

		[NUnit.Framework.Test]
		public virtual void testStatusEquals()
		{
			NUnit.Framework.Assert.AreNotSame(STATUS1, STATUS2);
			NUnit.Framework.Assert.AreNotSame(STATUS1, STATUS3);
			NUnit.Framework.Assert.AreNotSame(STATUS2, STATUS3);
			NUnit.Framework.Assert.AreEqual(STATUS1, STATUS1);
			NUnit.Framework.Assert.AreEqual(STATUS2, STATUS2);
			NUnit.Framework.Assert.AreEqual(STATUS1, STATUS2);
			NUnit.Framework.Assert.AreEqual(STATUS2, STATUS1);
			NUnit.Framework.Assert.IsFalse(STATUS1.Equals(STATUS3));
			NUnit.Framework.Assert.IsFalse(STATUS2.Equals(STATUS3));
			NUnit.Framework.Assert.IsFalse(STATUS1.Equals(null));
			NUnit.Framework.Assert.IsFalse(STATUS1.Equals(new object()));
		}

		[NUnit.Framework.Test]
		public virtual void testStatusHashCode()
		{
			NUnit.Framework.Assert.AreEqual(STATUS1.GetHashCode(), STATUS2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(STATUS1.GetHashCode() == STATUS3.GetHashCode());
		}

		[NUnit.Framework.Test]
		public virtual void testToString()
		{
			NUnit.Framework.Assert.AreEqual("user:user1:rwx", ENTRY1.ToString());
			NUnit.Framework.Assert.AreEqual("user:user1:rwx", ENTRY2.ToString());
			NUnit.Framework.Assert.AreEqual("group:group2:rw-", ENTRY3.ToString());
			NUnit.Framework.Assert.AreEqual("default:other::---", ENTRY4.ToString());
			NUnit.Framework.Assert.AreEqual("owner: owner1, group: group1, acl: {entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false}"
				, STATUS1.ToString());
			NUnit.Framework.Assert.AreEqual("owner: owner1, group: group1, acl: {entries: [user:user1:rwx, group:group2:rw-, default:other::---], stickyBit: false}"
				, STATUS2.ToString());
			NUnit.Framework.Assert.AreEqual("owner: owner2, group: group2, acl: {entries: [], stickyBit: true}"
				, STATUS3.ToString());
		}
	}
}
