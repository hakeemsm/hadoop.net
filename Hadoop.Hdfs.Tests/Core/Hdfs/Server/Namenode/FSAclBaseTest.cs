using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Tests NameNode interaction for all ACL modification APIs.</summary>
	/// <remarks>
	/// Tests NameNode interaction for all ACL modification APIs.  This test suite
	/// also covers interaction of setPermission with inodes that have ACLs.
	/// </remarks>
	public abstract class FSAclBaseTest
	{
		private static readonly UserGroupInformation Bruce = UserGroupInformation.CreateUserForTesting
			("bruce", new string[] {  });

		private static readonly UserGroupInformation Diana = UserGroupInformation.CreateUserForTesting
			("diana", new string[] {  });

		private static readonly UserGroupInformation SupergroupMember = UserGroupInformation
			.CreateUserForTesting("super", new string[] { DFSConfigKeys.DfsPermissionsSuperusergroupDefault
			 });

		private static readonly UserGroupInformation Bob = UserGroupInformation.CreateUserForTesting
			("bob", new string[] { "groupY", "groupZ" });

		protected internal static MiniDFSCluster cluster;

		protected internal static Configuration conf;

		private static int pathCount = 0;

		private static Path path;

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		private FileSystem fs;

		private FileSystem fsAsBruce;

		private FileSystem fsAsDiana;

		private FileSystem fsAsSupergroupMember;

		private FileSystem fsAsBob;

		// group member
		/// <exception cref="System.IO.IOException"/>
		protected internal static void StartCluster()
		{
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
		}

		[AfterClass]
		public static void Shutdown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			pathCount += 1;
			path = new Path("/p" + pathCount);
			InitFileSystems();
		}

		[TearDown]
		public virtual void DestroyFileSystems()
		{
			IOUtils.Cleanup(null, fs, fsAsBruce, fsAsDiana, fsAsSupergroupMember);
			fs = fsAsBruce = fsAsDiana = fsAsSupergroupMember = fsAsBob = null;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyAclEntries()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "foo", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, "foo", FsAction.ReadExecute));
			fs.ModifyAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11e8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyAclEntriesOnlyAccess()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "foo", FsAction.ReadExecute));
			fs.ModifyAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute) }, returned);
			AssertPermission((short)0x11e8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyAclEntriesOnlyDefault()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "foo", FsAction.ReadExecute));
			fs.ModifyAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, "foo", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11e8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyAclEntriesMinimal()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.ReadWrite));
			fs.ModifyAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read) }, returned);
			AssertPermission((short)0x11b0);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyAclEntriesMinimalDefault()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None));
			fs.ModifyAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11e8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyAclEntriesCustomMask()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Mask, FsAction.None));
			fs.ModifyAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read) }, returned);
			AssertPermission((short)0x1180);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyAclEntriesStickyBit()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x3e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "foo", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, "foo", FsAction.ReadExecute));
			fs.ModifyAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask, FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x13e8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestModifyAclEntriesPathNotFound()
		{
			// Path has not been created.
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None));
			fs.ModifyAclEntries(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestModifyAclEntriesDefaultOnFile()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.ModifyAclEntries(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclEntries()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "foo"), AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType.User, 
				"foo"));
			fs.RemoveAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11e8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclEntriesOnlyAccess()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bar", FsAction.ReadWrite), AclTestHelpers.AclEntry(
				AclEntryScope.Access, AclEntryType.Group, FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "foo"));
			fs.RemoveAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bar", FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadWrite) }, returned);
			AssertPermission((short)0x11f0);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclEntriesOnlyDefault()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bar", FsAction.ReadExecute));
			fs.SetAcl(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "foo"));
			fs.RemoveAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, "bar", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11e8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclEntriesMinimal()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1f0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "foo"), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Mask));
			fs.RemoveAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission((short)0x1f0);
			AssertAclFeature(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclEntriesMinimalDefault()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "foo"), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.Mask), 
				AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType.User, "foo"), AclTestHelpers.AclEntry
				(AclEntryScope.Default, AclEntryType.Mask));
			fs.RemoveAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11e8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclEntriesStickyBit()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x3e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "foo"), AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType.User, 
				"foo"));
			fs.RemoveAclEntries(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x13e8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRemoveAclEntriesPathNotFound()
		{
			// Path has not been created.
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo"));
			fs.RemoveAclEntries(path, aclSpec);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveDefaultAcl()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			fs.RemoveDefaultAcl(path);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute) }, returned);
			AssertPermission((short)0x11f8);
			AssertAclFeature(true);
			// restart of the cluster
			RestartCluster();
			s = fs.GetAclStatus(path);
			AclEntry[] afterRestart = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry
				[0]);
			Assert.AssertArrayEquals(returned, afterRestart);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveDefaultAclOnlyAccess()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			fs.RemoveDefaultAcl(path);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute) }, returned);
			AssertPermission((short)0x11f8);
			AssertAclFeature(true);
			// restart of the cluster
			RestartCluster();
			s = fs.GetAclStatus(path);
			AclEntry[] afterRestart = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry
				[0]);
			Assert.AssertArrayEquals(returned, afterRestart);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveDefaultAclOnlyDefault()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			fs.RemoveDefaultAcl(path);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission((short)0x1e8);
			AssertAclFeature(false);
			// restart of the cluster
			RestartCluster();
			s = fs.GetAclStatus(path);
			AclEntry[] afterRestart = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry
				[0]);
			Assert.AssertArrayEquals(returned, afterRestart);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveDefaultAclMinimal()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			fs.RemoveDefaultAcl(path);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission((short)0x1e8);
			AssertAclFeature(false);
			// restart of the cluster
			RestartCluster();
			s = fs.GetAclStatus(path);
			AclEntry[] afterRestart = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry
				[0]);
			Assert.AssertArrayEquals(returned, afterRestart);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveDefaultAclStickyBit()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x3e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			fs.RemoveDefaultAcl(path);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute) }, returned);
			AssertPermission((short)0x13f8);
			AssertAclFeature(true);
			// restart of the cluster
			RestartCluster();
			s = fs.GetAclStatus(path);
			AclEntry[] afterRestart = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry
				[0]);
			Assert.AssertArrayEquals(returned, afterRestart);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRemoveDefaultAclPathNotFound()
		{
			// Path has not been created.
			fs.RemoveDefaultAcl(path);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAcl()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			fs.RemoveAcl(path);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission((short)0x1e8);
			AssertAclFeature(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclMinimalAcl()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a0));
			fs.RemoveAcl(path);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission((short)0x1a0);
			AssertAclFeature(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclStickyBit()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x3e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			fs.RemoveAcl(path);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission((short)0x3e8);
			AssertAclFeature(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclOnlyDefault()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			fs.RemoveAcl(path);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission((short)0x1e8);
			AssertAclFeature(false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRemoveAclPathNotFound()
		{
			// Path has not been created.
			fs.RemoveAcl(path);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSetAcl()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11f8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSetAclOnlyAccess()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read) }, returned);
			AssertPermission((short)0x11a0);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSetAclOnlyDefault()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11e8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSetAclMinimal()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a4));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission((short)0x1a0);
			AssertAclFeature(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSetAclMinimalDefault()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11e8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSetAclCustomMask()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read) }, returned);
			AssertPermission((short)0x11b8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSetAclStickyBit()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x3e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x13f8);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetAclPathNotFound()
		{
			// Path has not been created.
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetAclDefaultOnFile()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSetPermission()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1c0));
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11c0);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSetPermissionOnlyAccess()
		{
			fs.Create(path).Close();
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1a0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x180));
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read) }, returned);
			AssertPermission((short)0x1180);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSetPermissionOnlyDefault()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1c0));
			AclStatus s = fs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11c0);
			AssertAclFeature(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSetPermissionCannotSetAclBit()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			fs.SetPermission(path, FsPermission.CreateImmutable((short)0x1c0));
			AssertPermission((short)0x1c0);
			fs.SetPermission(path, new FsPermissionExtension(FsPermission.CreateImmutable((short
				)0x1ed), true, true));
			INode inode = cluster.GetNamesystem().GetFSDirectory().GetINode(path.ToUri().GetPath
				(), false);
			NUnit.Framework.Assert.IsNotNull(inode);
			FsPermission perm = inode.GetFsPermission();
			NUnit.Framework.Assert.IsNotNull(perm);
			NUnit.Framework.Assert.AreEqual(0x1ed, perm.ToShort());
			NUnit.Framework.Assert.AreEqual(0x1ed, perm.ToExtendedShort());
			AssertAclFeature(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultAclNewFile()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			Path filePath = new Path(path, "file1");
			fs.Create(filePath).Close();
			AclStatus s = fs.GetAclStatus(filePath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute) }, returned);
			AssertPermission(filePath, (short)0x11a0);
			AssertAclFeature(filePath, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOnlyAccessAclNewFile()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All));
			fs.ModifyAclEntries(path, aclSpec);
			Path filePath = new Path(path, "file1");
			fs.Create(filePath).Close();
			AclStatus s = fs.GetAclStatus(filePath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission(filePath, (short)0x1a4);
			AssertAclFeature(filePath, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultMinimalAclNewFile()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			Path filePath = new Path(path, "file1");
			fs.Create(filePath).Close();
			AclStatus s = fs.GetAclStatus(filePath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission(filePath, (short)0x1a0);
			AssertAclFeature(filePath, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultAclNewDir()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			Path dirPath = new Path(path, "dir1");
			fs.Mkdirs(dirPath);
			AclStatus s = fs.GetAclStatus(dirPath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission(dirPath, (short)0x11e8);
			AssertAclFeature(dirPath, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOnlyAccessAclNewDir()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All));
			fs.ModifyAclEntries(path, aclSpec);
			Path dirPath = new Path(path, "dir1");
			fs.Mkdirs(dirPath);
			AclStatus s = fs.GetAclStatus(dirPath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission(dirPath, (short)0x1ed);
			AssertAclFeature(dirPath, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultMinimalAclNewDir()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None));
			fs.SetAcl(path, aclSpec);
			Path dirPath = new Path(path, "dir1");
			fs.Mkdirs(dirPath);
			AclStatus s = fs.GetAclStatus(dirPath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission(dirPath, (short)0x11e8);
			AssertAclFeature(dirPath, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultAclNewFileIntermediate()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			Path dirPath = new Path(path, "dir1");
			Path filePath = new Path(dirPath, "file1");
			fs.Create(filePath).Close();
			AclEntry[] expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) };
			AclStatus s = fs.GetAclStatus(dirPath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission(dirPath, (short)0x11e8);
			AssertAclFeature(dirPath, true);
			expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.ReadExecute) };
			s = fs.GetAclStatus(filePath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission(filePath, (short)0x11a0);
			AssertAclFeature(filePath, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultAclNewDirIntermediate()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			Path dirPath = new Path(path, "dir1");
			Path subdirPath = new Path(dirPath, "subdir1");
			fs.Mkdirs(subdirPath);
			AclEntry[] expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) };
			AclStatus s = fs.GetAclStatus(dirPath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission(dirPath, (short)0x11e8);
			AssertAclFeature(dirPath, true);
			s = fs.GetAclStatus(subdirPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission(subdirPath, (short)0x11e8);
			AssertAclFeature(subdirPath, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultAclNewSymlinkIntermediate()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1e8));
			Path filePath = new Path(path, "file1");
			fs.Create(filePath).Close();
			fs.SetPermission(filePath, FsPermission.CreateImmutable((short)0x1a0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			Path dirPath = new Path(path, "dir1");
			Path linkPath = new Path(dirPath, "link1");
			fs.CreateSymlink(filePath, linkPath, true);
			AclEntry[] expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) };
			AclStatus s = fs.GetAclStatus(dirPath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission(dirPath, (short)0x11e8);
			AssertAclFeature(dirPath, true);
			expected = new AclEntry[] {  };
			s = fs.GetAclStatus(linkPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission(linkPath, (short)0x1a0);
			AssertAclFeature(linkPath, false);
			s = fs.GetAclStatus(filePath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission(filePath, (short)0x1a0);
			AssertAclFeature(filePath, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultAclNewFileWithMode()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1ed));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			Path filePath = new Path(path, "file1");
			int bufferSize = cluster.GetConfiguration(0).GetInt(CommonConfigurationKeys.IoFileBufferSizeKey
				, CommonConfigurationKeys.IoFileBufferSizeDefault);
			fs.Create(filePath, new FsPermission((short)0x1e0), false, bufferSize, fs.GetDefaultReplication
				(filePath), fs.GetDefaultBlockSize(path), null).Close();
			AclStatus s = fs.GetAclStatus(filePath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute) }, returned);
			AssertPermission(filePath, (short)0x11e0);
			AssertAclFeature(filePath, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultAclNewDirWithMode()
		{
			FileSystem.Mkdirs(fs, path, FsPermission.CreateImmutable((short)0x1ed));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(path, aclSpec);
			Path dirPath = new Path(path, "dir1");
			fs.Mkdirs(dirPath, new FsPermission((short)0x1e0));
			AclStatus s = fs.GetAclStatus(dirPath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.ReadExecute) }, returned);
			AssertPermission(dirPath, (short)0x11e0);
			AssertAclFeature(dirPath, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultAclRenamedFile()
		{
			Path dirPath = new Path(path, "dir");
			FileSystem.Mkdirs(fs, dirPath, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(dirPath, aclSpec);
			Path filePath = new Path(path, "file1");
			fs.Create(filePath).Close();
			fs.SetPermission(filePath, FsPermission.CreateImmutable((short)0x1a0));
			Path renamedFilePath = new Path(dirPath, "file1");
			fs.Rename(filePath, renamedFilePath);
			AclEntry[] expected = new AclEntry[] {  };
			AclStatus s = fs.GetAclStatus(renamedFilePath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission(renamedFilePath, (short)0x1a0);
			AssertAclFeature(renamedFilePath, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultAclRenamedDir()
		{
			Path dirPath = new Path(path, "dir");
			FileSystem.Mkdirs(fs, dirPath, FsPermission.CreateImmutable((short)0x1e8));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(dirPath, aclSpec);
			Path subdirPath = new Path(path, "subdir");
			FileSystem.Mkdirs(fs, subdirPath, FsPermission.CreateImmutable((short)0x1e8));
			Path renamedSubdirPath = new Path(dirPath, "subdir");
			fs.Rename(subdirPath, renamedSubdirPath);
			AclEntry[] expected = new AclEntry[] {  };
			AclStatus s = fs.GetAclStatus(renamedSubdirPath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission(renamedSubdirPath, (short)0x1e8);
			AssertAclFeature(renamedSubdirPath, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSkipAclEnforcementPermsDisabled()
		{
			Path bruceDir = new Path(path, "bruce");
			Path bruceFile = new Path(bruceDir, "file");
			fs.Mkdirs(bruceDir);
			fs.SetOwner(bruceDir, "bruce", null);
			fsAsBruce.Create(bruceFile).Close();
			fsAsBruce.ModifyAclEntries(bruceFile, Lists.NewArrayList(AclTestHelpers.AclEntry(
				AclEntryScope.Access, AclEntryType.User, "diana", FsAction.None)));
			AclTestHelpers.AssertFilePermissionDenied(fsAsDiana, Diana, bruceFile);
			try
			{
				conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, false);
				RestartCluster();
				AclTestHelpers.AssertFilePermissionGranted(fsAsDiana, Diana, bruceFile);
			}
			finally
			{
				conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, true);
				RestartCluster();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSkipAclEnforcementSuper()
		{
			Path bruceDir = new Path(path, "bruce");
			Path bruceFile = new Path(bruceDir, "file");
			fs.Mkdirs(bruceDir);
			fs.SetOwner(bruceDir, "bruce", null);
			fsAsBruce.Create(bruceFile).Close();
			fsAsBruce.ModifyAclEntries(bruceFile, Lists.NewArrayList(AclTestHelpers.AclEntry(
				AclEntryScope.Access, AclEntryType.User, "diana", FsAction.None)));
			AclTestHelpers.AssertFilePermissionGranted(fs, Diana, bruceFile);
			AclTestHelpers.AssertFilePermissionGranted(fsAsBruce, Diana, bruceFile);
			AclTestHelpers.AssertFilePermissionDenied(fsAsDiana, Diana, bruceFile);
			AclTestHelpers.AssertFilePermissionGranted(fsAsSupergroupMember, SupergroupMember
				, bruceFile);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyAclEntriesMustBeOwnerOrSuper()
		{
			Path bruceDir = new Path(path, "bruce");
			Path bruceFile = new Path(bruceDir, "file");
			fs.Mkdirs(bruceDir);
			fs.SetOwner(bruceDir, "bruce", null);
			fsAsBruce.Create(bruceFile).Close();
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.All));
			fsAsBruce.ModifyAclEntries(bruceFile, aclSpec);
			fs.ModifyAclEntries(bruceFile, aclSpec);
			fsAsSupergroupMember.ModifyAclEntries(bruceFile, aclSpec);
			exception.Expect(typeof(AccessControlException));
			fsAsDiana.ModifyAclEntries(bruceFile, aclSpec);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclEntriesMustBeOwnerOrSuper()
		{
			Path bruceDir = new Path(path, "bruce");
			Path bruceFile = new Path(bruceDir, "file");
			fs.Mkdirs(bruceDir);
			fs.SetOwner(bruceDir, "bruce", null);
			fsAsBruce.Create(bruceFile).Close();
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana"));
			fsAsBruce.RemoveAclEntries(bruceFile, aclSpec);
			fs.RemoveAclEntries(bruceFile, aclSpec);
			fsAsSupergroupMember.RemoveAclEntries(bruceFile, aclSpec);
			exception.Expect(typeof(AccessControlException));
			fsAsDiana.RemoveAclEntries(bruceFile, aclSpec);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveDefaultAclMustBeOwnerOrSuper()
		{
			Path bruceDir = new Path(path, "bruce");
			Path bruceFile = new Path(bruceDir, "file");
			fs.Mkdirs(bruceDir);
			fs.SetOwner(bruceDir, "bruce", null);
			fsAsBruce.Create(bruceFile).Close();
			fsAsBruce.RemoveDefaultAcl(bruceFile);
			fs.RemoveDefaultAcl(bruceFile);
			fsAsSupergroupMember.RemoveDefaultAcl(bruceFile);
			exception.Expect(typeof(AccessControlException));
			fsAsDiana.RemoveDefaultAcl(bruceFile);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclMustBeOwnerOrSuper()
		{
			Path bruceDir = new Path(path, "bruce");
			Path bruceFile = new Path(bruceDir, "file");
			fs.Mkdirs(bruceDir);
			fs.SetOwner(bruceDir, "bruce", null);
			fsAsBruce.Create(bruceFile).Close();
			fsAsBruce.RemoveAcl(bruceFile);
			fs.RemoveAcl(bruceFile);
			fsAsSupergroupMember.RemoveAcl(bruceFile);
			exception.Expect(typeof(AccessControlException));
			fsAsDiana.RemoveAcl(bruceFile);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetAclMustBeOwnerOrSuper()
		{
			Path bruceDir = new Path(path, "bruce");
			Path bruceFile = new Path(bruceDir, "file");
			fs.Mkdirs(bruceDir);
			fs.SetOwner(bruceDir, "bruce", null);
			fsAsBruce.Create(bruceFile).Close();
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.Read));
			fsAsBruce.SetAcl(bruceFile, aclSpec);
			fs.SetAcl(bruceFile, aclSpec);
			fsAsSupergroupMember.SetAcl(bruceFile, aclSpec);
			exception.Expect(typeof(AccessControlException));
			fsAsDiana.SetAcl(bruceFile, aclSpec);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAclStatusRequiresTraverseOrSuper()
		{
			Path bruceDir = new Path(path, "bruce");
			Path bruceFile = new Path(bruceDir, "file");
			fs.Mkdirs(bruceDir);
			fs.SetOwner(bruceDir, "bruce", null);
			fsAsBruce.Create(bruceFile).Close();
			fsAsBruce.SetAcl(bruceDir, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None)));
			fsAsBruce.GetAclStatus(bruceFile);
			fs.GetAclStatus(bruceFile);
			fsAsSupergroupMember.GetAclStatus(bruceFile);
			exception.Expect(typeof(AccessControlException));
			fsAsDiana.GetAclStatus(bruceFile);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAccess()
		{
			Path p1 = new Path("/p1");
			fs.Mkdirs(p1);
			fs.SetOwner(p1, Bruce.GetShortUserName(), "groupX");
			fsAsBruce.SetAcl(p1, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.None)));
			fsAsBruce.Access(p1, FsAction.Read);
			try
			{
				fsAsBruce.Access(p1, FsAction.Write);
				NUnit.Framework.Assert.Fail("The access call should have failed.");
			}
			catch (AccessControlException)
			{
			}
			// expected
			Path badPath = new Path("/bad/bad");
			try
			{
				fsAsBruce.Access(badPath, FsAction.Read);
				NUnit.Framework.Assert.Fail("The access call should have failed");
			}
			catch (FileNotFoundException)
			{
			}
			// expected
			// Add a named group entry with only READ access
			fsAsBruce.ModifyAclEntries(p1, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, "groupY", FsAction.Read)));
			// Now bob should have read access, but not write
			fsAsBob.Access(p1, FsAction.Read);
			try
			{
				fsAsBob.Access(p1, FsAction.Write);
				NUnit.Framework.Assert.Fail("The access call should have failed.");
			}
			catch (AccessControlException)
			{
			}
			// expected;
			// Add another named group entry with WRITE access
			fsAsBruce.ModifyAclEntries(p1, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, "groupZ", FsAction.Write)));
			// Now bob should have write access
			fsAsBob.Access(p1, FsAction.Write);
			// Add a named user entry to deny bob
			fsAsBruce.ModifyAclEntries(p1, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bob", FsAction.None)));
			try
			{
				fsAsBob.Access(p1, FsAction.Read);
				NUnit.Framework.Assert.Fail("The access call should have failed.");
			}
			catch (AccessControlException)
			{
			}
		}

		// expected;
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEffectiveAccess()
		{
			Path p1 = new Path("/testEffectiveAccess");
			fs.Mkdirs(p1);
			// give all access at first
			fs.SetPermission(p1, FsPermission.ValueOf("-rwxrwxrwx"));
			AclStatus aclStatus = fs.GetAclStatus(p1);
			NUnit.Framework.Assert.AreEqual("Entries should be empty", 0, aclStatus.GetEntries
				().Count);
			NUnit.Framework.Assert.AreEqual("Permission should be carried by AclStatus", fs.GetFileStatus
				(p1).GetPermission(), aclStatus.GetPermission());
			// Add a named entries with all access
			fs.ModifyAclEntries(p1, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.
				Access, AclEntryType.User, "bruce", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, "groupY", FsAction.All)));
			aclStatus = fs.GetAclStatus(p1);
			NUnit.Framework.Assert.AreEqual("Entries should contain owner group entry also", 
				3, aclStatus.GetEntries().Count);
			// restrict the access
			fs.SetPermission(p1, FsPermission.ValueOf("-rwxr-----"));
			// latest permissions should be reflected as effective permission
			aclStatus = fs.GetAclStatus(p1);
			IList<AclEntry> entries = aclStatus.GetEntries();
			foreach (AclEntry aclEntry in entries)
			{
				if (aclEntry.GetName() != null || aclEntry.GetType() == AclEntryType.Group)
				{
					NUnit.Framework.Assert.AreEqual(FsAction.All, aclEntry.GetPermission());
					NUnit.Framework.Assert.AreEqual(FsAction.Read, aclStatus.GetEffectivePermission(aclEntry
						));
				}
			}
			fsAsBruce.Access(p1, FsAction.Read);
			try
			{
				fsAsBruce.Access(p1, FsAction.Write);
				NUnit.Framework.Assert.Fail("Access should not be given");
			}
			catch (AccessControlException)
			{
			}
			// expected
			fsAsBob.Access(p1, FsAction.Read);
			try
			{
				fsAsBob.Access(p1, FsAction.Write);
				NUnit.Framework.Assert.Fail("Access should not be given");
			}
			catch (AccessControlException)
			{
			}
		}

		// expected
		/// <summary>Verify the de-duplication of AclFeatures with same entries.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeDuplication()
		{
			// This test needs to verify the count of the references which is held by
			// static data structure. So shutting down entire cluster to get the fresh
			// data.
			Shutdown();
			AclStorage.GetUniqueAclFeatures().Clear();
			StartCluster();
			SetUp();
			int currentSize = 0;
			Path p1 = new Path("/testDeduplication");
			{
				// unique default AclEntries for this test
				IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
					.Default, AclEntryType.User, "testdeduplicateuser", FsAction.All), AclTestHelpers.AclEntry
					(AclEntryScope.Default, AclEntryType.Group, "testdeduplicategroup", FsAction.All
					));
				fs.Mkdirs(p1);
				fs.ModifyAclEntries(p1, aclSpec);
				NUnit.Framework.Assert.AreEqual("One more ACL feature should be unique", currentSize
					 + 1, AclStorage.GetUniqueAclFeatures().GetUniqueElementsSize());
				currentSize++;
			}
			Path child1 = new Path(p1, "child1");
			AclFeature child1AclFeature;
			{
				// new child dir should copy entries from its parent.
				fs.Mkdirs(child1);
				NUnit.Framework.Assert.AreEqual("One more ACL feature should be unique", currentSize
					 + 1, AclStorage.GetUniqueAclFeatures().GetUniqueElementsSize());
				child1AclFeature = GetAclFeature(child1, cluster);
				NUnit.Framework.Assert.AreEqual("Reference count should be 1", 1, child1AclFeature
					.GetRefCount());
				currentSize++;
			}
			Path child2 = new Path(p1, "child2");
			{
				// new child dir should copy entries from its parent. But all entries are
				// same as its sibling without any more acl changes.
				fs.Mkdirs(child2);
				NUnit.Framework.Assert.AreEqual("existing AclFeature should be re-used", currentSize
					, AclStorage.GetUniqueAclFeatures().GetUniqueElementsSize());
				AclFeature child2AclFeature = GetAclFeature(child1, cluster);
				NUnit.Framework.Assert.AreSame("Same Aclfeature should be re-used", child1AclFeature
					, child2AclFeature);
				NUnit.Framework.Assert.AreEqual("Reference count should be 2", 2, child2AclFeature
					.GetRefCount());
			}
			{
				// modification of ACL on should decrement the original reference count
				// and increase new one.
				IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.User, "user1", FsAction.All));
				fs.ModifyAclEntries(child1, aclSpec);
				AclFeature modifiedAclFeature = GetAclFeature(child1, cluster);
				NUnit.Framework.Assert.AreEqual("Old Reference count should be 1", 1, child1AclFeature
					.GetRefCount());
				NUnit.Framework.Assert.AreEqual("New Reference count should be 1", 1, modifiedAclFeature
					.GetRefCount());
				// removing the new added ACL entry should refer to old ACLfeature
				AclEntry aclEntry = new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType
					(AclEntryType.User).SetName("user1").Build();
				fs.RemoveAclEntries(child1, Lists.NewArrayList(aclEntry));
				NUnit.Framework.Assert.AreEqual("Old Reference count should be 2 again", 2, child1AclFeature
					.GetRefCount());
				NUnit.Framework.Assert.AreEqual("New Reference count should be 0", 0, modifiedAclFeature
					.GetRefCount());
			}
			{
				// verify the reference count on deletion of Acls
				fs.RemoveAcl(child2);
				NUnit.Framework.Assert.AreEqual("Reference count should be 1", 1, child1AclFeature
					.GetRefCount());
			}
			{
				// verify the reference count on deletion of dir with ACL
				fs.Delete(child1, true);
				NUnit.Framework.Assert.AreEqual("Reference count should be 0", 0, child1AclFeature
					.GetRefCount());
			}
			Path file1 = new Path(p1, "file1");
			Path file2 = new Path(p1, "file2");
			AclFeature fileAclFeature;
			{
				// Using same reference on creation of file
				fs.Create(file1).Close();
				fileAclFeature = GetAclFeature(file1, cluster);
				NUnit.Framework.Assert.AreEqual("Reference count should be 1", 1, fileAclFeature.
					GetRefCount());
				fs.Create(file2).Close();
				NUnit.Framework.Assert.AreEqual("Reference count should be 2", 2, fileAclFeature.
					GetRefCount());
			}
			{
				// modifying ACLs on file should decrease the reference count on old
				// instance and increase on the new instance
				IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.User, "user1", FsAction.All));
				// adding new ACL entry
				fs.ModifyAclEntries(file1, aclSpec);
				AclFeature modifiedFileAcl = GetAclFeature(file1, cluster);
				NUnit.Framework.Assert.AreEqual("Old Reference count should be 1", 1, fileAclFeature
					.GetRefCount());
				NUnit.Framework.Assert.AreEqual("New Reference count should be 1", 1, modifiedFileAcl
					.GetRefCount());
				// removing the new added ACL entry should refer to old ACLfeature
				AclEntry aclEntry = new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType
					(AclEntryType.User).SetName("user1").Build();
				fs.RemoveAclEntries(file1, Lists.NewArrayList(aclEntry));
				NUnit.Framework.Assert.AreEqual("Old Reference count should be 2", 2, fileAclFeature
					.GetRefCount());
				NUnit.Framework.Assert.AreEqual("New Reference count should be 0", 0, modifiedFileAcl
					.GetRefCount());
			}
			{
				// reference count should be decreased on deletion of files with ACLs
				fs.Delete(file2, true);
				NUnit.Framework.Assert.AreEqual("Reference count should be decreased on delete of the file"
					, 1, fileAclFeature.GetRefCount());
				fs.Delete(file1, true);
				NUnit.Framework.Assert.AreEqual("Reference count should be decreased on delete of the file"
					, 0, fileAclFeature.GetRefCount());
				// On reference count reaches 0 instance should be removed from map
				fs.Create(file1).Close();
				AclFeature newFileAclFeature = GetAclFeature(file1, cluster);
				NUnit.Framework.Assert.AreNotSame("Instance should be different on reference count 0"
					, fileAclFeature, newFileAclFeature);
				fileAclFeature = newFileAclFeature;
			}
			IDictionary<AclFeature, int> restartRefCounter = new Dictionary<AclFeature, int>(
				);
			// Restart the Namenode to check the references.
			// Here reference counts will not be same after restart because, while
			// shutting down namenode will not call any removal of AclFeature.
			// However this is applicable only in case of tests as in real-cluster JVM
			// itself will be new.
			IList<AclFeature> entriesBeforeRestart = AclStorage.GetUniqueAclFeatures().GetEntries
				();
			{
				//restart by loading edits
				foreach (AclFeature aclFeature in entriesBeforeRestart)
				{
					restartRefCounter[aclFeature] = aclFeature.GetRefCount();
				}
				cluster.RestartNameNode(true);
				IList<AclFeature> entriesAfterRestart = AclStorage.GetUniqueAclFeatures().GetEntries
					();
				NUnit.Framework.Assert.AreEqual("Entries before and after should be same", entriesBeforeRestart
					, entriesAfterRestart);
				foreach (AclFeature aclFeature_1 in entriesAfterRestart)
				{
					int before = restartRefCounter[aclFeature_1];
					NUnit.Framework.Assert.AreEqual("ReferenceCount After Restart should be doubled", 
						before * 2, aclFeature_1.GetRefCount());
				}
			}
			{
				//restart by loading fsimage
				cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, 
					false);
				cluster.GetNameNodeRpc().SaveNamespace();
				cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, 
					false);
				cluster.RestartNameNode(true);
				IList<AclFeature> entriesAfterRestart = AclStorage.GetUniqueAclFeatures().GetEntries
					();
				NUnit.Framework.Assert.AreEqual("Entries before and after should be same", entriesBeforeRestart
					, entriesAfterRestart);
				foreach (AclFeature aclFeature in entriesAfterRestart)
				{
					int before = restartRefCounter[aclFeature];
					NUnit.Framework.Assert.AreEqual("ReferenceCount After 2 Restarts should be tripled"
						, before * 3, aclFeature.GetRefCount());
				}
			}
		}

		/// <summary>Creates a FileSystem for the super-user.</summary>
		/// <returns>FileSystem for super-user</returns>
		/// <exception cref="System.Exception">if creation fails</exception>
		protected internal virtual FileSystem CreateFileSystem()
		{
			return cluster.GetFileSystem();
		}

		/// <summary>Creates a FileSystem for a specific user.</summary>
		/// <param name="user">UserGroupInformation specific user</param>
		/// <returns>FileSystem for specific user</returns>
		/// <exception cref="System.Exception">if creation fails</exception>
		protected internal virtual FileSystem CreateFileSystem(UserGroupInformation user)
		{
			return DFSTestUtil.GetFileSystemAs(user, cluster.GetConfiguration(0));
		}

		/// <summary>Initializes all FileSystem instances used in the tests.</summary>
		/// <exception cref="System.Exception">if initialization fails</exception>
		private void InitFileSystems()
		{
			fs = CreateFileSystem();
			fsAsBruce = CreateFileSystem(Bruce);
			fsAsDiana = CreateFileSystem(Diana);
			fsAsBob = CreateFileSystem(Bob);
			fsAsSupergroupMember = CreateFileSystem(SupergroupMember);
		}

		/// <summary>Restarts the cluster without formatting, so all data is preserved.</summary>
		/// <exception cref="System.Exception">if restart fails</exception>
		private void RestartCluster()
		{
			DestroyFileSystems();
			Shutdown();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(false).Build();
			cluster.WaitActive();
			InitFileSystems();
		}

		/// <summary>Asserts whether or not the inode for the test path has an AclFeature.</summary>
		/// <param name="expectAclFeature">
		/// boolean true if an AclFeature must be present,
		/// false if an AclFeature must not be present
		/// </param>
		/// <exception cref="System.IO.IOException">thrown if there is an I/O error</exception>
		private static void AssertAclFeature(bool expectAclFeature)
		{
			AssertAclFeature(path, expectAclFeature);
		}

		/// <summary>Asserts whether or not the inode for a specific path has an AclFeature.</summary>
		/// <param name="pathToCheck">Path inode to check</param>
		/// <param name="expectAclFeature">
		/// boolean true if an AclFeature must be present,
		/// false if an AclFeature must not be present
		/// </param>
		/// <exception cref="System.IO.IOException">thrown if there is an I/O error</exception>
		private static void AssertAclFeature(Path pathToCheck, bool expectAclFeature)
		{
			AclFeature aclFeature = GetAclFeature(pathToCheck, cluster);
			if (expectAclFeature)
			{
				NUnit.Framework.Assert.IsNotNull(aclFeature);
				// Intentionally capturing a reference to the entries, not using nested
				// calls.  This way, we get compile-time enforcement that the entries are
				// stored in an ImmutableList.
				ImmutableList<AclEntry> entries = AclStorage.GetEntriesFromAclFeature(aclFeature);
				NUnit.Framework.Assert.IsFalse(entries.IsEmpty());
			}
			else
			{
				NUnit.Framework.Assert.IsNull(aclFeature);
			}
		}

		/// <summary>Get AclFeature for the path</summary>
		/// <exception cref="System.IO.IOException"/>
		public static AclFeature GetAclFeature(Path pathToCheck, MiniDFSCluster cluster)
		{
			INode inode = cluster.GetNamesystem().GetFSDirectory().GetINode(pathToCheck.ToUri
				().GetPath(), false);
			NUnit.Framework.Assert.IsNotNull(inode);
			AclFeature aclFeature = inode.GetAclFeature();
			return aclFeature;
		}

		/// <summary>Asserts the value of the FsPermission bits on the inode of the test path.
		/// 	</summary>
		/// <param name="perm">short expected permission bits</param>
		/// <exception cref="System.IO.IOException">thrown if there is an I/O error</exception>
		private void AssertPermission(short perm)
		{
			AssertPermission(path, perm);
		}

		/// <summary>Asserts the value of the FsPermission bits on the inode of a specific path.
		/// 	</summary>
		/// <param name="pathToCheck">Path inode to check</param>
		/// <param name="perm">short expected permission bits</param>
		/// <exception cref="System.IO.IOException">thrown if there is an I/O error</exception>
		private void AssertPermission(Path pathToCheck, short perm)
		{
			AclTestHelpers.AssertPermission(fs, pathToCheck, perm);
		}
	}
}
