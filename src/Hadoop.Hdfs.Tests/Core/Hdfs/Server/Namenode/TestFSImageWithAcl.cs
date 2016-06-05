using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestFSImageWithAcl
	{
		private static Configuration conf;

		private static MiniDFSCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetUp()
		{
			conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
		}

		[AfterClass]
		public static void TearDown()
		{
			cluster.Shutdown();
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestAcl(bool persistNamespace)
		{
			Path p = new Path("/p");
			DistributedFileSystem fs = cluster.GetFileSystem();
			fs.Create(p).Close();
			fs.Mkdirs(new Path("/23"));
			AclEntry e = new AclEntry.Builder().SetName("foo").SetPermission(FsAction.ReadExecute
				).SetScope(AclEntryScope.Access).SetType(AclEntryType.User).Build();
			fs.ModifyAclEntries(p, Lists.NewArrayList(e));
			Restart(fs, persistNamespace);
			AclStatus s = cluster.GetNamesystem().GetAclStatus(p.ToString());
			AclEntry[] returned = Sharpen.Collections.ToArray(Lists.NewArrayList(s.GetEntries
				()), new AclEntry[0]);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read) }, returned);
			fs.RemoveAcl(p);
			if (persistNamespace)
			{
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				fs.SaveNamespace();
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			}
			cluster.RestartNameNode();
			cluster.WaitActive();
			s = cluster.GetNamesystem().GetAclStatus(p.ToString());
			returned = Sharpen.Collections.ToArray(Lists.NewArrayList(s.GetEntries()), new AclEntry
				[0]);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			fs.ModifyAclEntries(p, Lists.NewArrayList(e));
			s = cluster.GetNamesystem().GetAclStatus(p.ToString());
			returned = Sharpen.Collections.ToArray(Lists.NewArrayList(s.GetEntries()), new AclEntry
				[0]);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "foo", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read) }, returned);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPersistAcl()
		{
			TestAcl(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAclEditLog()
		{
			TestAcl(false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoTestDefaultAclNewChildren(bool persistNamespace)
		{
			Path dirPath = new Path("/dir");
			Path filePath = new Path(dirPath, "file1");
			Path subdirPath = new Path(dirPath, "subdir1");
			DistributedFileSystem fs = cluster.GetFileSystem();
			fs.Mkdirs(dirPath);
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All));
			fs.SetAcl(dirPath, aclSpec);
			fs.Create(filePath).Close();
			fs.Mkdirs(subdirPath);
			AclEntry[] fileExpected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.
				Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute) };
			AclEntry[] subdirExpected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.ReadExecute) };
			AclEntry[] fileReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(filePath).GetEntries
				(), new AclEntry[0]);
			Assert.AssertArrayEquals(fileExpected, fileReturned);
			AclEntry[] subdirReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(subdirPath
				).GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(subdirExpected, subdirReturned);
			AclTestHelpers.AssertPermission(fs, subdirPath, (short)0x11ed);
			Restart(fs, persistNamespace);
			fileReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(filePath).GetEntries()
				, new AclEntry[0]);
			Assert.AssertArrayEquals(fileExpected, fileReturned);
			subdirReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(subdirPath).GetEntries
				(), new AclEntry[0]);
			Assert.AssertArrayEquals(subdirExpected, subdirReturned);
			AclTestHelpers.AssertPermission(fs, subdirPath, (short)0x11ed);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Default, AclEntryType
				.User, "foo", FsAction.ReadWrite));
			fs.ModifyAclEntries(dirPath, aclSpec);
			fileReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(filePath).GetEntries()
				, new AclEntry[0]);
			Assert.AssertArrayEquals(fileExpected, fileReturned);
			subdirReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(subdirPath).GetEntries
				(), new AclEntry[0]);
			Assert.AssertArrayEquals(subdirExpected, subdirReturned);
			AclTestHelpers.AssertPermission(fs, subdirPath, (short)0x11ed);
			Restart(fs, persistNamespace);
			fileReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(filePath).GetEntries()
				, new AclEntry[0]);
			Assert.AssertArrayEquals(fileExpected, fileReturned);
			subdirReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(subdirPath).GetEntries
				(), new AclEntry[0]);
			Assert.AssertArrayEquals(subdirExpected, subdirReturned);
			AclTestHelpers.AssertPermission(fs, subdirPath, (short)0x11ed);
			fs.RemoveAcl(dirPath);
			fileReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(filePath).GetEntries()
				, new AclEntry[0]);
			Assert.AssertArrayEquals(fileExpected, fileReturned);
			subdirReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(subdirPath).GetEntries
				(), new AclEntry[0]);
			Assert.AssertArrayEquals(subdirExpected, subdirReturned);
			AclTestHelpers.AssertPermission(fs, subdirPath, (short)0x11ed);
			Restart(fs, persistNamespace);
			fileReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(filePath).GetEntries()
				, new AclEntry[0]);
			Assert.AssertArrayEquals(fileExpected, fileReturned);
			subdirReturned = Sharpen.Collections.ToArray(fs.GetAclStatus(subdirPath).GetEntries
				(), new AclEntry[0]);
			Assert.AssertArrayEquals(subdirExpected, subdirReturned);
			AclTestHelpers.AssertPermission(fs, subdirPath, (short)0x11ed);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFsImageDefaultAclNewChildren()
		{
			DoTestDefaultAclNewChildren(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLogDefaultAclNewChildren()
		{
			DoTestDefaultAclNewChildren(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRootACLAfterLoadingFsImage()
		{
			DistributedFileSystem fs = cluster.GetFileSystem();
			Path rootdir = new Path("/");
			AclEntry e1 = new AclEntry.Builder().SetName("foo").SetPermission(FsAction.All).SetScope
				(AclEntryScope.Access).SetType(AclEntryType.Group).Build();
			AclEntry e2 = new AclEntry.Builder().SetName("bar").SetPermission(FsAction.Read).
				SetScope(AclEntryScope.Access).SetType(AclEntryType.Group).Build();
			fs.ModifyAclEntries(rootdir, Lists.NewArrayList(e1, e2));
			AclStatus s = cluster.GetNamesystem().GetAclStatus(rootdir.ToString());
			AclEntry[] returned = Sharpen.Collections.ToArray(Lists.NewArrayList(s.GetEntries
				()), new AclEntry[0]);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, "bar", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, "foo", FsAction.All) }, returned);
			// restart - hence save and load from fsimage
			Restart(fs, true);
			s = cluster.GetNamesystem().GetAclStatus(rootdir.ToString());
			returned = Sharpen.Collections.ToArray(Lists.NewArrayList(s.GetEntries()), new AclEntry
				[0]);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, "bar", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, "foo", FsAction.All) }, returned);
		}

		/// <summary>Restart the NameNode, optionally saving a new checkpoint.</summary>
		/// <param name="fs">DistributedFileSystem used for saving namespace</param>
		/// <param name="persistNamespace">boolean true to save a new checkpoint</param>
		/// <exception cref="System.IO.IOException">if restart fails</exception>
		private void Restart(DistributedFileSystem fs, bool persistNamespace)
		{
			if (persistNamespace)
			{
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				fs.SaveNamespace();
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			}
			cluster.RestartNameNode();
			cluster.WaitActive();
		}
	}
}
