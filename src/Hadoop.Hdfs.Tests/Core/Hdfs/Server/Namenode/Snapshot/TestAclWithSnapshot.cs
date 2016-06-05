using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Tests interaction of ACLs with snapshots.</summary>
	public class TestAclWithSnapshot
	{
		private static readonly UserGroupInformation Bruce = UserGroupInformation.CreateUserForTesting
			("bruce", new string[] {  });

		private static readonly UserGroupInformation Diana = UserGroupInformation.CreateUserForTesting
			("diana", new string[] {  });

		private static MiniDFSCluster cluster;

		private static Configuration conf;

		private static FileSystem fsAsBruce;

		private static FileSystem fsAsDiana;

		private static DistributedFileSystem hdfs;

		private static int pathCount = 0;

		private static Path path;

		private static Path snapshotPath;

		private static string snapshotName;

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			InitCluster(true);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Shutdown()
		{
			IOUtils.Cleanup(null, hdfs, fsAsBruce, fsAsDiana);
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			++pathCount;
			path = new Path("/p" + pathCount);
			snapshotName = "snapshot" + pathCount;
			snapshotPath = new Path(path, new Path(".snapshot", snapshotName));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOriginalAclEnforcedForSnapshotRootAfterChange()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.None), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None));
			hdfs.SetAcl(path, aclSpec);
			AssertDirPermissionGranted(fsAsBruce, Bruce, path);
			AssertDirPermissionDenied(fsAsDiana, Diana, path);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			// Both original and snapshot still have same ACL.
			AclStatus s = hdfs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) }, returned);
			AssertPermission((short)0x11e8, path);
			s = hdfs.GetAclStatus(snapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) }, returned);
			AssertPermission((short)0x11e8, snapshotPath);
			AssertDirPermissionGranted(fsAsBruce, Bruce, snapshotPath);
			AssertDirPermissionDenied(fsAsDiana, Diana, snapshotPath);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "diana", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Other, FsAction.None));
			hdfs.SetAcl(path, aclSpec);
			// Original has changed, but snapshot still has old ACL.
			DoSnapshotRootChangeAssertions(path, snapshotPath);
			Restart(false);
			DoSnapshotRootChangeAssertions(path, snapshotPath);
			Restart(true);
			DoSnapshotRootChangeAssertions(path, snapshotPath);
		}

		/// <exception cref="System.Exception"/>
		private static void DoSnapshotRootChangeAssertions(Path path, Path snapshotPath)
		{
			AclStatus s = hdfs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "diana", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) }, returned);
			AssertPermission((short)0x1168, path);
			s = hdfs.GetAclStatus(snapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) }, returned);
			AssertPermission((short)0x11e8, snapshotPath);
			AssertDirPermissionDenied(fsAsBruce, Bruce, path);
			AssertDirPermissionGranted(fsAsDiana, Diana, path);
			AssertDirPermissionGranted(fsAsBruce, Bruce, snapshotPath);
			AssertDirPermissionDenied(fsAsDiana, Diana, snapshotPath);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOriginalAclEnforcedForSnapshotContentsAfterChange()
		{
			Path filePath = new Path(path, "file1");
			Path subdirPath = new Path(path, "subdir1");
			Path fileSnapshotPath = new Path(snapshotPath, "file1");
			Path subdirSnapshotPath = new Path(snapshotPath, "subdir1");
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1ff));
			FileSystem.Create(hdfs, filePath, FsPermission.CreateImmutable((short)0x180)).Close
				();
			FileSystem.Mkdirs(hdfs, subdirPath, FsPermission.CreateImmutable((short)0x1c0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.None), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None));
			hdfs.SetAcl(filePath, aclSpec);
			hdfs.SetAcl(subdirPath, aclSpec);
			AclTestHelpers.AssertFilePermissionGranted(fsAsBruce, Bruce, filePath);
			AclTestHelpers.AssertFilePermissionDenied(fsAsDiana, Diana, filePath);
			AssertDirPermissionGranted(fsAsBruce, Bruce, subdirPath);
			AssertDirPermissionDenied(fsAsDiana, Diana, subdirPath);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			// Both original and snapshot still have same ACL.
			AclEntry[] expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) };
			AclStatus s = hdfs.GetAclStatus(filePath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, filePath);
			s = hdfs.GetAclStatus(subdirPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, subdirPath);
			s = hdfs.GetAclStatus(fileSnapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, fileSnapshotPath);
			AclTestHelpers.AssertFilePermissionGranted(fsAsBruce, Bruce, fileSnapshotPath);
			AclTestHelpers.AssertFilePermissionDenied(fsAsDiana, Diana, fileSnapshotPath);
			s = hdfs.GetAclStatus(subdirSnapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, subdirSnapshotPath);
			AssertDirPermissionGranted(fsAsBruce, Bruce, subdirSnapshotPath);
			AssertDirPermissionDenied(fsAsDiana, Diana, subdirSnapshotPath);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "diana", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Other, FsAction.None));
			hdfs.SetAcl(filePath, aclSpec);
			hdfs.SetAcl(subdirPath, aclSpec);
			// Original has changed, but snapshot still has old ACL.
			DoSnapshotContentsChangeAssertions(filePath, fileSnapshotPath, subdirPath, subdirSnapshotPath
				);
			Restart(false);
			DoSnapshotContentsChangeAssertions(filePath, fileSnapshotPath, subdirPath, subdirSnapshotPath
				);
			Restart(true);
			DoSnapshotContentsChangeAssertions(filePath, fileSnapshotPath, subdirPath, subdirSnapshotPath
				);
		}

		/// <exception cref="System.Exception"/>
		private static void DoSnapshotContentsChangeAssertions(Path filePath, Path fileSnapshotPath
			, Path subdirPath, Path subdirSnapshotPath)
		{
			AclEntry[] expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "diana", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) };
			AclStatus s = hdfs.GetAclStatus(filePath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1178, filePath);
			AclTestHelpers.AssertFilePermissionDenied(fsAsBruce, Bruce, filePath);
			AclTestHelpers.AssertFilePermissionGranted(fsAsDiana, Diana, filePath);
			s = hdfs.GetAclStatus(subdirPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1178, subdirPath);
			AssertDirPermissionDenied(fsAsBruce, Bruce, subdirPath);
			AssertDirPermissionGranted(fsAsDiana, Diana, subdirPath);
			expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.None) };
			s = hdfs.GetAclStatus(fileSnapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, fileSnapshotPath);
			AclTestHelpers.AssertFilePermissionGranted(fsAsBruce, Bruce, fileSnapshotPath);
			AclTestHelpers.AssertFilePermissionDenied(fsAsDiana, Diana, fileSnapshotPath);
			s = hdfs.GetAclStatus(subdirSnapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, subdirSnapshotPath);
			AssertDirPermissionGranted(fsAsBruce, Bruce, subdirSnapshotPath);
			AssertDirPermissionDenied(fsAsDiana, Diana, subdirSnapshotPath);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOriginalAclEnforcedForSnapshotRootAfterRemoval()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.None), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None));
			hdfs.SetAcl(path, aclSpec);
			AssertDirPermissionGranted(fsAsBruce, Bruce, path);
			AssertDirPermissionDenied(fsAsDiana, Diana, path);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			// Both original and snapshot still have same ACL.
			AclStatus s = hdfs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) }, returned);
			AssertPermission((short)0x11e8, path);
			s = hdfs.GetAclStatus(snapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) }, returned);
			AssertPermission((short)0x11e8, snapshotPath);
			AssertDirPermissionGranted(fsAsBruce, Bruce, snapshotPath);
			AssertDirPermissionDenied(fsAsDiana, Diana, snapshotPath);
			hdfs.RemoveAcl(path);
			// Original has changed, but snapshot still has old ACL.
			DoSnapshotRootRemovalAssertions(path, snapshotPath);
			Restart(false);
			DoSnapshotRootRemovalAssertions(path, snapshotPath);
			Restart(true);
			DoSnapshotRootRemovalAssertions(path, snapshotPath);
		}

		/// <exception cref="System.Exception"/>
		private static void DoSnapshotRootRemovalAssertions(Path path, Path snapshotPath)
		{
			AclStatus s = hdfs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
			AssertPermission((short)0x1c0, path);
			s = hdfs.GetAclStatus(snapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) }, returned);
			AssertPermission((short)0x11e8, snapshotPath);
			AssertDirPermissionDenied(fsAsBruce, Bruce, path);
			AssertDirPermissionDenied(fsAsDiana, Diana, path);
			AssertDirPermissionGranted(fsAsBruce, Bruce, snapshotPath);
			AssertDirPermissionDenied(fsAsDiana, Diana, snapshotPath);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOriginalAclEnforcedForSnapshotContentsAfterRemoval()
		{
			Path filePath = new Path(path, "file1");
			Path subdirPath = new Path(path, "subdir1");
			Path fileSnapshotPath = new Path(snapshotPath, "file1");
			Path subdirSnapshotPath = new Path(snapshotPath, "subdir1");
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1ff));
			FileSystem.Create(hdfs, filePath, FsPermission.CreateImmutable((short)0x180)).Close
				();
			FileSystem.Mkdirs(hdfs, subdirPath, FsPermission.CreateImmutable((short)0x1c0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.None), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.None));
			hdfs.SetAcl(filePath, aclSpec);
			hdfs.SetAcl(subdirPath, aclSpec);
			AclTestHelpers.AssertFilePermissionGranted(fsAsBruce, Bruce, filePath);
			AclTestHelpers.AssertFilePermissionDenied(fsAsDiana, Diana, filePath);
			AssertDirPermissionGranted(fsAsBruce, Bruce, subdirPath);
			AssertDirPermissionDenied(fsAsDiana, Diana, subdirPath);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			// Both original and snapshot still have same ACL.
			AclEntry[] expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) };
			AclStatus s = hdfs.GetAclStatus(filePath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, filePath);
			s = hdfs.GetAclStatus(subdirPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, subdirPath);
			s = hdfs.GetAclStatus(fileSnapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, fileSnapshotPath);
			AclTestHelpers.AssertFilePermissionGranted(fsAsBruce, Bruce, fileSnapshotPath);
			AclTestHelpers.AssertFilePermissionDenied(fsAsDiana, Diana, fileSnapshotPath);
			s = hdfs.GetAclStatus(subdirSnapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, subdirSnapshotPath);
			AssertDirPermissionGranted(fsAsBruce, Bruce, subdirSnapshotPath);
			AssertDirPermissionDenied(fsAsDiana, Diana, subdirSnapshotPath);
			hdfs.RemoveAcl(filePath);
			hdfs.RemoveAcl(subdirPath);
			// Original has changed, but snapshot still has old ACL.
			DoSnapshotContentsRemovalAssertions(filePath, fileSnapshotPath, subdirPath, subdirSnapshotPath
				);
			Restart(false);
			DoSnapshotContentsRemovalAssertions(filePath, fileSnapshotPath, subdirPath, subdirSnapshotPath
				);
			Restart(true);
			DoSnapshotContentsRemovalAssertions(filePath, fileSnapshotPath, subdirPath, subdirSnapshotPath
				);
		}

		/// <exception cref="System.Exception"/>
		private static void DoSnapshotContentsRemovalAssertions(Path filePath, Path fileSnapshotPath
			, Path subdirPath, Path subdirSnapshotPath)
		{
			AclEntry[] expected = new AclEntry[] {  };
			AclStatus s = hdfs.GetAclStatus(filePath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x140, filePath);
			AclTestHelpers.AssertFilePermissionDenied(fsAsBruce, Bruce, filePath);
			AclTestHelpers.AssertFilePermissionDenied(fsAsDiana, Diana, filePath);
			s = hdfs.GetAclStatus(subdirPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x140, subdirPath);
			AssertDirPermissionDenied(fsAsBruce, Bruce, subdirPath);
			AssertDirPermissionDenied(fsAsDiana, Diana, subdirPath);
			expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.Group, FsAction.None) };
			s = hdfs.GetAclStatus(fileSnapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, fileSnapshotPath);
			AclTestHelpers.AssertFilePermissionGranted(fsAsBruce, Bruce, fileSnapshotPath);
			AclTestHelpers.AssertFilePermissionDenied(fsAsDiana, Diana, fileSnapshotPath);
			s = hdfs.GetAclStatus(subdirSnapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1168, subdirSnapshotPath);
			AssertDirPermissionGranted(fsAsBruce, Bruce, subdirSnapshotPath);
			AssertDirPermissionDenied(fsAsDiana, Diana, subdirSnapshotPath);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyReadsCurrentState()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.All));
			hdfs.ModifyAclEntries(path, aclSpec);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "diana", FsAction.ReadExecute));
			hdfs.ModifyAclEntries(path, aclSpec);
			AclEntry[] expected = new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "diana", FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.None) };
			AclStatus s = hdfs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x11f8, path);
			AssertDirPermissionGranted(fsAsBruce, Bruce, path);
			AssertDirPermissionGranted(fsAsDiana, Diana, path);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveReadsCurrentState()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.All));
			hdfs.ModifyAclEntries(path, aclSpec);
			hdfs.RemoveAcl(path);
			AclEntry[] expected = new AclEntry[] {  };
			AclStatus s = hdfs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(expected, returned);
			AssertPermission((short)0x1c0, path);
			AssertDirPermissionDenied(fsAsBruce, Bruce, path);
			AssertDirPermissionDenied(fsAsDiana, Diana, path);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultAclNotCopiedToAccessAclOfNewSnapshot()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.ReadExecute));
			hdfs.ModifyAclEntries(path, aclSpec);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			AclStatus s = hdfs.GetAclStatus(path);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11c0, path);
			s = hdfs.GetAclStatus(snapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default
				, AclEntryType.User, "bruce", FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Group, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Mask, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.Other, FsAction.None) }, returned);
			AssertPermission((short)0x11c0, snapshotPath);
			AssertDirPermissionDenied(fsAsBruce, Bruce, snapshotPath);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestModifyAclEntriesSnapshotPath()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce", FsAction.ReadExecute));
			exception.Expect(typeof(SnapshotAccessControlException));
			hdfs.ModifyAclEntries(snapshotPath, aclSpec);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclEntriesSnapshotPath()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce"));
			exception.Expect(typeof(SnapshotAccessControlException));
			hdfs.RemoveAclEntries(snapshotPath, aclSpec);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveDefaultAclSnapshotPath()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			exception.Expect(typeof(SnapshotAccessControlException));
			hdfs.RemoveDefaultAcl(snapshotPath);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclSnapshotPath()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			exception.Expect(typeof(SnapshotAccessControlException));
			hdfs.RemoveAcl(snapshotPath);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetAclSnapshotPath()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Default, AclEntryType.User, "bruce"));
			exception.Expect(typeof(SnapshotAccessControlException));
			hdfs.SetAcl(snapshotPath, aclSpec);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChangeAclExceedsQuota()
		{
			Path filePath = new Path(path, "file1");
			Path fileSnapshotPath = new Path(snapshotPath, "file1");
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1ed));
			hdfs.AllowSnapshot(path);
			hdfs.SetQuota(path, 3, HdfsConstants.QuotaDontSet);
			FileSystem.Create(hdfs, filePath, FsPermission.CreateImmutable((short)0x180)).Close
				();
			hdfs.SetPermission(filePath, FsPermission.CreateImmutable((short)0x180));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadWrite));
			hdfs.ModifyAclEntries(filePath, aclSpec);
			hdfs.CreateSnapshot(path, snapshotName);
			AclStatus s = hdfs.GetAclStatus(filePath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) }, returned);
			AssertPermission((short)0x11b0, filePath);
			s = hdfs.GetAclStatus(fileSnapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) }, returned);
			AssertPermission((short)0x11b0, filePath);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "bruce", FsAction.Read));
			hdfs.ModifyAclEntries(filePath, aclSpec);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveAclExceedsQuota()
		{
			Path filePath = new Path(path, "file1");
			Path fileSnapshotPath = new Path(snapshotPath, "file1");
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1ed));
			hdfs.AllowSnapshot(path);
			hdfs.SetQuota(path, 3, HdfsConstants.QuotaDontSet);
			FileSystem.Create(hdfs, filePath, FsPermission.CreateImmutable((short)0x180)).Close
				();
			hdfs.SetPermission(filePath, FsPermission.CreateImmutable((short)0x180));
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "bruce", FsAction.ReadWrite));
			hdfs.ModifyAclEntries(filePath, aclSpec);
			hdfs.CreateSnapshot(path, snapshotName);
			AclStatus s = hdfs.GetAclStatus(filePath);
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) }, returned);
			AssertPermission((short)0x11b0, filePath);
			s = hdfs.GetAclStatus(fileSnapshotPath);
			returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]);
			Assert.AssertArrayEquals(new AclEntry[] { AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, "bruce", FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.None) }, returned);
			AssertPermission((short)0x11b0, filePath);
			aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, "bruce", FsAction.Read));
			hdfs.RemoveAcl(filePath);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAclStatusDotSnapshotPath()
		{
			hdfs.Mkdirs(path);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			AclStatus s = hdfs.GetAclStatus(new Path(path, ".snapshot"));
			AclEntry[] returned = Sharpen.Collections.ToArray(s.GetEntries(), new AclEntry[0]
				);
			Assert.AssertArrayEquals(new AclEntry[] {  }, returned);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeDuplication()
		{
			int startSize = AclStorage.GetUniqueAclFeatures().GetUniqueElementsSize();
			// unique default AclEntries for this test
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "testdeduplicateuser", FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, "testdeduplicategroup", FsAction.All)
				);
			hdfs.Mkdirs(path);
			hdfs.ModifyAclEntries(path, aclSpec);
			NUnit.Framework.Assert.AreEqual("One more ACL feature should be unique", startSize
				 + 1, AclStorage.GetUniqueAclFeatures().GetUniqueElementsSize());
			Path subdir = new Path(path, "sub-dir");
			hdfs.Mkdirs(subdir);
			Path file = new Path(path, "file");
			hdfs.Create(file).Close();
			AclFeature aclFeature;
			{
				// create the snapshot with root directory having ACLs should refer to
				// same ACLFeature without incrementing the reference count
				aclFeature = FSAclBaseTest.GetAclFeature(path, cluster);
				NUnit.Framework.Assert.AreEqual("Reference count should be one before snapshot", 
					1, aclFeature.GetRefCount());
				Path snapshotPath = SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
				AclFeature snapshotAclFeature = FSAclBaseTest.GetAclFeature(snapshotPath, cluster
					);
				NUnit.Framework.Assert.AreSame(aclFeature, snapshotAclFeature);
				NUnit.Framework.Assert.AreEqual("Reference count should be increased", 2, snapshotAclFeature
					.GetRefCount());
			}
			{
				// deleting the snapshot with root directory having ACLs should not alter
				// the reference count of the ACLFeature
				DeleteSnapshotWithAclAndVerify(aclFeature, path, startSize);
			}
			{
				hdfs.ModifyAclEntries(subdir, aclSpec);
				aclFeature = FSAclBaseTest.GetAclFeature(subdir, cluster);
				NUnit.Framework.Assert.AreEqual("Reference count should be 1", 1, aclFeature.GetRefCount
					());
				Path snapshotPath = SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
				Path subdirInSnapshot = new Path(snapshotPath, "sub-dir");
				AclFeature snapshotAcl = FSAclBaseTest.GetAclFeature(subdirInSnapshot, cluster);
				NUnit.Framework.Assert.AreSame(aclFeature, snapshotAcl);
				NUnit.Framework.Assert.AreEqual("Reference count should remain same", 1, aclFeature
					.GetRefCount());
				// Delete the snapshot with sub-directory containing the ACLs should not
				// alter the reference count for AclFeature
				DeleteSnapshotWithAclAndVerify(aclFeature, subdir, startSize);
			}
			{
				hdfs.ModifyAclEntries(file, aclSpec);
				aclFeature = FSAclBaseTest.GetAclFeature(file, cluster);
				NUnit.Framework.Assert.AreEqual("Reference count should be 1", 1, aclFeature.GetRefCount
					());
				Path snapshotPath = SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
				Path fileInSnapshot = new Path(snapshotPath, file.GetName());
				AclFeature snapshotAcl = FSAclBaseTest.GetAclFeature(fileInSnapshot, cluster);
				NUnit.Framework.Assert.AreSame(aclFeature, snapshotAcl);
				NUnit.Framework.Assert.AreEqual("Reference count should remain same", 1, aclFeature
					.GetRefCount());
				// Delete the snapshot with contained file having ACLs should not
				// alter the reference count for AclFeature
				DeleteSnapshotWithAclAndVerify(aclFeature, file, startSize);
			}
			{
				// Modifying the ACLs of root directory of the snapshot should refer new
				// AclFeature. And old AclFeature should be referenced by snapshot
				hdfs.ModifyAclEntries(path, aclSpec);
				Path snapshotPath = SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
				AclFeature snapshotAcl = FSAclBaseTest.GetAclFeature(snapshotPath, cluster);
				aclFeature = FSAclBaseTest.GetAclFeature(path, cluster);
				NUnit.Framework.Assert.AreEqual("Before modification same ACL should be referenced twice"
					, 2, aclFeature.GetRefCount());
				IList<AclEntry> newAcl = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.User, "testNewUser", FsAction.All));
				hdfs.ModifyAclEntries(path, newAcl);
				aclFeature = FSAclBaseTest.GetAclFeature(path, cluster);
				AclFeature snapshotAclPostModification = FSAclBaseTest.GetAclFeature(snapshotPath
					, cluster);
				NUnit.Framework.Assert.AreSame(snapshotAcl, snapshotAclPostModification);
				NUnit.Framework.Assert.AreNotSame(aclFeature, snapshotAclPostModification);
				NUnit.Framework.Assert.AreEqual("Old ACL feature reference count should be same", 
					1, snapshotAcl.GetRefCount());
				NUnit.Framework.Assert.AreEqual("New ACL feature reference should be used", 1, aclFeature
					.GetRefCount());
				DeleteSnapshotWithAclAndVerify(aclFeature, path, startSize);
			}
			{
				// Modifying the ACLs of sub directory of the snapshot root should refer
				// new AclFeature. And old AclFeature should be referenced by snapshot
				hdfs.ModifyAclEntries(subdir, aclSpec);
				Path snapshotPath = SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
				Path subdirInSnapshot = new Path(snapshotPath, "sub-dir");
				AclFeature snapshotAclFeature = FSAclBaseTest.GetAclFeature(subdirInSnapshot, cluster
					);
				IList<AclEntry> newAcl = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.User, "testNewUser", FsAction.All));
				hdfs.ModifyAclEntries(subdir, newAcl);
				aclFeature = FSAclBaseTest.GetAclFeature(subdir, cluster);
				NUnit.Framework.Assert.AreNotSame(aclFeature, snapshotAclFeature);
				NUnit.Framework.Assert.AreEqual("Reference count should remain same", 1, snapshotAclFeature
					.GetRefCount());
				NUnit.Framework.Assert.AreEqual("New AclFeature should be used", 1, aclFeature.GetRefCount
					());
				DeleteSnapshotWithAclAndVerify(aclFeature, subdir, startSize);
			}
			{
				// Modifying the ACLs of file inside the snapshot root should refer new
				// AclFeature. And old AclFeature should be referenced by snapshot
				hdfs.ModifyAclEntries(file, aclSpec);
				Path snapshotPath = SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
				Path fileInSnapshot = new Path(snapshotPath, file.GetName());
				AclFeature snapshotAclFeature = FSAclBaseTest.GetAclFeature(fileInSnapshot, cluster
					);
				IList<AclEntry> newAcl = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.User, "testNewUser", FsAction.All));
				hdfs.ModifyAclEntries(file, newAcl);
				aclFeature = FSAclBaseTest.GetAclFeature(file, cluster);
				NUnit.Framework.Assert.AreNotSame(aclFeature, snapshotAclFeature);
				NUnit.Framework.Assert.AreEqual("Reference count should remain same", 1, snapshotAclFeature
					.GetRefCount());
				DeleteSnapshotWithAclAndVerify(aclFeature, file, startSize);
			}
			{
				// deleting the original directory containing dirs and files with ACLs
				// with snapshot
				hdfs.Delete(path, true);
				Path dir = new Path(subdir, "dir");
				hdfs.Mkdirs(dir);
				hdfs.ModifyAclEntries(dir, aclSpec);
				file = new Path(subdir, "file");
				hdfs.Create(file).Close();
				aclSpec.AddItem(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType.User, 
					"testNewUser", FsAction.All));
				hdfs.ModifyAclEntries(file, aclSpec);
				AclFeature fileAcl = FSAclBaseTest.GetAclFeature(file, cluster);
				AclFeature dirAcl = FSAclBaseTest.GetAclFeature(dir, cluster);
				Path snapshotPath = SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
				Path dirInSnapshot = new Path(snapshotPath, "sub-dir/dir");
				AclFeature snapshotDirAclFeature = FSAclBaseTest.GetAclFeature(dirInSnapshot, cluster
					);
				Path fileInSnapshot = new Path(snapshotPath, "sub-dir/file");
				AclFeature snapshotFileAclFeature = FSAclBaseTest.GetAclFeature(fileInSnapshot, cluster
					);
				NUnit.Framework.Assert.AreSame(fileAcl, snapshotFileAclFeature);
				NUnit.Framework.Assert.AreSame(dirAcl, snapshotDirAclFeature);
				hdfs.Delete(subdir, true);
				NUnit.Framework.Assert.AreEqual("Original ACLs references should be maintained for snapshot"
					, 1, snapshotFileAclFeature.GetRefCount());
				NUnit.Framework.Assert.AreEqual("Original ACLs references should be maintained for snapshot"
					, 1, snapshotDirAclFeature.GetRefCount());
				hdfs.DeleteSnapshot(path, snapshotName);
				NUnit.Framework.Assert.AreEqual("ACLs should be deleted from snapshot", startSize
					, AclStorage.GetUniqueAclFeatures().GetUniqueElementsSize());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DeleteSnapshotWithAclAndVerify(AclFeature aclFeature, Path pathToCheckAcl
			, int totalAclFeatures)
		{
			hdfs.DeleteSnapshot(path, snapshotName);
			AclFeature afterDeleteAclFeature = FSAclBaseTest.GetAclFeature(pathToCheckAcl, cluster
				);
			NUnit.Framework.Assert.AreSame(aclFeature, afterDeleteAclFeature);
			NUnit.Framework.Assert.AreEqual("Reference count should remain same" + " even after deletion of snapshot"
				, 1, afterDeleteAclFeature.GetRefCount());
			hdfs.RemoveAcl(pathToCheckAcl);
			NUnit.Framework.Assert.AreEqual("Reference count should be 0", 0, aclFeature.GetRefCount
				());
			NUnit.Framework.Assert.AreEqual("Unique ACL features should remain same", totalAclFeatures
				, AclStorage.GetUniqueAclFeatures().GetUniqueElementsSize());
		}

		/// <summary>
		/// Asserts that permission is denied to the given fs/user for the given
		/// directory.
		/// </summary>
		/// <param name="fs">FileSystem to check</param>
		/// <param name="user">UserGroupInformation owner of fs</param>
		/// <param name="pathToCheck">Path directory to check</param>
		/// <exception cref="System.Exception">if there is an unexpected error</exception>
		private static void AssertDirPermissionDenied(FileSystem fs, UserGroupInformation
			 user, Path pathToCheck)
		{
			try
			{
				fs.ListStatus(pathToCheck);
				NUnit.Framework.Assert.Fail("expected AccessControlException for user " + user + 
					", path = " + pathToCheck);
			}
			catch (AccessControlException)
			{
			}
			// expected
			try
			{
				fs.Access(pathToCheck, FsAction.Read);
				NUnit.Framework.Assert.Fail("The access call should have failed for " + pathToCheck
					);
			}
			catch (AccessControlException)
			{
			}
		}

		// expected
		/// <summary>
		/// Asserts that permission is granted to the given fs/user for the given
		/// directory.
		/// </summary>
		/// <param name="fs">FileSystem to check</param>
		/// <param name="user">UserGroupInformation owner of fs</param>
		/// <param name="pathToCheck">Path directory to check</param>
		/// <exception cref="System.Exception">if there is an unexpected error</exception>
		private static void AssertDirPermissionGranted(FileSystem fs, UserGroupInformation
			 user, Path pathToCheck)
		{
			try
			{
				fs.ListStatus(pathToCheck);
				fs.Access(pathToCheck, FsAction.Read);
			}
			catch (AccessControlException)
			{
				NUnit.Framework.Assert.Fail("expected permission granted for user " + user + ", path = "
					 + pathToCheck);
			}
		}

		/// <summary>Asserts the value of the FsPermission bits on the inode of the test path.
		/// 	</summary>
		/// <param name="perm">short expected permission bits</param>
		/// <param name="pathToCheck">Path to check</param>
		/// <exception cref="System.Exception">thrown if there is an unexpected error</exception>
		private static void AssertPermission(short perm, Path pathToCheck)
		{
			AclTestHelpers.AssertPermission(hdfs, pathToCheck, perm);
		}

		/// <summary>
		/// Initialize the cluster, wait for it to become active, and get FileSystem
		/// instances for our test users.
		/// </summary>
		/// <param name="format">if true, format the NameNode and DataNodes before starting up
		/// 	</param>
		/// <exception cref="System.Exception">if any step fails</exception>
		private static void InitCluster(bool format)
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(format).Build();
			cluster.WaitActive();
			hdfs = cluster.GetFileSystem();
			fsAsBruce = DFSTestUtil.GetFileSystemAs(Bruce, conf);
			fsAsDiana = DFSTestUtil.GetFileSystemAs(Diana, conf);
		}

		/// <summary>Restart the cluster, optionally saving a new checkpoint.</summary>
		/// <param name="checkpoint">boolean true to save a new checkpoint</param>
		/// <exception cref="System.Exception">if restart fails</exception>
		private static void Restart(bool checkpoint)
		{
			NameNode nameNode = cluster.GetNameNode();
			if (checkpoint)
			{
				NameNodeAdapter.EnterSafeMode(nameNode, false);
				NameNodeAdapter.SaveNamespace(nameNode);
			}
			Shutdown();
			InitCluster(false);
		}
	}
}
