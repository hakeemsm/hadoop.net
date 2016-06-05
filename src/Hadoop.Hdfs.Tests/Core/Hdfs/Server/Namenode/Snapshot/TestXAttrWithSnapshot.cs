using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Tests interaction of XAttrs with snapshots.</summary>
	public class TestXAttrWithSnapshot
	{
		private static MiniDFSCluster cluster;

		private static Configuration conf;

		private static DistributedFileSystem hdfs;

		private static int pathCount = 0;

		private static Path path;

		private static Path snapshotPath;

		private static Path snapshotPath2;

		private static Path snapshotPath3;

		private static string snapshotName;

		private static string snapshotName2;

		private static string snapshotName3;

		private readonly int Success = 0;

		private const string name1 = "user.a1";

		private static readonly byte[] value1 = new byte[] { unchecked((int)(0x31)), unchecked(
			(int)(0x32)), unchecked((int)(0x33)) };

		private static readonly byte[] newValue1 = new byte[] { unchecked((int)(0x31)), unchecked(
			(int)(0x31)), unchecked((int)(0x31)) };

		private const string name2 = "user.a2";

		private static readonly byte[] value2 = new byte[] { unchecked((int)(0x37)), unchecked(
			(int)(0x38)), unchecked((int)(0x39)) };

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		// XAttrs
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
			InitCluster(true);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Shutdown()
		{
			IOUtils.Cleanup(null, hdfs);
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
			snapshotName2 = snapshotName + "-2";
			snapshotName3 = snapshotName + "-3";
			snapshotPath = new Path(path, new Path(".snapshot", snapshotName));
			snapshotPath2 = new Path(path, new Path(".snapshot", snapshotName2));
			snapshotPath3 = new Path(path, new Path(".snapshot", snapshotName3));
		}

		/// <summary>Tests modifying xattrs on a directory that has been snapshotted</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestModifyReadsCurrentState()
		{
			// Init
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			hdfs.SetXAttr(path, name1, value1);
			hdfs.SetXAttr(path, name2, value2);
			// Verify that current path reflects xattrs, snapshot doesn't
			IDictionary<string, byte[]> xattrs = hdfs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			xattrs = hdfs.GetXAttrs(snapshotPath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 0);
			// Modify each xattr and make sure it's reflected
			hdfs.SetXAttr(path, name1, value2, EnumSet.Of(XAttrSetFlag.Replace));
			xattrs = hdfs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value2, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			hdfs.SetXAttr(path, name2, value1, EnumSet.Of(XAttrSetFlag.Replace));
			xattrs = hdfs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value2, xattrs[name1]);
			Assert.AssertArrayEquals(value1, xattrs[name2]);
			// Paranoia checks
			xattrs = hdfs.GetXAttrs(snapshotPath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 0);
			hdfs.RemoveXAttr(path, name1);
			hdfs.RemoveXAttr(path, name2);
			xattrs = hdfs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 0);
		}

		/// <summary>Tests removing xattrs on a directory that has been snapshotted</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRemoveReadsCurrentState()
		{
			// Init
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			hdfs.SetXAttr(path, name1, value1);
			hdfs.SetXAttr(path, name2, value2);
			// Verify that current path reflects xattrs, snapshot doesn't
			IDictionary<string, byte[]> xattrs = hdfs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			xattrs = hdfs.GetXAttrs(snapshotPath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 0);
			// Remove xattrs and verify one-by-one
			hdfs.RemoveXAttr(path, name2);
			xattrs = hdfs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			hdfs.RemoveXAttr(path, name1);
			xattrs = hdfs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 0);
		}

		/// <summary>1) Save xattrs, then create snapshot.</summary>
		/// <remarks>
		/// 1) Save xattrs, then create snapshot. Assert that inode of original and
		/// snapshot have same xattrs. 2) Change the original xattrs, assert snapshot
		/// still has old xattrs.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestXAttrForSnapshotRootAfterChange()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			hdfs.SetXAttr(path, name1, value1);
			hdfs.SetXAttr(path, name2, value2);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			// Both original and snapshot have same XAttrs.
			IDictionary<string, byte[]> xattrs = hdfs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			xattrs = hdfs.GetXAttrs(snapshotPath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			// Original XAttrs have changed, but snapshot still has old XAttrs.
			hdfs.SetXAttr(path, name1, newValue1);
			DoSnapshotRootChangeAssertions(path, snapshotPath);
			Restart(false);
			DoSnapshotRootChangeAssertions(path, snapshotPath);
			Restart(true);
			DoSnapshotRootChangeAssertions(path, snapshotPath);
		}

		/// <exception cref="System.Exception"/>
		private static void DoSnapshotRootChangeAssertions(Path path, Path snapshotPath)
		{
			IDictionary<string, byte[]> xattrs = hdfs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(newValue1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			xattrs = hdfs.GetXAttrs(snapshotPath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
		}

		/// <summary>1) Save xattrs, then create snapshot.</summary>
		/// <remarks>
		/// 1) Save xattrs, then create snapshot. Assert that inode of original and
		/// snapshot have same xattrs. 2) Remove some original xattrs, assert snapshot
		/// still has old xattrs.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestXAttrForSnapshotRootAfterRemove()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			hdfs.SetXAttr(path, name1, value1);
			hdfs.SetXAttr(path, name2, value2);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			// Both original and snapshot have same XAttrs.
			IDictionary<string, byte[]> xattrs = hdfs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			xattrs = hdfs.GetXAttrs(snapshotPath);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			// Original XAttrs have been removed, but snapshot still has old XAttrs.
			hdfs.RemoveXAttr(path, name1);
			hdfs.RemoveXAttr(path, name2);
			DoSnapshotRootRemovalAssertions(path, snapshotPath);
			Restart(false);
			DoSnapshotRootRemovalAssertions(path, snapshotPath);
			Restart(true);
			DoSnapshotRootRemovalAssertions(path, snapshotPath);
		}

		/// <exception cref="System.Exception"/>
		private static void DoSnapshotRootRemovalAssertions(Path path, Path snapshotPath)
		{
			IDictionary<string, byte[]> xattrs = hdfs.GetXAttrs(path);
			NUnit.Framework.Assert.AreEqual(0, xattrs.Count);
			xattrs = hdfs.GetXAttrs(snapshotPath);
			NUnit.Framework.Assert.AreEqual(2, xattrs.Count);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
		}

		/// <summary>Test successive snapshots in between modifications of XAttrs.</summary>
		/// <remarks>
		/// Test successive snapshots in between modifications of XAttrs.
		/// Also verify that snapshot XAttrs are not altered when a
		/// snapshot is deleted.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSuccessiveSnapshotXAttrChanges()
		{
			// First snapshot
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			hdfs.SetXAttr(path, name1, value1);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			IDictionary<string, byte[]> xattrs = hdfs.GetXAttrs(snapshotPath);
			NUnit.Framework.Assert.AreEqual(1, xattrs.Count);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			// Second snapshot
			hdfs.SetXAttr(path, name1, newValue1);
			hdfs.SetXAttr(path, name2, value2);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName2);
			xattrs = hdfs.GetXAttrs(snapshotPath2);
			NUnit.Framework.Assert.AreEqual(2, xattrs.Count);
			Assert.AssertArrayEquals(newValue1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			// Third snapshot
			hdfs.SetXAttr(path, name1, value1);
			hdfs.RemoveXAttr(path, name2);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName3);
			xattrs = hdfs.GetXAttrs(snapshotPath3);
			NUnit.Framework.Assert.AreEqual(1, xattrs.Count);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			// Check that the first and second snapshots'
			// XAttrs have stayed constant
			xattrs = hdfs.GetXAttrs(snapshotPath);
			NUnit.Framework.Assert.AreEqual(1, xattrs.Count);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			xattrs = hdfs.GetXAttrs(snapshotPath2);
			NUnit.Framework.Assert.AreEqual(2, xattrs.Count);
			Assert.AssertArrayEquals(newValue1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			// Remove the second snapshot and verify the first and
			// third snapshots' XAttrs have stayed constant
			hdfs.DeleteSnapshot(path, snapshotName2);
			xattrs = hdfs.GetXAttrs(snapshotPath);
			NUnit.Framework.Assert.AreEqual(1, xattrs.Count);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			xattrs = hdfs.GetXAttrs(snapshotPath3);
			NUnit.Framework.Assert.AreEqual(1, xattrs.Count);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			hdfs.DeleteSnapshot(path, snapshotName);
			hdfs.DeleteSnapshot(path, snapshotName3);
		}

		/// <summary>Assert exception of setting xattr on read-only snapshot.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetXAttrSnapshotPath()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			exception.Expect(typeof(SnapshotAccessControlException));
			hdfs.SetXAttr(snapshotPath, name1, value1);
		}

		/// <summary>Assert exception of removing xattr on read-only snapshot.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveXAttrSnapshotPath()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			hdfs.SetXAttr(path, name1, value1);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			exception.Expect(typeof(SnapshotAccessControlException));
			hdfs.RemoveXAttr(snapshotPath, name1);
		}

		/// <summary>Test that users can copy a snapshot while preserving its xattrs.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCopySnapshotShouldPreserveXAttrs()
		{
			FileSystem.Mkdirs(hdfs, path, FsPermission.CreateImmutable((short)0x1c0));
			hdfs.SetXAttr(path, name1, value1);
			hdfs.SetXAttr(path, name2, value2);
			SnapshotTestHelper.CreateSnapshot(hdfs, path, snapshotName);
			Path snapshotCopy = new Path(path.ToString() + "-copy");
			string[] argv = new string[] { "-cp", "-px", snapshotPath.ToUri().ToString(), snapshotCopy
				.ToUri().ToString() };
			int ret = ToolRunner.Run(new FsShell(conf), argv);
			NUnit.Framework.Assert.AreEqual("cp -px is not working on a snapshot", Success, ret
				);
			IDictionary<string, byte[]> xattrs = hdfs.GetXAttrs(snapshotCopy);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
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
