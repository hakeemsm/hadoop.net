using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Test snapshot related operations.</summary>
	public class TestSnapshotPathINodes
	{
		private const long seed = 0;

		private const short Replication = 3;

		private static readonly Path dir = new Path("/TestSnapshot");

		private static readonly Path sub1 = new Path(dir, "sub1");

		private static readonly Path file1 = new Path(sub1, "file1");

		private static readonly Path file2 = new Path(sub1, "file2");

		private static MiniDFSCluster cluster;

		private static FSDirectory fsdir;

		private static DistributedFileSystem hdfs;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			Configuration conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			FSNamesystem fsn = cluster.GetNamesystem();
			fsdir = fsn.GetFSDirectory();
			hdfs = cluster.GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void Reset()
		{
			DFSTestUtil.CreateFile(hdfs, file1, 1024, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file2, 1024, Replication, seed);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test allow-snapshot operation.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAllowSnapshot()
		{
			string pathStr = sub1.ToString();
			INode before = fsdir.GetINode(pathStr);
			// Before a directory is snapshottable
			NUnit.Framework.Assert.IsFalse(before.AsDirectory().IsSnapshottable());
			// After a directory is snapshottable
			Path path = new Path(pathStr);
			hdfs.AllowSnapshot(path);
			{
				INode after = fsdir.GetINode(pathStr);
				NUnit.Framework.Assert.IsTrue(after.AsDirectory().IsSnapshottable());
			}
			hdfs.DisallowSnapshot(path);
			{
				INode after = fsdir.GetINode(pathStr);
				NUnit.Framework.Assert.IsFalse(after.AsDirectory().IsSnapshottable());
			}
		}

		internal static Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot GetSnapshot
			(INodesInPath inodesInPath, string name, int index)
		{
			if (name == null)
			{
				return null;
			}
			INode inode = inodesInPath.GetINode(index - 1);
			return inode.AsDirectory().GetSnapshot(DFSUtil.String2Bytes(name));
		}

		internal static void AssertSnapshot(INodesInPath inodesInPath, bool isSnapshot, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			 snapshot, int index)
		{
			NUnit.Framework.Assert.AreEqual(isSnapshot, inodesInPath.IsSnapshot());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotId(isSnapshot ? snapshot : null), inodesInPath.GetPathSnapshotId());
			if (!isSnapshot)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.GetSnapshotId(snapshot), inodesInPath.GetLatestSnapshotId());
			}
			if (isSnapshot && index >= 0)
			{
				NUnit.Framework.Assert.AreEqual(typeof(Snapshot.Root), inodesInPath.GetINode(index
					).GetType());
			}
		}

		internal static void AssertINodeFile(INode inode, Path path)
		{
			NUnit.Framework.Assert.AreEqual(path.GetName(), inode.GetLocalName());
			NUnit.Framework.Assert.AreEqual(typeof(INodeFile), inode.GetType());
		}

		/// <summary>for normal (non-snapshot) file.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestNonSnapshotPathINodes()
		{
			// Get the inodes by resolving the path of a normal file
			string[] names = INode.GetPathNames(file1.ToString());
			byte[][] components = INode.GetPathComponents(names);
			INodesInPath nodesInPath = INodesInPath.Resolve(fsdir.rootDir, components, false);
			// The number of inodes should be equal to components.length
			NUnit.Framework.Assert.AreEqual(nodesInPath.Length(), components.Length);
			// The returned nodesInPath should be non-snapshot
			AssertSnapshot(nodesInPath, false, null, -1);
			// The last INode should be associated with file1
			NUnit.Framework.Assert.IsTrue("file1=" + file1 + ", nodesInPath=" + nodesInPath, 
				nodesInPath.GetINode(components.Length - 1) != null);
			NUnit.Framework.Assert.AreEqual(nodesInPath.GetINode(components.Length - 1).GetFullPathName
				(), file1.ToString());
			NUnit.Framework.Assert.AreEqual(nodesInPath.GetINode(components.Length - 2).GetFullPathName
				(), sub1.ToString());
			NUnit.Framework.Assert.AreEqual(nodesInPath.GetINode(components.Length - 3).GetFullPathName
				(), dir.ToString());
			nodesInPath = INodesInPath.Resolve(fsdir.rootDir, components, false);
			NUnit.Framework.Assert.AreEqual(nodesInPath.Length(), components.Length);
			AssertSnapshot(nodesInPath, false, null, -1);
			NUnit.Framework.Assert.AreEqual(nodesInPath.GetLastINode().GetFullPathName(), file1
				.ToString());
		}

		/// <summary>for snapshot file.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotPathINodes()
		{
			// Create a snapshot for the dir, and check the inodes for the path
			// pointing to a snapshot file
			hdfs.AllowSnapshot(sub1);
			hdfs.CreateSnapshot(sub1, "s1");
			// The path when accessing the snapshot file of file1 is
			// /TestSnapshot/sub1/.snapshot/s1/file1
			string snapshotPath = sub1.ToString() + "/.snapshot/s1/file1";
			string[] names = INode.GetPathNames(snapshotPath);
			byte[][] components = INode.GetPathComponents(names);
			INodesInPath nodesInPath = INodesInPath.Resolve(fsdir.rootDir, components, false);
			// Length of inodes should be (components.length - 1), since we will ignore
			// ".snapshot" 
			NUnit.Framework.Assert.AreEqual(nodesInPath.Length(), components.Length - 1);
			// SnapshotRootIndex should be 3: {root, Testsnapshot, sub1, s1, file1}
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot = GetSnapshot(nodesInPath
				, "s1", 3);
			AssertSnapshot(nodesInPath, true, snapshot, 3);
			// Check the INode for file1 (snapshot file)
			INode snapshotFileNode = nodesInPath.GetLastINode();
			AssertINodeFile(snapshotFileNode, file1);
			NUnit.Framework.Assert.IsTrue(snapshotFileNode.GetParent().IsWithSnapshot());
			// Call getExistingPathINodes and request only one INode.
			nodesInPath = INodesInPath.Resolve(fsdir.rootDir, components, false);
			NUnit.Framework.Assert.AreEqual(nodesInPath.Length(), components.Length - 1);
			AssertSnapshot(nodesInPath, true, snapshot, 3);
			// Check the INode for file1 (snapshot file)
			AssertINodeFile(nodesInPath.GetLastINode(), file1);
			// Resolve the path "/TestSnapshot/sub1/.snapshot"  
			string dotSnapshotPath = sub1.ToString() + "/.snapshot";
			names = INode.GetPathNames(dotSnapshotPath);
			components = INode.GetPathComponents(names);
			nodesInPath = INodesInPath.Resolve(fsdir.rootDir, components, false);
			// The number of INodes returned should still be components.length
			// since we put a null in the inode array for ".snapshot"
			NUnit.Framework.Assert.AreEqual(nodesInPath.Length(), components.Length);
			// No SnapshotRoot dir is included in the resolved inodes  
			AssertSnapshot(nodesInPath, true, snapshot, -1);
			// The last INode should be null, the last but 1 should be sub1
			NUnit.Framework.Assert.IsNull(nodesInPath.GetLastINode());
			NUnit.Framework.Assert.AreEqual(nodesInPath.GetINode(-2).GetFullPathName(), sub1.
				ToString());
			NUnit.Framework.Assert.IsTrue(nodesInPath.GetINode(-2).IsDirectory());
			string[] invalidPathComponent = new string[] { "invalidDir", "foo", ".snapshot", 
				"bar" };
			Path invalidPath = new Path(invalidPathComponent[0]);
			for (int i = 1; i < invalidPathComponent.Length; i++)
			{
				invalidPath = new Path(invalidPath, invalidPathComponent[i]);
				try
				{
					hdfs.GetFileStatus(invalidPath);
					NUnit.Framework.Assert.Fail();
				}
				catch (FileNotFoundException fnfe)
				{
					System.Console.Out.WriteLine("The exception is expected: " + fnfe);
				}
			}
			hdfs.DeleteSnapshot(sub1, "s1");
			hdfs.DisallowSnapshot(sub1);
		}

		/// <summary>for snapshot file after deleting the original file.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotPathINodesAfterDeletion()
		{
			// Create a snapshot for the dir, and check the inodes for the path
			// pointing to a snapshot file
			hdfs.AllowSnapshot(sub1);
			hdfs.CreateSnapshot(sub1, "s2");
			// Delete the original file /TestSnapshot/sub1/file1
			hdfs.Delete(file1, false);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot;
			{
				// Resolve the path for the snapshot file
				// /TestSnapshot/sub1/.snapshot/s2/file1
				string snapshotPath = sub1.ToString() + "/.snapshot/s2/file1";
				string[] names = INode.GetPathNames(snapshotPath);
				byte[][] components = INode.GetPathComponents(names);
				INodesInPath nodesInPath = INodesInPath.Resolve(fsdir.rootDir, components, false);
				// Length of inodes should be (components.length - 1), since we will ignore
				// ".snapshot" 
				NUnit.Framework.Assert.AreEqual(nodesInPath.Length(), components.Length - 1);
				// SnapshotRootIndex should be 3: {root, Testsnapshot, sub1, s2, file1}
				snapshot = GetSnapshot(nodesInPath, "s2", 3);
				AssertSnapshot(nodesInPath, true, snapshot, 3);
				// Check the INode for file1 (snapshot file)
				INode inode = nodesInPath.GetLastINode();
				NUnit.Framework.Assert.AreEqual(file1.GetName(), inode.GetLocalName());
				NUnit.Framework.Assert.IsTrue(inode.AsFile().IsWithSnapshot());
			}
			// Check the INodes for path /TestSnapshot/sub1/file1
			string[] names_1 = INode.GetPathNames(file1.ToString());
			byte[][] components_1 = INode.GetPathComponents(names_1);
			INodesInPath nodesInPath_1 = INodesInPath.Resolve(fsdir.rootDir, components_1, false
				);
			// The length of inodes should be equal to components.length
			NUnit.Framework.Assert.AreEqual(nodesInPath_1.Length(), components_1.Length);
			// The number of non-null elements should be components.length - 1 since
			// file1 has been deleted
			NUnit.Framework.Assert.AreEqual(GetNumNonNull(nodesInPath_1), components_1.Length
				 - 1);
			// The returned nodesInPath should be non-snapshot
			AssertSnapshot(nodesInPath_1, false, snapshot, -1);
			// The last INode should be null, and the one before should be associated
			// with sub1
			NUnit.Framework.Assert.IsNull(nodesInPath_1.GetINode(components_1.Length - 1));
			NUnit.Framework.Assert.AreEqual(nodesInPath_1.GetINode(components_1.Length - 2).GetFullPathName
				(), sub1.ToString());
			NUnit.Framework.Assert.AreEqual(nodesInPath_1.GetINode(components_1.Length - 3).GetFullPathName
				(), dir.ToString());
			hdfs.DeleteSnapshot(sub1, "s2");
			hdfs.DisallowSnapshot(sub1);
		}

		private int GetNumNonNull(INodesInPath iip)
		{
			IList<INode> inodes = iip.GetReadOnlyINodes();
			for (int i = inodes.Count - 1; i >= 0; i--)
			{
				if (inodes[i] != null)
				{
					return i + 1;
				}
			}
			return 0;
		}

		/// <summary>for snapshot file while adding a new file after snapshot.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotPathINodesWithAddedFile()
		{
			// Create a snapshot for the dir, and check the inodes for the path
			// pointing to a snapshot file
			hdfs.AllowSnapshot(sub1);
			hdfs.CreateSnapshot(sub1, "s4");
			// Add a new file /TestSnapshot/sub1/file3
			Path file3 = new Path(sub1, "file3");
			DFSTestUtil.CreateFile(hdfs, file3, 1024, Replication, seed);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s4;
			{
				// Check the inodes for /TestSnapshot/sub1/.snapshot/s4/file3
				string snapshotPath = sub1.ToString() + "/.snapshot/s4/file3";
				string[] names = INode.GetPathNames(snapshotPath);
				byte[][] components = INode.GetPathComponents(names);
				INodesInPath nodesInPath = INodesInPath.Resolve(fsdir.rootDir, components, false);
				// Length of inodes should be (components.length - 1), since we will ignore
				// ".snapshot" 
				NUnit.Framework.Assert.AreEqual(nodesInPath.Length(), components.Length - 1);
				// The number of non-null inodes should be components.length - 2, since
				// snapshot of file3 does not exist
				NUnit.Framework.Assert.AreEqual(GetNumNonNull(nodesInPath), components.Length - 2
					);
				s4 = GetSnapshot(nodesInPath, "s4", 3);
				// SnapshotRootIndex should still be 3: {root, Testsnapshot, sub1, s4, null}
				AssertSnapshot(nodesInPath, true, s4, 3);
				// Check the last INode in inodes, which should be null
				NUnit.Framework.Assert.IsNull(nodesInPath.GetINode(nodesInPath.Length() - 1));
			}
			// Check the inodes for /TestSnapshot/sub1/file3
			string[] names_1 = INode.GetPathNames(file3.ToString());
			byte[][] components_1 = INode.GetPathComponents(names_1);
			INodesInPath nodesInPath_1 = INodesInPath.Resolve(fsdir.rootDir, components_1, false
				);
			// The number of inodes should be equal to components.length
			NUnit.Framework.Assert.AreEqual(nodesInPath_1.Length(), components_1.Length);
			// The returned nodesInPath should be non-snapshot
			AssertSnapshot(nodesInPath_1, false, s4, -1);
			// The last INode should be associated with file3
			NUnit.Framework.Assert.AreEqual(nodesInPath_1.GetINode(components_1.Length - 1).GetFullPathName
				(), file3.ToString());
			NUnit.Framework.Assert.AreEqual(nodesInPath_1.GetINode(components_1.Length - 2).GetFullPathName
				(), sub1.ToString());
			NUnit.Framework.Assert.AreEqual(nodesInPath_1.GetINode(components_1.Length - 3).GetFullPathName
				(), dir.ToString());
			hdfs.DeleteSnapshot(sub1, "s4");
			hdfs.DisallowSnapshot(sub1);
		}

		/// <summary>for snapshot file while modifying file after snapshot.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotPathINodesAfterModification()
		{
			// First check the INode for /TestSnapshot/sub1/file1
			string[] names = INode.GetPathNames(file1.ToString());
			byte[][] components = INode.GetPathComponents(names);
			INodesInPath nodesInPath = INodesInPath.Resolve(fsdir.rootDir, components, false);
			// The number of inodes should be equal to components.length
			NUnit.Framework.Assert.AreEqual(nodesInPath.Length(), components.Length);
			// The last INode should be associated with file1
			NUnit.Framework.Assert.AreEqual(nodesInPath.GetINode(components.Length - 1).GetFullPathName
				(), file1.ToString());
			// record the modification time of the inode
			long modTime = nodesInPath.GetINode(nodesInPath.Length() - 1).GetModificationTime
				();
			// Create a snapshot for the dir, and check the inodes for the path
			// pointing to a snapshot file
			hdfs.AllowSnapshot(sub1);
			hdfs.CreateSnapshot(sub1, "s3");
			// Modify file1
			DFSTestUtil.AppendFile(hdfs, file1, "the content for appending");
			// Check the INodes for snapshot of file1
			string snapshotPath = sub1.ToString() + "/.snapshot/s3/file1";
			names = INode.GetPathNames(snapshotPath);
			components = INode.GetPathComponents(names);
			INodesInPath ssNodesInPath = INodesInPath.Resolve(fsdir.rootDir, components, false
				);
			// Length of ssInodes should be (components.length - 1), since we will
			// ignore ".snapshot" 
			NUnit.Framework.Assert.AreEqual(ssNodesInPath.Length(), components.Length - 1);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s3 = GetSnapshot(ssNodesInPath
				, "s3", 3);
			AssertSnapshot(ssNodesInPath, true, s3, 3);
			// Check the INode for snapshot of file1
			INode snapshotFileNode = ssNodesInPath.GetLastINode();
			NUnit.Framework.Assert.AreEqual(snapshotFileNode.GetLocalName(), file1.GetName());
			NUnit.Framework.Assert.IsTrue(snapshotFileNode.AsFile().IsWithSnapshot());
			// The modification time of the snapshot INode should be the same with the
			// original INode before modification
			NUnit.Framework.Assert.AreEqual(modTime, snapshotFileNode.GetModificationTime(ssNodesInPath
				.GetPathSnapshotId()));
			// Check the INode for /TestSnapshot/sub1/file1 again
			names = INode.GetPathNames(file1.ToString());
			components = INode.GetPathComponents(names);
			INodesInPath newNodesInPath = INodesInPath.Resolve(fsdir.rootDir, components, false
				);
			AssertSnapshot(newNodesInPath, false, s3, -1);
			// The number of inodes should be equal to components.length
			NUnit.Framework.Assert.AreEqual(newNodesInPath.Length(), components.Length);
			// The last INode should be associated with file1
			int last = components.Length - 1;
			NUnit.Framework.Assert.AreEqual(newNodesInPath.GetINode(last).GetFullPathName(), 
				file1.ToString());
			// The modification time of the INode for file3 should have been changed
			NUnit.Framework.Assert.IsFalse(modTime == newNodesInPath.GetINode(last).GetModificationTime
				());
			hdfs.DeleteSnapshot(sub1, "s3");
			hdfs.DisallowSnapshot(sub1);
		}
	}
}
