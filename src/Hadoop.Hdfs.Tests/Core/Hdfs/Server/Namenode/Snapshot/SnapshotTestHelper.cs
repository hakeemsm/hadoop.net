using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Helper for writing snapshot related tests</summary>
	public class SnapshotTestHelper
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.SnapshotTestHelper
			));

		/// <summary>Disable the logs that are not very useful for snapshot related tests.</summary>
		public static void DisableLogs()
		{
			string[] lognames = new string[] { "org.apache.hadoop.hdfs.server.datanode.BlockPoolSliceScanner"
				, "org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl", "org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetAsyncDiskService"
				 };
			foreach (string n in lognames)
			{
				GenericTestUtils.DisableLog(LogFactory.GetLog(n));
			}
			GenericTestUtils.DisableLog(LogFactory.GetLog(typeof(UserGroupInformation)));
			GenericTestUtils.DisableLog(LogFactory.GetLog(typeof(BlockManager)));
			GenericTestUtils.DisableLog(LogFactory.GetLog(typeof(FSNamesystem)));
			GenericTestUtils.DisableLog(LogFactory.GetLog(typeof(DirectoryScanner)));
			GenericTestUtils.DisableLog(LogFactory.GetLog(typeof(MetricsSystemImpl)));
			GenericTestUtils.DisableLog(BlockScanner.Log);
			GenericTestUtils.DisableLog(HttpServer2.Log);
			GenericTestUtils.DisableLog(DataNode.Log);
			GenericTestUtils.DisableLog(BlockPoolSliceStorage.Log);
			GenericTestUtils.DisableLog(LeaseManager.Log);
			GenericTestUtils.DisableLog(NameNode.stateChangeLog);
			GenericTestUtils.DisableLog(NameNode.blockStateChangeLog);
			GenericTestUtils.DisableLog(DFSClient.Log);
			GenericTestUtils.DisableLog(ProtobufRpcEngine.Server.Log);
		}

		private SnapshotTestHelper()
		{
		}

		// Cannot be instantinatied
		public static Path GetSnapshotRoot(Path snapshottedDir, string snapshotName)
		{
			return new Path(snapshottedDir, HdfsConstants.DotSnapshotDir + "/" + snapshotName
				);
		}

		public static Path GetSnapshotPath(Path snapshottedDir, string snapshotName, string
			 fileLocalName)
		{
			return new Path(GetSnapshotRoot(snapshottedDir, snapshotName), fileLocalName);
		}

		/// <summary>Create snapshot for a dir using a given snapshot name</summary>
		/// <param name="hdfs">DistributedFileSystem instance</param>
		/// <param name="snapshotRoot">The dir to be snapshotted</param>
		/// <param name="snapshotName">The name of the snapshot</param>
		/// <returns>The path of the snapshot root</returns>
		/// <exception cref="System.Exception"/>
		public static Path CreateSnapshot(DistributedFileSystem hdfs, Path snapshotRoot, 
			string snapshotName)
		{
			Log.Info("createSnapshot " + snapshotName + " for " + snapshotRoot);
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(snapshotRoot));
			hdfs.AllowSnapshot(snapshotRoot);
			hdfs.CreateSnapshot(snapshotRoot, snapshotName);
			// set quota to a large value for testing counts
			hdfs.SetQuota(snapshotRoot, long.MaxValue - 1, long.MaxValue - 1);
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.SnapshotTestHelper.GetSnapshotRoot
				(snapshotRoot, snapshotName);
		}

		/// <summary>Check the functionality of a snapshot.</summary>
		/// <param name="hdfs">DistributedFileSystem instance</param>
		/// <param name="snapshotRoot">The root of the snapshot</param>
		/// <param name="snapshottedDir">The snapshotted directory</param>
		/// <exception cref="System.Exception"/>
		public static void CheckSnapshotCreation(DistributedFileSystem hdfs, Path snapshotRoot
			, Path snapshottedDir)
		{
			// Currently we only check if the snapshot was created successfully
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(snapshotRoot));
			// Compare the snapshot with the current dir
			FileStatus[] currentFiles = hdfs.ListStatus(snapshottedDir);
			FileStatus[] snapshotFiles = hdfs.ListStatus(snapshotRoot);
			NUnit.Framework.Assert.AreEqual("snapshottedDir=" + snapshottedDir + ", snapshotRoot="
				 + snapshotRoot, currentFiles.Length, snapshotFiles.Length);
		}

		/// <summary>Compare two dumped trees that are stored in two files.</summary>
		/// <remarks>
		/// Compare two dumped trees that are stored in two files. The following is an
		/// example of the dumped tree:
		/// <pre>
		/// information of root
		/// +- the first child of root (e.g., /foo)
		/// +- the first child of /foo
		/// ...
		/// \- the last child of /foo (e.g., /foo/bar)
		/// +- the first child of /foo/bar
		/// ...
		/// snapshots of /foo
		/// +- snapshot s_1
		/// ...
		/// \- snapshot s_n
		/// +- second child of root
		/// ...
		/// \- last child of root
		/// The following information is dumped for each inode:
		/// localName (className@hashCode) parent permission group user
		/// Specific information for different types of INode:
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeDirectory"/>
		/// :childrenSize
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeFile"/>
		/// : fileSize, block list. Check
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.Block.ToString()"/>
		/// and
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguousUnderConstruction.ToString()
		/// 	"/>
		/// for detailed information.
		/// <see cref="FileWithSnapshot"/>
		/// : next link
		/// </pre>
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INode.DumpTreeRecursively()
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public static void CompareDumpedTreeInFile(FilePath file1, FilePath file2, bool compareQuota
			)
		{
			try
			{
				CompareDumpedTreeInFile(file1, file2, compareQuota, false);
			}
			catch (Exception t)
			{
				Log.Info("FAILED compareDumpedTreeInFile(" + file1 + ", " + file2 + ")", t);
				CompareDumpedTreeInFile(file1, file2, compareQuota, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CompareDumpedTreeInFile(FilePath file1, FilePath file2, bool 
			compareQuota, bool print)
		{
			if (print)
			{
				PrintFile(file1);
				PrintFile(file2);
			}
			BufferedReader reader1 = new BufferedReader(new FileReader(file1));
			BufferedReader reader2 = new BufferedReader(new FileReader(file2));
			try
			{
				string line1 = string.Empty;
				string line2 = string.Empty;
				while ((line1 = reader1.ReadLine()) != null && (line2 = reader2.ReadLine()) != null
					)
				{
					if (print)
					{
						System.Console.Out.WriteLine();
						System.Console.Out.WriteLine("1) " + line1);
						System.Console.Out.WriteLine("2) " + line2);
					}
					// skip the hashCode part of the object string during the comparison,
					// also ignore the difference between INodeFile/INodeFileWithSnapshot
					line1 = line1.ReplaceAll("INodeFileWithSnapshot", "INodeFile");
					line2 = line2.ReplaceAll("INodeFileWithSnapshot", "INodeFile");
					line1 = line1.ReplaceAll("@[\\dabcdef]+", string.Empty);
					line2 = line2.ReplaceAll("@[\\dabcdef]+", string.Empty);
					// skip the replica field of the last block of an
					// INodeFileUnderConstruction
					line1 = line1.ReplaceAll("replicas=\\[.*\\]", "replicas=[]");
					line2 = line2.ReplaceAll("replicas=\\[.*\\]", "replicas=[]");
					if (!compareQuota)
					{
						line1 = line1.ReplaceAll("Quota\\[.*\\]", "Quota[]");
						line2 = line2.ReplaceAll("Quota\\[.*\\]", "Quota[]");
					}
					// skip the specific fields of BlockInfoUnderConstruction when the node
					// is an INodeFileSnapshot or an INodeFileUnderConstructionSnapshot
					if (line1.Contains("(INodeFileSnapshot)") || line1.Contains("(INodeFileUnderConstructionSnapshot)"
						))
					{
						line1 = line1.ReplaceAll("\\{blockUCState=\\w+, primaryNodeIndex=[-\\d]+, replicas=\\[\\]\\}"
							, string.Empty);
						line2 = line2.ReplaceAll("\\{blockUCState=\\w+, primaryNodeIndex=[-\\d]+, replicas=\\[\\]\\}"
							, string.Empty);
					}
					NUnit.Framework.Assert.AreEqual(line1, line2);
				}
				NUnit.Framework.Assert.IsNull(reader1.ReadLine());
				NUnit.Framework.Assert.IsNull(reader2.ReadLine());
			}
			finally
			{
				reader1.Close();
				reader2.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void PrintFile(FilePath f)
		{
			System.Console.Out.WriteLine();
			System.Console.Out.WriteLine("File: " + f);
			BufferedReader @in = new BufferedReader(new FileReader(f));
			try
			{
				for (string line; (line = @in.ReadLine()) != null; )
				{
					System.Console.Out.WriteLine(line);
				}
			}
			finally
			{
				@in.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void DumpTree2File(FSDirectory fsdir, FilePath f)
		{
			PrintWriter @out = new PrintWriter(new FileWriter(f, false), true);
			fsdir.GetINode("/").DumpTreeRecursively(@out, new StringBuilder(), Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			@out.Close();
		}

		/// <summary>Generate the path for a snapshot file.</summary>
		/// <param name="snapshotRoot">
		/// of format
		/// <literal><snapshottble_dir>/.snapshot/<snapshot_name></literal>
		/// </param>
		/// <param name="file">path to a file</param>
		/// <returns>
		/// The path of the snapshot of the file assuming the file has a
		/// snapshot under the snapshot root of format
		/// <literal><snapshottble_dir>/.snapshot/<snapshot_name>/<path_to_file_inside_snapshot>
		/// 	</literal>
		/// . Null if the file is not under the directory associated with the
		/// snapshot root.
		/// </returns>
		internal static Path GetSnapshotFile(Path snapshotRoot, Path file)
		{
			Path rootParent = snapshotRoot.GetParent();
			if (rootParent != null && rootParent.GetName().Equals(".snapshot"))
			{
				Path snapshotDir = rootParent.GetParent();
				if (file.ToString().Contains(snapshotDir.ToString()) && !file.Equals(snapshotDir))
				{
					string fileName = Sharpen.Runtime.Substring(file.ToString(), snapshotDir.ToString
						().Length + 1);
					Path snapshotFile = new Path(snapshotRoot, fileName);
					return snapshotFile;
				}
			}
			return null;
		}

		/// <summary>A class creating directories trees for snapshot testing.</summary>
		/// <remarks>
		/// A class creating directories trees for snapshot testing. For simplicity,
		/// the directory tree is a binary tree, i.e., each directory has two children
		/// as snapshottable directories.
		/// </remarks>
		internal class TestDirectoryTree
		{
			/// <summary>Height of the directory tree</summary>
			internal readonly int height;

			/// <summary>Top node of the directory tree</summary>
			internal readonly SnapshotTestHelper.TestDirectoryTree.Node topNode;

			/// <summary>A map recording nodes for each tree level</summary>
			internal readonly IDictionary<int, AList<SnapshotTestHelper.TestDirectoryTree.Node
				>> levelMap;

			/// <summary>
			/// Constructor to build a tree of given
			/// <paramref name="height"/>
			/// </summary>
			/// <exception cref="System.Exception"/>
			internal TestDirectoryTree(int height, FileSystem fs)
			{
				this.height = height;
				this.topNode = new SnapshotTestHelper.TestDirectoryTree.Node(new Path("/TestSnapshot"
					), 0, null, fs);
				this.levelMap = new Dictionary<int, AList<SnapshotTestHelper.TestDirectoryTree.Node
					>>();
				AddDirNode(topNode, 0);
				GenChildren(topNode, height - 1, fs);
			}

			/// <summary>Add a node into the levelMap</summary>
			private void AddDirNode(SnapshotTestHelper.TestDirectoryTree.Node node, int atLevel
				)
			{
				AList<SnapshotTestHelper.TestDirectoryTree.Node> list = levelMap[atLevel];
				if (list == null)
				{
					list = new AList<SnapshotTestHelper.TestDirectoryTree.Node>();
					levelMap[atLevel] = list;
				}
				list.AddItem(node);
			}

			internal int id = 0;

			/// <summary>Recursively generate the tree based on the height.</summary>
			/// <param name="parent">The parent node</param>
			/// <param name="level">The remaining levels to generate</param>
			/// <param name="fs">The FileSystem where to generate the files/dirs</param>
			/// <exception cref="System.Exception"/>
			private void GenChildren(SnapshotTestHelper.TestDirectoryTree.Node parent, int level
				, FileSystem fs)
			{
				if (level == 0)
				{
					return;
				}
				parent.leftChild = new SnapshotTestHelper.TestDirectoryTree.Node(new Path(parent.
					nodePath, "left" + ++id), height - level, parent, fs);
				parent.rightChild = new SnapshotTestHelper.TestDirectoryTree.Node(new Path(parent
					.nodePath, "right" + ++id), height - level, parent, fs);
				AddDirNode(parent.leftChild, parent.leftChild.level);
				AddDirNode(parent.rightChild, parent.rightChild.level);
				GenChildren(parent.leftChild, level - 1, fs);
				GenChildren(parent.rightChild, level - 1, fs);
			}

			/// <summary>Randomly retrieve a node from the directory tree.</summary>
			/// <param name="random">A random instance passed by user.</param>
			/// <param name="excludedList">
			/// Excluded list, i.e., the randomly generated node
			/// cannot be one of the nodes in this list.
			/// </param>
			/// <returns>a random node from the tree.</returns>
			internal virtual SnapshotTestHelper.TestDirectoryTree.Node GetRandomDirNode(Random
				 random, IList<SnapshotTestHelper.TestDirectoryTree.Node> excludedList)
			{
				while (true)
				{
					int level = random.Next(height);
					AList<SnapshotTestHelper.TestDirectoryTree.Node> levelList = levelMap[level];
					int index = random.Next(levelList.Count);
					SnapshotTestHelper.TestDirectoryTree.Node randomNode = levelList[index];
					if (excludedList == null || !excludedList.Contains(randomNode))
					{
						return randomNode;
					}
				}
			}

			/// <summary>
			/// The class representing a node in
			/// <see cref="TestDirectoryTree"/>
			/// .
			/// <br />
			/// This contains:
			/// <ul>
			/// <li>Two children representing the two snapshottable directories</li>
			/// <li>A list of files for testing, so that we can check snapshots
			/// after file creation/deletion/modification.</li>
			/// <li>A list of non-snapshottable directories, to test snapshots with
			/// directory creation/deletion. Note that this is needed because the
			/// deletion of a snapshottale directory with snapshots is not allowed.</li>
			/// </ul>
			/// </summary>
			internal class Node
			{
				/// <summary>The level of this node in the directory tree</summary>
				internal readonly int level;

				/// <summary>Children</summary>
				internal SnapshotTestHelper.TestDirectoryTree.Node leftChild;

				internal SnapshotTestHelper.TestDirectoryTree.Node rightChild;

				/// <summary>Parent node of the node</summary>
				internal readonly SnapshotTestHelper.TestDirectoryTree.Node parent;

				/// <summary>File path of the node</summary>
				internal readonly Path nodePath;

				/// <summary>
				/// The file path list for testing snapshots before/after file
				/// creation/deletion/modification
				/// </summary>
				internal AList<Path> fileList;

				/// <summary>
				/// Each time for testing snapshots with file creation, since we do not
				/// want to insert new files into the fileList, we always create the file
				/// that was deleted last time.
				/// </summary>
				/// <remarks>
				/// Each time for testing snapshots with file creation, since we do not
				/// want to insert new files into the fileList, we always create the file
				/// that was deleted last time. Thus we record the index for deleted file
				/// in the fileList, and roll the file modification forward in the list.
				/// </remarks>
				internal int nullFileIndex = 0;

				/// <summary>
				/// A list of non-snapshottable directories for testing snapshots with
				/// directory creation/deletion
				/// </summary>
				internal readonly AList<SnapshotTestHelper.TestDirectoryTree.Node> nonSnapshotChildren;

				/// <exception cref="System.Exception"/>
				internal Node(Path path, int level, SnapshotTestHelper.TestDirectoryTree.Node parent
					, FileSystem fs)
				{
					this.nodePath = path;
					this.level = level;
					this.parent = parent;
					this.nonSnapshotChildren = new AList<SnapshotTestHelper.TestDirectoryTree.Node>();
					fs.Mkdirs(nodePath);
				}

				/// <summary>Create files and add them in the fileList.</summary>
				/// <remarks>
				/// Create files and add them in the fileList. Initially the last element
				/// in the fileList is set to null (where we start file creation).
				/// </remarks>
				/// <exception cref="System.Exception"/>
				internal virtual void InitFileList(FileSystem fs, string namePrefix, long fileLen
					, short replication, long seed, int numFiles)
				{
					fileList = new AList<Path>(numFiles);
					for (int i = 0; i < numFiles; i++)
					{
						Path file = new Path(nodePath, namePrefix + "-f" + i);
						fileList.AddItem(file);
						if (i < numFiles - 1)
						{
							DFSTestUtil.CreateFile(fs, file, fileLen, replication, seed);
						}
					}
					nullFileIndex = numFiles - 1;
				}

				public override bool Equals(object o)
				{
					if (o != null && o is SnapshotTestHelper.TestDirectoryTree.Node)
					{
						SnapshotTestHelper.TestDirectoryTree.Node node = (SnapshotTestHelper.TestDirectoryTree.Node
							)o;
						return node.nodePath.Equals(nodePath);
					}
					return false;
				}

				public override int GetHashCode()
				{
					return nodePath.GetHashCode();
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public static void DumpTree(string message, MiniDFSCluster cluster)
		{
			System.Console.Out.WriteLine("XXX " + message);
			cluster.GetNameNode().GetNamesystem().GetFSDirectory().GetINode("/").DumpTreeRecursively
				(System.Console.Out);
		}
	}
}
