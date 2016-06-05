using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>This class tests the replication handling/calculation of snapshots.</summary>
	/// <remarks>
	/// This class tests the replication handling/calculation of snapshots. In
	/// particular,
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeFile.GetFileReplication()"
	/// 	/>
	/// and
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeFile.GetBlockReplication()
	/// 	"/>
	/// are tested to make sure
	/// the number of replication is calculated correctly with/without snapshots.
	/// </remarks>
	public class TestSnapshotReplication
	{
		private const long seed = 0;

		private const short Replication = 3;

		private const int Numdatanode = 5;

		private const long Blocksize = 1024;

		private readonly Path dir = new Path("/TestSnapshot");

		private readonly Path sub1;

		private readonly Path file1;

		internal Configuration conf;

		internal MiniDFSCluster cluster;

		internal FSNamesystem fsn;

		internal DistributedFileSystem hdfs;

		internal FSDirectory fsdir;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Numdatanode).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			hdfs = cluster.GetFileSystem();
			fsdir = fsn.GetFSDirectory();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Check the replication of a given file.</summary>
		/// <remarks>
		/// Check the replication of a given file. We test both
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeFile.GetFileReplication()"
		/// 	/>
		/// and
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeFile.GetBlockReplication()
		/// 	"/>
		/// .
		/// </remarks>
		/// <param name="file">The given file</param>
		/// <param name="replication">The expected replication number</param>
		/// <param name="blockReplication">The expected replication number for the block</param>
		/// <exception cref="System.Exception"/>
		private void CheckFileReplication(Path file, short replication, short blockReplication
			)
		{
			// Get FileStatus of file1, and identify the replication number of file1.
			// Note that the replication number in FileStatus was derived from
			// INodeFile#getFileReplication().
			short fileReplication = hdfs.GetFileStatus(file1).GetReplication();
			NUnit.Framework.Assert.AreEqual(replication, fileReplication);
			// Check the correctness of getBlockReplication()
			INode inode = fsdir.GetINode(file1.ToString());
			NUnit.Framework.Assert.IsTrue(inode is INodeFile);
			NUnit.Framework.Assert.AreEqual(blockReplication, ((INodeFile)inode).GetBlockReplication
				());
		}

		/// <summary>Test replication number calculation for a normal file without snapshots.
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReplicationWithoutSnapshot()
		{
			// Create file1, set its replication to REPLICATION
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			// Check the replication of file1
			CheckFileReplication(file1, Replication, Replication);
			// Change the replication factor of file1 from 3 to 2
			hdfs.SetReplication(file1, (short)(Replication - 1));
			// Check the replication again
			CheckFileReplication(file1, (short)(Replication - 1), (short)(Replication - 1));
		}

		/// <exception cref="System.Exception"/>
		internal virtual INodeFile GetINodeFile(Path p)
		{
			string s = p.ToString();
			return INodeFile.ValueOf(fsdir.GetINode(s), s);
		}

		/// <summary>Check the replication for both the current file and all its prior snapshots
		/// 	</summary>
		/// <param name="currentFile">the Path of the current file</param>
		/// <param name="snapshotRepMap">
		/// A map maintaining all the snapshots of the current file, as well
		/// as their expected replication number stored in their corresponding
		/// INodes
		/// </param>
		/// <param name="expectedBlockRep">The expected replication number</param>
		/// <exception cref="System.Exception"/>
		private void CheckSnapshotFileReplication(Path currentFile, IDictionary<Path, short
			> snapshotRepMap, short expectedBlockRep)
		{
			// First check the getBlockReplication for the INode of the currentFile
			INodeFile inodeOfCurrentFile = GetINodeFile(currentFile);
			NUnit.Framework.Assert.AreEqual(expectedBlockRep, inodeOfCurrentFile.GetBlockReplication
				());
			// Then check replication for every snapshot
			foreach (Path ss in snapshotRepMap.Keys)
			{
				INodesInPath iip = fsdir.GetINodesInPath(ss.ToString(), true);
				INodeFile ssInode = iip.GetLastINode().AsFile();
				// The replication number derived from the
				// INodeFileWithLink#getBlockReplication should always == expectedBlockRep
				NUnit.Framework.Assert.AreEqual(expectedBlockRep, ssInode.GetBlockReplication());
				// Also check the number derived from INodeFile#getFileReplication
				NUnit.Framework.Assert.AreEqual(snapshotRepMap[ss], ssInode.GetFileReplication(iip
					.GetPathSnapshotId()));
			}
		}

		/// <summary>Test replication number calculation for a file with snapshots.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReplicationWithSnapshot()
		{
			short fileRep = 1;
			// Create file1, set its replication to 1
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, fileRep, seed);
			IDictionary<Path, short> snapshotRepMap = new Dictionary<Path, short>();
			// Change replication factor from 1 to 5. In the meanwhile, keep taking
			// snapshots for sub1
			for (; fileRep < Numdatanode; )
			{
				// Create snapshot for sub1
				Path snapshotRoot = SnapshotTestHelper.CreateSnapshot(hdfs, sub1, "s" + fileRep);
				Path snapshot = new Path(snapshotRoot, file1.GetName());
				// Check the replication stored in the INode of the snapshot of file1
				NUnit.Framework.Assert.AreEqual(fileRep, GetINodeFile(snapshot).GetFileReplication
					());
				snapshotRepMap[snapshot] = fileRep;
				// Increase the replication factor by 1
				hdfs.SetReplication(file1, ++fileRep);
				// Check the replication for file1
				CheckFileReplication(file1, fileRep, fileRep);
				// Also check the replication for all the prior snapshots of file1
				CheckSnapshotFileReplication(file1, snapshotRepMap, fileRep);
			}
			// Change replication factor back to 3.
			hdfs.SetReplication(file1, Replication);
			// Check the replication for file1
			// Currently the max replication among snapshots should be 4
			CheckFileReplication(file1, Replication, (short)(Numdatanode - 1));
			// Also check the replication for all the prior snapshots of file1.
			// Currently the max replication among snapshots should be 4
			CheckSnapshotFileReplication(file1, snapshotRepMap, (short)(Numdatanode - 1));
		}

		/// <summary>
		/// Test replication for a file with snapshots, also including the scenario
		/// where the original file is deleted
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReplicationAfterDeletion()
		{
			// Create file1, set its replication to 3
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			IDictionary<Path, short> snapshotRepMap = new Dictionary<Path, short>();
			// Take 3 snapshots of sub1
			for (int i = 1; i <= 3; i++)
			{
				Path root = SnapshotTestHelper.CreateSnapshot(hdfs, sub1, "s" + i);
				Path ssFile = new Path(root, file1.GetName());
				snapshotRepMap[ssFile] = Replication;
			}
			// Check replication
			CheckFileReplication(file1, Replication, Replication);
			CheckSnapshotFileReplication(file1, snapshotRepMap, Replication);
			// Delete file1
			hdfs.Delete(file1, true);
			// Check replication of snapshots
			foreach (Path ss in snapshotRepMap.Keys)
			{
				INodeFile ssInode = GetINodeFile(ss);
				// The replication number derived from the
				// INodeFileWithLink#getBlockReplication should always == expectedBlockRep
				NUnit.Framework.Assert.AreEqual(Replication, ssInode.GetBlockReplication());
				// Also check the number derived from INodeFile#getFileReplication
				NUnit.Framework.Assert.AreEqual(snapshotRepMap[ss], ssInode.GetFileReplication());
			}
		}

		public TestSnapshotReplication()
		{
			sub1 = new Path(dir, "sub1");
			file1 = new Path(sub1, "file1");
		}
	}
}
