using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>A JUnit test for corrupted file handling.</summary>
	public class TestFileCorruption
	{
		internal static Logger Log = NameNode.stateChangeLog;

		/// <summary>check if DFS can handle corrupted blocks properly</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCorruption()
		{
			MiniDFSCluster cluster = null;
			DFSTestUtil util = new DFSTestUtil.Builder().SetName("TestFileCorruption").SetNumFiles
				(20).Build();
			try
			{
				Configuration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				FileSystem fs = cluster.GetFileSystem();
				util.CreateFiles(fs, "/srcdat");
				// Now deliberately remove the blocks
				FilePath storageDir = cluster.GetInstanceStorageDir(2, 0);
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				FilePath data_dir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
				NUnit.Framework.Assert.IsTrue("data directory does not exist", data_dir.Exists());
				FilePath[] blocks = data_dir.ListFiles();
				NUnit.Framework.Assert.IsTrue("Blocks do not exist in data-dir", (blocks != null)
					 && (blocks.Length > 0));
				for (int idx = 0; idx < blocks.Length; idx++)
				{
					if (!blocks[idx].GetName().StartsWith(Block.BlockFilePrefix))
					{
						continue;
					}
					System.Console.Out.WriteLine("Deliberately removing file " + blocks[idx].GetName(
						));
					NUnit.Framework.Assert.IsTrue("Cannot remove file.", blocks[idx].Delete());
				}
				NUnit.Framework.Assert.IsTrue("Corrupted replicas not handled properly.", util.CheckFiles
					(fs, "/srcdat"));
				util.Cleanup(fs, "/srcdat");
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>check if local FS can handle corrupted blocks properly</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalFileCorruption()
		{
			Configuration conf = new HdfsConfiguration();
			Path file = new Path(PathUtils.GetTestDirName(GetType()), "corruptFile");
			FileSystem fs = FileSystem.GetLocal(conf);
			DataOutputStream dos = fs.Create(file);
			dos.WriteBytes("original bytes");
			dos.Close();
			// Now deliberately corrupt the file
			dos = new DataOutputStream(new FileOutputStream(file.ToString()));
			dos.WriteBytes("corruption");
			dos.Close();
			// Now attempt to read the file
			DataInputStream dis = fs.Open(file, 512);
			try
			{
				System.Console.Out.WriteLine("A ChecksumException is expected to be logged.");
				dis.ReadByte();
			}
			catch (ChecksumException)
			{
			}
			//expect this exception but let any NPE get thrown
			fs.Delete(file, true);
		}

		/// <summary>
		/// Test the case that a replica is reported corrupt while it is not
		/// in blocksMap.
		/// </summary>
		/// <remarks>
		/// Test the case that a replica is reported corrupt while it is not
		/// in blocksMap. Make sure that ArrayIndexOutOfBounds does not thrown.
		/// See Hadoop-4351.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestArrayOutOfBoundsException()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				Path FilePath = new Path("/tmp.txt");
				long FileLen = 1L;
				DFSTestUtil.CreateFile(fs, FilePath, FileLen, (short)2, 1L);
				// get the block
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				FilePath storageDir = cluster.GetInstanceStorageDir(0, 0);
				FilePath dataDir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
				NUnit.Framework.Assert.IsTrue("Data directory does not exist", dataDir.Exists());
				ExtendedBlock blk = GetBlock(bpid, dataDir);
				if (blk == null)
				{
					storageDir = cluster.GetInstanceStorageDir(0, 1);
					dataDir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
					blk = GetBlock(bpid, dataDir);
				}
				NUnit.Framework.Assert.IsFalse("Data directory does not contain any blocks or there was an "
					 + "IO error", blk == null);
				// start a third datanode
				cluster.StartDataNodes(conf, 1, true, null, null);
				AList<DataNode> datanodes = cluster.GetDataNodes();
				NUnit.Framework.Assert.AreEqual(datanodes.Count, 3);
				DataNode dataNode = datanodes[2];
				// report corrupted block by the third datanode
				DatanodeRegistration dnR = DataNodeTestUtils.GetDNRegistrationForBP(dataNode, blk
					.GetBlockPoolId());
				FSNamesystem ns = cluster.GetNamesystem();
				ns.WriteLock();
				try
				{
					cluster.GetNamesystem().GetBlockManager().FindAndMarkBlockAsCorrupt(blk, new DatanodeInfo
						(dnR), "TEST", "STORAGE_ID");
				}
				finally
				{
					ns.WriteUnlock();
				}
				// open the file
				fs.Open(FilePath);
				//clean up
				fs.Delete(FilePath, false);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		public static ExtendedBlock GetBlock(string bpid, FilePath dataDir)
		{
			IList<FilePath> metadataFiles = MiniDFSCluster.GetAllBlockMetadataFiles(dataDir);
			if (metadataFiles == null || metadataFiles.IsEmpty())
			{
				return null;
			}
			FilePath metadataFile = metadataFiles[0];
			FilePath blockFile = Block.MetaToBlockFile(metadataFile);
			return new ExtendedBlock(bpid, Block.GetBlockId(blockFile.GetName()), blockFile.Length
				(), Block.GetGenerationStamp(metadataFile.GetName()));
		}

		public TestFileCorruption()
		{
			{
				DFSTestUtil.SetNameNodeLogLevel(Level.All);
				GenericTestUtils.SetLogLevel(DataNode.Log, Level.All);
				GenericTestUtils.SetLogLevel(DFSClient.Log, Level.All);
			}
		}
	}
}
