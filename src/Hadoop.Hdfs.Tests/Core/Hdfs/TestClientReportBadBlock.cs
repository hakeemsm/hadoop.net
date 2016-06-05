using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Class is used to test client reporting corrupted block replica to name node.
	/// 	</summary>
	/// <remarks>
	/// Class is used to test client reporting corrupted block replica to name node.
	/// The reporting policy is if block replica is more than one, if all replicas
	/// are corrupted, client does not report (since the client can handicapped). If
	/// some of the replicas are corrupted, client reports the corrupted block
	/// replicas. In case of only one block replica, client always reports corrupted
	/// replica.
	/// </remarks>
	public class TestClientReportBadBlock
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestClientReportBadBlock
			));

		internal const long BlockSize = 64 * 1024;

		private static int buffersize;

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem dfs;

		private const int numDataNodes = 3;

		private static readonly Configuration conf = new HdfsConfiguration();

		internal Random rand = new Random();

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartUpCluster()
		{
			// disable block scanner
			conf.SetInt(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).Build();
			cluster.WaitActive();
			dfs = cluster.GetFileSystem();
			buffersize = conf.GetInt(CommonConfigurationKeys.IoFileBufferSizeKey, 4096);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutDownCluster()
		{
			dfs.Close();
			cluster.Shutdown();
		}

		/*
		* This test creates a file with one block replica. Corrupt the block. Make
		* DFSClient read the corrupted file. Corrupted block is expected to be
		* reported to name node.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOneBlockReplica()
		{
			short repl = 1;
			int corruptBlockNumber = 1;
			for (int i = 0; i < 2; i++)
			{
				// create a file
				string fileName = "/tmp/testClientReportBadBlock/OneBlockReplica" + i;
				Path filePath = new Path(fileName);
				CreateAFileWithCorruptedBlockReplicas(filePath, repl, corruptBlockNumber);
				if (i == 0)
				{
					DfsClientReadFile(filePath);
				}
				else
				{
					DfsClientReadFileFromPosition(filePath);
				}
				// the only block replica is corrupted. The LocatedBlock should be marked
				// as corrupted. But the corrupted replica is expected to be returned
				// when calling Namenode#getBlockLocations() since all(one) replicas are
				// corrupted.
				int expectedReplicaCount = 1;
				VerifyCorruptedBlockCount(filePath, expectedReplicaCount);
				VerifyFirstBlockCorrupted(filePath, true);
				VerifyFsckBlockCorrupted();
				TestFsckListCorruptFilesBlocks(filePath, -1);
			}
		}

		/// <summary>This test creates a file with three block replicas.</summary>
		/// <remarks>
		/// This test creates a file with three block replicas. Corrupt all of the
		/// replicas. Make dfs client read the file. No block corruption should be
		/// reported.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCorruptAllOfThreeReplicas()
		{
			short repl = 3;
			int corruptBlockNumber = 3;
			for (int i = 0; i < 2; i++)
			{
				// create a file
				string fileName = "/tmp/testClientReportBadBlock/testCorruptAllReplicas" + i;
				Path filePath = new Path(fileName);
				CreateAFileWithCorruptedBlockReplicas(filePath, repl, corruptBlockNumber);
				// ask dfs client to read the file
				if (i == 0)
				{
					DfsClientReadFile(filePath);
				}
				else
				{
					DfsClientReadFileFromPosition(filePath);
				}
				// As all replicas are corrupted. We expect DFSClient does NOT report
				// corrupted replicas to the name node.
				int expectedReplicasReturned = repl;
				VerifyCorruptedBlockCount(filePath, expectedReplicasReturned);
				// LocatedBlock should not have the block marked as corrupted.
				VerifyFirstBlockCorrupted(filePath, false);
				VerifyFsckHealth(string.Empty);
				TestFsckListCorruptFilesBlocks(filePath, 0);
			}
		}

		/// <summary>This test creates a file with three block replicas.</summary>
		/// <remarks>
		/// This test creates a file with three block replicas. Corrupt two of the
		/// replicas. Make dfs client read the file. The corrupted blocks with their
		/// owner data nodes should be reported to the name node.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCorruptTwoOutOfThreeReplicas()
		{
			short repl = 3;
			int corruptBlocReplicas = 2;
			for (int i = 0; i < 2; i++)
			{
				string fileName = "/tmp/testClientReportBadBlock/CorruptTwoOutOfThreeReplicas" + 
					i;
				Path filePath = new Path(fileName);
				CreateAFileWithCorruptedBlockReplicas(filePath, repl, corruptBlocReplicas);
				int replicaCount = 0;
				/*
				* The order of data nodes in LocatedBlock returned by name node is sorted
				* by NetworkToplology#pseudoSortByDistance. In current MiniDFSCluster,
				* when LocatedBlock is returned, the sorting is based on a random order.
				* That is to say, the DFS client and simulated data nodes in mini DFS
				* cluster are considered not on the same host nor the same rack.
				* Therefore, even we corrupted the first two block replicas based in
				* order. When DFSClient read some block replicas, it is not guaranteed
				* which block replicas (good/bad) will be returned first. So we try to
				* re-read the file until we know the expected replicas numbers is
				* returned.
				*/
				while (replicaCount != repl - corruptBlocReplicas)
				{
					if (i == 0)
					{
						DfsClientReadFile(filePath);
					}
					else
					{
						DfsClientReadFileFromPosition(filePath);
					}
					LocatedBlocks blocks = dfs.dfs.GetNamenode().GetBlockLocations(filePath.ToString(
						), 0, long.MaxValue);
					replicaCount = blocks.Get(0).GetLocations().Length;
				}
				VerifyFirstBlockCorrupted(filePath, false);
				int expectedReplicaCount = repl - corruptBlocReplicas;
				VerifyCorruptedBlockCount(filePath, expectedReplicaCount);
				VerifyFsckHealth("Target Replicas is 3 but found 1 replica");
				TestFsckListCorruptFilesBlocks(filePath, 0);
			}
		}

		/// <summary>Create a file with one block and corrupt some/all of the block replicas.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private void CreateAFileWithCorruptedBlockReplicas(Path filePath, short repl, int
			 corruptBlockCount)
		{
			DFSTestUtil.CreateFile(dfs, filePath, BlockSize, repl, 0);
			DFSTestUtil.WaitReplication(dfs, filePath, repl);
			// Locate the file blocks by asking name node
			LocatedBlocks locatedblocks = dfs.dfs.GetNamenode().GetBlockLocations(filePath.ToString
				(), 0L, BlockSize);
			NUnit.Framework.Assert.AreEqual(repl, locatedblocks.Get(0).GetLocations().Length);
			// The file only has one block
			LocatedBlock lblock = locatedblocks.Get(0);
			DatanodeInfo[] datanodeinfos = lblock.GetLocations();
			ExtendedBlock block = lblock.GetBlock();
			// corrupt some /all of the block replicas
			for (int i = 0; i < corruptBlockCount; i++)
			{
				DatanodeInfo dninfo = datanodeinfos[i];
				DataNode dn = cluster.GetDataNode(dninfo.GetIpcPort());
				CorruptBlock(block, dn);
				Log.Debug("Corrupted block " + block.GetBlockName() + " on data node " + dninfo);
			}
		}

		/// <summary>Verify the first block of the file is corrupted (for all its replica).</summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		private void VerifyFirstBlockCorrupted(Path filePath, bool isCorrupted)
		{
			LocatedBlocks locatedBlocks = dfs.dfs.GetNamenode().GetBlockLocations(filePath.ToUri
				().GetPath(), 0, long.MaxValue);
			LocatedBlock firstLocatedBlock = locatedBlocks.Get(0);
			NUnit.Framework.Assert.AreEqual(isCorrupted, firstLocatedBlock.IsCorrupt());
		}

		/// <summary>
		/// Verify the number of corrupted block replicas by fetching the block
		/// location from name node.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		private void VerifyCorruptedBlockCount(Path filePath, int expectedReplicas)
		{
			LocatedBlocks lBlocks = dfs.dfs.GetNamenode().GetBlockLocations(filePath.ToUri().
				GetPath(), 0, long.MaxValue);
			// we expect only the first block of the file is used for this test
			LocatedBlock firstLocatedBlock = lBlocks.Get(0);
			NUnit.Framework.Assert.AreEqual(expectedReplicas, firstLocatedBlock.GetLocations(
				).Length);
		}

		/// <summary>Ask dfs client to read the file</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		private void DfsClientReadFile(Path corruptedFile)
		{
			DFSInputStream @in = dfs.dfs.Open(corruptedFile.ToUri().GetPath());
			byte[] buf = new byte[buffersize];
			int nRead = 0;
			// total number of bytes read
			try
			{
				do
				{
					nRead = @in.Read(buf, 0, buf.Length);
				}
				while (nRead > 0);
			}
			catch (ChecksumException)
			{
				// caught ChecksumException if all replicas are bad, ignore and continue.
				Log.Debug("DfsClientReadFile caught ChecksumException.");
			}
			catch (BlockMissingException)
			{
				// caught BlockMissingException, ignore.
				Log.Debug("DfsClientReadFile caught BlockMissingException.");
			}
		}

		/// <summary>DFS client read bytes starting from the specified position.</summary>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		private void DfsClientReadFileFromPosition(Path corruptedFile)
		{
			DFSInputStream @in = dfs.dfs.Open(corruptedFile.ToUri().GetPath());
			byte[] buf = new byte[buffersize];
			int startPosition = 2;
			int nRead = 0;
			// total number of bytes read
			try
			{
				do
				{
					nRead = @in.Read(startPosition, buf, 0, buf.Length);
					startPosition += buf.Length;
				}
				while (nRead > 0);
			}
			catch (BlockMissingException)
			{
				Log.Debug("DfsClientReadFile caught BlockMissingException.");
			}
		}

		/// <summary>Corrupt a block on a data node.</summary>
		/// <remarks>
		/// Corrupt a block on a data node. Replace the block file content with content
		/// of 1, 2, ...BLOCK_SIZE.
		/// </remarks>
		/// <param name="block">the ExtendedBlock to be corrupted</param>
		/// <param name="dn">the data node where the block needs to be corrupted</param>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		private static void CorruptBlock(ExtendedBlock block, DataNode dn)
		{
			FilePath f = DataNodeTestUtils.GetBlockFile(dn, block.GetBlockPoolId(), block.GetLocalBlock
				());
			RandomAccessFile raFile = new RandomAccessFile(f, "rw");
			byte[] bytes = new byte[(int)BlockSize];
			for (int i = 0; i < BlockSize; i++)
			{
				bytes[i] = unchecked((byte)(i));
			}
			raFile.Write(bytes);
			raFile.Close();
		}

		/// <exception cref="System.Exception"/>
		private static void VerifyFsckHealth(string expected)
		{
			// Fsck health has error code 0.
			// Make sure filesystem is in healthy state
			string outStr = RunFsck(conf, 0, true, "/");
			Log.Info(outStr);
			NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
			if (!expected.Equals(string.Empty))
			{
				NUnit.Framework.Assert.IsTrue(outStr.Contains(expected));
			}
		}

		/// <exception cref="System.Exception"/>
		private static void VerifyFsckBlockCorrupted()
		{
			string outStr = RunFsck(conf, 1, true, "/");
			Log.Info(outStr);
			NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.CorruptStatus));
		}

		/// <exception cref="System.Exception"/>
		private static void TestFsckListCorruptFilesBlocks(Path filePath, int errorCode)
		{
			string outStr = RunFsck(conf, errorCode, true, filePath.ToString(), "-list-corruptfileblocks"
				);
			Log.Info("fsck -list-corruptfileblocks out: " + outStr);
			if (errorCode != 0)
			{
				NUnit.Framework.Assert.IsTrue(outStr.Contains("CORRUPT files"));
			}
		}

		/// <exception cref="System.Exception"/>
		internal static string RunFsck(Configuration conf, int expectedErrCode, bool checkErrorCode
			, params string[] path)
		{
			ByteArrayOutputStream bStream = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(bStream, true);
			int errCode = ToolRunner.Run(new DFSck(conf, @out), path);
			if (checkErrorCode)
			{
				NUnit.Framework.Assert.AreEqual(expectedErrCode, errCode);
			}
			return bStream.ToString();
		}
	}
}
