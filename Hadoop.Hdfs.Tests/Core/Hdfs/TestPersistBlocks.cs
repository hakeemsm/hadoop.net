using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// A JUnit test for checking if restarting DFS preserves the
	/// blocks that are part of an unclosed file.
	/// </summary>
	public class TestPersistBlocks
	{
		static TestPersistBlocks()
		{
			((Log4JLogger)FSImage.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)FSNamesystem.Log).GetLogger().SetLevel(Level.All);
		}

		private const int BlockSize = 4096;

		private const int NumBlocks = 5;

		private const string FileName = "/data";

		private static readonly Path FilePath = new Path(FileName);

		internal static readonly byte[] DataBeforeRestart = new byte[BlockSize * NumBlocks
			];

		internal static readonly byte[] DataAfterRestart = new byte[BlockSize * NumBlocks
			];

		private const string Hadoop10MultiblockTgz = "hadoop-1.0-multiblock-file.tgz";

		static TestPersistBlocks()
		{
			Random rand = new Random();
			rand.NextBytes(DataBeforeRestart);
			rand.NextBytes(DataAfterRestart);
		}

		/// <summary>check if DFS remains in proper condition after a restart</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRestartDfs()
		{
			Configuration conf = new HdfsConfiguration();
			// Turn off persistent IPC, so that the DFSClient can survive NN restart
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, 0);
			MiniDFSCluster cluster = null;
			long len = 0;
			FSDataOutputStream stream;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				FileSystem fs = cluster.GetFileSystem();
				// Creating a file with 4096 blockSize to write multiple blocks
				stream = fs.Create(FilePath, true, BlockSize, (short)1, BlockSize);
				stream.Write(DataBeforeRestart);
				stream.Hflush();
				// Wait for at least a few blocks to get through
				while (len <= BlockSize)
				{
					FileStatus status = fs.GetFileStatus(FilePath);
					len = status.GetLen();
					Sharpen.Thread.Sleep(100);
				}
				// explicitly do NOT close the file.
				cluster.RestartNameNode();
				// Check that the file has no less bytes than before the restart
				// This would mean that blocks were successfully persisted to the log
				FileStatus status_1 = fs.GetFileStatus(FilePath);
				NUnit.Framework.Assert.IsTrue("Length too short: " + status_1.GetLen(), status_1.
					GetLen() >= len);
				// And keep writing (ensures that leases are also persisted correctly)
				stream.Write(DataAfterRestart);
				stream.Close();
				// Verify that the data showed up, both from before and after the restart.
				FSDataInputStream readStream = fs.Open(FilePath);
				try
				{
					byte[] verifyBuf = new byte[DataBeforeRestart.Length];
					IOUtils.ReadFully(readStream, verifyBuf, 0, verifyBuf.Length);
					Assert.AssertArrayEquals(DataBeforeRestart, verifyBuf);
					IOUtils.ReadFully(readStream, verifyBuf, 0, verifyBuf.Length);
					Assert.AssertArrayEquals(DataAfterRestart, verifyBuf);
				}
				finally
				{
					IOUtils.CloseStream(readStream);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRestartDfsWithAbandonedBlock()
		{
			Configuration conf = new HdfsConfiguration();
			// Turn off persistent IPC, so that the DFSClient can survive NN restart
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, 0);
			MiniDFSCluster cluster = null;
			long len = 0;
			FSDataOutputStream stream;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				FileSystem fs = cluster.GetFileSystem();
				// Creating a file with 4096 blockSize to write multiple blocks
				stream = fs.Create(FilePath, true, BlockSize, (short)1, BlockSize);
				stream.Write(DataBeforeRestart);
				stream.Hflush();
				// Wait for all of the blocks to get through
				while (len < BlockSize * (NumBlocks - 1))
				{
					FileStatus status = fs.GetFileStatus(FilePath);
					len = status.GetLen();
					Sharpen.Thread.Sleep(100);
				}
				// Abandon the last block
				DFSClient dfsclient = DFSClientAdapter.GetDFSClient((DistributedFileSystem)fs);
				HdfsFileStatus fileStatus = dfsclient.GetNamenode().GetFileInfo(FileName);
				LocatedBlocks blocks = dfsclient.GetNamenode().GetBlockLocations(FileName, 0, BlockSize
					 * NumBlocks);
				NUnit.Framework.Assert.AreEqual(NumBlocks, blocks.GetLocatedBlocks().Count);
				LocatedBlock b = blocks.GetLastLocatedBlock();
				dfsclient.GetNamenode().AbandonBlock(b.GetBlock(), fileStatus.GetFileId(), FileName
					, dfsclient.clientName);
				// explicitly do NOT close the file.
				cluster.RestartNameNode();
				// Check that the file has no less bytes than before the restart
				// This would mean that blocks were successfully persisted to the log
				FileStatus status_1 = fs.GetFileStatus(FilePath);
				NUnit.Framework.Assert.IsTrue("Length incorrect: " + status_1.GetLen(), status_1.
					GetLen() == len - BlockSize);
				// Verify the data showed up from before restart, sans abandoned block.
				FSDataInputStream readStream = fs.Open(FilePath);
				try
				{
					byte[] verifyBuf = new byte[DataBeforeRestart.Length - BlockSize];
					IOUtils.ReadFully(readStream, verifyBuf, 0, verifyBuf.Length);
					byte[] expectedBuf = new byte[DataBeforeRestart.Length - BlockSize];
					System.Array.Copy(DataBeforeRestart, 0, expectedBuf, 0, expectedBuf.Length);
					Assert.AssertArrayEquals(expectedBuf, verifyBuf);
				}
				finally
				{
					IOUtils.CloseStream(readStream);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRestartWithPartialBlockHflushed()
		{
			Configuration conf = new HdfsConfiguration();
			// Turn off persistent IPC, so that the DFSClient can survive NN restart
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, 0);
			MiniDFSCluster cluster = null;
			FSDataOutputStream stream;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				FileSystem fs = cluster.GetFileSystem();
				NameNode.GetAddress(conf).Port;
				// Creating a file with 4096 blockSize to write multiple blocks
				stream = fs.Create(FilePath, true, BlockSize, (short)1, BlockSize);
				stream.Write(DataBeforeRestart);
				stream.Write(unchecked((byte)1));
				stream.Hflush();
				// explicitly do NOT close the file before restarting the NN.
				cluster.RestartNameNode();
				// this will fail if the final block of the file is prematurely COMPLETEd
				stream.Write(unchecked((byte)2));
				stream.Hflush();
				stream.Close();
				NUnit.Framework.Assert.AreEqual(DataBeforeRestart.Length + 2, fs.GetFileStatus(FilePath
					).GetLen());
				FSDataInputStream readStream = fs.Open(FilePath);
				try
				{
					byte[] verifyBuf = new byte[DataBeforeRestart.Length + 2];
					IOUtils.ReadFully(readStream, verifyBuf, 0, verifyBuf.Length);
					byte[] expectedBuf = new byte[DataBeforeRestart.Length + 2];
					System.Array.Copy(DataBeforeRestart, 0, expectedBuf, 0, DataBeforeRestart.Length);
					System.Array.Copy(new byte[] { 1, 2 }, 0, expectedBuf, DataBeforeRestart.Length, 
						2);
					Assert.AssertArrayEquals(expectedBuf, verifyBuf);
				}
				finally
				{
					IOUtils.CloseStream(readStream);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRestartWithAppend()
		{
			Configuration conf = new HdfsConfiguration();
			// Turn off persistent IPC, so that the DFSClient can survive NN restart
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, 0);
			MiniDFSCluster cluster = null;
			FSDataOutputStream stream;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				FileSystem fs = cluster.GetFileSystem();
				NameNode.GetAddress(conf).Port;
				// Creating a file with 4096 blockSize to write multiple blocks
				stream = fs.Create(FilePath, true, BlockSize, (short)1, BlockSize);
				stream.Write(DataBeforeRestart, 0, DataBeforeRestart.Length / 2);
				stream.Close();
				stream = fs.Append(FilePath, BlockSize);
				stream.Write(DataBeforeRestart, DataBeforeRestart.Length / 2, DataBeforeRestart.Length
					 / 2);
				stream.Close();
				NUnit.Framework.Assert.AreEqual(DataBeforeRestart.Length, fs.GetFileStatus(FilePath
					).GetLen());
				cluster.RestartNameNode();
				NUnit.Framework.Assert.AreEqual(DataBeforeRestart.Length, fs.GetFileStatus(FilePath
					).GetLen());
				FSDataInputStream readStream = fs.Open(FilePath);
				try
				{
					byte[] verifyBuf = new byte[DataBeforeRestart.Length];
					IOUtils.ReadFully(readStream, verifyBuf, 0, verifyBuf.Length);
					Assert.AssertArrayEquals(DataBeforeRestart, verifyBuf);
				}
				finally
				{
					IOUtils.CloseStream(readStream);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Earlier versions of HDFS didn't persist block allocation to the edit log.
		/// 	</summary>
		/// <remarks>
		/// Earlier versions of HDFS didn't persist block allocation to the edit log.
		/// This makes sure that we can still load an edit log when the OP_CLOSE
		/// is the opcode which adds all of the blocks. This is a regression
		/// test for HDFS-2773.
		/// This test uses a tarred pseudo-distributed cluster from Hadoop 1.0
		/// which has a multi-block file. This is similar to the tests in
		/// <see cref="TestDFSUpgradeFromImage"/>
		/// but none of those images include
		/// a multi-block file.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEarlierVersionEditLog()
		{
			Configuration conf = new HdfsConfiguration();
			string tarFile = Runtime.GetProperty("test.cache.data", "build/test/cache") + "/"
				 + Hadoop10MultiblockTgz;
			string testDir = PathUtils.GetTestDirName(GetType());
			FilePath dfsDir = new FilePath(testDir, "image-1.0");
			if (dfsDir.Exists() && !FileUtil.FullyDelete(dfsDir))
			{
				throw new IOException("Could not delete dfs directory '" + dfsDir + "'");
			}
			FileUtil.UnTar(new FilePath(tarFile), new FilePath(testDir));
			FilePath nameDir = new FilePath(dfsDir, "name");
			GenericTestUtils.AssertExists(nameDir);
			FilePath dataDir = new FilePath(dfsDir, "data");
			GenericTestUtils.AssertExists(dataDir);
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetAbsolutePath());
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dataDir.GetAbsolutePath());
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(
				false).ManageDataDfsDirs(false).ManageNameDfsDirs(false).NumDataNodes(1).StartupOption
				(HdfsServerConstants.StartupOption.Upgrade).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				Path testPath = new Path("/user/todd/4blocks");
				// Read it without caring about the actual data within - we just need
				// to make sure that the block states and locations are OK.
				DFSTestUtil.ReadFile(fs, testPath);
				// Ensure that we can append to it - if the blocks were in some funny
				// state we'd get some kind of issue here. 
				FSDataOutputStream stm = fs.Append(testPath);
				try
				{
					stm.Write(1);
				}
				finally
				{
					IOUtils.CloseStream(stm);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
