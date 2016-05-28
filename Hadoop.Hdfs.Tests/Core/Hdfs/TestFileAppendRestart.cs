using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// Unit test to make sure that Append properly logs the right
	/// things to the edit log, such that files aren't lost or truncated
	/// on restart.
	/// </summary>
	public class TestFileAppendRestart
	{
		private const int BlockSize = 4096;

		private const string Hadoop23BrokenAppendTgz = "image-with-buggy-append.tgz";

		/// <exception cref="System.IO.IOException"/>
		private void WriteAndAppend(FileSystem fs, Path p, int lengthForCreate, int lengthForAppend
			)
		{
			// Creating a file with 4096 blockSize to write multiple blocks
			FSDataOutputStream stream = fs.Create(p, true, BlockSize, (short)1, BlockSize);
			try
			{
				AppendTestUtil.Write(stream, 0, lengthForCreate);
				stream.Close();
				stream = fs.Append(p);
				AppendTestUtil.Write(stream, lengthForCreate, lengthForAppend);
				stream.Close();
			}
			finally
			{
				IOUtils.CloseStream(stream);
			}
			int totalLength = lengthForCreate + lengthForAppend;
			NUnit.Framework.Assert.AreEqual(totalLength, fs.GetFileStatus(p).GetLen());
		}

		/// <summary>Regression test for HDFS-2991.</summary>
		/// <remarks>
		/// Regression test for HDFS-2991. Creates and appends to files
		/// where blocks start/end on block boundaries.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendRestart()
		{
			Configuration conf = new HdfsConfiguration();
			// Turn off persistent IPC, so that the DFSClient can survive NN restart
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, 0);
			MiniDFSCluster cluster = null;
			FSDataOutputStream stream = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				FileSystem fs = cluster.GetFileSystem();
				FilePath editLog = new FilePath(FSImageTestUtil.GetNameNodeCurrentDirs(cluster, 0
					)[0], NNStorage.GetInProgressEditsFileName(1));
				EnumMap<FSEditLogOpCodes, Holder<int>> counts;
				Path p1 = new Path("/block-boundaries");
				WriteAndAppend(fs, p1, BlockSize, BlockSize);
				counts = FSImageTestUtil.CountEditLogOpTypes(editLog);
				// OP_ADD to create file
				// OP_ADD_BLOCK for first block
				// OP_CLOSE to close file
				// OP_APPEND to reopen file
				// OP_ADD_BLOCK for second block
				// OP_CLOSE to close file
				NUnit.Framework.Assert.AreEqual(1, (int)counts[FSEditLogOpCodes.OpAdd].held);
				NUnit.Framework.Assert.AreEqual(1, (int)counts[FSEditLogOpCodes.OpAppend].held);
				NUnit.Framework.Assert.AreEqual(2, (int)counts[FSEditLogOpCodes.OpAddBlock].held);
				NUnit.Framework.Assert.AreEqual(2, (int)counts[FSEditLogOpCodes.OpClose].held);
				Path p2 = new Path("/not-block-boundaries");
				WriteAndAppend(fs, p2, BlockSize / 2, BlockSize);
				counts = FSImageTestUtil.CountEditLogOpTypes(editLog);
				// OP_ADD to create file
				// OP_ADD_BLOCK for first block
				// OP_CLOSE to close file
				// OP_APPEND to re-establish the lease
				// OP_UPDATE_BLOCKS from the updatePipeline call (increments genstamp of last block)
				// OP_ADD_BLOCK at the start of the second block
				// OP_CLOSE to close file
				// Total: 2 OP_ADDs, 1 OP_UPDATE_BLOCKS, 2 OP_ADD_BLOCKs, and 2 OP_CLOSEs
				//       in addition to the ones above
				NUnit.Framework.Assert.AreEqual(2, (int)counts[FSEditLogOpCodes.OpAdd].held);
				NUnit.Framework.Assert.AreEqual(2, (int)counts[FSEditLogOpCodes.OpAppend].held);
				NUnit.Framework.Assert.AreEqual(1, (int)counts[FSEditLogOpCodes.OpUpdateBlocks].held
					);
				NUnit.Framework.Assert.AreEqual(2 + 2, (int)counts[FSEditLogOpCodes.OpAddBlock].held
					);
				NUnit.Framework.Assert.AreEqual(2 + 2, (int)counts[FSEditLogOpCodes.OpClose].held
					);
				cluster.RestartNameNode();
				AppendTestUtil.Check(fs, p1, 2 * BlockSize);
				AppendTestUtil.Check(fs, p2, 3 * BlockSize / 2);
			}
			finally
			{
				IOUtils.CloseStream(stream);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Earlier versions of HDFS had a bug (HDFS-2991) which caused
		/// append(), when called exactly at a block boundary,
		/// to not log an OP_ADD.
		/// </summary>
		/// <remarks>
		/// Earlier versions of HDFS had a bug (HDFS-2991) which caused
		/// append(), when called exactly at a block boundary,
		/// to not log an OP_ADD. This ensures that we can read from
		/// such buggy versions correctly, by loading an image created
		/// using a namesystem image created with 0.23.1-rc2 exhibiting
		/// the issue.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLoadLogsFromBuggyEarlierVersions()
		{
			Configuration conf = new HdfsConfiguration();
			string tarFile = Runtime.GetProperty("test.cache.data", "build/test/cache") + "/"
				 + Hadoop23BrokenAppendTgz;
			string testDir = PathUtils.GetTestDirName(GetType());
			FilePath dfsDir = new FilePath(testDir, "image-with-buggy-append");
			if (dfsDir.Exists() && !FileUtil.FullyDelete(dfsDir))
			{
				throw new IOException("Could not delete dfs directory '" + dfsDir + "'");
			}
			FileUtil.UnTar(new FilePath(tarFile), new FilePath(testDir));
			FilePath nameDir = new FilePath(dfsDir, "name");
			GenericTestUtils.AssertExists(nameDir);
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetAbsolutePath());
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(
				false).ManageDataDfsDirs(false).ManageNameDfsDirs(false).NumDataNodes(0).WaitSafeMode
				(false).StartupOption(HdfsServerConstants.StartupOption.Upgrade).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				Path testPath = new Path("/tmp/io_data/test_io_0");
				NUnit.Framework.Assert.AreEqual(2 * 1024 * 1024, fs.GetFileStatus(testPath).GetLen
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test to append to the file, when one of datanode in the existing pipeline
		/// is down.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendWithPipelineRecovery()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			FSDataOutputStream @out = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).ManageDataDfsDirs(true).ManageNameDfsDirs
					(true).NumDataNodes(4).Racks(new string[] { "/rack1", "/rack1", "/rack2", "/rack2"
					 }).Build();
				cluster.WaitActive();
				DistributedFileSystem fs = cluster.GetFileSystem();
				Path path = new Path("/test1");
				@out = fs.Create(path, true, BlockSize, (short)3, BlockSize);
				AppendTestUtil.Write(@out, 0, 1024);
				@out.Close();
				cluster.StopDataNode(3);
				@out = fs.Append(path);
				AppendTestUtil.Write(@out, 1024, 1024);
				@out.Close();
				cluster.RestartNameNode(true);
				AppendTestUtil.Check(fs, path, 2048);
			}
			finally
			{
				IOUtils.CloseStream(@out);
				if (null != cluster)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
