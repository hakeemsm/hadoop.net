using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Test AddBlockOp is written and read correctly</summary>
	public class TestAddBlock
	{
		private const short Replication = 3;

		private const int Blocksize = 1024;

		private MiniDFSCluster cluster;

		private Configuration conf;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, Blocksize);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test adding new blocks.</summary>
		/// <remarks>
		/// Test adding new blocks. Restart the NameNode in the test to make sure the
		/// AddBlockOp in the editlog is applied correctly.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddBlock()
		{
			DistributedFileSystem fs = cluster.GetFileSystem();
			Path file1 = new Path("/file1");
			Path file2 = new Path("/file2");
			Path file3 = new Path("/file3");
			Path file4 = new Path("/file4");
			DFSTestUtil.CreateFile(fs, file1, Blocksize - 1, Replication, 0L);
			DFSTestUtil.CreateFile(fs, file2, Blocksize, Replication, 0L);
			DFSTestUtil.CreateFile(fs, file3, Blocksize * 2 - 1, Replication, 0L);
			DFSTestUtil.CreateFile(fs, file4, Blocksize * 2, Replication, 0L);
			// restart NameNode
			cluster.RestartNameNode(true);
			FSDirectory fsdir = cluster.GetNamesystem().GetFSDirectory();
			// check file1
			INodeFile file1Node = fsdir.GetINode4Write(file1.ToString()).AsFile();
			BlockInfoContiguous[] file1Blocks = file1Node.GetBlocks();
			NUnit.Framework.Assert.AreEqual(1, file1Blocks.Length);
			NUnit.Framework.Assert.AreEqual(Blocksize - 1, file1Blocks[0].GetNumBytes());
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.BlockUCState.Complete, file1Blocks
				[0].GetBlockUCState());
			// check file2
			INodeFile file2Node = fsdir.GetINode4Write(file2.ToString()).AsFile();
			BlockInfoContiguous[] file2Blocks = file2Node.GetBlocks();
			NUnit.Framework.Assert.AreEqual(1, file2Blocks.Length);
			NUnit.Framework.Assert.AreEqual(Blocksize, file2Blocks[0].GetNumBytes());
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.BlockUCState.Complete, file2Blocks
				[0].GetBlockUCState());
			// check file3
			INodeFile file3Node = fsdir.GetINode4Write(file3.ToString()).AsFile();
			BlockInfoContiguous[] file3Blocks = file3Node.GetBlocks();
			NUnit.Framework.Assert.AreEqual(2, file3Blocks.Length);
			NUnit.Framework.Assert.AreEqual(Blocksize, file3Blocks[0].GetNumBytes());
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.BlockUCState.Complete, file3Blocks
				[0].GetBlockUCState());
			NUnit.Framework.Assert.AreEqual(Blocksize - 1, file3Blocks[1].GetNumBytes());
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.BlockUCState.Complete, file3Blocks
				[1].GetBlockUCState());
			// check file4
			INodeFile file4Node = fsdir.GetINode4Write(file4.ToString()).AsFile();
			BlockInfoContiguous[] file4Blocks = file4Node.GetBlocks();
			NUnit.Framework.Assert.AreEqual(2, file4Blocks.Length);
			NUnit.Framework.Assert.AreEqual(Blocksize, file4Blocks[0].GetNumBytes());
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.BlockUCState.Complete, file4Blocks
				[0].GetBlockUCState());
			NUnit.Framework.Assert.AreEqual(Blocksize, file4Blocks[1].GetNumBytes());
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.BlockUCState.Complete, file4Blocks
				[1].GetBlockUCState());
		}

		/// <summary>Test adding new blocks but without closing the corresponding the file</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddBlockUC()
		{
			DistributedFileSystem fs = cluster.GetFileSystem();
			Path file1 = new Path("/file1");
			DFSTestUtil.CreateFile(fs, file1, Blocksize - 1, Replication, 0L);
			FSDataOutputStream @out = null;
			try
			{
				// append files without closing the streams
				@out = fs.Append(file1);
				string appendContent = "appending-content";
				@out.WriteBytes(appendContent);
				((DFSOutputStream)@out.GetWrappedStream()).Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag
					.UpdateLength));
				// restart NN
				cluster.RestartNameNode(true);
				FSDirectory fsdir = cluster.GetNamesystem().GetFSDirectory();
				INodeFile fileNode = fsdir.GetINode4Write(file1.ToString()).AsFile();
				BlockInfoContiguous[] fileBlocks = fileNode.GetBlocks();
				NUnit.Framework.Assert.AreEqual(2, fileBlocks.Length);
				NUnit.Framework.Assert.AreEqual(Blocksize, fileBlocks[0].GetNumBytes());
				NUnit.Framework.Assert.AreEqual(HdfsServerConstants.BlockUCState.Complete, fileBlocks
					[0].GetBlockUCState());
				NUnit.Framework.Assert.AreEqual(appendContent.Length - 1, fileBlocks[1].GetNumBytes
					());
				NUnit.Framework.Assert.AreEqual(HdfsServerConstants.BlockUCState.UnderConstruction
					, fileBlocks[1].GetBlockUCState());
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
			}
		}
	}
}
