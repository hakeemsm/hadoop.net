using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestCommitBlockWithInvalidGenStamp
	{
		private const int BlockSize = 1024;

		private MiniDFSCluster cluster;

		private FSDirectory dir;

		private DistributedFileSystem dfs;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			cluster = new MiniDFSCluster.Builder(conf).Build();
			cluster.WaitActive();
			dir = cluster.GetNamesystem().GetFSDirectory();
			dfs = cluster.GetFileSystem();
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCommitWithInvalidGenStamp()
		{
			Path file = new Path("/file");
			FSDataOutputStream @out = null;
			try
			{
				@out = dfs.Create(file, (short)1);
				INodeFile fileNode = dir.GetINode4Write(file.ToString()).AsFile();
				ExtendedBlock previous = null;
				Block newBlock = DFSTestUtil.AddBlockToFile(cluster.GetDataNodes(), dfs, cluster.
					GetNamesystem(), file.ToString(), fileNode, dfs.GetClient().GetClientName(), previous
					, 100);
				Block newBlockClone = new Block(newBlock);
				previous = new ExtendedBlock(cluster.GetNamesystem().GetBlockPoolId(), newBlockClone
					);
				previous.SetGenerationStamp(123);
				try
				{
					dfs.GetClient().GetNamenode().Complete(file.ToString(), dfs.GetClient().GetClientName
						(), previous, fileNode.GetId());
					NUnit.Framework.Assert.Fail("should throw exception because invalid genStamp");
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue(e.ToString().Contains("Commit block with mismatching GS. NN has "
						 + newBlock + ", client submits " + newBlockClone));
				}
				previous = new ExtendedBlock(cluster.GetNamesystem().GetBlockPoolId(), newBlock);
				bool complete = dfs.GetClient().GetNamenode().Complete(file.ToString(), dfs.GetClient
					().GetClientName(), previous, fileNode.GetId());
				NUnit.Framework.Assert.IsTrue("should complete successfully", complete);
			}
			finally
			{
				IOUtils.Cleanup(null, @out);
			}
		}
	}
}
