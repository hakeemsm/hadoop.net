using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	public class TestUpdatePipelineWithSnapshots
	{
		// Regression test for HDFS-6647.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdatePipelineAfterDelete()
		{
			Configuration conf = new HdfsConfiguration();
			Path file = new Path("/test-file");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				NamenodeProtocols namenode = cluster.GetNameNodeRpc();
				DFSOutputStream @out = null;
				try
				{
					// Create a file and make sure a block is allocated for it.
					@out = (DFSOutputStream)(fs.Create(file).GetWrappedStream());
					@out.Write(1);
					@out.Hflush();
					// Create a snapshot that includes the file.
					SnapshotTestHelper.CreateSnapshot((DistributedFileSystem)fs, new Path("/"), "s1");
					// Grab the block info of this file for later use.
					FSDataInputStream @in = null;
					ExtendedBlock oldBlock = null;
					try
					{
						@in = fs.Open(file);
						oldBlock = DFSTestUtil.GetAllBlocks(@in)[0].GetBlock();
					}
					finally
					{
						IOUtils.CloseStream(@in);
					}
					// Allocate a new block ID/gen stamp so we can simulate pipeline
					// recovery.
					string clientName = ((DistributedFileSystem)fs).GetClient().GetClientName();
					LocatedBlock newLocatedBlock = namenode.UpdateBlockForPipeline(oldBlock, clientName
						);
					ExtendedBlock newBlock = new ExtendedBlock(oldBlock.GetBlockPoolId(), oldBlock.GetBlockId
						(), oldBlock.GetNumBytes(), newLocatedBlock.GetBlock().GetGenerationStamp());
					// Delete the file from the present FS. It will still exist the
					// previously-created snapshot. This will log an OP_DELETE for the
					// file in question.
					fs.Delete(file, true);
					// Simulate a pipeline recovery, wherein a new block is allocated
					// for the existing block, resulting in an OP_UPDATE_BLOCKS being
					// logged for the file in question.
					try
					{
						namenode.UpdatePipeline(clientName, oldBlock, newBlock, newLocatedBlock.GetLocations
							(), newLocatedBlock.GetStorageIDs());
					}
					catch (IOException ioe)
					{
						// normal
						GenericTestUtils.AssertExceptionContains("does not exist or it is not under construction"
							, ioe);
					}
					// Make sure the NN can restart with the edit logs as we have them now.
					cluster.RestartNameNode(true);
				}
				finally
				{
					IOUtils.CloseStream(@out);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
