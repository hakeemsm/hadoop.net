using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Hamcrest;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// This test verifies NameNode behavior when it gets unexpected block reports
	/// from DataNodes.
	/// </summary>
	/// <remarks>
	/// This test verifies NameNode behavior when it gets unexpected block reports
	/// from DataNodes. The same block is reported by two different storages on
	/// the same DataNode. Excess replicas on the same DN should be ignored by the NN.
	/// </remarks>
	public class TestBlockHasMultipleReplicasOnSameDN
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.TestBlockHasMultipleReplicasOnSameDN
			));

		private const short NumDatanodes = 2;

		private const int BlockSize = 1024;

		private const long NumBlocks = 5;

		private const long seed = unchecked((long)(0x1BADF00DL));

		private Configuration conf;

		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		private DFSClient client;

		private string bpid;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartUpCluster()
		{
			conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDatanodes).Build();
			fs = cluster.GetFileSystem();
			client = fs.GetClient();
			bpid = cluster.GetNamesystem().GetBlockPoolId();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutDownCluster()
		{
			if (cluster != null)
			{
				fs.Close();
				cluster.Shutdown();
				cluster = null;
			}
		}

		private string MakeFileName(string prefix)
		{
			return "/" + prefix + ".dat";
		}

		/// <summary>
		/// Verify NameNode behavior when a given DN reports multiple replicas
		/// of a given block.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockHasMultipleReplicasOnSameDN()
		{
			string filename = MakeFileName(GenericTestUtils.GetMethodName());
			Path filePath = new Path(filename);
			// Write out a file with a few blocks.
			DFSTestUtil.CreateFile(fs, filePath, BlockSize, BlockSize * NumBlocks, BlockSize, 
				NumDatanodes, seed);
			// Get the block list for the file with the block locations.
			LocatedBlocks locatedBlocks = client.GetLocatedBlocks(filePath.ToString(), 0, BlockSize
				 * NumBlocks);
			// Generate a fake block report from one of the DataNodes, such
			// that it reports one copy of each block on either storage.
			DataNode dn = cluster.GetDataNodes()[0];
			DatanodeRegistration dnReg = dn.GetDNRegistrationForBP(bpid);
			StorageBlockReport[] reports = new StorageBlockReport[cluster.GetStoragesPerDatanode
				()];
			AList<Replica> blocks = new AList<Replica>();
			foreach (LocatedBlock locatedBlock in locatedBlocks.GetLocatedBlocks())
			{
				Block localBlock = locatedBlock.GetBlock().GetLocalBlock();
				blocks.AddItem(new FinalizedReplica(localBlock, null, null));
			}
			BlockListAsLongs bll = BlockListAsLongs.Encode(blocks);
			for (int i = 0; i < cluster.GetStoragesPerDatanode(); ++i)
			{
				FsVolumeSpi v = dn.GetFSDataset().GetVolumes()[i];
				DatanodeStorage dns = new DatanodeStorage(v.GetStorageID());
				reports[i] = new StorageBlockReport(dns, bll);
			}
			// Should not assert!
			cluster.GetNameNodeRpc().BlockReport(dnReg, bpid, reports, new BlockReportContext
				(1, 0, Runtime.NanoTime()));
			// Get the block locations once again.
			locatedBlocks = client.GetLocatedBlocks(filename, 0, BlockSize * NumBlocks);
			// Make sure that each block has two replicas, one on each DataNode.
			foreach (LocatedBlock locatedBlock_1 in locatedBlocks.GetLocatedBlocks())
			{
				DatanodeInfo[] locations = locatedBlock_1.GetLocations();
				Assert.AssertThat(locations.Length, IS.Is((int)NumDatanodes));
				Assert.AssertThat(locations[0].GetDatanodeUuid(), CoreMatchers.Not(locations[1].GetDatanodeUuid
					()));
			}
		}
	}
}
