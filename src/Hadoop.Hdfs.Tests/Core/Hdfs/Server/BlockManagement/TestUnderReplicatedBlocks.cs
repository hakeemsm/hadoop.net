using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestUnderReplicatedBlocks
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestSetrepIncWithUnderReplicatedBlocks()
		{
			// 1 min timeout
			Configuration conf = new HdfsConfiguration();
			short ReplicationFactor = 2;
			string FileName = "/testFile";
			Path FilePath = new Path(FileName);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(ReplicationFactor
				 + 1).Build();
			try
			{
				// create a file with one block with a replication factor of 2
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, FilePath, 1L, ReplicationFactor, 1L);
				DFSTestUtil.WaitReplication(fs, FilePath, ReplicationFactor);
				// remove one replica from the blocksMap so block becomes under-replicated
				// but the block does not get put into the under-replicated blocks queue
				BlockManager bm = cluster.GetNamesystem().GetBlockManager();
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, FilePath);
				DatanodeDescriptor dn = bm.blocksMap.GetStorages(b.GetLocalBlock()).GetEnumerator
					().Next().GetDatanodeDescriptor();
				bm.AddToInvalidates(b.GetLocalBlock(), dn);
				Sharpen.Thread.Sleep(5000);
				bm.blocksMap.RemoveNode(b.GetLocalBlock(), dn);
				// increment this file's replication factor
				FsShell shell = new FsShell(conf);
				NUnit.Framework.Assert.AreEqual(0, shell.Run(new string[] { "-setrep", "-w", Sharpen.Extensions.ToString
					(1 + ReplicationFactor), FileName }));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// The test verifies the number of outstanding replication requests for a
		/// given DN shouldn't exceed the limit set by configuration property
		/// dfs.namenode.replication.max-streams-hard-limit.
		/// </summary>
		/// <remarks>
		/// The test verifies the number of outstanding replication requests for a
		/// given DN shouldn't exceed the limit set by configuration property
		/// dfs.namenode.replication.max-streams-hard-limit.
		/// The test does the followings:
		/// 1. Create a mini cluster with 2 DNs. Set large heartbeat interval so that
		/// replication requests won't be picked by any DN right away.
		/// 2. Create a file with 10 blocks and replication factor 2. Thus each
		/// of the 2 DNs have one replica of each block.
		/// 3. Add a DN to the cluster for later replication.
		/// 4. Remove a DN that has data.
		/// 5. Ask BlockManager to compute the replication work. This will assign
		/// replication requests to the only DN that has data.
		/// 6. Make sure the number of pending replication requests of that DN don't
		/// exceed the limit.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestNumberOfBlocksToBeReplicated()
		{
			// 1 min timeout
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsNamenodeMinBlockSizeKey, 0);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 1);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, 1);
			// Large value to make sure the pending replication request can stay in
			// DatanodeDescriptor.replicateBlocks before test timeout.
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 100);
			// Make sure BlockManager can pull all blocks from UnderReplicatedBlocks via
			// chooseUnderReplicatedBlocks at once.
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationWorkMultiplierPerIteration, 5);
			int NumOfBlocks = 10;
			short RepFactor = 2;
			string FileName = "/testFile";
			Path FilePath = new Path(FileName);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(RepFactor)
				.Build();
			try
			{
				// create a file with 10 blocks with a replication factor of 2
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, FilePath, NumOfBlocks, RepFactor, 1L);
				DFSTestUtil.WaitReplication(fs, FilePath, RepFactor);
				cluster.StartDataNodes(conf, 1, true, null, null, null, null);
				BlockManager bm = cluster.GetNamesystem().GetBlockManager();
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, FilePath);
				IEnumerator<DatanodeStorageInfo> storageInfos = bm.blocksMap.GetStorages(b.GetLocalBlock
					()).GetEnumerator();
				DatanodeDescriptor firstDn = storageInfos.Next().GetDatanodeDescriptor();
				DatanodeDescriptor secondDn = storageInfos.Next().GetDatanodeDescriptor();
				bm.GetDatanodeManager().RemoveDatanode(firstDn);
				NUnit.Framework.Assert.AreEqual(NumOfBlocks, bm.GetUnderReplicatedNotMissingBlocks
					());
				bm.ComputeDatanodeWork();
				NUnit.Framework.Assert.IsTrue("The number of blocks to be replicated should be less than "
					 + "or equal to " + bm.replicationStreamsHardLimit, secondDn.GetNumberOfBlocksToBeReplicated
					() <= bm.replicationStreamsHardLimit);
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
