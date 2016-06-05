using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// Test if live nodes count per node is correct
	/// so NN makes right decision for under/over-replicated blocks
	/// Two of the "while" loops below use "busy wait"
	/// because they are detecting transient states.
	/// </summary>
	public class TestNodeCount
	{
		internal readonly short ReplicationFactor = (short)2;

		internal readonly long Timeout = 20000L;

		internal long timeout = 0;

		internal long failtime = 0;

		internal Block lastBlock = null;

		internal NumberReplicas lastNum = null;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeCount()
		{
			// start a mini dfs cluster of 2 nodes
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(ReplicationFactor
				).Build();
			try
			{
				FSNamesystem namesystem = cluster.GetNamesystem();
				BlockManager bm = namesystem.GetBlockManager();
				HeartbeatManager hm = bm.GetDatanodeManager().GetHeartbeatManager();
				FileSystem fs = cluster.GetFileSystem();
				// populate the cluster with a one block file
				Path FilePath = new Path("/testfile");
				DFSTestUtil.CreateFile(fs, FilePath, 1L, ReplicationFactor, 1L);
				DFSTestUtil.WaitReplication(fs, FilePath, ReplicationFactor);
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, FilePath);
				// keep a copy of all datanode descriptor
				DatanodeDescriptor[] datanodes = hm.GetDatanodes();
				// start two new nodes
				cluster.StartDataNodes(conf, 2, true, null, null);
				cluster.WaitActive();
				// bring down first datanode
				DatanodeDescriptor datanode = datanodes[0];
				MiniDFSCluster.DataNodeProperties dnprop = cluster.StopDataNode(datanode.GetXferAddr
					());
				// make sure that NN detects that the datanode is down
				BlockManagerTestUtil.NoticeDeadDatanode(cluster.GetNameNode(), datanode.GetXferAddr
					());
				// the block will be replicated
				DFSTestUtil.WaitReplication(fs, FilePath, ReplicationFactor);
				// restart the first datanode
				cluster.RestartDataNode(dnprop);
				cluster.WaitActive();
				// check if excessive replica is detected (transient)
				InitializeTimeout(Timeout);
				while (CountNodes(block.GetLocalBlock(), namesystem).ExcessReplicas() == 0)
				{
					CheckTimeout("excess replicas not detected");
				}
				// find out a non-excess node
				DatanodeDescriptor nonExcessDN = null;
				foreach (DatanodeStorageInfo storage in bm.blocksMap.GetStorages(block.GetLocalBlock
					()))
				{
					DatanodeDescriptor dn = storage.GetDatanodeDescriptor();
					ICollection<Block> blocks = bm.excessReplicateMap[dn.GetDatanodeUuid()];
					if (blocks == null || !blocks.Contains(block.GetLocalBlock()))
					{
						nonExcessDN = dn;
						break;
					}
				}
				NUnit.Framework.Assert.IsTrue(nonExcessDN != null);
				// bring down non excessive datanode
				dnprop = cluster.StopDataNode(nonExcessDN.GetXferAddr());
				// make sure that NN detects that the datanode is down
				BlockManagerTestUtil.NoticeDeadDatanode(cluster.GetNameNode(), nonExcessDN.GetXferAddr
					());
				// The block should be replicated
				InitializeTimeout(Timeout);
				while (CountNodes(block.GetLocalBlock(), namesystem).LiveReplicas() != ReplicationFactor
					)
				{
					CheckTimeout("live replica count not correct", 1000);
				}
				// restart the first datanode
				cluster.RestartDataNode(dnprop);
				cluster.WaitActive();
				// check if excessive replica is detected (transient)
				InitializeTimeout(Timeout);
				while (CountNodes(block.GetLocalBlock(), namesystem).ExcessReplicas() != 2)
				{
					CheckTimeout("excess replica count not equal to 2");
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		internal virtual void InitializeTimeout(long timeout)
		{
			this.timeout = timeout;
			this.failtime = Time.MonotonicNow() + ((timeout <= 0) ? long.MaxValue : timeout);
		}

		/* busy wait on transient conditions */
		/// <exception cref="Sharpen.TimeoutException"/>
		internal virtual void CheckTimeout(string testLabel)
		{
			CheckTimeout(testLabel, 0);
		}

		/* check for timeout, then wait for cycleTime msec */
		/// <exception cref="Sharpen.TimeoutException"/>
		internal virtual void CheckTimeout(string testLabel, long cycleTime)
		{
			if (Time.MonotonicNow() > failtime)
			{
				throw new TimeoutException("Timeout: " + testLabel + " for block " + lastBlock + 
					" after " + timeout + " msec.  Last counts: live = " + lastNum.LiveReplicas() + 
					", excess = " + lastNum.ExcessReplicas() + ", corrupt = " + lastNum.CorruptReplicas
					());
			}
			if (cycleTime > 0)
			{
				try
				{
					Sharpen.Thread.Sleep(cycleTime);
				}
				catch (Exception)
				{
				}
			}
		}

		//ignore
		/* threadsafe read of the replication counts for this block */
		internal virtual NumberReplicas CountNodes(Block block, FSNamesystem namesystem)
		{
			namesystem.ReadLock();
			try
			{
				lastBlock = block;
				lastNum = namesystem.GetBlockManager().CountNodes(block);
				return lastNum;
			}
			finally
			{
				namesystem.ReadUnlock();
			}
		}
	}
}
