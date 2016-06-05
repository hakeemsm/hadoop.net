using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Test if FSNamesystem handles heartbeat right</summary>
	public class TestComputeInvalidateWork
	{
		private Configuration conf;

		private readonly int NumOfDatanodes = 3;

		private MiniDFSCluster cluster;

		private FSNamesystem namesystem;

		private BlockManager bm;

		private DatanodeDescriptor[] nodes;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumOfDatanodes).Build();
			cluster.WaitActive();
			namesystem = cluster.GetNamesystem();
			bm = namesystem.GetBlockManager();
			nodes = bm.GetDatanodeManager().GetHeartbeatManager().GetDatanodes();
			NUnit.Framework.Assert.AreEqual(nodes.Length, NumOfDatanodes);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Teardown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test if
		/// <see cref="BlockManager.ComputeInvalidateWork(int)"/>
		/// can schedule invalidate work correctly
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCompInvalidate()
		{
			int blockInvalidateLimit = bm.GetDatanodeManager().blockInvalidateLimit;
			namesystem.WriteLock();
			try
			{
				for (int i = 0; i < nodes.Length; i++)
				{
					for (int j = 0; j < 3 * blockInvalidateLimit + 1; j++)
					{
						Block block = new Block(i * (blockInvalidateLimit + 1) + j, 0, GenerationStamp.LastReservedStamp
							);
						bm.AddToInvalidates(block, nodes[i]);
					}
				}
				NUnit.Framework.Assert.AreEqual(blockInvalidateLimit * NumOfDatanodes, bm.ComputeInvalidateWork
					(NumOfDatanodes + 1));
				NUnit.Framework.Assert.AreEqual(blockInvalidateLimit * NumOfDatanodes, bm.ComputeInvalidateWork
					(NumOfDatanodes));
				NUnit.Framework.Assert.AreEqual(blockInvalidateLimit * (NumOfDatanodes - 1), bm.ComputeInvalidateWork
					(NumOfDatanodes - 1));
				int workCount = bm.ComputeInvalidateWork(1);
				if (workCount == 1)
				{
					NUnit.Framework.Assert.AreEqual(blockInvalidateLimit + 1, bm.ComputeInvalidateWork
						(2));
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(workCount, blockInvalidateLimit);
					NUnit.Framework.Assert.AreEqual(2, bm.ComputeInvalidateWork(2));
				}
			}
			finally
			{
				namesystem.WriteUnlock();
			}
		}

		/// <summary>
		/// Reformatted DataNodes will replace the original UUID in the
		/// <see cref="DatanodeManager#datanodeMap"/>
		/// . This tests if block
		/// invalidation work on the original DataNode can be skipped.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDatanodeReformat()
		{
			namesystem.WriteLock();
			try
			{
				// Change the datanode UUID to emulate a reformat
				string poolId = cluster.GetNamesystem().GetBlockPoolId();
				DatanodeRegistration dnr = cluster.GetDataNode(nodes[0].GetIpcPort()).GetDNRegistrationForBP
					(poolId);
				dnr = new DatanodeRegistration(UUID.RandomUUID().ToString(), dnr);
				cluster.StopDataNode(nodes[0].GetXferAddr());
				Block block = new Block(0, 0, GenerationStamp.LastReservedStamp);
				bm.AddToInvalidates(block, nodes[0]);
				bm.GetDatanodeManager().RegisterDatanode(dnr);
				// Since UUID has changed, the invalidation work should be skipped
				NUnit.Framework.Assert.AreEqual(0, bm.ComputeInvalidateWork(1));
				NUnit.Framework.Assert.AreEqual(0, bm.GetPendingDeletionBlocksCount());
			}
			finally
			{
				namesystem.WriteUnlock();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDatanodeReRegistration()
		{
			// Create a test file
			DistributedFileSystem dfs = cluster.GetFileSystem();
			Path path = new Path("/testRR");
			// Create a file and shutdown the DNs, which populates InvalidateBlocks
			DFSTestUtil.CreateFile(dfs, path, dfs.GetDefaultBlockSize(), (short)NumOfDatanodes
				, unchecked((int)(0xED0ED0)));
			foreach (DataNode dn in cluster.GetDataNodes())
			{
				dn.Shutdown();
			}
			dfs.Delete(path, false);
			namesystem.WriteLock();
			InvalidateBlocks invalidateBlocks;
			int expected = NumOfDatanodes;
			try
			{
				invalidateBlocks = (InvalidateBlocks)Whitebox.GetInternalState(cluster.GetNamesystem
					().GetBlockManager(), "invalidateBlocks");
				NUnit.Framework.Assert.AreEqual("Expected invalidate blocks to be the number of DNs"
					, (long)expected, invalidateBlocks.NumBlocks());
			}
			finally
			{
				namesystem.WriteUnlock();
			}
			// Re-register each DN and see that it wipes the invalidation work
			foreach (DataNode dn_1 in cluster.GetDataNodes())
			{
				DatanodeID did = dn_1.GetDatanodeId();
				DatanodeRegistration reg = new DatanodeRegistration(new DatanodeID(UUID.RandomUUID
					().ToString(), did), new StorageInfo(HdfsServerConstants.NodeType.DataNode), new 
					ExportedBlockKeys(), VersionInfo.GetVersion());
				namesystem.WriteLock();
				try
				{
					bm.GetDatanodeManager().RegisterDatanode(reg);
					expected--;
					NUnit.Framework.Assert.AreEqual("Expected number of invalidate blocks to decrease"
						, (long)expected, invalidateBlocks.NumBlocks());
				}
				finally
				{
					namesystem.WriteUnlock();
				}
			}
		}
	}
}
