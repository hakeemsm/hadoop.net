using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class BlockManagerTestUtil
	{
		public static void SetNodeReplicationLimit(BlockManager blockManager, int limit)
		{
			blockManager.maxReplicationStreams = limit;
		}

		/// <returns>the datanode descriptor for the given the given storageID.</returns>
		public static DatanodeDescriptor GetDatanode(FSNamesystem ns, string storageID)
		{
			ns.ReadLock();
			try
			{
				return ns.GetBlockManager().GetDatanodeManager().GetDatanode(storageID);
			}
			finally
			{
				ns.ReadUnlock();
			}
		}

		/// <summary>Refresh block queue counts on the name-node.</summary>
		public static void UpdateState(BlockManager blockManager)
		{
			blockManager.UpdateState();
		}

		/// <returns>
		/// a tuple of the replica state (number racks, number live
		/// replicas, and number needed replicas) for the given block.
		/// </returns>
		public static int[] GetReplicaInfo(FSNamesystem namesystem, Block b)
		{
			BlockManager bm = namesystem.GetBlockManager();
			namesystem.ReadLock();
			try
			{
				return new int[] { GetNumberOfRacks(bm, b), bm.CountNodes(b).LiveReplicas(), bm.neededReplications
					.Contains(b) ? 1 : 0 };
			}
			finally
			{
				namesystem.ReadUnlock();
			}
		}

		/// <returns>
		/// the number of racks over which a given block is replicated
		/// decommissioning/decommissioned nodes are not counted. corrupt replicas
		/// are also ignored
		/// </returns>
		private static int GetNumberOfRacks(BlockManager blockManager, Block b)
		{
			ICollection<string> rackSet = new HashSet<string>(0);
			ICollection<DatanodeDescriptor> corruptNodes = GetCorruptReplicas(blockManager).GetNodes
				(b);
			foreach (DatanodeStorageInfo storage in blockManager.blocksMap.GetStorages(b))
			{
				DatanodeDescriptor cur = storage.GetDatanodeDescriptor();
				if (!cur.IsDecommissionInProgress() && !cur.IsDecommissioned())
				{
					if ((corruptNodes == null) || !corruptNodes.Contains(cur))
					{
						string rackName = cur.GetNetworkLocation();
						if (!rackSet.Contains(rackName))
						{
							rackSet.AddItem(rackName);
						}
					}
				}
			}
			return rackSet.Count;
		}

		/// <returns>replication monitor thread instance from block manager.</returns>
		public static Daemon GetReplicationThread(BlockManager blockManager)
		{
			return blockManager.replicationThread;
		}

		/// <summary>Stop the replication monitor thread</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void StopReplicationThread(BlockManager blockManager)
		{
			blockManager.EnableRMTerminationForTesting();
			blockManager.replicationThread.Interrupt();
			try
			{
				blockManager.replicationThread.Join();
			}
			catch (Exception)
			{
				throw new IOException("Interrupted while trying to stop ReplicationMonitor");
			}
		}

		/// <returns>corruptReplicas from block manager</returns>
		public static CorruptReplicasMap GetCorruptReplicas(BlockManager blockManager)
		{
			return blockManager.corruptReplicas;
		}

		/// <returns>
		/// computed block replication and block invalidation work that can be
		/// scheduled on data-nodes.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static int GetComputedDatanodeWork(BlockManager blockManager)
		{
			return blockManager.ComputeDatanodeWork();
		}

		public static int ComputeInvalidationWork(BlockManager bm)
		{
			return bm.ComputeInvalidateWork(int.MaxValue);
		}

		/// <summary>
		/// Compute all the replication and invalidation work for the
		/// given BlockManager.
		/// </summary>
		/// <remarks>
		/// Compute all the replication and invalidation work for the
		/// given BlockManager.
		/// This differs from the above functions in that it computes
		/// replication work for all DNs rather than a particular subset,
		/// regardless of invalidation/replication limit configurations.
		/// NB: you may want to set
		/// <see cref="Org.Apache.Hadoop.Hdfs.DFSConfigKeys.DfsNamenodeReplicationMaxStreamsKey
		/// 	"/>
		/// to
		/// a high value to ensure that all work is calculated.
		/// </remarks>
		public static int ComputeAllPendingWork(BlockManager bm)
		{
			int work = ComputeInvalidationWork(bm);
			work += bm.ComputeReplicationWork(int.MaxValue);
			return work;
		}

		/// <summary>
		/// Ensure that the given NameNode marks the specified DataNode as
		/// entirely dead/expired.
		/// </summary>
		/// <param name="nn">the NameNode to manipulate</param>
		/// <param name="dnName">the name of the DataNode</param>
		public static void NoticeDeadDatanode(NameNode nn, string dnName)
		{
			FSNamesystem namesystem = nn.GetNamesystem();
			namesystem.WriteLock();
			try
			{
				DatanodeManager dnm = namesystem.GetBlockManager().GetDatanodeManager();
				HeartbeatManager hbm = dnm.GetHeartbeatManager();
				DatanodeDescriptor[] dnds = hbm.GetDatanodes();
				DatanodeDescriptor theDND = null;
				foreach (DatanodeDescriptor dnd in dnds)
				{
					if (dnd.GetXferAddr().Equals(dnName))
					{
						theDND = dnd;
					}
				}
				NUnit.Framework.Assert.IsNotNull("Could not find DN with name: " + dnName, theDND
					);
				lock (hbm)
				{
					DFSTestUtil.SetDatanodeDead(theDND);
					hbm.HeartbeatCheck();
				}
			}
			finally
			{
				namesystem.WriteUnlock();
			}
		}

		/// <summary>
		/// Change whether the block placement policy will prefer the writer's
		/// local Datanode or not.
		/// </summary>
		/// <param name="prefer">if true, prefer local node</param>
		public static void SetWritingPrefersLocalNode(BlockManager bm, bool prefer)
		{
			BlockPlacementPolicy bpp = bm.GetBlockPlacementPolicy();
			Preconditions.CheckState(bpp is BlockPlacementPolicyDefault, "Must use default policy, got %s"
				, bpp.GetType());
			((BlockPlacementPolicyDefault)bpp).SetPreferLocalNode(prefer);
		}

		/// <summary>Call heartbeat check function of HeartbeatManager</summary>
		/// <param name="bm">the BlockManager to manipulate</param>
		public static void CheckHeartbeat(BlockManager bm)
		{
			bm.GetDatanodeManager().GetHeartbeatManager().HeartbeatCheck();
		}

		/// <summary>
		/// Call heartbeat check function of HeartbeatManager and get
		/// under replicated blocks count within write lock to make sure
		/// computeDatanodeWork doesn't interfere.
		/// </summary>
		/// <param name="namesystem">the FSNamesystem</param>
		/// <param name="bm">the BlockManager to manipulate</param>
		/// <returns>the number of under replicated blocks</returns>
		public static int CheckHeartbeatAndGetUnderReplicatedBlocksCount(FSNamesystem namesystem
			, BlockManager bm)
		{
			namesystem.WriteLock();
			try
			{
				bm.GetDatanodeManager().GetHeartbeatManager().HeartbeatCheck();
				return bm.GetUnderReplicatedNotMissingBlocks();
			}
			finally
			{
				namesystem.WriteUnlock();
			}
		}

		public static DatanodeStorageInfo UpdateStorage(DatanodeDescriptor dn, DatanodeStorage
			 s)
		{
			return dn.UpdateStorage(s);
		}

		/// <summary>Call heartbeat check function of HeartbeatManager</summary>
		/// <param name="bm">the BlockManager to manipulate</param>
		public static void RescanPostponedMisreplicatedBlocks(BlockManager bm)
		{
			bm.RescanPostponedMisreplicatedBlocks();
		}

		public static DatanodeDescriptor GetLocalDatanodeDescriptor(bool initializeStorage
			)
		{
			DatanodeDescriptor dn = new DatanodeDescriptor(DFSTestUtil.GetLocalDatanodeID());
			if (initializeStorage)
			{
				dn.UpdateStorage(new DatanodeStorage(DatanodeStorage.GenerateUuid()));
			}
			return dn;
		}

		public static DatanodeDescriptor GetDatanodeDescriptor(string ipAddr, string rackLocation
			, bool initializeStorage)
		{
			return GetDatanodeDescriptor(ipAddr, rackLocation, initializeStorage ? new DatanodeStorage
				(DatanodeStorage.GenerateUuid()) : null);
		}

		public static DatanodeDescriptor GetDatanodeDescriptor(string ipAddr, string rackLocation
			, DatanodeStorage storage)
		{
			return GetDatanodeDescriptor(ipAddr, rackLocation, storage, "host");
		}

		public static DatanodeDescriptor GetDatanodeDescriptor(string ipAddr, string rackLocation
			, DatanodeStorage storage, string hostname)
		{
			DatanodeDescriptor dn = DFSTestUtil.GetDatanodeDescriptor(ipAddr, DFSConfigKeys.DfsDatanodeDefaultPort
				, rackLocation, hostname);
			if (storage != null)
			{
				dn.UpdateStorage(storage);
			}
			return dn;
		}

		public static DatanodeStorageInfo NewDatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage
			 s)
		{
			return new DatanodeStorageInfo(dn, s);
		}

		public static StorageReport[] GetStorageReportsForDatanode(DatanodeDescriptor dnd
			)
		{
			AList<StorageReport> reports = new AList<StorageReport>();
			foreach (DatanodeStorageInfo storage in dnd.GetStorageInfos())
			{
				DatanodeStorage dns = new DatanodeStorage(storage.GetStorageID(), storage.GetState
					(), storage.GetStorageType());
				StorageReport report = new StorageReport(dns, false, storage.GetCapacity(), storage
					.GetDfsUsed(), storage.GetRemaining(), storage.GetBlockPoolUsed());
				reports.AddItem(report);
			}
			return Sharpen.Collections.ToArray(reports, StorageReport.EmptyArray);
		}

		/// <summary>Have DatanodeManager check decommission state.</summary>
		/// <param name="dm">the DatanodeManager to manipulate</param>
		/// <exception cref="Sharpen.ExecutionException"/>
		/// <exception cref="System.Exception"/>
		public static void RecheckDecommissionState(DatanodeManager dm)
		{
			dm.GetDecomManager().RunMonitor();
		}
	}
}
