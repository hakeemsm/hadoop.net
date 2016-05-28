using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestReplicationPolicyConsiderLoad
	{
		private static NameNode namenode;

		private static DatanodeManager dnManager;

		private static IList<DatanodeRegistration> dnrList;

		private static DatanodeDescriptor[] dataNodes;

		private static DatanodeStorageInfo[] storages;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetupCluster()
		{
			Configuration conf = new HdfsConfiguration();
			string[] racks = new string[] { "/rack1", "/rack1", "/rack1", "/rack2", "/rack2", 
				"/rack2" };
			storages = DFSTestUtil.CreateDatanodeStorageInfos(racks);
			dataNodes = DFSTestUtil.ToDatanodeDescriptor(storages);
			FileSystem.SetDefaultUri(conf, "hdfs://localhost:0");
			conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "0.0.0.0:0");
			FilePath baseDir = PathUtils.GetTestDir(typeof(TestReplicationPolicy));
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, new FilePath(baseDir, "name").GetPath
				());
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForReadKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForWriteKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeReplicationConsiderloadKey, true);
			DFSTestUtil.FormatNameNode(conf);
			namenode = new NameNode(conf);
			int blockSize = 1024;
			dnrList = new AList<DatanodeRegistration>();
			dnManager = namenode.GetNamesystem().GetBlockManager().GetDatanodeManager();
			// Register DNs
			for (int i = 0; i < 6; i++)
			{
				DatanodeRegistration dnr = new DatanodeRegistration(dataNodes[i], new StorageInfo
					(HdfsServerConstants.NodeType.DataNode), new ExportedBlockKeys(), VersionInfo.GetVersion
					());
				dnrList.AddItem(dnr);
				dnManager.RegisterDatanode(dnr);
				dataNodes[i].GetStorageInfos()[0].SetUtilizationForTesting(2 * HdfsConstants.MinBlocksForWrite
					 * blockSize, 0L, 2 * HdfsConstants.MinBlocksForWrite * blockSize, 0L);
				dataNodes[i].UpdateHeartbeat(BlockManagerTestUtil.GetStorageReportsForDatanode(dataNodes
					[i]), 0L, 0L, 0, 0, null);
			}
		}

		private readonly double Epsilon = 0.0001;

		/// <summary>
		/// Tests that chooseTarget with considerLoad set to true correctly calculates
		/// load with decommissioned nodes.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTargetWithDecomNodes()
		{
			namenode.GetNamesystem().WriteLock();
			try
			{
				string blockPoolId = namenode.GetNamesystem().GetBlockPoolId();
				dnManager.HandleHeartbeat(dnrList[3], BlockManagerTestUtil.GetStorageReportsForDatanode
					(dataNodes[3]), blockPoolId, dataNodes[3].GetCacheCapacity(), dataNodes[3].GetCacheRemaining
					(), 2, 0, 0, null);
				dnManager.HandleHeartbeat(dnrList[4], BlockManagerTestUtil.GetStorageReportsForDatanode
					(dataNodes[4]), blockPoolId, dataNodes[4].GetCacheCapacity(), dataNodes[4].GetCacheRemaining
					(), 4, 0, 0, null);
				dnManager.HandleHeartbeat(dnrList[5], BlockManagerTestUtil.GetStorageReportsForDatanode
					(dataNodes[5]), blockPoolId, dataNodes[5].GetCacheCapacity(), dataNodes[5].GetCacheRemaining
					(), 4, 0, 0, null);
				// value in the above heartbeats
				int load = 2 + 4 + 4;
				FSNamesystem fsn = namenode.GetNamesystem();
				NUnit.Framework.Assert.AreEqual((double)load / 6, dnManager.GetFSClusterStats().GetInServiceXceiverAverage
					(), Epsilon);
				// Decommission DNs so BlockPlacementPolicyDefault.isGoodTarget()
				// returns false
				for (int i = 0; i < 3; i++)
				{
					DatanodeDescriptor d = dnManager.GetDatanode(dnrList[i]);
					dnManager.GetDecomManager().StartDecommission(d);
					d.SetDecommissioned();
				}
				NUnit.Framework.Assert.AreEqual((double)load / 3, dnManager.GetFSClusterStats().GetInServiceXceiverAverage
					(), Epsilon);
				// update references of writer DN to update the de-commissioned state
				IList<DatanodeDescriptor> liveNodes = new AList<DatanodeDescriptor>();
				dnManager.FetchDatanodes(liveNodes, null, false);
				DatanodeDescriptor writerDn = null;
				if (liveNodes.Contains(dataNodes[0]))
				{
					writerDn = liveNodes[liveNodes.IndexOf(dataNodes[0])];
				}
				// Call chooseTarget()
				DatanodeStorageInfo[] targets = namenode.GetNamesystem().GetBlockManager().GetBlockPlacementPolicy
					().ChooseTarget("testFile.txt", 3, writerDn, new AList<DatanodeStorageInfo>(), false
					, null, 1024, TestBlockStoragePolicy.DefaultStoragePolicy);
				NUnit.Framework.Assert.AreEqual(3, targets.Length);
				ICollection<DatanodeStorageInfo> targetSet = new HashSet<DatanodeStorageInfo>(Arrays
					.AsList(targets));
				for (int i_1 = 3; i_1 < storages.Length; i_1++)
				{
					NUnit.Framework.Assert.IsTrue(targetSet.Contains(storages[i_1]));
				}
			}
			finally
			{
				dataNodes[0].StopDecommission();
				dataNodes[1].StopDecommission();
				dataNodes[2].StopDecommission();
				namenode.GetNamesystem().WriteUnlock();
			}
		}

		[AfterClass]
		public static void TeardownCluster()
		{
			if (namenode != null)
			{
				namenode.Stop();
			}
		}
	}
}
