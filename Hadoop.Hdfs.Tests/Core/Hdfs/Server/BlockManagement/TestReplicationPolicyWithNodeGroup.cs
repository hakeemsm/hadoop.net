using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestReplicationPolicyWithNodeGroup
	{
		private const int BlockSize = 1024;

		private const int NumOfDatanodes = 8;

		private const int NumOfDatanodesBoundary = 6;

		private const int NumOfDatanodesMoreTargets = 12;

		private const int NumOfDatanodesForDependencies = 6;

		private readonly Configuration Conf = new HdfsConfiguration();

		private NetworkTopology cluster;

		private NameNode namenode;

		private BlockPlacementPolicy replicator;

		private const string filename = "/dummyfile.txt";

		private static readonly DatanodeStorageInfo[] storages;

		private static readonly DatanodeDescriptor[] dataNodes;

		static TestReplicationPolicyWithNodeGroup()
		{
			string[] racks = new string[] { "/d1/r1/n1", "/d1/r1/n1", "/d1/r1/n2", "/d1/r2/n3"
				, "/d1/r2/n3", "/d1/r2/n4", "/d2/r3/n5", "/d2/r3/n6" };
			storages = DFSTestUtil.CreateDatanodeStorageInfos(racks);
			dataNodes = DFSTestUtil.ToDatanodeDescriptor(storages);
		}

		private static readonly DatanodeStorageInfo[] storagesInBoundaryCase;

		private static readonly DatanodeDescriptor[] dataNodesInBoundaryCase;

		static TestReplicationPolicyWithNodeGroup()
		{
			string[] racksInBoundaryCase = new string[] { "/d1/r1/n1", "/d1/r1/n1", "/d1/r1/n1"
				, "/d1/r1/n2", "/d1/r2/n3", "/d1/r2/n3" };
			storagesInBoundaryCase = DFSTestUtil.CreateDatanodeStorageInfos(racksInBoundaryCase
				);
			dataNodesInBoundaryCase = DFSTestUtil.ToDatanodeDescriptor(storagesInBoundaryCase
				);
		}

		private static readonly DatanodeStorageInfo[] storagesInMoreTargetsCase;

		private static readonly DatanodeDescriptor[] dataNodesInMoreTargetsCase;

		static TestReplicationPolicyWithNodeGroup()
		{
			string[] racksInMoreTargetsCase = new string[] { "/r1/n1", "/r1/n1", "/r1/n2", "/r1/n2"
				, "/r1/n3", "/r1/n3", "/r2/n4", "/r2/n4", "/r2/n5", "/r2/n5", "/r2/n6", "/r2/n6"
				 };
			storagesInMoreTargetsCase = DFSTestUtil.CreateDatanodeStorageInfos(racksInMoreTargetsCase
				);
			dataNodesInMoreTargetsCase = DFSTestUtil.ToDatanodeDescriptor(storagesInMoreTargetsCase
				);
		}

		private static readonly DatanodeDescriptor Node = new DatanodeDescriptor(DFSTestUtil
			.GetDatanodeDescriptor("9.9.9.9", "/d2/r4/n7"));

		private static readonly DatanodeStorageInfo[] storagesForDependencies;

		private static readonly DatanodeDescriptor[] dataNodesForDependencies;

		static TestReplicationPolicyWithNodeGroup()
		{
			string[] racksForDependencies = new string[] { "/d1/r1/n1", "/d1/r1/n1", "/d1/r1/n2"
				, "/d1/r1/n2", "/d1/r1/n3", "/d1/r1/n4" };
			string[] hostNamesForDependencies = new string[] { "h1", "h2", "h3", "h4", "h5", 
				"h6" };
			storagesForDependencies = DFSTestUtil.CreateDatanodeStorageInfos(racksForDependencies
				, hostNamesForDependencies);
			dataNodesForDependencies = DFSTestUtil.ToDatanodeDescriptor(storagesForDependencies
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			FileSystem.SetDefaultUri(Conf, "hdfs://localhost:0");
			Conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "0.0.0.0:0");
			// Set properties to make HDFS aware of NodeGroup.
			Conf.Set(DFSConfigKeys.DfsBlockReplicatorClassnameKey, typeof(BlockPlacementPolicyWithNodeGroup
				).FullName);
			Conf.Set(CommonConfigurationKeysPublic.NetTopologyImplKey, typeof(NetworkTopologyWithNodeGroup
				).FullName);
			Conf.SetBoolean(DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForWriteKey, true);
			FilePath baseDir = PathUtils.GetTestDir(typeof(TestReplicationPolicyWithNodeGroup
				));
			Conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, new FilePath(baseDir, "name").GetPath
				());
			DFSTestUtil.FormatNameNode(Conf);
			namenode = new NameNode(Conf);
			BlockManager bm = namenode.GetNamesystem().GetBlockManager();
			replicator = bm.GetBlockPlacementPolicy();
			cluster = bm.GetDatanodeManager().GetNetworkTopology();
			// construct network topology
			for (int i = 0; i < NumOfDatanodes; i++)
			{
				cluster.Add(dataNodes[i]);
			}
			SetupDataNodeCapacity();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			namenode.Stop();
		}

		private static void UpdateHeartbeatWithUsage(DatanodeDescriptor dn, long capacity
			, long dfsUsed, long remaining, long blockPoolUsed, long dnCacheCapacity, long dnCacheUsed
			, int xceiverCount, int volFailures)
		{
			dn.GetStorageInfos()[0].SetUtilizationForTesting(capacity, dfsUsed, remaining, blockPoolUsed
				);
			dn.UpdateHeartbeat(BlockManagerTestUtil.GetStorageReportsForDatanode(dn), dnCacheCapacity
				, dnCacheUsed, xceiverCount, volFailures, null);
		}

		private static void SetupDataNodeCapacity()
		{
			for (int i = 0; i < NumOfDatanodes; i++)
			{
				UpdateHeartbeatWithUsage(dataNodes[i], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
					, 0L, 2 * HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0, 0);
			}
		}

		/// <summary>Scan the targets list: all targets should be on different NodeGroups.</summary>
		/// <remarks>
		/// Scan the targets list: all targets should be on different NodeGroups.
		/// Return false if two targets are found on the same NodeGroup.
		/// </remarks>
		private static bool CheckTargetsOnDifferentNodeGroup(DatanodeStorageInfo[] targets
			)
		{
			if (targets.Length == 0)
			{
				return true;
			}
			ICollection<string> targetSet = new HashSet<string>();
			foreach (DatanodeStorageInfo storage in targets)
			{
				DatanodeDescriptor node = storage.GetDatanodeDescriptor();
				string nodeGroup = NetworkTopology.GetLastHalf(node.GetNetworkLocation());
				if (targetSet.Contains(nodeGroup))
				{
					return false;
				}
				else
				{
					targetSet.AddItem(nodeGroup);
				}
			}
			return true;
		}

		private bool IsOnSameRack(DatanodeStorageInfo left, DatanodeStorageInfo right)
		{
			return IsOnSameRack(left.GetDatanodeDescriptor(), right);
		}

		private bool IsOnSameRack(DatanodeDescriptor left, DatanodeStorageInfo right)
		{
			return cluster.IsOnSameRack(left, right.GetDatanodeDescriptor());
		}

		private bool IsOnSameNodeGroup(DatanodeStorageInfo left, DatanodeStorageInfo right
			)
		{
			return IsOnSameNodeGroup(left.GetDatanodeDescriptor(), right);
		}

		private bool IsOnSameNodeGroup(DatanodeDescriptor left, DatanodeStorageInfo right
			)
		{
			return cluster.IsOnSameNodeGroup(left, right.GetDatanodeDescriptor());
		}

		private DatanodeStorageInfo[] ChooseTarget(int numOfReplicas)
		{
			return ChooseTarget(numOfReplicas, dataNodes[0]);
		}

		private DatanodeStorageInfo[] ChooseTarget(int numOfReplicas, DatanodeDescriptor 
			writer)
		{
			return ChooseTarget(numOfReplicas, writer, new AList<DatanodeStorageInfo>());
		}

		private DatanodeStorageInfo[] ChooseTarget(int numOfReplicas, IList<DatanodeStorageInfo
			> chosenNodes)
		{
			return ChooseTarget(numOfReplicas, dataNodes[0], chosenNodes);
		}

		private DatanodeStorageInfo[] ChooseTarget(int numOfReplicas, DatanodeDescriptor 
			writer, IList<DatanodeStorageInfo> chosenNodes)
		{
			return ChooseTarget(numOfReplicas, writer, chosenNodes, null);
		}

		private DatanodeStorageInfo[] ChooseTarget(int numOfReplicas, DatanodeDescriptor 
			writer, IList<DatanodeStorageInfo> chosenNodes, ICollection<Node> excludedNodes)
		{
			return replicator.ChooseTarget(filename, numOfReplicas, writer, chosenNodes, false
				, excludedNodes, BlockSize, TestBlockStoragePolicy.DefaultStoragePolicy);
		}

		/// <summary>In this testcase, client is dataNodes[0].</summary>
		/// <remarks>
		/// In this testcase, client is dataNodes[0]. So the 1st replica should be
		/// placed on dataNodes[0], the 2nd replica should be placed on
		/// different rack and third should be placed on different node (and node group)
		/// of rack chosen for 2nd node.
		/// The only excpetion is when the <i>numOfReplicas</i> is 2,
		/// the 1st is on dataNodes[0] and the 2nd is on a different rack.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTarget1()
		{
			UpdateHeartbeatWithUsage(dataNodes[0], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
				, 0L, HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 4, 0);
			// overloaded
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.AreEqual(storages[0], targets[0]);
			targets = ChooseTarget(2);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.AreEqual(storages[0], targets[0]);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			targets = ChooseTarget(3);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			NUnit.Framework.Assert.AreEqual(storages[0], targets[0]);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[1], targets[2]));
			NUnit.Framework.Assert.IsFalse(IsOnSameNodeGroup(targets[1], targets[2]));
			targets = ChooseTarget(4);
			NUnit.Framework.Assert.AreEqual(targets.Length, 4);
			NUnit.Framework.Assert.AreEqual(storages[0], targets[0]);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[1], targets[2]) || IsOnSameRack
				(targets[2], targets[3]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[2]));
			// Make sure no more than one replicas are on the same nodegroup 
			VerifyNoTwoTargetsOnSameNodeGroup(targets);
			UpdateHeartbeatWithUsage(dataNodes[0], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
				, 0L, HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0, 0);
		}

		private void VerifyNoTwoTargetsOnSameNodeGroup(DatanodeStorageInfo[] targets)
		{
			ICollection<string> nodeGroupSet = new HashSet<string>();
			foreach (DatanodeStorageInfo target in targets)
			{
				nodeGroupSet.AddItem(target.GetDatanodeDescriptor().GetNetworkLocation());
			}
			NUnit.Framework.Assert.AreEqual(nodeGroupSet.Count, targets.Length);
		}

		/// <summary>
		/// In this testcase, client is dataNodes[0], but the dataNodes[1] is
		/// not allowed to be chosen.
		/// </summary>
		/// <remarks>
		/// In this testcase, client is dataNodes[0], but the dataNodes[1] is
		/// not allowed to be chosen. So the 1st replica should be
		/// placed on dataNodes[0], the 2nd replica should be placed on a different
		/// rack, the 3rd should be on same rack as the 2nd replica but in different
		/// node group, and the rest should be placed on a third rack.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTarget2()
		{
			DatanodeStorageInfo[] targets;
			BlockPlacementPolicyDefault repl = (BlockPlacementPolicyDefault)replicator;
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			ICollection<Node> excludedNodes = new HashSet<Node>();
			excludedNodes.AddItem(dataNodes[1]);
			targets = repl.ChooseTarget(filename, 4, dataNodes[0], chosenNodes, false, excludedNodes
				, BlockSize, TestBlockStoragePolicy.DefaultStoragePolicy);
			NUnit.Framework.Assert.AreEqual(targets.Length, 4);
			NUnit.Framework.Assert.AreEqual(storages[0], targets[0]);
			NUnit.Framework.Assert.IsTrue(cluster.IsNodeGroupAware());
			// Make sure no replicas are on the same nodegroup 
			for (int i = 1; i < 4; i++)
			{
				NUnit.Framework.Assert.IsFalse(IsOnSameNodeGroup(targets[0], targets[i]));
			}
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[1], targets[2]) || IsOnSameRack
				(targets[2], targets[3]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[1], targets[3]));
			excludedNodes.Clear();
			chosenNodes.Clear();
			excludedNodes.AddItem(dataNodes[1]);
			chosenNodes.AddItem(storages[2]);
			targets = repl.ChooseTarget(filename, 1, dataNodes[0], chosenNodes, true, excludedNodes
				, BlockSize, TestBlockStoragePolicy.DefaultStoragePolicy);
			System.Console.Out.WriteLine("targets=" + Arrays.AsList(targets));
			NUnit.Framework.Assert.AreEqual(2, targets.Length);
			//make sure that the chosen node is in the target.
			int i_1 = 0;
			for (; i_1 < targets.Length && !storages[2].Equals(targets[i_1]); i_1++)
			{
			}
			NUnit.Framework.Assert.IsTrue(i_1 < targets.Length);
		}

		/// <summary>
		/// In this testcase, client is dataNodes[0], but dataNodes[0] is not qualified
		/// to be chosen.
		/// </summary>
		/// <remarks>
		/// In this testcase, client is dataNodes[0], but dataNodes[0] is not qualified
		/// to be chosen. So the 1st replica should be placed on dataNodes[1],
		/// the 2nd replica should be placed on a different rack,
		/// the 3rd replica should be placed on the same rack as the 2nd replica but in different nodegroup,
		/// and the rest should be placed on the third rack.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTarget3()
		{
			// make data node 0 to be not qualified to choose
			UpdateHeartbeatWithUsage(dataNodes[0], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
				, 0L, (HdfsConstants.MinBlocksForWrite - 1) * BlockSize, 0L, 0L, 0L, 0, 0);
			// no space
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.AreEqual(storages[1], targets[0]);
			targets = ChooseTarget(2);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.AreEqual(storages[1], targets[0]);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			targets = ChooseTarget(3);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			NUnit.Framework.Assert.AreEqual(storages[1], targets[0]);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[1], targets[2]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			targets = ChooseTarget(4);
			NUnit.Framework.Assert.AreEqual(targets.Length, 4);
			NUnit.Framework.Assert.AreEqual(storages[1], targets[0]);
			NUnit.Framework.Assert.IsTrue(cluster.IsNodeGroupAware());
			VerifyNoTwoTargetsOnSameNodeGroup(targets);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[1], targets[2]) || IsOnSameRack
				(targets[2], targets[3]));
			UpdateHeartbeatWithUsage(dataNodes[0], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
				, 0L, HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0, 0);
		}

		/// <summary>
		/// In this testcase, client is dataNodes[0], but none of the nodes on rack 1
		/// is qualified to be chosen.
		/// </summary>
		/// <remarks>
		/// In this testcase, client is dataNodes[0], but none of the nodes on rack 1
		/// is qualified to be chosen. So the 1st replica should be placed on either
		/// rack 2 or rack 3.
		/// the 2nd replica should be placed on a different rack,
		/// the 3rd replica should be placed on the same rack as the 1st replica, but
		/// in different node group.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTarget4()
		{
			// make data node 0-2 to be not qualified to choose: not enough disk space
			for (int i = 0; i < 3; i++)
			{
				UpdateHeartbeatWithUsage(dataNodes[i], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
					, 0L, (HdfsConstants.MinBlocksForWrite - 1) * BlockSize, 0L, 0L, 0L, 0, 0);
			}
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(dataNodes[0], targets[0]));
			targets = ChooseTarget(2);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(dataNodes[0], targets[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			targets = ChooseTarget(3);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			for (int i_1 = 0; i_1 < 3; i_1++)
			{
				NUnit.Framework.Assert.IsFalse(IsOnSameRack(dataNodes[0], targets[i_1]));
			}
			VerifyNoTwoTargetsOnSameNodeGroup(targets);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[0], targets[1]) || IsOnSameRack
				(targets[1], targets[2]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[2]));
		}

		/// <summary>In this testcase, client is is a node outside of file system.</summary>
		/// <remarks>
		/// In this testcase, client is is a node outside of file system.
		/// So the 1st replica can be placed on any node.
		/// the 2nd replica should be placed on a different rack,
		/// the 3rd replica should be placed on the same rack as the 2nd replica,
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTarget5()
		{
			SetupDataNodeCapacity();
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0, Node);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1, Node);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			targets = ChooseTarget(2, Node);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			targets = ChooseTarget(3, Node);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[1], targets[2]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			VerifyNoTwoTargetsOnSameNodeGroup(targets);
		}

		/// <summary>This testcase tests re-replication, when dataNodes[0] is already chosen.
		/// 	</summary>
		/// <remarks>
		/// This testcase tests re-replication, when dataNodes[0] is already chosen.
		/// So the 1st replica can be placed on random rack.
		/// the 2nd replica should be placed on different node and nodegroup by same rack as
		/// the 1st replica. The 3rd replica can be placed randomly.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRereplicate1()
		{
			SetupDataNodeCapacity();
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			chosenNodes.AddItem(storages[0]);
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(dataNodes[0], targets[0]));
			targets = ChooseTarget(2, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(dataNodes[0], targets[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			targets = ChooseTarget(3, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(dataNodes[0], targets[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameNodeGroup(dataNodes[0], targets[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[2]));
		}

		/// <summary>
		/// This testcase tests re-replication,
		/// when dataNodes[0] and dataNodes[1] are already chosen.
		/// </summary>
		/// <remarks>
		/// This testcase tests re-replication,
		/// when dataNodes[0] and dataNodes[1] are already chosen.
		/// So the 1st replica should be placed on a different rack of rack 1.
		/// the rest replicas can be placed randomly,
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRereplicate2()
		{
			SetupDataNodeCapacity();
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			chosenNodes.AddItem(storages[0]);
			chosenNodes.AddItem(storages[1]);
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(dataNodes[0], targets[0]));
			targets = ChooseTarget(2, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(dataNodes[0], targets[0]) && IsOnSameRack
				(dataNodes[0], targets[1]));
		}

		/// <summary>
		/// This testcase tests re-replication,
		/// when dataNodes[0] and dataNodes[3] are already chosen.
		/// </summary>
		/// <remarks>
		/// This testcase tests re-replication,
		/// when dataNodes[0] and dataNodes[3] are already chosen.
		/// So the 1st replica should be placed on the rack that the writer resides.
		/// the rest replicas can be placed randomly,
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRereplicate3()
		{
			SetupDataNodeCapacity();
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			chosenNodes.AddItem(storages[0]);
			chosenNodes.AddItem(storages[3]);
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(dataNodes[0], targets[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(dataNodes[3], targets[0]));
			targets = ChooseTarget(1, dataNodes[3], chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(dataNodes[3], targets[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameNodeGroup(dataNodes[3], targets[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(dataNodes[0], targets[0]));
			targets = ChooseTarget(2, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(dataNodes[0], targets[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameNodeGroup(dataNodes[0], targets[0]));
			targets = ChooseTarget(2, dataNodes[3], chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(dataNodes[3], targets[0]));
		}

		/// <summary>
		/// Test for the chooseReplicaToDelete are processed based on
		/// block locality and free space
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseReplicaToDelete()
		{
			IList<DatanodeStorageInfo> replicaList = new AList<DatanodeStorageInfo>();
			IDictionary<string, IList<DatanodeStorageInfo>> rackMap = new Dictionary<string, 
				IList<DatanodeStorageInfo>>();
			dataNodes[0].SetRemaining(4 * 1024 * 1024);
			replicaList.AddItem(storages[0]);
			dataNodes[1].SetRemaining(3 * 1024 * 1024);
			replicaList.AddItem(storages[1]);
			dataNodes[2].SetRemaining(2 * 1024 * 1024);
			replicaList.AddItem(storages[2]);
			dataNodes[5].SetRemaining(1 * 1024 * 1024);
			replicaList.AddItem(storages[5]);
			IList<DatanodeStorageInfo> first = new AList<DatanodeStorageInfo>();
			IList<DatanodeStorageInfo> second = new AList<DatanodeStorageInfo>();
			replicator.SplitNodesWithRack(replicaList, rackMap, first, second);
			NUnit.Framework.Assert.AreEqual(3, first.Count);
			NUnit.Framework.Assert.AreEqual(1, second.Count);
			IList<StorageType> excessTypes = new AList<StorageType>();
			excessTypes.AddItem(StorageType.Default);
			DatanodeStorageInfo chosen = replicator.ChooseReplicaToDelete(null, null, (short)
				3, first, second, excessTypes);
			// Within first set {dataNodes[0], dataNodes[1], dataNodes[2]}, 
			// dataNodes[0] and dataNodes[1] are in the same nodegroup, 
			// but dataNodes[1] is chosen as less free space
			NUnit.Framework.Assert.AreEqual(chosen, storages[1]);
			replicator.AdjustSetsWithChosenReplica(rackMap, first, second, chosen);
			NUnit.Framework.Assert.AreEqual(2, first.Count);
			NUnit.Framework.Assert.AreEqual(1, second.Count);
			// Within first set {dataNodes[0], dataNodes[2]}, dataNodes[2] is chosen
			// as less free space
			excessTypes.AddItem(StorageType.Default);
			chosen = replicator.ChooseReplicaToDelete(null, null, (short)2, first, second, excessTypes
				);
			NUnit.Framework.Assert.AreEqual(chosen, storages[2]);
			replicator.AdjustSetsWithChosenReplica(rackMap, first, second, chosen);
			NUnit.Framework.Assert.AreEqual(0, first.Count);
			NUnit.Framework.Assert.AreEqual(2, second.Count);
			// Within second set, dataNodes[5] with less free space
			excessTypes.AddItem(StorageType.Default);
			chosen = replicator.ChooseReplicaToDelete(null, null, (short)1, first, second, excessTypes
				);
			NUnit.Framework.Assert.AreEqual(chosen, storages[5]);
		}

		/// <summary>Test replica placement policy in case of boundary topology.</summary>
		/// <remarks>
		/// Test replica placement policy in case of boundary topology.
		/// Rack 2 has only 1 node group & can't be placed with two replicas
		/// The 1st replica will be placed on writer.
		/// The 2nd replica should be placed on a different rack
		/// The 3rd replica should be placed on the same rack with writer, but on a
		/// different node group.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTargetsOnBoundaryTopology()
		{
			for (int i = 0; i < NumOfDatanodes; i++)
			{
				cluster.Remove(dataNodes[i]);
			}
			for (int i_1 = 0; i_1 < NumOfDatanodesBoundary; i_1++)
			{
				cluster.Add(dataNodesInBoundaryCase[i_1]);
			}
			for (int i_2 = 0; i_2 < NumOfDatanodesBoundary; i_2++)
			{
				UpdateHeartbeatWithUsage(dataNodes[0], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
					, 0L, (HdfsConstants.MinBlocksForWrite - 1) * BlockSize, 0L, 0L, 0L, 0, 0);
				UpdateHeartbeatWithUsage(dataNodesInBoundaryCase[i_2], 2 * HdfsConstants.MinBlocksForWrite
					 * BlockSize, 0L, 2 * HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0
					, 0);
			}
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0, dataNodesInBoundaryCase[0]);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1, dataNodesInBoundaryCase[0]);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			targets = ChooseTarget(2, dataNodesInBoundaryCase[0]);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			targets = ChooseTarget(3, dataNodesInBoundaryCase[0]);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			NUnit.Framework.Assert.IsTrue(CheckTargetsOnDifferentNodeGroup(targets));
		}

		/// <summary>Test re-replication policy in boundary case.</summary>
		/// <remarks>
		/// Test re-replication policy in boundary case.
		/// Rack 2 has only one node group & the node in this node group is chosen
		/// Rack 1 has two nodegroups & one of them is chosen.
		/// Replica policy should choose the node from node group of Rack1 but not the
		/// same nodegroup with chosen nodes.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRereplicateOnBoundaryTopology()
		{
			for (int i = 0; i < NumOfDatanodesBoundary; i++)
			{
				UpdateHeartbeatWithUsage(dataNodesInBoundaryCase[i], 2 * HdfsConstants.MinBlocksForWrite
					 * BlockSize, 0L, 2 * HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0
					, 0);
			}
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			chosenNodes.AddItem(storagesInBoundaryCase[0]);
			chosenNodes.AddItem(storagesInBoundaryCase[5]);
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(1, dataNodesInBoundaryCase[0], chosenNodes);
			NUnit.Framework.Assert.IsFalse(IsOnSameNodeGroup(dataNodesInBoundaryCase[0], targets
				[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameNodeGroup(dataNodesInBoundaryCase[5], targets
				[0]));
			NUnit.Framework.Assert.IsTrue(CheckTargetsOnDifferentNodeGroup(targets));
		}

		/// <summary>
		/// Test replica placement policy in case of targets more than number of
		/// NodeGroups.
		/// </summary>
		/// <remarks>
		/// Test replica placement policy in case of targets more than number of
		/// NodeGroups.
		/// The 12-nodes cluster only has 6 NodeGroups, but in some cases, like:
		/// placing submitted job file, there is requirement to choose more (10)
		/// targets for placing replica. We should test it can return 6 targets.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseMoreTargetsThanNodeGroups()
		{
			for (int i = 0; i < NumOfDatanodes; i++)
			{
				cluster.Remove(dataNodes[i]);
			}
			for (int i_1 = 0; i_1 < NumOfDatanodesBoundary; i_1++)
			{
				DatanodeDescriptor node = dataNodesInBoundaryCase[i_1];
				if (cluster.Contains(node))
				{
					cluster.Remove(node);
				}
			}
			for (int i_2 = 0; i_2 < NumOfDatanodesMoreTargets; i_2++)
			{
				cluster.Add(dataNodesInMoreTargetsCase[i_2]);
			}
			for (int i_3 = 0; i_3 < NumOfDatanodesMoreTargets; i_3++)
			{
				UpdateHeartbeatWithUsage(dataNodesInMoreTargetsCase[i_3], 2 * HdfsConstants.MinBlocksForWrite
					 * BlockSize, 0L, 2 * HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0
					, 0);
			}
			DatanodeStorageInfo[] targets;
			// Test normal case -- 3 replicas
			targets = ChooseTarget(3, dataNodesInMoreTargetsCase[0]);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			NUnit.Framework.Assert.IsTrue(CheckTargetsOnDifferentNodeGroup(targets));
			// Test special case -- replica number over node groups.
			targets = ChooseTarget(10, dataNodesInMoreTargetsCase[0]);
			NUnit.Framework.Assert.IsTrue(CheckTargetsOnDifferentNodeGroup(targets));
			// Verify it only can find 6 targets for placing replicas.
			NUnit.Framework.Assert.AreEqual(targets.Length, 6);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTargetWithDependencies()
		{
			for (int i = 0; i < NumOfDatanodes; i++)
			{
				cluster.Remove(dataNodes[i]);
			}
			for (int i_1 = 0; i_1 < NumOfDatanodesMoreTargets; i_1++)
			{
				DatanodeDescriptor node = dataNodesInMoreTargetsCase[i_1];
				if (cluster.Contains(node))
				{
					cluster.Remove(node);
				}
			}
			Host2NodesMap host2DatanodeMap = namenode.GetNamesystem().GetBlockManager().GetDatanodeManager
				().GetHost2DatanodeMap();
			for (int i_2 = 0; i_2 < NumOfDatanodesForDependencies; i_2++)
			{
				cluster.Add(dataNodesForDependencies[i_2]);
				host2DatanodeMap.Add(dataNodesForDependencies[i_2]);
			}
			//add dependencies (node1 <-> node2, and node3<->node4)
			dataNodesForDependencies[1].AddDependentHostName(dataNodesForDependencies[2].GetHostName
				());
			dataNodesForDependencies[2].AddDependentHostName(dataNodesForDependencies[1].GetHostName
				());
			dataNodesForDependencies[3].AddDependentHostName(dataNodesForDependencies[4].GetHostName
				());
			dataNodesForDependencies[4].AddDependentHostName(dataNodesForDependencies[3].GetHostName
				());
			//Update heartbeat
			for (int i_3 = 0; i_3 < NumOfDatanodesForDependencies; i_3++)
			{
				UpdateHeartbeatWithUsage(dataNodesForDependencies[i_3], 2 * HdfsConstants.MinBlocksForWrite
					 * BlockSize, 0L, 2 * HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0
					, 0);
			}
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			DatanodeStorageInfo[] targets;
			ICollection<Node> excludedNodes = new HashSet<Node>();
			excludedNodes.AddItem(dataNodesForDependencies[5]);
			//try to select three targets as there are three node groups
			targets = ChooseTarget(3, dataNodesForDependencies[1], chosenNodes, excludedNodes
				);
			//Even there are three node groups, verify that 
			//only two targets are selected due to dependencies
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.AreEqual(targets[0], storagesForDependencies[1]);
			NUnit.Framework.Assert.IsTrue(targets[1].Equals(storagesForDependencies[3]) || targets
				[1].Equals(storagesForDependencies[4]));
			//verify that all data nodes are in the excluded list
			NUnit.Framework.Assert.AreEqual(excludedNodes.Count, NumOfDatanodesForDependencies
				);
			for (int i_4 = 0; i_4 < NumOfDatanodesForDependencies; i_4++)
			{
				NUnit.Framework.Assert.IsTrue(excludedNodes.Contains(dataNodesForDependencies[i_4
					]));
			}
		}
	}
}
