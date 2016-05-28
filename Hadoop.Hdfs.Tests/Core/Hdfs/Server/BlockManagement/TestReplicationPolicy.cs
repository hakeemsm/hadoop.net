using System;
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Apache.Log4j.Spi;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestReplicationPolicy
	{
		private readonly Random random = DFSUtil.GetRandom();

		private const int BlockSize = 1024;

		private const int NumOfDatanodes = 6;

		private static NetworkTopology cluster;

		private static NameNode namenode;

		private static BlockPlacementPolicy replicator;

		private const string filename = "/dummyfile.txt";

		private static DatanodeDescriptor[] dataNodes;

		private static DatanodeStorageInfo[] storages;

		private const long staleInterval = DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalDefault;

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		// The interval for marking a datanode as stale,
		private static void UpdateHeartbeatWithUsage(DatanodeDescriptor dn, long capacity
			, long dfsUsed, long remaining, long blockPoolUsed, long dnCacheCapacity, long dnCacheUsed
			, int xceiverCount, int volFailures)
		{
			dn.GetStorageInfos()[0].SetUtilizationForTesting(capacity, dfsUsed, remaining, blockPoolUsed
				);
			dn.UpdateHeartbeat(BlockManagerTestUtil.GetStorageReportsForDatanode(dn), dnCacheCapacity
				, dnCacheUsed, xceiverCount, volFailures, null);
		}

		private static void UpdateHeartbeatForExtraStorage(long capacity, long dfsUsed, long
			 remaining, long blockPoolUsed)
		{
			DatanodeDescriptor dn = dataNodes[5];
			dn.GetStorageInfos()[1].SetUtilizationForTesting(capacity, dfsUsed, remaining, blockPoolUsed
				);
			dn.UpdateHeartbeat(BlockManagerTestUtil.GetStorageReportsForDatanode(dn), 0L, 0L, 
				0, 0, null);
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupCluster()
		{
			Configuration conf = new HdfsConfiguration();
			string[] racks = new string[] { "/d1/r1", "/d1/r1", "/d1/r2", "/d1/r2", "/d2/r3", 
				"/d2/r3" };
			storages = DFSTestUtil.CreateDatanodeStorageInfos(racks);
			dataNodes = DFSTestUtil.ToDatanodeDescriptor(storages);
			// create an extra storage for dn5.
			DatanodeStorage extraStorage = new DatanodeStorage(storages[5].GetStorageID() + "-extra"
				, DatanodeStorage.State.Normal, StorageType.Default);
			/*    DatanodeStorageInfo si = new DatanodeStorageInfo(
			storages[5].getDatanodeDescriptor(), extraStorage);
			*/
			BlockManagerTestUtil.UpdateStorage(storages[5].GetDatanodeDescriptor(), extraStorage
				);
			FileSystem.SetDefaultUri(conf, "hdfs://localhost:0");
			conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "0.0.0.0:0");
			FilePath baseDir = PathUtils.GetTestDir(typeof(TestReplicationPolicy));
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, new FilePath(baseDir, "name").GetPath
				());
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForReadKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForWriteKey, true);
			DFSTestUtil.FormatNameNode(conf);
			namenode = new NameNode(conf);
			BlockManager bm = namenode.GetNamesystem().GetBlockManager();
			replicator = bm.GetBlockPlacementPolicy();
			cluster = bm.GetDatanodeManager().GetNetworkTopology();
			// construct network topology
			for (int i = 0; i < NumOfDatanodes; i++)
			{
				cluster.Add(dataNodes[i]);
				bm.GetDatanodeManager().GetHeartbeatManager().AddDatanode(dataNodes[i]);
			}
			ResetHeartbeatForStorages();
		}

		private static void ResetHeartbeatForStorages()
		{
			for (int i = 0; i < NumOfDatanodes; i++)
			{
				UpdateHeartbeatWithUsage(dataNodes[i], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
					, 0L, HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0, 0);
			}
			// No available space in the extra storage of dn0
			UpdateHeartbeatForExtraStorage(0L, 0L, 0L, 0L);
		}

		private static bool IsOnSameRack(DatanodeStorageInfo left, DatanodeStorageInfo right
			)
		{
			return IsOnSameRack(left, right.GetDatanodeDescriptor());
		}

		private static bool IsOnSameRack(DatanodeStorageInfo left, DatanodeDescriptor right
			)
		{
			return cluster.IsOnSameRack(left.GetDatanodeDescriptor(), right);
		}

		/// <summary>
		/// Test whether the remaining space per storage is individually
		/// considered.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseNodeWithMultipleStorages()
		{
			UpdateHeartbeatWithUsage(dataNodes[5], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
				, 0L, (2 * HdfsConstants.MinBlocksForWrite * BlockSize) / 3, 0L, 0L, 0L, 0, 0);
			UpdateHeartbeatForExtraStorage(2 * HdfsConstants.MinBlocksForWrite * BlockSize, 0L
				, (2 * HdfsConstants.MinBlocksForWrite * BlockSize) / 3, 0L);
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(1, dataNodes[5], new AList<DatanodeStorageInfo>(), null);
			NUnit.Framework.Assert.AreEqual(1, targets.Length);
			NUnit.Framework.Assert.AreEqual(storages[4], targets[0]);
			ResetHeartbeatForStorages();
		}

		/// <summary>In this testcase, client is dataNodes[0].</summary>
		/// <remarks>
		/// In this testcase, client is dataNodes[0]. So the 1st replica should be
		/// placed on dataNodes[0], the 2nd replica should be placed on
		/// different rack and third should be placed on different node
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
			targets = ChooseTarget(4);
			NUnit.Framework.Assert.AreEqual(targets.Length, 4);
			NUnit.Framework.Assert.AreEqual(storages[0], targets[0]);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[1], targets[2]) || IsOnSameRack
				(targets[2], targets[3]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[2]));
			UpdateHeartbeatWithUsage(dataNodes[0], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
				, 0L, HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0, 0);
		}

		private static DatanodeStorageInfo[] ChooseTarget(int numOfReplicas)
		{
			return ChooseTarget(numOfReplicas, dataNodes[0]);
		}

		private static DatanodeStorageInfo[] ChooseTarget(int numOfReplicas, DatanodeDescriptor
			 writer)
		{
			return ChooseTarget(numOfReplicas, writer, new AList<DatanodeStorageInfo>());
		}

		private static DatanodeStorageInfo[] ChooseTarget(int numOfReplicas, IList<DatanodeStorageInfo
			> chosenNodes)
		{
			return ChooseTarget(numOfReplicas, dataNodes[0], chosenNodes);
		}

		private static DatanodeStorageInfo[] ChooseTarget(int numOfReplicas, DatanodeDescriptor
			 writer, IList<DatanodeStorageInfo> chosenNodes)
		{
			return ChooseTarget(numOfReplicas, writer, chosenNodes, null);
		}

		private static DatanodeStorageInfo[] ChooseTarget(int numOfReplicas, IList<DatanodeStorageInfo
			> chosenNodes, ICollection<Node> excludedNodes)
		{
			return ChooseTarget(numOfReplicas, dataNodes[0], chosenNodes, excludedNodes);
		}

		private static DatanodeStorageInfo[] ChooseTarget(int numOfReplicas, DatanodeDescriptor
			 writer, IList<DatanodeStorageInfo> chosenNodes, ICollection<Node> excludedNodes
			)
		{
			return replicator.ChooseTarget(filename, numOfReplicas, writer, chosenNodes, false
				, excludedNodes, BlockSize, TestBlockStoragePolicy.DefaultStoragePolicy);
		}

		/// <summary>
		/// In this testcase, client is dataNodes[0], but the dataNodes[1] is
		/// not allowed to be chosen.
		/// </summary>
		/// <remarks>
		/// In this testcase, client is dataNodes[0], but the dataNodes[1] is
		/// not allowed to be chosen. So the 1st replica should be
		/// placed on dataNodes[0], the 2nd replica should be placed on a different
		/// rack, the 3rd should be on same rack as the 2nd replica, and the rest
		/// should be placed on a third rack.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTarget2()
		{
			ICollection<Node> excludedNodes;
			DatanodeStorageInfo[] targets;
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			excludedNodes = new HashSet<Node>();
			excludedNodes.AddItem(dataNodes[1]);
			targets = ChooseTarget(0, chosenNodes, excludedNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			excludedNodes.Clear();
			chosenNodes.Clear();
			excludedNodes.AddItem(dataNodes[1]);
			targets = ChooseTarget(1, chosenNodes, excludedNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.AreEqual(storages[0], targets[0]);
			excludedNodes.Clear();
			chosenNodes.Clear();
			excludedNodes.AddItem(dataNodes[1]);
			targets = ChooseTarget(2, chosenNodes, excludedNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.AreEqual(storages[0], targets[0]);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			excludedNodes.Clear();
			chosenNodes.Clear();
			excludedNodes.AddItem(dataNodes[1]);
			targets = ChooseTarget(3, chosenNodes, excludedNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			NUnit.Framework.Assert.AreEqual(storages[0], targets[0]);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[1], targets[2]));
			excludedNodes.Clear();
			chosenNodes.Clear();
			excludedNodes.AddItem(dataNodes[1]);
			targets = ChooseTarget(4, chosenNodes, excludedNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 4);
			NUnit.Framework.Assert.AreEqual(storages[0], targets[0]);
			for (int i = 1; i < 4; i++)
			{
				NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[i]));
			}
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[1], targets[2]) || IsOnSameRack
				(targets[2], targets[3]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[1], targets[3]));
			excludedNodes.Clear();
			chosenNodes.Clear();
			excludedNodes.AddItem(dataNodes[1]);
			chosenNodes.AddItem(storages[2]);
			targets = replicator.ChooseTarget(filename, 1, dataNodes[0], chosenNodes, true, excludedNodes
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
		/// the 3rd replica should be placed on the same rack as the 2nd replica,
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
			for (int i = 1; i < 4; i++)
			{
				NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[i]));
			}
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[1], targets[2]) || IsOnSameRack
				(targets[2], targets[3]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[1], targets[3]));
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
		/// the 3rd replica should be placed on the same rack as the 1st replica,
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChoooseTarget4()
		{
			// make data node 0 & 1 to be not qualified to choose: not enough disk space
			for (int i = 0; i < 2; i++)
			{
				UpdateHeartbeatWithUsage(dataNodes[i], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
					, 0L, (HdfsConstants.MinBlocksForWrite - 1) * BlockSize, 0L, 0L, 0L, 0, 0);
			}
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], dataNodes[0]));
			targets = ChooseTarget(2);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], dataNodes[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			targets = ChooseTarget(3);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			for (int i_1 = 0; i_1 < 3; i_1++)
			{
				NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[i_1], dataNodes[0]));
			}
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[0], targets[1]) || IsOnSameRack
				(targets[1], targets[2]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[2]));
			for (int i_2 = 0; i_2 < 2; i_2++)
			{
				UpdateHeartbeatWithUsage(dataNodes[i_2], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
					, 0L, HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0, 0);
			}
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
			DatanodeDescriptor writerDesc = DFSTestUtil.GetDatanodeDescriptor("7.7.7.7", "/d2/r4"
				);
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0, writerDesc);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1, writerDesc);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			targets = ChooseTarget(2, writerDesc);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			targets = ChooseTarget(3, writerDesc);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[1], targets[2]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
		}

		/// <summary>
		/// In this testcase, there are enough total number of nodes, but only
		/// one rack is actually available.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTarget6()
		{
			DatanodeStorageInfo storage = DFSTestUtil.CreateDatanodeStorageInfo("DS-xxxx", "7.7.7.7"
				, "/d2/r3", "host7");
			DatanodeDescriptor newDn = storage.GetDatanodeDescriptor();
			ICollection<Node> excludedNodes;
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			excludedNodes = new HashSet<Node>();
			excludedNodes.AddItem(dataNodes[0]);
			excludedNodes.AddItem(dataNodes[1]);
			excludedNodes.AddItem(dataNodes[2]);
			excludedNodes.AddItem(dataNodes[3]);
			DatanodeStorageInfo[] targets;
			// Only two nodes available in a rack. Try picking two nodes. Only one
			// should return.
			targets = ChooseTarget(2, chosenNodes, excludedNodes);
			NUnit.Framework.Assert.AreEqual(1, targets.Length);
			// Make three nodes available in a rack.
			BlockManager bm = namenode.GetNamesystem().GetBlockManager();
			bm.GetDatanodeManager().GetNetworkTopology().Add(newDn);
			bm.GetDatanodeManager().GetHeartbeatManager().AddDatanode(newDn);
			UpdateHeartbeatWithUsage(newDn, 2 * HdfsConstants.MinBlocksForWrite * BlockSize, 
				0L, 2 * HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0, 0);
			// Try picking three nodes. Only two should return.
			excludedNodes.Clear();
			excludedNodes.AddItem(dataNodes[0]);
			excludedNodes.AddItem(dataNodes[1]);
			excludedNodes.AddItem(dataNodes[2]);
			excludedNodes.AddItem(dataNodes[3]);
			chosenNodes.Clear();
			try
			{
				targets = ChooseTarget(3, chosenNodes, excludedNodes);
				NUnit.Framework.Assert.AreEqual(2, targets.Length);
			}
			finally
			{
				bm.GetDatanodeManager().GetNetworkTopology().Remove(newDn);
			}
		}

		/// <summary>
		/// In this testcase, it tries to choose more targets than available nodes and
		/// check the result, with stale node avoidance on the write path enabled.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTargetWithMoreThanAvailableNodesWithStaleness()
		{
			try
			{
				namenode.GetNamesystem().GetBlockManager().GetDatanodeManager().SetNumStaleNodes(
					NumOfDatanodes);
				TestChooseTargetWithMoreThanAvailableNodes();
			}
			finally
			{
				namenode.GetNamesystem().GetBlockManager().GetDatanodeManager().SetNumStaleNodes(
					0);
			}
		}

		/// <summary>
		/// In this testcase, it tries to choose more targets than available nodes and
		/// check the result.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTargetWithMoreThanAvailableNodes()
		{
			// make data node 0 & 1 to be not qualified to choose: not enough disk space
			for (int i = 0; i < 2; i++)
			{
				UpdateHeartbeatWithUsage(dataNodes[i], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
					, 0L, (HdfsConstants.MinBlocksForWrite - 1) * BlockSize, 0L, 0L, 0L, 0, 0);
			}
			LogVerificationAppender appender = new LogVerificationAppender();
			Logger logger = Logger.GetRootLogger();
			logger.AddAppender(appender);
			// try to choose NUM_OF_DATANODES which is more than actually available
			// nodes.
			DatanodeStorageInfo[] targets = ChooseTarget(NumOfDatanodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, NumOfDatanodes - 2);
			IList<LoggingEvent> log = appender.GetLog();
			NUnit.Framework.Assert.IsNotNull(log);
			NUnit.Framework.Assert.IsFalse(log.Count == 0);
			LoggingEvent lastLogEntry = log[log.Count - 1];
			NUnit.Framework.Assert.IsTrue(Level.Warn.IsGreaterOrEqual(lastLogEntry.GetLevel()
				));
			// Suppose to place replicas on each node but two data nodes are not
			// available for placing replica, so here we expect a short of 2
			NUnit.Framework.Assert.IsTrue(((string)lastLogEntry.GetMessage()).Contains("in need of 2"
				));
			for (int i_1 = 0; i_1 < 2; i_1++)
			{
				UpdateHeartbeatWithUsage(dataNodes[i_1], 2 * HdfsConstants.MinBlocksForWrite * BlockSize
					, 0L, HdfsConstants.MinBlocksForWrite * BlockSize, 0L, 0L, 0L, 0, 0);
			}
		}

		private bool ContainsWithinRange(DatanodeStorageInfo target, DatanodeDescriptor[]
			 nodes, int startIndex, int endIndex)
		{
			System.Diagnostics.Debug.Assert(startIndex >= 0 && startIndex < nodes.Length);
			System.Diagnostics.Debug.Assert(endIndex >= startIndex && endIndex < nodes.Length
				);
			for (int i = startIndex; i <= endIndex; i++)
			{
				if (nodes[i].Equals(target.GetDatanodeDescriptor()))
				{
					return true;
				}
			}
			return false;
		}

		private bool ContainsWithinRange(DatanodeDescriptor target, DatanodeStorageInfo[]
			 nodes, int startIndex, int endIndex)
		{
			System.Diagnostics.Debug.Assert(startIndex >= 0 && startIndex < nodes.Length);
			System.Diagnostics.Debug.Assert(endIndex >= startIndex && endIndex < nodes.Length
				);
			for (int i = startIndex; i <= endIndex; i++)
			{
				if (nodes[i].GetDatanodeDescriptor().Equals(target))
				{
					return true;
				}
			}
			return false;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTargetWithStaleNodes()
		{
			// Set dataNodes[0] as stale
			DFSTestUtil.ResetLastUpdatesWithOffset(dataNodes[0], -(staleInterval + 1));
			namenode.GetNamesystem().GetBlockManager().GetDatanodeManager().GetHeartbeatManager
				().HeartbeatCheck();
			NUnit.Framework.Assert.IsTrue(namenode.GetNamesystem().GetBlockManager().GetDatanodeManager
				().ShouldAvoidStaleDataNodesForWrite());
			DatanodeStorageInfo[] targets;
			// We set the datanode[0] as stale, thus should choose datanode[1] since
			// datanode[1] is on the same rack with datanode[0] (writer)
			targets = ChooseTarget(1);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.AreEqual(storages[1], targets[0]);
			ICollection<Node> excludedNodes = new HashSet<Node>();
			excludedNodes.AddItem(dataNodes[1]);
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			targets = ChooseTarget(1, chosenNodes, excludedNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], dataNodes[0]));
			// reset
			DFSTestUtil.ResetLastUpdatesWithOffset(dataNodes[0], 0);
			namenode.GetNamesystem().GetBlockManager().GetDatanodeManager().GetHeartbeatManager
				().HeartbeatCheck();
		}

		/// <summary>
		/// In this testcase, we set 3 nodes (dataNodes[0] ~ dataNodes[2]) as stale,
		/// and when the number of replicas is less or equal to 3, all the healthy
		/// datanodes should be returned by the chooseTarget method.
		/// </summary>
		/// <remarks>
		/// In this testcase, we set 3 nodes (dataNodes[0] ~ dataNodes[2]) as stale,
		/// and when the number of replicas is less or equal to 3, all the healthy
		/// datanodes should be returned by the chooseTarget method. When the number
		/// of replicas is 4, a stale node should be included.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTargetWithHalfStaleNodes()
		{
			// Set dataNodes[0], dataNodes[1], and dataNodes[2] as stale
			for (int i = 0; i < 3; i++)
			{
				DFSTestUtil.ResetLastUpdatesWithOffset(dataNodes[i], -(staleInterval + 1));
			}
			namenode.GetNamesystem().GetBlockManager().GetDatanodeManager().GetHeartbeatManager
				().HeartbeatCheck();
			DatanodeStorageInfo[] targets = ChooseTarget(0);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			// Since we have 6 datanodes total, stale nodes should
			// not be returned until we ask for more than 3 targets
			targets = ChooseTarget(1);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsFalse(ContainsWithinRange(targets[0], dataNodes, 0, 2));
			targets = ChooseTarget(2);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsFalse(ContainsWithinRange(targets[0], dataNodes, 0, 2));
			NUnit.Framework.Assert.IsFalse(ContainsWithinRange(targets[1], dataNodes, 0, 2));
			targets = ChooseTarget(3);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			NUnit.Framework.Assert.IsTrue(ContainsWithinRange(targets[0], dataNodes, 3, 5));
			NUnit.Framework.Assert.IsTrue(ContainsWithinRange(targets[1], dataNodes, 3, 5));
			NUnit.Framework.Assert.IsTrue(ContainsWithinRange(targets[2], dataNodes, 3, 5));
			targets = ChooseTarget(4);
			NUnit.Framework.Assert.AreEqual(targets.Length, 4);
			NUnit.Framework.Assert.IsTrue(ContainsWithinRange(dataNodes[3], targets, 0, 3));
			NUnit.Framework.Assert.IsTrue(ContainsWithinRange(dataNodes[4], targets, 0, 3));
			NUnit.Framework.Assert.IsTrue(ContainsWithinRange(dataNodes[5], targets, 0, 3));
			for (int i_1 = 0; i_1 < dataNodes.Length; i_1++)
			{
				DFSTestUtil.ResetLastUpdatesWithOffset(dataNodes[i_1], 0);
			}
			namenode.GetNamesystem().GetBlockManager().GetDatanodeManager().GetHeartbeatManager
				().HeartbeatCheck();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseTargetWithMoreThanHalfStaleNodes()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForWriteKey, true);
			string[] hosts = new string[] { "host1", "host2", "host3", "host4", "host5", "host6"
				 };
			string[] racks = new string[] { "/d1/r1", "/d1/r1", "/d1/r2", "/d1/r2", "/d2/r3", 
				"/d2/r3" };
			MiniDFSCluster miniCluster = new MiniDFSCluster.Builder(conf).Racks(racks).Hosts(
				hosts).NumDataNodes(hosts.Length).Build();
			miniCluster.WaitActive();
			try
			{
				// Step 1. Make two datanodes as stale, check whether the 
				// avoidStaleDataNodesForWrite calculation is correct.
				// First stop the heartbeat of host1 and host2
				for (int i = 0; i < 2; i++)
				{
					DataNode dn = miniCluster.GetDataNodes()[i];
					DataNodeTestUtils.SetHeartbeatsDisabledForTests(dn, true);
					DatanodeDescriptor dnDes = miniCluster.GetNameNode().GetNamesystem().GetBlockManager
						().GetDatanodeManager().GetDatanode(dn.GetDatanodeId());
					DFSTestUtil.ResetLastUpdatesWithOffset(dnDes, -(staleInterval + 1));
				}
				// Instead of waiting, explicitly call heartbeatCheck to 
				// let heartbeat manager to detect stale nodes
				miniCluster.GetNameNode().GetNamesystem().GetBlockManager().GetDatanodeManager().
					GetHeartbeatManager().HeartbeatCheck();
				int numStaleNodes = miniCluster.GetNameNode().GetNamesystem().GetBlockManager().GetDatanodeManager
					().GetNumStaleNodes();
				NUnit.Framework.Assert.AreEqual(numStaleNodes, 2);
				NUnit.Framework.Assert.IsTrue(miniCluster.GetNameNode().GetNamesystem().GetBlockManager
					().GetDatanodeManager().ShouldAvoidStaleDataNodesForWrite());
				// Call chooseTarget
				DatanodeDescriptor staleNodeInfo = miniCluster.GetNameNode().GetNamesystem().GetBlockManager
					().GetDatanodeManager().GetDatanode(miniCluster.GetDataNodes()[0].GetDatanodeId(
					));
				BlockPlacementPolicy replicator = miniCluster.GetNameNode().GetNamesystem().GetBlockManager
					().GetBlockPlacementPolicy();
				DatanodeStorageInfo[] targets = replicator.ChooseTarget(filename, 3, staleNodeInfo
					, new AList<DatanodeStorageInfo>(), false, null, BlockSize, TestBlockStoragePolicy
					.DefaultStoragePolicy);
				NUnit.Framework.Assert.AreEqual(targets.Length, 3);
				NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], staleNodeInfo));
				// Step 2. Set more than half of the datanodes as stale
				for (int i_1 = 0; i_1 < 4; i_1++)
				{
					DataNode dn = miniCluster.GetDataNodes()[i_1];
					DataNodeTestUtils.SetHeartbeatsDisabledForTests(dn, true);
					DatanodeDescriptor dnDesc = miniCluster.GetNameNode().GetNamesystem().GetBlockManager
						().GetDatanodeManager().GetDatanode(dn.GetDatanodeId());
					DFSTestUtil.ResetLastUpdatesWithOffset(dnDesc, -(staleInterval + 1));
				}
				// Explicitly call heartbeatCheck
				miniCluster.GetNameNode().GetNamesystem().GetBlockManager().GetDatanodeManager().
					GetHeartbeatManager().HeartbeatCheck();
				numStaleNodes = miniCluster.GetNameNode().GetNamesystem().GetBlockManager().GetDatanodeManager
					().GetNumStaleNodes();
				NUnit.Framework.Assert.AreEqual(numStaleNodes, 4);
				// According to our strategy, stale datanodes will be included for writing
				// to avoid hotspots
				NUnit.Framework.Assert.IsFalse(miniCluster.GetNameNode().GetNamesystem().GetBlockManager
					().GetDatanodeManager().ShouldAvoidStaleDataNodesForWrite());
				// Call chooseTarget
				targets = replicator.ChooseTarget(filename, 3, staleNodeInfo, new AList<DatanodeStorageInfo
					>(), false, null, BlockSize, TestBlockStoragePolicy.DefaultStoragePolicy);
				NUnit.Framework.Assert.AreEqual(targets.Length, 3);
				NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[0], staleNodeInfo));
				// Step 3. Set 2 stale datanodes back to healthy nodes,
				// still have 2 stale nodes
				for (int i_2 = 2; i_2 < 4; i_2++)
				{
					DataNode dn = miniCluster.GetDataNodes()[i_2];
					DataNodeTestUtils.SetHeartbeatsDisabledForTests(dn, false);
					DatanodeDescriptor dnDesc = miniCluster.GetNameNode().GetNamesystem().GetBlockManager
						().GetDatanodeManager().GetDatanode(dn.GetDatanodeId());
					DFSTestUtil.ResetLastUpdatesWithOffset(dnDesc, 0);
				}
				// Explicitly call heartbeatCheck
				miniCluster.GetNameNode().GetNamesystem().GetBlockManager().GetDatanodeManager().
					GetHeartbeatManager().HeartbeatCheck();
				numStaleNodes = miniCluster.GetNameNode().GetNamesystem().GetBlockManager().GetDatanodeManager
					().GetNumStaleNodes();
				NUnit.Framework.Assert.AreEqual(numStaleNodes, 2);
				NUnit.Framework.Assert.IsTrue(miniCluster.GetNameNode().GetNamesystem().GetBlockManager
					().GetDatanodeManager().ShouldAvoidStaleDataNodesForWrite());
				// Call chooseTarget
				targets = ChooseTarget(3, staleNodeInfo);
				NUnit.Framework.Assert.AreEqual(targets.Length, 3);
				NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], staleNodeInfo));
			}
			finally
			{
				miniCluster.Shutdown();
			}
		}

		/// <summary>This testcase tests re-replication, when dataNodes[0] is already chosen.
		/// 	</summary>
		/// <remarks>
		/// This testcase tests re-replication, when dataNodes[0] is already chosen.
		/// So the 1st replica can be placed on random rack.
		/// the 2nd replica should be placed on different node by same rack as
		/// the 1st replica. The 3rd replica can be placed randomly.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRereplicate1()
		{
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			chosenNodes.AddItem(storages[0]);
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], dataNodes[0]));
			targets = ChooseTarget(2, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[0], dataNodes[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[1]));
			targets = ChooseTarget(3, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 3);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[0], dataNodes[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], targets[2]));
		}

		/// <summary>
		/// This testcase tests re-replication,
		/// when dataNodes[0] and dataNodes[1] are already chosen.
		/// </summary>
		/// <remarks>
		/// This testcase tests re-replication,
		/// when dataNodes[0] and dataNodes[1] are already chosen.
		/// So the 1st replica should be placed on a different rack than rack 1.
		/// the rest replicas can be placed randomly,
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRereplicate2()
		{
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			chosenNodes.AddItem(storages[0]);
			chosenNodes.AddItem(storages[1]);
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], dataNodes[0]));
			targets = ChooseTarget(2, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], dataNodes[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[1], dataNodes[0]));
		}

		/// <summary>
		/// This testcase tests re-replication,
		/// when dataNodes[0] and dataNodes[2] are already chosen.
		/// </summary>
		/// <remarks>
		/// This testcase tests re-replication,
		/// when dataNodes[0] and dataNodes[2] are already chosen.
		/// So the 1st replica should be placed on the rack that the writer resides.
		/// the rest replicas can be placed randomly,
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRereplicate3()
		{
			IList<DatanodeStorageInfo> chosenNodes = new AList<DatanodeStorageInfo>();
			chosenNodes.AddItem(storages[0]);
			chosenNodes.AddItem(storages[2]);
			DatanodeStorageInfo[] targets;
			targets = ChooseTarget(0, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 0);
			targets = ChooseTarget(1, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[0], dataNodes[0]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], dataNodes[2]));
			targets = ChooseTarget(1, dataNodes[2], chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 1);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[0], dataNodes[2]));
			NUnit.Framework.Assert.IsFalse(IsOnSameRack(targets[0], dataNodes[0]));
			targets = ChooseTarget(2, chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[0], dataNodes[0]));
			targets = ChooseTarget(2, dataNodes[2], chosenNodes);
			NUnit.Framework.Assert.AreEqual(targets.Length, 2);
			NUnit.Framework.Assert.IsTrue(IsOnSameRack(targets[0], dataNodes[2]));
		}

		/// <summary>
		/// Test for the high priority blocks are processed before the low priority
		/// blocks.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReplicationWithPriority()
		{
			int DfsNamenodeReplicationInterval = 1000;
			int HighPriority = 0;
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Format(
				true).Build();
			try
			{
				cluster.WaitActive();
				UnderReplicatedBlocks neededReplications = cluster.GetNameNode().GetNamesystem().
					GetBlockManager().neededReplications;
				for (int i = 0; i < 100; i++)
				{
					// Adding the blocks directly to normal priority
					neededReplications.Add(new Block(random.NextLong()), 2, 0, 3);
				}
				// Lets wait for the replication interval, to start process normal
				// priority blocks
				Sharpen.Thread.Sleep(DfsNamenodeReplicationInterval);
				// Adding the block directly to high priority list
				neededReplications.Add(new Block(random.NextLong()), 1, 0, 3);
				// Lets wait for the replication interval
				Sharpen.Thread.Sleep(DfsNamenodeReplicationInterval);
				// Check replication completed successfully. Need not wait till it process
				// all the 100 normal blocks.
				NUnit.Framework.Assert.IsFalse("Not able to clear the element from high priority list"
					, neededReplications.Iterator(HighPriority).HasNext());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test for the ChooseUnderReplicatedBlocks are processed based on priority
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseUnderReplicatedBlocks()
		{
			UnderReplicatedBlocks underReplicatedBlocks = new UnderReplicatedBlocks();
			for (int i = 0; i < 5; i++)
			{
				// Adding QUEUE_HIGHEST_PRIORITY block
				underReplicatedBlocks.Add(new Block(random.NextLong()), 1, 0, 3);
				// Adding QUEUE_VERY_UNDER_REPLICATED block
				underReplicatedBlocks.Add(new Block(random.NextLong()), 2, 0, 7);
				// Adding QUEUE_REPLICAS_BADLY_DISTRIBUTED block
				underReplicatedBlocks.Add(new Block(random.NextLong()), 6, 0, 6);
				// Adding QUEUE_UNDER_REPLICATED block
				underReplicatedBlocks.Add(new Block(random.NextLong()), 5, 0, 6);
				// Adding QUEUE_WITH_CORRUPT_BLOCKS block
				underReplicatedBlocks.Add(new Block(random.NextLong()), 0, 0, 3);
			}
			// Choose 6 blocks from UnderReplicatedBlocks. Then it should pick 5 blocks
			// from
			// QUEUE_HIGHEST_PRIORITY and 1 block from QUEUE_VERY_UNDER_REPLICATED.
			IList<IList<Block>> chosenBlocks = underReplicatedBlocks.ChooseUnderReplicatedBlocks
				(6);
			AssertTheChosenBlocks(chosenBlocks, 5, 1, 0, 0, 0);
			// Choose 10 blocks from UnderReplicatedBlocks. Then it should pick 4 blocks from
			// QUEUE_VERY_UNDER_REPLICATED, 5 blocks from QUEUE_UNDER_REPLICATED and 1
			// block from QUEUE_REPLICAS_BADLY_DISTRIBUTED.
			chosenBlocks = underReplicatedBlocks.ChooseUnderReplicatedBlocks(10);
			AssertTheChosenBlocks(chosenBlocks, 0, 4, 5, 1, 0);
			// Adding QUEUE_HIGHEST_PRIORITY
			underReplicatedBlocks.Add(new Block(random.NextLong()), 1, 0, 3);
			// Choose 10 blocks from UnderReplicatedBlocks. Then it should pick 1 block from
			// QUEUE_HIGHEST_PRIORITY, 4 blocks from QUEUE_REPLICAS_BADLY_DISTRIBUTED
			// and 5 blocks from QUEUE_WITH_CORRUPT_BLOCKS.
			chosenBlocks = underReplicatedBlocks.ChooseUnderReplicatedBlocks(10);
			AssertTheChosenBlocks(chosenBlocks, 1, 0, 0, 4, 5);
			// Since it is reached to end of all lists,
			// should start picking the blocks from start.
			// Choose 7 blocks from UnderReplicatedBlocks. Then it should pick 6 blocks from
			// QUEUE_HIGHEST_PRIORITY, 1 block from QUEUE_VERY_UNDER_REPLICATED.
			chosenBlocks = underReplicatedBlocks.ChooseUnderReplicatedBlocks(7);
			AssertTheChosenBlocks(chosenBlocks, 6, 1, 0, 0, 0);
		}

		/// <summary>asserts the chosen blocks with expected priority blocks</summary>
		private void AssertTheChosenBlocks(IList<IList<Block>> chosenBlocks, int firstPrioritySize
			, int secondPrioritySize, int thirdPrioritySize, int fourthPrioritySize, int fifthPrioritySize
			)
		{
			NUnit.Framework.Assert.AreEqual("Not returned the expected number of QUEUE_HIGHEST_PRIORITY blocks"
				, firstPrioritySize, chosenBlocks[UnderReplicatedBlocks.QueueHighestPriority].Count
				);
			NUnit.Framework.Assert.AreEqual("Not returned the expected number of QUEUE_VERY_UNDER_REPLICATED blocks"
				, secondPrioritySize, chosenBlocks[UnderReplicatedBlocks.QueueVeryUnderReplicated
				].Count);
			NUnit.Framework.Assert.AreEqual("Not returned the expected number of QUEUE_UNDER_REPLICATED blocks"
				, thirdPrioritySize, chosenBlocks[UnderReplicatedBlocks.QueueUnderReplicated].Count
				);
			NUnit.Framework.Assert.AreEqual("Not returned the expected number of QUEUE_REPLICAS_BADLY_DISTRIBUTED blocks"
				, fourthPrioritySize, chosenBlocks[UnderReplicatedBlocks.QueueReplicasBadlyDistributed
				].Count);
			NUnit.Framework.Assert.AreEqual("Not returned the expected number of QUEUE_WITH_CORRUPT_BLOCKS blocks"
				, fifthPrioritySize, chosenBlocks[UnderReplicatedBlocks.QueueWithCorruptBlocks].
				Count);
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
			// Refresh the last update time for all the datanodes
			for (int i = 0; i < dataNodes.Length; i++)
			{
				DFSTestUtil.ResetLastUpdatesWithOffset(dataNodes[i], 0);
			}
			IList<DatanodeStorageInfo> first = new AList<DatanodeStorageInfo>();
			IList<DatanodeStorageInfo> second = new AList<DatanodeStorageInfo>();
			replicator.SplitNodesWithRack(replicaList, rackMap, first, second);
			// storages[0] and storages[1] are in first set as their rack has two 
			// replica nodes, while storages[2] and dataNodes[5] are in second set.
			NUnit.Framework.Assert.AreEqual(2, first.Count);
			NUnit.Framework.Assert.AreEqual(2, second.Count);
			IList<StorageType> excessTypes = new AList<StorageType>();
			{
				// test returning null
				excessTypes.AddItem(StorageType.Ssd);
				NUnit.Framework.Assert.IsNull(replicator.ChooseReplicaToDelete(null, null, (short
					)3, first, second, excessTypes));
			}
			excessTypes.AddItem(StorageType.Default);
			DatanodeStorageInfo chosen = replicator.ChooseReplicaToDelete(null, null, (short)
				3, first, second, excessTypes);
			// Within first set, storages[1] with less free space
			NUnit.Framework.Assert.AreEqual(chosen, storages[1]);
			replicator.AdjustSetsWithChosenReplica(rackMap, first, second, chosen);
			NUnit.Framework.Assert.AreEqual(0, first.Count);
			NUnit.Framework.Assert.AreEqual(3, second.Count);
			// Within second set, storages[5] with less free space
			excessTypes.AddItem(StorageType.Default);
			chosen = replicator.ChooseReplicaToDelete(null, null, (short)2, first, second, excessTypes
				);
			NUnit.Framework.Assert.AreEqual(chosen, storages[5]);
		}

		/// <summary>
		/// This testcase tests whether the default value returned by
		/// DFSUtil.getInvalidateWorkPctPerIteration() is positive,
		/// and whether an IllegalArgumentException will be thrown
		/// when 0.0f is retrieved
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestGetInvalidateWorkPctPerIteration()
		{
			Configuration conf = new Configuration();
			float blocksInvalidateWorkPct = DFSUtil.GetInvalidateWorkPctPerIteration(conf);
			NUnit.Framework.Assert.IsTrue(blocksInvalidateWorkPct > 0);
			conf.Set(DFSConfigKeys.DfsNamenodeInvalidateWorkPctPerIteration, "0.5f");
			blocksInvalidateWorkPct = DFSUtil.GetInvalidateWorkPctPerIteration(conf);
			NUnit.Framework.Assert.AreEqual(blocksInvalidateWorkPct, 0.5f, blocksInvalidateWorkPct
				 * 1e-7);
			conf.Set(DFSConfigKeys.DfsNamenodeInvalidateWorkPctPerIteration, "1.0f");
			blocksInvalidateWorkPct = DFSUtil.GetInvalidateWorkPctPerIteration(conf);
			NUnit.Framework.Assert.AreEqual(blocksInvalidateWorkPct, 1.0f, blocksInvalidateWorkPct
				 * 1e-7);
			conf.Set(DFSConfigKeys.DfsNamenodeInvalidateWorkPctPerIteration, "0.0f");
			exception.Expect(typeof(ArgumentException));
			blocksInvalidateWorkPct = DFSUtil.GetInvalidateWorkPctPerIteration(conf);
		}

		/// <summary>
		/// This testcase tests whether an IllegalArgumentException
		/// will be thrown when a negative value is retrieved by
		/// DFSUtil#getInvalidateWorkPctPerIteration
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestGetInvalidateWorkPctPerIteration_NegativeValue()
		{
			Configuration conf = new Configuration();
			float blocksInvalidateWorkPct = DFSUtil.GetInvalidateWorkPctPerIteration(conf);
			NUnit.Framework.Assert.IsTrue(blocksInvalidateWorkPct > 0);
			conf.Set(DFSConfigKeys.DfsNamenodeInvalidateWorkPctPerIteration, "-0.5f");
			exception.Expect(typeof(ArgumentException));
			blocksInvalidateWorkPct = DFSUtil.GetInvalidateWorkPctPerIteration(conf);
		}

		/// <summary>
		/// This testcase tests whether an IllegalArgumentException
		/// will be thrown when a value greater than 1 is retrieved by
		/// DFSUtil#getInvalidateWorkPctPerIteration
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestGetInvalidateWorkPctPerIteration_GreaterThanOne()
		{
			Configuration conf = new Configuration();
			float blocksInvalidateWorkPct = DFSUtil.GetInvalidateWorkPctPerIteration(conf);
			NUnit.Framework.Assert.IsTrue(blocksInvalidateWorkPct > 0);
			conf.Set(DFSConfigKeys.DfsNamenodeInvalidateWorkPctPerIteration, "1.5f");
			exception.Expect(typeof(ArgumentException));
			blocksInvalidateWorkPct = DFSUtil.GetInvalidateWorkPctPerIteration(conf);
		}

		/// <summary>
		/// This testcase tests whether the value returned by
		/// DFSUtil.getReplWorkMultiplier() is positive,
		/// and whether an IllegalArgumentException will be thrown
		/// when a non-positive value is retrieved
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestGetReplWorkMultiplier()
		{
			Configuration conf = new Configuration();
			int blocksReplWorkMultiplier = DFSUtil.GetReplWorkMultiplier(conf);
			NUnit.Framework.Assert.IsTrue(blocksReplWorkMultiplier > 0);
			conf.Set(DFSConfigKeys.DfsNamenodeReplicationWorkMultiplierPerIteration, "3");
			blocksReplWorkMultiplier = DFSUtil.GetReplWorkMultiplier(conf);
			NUnit.Framework.Assert.AreEqual(blocksReplWorkMultiplier, 3);
			conf.Set(DFSConfigKeys.DfsNamenodeReplicationWorkMultiplierPerIteration, "-1");
			exception.Expect(typeof(ArgumentException));
			blocksReplWorkMultiplier = DFSUtil.GetReplWorkMultiplier(conf);
		}

		public TestReplicationPolicy()
		{
			{
				((Log4JLogger)BlockPlacementPolicy.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
