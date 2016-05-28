using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests the decommissioning of nodes.</summary>
	public class TestDecommission
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.TestDecommission
			));

		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 8192;

		internal const int fileSize = 16384;

		internal const int HeartbeatInterval = 1;

		internal const int BlockreportIntervalMsec = 1000;

		internal const int NamenodeReplicationInterval = 1;

		internal readonly Random myrand = new Random();

		internal Path dir;

		internal Path hostsFile;

		internal Path excludeFile;

		internal FileSystem localFileSys;

		internal Configuration conf;

		internal MiniDFSCluster cluster = null;

		// heartbeat interval in seconds
		//block report in msec
		//replication interval
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new HdfsConfiguration();
			// Set up the hosts/exclude files.
			localFileSys = FileSystem.GetLocal(conf);
			Path workingDir = localFileSys.GetWorkingDirectory();
			dir = new Path(workingDir, PathUtils.GetTestDirName(GetType()) + "/work-dir/decommission"
				);
			hostsFile = new Path(dir, "hosts");
			excludeFile = new Path(dir, "exclude");
			// Setup conf
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeReplicationConsiderloadKey, false);
			conf.Set(DFSConfigKeys.DfsHosts, hostsFile.ToUri().GetPath());
			conf.Set(DFSConfigKeys.DfsHostsExclude, excludeFile.ToUri().GetPath());
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 2000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, HeartbeatInterval);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsBlockreportIntervalMsecKey, BlockreportIntervalMsec);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, 4);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, NamenodeReplicationInterval
				);
			WriteConfigFile(hostsFile, null);
			WriteConfigFile(excludeFile, null);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Teardown()
		{
			CleanupFile(localFileSys, dir);
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteConfigFile(Path name, IList<string> nodes)
		{
			// delete if it already exists
			if (localFileSys.Exists(name))
			{
				localFileSys.Delete(name, true);
			}
			FSDataOutputStream stm = localFileSys.Create(name);
			if (nodes != null)
			{
				for (IEnumerator<string> it = nodes.GetEnumerator(); it.HasNext(); )
				{
					string node = it.Next();
					stm.WriteBytes(node);
					stm.WriteBytes("\n");
				}
			}
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name, int repl)
		{
			// create and write a file that contains three blocks of data
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, blockSize);
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
			Log.Info("Created file " + name + " with " + repl + " replicas.");
		}

		/// <summary>
		/// Verify that the number of replicas are as expected for each block in
		/// the given file.
		/// </summary>
		/// <remarks>
		/// Verify that the number of replicas are as expected for each block in
		/// the given file.
		/// For blocks with a decommissioned node, verify that their replication
		/// is 1 more than what is specified.
		/// For blocks without decommissioned nodes, verify their replication is
		/// equal to what is specified.
		/// </remarks>
		/// <param name="downnode">- if null, there is no decommissioned node for this file.</param>
		/// <returns>- null if no failure found, else an error message string.</returns>
		/// <exception cref="System.IO.IOException"/>
		private static string CheckFile(FileSystem fileSys, Path name, int repl, string downnode
			, int numDatanodes)
		{
			bool isNodeDown = (downnode != null);
			// need a raw stream
			NUnit.Framework.Assert.IsTrue("Not HDFS:" + fileSys.GetUri(), fileSys is DistributedFileSystem
				);
			HdfsDataInputStream dis = (HdfsDataInputStream)fileSys.Open(name);
			ICollection<LocatedBlock> dinfo = dis.GetAllBlocks();
			foreach (LocatedBlock blk in dinfo)
			{
				// for each block
				int hasdown = 0;
				DatanodeInfo[] nodes = blk.GetLocations();
				for (int j = 0; j < nodes.Length; j++)
				{
					// for each replica
					if (isNodeDown && nodes[j].GetXferAddr().Equals(downnode))
					{
						hasdown++;
						//Downnode must actually be decommissioned
						if (!nodes[j].IsDecommissioned())
						{
							return "For block " + blk.GetBlock() + " replica on " + nodes[j] + " is given as downnode, "
								 + "but is not decommissioned";
						}
						//Decommissioned node (if any) should only be last node in list.
						if (j != nodes.Length - 1)
						{
							return "For block " + blk.GetBlock() + " decommissioned node " + nodes[j] + " was not last node in list: "
								 + (j + 1) + " of " + nodes.Length;
						}
						Log.Info("Block " + blk.GetBlock() + " replica on " + nodes[j] + " is decommissioned."
							);
					}
					else
					{
						//Non-downnodes must not be decommissioned
						if (nodes[j].IsDecommissioned())
						{
							return "For block " + blk.GetBlock() + " replica on " + nodes[j] + " is unexpectedly decommissioned";
						}
					}
				}
				Log.Info("Block " + blk.GetBlock() + " has " + hasdown + " decommissioned replica."
					);
				if (Math.Min(numDatanodes, repl + hasdown) != nodes.Length)
				{
					return "Wrong number of replicas for block " + blk.GetBlock() + ": " + nodes.Length
						 + ", expected " + Math.Min(numDatanodes, repl + hasdown);
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		/*
		* decommission the DN at index dnIndex or one random node if dnIndex is set
		* to -1 and wait for the node to reach the given {@code waitForState}.
		*/
		/// <exception cref="System.IO.IOException"/>
		private DatanodeInfo DecommissionNode(int nnIndex, string datanodeUuid, AList<DatanodeInfo
			> decommissionedNodes, DatanodeInfo.AdminStates waitForState)
		{
			DFSClient client = GetDfsClient(cluster.GetNameNode(nnIndex), conf);
			DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
				);
			//
			// pick one datanode randomly unless the caller specifies one.
			//
			int index = 0;
			if (datanodeUuid == null)
			{
				bool found = false;
				while (!found)
				{
					index = myrand.Next(info.Length);
					if (!info[index].IsDecommissioned())
					{
						found = true;
					}
				}
			}
			else
			{
				// The caller specifies a DN
				for (; index < info.Length; index++)
				{
					if (info[index].GetDatanodeUuid().Equals(datanodeUuid))
					{
						break;
					}
				}
				if (index == info.Length)
				{
					throw new IOException("invalid datanodeUuid " + datanodeUuid);
				}
			}
			string nodename = info[index].GetXferAddr();
			Log.Info("Decommissioning node: " + nodename);
			// write nodename into the exclude file.
			AList<string> nodes = new AList<string>();
			if (decommissionedNodes != null)
			{
				foreach (DatanodeInfo dn in decommissionedNodes)
				{
					nodes.AddItem(dn.GetName());
				}
			}
			nodes.AddItem(nodename);
			WriteConfigFile(excludeFile, nodes);
			RefreshNodes(cluster.GetNamesystem(nnIndex), conf);
			DatanodeInfo ret = NameNodeAdapter.GetDatanode(cluster.GetNamesystem(nnIndex), info
				[index]);
			WaitNodeState(ret, waitForState);
			return ret;
		}

		/* Ask a specific NN to stop decommission of the datanode and wait for each
		* to reach the NORMAL state.
		*/
		/// <exception cref="System.IO.IOException"/>
		private void RecommissionNode(int nnIndex, DatanodeInfo decommissionedNode)
		{
			Log.Info("Recommissioning node: " + decommissionedNode);
			WriteConfigFile(excludeFile, null);
			RefreshNodes(cluster.GetNamesystem(nnIndex), conf);
			WaitNodeState(decommissionedNode, DatanodeInfo.AdminStates.Normal);
		}

		/*
		* Wait till node is fully decommissioned.
		*/
		private void WaitNodeState(DatanodeInfo node, DatanodeInfo.AdminStates state)
		{
			bool done = state == node.GetAdminState();
			while (!done)
			{
				Log.Info("Waiting for node " + node + " to change state to " + state + " current state: "
					 + node.GetAdminState());
				try
				{
					Sharpen.Thread.Sleep(HeartbeatInterval * 500);
				}
				catch (Exception)
				{
				}
				// nothing
				done = state == node.GetAdminState();
			}
			Log.Info("node " + node + " reached the state " + state);
		}

		/* Get DFSClient to the namenode */
		/// <exception cref="System.IO.IOException"/>
		private static DFSClient GetDfsClient(NameNode nn, Configuration conf)
		{
			return new DFSClient(nn.GetNameNodeAddress(), conf);
		}

		/* Validate cluster has expected number of datanodes */
		/// <exception cref="System.IO.IOException"/>
		private static void ValidateCluster(DFSClient client, int numDNs)
		{
			DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
				);
			NUnit.Framework.Assert.AreEqual("Number of Datanodes ", numDNs, info.Length);
		}

		/// <summary>Start a MiniDFSCluster</summary>
		/// <exception cref="System.IO.IOException"></exception>
		private void StartCluster(int numNameNodes, int numDatanodes, Configuration conf)
		{
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleFederatedTopology
				(numNameNodes)).NumDataNodes(numDatanodes).Build();
			cluster.WaitActive();
			for (int i = 0; i < numNameNodes; i++)
			{
				DFSClient client = GetDfsClient(cluster.GetNameNode(i), conf);
				ValidateCluster(client, numDatanodes);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void RefreshNodes(FSNamesystem ns, Configuration conf)
		{
			ns.GetBlockManager().GetDatanodeManager().RefreshNodes(conf);
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		private void VerifyStats(NameNode namenode, FSNamesystem fsn, DatanodeInfo info, 
			DataNode node, bool decommissioning)
		{
			// Do the stats check over 10 heartbeats
			for (int i = 0; i < 10; i++)
			{
				long[] newStats = namenode.GetRpcServer().GetStats();
				// For decommissioning nodes, ensure capacity of the DN is no longer
				// counted. Only used space of the DN is counted in cluster capacity
				NUnit.Framework.Assert.AreEqual(newStats[0], decommissioning ? info.GetDfsUsed() : 
					info.GetCapacity());
				// Ensure cluster used capacity is counted for both normal and
				// decommissioning nodes
				NUnit.Framework.Assert.AreEqual(newStats[1], info.GetDfsUsed());
				// For decommissioning nodes, remaining space from the DN is not counted
				NUnit.Framework.Assert.AreEqual(newStats[2], decommissioning ? 0 : info.GetRemaining
					());
				// Ensure transceiver count is same as that DN
				NUnit.Framework.Assert.AreEqual(fsn.GetTotalLoad(), info.GetXceiverCount());
				DataNodeTestUtils.TriggerHeartbeat(node);
			}
		}

		/// <summary>Tests decommission for non federated cluster</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDecommission()
		{
			TestDecommission(1, 6);
		}

		/// <summary>
		/// Tests decommission with replicas on the target datanode cannot be migrated
		/// to other datanodes and satisfy the replication factor.
		/// </summary>
		/// <remarks>
		/// Tests decommission with replicas on the target datanode cannot be migrated
		/// to other datanodes and satisfy the replication factor. Make sure the
		/// datanode won't get stuck in decommissioning state.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDecommission2()
		{
			Log.Info("Starting test testDecommission");
			int numNamenodes = 1;
			int numDatanodes = 4;
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 3);
			StartCluster(numNamenodes, numDatanodes, conf);
			AList<AList<DatanodeInfo>> namenodeDecomList = new AList<AList<DatanodeInfo>>(numNamenodes
				);
			namenodeDecomList.Add(0, new AList<DatanodeInfo>(numDatanodes));
			Path file1 = new Path("testDecommission2.dat");
			int replicas = 4;
			// Start decommissioning one namenode at a time
			AList<DatanodeInfo> decommissionedNodes = namenodeDecomList[0];
			FileSystem fileSys = cluster.GetFileSystem(0);
			FSNamesystem ns = cluster.GetNamesystem(0);
			WriteFile(fileSys, file1, replicas);
			int deadDecomissioned = ns.GetNumDecomDeadDataNodes();
			int liveDecomissioned = ns.GetNumDecomLiveDataNodes();
			// Decommission one node. Verify that node is decommissioned.
			DatanodeInfo decomNode = DecommissionNode(0, null, decommissionedNodes, DatanodeInfo.AdminStates
				.Decommissioned);
			decommissionedNodes.AddItem(decomNode);
			NUnit.Framework.Assert.AreEqual(deadDecomissioned, ns.GetNumDecomDeadDataNodes());
			NUnit.Framework.Assert.AreEqual(liveDecomissioned + 1, ns.GetNumDecomLiveDataNodes
				());
			// Ensure decommissioned datanode is not automatically shutdown
			DFSClient client = GetDfsClient(cluster.GetNameNode(0), conf);
			NUnit.Framework.Assert.AreEqual("All datanodes must be alive", numDatanodes, client
				.DatanodeReport(HdfsConstants.DatanodeReportType.Live).Length);
			NUnit.Framework.Assert.IsNull(CheckFile(fileSys, file1, replicas, decomNode.GetXferAddr
				(), numDatanodes));
			CleanupFile(fileSys, file1);
			// Restart the cluster and ensure recommissioned datanodes
			// are allowed to register with the namenode
			cluster.Shutdown();
			StartCluster(1, 4, conf);
			cluster.Shutdown();
		}

		/// <summary>Test decommission for federeated cluster</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDecommissionFederation()
		{
			TestDecommission(2, 2);
		}

		/// <summary>Test decommission process on standby NN.</summary>
		/// <remarks>
		/// Test decommission process on standby NN.
		/// Verify admins can run "dfsadmin -refreshNodes" on SBN and decomm
		/// process can finish as long as admins run "dfsadmin -refreshNodes"
		/// on active NN.
		/// SBN used to mark excess replica upon recommission. The SBN's pick
		/// for excess replica could be different from the one picked by ANN.
		/// That creates inconsistent state and prevent SBN from finishing
		/// decommission.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDecommissionOnStandby()
		{
			Configuration hdfsConf = new HdfsConfiguration(conf);
			hdfsConf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			hdfsConf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 30000);
			hdfsConf.SetInt(DFSConfigKeys.DfsNamenodeTolerateHeartbeatMultiplierKey, 2);
			// The time to wait so that the slow DN's heartbeat is considered old
			// by BlockPlacementPolicyDefault and thus will choose that DN for
			// excess replica.
			long slowHeartbeatDNwaitTime = hdfsConf.GetLong(DFSConfigKeys.DfsHeartbeatIntervalKey
				, DFSConfigKeys.DfsHeartbeatIntervalDefault) * 1000 * (hdfsConf.GetInt(DFSConfigKeys
				.DfsNamenodeTolerateHeartbeatMultiplierKey, DFSConfigKeys.DfsNamenodeTolerateHeartbeatMultiplierDefault
				) + 1);
			cluster = new MiniDFSCluster.Builder(hdfsConf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(3).Build();
			cluster.TransitionToActive(0);
			cluster.WaitActive();
			// Step 1, create a cluster with 4 DNs. Blocks are stored on the first 3 DNs.
			// The last DN is empty. Also configure the last DN to have slow heartbeat
			// so that it will be chosen as excess replica candidate during recommission.
			// Step 1.a, copy blocks to the first 3 DNs. Given the replica count is the
			// same as # of DNs, each DN will have a replica for any block.
			Path file1 = new Path("testDecommissionHA.dat");
			int replicas = 3;
			FileSystem activeFileSys = cluster.GetFileSystem(0);
			WriteFile(activeFileSys, file1, replicas);
			HATestUtil.WaitForStandbyToCatchUp(cluster.GetNameNode(0), cluster.GetNameNode(1)
				);
			// Step 1.b, start a DN with slow heartbeat, so that we can know for sure it
			// will be chosen as the target of excess replica during recommission.
			hdfsConf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 30);
			cluster.StartDataNodes(hdfsConf, 1, true, null, null, null);
			DataNode lastDN = cluster.GetDataNodes()[3];
			lastDN.GetDatanodeUuid();
			// Step 2, decommission the first DN at both ANN and SBN.
			DataNode firstDN = cluster.GetDataNodes()[0];
			// Step 2.a, ask ANN to decomm the first DN
			DatanodeInfo decommissionedNodeFromANN = DecommissionNode(0, firstDN.GetDatanodeUuid
				(), null, DatanodeInfo.AdminStates.Decommissioned);
			// Step 2.b, ask SBN to decomm the first DN
			DatanodeInfo decomNodeFromSBN = DecommissionNode(1, firstDN.GetDatanodeUuid(), null
				, DatanodeInfo.AdminStates.Decommissioned);
			// Step 3, recommission the first DN on SBN and ANN to create excess replica
			// It recommissions the node on SBN first to create potential
			// inconsistent state. In production cluster, such insistent state can happen
			// even if recommission command was issued on ANN first given the async nature
			// of the system.
			// Step 3.a, ask SBN to recomm the first DN.
			// SBN has been fixed so that it no longer invalidates excess replica during
			// recommission.
			// Before the fix, SBN could get into the following state.
			//    1. the last DN would have been chosen as excess replica, given its
			//    heartbeat is considered old.
			//    Please refer to BlockPlacementPolicyDefault#chooseReplicaToDelete
			//    2. After recommissionNode finishes, SBN has 3 live replicas ( 0, 1, 2 )
			//    and one excess replica ( 3 )
			// After the fix,
			//    After recommissionNode finishes, SBN has 4 live replicas ( 0, 1, 2, 3 )
			Sharpen.Thread.Sleep(slowHeartbeatDNwaitTime);
			RecommissionNode(1, decomNodeFromSBN);
			// Step 3.b, ask ANN to recommission the first DN.
			// To verify the fix, the test makes sure the excess replica picked by ANN
			// is different from the one picked by SBN before the fix.
			// To achieve that, we make sure next-to-last DN is chosen as excess replica
			// by ANN.
			// 1. restore LastDNprop's heartbeat interval.
			// 2. Make next-to-last DN's heartbeat slow.
			MiniDFSCluster.DataNodeProperties LastDNprop = cluster.StopDataNode(3);
			LastDNprop.conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, HeartbeatInterval);
			cluster.RestartDataNode(LastDNprop);
			MiniDFSCluster.DataNodeProperties nextToLastDNprop = cluster.StopDataNode(2);
			nextToLastDNprop.conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 30);
			cluster.RestartDataNode(nextToLastDNprop);
			cluster.WaitActive();
			Sharpen.Thread.Sleep(slowHeartbeatDNwaitTime);
			RecommissionNode(0, decommissionedNodeFromANN);
			// Step 3.c, make sure the DN has deleted the block and report to NNs
			cluster.TriggerHeartbeats();
			HATestUtil.WaitForDNDeletions(cluster);
			cluster.TriggerDeletionReports();
			// Step 4, decommission the first DN on both ANN and SBN
			// With the fix to make sure SBN no longer marks excess replica
			// during recommission, SBN's decommission can finish properly
			DecommissionNode(0, firstDN.GetDatanodeUuid(), null, DatanodeInfo.AdminStates.Decommissioned
				);
			// Ask SBN to decomm the first DN
			DecommissionNode(1, firstDN.GetDatanodeUuid(), null, DatanodeInfo.AdminStates.Decommissioned
				);
			cluster.Shutdown();
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestDecommission(int numNamenodes, int numDatanodes)
		{
			Log.Info("Starting test testDecommission");
			StartCluster(numNamenodes, numDatanodes, conf);
			AList<AList<DatanodeInfo>> namenodeDecomList = new AList<AList<DatanodeInfo>>(numNamenodes
				);
			for (int i = 0; i < numNamenodes; i++)
			{
				namenodeDecomList.Add(i, new AList<DatanodeInfo>(numDatanodes));
			}
			Path file1 = new Path("testDecommission.dat");
			for (int iteration = 0; iteration < numDatanodes - 1; iteration++)
			{
				int replicas = numDatanodes - iteration - 1;
				// Start decommissioning one namenode at a time
				for (int i_1 = 0; i_1 < numNamenodes; i_1++)
				{
					AList<DatanodeInfo> decommissionedNodes = namenodeDecomList[i_1];
					FileSystem fileSys = cluster.GetFileSystem(i_1);
					FSNamesystem ns = cluster.GetNamesystem(i_1);
					WriteFile(fileSys, file1, replicas);
					int deadDecomissioned = ns.GetNumDecomDeadDataNodes();
					int liveDecomissioned = ns.GetNumDecomLiveDataNodes();
					// Decommission one node. Verify that node is decommissioned.
					DatanodeInfo decomNode = DecommissionNode(i_1, null, decommissionedNodes, DatanodeInfo.AdminStates
						.Decommissioned);
					decommissionedNodes.AddItem(decomNode);
					NUnit.Framework.Assert.AreEqual(deadDecomissioned, ns.GetNumDecomDeadDataNodes());
					NUnit.Framework.Assert.AreEqual(liveDecomissioned + 1, ns.GetNumDecomLiveDataNodes
						());
					// Ensure decommissioned datanode is not automatically shutdown
					DFSClient client = GetDfsClient(cluster.GetNameNode(i_1), conf);
					NUnit.Framework.Assert.AreEqual("All datanodes must be alive", numDatanodes, client
						.DatanodeReport(HdfsConstants.DatanodeReportType.Live).Length);
					// wait for the block to be replicated
					int tries = 0;
					while (tries++ < 20)
					{
						try
						{
							Sharpen.Thread.Sleep(1000);
							if (CheckFile(fileSys, file1, replicas, decomNode.GetXferAddr(), numDatanodes) ==
								 null)
							{
								break;
							}
						}
						catch (Exception)
						{
						}
					}
					NUnit.Framework.Assert.IsTrue("Checked if block was replicated after decommission, tried "
						 + tries + " times.", tries < 20);
					CleanupFile(fileSys, file1);
				}
			}
			// Restart the cluster and ensure decommissioned datanodes
			// are allowed to register with the namenode
			cluster.Shutdown();
			StartCluster(numNamenodes, numDatanodes, conf);
			cluster.Shutdown();
		}

		/// <summary>Test that over-replicated blocks are deleted on recommission.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRecommission()
		{
			int numDatanodes = 6;
			try
			{
				Log.Info("Starting test testRecommission");
				StartCluster(1, numDatanodes, conf);
				Path file1 = new Path("testDecommission.dat");
				int replicas = numDatanodes - 1;
				AList<DatanodeInfo> decommissionedNodes = Lists.NewArrayList();
				FileSystem fileSys = cluster.GetFileSystem();
				// Write a file to n-1 datanodes
				WriteFile(fileSys, file1, replicas);
				// Decommission one of the datanodes with a replica
				BlockLocation loc = fileSys.GetFileBlockLocations(file1, 0, 1)[0];
				NUnit.Framework.Assert.AreEqual("Unexpected number of replicas from getFileBlockLocations"
					, replicas, loc.GetHosts().Length);
				string toDecomHost = loc.GetNames()[0];
				string toDecomUuid = null;
				foreach (DataNode d in cluster.GetDataNodes())
				{
					if (d.GetDatanodeId().GetXferAddr().Equals(toDecomHost))
					{
						toDecomUuid = d.GetDatanodeId().GetDatanodeUuid();
						break;
					}
				}
				NUnit.Framework.Assert.IsNotNull("Could not find a dn with the block!", toDecomUuid
					);
				DatanodeInfo decomNode = DecommissionNode(0, toDecomUuid, decommissionedNodes, DatanodeInfo.AdminStates
					.Decommissioned);
				decommissionedNodes.AddItem(decomNode);
				BlockManager blockManager = cluster.GetNamesystem().GetBlockManager();
				DatanodeManager datanodeManager = blockManager.GetDatanodeManager();
				BlockManagerTestUtil.RecheckDecommissionState(datanodeManager);
				// Ensure decommissioned datanode is not automatically shutdown
				DFSClient client = GetDfsClient(cluster.GetNameNode(), conf);
				NUnit.Framework.Assert.AreEqual("All datanodes must be alive", numDatanodes, client
					.DatanodeReport(HdfsConstants.DatanodeReportType.Live).Length);
				// wait for the block to be replicated
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fileSys, file1);
				string uuid = toDecomUuid;
				GenericTestUtils.WaitFor(new _Supplier_671(blockManager, b, uuid, replicas), 500, 
					30000);
				// redecommission and wait for over-replication to be fixed
				RecommissionNode(0, decomNode);
				BlockManagerTestUtil.RecheckDecommissionState(datanodeManager);
				DFSTestUtil.WaitForReplication(cluster, b, 1, replicas, 0);
				CleanupFile(fileSys, file1);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private sealed class _Supplier_671 : Supplier<bool>
		{
			public _Supplier_671(BlockManager blockManager, ExtendedBlock b, string uuid, int
				 replicas)
			{
				this.blockManager = blockManager;
				this.b = b;
				this.uuid = uuid;
				this.replicas = replicas;
			}

			public bool Get()
			{
				BlockInfoContiguous info = blockManager.GetStoredBlock(b.GetLocalBlock());
				int count = 0;
				StringBuilder sb = new StringBuilder("Replica locations: ");
				for (int i = 0; i < info.NumNodes(); i++)
				{
					DatanodeDescriptor dn = info.GetDatanode(i);
					sb.Append(dn + ", ");
					if (!dn.GetDatanodeUuid().Equals(uuid))
					{
						count++;
					}
				}
				Org.Apache.Hadoop.Hdfs.TestDecommission.Log.Info(sb.ToString());
				Org.Apache.Hadoop.Hdfs.TestDecommission.Log.Info("Count: " + count);
				return count == replicas;
			}

			private readonly BlockManager blockManager;

			private readonly ExtendedBlock b;

			private readonly string uuid;

			private readonly int replicas;
		}

		/// <summary>
		/// Tests cluster storage statistics during decommissioning for non
		/// federated cluster
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestClusterStats()
		{
			TestClusterStats(1);
		}

		/// <summary>
		/// Tests cluster storage statistics during decommissioning for
		/// federated cluster
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestClusterStatsFederation()
		{
			TestClusterStats(3);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestClusterStats(int numNameNodes)
		{
			Log.Info("Starting test testClusterStats");
			int numDatanodes = 1;
			StartCluster(numNameNodes, numDatanodes, conf);
			for (int i = 0; i < numNameNodes; i++)
			{
				FileSystem fileSys = cluster.GetFileSystem(i);
				Path file = new Path("testClusterStats.dat");
				WriteFile(fileSys, file, 1);
				FSNamesystem fsn = cluster.GetNamesystem(i);
				NameNode namenode = cluster.GetNameNode(i);
				DatanodeInfo decomInfo = DecommissionNode(i, null, null, DatanodeInfo.AdminStates
					.DecommissionInprogress);
				DataNode decomNode = GetDataNode(decomInfo);
				// Check namenode stats for multiple datanode heartbeats
				VerifyStats(namenode, fsn, decomInfo, decomNode, true);
				// Stop decommissioning and verify stats
				WriteConfigFile(excludeFile, null);
				RefreshNodes(fsn, conf);
				DatanodeInfo retInfo = NameNodeAdapter.GetDatanode(fsn, decomInfo);
				DataNode retNode = GetDataNode(decomInfo);
				WaitNodeState(retInfo, DatanodeInfo.AdminStates.Normal);
				VerifyStats(namenode, fsn, retInfo, retNode, false);
			}
		}

		private DataNode GetDataNode(DatanodeInfo decomInfo)
		{
			DataNode decomNode = null;
			foreach (DataNode dn in cluster.GetDataNodes())
			{
				if (decomInfo.Equals(dn.GetDatanodeId()))
				{
					decomNode = dn;
					break;
				}
			}
			NUnit.Framework.Assert.IsNotNull("Could not find decomNode in cluster!", decomNode
				);
			return decomNode;
		}

		/// <summary>Test host/include file functionality.</summary>
		/// <remarks>
		/// Test host/include file functionality. Only datanodes
		/// in the include file are allowed to connect to the namenode in a non
		/// federated cluster.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestHostsFile()
		{
			// Test for a single namenode cluster
			TestHostsFile(1);
		}

		/// <summary>Test host/include file functionality.</summary>
		/// <remarks>
		/// Test host/include file functionality. Only datanodes
		/// in the include file are allowed to connect to the namenode in a
		/// federated cluster.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestHostsFileFederation()
		{
			// Test for 3 namenode federated cluster
			TestHostsFile(3);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestHostsFile(int numNameNodes)
		{
			int numDatanodes = 1;
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleFederatedTopology
				(numNameNodes)).NumDataNodes(numDatanodes).SetupHostsFile(true).Build();
			cluster.WaitActive();
			// Now empty hosts file and ensure the datanode is disallowed
			// from talking to namenode, resulting in it's shutdown.
			AList<string> list = new AList<string>();
			string bogusIp = "127.0.30.1";
			list.AddItem(bogusIp);
			WriteConfigFile(hostsFile, list);
			for (int j = 0; j < numNameNodes; j++)
			{
				RefreshNodes(cluster.GetNamesystem(j), conf);
				DFSClient client = GetDfsClient(cluster.GetNameNode(j), conf);
				DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
					);
				for (int i = 0; i < 5 && info.Length != 0; i++)
				{
					Log.Info("Waiting for datanode to be marked dead");
					Sharpen.Thread.Sleep(HeartbeatInterval * 1000);
					info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live);
				}
				NUnit.Framework.Assert.AreEqual("Number of live nodes should be 0", 0, info.Length
					);
				// Test that bogus hostnames are considered "dead".
				// The dead report should have an entry for the bogus entry in the hosts
				// file.  The original datanode is excluded from the report because it
				// is no longer in the included list.
				info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Dead);
				NUnit.Framework.Assert.AreEqual("There should be 1 dead node", 1, info.Length);
				NUnit.Framework.Assert.AreEqual(bogusIp, info[0].GetHostName());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDecommissionWithOpenfile()
		{
			Log.Info("Starting test testDecommissionWithOpenfile");
			//At most 4 nodes will be decommissioned
			StartCluster(1, 7, conf);
			FileSystem fileSys = cluster.GetFileSystem(0);
			FSNamesystem ns = cluster.GetNamesystem(0);
			string openFile = "/testDecommissionWithOpenfile.dat";
			WriteFile(fileSys, new Path(openFile), (short)3);
			// make sure the file was open for write
			FSDataOutputStream fdos = fileSys.Append(new Path(openFile));
			LocatedBlocks lbs = NameNodeAdapter.GetBlockLocations(cluster.GetNameNode(0), openFile
				, 0, fileSize);
			DatanodeInfo[] dnInfos4LastBlock = lbs.GetLastLocatedBlock().GetLocations();
			DatanodeInfo[] dnInfos4FirstBlock = lbs.Get(0).GetLocations();
			AList<string> nodes = new AList<string>();
			AList<DatanodeInfo> dnInfos = new AList<DatanodeInfo>();
			DatanodeManager dm = ns.GetBlockManager().GetDatanodeManager();
			foreach (DatanodeInfo datanodeInfo in dnInfos4FirstBlock)
			{
				DatanodeInfo found = datanodeInfo;
				foreach (DatanodeInfo dif in dnInfos4LastBlock)
				{
					if (datanodeInfo.Equals(dif))
					{
						found = null;
					}
				}
				if (found != null)
				{
					nodes.AddItem(found.GetXferAddr());
					dnInfos.AddItem(dm.GetDatanode(found));
				}
			}
			//decommission one of the 3 nodes which have last block
			nodes.AddItem(dnInfos4LastBlock[0].GetXferAddr());
			dnInfos.AddItem(dm.GetDatanode(dnInfos4LastBlock[0]));
			WriteConfigFile(excludeFile, nodes);
			RefreshNodes(ns, conf);
			foreach (DatanodeInfo dn in dnInfos)
			{
				WaitNodeState(dn, DatanodeInfo.AdminStates.Decommissioned);
			}
			fdos.Close();
		}

		/// <summary>Tests restart of namenode while datanode hosts are added to exclude file
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDecommissionWithNamenodeRestart()
		{
			Log.Info("Starting test testDecommissionWithNamenodeRestart");
			int numNamenodes = 1;
			int numDatanodes = 1;
			int replicas = 1;
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, DFSConfigKeys.DfsBlockreportIntervalMsecDefault
				);
			conf.SetLong(DFSConfigKeys.DfsBlockreportInitialDelayKey, 5);
			StartCluster(numNamenodes, numDatanodes, conf);
			Path file1 = new Path("testDecommissionWithNamenodeRestart.dat");
			FileSystem fileSys = cluster.GetFileSystem();
			WriteFile(fileSys, file1, replicas);
			DFSClient client = GetDfsClient(cluster.GetNameNode(), conf);
			DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
				);
			DatanodeID excludedDatanodeID = info[0];
			string excludedDatanodeName = info[0].GetXferAddr();
			WriteConfigFile(excludeFile, new AList<string>(Arrays.AsList(excludedDatanodeName
				)));
			//Add a new datanode to cluster
			cluster.StartDataNodes(conf, 1, true, null, null, null, null);
			numDatanodes += 1;
			NUnit.Framework.Assert.AreEqual("Number of datanodes should be 2 ", 2, cluster.GetDataNodes
				().Count);
			//Restart the namenode
			cluster.RestartNameNode();
			DatanodeInfo datanodeInfo = NameNodeAdapter.GetDatanode(cluster.GetNamesystem(), 
				excludedDatanodeID);
			WaitNodeState(datanodeInfo, DatanodeInfo.AdminStates.Decommissioned);
			// Ensure decommissioned datanode is not automatically shutdown
			NUnit.Framework.Assert.AreEqual("All datanodes must be alive", numDatanodes, client
				.DatanodeReport(HdfsConstants.DatanodeReportType.Live).Length);
			NUnit.Framework.Assert.IsTrue("Checked if block was replicated after decommission."
				, CheckFile(fileSys, file1, replicas, datanodeInfo.GetXferAddr(), numDatanodes) 
				== null);
			CleanupFile(fileSys, file1);
			// Restart the cluster and ensure recommissioned datanodes
			// are allowed to register with the namenode
			cluster.Shutdown();
			StartCluster(numNamenodes, numDatanodes, conf);
			cluster.Shutdown();
		}

		/// <summary>Test using a "registration name" in a host include file.</summary>
		/// <remarks>
		/// Test using a "registration name" in a host include file.
		/// Registration names are DataNode names specified in the configuration by
		/// dfs.datanode.hostname.  The DataNode will send this name to the NameNode
		/// as part of its registration.  Registration names are helpful when you
		/// want to override the normal first result of DNS resolution on the
		/// NameNode.  For example, a given datanode IP may map to two hostnames,
		/// and you may want to choose which hostname is used internally in the
		/// cluster.
		/// It is not recommended to use a registration name which is not also a
		/// valid DNS hostname for the DataNode.  See HDFS-5237 for background.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Ignore]
		public virtual void TestIncludeByRegistrationName()
		{
			Configuration hdfsConf = new Configuration(conf);
			// Any IPv4 address starting with 127 functions as a "loopback" address
			// which is connected to the current host.  So by choosing 127.0.0.100
			// as our registration name, we have chosen a name which is also a valid
			// way of reaching the local DataNode we're going to start.
			// Typically, a registration name would be a hostname, but we don't want
			// to deal with DNS in this test.
			string registrationName = "127.0.0.100";
			string nonExistentDn = "127.0.0.10";
			hdfsConf.Set(DFSConfigKeys.DfsDatanodeHostNameKey, registrationName);
			cluster = new MiniDFSCluster.Builder(hdfsConf).NumDataNodes(1).CheckDataNodeHostConfig
				(true).SetupHostsFile(true).Build();
			cluster.WaitActive();
			// Set up an includes file that doesn't have our datanode.
			AList<string> nodes = new AList<string>();
			nodes.AddItem(nonExistentDn);
			WriteConfigFile(hostsFile, nodes);
			RefreshNodes(cluster.GetNamesystem(0), hdfsConf);
			// Wait for the DN to be marked dead.
			Log.Info("Waiting for DN to be marked as dead.");
			DFSClient client = GetDfsClient(cluster.GetNameNode(0), hdfsConf);
			GenericTestUtils.WaitFor(new _Supplier_965(this, client), 500, 5000);
			// Use a non-empty include file with our registration name.
			// It should work.
			int dnPort = cluster.GetDataNodes()[0].GetXferPort();
			nodes = new AList<string>();
			nodes.AddItem(registrationName + ":" + dnPort);
			WriteConfigFile(hostsFile, nodes);
			RefreshNodes(cluster.GetNamesystem(0), hdfsConf);
			cluster.RestartDataNode(0);
			cluster.TriggerHeartbeats();
			// Wait for the DN to come back.
			Log.Info("Waiting for DN to come back.");
			GenericTestUtils.WaitFor(new _Supplier_992(this, client, registrationName), 500, 
				5000);
		}

		private sealed class _Supplier_965 : Supplier<bool>
		{
			public _Supplier_965(TestDecommission _enclosing, DFSClient client)
			{
				this._enclosing = _enclosing;
				this.client = client;
			}

			public bool Get()
			{
				BlockManagerTestUtil.CheckHeartbeat(this._enclosing.cluster.GetNamesystem().GetBlockManager
					());
				try
				{
					DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Dead
						);
					return info.Length == 1;
				}
				catch (IOException e)
				{
					Org.Apache.Hadoop.Hdfs.TestDecommission.Log.Warn("Failed to check dead DNs", e);
					return false;
				}
			}

			private readonly TestDecommission _enclosing;

			private readonly DFSClient client;
		}

		private sealed class _Supplier_992 : Supplier<bool>
		{
			public _Supplier_992(TestDecommission _enclosing, DFSClient client, string registrationName
				)
			{
				this._enclosing = _enclosing;
				this.client = client;
				this.registrationName = registrationName;
			}

			public bool Get()
			{
				BlockManagerTestUtil.CheckHeartbeat(this._enclosing.cluster.GetNamesystem().GetBlockManager
					());
				try
				{
					DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
						);
					if (info.Length == 1)
					{
						NUnit.Framework.Assert.IsFalse(info[0].IsDecommissioned());
						NUnit.Framework.Assert.IsFalse(info[0].IsDecommissionInProgress());
						NUnit.Framework.Assert.AreEqual(registrationName, info[0].GetHostName());
						return true;
					}
				}
				catch (IOException e)
				{
					Org.Apache.Hadoop.Hdfs.TestDecommission.Log.Warn("Failed to check dead DNs", e);
				}
				return false;
			}

			private readonly TestDecommission _enclosing;

			private readonly DFSClient client;

			private readonly string registrationName;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBlocksPerInterval()
		{
			Configuration newConf = new Configuration(conf);
			Logger.GetLogger(typeof(DecommissionManager)).SetLevel(Level.Trace);
			// Turn the blocks per interval way down
			newConf.SetInt(DFSConfigKeys.DfsNamenodeDecommissionBlocksPerIntervalKey, 3);
			// Disable the normal monitor runs
			newConf.SetInt(DFSConfigKeys.DfsNamenodeDecommissionIntervalKey, int.MaxValue);
			StartCluster(1, 3, newConf);
			FileSystem fs = cluster.GetFileSystem();
			DatanodeManager datanodeManager = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
				();
			DecommissionManager decomManager = datanodeManager.GetDecomManager();
			// Write a 3 block file, so each node has one block. Should scan 3 nodes.
			DFSTestUtil.CreateFile(fs, new Path("/file1"), 64, (short)3, unchecked((int)(0xBAD1DEA
				)));
			DoDecomCheck(datanodeManager, decomManager, 3);
			// Write another file, should only scan two
			DFSTestUtil.CreateFile(fs, new Path("/file2"), 64, (short)3, unchecked((int)(0xBAD1DEA
				)));
			DoDecomCheck(datanodeManager, decomManager, 2);
			// One more file, should only scan 1
			DFSTestUtil.CreateFile(fs, new Path("/file3"), 64, (short)3, unchecked((int)(0xBAD1DEA
				)));
			DoDecomCheck(datanodeManager, decomManager, 1);
			// blocks on each DN now exceeds limit, still scan at least one node
			DFSTestUtil.CreateFile(fs, new Path("/file4"), 64, (short)3, unchecked((int)(0xBAD1DEA
				)));
			DoDecomCheck(datanodeManager, decomManager, 1);
		}

		/// <exception cref="System.Exception"/>
		[Obsolete]
		public virtual void TestNodesPerInterval()
		{
			Configuration newConf = new Configuration(conf);
			Logger.GetLogger(typeof(DecommissionManager)).SetLevel(Level.Trace);
			// Set the deprecated configuration key which limits the # of nodes per 
			// interval
			newConf.SetInt("dfs.namenode.decommission.nodes.per.interval", 1);
			// Disable the normal monitor runs
			newConf.SetInt(DFSConfigKeys.DfsNamenodeDecommissionIntervalKey, int.MaxValue);
			StartCluster(1, 3, newConf);
			FileSystem fs = cluster.GetFileSystem();
			DatanodeManager datanodeManager = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
				();
			DecommissionManager decomManager = datanodeManager.GetDecomManager();
			// Write a 3 block file, so each node has one block. Should scan 1 node 
			// each time.
			DFSTestUtil.CreateFile(fs, new Path("/file1"), 64, (short)3, unchecked((int)(0xBAD1DEA
				)));
			for (int i = 0; i < 3; i++)
			{
				DoDecomCheck(datanodeManager, decomManager, 1);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.ExecutionException"/>
		/// <exception cref="System.Exception"/>
		private void DoDecomCheck(DatanodeManager datanodeManager, DecommissionManager decomManager
			, int expectedNumCheckedNodes)
		{
			// Decom all nodes
			AList<DatanodeInfo> decommissionedNodes = Lists.NewArrayList();
			foreach (DataNode d in cluster.GetDataNodes())
			{
				DatanodeInfo dn = DecommissionNode(0, d.GetDatanodeUuid(), decommissionedNodes, DatanodeInfo.AdminStates
					.DecommissionInprogress);
				decommissionedNodes.AddItem(dn);
			}
			// Run decom scan and check
			BlockManagerTestUtil.RecheckDecommissionState(datanodeManager);
			NUnit.Framework.Assert.AreEqual("Unexpected # of nodes checked", expectedNumCheckedNodes
				, decomManager.GetNumNodesChecked());
			// Recommission all nodes
			foreach (DatanodeInfo dn_1 in decommissionedNodes)
			{
				RecommissionNode(0, dn_1);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPendingNodes()
		{
			Configuration newConf = new Configuration(conf);
			Logger.GetLogger(typeof(DecommissionManager)).SetLevel(Level.Trace);
			// Only allow one node to be decom'd at a time
			newConf.SetInt(DFSConfigKeys.DfsNamenodeDecommissionMaxConcurrentTrackedNodes, 1);
			// Disable the normal monitor runs
			newConf.SetInt(DFSConfigKeys.DfsNamenodeDecommissionIntervalKey, int.MaxValue);
			StartCluster(1, 3, newConf);
			FileSystem fs = cluster.GetFileSystem();
			DatanodeManager datanodeManager = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
				();
			DecommissionManager decomManager = datanodeManager.GetDecomManager();
			// Keep a file open to prevent decom from progressing
			HdfsDataOutputStream open1 = (HdfsDataOutputStream)fs.Create(new Path("/openFile1"
				), (short)3);
			// Flush and trigger block reports so the block definitely shows up on NN
			open1.Write(123);
			open1.Hflush();
			foreach (DataNode d in cluster.GetDataNodes())
			{
				DataNodeTestUtils.TriggerBlockReport(d);
			}
			// Decom two nodes, so one is still alive
			AList<DatanodeInfo> decommissionedNodes = Lists.NewArrayList();
			for (int i = 0; i < 2; i++)
			{
				DataNode d_1 = cluster.GetDataNodes()[i];
				DatanodeInfo dn = DecommissionNode(0, d_1.GetDatanodeUuid(), decommissionedNodes, 
					DatanodeInfo.AdminStates.DecommissionInprogress);
				decommissionedNodes.AddItem(dn);
			}
			for (int i_1 = 2; i_1 >= 0; i_1--)
			{
				AssertTrackedAndPending(decomManager, 0, i_1);
				BlockManagerTestUtil.RecheckDecommissionState(datanodeManager);
			}
			// Close file, try to decom the last node, should get stuck in tracked
			open1.Close();
			DataNode d_2 = cluster.GetDataNodes()[2];
			DatanodeInfo dn_1 = DecommissionNode(0, d_2.GetDatanodeUuid(), decommissionedNodes
				, DatanodeInfo.AdminStates.DecommissionInprogress);
			decommissionedNodes.AddItem(dn_1);
			BlockManagerTestUtil.RecheckDecommissionState(datanodeManager);
			AssertTrackedAndPending(decomManager, 1, 0);
		}

		private void AssertTrackedAndPending(DecommissionManager decomManager, int tracked
			, int pending)
		{
			NUnit.Framework.Assert.AreEqual("Unexpected number of tracked nodes", tracked, decomManager
				.GetNumTrackedNodes());
			NUnit.Framework.Assert.AreEqual("Unexpected number of pending nodes", pending, decomManager
				.GetNumPendingNodes());
		}
	}
}
