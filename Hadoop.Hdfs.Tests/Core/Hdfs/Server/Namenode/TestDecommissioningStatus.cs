using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.IO.Output;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This class tests the decommissioning of nodes.</summary>
	public class TestDecommissioningStatus
	{
		private const long seed = unchecked((long)(0xDEADBEEFL));

		private const int blockSize = 8192;

		private const int fileSize = 16384;

		private const int numDatanodes = 2;

		private static MiniDFSCluster cluster;

		private static FileSystem fileSys;

		private static Path excludeFile;

		private static FileSystem localFileSys;

		private static Configuration conf;

		private static Path dir;

		internal readonly AList<string> decommissionedNodes = new AList<string>(numDatanodes
			);

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeReplicationConsiderloadKey, false);
			// Set up the hosts/exclude files.
			localFileSys = FileSystem.GetLocal(conf);
			Path workingDir = localFileSys.GetWorkingDirectory();
			dir = new Path(workingDir, "build/test/data/work-dir/decommission");
			NUnit.Framework.Assert.IsTrue(localFileSys.Mkdirs(dir));
			excludeFile = new Path(dir, "exclude");
			conf.Set(DFSConfigKeys.DfsHostsExclude, excludeFile.ToUri().GetPath());
			Path includeFile = new Path(dir, "include");
			conf.Set(DFSConfigKeys.DfsHosts, includeFile.ToUri().GetPath());
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, 4);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsNamenodeDecommissionIntervalKey, 1);
			conf.SetLong(DFSConfigKeys.DfsDatanodeBalanceBandwidthpersecKey, 1);
			WriteConfigFile(localFileSys, excludeFile, null);
			WriteConfigFile(localFileSys, includeFile, null);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
			cluster.WaitActive();
			fileSys = cluster.GetFileSystem();
			cluster.GetNamesystem().GetBlockManager().GetDatanodeManager().SetHeartbeatExpireInterval
				(3000);
			Logger.GetLogger(typeof(DecommissionManager)).SetLevel(Level.Debug);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (localFileSys != null)
			{
				CleanupFile(localFileSys, dir);
			}
			if (fileSys != null)
			{
				fileSys.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteConfigFile(FileSystem fs, Path name, AList<string> nodes
			)
		{
			// delete if it already exists
			if (fs.Exists(name))
			{
				fs.Delete(name, true);
			}
			FSDataOutputStream stm = fs.Create(name);
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
		private void WriteFile(FileSystem fileSys, Path name, short repl)
		{
			// create and write a file that contains three blocks of data
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), repl, blockSize);
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private FSDataOutputStream WriteIncompleteFile(FileSystem fileSys, Path name, short
			 repl)
		{
			// create and write a file that contains three blocks of data
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), repl, blockSize);
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			// need to make sure that we actually write out both file blocks
			// (see FSOutputSummer#flush)
			stm.Flush();
			// Do not close stream, return it
			// so that it is not garbage collected
			return stm;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		/*
		* Decommissions the node at the given index
		*/
		/// <exception cref="System.IO.IOException"/>
		private string DecommissionNode(FSNamesystem namesystem, DFSClient client, FileSystem
			 localFileSys, int nodeIndex)
		{
			DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
				);
			string nodename = info[nodeIndex].GetXferAddr();
			DecommissionNode(namesystem, localFileSys, nodename);
			return nodename;
		}

		/*
		* Decommissions the node by name
		*/
		/// <exception cref="System.IO.IOException"/>
		private void DecommissionNode(FSNamesystem namesystem, FileSystem localFileSys, string
			 dnName)
		{
			System.Console.Out.WriteLine("Decommissioning node: " + dnName);
			// write nodename into the exclude file.
			AList<string> nodes = new AList<string>(decommissionedNodes);
			nodes.AddItem(dnName);
			WriteConfigFile(localFileSys, excludeFile, nodes);
		}

		private void CheckDecommissionStatus(DatanodeDescriptor decommNode, int expectedUnderRep
			, int expectedDecommissionOnly, int expectedUnderRepInOpenFiles)
		{
			NUnit.Framework.Assert.AreEqual("Unexpected num under-replicated blocks", expectedUnderRep
				, decommNode.decommissioningStatus.GetUnderReplicatedBlocks());
			NUnit.Framework.Assert.AreEqual("Unexpected number of decom-only replicas", expectedDecommissionOnly
				, decommNode.decommissioningStatus.GetDecommissionOnlyReplicas());
			NUnit.Framework.Assert.AreEqual("Unexpected number of replicas in under-replicated open files"
				, expectedUnderRepInOpenFiles, decommNode.decommissioningStatus.GetUnderReplicatedInOpenFiles
				());
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckDFSAdminDecommissionStatus(IList<DatanodeDescriptor> expectedDecomm
			, DistributedFileSystem dfs, DFSAdmin admin)
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			TextWriter ps = new TextWriter(baos);
			TextWriter oldOut = System.Console.Out;
			Runtime.SetOut(ps);
			try
			{
				// Parse DFSAdmin just to check the count
				admin.Report(new string[] { "-decommissioning" }, 0);
				string[] lines = baos.ToString().Split("\n");
				int num = null;
				int count = 0;
				foreach (string line in lines)
				{
					if (line.StartsWith("Decommissioning datanodes"))
					{
						// Pull out the "(num)" and parse it into an int
						string temp = line.Split(" ")[2];
						num = System.Convert.ToInt32((string)temp.SubSequence(1, temp.Length - 2));
					}
					if (line.Contains("Decommission in progress"))
					{
						count++;
					}
				}
				NUnit.Framework.Assert.IsTrue("No decommissioning output", num != null);
				NUnit.Framework.Assert.AreEqual("Unexpected number of decomming DNs", expectedDecomm
					.Count, num);
				NUnit.Framework.Assert.AreEqual("Unexpected number of decomming DNs", expectedDecomm
					.Count, count);
				// Check Java API for correct contents
				IList<DatanodeInfo> decomming = new AList<DatanodeInfo>(Arrays.AsList(dfs.GetDataNodeStats
					(HdfsConstants.DatanodeReportType.Decommissioning)));
				NUnit.Framework.Assert.AreEqual("Unexpected number of decomming DNs", expectedDecomm
					.Count, decomming.Count);
				foreach (DatanodeID id in expectedDecomm)
				{
					NUnit.Framework.Assert.IsTrue("Did not find expected decomming DN " + id, decomming
						.Contains(id));
				}
			}
			finally
			{
				Runtime.SetOut(oldOut);
			}
		}

		/// <summary>Tests Decommissioning Status in DFS.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDecommissionStatus()
		{
			IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
			DFSClient client = new DFSClient(addr, conf);
			DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
				);
			NUnit.Framework.Assert.AreEqual("Number of Datanodes ", 2, info.Length);
			DistributedFileSystem fileSys = cluster.GetFileSystem();
			DFSAdmin admin = new DFSAdmin(cluster.GetConfiguration(0));
			short replicas = numDatanodes;
			//
			// Decommission one node. Verify the decommission status
			// 
			Path file1 = new Path("decommission.dat");
			WriteFile(fileSys, file1, replicas);
			Path file2 = new Path("decommission1.dat");
			FSDataOutputStream st1 = WriteIncompleteFile(fileSys, file2, replicas);
			foreach (DataNode d in cluster.GetDataNodes())
			{
				DataNodeTestUtils.TriggerBlockReport(d);
			}
			FSNamesystem fsn = cluster.GetNamesystem();
			DatanodeManager dm = fsn.GetBlockManager().GetDatanodeManager();
			for (int iteration = 0; iteration < numDatanodes; iteration++)
			{
				string downnode = DecommissionNode(fsn, client, localFileSys, iteration);
				dm.RefreshNodes(conf);
				decommissionedNodes.AddItem(downnode);
				BlockManagerTestUtil.RecheckDecommissionState(dm);
				IList<DatanodeDescriptor> decommissioningNodes = dm.GetDecommissioningNodes();
				if (iteration == 0)
				{
					NUnit.Framework.Assert.AreEqual(decommissioningNodes.Count, 1);
					DatanodeDescriptor decommNode = decommissioningNodes[0];
					CheckDecommissionStatus(decommNode, 3, 0, 1);
					CheckDFSAdminDecommissionStatus(decommissioningNodes.SubList(0, 1), fileSys, admin
						);
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(decommissioningNodes.Count, 2);
					DatanodeDescriptor decommNode1 = decommissioningNodes[0];
					DatanodeDescriptor decommNode2 = decommissioningNodes[1];
					// This one is still 3,3,1 since it passed over the UC block 
					// earlier, before node 2 was decommed
					CheckDecommissionStatus(decommNode1, 3, 3, 1);
					// This one is 4,4,2 since it has the full state
					CheckDecommissionStatus(decommNode2, 4, 4, 2);
					CheckDFSAdminDecommissionStatus(decommissioningNodes.SubList(0, 2), fileSys, admin
						);
				}
			}
			// Call refreshNodes on FSNamesystem with empty exclude file.
			// This will remove the datanodes from decommissioning list and
			// make them available again.
			WriteConfigFile(localFileSys, excludeFile, null);
			dm.RefreshNodes(conf);
			st1.Close();
			CleanupFile(fileSys, file1);
			CleanupFile(fileSys, file2);
		}

		/// <summary>
		/// Verify a DN remains in DECOMMISSION_INPROGRESS state if it is marked
		/// as dead before decommission has completed.
		/// </summary>
		/// <remarks>
		/// Verify a DN remains in DECOMMISSION_INPROGRESS state if it is marked
		/// as dead before decommission has completed. That will allow DN to resume
		/// the replication process after it rejoins the cluster.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDecommissionStatusAfterDNRestart()
		{
			DistributedFileSystem fileSys = (DistributedFileSystem)cluster.GetFileSystem();
			// Create a file with one block. That block has one replica.
			Path f = new Path("decommission.dat");
			DFSTestUtil.CreateFile(fileSys, f, fileSize, fileSize, fileSize, (short)1, seed);
			// Find the DN that owns the only replica.
			RemoteIterator<LocatedFileStatus> fileList = fileSys.ListLocatedStatus(f);
			BlockLocation[] blockLocations = fileList.Next().GetBlockLocations();
			string dnName = blockLocations[0].GetNames()[0];
			// Decommission the DN.
			FSNamesystem fsn = cluster.GetNamesystem();
			DatanodeManager dm = fsn.GetBlockManager().GetDatanodeManager();
			DecommissionNode(fsn, localFileSys, dnName);
			dm.RefreshNodes(conf);
			// Stop the DN when decommission is in progress.
			// Given DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY is to 1 and the size of
			// the block, it will take much longer time that test timeout value for
			// the decommission to complete. So when stopDataNode is called,
			// decommission should be in progress.
			MiniDFSCluster.DataNodeProperties dataNodeProperties = cluster.StopDataNode(dnName
				);
			IList<DatanodeDescriptor> dead = new AList<DatanodeDescriptor>();
			while (true)
			{
				dm.FetchDatanodes(null, dead, false);
				if (dead.Count == 1)
				{
					break;
				}
				Sharpen.Thread.Sleep(1000);
			}
			// Force removal of the dead node's blocks.
			BlockManagerTestUtil.CheckHeartbeat(fsn.GetBlockManager());
			// Force DatanodeManager to check decommission state.
			BlockManagerTestUtil.RecheckDecommissionState(dm);
			// Verify that the DN remains in DECOMMISSION_INPROGRESS state.
			NUnit.Framework.Assert.IsTrue("the node should be DECOMMISSION_IN_PROGRESSS", dead
				[0].IsDecommissionInProgress());
			// Check DatanodeManager#getDecommissionNodes, make sure it returns
			// the node as decommissioning, even if it's dead
			IList<DatanodeDescriptor> decomlist = dm.GetDecommissioningNodes();
			NUnit.Framework.Assert.IsTrue("The node should be be decommissioning", decomlist.
				Count == 1);
			// Delete the under-replicated file, which should let the 
			// DECOMMISSION_IN_PROGRESS node become DECOMMISSIONED
			CleanupFile(fileSys, f);
			BlockManagerTestUtil.RecheckDecommissionState(dm);
			NUnit.Framework.Assert.IsTrue("the node should be decommissioned", dead[0].IsDecommissioned
				());
			// Add the node back
			cluster.RestartDataNode(dataNodeProperties, true);
			cluster.WaitActive();
			// Call refreshNodes on FSNamesystem with empty exclude file.
			// This will remove the datanodes from decommissioning list and
			// make them available again.
			WriteConfigFile(localFileSys, excludeFile, null);
			dm.RefreshNodes(conf);
		}

		/// <summary>Verify the support for decommissioning a datanode that is already dead.</summary>
		/// <remarks>
		/// Verify the support for decommissioning a datanode that is already dead.
		/// Under this scenario the datanode should immediately be marked as
		/// DECOMMISSIONED
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDecommissionDeadDN()
		{
			Logger log = Logger.GetLogger(typeof(DecommissionManager));
			log.SetLevel(Level.Debug);
			DatanodeID dnID = cluster.GetDataNodes()[0].GetDatanodeId();
			string dnName = dnID.GetXferAddr();
			MiniDFSCluster.DataNodeProperties stoppedDN = cluster.StopDataNode(0);
			DFSTestUtil.WaitForDatanodeState(cluster, dnID.GetDatanodeUuid(), false, 30000);
			FSNamesystem fsn = cluster.GetNamesystem();
			DatanodeManager dm = fsn.GetBlockManager().GetDatanodeManager();
			DatanodeDescriptor dnDescriptor = dm.GetDatanode(dnID);
			DecommissionNode(fsn, localFileSys, dnName);
			dm.RefreshNodes(conf);
			BlockManagerTestUtil.RecheckDecommissionState(dm);
			NUnit.Framework.Assert.IsTrue(dnDescriptor.IsDecommissioned());
			// Add the node back
			cluster.RestartDataNode(stoppedDN, true);
			cluster.WaitActive();
			// Call refreshNodes on FSNamesystem with empty exclude file to remove the
			// datanode from decommissioning list and make it available again.
			WriteConfigFile(localFileSys, excludeFile, null);
			dm.RefreshNodes(conf);
		}
	}
}
