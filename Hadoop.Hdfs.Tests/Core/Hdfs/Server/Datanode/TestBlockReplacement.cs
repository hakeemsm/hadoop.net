using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This class tests if block replacement request to data nodes work correctly.
	/// 	</summary>
	public class TestBlockReplacement
	{
		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestBlockReplacement"
			);

		internal MiniDFSCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestThrottler()
		{
			Configuration conf = new HdfsConfiguration();
			FileSystem.SetDefaultUri(conf, "hdfs://localhost:0");
			long bandwidthPerSec = 1024 * 1024L;
			long TotalBytes = 6 * bandwidthPerSec;
			long bytesToSend = TotalBytes;
			long start = Time.MonotonicNow();
			DataTransferThrottler throttler = new DataTransferThrottler(bandwidthPerSec);
			long totalBytes = 0L;
			long bytesSent = 1024 * 512L;
			// 0.5MB
			throttler.Throttle(bytesSent);
			bytesToSend -= bytesSent;
			bytesSent = 1024 * 768L;
			// 0.75MB
			throttler.Throttle(bytesSent);
			bytesToSend -= bytesSent;
			try
			{
				Sharpen.Thread.Sleep(1000);
			}
			catch (Exception)
			{
			}
			throttler.Throttle(bytesToSend);
			long end = Time.MonotonicNow();
			NUnit.Framework.Assert.IsTrue(totalBytes * 1000 / (end - start) <= bandwidthPerSec
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReplacement()
		{
			Configuration Conf = new HdfsConfiguration();
			string[] InitialRacks = new string[] { "/RACK0", "/RACK1", "/RACK2" };
			string[] NewRacks = new string[] { "/RACK2" };
			short ReplicationFactor = (short)3;
			int DefaultBlockSize = 1024;
			Random r = new Random();
			Conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, DefaultBlockSize);
			Conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, DefaultBlockSize / 2);
			Conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 500);
			cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(ReplicationFactor).Racks(
				InitialRacks).Build();
			try
			{
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				Path fileName = new Path("/tmp.txt");
				// create a file with one block
				DFSTestUtil.CreateFile(fs, fileName, DefaultBlockSize, ReplicationFactor, r.NextLong
					());
				DFSTestUtil.WaitReplication(fs, fileName, ReplicationFactor);
				// get all datanodes
				IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
				DFSClient client = new DFSClient(addr, Conf);
				IList<LocatedBlock> locatedBlocks = client.GetNamenode().GetBlockLocations("/tmp.txt"
					, 0, DefaultBlockSize).GetLocatedBlocks();
				NUnit.Framework.Assert.AreEqual(1, locatedBlocks.Count);
				LocatedBlock block = locatedBlocks[0];
				DatanodeInfo[] oldNodes = block.GetLocations();
				NUnit.Framework.Assert.AreEqual(oldNodes.Length, 3);
				ExtendedBlock b = block.GetBlock();
				// add a fourth datanode to the cluster
				cluster.StartDataNodes(Conf, 1, true, null, NewRacks);
				cluster.WaitActive();
				DatanodeInfo[] datanodes = client.DatanodeReport(HdfsConstants.DatanodeReportType
					.All);
				// find out the new node
				DatanodeInfo newNode = null;
				foreach (DatanodeInfo node in datanodes)
				{
					bool isNewNode = true;
					foreach (DatanodeInfo oldNode in oldNodes)
					{
						if (node.Equals(oldNode))
						{
							isNewNode = false;
							break;
						}
					}
					if (isNewNode)
					{
						newNode = node;
						break;
					}
				}
				NUnit.Framework.Assert.IsTrue(newNode != null);
				DatanodeInfo source = null;
				AList<DatanodeInfo> proxies = new AList<DatanodeInfo>(2);
				foreach (DatanodeInfo node_1 in datanodes)
				{
					if (node_1 != newNode)
					{
						if (node_1.GetNetworkLocation().Equals(newNode.GetNetworkLocation()))
						{
							source = node_1;
						}
						else
						{
							proxies.AddItem(node_1);
						}
					}
				}
				//current state: the newNode is on RACK2, and "source" is the other dn on RACK2.
				//the two datanodes on RACK0 and RACK1 are in "proxies".
				//"source" and both "proxies" all contain the block, while newNode doesn't yet.
				NUnit.Framework.Assert.IsTrue(source != null && proxies.Count == 2);
				// start to replace the block
				// case 1: proxySource does not contain the block
				Log.Info("Testcase 1: Proxy " + newNode + " does not contain the block " + b);
				NUnit.Framework.Assert.IsFalse(ReplaceBlock(b, source, newNode, proxies[0]));
				// case 2: destination already contains the block
				Log.Info("Testcase 2: Destination " + proxies[1] + " contains the block " + b);
				NUnit.Framework.Assert.IsFalse(ReplaceBlock(b, source, proxies[0], proxies[1]));
				// case 3: correct case
				Log.Info("Testcase 3: Source=" + source + " Proxy=" + proxies[0] + " Destination="
					 + newNode);
				NUnit.Framework.Assert.IsTrue(ReplaceBlock(b, source, proxies[0], newNode));
				// after cluster has time to resolve the over-replication,
				// block locations should contain two proxies and newNode
				// but not source
				CheckBlocks(new DatanodeInfo[] { newNode, proxies[0], proxies[1] }, fileName.ToString
					(), DefaultBlockSize, ReplicationFactor, client);
				// case 4: proxies.get(0) is not a valid del hint
				// expect either source or newNode replica to be deleted instead
				Log.Info("Testcase 4: invalid del hint " + proxies[0]);
				NUnit.Framework.Assert.IsTrue(ReplaceBlock(b, proxies[0], proxies[1], source));
				// after cluster has time to resolve the over-replication,
				// block locations should contain two proxies,
				// and either source or newNode, but not both.
				CheckBlocks(Sharpen.Collections.ToArray(proxies, new DatanodeInfo[proxies.Count])
					, fileName.ToString(), DefaultBlockSize, ReplicationFactor, client);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockMoveAcrossStorageInSameNode()
		{
			Configuration conf = new HdfsConfiguration();
			// create only one datanode in the cluster to verify movement within
			// datanode.
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).StorageTypes
				(new StorageType[] { StorageType.Disk, StorageType.Archive }).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				Path file = new Path("/testBlockMoveAcrossStorageInSameNode/file");
				DFSTestUtil.CreateFile(dfs, file, 1024, (short)1, 1024);
				LocatedBlocks locatedBlocks = dfs.GetClient().GetLocatedBlocks(file.ToString(), 0
					);
				// get the current 
				LocatedBlock locatedBlock = locatedBlocks.Get(0);
				ExtendedBlock block = locatedBlock.GetBlock();
				DatanodeInfo[] locations = locatedBlock.GetLocations();
				NUnit.Framework.Assert.AreEqual(1, locations.Length);
				StorageType[] storageTypes = locatedBlock.GetStorageTypes();
				// current block should be written to DISK
				NUnit.Framework.Assert.IsTrue(storageTypes[0] == StorageType.Disk);
				DatanodeInfo source = locations[0];
				// move block to ARCHIVE by using same DataNodeInfo for source, proxy and
				// destination so that movement happens within datanode 
				NUnit.Framework.Assert.IsTrue(ReplaceBlock(block, source, source, source, StorageType
					.Archive));
				// wait till namenode notified
				Sharpen.Thread.Sleep(3000);
				locatedBlocks = dfs.GetClient().GetLocatedBlocks(file.ToString(), 0);
				// get the current 
				locatedBlock = locatedBlocks.Get(0);
				NUnit.Framework.Assert.AreEqual("Storage should be only one", 1, locatedBlock.GetLocations
					().Length);
				NUnit.Framework.Assert.IsTrue("Block should be moved to ARCHIVE", locatedBlock.GetStorageTypes
					()[0] == StorageType.Archive);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/* check if file's blocks have expected number of replicas,
		* and exist at all of includeNodes
		*/
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private void CheckBlocks(DatanodeInfo[] includeNodes, string fileName, long fileLen
			, short replFactor, DFSClient client)
		{
			bool notDone;
			long Timeout = 20000L;
			long starttime = Time.MonotonicNow();
			long failtime = starttime + Timeout;
			do
			{
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception)
				{
				}
				IList<LocatedBlock> blocks = client.GetNamenode().GetBlockLocations(fileName, 0, 
					fileLen).GetLocatedBlocks();
				NUnit.Framework.Assert.AreEqual(1, blocks.Count);
				DatanodeInfo[] nodes = blocks[0].GetLocations();
				notDone = (nodes.Length != replFactor);
				if (notDone)
				{
					Log.Info("Expected replication factor is " + replFactor + " but the real replication factor is "
						 + nodes.Length);
				}
				else
				{
					IList<DatanodeInfo> nodeLocations = Arrays.AsList(nodes);
					foreach (DatanodeInfo node in includeNodes)
					{
						if (!nodeLocations.Contains(node))
						{
							notDone = true;
							Log.Info("Block is not located at " + node);
							break;
						}
					}
				}
				if (Time.MonotonicNow() > failtime)
				{
					string expectedNodesList = string.Empty;
					string currentNodesList = string.Empty;
					foreach (DatanodeInfo dn in includeNodes)
					{
						expectedNodesList += dn + ", ";
					}
					foreach (DatanodeInfo dn_1 in nodes)
					{
						currentNodesList += dn_1 + ", ";
					}
					Log.Info("Expected replica nodes are: " + expectedNodesList);
					Log.Info("Current actual replica nodes are: " + currentNodesList);
					throw new TimeoutException("Did not achieve expected replication to expected nodes "
						 + "after more than " + Timeout + " msec.  See logs for details.");
				}
			}
			while (notDone);
			Log.Info("Achieved expected replication values in " + (Time.Now() - starttime) + 
				" msec.");
		}

		/* Copy a block from sourceProxy to destination. If the block becomes
		* over-replicated, preferably remove it from source.
		*
		* Return true if a block is successfully copied; otherwise false.
		*/
		/// <exception cref="System.IO.IOException"/>
		private bool ReplaceBlock(ExtendedBlock block, DatanodeInfo source, DatanodeInfo 
			sourceProxy, DatanodeInfo destination)
		{
			return ReplaceBlock(block, source, sourceProxy, destination, StorageType.Default);
		}

		/*
		* Replace block
		*/
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Net.Sockets.SocketException"/>
		private bool ReplaceBlock(ExtendedBlock block, DatanodeInfo source, DatanodeInfo 
			sourceProxy, DatanodeInfo destination, StorageType targetStorageType)
		{
			Socket sock = new Socket();
			try
			{
				sock.Connect(NetUtils.CreateSocketAddr(destination.GetXferAddr()), HdfsServerConstants
					.ReadTimeout);
				sock.SetKeepAlive(true);
				// sendRequest
				DataOutputStream @out = new DataOutputStream(sock.GetOutputStream());
				new Sender(@out).ReplaceBlock(block, targetStorageType, BlockTokenSecretManager.DummyToken
					, source.GetDatanodeUuid(), sourceProxy);
				@out.Flush();
				// receiveResponse
				DataInputStream reply = new DataInputStream(sock.GetInputStream());
				DataTransferProtos.BlockOpResponseProto proto = DataTransferProtos.BlockOpResponseProto
					.ParseDelimitedFrom(reply);
				while (proto.GetStatus() == DataTransferProtos.Status.InProgress)
				{
					proto = DataTransferProtos.BlockOpResponseProto.ParseDelimitedFrom(reply);
				}
				return proto.GetStatus() == DataTransferProtos.Status.Success;
			}
			finally
			{
				sock.Close();
			}
		}

		/// <summary>
		/// Standby namenode doesn't queue Delete block request when the add block
		/// request is in the edit log which are yet to be read.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletedBlockWhenAddBlockIsInEdit()
		{
			Configuration conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(1).Build();
			DFSClient client = null;
			try
			{
				cluster.WaitActive();
				NUnit.Framework.Assert.AreEqual("Number of namenodes is not 2", 2, cluster.GetNumNameNodes
					());
				// Transitioning the namenode 0 to active.
				cluster.TransitionToActive(0);
				NUnit.Framework.Assert.IsTrue("Namenode 0 should be in active state", cluster.GetNameNode
					(0).IsActiveState());
				NUnit.Framework.Assert.IsTrue("Namenode 1 should be in standby state", cluster.GetNameNode
					(1).IsStandbyState());
				// Trigger heartbeat to mark DatanodeStorageInfo#heartbeatedSinceFailover
				// to true.
				DataNodeTestUtils.TriggerHeartbeat(cluster.GetDataNodes()[0]);
				FileSystem fs = cluster.GetFileSystem(0);
				// Trigger blockReport to mark DatanodeStorageInfo#blockContentsStale
				// to false.
				cluster.GetDataNodes()[0].TriggerBlockReport(new BlockReportOptions.Factory().SetIncremental
					(false).Build());
				Path fileName = new Path("/tmp.txt");
				// create a file with one block
				DFSTestUtil.CreateFile(fs, fileName, 10L, (short)1, 1234L);
				DFSTestUtil.WaitReplication(fs, fileName, (short)1);
				client = new DFSClient(cluster.GetFileSystem(0).GetUri(), conf);
				IList<LocatedBlock> locatedBlocks = client.GetNamenode().GetBlockLocations("/tmp.txt"
					, 0, 10L).GetLocatedBlocks();
				NUnit.Framework.Assert.IsTrue(locatedBlocks.Count == 1);
				NUnit.Framework.Assert.IsTrue(locatedBlocks[0].GetLocations().Length == 1);
				// add a second datanode to the cluster
				cluster.StartDataNodes(conf, 1, true, null, null, null, null);
				NUnit.Framework.Assert.AreEqual("Number of datanodes should be 2", 2, cluster.GetDataNodes
					().Count);
				DataNode dn0 = cluster.GetDataNodes()[0];
				DataNode dn1 = cluster.GetDataNodes()[1];
				string activeNNBPId = cluster.GetNamesystem(0).GetBlockPoolId();
				DatanodeDescriptor sourceDnDesc = NameNodeAdapter.GetDatanode(cluster.GetNamesystem
					(0), dn0.GetDNRegistrationForBP(activeNNBPId));
				DatanodeDescriptor destDnDesc = NameNodeAdapter.GetDatanode(cluster.GetNamesystem
					(0), dn1.GetDNRegistrationForBP(activeNNBPId));
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, fileName);
				Log.Info("replaceBlock:  " + ReplaceBlock(block, (DatanodeInfo)sourceDnDesc, (DatanodeInfo
					)sourceDnDesc, (DatanodeInfo)destDnDesc));
				// Waiting for the FsDatasetAsyncDsikService to delete the block
				Sharpen.Thread.Sleep(3000);
				// Triggering the incremental block report to report the deleted block to
				// namnemode
				cluster.GetDataNodes()[0].TriggerBlockReport(new BlockReportOptions.Factory().SetIncremental
					(true).Build());
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
				NUnit.Framework.Assert.IsTrue("Namenode 1 should be in active state", cluster.GetNameNode
					(1).IsActiveState());
				NUnit.Framework.Assert.IsTrue("Namenode 0 should be in standby state", cluster.GetNameNode
					(0).IsStandbyState());
				client.Close();
				// Opening a new client for new active  namenode
				client = new DFSClient(cluster.GetFileSystem(1).GetUri(), conf);
				IList<LocatedBlock> locatedBlocks1 = client.GetNamenode().GetBlockLocations("/tmp.txt"
					, 0, 10L).GetLocatedBlocks();
				NUnit.Framework.Assert.AreEqual(1, locatedBlocks1.Count);
				NUnit.Framework.Assert.AreEqual("The block should be only on 1 datanode ", 1, locatedBlocks1
					[0].GetLocations().Length);
			}
			finally
			{
				IOUtils.Cleanup(null, client);
				cluster.Shutdown();
			}
		}

		/// <param name="args"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			(new Org.Apache.Hadoop.Hdfs.Server.Datanode.TestBlockReplacement()).TestBlockReplacement
				();
		}
	}
}
