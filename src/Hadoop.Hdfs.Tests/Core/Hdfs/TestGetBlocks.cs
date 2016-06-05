using System;
using System.Collections.Generic;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests if getblocks request works correctly.</summary>
	public class TestGetBlocks
	{
		private const int blockSize = 8192;

		private static readonly string[] racks = new string[] { "/d1/r1", "/d1/r1", "/d1/r2"
			, "/d1/r2", "/d1/r2", "/d2/r3", "/d2/r3" };

		private static readonly int numDatanodes = racks.Length;

		/// <summary>Stop the heartbeat of a datanode in the MiniDFSCluster</summary>
		/// <param name="cluster">The MiniDFSCluster</param>
		/// <param name="hostName">The hostName of the datanode to be stopped</param>
		/// <returns>The DataNode whose heartbeat has been stopped</returns>
		private DataNode StopDataNodeHeartbeat(MiniDFSCluster cluster, string hostName)
		{
			foreach (DataNode dn in cluster.GetDataNodes())
			{
				if (dn.GetDatanodeId().GetHostName().Equals(hostName))
				{
					DataNodeTestUtils.SetHeartbeatsDisabledForTests(dn, true);
					return dn;
				}
			}
			return null;
		}

		/// <summary>
		/// Test if the datanodes returned by
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetBlockLocations(string, long, long)
		/// 	"/>
		/// is correct
		/// when stale nodes checking is enabled. Also test during the scenario when 1)
		/// stale nodes checking is enabled, 2) a writing is going on, 3) a datanode
		/// becomes stale happen simultaneously
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReadSelectNonStaleDatanode()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForReadKey, true);
			long staleInterval = 30 * 1000 * 60;
			conf.SetLong(DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalKey, staleInterval);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes
				).Racks(racks).Build();
			cluster.WaitActive();
			IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
			DFSClient client = new DFSClient(addr, conf);
			IList<DatanodeDescriptor> nodeInfoList = cluster.GetNameNode().GetNamesystem().GetBlockManager
				().GetDatanodeManager().GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.Live);
			NUnit.Framework.Assert.AreEqual("Unexpected number of datanodes", numDatanodes, nodeInfoList
				.Count);
			FileSystem fileSys = cluster.GetFileSystem();
			FSDataOutputStream stm = null;
			try
			{
				// do the writing but do not close the FSDataOutputStream
				// in order to mimic the ongoing writing
				Path fileName = new Path("/file1");
				stm = fileSys.Create(fileName, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
					.IoFileBufferSizeKey, 4096), (short)3, blockSize);
				stm.Write(new byte[(blockSize * 3) / 2]);
				// We do not close the stream so that
				// the writing seems to be still ongoing
				stm.Hflush();
				LocatedBlocks blocks = client.GetNamenode().GetBlockLocations(fileName.ToString()
					, 0, blockSize);
				DatanodeInfo[] nodes = blocks.Get(0).GetLocations();
				NUnit.Framework.Assert.AreEqual(nodes.Length, 3);
				DataNode staleNode = null;
				DatanodeDescriptor staleNodeInfo = null;
				// stop the heartbeat of the first node
				staleNode = this.StopDataNodeHeartbeat(cluster, nodes[0].GetHostName());
				NUnit.Framework.Assert.IsNotNull(staleNode);
				// set the first node as stale
				staleNodeInfo = cluster.GetNameNode().GetNamesystem().GetBlockManager().GetDatanodeManager
					().GetDatanode(staleNode.GetDatanodeId());
				DFSTestUtil.ResetLastUpdatesWithOffset(staleNodeInfo, -(staleInterval + 1));
				LocatedBlocks blocksAfterStale = client.GetNamenode().GetBlockLocations(fileName.
					ToString(), 0, blockSize);
				DatanodeInfo[] nodesAfterStale = blocksAfterStale.Get(0).GetLocations();
				NUnit.Framework.Assert.AreEqual(nodesAfterStale.Length, 3);
				NUnit.Framework.Assert.AreEqual(nodesAfterStale[2].GetHostName(), nodes[0].GetHostName
					());
				// restart the staleNode's heartbeat
				DataNodeTestUtils.SetHeartbeatsDisabledForTests(staleNode, false);
				// reset the first node as non-stale, so as to avoid two stale nodes
				DFSTestUtil.ResetLastUpdatesWithOffset(staleNodeInfo, 0);
				LocatedBlock lastBlock = client.GetLocatedBlocks(fileName.ToString(), 0, long.MaxValue
					).GetLastLocatedBlock();
				nodes = lastBlock.GetLocations();
				NUnit.Framework.Assert.AreEqual(nodes.Length, 3);
				// stop the heartbeat of the first node for the last block
				staleNode = this.StopDataNodeHeartbeat(cluster, nodes[0].GetHostName());
				NUnit.Framework.Assert.IsNotNull(staleNode);
				// set the node as stale
				DatanodeDescriptor dnDesc = cluster.GetNameNode().GetNamesystem().GetBlockManager
					().GetDatanodeManager().GetDatanode(staleNode.GetDatanodeId());
				DFSTestUtil.ResetLastUpdatesWithOffset(dnDesc, -(staleInterval + 1));
				LocatedBlock lastBlockAfterStale = client.GetLocatedBlocks(fileName.ToString(), 0
					, long.MaxValue).GetLastLocatedBlock();
				nodesAfterStale = lastBlockAfterStale.GetLocations();
				NUnit.Framework.Assert.AreEqual(nodesAfterStale.Length, 3);
				NUnit.Framework.Assert.AreEqual(nodesAfterStale[2].GetHostName(), nodes[0].GetHostName
					());
			}
			finally
			{
				if (stm != null)
				{
					stm.Close();
				}
				client.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>test getBlocks</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetBlocks()
		{
			Configuration Conf = new HdfsConfiguration();
			short ReplicationFactor = (short)2;
			int DefaultBlockSize = 1024;
			Random r = new Random();
			Conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, DefaultBlockSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(ReplicationFactor
				).Build();
			try
			{
				cluster.WaitActive();
				// create a file with two blocks
				FileSystem fs = cluster.GetFileSystem();
				FSDataOutputStream @out = fs.Create(new Path("/tmp.txt"), ReplicationFactor);
				byte[] data = new byte[1024];
				long fileLen = 2 * DefaultBlockSize;
				long bytesToWrite = fileLen;
				while (bytesToWrite > 0)
				{
					r.NextBytes(data);
					int bytesToWriteNext = (1024 < bytesToWrite) ? 1024 : (int)bytesToWrite;
					@out.Write(data, 0, bytesToWriteNext);
					bytesToWrite -= bytesToWriteNext;
				}
				@out.Close();
				// get blocks & data nodes
				IList<LocatedBlock> locatedBlocks;
				DatanodeInfo[] dataNodes = null;
				bool notWritten;
				do
				{
					DFSClient dfsclient = new DFSClient(NameNode.GetAddress(Conf), Conf);
					locatedBlocks = dfsclient.GetNamenode().GetBlockLocations("/tmp.txt", 0, fileLen)
						.GetLocatedBlocks();
					NUnit.Framework.Assert.AreEqual(2, locatedBlocks.Count);
					notWritten = false;
					for (int i = 0; i < 2; i++)
					{
						dataNodes = locatedBlocks[i].GetLocations();
						if (dataNodes.Length != ReplicationFactor)
						{
							notWritten = true;
							try
							{
								Sharpen.Thread.Sleep(10);
							}
							catch (Exception)
							{
							}
							break;
						}
					}
				}
				while (notWritten);
				// get RPC client to namenode
				IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
				NamenodeProtocol namenode = NameNodeProxies.CreateProxy<NamenodeProtocol>(Conf, NameNode
					.GetUri(addr)).GetProxy();
				// get blocks of size fileLen from dataNodes[0]
				BlocksWithLocations.BlockWithLocations[] locs;
				locs = namenode.GetBlocks(dataNodes[0], fileLen).GetBlocks();
				NUnit.Framework.Assert.AreEqual(locs.Length, 2);
				NUnit.Framework.Assert.AreEqual(locs[0].GetStorageIDs().Length, 2);
				NUnit.Framework.Assert.AreEqual(locs[1].GetStorageIDs().Length, 2);
				// get blocks of size BlockSize from dataNodes[0]
				locs = namenode.GetBlocks(dataNodes[0], DefaultBlockSize).GetBlocks();
				NUnit.Framework.Assert.AreEqual(locs.Length, 1);
				NUnit.Framework.Assert.AreEqual(locs[0].GetStorageIDs().Length, 2);
				// get blocks of size 1 from dataNodes[0]
				locs = namenode.GetBlocks(dataNodes[0], 1).GetBlocks();
				NUnit.Framework.Assert.AreEqual(locs.Length, 1);
				NUnit.Framework.Assert.AreEqual(locs[0].GetStorageIDs().Length, 2);
				// get blocks of size 0 from dataNodes[0]
				GetBlocksWithException(namenode, dataNodes[0], 0);
				// get blocks of size -1 from dataNodes[0]
				GetBlocksWithException(namenode, dataNodes[0], -1);
				// get blocks of size BlockSize from a non-existent datanode
				DatanodeInfo info = DFSTestUtil.GetDatanodeInfo("1.2.3.4");
				GetBlocksWithException(namenode, info, 2);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void GetBlocksWithException(NamenodeProtocol namenode, DatanodeInfo datanode
			, long size)
		{
			bool getException = false;
			try
			{
				namenode.GetBlocks(DFSTestUtil.GetLocalDatanodeInfo(), 2);
			}
			catch (RemoteException e)
			{
				getException = true;
				NUnit.Framework.Assert.IsTrue(e.GetClassName().Contains("HadoopIllegalArgumentException"
					));
			}
			NUnit.Framework.Assert.IsTrue(getException);
		}

		[NUnit.Framework.Test]
		public virtual void TestBlockKey()
		{
			IDictionary<Block, long> map = new Dictionary<Block, long>();
			Random Ran = new Random();
			long seed = Ran.NextLong();
			System.Console.Out.WriteLine("seed=" + seed);
			Ran.SetSeed(seed);
			long[] blkids = new long[10];
			for (int i = 0; i < blkids.Length; i++)
			{
				blkids[i] = 1000L + Ran.Next(100000);
				map[new Block(blkids[i], 0, blkids[i])] = blkids[i];
			}
			System.Console.Out.WriteLine("map=" + map.ToString().Replace(",", "\n  "));
			for (int i_1 = 0; i_1 < blkids.Length; i_1++)
			{
				Block b = new Block(blkids[i_1], 0, GenerationStamp.GrandfatherGenerationStamp);
				long v = map[b];
				System.Console.Out.WriteLine(b + " => " + v);
				NUnit.Framework.Assert.AreEqual(blkids[i_1], v);
			}
		}
	}
}
