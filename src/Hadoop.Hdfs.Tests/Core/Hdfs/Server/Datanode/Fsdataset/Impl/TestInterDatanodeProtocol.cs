using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>This tests InterDataNodeProtocol for block handling.</summary>
	public class TestInterDatanodeProtocol
	{
		private const string Address = "0.0.0.0";

		private const int PingInterval = 1000;

		private const int MinSleepTime = 1000;

		private static readonly Configuration conf = new HdfsConfiguration();

		private class TestServer : Org.Apache.Hadoop.Ipc.Server
		{
			private bool sleep;

			private Type responseClass;

			/// <exception cref="System.IO.IOException"/>
			public TestServer(int handlerCount, bool sleep)
				: this(handlerCount, sleep, typeof(LongWritable), null)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public TestServer(int handlerCount, bool sleep, Type paramClass, Type responseClass
				)
				: base(Address, 0, paramClass, handlerCount, conf)
			{
				this.sleep = sleep;
				this.responseClass = responseClass;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Writable Call(RPC.RpcKind rpcKind, string protocol, Writable param
				, long receiveTime)
			{
				if (sleep)
				{
					// sleep a bit
					try
					{
						Sharpen.Thread.Sleep(PingInterval + MinSleepTime);
					}
					catch (Exception)
					{
					}
				}
				if (responseClass != null)
				{
					try
					{
						return System.Activator.CreateInstance(responseClass);
					}
					catch (Exception e)
					{
						throw new RuntimeException(e);
					}
				}
				else
				{
					return param;
				}
			}
			// echo param as result
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CheckMetaInfo(ExtendedBlock b, DataNode dn)
		{
			Block metainfo = DataNodeTestUtils.GetFSDataset(dn).GetStoredBlock(b.GetBlockPoolId
				(), b.GetBlockId());
			NUnit.Framework.Assert.AreEqual(b.GetBlockId(), metainfo.GetBlockId());
			NUnit.Framework.Assert.AreEqual(b.GetNumBytes(), metainfo.GetNumBytes());
		}

		/// <exception cref="System.IO.IOException"/>
		public static LocatedBlock GetLastLocatedBlock(ClientProtocol namenode, string src
			)
		{
			//get block info for the last block
			LocatedBlocks locations = namenode.GetBlockLocations(src, 0, long.MaxValue);
			IList<LocatedBlock> blocks = locations.GetLocatedBlocks();
			DataNode.Log.Info("blocks.size()=" + blocks.Count);
			NUnit.Framework.Assert.IsTrue(blocks.Count > 0);
			return blocks[blocks.Count - 1];
		}

		/// <summary>Test block MD access via a DN</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockMetaDataInfo()
		{
			CheckBlockMetaDataInfo(false);
		}

		/// <summary>The same as above, but use hostnames for DN<->DN communication</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockMetaDataInfoWithHostname()
		{
			Assume.AssumeTrue(Runtime.GetProperty("os.name").StartsWith("Linux"));
			CheckBlockMetaDataInfo(true);
		}

		/// <summary>The following test first creates a file.</summary>
		/// <remarks>
		/// The following test first creates a file.
		/// It verifies the block information from a datanode.
		/// Then, it updates the block with new information and verifies again.
		/// </remarks>
		/// <param name="useDnHostname">whether DNs should connect to other DNs by hostname</param>
		/// <exception cref="System.Exception"/>
		private void CheckBlockMetaDataInfo(bool useDnHostname)
		{
			MiniDFSCluster cluster = null;
			conf.SetBoolean(DFSConfigKeys.DfsDatanodeUseDnHostname, useDnHostname);
			if (useDnHostname)
			{
				// Since the mini cluster only listens on the loopback we have to
				// ensure the hostname used to access DNs maps to the loopback. We
				// do this by telling the DN to advertise localhost as its hostname
				// instead of the default hostname.
				conf.Set(DFSConfigKeys.DfsDatanodeHostNameKey, "localhost");
			}
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).CheckDataNodeHostConfig
					(true).Build();
				cluster.WaitActive();
				//create a file
				DistributedFileSystem dfs = cluster.GetFileSystem();
				string filestr = "/foo";
				Path filepath = new Path(filestr);
				DFSTestUtil.CreateFile(dfs, filepath, 1024L, (short)3, 0L);
				NUnit.Framework.Assert.IsTrue(dfs.Exists(filepath));
				//get block info
				LocatedBlock locatedblock = GetLastLocatedBlock(DFSClientAdapter.GetDFSClient(dfs
					).GetNamenode(), filestr);
				DatanodeInfo[] datanodeinfo = locatedblock.GetLocations();
				NUnit.Framework.Assert.IsTrue(datanodeinfo.Length > 0);
				//connect to a data node
				DataNode datanode = cluster.GetDataNode(datanodeinfo[0].GetIpcPort());
				InterDatanodeProtocol idp = DataNodeTestUtils.CreateInterDatanodeProtocolProxy(datanode
					, datanodeinfo[0], conf, useDnHostname);
				// Stop the block scanners.
				datanode.GetBlockScanner().RemoveAllVolumeScanners();
				//verify BlockMetaDataInfo
				ExtendedBlock b = locatedblock.GetBlock();
				InterDatanodeProtocol.Log.Info("b=" + b + ", " + b.GetType());
				CheckMetaInfo(b, datanode);
				long recoveryId = b.GetGenerationStamp() + 1;
				idp.InitReplicaRecovery(new BlockRecoveryCommand.RecoveringBlock(b, locatedblock.
					GetLocations(), recoveryId));
				//verify updateBlock
				ExtendedBlock newblock = new ExtendedBlock(b.GetBlockPoolId(), b.GetBlockId(), b.
					GetNumBytes() / 2, b.GetGenerationStamp() + 1);
				idp.UpdateReplicaUnderRecovery(b, recoveryId, b.GetBlockId(), newblock.GetNumBytes
					());
				CheckMetaInfo(newblock, datanode);
				// Verify correct null response trying to init recovery for a missing block
				ExtendedBlock badBlock = new ExtendedBlock("fake-pool", b.GetBlockId(), 0, 0);
				NUnit.Framework.Assert.IsNull(idp.InitReplicaRecovery(new BlockRecoveryCommand.RecoveringBlock
					(badBlock, locatedblock.GetLocations(), recoveryId)));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private static ReplicaInfo CreateReplicaInfo(Block b)
		{
			return new FinalizedReplica(b, null, null);
		}

		private static void AssertEquals(ReplicaInfo originalInfo, ReplicaRecoveryInfo recoveryInfo
			)
		{
			NUnit.Framework.Assert.AreEqual(originalInfo.GetBlockId(), recoveryInfo.GetBlockId
				());
			NUnit.Framework.Assert.AreEqual(originalInfo.GetGenerationStamp(), recoveryInfo.GetGenerationStamp
				());
			NUnit.Framework.Assert.AreEqual(originalInfo.GetBytesOnDisk(), recoveryInfo.GetNumBytes
				());
			NUnit.Framework.Assert.AreEqual(originalInfo.GetState(), recoveryInfo.GetOriginalReplicaState
				());
		}

		/// <summary>
		/// Test
		/// <see cref="FsDatasetImpl.InitReplicaRecovery(string, ReplicaMap, Org.Apache.Hadoop.Hdfs.Protocol.Block, long, long)
		/// 	"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInitReplicaRecovery()
		{
			long firstblockid = 10000L;
			long gs = 7777L;
			long length = 22L;
			ReplicaMap map = new ReplicaMap(this);
			string bpid = "BP-TEST";
			Block[] blocks = new Block[5];
			for (int i = 0; i < blocks.Length; i++)
			{
				blocks[i] = new Block(firstblockid + i, length, gs);
				map.Add(bpid, CreateReplicaInfo(blocks[i]));
			}
			{
				//normal case
				Block b = blocks[0];
				ReplicaInfo originalInfo = map.Get(bpid, b);
				long recoveryid = gs + 1;
				ReplicaRecoveryInfo recoveryInfo = FsDatasetImpl.InitReplicaRecovery(bpid, map, blocks
					[0], recoveryid, DFSConfigKeys.DfsDatanodeXceiverStopTimeoutMillisDefault);
				AssertEquals(originalInfo, recoveryInfo);
				ReplicaUnderRecovery updatedInfo = (ReplicaUnderRecovery)map.Get(bpid, b);
				NUnit.Framework.Assert.AreEqual(originalInfo.GetBlockId(), updatedInfo.GetBlockId
					());
				NUnit.Framework.Assert.AreEqual(recoveryid, updatedInfo.GetRecoveryID());
				//recover one more time 
				long recoveryid2 = gs + 2;
				ReplicaRecoveryInfo recoveryInfo2 = FsDatasetImpl.InitReplicaRecovery(bpid, map, 
					blocks[0], recoveryid2, DFSConfigKeys.DfsDatanodeXceiverStopTimeoutMillisDefault
					);
				AssertEquals(originalInfo, recoveryInfo2);
				ReplicaUnderRecovery updatedInfo2 = (ReplicaUnderRecovery)map.Get(bpid, b);
				NUnit.Framework.Assert.AreEqual(originalInfo.GetBlockId(), updatedInfo2.GetBlockId
					());
				NUnit.Framework.Assert.AreEqual(recoveryid2, updatedInfo2.GetRecoveryID());
				//case RecoveryInProgressException
				try
				{
					FsDatasetImpl.InitReplicaRecovery(bpid, map, b, recoveryid, DFSConfigKeys.DfsDatanodeXceiverStopTimeoutMillisDefault
						);
					NUnit.Framework.Assert.Fail();
				}
				catch (RecoveryInProgressException ripe)
				{
					System.Console.Out.WriteLine("GOOD: getting " + ripe);
				}
			}
			{
				// BlockRecoveryFI_01: replica not found
				long recoveryid = gs + 1;
				Block b = new Block(firstblockid - 1, length, gs);
				ReplicaRecoveryInfo r = FsDatasetImpl.InitReplicaRecovery(bpid, map, b, recoveryid
					, DFSConfigKeys.DfsDatanodeXceiverStopTimeoutMillisDefault);
				NUnit.Framework.Assert.IsNull("Data-node should not have this replica.", r);
			}
			{
				// BlockRecoveryFI_02: "THIS IS NOT SUPPOSED TO HAPPEN" with recovery id < gs  
				long recoveryid = gs - 1;
				Block b = new Block(firstblockid + 1, length, gs);
				try
				{
					FsDatasetImpl.InitReplicaRecovery(bpid, map, b, recoveryid, DFSConfigKeys.DfsDatanodeXceiverStopTimeoutMillisDefault
						);
					NUnit.Framework.Assert.Fail();
				}
				catch (IOException ioe)
				{
					System.Console.Out.WriteLine("GOOD: getting " + ioe);
				}
			}
			{
				// BlockRecoveryFI_03: Replica's gs is less than the block's gs
				long recoveryid = gs + 1;
				Block b = new Block(firstblockid, length, gs + 1);
				try
				{
					FsDatasetImpl.InitReplicaRecovery(bpid, map, b, recoveryid, DFSConfigKeys.DfsDatanodeXceiverStopTimeoutMillisDefault
						);
					NUnit.Framework.Assert.Fail("InitReplicaRecovery should fail because replica's " 
						+ "gs is less than the block's gs");
				}
				catch (IOException e)
				{
					e.Message.StartsWith("replica.getGenerationStamp() < block.getGenerationStamp(), block="
						);
				}
			}
		}

		/// <summary>
		/// Test  for
		/// <see cref="FsDatasetImpl.UpdateReplicaUnderRecovery(Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, long, long, long)
		/// 	"/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdateReplicaUnderRecovery()
		{
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				cluster.WaitActive();
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				//create a file
				DistributedFileSystem dfs = cluster.GetFileSystem();
				string filestr = "/foo";
				Path filepath = new Path(filestr);
				DFSTestUtil.CreateFile(dfs, filepath, 1024L, (short)3, 0L);
				//get block info
				LocatedBlock locatedblock = GetLastLocatedBlock(DFSClientAdapter.GetDFSClient(dfs
					).GetNamenode(), filestr);
				DatanodeInfo[] datanodeinfo = locatedblock.GetLocations();
				NUnit.Framework.Assert.IsTrue(datanodeinfo.Length > 0);
				//get DataNode and FSDataset objects
				DataNode datanode = cluster.GetDataNode(datanodeinfo[0].GetIpcPort());
				NUnit.Framework.Assert.IsTrue(datanode != null);
				//initReplicaRecovery
				ExtendedBlock b = locatedblock.GetBlock();
				long recoveryid = b.GetGenerationStamp() + 1;
				long newlength = b.GetNumBytes() - 1;
				FsDatasetSpi<object> fsdataset = DataNodeTestUtils.GetFSDataset(datanode);
				ReplicaRecoveryInfo rri = fsdataset.InitReplicaRecovery(new BlockRecoveryCommand.RecoveringBlock
					(b, null, recoveryid));
				//check replica
				ReplicaInfo replica = FsDatasetTestUtil.FetchReplicaInfo(fsdataset, bpid, b.GetBlockId
					());
				NUnit.Framework.Assert.AreEqual(HdfsServerConstants.ReplicaState.Rur, replica.GetState
					());
				//check meta data before update
				FsDatasetImpl.CheckReplicaFiles(replica);
				{
					//case "THIS IS NOT SUPPOSED TO HAPPEN"
					//with (block length) != (stored replica's on disk length). 
					//create a block with same id and gs but different length.
					ExtendedBlock tmp = new ExtendedBlock(b.GetBlockPoolId(), rri.GetBlockId(), rri.GetNumBytes
						() - 1, rri.GetGenerationStamp());
					try
					{
						//update should fail
						fsdataset.UpdateReplicaUnderRecovery(tmp, recoveryid, tmp.GetBlockId(), newlength
							);
						NUnit.Framework.Assert.Fail();
					}
					catch (IOException ioe)
					{
						System.Console.Out.WriteLine("GOOD: getting " + ioe);
					}
				}
				//update
				string storageID = fsdataset.UpdateReplicaUnderRecovery(new ExtendedBlock(b.GetBlockPoolId
					(), rri), recoveryid, rri.GetBlockId(), newlength);
				NUnit.Framework.Assert.IsTrue(storageID != null);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test to verify that InterDatanode RPC timesout as expected when
		/// the server DN does not respond.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestInterDNProtocolTimeout()
		{
			Org.Apache.Hadoop.Ipc.Server server = new TestInterDatanodeProtocol.TestServer(1, 
				true);
			server.Start();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			DatanodeID fakeDnId = DFSTestUtil.GetLocalDatanodeID(addr.Port);
			DatanodeInfo dInfo = new DatanodeInfo(fakeDnId);
			InterDatanodeProtocol proxy = null;
			try
			{
				proxy = DataNode.CreateInterDataNodeProtocolProxy(dInfo, conf, 500, false);
				proxy.InitReplicaRecovery(new BlockRecoveryCommand.RecoveringBlock(new ExtendedBlock
					("bpid", 1), null, 100));
				NUnit.Framework.Assert.Fail("Expected SocketTimeoutException exception, but did not get."
					);
			}
			finally
			{
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
				server.Stop();
			}
		}
	}
}
