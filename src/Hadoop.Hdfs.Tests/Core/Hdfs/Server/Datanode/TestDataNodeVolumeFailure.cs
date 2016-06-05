using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Fine-grain testing of block files and locations after volume failure.</summary>
	public class TestDataNodeVolumeFailure
	{
		private readonly int block_size = 512;

		internal MiniDFSCluster cluster = null;

		private Configuration conf;

		internal readonly int dn_num = 2;

		internal readonly int blocks_num = 30;

		internal readonly short repl = 2;

		internal FilePath dataDir = null;

		internal FilePath data_fail = null;

		internal FilePath failedDir = null;

		private FileSystem fs;

		private class BlockLocs
		{
			public int num_files = 0;

			public int num_locs = 0;

			internal BlockLocs(TestDataNodeVolumeFailure _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDataNodeVolumeFailure _enclosing;
			// mapping blocks to Meta files(physical files) and locs(NameNode locations)
		}

		internal readonly IDictionary<string, TestDataNodeVolumeFailure.BlockLocs> block_map
			 = new Dictionary<string, TestDataNodeVolumeFailure.BlockLocs>();

		// block id to BlockLocs
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			// bring up a cluster of 2
			conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, block_size);
			// Allow a single volume failure (there are two volumes)
			conf.SetInt(DFSConfigKeys.DfsDatanodeFailedVolumesToleratedKey, 1);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(dn_num).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			dataDir = new FilePath(cluster.GetDataDirectory());
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TearDown()
		{
			if (data_fail != null)
			{
				FileUtil.SetWritable(data_fail, true);
			}
			if (failedDir != null)
			{
				FileUtil.SetWritable(failedDir, true);
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/*
		* Verify the number of blocks and files are correct after volume failure,
		* and that we can replicate to both datanodes even after a single volume
		* failure if the configuration parameter allows this.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVolumeFailure()
		{
			System.Console.Out.WriteLine("Data dir: is " + dataDir.GetPath());
			// Data dir structure is dataDir/data[1-4]/[current,tmp...]
			// data1,2 is for datanode 1, data2,3 - datanode2 
			string filename = "/test.txt";
			Path filePath = new Path(filename);
			// we use only small number of blocks to avoid creating subdirs in the data dir..
			int filesize = block_size * blocks_num;
			DFSTestUtil.CreateFile(fs, filePath, filesize, repl, 1L);
			DFSTestUtil.WaitReplication(fs, filePath, repl);
			System.Console.Out.WriteLine("file " + filename + "(size " + filesize + ") is created and replicated"
				);
			// fail the volume
			// delete/make non-writable one of the directories (failed volume)
			data_fail = new FilePath(dataDir, "data3");
			failedDir = MiniDFSCluster.GetFinalizedDir(dataDir, cluster.GetNamesystem().GetBlockPoolId
				());
			if (failedDir.Exists() && !DeteteBlocks(failedDir))
			{
				//!FileUtil.fullyDelete(failedDir)
				throw new IOException("Could not delete hdfs directory '" + failedDir + "'");
			}
			data_fail.SetReadOnly();
			failedDir.SetReadOnly();
			System.Console.Out.WriteLine("Deleteing " + failedDir.GetPath() + "; exist=" + failedDir
				.Exists());
			// access all the blocks on the "failed" DataNode, 
			// we need to make sure that the "failed" volume is being accessed - 
			// and that will cause failure, blocks removal, "emergency" block report
			TriggerFailure(filename, filesize);
			// make sure a block report is sent 
			DataNode dn = cluster.GetDataNodes()[1];
			//corresponds to dir data3
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			DatanodeRegistration dnR = dn.GetDNRegistrationForBP(bpid);
			IDictionary<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists = dn.GetFSDataset
				().GetBlockReports(bpid);
			// Send block report
			StorageBlockReport[] reports = new StorageBlockReport[perVolumeBlockLists.Count];
			int reportIndex = 0;
			foreach (KeyValuePair<DatanodeStorage, BlockListAsLongs> kvPair in perVolumeBlockLists)
			{
				DatanodeStorage dnStorage = kvPair.Key;
				BlockListAsLongs blockList = kvPair.Value;
				reports[reportIndex++] = new StorageBlockReport(dnStorage, blockList);
			}
			cluster.GetNameNodeRpc().BlockReport(dnR, bpid, reports, null);
			// verify number of blocks and files...
			Verify(filename, filesize);
			// create another file (with one volume failed).
			System.Console.Out.WriteLine("creating file test1.txt");
			Path fileName1 = new Path("/test1.txt");
			DFSTestUtil.CreateFile(fs, fileName1, filesize, repl, 1L);
			// should be able to replicate to both nodes (2 DN, repl=2)
			DFSTestUtil.WaitReplication(fs, fileName1, repl);
			System.Console.Out.WriteLine("file " + fileName1.GetName() + " is created and replicated"
				);
		}

		/// <summary>
		/// Test that DataStorage and BlockPoolSliceStorage remove the failed volume
		/// after failure.
		/// </summary>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		public virtual void TestFailedVolumeBeingRemovedFromDataNode()
		{
			Path file1 = new Path("/test1");
			DFSTestUtil.CreateFile(fs, file1, 1024, (short)2, 1L);
			DFSTestUtil.WaitReplication(fs, file1, (short)2);
			FilePath dn0Vol1 = new FilePath(dataDir, "data" + (2 * 0 + 1));
			DataNodeTestUtils.InjectDataDirFailure(dn0Vol1);
			DataNode dn0 = cluster.GetDataNodes()[0];
			long lastDiskErrorCheck = dn0.GetLastDiskErrorCheck();
			dn0.CheckDiskErrorAsync();
			// Wait checkDiskError thread finish to discover volume failure.
			while (dn0.GetLastDiskErrorCheck() == lastDiskErrorCheck)
			{
				Sharpen.Thread.Sleep(100);
			}
			// Verify dn0Vol1 has been completely removed from DN0.
			// 1. dn0Vol1 is removed from DataStorage.
			DataStorage storage = dn0.GetStorage();
			NUnit.Framework.Assert.AreEqual(1, storage.GetNumStorageDirs());
			for (int i = 0; i < storage.GetNumStorageDirs(); i++)
			{
				Storage.StorageDirectory sd = storage.GetStorageDir(i);
				NUnit.Framework.Assert.IsFalse(sd.GetRoot().GetAbsolutePath().StartsWith(dn0Vol1.
					GetAbsolutePath()));
			}
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			BlockPoolSliceStorage bpsStorage = storage.GetBPStorage(bpid);
			NUnit.Framework.Assert.AreEqual(1, bpsStorage.GetNumStorageDirs());
			for (int i_1 = 0; i_1 < bpsStorage.GetNumStorageDirs(); i_1++)
			{
				Storage.StorageDirectory sd = bpsStorage.GetStorageDir(i_1);
				NUnit.Framework.Assert.IsFalse(sd.GetRoot().GetAbsolutePath().StartsWith(dn0Vol1.
					GetAbsolutePath()));
			}
			// 2. dn0Vol1 is removed from FsDataset
			FsDatasetSpi<FsVolumeSpi> data = dn0.GetFSDataset();
			foreach (FsVolumeSpi volume in data.GetVolumes())
			{
				Assert.AssertNotEquals(new FilePath(volume.GetBasePath()).GetAbsoluteFile(), dn0Vol1
					.GetAbsoluteFile());
			}
			// 3. all blocks on dn0Vol1 have been removed.
			foreach (ReplicaInfo replica in FsDatasetTestUtil.GetReplicas(data, bpid))
			{
				NUnit.Framework.Assert.IsNotNull(replica.GetVolume());
				Assert.AssertNotEquals(new FilePath(replica.GetVolume().GetBasePath()).GetAbsoluteFile
					(), dn0Vol1.GetAbsoluteFile());
			}
			// 4. dn0Vol1 is not in DN0's configuration and dataDirs anymore.
			string[] dataDirStrs = dn0.GetConf().Get(DFSConfigKeys.DfsDatanodeDataDirKey).Split
				(",");
			NUnit.Framework.Assert.AreEqual(1, dataDirStrs.Length);
			NUnit.Framework.Assert.IsFalse(dataDirStrs[0].Contains(dn0Vol1.GetAbsolutePath())
				);
		}

		/// <summary>Test that there are under replication blocks after vol failures</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnderReplicationAfterVolFailure()
		{
			// This test relies on denying access to data volumes to simulate data volume
			// failure.  This doesn't work on Windows, because an owner of an object
			// always has the ability to read and change permissions on the object.
			Assume.AssumeTrue(!Path.Windows);
			// Bring up one more datanode
			cluster.StartDataNodes(conf, 1, true, null, null);
			cluster.WaitActive();
			BlockManager bm = cluster.GetNamesystem().GetBlockManager();
			Path file1 = new Path("/test1");
			DFSTestUtil.CreateFile(fs, file1, 1024, (short)3, 1L);
			DFSTestUtil.WaitReplication(fs, file1, (short)3);
			// Fail the first volume on both datanodes
			FilePath dn1Vol1 = new FilePath(dataDir, "data" + (2 * 0 + 1));
			FilePath dn2Vol1 = new FilePath(dataDir, "data" + (2 * 1 + 1));
			DataNodeTestUtils.InjectDataDirFailure(dn1Vol1, dn2Vol1);
			Path file2 = new Path("/test2");
			DFSTestUtil.CreateFile(fs, file2, 1024, (short)3, 1L);
			DFSTestUtil.WaitReplication(fs, file2, (short)3);
			// underReplicatedBlocks are due to failed volumes
			int underReplicatedBlocks = BlockManagerTestUtil.CheckHeartbeatAndGetUnderReplicatedBlocksCount
				(cluster.GetNamesystem(), bm);
			NUnit.Framework.Assert.IsTrue("There is no under replicated block after volume failure"
				, underReplicatedBlocks > 0);
		}

		/// <summary>
		/// verifies two things:
		/// 1.
		/// </summary>
		/// <remarks>
		/// verifies two things:
		/// 1. number of locations of each block in the name node
		/// matches number of actual files
		/// 2. block files + pending block equals to total number of blocks that a file has
		/// including the replication (HDFS file has 30 blocks, repl=2 - total 60
		/// </remarks>
		/// <param name="fn">- file name</param>
		/// <param name="fs">- file size</param>
		/// <exception cref="System.IO.IOException"/>
		private void Verify(string fn, int fs)
		{
			// now count how many physical blocks are there
			int totalReal = CountRealBlocks(block_map);
			System.Console.Out.WriteLine("countRealBlocks counted " + totalReal + " blocks");
			// count how many blocks store in NN structures.
			int totalNN = CountNNBlocks(block_map, fn, fs);
			System.Console.Out.WriteLine("countNNBlocks counted " + totalNN + " blocks");
			foreach (string bid in block_map.Keys)
			{
				TestDataNodeVolumeFailure.BlockLocs bl = block_map[bid];
				// System.out.println(bid + "->" + bl.num_files + "vs." + bl.num_locs);
				// number of physical files (1 or 2) should be same as number of datanodes
				// in the list of the block locations
				NUnit.Framework.Assert.AreEqual("Num files should match num locations", bl.num_files
					, bl.num_locs);
			}
			NUnit.Framework.Assert.AreEqual("Num physical blocks should match num stored in the NN"
				, totalReal, totalNN);
			// now check the number of under-replicated blocks
			FSNamesystem fsn = cluster.GetNamesystem();
			// force update of all the metric counts by calling computeDatanodeWork
			BlockManagerTestUtil.GetComputedDatanodeWork(fsn.GetBlockManager());
			// get all the counts 
			long underRepl = fsn.GetUnderReplicatedBlocks();
			long pendRepl = fsn.GetPendingReplicationBlocks();
			long totalRepl = underRepl + pendRepl;
			System.Console.Out.WriteLine("underreplicated after = " + underRepl + " and pending repl ="
				 + pendRepl + "; total underRepl = " + totalRepl);
			System.Console.Out.WriteLine("total blocks (real and replicating):" + (totalReal 
				+ totalRepl) + " vs. all files blocks " + blocks_num * 2);
			// together all the blocks should be equal to all real + all underreplicated
			NUnit.Framework.Assert.AreEqual("Incorrect total block count", totalReal + totalRepl
				, blocks_num * repl);
		}

		/// <summary>go to each block on the 2nd DataNode until it fails...</summary>
		/// <param name="path"/>
		/// <param name="size"/>
		/// <exception cref="System.IO.IOException"/>
		private void TriggerFailure(string path, long size)
		{
			NamenodeProtocols nn = cluster.GetNameNodeRpc();
			IList<LocatedBlock> locatedBlocks = nn.GetBlockLocations(path, 0, size).GetLocatedBlocks
				();
			foreach (LocatedBlock lb in locatedBlocks)
			{
				DatanodeInfo dinfo = lb.GetLocations()[1];
				ExtendedBlock b = lb.GetBlock();
				try
				{
					AccessBlock(dinfo, lb);
				}
				catch (IOException)
				{
					System.Console.Out.WriteLine("Failure triggered, on block: " + b.GetBlockId() + "; corresponding volume should be removed by now"
						);
					break;
				}
			}
		}

		/// <summary>simulate failure delete all the block files</summary>
		/// <param name="dir"/>
		/// <exception cref="System.IO.IOException"/>
		private bool DeteteBlocks(FilePath dir)
		{
			FilePath[] fileList = dir.ListFiles();
			foreach (FilePath f in fileList)
			{
				if (f.GetName().StartsWith(Block.BlockFilePrefix))
				{
					if (!f.Delete())
					{
						return false;
					}
				}
			}
			return true;
		}

		/// <summary>try to access a block on a data node.</summary>
		/// <remarks>try to access a block on a data node. If fails - throws exception</remarks>
		/// <param name="datanode"/>
		/// <param name="lblock"/>
		/// <exception cref="System.IO.IOException"/>
		private void AccessBlock(DatanodeInfo datanode, LocatedBlock lblock)
		{
			IPEndPoint targetAddr = null;
			ExtendedBlock block = lblock.GetBlock();
			targetAddr = NetUtils.CreateSocketAddr(datanode.GetXferAddr());
			BlockReader blockReader = new BlockReaderFactory(new DFSClient.Conf(conf)).SetInetSocketAddress
				(targetAddr).SetBlock(block).SetFileName(BlockReaderFactory.GetFileName(targetAddr
				, "test-blockpoolid", block.GetBlockId())).SetBlockToken(lblock.GetBlockToken())
				.SetStartOffset(0).SetLength(-1).SetVerifyChecksum(true).SetClientName("TestDataNodeVolumeFailure"
				).SetDatanodeInfo(datanode).SetCachingStrategy(CachingStrategy.NewDefaultStrategy
				()).SetClientCacheContext(ClientContext.GetFromConf(conf)).SetConfiguration(conf
				).SetRemotePeerFactory(new _RemotePeerFactory_422(this)).Build();
			blockReader.Close();
		}

		private sealed class _RemotePeerFactory_422 : RemotePeerFactory
		{
			public _RemotePeerFactory_422(TestDataNodeVolumeFailure _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public Peer NewConnectedPeer(IPEndPoint addr, Org.Apache.Hadoop.Security.Token.Token
				<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
			{
				Peer peer = null;
				Socket sock = NetUtils.GetDefaultSocketFactory(this._enclosing.conf).CreateSocket
					();
				try
				{
					sock.Connect(addr, HdfsServerConstants.ReadTimeout);
					sock.ReceiveTimeout = HdfsServerConstants.ReadTimeout;
					peer = TcpPeerServer.PeerFromSocket(sock);
				}
				finally
				{
					if (peer == null)
					{
						IOUtils.CloseSocket(sock);
					}
				}
				return peer;
			}

			private readonly TestDataNodeVolumeFailure _enclosing;
		}

		/// <summary>
		/// Count datanodes that have copies of the blocks for a file
		/// put it into the map
		/// </summary>
		/// <param name="map"/>
		/// <param name="path"/>
		/// <param name="size"/>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		private int CountNNBlocks(IDictionary<string, TestDataNodeVolumeFailure.BlockLocs
			> map, string path, long size)
		{
			int total = 0;
			NamenodeProtocols nn = cluster.GetNameNodeRpc();
			IList<LocatedBlock> locatedBlocks = nn.GetBlockLocations(path, 0, size).GetLocatedBlocks
				();
			//System.out.println("Number of blocks: " + locatedBlocks.size()); 
			foreach (LocatedBlock lb in locatedBlocks)
			{
				string blockId = string.Empty + lb.GetBlock().GetBlockId();
				//System.out.print(blockId + ": ");
				DatanodeInfo[] dn_locs = lb.GetLocations();
				TestDataNodeVolumeFailure.BlockLocs bl = map[blockId];
				if (bl == null)
				{
					bl = new TestDataNodeVolumeFailure.BlockLocs(this);
				}
				//System.out.print(dn_info.name+",");
				total += dn_locs.Length;
				bl.num_locs += dn_locs.Length;
				map[blockId] = bl;
			}
			//System.out.println();
			return total;
		}

		/// <summary>
		/// look for real blocks
		/// by counting *.meta files in all the storage dirs
		/// </summary>
		/// <param name="map"/>
		/// <returns/>
		private int CountRealBlocks(IDictionary<string, TestDataNodeVolumeFailure.BlockLocs
			> map)
		{
			int total = 0;
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			for (int i = 0; i < dn_num; i++)
			{
				for (int j = 0; j <= 1; j++)
				{
					FilePath storageDir = cluster.GetInstanceStorageDir(i, j);
					FilePath dir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
					if (dir == null)
					{
						System.Console.Out.WriteLine("dir is null for dn=" + i + " and data_dir=" + j);
						continue;
					}
					IList<FilePath> res = MiniDFSCluster.GetAllBlockMetadataFiles(dir);
					if (res == null)
					{
						System.Console.Out.WriteLine("res is null for dir = " + dir + " i=" + i + " and j="
							 + j);
						continue;
					}
					//System.out.println("for dn" + i + "." + j + ": " + dir + "=" + res.length+ " files");
					//int ii = 0;
					foreach (FilePath f in res)
					{
						string s = f.GetName();
						// cut off "blk_-" at the beginning and ".meta" at the end
						NUnit.Framework.Assert.IsNotNull("Block file name should not be null", s);
						string bid = Sharpen.Runtime.Substring(s, s.IndexOf("_") + 1, s.LastIndexOf("_"));
						//System.out.println(ii++ + ". block " + s + "; id=" + bid);
						TestDataNodeVolumeFailure.BlockLocs val = map[bid];
						if (val == null)
						{
							val = new TestDataNodeVolumeFailure.BlockLocs(this);
						}
						val.num_files++;
						// one more file for the block
						map[bid] = val;
					}
					//System.out.println("dir1="+dir.getPath() + "blocks=" + res.length);
					//System.out.println("dir2="+dir2.getPath() + "blocks=" + res2.length);
					total += res.Count;
				}
			}
			return total;
		}
	}
}
