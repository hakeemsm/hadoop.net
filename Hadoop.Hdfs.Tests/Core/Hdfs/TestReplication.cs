using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests the replication of a DFS file.</summary>
	public class TestReplication
	{
		private const long seed = unchecked((long)(0xDEADBEEFL));

		private const int blockSize = 8192;

		private const int fileSize = 16384;

		private static readonly string[] racks = new string[] { "/d1/r1", "/d1/r1", "/d1/r2"
			, "/d1/r2", "/d1/r2", "/d2/r3", "/d2/r3" };

		private static readonly int numDatanodes = racks.Length;

		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestReplication"
			);

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
		}

		/* check if there are at least two nodes are on the same rack */
		/// <exception cref="System.IO.IOException"/>
		private void CheckFile(FileSystem fileSys, Path name, int repl)
		{
			Configuration conf = fileSys.GetConf();
			ClientProtocol namenode = NameNodeProxies.CreateProxy<ClientProtocol>(conf, fileSys
				.GetUri()).GetProxy();
			WaitForBlockReplication(name.ToString(), namenode, Math.Min(numDatanodes, repl), 
				-1);
			LocatedBlocks locations = namenode.GetBlockLocations(name.ToString(), 0, long.MaxValue
				);
			FileStatus stat = fileSys.GetFileStatus(name);
			BlockLocation[] blockLocations = fileSys.GetFileBlockLocations(stat, 0L, long.MaxValue
				);
			// verify that rack locations match
			NUnit.Framework.Assert.IsTrue(blockLocations.Length == locations.LocatedBlockCount
				());
			for (int i = 0; i < blockLocations.Length; i++)
			{
				LocatedBlock blk = locations.Get(i);
				DatanodeInfo[] datanodes = blk.GetLocations();
				string[] topologyPaths = blockLocations[i].GetTopologyPaths();
				NUnit.Framework.Assert.IsTrue(topologyPaths.Length == datanodes.Length);
				for (int j = 0; j < topologyPaths.Length; j++)
				{
					bool found = false;
					for (int k = 0; k < racks.Length; k++)
					{
						if (topologyPaths[j].StartsWith(racks[k]))
						{
							found = true;
							break;
						}
					}
					NUnit.Framework.Assert.IsTrue(found);
				}
			}
			bool isOnSameRack = true;
			bool isNotOnSameRack = true;
			foreach (LocatedBlock blk_1 in locations.GetLocatedBlocks())
			{
				DatanodeInfo[] datanodes = blk_1.GetLocations();
				if (datanodes.Length <= 1)
				{
					break;
				}
				if (datanodes.Length == 2)
				{
					isNotOnSameRack = !(datanodes[0].GetNetworkLocation().Equals(datanodes[1].GetNetworkLocation
						()));
					break;
				}
				isOnSameRack = false;
				isNotOnSameRack = false;
				for (int i_1 = 0; i_1 < datanodes.Length - 1; i_1++)
				{
					Log.Info("datanode " + i_1 + ": " + datanodes[i_1]);
					bool onRack = false;
					for (int j = i_1 + 1; j < datanodes.Length; j++)
					{
						if (datanodes[i_1].GetNetworkLocation().Equals(datanodes[j].GetNetworkLocation()))
						{
							onRack = true;
						}
					}
					if (onRack)
					{
						isOnSameRack = true;
					}
					if (!onRack)
					{
						isNotOnSameRack = true;
					}
					if (isOnSameRack && isNotOnSameRack)
					{
						break;
					}
				}
				if (!isOnSameRack || !isNotOnSameRack)
				{
					break;
				}
			}
			NUnit.Framework.Assert.IsTrue(isOnSameRack);
			NUnit.Framework.Assert.IsTrue(isNotOnSameRack);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		/// <exception cref="System.Exception"/>
		private void TestBadBlockReportOnTransfer(bool corruptBlockByDeletingBlockFile)
		{
			Configuration conf = new HdfsConfiguration();
			FileSystem fs = null;
			DFSClient dfsClient = null;
			LocatedBlocks blocks = null;
			int replicaCount = 0;
			short replFactor = 1;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			dfsClient = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort()), 
				conf);
			// Create file with replication factor of 1
			Path file1 = new Path("/tmp/testBadBlockReportOnTransfer/file1");
			DFSTestUtil.CreateFile(fs, file1, 1024, replFactor, 0);
			DFSTestUtil.WaitReplication(fs, file1, replFactor);
			// Corrupt the block belonging to the created file
			ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, file1);
			int blockFilesCorrupted = corruptBlockByDeletingBlockFile ? cluster.CorruptBlockOnDataNodesByDeletingBlockFile
				(block) : cluster.CorruptBlockOnDataNodes(block);
			NUnit.Framework.Assert.AreEqual("Corrupted too few blocks", replFactor, blockFilesCorrupted
				);
			// Increase replication factor, this should invoke transfer request
			// Receiving datanode fails on checksum and reports it to namenode
			replFactor = 2;
			fs.SetReplication(file1, replFactor);
			// Now get block details and check if the block is corrupt
			blocks = dfsClient.GetNamenode().GetBlockLocations(file1.ToString(), 0, long.MaxValue
				);
			while (blocks.Get(0).IsCorrupt() != true)
			{
				try
				{
					Log.Info("Waiting until block is marked as corrupt...");
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
				blocks = dfsClient.GetNamenode().GetBlockLocations(file1.ToString(), 0, long.MaxValue
					);
			}
			replicaCount = blocks.Get(0).GetLocations().Length;
			NUnit.Framework.Assert.IsTrue(replicaCount == 1);
			cluster.Shutdown();
		}

		/*
		* Test if Datanode reports bad blocks during replication request
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBadBlockReportOnTransfer()
		{
			TestBadBlockReportOnTransfer(false);
		}

		/*
		* Test if Datanode reports bad blocks during replication request
		* with missing block file
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBadBlockReportOnTransferMissingBlockFile()
		{
			TestBadBlockReportOnTransfer(true);
		}

		/// <summary>Tests replication in DFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RunReplication(bool simulated)
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeReplicationConsiderloadKey, false);
			if (simulated)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes
				).Racks(racks).Build();
			cluster.WaitActive();
			IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
			DFSClient client = new DFSClient(addr, conf);
			DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
				);
			NUnit.Framework.Assert.AreEqual("Number of Datanodes ", numDatanodes, info.Length
				);
			FileSystem fileSys = cluster.GetFileSystem();
			try
			{
				Path file1 = new Path("/smallblocktest.dat");
				WriteFile(fileSys, file1, 3);
				CheckFile(fileSys, file1, 3);
				CleanupFile(fileSys, file1);
				WriteFile(fileSys, file1, 10);
				CheckFile(fileSys, file1, 10);
				CleanupFile(fileSys, file1);
				WriteFile(fileSys, file1, 4);
				CheckFile(fileSys, file1, 4);
				CleanupFile(fileSys, file1);
				WriteFile(fileSys, file1, 1);
				CheckFile(fileSys, file1, 1);
				CleanupFile(fileSys, file1);
				WriteFile(fileSys, file1, 2);
				CheckFile(fileSys, file1, 2);
				CleanupFile(fileSys, file1);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplicationSimulatedStorag()
		{
			RunReplication(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReplication()
		{
			RunReplication(false);
		}

		// Waits for all of the blocks to have expected replication
		/// <exception cref="System.IO.IOException"/>
		private void WaitForBlockReplication(string filename, ClientProtocol namenode, int
			 expected, long maxWaitSec)
		{
			long start = Time.MonotonicNow();
			//wait for all the blocks to be replicated;
			Log.Info("Checking for block replication for " + filename);
			while (true)
			{
				bool replOk = true;
				LocatedBlocks blocks = namenode.GetBlockLocations(filename, 0, long.MaxValue);
				for (IEnumerator<LocatedBlock> iter = blocks.GetLocatedBlocks().GetEnumerator(); 
					iter.HasNext(); )
				{
					LocatedBlock block = iter.Next();
					int actual = block.GetLocations().Length;
					if (actual < expected)
					{
						Log.Info("Not enough replicas for " + block.GetBlock() + " yet. Expecting " + expected
							 + ", got " + actual + ".");
						replOk = false;
						break;
					}
				}
				if (replOk)
				{
					return;
				}
				if (maxWaitSec > 0 && (Time.MonotonicNow() - start) > (maxWaitSec * 1000))
				{
					throw new IOException("Timedout while waiting for all blocks to " + " be replicated for "
						 + filename);
				}
				try
				{
					Sharpen.Thread.Sleep(500);
				}
				catch (Exception)
				{
				}
			}
		}

		/* This test makes sure that NameNode retries all the available blocks
		* for under replicated blocks.
		*
		* It creates a file with one block and replication of 4. It corrupts
		* two of the blocks and removes one of the replicas. Expected behavior is
		* that missing replica will be copied from one valid source.
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPendingReplicationRetry()
		{
			MiniDFSCluster cluster = null;
			int numDataNodes = 4;
			string testFile = "/replication-test-file";
			Path testPath = new Path(testFile);
			byte[] buffer = new byte[1024];
			for (int i = 0; i < buffer.Length; i++)
			{
				buffer[i] = (byte)('1');
			}
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.Set(DFSConfigKeys.DfsReplicationKey, Sharpen.Extensions.ToString(numDataNodes
					));
				//first time format
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).Build();
				cluster.WaitActive();
				DFSClient dfsClient = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort
					()), conf);
				OutputStream @out = cluster.GetFileSystem().Create(testPath);
				@out.Write(buffer);
				@out.Close();
				WaitForBlockReplication(testFile, dfsClient.GetNamenode(), numDataNodes, -1);
				// get first block of the file.
				ExtendedBlock block = dfsClient.GetNamenode().GetBlockLocations(testFile, 0, long.MaxValue
					).Get(0).GetBlock();
				cluster.Shutdown();
				for (int i_1 = 0; i_1 < 25; i_1++)
				{
					buffer[i_1] = (byte)('0');
				}
				int fileCount = 0;
				// Choose 3 copies of block file - delete 1 and corrupt the remaining 2
				for (int dnIndex = 0; dnIndex < 3; dnIndex++)
				{
					FilePath blockFile = cluster.GetBlockFile(dnIndex, block);
					Log.Info("Checking for file " + blockFile);
					if (blockFile != null && blockFile.Exists())
					{
						if (fileCount == 0)
						{
							Log.Info("Deleting file " + blockFile);
							NUnit.Framework.Assert.IsTrue(blockFile.Delete());
						}
						else
						{
							// corrupt it.
							Log.Info("Corrupting file " + blockFile);
							long len = blockFile.Length();
							NUnit.Framework.Assert.IsTrue(len > 50);
							RandomAccessFile blockOut = new RandomAccessFile(blockFile, "rw");
							try
							{
								blockOut.Seek(len / 3);
								blockOut.Write(buffer, 0, 25);
							}
							finally
							{
								blockOut.Close();
							}
						}
						fileCount++;
					}
				}
				NUnit.Framework.Assert.AreEqual(3, fileCount);
				/* Start the MiniDFSCluster with more datanodes since once a writeBlock
				* to a datanode node fails, same block can not be written to it
				* immediately. In our case some replication attempts will fail.
				*/
				Log.Info("Restarting minicluster after deleting a replica and corrupting 2 crcs");
				conf = new HdfsConfiguration();
				conf.Set(DFSConfigKeys.DfsReplicationKey, Sharpen.Extensions.ToString(numDataNodes
					));
				conf.Set(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, Sharpen.Extensions.ToString
					(2));
				conf.Set("dfs.datanode.block.write.timeout.sec", Sharpen.Extensions.ToString(5));
				conf.Set(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, "0.75f");
				// only 3 copies exist
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes * 2).Format(
					false).Build();
				cluster.WaitActive();
				dfsClient = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort()), 
					conf);
				WaitForBlockReplication(testFile, dfsClient.GetNamenode(), numDataNodes, -1);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test if replication can detect mismatched length on-disk blocks</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplicateLenMismatchedBlock()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).NumDataNodes
				(2).Build();
			try
			{
				cluster.WaitActive();
				// test truncated block
				ChangeBlockLen(cluster, -1);
				// test extended block
				ChangeBlockLen(cluster, 1);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private void ChangeBlockLen(MiniDFSCluster cluster, int lenDelta)
		{
			Path fileName = new Path("/file1");
			short ReplicationFactor = (short)1;
			FileSystem fs = cluster.GetFileSystem();
			int fileLen = fs.GetConf().GetInt(DFSConfigKeys.DfsBytesPerChecksumKey, 512);
			DFSTestUtil.CreateFile(fs, fileName, fileLen, ReplicationFactor, 0);
			DFSTestUtil.WaitReplication(fs, fileName, ReplicationFactor);
			ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, fileName);
			// Change the length of a replica
			for (int i = 0; i < cluster.GetDataNodes().Count; i++)
			{
				if (DFSTestUtil.ChangeReplicaLength(cluster, block, i, lenDelta))
				{
					break;
				}
			}
			// increase the file's replication factor
			fs.SetReplication(fileName, (short)(ReplicationFactor + 1));
			// block replication triggers corrupt block detection
			DFSClient dfsClient = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort
				()), fs.GetConf());
			LocatedBlocks blocks = dfsClient.GetNamenode().GetBlockLocations(fileName.ToString
				(), 0, fileLen);
			if (lenDelta < 0)
			{
				// replica truncated
				while (!blocks.Get(0).IsCorrupt() || ReplicationFactor != blocks.Get(0).GetLocations
					().Length)
				{
					Sharpen.Thread.Sleep(100);
					blocks = dfsClient.GetNamenode().GetBlockLocations(fileName.ToString(), 0, fileLen
						);
				}
			}
			else
			{
				// no corruption detected; block replicated
				while (ReplicationFactor + 1 != blocks.Get(0).GetLocations().Length)
				{
					Sharpen.Thread.Sleep(100);
					blocks = dfsClient.GetNamenode().GetBlockLocations(fileName.ToString(), 0, fileLen
						);
				}
			}
			fs.Delete(fileName, true);
		}

		/// <summary>
		/// Test that blocks should get replicated if we have corrupted blocks and
		/// having good replicas at least equal or greater to minreplication
		/// Simulate rbw blocks by creating dummy copies, then a DN restart to detect
		/// those corrupted blocks asap.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReplicationWhenBlockCorruption()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.SetLong(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, 1);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				FileSystem fs = cluster.GetFileSystem();
				FSDataOutputStream create = fs.Create(new Path("/test"));
				fs.SetReplication(new Path("/test"), (short)1);
				create.Write(new byte[1024]);
				create.Close();
				IList<FilePath> nonParticipatedNodeDirs = new AList<FilePath>();
				FilePath participatedNodeDirs = null;
				for (int i = 0; i < cluster.GetDataNodes().Count; i++)
				{
					FilePath storageDir = cluster.GetInstanceStorageDir(i, 0);
					string bpid = cluster.GetNamesystem().GetBlockPoolId();
					FilePath data_dir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
					if (data_dir.ListFiles().Length == 0)
					{
						nonParticipatedNodeDirs.AddItem(data_dir);
					}
					else
					{
						participatedNodeDirs = data_dir;
					}
				}
				string blockFile = null;
				FilePath[] listFiles = participatedNodeDirs.ListFiles();
				foreach (FilePath file in listFiles)
				{
					if (file.GetName().StartsWith(Block.BlockFilePrefix) && !file.GetName().EndsWith(
						"meta"))
					{
						blockFile = file.GetName();
						foreach (FilePath file1 in nonParticipatedNodeDirs)
						{
							file1.Mkdirs();
							new FilePath(file1, blockFile).CreateNewFile();
							new FilePath(file1, blockFile + "_1000.meta").CreateNewFile();
						}
						break;
					}
				}
				fs.SetReplication(new Path("/test"), (short)3);
				cluster.RestartDataNodes();
				// Lets detect all DNs about dummy copied
				// blocks
				cluster.WaitActive();
				cluster.TriggerBlockReports();
				DFSTestUtil.WaitReplication(fs, new Path("/test"), (short)3);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
