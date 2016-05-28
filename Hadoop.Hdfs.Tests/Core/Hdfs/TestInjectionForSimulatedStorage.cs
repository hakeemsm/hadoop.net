using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests the replication and injection of blocks of a DFS file for simulated storage.
	/// 	</summary>
	public class TestInjectionForSimulatedStorage
	{
		private readonly int checksumSize = 16;

		private readonly int blockSize = checksumSize * 2;

		private readonly int numBlocks = 4;

		private readonly int filesize = blockSize * numBlocks;

		private readonly int numDataNodes = 4;

		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestInjectionForSimulatedStorage"
			);

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name, int repl)
		{
			// create and write a file that contains three blocks of data
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, blockSize);
			byte[] buffer = new byte[filesize];
			for (int i = 0; i < buffer.Length; i++)
			{
				buffer[i] = (byte)('1');
			}
			stm.Write(buffer);
			stm.Close();
		}

		// Waits for all of the blocks to have expected replication
		// Waits for all of the blocks to have expected replication
		/// <exception cref="System.IO.IOException"/>
		private void WaitForBlockReplication(string filename, ClientProtocol namenode, int
			 expected, long maxWaitSec)
		{
			long start = Time.MonotonicNow();
			//wait for all the blocks to be replicated;
			Log.Info("Checking for block replication for " + filename);
			LocatedBlocks blocks = namenode.GetBlockLocations(filename, 0, long.MaxValue);
			NUnit.Framework.Assert.AreEqual(numBlocks, blocks.LocatedBlockCount());
			for (int i = 0; i < numBlocks; ++i)
			{
				Log.Info("Checking for block:" + (i + 1));
				while (true)
				{
					// Loop to check for block i (usually when 0 is done all will be done
					blocks = namenode.GetBlockLocations(filename, 0, long.MaxValue);
					NUnit.Framework.Assert.AreEqual(numBlocks, blocks.LocatedBlockCount());
					LocatedBlock block = blocks.Get(i);
					int actual = block.GetLocations().Length;
					if (actual == expected)
					{
						Log.Info("Got enough replicas for " + (i + 1) + "th block " + block.GetBlock() + 
							", got " + actual + ".");
						break;
					}
					Log.Info("Not enough replicas for " + (i + 1) + "th block " + block.GetBlock() + 
						" yet. Expecting " + expected + ", got " + actual + ".");
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
		}

		/* This test makes sure that NameNode retries all the available blocks
		* for under replicated blocks. This test uses simulated storage and one
		* of its features to inject blocks,
		*
		* It creates a file with several blocks and replication of 4.
		* The cluster is then shut down - NN retains its state but the DNs are
		* all simulated and hence loose their blocks.
		* The blocks are then injected in one of the DNs. The  expected behaviour is
		* that the NN will arrange for themissing replica will be copied from a valid source.
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInjection()
		{
			MiniDFSCluster cluster = null;
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
				conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, checksumSize);
				SimulatedFSDataset.SetFactory(conf);
				//first time format
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).Build();
				cluster.WaitActive();
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				DFSClient dfsClient = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort
					()), conf);
				WriteFile(cluster.GetFileSystem(), testPath, numDataNodes);
				WaitForBlockReplication(testFile, dfsClient.GetNamenode(), numDataNodes, 20);
				IList<IDictionary<DatanodeStorage, BlockListAsLongs>> blocksList = cluster.GetAllBlockReports
					(bpid);
				cluster.Shutdown();
				cluster = null;
				/* Start the MiniDFSCluster with more datanodes since once a writeBlock
				* to a datanode node fails, same block can not be written to it
				* immediately. In our case some replication attempts will fail.
				*/
				Log.Info("Restarting minicluster");
				conf = new HdfsConfiguration();
				SimulatedFSDataset.SetFactory(conf);
				conf.Set(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, "0.0f");
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes * 2).Format(
					false).Build();
				cluster.WaitActive();
				ICollection<Block> uniqueBlocks = new HashSet<Block>();
				foreach (IDictionary<DatanodeStorage, BlockListAsLongs> map in blocksList)
				{
					foreach (BlockListAsLongs blockList in map.Values)
					{
						foreach (Block b in blockList)
						{
							uniqueBlocks.AddItem(new Block(b));
						}
					}
				}
				// Insert all the blocks in the first data node
				Log.Info("Inserting " + uniqueBlocks.Count + " blocks");
				cluster.InjectBlocks(0, uniqueBlocks, null);
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
	}
}
