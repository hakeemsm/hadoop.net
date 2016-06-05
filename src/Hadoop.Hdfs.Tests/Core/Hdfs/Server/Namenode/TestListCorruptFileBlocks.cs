using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This class tests the listCorruptFileBlocks API.</summary>
	/// <remarks>
	/// This class tests the listCorruptFileBlocks API.
	/// We create 3 files; intentionally delete their blocks
	/// Use listCorruptFileBlocks to validate that we get the list of corrupt
	/// files/blocks; also test the "paging" support by calling the API
	/// with a block # from a previous call and validate that the subsequent
	/// blocks/files are also returned.
	/// </remarks>
	public class TestListCorruptFileBlocks
	{
		internal static readonly Logger Log = NameNode.stateChangeLog;

		/// <summary>check if nn.getCorruptFiles() returns a file that has corrupted blocks</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestListCorruptFilesCorruptedBlock()
		{
			MiniDFSCluster cluster = null;
			Random random = new Random();
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.SetInt(DFSConfigKeys.DfsDatanodeDirectoryscanIntervalKey, 1);
				// datanode scans directories
				conf.SetInt(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 3 * 1000);
				// datanode sends block reports
				// Set short retry timeouts so this test runs faster
				conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = cluster.GetFileSystem();
				// create two files with one block each
				DFSTestUtil util = new DFSTestUtil.Builder().SetName("testCorruptFilesCorruptedBlock"
					).SetNumFiles(2).SetMaxLevels(1).SetMaxSize(512).Build();
				util.CreateFiles(fs, "/srcdat10");
				// fetch bad file list from namenode. There should be none.
				NameNode namenode = cluster.GetNameNode();
				ICollection<FSNamesystem.CorruptFileBlockInfo> badFiles = namenode.GetNamesystem(
					).ListCorruptFileBlocks("/", null);
				NUnit.Framework.Assert.IsTrue("Namenode has " + badFiles.Count + " corrupt files. Expecting None."
					, badFiles.Count == 0);
				// Now deliberately corrupt one block
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				FilePath storageDir = cluster.GetInstanceStorageDir(0, 1);
				FilePath data_dir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
				NUnit.Framework.Assert.IsTrue("data directory does not exist", data_dir.Exists());
				IList<FilePath> metaFiles = MiniDFSCluster.GetAllBlockMetadataFiles(data_dir);
				NUnit.Framework.Assert.IsTrue("Data directory does not contain any blocks or there was an "
					 + "IO error", metaFiles != null && !metaFiles.IsEmpty());
				FilePath metaFile = metaFiles[0];
				RandomAccessFile file = new RandomAccessFile(metaFile, "rw");
				FileChannel channel = file.GetChannel();
				long position = channel.Size() - 2;
				int length = 2;
				byte[] buffer = new byte[length];
				random.NextBytes(buffer);
				channel.Write(ByteBuffer.Wrap(buffer), position);
				file.Close();
				Log.Info("Deliberately corrupting file " + metaFile.GetName() + " at offset " + position
					 + " length " + length);
				// read all files to trigger detection of corrupted replica
				try
				{
					util.CheckFiles(fs, "/srcdat10");
				}
				catch (BlockMissingException)
				{
					System.Console.Out.WriteLine("Received BlockMissingException as expected.");
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue("Corrupted replicas not handled properly. Expecting BlockMissingException "
						 + " but received IOException " + e, false);
				}
				// fetch bad file list from namenode. There should be one file.
				badFiles = namenode.GetNamesystem().ListCorruptFileBlocks("/", null);
				Log.Info("Namenode has bad files. " + badFiles.Count);
				NUnit.Framework.Assert.IsTrue("Namenode has " + badFiles.Count + " bad files. Expecting 1."
					, badFiles.Count == 1);
				util.Cleanup(fs, "/srcdat10");
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Check that listCorruptFileBlocks works while the namenode is still in safemode.
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestListCorruptFileBlocksInSafeMode()
		{
			MiniDFSCluster cluster = null;
			Random random = new Random();
			try
			{
				Configuration conf = new HdfsConfiguration();
				// datanode scans directories
				conf.SetInt(DFSConfigKeys.DfsDatanodeDirectoryscanIntervalKey, 1);
				// datanode sends block reports
				conf.SetInt(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 3 * 1000);
				// never leave safemode automatically
				conf.SetFloat(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, 1.5f);
				// start populating repl queues immediately 
				conf.SetFloat(DFSConfigKeys.DfsNamenodeReplQueueThresholdPctKey, 0f);
				// Set short retry timeouts so this test runs faster
				conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
				cluster = new MiniDFSCluster.Builder(conf).WaitSafeMode(false).Build();
				cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, 
					false);
				FileSystem fs = cluster.GetFileSystem();
				// create two files with one block each
				DFSTestUtil util = new DFSTestUtil.Builder().SetName("testListCorruptFileBlocksInSafeMode"
					).SetNumFiles(2).SetMaxLevels(1).SetMaxSize(512).Build();
				util.CreateFiles(fs, "/srcdat10");
				// fetch bad file list from namenode. There should be none.
				ICollection<FSNamesystem.CorruptFileBlockInfo> badFiles = cluster.GetNameNode().GetNamesystem
					().ListCorruptFileBlocks("/", null);
				NUnit.Framework.Assert.IsTrue("Namenode has " + badFiles.Count + " corrupt files. Expecting None."
					, badFiles.Count == 0);
				// Now deliberately corrupt one block
				FilePath storageDir = cluster.GetInstanceStorageDir(0, 0);
				FilePath data_dir = MiniDFSCluster.GetFinalizedDir(storageDir, cluster.GetNamesystem
					().GetBlockPoolId());
				NUnit.Framework.Assert.IsTrue("data directory does not exist", data_dir.Exists());
				IList<FilePath> metaFiles = MiniDFSCluster.GetAllBlockMetadataFiles(data_dir);
				NUnit.Framework.Assert.IsTrue("Data directory does not contain any blocks or there was an "
					 + "IO error", metaFiles != null && !metaFiles.IsEmpty());
				FilePath metaFile = metaFiles[0];
				RandomAccessFile file = new RandomAccessFile(metaFile, "rw");
				FileChannel channel = file.GetChannel();
				long position = channel.Size() - 2;
				int length = 2;
				byte[] buffer = new byte[length];
				random.NextBytes(buffer);
				channel.Write(ByteBuffer.Wrap(buffer), position);
				file.Close();
				Log.Info("Deliberately corrupting file " + metaFile.GetName() + " at offset " + position
					 + " length " + length);
				// read all files to trigger detection of corrupted replica
				try
				{
					util.CheckFiles(fs, "/srcdat10");
				}
				catch (BlockMissingException)
				{
					System.Console.Out.WriteLine("Received BlockMissingException as expected.");
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue("Corrupted replicas not handled properly. " + "Expecting BlockMissingException "
						 + " but received IOException " + e, false);
				}
				// fetch bad file list from namenode. There should be one file.
				badFiles = cluster.GetNameNode().GetNamesystem().ListCorruptFileBlocks("/", null);
				Log.Info("Namenode has bad files. " + badFiles.Count);
				NUnit.Framework.Assert.IsTrue("Namenode has " + badFiles.Count + " bad files. Expecting 1."
					, badFiles.Count == 1);
				// restart namenode
				cluster.RestartNameNode(0);
				fs = cluster.GetFileSystem();
				// wait until replication queues have been initialized
				while (!cluster.GetNameNode().namesystem.IsPopulatingReplQueues())
				{
					try
					{
						Log.Info("waiting for replication queues");
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
				}
				// read all files to trigger detection of corrupted replica
				try
				{
					util.CheckFiles(fs, "/srcdat10");
				}
				catch (BlockMissingException)
				{
					System.Console.Out.WriteLine("Received BlockMissingException as expected.");
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue("Corrupted replicas not handled properly. " + "Expecting BlockMissingException "
						 + " but received IOException " + e, false);
				}
				// fetch bad file list from namenode. There should be one file.
				badFiles = cluster.GetNameNode().GetNamesystem().ListCorruptFileBlocks("/", null);
				Log.Info("Namenode has bad files. " + badFiles.Count);
				NUnit.Framework.Assert.IsTrue("Namenode has " + badFiles.Count + " bad files. Expecting 1."
					, badFiles.Count == 1);
				// check that we are still in safe mode
				NUnit.Framework.Assert.IsTrue("Namenode is not in safe mode", cluster.GetNameNode
					().IsInSafeMode());
				// now leave safe mode so that we can clean up
				cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, 
					false);
				util.Cleanup(fs, "/srcdat10");
			}
			catch (Exception e)
			{
				Log.Error(StringUtils.StringifyException(e));
				throw;
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		// deliberately remove blocks from a file and validate the list-corrupt-file-blocks API
		/// <exception cref="System.Exception"/>
		public virtual void TestlistCorruptFileBlocks()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsDatanodeDirectoryscanIntervalKey, 1);
			// datanode scans
			// directories
			FileSystem fs = null;
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DFSTestUtil util = new DFSTestUtil.Builder().SetName("testGetCorruptFiles").SetNumFiles
					(3).SetMaxLevels(1).SetMaxSize(1024).Build();
				util.CreateFiles(fs, "/corruptData");
				NameNode namenode = cluster.GetNameNode();
				ICollection<FSNamesystem.CorruptFileBlockInfo> corruptFileBlocks = namenode.GetNamesystem
					().ListCorruptFileBlocks("/corruptData", null);
				int numCorrupt = corruptFileBlocks.Count;
				NUnit.Framework.Assert.IsTrue(numCorrupt == 0);
				// delete the blocks
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				for (int i = 0; i < 4; i++)
				{
					for (int j = 0; j <= 1; j++)
					{
						FilePath storageDir = cluster.GetInstanceStorageDir(i, j);
						FilePath data_dir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
						IList<FilePath> metadataFiles = MiniDFSCluster.GetAllBlockMetadataFiles(data_dir);
						if (metadataFiles == null)
						{
							continue;
						}
						// assertTrue("Blocks do not exist in data-dir", (blocks != null) &&
						// (blocks.length > 0));
						foreach (FilePath metadataFile in metadataFiles)
						{
							FilePath blockFile = Block.MetaToBlockFile(metadataFile);
							Log.Info("Deliberately removing file " + blockFile.GetName());
							NUnit.Framework.Assert.IsTrue("Cannot remove file.", blockFile.Delete());
							Log.Info("Deliberately removing file " + metadataFile.GetName());
							NUnit.Framework.Assert.IsTrue("Cannot remove file.", metadataFile.Delete());
						}
					}
				}
				// break;
				int count = 0;
				corruptFileBlocks = namenode.GetNamesystem().ListCorruptFileBlocks("/corruptData"
					, null);
				numCorrupt = corruptFileBlocks.Count;
				while (numCorrupt < 3)
				{
					Sharpen.Thread.Sleep(1000);
					corruptFileBlocks = namenode.GetNamesystem().ListCorruptFileBlocks("/corruptData"
						, null);
					numCorrupt = corruptFileBlocks.Count;
					count++;
					if (count > 30)
					{
						break;
					}
				}
				// Validate we get all the corrupt files
				Log.Info("Namenode has bad files. " + numCorrupt);
				NUnit.Framework.Assert.IsTrue(numCorrupt == 3);
				// test the paging here
				FSNamesystem.CorruptFileBlockInfo[] cfb = Sharpen.Collections.ToArray(corruptFileBlocks
					, new FSNamesystem.CorruptFileBlockInfo[0]);
				// now get the 2nd and 3rd file that is corrupt
				string[] cookie = new string[] { "1" };
				ICollection<FSNamesystem.CorruptFileBlockInfo> nextCorruptFileBlocks = namenode.GetNamesystem
					().ListCorruptFileBlocks("/corruptData", cookie);
				FSNamesystem.CorruptFileBlockInfo[] ncfb = Sharpen.Collections.ToArray(nextCorruptFileBlocks
					, new FSNamesystem.CorruptFileBlockInfo[0]);
				numCorrupt = nextCorruptFileBlocks.Count;
				NUnit.Framework.Assert.IsTrue(numCorrupt == 2);
				NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.EqualsIgnoreCase(ncfb[0].block.GetBlockName
					(), cfb[1].block.GetBlockName()));
				corruptFileBlocks = namenode.GetNamesystem().ListCorruptFileBlocks("/corruptData"
					, cookie);
				numCorrupt = corruptFileBlocks.Count;
				NUnit.Framework.Assert.IsTrue(numCorrupt == 0);
				// Do a listing on a dir which doesn't have any corrupt blocks and
				// validate
				util.CreateFiles(fs, "/goodData");
				corruptFileBlocks = namenode.GetNamesystem().ListCorruptFileBlocks("/goodData", null
					);
				numCorrupt = corruptFileBlocks.Count;
				NUnit.Framework.Assert.IsTrue(numCorrupt == 0);
				util.Cleanup(fs, "/corruptData");
				util.Cleanup(fs, "/goodData");
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int CountPaths(RemoteIterator<Path> iter)
		{
			int i = 0;
			while (iter.HasNext())
			{
				Log.Info("PATH: " + iter.Next().ToUri().GetPath());
				i++;
			}
			return i;
		}

		/// <summary>test listCorruptFileBlocks in DistributedFileSystem</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestlistCorruptFileBlocksDFS()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsDatanodeDirectoryscanIntervalKey, 1);
			// datanode scans
			// directories
			FileSystem fs = null;
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DistributedFileSystem dfs = (DistributedFileSystem)fs;
				DFSTestUtil util = new DFSTestUtil.Builder().SetName("testGetCorruptFiles").SetNumFiles
					(3).SetMaxLevels(1).SetMaxSize(1024).Build();
				util.CreateFiles(fs, "/corruptData");
				RemoteIterator<Path> corruptFileBlocks = dfs.ListCorruptFileBlocks(new Path("/corruptData"
					));
				int numCorrupt = CountPaths(corruptFileBlocks);
				NUnit.Framework.Assert.IsTrue(numCorrupt == 0);
				// delete the blocks
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				// For loop through number of datadirectories per datanode (2)
				for (int i = 0; i < 2; i++)
				{
					FilePath storageDir = cluster.GetInstanceStorageDir(0, i);
					FilePath data_dir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
					IList<FilePath> metadataFiles = MiniDFSCluster.GetAllBlockMetadataFiles(data_dir);
					if (metadataFiles == null)
					{
						continue;
					}
					// assertTrue("Blocks do not exist in data-dir", (blocks != null) &&
					// (blocks.length > 0));
					foreach (FilePath metadataFile in metadataFiles)
					{
						FilePath blockFile = Block.MetaToBlockFile(metadataFile);
						Log.Info("Deliberately removing file " + blockFile.GetName());
						NUnit.Framework.Assert.IsTrue("Cannot remove file.", blockFile.Delete());
						Log.Info("Deliberately removing file " + metadataFile.GetName());
						NUnit.Framework.Assert.IsTrue("Cannot remove file.", metadataFile.Delete());
					}
				}
				// break;
				int count = 0;
				corruptFileBlocks = dfs.ListCorruptFileBlocks(new Path("/corruptData"));
				numCorrupt = CountPaths(corruptFileBlocks);
				while (numCorrupt < 3)
				{
					Sharpen.Thread.Sleep(1000);
					corruptFileBlocks = dfs.ListCorruptFileBlocks(new Path("/corruptData"));
					numCorrupt = CountPaths(corruptFileBlocks);
					count++;
					if (count > 30)
					{
						break;
					}
				}
				// Validate we get all the corrupt files
				Log.Info("Namenode has bad files. " + numCorrupt);
				NUnit.Framework.Assert.IsTrue(numCorrupt == 3);
				util.Cleanup(fs, "/corruptData");
				util.Cleanup(fs, "/goodData");
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test if NN.listCorruptFiles() returns the right number of results.</summary>
		/// <remarks>
		/// Test if NN.listCorruptFiles() returns the right number of results.
		/// The corrupt blocks are detected by the BlockPoolSliceScanner.
		/// Also, test that DFS.listCorruptFileBlocks can make multiple successive
		/// calls.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestMaxCorruptFiles()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.SetInt(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 3 * 1000);
				// datanode sends block reports
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = cluster.GetFileSystem();
				int maxCorruptFileBlocks = FSNamesystem.DefaultMaxCorruptFileblocksReturned;
				// create 110 files with one block each
				DFSTestUtil util = new DFSTestUtil.Builder().SetName("testMaxCorruptFiles").SetNumFiles
					(maxCorruptFileBlocks * 3).SetMaxLevels(1).SetMaxSize(512).Build();
				util.CreateFiles(fs, "/srcdat2", (short)1);
				util.WaitReplication(fs, "/srcdat2", (short)1);
				// verify that there are no bad blocks.
				NameNode namenode = cluster.GetNameNode();
				ICollection<FSNamesystem.CorruptFileBlockInfo> badFiles = namenode.GetNamesystem(
					).ListCorruptFileBlocks("/srcdat2", null);
				NUnit.Framework.Assert.IsTrue("Namenode has " + badFiles.Count + " corrupt files. Expecting none."
					, badFiles.Count == 0);
				// Now deliberately blocks from all files
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				for (int i = 0; i < 4; i++)
				{
					for (int j = 0; j <= 1; j++)
					{
						FilePath storageDir = cluster.GetInstanceStorageDir(i, j);
						FilePath data_dir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
						Log.Info("Removing files from " + data_dir);
						IList<FilePath> metadataFiles = MiniDFSCluster.GetAllBlockMetadataFiles(data_dir);
						if (metadataFiles == null)
						{
							continue;
						}
						foreach (FilePath metadataFile in metadataFiles)
						{
							FilePath blockFile = Block.MetaToBlockFile(metadataFile);
							NUnit.Framework.Assert.IsTrue("Cannot remove file.", blockFile.Delete());
							NUnit.Framework.Assert.IsTrue("Cannot remove file.", metadataFile.Delete());
						}
					}
				}
				// Occasionally the BlockPoolSliceScanner can run before we have removed
				// the blocks. Restart the Datanode to trigger the scanner into running
				// once more.
				Log.Info("Restarting Datanode to trigger BlockPoolSliceScanner");
				cluster.RestartDataNodes();
				cluster.WaitActive();
				badFiles = namenode.GetNamesystem().ListCorruptFileBlocks("/srcdat2", null);
				while (badFiles.Count < maxCorruptFileBlocks)
				{
					Log.Info("# of corrupt files is: " + badFiles.Count);
					Sharpen.Thread.Sleep(10000);
					badFiles = namenode.GetNamesystem().ListCorruptFileBlocks("/srcdat2", null);
				}
				badFiles = namenode.GetNamesystem().ListCorruptFileBlocks("/srcdat2", null);
				Log.Info("Namenode has bad files. " + badFiles.Count);
				NUnit.Framework.Assert.IsTrue("Namenode has " + badFiles.Count + " bad files. Expecting "
					 + maxCorruptFileBlocks + ".", badFiles.Count == maxCorruptFileBlocks);
				CorruptFileBlockIterator iter = (CorruptFileBlockIterator)fs.ListCorruptFileBlocks
					(new Path("/srcdat2"));
				int corruptPaths = CountPaths(iter);
				NUnit.Framework.Assert.IsTrue("Expected more than " + maxCorruptFileBlocks + " corrupt file blocks but got "
					 + corruptPaths, corruptPaths > maxCorruptFileBlocks);
				NUnit.Framework.Assert.IsTrue("Iterator should have made more than 1 call but made "
					 + iter.GetCallsMade(), iter.GetCallsMade() > 1);
				util.Cleanup(fs, "/srcdat2");
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
