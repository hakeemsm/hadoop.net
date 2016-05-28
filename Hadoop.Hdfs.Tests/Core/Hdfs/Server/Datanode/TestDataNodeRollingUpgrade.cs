using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Test;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Ensure that the DataNode correctly handles rolling upgrade
	/// finalize and rollback.
	/// </summary>
	public class TestDataNodeRollingUpgrade
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDataNodeRollingUpgrade
			));

		private const short ReplFactor = 1;

		private const int BlockSize = 1024 * 1024;

		private const long FileSize = BlockSize;

		private const long Seed = unchecked((long)(0x1BADF00DL));

		internal Configuration conf;

		internal MiniDFSCluster cluster = null;

		internal DistributedFileSystem fs = null;

		internal DataNode dn0 = null;

		internal NameNode nn = null;

		internal string blockPoolId = null;

		/// <exception cref="System.IO.IOException"/>
		private void StartCluster()
		{
			conf = new HdfsConfiguration();
			conf.SetInt("dfs.blocksize", 1024 * 1024);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(ReplFactor).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			nn = cluster.GetNameNode(0);
			NUnit.Framework.Assert.IsNotNull(nn);
			dn0 = cluster.GetDataNodes()[0];
			NUnit.Framework.Assert.IsNotNull(dn0);
			blockPoolId = cluster.GetNameNode(0).GetNamesystem().GetBlockPoolId();
		}

		private void ShutdownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
			fs = null;
			nn = null;
			dn0 = null;
			blockPoolId = null;
		}

		/// <exception cref="System.Exception"/>
		private void TriggerHeartBeats()
		{
			// Sleep briefly so that DN learns of the rolling upgrade
			// state and other states from heartbeats.
			cluster.TriggerHeartbeats();
			Sharpen.Thread.Sleep(5000);
		}

		/// <summary>Test assumes that the file has a single block</summary>
		/// <exception cref="System.IO.IOException"/>
		private FilePath GetBlockForFile(Path path, bool exists)
		{
			LocatedBlocks blocks = nn.GetRpcServer().GetBlockLocations(path.ToString(), 0, long.MaxValue
				);
			NUnit.Framework.Assert.AreEqual("The test helper functions assume that each file has a single block"
				, 1, blocks.GetLocatedBlocks().Count);
			ExtendedBlock block = blocks.GetLocatedBlocks()[0].GetBlock();
			BlockLocalPathInfo bInfo = dn0.GetFSDataset().GetBlockLocalPathInfo(block);
			FilePath blockFile = new FilePath(bInfo.GetBlockPath());
			NUnit.Framework.Assert.AreEqual(exists, blockFile.Exists());
			return blockFile;
		}

		private FilePath GetTrashFileForBlock(FilePath blockFile, bool exists)
		{
			FilePath trashFile = new FilePath(dn0.GetStorage().GetTrashDirectoryForBlockFile(
				blockPoolId, blockFile));
			NUnit.Framework.Assert.AreEqual(exists, trashFile.Exists());
			return trashFile;
		}

		/// <summary>Ensures that the blocks belonging to the deleted file are in trash</summary>
		/// <exception cref="System.Exception"/>
		private void DeleteAndEnsureInTrash(Path pathToDelete, FilePath blockFile, FilePath
			 trashFile)
		{
			NUnit.Framework.Assert.IsTrue(blockFile.Exists());
			NUnit.Framework.Assert.IsFalse(trashFile.Exists());
			// Now delete the file and ensure the corresponding block in trash
			Log.Info("Deleting file " + pathToDelete + " during rolling upgrade");
			fs.Delete(pathToDelete, false);
			System.Diagnostics.Debug.Assert((!fs.Exists(pathToDelete)));
			TriggerHeartBeats();
			NUnit.Framework.Assert.IsTrue(trashFile.Exists());
			NUnit.Framework.Assert.IsFalse(blockFile.Exists());
		}

		private bool IsTrashRootPresent()
		{
			// Trash is disabled; trash root does not exist
			BlockPoolSliceStorage bps = dn0.GetStorage().GetBPStorage(blockPoolId);
			return bps.TrashEnabled();
		}

		/// <summary>Ensures that the blocks from trash are restored</summary>
		/// <exception cref="System.Exception"/>
		private void EnsureTrashRestored(FilePath blockFile, FilePath trashFile)
		{
			NUnit.Framework.Assert.IsTrue(blockFile.Exists());
			NUnit.Framework.Assert.IsFalse(trashFile.Exists());
			NUnit.Framework.Assert.IsFalse(IsTrashRootPresent());
		}

		private bool IsBlockFileInPrevious(FilePath blockFile)
		{
			Sharpen.Pattern blockFilePattern = Sharpen.Pattern.Compile(string.Format("^(.*%1$scurrent%1$s.*%1$s)(current)(%1$s.*)$"
				, Sharpen.Pattern.Quote(FilePath.separator)));
			Matcher matcher = blockFilePattern.Matcher(blockFile.ToString());
			string previousFileName = matcher.ReplaceFirst("$1" + "previous" + "$3");
			return ((new FilePath(previousFileName)).Exists());
		}

		/// <exception cref="System.Exception"/>
		private void StartRollingUpgrade()
		{
			Log.Info("Starting rolling upgrade");
			fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			DFSAdmin dfsadmin = new DFSAdmin(conf);
			TestRollingUpgrade.RunCmd(dfsadmin, true, "-rollingUpgrade", "prepare");
			TriggerHeartBeats();
			// Ensure datanode rolling upgrade is started
			NUnit.Framework.Assert.IsTrue(dn0.GetFSDataset().TrashEnabled(blockPoolId));
		}

		/// <exception cref="System.Exception"/>
		private void FinalizeRollingUpgrade()
		{
			Log.Info("Finalizing rolling upgrade");
			DFSAdmin dfsadmin = new DFSAdmin(conf);
			TestRollingUpgrade.RunCmd(dfsadmin, true, "-rollingUpgrade", "finalize");
			TriggerHeartBeats();
			// Ensure datanode rolling upgrade is started
			NUnit.Framework.Assert.IsFalse(dn0.GetFSDataset().TrashEnabled(blockPoolId));
			BlockPoolSliceStorage bps = dn0.GetStorage().GetBPStorage(blockPoolId);
			NUnit.Framework.Assert.IsFalse(bps.TrashEnabled());
		}

		/// <exception cref="System.Exception"/>
		private void RollbackRollingUpgrade()
		{
			// Shutdown datanodes and namenodes
			// Restart the namenode with rolling upgrade rollback
			Log.Info("Starting rollback of the rolling upgrade");
			MiniDFSCluster.DataNodeProperties dnprop = cluster.StopDataNode(0);
			dnprop.SetDnArgs("-rollback");
			cluster.ShutdownNameNodes();
			cluster.RestartNameNode("-rollingupgrade", "rollback");
			cluster.RestartDataNode(dnprop);
			cluster.WaitActive();
			nn = cluster.GetNameNode(0);
			dn0 = cluster.GetDataNodes()[0];
			TriggerHeartBeats();
			Log.Info("The cluster is active after rollback");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDatanodeRollingUpgradeWithFinalize()
		{
			try
			{
				StartCluster();
				RollingUpgradeAndFinalize();
				// Do it again
				RollingUpgradeAndFinalize();
			}
			finally
			{
				ShutdownCluster();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDatanodeRUwithRegularUpgrade()
		{
			try
			{
				StartCluster();
				RollingUpgradeAndFinalize();
				MiniDFSCluster.DataNodeProperties dn = cluster.StopDataNode(0);
				cluster.RestartNameNode(0, true, "-upgrade");
				cluster.RestartDataNode(dn, true);
				cluster.WaitActive();
				fs = cluster.GetFileSystem(0);
				Path testFile3 = new Path("/" + GenericTestUtils.GetMethodName() + ".03.dat");
				DFSTestUtil.CreateFile(fs, testFile3, FileSize, ReplFactor, Seed);
				cluster.GetFileSystem().FinalizeUpgrade();
			}
			finally
			{
				ShutdownCluster();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void RollingUpgradeAndFinalize()
		{
			// Create files in DFS.
			Path testFile1 = new Path("/" + GenericTestUtils.GetMethodName() + ".01.dat");
			Path testFile2 = new Path("/" + GenericTestUtils.GetMethodName() + ".02.dat");
			DFSTestUtil.CreateFile(fs, testFile1, FileSize, ReplFactor, Seed);
			DFSTestUtil.CreateFile(fs, testFile2, FileSize, ReplFactor, Seed);
			StartRollingUpgrade();
			FilePath blockFile = GetBlockForFile(testFile2, true);
			FilePath trashFile = GetTrashFileForBlock(blockFile, false);
			cluster.TriggerBlockReports();
			DeleteAndEnsureInTrash(testFile2, blockFile, trashFile);
			FinalizeRollingUpgrade();
			// Ensure that delete file testFile2 stays deleted after finalize
			NUnit.Framework.Assert.IsFalse(IsTrashRootPresent());
			System.Diagnostics.Debug.Assert((!fs.Exists(testFile2)));
			System.Diagnostics.Debug.Assert((fs.Exists(testFile1)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDatanodeRollingUpgradeWithRollback()
		{
			try
			{
				StartCluster();
				// Create files in DFS.
				Path testFile1 = new Path("/" + GenericTestUtils.GetMethodName() + ".01.dat");
				DFSTestUtil.CreateFile(fs, testFile1, FileSize, ReplFactor, Seed);
				string fileContents1 = DFSTestUtil.ReadFile(fs, testFile1);
				StartRollingUpgrade();
				FilePath blockFile = GetBlockForFile(testFile1, true);
				FilePath trashFile = GetTrashFileForBlock(blockFile, false);
				DeleteAndEnsureInTrash(testFile1, blockFile, trashFile);
				// Now perform a rollback to restore DFS to the pre-rollback state.
				RollbackRollingUpgrade();
				// Ensure that block was restored from trash
				EnsureTrashRestored(blockFile, trashFile);
				// Ensure that files exist and restored file contents are the same.
				System.Diagnostics.Debug.Assert((fs.Exists(testFile1)));
				string fileContents2 = DFSTestUtil.ReadFile(fs, testFile1);
				Assert.AssertThat(fileContents1, IS.Is(fileContents2));
			}
			finally
			{
				ShutdownCluster();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDatanodePeersXceiver()
		{
			// Test DatanodeXceiver has correct peer-dataxceiver pairs for sending OOB message
			try
			{
				StartCluster();
				// Create files in DFS.
				string testFile1 = "/" + GenericTestUtils.GetMethodName() + ".01.dat";
				string testFile2 = "/" + GenericTestUtils.GetMethodName() + ".02.dat";
				string testFile3 = "/" + GenericTestUtils.GetMethodName() + ".03.dat";
				DFSClient client1 = new DFSClient(NameNode.GetAddress(conf), conf);
				DFSClient client2 = new DFSClient(NameNode.GetAddress(conf), conf);
				DFSClient client3 = new DFSClient(NameNode.GetAddress(conf), conf);
				DFSOutputStream s1 = (DFSOutputStream)client1.Create(testFile1, true);
				DFSOutputStream s2 = (DFSOutputStream)client2.Create(testFile2, true);
				DFSOutputStream s3 = (DFSOutputStream)client3.Create(testFile3, true);
				byte[] toWrite = new byte[1024 * 1024 * 8];
				Random rb = new Random(1111);
				rb.NextBytes(toWrite);
				s1.Write(toWrite, 0, 1024 * 1024 * 8);
				s1.Flush();
				s2.Write(toWrite, 0, 1024 * 1024 * 8);
				s2.Flush();
				s3.Write(toWrite, 0, 1024 * 1024 * 8);
				s3.Flush();
				NUnit.Framework.Assert.IsTrue(dn0.GetXferServer().GetNumPeersXceiver() == dn0.GetXferServer
					().GetNumPeersXceiver());
				s1.Close();
				s2.Close();
				s3.Close();
				NUnit.Framework.Assert.IsTrue(dn0.GetXferServer().GetNumPeersXceiver() == dn0.GetXferServer
					().GetNumPeersXceiver());
				client1.Close();
				client2.Close();
				client3.Close();
			}
			finally
			{
				ShutdownCluster();
			}
		}

		/// <summary>
		/// Support for layout version change with rolling upgrade was
		/// added by HDFS-6800 and HDFS-6981.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestWithLayoutChangeAndFinalize()
		{
			long seed = unchecked((int)(0x600DF00D));
			try
			{
				StartCluster();
				Path[] paths = new Path[3];
				FilePath[] blockFiles = new FilePath[3];
				// Create two files in DFS.
				for (int i = 0; i < 2; ++i)
				{
					paths[i] = new Path("/" + GenericTestUtils.GetMethodName() + "." + i + ".dat");
					DFSTestUtil.CreateFile(fs, paths[i], BlockSize, (short)2, seed);
				}
				StartRollingUpgrade();
				// Delete the first file. The DN will save its block files in trash.
				blockFiles[0] = GetBlockForFile(paths[0], true);
				FilePath trashFile0 = GetTrashFileForBlock(blockFiles[0], false);
				DeleteAndEnsureInTrash(paths[0], blockFiles[0], trashFile0);
				// Restart the DN with a new layout version to trigger layout upgrade.
				Log.Info("Shutting down the Datanode");
				MiniDFSCluster.DataNodeProperties dnprop = cluster.StopDataNode(0);
				DFSTestUtil.AddDataNodeLayoutVersion(DataNodeLayoutVersion.CurrentLayoutVersion -
					 1, "Test Layout for TestDataNodeRollingUpgrade");
				Log.Info("Restarting the DataNode");
				cluster.RestartDataNode(dnprop, true);
				cluster.WaitActive();
				dn0 = cluster.GetDataNodes()[0];
				Log.Info("The DN has been restarted");
				NUnit.Framework.Assert.IsFalse(trashFile0.Exists());
				NUnit.Framework.Assert.IsFalse(dn0.GetStorage().GetBPStorage(blockPoolId).IsTrashAllowed
					(blockFiles[0]));
				// Ensure that the block file for the first file was moved from 'trash' to 'previous'.
				NUnit.Framework.Assert.IsTrue(IsBlockFileInPrevious(blockFiles[0]));
				NUnit.Framework.Assert.IsFalse(IsTrashRootPresent());
				// Delete the second file. Ensure that its block file is in previous.
				blockFiles[1] = GetBlockForFile(paths[1], true);
				fs.Delete(paths[1], false);
				NUnit.Framework.Assert.IsTrue(IsBlockFileInPrevious(blockFiles[1]));
				NUnit.Framework.Assert.IsFalse(IsTrashRootPresent());
				// Finalize and ensure that neither block file exists in trash or previous.
				FinalizeRollingUpgrade();
				NUnit.Framework.Assert.IsFalse(IsTrashRootPresent());
				NUnit.Framework.Assert.IsFalse(IsBlockFileInPrevious(blockFiles[0]));
				NUnit.Framework.Assert.IsFalse(IsBlockFileInPrevious(blockFiles[1]));
			}
			finally
			{
				ShutdownCluster();
			}
		}

		/// <summary>
		/// Support for layout version change with rolling upgrade was
		/// added by HDFS-6800 and HDFS-6981.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestWithLayoutChangeAndRollback()
		{
			long seed = unchecked((int)(0x600DF00D));
			try
			{
				StartCluster();
				Path[] paths = new Path[3];
				FilePath[] blockFiles = new FilePath[3];
				// Create two files in DFS.
				for (int i = 0; i < 2; ++i)
				{
					paths[i] = new Path("/" + GenericTestUtils.GetMethodName() + "." + i + ".dat");
					DFSTestUtil.CreateFile(fs, paths[i], BlockSize, (short)1, seed);
				}
				StartRollingUpgrade();
				// Delete the first file. The DN will save its block files in trash.
				blockFiles[0] = GetBlockForFile(paths[0], true);
				FilePath trashFile0 = GetTrashFileForBlock(blockFiles[0], false);
				DeleteAndEnsureInTrash(paths[0], blockFiles[0], trashFile0);
				// Restart the DN with a new layout version to trigger layout upgrade.
				Log.Info("Shutting down the Datanode");
				MiniDFSCluster.DataNodeProperties dnprop = cluster.StopDataNode(0);
				DFSTestUtil.AddDataNodeLayoutVersion(DataNodeLayoutVersion.CurrentLayoutVersion -
					 1, "Test Layout for TestDataNodeRollingUpgrade");
				Log.Info("Restarting the DataNode");
				cluster.RestartDataNode(dnprop, true);
				cluster.WaitActive();
				dn0 = cluster.GetDataNodes()[0];
				Log.Info("The DN has been restarted");
				NUnit.Framework.Assert.IsFalse(trashFile0.Exists());
				NUnit.Framework.Assert.IsFalse(dn0.GetStorage().GetBPStorage(blockPoolId).IsTrashAllowed
					(blockFiles[0]));
				// Ensure that the block file for the first file was moved from 'trash' to 'previous'.
				NUnit.Framework.Assert.IsTrue(IsBlockFileInPrevious(blockFiles[0]));
				NUnit.Framework.Assert.IsFalse(IsTrashRootPresent());
				// Delete the second file. Ensure that its block file is in previous.
				blockFiles[1] = GetBlockForFile(paths[1], true);
				fs.Delete(paths[1], false);
				NUnit.Framework.Assert.IsTrue(IsBlockFileInPrevious(blockFiles[1]));
				NUnit.Framework.Assert.IsFalse(IsTrashRootPresent());
				// Create and delete a third file. Its block file should not be
				// in either trash or previous after deletion.
				paths[2] = new Path("/" + GenericTestUtils.GetMethodName() + ".2.dat");
				DFSTestUtil.CreateFile(fs, paths[2], BlockSize, (short)1, seed);
				blockFiles[2] = GetBlockForFile(paths[2], true);
				fs.Delete(paths[2], false);
				NUnit.Framework.Assert.IsFalse(IsBlockFileInPrevious(blockFiles[2]));
				NUnit.Framework.Assert.IsFalse(IsTrashRootPresent());
				// Rollback and ensure that the first two file contents were restored.
				RollbackRollingUpgrade();
				for (int i_1 = 0; i_1 < 2; ++i_1)
				{
					byte[] actual = DFSTestUtil.ReadFileBuffer(fs, paths[i_1]);
					byte[] calculated = DFSTestUtil.CalculateFileContentsFromSeed(seed, BlockSize);
					Assert.AssertArrayEquals(actual, calculated);
				}
				// And none of the block files must be in previous or trash.
				NUnit.Framework.Assert.IsFalse(IsTrashRootPresent());
				for (int i_2 = 0; i_2 < 3; ++i_2)
				{
					NUnit.Framework.Assert.IsFalse(IsBlockFileInPrevious(blockFiles[i_2]));
				}
			}
			finally
			{
				ShutdownCluster();
			}
		}
	}
}
