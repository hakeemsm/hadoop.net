using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestBackupNode
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.TestBackupNode
			));

		static TestBackupNode()
		{
			((Log4JLogger)Checkpointer.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)BackupImage.Log).GetLogger().SetLevel(Level.All);
		}

		internal static readonly string BaseDir = MiniDFSCluster.GetBaseDirectory();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			FilePath baseDir = new FilePath(BaseDir);
			if (baseDir.Exists())
			{
				if (!(FileUtil.FullyDelete(baseDir)))
				{
					throw new IOException("Cannot remove directory: " + baseDir);
				}
			}
			FilePath dirC = new FilePath(GetBackupNodeDir(HdfsServerConstants.StartupOption.Checkpoint
				, 1));
			dirC.Mkdirs();
			FilePath dirB = new FilePath(GetBackupNodeDir(HdfsServerConstants.StartupOption.Backup
				, 1));
			dirB.Mkdirs();
			dirB = new FilePath(GetBackupNodeDir(HdfsServerConstants.StartupOption.Backup, 2)
				);
			dirB.Mkdirs();
		}

		internal static string GetBackupNodeDir(HdfsServerConstants.StartupOption t, int 
			idx)
		{
			return BaseDir + "name" + t.GetName() + idx + "/";
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual BackupNode StartBackupNode(Configuration conf, HdfsServerConstants.StartupOption
			 startupOpt, int idx)
		{
			Configuration c = new HdfsConfiguration(conf);
			string dirs = GetBackupNodeDir(startupOpt, idx);
			c.Set(DFSConfigKeys.DfsNamenodeNameDirKey, dirs);
			c.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, "${" + DFSConfigKeys.DfsNamenodeNameDirKey
				 + "}");
			c.Set(DFSConfigKeys.DfsNamenodeBackupAddressKey, "127.0.0.1:0");
			c.Set(DFSConfigKeys.DfsNamenodeBackupHttpAddressKey, "127.0.0.1:0");
			BackupNode bn = (BackupNode)NameNode.CreateNameNode(new string[] { startupOpt.GetName
				() }, c);
			NUnit.Framework.Assert.IsTrue(bn.GetRole() + " must be in SafeMode.", bn.IsInSafeMode
				());
			NUnit.Framework.Assert.IsTrue(bn.GetRole() + " must be in StandbyState", Sharpen.Runtime.EqualsIgnoreCase
				(bn.GetNamesystem().GetHAState(), HAServiceProtocol.HAServiceState.Standby.ToString
				()));
			return bn;
		}

		internal virtual void WaitCheckpointDone(MiniDFSCluster cluster, long txid)
		{
			long thisCheckpointTxId;
			do
			{
				try
				{
					Log.Info("Waiting checkpoint to complete... " + "checkpoint txid should increase above "
						 + txid);
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
				// The checkpoint is not done until the nn has received it from the bn
				thisCheckpointTxId = cluster.GetNameNode().GetFSImage().GetStorage().GetMostRecentCheckpointTxId
					();
			}
			while (thisCheckpointTxId < txid);
			// Check that the checkpoint got uploaded to NN successfully
			FSImageTestUtil.AssertNNHasCheckpoints(cluster, Collections.SingletonList((int)thisCheckpointTxId
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckpointNode()
		{
			TestCheckpoint(HdfsServerConstants.StartupOption.Checkpoint);
		}

		/// <summary>
		/// Ensure that the backupnode will tail edits from the NN
		/// and keep in sync, even while the NN rolls, checkpoints
		/// occur, etc.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBackupNodeTailsEdits()
		{
			Configuration conf = new HdfsConfiguration();
			HAUtil.SetAllowStandbyReads(conf, true);
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			BackupNode backup = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				fileSys = cluster.GetFileSystem();
				backup = StartBackupNode(conf, HdfsServerConstants.StartupOption.Backup, 1);
				BackupImage bnImage = (BackupImage)backup.GetFSImage();
				TestBNInSync(cluster, backup, 1);
				// Force a roll -- BN should roll with NN.
				NameNode nn = cluster.GetNameNode();
				NamenodeProtocols nnRpc = nn.GetRpcServer();
				nnRpc.RollEditLog();
				NUnit.Framework.Assert.AreEqual(bnImage.GetEditLog().GetCurSegmentTxId(), nn.GetFSImage
					().GetEditLog().GetCurSegmentTxId());
				// BN should stay in sync after roll
				TestBNInSync(cluster, backup, 2);
				long nnImageBefore = nn.GetFSImage().GetStorage().GetMostRecentCheckpointTxId();
				// BN checkpoint
				backup.DoCheckpoint();
				// NN should have received a new image
				long nnImageAfter = nn.GetFSImage().GetStorage().GetMostRecentCheckpointTxId();
				NUnit.Framework.Assert.IsTrue("nn should have received new checkpoint. before: " 
					+ nnImageBefore + " after: " + nnImageAfter, nnImageAfter > nnImageBefore);
				// BN should stay in sync after checkpoint
				TestBNInSync(cluster, backup, 3);
				// Stop BN
				Storage.StorageDirectory sd = bnImage.GetStorage().GetStorageDir(0);
				backup.Stop();
				backup = null;
				// When shutting down the BN, it shouldn't finalize logs that are
				// still open on the NN
				FileJournalManager.EditLogFile editsLog = FSImageTestUtil.FindLatestEditsLog(sd);
				NUnit.Framework.Assert.AreEqual(editsLog.GetFirstTxId(), nn.GetFSImage().GetEditLog
					().GetCurSegmentTxId());
				NUnit.Framework.Assert.IsTrue("Should not have finalized " + editsLog, editsLog.IsInProgress
					());
				// do some edits
				NUnit.Framework.Assert.IsTrue(fileSys.Mkdirs(new Path("/edit-while-bn-down")));
				// start a new backup node
				backup = StartBackupNode(conf, HdfsServerConstants.StartupOption.Backup, 1);
				TestBNInSync(cluster, backup, 4);
				NUnit.Framework.Assert.IsNotNull(backup.GetNamesystem().GetFileInfo("/edit-while-bn-down"
					, false));
			}
			finally
			{
				Log.Info("Shutting down...");
				if (backup != null)
				{
					backup.Stop();
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
			AssertStorageDirsMatch(cluster.GetNameNode(), backup);
		}

		/// <exception cref="System.Exception"/>
		private void TestBNInSync(MiniDFSCluster cluster, BackupNode backup, int testIdx)
		{
			NameNode nn = cluster.GetNameNode();
			FileSystem fs = cluster.GetFileSystem();
			// Do a bunch of namespace operations, make sure they're replicated
			// to the BN.
			for (int i = 0; i < 10; i++)
			{
				string src = "/test_" + testIdx + "_" + i;
				Log.Info("Creating " + src + " on NN");
				Path p = new Path(src);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(p));
				GenericTestUtils.WaitFor(new _Supplier_226(src, backup, nn), 30, 10000);
			}
			AssertStorageDirsMatch(nn, backup);
		}

		private sealed class _Supplier_226 : Supplier<bool>
		{
			public _Supplier_226(string src, BackupNode backup, NameNode nn)
			{
				this.src = src;
				this.backup = backup;
				this.nn = nn;
			}

			public bool Get()
			{
				Org.Apache.Hadoop.Hdfs.Server.Namenode.TestBackupNode.Log.Info("Checking for " + 
					src + " on BN");
				try
				{
					bool hasFile = backup.GetNamesystem().GetFileInfo(src, false) != null;
					bool txnIdMatch = backup.GetRpcServer().GetTransactionID() == nn.GetRpcServer().GetTransactionID
						();
					return hasFile && txnIdMatch;
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}

			private readonly string src;

			private readonly BackupNode backup;

			private readonly NameNode nn;
		}

		/// <exception cref="System.Exception"/>
		private void AssertStorageDirsMatch(NameNode nn, BackupNode backup)
		{
			// Check that the stored files in the name dirs are identical
			IList<FilePath> dirs = Lists.NewArrayList(FSImageTestUtil.GetCurrentDirs(nn.GetFSImage
				().GetStorage(), null));
			Sharpen.Collections.AddAll(dirs, FSImageTestUtil.GetCurrentDirs(backup.GetFSImage
				().GetStorage(), null));
			FSImageTestUtil.AssertParallelFilesAreIdentical(dirs, ImmutableSet.Of("VERSION"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBackupNode()
		{
			TestCheckpoint(HdfsServerConstants.StartupOption.Backup);
		}

		/// <exception cref="System.Exception"/>
		internal virtual void TestCheckpoint(HdfsServerConstants.StartupOption op)
		{
			Path file1 = new Path("/checkpoint.dat");
			Path file2 = new Path("/checkpoint2.dat");
			Path file3 = new Path("/backup.dat");
			Configuration conf = new HdfsConfiguration();
			HAUtil.SetAllowStandbyReads(conf, true);
			short replication = (short)conf.GetInt("dfs.replication", 3);
			int numDatanodes = Math.Max(3, replication);
			conf.Set(DFSConfigKeys.DfsNamenodeBackupHttpAddressKey, "localhost:0");
			conf.Set(DFSConfigKeys.DfsBlockreportInitialDelayKey, "0");
			conf.SetInt(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
			// disable block scanner
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointTxnsKey, 1);
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			BackupNode backup = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				fileSys = cluster.GetFileSystem();
				//
				// verify that 'format' really blew away all pre-existing files
				//
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file2));
				//
				// Create file1
				//
				NUnit.Framework.Assert.IsTrue(fileSys.Mkdirs(file1));
				//
				// Take a checkpoint
				//
				long txid = cluster.GetNameNodeRpc().GetTransactionID();
				backup = StartBackupNode(conf, op, 1);
				WaitCheckpointDone(cluster, txid);
			}
			catch (IOException e)
			{
				Log.Error("Error in TestBackupNode:", e);
				NUnit.Framework.Assert.IsTrue(e.GetLocalizedMessage(), false);
			}
			finally
			{
				if (backup != null)
				{
					backup.Stop();
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
			FilePath nnCurDir = new FilePath(BaseDir, "name1/current/");
			FilePath bnCurDir = new FilePath(GetBackupNodeDir(op, 1), "/current/");
			FSImageTestUtil.AssertParallelFilesAreIdentical(ImmutableList.Of(bnCurDir, nnCurDir
				), ImmutableSet.Of<string>("VERSION"));
			try
			{
				//
				// Restart cluster and verify that file1 still exist.
				//
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
					).Build();
				fileSys = cluster.GetFileSystem();
				// check that file1 still exists
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(file1));
				fileSys.Delete(file1, true);
				// create new file file2
				fileSys.Mkdirs(file2);
				//
				// Take a checkpoint
				//
				long txid = cluster.GetNameNodeRpc().GetTransactionID();
				backup = StartBackupNode(conf, op, 1);
				WaitCheckpointDone(cluster, txid);
				for (int i = 0; i < 10; i++)
				{
					fileSys.Mkdirs(new Path("file_" + i));
				}
				txid = cluster.GetNameNodeRpc().GetTransactionID();
				backup.DoCheckpoint();
				WaitCheckpointDone(cluster, txid);
				txid = cluster.GetNameNodeRpc().GetTransactionID();
				backup.DoCheckpoint();
				WaitCheckpointDone(cluster, txid);
				// Try BackupNode operations
				IPEndPoint add = backup.GetNameNodeAddress();
				// Write to BN
				FileSystem bnFS = FileSystem.Get(new Path("hdfs://" + NetUtils.GetHostPortString(
					add)).ToUri(), conf);
				bool canWrite = true;
				try
				{
					Org.Apache.Hadoop.Hdfs.Server.Namenode.TestCheckpoint.WriteFile(bnFS, file3, replication
						);
				}
				catch (IOException eio)
				{
					Log.Info("Write to " + backup.GetRole() + " failed as expected: ", eio);
					canWrite = false;
				}
				NUnit.Framework.Assert.IsFalse("Write to BackupNode must be prohibited.", canWrite
					);
				// Reads are allowed for BackupNode, but not for CheckpointNode
				bool canRead = true;
				try
				{
					bnFS.Exists(file2);
				}
				catch (IOException eio)
				{
					Log.Info("Read from " + backup.GetRole() + " failed: ", eio);
					canRead = false;
				}
				NUnit.Framework.Assert.AreEqual("Reads to BackupNode are allowed, but not CheckpointNode."
					, canRead, backup.IsRole(HdfsServerConstants.NamenodeRole.Backup));
				Org.Apache.Hadoop.Hdfs.Server.Namenode.TestCheckpoint.WriteFile(fileSys, file3, replication
					);
				Org.Apache.Hadoop.Hdfs.Server.Namenode.TestCheckpoint.CheckFile(fileSys, file3, replication
					);
				// should also be on BN right away
				NUnit.Framework.Assert.IsTrue("file3 does not exist on BackupNode", op != HdfsServerConstants.StartupOption
					.Backup || backup.GetNamesystem().GetFileInfo(file3.ToUri().GetPath(), false) !=
					 null);
			}
			catch (IOException e)
			{
				Log.Error("Error in TestBackupNode:", e);
				throw new Exception(e);
			}
			finally
			{
				if (backup != null)
				{
					backup.Stop();
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
			FSImageTestUtil.AssertParallelFilesAreIdentical(ImmutableList.Of(bnCurDir, nnCurDir
				), ImmutableSet.Of<string>("VERSION"));
			try
			{
				//
				// Restart cluster and verify that file2 exists and
				// file1 does not exist.
				//
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).Build();
				fileSys = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(!fileSys.Exists(file1));
				// verify that file2 exists
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(file2));
			}
			catch (IOException e)
			{
				Log.Error("Error in TestBackupNode: ", e);
				NUnit.Framework.Assert.IsTrue(e.GetLocalizedMessage(), false);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Verify that a file can be read both from NameNode and BackupNode.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCanReadData()
		{
			Path file1 = new Path("/fileToRead.dat");
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			BackupNode backup = null;
			try
			{
				// Start NameNode and BackupNode
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(true).Build();
				fileSys = cluster.GetFileSystem();
				long txid = cluster.GetNameNodeRpc().GetTransactionID();
				backup = StartBackupNode(conf, HdfsServerConstants.StartupOption.Backup, 1);
				WaitCheckpointDone(cluster, txid);
				// Setup dual NameNode configuration for DataNodes
				string rpcAddrKeyPreffix = DFSConfigKeys.DfsNamenodeRpcAddressKey + ".bnCluster";
				string nnAddr = cluster.GetNameNode().GetNameNodeAddressHostPortString();
				conf.Get(DFSConfigKeys.DfsNamenodeRpcAddressKey);
				string bnAddr = backup.GetNameNodeAddressHostPortString();
				conf.Set(DFSConfigKeys.DfsNameservices, "bnCluster");
				conf.Set(DFSConfigKeys.DfsNameserviceId, "bnCluster");
				conf.Set(DFSConfigKeys.DfsHaNamenodesKeyPrefix + ".bnCluster", "nnActive, nnBackup"
					);
				conf.Set(rpcAddrKeyPreffix + ".nnActive", nnAddr);
				conf.Set(rpcAddrKeyPreffix + ".nnBackup", bnAddr);
				cluster.StartDataNodes(conf, 3, true, HdfsServerConstants.StartupOption.Regular, 
					null);
				DFSTestUtil.CreateFile(fileSys, file1, 8192, (short)3, 0);
				// Read the same file from file systems pointing to NN and BN
				FileSystem bnFS = FileSystem.Get(new Path("hdfs://" + bnAddr).ToUri(), conf);
				string nnData = DFSTestUtil.ReadFile(fileSys, file1);
				string bnData = DFSTestUtil.ReadFile(bnFS, file1);
				NUnit.Framework.Assert.AreEqual("Data read from BackupNode and NameNode is not the same."
					, nnData, bnData);
			}
			catch (IOException e)
			{
				Log.Error("Error in TestBackupNode: ", e);
				NUnit.Framework.Assert.IsTrue(e.GetLocalizedMessage(), false);
			}
			finally
			{
				if (fileSys != null)
				{
					fileSys.Close();
				}
				if (backup != null)
				{
					backup.Stop();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
