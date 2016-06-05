using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Tools;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests rollback for rolling upgrade.</summary>
	public class TestRollingUpgradeRollback
	{
		private const int NumJournalNodes = 3;

		private const string JournalId = "myjournal";

		private static bool FileExists(IList<FilePath> files)
		{
			foreach (FilePath file in files)
			{
				if (file.Exists())
				{
					return true;
				}
			}
			return false;
		}

		private void CheckNNStorage(NNStorage storage, long imageTxId, long trashEndTxId)
		{
			IList<FilePath> finalizedEdits = storage.GetFiles(NNStorage.NameNodeDirType.Edits
				, NNStorage.GetFinalizedEditsFileName(1, imageTxId));
			NUnit.Framework.Assert.IsTrue(FileExists(finalizedEdits));
			IList<FilePath> inprogressEdits = storage.GetFiles(NNStorage.NameNodeDirType.Edits
				, NNStorage.GetInProgressEditsFileName(imageTxId + 1));
			// For rollback case we will have an inprogress file for future transactions
			NUnit.Framework.Assert.IsTrue(FileExists(inprogressEdits));
			if (trashEndTxId > 0)
			{
				IList<FilePath> trashedEdits = storage.GetFiles(NNStorage.NameNodeDirType.Edits, 
					NNStorage.GetFinalizedEditsFileName(imageTxId + 1, trashEndTxId) + ".trash");
				NUnit.Framework.Assert.IsTrue(FileExists(trashedEdits));
			}
			string imageFileName = trashEndTxId > 0 ? NNStorage.GetImageFileName(imageTxId) : 
				NNStorage.GetRollbackImageFileName(imageTxId);
			IList<FilePath> imageFiles = storage.GetFiles(NNStorage.NameNodeDirType.Image, imageFileName
				);
			NUnit.Framework.Assert.IsTrue(FileExists(imageFiles));
		}

		private void CheckJNStorage(FilePath dir, long discardStartTxId, long discardEndTxId
			)
		{
			FilePath finalizedEdits = new FilePath(dir, NNStorage.GetFinalizedEditsFileName(1
				, discardStartTxId - 1));
			NUnit.Framework.Assert.IsTrue(finalizedEdits.Exists());
			FilePath trashEdits = new FilePath(dir, NNStorage.GetFinalizedEditsFileName(discardStartTxId
				, discardEndTxId) + ".trash");
			NUnit.Framework.Assert.IsTrue(trashEdits.Exists());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRollbackCommand()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			Path foo = new Path("/foo");
			Path bar = new Path("/bar");
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				DFSAdmin dfsadmin = new DFSAdmin(conf);
				dfs.Mkdirs(foo);
				// start rolling upgrade
				dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				NUnit.Framework.Assert.AreEqual(0, dfsadmin.Run(new string[] { "-rollingUpgrade", 
					"prepare" }));
				dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				// create new directory
				dfs.Mkdirs(bar);
				// check NNStorage
				NNStorage storage = cluster.GetNamesystem().GetFSImage().GetStorage();
				CheckNNStorage(storage, 3, -1);
			}
			finally
			{
				// (startSegment, mkdir, endSegment) 
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			NameNode nn = null;
			try
			{
				nn = NameNode.CreateNameNode(new string[] { "-rollingUpgrade", "rollback" }, conf
					);
				// make sure /foo is still there, but /bar is not
				INode fooNode = nn.GetNamesystem().GetFSDirectory().GetINode4Write(foo.ToString()
					);
				NUnit.Framework.Assert.IsNotNull(fooNode);
				INode barNode = nn.GetNamesystem().GetFSDirectory().GetINode4Write(bar.ToString()
					);
				NUnit.Framework.Assert.IsNull(barNode);
				// check the details of NNStorage
				NNStorage storage = nn.GetNamesystem().GetFSImage().GetStorage();
				// (startSegment, upgrade marker, mkdir, endSegment)
				CheckNNStorage(storage, 3, 7);
			}
			finally
			{
				if (nn != null)
				{
					nn.Stop();
					nn.Join();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRollbackWithQJM()
		{
			Configuration conf = new HdfsConfiguration();
			MiniJournalCluster mjc = null;
			MiniDFSCluster cluster = null;
			Path foo = new Path("/foo");
			Path bar = new Path("/bar");
			try
			{
				mjc = new MiniJournalCluster.Builder(conf).NumJournalNodes(NumJournalNodes).Build
					();
				conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, mjc.GetQuorumJournalURI(JournalId)
					.ToString());
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				DFSAdmin dfsadmin = new DFSAdmin(conf);
				dfs.Mkdirs(foo);
				// start rolling upgrade
				dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				NUnit.Framework.Assert.AreEqual(0, dfsadmin.Run(new string[] { "-rollingUpgrade", 
					"prepare" }));
				dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				// create new directory
				dfs.Mkdirs(bar);
				dfs.Close();
				// rollback
				cluster.RestartNameNode("-rollingUpgrade", "rollback");
				// make sure /foo is still there, but /bar is not
				dfs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(dfs.Exists(foo));
				NUnit.Framework.Assert.IsFalse(dfs.Exists(bar));
				// check storage in JNs
				for (int i = 0; i < NumJournalNodes; i++)
				{
					FilePath dir = mjc.GetCurrentDir(0, JournalId);
					// segments:(startSegment, mkdir, endSegment), (startSegment, upgrade
					// marker, mkdir, endSegment)
					CheckJNStorage(dir, 4, 7);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				if (mjc != null)
				{
					mjc.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test rollback scenarios where StandbyNameNode does checkpoints during
		/// rolling upgrade.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRollbackWithHAQJM()
		{
			Configuration conf = new HdfsConfiguration();
			MiniQJMHACluster cluster = null;
			Path foo = new Path("/foo");
			Path bar = new Path("/bar");
			try
			{
				cluster = new MiniQJMHACluster.Builder(conf).Build();
				MiniDFSCluster dfsCluster = cluster.GetDfsCluster();
				dfsCluster.WaitActive();
				// let NN1 tail editlog every 1s
				dfsCluster.GetConfiguration(1).SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
				dfsCluster.RestartNameNode(1);
				dfsCluster.TransitionToActive(0);
				DistributedFileSystem dfs = dfsCluster.GetFileSystem(0);
				dfs.Mkdirs(foo);
				// start rolling upgrade
				RollingUpgradeInfo info = dfs.RollingUpgrade(HdfsConstants.RollingUpgradeAction.Prepare
					);
				NUnit.Framework.Assert.IsTrue(info.IsStarted());
				// create new directory
				dfs.Mkdirs(bar);
				dfs.Close();
				TestRollingUpgrade.QueryForPreparation(dfs);
				// If the query returns true, both active and the standby NN should have
				// rollback fsimage ready.
				NUnit.Framework.Assert.IsTrue(dfsCluster.GetNameNode(0).GetFSImage().HasRollbackFSImage
					());
				NUnit.Framework.Assert.IsTrue(dfsCluster.GetNameNode(1).GetFSImage().HasRollbackFSImage
					());
				// rollback NN0
				dfsCluster.RestartNameNode(0, true, "-rollingUpgrade", "rollback");
				// shutdown NN1
				dfsCluster.ShutdownNameNode(1);
				dfsCluster.TransitionToActive(0);
				// make sure /foo is still there, but /bar is not
				dfs = dfsCluster.GetFileSystem(0);
				NUnit.Framework.Assert.IsTrue(dfs.Exists(foo));
				NUnit.Framework.Assert.IsFalse(dfs.Exists(bar));
				// check the details of NNStorage
				NNStorage storage = dfsCluster.GetNamesystem(0).GetFSImage().GetStorage();
				// segments:(startSegment, mkdir, start upgrade endSegment), 
				// (startSegment, mkdir, endSegment)
				CheckNNStorage(storage, 4, 7);
				// check storage in JNs
				for (int i = 0; i < NumJournalNodes; i++)
				{
					FilePath dir = cluster.GetJournalCluster().GetCurrentDir(0, MiniQJMHACluster.Nameservice
						);
					CheckJNStorage(dir, 5, 7);
				}
				// restart NN0 again to make sure we can start using the new fsimage and
				// the corresponding md5 checksum
				dfsCluster.RestartNameNode(0);
				// start the rolling upgrade again to make sure we do not load upgrade
				// status after the rollback
				dfsCluster.TransitionToActive(0);
				dfs.RollingUpgrade(HdfsConstants.RollingUpgradeAction.Prepare);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
		// TODO: rollback could not succeed in all JN
	}
}
