using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Qjournal.Server;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Test;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Tests for upgrading with HA enabled.</summary>
	public class TestDFSUpgradeWithHA
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDFSUpgradeWithHA));

		private Configuration conf;

		[SetUp]
		public virtual void CreateConfiguration()
		{
			conf = new HdfsConfiguration();
			// Turn off persistent IPC, so that the DFSClient can survive NN restart
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, 0);
		}

		private static void AssertCTimesEqual(MiniDFSCluster cluster)
		{
			long nn1CTime = cluster.GetNamesystem(0).GetFSImage().GetStorage().GetCTime();
			long nn2CTime = cluster.GetNamesystem(1).GetFSImage().GetStorage().GetCTime();
			NUnit.Framework.Assert.AreEqual(nn1CTime, nn2CTime);
		}

		private static void CheckClusterPreviousDirExistence(MiniDFSCluster cluster, bool
			 shouldExist)
		{
			for (int i = 0; i < 2; i++)
			{
				CheckNnPreviousDirExistence(cluster, i, shouldExist);
			}
		}

		private static void CheckNnPreviousDirExistence(MiniDFSCluster cluster, int index
			, bool shouldExist)
		{
			ICollection<URI> nameDirs = cluster.GetNameDirs(index);
			foreach (URI nnDir in nameDirs)
			{
				CheckPreviousDirExistence(new FilePath(nnDir), shouldExist);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CheckJnPreviousDirExistence(MiniQJMHACluster jnCluster, bool 
			shouldExist)
		{
			for (int i = 0; i < 3; i++)
			{
				CheckPreviousDirExistence(jnCluster.GetJournalCluster().GetJournalDir(i, "ns1"), 
					shouldExist);
			}
			if (shouldExist)
			{
				AssertEpochFilesCopied(jnCluster);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void AssertEpochFilesCopied(MiniQJMHACluster jnCluster)
		{
			for (int i = 0; i < 3; i++)
			{
				FilePath journalDir = jnCluster.GetJournalCluster().GetJournalDir(i, "ns1");
				FilePath currDir = new FilePath(journalDir, "current");
				FilePath prevDir = new FilePath(journalDir, "previous");
				foreach (string fileName in new string[] { Journal.LastPromisedFilename, Journal.
					LastWriterEpoch })
				{
					FilePath prevFile = new FilePath(prevDir, fileName);
					// Possible the prev file doesn't exist, e.g. if there has never been a
					// writer before the upgrade.
					if (prevFile.Exists())
					{
						PersistentLongFile prevLongFile = new PersistentLongFile(prevFile, -10);
						PersistentLongFile currLongFile = new PersistentLongFile(new FilePath(currDir, fileName
							), -11);
						NUnit.Framework.Assert.IsTrue("Value in " + fileName + " has decreased on upgrade in "
							 + journalDir, prevLongFile.Get() <= currLongFile.Get());
					}
				}
			}
		}

		private static void CheckPreviousDirExistence(FilePath rootDir, bool shouldExist)
		{
			FilePath previousDir = new FilePath(rootDir, "previous");
			if (shouldExist)
			{
				NUnit.Framework.Assert.IsTrue(previousDir + " does not exist", previousDir.Exists
					());
			}
			else
			{
				NUnit.Framework.Assert.IsFalse(previousDir + " does exist", previousDir.Exists());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void RunFinalizeCommand(MiniDFSCluster cluster)
		{
			HATestUtil.SetFailoverConfigurations(cluster, conf);
			new DFSAdmin(conf).FinalizeUpgrade();
		}

		/// <summary>
		/// Ensure that an admin cannot finalize an HA upgrade without at least one NN
		/// being active.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestCannotFinalizeIfNoActive()
		{
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).Build();
				FilePath sharedDir = new FilePath(cluster.GetSharedEditsDir(0, 1));
				// No upgrade is in progress at the moment.
				CheckClusterPreviousDirExistence(cluster, false);
				AssertCTimesEqual(cluster);
				CheckPreviousDirExistence(sharedDir, false);
				// Transition NN0 to active and do some FS ops.
				cluster.TransitionToActive(0);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo1")));
				// Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
				// flag.
				cluster.ShutdownNameNode(1);
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Upgrade
					);
				cluster.RestartNameNode(0, false);
				CheckNnPreviousDirExistence(cluster, 0, true);
				CheckNnPreviousDirExistence(cluster, 1, false);
				CheckPreviousDirExistence(sharedDir, true);
				// NN0 should come up in the active state when given the -upgrade option,
				// so no need to transition it to active.
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo2")));
				// Restart NN0 without the -upgrade flag, to make sure that works.
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Regular
					);
				cluster.RestartNameNode(0, false);
				// Make sure we can still do FS ops after upgrading.
				cluster.TransitionToActive(0);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo3")));
				// Now bootstrap the standby with the upgraded info.
				int rc = BootstrapStandby.Run(new string[] { "-force" }, cluster.GetConfiguration
					(1));
				NUnit.Framework.Assert.AreEqual(0, rc);
				// Now restart NN1 and make sure that we can do ops against that as well.
				cluster.RestartNameNode(1);
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo4")));
				AssertCTimesEqual(cluster);
				// Now there's no active NN.
				cluster.TransitionToStandby(1);
				try
				{
					RunFinalizeCommand(cluster);
					NUnit.Framework.Assert.Fail("Should not have been able to finalize upgrade with no NN active"
						);
				}
				catch (IOException ioe)
				{
					GenericTestUtils.AssertExceptionContains("Cannot finalize with no NameNode active"
						, ioe);
				}
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Make sure that an HA NN with NFS-based HA can successfully start and
		/// upgrade.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestNfsUpgrade()
		{
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).Build();
				FilePath sharedDir = new FilePath(cluster.GetSharedEditsDir(0, 1));
				// No upgrade is in progress at the moment.
				CheckClusterPreviousDirExistence(cluster, false);
				AssertCTimesEqual(cluster);
				CheckPreviousDirExistence(sharedDir, false);
				// Transition NN0 to active and do some FS ops.
				cluster.TransitionToActive(0);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo1")));
				// Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
				// flag.
				cluster.ShutdownNameNode(1);
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Upgrade
					);
				cluster.RestartNameNode(0, false);
				CheckNnPreviousDirExistence(cluster, 0, true);
				CheckNnPreviousDirExistence(cluster, 1, false);
				CheckPreviousDirExistence(sharedDir, true);
				// NN0 should come up in the active state when given the -upgrade option,
				// so no need to transition it to active.
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo2")));
				// Restart NN0 without the -upgrade flag, to make sure that works.
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Regular
					);
				cluster.RestartNameNode(0, false);
				// Make sure we can still do FS ops after upgrading.
				cluster.TransitionToActive(0);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo3")));
				// Now bootstrap the standby with the upgraded info.
				int rc = BootstrapStandby.Run(new string[] { "-force" }, cluster.GetConfiguration
					(1));
				NUnit.Framework.Assert.AreEqual(0, rc);
				// Now restart NN1 and make sure that we can do ops against that as well.
				cluster.RestartNameNode(1);
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo4")));
				AssertCTimesEqual(cluster);
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private long GetCommittedTxnIdValue(MiniQJMHACluster qjCluster)
		{
			Journal journal1 = qjCluster.GetJournalCluster().GetJournalNode(0).GetOrCreateJournal
				(MiniQJMHACluster.Nameservice);
			BestEffortLongFile committedTxnId = (BestEffortLongFile)Whitebox.GetInternalState
				(journal1, "committedTxnId");
			return committedTxnId != null ? committedTxnId.Get() : HdfsConstants.InvalidTxid;
		}

		/// <summary>
		/// Make sure that an HA NN can successfully upgrade when configured using
		/// JournalNodes.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeWithJournalNodes()
		{
			MiniQJMHACluster qjCluster = null;
			FileSystem fs = null;
			try
			{
				MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
				builder.GetDfsBuilder().NumDataNodes(0);
				qjCluster = builder.Build();
				MiniDFSCluster cluster = qjCluster.GetDfsCluster();
				// No upgrade is in progress at the moment.
				CheckJnPreviousDirExistence(qjCluster, false);
				CheckClusterPreviousDirExistence(cluster, false);
				AssertCTimesEqual(cluster);
				// Transition NN0 to active and do some FS ops.
				cluster.TransitionToActive(0);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo1")));
				// get the value of the committedTxnId in journal nodes
				long cidBeforeUpgrade = GetCommittedTxnIdValue(qjCluster);
				// Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
				// flag.
				cluster.ShutdownNameNode(1);
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Upgrade
					);
				cluster.RestartNameNode(0, false);
				CheckNnPreviousDirExistence(cluster, 0, true);
				CheckNnPreviousDirExistence(cluster, 1, false);
				CheckJnPreviousDirExistence(qjCluster, true);
				NUnit.Framework.Assert.IsTrue(cidBeforeUpgrade <= GetCommittedTxnIdValue(qjCluster
					));
				// NN0 should come up in the active state when given the -upgrade option,
				// so no need to transition it to active.
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo2")));
				// Restart NN0 without the -upgrade flag, to make sure that works.
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Regular
					);
				cluster.RestartNameNode(0, false);
				// Make sure we can still do FS ops after upgrading.
				cluster.TransitionToActive(0);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo3")));
				NUnit.Framework.Assert.IsTrue(GetCommittedTxnIdValue(qjCluster) > cidBeforeUpgrade
					);
				// Now bootstrap the standby with the upgraded info.
				int rc = BootstrapStandby.Run(new string[] { "-force" }, cluster.GetConfiguration
					(1));
				NUnit.Framework.Assert.AreEqual(0, rc);
				// Now restart NN1 and make sure that we can do ops against that as well.
				cluster.RestartNameNode(1);
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo4")));
				AssertCTimesEqual(cluster);
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (qjCluster != null)
				{
					qjCluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestFinalizeWithJournalNodes()
		{
			MiniQJMHACluster qjCluster = null;
			FileSystem fs = null;
			try
			{
				MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
				builder.GetDfsBuilder().NumDataNodes(0);
				qjCluster = builder.Build();
				MiniDFSCluster cluster = qjCluster.GetDfsCluster();
				// No upgrade is in progress at the moment.
				CheckJnPreviousDirExistence(qjCluster, false);
				CheckClusterPreviousDirExistence(cluster, false);
				AssertCTimesEqual(cluster);
				// Transition NN0 to active and do some FS ops.
				cluster.TransitionToActive(0);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo1")));
				long cidBeforeUpgrade = GetCommittedTxnIdValue(qjCluster);
				// Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
				// flag.
				cluster.ShutdownNameNode(1);
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Upgrade
					);
				cluster.RestartNameNode(0, false);
				NUnit.Framework.Assert.IsTrue(cidBeforeUpgrade <= GetCommittedTxnIdValue(qjCluster
					));
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo2")));
				CheckNnPreviousDirExistence(cluster, 0, true);
				CheckNnPreviousDirExistence(cluster, 1, false);
				CheckJnPreviousDirExistence(qjCluster, true);
				// Now bootstrap the standby with the upgraded info.
				int rc = BootstrapStandby.Run(new string[] { "-force" }, cluster.GetConfiguration
					(1));
				NUnit.Framework.Assert.AreEqual(0, rc);
				cluster.RestartNameNode(1);
				long cidDuringUpgrade = GetCommittedTxnIdValue(qjCluster);
				NUnit.Framework.Assert.IsTrue(cidDuringUpgrade > cidBeforeUpgrade);
				RunFinalizeCommand(cluster);
				NUnit.Framework.Assert.AreEqual(cidDuringUpgrade, GetCommittedTxnIdValue(qjCluster
					));
				CheckClusterPreviousDirExistence(cluster, false);
				CheckJnPreviousDirExistence(qjCluster, false);
				AssertCTimesEqual(cluster);
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (qjCluster != null)
				{
					qjCluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Make sure that even if the NN which initiated the upgrade is in the standby
		/// state that we're allowed to finalize.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestFinalizeFromSecondNameNodeWithJournalNodes()
		{
			MiniQJMHACluster qjCluster = null;
			FileSystem fs = null;
			try
			{
				MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
				builder.GetDfsBuilder().NumDataNodes(0);
				qjCluster = builder.Build();
				MiniDFSCluster cluster = qjCluster.GetDfsCluster();
				// No upgrade is in progress at the moment.
				CheckJnPreviousDirExistence(qjCluster, false);
				CheckClusterPreviousDirExistence(cluster, false);
				AssertCTimesEqual(cluster);
				// Transition NN0 to active and do some FS ops.
				cluster.TransitionToActive(0);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo1")));
				// Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
				// flag.
				cluster.ShutdownNameNode(1);
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Upgrade
					);
				cluster.RestartNameNode(0, false);
				CheckNnPreviousDirExistence(cluster, 0, true);
				CheckNnPreviousDirExistence(cluster, 1, false);
				CheckJnPreviousDirExistence(qjCluster, true);
				// Now bootstrap the standby with the upgraded info.
				int rc = BootstrapStandby.Run(new string[] { "-force" }, cluster.GetConfiguration
					(1));
				NUnit.Framework.Assert.AreEqual(0, rc);
				cluster.RestartNameNode(1);
				// Make the second NN (not the one that initiated the upgrade) active when
				// the finalize command is run.
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
				RunFinalizeCommand(cluster);
				CheckClusterPreviousDirExistence(cluster, false);
				CheckJnPreviousDirExistence(qjCluster, false);
				AssertCTimesEqual(cluster);
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (qjCluster != null)
				{
					qjCluster.Shutdown();
				}
			}
		}

		/// <summary>Make sure that an HA NN will start if a previous upgrade was in progress.
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartingWithUpgradeInProgressSucceeds()
		{
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).Build();
				// Simulate an upgrade having started.
				for (int i = 0; i < 2; i++)
				{
					foreach (URI uri in cluster.GetNameDirs(i))
					{
						FilePath prevTmp = new FilePath(new FilePath(uri), Storage.StorageTmpPrevious);
						Log.Info("creating previous tmp dir: " + prevTmp);
						NUnit.Framework.Assert.IsTrue(prevTmp.Mkdirs());
					}
				}
				cluster.RestartNameNodes();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test rollback with NFS shared dir.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRollbackWithNfs()
		{
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).Build();
				FilePath sharedDir = new FilePath(cluster.GetSharedEditsDir(0, 1));
				// No upgrade is in progress at the moment.
				CheckClusterPreviousDirExistence(cluster, false);
				AssertCTimesEqual(cluster);
				CheckPreviousDirExistence(sharedDir, false);
				// Transition NN0 to active and do some FS ops.
				cluster.TransitionToActive(0);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo1")));
				// Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
				// flag.
				cluster.ShutdownNameNode(1);
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Upgrade
					);
				cluster.RestartNameNode(0, false);
				CheckNnPreviousDirExistence(cluster, 0, true);
				CheckNnPreviousDirExistence(cluster, 1, false);
				CheckPreviousDirExistence(sharedDir, true);
				// NN0 should come up in the active state when given the -upgrade option,
				// so no need to transition it to active.
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo2")));
				// Now bootstrap the standby with the upgraded info.
				int rc = BootstrapStandby.Run(new string[] { "-force" }, cluster.GetConfiguration
					(1));
				NUnit.Framework.Assert.AreEqual(0, rc);
				cluster.RestartNameNode(1);
				CheckNnPreviousDirExistence(cluster, 0, true);
				CheckNnPreviousDirExistence(cluster, 1, true);
				CheckPreviousDirExistence(sharedDir, true);
				AssertCTimesEqual(cluster);
				// Now shut down the cluster and do the rollback.
				ICollection<URI> nn1NameDirs = cluster.GetNameDirs(0);
				cluster.Shutdown();
				conf.SetStrings(DFSConfigKeys.DfsNamenodeNameDirKey, Joiner.On(",").Join(nn1NameDirs
					));
				NameNode.DoRollback(conf, false);
				// The rollback operation should have rolled back the first NN's local
				// dirs, and the shared dir, but not the other NN's dirs. Those have to be
				// done by bootstrapping the standby.
				CheckNnPreviousDirExistence(cluster, 0, false);
				CheckPreviousDirExistence(sharedDir, false);
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestRollbackWithJournalNodes()
		{
			MiniQJMHACluster qjCluster = null;
			FileSystem fs = null;
			try
			{
				MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
				builder.GetDfsBuilder().NumDataNodes(0);
				qjCluster = builder.Build();
				MiniDFSCluster cluster = qjCluster.GetDfsCluster();
				// No upgrade is in progress at the moment.
				CheckClusterPreviousDirExistence(cluster, false);
				AssertCTimesEqual(cluster);
				CheckJnPreviousDirExistence(qjCluster, false);
				// Transition NN0 to active and do some FS ops.
				cluster.TransitionToActive(0);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo1")));
				long cidBeforeUpgrade = GetCommittedTxnIdValue(qjCluster);
				// Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
				// flag.
				cluster.ShutdownNameNode(1);
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Upgrade
					);
				cluster.RestartNameNode(0, false);
				CheckNnPreviousDirExistence(cluster, 0, true);
				CheckNnPreviousDirExistence(cluster, 1, false);
				CheckJnPreviousDirExistence(qjCluster, true);
				// NN0 should come up in the active state when given the -upgrade option,
				// so no need to transition it to active.
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo2")));
				long cidDuringUpgrade = GetCommittedTxnIdValue(qjCluster);
				NUnit.Framework.Assert.IsTrue(cidDuringUpgrade > cidBeforeUpgrade);
				// Now bootstrap the standby with the upgraded info.
				int rc = BootstrapStandby.Run(new string[] { "-force" }, cluster.GetConfiguration
					(1));
				NUnit.Framework.Assert.AreEqual(0, rc);
				cluster.RestartNameNode(1);
				CheckNnPreviousDirExistence(cluster, 0, true);
				CheckNnPreviousDirExistence(cluster, 1, true);
				CheckJnPreviousDirExistence(qjCluster, true);
				AssertCTimesEqual(cluster);
				// Shut down the NNs, but deliberately leave the JNs up and running.
				ICollection<URI> nn1NameDirs = cluster.GetNameDirs(0);
				cluster.Shutdown();
				conf.SetStrings(DFSConfigKeys.DfsNamenodeNameDirKey, Joiner.On(",").Join(nn1NameDirs
					));
				NameNode.DoRollback(conf, false);
				long cidAfterRollback = GetCommittedTxnIdValue(qjCluster);
				NUnit.Framework.Assert.IsTrue(cidBeforeUpgrade < cidAfterRollback);
				// make sure the committedTxnId has been reset correctly after rollback
				NUnit.Framework.Assert.IsTrue(cidDuringUpgrade > cidAfterRollback);
				// The rollback operation should have rolled back the first NN's local
				// dirs, and the shared dir, but not the other NN's dirs. Those have to be
				// done by bootstrapping the standby.
				CheckNnPreviousDirExistence(cluster, 0, false);
				CheckJnPreviousDirExistence(qjCluster, false);
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (qjCluster != null)
				{
					qjCluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Make sure that starting a second NN with the -upgrade flag fails if the
		/// other NN has already done that.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestCannotUpgradeSecondNameNode()
		{
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).Build();
				FilePath sharedDir = new FilePath(cluster.GetSharedEditsDir(0, 1));
				// No upgrade is in progress at the moment.
				CheckClusterPreviousDirExistence(cluster, false);
				AssertCTimesEqual(cluster);
				CheckPreviousDirExistence(sharedDir, false);
				// Transition NN0 to active and do some FS ops.
				cluster.TransitionToActive(0);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo1")));
				// Do the upgrade. Shut down NN1 and then restart NN0 with the upgrade
				// flag.
				cluster.ShutdownNameNode(1);
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Upgrade
					);
				cluster.RestartNameNode(0, false);
				CheckNnPreviousDirExistence(cluster, 0, true);
				CheckNnPreviousDirExistence(cluster, 1, false);
				CheckPreviousDirExistence(sharedDir, true);
				// NN0 should come up in the active state when given the -upgrade option,
				// so no need to transition it to active.
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo2")));
				// Restart NN0 without the -upgrade flag, to make sure that works.
				cluster.GetNameNodeInfos()[0].SetStartOpt(HdfsServerConstants.StartupOption.Regular
					);
				cluster.RestartNameNode(0, false);
				// Make sure we can still do FS ops after upgrading.
				cluster.TransitionToActive(0);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(new Path("/foo3")));
				// Make sure that starting the second NN with the -upgrade flag fails.
				cluster.GetNameNodeInfos()[1].SetStartOpt(HdfsServerConstants.StartupOption.Upgrade
					);
				try
				{
					cluster.RestartNameNode(1, false);
					NUnit.Framework.Assert.Fail("Should not have been able to start second NN with -upgrade"
						);
				}
				catch (IOException ioe)
				{
					GenericTestUtils.AssertExceptionContains("It looks like the shared log is already being upgraded"
						, ioe);
				}
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
