using System.IO;
using NUnit.Framework;
using Org.Apache.Bookkeeper.Proto;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	/// <summary>
	/// Integration test to ensure that the BookKeeper JournalManager
	/// works for HDFS Namenode HA
	/// </summary>
	public class TestBookKeeperAsHASharedDir
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestBookKeeperAsHASharedDir
			));

		private static BKJMUtil bkutil;

		internal static int numBookies = 3;

		private const string TestFileData = "HA BookKeeperJournalManager";

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupBookkeeper()
		{
			bkutil = new BKJMUtil(numBookies);
			bkutil.Start();
		}

		[SetUp]
		public virtual void ClearExitStatus()
		{
			ExitUtil.ResetFirstExitException();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TeardownBookkeeper()
		{
			bkutil.Teardown();
		}

		/// <summary>Test simple HA failover usecase with BK</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverWithBK()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
				conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, BKJMUtil.CreateJournalURI("/hotfailover"
					).ToString());
				BKJMUtil.AddJournalManagerDefinition(conf);
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).ManageNameDfsSharedDirs(false).Build();
				NameNode nn1 = cluster.GetNameNode(0);
				NameNode nn2 = cluster.GetNameNode(1);
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				Path p = new Path("/testBKJMfailover");
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				fs.Mkdirs(p);
				cluster.ShutdownNameNode(0);
				cluster.TransitionToActive(1);
				NUnit.Framework.Assert.IsTrue(fs.Exists(p));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test HA failover, where BK, as the shared storage, fails.</summary>
		/// <remarks>
		/// Test HA failover, where BK, as the shared storage, fails.
		/// Once it becomes available again, a standby can come up.
		/// Verify that any write happening after the BK fail is not
		/// available on the standby.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverWithFailingBKCluster()
		{
			int ensembleSize = numBookies + 1;
			BookieServer newBookie = bkutil.NewBookie();
			NUnit.Framework.Assert.AreEqual("New bookie didn't start", ensembleSize, bkutil.CheckBookiesUp
				(ensembleSize, 10));
			BookieServer replacementBookie = null;
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
				conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, BKJMUtil.CreateJournalURI("/hotfailoverWithFail"
					).ToString());
				conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperEnsembleSize, ensembleSize);
				conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperQuorumSize, ensembleSize);
				BKJMUtil.AddJournalManagerDefinition(conf);
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).ManageNameDfsSharedDirs(false).CheckExitOnShutdown(false).Build
					();
				NameNode nn1 = cluster.GetNameNode(0);
				NameNode nn2 = cluster.GetNameNode(1);
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				Path p1 = new Path("/testBKJMFailingBKCluster1");
				Path p2 = new Path("/testBKJMFailingBKCluster2");
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				fs.Mkdirs(p1);
				newBookie.Shutdown();
				// will take down shared storage
				NUnit.Framework.Assert.AreEqual("New bookie didn't stop", numBookies, bkutil.CheckBookiesUp
					(numBookies, 10));
				try
				{
					fs.Mkdirs(p2);
					NUnit.Framework.Assert.Fail("mkdirs should result in the NN exiting");
				}
				catch (RemoteException re)
				{
					NUnit.Framework.Assert.IsTrue(re.GetClassName().Contains("ExitException"));
				}
				cluster.ShutdownNameNode(0);
				try
				{
					cluster.TransitionToActive(1);
					NUnit.Framework.Assert.Fail("Shouldn't have been able to transition with bookies down"
						);
				}
				catch (ExitUtil.ExitException ee)
				{
					NUnit.Framework.Assert.IsTrue("Should shutdown due to required journal failure", 
						ee.Message.Contains("starting log segment 3 failed for required journal"));
				}
				replacementBookie = bkutil.NewBookie();
				NUnit.Framework.Assert.AreEqual("Replacement bookie didn't start", ensembleSize, 
					bkutil.CheckBookiesUp(ensembleSize, 10));
				cluster.TransitionToActive(1);
				// should work fine now
				NUnit.Framework.Assert.IsTrue(fs.Exists(p1));
				NUnit.Framework.Assert.IsFalse(fs.Exists(p2));
			}
			finally
			{
				newBookie.Shutdown();
				if (replacementBookie != null)
				{
					replacementBookie.Shutdown();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test that two namenodes can't continue as primary</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultiplePrimariesStarted()
		{
			Path p1 = new Path("/testBKJMMultiplePrimary");
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
				conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, BKJMUtil.CreateJournalURI("/hotfailoverMultiple"
					).ToString());
				BKJMUtil.AddJournalManagerDefinition(conf);
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).ManageNameDfsSharedDirs(false).CheckExitOnShutdown(false).Build
					();
				NameNode nn1 = cluster.GetNameNode(0);
				NameNode nn2 = cluster.GetNameNode(1);
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				fs.Mkdirs(p1);
				nn1.GetRpcServer().RollEditLog();
				cluster.TransitionToActive(1);
				fs = cluster.GetFileSystem(0);
				// get the older active server.
				try
				{
					fs.Delete(p1, true);
					NUnit.Framework.Assert.Fail("Log update on older active should cause it to exit");
				}
				catch (RemoteException re)
				{
					NUnit.Framework.Assert.IsTrue(re.GetClassName().Contains("ExitException"));
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Use NameNode INTIALIZESHAREDEDITS to initialize the shared edits.</summary>
		/// <remarks>
		/// Use NameNode INTIALIZESHAREDEDITS to initialize the shared edits. i.e. copy
		/// the edits log segments to new bkjm shared edits.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInitializeBKSharedEdits()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				HAUtil.SetAllowStandbyReads(conf, true);
				conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
				MiniDFSNNTopology topology = MiniDFSNNTopology.SimpleHATopology();
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).NumDataNodes(0).Build
					();
				cluster.WaitActive();
				// Shutdown and clear the current filebased shared dir.
				cluster.ShutdownNameNodes();
				FilePath shareddir = new FilePath(cluster.GetSharedEditsDir(0, 1));
				NUnit.Framework.Assert.IsTrue("Initial Shared edits dir not fully deleted", FileUtil
					.FullyDelete(shareddir));
				// Check namenodes should not start without shared dir.
				AssertCanNotStartNamenode(cluster, 0);
				AssertCanNotStartNamenode(cluster, 1);
				// Configure bkjm as new shared edits dir in both namenodes
				Configuration nn1Conf = cluster.GetConfiguration(0);
				Configuration nn2Conf = cluster.GetConfiguration(1);
				nn1Conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, BKJMUtil.CreateJournalURI
					("/initializeSharedEdits").ToString());
				nn2Conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, BKJMUtil.CreateJournalURI
					("/initializeSharedEdits").ToString());
				BKJMUtil.AddJournalManagerDefinition(nn1Conf);
				BKJMUtil.AddJournalManagerDefinition(nn2Conf);
				// Initialize the BKJM shared edits.
				NUnit.Framework.Assert.IsFalse(NameNode.InitializeSharedEdits(nn1Conf));
				// NameNode should be able to start and should be in sync with BKJM as
				// shared dir
				AssertCanStartHANameNodes(cluster, conf, "/testBKJMInitialize");
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private void AssertCanNotStartNamenode(MiniDFSCluster cluster, int nnIndex)
		{
			try
			{
				cluster.RestartNameNode(nnIndex, false);
				NUnit.Framework.Assert.Fail("Should not have been able to start NN" + (nnIndex) +
					 " without shared dir");
			}
			catch (IOException ioe)
			{
				Log.Info("Got expected exception", ioe);
				GenericTestUtils.AssertExceptionContains("storage directory does not exist or is not accessible"
					, ioe);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		private void AssertCanStartHANameNodes(MiniDFSCluster cluster, Configuration conf
			, string path)
		{
			// Now should be able to start both NNs. Pass "false" here so that we don't
			// try to waitActive on all NNs, since the second NN doesn't exist yet.
			cluster.RestartNameNode(0, false);
			cluster.RestartNameNode(1, true);
			// Make sure HA is working.
			cluster.GetNameNode(0).GetRpcServer().TransitionToActive(new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser));
			FileSystem fs = null;
			try
			{
				Path newPath = new Path(path);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(newPath));
				HATestUtil.WaitForStandbyToCatchUp(cluster.GetNameNode(0), cluster.GetNameNode(1)
					);
				NUnit.Framework.Assert.IsTrue(NameNodeAdapter.GetFileInfo(cluster.GetNameNode(1), 
					newPath.ToString(), false).IsDir());
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
			}
		}

		/// <summary>
		/// NameNode should load the edits correctly if the applicable edits are
		/// present in the BKJM.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNameNodeMultipleSwitchesUsingBKJM()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
				conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, BKJMUtil.CreateJournalURI("/correctEditLogSelection"
					).ToString());
				BKJMUtil.AddJournalManagerDefinition(conf);
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).ManageNameDfsSharedDirs(false).Build();
				NameNode nn1 = cluster.GetNameNode(0);
				NameNode nn2 = cluster.GetNameNode(1);
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				nn1.GetRpcServer().RollEditLog();
				// Roll Edits from current Active.
				// Transition to standby current active gracefully.
				cluster.TransitionToStandby(0);
				// Make the other Active and Roll edits multiple times
				cluster.TransitionToActive(1);
				nn2.GetRpcServer().RollEditLog();
				nn2.GetRpcServer().RollEditLog();
				// Now One more failover. So NN1 should be able to failover successfully.
				cluster.TransitionToStandby(1);
				cluster.TransitionToActive(0);
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
