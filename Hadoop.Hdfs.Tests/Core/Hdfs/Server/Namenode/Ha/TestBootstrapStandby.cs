using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestBootstrapStandby
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestBootstrapStandby));

		private MiniDFSCluster cluster;

		private NameNode nn0;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetupCluster()
		{
			Configuration conf = new Configuration();
			MiniDFSNNTopology topology = new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
				("ns1").AddNN(new MiniDFSNNTopology.NNConf("nn1").SetHttpPort(20001)).AddNN(new 
				MiniDFSNNTopology.NNConf("nn2").SetHttpPort(20002)));
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).NumDataNodes(0).Build
				();
			cluster.WaitActive();
			nn0 = cluster.GetNameNode(0);
			cluster.TransitionToActive(0);
			cluster.ShutdownNameNode(1);
		}

		[TearDown]
		public virtual void ShutdownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test for the base success case.</summary>
		/// <remarks>
		/// Test for the base success case. The primary NN
		/// hasn't made any checkpoints, and we copy the fsimage_0
		/// file over and start up.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSuccessfulBaseCase()
		{
			RemoveStandbyNameDirs();
			try
			{
				cluster.RestartNameNode(1);
				NUnit.Framework.Assert.Fail("Did not throw");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("storage directory does not exist or is not accessible"
					, ioe);
			}
			int rc = BootstrapStandby.Run(new string[] { "-nonInteractive" }, cluster.GetConfiguration
				(1));
			NUnit.Framework.Assert.AreEqual(0, rc);
			// Should have copied over the namespace from the active
			FSImageTestUtil.AssertNNHasCheckpoints(cluster, 1, ImmutableList.Of(0));
			FSImageTestUtil.AssertNNFilesMatch(cluster);
			// We should now be able to start the standby successfully.
			cluster.RestartNameNode(1);
		}

		/// <summary>
		/// Test for downloading a checkpoint made at a later checkpoint
		/// from the active.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDownloadingLaterCheckpoint()
		{
			// Roll edit logs a few times to inflate txid
			nn0.GetRpcServer().RollEditLog();
			nn0.GetRpcServer().RollEditLog();
			// Make checkpoint
			NameNodeAdapter.EnterSafeMode(nn0, false);
			NameNodeAdapter.SaveNamespace(nn0);
			NameNodeAdapter.LeaveSafeMode(nn0);
			long expectedCheckpointTxId = NameNodeAdapter.GetNamesystem(nn0).GetFSImage().GetMostRecentCheckpointTxId
				();
			NUnit.Framework.Assert.AreEqual(6, expectedCheckpointTxId);
			int rc = BootstrapStandby.Run(new string[] { "-force" }, cluster.GetConfiguration
				(1));
			NUnit.Framework.Assert.AreEqual(0, rc);
			// Should have copied over the namespace from the active
			FSImageTestUtil.AssertNNHasCheckpoints(cluster, 1, ImmutableList.Of((int)expectedCheckpointTxId
				));
			FSImageTestUtil.AssertNNFilesMatch(cluster);
			// We should now be able to start the standby successfully.
			cluster.RestartNameNode(1);
		}

		/// <summary>
		/// Test for the case where the shared edits dir doesn't have
		/// all of the recent edit logs.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSharedEditsMissingLogs()
		{
			RemoveStandbyNameDirs();
			CheckpointSignature sig = nn0.GetRpcServer().RollEditLog();
			NUnit.Framework.Assert.AreEqual(3, sig.GetCurSegmentTxId());
			// Should have created edits_1-2 in shared edits dir
			URI editsUri = cluster.GetSharedEditsDir(0, 1);
			FilePath editsDir = new FilePath(editsUri);
			FilePath editsSegment = new FilePath(new FilePath(editsDir, "current"), NNStorage
				.GetFinalizedEditsFileName(1, 2));
			GenericTestUtils.AssertExists(editsSegment);
			// Delete the segment.
			NUnit.Framework.Assert.IsTrue(editsSegment.Delete());
			// Trying to bootstrap standby should now fail since the edit
			// logs aren't available in the shared dir.
			GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
				.GetLog(typeof(BootstrapStandby)));
			try
			{
				int rc = BootstrapStandby.Run(new string[] { "-force" }, cluster.GetConfiguration
					(1));
				NUnit.Framework.Assert.AreEqual(BootstrapStandby.ErrCodeLogsUnavailable, rc);
			}
			finally
			{
				logs.StopCapturing();
			}
			GenericTestUtils.AssertMatches(logs.GetOutput(), "FATAL.*Unable to read transaction ids 1-3 from the configured shared"
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStandbyDirsAlreadyExist()
		{
			// Should not pass since standby dirs exist, force not given
			int rc = BootstrapStandby.Run(new string[] { "-nonInteractive" }, cluster.GetConfiguration
				(1));
			NUnit.Framework.Assert.AreEqual(BootstrapStandby.ErrCodeAlreadyFormatted, rc);
			// Should pass with -force
			rc = BootstrapStandby.Run(new string[] { "-force" }, cluster.GetConfiguration(1));
			NUnit.Framework.Assert.AreEqual(0, rc);
		}

		/// <summary>
		/// Test that, even if the other node is not active, we are able
		/// to bootstrap standby from it.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestOtherNodeNotActive()
		{
			cluster.TransitionToStandby(0);
			int rc = BootstrapStandby.Run(new string[] { "-force" }, cluster.GetConfiguration
				(1));
			NUnit.Framework.Assert.AreEqual(0, rc);
		}

		private void RemoveStandbyNameDirs()
		{
			foreach (URI u in cluster.GetNameDirs(1))
			{
				NUnit.Framework.Assert.IsTrue(u.GetScheme().Equals("file"));
				FilePath dir = new FilePath(u.GetPath());
				Log.Info("Removing standby dir " + dir);
				NUnit.Framework.Assert.IsTrue(FileUtil.FullyDelete(dir));
			}
		}
	}
}
