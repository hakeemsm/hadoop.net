using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Test BootstrapStandby when QJM is used for shared edits.</summary>
	public class TestBootstrapStandbyWithQJM
	{
		internal enum UpgradeState
		{
			Normal,
			Recover,
			Format
		}

		private MiniDFSCluster cluster;

		private MiniJournalCluster jCluster;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			Configuration conf = new Configuration();
			// Turn off IPC client caching, so that the suite can handle
			// the restart of the daemons between test cases.
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, 0);
			MiniQJMHACluster miniQjmHaCluster = new MiniQJMHACluster.Builder(conf).Build();
			cluster = miniQjmHaCluster.GetDfsCluster();
			jCluster = miniQjmHaCluster.GetJournalCluster();
			// make nn0 active
			cluster.TransitionToActive(0);
			// do sth to generate in-progress edit log data
			DistributedFileSystem dfs = (DistributedFileSystem)HATestUtil.ConfigureFailoverFs
				(cluster, conf);
			dfs.Mkdirs(new Path("/test2"));
			dfs.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Cleanup()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			if (jCluster != null)
			{
				jCluster.Shutdown();
			}
		}

		/// <summary>BootstrapStandby when the existing NN is standby</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBootstrapStandbyWithStandbyNN()
		{
			// make the first NN in standby state
			cluster.TransitionToStandby(0);
			Configuration confNN1 = cluster.GetConfiguration(1);
			// shut down nn1
			cluster.ShutdownNameNode(1);
			int rc = BootstrapStandby.Run(new string[] { "-force" }, confNN1);
			NUnit.Framework.Assert.AreEqual(0, rc);
			// Should have copied over the namespace from the standby
			FSImageTestUtil.AssertNNHasCheckpoints(cluster, 1, ImmutableList.Of(0));
			FSImageTestUtil.AssertNNFilesMatch(cluster);
		}

		/// <summary>BootstrapStandby when the existing NN is active</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBootstrapStandbyWithActiveNN()
		{
			// make the first NN in active state
			cluster.TransitionToActive(0);
			Configuration confNN1 = cluster.GetConfiguration(1);
			// shut down nn1
			cluster.ShutdownNameNode(1);
			int rc = BootstrapStandby.Run(new string[] { "-force" }, confNN1);
			NUnit.Framework.Assert.AreEqual(0, rc);
			// Should have copied over the namespace from the standby
			FSImageTestUtil.AssertNNHasCheckpoints(cluster, 1, ImmutableList.Of(0));
			FSImageTestUtil.AssertNNFilesMatch(cluster);
		}

		/// <summary>Test the bootstrapstandby while the other namenode is in upgrade state.</summary>
		/// <remarks>
		/// Test the bootstrapstandby while the other namenode is in upgrade state.
		/// Make sure a previous directory can be created.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgrade()
		{
			TestUpgrade(TestBootstrapStandbyWithQJM.UpgradeState.Normal);
		}

		/// <summary>
		/// Similar with testUpgrade, but rename nn1's current directory to
		/// previous.tmp before bootstrapStandby, and make sure the nn1 is recovered
		/// first then converted into upgrade state.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeWithRecover()
		{
			TestUpgrade(TestBootstrapStandbyWithQJM.UpgradeState.Recover);
		}

		/// <summary>
		/// Similar with testUpgrade, but rename nn1's current directory to a random
		/// name so that it's not formatted.
		/// </summary>
		/// <remarks>
		/// Similar with testUpgrade, but rename nn1's current directory to a random
		/// name so that it's not formatted. Make sure the nn1 is formatted and then
		/// converted into upgrade state.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeWithFormat()
		{
			TestUpgrade(TestBootstrapStandbyWithQJM.UpgradeState.Format);
		}

		/// <exception cref="System.Exception"/>
		private void TestUpgrade(TestBootstrapStandbyWithQJM.UpgradeState state)
		{
			cluster.TransitionToActive(0);
			Configuration confNN1 = cluster.GetConfiguration(1);
			FilePath current = cluster.GetNameNode(1).GetFSImage().GetStorage().GetStorageDir
				(0).GetCurrentDir();
			FilePath tmp = cluster.GetNameNode(1).GetFSImage().GetStorage().GetStorageDir(0).
				GetPreviousTmp();
			// shut down nn1
			cluster.ShutdownNameNode(1);
			// make NN0 in upgrade state
			FSImage fsImage0 = cluster.GetNameNode(0).GetNamesystem().GetFSImage();
			Whitebox.SetInternalState(fsImage0, "isUpgradeFinalized", false);
			switch (state)
			{
				case TestBootstrapStandbyWithQJM.UpgradeState.Recover:
				{
					// rename the current directory to previous.tmp in nn1
					NNStorage.Rename(current, tmp);
					break;
				}

				case TestBootstrapStandbyWithQJM.UpgradeState.Format:
				{
					// rename the current directory to a random name so it's not formatted
					FilePath wrongPath = new FilePath(current.GetParentFile(), "wrong");
					NNStorage.Rename(current, wrongPath);
					break;
				}

				default:
				{
					break;
				}
			}
			int rc = BootstrapStandby.Run(new string[] { "-force" }, confNN1);
			NUnit.Framework.Assert.AreEqual(0, rc);
			// Should have copied over the namespace from the standby
			FSImageTestUtil.AssertNNHasCheckpoints(cluster, 1, ImmutableList.Of(0));
			FSImageTestUtil.AssertNNFilesMatch(cluster);
			// make sure the NN1 is in upgrade state, i.e., the previous directory has
			// been successfully created
			cluster.RestartNameNode(1);
			NUnit.Framework.Assert.IsFalse(cluster.GetNameNode(1).GetNamesystem().IsUpgradeFinalized
				());
		}
	}
}
