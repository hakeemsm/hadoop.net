using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Test the ability of a DN to tolerate volume failures.</summary>
	public class TestDataNodeVolumeFailureToleration
	{
		private FileSystem fs;

		private MiniDFSCluster cluster;

		private Configuration conf;

		private string dataDir;

		internal readonly int WaitForHeartbeats = 3000;

		internal readonly int WaitForDeath = 15000;

		// Sleep at least 3 seconds (a 1s heartbeat plus padding) to allow
		// for heartbeats to propagate from the datanodes to the namenode.
		// Wait at least (2 * re-check + 10 * heartbeat) seconds for
		// a datanode to be considered dead by the namenode.  
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 512L);
			/*
			* Lower the DN heartbeat, DF rate, and recheck interval to one second
			* so state about failures and datanode death propagates faster.
			*/
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsDfIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			// Allow a single volume failure (there are two volumes)
			conf.SetInt(DFSConfigKeys.DfsDatanodeFailedVolumesToleratedKey, 1);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			dataDir = cluster.GetDataDirectory();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			cluster.Shutdown();
		}

		/// <summary>
		/// Test the DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY configuration
		/// option, ie the DN tolerates a failed-to-use scenario during
		/// its start-up.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestValidVolumesAtStartup()
		{
			Assume.AssumeTrue(!Runtime.GetProperty("os.name").StartsWith("Windows"));
			// Make sure no DNs are running.
			cluster.ShutdownDataNodes();
			// Bring up a datanode with two default data dirs, but with one bad one.
			conf.SetInt(DFSConfigKeys.DfsDatanodeFailedVolumesToleratedKey, 1);
			// We use subdirectories 0 and 1 in order to have only a single
			// data dir's parent inject a failure.
			FilePath tld = new FilePath(MiniDFSCluster.GetBaseDirectory(), "badData");
			FilePath dataDir1 = new FilePath(tld, "data1");
			FilePath dataDir1Actual = new FilePath(dataDir1, "1");
			dataDir1Actual.Mkdirs();
			// Force an IOE to occur on one of the dfs.data.dir.
			FilePath dataDir2 = new FilePath(tld, "data2");
			PrepareDirToFail(dataDir2);
			FilePath dataDir2Actual = new FilePath(dataDir2, "2");
			// Start one DN, with manually managed DN dir
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dataDir1Actual.GetPath() + "," + dataDir2Actual
				.GetPath());
			cluster.StartDataNodes(conf, 1, false, null, null);
			cluster.WaitActive();
			try
			{
				NUnit.Framework.Assert.IsTrue("The DN should have started up fine.", cluster.IsDataNodeUp
					());
				DataNode dn = cluster.GetDataNodes()[0];
				string si = DataNodeTestUtils.GetFSDataset(dn).GetStorageInfo();
				NUnit.Framework.Assert.IsTrue("The DN should have started with this directory", si
					.Contains(dataDir1Actual.GetPath()));
				NUnit.Framework.Assert.IsFalse("The DN shouldn't have a bad directory.", si.Contains
					(dataDir2Actual.GetPath()));
			}
			finally
			{
				cluster.ShutdownDataNodes();
				FileUtil.Chmod(dataDir2.ToString(), "755");
			}
		}

		/// <summary>
		/// Test the DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY configuration
		/// option, ie the DN shuts itself down when the number of failures
		/// experienced drops below the tolerated amount.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConfigureMinValidVolumes()
		{
			Assume.AssumeTrue(!Runtime.GetProperty("os.name").StartsWith("Windows"));
			// Bring up two additional datanodes that need both of their volumes
			// functioning in order to stay up.
			conf.SetInt(DFSConfigKeys.DfsDatanodeFailedVolumesToleratedKey, 0);
			cluster.StartDataNodes(conf, 2, true, null, null);
			cluster.WaitActive();
			DatanodeManager dm = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
				();
			long origCapacity = DFSTestUtil.GetLiveDatanodeCapacity(dm);
			long dnCapacity = DFSTestUtil.GetDatanodeCapacity(dm, 0);
			// Fail a volume on the 2nd DN
			FilePath dn2Vol1 = new FilePath(dataDir, "data" + (2 * 1 + 1));
			DataNodeTestUtils.InjectDataDirFailure(dn2Vol1);
			// Should only get two replicas (the first DN and the 3rd)
			Path file1 = new Path("/test1");
			DFSTestUtil.CreateFile(fs, file1, 1024, (short)3, 1L);
			DFSTestUtil.WaitReplication(fs, file1, (short)2);
			// Check that this single failure caused a DN to die.
			DFSTestUtil.WaitForDatanodeStatus(dm, 2, 1, 0, origCapacity - (1 * dnCapacity), WaitForHeartbeats
				);
			// If we restore the volume we should still only be able to get
			// two replicas since the DN is still considered dead.
			DataNodeTestUtils.RestoreDataDirFromFailure(dn2Vol1);
			Path file2 = new Path("/test2");
			DFSTestUtil.CreateFile(fs, file2, 1024, (short)3, 1L);
			DFSTestUtil.WaitReplication(fs, file2, (short)2);
		}

		/// <summary>Restart the datanodes with a new volume tolerated value.</summary>
		/// <param name="volTolerated">number of dfs data dir failures to tolerate</param>
		/// <param name="manageDfsDirs">whether the mini cluster should manage data dirs</param>
		/// <exception cref="System.IO.IOException"/>
		private void RestartDatanodes(int volTolerated, bool manageDfsDirs)
		{
			// Make sure no datanode is running
			cluster.ShutdownDataNodes();
			conf.SetInt(DFSConfigKeys.DfsDatanodeFailedVolumesToleratedKey, volTolerated);
			cluster.StartDataNodes(conf, 1, manageDfsDirs, null, null);
			cluster.WaitActive();
		}

		/// <summary>
		/// Test for different combination of volume configs and volumes tolerated
		/// values.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVolumeAndTolerableConfiguration()
		{
			// Check if Block Pool Service exit for an invalid conf value.
			TestVolumeConfig(-1, 0, false, true);
			// Ditto if the value is too big.
			TestVolumeConfig(100, 0, false, true);
			// Test for one failed volume
			TestVolumeConfig(0, 1, false, false);
			// Test for one failed volume with 1 tolerable volume
			TestVolumeConfig(1, 1, true, false);
			// Test all good volumes
			TestVolumeConfig(0, 0, true, false);
			// Test all failed volumes
			TestVolumeConfig(0, 2, false, false);
		}

		/// <summary>Tests for a given volumes to be tolerated and volumes failed.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void TestVolumeConfig(int volumesTolerated, int volumesFailed, bool expectedBPServiceState
			, bool manageDfsDirs)
		{
			Assume.AssumeTrue(!Runtime.GetProperty("os.name").StartsWith("Windows"));
			int dnIndex = 0;
			// Fail the current directory since invalid storage directory perms
			// get fixed up automatically on datanode startup.
			FilePath[] dirs = new FilePath[] { new FilePath(cluster.GetInstanceStorageDir(dnIndex
				, 0), "current"), new FilePath(cluster.GetInstanceStorageDir(dnIndex, 1), "current"
				) };
			try
			{
				for (int i = 0; i < volumesFailed; i++)
				{
					PrepareDirToFail(dirs[i]);
				}
				RestartDatanodes(volumesTolerated, manageDfsDirs);
				NUnit.Framework.Assert.AreEqual(expectedBPServiceState, cluster.GetDataNodes()[0]
					.IsBPServiceAlive(cluster.GetNamesystem().GetBlockPoolId()));
			}
			finally
			{
				foreach (FilePath dir in dirs)
				{
					FileUtil.Chmod(dir.ToString(), "755");
				}
			}
		}

		/// <summary>Prepare directories for a failure, set dir permission to 000</summary>
		/// <param name="dir"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void PrepareDirToFail(FilePath dir)
		{
			dir.Mkdirs();
			NUnit.Framework.Assert.AreEqual("Couldn't chmod local vol", 0, FileUtil.Chmod(dir
				.ToString(), "000"));
		}

		/// <summary>
		/// Test that a volume that is considered failed on startup is seen as
		/// a failed volume by the NN.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailedVolumeOnStartupIsCounted()
		{
			Assume.AssumeTrue(!Runtime.GetProperty("os.name").StartsWith("Windows"));
			DatanodeManager dm = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
				();
			long origCapacity = DFSTestUtil.GetLiveDatanodeCapacity(dm);
			FilePath dir = new FilePath(cluster.GetInstanceStorageDir(0, 0), "current");
			try
			{
				PrepareDirToFail(dir);
				RestartDatanodes(1, false);
				// The cluster is up..
				NUnit.Framework.Assert.AreEqual(true, cluster.GetDataNodes()[0].IsBPServiceAlive(
					cluster.GetNamesystem().GetBlockPoolId()));
				// but there has been a single volume failure
				DFSTestUtil.WaitForDatanodeStatus(dm, 1, 0, 1, origCapacity / 2, WaitForHeartbeats
					);
			}
			finally
			{
				FileUtil.Chmod(dir.ToString(), "755");
			}
		}
	}
}
