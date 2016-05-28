using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Test reporting of DN volume failure counts and metrics.</summary>
	public class TestDataNodeVolumeFailureReporting
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDataNodeVolumeFailureReporting
			));

		private FileSystem fs;

		private MiniDFSCluster cluster;

		private Configuration conf;

		private string dataDir;

		private long volumeCapacity;

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
			// These tests simulate volume failures by denying execute permission on the
			// volume's path.  On Windows, the owner of an object is always allowed
			// access, so we can't run these tests on Windows.
			Assume.AssumeTrue(!Path.Windows);
			// Allow a single volume failure (there are two volumes)
			InitCluster(1, 2, 1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			IOUtils.Cleanup(Log, fs);
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test that individual volume failures do not cause DNs to fail, that
		/// all volumes failed on a single datanode do cause it to fail, and
		/// that the capacities and liveliness is adjusted correctly in the NN.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSuccessiveVolumeFailures()
		{
			// Bring up two more datanodes
			cluster.StartDataNodes(conf, 2, true, null, null);
			cluster.WaitActive();
			/*
			* Calculate the total capacity of all the datanodes. Sleep for
			* three seconds to be sure the datanodes have had a chance to
			* heartbeat their capacities.
			*/
			Sharpen.Thread.Sleep(WaitForHeartbeats);
			DatanodeManager dm = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
				();
			long origCapacity = DFSTestUtil.GetLiveDatanodeCapacity(dm);
			long dnCapacity = DFSTestUtil.GetDatanodeCapacity(dm, 0);
			FilePath dn1Vol1 = new FilePath(dataDir, "data" + (2 * 0 + 1));
			FilePath dn2Vol1 = new FilePath(dataDir, "data" + (2 * 1 + 1));
			FilePath dn3Vol1 = new FilePath(dataDir, "data" + (2 * 2 + 1));
			FilePath dn3Vol2 = new FilePath(dataDir, "data" + (2 * 2 + 2));
			/*
			* Make the 1st volume directories on the first two datanodes
			* non-accessible.  We don't make all three 1st volume directories
			* readonly since that would cause the entire pipeline to
			* fail. The client does not retry failed nodes even though
			* perhaps they could succeed because just a single volume failed.
			*/
			DataNodeTestUtils.InjectDataDirFailure(dn1Vol1, dn2Vol1);
			/*
			* Create file1 and wait for 3 replicas (ie all DNs can still
			* store a block).  Then assert that all DNs are up, despite the
			* volume failures.
			*/
			Path file1 = new Path("/test1");
			DFSTestUtil.CreateFile(fs, file1, 1024, (short)3, 1L);
			DFSTestUtil.WaitReplication(fs, file1, (short)3);
			AList<DataNode> dns = cluster.GetDataNodes();
			NUnit.Framework.Assert.IsTrue("DN1 should be up", dns[0].IsDatanodeUp());
			NUnit.Framework.Assert.IsTrue("DN2 should be up", dns[1].IsDatanodeUp());
			NUnit.Framework.Assert.IsTrue("DN3 should be up", dns[2].IsDatanodeUp());
			/*
			* The metrics should confirm the volume failures.
			*/
			CheckFailuresAtDataNode(dns[0], 1, true, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtDataNode(dns[1], 1, true, dn2Vol1.GetAbsolutePath());
			CheckFailuresAtDataNode(dns[2], 0, true);
			// Ensure we wait a sufficient amount of time
			System.Diagnostics.Debug.Assert((WaitForHeartbeats * 10) > WaitForDeath);
			// Eventually the NN should report two volume failures
			DFSTestUtil.WaitForDatanodeStatus(dm, 3, 0, 2, origCapacity - (1 * dnCapacity), WaitForHeartbeats
				);
			CheckAggregateFailuresAtNameNode(true, 2);
			CheckFailuresAtNameNode(dm, dns[0], true, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtNameNode(dm, dns[1], true, dn2Vol1.GetAbsolutePath());
			CheckFailuresAtNameNode(dm, dns[2], true);
			/*
			* Now fail a volume on the third datanode. We should be able to get
			* three replicas since we've already identified the other failures.
			*/
			DataNodeTestUtils.InjectDataDirFailure(dn3Vol1);
			Path file2 = new Path("/test2");
			DFSTestUtil.CreateFile(fs, file2, 1024, (short)3, 1L);
			DFSTestUtil.WaitReplication(fs, file2, (short)3);
			NUnit.Framework.Assert.IsTrue("DN3 should still be up", dns[2].IsDatanodeUp());
			CheckFailuresAtDataNode(dns[2], 1, true, dn3Vol1.GetAbsolutePath());
			DataNodeTestUtils.TriggerHeartbeat(dns[2]);
			CheckFailuresAtNameNode(dm, dns[2], true, dn3Vol1.GetAbsolutePath());
			/*
			* Once the datanodes have a chance to heartbeat their new capacity the
			* total capacity should be down by three volumes (assuming the host
			* did not grow or shrink the data volume while the test was running).
			*/
			dnCapacity = DFSTestUtil.GetDatanodeCapacity(dm, 0);
			DFSTestUtil.WaitForDatanodeStatus(dm, 3, 0, 3, origCapacity - (3 * dnCapacity), WaitForHeartbeats
				);
			CheckAggregateFailuresAtNameNode(true, 3);
			CheckFailuresAtNameNode(dm, dns[0], true, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtNameNode(dm, dns[1], true, dn2Vol1.GetAbsolutePath());
			CheckFailuresAtNameNode(dm, dns[2], true, dn3Vol1.GetAbsolutePath());
			/*
			* Now fail the 2nd volume on the 3rd datanode. All its volumes
			* are now failed and so it should report two volume failures
			* and that it's no longer up. Only wait for two replicas since
			* we'll never get a third.
			*/
			DataNodeTestUtils.InjectDataDirFailure(dn3Vol2);
			Path file3 = new Path("/test3");
			DFSTestUtil.CreateFile(fs, file3, 1024, (short)3, 1L);
			DFSTestUtil.WaitReplication(fs, file3, (short)2);
			// The DN should consider itself dead
			DFSTestUtil.WaitForDatanodeDeath(dns[2]);
			// And report two failed volumes
			CheckFailuresAtDataNode(dns[2], 2, true, dn3Vol1.GetAbsolutePath(), dn3Vol2.GetAbsolutePath
				());
			// The NN considers the DN dead
			DFSTestUtil.WaitForDatanodeStatus(dm, 2, 1, 2, origCapacity - (4 * dnCapacity), WaitForHeartbeats
				);
			CheckAggregateFailuresAtNameNode(true, 2);
			CheckFailuresAtNameNode(dm, dns[0], true, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtNameNode(dm, dns[1], true, dn2Vol1.GetAbsolutePath());
			/*
			* The datanode never tries to restore the failed volume, even if
			* it's subsequently repaired, but it should see this volume on
			* restart, so file creation should be able to succeed after
			* restoring the data directories and restarting the datanodes.
			*/
			DataNodeTestUtils.RestoreDataDirFromFailure(dn1Vol1, dn2Vol1, dn3Vol1, dn3Vol2);
			cluster.RestartDataNodes();
			cluster.WaitActive();
			Path file4 = new Path("/test4");
			DFSTestUtil.CreateFile(fs, file4, 1024, (short)3, 1L);
			DFSTestUtil.WaitReplication(fs, file4, (short)3);
			/*
			* Eventually the capacity should be restored to its original value,
			* and that the volume failure count should be reported as zero by
			* both the metrics and the NN.
			*/
			DFSTestUtil.WaitForDatanodeStatus(dm, 3, 0, 0, origCapacity, WaitForHeartbeats);
			CheckAggregateFailuresAtNameNode(true, 0);
			dns = cluster.GetDataNodes();
			CheckFailuresAtNameNode(dm, dns[0], true);
			CheckFailuresAtNameNode(dm, dns[1], true);
			CheckFailuresAtNameNode(dm, dns[2], true);
		}

		/// <summary>Test that the NN re-learns of volume failures after restart.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVolFailureStatsPreservedOnNNRestart()
		{
			// Bring up two more datanodes that can tolerate 1 failure
			cluster.StartDataNodes(conf, 2, true, null, null);
			cluster.WaitActive();
			DatanodeManager dm = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
				();
			long origCapacity = DFSTestUtil.GetLiveDatanodeCapacity(dm);
			long dnCapacity = DFSTestUtil.GetDatanodeCapacity(dm, 0);
			// Fail the first volume on both datanodes (we have to keep the 
			// third healthy so one node in the pipeline will not fail). 
			FilePath dn1Vol1 = new FilePath(dataDir, "data" + (2 * 0 + 1));
			FilePath dn2Vol1 = new FilePath(dataDir, "data" + (2 * 1 + 1));
			DataNodeTestUtils.InjectDataDirFailure(dn1Vol1, dn2Vol1);
			Path file1 = new Path("/test1");
			DFSTestUtil.CreateFile(fs, file1, 1024, (short)2, 1L);
			DFSTestUtil.WaitReplication(fs, file1, (short)2);
			AList<DataNode> dns = cluster.GetDataNodes();
			// The NN reports two volumes failures
			DFSTestUtil.WaitForDatanodeStatus(dm, 3, 0, 2, origCapacity - (1 * dnCapacity), WaitForHeartbeats
				);
			CheckAggregateFailuresAtNameNode(true, 2);
			CheckFailuresAtNameNode(dm, dns[0], true, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtNameNode(dm, dns[1], true, dn2Vol1.GetAbsolutePath());
			// After restarting the NN it still see the two failures
			cluster.RestartNameNode(0);
			cluster.WaitActive();
			DFSTestUtil.WaitForDatanodeStatus(dm, 3, 0, 2, origCapacity - (1 * dnCapacity), WaitForHeartbeats
				);
			CheckAggregateFailuresAtNameNode(true, 2);
			CheckFailuresAtNameNode(dm, dns[0], true, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtNameNode(dm, dns[1], true, dn2Vol1.GetAbsolutePath());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleVolFailuresOnNode()
		{
			// Reinitialize the cluster, configured with 4 storage locations per DataNode
			// and tolerating up to 2 failures.
			TearDown();
			InitCluster(3, 4, 2);
			// Calculate the total capacity of all the datanodes. Sleep for three seconds
			// to be sure the datanodes have had a chance to heartbeat their capacities.
			Sharpen.Thread.Sleep(WaitForHeartbeats);
			DatanodeManager dm = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
				();
			long origCapacity = DFSTestUtil.GetLiveDatanodeCapacity(dm);
			long dnCapacity = DFSTestUtil.GetDatanodeCapacity(dm, 0);
			FilePath dn1Vol1 = new FilePath(dataDir, "data" + (4 * 0 + 1));
			FilePath dn1Vol2 = new FilePath(dataDir, "data" + (4 * 0 + 2));
			FilePath dn2Vol1 = new FilePath(dataDir, "data" + (4 * 1 + 1));
			FilePath dn2Vol2 = new FilePath(dataDir, "data" + (4 * 1 + 2));
			// Make the first two volume directories on the first two datanodes
			// non-accessible.
			DataNodeTestUtils.InjectDataDirFailure(dn1Vol1, dn1Vol2, dn2Vol1, dn2Vol2);
			// Create file1 and wait for 3 replicas (ie all DNs can still store a block).
			// Then assert that all DNs are up, despite the volume failures.
			Path file1 = new Path("/test1");
			DFSTestUtil.CreateFile(fs, file1, 1024, (short)3, 1L);
			DFSTestUtil.WaitReplication(fs, file1, (short)3);
			AList<DataNode> dns = cluster.GetDataNodes();
			NUnit.Framework.Assert.IsTrue("DN1 should be up", dns[0].IsDatanodeUp());
			NUnit.Framework.Assert.IsTrue("DN2 should be up", dns[1].IsDatanodeUp());
			NUnit.Framework.Assert.IsTrue("DN3 should be up", dns[2].IsDatanodeUp());
			CheckFailuresAtDataNode(dns[0], 1, true, dn1Vol1.GetAbsolutePath(), dn1Vol2.GetAbsolutePath
				());
			CheckFailuresAtDataNode(dns[1], 1, true, dn2Vol1.GetAbsolutePath(), dn2Vol2.GetAbsolutePath
				());
			CheckFailuresAtDataNode(dns[2], 0, true);
			// Ensure we wait a sufficient amount of time
			System.Diagnostics.Debug.Assert((WaitForHeartbeats * 10) > WaitForDeath);
			// Eventually the NN should report four volume failures
			DFSTestUtil.WaitForDatanodeStatus(dm, 3, 0, 4, origCapacity - (1 * dnCapacity), WaitForHeartbeats
				);
			CheckAggregateFailuresAtNameNode(true, 4);
			CheckFailuresAtNameNode(dm, dns[0], true, dn1Vol1.GetAbsolutePath(), dn1Vol2.GetAbsolutePath
				());
			CheckFailuresAtNameNode(dm, dns[1], true, dn2Vol1.GetAbsolutePath(), dn2Vol2.GetAbsolutePath
				());
			CheckFailuresAtNameNode(dm, dns[2], true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDataNodeReconfigureWithVolumeFailures()
		{
			// Bring up two more datanodes
			cluster.StartDataNodes(conf, 2, true, null, null);
			cluster.WaitActive();
			DatanodeManager dm = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
				();
			long origCapacity = DFSTestUtil.GetLiveDatanodeCapacity(dm);
			long dnCapacity = DFSTestUtil.GetDatanodeCapacity(dm, 0);
			// Fail the first volume on both datanodes (we have to keep the
			// third healthy so one node in the pipeline will not fail).
			FilePath dn1Vol1 = new FilePath(dataDir, "data" + (2 * 0 + 1));
			FilePath dn1Vol2 = new FilePath(dataDir, "data" + (2 * 0 + 2));
			FilePath dn2Vol1 = new FilePath(dataDir, "data" + (2 * 1 + 1));
			FilePath dn2Vol2 = new FilePath(dataDir, "data" + (2 * 1 + 2));
			DataNodeTestUtils.InjectDataDirFailure(dn1Vol1);
			DataNodeTestUtils.InjectDataDirFailure(dn2Vol1);
			Path file1 = new Path("/test1");
			DFSTestUtil.CreateFile(fs, file1, 1024, (short)2, 1L);
			DFSTestUtil.WaitReplication(fs, file1, (short)2);
			AList<DataNode> dns = cluster.GetDataNodes();
			NUnit.Framework.Assert.IsTrue("DN1 should be up", dns[0].IsDatanodeUp());
			NUnit.Framework.Assert.IsTrue("DN2 should be up", dns[1].IsDatanodeUp());
			NUnit.Framework.Assert.IsTrue("DN3 should be up", dns[2].IsDatanodeUp());
			CheckFailuresAtDataNode(dns[0], 1, true, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtDataNode(dns[1], 1, true, dn2Vol1.GetAbsolutePath());
			CheckFailuresAtDataNode(dns[2], 0, true);
			// Ensure we wait a sufficient amount of time
			System.Diagnostics.Debug.Assert((WaitForHeartbeats * 10) > WaitForDeath);
			// The NN reports two volume failures
			DFSTestUtil.WaitForDatanodeStatus(dm, 3, 0, 2, origCapacity - (1 * dnCapacity), WaitForHeartbeats
				);
			CheckAggregateFailuresAtNameNode(true, 2);
			CheckFailuresAtNameNode(dm, dns[0], true, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtNameNode(dm, dns[1], true, dn2Vol1.GetAbsolutePath());
			// Reconfigure again to try to add back the failed volumes.
			ReconfigureDataNode(dns[0], dn1Vol1, dn1Vol2);
			ReconfigureDataNode(dns[1], dn2Vol1, dn2Vol2);
			DataNodeTestUtils.TriggerHeartbeat(dns[0]);
			DataNodeTestUtils.TriggerHeartbeat(dns[1]);
			CheckFailuresAtDataNode(dns[0], 1, false, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtDataNode(dns[1], 1, false, dn2Vol1.GetAbsolutePath());
			// Ensure we wait a sufficient amount of time.
			System.Diagnostics.Debug.Assert((WaitForHeartbeats * 10) > WaitForDeath);
			// The NN reports two volume failures again.
			DFSTestUtil.WaitForDatanodeStatus(dm, 3, 0, 2, origCapacity - (1 * dnCapacity), WaitForHeartbeats
				);
			CheckAggregateFailuresAtNameNode(false, 2);
			CheckFailuresAtNameNode(dm, dns[0], false, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtNameNode(dm, dns[1], false, dn2Vol1.GetAbsolutePath());
			// Reconfigure a third time with the failed volumes.  Afterwards, we expect
			// the same volume failures to be reported.  (No double-counting.)
			ReconfigureDataNode(dns[0], dn1Vol1, dn1Vol2);
			ReconfigureDataNode(dns[1], dn2Vol1, dn2Vol2);
			DataNodeTestUtils.TriggerHeartbeat(dns[0]);
			DataNodeTestUtils.TriggerHeartbeat(dns[1]);
			CheckFailuresAtDataNode(dns[0], 1, false, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtDataNode(dns[1], 1, false, dn2Vol1.GetAbsolutePath());
			// Ensure we wait a sufficient amount of time.
			System.Diagnostics.Debug.Assert((WaitForHeartbeats * 10) > WaitForDeath);
			// The NN reports two volume failures again.
			DFSTestUtil.WaitForDatanodeStatus(dm, 3, 0, 2, origCapacity - (1 * dnCapacity), WaitForHeartbeats
				);
			CheckAggregateFailuresAtNameNode(false, 2);
			CheckFailuresAtNameNode(dm, dns[0], false, dn1Vol1.GetAbsolutePath());
			CheckFailuresAtNameNode(dm, dns[1], false, dn2Vol1.GetAbsolutePath());
			// Replace failed volume with healthy volume and run reconfigure DataNode.
			// The failed volume information should be cleared.
			DataNodeTestUtils.RestoreDataDirFromFailure(dn1Vol1, dn2Vol1);
			ReconfigureDataNode(dns[0], dn1Vol1, dn1Vol2);
			ReconfigureDataNode(dns[1], dn2Vol1, dn2Vol2);
			DataNodeTestUtils.TriggerHeartbeat(dns[0]);
			DataNodeTestUtils.TriggerHeartbeat(dns[1]);
			CheckFailuresAtDataNode(dns[0], 1, true);
			CheckFailuresAtDataNode(dns[1], 1, true);
			DFSTestUtil.WaitForDatanodeStatus(dm, 3, 0, 0, origCapacity, WaitForHeartbeats);
			CheckAggregateFailuresAtNameNode(true, 0);
			CheckFailuresAtNameNode(dm, dns[0], true);
			CheckFailuresAtNameNode(dm, dns[1], true);
		}

		/// <summary>
		/// Checks the NameNode for correct values of aggregate counters tracking failed
		/// volumes across all DataNodes.
		/// </summary>
		/// <param name="expectCapacityKnown">
		/// if true, then expect that the capacities of the
		/// volumes were known before the failures, and therefore the lost capacity
		/// can be reported
		/// </param>
		/// <param name="expectedVolumeFailuresTotal">expected number of failed volumes</param>
		private void CheckAggregateFailuresAtNameNode(bool expectCapacityKnown, int expectedVolumeFailuresTotal
			)
		{
			FSNamesystem ns = cluster.GetNamesystem();
			NUnit.Framework.Assert.AreEqual(expectedVolumeFailuresTotal, ns.GetVolumeFailuresTotal
				());
			long expectedCapacityLost = GetExpectedCapacityLost(expectCapacityKnown, expectedVolumeFailuresTotal
				);
			NUnit.Framework.Assert.AreEqual(expectedCapacityLost, ns.GetEstimatedCapacityLostTotal
				());
		}

		/// <summary>Checks a DataNode for correct reporting of failed volumes.</summary>
		/// <param name="dn">DataNode to check</param>
		/// <param name="expectedVolumeFailuresCounter">
		/// metric counter value for
		/// VolumeFailures.  The current implementation actually counts the number
		/// of failed disk checker cycles, which may be different from the length of
		/// expectedFailedVolumes if multiple disks fail in the same disk checker
		/// cycle
		/// </param>
		/// <param name="expectCapacityKnown">
		/// if true, then expect that the capacities of the
		/// volumes were known before the failures, and therefore the lost capacity
		/// can be reported
		/// </param>
		/// <param name="expectedFailedVolumes">expected locations of failed volumes</param>
		/// <exception cref="System.Exception">if there is any failure</exception>
		private void CheckFailuresAtDataNode(DataNode dn, long expectedVolumeFailuresCounter
			, bool expectCapacityKnown, params string[] expectedFailedVolumes)
		{
			MetricsAsserts.AssertCounter("VolumeFailures", expectedVolumeFailuresCounter, MetricsAsserts.GetMetrics
				(dn.GetMetrics().Name()));
			FsDatasetSpi<object> fsd = dn.GetFSDataset();
			NUnit.Framework.Assert.AreEqual(expectedFailedVolumes.Length, fsd.GetNumFailedVolumes
				());
			Assert.AssertArrayEquals(expectedFailedVolumes, fsd.GetFailedStorageLocations());
			if (expectedFailedVolumes.Length > 0)
			{
				NUnit.Framework.Assert.IsTrue(fsd.GetLastVolumeFailureDate() > 0);
				long expectedCapacityLost = GetExpectedCapacityLost(expectCapacityKnown, expectedFailedVolumes
					.Length);
				NUnit.Framework.Assert.AreEqual(expectedCapacityLost, fsd.GetEstimatedCapacityLostTotal
					());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(0, fsd.GetLastVolumeFailureDate());
				NUnit.Framework.Assert.AreEqual(0, fsd.GetEstimatedCapacityLostTotal());
			}
		}

		/// <summary>
		/// Checks NameNode tracking of a particular DataNode for correct reporting of
		/// failed volumes.
		/// </summary>
		/// <param name="dm">DatanodeManager to check</param>
		/// <param name="dn">DataNode to check</param>
		/// <param name="expectCapacityKnown">
		/// if true, then expect that the capacities of the
		/// volumes were known before the failures, and therefore the lost capacity
		/// can be reported
		/// </param>
		/// <param name="expectedFailedVolumes">expected locations of failed volumes</param>
		/// <exception cref="System.Exception">if there is any failure</exception>
		private void CheckFailuresAtNameNode(DatanodeManager dm, DataNode dn, bool expectCapacityKnown
			, params string[] expectedFailedVolumes)
		{
			DatanodeDescriptor dd = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
				().GetDatanode(dn.GetDatanodeId());
			NUnit.Framework.Assert.AreEqual(expectedFailedVolumes.Length, dd.GetVolumeFailures
				());
			VolumeFailureSummary volumeFailureSummary = dd.GetVolumeFailureSummary();
			if (expectedFailedVolumes.Length > 0)
			{
				Assert.AssertArrayEquals(expectedFailedVolumes, volumeFailureSummary.GetFailedStorageLocations
					());
				NUnit.Framework.Assert.IsTrue(volumeFailureSummary.GetLastVolumeFailureDate() > 0
					);
				long expectedCapacityLost = GetExpectedCapacityLost(expectCapacityKnown, expectedFailedVolumes
					.Length);
				NUnit.Framework.Assert.AreEqual(expectedCapacityLost, volumeFailureSummary.GetEstimatedCapacityLostTotal
					());
			}
			else
			{
				NUnit.Framework.Assert.IsNull(volumeFailureSummary);
			}
		}

		/// <summary>Returns expected capacity lost for use in assertions.</summary>
		/// <remarks>
		/// Returns expected capacity lost for use in assertions.  The return value is
		/// dependent on whether or not it is expected that the volume capacities were
		/// known prior to the failures.
		/// </remarks>
		/// <param name="expectCapacityKnown">
		/// if true, then expect that the capacities of the
		/// volumes were known before the failures, and therefore the lost capacity
		/// can be reported
		/// </param>
		/// <param name="expectedVolumeFailuresTotal">expected number of failed volumes</param>
		/// <returns>estimated capacity lost in bytes</returns>
		private long GetExpectedCapacityLost(bool expectCapacityKnown, int expectedVolumeFailuresTotal
			)
		{
			return expectCapacityKnown ? expectedVolumeFailuresTotal * volumeCapacity : 0;
		}

		/// <summary>Initializes the cluster.</summary>
		/// <param name="numDataNodes">number of datanodes</param>
		/// <param name="storagesPerDatanode">number of storage locations on each datanode</param>
		/// <param name="failedVolumesTolerated">number of acceptable volume failures</param>
		/// <exception cref="System.Exception">if there is any failure</exception>
		private void InitCluster(int numDataNodes, int storagesPerDatanode, int failedVolumesTolerated
			)
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
			conf.SetInt(DFSConfigKeys.DfsDatanodeFailedVolumesToleratedKey, failedVolumesTolerated
				);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).StoragesPerDatanode
				(storagesPerDatanode).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			dataDir = cluster.GetDataDirectory();
			long dnCapacity = DFSTestUtil.GetDatanodeCapacity(cluster.GetNamesystem().GetBlockManager
				().GetDatanodeManager(), 0);
			volumeCapacity = dnCapacity / cluster.GetStoragesPerDatanode();
		}

		/// <summary>Reconfigure a DataNode by setting a new list of volumes.</summary>
		/// <param name="dn">DataNode to reconfigure</param>
		/// <param name="newVols">new volumes to configure</param>
		/// <exception cref="System.Exception">if there is any failure</exception>
		private static void ReconfigureDataNode(DataNode dn, params FilePath[] newVols)
		{
			StringBuilder dnNewDataDirs = new StringBuilder();
			foreach (FilePath newVol in newVols)
			{
				if (dnNewDataDirs.Length > 0)
				{
					dnNewDataDirs.Append(',');
				}
				dnNewDataDirs.Append(newVol.GetAbsolutePath());
			}
			try
			{
				dn.ReconfigurePropertyImpl(DFSConfigKeys.DfsDatanodeDataDirKey, dnNewDataDirs.ToString
					());
			}
			catch (ReconfigurationException e)
			{
				// This can be thrown if reconfiguration tries to use a failed volume.
				// We need to swallow the exception, because some of our tests want to
				// cover this case.
				Log.Warn("Could not reconfigure DataNode.", e);
			}
		}

		public TestDataNodeVolumeFailureReporting()
		{
			{
				((Log4JLogger)TestDataNodeVolumeFailureReporting.Log).GetLogger().SetLevel(Level.
					All);
			}
		}
	}
}
