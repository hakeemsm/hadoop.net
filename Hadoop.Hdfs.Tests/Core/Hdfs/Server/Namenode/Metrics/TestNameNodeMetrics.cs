using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics
{
	/// <summary>Test for metrics published by the Namenode</summary>
	public class TestNameNodeMetrics
	{
		private static readonly Configuration Conf = new HdfsConfiguration();

		private const int DfsReplicationInterval = 1;

		private static readonly Path TestRootDirPath = new Path("/testNameNodeMetrics");

		private const string NnMetrics = "NameNodeActivity";

		private const string NsMetrics = "FSNamesystem";

		private const int DatanodeCount = 3;

		private const int WaitGaugeValueRetries = 20;

		private const int PercentilesInterval = 1;

		static TestNameNodeMetrics()
		{
			// Number of datanodes in the cluster
			// Rollover interval of percentile metrics (in seconds)
			Conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 100);
			Conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, 1);
			Conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, DfsReplicationInterval);
			Conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, DfsReplicationInterval
				);
			Conf.Set(DFSConfigKeys.DfsMetricsPercentilesIntervalsKey, string.Empty + PercentilesInterval
				);
			// Enable stale DataNodes checking
			Conf.SetBoolean(DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForReadKey, true);
			((Log4JLogger)LogFactory.GetLog(typeof(MetricsAsserts))).GetLogger().SetLevel(Level
				.Debug);
		}

		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		private readonly Random rand = new Random();

		private FSNamesystem namesystem;

		private BlockManager bm;

		private static Path GetTestPath(string fileName)
		{
			return new Path(TestRootDirPath, fileName);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(DatanodeCount).Build();
			cluster.WaitActive();
			namesystem = cluster.GetNamesystem();
			bm = namesystem.GetBlockManager();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			MetricsSource source = DefaultMetricsSystem.Instance().GetSource("UgiMetrics");
			if (source != null)
			{
				// Run only once since the UGI metrics is cleaned up during teardown
				MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(source);
				MetricsAsserts.AssertQuantileGauges("GetGroups1s", rb);
			}
			cluster.Shutdown();
		}

		/// <summary>create a file with a length of <code>fileLen</code></summary>
		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(Path file, long fileLen, short replicas)
		{
			DFSTestUtil.CreateFile(fs, file, fileLen, replicas, rand.NextLong());
		}

		/// <exception cref="System.Exception"/>
		private void UpdateMetrics()
		{
			// Wait for metrics update (corresponds to dfs.namenode.replication.interval
			// for some block related metrics to get updated)
			Sharpen.Thread.Sleep(1000);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadFile(FileSystem fileSys, Path name)
		{
			//Just read file so that getNumBlockLocations are incremented
			DataInputStream stm = fileSys.Open(name);
			byte[] buffer = new byte[4];
			stm.Read(buffer, 0, 4);
			stm.Close();
		}

		/// <summary>
		/// Test that capacity metrics are exported and pass
		/// basic sanity tests.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCapacityMetrics()
		{
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(NsMetrics);
			long capacityTotal = MetricsAsserts.GetLongGauge("CapacityTotal", rb);
			System.Diagnostics.Debug.Assert((capacityTotal != 0));
			long capacityUsed = MetricsAsserts.GetLongGauge("CapacityUsed", rb);
			long capacityRemaining = MetricsAsserts.GetLongGauge("CapacityRemaining", rb);
			long capacityUsedNonDFS = MetricsAsserts.GetLongGauge("CapacityUsedNonDFS", rb);
			System.Diagnostics.Debug.Assert((capacityUsed + capacityRemaining + capacityUsedNonDFS
				 == capacityTotal));
		}

		/// <summary>Test metrics indicating the number of stale DataNodes</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStaleNodes()
		{
			// Set two datanodes as stale
			for (int i = 0; i < 2; i++)
			{
				DataNode dn = cluster.GetDataNodes()[i];
				DataNodeTestUtils.SetHeartbeatsDisabledForTests(dn, true);
				long staleInterval = Conf.GetLong(DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalKey
					, DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalDefault);
				DatanodeDescriptor dnDes = cluster.GetNameNode().GetNamesystem().GetBlockManager(
					).GetDatanodeManager().GetDatanode(dn.GetDatanodeId());
				DFSTestUtil.ResetLastUpdatesWithOffset(dnDes, -(staleInterval + 1));
			}
			// Let HeartbeatManager to check heartbeat
			BlockManagerTestUtil.CheckHeartbeat(cluster.GetNameNode().GetNamesystem().GetBlockManager
				());
			MetricsAsserts.AssertGauge("StaleDataNodes", 2, MetricsAsserts.GetMetrics(NsMetrics
				));
			// Reset stale datanodes
			for (int i_1 = 0; i_1 < 2; i_1++)
			{
				DataNode dn = cluster.GetDataNodes()[i_1];
				DataNodeTestUtils.SetHeartbeatsDisabledForTests(dn, false);
				DatanodeDescriptor dnDes = cluster.GetNameNode().GetNamesystem().GetBlockManager(
					).GetDatanodeManager().GetDatanode(dn.GetDatanodeId());
				DFSTestUtil.ResetLastUpdatesWithOffset(dnDes, 0);
			}
			// Let HeartbeatManager to refresh
			BlockManagerTestUtil.CheckHeartbeat(cluster.GetNameNode().GetNamesystem().GetBlockManager
				());
			MetricsAsserts.AssertGauge("StaleDataNodes", 0, MetricsAsserts.GetMetrics(NsMetrics
				));
		}

		/// <summary>Test metrics associated with addition of a file</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFileAdd()
		{
			// Add files with 100 blocks
			Path file = GetTestPath("testFileAdd");
			CreateFile(file, 3200, (short)3);
			long blockCount = 32;
			int blockCapacity = namesystem.GetBlockCapacity();
			UpdateMetrics();
			MetricsAsserts.AssertGauge("BlockCapacity", blockCapacity, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(NnMetrics);
			// File create operations is 1
			// Number of files created is depth of <code>file</code> path
			MetricsAsserts.AssertCounter("CreateFileOps", 1L, rb);
			MetricsAsserts.AssertCounter("FilesCreated", (long)file.Depth(), rb);
			UpdateMetrics();
			long filesTotal = file.Depth() + 1;
			// Add 1 for root
			rb = MetricsAsserts.GetMetrics(NsMetrics);
			MetricsAsserts.AssertGauge("FilesTotal", filesTotal, rb);
			MetricsAsserts.AssertGauge("BlocksTotal", blockCount, rb);
			fs.Delete(file, true);
			filesTotal--;
			// reduce the filecount for deleted file
			rb = WaitForDnMetricValue(NsMetrics, "FilesTotal", filesTotal);
			MetricsAsserts.AssertGauge("BlocksTotal", 0L, rb);
			MetricsAsserts.AssertGauge("PendingDeletionBlocks", 0L, rb);
			rb = MetricsAsserts.GetMetrics(NnMetrics);
			// Delete file operations and number of files deleted must be 1
			MetricsAsserts.AssertCounter("DeleteFileOps", 1L, rb);
			MetricsAsserts.AssertCounter("FilesDeleted", 1L, rb);
		}

		/// <summary>Corrupt a block and ensure metrics reflects it</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCorruptBlock()
		{
			// Create a file with single block with two replicas
			Path file = GetTestPath("testCorruptBlock");
			CreateFile(file, 100, (short)2);
			// Corrupt first replica of the block
			LocatedBlock block = NameNodeAdapter.GetBlockLocations(cluster.GetNameNode(), file
				.ToString(), 0, 1).Get(0);
			cluster.GetNamesystem().WriteLock();
			try
			{
				bm.FindAndMarkBlockAsCorrupt(block.GetBlock(), block.GetLocations()[0], "STORAGE_ID"
					, "TEST");
			}
			finally
			{
				cluster.GetNamesystem().WriteUnlock();
			}
			UpdateMetrics();
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(NsMetrics);
			MetricsAsserts.AssertGauge("CorruptBlocks", 1L, rb);
			MetricsAsserts.AssertGauge("PendingReplicationBlocks", 1L, rb);
			MetricsAsserts.AssertGauge("ScheduledReplicationBlocks", 1L, rb);
			fs.Delete(file, true);
			rb = WaitForDnMetricValue(NsMetrics, "CorruptBlocks", 0L);
			MetricsAsserts.AssertGauge("PendingReplicationBlocks", 0L, rb);
			MetricsAsserts.AssertGauge("ScheduledReplicationBlocks", 0L, rb);
		}

		/// <summary>
		/// Create excess blocks by reducing the replication factor for
		/// for a file and ensure metrics reflects it
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExcessBlocks()
		{
			Path file = GetTestPath("testExcessBlocks");
			CreateFile(file, 100, (short)2);
			NameNodeAdapter.SetReplication(namesystem, file.ToString(), (short)1);
			UpdateMetrics();
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(NsMetrics);
			MetricsAsserts.AssertGauge("ExcessBlocks", 1L, rb);
			// verify ExcessBlocks metric is decremented and
			// excessReplicateMap is cleared after deleting a file
			fs.Delete(file, true);
			rb = MetricsAsserts.GetMetrics(NsMetrics);
			MetricsAsserts.AssertGauge("ExcessBlocks", 0L, rb);
			NUnit.Framework.Assert.IsTrue(bm.excessReplicateMap.IsEmpty());
		}

		/// <summary>Test to ensure metrics reflects missing blocks</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMissingBlock()
		{
			// Create a file with single block with two replicas
			Path file = GetTestPath("testMissingBlocks");
			CreateFile(file, 100, (short)1);
			// Corrupt the only replica of the block to result in a missing block
			LocatedBlock block = NameNodeAdapter.GetBlockLocations(cluster.GetNameNode(), file
				.ToString(), 0, 1).Get(0);
			cluster.GetNamesystem().WriteLock();
			try
			{
				bm.FindAndMarkBlockAsCorrupt(block.GetBlock(), block.GetLocations()[0], "STORAGE_ID"
					, "TEST");
			}
			finally
			{
				cluster.GetNamesystem().WriteUnlock();
			}
			UpdateMetrics();
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(NsMetrics);
			MetricsAsserts.AssertGauge("UnderReplicatedBlocks", 1L, rb);
			MetricsAsserts.AssertGauge("MissingBlocks", 1L, rb);
			MetricsAsserts.AssertGauge("MissingReplOneBlocks", 1L, rb);
			fs.Delete(file, true);
			WaitForDnMetricValue(NsMetrics, "UnderReplicatedBlocks", 0L);
		}

		/// <exception cref="System.Exception"/>
		private void WaitForDeletion()
		{
			// Wait for more than DATANODE_COUNT replication intervals to ensure all
			// the blocks pending deletion are sent for deletion to the datanodes.
			Sharpen.Thread.Sleep(DfsReplicationInterval * (DatanodeCount + 1) * 1000);
		}

		/// <summary>
		/// Wait for the named gauge value from the metrics source to reach the
		/// desired value.
		/// </summary>
		/// <remarks>
		/// Wait for the named gauge value from the metrics source to reach the
		/// desired value.
		/// There's an initial delay then a spin cycle of sleep and poll. Because
		/// all the tests use a shared FS instance, these tests are not independent;
		/// that's why the initial sleep is in there.
		/// </remarks>
		/// <param name="source">metrics source</param>
		/// <param name="name">gauge name</param>
		/// <param name="expected">expected value</param>
		/// <returns>the last metrics record polled</returns>
		/// <exception cref="System.Exception">if something went wrong.</exception>
		private MetricsRecordBuilder WaitForDnMetricValue(string source, string name, long
			 expected)
		{
			MetricsRecordBuilder rb;
			long gauge;
			//initial wait.
			WaitForDeletion();
			//lots of retries are allowed for slow systems; fast ones will still
			//exit early
			int retries = (DatanodeCount + 1) * WaitGaugeValueRetries;
			rb = MetricsAsserts.GetMetrics(source);
			gauge = MetricsAsserts.GetLongGauge(name, rb);
			while (gauge != expected && (--retries > 0))
			{
				Sharpen.Thread.Sleep(DfsReplicationInterval * 500);
				rb = MetricsAsserts.GetMetrics(source);
				gauge = MetricsAsserts.GetLongGauge(name, rb);
			}
			//at this point the assertion is valid or the retry count ran out
			MetricsAsserts.AssertGauge(name, expected, rb);
			return rb;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameMetrics()
		{
			Path src = GetTestPath("src");
			CreateFile(src, 100, (short)1);
			Path target = GetTestPath("target");
			CreateFile(target, 100, (short)1);
			fs.Rename(src, target, Options.Rename.Overwrite);
			UpdateMetrics();
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(NnMetrics);
			MetricsAsserts.AssertCounter("FilesRenamed", 1L, rb);
			MetricsAsserts.AssertCounter("FilesDeleted", 1L, rb);
		}

		/// <summary>
		/// Test numGetBlockLocations metric
		/// Test initiates and performs file operations (create,read,close,open file )
		/// which results in metrics changes.
		/// </summary>
		/// <remarks>
		/// Test numGetBlockLocations metric
		/// Test initiates and performs file operations (create,read,close,open file )
		/// which results in metrics changes. These metrics changes are updated and
		/// tested for correctness.
		/// create file operation does not increment numGetBlockLocation
		/// one read file operation increments numGetBlockLocation by 1
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetBlockLocationMetric()
		{
			Path file1_Path = new Path(TestRootDirPath, "file1.dat");
			// When cluster starts first time there are no file  (read,create,open)
			// operations so metric GetBlockLocations should be 0.
			MetricsAsserts.AssertCounter("GetBlockLocations", 0L, MetricsAsserts.GetMetrics(NnMetrics
				));
			//Perform create file operation
			CreateFile(file1_Path, 100, (short)2);
			UpdateMetrics();
			//Create file does not change numGetBlockLocations metric
			//expect numGetBlockLocations = 0 for previous and current interval 
			MetricsAsserts.AssertCounter("GetBlockLocations", 0L, MetricsAsserts.GetMetrics(NnMetrics
				));
			// Open and read file operation increments GetBlockLocations
			// Perform read file operation on earlier created file
			ReadFile(fs, file1_Path);
			UpdateMetrics();
			// Verify read file operation has incremented numGetBlockLocations by 1
			MetricsAsserts.AssertCounter("GetBlockLocations", 1L, MetricsAsserts.GetMetrics(NnMetrics
				));
			// opening and reading file  twice will increment numGetBlockLocations by 2
			ReadFile(fs, file1_Path);
			ReadFile(fs, file1_Path);
			UpdateMetrics();
			MetricsAsserts.AssertCounter("GetBlockLocations", 3L, MetricsAsserts.GetMetrics(NnMetrics
				));
		}

		/// <summary>Test NN checkpoint and transaction-related metrics.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTransactionAndCheckpointMetrics()
		{
			long lastCkptTime = MetricsAsserts.GetLongGauge("LastCheckpointTime", MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("LastCheckpointTime", lastCkptTime, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("LastWrittenTransactionId", 1L, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("TransactionsSinceLastCheckpoint", 1L, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("TransactionsSinceLastLogRoll", 1L, MetricsAsserts.GetMetrics
				(NsMetrics));
			fs.Mkdirs(new Path(TestRootDirPath, "/tmp"));
			UpdateMetrics();
			MetricsAsserts.AssertGauge("LastCheckpointTime", lastCkptTime, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("LastWrittenTransactionId", 2L, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("TransactionsSinceLastCheckpoint", 2L, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("TransactionsSinceLastLogRoll", 2L, MetricsAsserts.GetMetrics
				(NsMetrics));
			cluster.GetNameNodeRpc().RollEditLog();
			UpdateMetrics();
			MetricsAsserts.AssertGauge("LastCheckpointTime", lastCkptTime, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("LastWrittenTransactionId", 4L, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("TransactionsSinceLastCheckpoint", 4L, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("TransactionsSinceLastLogRoll", 1L, MetricsAsserts.GetMetrics
				(NsMetrics));
			cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, 
				false);
			cluster.GetNameNodeRpc().SaveNamespace();
			cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, 
				false);
			UpdateMetrics();
			long newLastCkptTime = MetricsAsserts.GetLongGauge("LastCheckpointTime", MetricsAsserts.GetMetrics
				(NsMetrics));
			NUnit.Framework.Assert.IsTrue(lastCkptTime < newLastCkptTime);
			MetricsAsserts.AssertGauge("LastWrittenTransactionId", 6L, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("TransactionsSinceLastCheckpoint", 1L, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertGauge("TransactionsSinceLastLogRoll", 1L, MetricsAsserts.GetMetrics
				(NsMetrics));
		}

		/// <summary>
		/// Tests that the sync and block report metrics get updated on cluster
		/// startup.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSyncAndBlockReportMetric()
		{
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(NnMetrics);
			// We have one sync when the cluster starts up, just opening the journal
			MetricsAsserts.AssertCounter("SyncsNumOps", 1L, rb);
			// Each datanode reports in when the cluster comes up
			MetricsAsserts.AssertCounter("BlockReportNumOps", (long)DatanodeCount * cluster.GetStoragesPerDatanode
				(), rb);
			// Sleep for an interval+slop to let the percentiles rollover
			Sharpen.Thread.Sleep((PercentilesInterval + 1) * 1000);
			// Check that the percentiles were updated
			MetricsAsserts.AssertQuantileGauges("Syncs1s", rb);
			MetricsAsserts.AssertQuantileGauges("BlockReport1s", rb);
		}

		/// <summary>Test NN ReadOps Count and WriteOps Count</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReadWriteOps()
		{
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(NnMetrics);
			long startWriteCounter = MetricsAsserts.GetLongCounter("TransactionsNumOps", rb);
			Path file1_Path = new Path(TestRootDirPath, "ReadData.dat");
			//Perform create file operation
			CreateFile(file1_Path, 1024 * 1024, (short)2);
			// Perform read file operation on earlier created file
			ReadFile(fs, file1_Path);
			MetricsRecordBuilder rbNew = MetricsAsserts.GetMetrics(NnMetrics);
			NUnit.Framework.Assert.IsTrue(MetricsAsserts.GetLongCounter("TransactionsNumOps", 
				rbNew) > startWriteCounter);
		}
	}
}
