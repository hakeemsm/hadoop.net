using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Test the snapshot-related metrics</summary>
	public class TestSnapshotMetrics
	{
		private const long seed = 0;

		private const short Replication = 3;

		private const string NnMetrics = "NameNodeActivity";

		private const string NsMetrics = "FSNamesystem";

		private readonly Path dir = new Path("/TestSnapshot");

		private readonly Path sub1;

		private readonly Path file1;

		private readonly Path file2;

		private Configuration conf;

		private MiniDFSCluster cluster;

		private DistributedFileSystem hdfs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			hdfs = cluster.GetFileSystem();
			DFSTestUtil.CreateFile(hdfs, file1, 1024, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file2, 1024, Replication, seed);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test the metric SnapshottableDirectories, AllowSnapshotOps,
		/// DisallowSnapshotOps, and listSnapshottableDirOps
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSnapshottableDirs()
		{
			cluster.GetNamesystem().GetSnapshotManager().SetAllowNestedSnapshots(true);
			MetricsAsserts.AssertGauge("SnapshottableDirectories", 0, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertCounter("AllowSnapshotOps", 0L, MetricsAsserts.GetMetrics(NnMetrics
				));
			MetricsAsserts.AssertCounter("DisallowSnapshotOps", 0L, MetricsAsserts.GetMetrics
				(NnMetrics));
			// Allow snapshots for directories, and check the metrics
			hdfs.AllowSnapshot(sub1);
			MetricsAsserts.AssertGauge("SnapshottableDirectories", 1, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertCounter("AllowSnapshotOps", 1L, MetricsAsserts.GetMetrics(NnMetrics
				));
			Path sub2 = new Path(dir, "sub2");
			Path file = new Path(sub2, "file");
			DFSTestUtil.CreateFile(hdfs, file, 1024, Replication, seed);
			hdfs.AllowSnapshot(sub2);
			MetricsAsserts.AssertGauge("SnapshottableDirectories", 2, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertCounter("AllowSnapshotOps", 2L, MetricsAsserts.GetMetrics(NnMetrics
				));
			Path subsub1 = new Path(sub1, "sub1sub1");
			Path subfile = new Path(subsub1, "file");
			DFSTestUtil.CreateFile(hdfs, subfile, 1024, Replication, seed);
			hdfs.AllowSnapshot(subsub1);
			MetricsAsserts.AssertGauge("SnapshottableDirectories", 3, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertCounter("AllowSnapshotOps", 3L, MetricsAsserts.GetMetrics(NnMetrics
				));
			// Set an already snapshottable directory to snapshottable, should not
			// change the metrics
			hdfs.AllowSnapshot(sub1);
			MetricsAsserts.AssertGauge("SnapshottableDirectories", 3, MetricsAsserts.GetMetrics
				(NsMetrics));
			// But the number of allowSnapshot operations still increases
			MetricsAsserts.AssertCounter("AllowSnapshotOps", 4L, MetricsAsserts.GetMetrics(NnMetrics
				));
			// Disallow the snapshot for snapshottable directories, then check the
			// metrics again
			hdfs.DisallowSnapshot(sub1);
			MetricsAsserts.AssertGauge("SnapshottableDirectories", 2, MetricsAsserts.GetMetrics
				(NsMetrics));
			MetricsAsserts.AssertCounter("DisallowSnapshotOps", 1L, MetricsAsserts.GetMetrics
				(NnMetrics));
			// delete subsub1, snapshottable directories should be 1
			hdfs.Delete(subsub1, true);
			MetricsAsserts.AssertGauge("SnapshottableDirectories", 1, MetricsAsserts.GetMetrics
				(NsMetrics));
			// list all the snapshottable directories
			SnapshottableDirectoryStatus[] status = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.AreEqual(1, status.Length);
			MetricsAsserts.AssertCounter("ListSnapshottableDirOps", 1L, MetricsAsserts.GetMetrics
				(NnMetrics));
		}

		/// <summary>
		/// Test the metrics Snapshots, CreateSnapshotOps, DeleteSnapshotOps,
		/// RenameSnapshotOps
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSnapshots()
		{
			cluster.GetNamesystem().GetSnapshotManager().SetAllowNestedSnapshots(true);
			MetricsAsserts.AssertGauge("Snapshots", 0, MetricsAsserts.GetMetrics(NsMetrics));
			MetricsAsserts.AssertCounter("CreateSnapshotOps", 0L, MetricsAsserts.GetMetrics(NnMetrics
				));
			// Create a snapshot for a non-snapshottable directory, thus should not
			// change the metrics
			try
			{
				hdfs.CreateSnapshot(sub1, "s1");
			}
			catch (Exception)
			{
			}
			MetricsAsserts.AssertGauge("Snapshots", 0, MetricsAsserts.GetMetrics(NsMetrics));
			MetricsAsserts.AssertCounter("CreateSnapshotOps", 1L, MetricsAsserts.GetMetrics(NnMetrics
				));
			// Create snapshot for sub1
			hdfs.AllowSnapshot(sub1);
			hdfs.CreateSnapshot(sub1, "s1");
			MetricsAsserts.AssertGauge("Snapshots", 1, MetricsAsserts.GetMetrics(NsMetrics));
			MetricsAsserts.AssertCounter("CreateSnapshotOps", 2L, MetricsAsserts.GetMetrics(NnMetrics
				));
			hdfs.CreateSnapshot(sub1, "s2");
			MetricsAsserts.AssertGauge("Snapshots", 2, MetricsAsserts.GetMetrics(NsMetrics));
			MetricsAsserts.AssertCounter("CreateSnapshotOps", 3L, MetricsAsserts.GetMetrics(NnMetrics
				));
			hdfs.GetSnapshotDiffReport(sub1, "s1", "s2");
			MetricsAsserts.AssertCounter("SnapshotDiffReportOps", 1L, MetricsAsserts.GetMetrics
				(NnMetrics));
			// Create snapshot for a directory under sub1
			Path subsub1 = new Path(sub1, "sub1sub1");
			Path subfile = new Path(subsub1, "file");
			DFSTestUtil.CreateFile(hdfs, subfile, 1024, Replication, seed);
			hdfs.AllowSnapshot(subsub1);
			hdfs.CreateSnapshot(subsub1, "s11");
			MetricsAsserts.AssertGauge("Snapshots", 3, MetricsAsserts.GetMetrics(NsMetrics));
			MetricsAsserts.AssertCounter("CreateSnapshotOps", 4L, MetricsAsserts.GetMetrics(NnMetrics
				));
			// delete snapshot
			hdfs.DeleteSnapshot(sub1, "s2");
			MetricsAsserts.AssertGauge("Snapshots", 2, MetricsAsserts.GetMetrics(NsMetrics));
			MetricsAsserts.AssertCounter("DeleteSnapshotOps", 1L, MetricsAsserts.GetMetrics(NnMetrics
				));
			// rename snapshot
			hdfs.RenameSnapshot(sub1, "s1", "NewS1");
			MetricsAsserts.AssertGauge("Snapshots", 2, MetricsAsserts.GetMetrics(NsMetrics));
			MetricsAsserts.AssertCounter("RenameSnapshotOps", 1L, MetricsAsserts.GetMetrics(NnMetrics
				));
		}

		public TestSnapshotMetrics()
		{
			sub1 = new Path(dir, "sub1");
			file1 = new Path(sub1, "file1");
			file2 = new Path(sub1, "file2");
		}
	}
}
