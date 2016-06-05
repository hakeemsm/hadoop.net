using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	public class TestSnapshotListing
	{
		internal const long seed = 0;

		internal const short Replication = 3;

		internal const long Blocksize = 1024;

		private readonly Path dir = new Path("/test.snapshot/dir");

		internal Configuration conf;

		internal MiniDFSCluster cluster;

		internal FSNamesystem fsn;

		internal DistributedFileSystem hdfs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			hdfs = cluster.GetFileSystem();
			hdfs.Mkdirs(dir);
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

		/// <summary>Test listing snapshots under a snapshottable directory</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestListSnapshots()
		{
			Path snapshotsPath = new Path(dir, ".snapshot");
			FileStatus[] stats = null;
			// special case: snapshots of root
			stats = hdfs.ListStatus(new Path("/.snapshot"));
			// should be 0 since root's snapshot quota is 0
			NUnit.Framework.Assert.AreEqual(0, stats.Length);
			// list before set dir as snapshottable
			try
			{
				stats = hdfs.ListStatus(snapshotsPath);
				NUnit.Framework.Assert.Fail("expect SnapshotException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Directory is not a snapshottable directory: "
					 + dir.ToString(), e);
			}
			// list before creating snapshots
			hdfs.AllowSnapshot(dir);
			stats = hdfs.ListStatus(snapshotsPath);
			NUnit.Framework.Assert.AreEqual(0, stats.Length);
			// list while creating snapshots
			int snapshotNum = 5;
			for (int sNum = 0; sNum < snapshotNum; sNum++)
			{
				hdfs.CreateSnapshot(dir, "s_" + sNum);
				stats = hdfs.ListStatus(snapshotsPath);
				NUnit.Framework.Assert.AreEqual(sNum + 1, stats.Length);
				for (int i = 0; i <= sNum; i++)
				{
					NUnit.Framework.Assert.AreEqual("s_" + i, stats[i].GetPath().GetName());
				}
			}
			// list while deleting snapshots
			for (int sNum_1 = snapshotNum - 1; sNum_1 > 0; sNum_1--)
			{
				hdfs.DeleteSnapshot(dir, "s_" + sNum_1);
				stats = hdfs.ListStatus(snapshotsPath);
				NUnit.Framework.Assert.AreEqual(sNum_1, stats.Length);
				for (int i = 0; i < sNum_1; i++)
				{
					NUnit.Framework.Assert.AreEqual("s_" + i, stats[i].GetPath().GetName());
				}
			}
			// remove the last snapshot
			hdfs.DeleteSnapshot(dir, "s_0");
			stats = hdfs.ListStatus(snapshotsPath);
			NUnit.Framework.Assert.AreEqual(0, stats.Length);
		}
	}
}
