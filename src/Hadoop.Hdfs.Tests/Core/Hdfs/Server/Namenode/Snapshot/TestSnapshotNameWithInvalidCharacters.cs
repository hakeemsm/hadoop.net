using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	public class TestSnapshotNameWithInvalidCharacters
	{
		private const long Seed = 0;

		private const short Replication = 1;

		private const int Blocksize = 1024;

		private static readonly Configuration conf = new Configuration();

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem hdfs;

		private readonly Path dir1 = new Path("/");

		private readonly string file1Name = "file1";

		private readonly string snapshot1 = "a:b:c";

		private readonly string snapshot2 = "a/b/c";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			hdfs = cluster.GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotWithInvalidName()
		{
			Path file1 = new Path(dir1, file1Name);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, Seed);
			hdfs.AllowSnapshot(dir1);
			try
			{
				hdfs.CreateSnapshot(dir1, snapshot1);
			}
			catch (RemoteException)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotWithInvalidName1()
		{
			Path file1 = new Path(dir1, file1Name);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, Seed);
			hdfs.AllowSnapshot(dir1);
			try
			{
				hdfs.CreateSnapshot(dir1, snapshot2);
			}
			catch (RemoteException)
			{
			}
		}
	}
}
