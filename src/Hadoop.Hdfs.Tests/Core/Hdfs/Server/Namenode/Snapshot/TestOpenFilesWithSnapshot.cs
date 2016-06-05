using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	public class TestOpenFilesWithSnapshot
	{
		private readonly Configuration conf = new Configuration();

		internal MiniDFSCluster cluster = null;

		internal DistributedFileSystem fs = null;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			conf.Set("dfs.blocksize", "1048576");
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Teardown()
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

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUCFileDeleteWithSnapShot()
		{
			Path path = new Path("/test");
			DoWriteAndAbort(fs, path);
			// delete files separately
			fs.Delete(new Path("/test/test/test2"), true);
			fs.Delete(new Path("/test/test/test3"), true);
			cluster.RestartNameNode();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestParentDirWithUCFileDeleteWithSnapShot()
		{
			Path path = new Path("/test");
			DoWriteAndAbort(fs, path);
			// delete parent directory
			fs.Delete(new Path("/test/test"), true);
			cluster.RestartNameNode();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWithCheckpoint()
		{
			Path path = new Path("/test");
			DoWriteAndAbort(fs, path);
			fs.Delete(new Path("/test/test"), true);
			NameNode nameNode = cluster.GetNameNode();
			NameNodeAdapter.EnterSafeMode(nameNode, false);
			NameNodeAdapter.SaveNamespace(nameNode);
			NameNodeAdapter.LeaveSafeMode(nameNode);
			cluster.RestartNameNode(true);
			// read snapshot file after restart
			string test2snapshotPath = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotPath(path.ToString(), "s1/test/test2");
			DFSTestUtil.ReadFile(fs, new Path(test2snapshotPath));
			string test3snapshotPath = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotPath(path.ToString(), "s1/test/test3");
			DFSTestUtil.ReadFile(fs, new Path(test3snapshotPath));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFilesDeletionWithCheckpoint()
		{
			Path path = new Path("/test");
			DoWriteAndAbort(fs, path);
			fs.Delete(new Path("/test/test/test2"), true);
			fs.Delete(new Path("/test/test/test3"), true);
			NameNode nameNode = cluster.GetNameNode();
			NameNodeAdapter.EnterSafeMode(nameNode, false);
			NameNodeAdapter.SaveNamespace(nameNode);
			NameNodeAdapter.LeaveSafeMode(nameNode);
			cluster.RestartNameNode(true);
			// read snapshot file after restart
			string test2snapshotPath = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotPath(path.ToString(), "s1/test/test2");
			DFSTestUtil.ReadFile(fs, new Path(test2snapshotPath));
			string test3snapshotPath = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotPath(path.ToString(), "s1/test/test3");
			DFSTestUtil.ReadFile(fs, new Path(test3snapshotPath));
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoWriteAndAbort(DistributedFileSystem fs, Path path)
		{
			fs.Mkdirs(path);
			fs.AllowSnapshot(path);
			DFSTestUtil.CreateFile(fs, new Path("/test/test1"), 100, (short)2, 100024L);
			DFSTestUtil.CreateFile(fs, new Path("/test/test2"), 100, (short)2, 100024L);
			Path file = new Path("/test/test/test2");
			FSDataOutputStream @out = fs.Create(file);
			for (int i = 0; i < 2; i++)
			{
				long count = 0;
				while (count < 1048576)
				{
					@out.WriteBytes("hell");
					count += 4;
				}
			}
			((DFSOutputStream)@out.GetWrappedStream()).Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag
				.UpdateLength));
			DFSTestUtil.AbortStream((DFSOutputStream)@out.GetWrappedStream());
			Path file2 = new Path("/test/test/test3");
			FSDataOutputStream out2 = fs.Create(file2);
			for (int i_1 = 0; i_1 < 2; i_1++)
			{
				long count = 0;
				while (count < 1048576)
				{
					out2.WriteBytes("hell");
					count += 4;
				}
			}
			((DFSOutputStream)out2.GetWrappedStream()).Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag
				.UpdateLength));
			DFSTestUtil.AbortStream((DFSOutputStream)out2.GetWrappedStream());
			fs.CreateSnapshot(path, "s1");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOpenFilesWithMultipleSnapshots()
		{
			DoTestMultipleSnapshots(true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOpenFilesWithMultipleSnapshotsWithoutCheckpoint()
		{
			DoTestMultipleSnapshots(false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoTestMultipleSnapshots(bool saveNamespace)
		{
			Path path = new Path("/test");
			DoWriteAndAbort(fs, path);
			fs.CreateSnapshot(path, "s2");
			fs.Delete(new Path("/test/test"), true);
			fs.DeleteSnapshot(path, "s2");
			cluster.TriggerBlockReports();
			if (saveNamespace)
			{
				NameNode nameNode = cluster.GetNameNode();
				NameNodeAdapter.EnterSafeMode(nameNode, false);
				NameNodeAdapter.SaveNamespace(nameNode);
				NameNodeAdapter.LeaveSafeMode(nameNode);
			}
			cluster.RestartNameNode(true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOpenFilesWithRename()
		{
			Path path = new Path("/test");
			DoWriteAndAbort(fs, path);
			// check for zero sized blocks
			Path fileWithEmptyBlock = new Path("/test/test/test4");
			fs.Create(fileWithEmptyBlock);
			NamenodeProtocols nameNodeRpc = cluster.GetNameNodeRpc();
			string clientName = fs.GetClient().GetClientName();
			// create one empty block
			nameNodeRpc.AddBlock(fileWithEmptyBlock.ToString(), clientName, null, null, INodeId
				.GrandfatherInodeId, null);
			fs.CreateSnapshot(path, "s2");
			fs.Rename(new Path("/test/test"), new Path("/test/test-renamed"));
			fs.Delete(new Path("/test/test-renamed"), true);
			NameNode nameNode = cluster.GetNameNode();
			NameNodeAdapter.EnterSafeMode(nameNode, false);
			NameNodeAdapter.SaveNamespace(nameNode);
			NameNodeAdapter.LeaveSafeMode(nameNode);
			cluster.RestartNameNode(true);
		}
	}
}
