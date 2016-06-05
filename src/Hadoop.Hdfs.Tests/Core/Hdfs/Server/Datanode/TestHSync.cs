using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestHSync
	{
		private void CheckSyncMetric(MiniDFSCluster cluster, int dn, long value)
		{
			DataNode datanode = cluster.GetDataNodes()[dn];
			MetricsAsserts.AssertCounter("FsyncCount", value, MetricsAsserts.GetMetrics(datanode
				.GetMetrics().Name()));
		}

		private void CheckSyncMetric(MiniDFSCluster cluster, long value)
		{
			CheckSyncMetric(cluster, 0, value);
		}

		/// <summary>Test basic hsync cases</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHSync()
		{
			TestHSyncOperation(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHSyncWithAppend()
		{
			TestHSyncOperation(true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestHSyncOperation(bool testWithAppend)
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			DistributedFileSystem fs = cluster.GetFileSystem();
			Path p = new Path("/testHSync/foo");
			int len = 1 << 16;
			FSDataOutputStream @out = fs.Create(p, FsPermission.GetDefault(), EnumSet.Of(CreateFlag
				.Create, CreateFlag.Overwrite, CreateFlag.SyncBlock), 4096, (short)1, len, null);
			if (testWithAppend)
			{
				// re-open the file with append call
				@out.Close();
				@out = fs.Append(p, EnumSet.Of(CreateFlag.Append, CreateFlag.SyncBlock), 4096, null
					);
			}
			@out.Hflush();
			// hflush does not sync
			CheckSyncMetric(cluster, 0);
			@out.Hsync();
			// hsync on empty file does nothing
			CheckSyncMetric(cluster, 0);
			@out.Write(1);
			CheckSyncMetric(cluster, 0);
			@out.Hsync();
			CheckSyncMetric(cluster, 1);
			// avoiding repeated hsyncs is a potential future optimization
			@out.Hsync();
			CheckSyncMetric(cluster, 2);
			@out.Hflush();
			// hflush still does not sync
			CheckSyncMetric(cluster, 2);
			@out.Close();
			// close is sync'ing
			CheckSyncMetric(cluster, 3);
			// same with a file created with out SYNC_BLOCK
			@out = fs.Create(p, FsPermission.GetDefault(), EnumSet.Of(CreateFlag.Create, CreateFlag
				.Overwrite), 4096, (short)1, len, null);
			@out.Hsync();
			CheckSyncMetric(cluster, 3);
			@out.Write(1);
			CheckSyncMetric(cluster, 3);
			@out.Hsync();
			CheckSyncMetric(cluster, 4);
			// repeated hsyncs
			@out.Hsync();
			CheckSyncMetric(cluster, 5);
			@out.Close();
			// close does not sync (not opened with SYNC_BLOCK)
			CheckSyncMetric(cluster, 5);
			cluster.Shutdown();
		}

		/// <summary>Test hsync on an exact block boundary</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHSyncBlockBoundary()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			Path p = new Path("/testHSyncBlockBoundary/foo");
			int len = 1 << 16;
			byte[] fileContents = AppendTestUtil.InitBuffer(len);
			FSDataOutputStream @out = fs.Create(p, FsPermission.GetDefault(), EnumSet.Of(CreateFlag
				.Create, CreateFlag.Overwrite, CreateFlag.SyncBlock), 4096, (short)1, len, null);
			// fill exactly one block (tests the SYNC_BLOCK case) and flush
			@out.Write(fileContents, 0, len);
			@out.Hflush();
			// the full block should have caused a sync
			CheckSyncMetric(cluster, 1);
			@out.Hsync();
			// first on block again
			CheckSyncMetric(cluster, 1);
			// write one more byte and sync again
			@out.Write(1);
			@out.Hsync();
			CheckSyncMetric(cluster, 2);
			@out.Close();
			CheckSyncMetric(cluster, 3);
			cluster.Shutdown();
		}

		/// <summary>Test hsync via SequenceFiles</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSequenceFileSync()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			Path p = new Path("/testSequenceFileSync/foo");
			int len = 1 << 16;
			FSDataOutputStream @out = fs.Create(p, FsPermission.GetDefault(), EnumSet.Of(CreateFlag
				.Create, CreateFlag.Overwrite, CreateFlag.SyncBlock), 4096, (short)1, len, null);
			SequenceFile.Writer w = SequenceFile.CreateWriter(new Configuration(), SequenceFile.Writer
				.Stream(@out), SequenceFile.Writer.KeyClass(typeof(RandomDatum)), SequenceFile.Writer
				.ValueClass(typeof(RandomDatum)), SequenceFile.Writer.Compression(SequenceFile.CompressionType
				.None, new DefaultCodec()));
			w.Hflush();
			CheckSyncMetric(cluster, 0);
			w.Hsync();
			CheckSyncMetric(cluster, 1);
			int seed = new Random().Next();
			RandomDatum.Generator generator = new RandomDatum.Generator(seed);
			generator.Next();
			w.Append(generator.GetKey(), generator.GetValue());
			w.Hsync();
			CheckSyncMetric(cluster, 2);
			w.Close();
			CheckSyncMetric(cluster, 2);
			@out.Close();
			CheckSyncMetric(cluster, 3);
			cluster.Shutdown();
		}

		/// <summary>Test that syncBlock is correctly performed at replicas</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHSyncWithReplication()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			FileSystem fs = cluster.GetFileSystem();
			Path p = new Path("/testHSyncWithReplication/foo");
			int len = 1 << 16;
			FSDataOutputStream @out = fs.Create(p, FsPermission.GetDefault(), EnumSet.Of(CreateFlag
				.Create, CreateFlag.Overwrite, CreateFlag.SyncBlock), 4096, (short)3, len, null);
			@out.Write(1);
			@out.Hflush();
			CheckSyncMetric(cluster, 0, 0);
			CheckSyncMetric(cluster, 1, 0);
			CheckSyncMetric(cluster, 2, 0);
			@out.Hsync();
			CheckSyncMetric(cluster, 0, 1);
			CheckSyncMetric(cluster, 1, 1);
			CheckSyncMetric(cluster, 2, 1);
			@out.Hsync();
			CheckSyncMetric(cluster, 0, 2);
			CheckSyncMetric(cluster, 1, 2);
			CheckSyncMetric(cluster, 2, 2);
			cluster.Shutdown();
		}
	}
}
