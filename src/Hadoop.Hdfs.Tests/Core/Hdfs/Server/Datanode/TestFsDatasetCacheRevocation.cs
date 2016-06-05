using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestFsDatasetCacheRevocation
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestFsDatasetCacheRevocation
			));

		private static NativeIO.POSIX.CacheManipulator prevCacheManipulator;

		private static TemporarySocketDirectory sockDir;

		private const int BlockSize = 4096;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			prevCacheManipulator = NativeIO.POSIX.GetCacheManipulator();
			NativeIO.POSIX.SetCacheManipulator(new NativeIO.POSIX.NoMlockCacheManipulator());
			DomainSocket.DisableBindPathValidation();
			sockDir = new TemporarySocketDirectory();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			// Restore the original CacheManipulator
			NativeIO.POSIX.SetCacheManipulator(prevCacheManipulator);
			sockDir.Close();
		}

		private static Configuration GetDefaultConf()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsNamenodePathBasedCacheRefreshIntervalMs, 50);
			conf.SetLong(DFSConfigKeys.DfsCachereportIntervalMsecKey, 250);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetLong(DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey, TestFsDatasetCache.CacheCapacity
				);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, new FilePath(sockDir.GetDir(), "sock"
				).GetAbsolutePath());
			return conf;
		}

		/// <summary>
		/// Test that when a client has a replica mmapped, we will not un-mlock that
		/// replica for a reasonable amount of time, even if an uncache request
		/// occurs.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestPinning()
		{
			Assume.AssumeTrue(NativeCodeLoader.IsNativeCodeLoaded() && !Path.Windows);
			Configuration conf = GetDefaultConf();
			// Set a really long revocation timeout, so that we won't reach it during
			// this test.
			conf.SetLong(DFSConfigKeys.DfsDatanodeCacheRevocationTimeoutMs, 1800000L);
			// Poll very often
			conf.SetLong(DFSConfigKeys.DfsDatanodeCacheRevocationPollingMs, 2L);
			MiniDFSCluster cluster = null;
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			// Create and cache a file.
			string TestFile = "/test_file";
			DFSTestUtil.CreateFile(dfs, new Path(TestFile), BlockSize, (short)1, unchecked((int
				)(0xcafe)));
			dfs.AddCachePool(new CachePoolInfo("pool"));
			long cacheDirectiveId = dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPool
				("pool").SetPath(new Path(TestFile)).SetReplication((short)1).Build());
			FsDatasetSpi<object> fsd = cluster.GetDataNodes()[0].GetFSDataset();
			DFSTestUtil.VerifyExpectedCacheUsage(BlockSize, 1, fsd);
			// Mmap the file.
			FSDataInputStream @in = dfs.Open(new Path(TestFile));
			ByteBuffer buf = @in.Read(null, BlockSize, EnumSet.NoneOf<ReadOption>());
			// Attempt to uncache file.  The file should still be cached.
			dfs.RemoveCacheDirective(cacheDirectiveId);
			Sharpen.Thread.Sleep(500);
			DFSTestUtil.VerifyExpectedCacheUsage(BlockSize, 1, fsd);
			// Un-mmap the file.  The file should be uncached after this.
			@in.ReleaseBuffer(buf);
			DFSTestUtil.VerifyExpectedCacheUsage(0, 0, fsd);
			// Cleanup
			@in.Close();
			cluster.Shutdown();
		}

		/// <summary>
		/// Test that when we have an uncache request, and the client refuses to release
		/// the replica for a long time, we will un-mlock it.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRevocation()
		{
			Assume.AssumeTrue(NativeCodeLoader.IsNativeCodeLoaded() && !Path.Windows);
			BlockReaderTestUtil.EnableHdfsCachingTracing();
			BlockReaderTestUtil.EnableShortCircuitShmTracing();
			Configuration conf = GetDefaultConf();
			// Set a really short revocation timeout.
			conf.SetLong(DFSConfigKeys.DfsDatanodeCacheRevocationTimeoutMs, 250L);
			// Poll very often
			conf.SetLong(DFSConfigKeys.DfsDatanodeCacheRevocationPollingMs, 2L);
			MiniDFSCluster cluster = null;
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			// Create and cache a file.
			string TestFile = "/test_file2";
			DFSTestUtil.CreateFile(dfs, new Path(TestFile), BlockSize, (short)1, unchecked((int
				)(0xcafe)));
			dfs.AddCachePool(new CachePoolInfo("pool"));
			long cacheDirectiveId = dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPool
				("pool").SetPath(new Path(TestFile)).SetReplication((short)1).Build());
			FsDatasetSpi<object> fsd = cluster.GetDataNodes()[0].GetFSDataset();
			DFSTestUtil.VerifyExpectedCacheUsage(BlockSize, 1, fsd);
			// Mmap the file.
			FSDataInputStream @in = dfs.Open(new Path(TestFile));
			ByteBuffer buf = @in.Read(null, BlockSize, EnumSet.NoneOf<ReadOption>());
			// Attempt to uncache file.  The file should get uncached.
			Log.Info("removing cache directive {}", cacheDirectiveId);
			dfs.RemoveCacheDirective(cacheDirectiveId);
			Log.Info("finished removing cache directive {}", cacheDirectiveId);
			Sharpen.Thread.Sleep(1000);
			DFSTestUtil.VerifyExpectedCacheUsage(0, 0, fsd);
			// Cleanup
			@in.ReleaseBuffer(buf);
			@in.Close();
			cluster.Shutdown();
		}
	}
}
