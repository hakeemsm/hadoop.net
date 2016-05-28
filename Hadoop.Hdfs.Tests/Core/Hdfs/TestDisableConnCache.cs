using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests disabling client connection caching in a single node
	/// mini-cluster.
	/// </summary>
	public class TestDisableConnCache
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestDisableConnCache)
			);

		internal const int BlockSize = 4096;

		internal const int FileSize = 3 * BlockSize;

		/// <summary>
		/// Test that the socket cache can be disabled by setting the capacity to
		/// 0.
		/// </summary>
		/// <remarks>
		/// Test that the socket cache can be disabled by setting the capacity to
		/// 0. Regression test for HDFS-3365.
		/// </remarks>
		/// <exception cref="System.Exception"></exception>
		[NUnit.Framework.Test]
		public virtual void TestDisableCache()
		{
			HdfsConfiguration confWithoutCache = new HdfsConfiguration();
			// Configure a new instance with no peer caching, ensure that it doesn't
			// cache anything
			confWithoutCache.SetInt(DFSConfigKeys.DfsClientSocketCacheCapacityKey, 0);
			BlockReaderTestUtil util = new BlockReaderTestUtil(1, confWithoutCache);
			Path testFile = new Path("/testConnCache.dat");
			util.WriteFile(testFile, FileSize / 1024);
			FileSystem fsWithoutCache = FileSystem.NewInstance(util.GetConf());
			try
			{
				DFSTestUtil.ReadFile(fsWithoutCache, testFile);
				NUnit.Framework.Assert.AreEqual(0, ((DistributedFileSystem)fsWithoutCache).dfs.GetClientContext
					().GetPeerCache().Size());
			}
			finally
			{
				fsWithoutCache.Close();
				util.Shutdown();
			}
		}
	}
}
