using NUnit.Framework;
using Org.Apache.Hadoop.Net.Unix;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests short-circuit local reads without any FileInputStream or
	/// Socket caching.
	/// </summary>
	/// <remarks>
	/// This class tests short-circuit local reads without any FileInputStream or
	/// Socket caching.  This is a regression test for HDFS-4417.
	/// </remarks>
	public class TestParallelShortCircuitReadUnCached : TestParallelReadUtil
	{
		private static TemporarySocketDirectory sockDir;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupCluster()
		{
			if (DomainSocket.GetLoadingFailureReason() != null)
			{
				return;
			}
			sockDir = new TemporarySocketDirectory();
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, new FilePath(sockDir.GetDir(), "TestParallelShortCircuitReadUnCached._PORT.sock"
				).GetAbsolutePath());
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			// Enabling data transfer encryption should have no effect when using
			// short-circuit local reads.  This is a regression test for HDFS-5353.
			conf.SetBoolean(DFSConfigKeys.DfsEncryptDataTransferKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsBlockAccessTokenEnableKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, false);
			conf.SetBoolean(DFSConfigKeys.DfsClientDomainSocketDataTraffic, true);
			// We want to test reading from stale sockets.
			conf.SetInt(DFSConfigKeys.DfsDatanodeSocketReuseKeepaliveKey, 1);
			conf.SetLong(DFSConfigKeys.DfsClientSocketCacheExpiryMsecKey, 5 * 60 * 1000);
			conf.SetInt(DFSConfigKeys.DfsClientSocketCacheCapacityKey, 32);
			// Avoid using the FileInputStreamCache.
			conf.SetInt(DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheSizeKey, 0);
			DomainSocket.DisableBindPathValidation();
			DFSInputStream.tcpReadsDisabledForTesting = true;
			SetupCluster(1, conf);
		}

		[SetUp]
		public virtual void Before()
		{
			Assume.AssumeThat(DomainSocket.GetLoadingFailureReason(), CoreMatchers.EqualTo(null
				));
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TeardownCluster()
		{
			if (DomainSocket.GetLoadingFailureReason() != null)
			{
				return;
			}
			sockDir.Close();
			TestParallelReadUtil.TeardownCluster();
		}
	}
}
