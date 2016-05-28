using NUnit.Framework;
using Org.Apache.Hadoop.Net.Unix;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestParallelShortCircuitReadNoChecksum : TestParallelReadUtil
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
			DFSInputStream.tcpReadsDisabledForTesting = true;
			sockDir = new TemporarySocketDirectory();
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, new FilePath(sockDir.GetDir(), "TestParallelLocalRead.%d.sock"
				).GetAbsolutePath());
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, true);
			DomainSocket.DisableBindPathValidation();
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
