using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestParallelRead : TestParallelReadUtil
	{
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupCluster()
		{
			// This is a test of the normal (TCP) read path.  For this reason, we turn
			// off both short-circuit local reads and UNIX domain socket data traffic.
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, false);
			conf.SetBoolean(DFSConfigKeys.DfsClientDomainSocketDataTraffic, false);
			// dfs.domain.socket.path should be ignored because the previous two keys
			// were set to false.  This is a regression test for HDFS-4473.
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, "/will/not/be/created");
			SetupCluster(DefaultReplicationFactor, conf);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TeardownCluster()
		{
			TestParallelReadUtil.TeardownCluster();
		}
	}
}
