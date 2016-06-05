using NUnit.Framework;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestParallelShortCircuitLegacyRead : TestParallelReadUtil
	{
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupCluster()
		{
			DFSInputStream.tcpReadsDisabledForTesting = true;
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, string.Empty);
			conf.SetBoolean(DFSConfigKeys.DfsClientUseLegacyBlockreaderlocal, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientDomainSocketDataTraffic, false);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, false);
			conf.Set(DFSConfigKeys.DfsBlockLocalPathAccessUserKey, UserGroupInformation.GetCurrentUser
				().GetShortUserName());
			DomainSocket.DisableBindPathValidation();
			SetupCluster(1, conf);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TeardownCluster()
		{
			TestParallelReadUtil.TeardownCluster();
		}
	}
}
