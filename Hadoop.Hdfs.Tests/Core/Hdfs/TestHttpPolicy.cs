using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public sealed class TestHttpPolicy
	{
		public void TestInvalidPolicyValue()
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsHttpPolicyKey, "invalid");
			DFSUtil.GetHttpPolicy(conf);
		}

		[NUnit.Framework.Test]
		public void TestDeprecatedConfiguration()
		{
			Configuration conf = new Configuration(false);
			NUnit.Framework.Assert.AreSame(HttpConfig.Policy.HttpOnly, DFSUtil.GetHttpPolicy(
				conf));
			conf.SetBoolean(DFSConfigKeys.DfsHttpsEnableKey, true);
			NUnit.Framework.Assert.AreSame(HttpConfig.Policy.HttpAndHttps, DFSUtil.GetHttpPolicy
				(conf));
			conf = new Configuration(false);
			conf.SetBoolean(DFSConfigKeys.HadoopSslEnabledKey, true);
			NUnit.Framework.Assert.AreSame(HttpConfig.Policy.HttpAndHttps, DFSUtil.GetHttpPolicy
				(conf));
			conf = new Configuration(false);
			conf.Set(DFSConfigKeys.DfsHttpPolicyKey, HttpConfig.Policy.HttpOnly.ToString());
			conf.SetBoolean(DFSConfigKeys.DfsHttpsEnableKey, true);
			NUnit.Framework.Assert.AreSame(HttpConfig.Policy.HttpOnly, DFSUtil.GetHttpPolicy(
				conf));
		}
	}
}
