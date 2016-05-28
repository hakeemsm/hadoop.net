using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	public class TestBalancerWithEncryptedTransfer
	{
		private readonly Configuration conf = new HdfsConfiguration();

		[SetUp]
		public virtual void SetUpConf()
		{
			conf.SetBoolean(DFSConfigKeys.DfsEncryptDataTransferKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsBlockAccessTokenEnableKey, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEncryptedBalancer0()
		{
			new TestBalancer().TestBalancer0Internal(conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEncryptedBalancer1()
		{
			new TestBalancer().TestBalancer1Internal(conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEncryptedBalancer2()
		{
			new TestBalancer().TestBalancer2Internal(conf);
		}
	}
}
