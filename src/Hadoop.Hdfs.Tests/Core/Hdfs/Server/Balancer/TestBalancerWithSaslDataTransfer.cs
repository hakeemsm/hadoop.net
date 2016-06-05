using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	public class TestBalancerWithSaslDataTransfer : SaslDataTransferTestCase
	{
		private static readonly TestBalancer TestBalancer = new TestBalancer();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBalancer0Authentication()
		{
			TestBalancer.TestBalancer0Internal(CreateSecureConfig("authentication"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBalancer0Integrity()
		{
			TestBalancer.TestBalancer0Internal(CreateSecureConfig("integrity"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBalancer0Privacy()
		{
			TestBalancer.TestBalancer0Internal(CreateSecureConfig("privacy"));
		}
	}
}
