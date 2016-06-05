using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server
{
	public class TestMiniYARNClusterForHA
	{
		internal MiniYARNCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			conf.Set(YarnConfiguration.RmWebappAddress, "localhost:0");
			cluster = new MiniYARNCluster(typeof(TestMiniYARNClusterForHA).FullName, 2, 1, 1, 
				1);
			cluster.Init(conf);
			cluster.Start();
			cluster.GetResourceManager(0).GetRMContext().GetRMAdminService().TransitionToActive
				(new HAServiceProtocol.StateChangeRequestInfo(HAServiceProtocol.RequestSource.RequestByUser
				));
			NUnit.Framework.Assert.IsFalse("RM never turned active", -1 == cluster.GetActiveRMIndex
				());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterWorks()
		{
			NUnit.Framework.Assert.IsTrue("NMs fail to connect to the RM", cluster.WaitForNodeManagersToConnect
				(5000));
		}
	}
}
