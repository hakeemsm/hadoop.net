using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestNNHealthCheck
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNNHealthCheck()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).NnTopology(MiniDFSNNTopology
					.SimpleHATopology()).Build();
				NameNodeResourceChecker mockResourceChecker = Org.Mockito.Mockito.Mock<NameNodeResourceChecker
					>();
				Org.Mockito.Mockito.DoReturn(true).When(mockResourceChecker).HasAvailableDiskSpace
					();
				cluster.GetNameNode(0).GetNamesystem().SetNNResourceChecker(mockResourceChecker);
				NamenodeProtocols rpc = cluster.GetNameNodeRpc(0);
				// Should not throw error, which indicates healthy.
				rpc.MonitorHealth();
				Org.Mockito.Mockito.DoReturn(false).When(mockResourceChecker).HasAvailableDiskSpace
					();
				try
				{
					// Should throw error - NN is unhealthy.
					rpc.MonitorHealth();
					NUnit.Framework.Assert.Fail("Should not have succeeded in calling monitorHealth");
				}
				catch (HealthCheckFailedException hcfe)
				{
					GenericTestUtils.AssertExceptionContains("The NameNode has no resources available"
						, hcfe);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
