using Javax.Management;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestSecondaryWebUi
	{
		private static MiniDFSCluster cluster;

		private static SecondaryNameNode snn;

		private static readonly Configuration conf = new Configuration();

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetUpCluster()
		{
			conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "0.0.0.0:0");
			conf.SetLong(DFSConfigKeys.DfsNamenodeCheckpointTxnsKey, 500);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
			cluster.WaitActive();
			snn = new SecondaryNameNode(conf);
		}

		[AfterClass]
		public static void ShutDownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			if (snn != null)
			{
				snn.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Management.MalformedObjectNameException"/>
		/// <exception cref="Javax.Management.AttributeNotFoundException"/>
		/// <exception cref="Javax.Management.MBeanException"/>
		/// <exception cref="Javax.Management.ReflectionException"/>
		/// <exception cref="Javax.Management.InstanceNotFoundException"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryWebUi()
		{
			MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
			ObjectName mxbeanName = new ObjectName("Hadoop:service=SecondaryNameNode,name=SecondaryNameNodeInfo"
				);
			string[] checkpointDir = (string[])mbs.GetAttribute(mxbeanName, "CheckpointDirectories"
				);
			Assert.AssertArrayEquals(checkpointDir, snn.GetCheckpointDirectories());
			string[] checkpointEditlogDir = (string[])mbs.GetAttribute(mxbeanName, "CheckpointEditlogDirectories"
				);
			Assert.AssertArrayEquals(checkpointEditlogDir, snn.GetCheckpointEditlogDirectories
				());
		}
	}
}
