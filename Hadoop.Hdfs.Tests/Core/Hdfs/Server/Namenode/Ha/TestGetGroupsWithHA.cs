using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestGetGroupsWithHA : GetGroupsTestBase
	{
		private MiniDFSCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetUpNameNode()
		{
			conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(0).Build();
			HATestUtil.SetFailoverConfigurations(cluster, conf);
		}

		[TearDown]
		public virtual void TearDownNameNode()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		protected override Tool GetTool(TextWriter o)
		{
			return new GetGroups(conf, o);
		}
	}
}
