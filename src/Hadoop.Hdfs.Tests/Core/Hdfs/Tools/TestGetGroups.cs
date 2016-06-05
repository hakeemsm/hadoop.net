using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>
	/// Tests for the HDFS implementation of
	/// <see cref="GetGroups"/>
	/// </summary>
	public class TestGetGroups : GetGroupsTestBase
	{
		private MiniDFSCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetUpNameNode()
		{
			conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
		}

		[TearDown]
		public virtual void TearDownNameNode()
		{
			cluster.Shutdown();
		}

		protected override Tool GetTool(TextWriter o)
		{
			return new GetGroups(conf, o);
		}
	}
}
