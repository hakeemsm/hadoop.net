using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestMalformedURLs
	{
		private MiniDFSCluster cluster;

		internal Configuration config;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Configuration.AddDefaultResource("hdfs-site.malformed.xml");
			config = new Configuration();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTryStartingCluster()
		{
			// if we are able to start the cluster, it means
			// that we were able to read the configuration
			// correctly.
			Assert.AssertNotEquals(config.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey), config
				.GetTrimmed(DFSConfigKeys.DfsNamenodeHttpAddressKey));
			cluster = new MiniDFSCluster.Builder(config).Build();
			cluster.WaitActive();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}
	}
}
