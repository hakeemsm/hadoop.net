using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Tests MiniDFS cluster setup/teardown and isolation.</summary>
	/// <remarks>
	/// Tests MiniDFS cluster setup/teardown and isolation.
	/// Every instance is brought up with a new data dir, to ensure that
	/// shutdown work in background threads don't interfere with bringing up
	/// the new cluster.
	/// </remarks>
	public class TestMiniDFSCluster
	{
		private const string Cluster1 = "cluster1";

		private const string Cluster2 = "cluster2";

		private const string Cluster3 = "cluster3";

		private const string Cluster4 = "cluster4";

		private const string Cluster5 = "cluster5";

		protected internal FilePath testDataPath;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			testDataPath = new FilePath(PathUtils.GetTestDir(GetType()), "miniclusters");
		}

		/// <summary>
		/// Verify that without system properties the cluster still comes up, provided
		/// the configuration is set
		/// </summary>
		/// <exception cref="System.Exception">on a failure</exception>
		public virtual void TestClusterWithoutSystemProperties()
		{
			Runtime.ClearProperty(MiniDFSCluster.PropTestBuildData);
			Configuration conf = new HdfsConfiguration();
			FilePath testDataCluster1 = new FilePath(testDataPath, Cluster1);
			string c1Path = testDataCluster1.GetAbsolutePath();
			conf.Set(MiniDFSCluster.HdfsMinidfsBasedir, c1Path);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				NUnit.Framework.Assert.AreEqual(new FilePath(c1Path + "/data"), new FilePath(cluster
					.GetDataDirectory()));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Bring up two clusters and assert that they are in different directories.
		/// 	</summary>
		/// <exception cref="System.Exception">on a failure</exception>
		public virtual void TestDualClusters()
		{
			FilePath testDataCluster2 = new FilePath(testDataPath, Cluster2);
			FilePath testDataCluster3 = new FilePath(testDataPath, Cluster3);
			Configuration conf = new HdfsConfiguration();
			string c2Path = testDataCluster2.GetAbsolutePath();
			conf.Set(MiniDFSCluster.HdfsMinidfsBasedir, c2Path);
			MiniDFSCluster cluster2 = new MiniDFSCluster.Builder(conf).Build();
			MiniDFSCluster cluster3 = null;
			try
			{
				string dataDir2 = cluster2.GetDataDirectory();
				NUnit.Framework.Assert.AreEqual(new FilePath(c2Path + "/data"), new FilePath(dataDir2
					));
				//change the data dir
				conf.Set(MiniDFSCluster.HdfsMinidfsBasedir, testDataCluster3.GetAbsolutePath());
				MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
				cluster3 = builder.Build();
				string dataDir3 = cluster3.GetDataDirectory();
				NUnit.Framework.Assert.IsTrue("Clusters are bound to the same directory: " + dataDir2
					, !dataDir2.Equals(dataDir3));
			}
			finally
			{
				MiniDFSCluster.ShutdownCluster(cluster3);
				MiniDFSCluster.ShutdownCluster(cluster2);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIsClusterUpAfterShutdown()
		{
			Configuration conf = new HdfsConfiguration();
			FilePath testDataCluster4 = new FilePath(testDataPath, Cluster4);
			string c4Path = testDataCluster4.GetAbsolutePath();
			conf.Set(MiniDFSCluster.HdfsMinidfsBasedir, c4Path);
			MiniDFSCluster cluster4 = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				DistributedFileSystem dfs = cluster4.GetFileSystem();
				dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				cluster4.Shutdown();
			}
			finally
			{
				while (cluster4.IsClusterUp())
				{
					Sharpen.Thread.Sleep(1000);
				}
			}
		}

		/// <summary>MiniDFSCluster should not clobber dfs.datanode.hostname if requested</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestClusterSetDatanodeHostname()
		{
			Assume.AssumeTrue(Runtime.GetProperty("os.name").StartsWith("Linux"));
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsDatanodeHostNameKey, "MYHOST");
			FilePath testDataCluster5 = new FilePath(testDataPath, Cluster5);
			string c5Path = testDataCluster5.GetAbsolutePath();
			conf.Set(MiniDFSCluster.HdfsMinidfsBasedir, c5Path);
			MiniDFSCluster cluster5 = new MiniDFSCluster.Builder(conf).NumDataNodes(1).CheckDataNodeHostConfig
				(true).Build();
			try
			{
				NUnit.Framework.Assert.AreEqual("DataNode hostname config not respected", "MYHOST"
					, cluster5.GetDataNodes()[0].GetDatanodeId().GetHostName());
			}
			finally
			{
				MiniDFSCluster.ShutdownCluster(cluster5);
			}
		}
	}
}
