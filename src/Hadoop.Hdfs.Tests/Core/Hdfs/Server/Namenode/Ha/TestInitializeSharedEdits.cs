using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestInitializeSharedEdits
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.TestInitializeSharedEdits
			));

		private static readonly Path TestPath = new Path("/test");

		private Configuration conf;

		private MiniDFSCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetupCluster()
		{
			conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsHaLogrollPeriodKey, 1);
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			HAUtil.SetAllowStandbyReads(conf, true);
			MiniDFSNNTopology topology = MiniDFSNNTopology.SimpleHATopology();
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).NumDataNodes(0).Build
				();
			cluster.WaitActive();
			ShutdownClusterAndRemoveSharedEditsDir();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutdownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ShutdownClusterAndRemoveSharedEditsDir()
		{
			cluster.ShutdownNameNode(0);
			cluster.ShutdownNameNode(1);
			FilePath sharedEditsDir = new FilePath(cluster.GetSharedEditsDir(0, 1));
			NUnit.Framework.Assert.IsTrue(FileUtil.FullyDelete(sharedEditsDir));
		}

		private void AssertCannotStartNameNodes()
		{
			// Make sure we can't currently start either NN.
			try
			{
				cluster.RestartNameNode(0, false);
				NUnit.Framework.Assert.Fail("Should not have been able to start NN1 without shared dir"
					);
			}
			catch (IOException ioe)
			{
				Log.Info("Got expected exception", ioe);
				GenericTestUtils.AssertExceptionContains("storage directory does not exist or is not accessible"
					, ioe);
			}
			try
			{
				cluster.RestartNameNode(1, false);
				NUnit.Framework.Assert.Fail("Should not have been able to start NN2 without shared dir"
					);
			}
			catch (IOException ioe)
			{
				Log.Info("Got expected exception", ioe);
				GenericTestUtils.AssertExceptionContains("storage directory does not exist or is not accessible"
					, ioe);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		private void AssertCanStartHaNameNodes(string pathSuffix)
		{
			// Now should be able to start both NNs. Pass "false" here so that we don't
			// try to waitActive on all NNs, since the second NN doesn't exist yet.
			cluster.RestartNameNode(0, false);
			cluster.RestartNameNode(1, true);
			// Make sure HA is working.
			cluster.GetNameNode(0).GetRpcServer().TransitionToActive(new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser));
			FileSystem fs = null;
			try
			{
				Path newPath = new Path(TestPath, pathSuffix);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(newPath));
				HATestUtil.WaitForStandbyToCatchUp(cluster.GetNameNode(0), cluster.GetNameNode(1)
					);
				NUnit.Framework.Assert.IsTrue(NameNodeAdapter.GetFileInfo(cluster.GetNameNode(1), 
					newPath.ToString(), false).IsDir());
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInitializeSharedEdits()
		{
			AssertCannotStartNameNodes();
			// Initialize the shared edits dir.
			NUnit.Framework.Assert.IsFalse(NameNode.InitializeSharedEdits(cluster.GetConfiguration
				(0)));
			AssertCanStartHaNameNodes("1");
			// Now that we've done a metadata operation, make sure that deleting and
			// re-initializing the shared edits dir will let the standby still start.
			ShutdownClusterAndRemoveSharedEditsDir();
			AssertCannotStartNameNodes();
			// Re-initialize the shared edits dir.
			NUnit.Framework.Assert.IsFalse(NameNode.InitializeSharedEdits(cluster.GetConfiguration
				(0)));
			// Should *still* be able to start both NNs
			AssertCanStartHaNameNodes("2");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailWhenNoSharedEditsSpecified()
		{
			Configuration confNoShared = new Configuration(conf);
			confNoShared.Unset(DFSConfigKeys.DfsNamenodeSharedEditsDirKey);
			NUnit.Framework.Assert.IsFalse(NameNode.InitializeSharedEdits(confNoShared, true)
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDontOverWriteExistingDir()
		{
			NUnit.Framework.Assert.IsFalse(NameNode.InitializeSharedEdits(conf, false));
			NUnit.Framework.Assert.IsTrue(NameNode.InitializeSharedEdits(conf, false));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInitializeSharedEditsConfiguresGenericConfKeys()
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, "ns1"), "nn1,nn2"
				);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "ns1", "nn1"
				), "localhost:1234");
			NUnit.Framework.Assert.IsNull(conf.Get(DFSConfigKeys.DfsNamenodeRpcAddressKey));
			NameNode.InitializeSharedEdits(conf);
			NUnit.Framework.Assert.IsNotNull(conf.Get(DFSConfigKeys.DfsNamenodeRpcAddressKey)
				);
		}
	}
}
