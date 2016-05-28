using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// This class tests the validation of the configuration object when passed
	/// to the NameNode
	/// </summary>
	public class TestValidateConfigurationSettings
	{
		[TearDown]
		public virtual void CleanUp()
		{
			FileUtil.FullyDeleteContents(new FilePath(MiniDFSCluster.GetBaseDirectory()));
		}

		/// <summary>
		/// Tests setting the rpc port to the same as the web port to test that
		/// an exception
		/// is thrown when trying to re-use the same port
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestThatMatchingRPCandHttpPortsThrowException()
		{
			NameNode nameNode = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				FilePath nameDir = new FilePath(MiniDFSCluster.GetBaseDirectory(), "name");
				conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetAbsolutePath());
				Random rand = new Random();
				int port = 30000 + rand.Next(30000);
				// set both of these to the same port. It should fail.
				FileSystem.SetDefaultUri(conf, "hdfs://localhost:" + port);
				conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "127.0.0.1:" + port);
				DFSTestUtil.FormatNameNode(conf);
				nameNode = new NameNode(conf);
			}
			finally
			{
				if (nameNode != null)
				{
					nameNode.Stop();
				}
			}
		}

		/// <summary>
		/// Tests setting the rpc port to a different as the web port that an
		/// exception is NOT thrown
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestThatDifferentRPCandHttpPortsAreOK()
		{
			Configuration conf = new HdfsConfiguration();
			FilePath nameDir = new FilePath(MiniDFSCluster.GetBaseDirectory(), "name");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetAbsolutePath());
			Random rand = new Random();
			// A few retries in case the ports we choose are in use.
			for (int i = 0; i < 5; ++i)
			{
				int port1 = 30000 + rand.Next(10000);
				int port2 = port1 + 1 + rand.Next(10000);
				FileSystem.SetDefaultUri(conf, "hdfs://localhost:" + port1);
				conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "127.0.0.1:" + port2);
				DFSTestUtil.FormatNameNode(conf);
				NameNode nameNode = null;
				try
				{
					nameNode = new NameNode(conf);
					// should be OK!
					break;
				}
				catch (BindException)
				{
					continue;
				}
				finally
				{
					// Port in use? Try another.
					if (nameNode != null)
					{
						nameNode.Stop();
					}
				}
			}
		}

		/// <summary>
		/// HDFS-3013: NameNode format command doesn't pick up
		/// dfs.namenode.name.dir.NameServiceId configuration.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGenericKeysForNameNodeFormat()
		{
			Configuration conf = new HdfsConfiguration();
			// Set ephemeral ports 
			conf.Set(DFSConfigKeys.DfsNamenodeRpcAddressKey, "127.0.0.1:0");
			conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "127.0.0.1:0");
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1");
			// Set a nameservice-specific configuration for name dir
			FilePath dir = new FilePath(MiniDFSCluster.GetBaseDirectory(), "testGenericKeysForNameNodeFormat"
				);
			if (dir.Exists())
			{
				FileUtil.FullyDelete(dir);
			}
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey + ".ns1", dir.GetAbsolutePath());
			// Format and verify the right dir is formatted.
			DFSTestUtil.FormatNameNode(conf);
			GenericTestUtils.AssertExists(dir);
			// Ensure that the same dir is picked up by the running NN
			NameNode nameNode = new NameNode(conf);
			nameNode.Stop();
		}
	}
}
