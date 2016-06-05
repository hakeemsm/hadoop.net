using System.IO;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl
{
	public class TestSaslDataTransfer : SaslDataTransferTestCase
	{
		private const int BlockSize = 4096;

		private const int BufferSize = 1024;

		private const int NumBlocks = 3;

		private static readonly Path Path = new Path("/file1");

		private const short Replication = 3;

		private MiniDFSCluster cluster;

		private FileSystem fs;

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		[Rule]
		public Timeout timeout = new Timeout(60000);

		[TearDown]
		public virtual void Shutdown()
		{
			IOUtils.Cleanup(null, fs);
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuthentication()
		{
			HdfsConfiguration clusterConf = CreateSecureConfig("authentication,integrity,privacy"
				);
			StartCluster(clusterConf);
			HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
			clientConf.Set(DFSConfigKeys.DfsDataTransferProtectionKey, "authentication");
			DoTest(clientConf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIntegrity()
		{
			HdfsConfiguration clusterConf = CreateSecureConfig("authentication,integrity,privacy"
				);
			StartCluster(clusterConf);
			HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
			clientConf.Set(DFSConfigKeys.DfsDataTransferProtectionKey, "integrity");
			DoTest(clientConf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPrivacy()
		{
			HdfsConfiguration clusterConf = CreateSecureConfig("authentication,integrity,privacy"
				);
			StartCluster(clusterConf);
			HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
			clientConf.Set(DFSConfigKeys.DfsDataTransferProtectionKey, "privacy");
			DoTest(clientConf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClientAndServerDoNotHaveCommonQop()
		{
			HdfsConfiguration clusterConf = CreateSecureConfig("privacy");
			StartCluster(clusterConf);
			HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
			clientConf.Set(DFSConfigKeys.DfsDataTransferProtectionKey, "authentication");
			exception.Expect(typeof(IOException));
			exception.ExpectMessage("could only be replicated to 0 nodes");
			DoTest(clientConf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestServerSaslNoClientSasl()
		{
			HdfsConfiguration clusterConf = CreateSecureConfig("authentication,integrity,privacy"
				);
			// Set short retry timeouts so this test runs faster
			clusterConf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			StartCluster(clusterConf);
			HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
			clientConf.Set(DFSConfigKeys.DfsDataTransferProtectionKey, string.Empty);
			GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
				.GetLog(typeof(DataNode)));
			try
			{
				DoTest(clientConf);
				NUnit.Framework.Assert.Fail("Should fail if SASL data transfer protection is not "
					 + "configured or not supported in client");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertMatches(e.Message, "could only be replicated to 0 nodes");
			}
			finally
			{
				logs.StopCapturing();
			}
			GenericTestUtils.AssertMatches(logs.GetOutput(), "Failed to read expected SASL data transfer protection "
				 + "handshake from client at");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDataNodeAbortsIfNoSasl()
		{
			HdfsConfiguration clusterConf = CreateSecureConfig(string.Empty);
			exception.Expect(typeof(RuntimeException));
			exception.ExpectMessage("Cannot start secure DataNode");
			StartCluster(clusterConf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDataNodeAbortsIfNotHttpsOnly()
		{
			HdfsConfiguration clusterConf = CreateSecureConfig("authentication");
			clusterConf.Set(DFSConfigKeys.DfsHttpPolicyKey, HttpConfig.Policy.HttpAndHttps.ToString
				());
			exception.Expect(typeof(RuntimeException));
			exception.ExpectMessage("Cannot start secure DataNode");
			StartCluster(clusterConf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoSaslAndSecurePortsIgnored()
		{
			HdfsConfiguration clusterConf = CreateSecureConfig(string.Empty);
			clusterConf.SetBoolean(DFSConfigKeys.IgnoreSecurePortsForTestingKey, true);
			StartCluster(clusterConf);
			DoTest(clusterConf);
		}

		/// <summary>Tests DataTransferProtocol with the given client configuration.</summary>
		/// <param name="conf">client configuration</param>
		/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
		private void DoTest(HdfsConfiguration conf)
		{
			fs = FileSystem.Get(cluster.GetURI(), conf);
			FileSystemTestHelper.CreateFile(fs, Path, NumBlocks, BlockSize);
			Assert.AssertArrayEquals(FileSystemTestHelper.GetFileData(NumBlocks, BlockSize), 
				Sharpen.Runtime.GetBytesForString(DFSTestUtil.ReadFile(fs, Path), "UTF-8"));
			BlockLocation[] blockLocations = fs.GetFileBlockLocations(Path, 0, long.MaxValue);
			NUnit.Framework.Assert.IsNotNull(blockLocations);
			NUnit.Framework.Assert.AreEqual(NumBlocks, blockLocations.Length);
			foreach (BlockLocation blockLocation in blockLocations)
			{
				NUnit.Framework.Assert.IsNotNull(blockLocation.GetHosts());
				NUnit.Framework.Assert.AreEqual(3, blockLocation.GetHosts().Length);
			}
		}

		/// <summary>Starts a cluster with the given configuration.</summary>
		/// <param name="conf">cluster configuration</param>
		/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
		private void StartCluster(HdfsConfiguration conf)
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			cluster.WaitActive();
		}
	}
}
