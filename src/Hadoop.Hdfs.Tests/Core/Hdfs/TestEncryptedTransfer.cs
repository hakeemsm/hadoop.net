using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestEncryptedTransfer
	{
		[Parameterized.Parameters]
		public static ICollection<object[]> Data()
		{
			ICollection<object[]> @params = new AList<object[]>();
			@params.AddItem(new object[] { null });
			@params.AddItem(new object[] { "org.apache.hadoop.hdfs.TestEncryptedTransfer$TestTrustedChannelResolver"
				 });
			return @params;
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.TestEncryptedTransfer
			));

		private const string PlainText = "this is very secret plain text";

		private static readonly Path TestPath = new Path("/non-encrypted-file");

		private void SetEncryptionConfigKeys(Configuration conf)
		{
			conf.SetBoolean(DFSConfigKeys.DfsEncryptDataTransferKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsBlockAccessTokenEnableKey, true);
			if (resolverClazz != null)
			{
				conf.Set(DFSConfigKeys.DfsTrustedchannelResolverClass, resolverClazz);
			}
		}

		// Unset DFS_ENCRYPT_DATA_TRANSFER_KEY and DFS_DATA_ENCRYPTION_ALGORITHM_KEY
		// on the client side to ensure that clients will detect this setting
		// automatically from the NN.
		/// <exception cref="System.IO.IOException"/>
		private static FileSystem GetFileSystem(Configuration conf)
		{
			Configuration localConf = new Configuration(conf);
			localConf.SetBoolean(DFSConfigKeys.DfsEncryptDataTransferKey, false);
			localConf.Unset(DFSConfigKeys.DfsDataEncryptionAlgorithmKey);
			return FileSystem.Get(localConf);
		}

		internal string resolverClazz;

		public TestEncryptedTransfer(string resolverClazz)
		{
			{
				LogManager.GetLogger(typeof(SaslDataTransferServer)).SetLevel(Level.Debug);
				LogManager.GetLogger(typeof(DataTransferSaslUtil)).SetLevel(Level.Debug);
			}
			this.resolverClazz = resolverClazz;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEncryptedRead()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = GetFileSystem(conf);
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				FileChecksum checksum = fs.GetFileChecksum(TestPath);
				fs.Close();
				cluster.Shutdown();
				SetEncryptionConfigKeys(conf);
				cluster = new MiniDFSCluster.Builder(conf).ManageDataDfsDirs(false).ManageNameDfsDirs
					(false).Format(false).StartupOption(HdfsServerConstants.StartupOption.Regular).Build
					();
				fs = GetFileSystem(conf);
				GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
					.GetLog(typeof(SaslDataTransferServer)));
				GenericTestUtils.LogCapturer logs1 = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
					.GetLog(typeof(DataTransferSaslUtil)));
				try
				{
					NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
					NUnit.Framework.Assert.AreEqual(checksum, fs.GetFileChecksum(TestPath));
				}
				finally
				{
					logs.StopCapturing();
					logs1.StopCapturing();
				}
				fs.Close();
				if (resolverClazz == null)
				{
					// Test client and server negotiate cipher option
					GenericTestUtils.AssertDoesNotMatch(logs.GetOutput(), "Server using cipher suite"
						);
					// Check the IOStreamPair
					GenericTestUtils.AssertDoesNotMatch(logs1.GetOutput(), "Creating IOStreamPair of CryptoInputStream and CryptoOutputStream."
						);
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

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEncryptedReadWithRC4()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = GetFileSystem(conf);
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				FileChecksum checksum = fs.GetFileChecksum(TestPath);
				fs.Close();
				cluster.Shutdown();
				SetEncryptionConfigKeys(conf);
				// It'll use 3DES by default, but we set it to rc4 here.
				conf.Set(DFSConfigKeys.DfsDataEncryptionAlgorithmKey, "rc4");
				cluster = new MiniDFSCluster.Builder(conf).ManageDataDfsDirs(false).ManageNameDfsDirs
					(false).Format(false).StartupOption(HdfsServerConstants.StartupOption.Regular).Build
					();
				fs = GetFileSystem(conf);
				GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
					.GetLog(typeof(SaslDataTransferServer)));
				GenericTestUtils.LogCapturer logs1 = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
					.GetLog(typeof(DataTransferSaslUtil)));
				try
				{
					NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
					NUnit.Framework.Assert.AreEqual(checksum, fs.GetFileChecksum(TestPath));
				}
				finally
				{
					logs.StopCapturing();
					logs1.StopCapturing();
				}
				fs.Close();
				if (resolverClazz == null)
				{
					// Test client and server negotiate cipher option
					GenericTestUtils.AssertDoesNotMatch(logs.GetOutput(), "Server using cipher suite"
						);
					// Check the IOStreamPair
					GenericTestUtils.AssertDoesNotMatch(logs1.GetOutput(), "Creating IOStreamPair of CryptoInputStream and CryptoOutputStream."
						);
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

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEncryptedReadWithAES()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				conf.Set(DFSConfigKeys.DfsEncryptDataTransferCipherSuitesKey, "AES/CTR/NoPadding"
					);
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = GetFileSystem(conf);
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				FileChecksum checksum = fs.GetFileChecksum(TestPath);
				fs.Close();
				cluster.Shutdown();
				SetEncryptionConfigKeys(conf);
				cluster = new MiniDFSCluster.Builder(conf).ManageDataDfsDirs(false).ManageNameDfsDirs
					(false).Format(false).StartupOption(HdfsServerConstants.StartupOption.Regular).Build
					();
				fs = GetFileSystem(conf);
				GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
					.GetLog(typeof(SaslDataTransferServer)));
				GenericTestUtils.LogCapturer logs1 = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
					.GetLog(typeof(DataTransferSaslUtil)));
				try
				{
					NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
					NUnit.Framework.Assert.AreEqual(checksum, fs.GetFileChecksum(TestPath));
				}
				finally
				{
					logs.StopCapturing();
					logs1.StopCapturing();
				}
				fs.Close();
				if (resolverClazz == null)
				{
					// Test client and server negotiate cipher option
					GenericTestUtils.AssertMatches(logs.GetOutput(), "Server using cipher suite");
					// Check the IOStreamPair
					GenericTestUtils.AssertMatches(logs1.GetOutput(), "Creating IOStreamPair of CryptoInputStream and CryptoOutputStream."
						);
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

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEncryptedReadAfterNameNodeRestart()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = GetFileSystem(conf);
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				FileChecksum checksum = fs.GetFileChecksum(TestPath);
				fs.Close();
				cluster.Shutdown();
				SetEncryptionConfigKeys(conf);
				cluster = new MiniDFSCluster.Builder(conf).ManageDataDfsDirs(false).ManageNameDfsDirs
					(false).Format(false).StartupOption(HdfsServerConstants.StartupOption.Regular).Build
					();
				fs = GetFileSystem(conf);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				NUnit.Framework.Assert.AreEqual(checksum, fs.GetFileChecksum(TestPath));
				fs.Close();
				cluster.RestartNameNode();
				fs = GetFileSystem(conf);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				NUnit.Framework.Assert.AreEqual(checksum, fs.GetFileChecksum(TestPath));
				fs.Close();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestClientThatDoesNotSupportEncryption()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				// Set short retry timeouts so this test runs faster
				conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = GetFileSystem(conf);
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				fs.Close();
				cluster.Shutdown();
				SetEncryptionConfigKeys(conf);
				cluster = new MiniDFSCluster.Builder(conf).ManageDataDfsDirs(false).ManageNameDfsDirs
					(false).Format(false).StartupOption(HdfsServerConstants.StartupOption.Regular).Build
					();
				fs = GetFileSystem(conf);
				DFSClient client = DFSClientAdapter.GetDFSClient((DistributedFileSystem)fs);
				DFSClient spyClient = Org.Mockito.Mockito.Spy(client);
				Org.Mockito.Mockito.DoReturn(false).When(spyClient).ShouldEncryptData();
				DFSClientAdapter.SetDFSClient((DistributedFileSystem)fs, spyClient);
				GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
					.GetLog(typeof(DataNode)));
				try
				{
					NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
					if (resolverClazz != null && !resolverClazz.EndsWith("TestTrustedChannelResolver"
						))
					{
						NUnit.Framework.Assert.Fail("Should not have been able to read without encryption enabled."
							);
					}
				}
				catch (IOException ioe)
				{
					GenericTestUtils.AssertExceptionContains("Could not obtain block:", ioe);
				}
				finally
				{
					logs.StopCapturing();
				}
				fs.Close();
				if (resolverClazz == null)
				{
					GenericTestUtils.AssertMatches(logs.GetOutput(), "Failed to read expected encryption handshake from client at"
						);
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

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLongLivedReadClientAfterRestart()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = GetFileSystem(conf);
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				FileChecksum checksum = fs.GetFileChecksum(TestPath);
				fs.Close();
				cluster.Shutdown();
				SetEncryptionConfigKeys(conf);
				cluster = new MiniDFSCluster.Builder(conf).ManageDataDfsDirs(false).ManageNameDfsDirs
					(false).Format(false).StartupOption(HdfsServerConstants.StartupOption.Regular).Build
					();
				fs = GetFileSystem(conf);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				NUnit.Framework.Assert.AreEqual(checksum, fs.GetFileChecksum(TestPath));
				// Restart the NN and DN, after which the client's encryption key will no
				// longer be valid.
				cluster.RestartNameNode();
				NUnit.Framework.Assert.IsTrue(cluster.RestartDataNode(0));
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				NUnit.Framework.Assert.AreEqual(checksum, fs.GetFileChecksum(TestPath));
				fs.Close();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLongLivedWriteClientAfterRestart()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				SetEncryptionConfigKeys(conf);
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = GetFileSystem(conf);
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				// Restart the NN and DN, after which the client's encryption key will no
				// longer be valid.
				cluster.RestartNameNode();
				NUnit.Framework.Assert.IsTrue(cluster.RestartDataNodes());
				cluster.WaitActive();
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText + PlainText, DFSTestUtil.ReadFile(fs, TestPath
					));
				fs.Close();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLongLivedClient()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = GetFileSystem(conf);
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				FileChecksum checksum = fs.GetFileChecksum(TestPath);
				fs.Close();
				cluster.Shutdown();
				SetEncryptionConfigKeys(conf);
				cluster = new MiniDFSCluster.Builder(conf).ManageDataDfsDirs(false).ManageNameDfsDirs
					(false).Format(false).StartupOption(HdfsServerConstants.StartupOption.Regular).Build
					();
				BlockTokenSecretManager btsm = cluster.GetNamesystem().GetBlockManager().GetBlockTokenSecretManager
					();
				btsm.SetKeyUpdateIntervalForTesting(2 * 1000);
				btsm.SetTokenLifetime(2 * 1000);
				btsm.ClearAllKeysForTesting();
				fs = GetFileSystem(conf);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				NUnit.Framework.Assert.AreEqual(checksum, fs.GetFileChecksum(TestPath));
				// Sleep for 15 seconds, after which the encryption key will no longer be
				// valid. It needs to be a few multiples of the block token lifetime,
				// since several block tokens are valid at any given time (the current
				// and the last two, by default.)
				Log.Info("Sleeping so that encryption keys expire...");
				Sharpen.Thread.Sleep(15 * 1000);
				Log.Info("Done sleeping.");
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				NUnit.Framework.Assert.AreEqual(checksum, fs.GetFileChecksum(TestPath));
				fs.Close();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEncryptedWriteWithOneDn()
		{
			TestEncryptedWrite(1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEncryptedWriteWithTwoDns()
		{
			TestEncryptedWrite(2);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEncryptedWriteWithMultipleDns()
		{
			TestEncryptedWrite(10);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestEncryptedWrite(int numDns)
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				SetEncryptionConfigKeys(conf);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDns).Build();
				FileSystem fs = GetFileSystem(conf);
				GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
					.GetLog(typeof(SaslDataTransferServer)));
				GenericTestUtils.LogCapturer logs1 = GenericTestUtils.LogCapturer.CaptureLogs(LogFactory
					.GetLog(typeof(DataTransferSaslUtil)));
				try
				{
					WriteTestDataToFile(fs);
				}
				finally
				{
					logs.StopCapturing();
					logs1.StopCapturing();
				}
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				fs.Close();
				if (resolverClazz == null)
				{
					// Test client and server negotiate cipher option
					GenericTestUtils.AssertDoesNotMatch(logs.GetOutput(), "Server using cipher suite"
						);
					// Check the IOStreamPair
					GenericTestUtils.AssertDoesNotMatch(logs1.GetOutput(), "Creating IOStreamPair of CryptoInputStream and CryptoOutputStream."
						);
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

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEncryptedAppend()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				SetEncryptionConfigKeys(conf);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				FileSystem fs = GetFileSystem(conf);
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText + PlainText, DFSTestUtil.ReadFile(fs, TestPath
					));
				fs.Close();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEncryptedAppendRequiringBlockTransfer()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				SetEncryptionConfigKeys(conf);
				// start up 4 DNs
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
				FileSystem fs = GetFileSystem(conf);
				// Create a file with replication 3, so its block is on 3 / 4 DNs.
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText, DFSTestUtil.ReadFile(fs, TestPath));
				// Shut down one of the DNs holding a block replica.
				FSDataInputStream @in = fs.Open(TestPath);
				IList<LocatedBlock> locatedBlocks = DFSTestUtil.GetAllBlocks(@in);
				@in.Close();
				NUnit.Framework.Assert.AreEqual(1, locatedBlocks.Count);
				NUnit.Framework.Assert.AreEqual(3, locatedBlocks[0].GetLocations().Length);
				DataNode dn = cluster.GetDataNode(locatedBlocks[0].GetLocations()[0].GetIpcPort()
					);
				dn.Shutdown();
				// Reopen the file for append, which will need to add another DN to the
				// pipeline and in doing so trigger a block transfer.
				WriteTestDataToFile(fs);
				NUnit.Framework.Assert.AreEqual(PlainText + PlainText, DFSTestUtil.ReadFile(fs, TestPath
					));
				fs.Close();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteTestDataToFile(FileSystem fs)
		{
			OutputStream @out = null;
			if (!fs.Exists(TestPath))
			{
				@out = fs.Create(TestPath);
			}
			else
			{
				@out = fs.Append(TestPath);
			}
			@out.Write(Sharpen.Runtime.GetBytesForString(PlainText));
			@out.Close();
		}

		internal class TestTrustedChannelResolver : TrustedChannelResolver
		{
			public override bool IsTrusted()
			{
				return true;
			}

			public override bool IsTrusted(IPAddress peerAddress)
			{
				return true;
			}
		}
	}
}
