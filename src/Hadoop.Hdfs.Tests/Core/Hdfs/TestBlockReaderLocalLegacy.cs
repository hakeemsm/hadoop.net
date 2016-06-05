using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestBlockReaderLocalLegacy
	{
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetupCluster()
		{
			DFSInputStream.tcpReadsDisabledForTesting = true;
			DomainSocket.DisableBindPathValidation();
		}

		/// <exception cref="System.IO.IOException"/>
		private static HdfsConfiguration GetConfiguration(TemporarySocketDirectory socketDir
			)
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			if (socketDir == null)
			{
				conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, string.Empty);
			}
			else
			{
				conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, new FilePath(socketDir.GetDir(), "TestBlockReaderLocalLegacy.%d.sock"
					).GetAbsolutePath());
			}
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientUseLegacyBlockreaderlocal, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, false);
			conf.Set(DFSConfigKeys.DfsBlockLocalPathAccessUserKey, UserGroupInformation.GetCurrentUser
				().GetShortUserName());
			conf.SetBoolean(DFSConfigKeys.DfsClientDomainSocketDataTraffic, false);
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			return conf;
		}

		/// <summary>
		/// Test that, in the case of an error, the position and limit of a ByteBuffer
		/// are left unchanged.
		/// </summary>
		/// <remarks>
		/// Test that, in the case of an error, the position and limit of a ByteBuffer
		/// are left unchanged. This is not mandated by ByteBufferReadable, but clients
		/// of this class might immediately issue a retry on failure, so it's polite.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStablePositionAfterCorruptRead()
		{
			short ReplFactor = 1;
			long FileLength = 512L;
			HdfsConfiguration conf = GetConfiguration(null);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			Path path = new Path("/corrupted");
			DFSTestUtil.CreateFile(fs, path, FileLength, ReplFactor, 12345L);
			DFSTestUtil.WaitReplication(fs, path, ReplFactor);
			ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, path);
			int blockFilesCorrupted = cluster.CorruptBlockOnDataNodes(block);
			NUnit.Framework.Assert.AreEqual("All replicas not corrupted", ReplFactor, blockFilesCorrupted
				);
			FSDataInputStream dis = cluster.GetFileSystem().Open(path);
			ByteBuffer buf = ByteBuffer.AllocateDirect((int)FileLength);
			bool sawException = false;
			try
			{
				dis.Read(buf);
			}
			catch (ChecksumException)
			{
				sawException = true;
			}
			NUnit.Framework.Assert.IsTrue(sawException);
			NUnit.Framework.Assert.AreEqual(0, buf.Position());
			NUnit.Framework.Assert.AreEqual(buf.Capacity(), buf.Limit());
			dis = cluster.GetFileSystem().Open(path);
			buf.Position(3);
			buf.Limit(25);
			sawException = false;
			try
			{
				dis.Read(buf);
			}
			catch (ChecksumException)
			{
				sawException = true;
			}
			NUnit.Framework.Assert.IsTrue(sawException);
			NUnit.Framework.Assert.AreEqual(3, buf.Position());
			NUnit.Framework.Assert.AreEqual(25, buf.Limit());
			cluster.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBothOldAndNewShortCircuitConfigured()
		{
			short ReplFactor = 1;
			int FileLength = 512;
			Assume.AssumeTrue(null == DomainSocket.GetLoadingFailureReason());
			TemporarySocketDirectory socketDir = new TemporarySocketDirectory();
			HdfsConfiguration conf = GetConfiguration(socketDir);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			socketDir.Close();
			FileSystem fs = cluster.GetFileSystem();
			Path path = new Path("/foo");
			byte[] orig = new byte[FileLength];
			for (int i = 0; i < orig.Length; i++)
			{
				orig[i] = unchecked((byte)(i % 10));
			}
			FSDataOutputStream fos = fs.Create(path, (short)1);
			fos.Write(orig);
			fos.Close();
			DFSTestUtil.WaitReplication(fs, path, ReplFactor);
			FSDataInputStream fis = cluster.GetFileSystem().Open(path);
			byte[] buf = new byte[FileLength];
			IOUtils.ReadFully(fis, buf, 0, FileLength);
			fis.Close();
			Assert.AssertArrayEquals(orig, buf);
			Arrays.Equals(orig, buf);
			cluster.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBlockReaderLocalLegacyWithAppend()
		{
			short ReplFactor = 1;
			HdfsConfiguration conf = GetConfiguration(null);
			conf.SetBoolean(DFSConfigKeys.DfsClientUseLegacyBlockreaderlocal, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			Path path = new Path("/testBlockReaderLocalLegacy");
			DFSTestUtil.CreateFile(dfs, path, 10, ReplFactor, 0);
			DFSTestUtil.WaitReplication(dfs, path, ReplFactor);
			ClientDatanodeProtocol proxy;
			Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token;
			ExtendedBlock originalBlock;
			long originalGS;
			{
				LocatedBlock lb = cluster.GetNameNode().GetRpcServer().GetBlockLocations(path.ToString
					(), 0, 1).Get(0);
				proxy = DFSUtil.CreateClientDatanodeProtocolProxy(lb.GetLocations()[0], conf, 60000
					, false);
				token = lb.GetBlockToken();
				// get block and generation stamp
				ExtendedBlock blk = new ExtendedBlock(lb.GetBlock());
				originalBlock = new ExtendedBlock(blk);
				originalGS = originalBlock.GetGenerationStamp();
				// test getBlockLocalPathInfo
				BlockLocalPathInfo info = proxy.GetBlockLocalPathInfo(blk, token);
				NUnit.Framework.Assert.AreEqual(originalGS, info.GetBlock().GetGenerationStamp());
			}
			{
				// append one byte
				FSDataOutputStream @out = dfs.Append(path);
				@out.Write(1);
				@out.Close();
			}
			{
				// get new generation stamp
				LocatedBlock lb = cluster.GetNameNode().GetRpcServer().GetBlockLocations(path.ToString
					(), 0, 1).Get(0);
				long newGS = lb.GetBlock().GetGenerationStamp();
				NUnit.Framework.Assert.IsTrue(newGS > originalGS);
				// getBlockLocalPathInfo using the original block.
				NUnit.Framework.Assert.AreEqual(originalGS, originalBlock.GetGenerationStamp());
				BlockLocalPathInfo info = proxy.GetBlockLocalPathInfo(originalBlock, token);
				NUnit.Framework.Assert.AreEqual(newGS, info.GetBlock().GetGenerationStamp());
			}
			cluster.Shutdown();
		}
	}
}
