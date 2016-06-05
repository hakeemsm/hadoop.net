using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDataTransferKeepalive
	{
		internal readonly Configuration conf = new HdfsConfiguration();

		private MiniDFSCluster cluster;

		private DataNode dn;

		private static readonly Path TestFile = new Path("/test");

		private const int KeepaliveTimeout = 1000;

		private const int WriteTimeout = 3000;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf.SetInt(DFSConfigKeys.DfsDatanodeSocketReuseKeepaliveKey, KeepaliveTimeout);
			conf.SetInt(DFSConfigKeys.DfsClientMaxBlockAcquireFailuresKey, 0);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			dn = cluster.GetDataNodes()[0];
		}

		[TearDown]
		public virtual void Teardown()
		{
			cluster.Shutdown();
		}

		/// <summary>Regression test for HDFS-3357.</summary>
		/// <remarks>
		/// Regression test for HDFS-3357. Check that the datanode is respecting
		/// its configured keepalive timeout.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDatanodeRespectsKeepAliveTimeout()
		{
			Configuration clientConf = new Configuration(conf);
			// Set a client socket cache expiry time much longer than 
			// the datanode-side expiration time.
			long ClientExpiryMs = 60000L;
			clientConf.SetLong(DFSConfigKeys.DfsClientSocketCacheExpiryMsecKey, ClientExpiryMs
				);
			clientConf.Set(DFSConfigKeys.DfsClientContext, "testDatanodeRespectsKeepAliveTimeout"
				);
			DistributedFileSystem fs = (DistributedFileSystem)FileSystem.Get(cluster.GetURI()
				, clientConf);
			PeerCache peerCache = ClientContext.GetFromConf(clientConf).GetPeerCache();
			DFSTestUtil.CreateFile(fs, TestFile, 1L, (short)1, 0L);
			// Clients that write aren't currently re-used.
			NUnit.Framework.Assert.AreEqual(0, peerCache.Size());
			AssertXceiverCount(0);
			// Reads the file, so we should get a
			// cached socket, and should have an xceiver on the other side.
			DFSTestUtil.ReadFile(fs, TestFile);
			NUnit.Framework.Assert.AreEqual(1, peerCache.Size());
			AssertXceiverCount(1);
			// Sleep for a bit longer than the keepalive timeout
			// and make sure the xceiver died.
			Sharpen.Thread.Sleep(DFSConfigKeys.DfsDatanodeSocketReuseKeepaliveDefault + 50);
			AssertXceiverCount(0);
			// The socket is still in the cache, because we don't
			// notice that it's closed until we try to read
			// from it again.
			NUnit.Framework.Assert.AreEqual(1, peerCache.Size());
			// Take it out of the cache - reading should
			// give an EOF.
			Peer peer = peerCache.Get(dn.GetDatanodeId(), false);
			NUnit.Framework.Assert.IsNotNull(peer);
			NUnit.Framework.Assert.AreEqual(-1, peer.GetInputStream().Read());
		}

		/// <summary>Test that the client respects its keepalive timeout.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestClientResponsesKeepAliveTimeout()
		{
			Configuration clientConf = new Configuration(conf);
			// Set a client socket cache expiry time much shorter than 
			// the datanode-side expiration time.
			long ClientExpiryMs = 10L;
			clientConf.SetLong(DFSConfigKeys.DfsClientSocketCacheExpiryMsecKey, ClientExpiryMs
				);
			clientConf.Set(DFSConfigKeys.DfsClientContext, "testClientResponsesKeepAliveTimeout"
				);
			DistributedFileSystem fs = (DistributedFileSystem)FileSystem.Get(cluster.GetURI()
				, clientConf);
			PeerCache peerCache = ClientContext.GetFromConf(clientConf).GetPeerCache();
			DFSTestUtil.CreateFile(fs, TestFile, 1L, (short)1, 0L);
			// Clients that write aren't currently re-used.
			NUnit.Framework.Assert.AreEqual(0, peerCache.Size());
			AssertXceiverCount(0);
			// Reads the file, so we should get a
			// cached socket, and should have an xceiver on the other side.
			DFSTestUtil.ReadFile(fs, TestFile);
			NUnit.Framework.Assert.AreEqual(1, peerCache.Size());
			AssertXceiverCount(1);
			// Sleep for a bit longer than the client keepalive timeout.
			Sharpen.Thread.Sleep(ClientExpiryMs + 50);
			// Taking out a peer which is expired should give a null.
			Peer peer = peerCache.Get(dn.GetDatanodeId(), false);
			NUnit.Framework.Assert.IsTrue(peer == null);
			// The socket cache is now empty.
			NUnit.Framework.Assert.AreEqual(0, peerCache.Size());
		}

		/// <summary>
		/// Test for the case where the client beings to read a long block, but doesn't
		/// read bytes off the stream quickly.
		/// </summary>
		/// <remarks>
		/// Test for the case where the client beings to read a long block, but doesn't
		/// read bytes off the stream quickly. The datanode should time out sending the
		/// chunks and the transceiver should die, even if it has a long keepalive.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestSlowReader()
		{
			// Set a client socket cache expiry time much longer than 
			// the datanode-side expiration time.
			long ClientExpiryMs = 600000L;
			Configuration clientConf = new Configuration(conf);
			clientConf.SetLong(DFSConfigKeys.DfsClientSocketCacheExpiryMsecKey, ClientExpiryMs
				);
			clientConf.Set(DFSConfigKeys.DfsClientContext, "testSlowReader");
			DistributedFileSystem fs = (DistributedFileSystem)FileSystem.Get(cluster.GetURI()
				, clientConf);
			// Restart the DN with a shorter write timeout.
			MiniDFSCluster.DataNodeProperties props = cluster.StopDataNode(0);
			props.conf.SetInt(DFSConfigKeys.DfsDatanodeSocketWriteTimeoutKey, WriteTimeout);
			props.conf.SetInt(DFSConfigKeys.DfsDatanodeSocketReuseKeepaliveKey, 120000);
			NUnit.Framework.Assert.IsTrue(cluster.RestartDataNode(props, true));
			dn = cluster.GetDataNodes()[0];
			// Wait for heartbeats to avoid a startup race where we
			// try to write the block while the DN is still starting.
			cluster.TriggerHeartbeats();
			DFSTestUtil.CreateFile(fs, TestFile, 1024 * 1024 * 8L, (short)1, 0L);
			FSDataInputStream stm = fs.Open(TestFile);
			stm.Read();
			AssertXceiverCount(1);
			GenericTestUtils.WaitFor(new _Supplier_193(this), 500, 50000);
			// DN should time out in sendChunks, and this should force
			// the xceiver to exit.
			IOUtils.CloseStream(stm);
		}

		private sealed class _Supplier_193 : Supplier<bool>
		{
			public _Supplier_193(TestDataTransferKeepalive _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public bool Get()
			{
				return this._enclosing.GetXceiverCountWithoutServer() == 0;
			}

			private readonly TestDataTransferKeepalive _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestManyClosedSocketsInCache()
		{
			// Make a small file
			Configuration clientConf = new Configuration(conf);
			clientConf.Set(DFSConfigKeys.DfsClientContext, "testManyClosedSocketsInCache");
			DistributedFileSystem fs = (DistributedFileSystem)FileSystem.Get(cluster.GetURI()
				, clientConf);
			PeerCache peerCache = ClientContext.GetFromConf(clientConf).GetPeerCache();
			DFSTestUtil.CreateFile(fs, TestFile, 1L, (short)1, 0L);
			// Insert a bunch of dead sockets in the cache, by opening
			// many streams concurrently, reading all of the data,
			// and then closing them.
			InputStream[] stms = new InputStream[5];
			try
			{
				for (int i = 0; i < stms.Length; i++)
				{
					stms[i] = fs.Open(TestFile);
				}
				foreach (InputStream stm in stms)
				{
					IOUtils.CopyBytes(stm, new IOUtils.NullOutputStream(), 1024);
				}
			}
			finally
			{
				IOUtils.Cleanup(null, stms);
			}
			NUnit.Framework.Assert.AreEqual(5, peerCache.Size());
			// Let all the xceivers timeout
			Sharpen.Thread.Sleep(1500);
			AssertXceiverCount(0);
			// Client side still has the sockets cached
			NUnit.Framework.Assert.AreEqual(5, peerCache.Size());
			// Reading should not throw an exception.
			DFSTestUtil.ReadFile(fs, TestFile);
		}

		private void AssertXceiverCount(int expected)
		{
			int count = GetXceiverCountWithoutServer();
			if (count != expected)
			{
				ReflectionUtils.PrintThreadInfo(System.Console.Error, "Thread dumps");
				NUnit.Framework.Assert.Fail("Expected " + expected + " xceivers, found " + count);
			}
		}

		/// <summary>
		/// Returns the datanode's xceiver count, but subtracts 1, since the
		/// DataXceiverServer counts as one.
		/// </summary>
		/// <returns>int xceiver count, not including DataXceiverServer</returns>
		private int GetXceiverCountWithoutServer()
		{
			return dn.GetXceiverCount() - 1;
		}
	}
}
