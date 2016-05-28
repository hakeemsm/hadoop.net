using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestUnbuffer
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestUnbuffer).FullName
			);

		/// <summary>Test that calling Unbuffer closes sockets.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnbufferClosesSockets()
		{
			Configuration conf = new Configuration();
			// Set a new ClientContext.  This way, we will have our own PeerCache,
			// rather than sharing one with other unit tests.
			conf.Set(DFSConfigKeys.DfsClientContext, "testUnbufferClosesSocketsContext");
			// Disable short-circuit reads.  With short-circuit, we wouldn't hold open a
			// TCP socket.
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, false);
			// Set a really long socket timeout to avoid test timing issues.
			conf.SetLong(DFSConfigKeys.DfsClientSocketTimeoutKey, 100000000L);
			conf.SetLong(DFSConfigKeys.DfsClientSocketCacheExpiryMsecKey, 100000000L);
			MiniDFSCluster cluster = null;
			FSDataInputStream stream = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				DistributedFileSystem dfs = (DistributedFileSystem)FileSystem.NewInstance(conf);
				Path TestPath = new Path("/test1");
				DFSTestUtil.CreateFile(dfs, TestPath, 128, (short)1, 1);
				stream = dfs.Open(TestPath);
				// Read a byte.  This will trigger the creation of a block reader.
				stream.Seek(2);
				int b = stream.Read();
				NUnit.Framework.Assert.IsTrue(-1 != b);
				// The Peer cache should start off empty.
				PeerCache cache = dfs.GetClient().GetClientContext().GetPeerCache();
				NUnit.Framework.Assert.AreEqual(0, cache.Size());
				// Unbuffer should clear the block reader and return the socket to the
				// cache.
				stream.Unbuffer();
				stream.Seek(2);
				NUnit.Framework.Assert.AreEqual(1, cache.Size());
				int b2 = stream.Read();
				NUnit.Framework.Assert.AreEqual(b, b2);
			}
			finally
			{
				if (stream != null)
				{
					IOUtils.Cleanup(null, stream);
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test opening many files via TCP (not short-circuit).</summary>
		/// <remarks>
		/// Test opening many files via TCP (not short-circuit).
		/// This is practical when using unbuffer, because it reduces the number of
		/// sockets and amount of memory that we use.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOpenManyFilesViaTcp()
		{
			int NumOpens = 500;
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, false);
			MiniDFSCluster cluster = null;
			FSDataInputStream[] streams = new FSDataInputStream[NumOpens];
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				Path TestPath = new Path("/testFile");
				DFSTestUtil.CreateFile(dfs, TestPath, 131072, (short)1, 1);
				for (int i = 0; i < NumOpens; i++)
				{
					streams[i] = dfs.Open(TestPath);
					Log.Info("opening file " + i + "...");
					NUnit.Framework.Assert.IsTrue(-1 != streams[i].Read());
					streams[i].Unbuffer();
				}
			}
			finally
			{
				foreach (FSDataInputStream stream in streams)
				{
					IOUtils.Cleanup(null, stream);
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
