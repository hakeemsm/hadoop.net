using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests the client connection caching in a single node
	/// mini-cluster.
	/// </summary>
	public class TestConnCache
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestConnCache));

		internal const int BlockSize = 4096;

		internal const int FileSize = 3 * BlockSize;

		/// <summary>(Optionally) seek to position, read and verify data.</summary>
		/// <remarks>
		/// (Optionally) seek to position, read and verify data.
		/// Seek to specified position if pos is non-negative.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void Pread(DFSInputStream @in, long pos, byte[] buffer, int offset, int length
			, byte[] authenticData)
		{
			NUnit.Framework.Assert.IsTrue("Test buffer too small", buffer.Length >= offset + 
				length);
			if (pos >= 0)
			{
				@in.Seek(pos);
			}
			Log.Info("Reading from file of size " + @in.GetFileLength() + " at offset " + @in
				.GetPos());
			while (length > 0)
			{
				int cnt = @in.Read(buffer, offset, length);
				NUnit.Framework.Assert.IsTrue("Error in read", cnt > 0);
				offset += cnt;
				length -= cnt;
			}
			// Verify
			for (int i = 0; i < length; ++i)
			{
				byte actual = buffer[i];
				byte expect = authenticData[(int)pos + i];
				NUnit.Framework.Assert.AreEqual("Read data mismatch at file offset " + (pos + i) 
					+ ". Expects " + expect + "; got " + actual, actual, expect);
			}
		}

		/// <summary>Read a file served entirely from one DN.</summary>
		/// <remarks>
		/// Read a file served entirely from one DN. Seek around and read from
		/// different offsets. And verify that they all use the same socket.
		/// </remarks>
		/// <exception cref="System.Exception"></exception>
		[NUnit.Framework.Test]
		public virtual void TestReadFromOneDN()
		{
			HdfsConfiguration configuration = new HdfsConfiguration();
			// One of the goals of this test is to verify that we don't open more
			// than one socket.  So use a different client context, so that we
			// get our own socket cache, rather than sharing with the other test 
			// instances.  Also use a really long socket timeout so that nothing
			// gets closed before we get around to checking the cache size at the end.
			string contextName = "testReadFromOneDNContext";
			configuration.Set(DFSConfigKeys.DfsClientContext, contextName);
			configuration.SetLong(DFSConfigKeys.DfsClientSocketTimeoutKey, 100000000L);
			BlockReaderTestUtil util = new BlockReaderTestUtil(1, configuration);
			Path testFile = new Path("/testConnCache.dat");
			byte[] authenticData = util.WriteFile(testFile, FileSize / 1024);
			DFSClient client = new DFSClient(new IPEndPoint("localhost", util.GetCluster().GetNameNodePort
				()), util.GetConf());
			ClientContext cacheContext = ClientContext.Get(contextName, client.GetConf());
			DFSInputStream @in = client.Open(testFile.ToString());
			Log.Info("opened " + testFile.ToString());
			byte[] dataBuf = new byte[BlockSize];
			// Initial read
			Pread(@in, 0, dataBuf, 0, dataBuf.Length, authenticData);
			// Read again and verify that the socket is the same
			Pread(@in, FileSize - dataBuf.Length, dataBuf, 0, dataBuf.Length, authenticData);
			Pread(@in, 1024, dataBuf, 0, dataBuf.Length, authenticData);
			// No seek; just read
			Pread(@in, -1, dataBuf, 0, dataBuf.Length, authenticData);
			Pread(@in, 64, dataBuf, 0, dataBuf.Length / 2, authenticData);
			@in.Close();
			client.Close();
			NUnit.Framework.Assert.AreEqual(1, ClientContext.GetFromConf(configuration).GetPeerCache
				().Size());
		}
	}
}
