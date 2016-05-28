using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestRead
	{
		private readonly int BlockSize = 512;

		/// <exception cref="System.IO.IOException"/>
		private void TestEOF(MiniDFSCluster cluster, int fileLength)
		{
			FileSystem fs = cluster.GetFileSystem();
			Path path = new Path("testEOF." + fileLength);
			DFSTestUtil.CreateFile(fs, path, fileLength, (short)1, unchecked((int)(0xBEEFBEEF
				)));
			FSDataInputStream fis = fs.Open(path);
			ByteBuffer empty = ByteBuffer.Allocate(0);
			// A read into an empty bytebuffer at the beginning of the file gives 0.
			NUnit.Framework.Assert.AreEqual(0, fis.Read(empty));
			fis.Seek(fileLength);
			// A read into an empty bytebuffer at the end of the file gives -1.
			NUnit.Framework.Assert.AreEqual(-1, fis.Read(empty));
			if (fileLength > BlockSize)
			{
				fis.Seek(fileLength - BlockSize + 1);
				ByteBuffer dbb = ByteBuffer.AllocateDirect(BlockSize);
				NUnit.Framework.Assert.AreEqual(BlockSize - 1, fis.Read(dbb));
			}
			fis.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEOFWithBlockReaderLocal()
		{
			DFSTestUtil.ShortCircuitTestContext testContext = new DFSTestUtil.ShortCircuitTestContext
				("testEOFWithBlockReaderLocal");
			try
			{
				Configuration conf = testContext.NewConfiguration();
				conf.SetLong(DFSConfigKeys.DfsClientCacheReadahead, BlockSize);
				MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
					true).Build();
				TestEOF(cluster, 1);
				TestEOF(cluster, 14);
				TestEOF(cluster, 10000);
				cluster.Shutdown();
			}
			finally
			{
				testContext.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEOFWithRemoteBlockReader()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsClientCacheReadahead, BlockSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			TestEOF(cluster, 1);
			TestEOF(cluster, 14);
			TestEOF(cluster, 10000);
			cluster.Shutdown();
		}

		/// <summary>Regression test for HDFS-7045.</summary>
		/// <remarks>
		/// Regression test for HDFS-7045.
		/// If deadlock happen, the test will time out.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestReadReservedPath()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				fs.Open(new Path("/.reserved/.inodes/file"));
				NUnit.Framework.Assert.Fail("Open a non existing file should fail.");
			}
			catch (FileNotFoundException)
			{
			}
			finally
			{
				// Expected
				cluster.Shutdown();
			}
		}
	}
}
