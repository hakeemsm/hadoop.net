using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// Test cases for trying to append to a file with a different
	/// checksum than the file was originally written with.
	/// </summary>
	public class TestAppendDifferentChecksum
	{
		private const int SegmentLength = 1500;

		private const long RandomTestRuntime = 5000;

		private static MiniDFSCluster cluster;

		private static FileSystem fs;

		// run the randomized test for 5 seconds
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetupCluster()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, 4096);
			conf.Set("fs.hdfs.impl.disable.cache", "true");
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void Teardown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// This test does not run, since switching chunksize with append
		/// is not implemented.
		/// </summary>
		/// <remarks>
		/// This test does not run, since switching chunksize with append
		/// is not implemented. Please see HDFS-2130 for a discussion of the
		/// difficulties in doing so.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSwitchChunkSize()
		{
			FileSystem fsWithSmallChunk = CreateFsWithChecksum("CRC32", 512);
			FileSystem fsWithBigChunk = CreateFsWithChecksum("CRC32", 1024);
			Path p = new Path("/testSwitchChunkSize");
			AppendWithTwoFs(p, fsWithSmallChunk, fsWithBigChunk);
			AppendTestUtil.Check(fsWithSmallChunk, p, SegmentLength * 2);
			AppendTestUtil.Check(fsWithBigChunk, p, SegmentLength * 2);
		}

		/// <summary>
		/// Simple unit test which writes some data with one algorithm,
		/// then appends with another.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSwitchAlgorithms()
		{
			FileSystem fsWithCrc32 = CreateFsWithChecksum("CRC32", 512);
			FileSystem fsWithCrc32C = CreateFsWithChecksum("CRC32C", 512);
			Path p = new Path("/testSwitchAlgorithms");
			AppendWithTwoFs(p, fsWithCrc32, fsWithCrc32C);
			// Regardless of which FS is used to read, it should pick up
			// the on-disk checksum!
			AppendTestUtil.Check(fsWithCrc32C, p, SegmentLength * 2);
			AppendTestUtil.Check(fsWithCrc32, p, SegmentLength * 2);
		}

		/// <summary>
		/// Test which randomly alternates between appending with
		/// CRC32 and with CRC32C, crossing several block boundaries.
		/// </summary>
		/// <remarks>
		/// Test which randomly alternates between appending with
		/// CRC32 and with CRC32C, crossing several block boundaries.
		/// Then, checks that all of the data can be read back correct.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAlgoSwitchRandomized()
		{
			FileSystem fsWithCrc32 = CreateFsWithChecksum("CRC32", 512);
			FileSystem fsWithCrc32C = CreateFsWithChecksum("CRC32C", 512);
			Path p = new Path("/testAlgoSwitchRandomized");
			long seed = Time.Now();
			System.Console.Out.WriteLine("seed: " + seed);
			Random r = new Random(seed);
			// Create empty to start
			IOUtils.CloseStream(fsWithCrc32.Create(p));
			long st = Time.Now();
			int len = 0;
			while (Time.Now() - st < RandomTestRuntime)
			{
				int thisLen = r.Next(500);
				FileSystem fs = (r.NextBoolean() ? fsWithCrc32 : fsWithCrc32C);
				FSDataOutputStream stm = fs.Append(p);
				try
				{
					AppendTestUtil.Write(stm, len, thisLen);
				}
				finally
				{
					stm.Close();
				}
				len += thisLen;
			}
			AppendTestUtil.Check(fsWithCrc32, p, len);
			AppendTestUtil.Check(fsWithCrc32C, p, len);
		}

		/// <exception cref="System.IO.IOException"/>
		private FileSystem CreateFsWithChecksum(string type, int bytes)
		{
			Configuration conf = new Configuration(fs.GetConf());
			conf.Set(DFSConfigKeys.DfsChecksumTypeKey, type);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, bytes);
			return FileSystem.Get(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void AppendWithTwoFs(Path p, FileSystem fs1, FileSystem fs2)
		{
			FSDataOutputStream stm = fs1.Create(p);
			try
			{
				AppendTestUtil.Write(stm, 0, SegmentLength);
			}
			finally
			{
				stm.Close();
			}
			stm = fs2.Append(p);
			try
			{
				AppendTestUtil.Write(stm, SegmentLength, SegmentLength);
			}
			finally
			{
				stm.Close();
			}
		}
	}
}
