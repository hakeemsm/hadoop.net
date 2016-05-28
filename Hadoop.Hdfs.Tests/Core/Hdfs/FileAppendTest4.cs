using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This is a comprehensive append test that tries
	/// all combinations of file length and number of appended bytes
	/// In each iteration, it creates a file of len1.
	/// </summary>
	/// <remarks>
	/// This is a comprehensive append test that tries
	/// all combinations of file length and number of appended bytes
	/// In each iteration, it creates a file of len1. Then reopen
	/// the file for append. It first append len2 bytes, calls hflush,
	/// append len3 bytes and close the file. Afterwards, the content of
	/// the file is validated.
	/// Len1 ranges from [0, 2*BLOCK_SIZE+1], len2 ranges from [0, BLOCK_SIZE+1],
	/// and len3 ranges from [0, BLOCK_SIZE+1].
	/// </remarks>
	public class FileAppendTest4
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(FileAppendTest4));

		private const int BytesPerChecksum = 4;

		private const int PacketSize = BytesPerChecksum;

		private const int BlockSize = 2 * PacketSize;

		private const short Replication = 3;

		private const int DatanodeNum = 5;

		private static Configuration conf;

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem fs;

		private static void Init(Configuration conf)
		{
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, BytesPerChecksum);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsClientWritePacketSizeKey, PacketSize);
		}

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void StartUp()
		{
			conf = new HdfsConfiguration();
			Init(conf);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DatanodeNum).Build();
			fs = cluster.GetFileSystem();
		}

		[AfterClass]
		public static void TearDown()
		{
			cluster.Shutdown();
		}

		/// <summary>Comprehensive test for append</summary>
		/// <exception cref="System.IO.IOException">an exception might be thrown</exception>
		[NUnit.Framework.Test]
		public virtual void TestAppend()
		{
			int maxOldFileLen = 2 * BlockSize + 1;
			int maxFlushedBytes = BlockSize;
			byte[] contents = AppendTestUtil.InitBuffer(maxOldFileLen + 2 * maxFlushedBytes);
			for (int oldFileLen = 0; oldFileLen <= maxOldFileLen; oldFileLen++)
			{
				for (int flushedBytes1 = 0; flushedBytes1 <= maxFlushedBytes; flushedBytes1++)
				{
					for (int flushedBytes2 = 0; flushedBytes2 <= maxFlushedBytes; flushedBytes2++)
					{
						int fileLen = oldFileLen + flushedBytes1 + flushedBytes2;
						// create the initial file of oldFileLen
						Path p = new Path("foo" + oldFileLen + "_" + flushedBytes1 + "_" + flushedBytes2);
						Log.Info("Creating file " + p);
						FSDataOutputStream @out = fs.Create(p, false, conf.GetInt(CommonConfigurationKeys
							.IoFileBufferSizeKey, 4096), Replication, BlockSize);
						@out.Write(contents, 0, oldFileLen);
						@out.Close();
						// append flushedBytes bytes to the file
						@out = fs.Append(p);
						@out.Write(contents, oldFileLen, flushedBytes1);
						@out.Hflush();
						// write another flushedBytes2 bytes to the file
						@out.Write(contents, oldFileLen + flushedBytes1, flushedBytes2);
						@out.Close();
						// validate the file content
						AppendTestUtil.CheckFullFile(fs, p, fileLen, contents, p.ToString());
						fs.Delete(p, false);
					}
				}
			}
		}
	}
}
