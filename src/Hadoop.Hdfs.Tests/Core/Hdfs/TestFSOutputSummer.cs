using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests if FSOutputSummer works correctly.</summary>
	public class TestFSOutputSummer
	{
		private const long seed = unchecked((long)(0xDEADBEEFL));

		private const int BytesPerChecksum = 10;

		private const int BlockSize = 2 * BytesPerChecksum;

		private const int HalfChunkSize = BytesPerChecksum / 2;

		private const int FileSize = 2 * BlockSize - 1;

		private const short NumOfDatanodes = 2;

		private readonly byte[] expected = new byte[FileSize];

		private readonly byte[] actual = new byte[FileSize];

		private FileSystem fileSys;

		/* create a file, write all data at once */
		/// <exception cref="System.Exception"/>
		private void WriteFile1(Path name)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeysPublic
				.IoFileBufferSizeKey, 4096), NumOfDatanodes, BlockSize);
			stm.Write(expected);
			stm.Close();
			CheckFile(name);
			CleanupFile(name);
		}

		/* create a file, write data chunk by chunk */
		/// <exception cref="System.Exception"/>
		private void WriteFile2(Path name)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeysPublic
				.IoFileBufferSizeKey, 4096), NumOfDatanodes, BlockSize);
			int i = 0;
			for (; i < FileSize - BytesPerChecksum; i += BytesPerChecksum)
			{
				stm.Write(expected, i, BytesPerChecksum);
			}
			stm.Write(expected, i, FileSize - 3 * BytesPerChecksum);
			stm.Close();
			CheckFile(name);
			CleanupFile(name);
		}

		/* create a file, write data with variable amount of data */
		/// <exception cref="System.Exception"/>
		private void WriteFile3(Path name)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeysPublic
				.IoFileBufferSizeKey, 4096), NumOfDatanodes, BlockSize);
			stm.Write(expected, 0, HalfChunkSize);
			stm.Write(expected, HalfChunkSize, BytesPerChecksum + 2);
			stm.Write(expected, HalfChunkSize + BytesPerChecksum + 2, 2);
			stm.Write(expected, HalfChunkSize + BytesPerChecksum + 4, HalfChunkSize);
			stm.Write(expected, BlockSize + 4, BytesPerChecksum - 4);
			stm.Write(expected, BlockSize + BytesPerChecksum, FileSize - 3 * BytesPerChecksum
				);
			stm.Close();
			CheckFile(name);
			CleanupFile(name);
		}

		/// <exception cref="System.Exception"/>
		private void CheckAndEraseData(byte[] actual, int from, byte[] expected, string message
			)
		{
			for (int idx = 0; idx < actual.Length; idx++)
			{
				NUnit.Framework.Assert.AreEqual(message + " byte " + (from + idx) + " differs. expected "
					 + expected[from + idx] + " actual " + actual[idx], actual[idx], expected[from +
					 idx]);
				actual[idx] = 0;
			}
		}

		/// <exception cref="System.Exception"/>
		private void CheckFile(Path name)
		{
			FSDataInputStream stm = fileSys.Open(name);
			// do a sanity check. Read the file
			stm.ReadFully(0, actual);
			CheckAndEraseData(actual, 0, expected, "Read Sanity Test");
			stm.Close();
			// do a sanity check. Get the file checksum
			fileSys.GetFileChecksum(name);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		/// <summary>Test write operation for output stream in DFS.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFSOutputSummer()
		{
			DoTestFSOutputSummer("CRC32");
			DoTestFSOutputSummer("CRC32C");
			DoTestFSOutputSummer("NULL");
		}

		/// <exception cref="System.Exception"/>
		private void DoTestFSOutputSummer(string checksumType)
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, BytesPerChecksum);
			conf.Set(DFSConfigKeys.DfsChecksumTypeKey, checksumType);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumOfDatanodes
				).Build();
			fileSys = cluster.GetFileSystem();
			try
			{
				Path file = new Path("try.dat");
				Random rand = new Random(seed);
				rand.NextBytes(expected);
				WriteFile1(file);
				WriteFile2(file);
				WriteFile3(file);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDFSCheckSumType()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, BytesPerChecksum);
			conf.Set(DFSConfigKeys.DfsChecksumTypeKey, "NULL");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumOfDatanodes
				).Build();
			fileSys = cluster.GetFileSystem();
			try
			{
				Path file = new Path("try.dat");
				Random rand = new Random(seed);
				rand.NextBytes(expected);
				WriteFile1(file);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}
	}
}
