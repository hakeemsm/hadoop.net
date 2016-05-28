using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests the creation of files with block-size
	/// smaller than the default buffer size of 4K.
	/// </summary>
	public class TestSmallBlock
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 1;

		internal const int fileSize = 20;

		internal bool simulatedStorage = false;

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name)
		{
			// create and write a file that contains three blocks of data
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)1, blockSize);
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

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

		/// <exception cref="System.IO.IOException"/>
		private void CheckFile(FileSystem fileSys, Path name)
		{
			BlockLocation[] locations = fileSys.GetFileBlockLocations(fileSys.GetFileStatus(name
				), 0, fileSize);
			NUnit.Framework.Assert.AreEqual("Number of blocks", fileSize, locations.Length);
			FSDataInputStream stm = fileSys.Open(name);
			byte[] expected = new byte[fileSize];
			if (simulatedStorage)
			{
				for (int i = 0; i < expected.Length; ++i)
				{
					expected[i] = SimulatedFSDataset.DefaultDatabyte;
				}
			}
			else
			{
				Random rand = new Random(seed);
				rand.NextBytes(expected);
			}
			// do a sanity check. Read the file
			byte[] actual = new byte[fileSize];
			stm.ReadFully(0, actual);
			CheckAndEraseData(actual, 0, expected, "Read Sanity Test");
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		/// <summary>Tests small block size in in DFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSmallBlock()
		{
			Configuration conf = new HdfsConfiguration();
			if (simulatedStorage)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			conf.Set(DFSConfigKeys.DfsBytesPerChecksumKey, "1");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fileSys = cluster.GetFileSystem();
			try
			{
				Path file1 = new Path("smallblocktest.dat");
				WriteFile(fileSys, file1);
				CheckFile(fileSys, file1);
				CleanupFile(fileSys, file1);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSmallBlockSimulatedStorage()
		{
			simulatedStorage = true;
			TestSmallBlock();
			simulatedStorage = false;
		}
	}
}
