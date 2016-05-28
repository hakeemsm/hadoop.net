using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests that blocks can be larger than 2GB</summary>
	public class TestLargeBlock
	{
		/// <summary>
		/// {
		/// ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
		/// ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
		/// ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
		/// ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
		/// ((Log4JLogger)TestLargeBlock.LOG).getLogger().setLevel(Level.ALL);
		/// }
		/// </summary>
		private static readonly Log Log = LogFactory.GetLog(typeof(TestLargeBlock));

		internal const bool verifyData = true;

		internal static readonly byte[] pattern = new byte[] { (byte)('D'), (byte)('E'), 
			(byte)('A'), (byte)('D'), (byte)('B'), (byte)('E'), (byte)('E'), (byte)('F') };

		internal const bool simulatedStorage = false;

		// should we verify the data read back from the file? (slow)
		// creates a file 
		/// <exception cref="System.IO.IOException"/>
		internal static FSDataOutputStream CreateFile(FileSystem fileSys, Path name, int 
			repl, long blockSize)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, blockSize);
			Log.Info("createFile: Created " + name + " with " + repl + " replica.");
			return stm;
		}

		/// <summary>Writes pattern to file</summary>
		/// <param name="stm">FSDataOutputStream to write the file</param>
		/// <param name="fileSize">size of the file to be written</param>
		/// <exception cref="System.IO.IOException">in case of errors</exception>
		internal static void WriteFile(FSDataOutputStream stm, long fileSize)
		{
			// write in chunks of 64 MB
			int writeSize = pattern.Length * 8 * 1024 * 1024;
			if (writeSize > int.MaxValue)
			{
				throw new IOException("A single write is too large " + writeSize);
			}
			long bytesToWrite = fileSize;
			byte[] b = new byte[writeSize];
			// initialize buffer
			for (int j = 0; j < writeSize; j++)
			{
				b[j] = pattern[j % pattern.Length];
			}
			while (bytesToWrite > 0)
			{
				// how many bytes we are writing in this iteration
				int thiswrite = (int)Math.Min(writeSize, bytesToWrite);
				stm.Write(b, 0, thiswrite);
				bytesToWrite -= thiswrite;
			}
		}

		/// <summary>Reads from file and makes sure that it matches the pattern</summary>
		/// <param name="fs">a reference to FileSystem</param>
		/// <param name="name">Path of a file</param>
		/// <param name="fileSize">size of the file</param>
		/// <exception cref="System.IO.IOException">in case of errors</exception>
		internal static void CheckFullFile(FileSystem fs, Path name, long fileSize)
		{
			// read in chunks of 128 MB
			int readSize = pattern.Length * 16 * 1024 * 1024;
			if (readSize > int.MaxValue)
			{
				throw new IOException("A single read is too large " + readSize);
			}
			byte[] b = new byte[readSize];
			long bytesToRead = fileSize;
			byte[] compb = new byte[readSize];
			// buffer with correct data for comparison
			// initialize compare buffer
			for (int j = 0; j < readSize; j++)
			{
				compb[j] = pattern[j % pattern.Length];
			}
			FSDataInputStream stm = fs.Open(name);
			while (bytesToRead > 0)
			{
				// how many bytes we are reading in this iteration
				int thisread = (int)Math.Min(readSize, bytesToRead);
				stm.ReadFully(b, 0, thisread);
				// verify data read
				if (thisread == readSize)
				{
					NUnit.Framework.Assert.IsTrue("file is corrupted at or after byte " + (fileSize -
						 bytesToRead), Arrays.Equals(b, compb));
				}
				else
				{
					// b was only partially filled by last read
					for (int k = 0; k < thisread; k++)
					{
						NUnit.Framework.Assert.IsTrue("file is corrupted at or after byte " + (fileSize -
							 bytesToRead), b[k] == compb[k]);
					}
				}
				Log.Debug("Before update: to read: " + bytesToRead + "; read already: " + thisread
					);
				bytesToRead -= thisread;
				Log.Debug("After  update: to read: " + bytesToRead + "; read already: " + thisread
					);
			}
			stm.Close();
		}

		/// <summary>Test for block size of 2GB + 512B.</summary>
		/// <remarks>
		/// Test for block size of 2GB + 512B. This test can take a rather long time to
		/// complete on Windows (reading the file back can be slow) so we use a larger
		/// timeout here.
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of errors</exception>
		public virtual void TestLargeBlockSize()
		{
			long blockSize = 2L * 1024L * 1024L * 1024L + 512L;
			// 2GB + 512B
			RunTest(blockSize);
		}

		/// <summary>Test that we can write to and read from large blocks</summary>
		/// <param name="blockSize">size of the block</param>
		/// <exception cref="System.IO.IOException">in case of errors</exception>
		public virtual void RunTest(long blockSize)
		{
			// write a file that is slightly larger than 1 block
			long fileSize = blockSize + 1L;
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				// create a new file in test data directory
				Path file1 = new Path("/tmp/TestLargeBlock", blockSize + ".dat");
				FSDataOutputStream stm = CreateFile(fs, file1, 1, blockSize);
				Log.Info("File " + file1 + " created with file size " + fileSize + " blocksize " 
					+ blockSize);
				// verify that file exists in FS namespace
				NUnit.Framework.Assert.IsTrue(file1 + " should be a file", fs.GetFileStatus(file1
					).IsFile());
				// write to file
				WriteFile(stm, fileSize);
				Log.Info("File " + file1 + " written to.");
				// close file
				stm.Close();
				Log.Info("File " + file1 + " closed.");
				// Make sure a client can read it
				CheckFullFile(fs, file1, fileSize);
				// verify that file size has changed
				long len = fs.GetFileStatus(file1).GetLen();
				NUnit.Framework.Assert.IsTrue(file1 + " should be of size " + fileSize + " but found to be of size "
					 + len, len == fileSize);
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
