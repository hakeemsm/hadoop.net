using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests if FSInputChecker works correctly.</summary>
	public class TestFSInputChecker
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int BytesPerSum = 10;

		internal const int BlockSize = 2 * BytesPerSum;

		internal const int HalfChunkSize = BytesPerSum / 2;

		internal const int FileSize = 2 * BlockSize - 1;

		internal const short NumOfDatanodes = 2;

		internal readonly byte[] expected = new byte[FileSize];

		internal byte[] actual;

		internal FSDataInputStream stm;

		internal readonly Random rand = new Random(seed);

		/* create a file */
		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name)
		{
			// create and write a file that contains three blocks of data
			FSDataOutputStream stm = fileSys.Create(name, new FsPermission((short)0x1ff), true
				, fileSys.GetConf().GetInt(CommonConfigurationKeys.IoFileBufferSizeKey, 4096), NumOfDatanodes
				, BlockSize, null);
			stm.Write(expected);
			stm.Close();
		}

		/*validate data*/
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

		/* test read and getPos */
		/// <exception cref="System.Exception"/>
		private void CheckReadAndGetPos()
		{
			actual = new byte[FileSize];
			// test reads that do not cross checksum boundary
			stm.Seek(0);
			int offset;
			for (offset = 0; offset < BlockSize + BytesPerSum; offset += BytesPerSum)
			{
				NUnit.Framework.Assert.AreEqual(stm.GetPos(), offset);
				stm.ReadFully(actual, offset, BytesPerSum);
			}
			stm.ReadFully(actual, offset, FileSize - BlockSize - BytesPerSum);
			NUnit.Framework.Assert.AreEqual(stm.GetPos(), FileSize);
			CheckAndEraseData(actual, 0, expected, "Read Sanity Test");
			// test reads that cross checksum boundary
			stm.Seek(0L);
			NUnit.Framework.Assert.AreEqual(stm.GetPos(), 0L);
			stm.ReadFully(actual, 0, HalfChunkSize);
			NUnit.Framework.Assert.AreEqual(stm.GetPos(), HalfChunkSize);
			stm.ReadFully(actual, HalfChunkSize, BlockSize - HalfChunkSize);
			NUnit.Framework.Assert.AreEqual(stm.GetPos(), BlockSize);
			stm.ReadFully(actual, BlockSize, BytesPerSum + HalfChunkSize);
			NUnit.Framework.Assert.AreEqual(stm.GetPos(), BlockSize + BytesPerSum + HalfChunkSize
				);
			stm.ReadFully(actual, 2 * BlockSize - HalfChunkSize, FileSize - (2 * BlockSize - 
				HalfChunkSize));
			NUnit.Framework.Assert.AreEqual(stm.GetPos(), FileSize);
			CheckAndEraseData(actual, 0, expected, "Read Sanity Test");
			// test read that cross block boundary
			stm.Seek(0L);
			stm.ReadFully(actual, 0, BytesPerSum + HalfChunkSize);
			NUnit.Framework.Assert.AreEqual(stm.GetPos(), BytesPerSum + HalfChunkSize);
			stm.ReadFully(actual, BytesPerSum + HalfChunkSize, BytesPerSum);
			NUnit.Framework.Assert.AreEqual(stm.GetPos(), BlockSize + HalfChunkSize);
			stm.ReadFully(actual, BlockSize + HalfChunkSize, FileSize - BlockSize - HalfChunkSize
				);
			NUnit.Framework.Assert.AreEqual(stm.GetPos(), FileSize);
			CheckAndEraseData(actual, 0, expected, "Read Sanity Test");
		}

		/* test if one seek is correct */
		/// <exception cref="System.Exception"/>
		private void TestSeek1(int offset)
		{
			stm.Seek(offset);
			NUnit.Framework.Assert.AreEqual(offset, stm.GetPos());
			stm.ReadFully(actual);
			CheckAndEraseData(actual, offset, expected, "Read Sanity Test");
		}

		/* test seek() */
		/// <exception cref="System.Exception"/>
		private void CheckSeek()
		{
			actual = new byte[HalfChunkSize];
			// test seeks to checksum boundary
			TestSeek1(0);
			TestSeek1(BytesPerSum);
			TestSeek1(BlockSize);
			// test seek to non-checksum-boundary pos
			TestSeek1(BlockSize + HalfChunkSize);
			TestSeek1(HalfChunkSize);
			// test seek to a position at the same checksum chunk
			TestSeek1(HalfChunkSize / 2);
			TestSeek1(HalfChunkSize * 3 / 2);
			// test end of file
			actual = new byte[1];
			TestSeek1(FileSize - 1);
			string errMsg = null;
			try
			{
				stm.Seek(FileSize);
			}
			catch (IOException e)
			{
				errMsg = e.Message;
			}
			NUnit.Framework.Assert.IsTrue(errMsg == null);
		}

		/* test if one skip is correct */
		/// <exception cref="System.Exception"/>
		private void TestSkip1(int skippedBytes)
		{
			long oldPos = stm.GetPos();
			IOUtils.SkipFully(stm, skippedBytes);
			long newPos = oldPos + skippedBytes;
			NUnit.Framework.Assert.AreEqual(stm.GetPos(), newPos);
			stm.ReadFully(actual);
			CheckAndEraseData(actual, (int)newPos, expected, "Read Sanity Test");
		}

		/* test skip() */
		/// <exception cref="System.Exception"/>
		private void CheckSkip()
		{
			actual = new byte[HalfChunkSize];
			// test skip to a checksum boundary
			stm.Seek(0);
			TestSkip1(BytesPerSum);
			TestSkip1(HalfChunkSize);
			TestSkip1(HalfChunkSize);
			// test skip to non-checksum-boundary pos
			stm.Seek(0);
			TestSkip1(HalfChunkSize + 1);
			TestSkip1(BytesPerSum);
			TestSkip1(HalfChunkSize);
			// test skip to a position at the same checksum chunk
			stm.Seek(0);
			TestSkip1(1);
			TestSkip1(1);
			// test skip to end of file
			stm.Seek(0);
			actual = new byte[1];
			TestSkip1(FileSize - 1);
			stm.Seek(0);
			IOUtils.SkipFully(stm, FileSize);
			try
			{
				IOUtils.SkipFully(stm, 10);
				NUnit.Framework.Assert.Fail("expected to get a PrematureEOFException");
			}
			catch (EOFException e)
			{
				NUnit.Framework.Assert.AreEqual(e.Message, "Premature EOF from inputStream " + "after skipping 0 byte(s)."
					);
			}
			stm.Seek(0);
			try
			{
				IOUtils.SkipFully(stm, FileSize + 10);
				NUnit.Framework.Assert.Fail("expected to get a PrematureEOFException");
			}
			catch (EOFException e)
			{
				NUnit.Framework.Assert.AreEqual(e.Message, "Premature EOF from inputStream " + "after skipping "
					 + FileSize + " byte(s).");
			}
			stm.Seek(10);
			try
			{
				IOUtils.SkipFully(stm, FileSize);
				NUnit.Framework.Assert.Fail("expected to get a PrematureEOFException");
			}
			catch (EOFException e)
			{
				NUnit.Framework.Assert.AreEqual(e.Message, "Premature EOF from inputStream " + "after skipping "
					 + (FileSize - 10) + " byte(s).");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		/// <summary>Tests read/seek/getPos/skipped opeation for input stream.</summary>
		/// <exception cref="System.Exception"/>
		private void TestChecker(FileSystem fileSys, bool readCS)
		{
			Path file = new Path("try.dat");
			WriteFile(fileSys, file);
			try
			{
				if (!readCS)
				{
					fileSys.SetVerifyChecksum(false);
				}
				stm = fileSys.Open(file);
				CheckReadAndGetPos();
				CheckSeek();
				CheckSkip();
				//checkMark
				NUnit.Framework.Assert.IsFalse(stm.MarkSupported());
				stm.Close();
			}
			finally
			{
				if (!readCS)
				{
					fileSys.SetVerifyChecksum(true);
				}
				CleanupFile(fileSys, file);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestFileCorruption(LocalFileSystem fileSys)
		{
			// create a file and verify that checksum corruption results in 
			// a checksum exception on LocalFS
			string dir = PathUtils.GetTestDirName(GetType());
			Path file = new Path(dir + "/corruption-test.dat");
			Path crcFile = new Path(dir + "/.corruption-test.dat.crc");
			WriteFile(fileSys, file);
			int fileLen = (int)fileSys.GetFileStatus(file).GetLen();
			byte[] buf = new byte[fileLen];
			InputStream @in = fileSys.Open(file);
			IOUtils.ReadFully(@in, buf, 0, buf.Length);
			@in.Close();
			// check .crc corruption
			CheckFileCorruption(fileSys, file, crcFile);
			fileSys.Delete(file, true);
			WriteFile(fileSys, file);
			// check data corrutpion
			CheckFileCorruption(fileSys, file, file);
			fileSys.Delete(file, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckFileCorruption(LocalFileSystem fileSys, Path file, Path fileToCorrupt
			)
		{
			// corrupt the file 
			RandomAccessFile @out = new RandomAccessFile(new FilePath(fileToCorrupt.ToString(
				)), "rw");
			byte[] buf = new byte[(int)fileSys.GetFileStatus(file).GetLen()];
			int corruptFileLen = (int)fileSys.GetFileStatus(fileToCorrupt).GetLen();
			NUnit.Framework.Assert.IsTrue(buf.Length >= corruptFileLen);
			rand.NextBytes(buf);
			@out.Seek(corruptFileLen / 2);
			@out.Write(buf, 0, corruptFileLen / 4);
			@out.Close();
			bool gotException = false;
			InputStream @in = fileSys.Open(file);
			try
			{
				IOUtils.ReadFully(@in, buf, 0, buf.Length);
			}
			catch (ChecksumException)
			{
				gotException = true;
			}
			NUnit.Framework.Assert.IsTrue(gotException);
			@in.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFSInputChecker()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, BytesPerSum);
			rand.NextBytes(expected);
			// test DFS
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fileSys = cluster.GetFileSystem();
			try
			{
				TestChecker(fileSys, true);
				TestChecker(fileSys, false);
				TestSeekAndRead(fileSys);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
			// test Local FS
			fileSys = FileSystem.GetLocal(conf);
			try
			{
				TestChecker(fileSys, true);
				TestChecker(fileSys, false);
				TestFileCorruption((LocalFileSystem)fileSys);
				TestSeekAndRead(fileSys);
			}
			finally
			{
				fileSys.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestSeekAndRead(FileSystem fileSys)
		{
			Path file = new Path("try.dat");
			WriteFile(fileSys, file);
			stm = fileSys.Open(file, fileSys.GetConf().GetInt(CommonConfigurationKeys.IoFileBufferSizeKey
				, 4096));
			CheckSeekAndRead();
			stm.Close();
			CleanupFile(fileSys, file);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckSeekAndRead()
		{
			int position = 1;
			int len = 2 * BytesPerSum - position;
			ReadAndCompare(stm, position, len);
			position = BytesPerSum;
			len = BytesPerSum;
			ReadAndCompare(stm, position, len);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadAndCompare(FSDataInputStream @in, int position, int len)
		{
			byte[] b = new byte[len];
			@in.Seek(position);
			IOUtils.ReadFully(@in, b, 0, b.Length);
			for (int i = 0; i < b.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(expected[position + i], b[i]);
			}
		}
	}
}
