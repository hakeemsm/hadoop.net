using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests the presence of seek bug as described
	/// in HADOOP-508
	/// </summary>
	public class TestSeekBug
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int Onemb = 1 << 20;

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name)
		{
			// create and write a file that contains 1MB
			DataOutputStream stm = fileSys.Create(name);
			byte[] buffer = new byte[Onemb];
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
		private void SeekReadFile(FileSystem fileSys, Path name)
		{
			FSDataInputStream stm = fileSys.Open(name, 4096);
			byte[] expected = new byte[Onemb];
			Random rand = new Random(seed);
			rand.NextBytes(expected);
			// First read 128 bytes to set count in BufferedInputStream
			byte[] actual = new byte[128];
			stm.Read(actual, 0, actual.Length);
			// Now read a byte array that is bigger than the internal buffer
			actual = new byte[100000];
			IOUtils.ReadFully(stm, actual, 0, actual.Length);
			CheckAndEraseData(actual, 128, expected, "First Read Test");
			// now do a small seek, within the range that is already read
			stm.Seek(96036);
			// 4 byte seek
			actual = new byte[128];
			IOUtils.ReadFully(stm, actual, 0, actual.Length);
			CheckAndEraseData(actual, 96036, expected, "Seek Bug");
			// all done
			stm.Close();
		}

		/*
		* Read some data, skip a few bytes and read more. HADOOP-922.
		*/
		/// <exception cref="System.IO.IOException"/>
		private void SmallReadSeek(FileSystem fileSys, Path name)
		{
			if (fileSys is ChecksumFileSystem)
			{
				fileSys = ((ChecksumFileSystem)fileSys).GetRawFileSystem();
			}
			// Make the buffer size small to trigger code for HADOOP-922
			FSDataInputStream stmRaw = fileSys.Open(name, 1);
			byte[] expected = new byte[Onemb];
			Random rand = new Random(seed);
			rand.NextBytes(expected);
			// Issue a simple read first.
			byte[] actual = new byte[128];
			stmRaw.Seek(100000);
			stmRaw.Read(actual, 0, actual.Length);
			CheckAndEraseData(actual, 100000, expected, "First Small Read Test");
			// now do a small seek of 4 bytes, within the same block.
			int newpos1 = 100000 + 128 + 4;
			stmRaw.Seek(newpos1);
			stmRaw.Read(actual, 0, actual.Length);
			CheckAndEraseData(actual, newpos1, expected, "Small Seek Bug 1");
			// seek another 256 bytes this time
			int newpos2 = newpos1 + 256;
			stmRaw.Seek(newpos2);
			stmRaw.Read(actual, 0, actual.Length);
			CheckAndEraseData(actual, newpos2, expected, "Small Seek Bug 2");
			// all done
			stmRaw.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		/// <summary>Test if the seek bug exists in FSDataInputStream in DFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSeekBugDFS()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fileSys = cluster.GetFileSystem();
			try
			{
				Path file1 = new Path("seektest.dat");
				WriteFile(fileSys, file1);
				SeekReadFile(fileSys, file1);
				SmallReadSeek(fileSys, file1);
				CleanupFile(fileSys, file1);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test (expected to throw IOE) for negative
		/// <code>FSDataInpuStream#seek</code> argument
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNegativeSeek()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				Path seekFile = new Path("seekboundaries.dat");
				DFSTestUtil.CreateFile(fs, seekFile, Onemb, fs.GetDefaultReplication(seekFile), seed
					);
				FSDataInputStream stream = fs.Open(seekFile);
				// Perform "safe seek" (expected to pass)
				stream.Seek(65536);
				NUnit.Framework.Assert.AreEqual(65536, stream.GetPos());
				// expect IOE for this call
				stream.Seek(-73);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test (expected to throw IOE) for <code>FSDataInpuStream#seek</code>
		/// when the position argument is larger than the file size.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSeekPastFileSize()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				Path seekFile = new Path("seekboundaries.dat");
				DFSTestUtil.CreateFile(fs, seekFile, Onemb, fs.GetDefaultReplication(seekFile), seed
					);
				FSDataInputStream stream = fs.Open(seekFile);
				// Perform "safe seek" (expected to pass)
				stream.Seek(65536);
				NUnit.Framework.Assert.AreEqual(65536, stream.GetPos());
				// expect IOE for this call
				stream.Seek(Onemb + Onemb + Onemb);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Tests if the seek bug exists in FSDataInputStream in LocalFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSeekBugLocalFS()
		{
			Configuration conf = new HdfsConfiguration();
			FileSystem fileSys = FileSystem.GetLocal(conf);
			try
			{
				Path file1 = new Path("build/test/data", "seektest.dat");
				WriteFile(fileSys, file1);
				SeekReadFile(fileSys, file1);
				CleanupFile(fileSys, file1);
			}
			finally
			{
				fileSys.Close();
			}
		}
	}
}
