using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>test for the input truncation bug when mark/reset is used.</summary>
	/// <remarks>
	/// test for the input truncation bug when mark/reset is used.
	/// HADOOP-1489
	/// </remarks>
	public class TestTruncatedInputBug : NUnit.Framework.TestCase
	{
		private static string TEST_ROOT_DIR = new org.apache.hadoop.fs.Path(Sharpen.Runtime
			.getProperty("test.build.data", "/tmp")).ToString().Replace(' ', '+');

		/// <exception cref="System.IO.IOException"/>
		private void writeFile(org.apache.hadoop.fs.FileSystem fileSys, org.apache.hadoop.fs.Path
			 name, int nBytesToWrite)
		{
			java.io.DataOutputStream @out = fileSys.create(name);
			for (int i = 0; i < nBytesToWrite; ++i)
			{
				@out.writeByte(0);
			}
			@out.close();
		}

		/// <summary>
		/// When mark() is used on BufferedInputStream, the request
		/// size on the checksum file system can be small.
		/// </summary>
		/// <remarks>
		/// When mark() is used on BufferedInputStream, the request
		/// size on the checksum file system can be small.  However,
		/// checksum file system currently depends on the request size
		/// &gt;= bytesPerSum to work properly.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testTruncatedInputBug()
		{
			int ioBufSize = 512;
			int fileSize = ioBufSize * 4;
			int filePos = 0;
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setInt("io.file.buffer.size", ioBufSize);
			org.apache.hadoop.fs.FileSystem fileSys = org.apache.hadoop.fs.FileSystem.getLocal
				(conf);
			try
			{
				// First create a test input file.
				org.apache.hadoop.fs.Path testFile = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
					"HADOOP-1489");
				writeFile(fileSys, testFile, fileSize);
				NUnit.Framework.Assert.IsTrue(fileSys.exists(testFile));
				NUnit.Framework.Assert.IsTrue(fileSys.getFileStatus(testFile).getLen() == fileSize
					);
				// Now read the file for ioBufSize bytes
				org.apache.hadoop.fs.FSDataInputStream @in = fileSys.open(testFile, ioBufSize);
				// seek beyond data buffered by open
				filePos += ioBufSize * 2 + (ioBufSize - 10);
				@in.seek(filePos);
				// read 4 more bytes before marking
				for (int i = 0; i < 4; ++i)
				{
					if (@in.read() == -1)
					{
						break;
					}
					++filePos;
				}
				// Now set mark() to trigger the bug
				// NOTE: in the fixed code, mark() does nothing (not supported) and
				//   hence won't trigger this bug.
				@in.mark(1);
				System.Console.Out.WriteLine("MARKED");
				// Try to read the rest
				while (filePos < fileSize)
				{
					if (@in.read() == -1)
					{
						break;
					}
					++filePos;
				}
				@in.close();
				System.Console.Out.WriteLine("Read " + filePos + " bytes." + " file size=" + fileSize
					);
				NUnit.Framework.Assert.IsTrue(filePos == fileSize);
			}
			finally
			{
				try
				{
					fileSys.close();
				}
				catch (System.Exception)
				{
				}
			}
		}
		// noop
		// end testTruncatedInputBug
	}
}
