using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>test for the input truncation bug when mark/reset is used.</summary>
	/// <remarks>
	/// test for the input truncation bug when mark/reset is used.
	/// HADOOP-1489
	/// </remarks>
	public class TestTruncatedInputBug : TestCase
	{
		private static string TestRootDir = new Path(Runtime.GetProperty("test.build.data"
			, "/tmp")).ToString().Replace(' ', '+');

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name, int nBytesToWrite)
		{
			DataOutputStream @out = fileSys.Create(name);
			for (int i = 0; i < nBytesToWrite; ++i)
			{
				@out.WriteByte(0);
			}
			@out.Close();
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
		public virtual void TestTruncatedInputBug()
		{
			int ioBufSize = 512;
			int fileSize = ioBufSize * 4;
			int filePos = 0;
			Configuration conf = new Configuration();
			conf.SetInt("io.file.buffer.size", ioBufSize);
			FileSystem fileSys = FileSystem.GetLocal(conf);
			try
			{
				// First create a test input file.
				Path testFile = new Path(TestRootDir, "HADOOP-1489");
				WriteFile(fileSys, testFile, fileSize);
				Assert.True(fileSys.Exists(testFile));
				Assert.True(fileSys.GetFileStatus(testFile).GetLen() == fileSize
					);
				// Now read the file for ioBufSize bytes
				FSDataInputStream @in = fileSys.Open(testFile, ioBufSize);
				// seek beyond data buffered by open
				filePos += ioBufSize * 2 + (ioBufSize - 10);
				@in.Seek(filePos);
				// read 4 more bytes before marking
				for (int i = 0; i < 4; ++i)
				{
					if (@in.Read() == -1)
					{
						break;
					}
					++filePos;
				}
				// Now set mark() to trigger the bug
				// NOTE: in the fixed code, mark() does nothing (not supported) and
				//   hence won't trigger this bug.
				@in.Mark(1);
				System.Console.Out.WriteLine("MARKED");
				// Try to read the rest
				while (filePos < fileSize)
				{
					if (@in.Read() == -1)
					{
						break;
					}
					++filePos;
				}
				@in.Close();
				System.Console.Out.WriteLine("Read " + filePos + " bytes." + " file size=" + fileSize
					);
				Assert.True(filePos == fileSize);
			}
			finally
			{
				try
				{
					fileSys.Close();
				}
				catch (Exception)
				{
				}
			}
		}
		// noop
		// end testTruncatedInputBug
	}
}
