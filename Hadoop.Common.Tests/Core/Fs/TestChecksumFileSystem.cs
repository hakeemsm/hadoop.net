using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestChecksumFileSystem
	{
		internal static readonly string TestRootDir = Runtime.GetProperty("test.build.data"
			, "build/test/data/work-dir/localfs");

		internal static LocalFileSystem localFs;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void ResetLocalFs()
		{
			localFs = FileSystem.GetLocal(new Configuration());
			localFs.SetVerifyChecksum(true);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestgetChecksumLength()
		{
			Assert.Equal(8, ChecksumFileSystem.GetChecksumLength(0L, 512));
			Assert.Equal(12, ChecksumFileSystem.GetChecksumLength(1L, 512)
				);
			Assert.Equal(12, ChecksumFileSystem.GetChecksumLength(512L, 512
				));
			Assert.Equal(16, ChecksumFileSystem.GetChecksumLength(513L, 512
				));
			Assert.Equal(16, ChecksumFileSystem.GetChecksumLength(1023L, 512
				));
			Assert.Equal(16, ChecksumFileSystem.GetChecksumLength(1024L, 512
				));
			Assert.Equal(408, ChecksumFileSystem.GetChecksumLength(100L, 1
				));
			Assert.Equal(4000000000008L, ChecksumFileSystem.GetChecksumLength
				(10000000000000L, 10));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestVerifyChecksum()
		{
			Path testPath = new Path(TestRootDir, "testPath");
			Path testPath11 = new Path(TestRootDir, "testPath11");
			FSDataOutputStream fout = localFs.Create(testPath);
			fout.Write(Sharpen.Runtime.GetBytesForString("testing"));
			fout.Close();
			fout = localFs.Create(testPath11);
			fout.Write(Sharpen.Runtime.GetBytesForString("testing you"));
			fout.Close();
			// Exercise some boundary cases - a divisor of the chunk size
			// the chunk size, 2x chunk size, and +/-1 around these.
			FileSystemTestHelper.ReadFile(localFs, testPath, 128);
			FileSystemTestHelper.ReadFile(localFs, testPath, 511);
			FileSystemTestHelper.ReadFile(localFs, testPath, 512);
			FileSystemTestHelper.ReadFile(localFs, testPath, 513);
			FileSystemTestHelper.ReadFile(localFs, testPath, 1023);
			FileSystemTestHelper.ReadFile(localFs, testPath, 1024);
			FileSystemTestHelper.ReadFile(localFs, testPath, 1025);
			localFs.Delete(localFs.GetChecksumFile(testPath), true);
			Assert.True("checksum deleted", !localFs.Exists(localFs.GetChecksumFile
				(testPath)));
			//copying the wrong checksum file
			FileUtil.Copy(localFs, localFs.GetChecksumFile(testPath11), localFs, localFs.GetChecksumFile
				(testPath), false, true, localFs.GetConf());
			Assert.True("checksum exists", localFs.Exists(localFs.GetChecksumFile
				(testPath)));
			bool errorRead = false;
			try
			{
				FileSystemTestHelper.ReadFile(localFs, testPath, 1024);
			}
			catch (ChecksumException)
			{
				errorRead = true;
			}
			Assert.True("error reading", errorRead);
			//now setting verify false, the read should succeed
			localFs.SetVerifyChecksum(false);
			string str = FileSystemTestHelper.ReadFile(localFs, testPath, 1024).ToString();
			Assert.True("read", "testing".Equals(str));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMultiChunkFile()
		{
			Path testPath = new Path(TestRootDir, "testMultiChunk");
			FSDataOutputStream fout = localFs.Create(testPath);
			for (int i = 0; i < 1000; i++)
			{
				fout.Write(Sharpen.Runtime.GetBytesForString(("testing" + i)));
			}
			fout.Close();
			// Exercise some boundary cases - a divisor of the chunk size
			// the chunk size, 2x chunk size, and +/-1 around these.
			FileSystemTestHelper.ReadFile(localFs, testPath, 128);
			FileSystemTestHelper.ReadFile(localFs, testPath, 511);
			FileSystemTestHelper.ReadFile(localFs, testPath, 512);
			FileSystemTestHelper.ReadFile(localFs, testPath, 513);
			FileSystemTestHelper.ReadFile(localFs, testPath, 1023);
			FileSystemTestHelper.ReadFile(localFs, testPath, 1024);
			FileSystemTestHelper.ReadFile(localFs, testPath, 1025);
		}

		/// <summary>
		/// Test to ensure that if the checksum file is truncated, a
		/// ChecksumException is thrown
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestTruncatedChecksum()
		{
			Path testPath = new Path(TestRootDir, "testtruncatedcrc");
			FSDataOutputStream fout = localFs.Create(testPath);
			fout.Write(Sharpen.Runtime.GetBytesForString("testing truncation"));
			fout.Close();
			// Read in the checksum
			Path checksumFile = localFs.GetChecksumFile(testPath);
			FileSystem rawFs = localFs.GetRawFileSystem();
			FSDataInputStream checksumStream = rawFs.Open(checksumFile);
			byte[] buf = new byte[8192];
			int read = checksumStream.Read(buf, 0, buf.Length);
			checksumStream.Close();
			// Now rewrite the checksum file with the last byte missing
			FSDataOutputStream replaceStream = rawFs.Create(checksumFile);
			replaceStream.Write(buf, 0, read - 1);
			replaceStream.Close();
			// Now reading the file should fail with a ChecksumException
			try
			{
				FileSystemTestHelper.ReadFile(localFs, testPath, 1024);
				NUnit.Framework.Assert.Fail("Did not throw a ChecksumException when reading truncated "
					 + "crc file");
			}
			catch (ChecksumException)
			{
			}
			// telling it not to verify checksums, should avoid issue.
			localFs.SetVerifyChecksum(false);
			string str = FileSystemTestHelper.ReadFile(localFs, testPath, 1024).ToString();
			Assert.True("read", "testing truncation".Equals(str));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestStreamType()
		{
			Path testPath = new Path(TestRootDir, "testStreamType");
			localFs.Create(testPath).Close();
			FSDataInputStream @in = null;
			localFs.SetVerifyChecksum(true);
			@in = localFs.Open(testPath);
			Assert.True("stream is input checker", @in.GetWrappedStream() is
				 FSInputChecker);
			localFs.SetVerifyChecksum(false);
			@in = localFs.Open(testPath);
			NUnit.Framework.Assert.IsFalse("stream is not input checker", @in.GetWrappedStream
				() is FSInputChecker);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCorruptedChecksum()
		{
			Path testPath = new Path(TestRootDir, "testCorruptChecksum");
			Path checksumPath = localFs.GetChecksumFile(testPath);
			// write a file to generate checksum
			FSDataOutputStream @out = localFs.Create(testPath, true);
			@out.Write(Sharpen.Runtime.GetBytesForString("testing 1 2 3"));
			@out.Close();
			Assert.True(localFs.Exists(checksumPath));
			FileStatus stat = localFs.GetFileStatus(checksumPath);
			// alter file directly so checksum is invalid
			@out = localFs.GetRawFileSystem().Create(testPath, true);
			@out.Write(Sharpen.Runtime.GetBytesForString("testing stale checksum"));
			@out.Close();
			Assert.True(localFs.Exists(checksumPath));
			// checksum didn't change on disk
			Assert.Equal(stat, localFs.GetFileStatus(checksumPath));
			Exception e = null;
			try
			{
				localFs.SetVerifyChecksum(true);
				FileSystemTestHelper.ReadFile(localFs, testPath, 1024);
			}
			catch (ChecksumException ce)
			{
				e = ce;
			}
			finally
			{
				NUnit.Framework.Assert.IsNotNull("got checksum error", e);
			}
			localFs.SetVerifyChecksum(false);
			string str = FileSystemTestHelper.ReadFile(localFs, testPath, 1024);
			Assert.Equal("testing stale checksum", str);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameFileToFile()
		{
			Path srcPath = new Path(TestRootDir, "testRenameSrc");
			Path dstPath = new Path(TestRootDir, "testRenameDst");
			VerifyRename(srcPath, dstPath, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameFileIntoDir()
		{
			Path srcPath = new Path(TestRootDir, "testRenameSrc");
			Path dstPath = new Path(TestRootDir, "testRenameDir");
			localFs.Mkdirs(dstPath);
			VerifyRename(srcPath, dstPath, true);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameFileIntoDirFile()
		{
			Path srcPath = new Path(TestRootDir, "testRenameSrc");
			Path dstPath = new Path(TestRootDir, "testRenameDir/testRenameDst");
			Assert.True(localFs.Mkdirs(dstPath));
			VerifyRename(srcPath, dstPath, false);
		}

		/// <exception cref="System.Exception"/>
		internal virtual void VerifyRename(Path srcPath, Path dstPath, bool dstIsDir)
		{
			localFs.Delete(srcPath, true);
			localFs.Delete(dstPath, true);
			Path realDstPath = dstPath;
			if (dstIsDir)
			{
				localFs.Mkdirs(dstPath);
				realDstPath = new Path(dstPath, srcPath.GetName());
			}
			// ensure file + checksum are moved
			FileSystemTestHelper.WriteFile(localFs, srcPath, 1);
			Assert.True(localFs.Exists(localFs.GetChecksumFile(srcPath)));
			Assert.True(localFs.Rename(srcPath, dstPath));
			Assert.True(localFs.Exists(localFs.GetChecksumFile(realDstPath)
				));
			// create a file with no checksum, rename, ensure dst checksum is removed    
			FileSystemTestHelper.WriteFile(localFs.GetRawFileSystem(), srcPath, 1);
			NUnit.Framework.Assert.IsFalse(localFs.Exists(localFs.GetChecksumFile(srcPath)));
			Assert.True(localFs.Rename(srcPath, dstPath));
			NUnit.Framework.Assert.IsFalse(localFs.Exists(localFs.GetChecksumFile(realDstPath
				)));
			// create file with checksum, rename over prior dst with no checksum
			FileSystemTestHelper.WriteFile(localFs, srcPath, 1);
			Assert.True(localFs.Exists(localFs.GetChecksumFile(srcPath)));
			Assert.True(localFs.Rename(srcPath, dstPath));
			Assert.True(localFs.Exists(localFs.GetChecksumFile(realDstPath)
				));
		}
	}
}
