using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestChecksumFileSystem
	{
		internal static readonly string TEST_ROOT_DIR = Sharpen.Runtime.getProperty("test.build.data"
			, "build/test/data/work-dir/localfs");

		internal static org.apache.hadoop.fs.LocalFileSystem localFs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void resetLocalFs()
		{
			localFs = org.apache.hadoop.fs.FileSystem.getLocal(new org.apache.hadoop.conf.Configuration
				());
			localFs.setVerifyChecksum(true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testgetChecksumLength()
		{
			NUnit.Framework.Assert.AreEqual(8, org.apache.hadoop.fs.ChecksumFileSystem.getChecksumLength
				(0L, 512));
			NUnit.Framework.Assert.AreEqual(12, org.apache.hadoop.fs.ChecksumFileSystem.getChecksumLength
				(1L, 512));
			NUnit.Framework.Assert.AreEqual(12, org.apache.hadoop.fs.ChecksumFileSystem.getChecksumLength
				(512L, 512));
			NUnit.Framework.Assert.AreEqual(16, org.apache.hadoop.fs.ChecksumFileSystem.getChecksumLength
				(513L, 512));
			NUnit.Framework.Assert.AreEqual(16, org.apache.hadoop.fs.ChecksumFileSystem.getChecksumLength
				(1023L, 512));
			NUnit.Framework.Assert.AreEqual(16, org.apache.hadoop.fs.ChecksumFileSystem.getChecksumLength
				(1024L, 512));
			NUnit.Framework.Assert.AreEqual(408, org.apache.hadoop.fs.ChecksumFileSystem.getChecksumLength
				(100L, 1));
			NUnit.Framework.Assert.AreEqual(4000000000008L, org.apache.hadoop.fs.ChecksumFileSystem
				.getChecksumLength(10000000000000L, 10));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testVerifyChecksum()
		{
			org.apache.hadoop.fs.Path testPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"testPath");
			org.apache.hadoop.fs.Path testPath11 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR
				, "testPath11");
			org.apache.hadoop.fs.FSDataOutputStream fout = localFs.create(testPath);
			fout.write(Sharpen.Runtime.getBytesForString("testing"));
			fout.close();
			fout = localFs.create(testPath11);
			fout.write(Sharpen.Runtime.getBytesForString("testing you"));
			fout.close();
			// Exercise some boundary cases - a divisor of the chunk size
			// the chunk size, 2x chunk size, and +/-1 around these.
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 128);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 511);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 512);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 513);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 1023);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 1024);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 1025);
			localFs.delete(localFs.getChecksumFile(testPath), true);
			NUnit.Framework.Assert.IsTrue("checksum deleted", !localFs.exists(localFs.getChecksumFile
				(testPath)));
			//copying the wrong checksum file
			org.apache.hadoop.fs.FileUtil.copy(localFs, localFs.getChecksumFile(testPath11), 
				localFs, localFs.getChecksumFile(testPath), false, true, localFs.getConf());
			NUnit.Framework.Assert.IsTrue("checksum exists", localFs.exists(localFs.getChecksumFile
				(testPath)));
			bool errorRead = false;
			try
			{
				org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 1024);
			}
			catch (org.apache.hadoop.fs.ChecksumException)
			{
				errorRead = true;
			}
			NUnit.Framework.Assert.IsTrue("error reading", errorRead);
			//now setting verify false, the read should succeed
			localFs.setVerifyChecksum(false);
			string str = org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath
				, 1024).ToString();
			NUnit.Framework.Assert.IsTrue("read", "testing".Equals(str));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMultiChunkFile()
		{
			org.apache.hadoop.fs.Path testPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"testMultiChunk");
			org.apache.hadoop.fs.FSDataOutputStream fout = localFs.create(testPath);
			for (int i = 0; i < 1000; i++)
			{
				fout.write(Sharpen.Runtime.getBytesForString(("testing" + i)));
			}
			fout.close();
			// Exercise some boundary cases - a divisor of the chunk size
			// the chunk size, 2x chunk size, and +/-1 around these.
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 128);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 511);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 512);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 513);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 1023);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 1024);
			org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 1025);
		}

		/// <summary>
		/// Test to ensure that if the checksum file is truncated, a
		/// ChecksumException is thrown
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testTruncatedChecksum()
		{
			org.apache.hadoop.fs.Path testPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"testtruncatedcrc");
			org.apache.hadoop.fs.FSDataOutputStream fout = localFs.create(testPath);
			fout.write(Sharpen.Runtime.getBytesForString("testing truncation"));
			fout.close();
			// Read in the checksum
			org.apache.hadoop.fs.Path checksumFile = localFs.getChecksumFile(testPath);
			org.apache.hadoop.fs.FileSystem rawFs = localFs.getRawFileSystem();
			org.apache.hadoop.fs.FSDataInputStream checksumStream = rawFs.open(checksumFile);
			byte[] buf = new byte[8192];
			int read = checksumStream.read(buf, 0, buf.Length);
			checksumStream.close();
			// Now rewrite the checksum file with the last byte missing
			org.apache.hadoop.fs.FSDataOutputStream replaceStream = rawFs.create(checksumFile
				);
			replaceStream.write(buf, 0, read - 1);
			replaceStream.close();
			// Now reading the file should fail with a ChecksumException
			try
			{
				org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 1024);
				NUnit.Framework.Assert.Fail("Did not throw a ChecksumException when reading truncated "
					 + "crc file");
			}
			catch (org.apache.hadoop.fs.ChecksumException)
			{
			}
			// telling it not to verify checksums, should avoid issue.
			localFs.setVerifyChecksum(false);
			string str = org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath
				, 1024).ToString();
			NUnit.Framework.Assert.IsTrue("read", "testing truncation".Equals(str));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testStreamType()
		{
			org.apache.hadoop.fs.Path testPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"testStreamType");
			localFs.create(testPath).close();
			org.apache.hadoop.fs.FSDataInputStream @in = null;
			localFs.setVerifyChecksum(true);
			@in = localFs.open(testPath);
			NUnit.Framework.Assert.IsTrue("stream is input checker", @in.getWrappedStream() is
				 org.apache.hadoop.fs.FSInputChecker);
			localFs.setVerifyChecksum(false);
			@in = localFs.open(testPath);
			NUnit.Framework.Assert.IsFalse("stream is not input checker", @in.getWrappedStream
				() is org.apache.hadoop.fs.FSInputChecker);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCorruptedChecksum()
		{
			org.apache.hadoop.fs.Path testPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"testCorruptChecksum");
			org.apache.hadoop.fs.Path checksumPath = localFs.getChecksumFile(testPath);
			// write a file to generate checksum
			org.apache.hadoop.fs.FSDataOutputStream @out = localFs.create(testPath, true);
			@out.write(Sharpen.Runtime.getBytesForString("testing 1 2 3"));
			@out.close();
			NUnit.Framework.Assert.IsTrue(localFs.exists(checksumPath));
			org.apache.hadoop.fs.FileStatus stat = localFs.getFileStatus(checksumPath);
			// alter file directly so checksum is invalid
			@out = localFs.getRawFileSystem().create(testPath, true);
			@out.write(Sharpen.Runtime.getBytesForString("testing stale checksum"));
			@out.close();
			NUnit.Framework.Assert.IsTrue(localFs.exists(checksumPath));
			// checksum didn't change on disk
			NUnit.Framework.Assert.AreEqual(stat, localFs.getFileStatus(checksumPath));
			System.Exception e = null;
			try
			{
				localFs.setVerifyChecksum(true);
				org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath, 1024);
			}
			catch (org.apache.hadoop.fs.ChecksumException ce)
			{
				e = ce;
			}
			finally
			{
				NUnit.Framework.Assert.IsNotNull("got checksum error", e);
			}
			localFs.setVerifyChecksum(false);
			string str = org.apache.hadoop.fs.FileSystemTestHelper.readFile(localFs, testPath
				, 1024);
			NUnit.Framework.Assert.AreEqual("testing stale checksum", str);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileToFile()
		{
			org.apache.hadoop.fs.Path srcPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"testRenameSrc");
			org.apache.hadoop.fs.Path dstPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"testRenameDst");
			verifyRename(srcPath, dstPath, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileIntoDir()
		{
			org.apache.hadoop.fs.Path srcPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"testRenameSrc");
			org.apache.hadoop.fs.Path dstPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"testRenameDir");
			localFs.mkdirs(dstPath);
			verifyRename(srcPath, dstPath, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileIntoDirFile()
		{
			org.apache.hadoop.fs.Path srcPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"testRenameSrc");
			org.apache.hadoop.fs.Path dstPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"testRenameDir/testRenameDst");
			NUnit.Framework.Assert.IsTrue(localFs.mkdirs(dstPath));
			verifyRename(srcPath, dstPath, false);
		}

		/// <exception cref="System.Exception"/>
		internal virtual void verifyRename(org.apache.hadoop.fs.Path srcPath, org.apache.hadoop.fs.Path
			 dstPath, bool dstIsDir)
		{
			localFs.delete(srcPath, true);
			localFs.delete(dstPath, true);
			org.apache.hadoop.fs.Path realDstPath = dstPath;
			if (dstIsDir)
			{
				localFs.mkdirs(dstPath);
				realDstPath = new org.apache.hadoop.fs.Path(dstPath, srcPath.getName());
			}
			// ensure file + checksum are moved
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(localFs, srcPath, 1);
			NUnit.Framework.Assert.IsTrue(localFs.exists(localFs.getChecksumFile(srcPath)));
			NUnit.Framework.Assert.IsTrue(localFs.rename(srcPath, dstPath));
			NUnit.Framework.Assert.IsTrue(localFs.exists(localFs.getChecksumFile(realDstPath)
				));
			// create a file with no checksum, rename, ensure dst checksum is removed    
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(localFs.getRawFileSystem(), srcPath
				, 1);
			NUnit.Framework.Assert.IsFalse(localFs.exists(localFs.getChecksumFile(srcPath)));
			NUnit.Framework.Assert.IsTrue(localFs.rename(srcPath, dstPath));
			NUnit.Framework.Assert.IsFalse(localFs.exists(localFs.getChecksumFile(realDstPath
				)));
			// create file with checksum, rename over prior dst with no checksum
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(localFs, srcPath, 1);
			NUnit.Framework.Assert.IsTrue(localFs.exists(localFs.getChecksumFile(srcPath)));
			NUnit.Framework.Assert.IsTrue(localFs.rename(srcPath, dstPath));
			NUnit.Framework.Assert.IsTrue(localFs.exists(localFs.getChecksumFile(realDstPath)
				));
		}
	}
}
