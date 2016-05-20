using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>This class tests the local file system via the FileSystem abstraction.</summary>
	public class TestLocalFileSystem
	{
		private static readonly string TEST_ROOT_DIR = Sharpen.Runtime.getProperty("test.build.data"
			, "build/test/data") + "/work-dir/localfs";

		private readonly java.io.File @base = new java.io.File(TEST_ROOT_DIR);

		private readonly org.apache.hadoop.fs.Path TEST_PATH = new org.apache.hadoop.fs.Path
			(TEST_ROOT_DIR, "test-file");

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.fs.LocalFileSystem fileSys;

		/// <exception cref="System.IO.IOException"/>
		private void cleanupFile(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 name)
		{
			NUnit.Framework.Assert.IsTrue(fs.exists(name));
			fs.delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fs.exists(name));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			conf = new org.apache.hadoop.conf.Configuration(false);
			conf.set("fs.file.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.LocalFileSystem
				)).getName());
			fileSys = org.apache.hadoop.fs.FileSystem.getLocal(conf);
			fileSys.delete(new org.apache.hadoop.fs.Path(TEST_ROOT_DIR), true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void after()
		{
			org.apache.hadoop.fs.FileUtil.setWritable(@base, true);
			org.apache.hadoop.fs.FileUtil.fullyDelete(@base);
			NUnit.Framework.Assert.IsTrue(!@base.exists());
		}

		/// <summary>Test the capability of setting the working directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testWorkingDirectory()
		{
			org.apache.hadoop.fs.Path origDir = fileSys.getWorkingDirectory();
			org.apache.hadoop.fs.Path subdir = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "new"
				);
			try
			{
				// make sure it doesn't already exist
				NUnit.Framework.Assert.IsTrue(!fileSys.exists(subdir));
				// make it and check for it
				NUnit.Framework.Assert.IsTrue(fileSys.mkdirs(subdir));
				NUnit.Framework.Assert.IsTrue(fileSys.isDirectory(subdir));
				fileSys.setWorkingDirectory(subdir);
				// create a directory and check for it
				org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path("dir1");
				NUnit.Framework.Assert.IsTrue(fileSys.mkdirs(dir1));
				NUnit.Framework.Assert.IsTrue(fileSys.isDirectory(dir1));
				// delete the directory and make sure it went away
				fileSys.delete(dir1, true);
				NUnit.Framework.Assert.IsTrue(!fileSys.exists(dir1));
				// create files and manipulate them.
				org.apache.hadoop.fs.Path file1 = new org.apache.hadoop.fs.Path("file1");
				org.apache.hadoop.fs.Path file2 = new org.apache.hadoop.fs.Path("sub/file2");
				string contents = org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, file1
					, 1);
				fileSys.copyFromLocalFile(file1, file2);
				NUnit.Framework.Assert.IsTrue(fileSys.exists(file1));
				NUnit.Framework.Assert.IsTrue(fileSys.isFile(file1));
				cleanupFile(fileSys, file2);
				fileSys.copyToLocalFile(file1, file2);
				cleanupFile(fileSys, file2);
				// try a rename
				fileSys.rename(file1, file2);
				NUnit.Framework.Assert.IsTrue(!fileSys.exists(file1));
				NUnit.Framework.Assert.IsTrue(fileSys.exists(file2));
				fileSys.rename(file2, file1);
				// try reading a file
				java.io.InputStream stm = fileSys.open(file1);
				byte[] buffer = new byte[3];
				int bytesRead = stm.read(buffer, 0, 3);
				NUnit.Framework.Assert.AreEqual(contents, Sharpen.Runtime.getStringForBytes(buffer
					, 0, bytesRead));
				stm.close();
			}
			finally
			{
				fileSys.setWorkingDirectory(origDir);
			}
		}

		/// <summary>test Syncable interface on raw local file system</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testSyncable()
		{
			org.apache.hadoop.fs.FileSystem fs = fileSys.getRawFileSystem();
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "syncable"
				);
			org.apache.hadoop.fs.FSDataOutputStream @out = fs.create(file);
			int bytesWritten = 1;
			byte[] expectedBuf = new byte[] { (byte)('0'), (byte)('1'), (byte)('2'), (byte)('3'
				) };
			try
			{
				@out.write(expectedBuf, 0, 1);
				@out.hflush();
				verifyFile(fs, file, bytesWritten, expectedBuf);
				@out.write(expectedBuf, bytesWritten, expectedBuf.Length - bytesWritten);
				@out.hsync();
				verifyFile(fs, file, expectedBuf.Length, expectedBuf);
			}
			finally
			{
				@out.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void verifyFile(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 file, int bytesToVerify, byte[] expectedBytes)
		{
			org.apache.hadoop.fs.FSDataInputStream @in = fs.open(file);
			try
			{
				byte[] readBuf = new byte[bytesToVerify];
				@in.readFully(readBuf, 0, bytesToVerify);
				for (int i = 0; i < bytesToVerify; i++)
				{
					NUnit.Framework.Assert.AreEqual(expectedBytes[i], readBuf[i]);
				}
			}
			finally
			{
				@in.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCopy()
		{
			org.apache.hadoop.fs.Path src = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "dingo"
				);
			org.apache.hadoop.fs.Path dst = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "yak"
				);
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, src, 1);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileUtil.copy(fileSys, src, fileSys
				, dst, true, false, conf));
			NUnit.Framework.Assert.IsTrue(!fileSys.exists(src) && fileSys.exists(dst));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileUtil.copy(fileSys, dst, fileSys
				, src, false, false, conf));
			NUnit.Framework.Assert.IsTrue(fileSys.exists(src) && fileSys.exists(dst));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileUtil.copy(fileSys, src, fileSys
				, dst, true, true, conf));
			NUnit.Framework.Assert.IsTrue(!fileSys.exists(src) && fileSys.exists(dst));
			fileSys.mkdirs(src);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileUtil.copy(fileSys, dst, fileSys
				, src, false, false, conf));
			org.apache.hadoop.fs.Path tmp = new org.apache.hadoop.fs.Path(src, dst.getName());
			NUnit.Framework.Assert.IsTrue(fileSys.exists(tmp) && fileSys.exists(dst));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileUtil.copy(fileSys, dst, fileSys
				, src, false, true, conf));
			NUnit.Framework.Assert.IsTrue(fileSys.delete(tmp, true));
			fileSys.mkdirs(tmp);
			try
			{
				org.apache.hadoop.fs.FileUtil.copy(fileSys, dst, fileSys, src, true, true, conf);
				NUnit.Framework.Assert.Fail("Failed to detect existing dir");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void testHomeDirectory()
		{
			org.apache.hadoop.fs.Path home = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("user.home")).makeQualified(fileSys);
			org.apache.hadoop.fs.Path fsHome = fileSys.getHomeDirectory();
			NUnit.Framework.Assert.AreEqual(home, fsHome);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testPathEscapes()
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "foo%bar"
				);
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, path, 1);
			org.apache.hadoop.fs.FileStatus status = fileSys.getFileStatus(path);
			NUnit.Framework.Assert.AreEqual(path.makeQualified(fileSys), status.getPath());
			cleanupFile(fileSys, path);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateFileAndMkdirs()
		{
			org.apache.hadoop.fs.Path test_dir = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
				"test_dir");
			org.apache.hadoop.fs.Path test_file = new org.apache.hadoop.fs.Path(test_dir, "file1"
				);
			NUnit.Framework.Assert.IsTrue(fileSys.mkdirs(test_dir));
			int fileSize = new java.util.Random().nextInt(1 << 20) + 1;
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, test_file, fileSize);
			{
				//check FileStatus and ContentSummary 
				org.apache.hadoop.fs.FileStatus status = fileSys.getFileStatus(test_file);
				NUnit.Framework.Assert.AreEqual(fileSize, status.getLen());
				org.apache.hadoop.fs.ContentSummary summary = fileSys.getContentSummary(test_dir);
				NUnit.Framework.Assert.AreEqual(fileSize, summary.getLength());
			}
			// creating dir over a file
			org.apache.hadoop.fs.Path bad_dir = new org.apache.hadoop.fs.Path(test_file, "another_dir"
				);
			try
			{
				fileSys.mkdirs(bad_dir);
				NUnit.Framework.Assert.Fail("Failed to detect existing file in path");
			}
			catch (org.apache.hadoop.fs.ParentNotDirectoryException)
			{
			}
			// Expected
			try
			{
				fileSys.mkdirs(null);
				NUnit.Framework.Assert.Fail("Failed to detect null in mkdir arg");
			}
			catch (System.ArgumentException)
			{
			}
		}

		// Expected
		/// <summary>Test deleting a file, directory, and non-existent path</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testBasicDelete()
		{
			org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "dir1"
				);
			org.apache.hadoop.fs.Path file1 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "file1"
				);
			org.apache.hadoop.fs.Path file2 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR + "/dir1"
				, "file2");
			org.apache.hadoop.fs.Path file3 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "does-not-exist"
				);
			NUnit.Framework.Assert.IsTrue(fileSys.mkdirs(dir1));
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, file1, 1);
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, file2, 1);
			NUnit.Framework.Assert.IsFalse("Returned true deleting non-existant path", fileSys
				.delete(file3));
			NUnit.Framework.Assert.IsTrue("Did not delete file", fileSys.delete(file1));
			NUnit.Framework.Assert.IsTrue("Did not delete non-empty dir", fileSys.delete(dir1
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testStatistics()
		{
			int fileSchemeCount = 0;
			foreach (org.apache.hadoop.fs.FileSystem.Statistics stats in org.apache.hadoop.fs.FileSystem
				.getAllStatistics())
			{
				if (stats.getScheme().Equals("file"))
				{
					fileSchemeCount++;
				}
			}
			NUnit.Framework.Assert.AreEqual(1, fileSchemeCount);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testHasFileDescriptor()
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "test-file"
				);
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, path, 1);
			org.apache.hadoop.fs.BufferedFSInputStream bis = new org.apache.hadoop.fs.BufferedFSInputStream
				(new org.apache.hadoop.fs.RawLocalFileSystem.LocalFSFileInputStream(this, path), 
				1024);
			NUnit.Framework.Assert.IsNotNull(bis.getFileDescriptor());
			bis.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testListStatusWithColons()
		{
			NUnit.Framework.Assume.assumeTrue(!org.apache.hadoop.util.Shell.WINDOWS);
			java.io.File colonFile = new java.io.File(TEST_ROOT_DIR, "foo:bar");
			colonFile.mkdirs();
			org.apache.hadoop.fs.FileStatus[] stats = fileSys.listStatus(new org.apache.hadoop.fs.Path
				(TEST_ROOT_DIR));
			NUnit.Framework.Assert.AreEqual("Unexpected number of stats", 1, stats.Length);
			NUnit.Framework.Assert.AreEqual("Bad path from stat", colonFile.getAbsolutePath()
				, stats[0].getPath().toUri().getPath());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testListStatusReturnConsistentPathOnWindows()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.util.Shell.WINDOWS);
			string dirNoDriveSpec = TEST_ROOT_DIR;
			if (dirNoDriveSpec[1] == ':')
			{
				dirNoDriveSpec = Sharpen.Runtime.substring(dirNoDriveSpec, 2);
			}
			java.io.File file = new java.io.File(dirNoDriveSpec, "foo");
			file.mkdirs();
			org.apache.hadoop.fs.FileStatus[] stats = fileSys.listStatus(new org.apache.hadoop.fs.Path
				(dirNoDriveSpec));
			NUnit.Framework.Assert.AreEqual("Unexpected number of stats", 1, stats.Length);
			NUnit.Framework.Assert.AreEqual("Bad path from stat", new org.apache.hadoop.fs.Path
				(file.getPath()).toUri().getPath(), stats[0].getPath().toUri().getPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testReportChecksumFailure()
		{
			@base.mkdirs();
			NUnit.Framework.Assert.IsTrue(@base.exists() && @base.isDirectory());
			java.io.File dir1 = new java.io.File(@base, "dir1");
			java.io.File dir2 = new java.io.File(dir1, "dir2");
			dir2.mkdirs();
			NUnit.Framework.Assert.IsTrue(dir2.exists() && org.apache.hadoop.fs.FileUtil.canWrite
				(dir2));
			string dataFileName = "corruptedData";
			org.apache.hadoop.fs.Path dataPath = new org.apache.hadoop.fs.Path(new java.io.File
				(dir2, dataFileName).toURI());
			org.apache.hadoop.fs.Path checksumPath = fileSys.getChecksumFile(dataPath);
			org.apache.hadoop.fs.FSDataOutputStream fsdos = fileSys.create(dataPath);
			try
			{
				fsdos.writeUTF("foo");
			}
			finally
			{
				fsdos.close();
			}
			NUnit.Framework.Assert.IsTrue(fileSys.pathToFile(dataPath).exists());
			long dataFileLength = fileSys.getFileStatus(dataPath).getLen();
			NUnit.Framework.Assert.IsTrue(dataFileLength > 0);
			// check the the checksum file is created and not empty:
			NUnit.Framework.Assert.IsTrue(fileSys.pathToFile(checksumPath).exists());
			long checksumFileLength = fileSys.getFileStatus(checksumPath).getLen();
			NUnit.Framework.Assert.IsTrue(checksumFileLength > 0);
			// this is a hack to force the #reportChecksumFailure() method to stop
			// climbing up at the 'base' directory and use 'dir1/bad_files' as the 
			// corrupted files storage:
			org.apache.hadoop.fs.FileUtil.setWritable(@base, false);
			org.apache.hadoop.fs.FSDataInputStream dataFsdis = fileSys.open(dataPath);
			org.apache.hadoop.fs.FSDataInputStream checksumFsdis = fileSys.open(checksumPath);
			bool retryIsNecessary = fileSys.reportChecksumFailure(dataPath, dataFsdis, 0, checksumFsdis
				, 0);
			NUnit.Framework.Assert.IsTrue(!retryIsNecessary);
			// the data file should be moved:
			NUnit.Framework.Assert.IsTrue(!fileSys.pathToFile(dataPath).exists());
			// the checksum file should be moved:
			NUnit.Framework.Assert.IsTrue(!fileSys.pathToFile(checksumPath).exists());
			// check that the files exist in the new location where they were moved:
			java.io.File[] dir1files = dir1.listFiles(new _FileFilter_352());
			NUnit.Framework.Assert.IsTrue(dir1files != null);
			NUnit.Framework.Assert.IsTrue(dir1files.Length == 1);
			java.io.File badFilesDir = dir1files[0];
			java.io.File[] badFiles = badFilesDir.listFiles();
			NUnit.Framework.Assert.IsTrue(badFiles != null);
			NUnit.Framework.Assert.IsTrue(badFiles.Length == 2);
			bool dataFileFound = false;
			bool checksumFileFound = false;
			foreach (java.io.File badFile in badFiles)
			{
				if (badFile.getName().StartsWith(dataFileName))
				{
					NUnit.Framework.Assert.IsTrue(dataFileLength == badFile.length());
					dataFileFound = true;
				}
				else
				{
					if (badFile.getName().contains(dataFileName + ".crc"))
					{
						NUnit.Framework.Assert.IsTrue(checksumFileLength == badFile.length());
						checksumFileFound = true;
					}
				}
			}
			NUnit.Framework.Assert.IsTrue(dataFileFound);
			NUnit.Framework.Assert.IsTrue(checksumFileFound);
		}

		private sealed class _FileFilter_352 : java.io.FileFilter
		{
			public _FileFilter_352()
			{
			}

			public bool accept(java.io.File pathname)
			{
				return pathname != null && !pathname.getName().Equals("dir2");
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testSetTimes()
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "set-times"
				);
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, path, 1);
			// test only to the nearest second, as the raw FS may not
			// support millisecond timestamps
			long newModTime = 12345000;
			org.apache.hadoop.fs.FileStatus status = fileSys.getFileStatus(path);
			NUnit.Framework.Assert.IsTrue("check we're actually changing something", newModTime
				 != status.getModificationTime());
			long accessTime = status.getAccessTime();
			fileSys.setTimes(path, newModTime, -1);
			status = fileSys.getFileStatus(path);
			NUnit.Framework.Assert.AreEqual(newModTime, status.getModificationTime());
			NUnit.Framework.Assert.AreEqual(accessTime, status.getAccessTime());
		}

		/// <summary>
		/// Regression test for HADOOP-9307: BufferedFSInputStream returning
		/// wrong results after certain sequences of seeks and reads.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testBufferedFSInputStream()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setClass("fs.file.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.RawLocalFileSystem
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY
				, 4096);
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.newInstance(
				conf);
			byte[] buf = new byte[10 * 1024];
			new java.util.Random().nextBytes(buf);
			// Write random bytes to file
			org.apache.hadoop.fs.FSDataOutputStream stream = fs.create(TEST_PATH);
			try
			{
				stream.write(buf);
			}
			finally
			{
				stream.close();
			}
			java.util.Random r = new java.util.Random();
			org.apache.hadoop.fs.FSDataInputStream stm = fs.open(TEST_PATH);
			// Record the sequence of seeks and reads which trigger a failure.
			int[] seeks = new int[10];
			int[] reads = new int[10];
			try
			{
				for (int i = 0; i < 1000; i++)
				{
					int seekOff = r.nextInt(buf.Length);
					int toRead = r.nextInt(System.Math.min(buf.Length - seekOff, 32000));
					seeks[i % seeks.Length] = seekOff;
					reads[i % reads.Length] = toRead;
					verifyRead(stm, buf, seekOff, toRead);
				}
			}
			catch (java.lang.AssertionError afe)
			{
				java.lang.StringBuilder sb = new java.lang.StringBuilder();
				sb.Append("Sequence of actions:\n");
				for (int j = 0; j < seeks.Length; j++)
				{
					sb.Append("seek @ ").Append(seeks[j]).Append("  ").Append("read ").Append(reads[j
						]).Append("\n");
				}
				System.Console.Error.WriteLine(sb.ToString());
				throw;
			}
			finally
			{
				stm.close();
			}
		}

		/// <summary>Tests a simple rename of a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testRenameDirectory()
		{
			org.apache.hadoop.fs.Path src = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "dir1"
				);
			org.apache.hadoop.fs.Path dst = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "dir2"
				);
			fileSys.delete(src, true);
			fileSys.delete(dst, true);
			NUnit.Framework.Assert.IsTrue(fileSys.mkdirs(src));
			NUnit.Framework.Assert.IsTrue(fileSys.rename(src, dst));
			NUnit.Framework.Assert.IsTrue(fileSys.exists(dst));
			NUnit.Framework.Assert.IsFalse(fileSys.exists(src));
		}

		/// <summary>
		/// Tests that renaming a directory replaces the destination if the destination
		/// is an existing empty directory.
		/// </summary>
		/// <remarks>
		/// Tests that renaming a directory replaces the destination if the destination
		/// is an existing empty directory.
		/// Before:
		/// /dir1
		/// /file1
		/// /file2
		/// /dir2
		/// After rename("/dir1", "/dir2"):
		/// /dir2
		/// /file1
		/// /file2
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testRenameReplaceExistingEmptyDirectory()
		{
			org.apache.hadoop.fs.Path src = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "dir1"
				);
			org.apache.hadoop.fs.Path dst = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "dir2"
				);
			fileSys.delete(src, true);
			fileSys.delete(dst, true);
			NUnit.Framework.Assert.IsTrue(fileSys.mkdirs(src));
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, new org.apache.hadoop.fs.Path
				(src, "file1"), 1);
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, new org.apache.hadoop.fs.Path
				(src, "file2"), 1);
			NUnit.Framework.Assert.IsTrue(fileSys.mkdirs(dst));
			NUnit.Framework.Assert.IsTrue(fileSys.rename(src, dst));
			NUnit.Framework.Assert.IsTrue(fileSys.exists(dst));
			NUnit.Framework.Assert.IsTrue(fileSys.exists(new org.apache.hadoop.fs.Path(dst, "file1"
				)));
			NUnit.Framework.Assert.IsTrue(fileSys.exists(new org.apache.hadoop.fs.Path(dst, "file2"
				)));
			NUnit.Framework.Assert.IsFalse(fileSys.exists(src));
		}

		/// <summary>
		/// Tests that renaming a directory to an existing directory that is not empty
		/// results in a full copy of source to destination.
		/// </summary>
		/// <remarks>
		/// Tests that renaming a directory to an existing directory that is not empty
		/// results in a full copy of source to destination.
		/// Before:
		/// /dir1
		/// /dir2
		/// /dir3
		/// /file1
		/// /file2
		/// After rename("/dir1/dir2/dir3", "/dir1"):
		/// /dir1
		/// /dir3
		/// /file1
		/// /file2
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testRenameMoveToExistingNonEmptyDirectory()
		{
			org.apache.hadoop.fs.Path src = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "dir1/dir2/dir3"
				);
			org.apache.hadoop.fs.Path dst = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "dir1"
				);
			fileSys.delete(src, true);
			fileSys.delete(dst, true);
			NUnit.Framework.Assert.IsTrue(fileSys.mkdirs(src));
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, new org.apache.hadoop.fs.Path
				(src, "file1"), 1);
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fileSys, new org.apache.hadoop.fs.Path
				(src, "file2"), 1);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(dst));
			NUnit.Framework.Assert.IsTrue(fileSys.rename(src, dst));
			NUnit.Framework.Assert.IsTrue(fileSys.exists(dst));
			NUnit.Framework.Assert.IsTrue(fileSys.exists(new org.apache.hadoop.fs.Path(dst, "dir3"
				)));
			NUnit.Framework.Assert.IsTrue(fileSys.exists(new org.apache.hadoop.fs.Path(dst, "dir3/file1"
				)));
			NUnit.Framework.Assert.IsTrue(fileSys.exists(new org.apache.hadoop.fs.Path(dst, "dir3/file2"
				)));
			NUnit.Framework.Assert.IsFalse(fileSys.exists(src));
		}

		/// <exception cref="System.IO.IOException"/>
		private void verifyRead(org.apache.hadoop.fs.FSDataInputStream stm, byte[] fileContents
			, int seekOff, int toRead)
		{
			byte[] @out = new byte[toRead];
			stm.seek(seekOff);
			stm.readFully(@out);
			byte[] expected = java.util.Arrays.copyOfRange(fileContents, seekOff, seekOff + toRead
				);
			if (!java.util.Arrays.equals(@out, expected))
			{
				string s = "\nExpected: " + org.apache.hadoop.util.StringUtils.byteToHexString(expected
					) + "\ngot:      " + org.apache.hadoop.util.StringUtils.byteToHexString(@out) + 
					"\noff=" + seekOff + " len=" + toRead;
				NUnit.Framework.Assert.Fail(s);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testStripFragmentFromPath()
		{
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(new 
				org.apache.hadoop.conf.Configuration());
			org.apache.hadoop.fs.Path pathQualified = TEST_PATH.makeQualified(fs.getUri(), fs
				.getWorkingDirectory());
			org.apache.hadoop.fs.Path pathWithFragment = new org.apache.hadoop.fs.Path(new java.net.URI
				(pathQualified.ToString() + "#glacier"));
			// Create test file with fragment
			org.apache.hadoop.fs.FileSystemTestHelper.createFile(fs, pathWithFragment);
			org.apache.hadoop.fs.Path resolved = fs.resolvePath(pathWithFragment);
			NUnit.Framework.Assert.AreEqual("resolvePath did not strip fragment from Path", pathQualified
				, resolved);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFileStatusPipeFile()
		{
			org.apache.hadoop.fs.RawLocalFileSystem origFs = new org.apache.hadoop.fs.RawLocalFileSystem
				();
			org.apache.hadoop.fs.RawLocalFileSystem fs = org.mockito.Mockito.spy(origFs);
			org.apache.hadoop.conf.Configuration conf = org.mockito.Mockito.mock<org.apache.hadoop.conf.Configuration
				>();
			fs.setConf(conf);
			org.mockito.@internal.util.reflection.Whitebox.setInternalState(fs, "useDeprecatedFileStatus"
				, false);
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/foo");
			java.io.File pipe = org.mockito.Mockito.mock<java.io.File>();
			org.mockito.Mockito.when(pipe.isFile()).thenReturn(false);
			org.mockito.Mockito.when(pipe.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(pipe.exists()).thenReturn(true);
			org.apache.hadoop.fs.FileStatus stat = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileStatus
				>();
			org.mockito.Mockito.doReturn(pipe).when(fs).pathToFile(path);
			org.mockito.Mockito.doReturn(stat).when(fs).getFileStatus(path);
			org.apache.hadoop.fs.FileStatus[] stats = fs.listStatus(path);
			NUnit.Framework.Assert.IsTrue(stats != null && stats.Length == 1 && stats[0] == stat
				);
		}
	}
}
