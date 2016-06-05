using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Mockito.Internal.Util.Reflection;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>This class tests the local file system via the FileSystem abstraction.</summary>
	public class TestLocalFileSystem
	{
		private static readonly string TestRootDir = Runtime.GetProperty("test.build.data"
			, "build/test/data") + "/work-dir/localfs";

		private readonly FilePath @base = new FilePath(TestRootDir);

		private readonly Path TestPath = new Path(TestRootDir, "test-file");

		private Configuration conf;

		private LocalFileSystem fileSys;

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fs, Path name)
		{
			Assert.True(fs.Exists(name));
			fs.Delete(name, true);
			Assert.True(!fs.Exists(name));
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new Configuration(false);
			conf.Set("fs.file.impl", typeof(LocalFileSystem).FullName);
			fileSys = FileSystem.GetLocal(conf);
			fileSys.Delete(new Path(TestRootDir), true);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void After()
		{
			FileUtil.SetWritable(@base, true);
			FileUtil.FullyDelete(@base);
			Assert.True(!@base.Exists());
		}

		/// <summary>Test the capability of setting the working directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWorkingDirectory()
		{
			Path origDir = fileSys.GetWorkingDirectory();
			Path subdir = new Path(TestRootDir, "new");
			try
			{
				// make sure it doesn't already exist
				Assert.True(!fileSys.Exists(subdir));
				// make it and check for it
				Assert.True(fileSys.Mkdirs(subdir));
				Assert.True(fileSys.IsDirectory(subdir));
				fileSys.SetWorkingDirectory(subdir);
				// create a directory and check for it
				Path dir1 = new Path("dir1");
				Assert.True(fileSys.Mkdirs(dir1));
				Assert.True(fileSys.IsDirectory(dir1));
				// delete the directory and make sure it went away
				fileSys.Delete(dir1, true);
				Assert.True(!fileSys.Exists(dir1));
				// create files and manipulate them.
				Path file1 = new Path("file1");
				Path file2 = new Path("sub/file2");
				string contents = FileSystemTestHelper.WriteFile(fileSys, file1, 1);
				fileSys.CopyFromLocalFile(file1, file2);
				Assert.True(fileSys.Exists(file1));
				Assert.True(fileSys.IsFile(file1));
				CleanupFile(fileSys, file2);
				fileSys.CopyToLocalFile(file1, file2);
				CleanupFile(fileSys, file2);
				// try a rename
				fileSys.Rename(file1, file2);
				Assert.True(!fileSys.Exists(file1));
				Assert.True(fileSys.Exists(file2));
				fileSys.Rename(file2, file1);
				// try reading a file
				InputStream stm = fileSys.Open(file1);
				byte[] buffer = new byte[3];
				int bytesRead = stm.Read(buffer, 0, 3);
				Assert.Equal(contents, Runtime.GetStringForBytes(buffer
					, 0, bytesRead));
				stm.Close();
			}
			finally
			{
				fileSys.SetWorkingDirectory(origDir);
			}
		}

		/// <summary>test Syncable interface on raw local file system</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSyncable()
		{
			FileSystem fs = fileSys.GetRawFileSystem();
			Path file = new Path(TestRootDir, "syncable");
			FSDataOutputStream @out = fs.Create(file);
			int bytesWritten = 1;
			byte[] expectedBuf = new byte[] { (byte)('0'), (byte)('1'), (byte)('2'), (byte)('3'
				) };
			try
			{
				@out.Write(expectedBuf, 0, 1);
				@out.Hflush();
				VerifyFile(fs, file, bytesWritten, expectedBuf);
				@out.Write(expectedBuf, bytesWritten, expectedBuf.Length - bytesWritten);
				@out.Hsync();
				VerifyFile(fs, file, expectedBuf.Length, expectedBuf);
			}
			finally
			{
				@out.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyFile(FileSystem fs, Path file, int bytesToVerify, byte[] expectedBytes
			)
		{
			FSDataInputStream @in = fs.Open(file);
			try
			{
				byte[] readBuf = new byte[bytesToVerify];
				@in.ReadFully(readBuf, 0, bytesToVerify);
				for (int i = 0; i < bytesToVerify; i++)
				{
					Assert.Equal(expectedBytes[i], readBuf[i]);
				}
			}
			finally
			{
				@in.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCopy()
		{
			Path src = new Path(TestRootDir, "dingo");
			Path dst = new Path(TestRootDir, "yak");
			FileSystemTestHelper.WriteFile(fileSys, src, 1);
			Assert.True(FileUtil.Copy(fileSys, src, fileSys, dst, true, false
				, conf));
			Assert.True(!fileSys.Exists(src) && fileSys.Exists(dst));
			Assert.True(FileUtil.Copy(fileSys, dst, fileSys, src, false, false
				, conf));
			Assert.True(fileSys.Exists(src) && fileSys.Exists(dst));
			Assert.True(FileUtil.Copy(fileSys, src, fileSys, dst, true, true
				, conf));
			Assert.True(!fileSys.Exists(src) && fileSys.Exists(dst));
			fileSys.Mkdirs(src);
			Assert.True(FileUtil.Copy(fileSys, dst, fileSys, src, false, false
				, conf));
			Path tmp = new Path(src, dst.GetName());
			Assert.True(fileSys.Exists(tmp) && fileSys.Exists(dst));
			Assert.True(FileUtil.Copy(fileSys, dst, fileSys, src, false, true
				, conf));
			Assert.True(fileSys.Delete(tmp, true));
			fileSys.Mkdirs(tmp);
			try
			{
				FileUtil.Copy(fileSys, dst, fileSys, src, true, true, conf);
				NUnit.Framework.Assert.Fail("Failed to detect existing dir");
			}
			catch (IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestHomeDirectory()
		{
			Path home = new Path(Runtime.GetProperty("user.home")).MakeQualified(fileSys);
			Path fsHome = fileSys.GetHomeDirectory();
			Assert.Equal(home, fsHome);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPathEscapes()
		{
			Path path = new Path(TestRootDir, "foo%bar");
			FileSystemTestHelper.WriteFile(fileSys, path, 1);
			FileStatus status = fileSys.GetFileStatus(path);
			Assert.Equal(path.MakeQualified(fileSys), status.GetPath());
			CleanupFile(fileSys, path);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateFileAndMkdirs()
		{
			Path test_dir = new Path(TestRootDir, "test_dir");
			Path test_file = new Path(test_dir, "file1");
			Assert.True(fileSys.Mkdirs(test_dir));
			int fileSize = new Random().Next(1 << 20) + 1;
			FileSystemTestHelper.WriteFile(fileSys, test_file, fileSize);
			{
				//check FileStatus and ContentSummary 
				FileStatus status = fileSys.GetFileStatus(test_file);
				Assert.Equal(fileSize, status.GetLen());
				ContentSummary summary = fileSys.GetContentSummary(test_dir);
				Assert.Equal(fileSize, summary.GetLength());
			}
			// creating dir over a file
			Path bad_dir = new Path(test_file, "another_dir");
			try
			{
				fileSys.Mkdirs(bad_dir);
				NUnit.Framework.Assert.Fail("Failed to detect existing file in path");
			}
			catch (ParentNotDirectoryException)
			{
			}
			// Expected
			try
			{
				fileSys.Mkdirs(null);
				NUnit.Framework.Assert.Fail("Failed to detect null in mkdir arg");
			}
			catch (ArgumentException)
			{
			}
		}

		// Expected
		/// <summary>Test deleting a file, directory, and non-existent path</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBasicDelete()
		{
			Path dir1 = new Path(TestRootDir, "dir1");
			Path file1 = new Path(TestRootDir, "file1");
			Path file2 = new Path(TestRootDir + "/dir1", "file2");
			Path file3 = new Path(TestRootDir, "does-not-exist");
			Assert.True(fileSys.Mkdirs(dir1));
			FileSystemTestHelper.WriteFile(fileSys, file1, 1);
			FileSystemTestHelper.WriteFile(fileSys, file2, 1);
			NUnit.Framework.Assert.IsFalse("Returned true deleting non-existant path", fileSys
				.Delete(file3));
			Assert.True("Did not delete file", fileSys.Delete(file1));
			Assert.True("Did not delete non-empty dir", fileSys.Delete(dir1
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStatistics()
		{
			int fileSchemeCount = 0;
			foreach (FileSystem.Statistics stats in FileSystem.GetAllStatistics())
			{
				if (stats.GetScheme().Equals("file"))
				{
					fileSchemeCount++;
				}
			}
			Assert.Equal(1, fileSchemeCount);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestHasFileDescriptor()
		{
			Path path = new Path(TestRootDir, "test-file");
			FileSystemTestHelper.WriteFile(fileSys, path, 1);
			BufferedFSInputStream bis = new BufferedFSInputStream(new RawLocalFileSystem.LocalFSFileInputStream
				(this, path), 1024);
			NUnit.Framework.Assert.IsNotNull(bis.GetFileDescriptor());
			bis.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestListStatusWithColons()
		{
			Assume.AssumeTrue(!Shell.Windows);
			FilePath colonFile = new FilePath(TestRootDir, "foo:bar");
			colonFile.Mkdirs();
			FileStatus[] stats = fileSys.ListStatus(new Path(TestRootDir));
			Assert.Equal("Unexpected number of stats", 1, stats.Length);
			Assert.Equal("Bad path from stat", colonFile.GetAbsolutePath()
				, stats[0].GetPath().ToUri().GetPath());
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestListStatusReturnConsistentPathOnWindows()
		{
			Assume.AssumeTrue(Shell.Windows);
			string dirNoDriveSpec = TestRootDir;
			if (dirNoDriveSpec[1] == ':')
			{
				dirNoDriveSpec = Runtime.Substring(dirNoDriveSpec, 2);
			}
			FilePath file = new FilePath(dirNoDriveSpec, "foo");
			file.Mkdirs();
			FileStatus[] stats = fileSys.ListStatus(new Path(dirNoDriveSpec));
			Assert.Equal("Unexpected number of stats", 1, stats.Length);
			Assert.Equal("Bad path from stat", new Path(file.GetPath()).ToUri
				().GetPath(), stats[0].GetPath().ToUri().GetPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReportChecksumFailure()
		{
			@base.Mkdirs();
			Assert.True(@base.Exists() && @base.IsDirectory());
			FilePath dir1 = new FilePath(@base, "dir1");
			FilePath dir2 = new FilePath(dir1, "dir2");
			dir2.Mkdirs();
			Assert.True(dir2.Exists() && FileUtil.CanWrite(dir2));
			string dataFileName = "corruptedData";
			Path dataPath = new Path(new FilePath(dir2, dataFileName).ToURI());
			Path checksumPath = fileSys.GetChecksumFile(dataPath);
			FSDataOutputStream fsdos = fileSys.Create(dataPath);
			try
			{
				fsdos.WriteUTF("foo");
			}
			finally
			{
				fsdos.Close();
			}
			Assert.True(fileSys.PathToFile(dataPath).Exists());
			long dataFileLength = fileSys.GetFileStatus(dataPath).GetLen();
			Assert.True(dataFileLength > 0);
			// check the the checksum file is created and not empty:
			Assert.True(fileSys.PathToFile(checksumPath).Exists());
			long checksumFileLength = fileSys.GetFileStatus(checksumPath).GetLen();
			Assert.True(checksumFileLength > 0);
			// this is a hack to force the #reportChecksumFailure() method to stop
			// climbing up at the 'base' directory and use 'dir1/bad_files' as the 
			// corrupted files storage:
			FileUtil.SetWritable(@base, false);
			FSDataInputStream dataFsdis = fileSys.Open(dataPath);
			FSDataInputStream checksumFsdis = fileSys.Open(checksumPath);
			bool retryIsNecessary = fileSys.ReportChecksumFailure(dataPath, dataFsdis, 0, checksumFsdis
				, 0);
			Assert.True(!retryIsNecessary);
			// the data file should be moved:
			Assert.True(!fileSys.PathToFile(dataPath).Exists());
			// the checksum file should be moved:
			Assert.True(!fileSys.PathToFile(checksumPath).Exists());
			// check that the files exist in the new location where they were moved:
			FilePath[] dir1files = dir1.ListFiles(new _FileFilter_352());
			Assert.True(dir1files != null);
			Assert.True(dir1files.Length == 1);
			FilePath badFilesDir = dir1files[0];
			FilePath[] badFiles = badFilesDir.ListFiles();
			Assert.True(badFiles != null);
			Assert.True(badFiles.Length == 2);
			bool dataFileFound = false;
			bool checksumFileFound = false;
			foreach (FilePath badFile in badFiles)
			{
				if (badFile.GetName().StartsWith(dataFileName))
				{
					Assert.True(dataFileLength == badFile.Length());
					dataFileFound = true;
				}
				else
				{
					if (badFile.GetName().Contains(dataFileName + ".crc"))
					{
						Assert.True(checksumFileLength == badFile.Length());
						checksumFileFound = true;
					}
				}
			}
			Assert.True(dataFileFound);
			Assert.True(checksumFileFound);
		}

		private sealed class _FileFilter_352 : FileFilter
		{
			public _FileFilter_352()
			{
			}

			public bool Accept(FilePath pathname)
			{
				return pathname != null && !pathname.GetName().Equals("dir2");
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetTimes()
		{
			Path path = new Path(TestRootDir, "set-times");
			FileSystemTestHelper.WriteFile(fileSys, path, 1);
			// test only to the nearest second, as the raw FS may not
			// support millisecond timestamps
			long newModTime = 12345000;
			FileStatus status = fileSys.GetFileStatus(path);
			Assert.True("check we're actually changing something", newModTime
				 != status.GetModificationTime());
			long accessTime = status.GetAccessTime();
			fileSys.SetTimes(path, newModTime, -1);
			status = fileSys.GetFileStatus(path);
			Assert.Equal(newModTime, status.GetModificationTime());
			Assert.Equal(accessTime, status.GetAccessTime());
		}

		/// <summary>
		/// Regression test for HADOOP-9307: BufferedFSInputStream returning
		/// wrong results after certain sequences of seeks and reads.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestBufferedFSInputStream()
		{
			Configuration conf = new Configuration();
			conf.SetClass("fs.file.impl", typeof(RawLocalFileSystem), typeof(FileSystem));
			conf.SetInt(CommonConfigurationKeysPublic.IoFileBufferSizeKey, 4096);
			FileSystem fs = FileSystem.NewInstance(conf);
			byte[] buf = new byte[10 * 1024];
			new Random().NextBytes(buf);
			// Write random bytes to file
			FSDataOutputStream stream = fs.Create(TestPath);
			try
			{
				stream.Write(buf);
			}
			finally
			{
				stream.Close();
			}
			Random r = new Random();
			FSDataInputStream stm = fs.Open(TestPath);
			// Record the sequence of seeks and reads which trigger a failure.
			int[] seeks = new int[10];
			int[] reads = new int[10];
			try
			{
				for (int i = 0; i < 1000; i++)
				{
					int seekOff = r.Next(buf.Length);
					int toRead = r.Next(Math.Min(buf.Length - seekOff, 32000));
					seeks[i % seeks.Length] = seekOff;
					reads[i % reads.Length] = toRead;
					VerifyRead(stm, buf, seekOff, toRead);
				}
			}
			catch (Exception afe)
			{
				StringBuilder sb = new StringBuilder();
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
				stm.Close();
			}
		}

		/// <summary>Tests a simple rename of a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestRenameDirectory()
		{
			Path src = new Path(TestRootDir, "dir1");
			Path dst = new Path(TestRootDir, "dir2");
			fileSys.Delete(src, true);
			fileSys.Delete(dst, true);
			Assert.True(fileSys.Mkdirs(src));
			Assert.True(fileSys.Rename(src, dst));
			Assert.True(fileSys.Exists(dst));
			NUnit.Framework.Assert.IsFalse(fileSys.Exists(src));
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
		[Fact]
		public virtual void TestRenameReplaceExistingEmptyDirectory()
		{
			Path src = new Path(TestRootDir, "dir1");
			Path dst = new Path(TestRootDir, "dir2");
			fileSys.Delete(src, true);
			fileSys.Delete(dst, true);
			Assert.True(fileSys.Mkdirs(src));
			FileSystemTestHelper.WriteFile(fileSys, new Path(src, "file1"), 1);
			FileSystemTestHelper.WriteFile(fileSys, new Path(src, "file2"), 1);
			Assert.True(fileSys.Mkdirs(dst));
			Assert.True(fileSys.Rename(src, dst));
			Assert.True(fileSys.Exists(dst));
			Assert.True(fileSys.Exists(new Path(dst, "file1")));
			Assert.True(fileSys.Exists(new Path(dst, "file2")));
			NUnit.Framework.Assert.IsFalse(fileSys.Exists(src));
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
		[Fact]
		public virtual void TestRenameMoveToExistingNonEmptyDirectory()
		{
			Path src = new Path(TestRootDir, "dir1/dir2/dir3");
			Path dst = new Path(TestRootDir, "dir1");
			fileSys.Delete(src, true);
			fileSys.Delete(dst, true);
			Assert.True(fileSys.Mkdirs(src));
			FileSystemTestHelper.WriteFile(fileSys, new Path(src, "file1"), 1);
			FileSystemTestHelper.WriteFile(fileSys, new Path(src, "file2"), 1);
			Assert.True(fileSys.Exists(dst));
			Assert.True(fileSys.Rename(src, dst));
			Assert.True(fileSys.Exists(dst));
			Assert.True(fileSys.Exists(new Path(dst, "dir3")));
			Assert.True(fileSys.Exists(new Path(dst, "dir3/file1")));
			Assert.True(fileSys.Exists(new Path(dst, "dir3/file2")));
			NUnit.Framework.Assert.IsFalse(fileSys.Exists(src));
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyRead(FSDataInputStream stm, byte[] fileContents, int seekOff, 
			int toRead)
		{
			byte[] @out = new byte[toRead];
			stm.Seek(seekOff);
			stm.ReadFully(@out);
			byte[] expected = Arrays.CopyOfRange(fileContents, seekOff, seekOff + toRead);
			if (!Arrays.Equals(@out, expected))
			{
				string s = "\nExpected: " + StringUtils.ByteToHexString(expected) + "\ngot:      "
					 + StringUtils.ByteToHexString(@out) + "\noff=" + seekOff + " len=" + toRead;
				NUnit.Framework.Assert.Fail(s);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestStripFragmentFromPath()
		{
			FileSystem fs = FileSystem.GetLocal(new Configuration());
			Path pathQualified = TestPath.MakeQualified(fs.GetUri(), fs.GetWorkingDirectory()
				);
			Path pathWithFragment = new Path(new URI(pathQualified.ToString() + "#glacier"));
			// Create test file with fragment
			FileSystemTestHelper.CreateFile(fs, pathWithFragment);
			Path resolved = fs.ResolvePath(pathWithFragment);
			Assert.Equal("resolvePath did not strip fragment from Path", pathQualified
				, resolved);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFileStatusPipeFile()
		{
			RawLocalFileSystem origFs = new RawLocalFileSystem();
			RawLocalFileSystem fs = Org.Mockito.Mockito.Spy(origFs);
			Configuration conf = Org.Mockito.Mockito.Mock<Configuration>();
			fs.SetConf(conf);
			Whitebox.SetInternalState(fs, "useDeprecatedFileStatus", false);
			Path path = new Path("/foo");
			FilePath pipe = Org.Mockito.Mockito.Mock<FilePath>();
			Org.Mockito.Mockito.When(pipe.IsFile()).ThenReturn(false);
			Org.Mockito.Mockito.When(pipe.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(pipe.Exists()).ThenReturn(true);
			FileStatus stat = Org.Mockito.Mockito.Mock<FileStatus>();
			Org.Mockito.Mockito.DoReturn(pipe).When(fs).PathToFile(path);
			Org.Mockito.Mockito.DoReturn(stat).When(fs).GetFileStatus(path);
			FileStatus[] stats = fs.ListStatus(path);
			Assert.True(stats != null && stats.Length == 1 && stats[0] == stat
				);
		}
	}
}
