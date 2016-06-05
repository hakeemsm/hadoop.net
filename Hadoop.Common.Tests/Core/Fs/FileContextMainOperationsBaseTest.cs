using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// <p>
	/// A collection of tests for the
	/// <see cref="FileContext"/>
	/// .
	/// This test should be used for testing an instance of FileContext
	/// that has been initialized to a specific default FileSystem such a
	/// LocalFileSystem, HDFS,S3, etc.
	/// </p>
	/// <p>
	/// To test a given
	/// <see cref="FileSystem"/>
	/// implementation create a subclass of this
	/// test and override
	/// <see cref="SetUp()"/>
	/// to initialize the <code>fc</code>
	/// <see cref="FileContext"/>
	/// instance variable.
	/// Since this a junit 4 you can also do a single setup before
	/// the start of any tests.
	/// E.g.
	/// </summary>
	/// <BeforeClass>public static void clusterSetupAtBegining()</BeforeClass>
	/// <AfterClass>
	/// public static void ClusterShutdownAtEnd()
	/// </p>
	/// </AfterClass>
	public abstract class FileContextMainOperationsBaseTest
	{
		private static string TestDirAaa2 = "test/hadoop2/aaa";

		private static string TestDirAaa = "test/hadoop/aaa";

		private static string TestDirAxa = "test/hadoop/axa";

		private static string TestDirAxx = "test/hadoop/axx";

		private static int numBlocks = 2;

		public Path localFsRootPath;

		protected internal readonly FileContextTestHelper fileContextTestHelper;

		protected internal virtual FileContextTestHelper CreateFileContextHelper()
		{
			return new FileContextTestHelper();
		}

		protected internal static FileContext fc;

		private sealed class _PathFilter_79 : PathFilter
		{
			public _PathFilter_79()
			{
			}

			public bool Accept(Path file)
			{
				return true;
			}
		}

		private static readonly PathFilter DefaultFilter = new _PathFilter_79();

		private sealed class _PathFilter_87 : PathFilter
		{
			public _PathFilter_87()
			{
			}

			//A test filter with returns any path containing an "x" or "X" 
			public bool Accept(Path file)
			{
				if (file.GetName().Contains("x") || file.GetName().Contains("X"))
				{
					return true;
				}
				else
				{
					return false;
				}
			}
		}

		private static readonly PathFilter TestXFilter = new _PathFilter_87();

		private static readonly byte[] data = FileContextTestHelper.GetFileData(numBlocks
			, FileContextTestHelper.GetDefaultBlockSize());

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			FilePath testBuildData = new FilePath(Runtime.GetProperty("test.build.data", "build/test/data"
				), RandomStringUtils.RandomAlphanumeric(10));
			Path rootPath = new Path(testBuildData.GetAbsolutePath(), "root-uri");
			localFsRootPath = rootPath.MakeQualified(LocalFileSystem.Name, null);
			fc.Mkdir(GetTestRootPath(fc, "test"), FileContext.DefaultPerm, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			bool del = fc.Delete(new Path(fileContextTestHelper.GetAbsoluteTestRootPath(fc), 
				new Path("test")), true);
			Assert.True(del);
			fc.Delete(localFsRootPath, true);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Path GetDefaultWorkingDirectory()
		{
			return GetTestRootPath(fc, "/user/" + Runtime.GetProperty("user.name")).MakeQualified
				(fc.GetDefaultFileSystem().GetUri(), fc.GetWorkingDirectory());
		}

		protected internal virtual bool RenameSupported()
		{
			return true;
		}

		protected internal virtual IOException UnwrapException(IOException e)
		{
			return e;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFsStatus()
		{
			FsStatus fsStatus = fc.GetFsStatus(null);
			NUnit.Framework.Assert.IsNotNull(fsStatus);
			//used, free and capacity are non-negative longs
			Assert.True(fsStatus.GetUsed() >= 0);
			Assert.True(fsStatus.GetRemaining() >= 0);
			Assert.True(fsStatus.GetCapacity() >= 0);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWorkingDirectory()
		{
			// First we cd to our test root
			Path workDir = new Path(fileContextTestHelper.GetAbsoluteTestRootPath(fc), new Path
				("test"));
			fc.SetWorkingDirectory(workDir);
			Assert.Equal(workDir, fc.GetWorkingDirectory());
			fc.SetWorkingDirectory(new Path("."));
			Assert.Equal(workDir, fc.GetWorkingDirectory());
			fc.SetWorkingDirectory(new Path(".."));
			Assert.Equal(workDir.GetParent(), fc.GetWorkingDirectory());
			// cd using a relative path
			// Go back to our test root
			workDir = new Path(fileContextTestHelper.GetAbsoluteTestRootPath(fc), new Path("test"
				));
			fc.SetWorkingDirectory(workDir);
			Assert.Equal(workDir, fc.GetWorkingDirectory());
			Path relativeDir = new Path("existingDir1");
			Path absoluteDir = new Path(workDir, "existingDir1");
			fc.Mkdir(absoluteDir, FileContext.DefaultPerm, true);
			fc.SetWorkingDirectory(relativeDir);
			Assert.Equal(absoluteDir, fc.GetWorkingDirectory());
			// cd using a absolute path
			absoluteDir = GetTestRootPath(fc, "test/existingDir2");
			fc.Mkdir(absoluteDir, FileContext.DefaultPerm, true);
			fc.SetWorkingDirectory(absoluteDir);
			Assert.Equal(absoluteDir, fc.GetWorkingDirectory());
			// Now open a file relative to the wd we just set above.
			Path absolutePath = new Path(absoluteDir, "foo");
			fc.Create(absolutePath, EnumSet.Of(CreateFlag.Create)).Close();
			fc.Open(new Path("foo")).Close();
			// Now mkdir relative to the dir we cd'ed to
			fc.Mkdir(new Path("newDir"), FileContext.DefaultPerm, true);
			Assert.True(FileContextTestHelper.IsDir(fc, new Path(absoluteDir
				, "newDir")));
			absoluteDir = GetTestRootPath(fc, "nonexistingPath");
			try
			{
				fc.SetWorkingDirectory(absoluteDir);
				NUnit.Framework.Assert.Fail("cd to non existing dir should have failed");
			}
			catch (Exception)
			{
			}
			// Exception as expected
			// Try a URI
			absoluteDir = new Path(localFsRootPath, "existingDir");
			fc.Mkdir(absoluteDir, FileContext.DefaultPerm, true);
			fc.SetWorkingDirectory(absoluteDir);
			Assert.Equal(absoluteDir, fc.GetWorkingDirectory());
			Path aRegularFile = new Path("aRegularFile");
			CreateFile(aRegularFile);
			try
			{
				fc.SetWorkingDirectory(aRegularFile);
				NUnit.Framework.Assert.Fail("An IOException expected.");
			}
			catch (IOException)
			{
			}
		}

		// okay
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMkdirs()
		{
			Path testDir = GetTestRootPath(fc, "test/hadoop");
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, testDir));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsFile(fc, testDir));
			fc.Mkdir(testDir, FsPermission.GetDefault(), true);
			Assert.True(FileContextTestHelper.Exists(fc, testDir));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsFile(fc, testDir));
			fc.Mkdir(testDir, FsPermission.GetDefault(), true);
			Assert.True(FileContextTestHelper.Exists(fc, testDir));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsFile(fc, testDir));
			Path parentDir = testDir.GetParent();
			Assert.True(FileContextTestHelper.Exists(fc, parentDir));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsFile(fc, parentDir));
			Path grandparentDir = parentDir.GetParent();
			Assert.True(FileContextTestHelper.Exists(fc, grandparentDir));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsFile(fc, grandparentDir));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMkdirsFailsForSubdirectoryOfExistingFile()
		{
			Path testDir = GetTestRootPath(fc, "test/hadoop");
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, testDir));
			fc.Mkdir(testDir, FsPermission.GetDefault(), true);
			Assert.True(FileContextTestHelper.Exists(fc, testDir));
			CreateFile(GetTestRootPath(fc, "test/hadoop/file"));
			Path testSubDir = GetTestRootPath(fc, "test/hadoop/file/subdir");
			try
			{
				fc.Mkdir(testSubDir, FsPermission.GetDefault(), true);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, testSubDir));
			Path testDeepSubDir = GetTestRootPath(fc, "test/hadoop/file/deep/sub/dir");
			try
			{
				fc.Mkdir(testDeepSubDir, FsPermission.GetDefault(), true);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, testDeepSubDir));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetFileStatusThrowsExceptionForNonExistentFile()
		{
			try
			{
				fc.GetFileStatus(GetTestRootPath(fc, "test/hadoop/file"));
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		public virtual void TestListStatusThrowsExceptionForNonExistentFile()
		{
			try
			{
				fc.ListStatus(GetTestRootPath(fc, "test/hadoop/file"));
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestListStatus()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, "test/hadoop/a"), GetTestRootPath
				(fc, "test/hadoop/b"), GetTestRootPath(fc, "test/hadoop/c/1") };
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, testDirs[0]));
			foreach (Path path in testDirs)
			{
				fc.Mkdir(path, FsPermission.GetDefault(), true);
			}
			// test listStatus that returns an array
			FileStatus[] paths = fc.Util().ListStatus(GetTestRootPath(fc, "test"));
			Assert.Equal(1, paths.Length);
			Assert.Equal(GetTestRootPath(fc, "test/hadoop"), paths[0].GetPath
				());
			paths = fc.Util().ListStatus(GetTestRootPath(fc, "test/hadoop"));
			Assert.Equal(3, paths.Length);
			Assert.True(ContainsPath(GetTestRootPath(fc, "test/hadoop/a"), 
				paths));
			Assert.True(ContainsPath(GetTestRootPath(fc, "test/hadoop/b"), 
				paths));
			Assert.True(ContainsPath(GetTestRootPath(fc, "test/hadoop/c"), 
				paths));
			paths = fc.Util().ListStatus(GetTestRootPath(fc, "test/hadoop/a"));
			Assert.Equal(0, paths.Length);
			// test listStatus that returns an iterator
			RemoteIterator<FileStatus> pathsIterator = fc.ListStatus(GetTestRootPath(fc, "test"
				));
			Assert.Equal(GetTestRootPath(fc, "test/hadoop"), pathsIterator
				.Next().GetPath());
			NUnit.Framework.Assert.IsFalse(pathsIterator.HasNext());
			pathsIterator = fc.ListStatus(GetTestRootPath(fc, "test/hadoop"));
			FileStatus[] subdirs = new FileStatus[3];
			int i = 0;
			while (i < 3 && pathsIterator.HasNext())
			{
				subdirs[i++] = pathsIterator.Next();
			}
			NUnit.Framework.Assert.IsFalse(pathsIterator.HasNext());
			Assert.True(i == 3);
			Assert.True(ContainsPath(GetTestRootPath(fc, "test/hadoop/a"), 
				subdirs));
			Assert.True(ContainsPath(GetTestRootPath(fc, "test/hadoop/b"), 
				subdirs));
			Assert.True(ContainsPath(GetTestRootPath(fc, "test/hadoop/c"), 
				subdirs));
			pathsIterator = fc.ListStatus(GetTestRootPath(fc, "test/hadoop/a"));
			NUnit.Framework.Assert.IsFalse(pathsIterator.HasNext());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestListStatusFilterWithNoMatches()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa2), GetTestRootPath(
				fc, TestDirAaa), GetTestRootPath(fc, TestDirAxa), GetTestRootPath(fc, TestDirAxx
				) };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			// listStatus with filters returns empty correctly
			FileStatus[] filteredPaths = fc.Util().ListStatus(GetTestRootPath(fc, "test"), TestXFilter
				);
			Assert.Equal(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestListStatusFilterWithSomeMatches()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa), GetTestRootPath(fc
				, TestDirAxa), GetTestRootPath(fc, TestDirAxx), GetTestRootPath(fc, TestDirAaa2)
				 };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			// should return 2 paths ("/test/hadoop/axa" and "/test/hadoop/axx")
			FileStatus[] filteredPaths = fc.Util().ListStatus(GetTestRootPath(fc, "test/hadoop"
				), TestXFilter);
			Assert.Equal(2, filteredPaths.Length);
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxa), filteredPaths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxx), filteredPaths
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGlobStatusNonExistentFile()
		{
			FileStatus[] paths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoopfsdf"));
			NUnit.Framework.Assert.IsNull(paths);
			paths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoopfsdf/?"));
			Assert.Equal(0, paths.Length);
			paths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoopfsdf/xyz*/?"));
			Assert.Equal(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGlobStatusWithNoMatchesInPath()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa), GetTestRootPath(fc
				, TestDirAxa), GetTestRootPath(fc, TestDirAxx), GetTestRootPath(fc, TestDirAaa2)
				 };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			// should return nothing
			FileStatus[] paths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoop/?"));
			Assert.Equal(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGlobStatusSomeMatchesInDirectories()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa), GetTestRootPath(fc
				, TestDirAxa), GetTestRootPath(fc, TestDirAxx), GetTestRootPath(fc, TestDirAaa2)
				 };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			// Should return two items ("/test/hadoop" and "/test/hadoop2")
			FileStatus[] paths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoop*"));
			Assert.Equal(2, paths.Length);
			Assert.True(ContainsPath(GetTestRootPath(fc, "test/hadoop"), paths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, "test/hadoop2"), paths
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGlobStatusWithMultipleWildCardMatches()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa), GetTestRootPath(fc
				, TestDirAxa), GetTestRootPath(fc, TestDirAxx), GetTestRootPath(fc, TestDirAaa2)
				 };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			//Should return all 4 items ("/test/hadoop/aaa", "/test/hadoop/axa"
			//"/test/hadoop/axx", and "/test/hadoop2/axx")
			FileStatus[] paths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoop*/*"));
			Assert.Equal(4, paths.Length);
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAaa), paths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxa), paths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxx), paths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAaa2), paths
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGlobStatusWithMultipleMatchesOfSingleChar()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa), GetTestRootPath(fc
				, TestDirAxa), GetTestRootPath(fc, TestDirAxx), GetTestRootPath(fc, TestDirAaa2)
				 };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			//Should return only 2 items ("/test/hadoop/axa", "/test/hadoop/axx")
			FileStatus[] paths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoop/ax?"));
			Assert.Equal(2, paths.Length);
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxa), paths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxx), paths
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGlobStatusFilterWithEmptyPathResults()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa), GetTestRootPath(fc
				, TestDirAxa), GetTestRootPath(fc, TestDirAxx), GetTestRootPath(fc, TestDirAxx) };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			//This should return an empty set
			FileStatus[] filteredPaths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoop/?"
				), DefaultFilter);
			Assert.Equal(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGlobStatusFilterWithSomePathMatchesAndTrivialFilter()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa), GetTestRootPath(fc
				, TestDirAxa), GetTestRootPath(fc, TestDirAxx), GetTestRootPath(fc, TestDirAxx) };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			//This should return all three (aaa, axa, axx)
			FileStatus[] filteredPaths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoop/*"
				), DefaultFilter);
			Assert.Equal(3, filteredPaths.Length);
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAaa), filteredPaths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxa), filteredPaths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxx), filteredPaths
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGlobStatusFilterWithMultipleWildCardMatchesAndTrivialFilter
			()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa), GetTestRootPath(fc
				, TestDirAxa), GetTestRootPath(fc, TestDirAxx), GetTestRootPath(fc, TestDirAxx) };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			//This should return all three (aaa, axa, axx)
			FileStatus[] filteredPaths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoop/a??"
				), DefaultFilter);
			Assert.Equal(3, filteredPaths.Length);
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAaa), filteredPaths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxa), filteredPaths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxx), filteredPaths
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGlobStatusFilterWithMultiplePathMatchesAndNonTrivialFilter
			()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa), GetTestRootPath(fc
				, TestDirAxa), GetTestRootPath(fc, TestDirAxx), GetTestRootPath(fc, TestDirAxx) };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			//This should return two (axa, axx)
			FileStatus[] filteredPaths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoop/*"
				), TestXFilter);
			Assert.Equal(2, filteredPaths.Length);
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxa), filteredPaths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxx), filteredPaths
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGlobStatusFilterWithNoMatchingPathsAndNonTrivialFilter()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa), GetTestRootPath(fc
				, TestDirAxa), GetTestRootPath(fc, TestDirAxx), GetTestRootPath(fc, TestDirAxx) };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			//This should return an empty set
			FileStatus[] filteredPaths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoop/?"
				), TestXFilter);
			Assert.Equal(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter
			()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fc, TestDirAaa), GetTestRootPath(fc
				, TestDirAxa), GetTestRootPath(fc, TestDirAxx), GetTestRootPath(fc, TestDirAxx) };
			if (FileContextTestHelper.Exists(fc, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fc.Mkdir(path, FsPermission.GetDefault(), true);
				}
			}
			//This should return two (axa, axx)
			FileStatus[] filteredPaths = fc.Util().GlobStatus(GetTestRootPath(fc, "test/hadoop/a??"
				), TestXFilter);
			Assert.Equal(2, filteredPaths.Length);
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxa), filteredPaths
				));
			Assert.True(ContainsPath(GetTestRootPath(fc, TestDirAxx), filteredPaths
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWriteReadAndDeleteEmptyFile()
		{
			WriteReadAndDelete(0);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWriteReadAndDeleteHalfABlock()
		{
			WriteReadAndDelete(FileContextTestHelper.GetDefaultBlockSize() / 2);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWriteReadAndDeleteOneBlock()
		{
			WriteReadAndDelete(FileContextTestHelper.GetDefaultBlockSize());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWriteReadAndDeleteOneAndAHalfBlocks()
		{
			int blockSize = FileContextTestHelper.GetDefaultBlockSize();
			WriteReadAndDelete(blockSize + (blockSize / 2));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWriteReadAndDeleteTwoBlocks()
		{
			WriteReadAndDelete(FileContextTestHelper.GetDefaultBlockSize() * 2);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteReadAndDelete(int len)
		{
			Path path = GetTestRootPath(fc, "test/hadoop/file");
			fc.Mkdir(path.GetParent(), FsPermission.GetDefault(), true);
			FSDataOutputStream @out = fc.Create(path, EnumSet.Of(CreateFlag.Create), Options.CreateOpts
				.RepFac((short)1), Options.CreateOpts.BlockSize(FileContextTestHelper.GetDefaultBlockSize
				()));
			@out.Write(data, 0, len);
			@out.Close();
			Assert.True("Exists", FileContextTestHelper.Exists(fc, path));
			Assert.Equal("Length", len, fc.GetFileStatus(path).GetLen());
			FSDataInputStream @in = fc.Open(path);
			byte[] buf = new byte[len];
			@in.ReadFully(0, buf);
			@in.Close();
			Assert.Equal(len, buf.Length);
			for (int i = 0; i < buf.Length; i++)
			{
				Assert.Equal("Position " + i, data[i], buf[i]);
			}
			Assert.True("Deleted", fc.Delete(path, false));
			NUnit.Framework.Assert.IsFalse("No longer exists", FileContextTestHelper.Exists(fc
				, path));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNullCreateFlag()
		{
			Path p = GetTestRootPath(fc, "test/file");
			fc.Create(p, null);
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestEmptyCreateFlag()
		{
			Path p = GetTestRootPath(fc, "test/file");
			fc.Create(p, EnumSet.NoneOf<CreateFlag>());
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateFlagCreateExistingFile()
		{
			Path p = GetTestRootPath(fc, "test/testCreateFlagCreateExistingFile");
			CreateFile(p);
			fc.Create(p, EnumSet.Of(CreateFlag.Create));
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateFlagOverwriteNonExistingFile()
		{
			Path p = GetTestRootPath(fc, "test/testCreateFlagOverwriteNonExistingFile");
			fc.Create(p, EnumSet.Of(CreateFlag.Overwrite));
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateFlagOverwriteExistingFile()
		{
			Path p = GetTestRootPath(fc, "test/testCreateFlagOverwriteExistingFile");
			CreateFile(p);
			FSDataOutputStream @out = fc.Create(p, EnumSet.Of(CreateFlag.Overwrite));
			WriteData(fc, p, @out, data, data.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateFlagAppendNonExistingFile()
		{
			Path p = GetTestRootPath(fc, "test/testCreateFlagAppendNonExistingFile");
			fc.Create(p, EnumSet.Of(CreateFlag.Append));
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateFlagAppendExistingFile()
		{
			Path p = GetTestRootPath(fc, "test/testCreateFlagAppendExistingFile");
			CreateFile(p);
			FSDataOutputStream @out = fc.Create(p, EnumSet.Of(CreateFlag.Append));
			WriteData(fc, p, @out, data, 2 * data.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateFlagCreateAppendNonExistingFile()
		{
			Path p = GetTestRootPath(fc, "test/testCreateFlagCreateAppendNonExistingFile");
			FSDataOutputStream @out = fc.Create(p, EnumSet.Of(CreateFlag.Create, CreateFlag.Append
				));
			WriteData(fc, p, @out, data, data.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateFlagCreateAppendExistingFile()
		{
			Path p = GetTestRootPath(fc, "test/testCreateFlagCreateAppendExistingFile");
			CreateFile(p);
			FSDataOutputStream @out = fc.Create(p, EnumSet.Of(CreateFlag.Create, CreateFlag.Append
				));
			WriteData(fc, p, @out, data, 2 * data.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateFlagAppendOverwrite()
		{
			Path p = GetTestRootPath(fc, "test/nonExistent");
			fc.Create(p, EnumSet.Of(CreateFlag.Append, CreateFlag.Overwrite));
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateFlagAppendCreateOverwrite()
		{
			Path p = GetTestRootPath(fc, "test/nonExistent");
			fc.Create(p, EnumSet.Of(CreateFlag.Create, CreateFlag.Append, CreateFlag.Overwrite
				));
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteData(FileContext fc, Path p, FSDataOutputStream @out, byte
			[] data, long expectedLen)
		{
			@out.Write(data, 0, data.Length);
			@out.Close();
			Assert.True("Exists", FileContextTestHelper.Exists(fc, p));
			Assert.Equal("Length", expectedLen, fc.GetFileStatus(p).GetLen
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestWriteInNonExistentDirectory()
		{
			Path path = GetTestRootPath(fc, "test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Parent doesn't exist", FileContextTestHelper.Exists
				(fc, path.GetParent()));
			CreateFile(path);
			Assert.True("Exists", FileContextTestHelper.Exists(fc, path));
			Assert.Equal("Length", data.Length, fc.GetFileStatus(path).GetLen
				());
			Assert.True("Parent exists", FileContextTestHelper.Exists(fc, path
				.GetParent()));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteNonExistentFile()
		{
			Path path = GetTestRootPath(fc, "test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Doesn't exist", FileContextTestHelper.Exists(fc, 
				path));
			NUnit.Framework.Assert.IsFalse("No deletion", fc.Delete(path, true));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteRecursively()
		{
			Path dir = GetTestRootPath(fc, "test/hadoop");
			Path file = GetTestRootPath(fc, "test/hadoop/file");
			Path subdir = GetTestRootPath(fc, "test/hadoop/subdir");
			CreateFile(file);
			fc.Mkdir(subdir, FsPermission.GetDefault(), true);
			Assert.True("File exists", FileContextTestHelper.Exists(fc, file
				));
			Assert.True("Dir exists", FileContextTestHelper.Exists(fc, dir)
				);
			Assert.True("Subdir exists", FileContextTestHelper.Exists(fc, subdir
				));
			try
			{
				fc.Delete(dir, false);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			Assert.True("File still exists", FileContextTestHelper.Exists(fc
				, file));
			Assert.True("Dir still exists", FileContextTestHelper.Exists(fc
				, dir));
			Assert.True("Subdir still exists", FileContextTestHelper.Exists
				(fc, subdir));
			Assert.True("Deleted", fc.Delete(dir, true));
			NUnit.Framework.Assert.IsFalse("File doesn't exist", FileContextTestHelper.Exists
				(fc, file));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", FileContextTestHelper.Exists(
				fc, dir));
			NUnit.Framework.Assert.IsFalse("Subdir doesn't exist", FileContextTestHelper.Exists
				(fc, subdir));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteEmptyDirectory()
		{
			Path dir = GetTestRootPath(fc, "test/hadoop");
			fc.Mkdir(dir, FsPermission.GetDefault(), true);
			Assert.True("Dir exists", FileContextTestHelper.Exists(fc, dir)
				);
			Assert.True("Deleted", fc.Delete(dir, false));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", FileContextTestHelper.Exists(
				fc, dir));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameNonExistentPath()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/nonExistent");
			Path dst = GetTestRootPath(fc, "test/new/newpath");
			try
			{
				Rename(src, dst, false, false, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (IOException e)
			{
				Assert.True(UnwrapException(e) is FileNotFoundException);
			}
			try
			{
				Rename(src, dst, false, false, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (IOException e)
			{
				Assert.True(UnwrapException(e) is FileNotFoundException);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameFileToNonExistentDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/file");
			CreateFile(src);
			Path dst = GetTestRootPath(fc, "test/nonExistent/newfile");
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				Assert.True(UnwrapException(e) is FileNotFoundException);
			}
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				Assert.True(UnwrapException(e) is FileNotFoundException);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameFileToDestinationWithParentFile()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/file");
			CreateFile(src);
			Path dst = GetTestRootPath(fc, "test/parentFile/newfile");
			CreateFile(dst.GetParent());
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException)
			{
			}
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameFileToExistingParent()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/file");
			CreateFile(src);
			Path dst = GetTestRootPath(fc, "test/new/newfile");
			fc.Mkdir(dst.GetParent(), FileContext.DefaultPerm, true);
			Rename(src, dst, true, false, true, Options.Rename.Overwrite);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameFileToItself()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/file");
			CreateFile(src);
			try
			{
				Rename(src, src, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Renamed file to itself");
			}
			catch (IOException e)
			{
				Assert.True(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Also fails with overwrite
			try
			{
				Rename(src, src, false, true, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Renamed file to itself");
			}
			catch (IOException e)
			{
				Assert.True(UnwrapException(e) is FileAlreadyExistsException);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameFileAsExistingFile()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/file");
			CreateFile(src);
			Path dst = GetTestRootPath(fc, "test/new/existingFile");
			CreateFile(dst);
			// Fails without overwrite option
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				Assert.True(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Succeeds with overwrite option
			Rename(src, dst, true, false, true, Options.Rename.Overwrite);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameFileAsExistingDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/file");
			CreateFile(src);
			Path dst = GetTestRootPath(fc, "test/new/existingDir");
			fc.Mkdir(dst, FileContext.DefaultPerm, true);
			// Fails without overwrite option
			try
			{
				Rename(src, dst, false, false, true, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException)
			{
			}
			// File cannot be renamed as directory
			try
			{
				Rename(src, dst, false, false, true, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameDirectoryToItself()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/dir");
			fc.Mkdir(src, FileContext.DefaultPerm, true);
			try
			{
				Rename(src, src, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Renamed directory to itself");
			}
			catch (IOException e)
			{
				Assert.True(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Also fails with overwrite
			try
			{
				Rename(src, src, false, true, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Renamed directory to itself");
			}
			catch (IOException e)
			{
				Assert.True(UnwrapException(e) is FileAlreadyExistsException);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameDirectoryToNonExistentParent()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/dir");
			fc.Mkdir(src, FileContext.DefaultPerm, true);
			Path dst = GetTestRootPath(fc, "test/nonExistent/newdir");
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				Assert.True(UnwrapException(e) is FileNotFoundException);
			}
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				Assert.True(UnwrapException(e) is FileNotFoundException);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameDirectoryAsNonExistentDirectory()
		{
			TestRenameDirectoryAsNonExistentDirectory(Options.Rename.None);
			TearDown();
			TestRenameDirectoryAsNonExistentDirectory(Options.Rename.Overwrite);
		}

		/// <exception cref="System.Exception"/>
		private void TestRenameDirectoryAsNonExistentDirectory(params Options.Rename[] options
			)
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/dir");
			fc.Mkdir(src, FileContext.DefaultPerm, true);
			CreateFile(GetTestRootPath(fc, "test/hadoop/dir/file1"));
			CreateFile(GetTestRootPath(fc, "test/hadoop/dir/subdir/file2"));
			Path dst = GetTestRootPath(fc, "test/new/newdir");
			fc.Mkdir(dst.GetParent(), FileContext.DefaultPerm, true);
			Rename(src, dst, true, false, true, options);
			NUnit.Framework.Assert.IsFalse("Nested file1 exists", FileContextTestHelper.Exists
				(fc, GetTestRootPath(fc, "test/hadoop/dir/file1")));
			NUnit.Framework.Assert.IsFalse("Nested file2 exists", FileContextTestHelper.Exists
				(fc, GetTestRootPath(fc, "test/hadoop/dir/subdir/file2")));
			Assert.True("Renamed nested file1 exists", FileContextTestHelper.Exists
				(fc, GetTestRootPath(fc, "test/new/newdir/file1")));
			Assert.True("Renamed nested exists", FileContextTestHelper.Exists
				(fc, GetTestRootPath(fc, "test/new/newdir/subdir/file2")));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameDirectoryAsEmptyDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/dir");
			fc.Mkdir(src, FileContext.DefaultPerm, true);
			CreateFile(GetTestRootPath(fc, "test/hadoop/dir/file1"));
			CreateFile(GetTestRootPath(fc, "test/hadoop/dir/subdir/file2"));
			Path dst = GetTestRootPath(fc, "test/new/newdir");
			fc.Mkdir(dst, FileContext.DefaultPerm, true);
			// Fails without overwrite option
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				// Expected (cannot over-write non-empty destination)
				Assert.True(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Succeeds with the overwrite option
			Rename(src, dst, true, false, true, Options.Rename.Overwrite);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameDirectoryAsNonEmptyDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/dir");
			fc.Mkdir(src, FileContext.DefaultPerm, true);
			CreateFile(GetTestRootPath(fc, "test/hadoop/dir/file1"));
			CreateFile(GetTestRootPath(fc, "test/hadoop/dir/subdir/file2"));
			Path dst = GetTestRootPath(fc, "test/new/newdir");
			fc.Mkdir(dst, FileContext.DefaultPerm, true);
			CreateFile(GetTestRootPath(fc, "test/new/newdir/file1"));
			// Fails without overwrite option
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				// Expected (cannot over-write non-empty destination)
				Assert.True(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Fails even with the overwrite option
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException)
			{
			}
		}

		// Expected (cannot over-write non-empty destination)
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenameDirectoryAsFile()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fc, "test/hadoop/dir");
			fc.Mkdir(src, FileContext.DefaultPerm, true);
			Path dst = GetTestRootPath(fc, "test/new/newfile");
			CreateFile(dst);
			// Fails without overwrite option
			try
			{
				Rename(src, dst, false, true, true, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException)
			{
			}
			// Directory cannot be renamed as existing file
			try
			{
				Rename(src, dst, false, true, true, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestInputStreamClosedTwice()
		{
			//HADOOP-4760 according to Closeable#close() closing already-closed 
			//streams should have no effect. 
			Path src = GetTestRootPath(fc, "test/hadoop/file");
			CreateFile(src);
			FSDataInputStream @in = fc.Open(src);
			@in.Close();
			@in.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestOutputStreamClosedTwice()
		{
			//HADOOP-4760 according to Closeable#close() closing already-closed 
			//streams should have no effect. 
			Path src = GetTestRootPath(fc, "test/hadoop/file");
			FSDataOutputStream @out = fc.Create(src, EnumSet.Of(CreateFlag.Create), Options.CreateOpts
				.CreateParent());
			@out.WriteChar('H');
			//write some data
			@out.Close();
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestUnsupportedSymlink()
		{
			Path file = GetTestRootPath(fc, "file");
			Path link = GetTestRootPath(fc, "linkToFile");
			if (!fc.GetDefaultFileSystem().SupportsSymlinks())
			{
				try
				{
					fc.CreateSymlink(file, link, false);
					NUnit.Framework.Assert.Fail("Created a symlink on a file system that " + "does not support symlinks."
						);
				}
				catch (IOException)
				{
				}
				// Expected
				CreateFile(file);
				try
				{
					fc.GetLinkTarget(file);
					NUnit.Framework.Assert.Fail("Got a link target on a file system that " + "does not support symlinks."
						);
				}
				catch (IOException)
				{
				}
				// Expected
				Assert.Equal(fc.GetFileStatus(file), fc.GetFileLinkStatus(file
					));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CreateFile(Path path)
		{
			FSDataOutputStream @out = fc.Create(path, EnumSet.Of(CreateFlag.Create), Options.CreateOpts
				.CreateParent());
			@out.Write(data, 0, data.Length);
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void Rename(Path src, Path dst, bool renameShouldSucceed, bool srcExists, 
			bool dstExists, params Options.Rename[] options)
		{
			fc.Rename(src, dst, options);
			if (!renameShouldSucceed)
			{
				NUnit.Framework.Assert.Fail("rename should have thrown exception");
			}
			Assert.Equal("Source exists", srcExists, FileContextTestHelper.Exists
				(fc, src));
			Assert.Equal("Destination exists", dstExists, FileContextTestHelper.Exists
				(fc, dst));
		}

		/// <exception cref="System.IO.IOException"/>
		private bool ContainsPath(Path path, FileStatus[] filteredPaths)
		{
			for (int i = 0; i < filteredPaths.Length; i++)
			{
				if (GetTestRootPath(fc, path.ToString()).Equals(filteredPaths[i].GetPath()))
				{
					return true;
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestOpen2()
		{
			Path rootPath = GetTestRootPath(fc, "test");
			//final Path rootPath = getAbsoluteTestRootPath(fc);
			Path path = new Path(rootPath, "zoo");
			CreateFile(path);
			long length = fc.GetFileStatus(path).GetLen();
			FSDataInputStream fsdis = fc.Open(path, 2048);
			try
			{
				byte[] bb = new byte[(int)length];
				fsdis.ReadFully(bb);
				Assert.AssertArrayEquals(data, bb);
			}
			finally
			{
				fsdis.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestSetVerifyChecksum()
		{
			Path rootPath = GetTestRootPath(fc, "test");
			Path path = new Path(rootPath, "zoo");
			FSDataOutputStream @out = fc.Create(path, EnumSet.Of(CreateFlag.Create), Options.CreateOpts
				.CreateParent());
			try
			{
				// instruct FS to verify checksum through the FileContext:
				fc.SetVerifyChecksum(true, path);
				@out.Write(data, 0, data.Length);
			}
			finally
			{
				@out.Close();
			}
			// NB: underlying FS may be different (this is an abstract test),
			// so we cannot assert .zoo.crc existence.
			// Instead, we check that the file is read correctly:
			FileStatus fileStatus = fc.GetFileStatus(path);
			long len = fileStatus.GetLen();
			Assert.True(len == data.Length);
			byte[] bb = new byte[(int)len];
			FSDataInputStream fsdis = fc.Open(path);
			try
			{
				fsdis.Read(bb);
			}
			finally
			{
				fsdis.Close();
			}
			Assert.AssertArrayEquals(data, bb);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestListCorruptFileBlocks()
		{
			Path rootPath = GetTestRootPath(fc, "test");
			Path path = new Path(rootPath, "zoo");
			CreateFile(path);
			try
			{
				RemoteIterator<Path> remoteIterator = fc.ListCorruptFileBlocks(path);
				if (ListCorruptedBlocksSupported())
				{
					Assert.True(remoteIterator != null);
					Path p;
					while (remoteIterator.HasNext())
					{
						p = remoteIterator.Next();
						System.Console.Out.WriteLine("corrupted block: " + p);
					}
					try
					{
						remoteIterator.Next();
						NUnit.Framework.Assert.Fail();
					}
					catch (NoSuchElementException)
					{
					}
				}
				else
				{
					// okay
					NUnit.Framework.Assert.Fail();
				}
			}
			catch (NotSupportedException uoe)
			{
				if (ListCorruptedBlocksSupported())
				{
					NUnit.Framework.Assert.Fail(uoe.ToString());
				}
			}
		}

		// okay
		protected internal abstract bool ListCorruptedBlocksSupported();

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteOnExitUnexisting()
		{
			Path rootPath = GetTestRootPath(fc, "test");
			Path path = new Path(rootPath, "zoo");
			bool registered = fc.DeleteOnExit(path);
			// because "zoo" does not exist:
			Assert.True(!registered);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestFileContextStatistics()
		{
			FileContext.ClearStatistics();
			Path rootPath = GetTestRootPath(fc, "test");
			Path path = new Path(rootPath, "zoo");
			CreateFile(path);
			byte[] bb = new byte[data.Length];
			FSDataInputStream fsdis = fc.Open(path);
			try
			{
				fsdis.Read(bb);
			}
			finally
			{
				fsdis.Close();
			}
			Assert.AssertArrayEquals(data, bb);
			FileContext.PrintStatistics();
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGetFileContext1()
		{
			/*
			* Test method
			*  org.apache.hadoop.fs.FileContext.getFileContext(AbstractFileSystem)
			*/
			Path rootPath = GetTestRootPath(fc, "test");
			AbstractFileSystem asf = fc.GetDefaultFileSystem();
			// create FileContext using the protected #getFileContext(1) method:
			FileContext fc2 = FileContext.GetFileContext(asf);
			// Now just check that this context can do something reasonable:
			Path path = new Path(rootPath, "zoo");
			FSDataOutputStream @out = fc2.Create(path, EnumSet.Of(CreateFlag.Create), Options.CreateOpts
				.CreateParent());
			@out.Close();
			Path pathResolved = fc2.ResolvePath(path);
			Assert.Equal(pathResolved.ToUri().GetPath(), path.ToUri().GetPath
				());
		}

		private Path GetTestRootPath(FileContext fc, string pathString)
		{
			return fileContextTestHelper.GetTestRootPath(fc, pathString);
		}

		public FileContextMainOperationsBaseTest()
		{
			fileContextTestHelper = CreateFileContextHelper();
		}
	}
}
