using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// <p>
	/// A collection of tests for the
	/// <see cref="FileSystem"/>
	/// .
	/// This test should be used for testing an instance of FileSystem
	/// that has been initialized to a specific default FileSystem such a
	/// LocalFileSystem, HDFS,S3, etc.
	/// </p>
	/// <p>
	/// To test a given
	/// <see cref="FileSystem"/>
	/// implementation create a subclass of this
	/// test and override
	/// <see cref="SetUp()"/>
	/// to initialize the <code>fSys</code>
	/// <see cref="FileSystem"/>
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
	public abstract class FSMainOperationsBaseTest : FileSystemTestHelper
	{
		private static string TestDirAaa2 = "test/hadoop2/aaa";

		private static string TestDirAaa = "test/hadoop/aaa";

		private static string TestDirAxa = "test/hadoop/axa";

		private static string TestDirAxx = "test/hadoop/axx";

		private static int numBlocks = 2;

		protected internal FileSystem fSys;

		private sealed class _PathFilter_67 : PathFilter
		{
			public _PathFilter_67()
			{
			}

			public bool Accept(Path file)
			{
				return true;
			}
		}

		private static readonly PathFilter DefaultFilter = new _PathFilter_67();

		private sealed class _PathFilter_75 : PathFilter
		{
			public _PathFilter_75()
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

		private static readonly PathFilter TestXFilter = new _PathFilter_75();

		protected internal static readonly byte[] data = GetFileData(numBlocks, GetDefaultBlockSize
			());

		/// <exception cref="System.Exception"/>
		protected internal abstract FileSystem CreateFileSystem();

		public FSMainOperationsBaseTest()
		{
		}

		public FSMainOperationsBaseTest(string testRootDir)
			: base(testRootDir)
		{
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			fSys = CreateFileSystem();
			fSys.Mkdirs(GetTestRootPath(fSys, "test"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fSys.Delete(new Path(GetAbsoluteTestRootPath(fSys), new Path("test")), true);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Path GetDefaultWorkingDirectory()
		{
			return GetTestRootPath(fSys, "/user/" + Runtime.GetProperty("user.name")).MakeQualified
				(fSys.GetUri(), fSys.GetWorkingDirectory());
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
		[NUnit.Framework.Test]
		public virtual void TestFsStatus()
		{
			FsStatus fsStatus = fSys.GetStatus(null);
			NUnit.Framework.Assert.IsNotNull(fsStatus);
			//used, free and capacity are non-negative longs
			NUnit.Framework.Assert.IsTrue(fsStatus.GetUsed() >= 0);
			NUnit.Framework.Assert.IsTrue(fsStatus.GetRemaining() >= 0);
			NUnit.Framework.Assert.IsTrue(fsStatus.GetCapacity() >= 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWorkingDirectory()
		{
			// First we cd to our test root
			Path workDir = new Path(GetAbsoluteTestRootPath(fSys), new Path("test"));
			fSys.SetWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(workDir, fSys.GetWorkingDirectory());
			fSys.SetWorkingDirectory(new Path("."));
			NUnit.Framework.Assert.AreEqual(workDir, fSys.GetWorkingDirectory());
			fSys.SetWorkingDirectory(new Path(".."));
			NUnit.Framework.Assert.AreEqual(workDir.GetParent(), fSys.GetWorkingDirectory());
			// cd using a relative path
			// Go back to our test root
			workDir = new Path(GetAbsoluteTestRootPath(fSys), new Path("test"));
			fSys.SetWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(workDir, fSys.GetWorkingDirectory());
			Path relativeDir = new Path("existingDir1");
			Path absoluteDir = new Path(workDir, "existingDir1");
			fSys.Mkdirs(absoluteDir);
			fSys.SetWorkingDirectory(relativeDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fSys.GetWorkingDirectory());
			// cd using a absolute path
			absoluteDir = GetTestRootPath(fSys, "test/existingDir2");
			fSys.Mkdirs(absoluteDir);
			fSys.SetWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fSys.GetWorkingDirectory());
			// Now open a file relative to the wd we just set above.
			Path absolutePath = new Path(absoluteDir, "foo");
			CreateFile(fSys, absolutePath);
			fSys.Open(new Path("foo")).Close();
			// Now mkdir relative to the dir we cd'ed to
			fSys.Mkdirs(new Path("newDir"));
			NUnit.Framework.Assert.IsTrue(IsDir(fSys, new Path(absoluteDir, "newDir")));
		}

		// Try a URI
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWDAbsolute()
		{
			Path absoluteDir = new Path(fSys.GetUri() + "/test/existingDir");
			fSys.Mkdirs(absoluteDir);
			fSys.SetWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fSys.GetWorkingDirectory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirs()
		{
			Path testDir = GetTestRootPath(fSys, "test/hadoop");
			NUnit.Framework.Assert.IsFalse(Exists(fSys, testDir));
			NUnit.Framework.Assert.IsFalse(IsFile(fSys, testDir));
			fSys.Mkdirs(testDir);
			NUnit.Framework.Assert.IsTrue(Exists(fSys, testDir));
			NUnit.Framework.Assert.IsFalse(IsFile(fSys, testDir));
			fSys.Mkdirs(testDir);
			NUnit.Framework.Assert.IsTrue(Exists(fSys, testDir));
			NUnit.Framework.Assert.IsFalse(IsFile(fSys, testDir));
			Path parentDir = testDir.GetParent();
			NUnit.Framework.Assert.IsTrue(Exists(fSys, parentDir));
			NUnit.Framework.Assert.IsFalse(IsFile(fSys, parentDir));
			Path grandparentDir = parentDir.GetParent();
			NUnit.Framework.Assert.IsTrue(Exists(fSys, grandparentDir));
			NUnit.Framework.Assert.IsFalse(IsFile(fSys, grandparentDir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirsFailsForSubdirectoryOfExistingFile()
		{
			Path testDir = GetTestRootPath(fSys, "test/hadoop");
			NUnit.Framework.Assert.IsFalse(Exists(fSys, testDir));
			fSys.Mkdirs(testDir);
			NUnit.Framework.Assert.IsTrue(Exists(fSys, testDir));
			CreateFile(GetTestRootPath(fSys, "test/hadoop/file"));
			Path testSubDir = GetTestRootPath(fSys, "test/hadoop/file/subdir");
			try
			{
				fSys.Mkdirs(testSubDir);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(Exists(fSys, testSubDir));
			Path testDeepSubDir = GetTestRootPath(fSys, "test/hadoop/file/deep/sub/dir");
			try
			{
				fSys.Mkdirs(testDeepSubDir);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(Exists(fSys, testDeepSubDir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFileStatusThrowsExceptionForNonExistentFile()
		{
			try
			{
				fSys.GetFileStatus(GetTestRootPath(fSys, "test/hadoop/file"));
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusThrowsExceptionForNonExistentFile()
		{
			try
			{
				fSys.ListStatus(GetTestRootPath(fSys, "test/hadoop/file"));
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// expected
		// TODO: update after fixing HADOOP-7352
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusThrowsExceptionForUnreadableDir()
		{
			Path testRootDir = GetTestRootPath(fSys, "test/hadoop/dir");
			Path obscuredDir = new Path(testRootDir, "foo");
			Path subDir = new Path(obscuredDir, "bar");
			//so foo is non-empty
			fSys.Mkdirs(subDir);
			fSys.SetPermission(obscuredDir, new FsPermission((short)0));
			//no access
			try
			{
				fSys.ListStatus(obscuredDir);
				NUnit.Framework.Assert.Fail("Should throw IOException");
			}
			catch (IOException)
			{
			}
			finally
			{
				// expected
				// make sure the test directory can be deleted
				fSys.SetPermission(obscuredDir, new FsPermission((short)0x1ed));
			}
		}

		//default
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatus()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, "test/hadoop/a"), GetTestRootPath
				(fSys, "test/hadoop/b"), GetTestRootPath(fSys, "test/hadoop/c/1") };
			NUnit.Framework.Assert.IsFalse(Exists(fSys, testDirs[0]));
			foreach (Path path in testDirs)
			{
				fSys.Mkdirs(path);
			}
			// test listStatus that returns an array
			FileStatus[] paths = fSys.ListStatus(GetTestRootPath(fSys, "test"));
			NUnit.Framework.Assert.AreEqual(1, paths.Length);
			NUnit.Framework.Assert.AreEqual(GetTestRootPath(fSys, "test/hadoop"), paths[0].GetPath
				());
			paths = fSys.ListStatus(GetTestRootPath(fSys, "test/hadoop"));
			NUnit.Framework.Assert.AreEqual(3, paths.Length);
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, "test/hadoop/a"
				), paths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, "test/hadoop/b"
				), paths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, "test/hadoop/c"
				), paths));
			paths = fSys.ListStatus(GetTestRootPath(fSys, "test/hadoop/a"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusFilterWithNoMatches()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa2), GetTestRootPath
				(fSys, TestDirAaa), GetTestRootPath(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			// listStatus with filters returns empty correctly
			FileStatus[] filteredPaths = fSys.ListStatus(GetTestRootPath(fSys, "test"), TestXFilter
				);
			NUnit.Framework.Assert.AreEqual(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusFilterWithSomeMatches()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa), GetTestRootPath
				(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx), GetTestRootPath(fSys, TestDirAaa2
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			// should return 2 paths ("/test/hadoop/axa" and "/test/hadoop/axx")
			FileStatus[] filteredPaths = fSys.ListStatus(GetTestRootPath(fSys, "test/hadoop")
				, TestXFilter);
			NUnit.Framework.Assert.AreEqual(2, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxa
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxx
				), filteredPaths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobStatusNonExistentFile()
		{
			FileStatus[] paths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoopfsdf"));
			NUnit.Framework.Assert.IsNull(paths);
			paths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoopfsdf/?"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
			paths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoopfsdf/xyz*/?"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobStatusWithNoMatchesInPath()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa), GetTestRootPath
				(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx), GetTestRootPath(fSys, TestDirAaa2
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			// should return nothing
			FileStatus[] paths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoop/?"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobStatusSomeMatchesInDirectories()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa), GetTestRootPath
				(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx), GetTestRootPath(fSys, TestDirAaa2
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			// Should return two items ("/test/hadoop" and "/test/hadoop2")
			FileStatus[] paths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoop*"));
			NUnit.Framework.Assert.AreEqual(2, paths.Length);
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, "test/hadoop"
				), paths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, "test/hadoop2"
				), paths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobStatusWithMultipleWildCardMatches()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa), GetTestRootPath
				(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx), GetTestRootPath(fSys, TestDirAaa2
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			//Should return all 4 items ("/test/hadoop/aaa", "/test/hadoop/axa"
			//"/test/hadoop/axx", and "/test/hadoop2/axx")
			FileStatus[] paths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoop*/*"));
			NUnit.Framework.Assert.AreEqual(4, paths.Length);
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAaa
				), paths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxa
				), paths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxx
				), paths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAaa2
				), paths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobStatusWithMultipleMatchesOfSingleChar()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa), GetTestRootPath
				(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx), GetTestRootPath(fSys, TestDirAaa2
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			//Should return only 2 items ("/test/hadoop/axa", "/test/hadoop/axx")
			FileStatus[] paths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoop/ax?"));
			NUnit.Framework.Assert.AreEqual(2, paths.Length);
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxa
				), paths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxx
				), paths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobStatusFilterWithEmptyPathResults()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa), GetTestRootPath
				(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx), GetTestRootPath(fSys, TestDirAxx
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			//This should return an empty set
			FileStatus[] filteredPaths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoop/?"
				), DefaultFilter);
			NUnit.Framework.Assert.AreEqual(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobStatusFilterWithSomePathMatchesAndTrivialFilter()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa), GetTestRootPath
				(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx), GetTestRootPath(fSys, TestDirAxx
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			//This should return all three (aaa, axa, axx)
			FileStatus[] filteredPaths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoop/*"
				), DefaultFilter);
			NUnit.Framework.Assert.AreEqual(3, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAaa
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxa
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxx
				), filteredPaths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobStatusFilterWithMultipleWildCardMatchesAndTrivialFilter
			()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa), GetTestRootPath
				(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx), GetTestRootPath(fSys, TestDirAxx
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			//This should return all three (aaa, axa, axx)
			FileStatus[] filteredPaths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoop/a??"
				), DefaultFilter);
			NUnit.Framework.Assert.AreEqual(3, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAaa
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxa
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxx
				), filteredPaths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobStatusFilterWithMultiplePathMatchesAndNonTrivialFilter
			()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa), GetTestRootPath
				(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx), GetTestRootPath(fSys, TestDirAxx
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			//This should return two (axa, axx)
			FileStatus[] filteredPaths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoop/*"
				), TestXFilter);
			NUnit.Framework.Assert.AreEqual(2, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxa
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxx
				), filteredPaths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobStatusFilterWithNoMatchingPathsAndNonTrivialFilter()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa), GetTestRootPath
				(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx), GetTestRootPath(fSys, TestDirAxx
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			//This should return an empty set
			FileStatus[] filteredPaths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoop/?"
				), TestXFilter);
			NUnit.Framework.Assert.AreEqual(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter
			()
		{
			Path[] testDirs = new Path[] { GetTestRootPath(fSys, TestDirAaa), GetTestRootPath
				(fSys, TestDirAxa), GetTestRootPath(fSys, TestDirAxx), GetTestRootPath(fSys, TestDirAxx
				) };
			if (Exists(fSys, testDirs[0]) == false)
			{
				foreach (Path path in testDirs)
				{
					fSys.Mkdirs(path);
				}
			}
			//This should return two (axa, axx)
			FileStatus[] filteredPaths = fSys.GlobStatus(GetTestRootPath(fSys, "test/hadoop/a??"
				), TestXFilter);
			NUnit.Framework.Assert.AreEqual(2, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxa
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(ContainsTestRootPath(GetTestRootPath(fSys, TestDirAxx
				), filteredPaths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteReadAndDeleteEmptyFile()
		{
			WriteReadAndDelete(0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteReadAndDeleteHalfABlock()
		{
			WriteReadAndDelete(GetDefaultBlockSize() / 2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteReadAndDeleteOneBlock()
		{
			WriteReadAndDelete(GetDefaultBlockSize());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteReadAndDeleteOneAndAHalfBlocks()
		{
			int blockSize = GetDefaultBlockSize();
			WriteReadAndDelete(blockSize + (blockSize / 2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteReadAndDeleteTwoBlocks()
		{
			WriteReadAndDelete(GetDefaultBlockSize() * 2);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void WriteReadAndDelete(int len)
		{
			Path path = GetTestRootPath(fSys, "test/hadoop/file");
			fSys.Mkdirs(path.GetParent());
			FSDataOutputStream @out = fSys.Create(path, false, 4096, (short)1, GetDefaultBlockSize
				());
			@out.Write(data, 0, len);
			@out.Close();
			NUnit.Framework.Assert.IsTrue("Exists", Exists(fSys, path));
			NUnit.Framework.Assert.AreEqual("Length", len, fSys.GetFileStatus(path).GetLen());
			FSDataInputStream @in = fSys.Open(path);
			byte[] buf = new byte[len];
			@in.ReadFully(0, buf);
			@in.Close();
			NUnit.Framework.Assert.AreEqual(len, buf.Length);
			for (int i = 0; i < buf.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual("Position " + i, data[i], buf[i]);
			}
			NUnit.Framework.Assert.IsTrue("Deleted", fSys.Delete(path, false));
			NUnit.Framework.Assert.IsFalse("No longer exists", Exists(fSys, path));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestOverwrite()
		{
			Path path = GetTestRootPath(fSys, "test/hadoop/file");
			fSys.Mkdirs(path.GetParent());
			CreateFile(path);
			NUnit.Framework.Assert.IsTrue("Exists", Exists(fSys, path));
			NUnit.Framework.Assert.AreEqual("Length", data.Length, fSys.GetFileStatus(path).GetLen
				());
			try
			{
				CreateFile(path);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// Expected
			FSDataOutputStream @out = fSys.Create(path, true, 4096);
			@out.Write(data, 0, data.Length);
			@out.Close();
			NUnit.Framework.Assert.IsTrue("Exists", Exists(fSys, path));
			NUnit.Framework.Assert.AreEqual("Length", data.Length, fSys.GetFileStatus(path).GetLen
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteInNonExistentDirectory()
		{
			Path path = GetTestRootPath(fSys, "test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Parent doesn't exist", Exists(fSys, path.GetParent
				()));
			CreateFile(path);
			NUnit.Framework.Assert.IsTrue("Exists", Exists(fSys, path));
			NUnit.Framework.Assert.AreEqual("Length", data.Length, fSys.GetFileStatus(path).GetLen
				());
			NUnit.Framework.Assert.IsTrue("Parent exists", Exists(fSys, path.GetParent()));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteNonExistentFile()
		{
			Path path = GetTestRootPath(fSys, "test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Doesn't exist", Exists(fSys, path));
			NUnit.Framework.Assert.IsFalse("No deletion", fSys.Delete(path, true));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteRecursively()
		{
			Path dir = GetTestRootPath(fSys, "test/hadoop");
			Path file = GetTestRootPath(fSys, "test/hadoop/file");
			Path subdir = GetTestRootPath(fSys, "test/hadoop/subdir");
			CreateFile(file);
			fSys.Mkdirs(subdir);
			NUnit.Framework.Assert.IsTrue("File exists", Exists(fSys, file));
			NUnit.Framework.Assert.IsTrue("Dir exists", Exists(fSys, dir));
			NUnit.Framework.Assert.IsTrue("Subdir exists", Exists(fSys, subdir));
			try
			{
				fSys.Delete(dir, false);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsTrue("File still exists", Exists(fSys, file));
			NUnit.Framework.Assert.IsTrue("Dir still exists", Exists(fSys, dir));
			NUnit.Framework.Assert.IsTrue("Subdir still exists", Exists(fSys, subdir));
			NUnit.Framework.Assert.IsTrue("Deleted", fSys.Delete(dir, true));
			NUnit.Framework.Assert.IsFalse("File doesn't exist", Exists(fSys, file));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", Exists(fSys, dir));
			NUnit.Framework.Assert.IsFalse("Subdir doesn't exist", Exists(fSys, subdir));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteEmptyDirectory()
		{
			Path dir = GetTestRootPath(fSys, "test/hadoop");
			fSys.Mkdirs(dir);
			NUnit.Framework.Assert.IsTrue("Dir exists", Exists(fSys, dir));
			NUnit.Framework.Assert.IsTrue("Deleted", fSys.Delete(dir, false));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", Exists(fSys, dir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameNonExistentPath()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/nonExistent");
			Path dst = GetTestRootPath(fSys, "test/new/newpath");
			try
			{
				Rename(src, dst, false, false, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (IOException e)
			{
				Org.Mortbay.Log.Log.Info("XXX", e);
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileNotFoundException);
			}
			try
			{
				Rename(src, dst, false, false, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileNotFoundException);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameFileToNonExistentDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/file");
			CreateFile(src);
			Path dst = GetTestRootPath(fSys, "test/nonExistent/newfile");
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileNotFoundException);
			}
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileNotFoundException);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameFileToDestinationWithParentFile()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/file");
			CreateFile(src);
			Path dst = GetTestRootPath(fSys, "test/parentFile/newfile");
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
		[NUnit.Framework.Test]
		public virtual void TestRenameFileToExistingParent()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/file");
			CreateFile(src);
			Path dst = GetTestRootPath(fSys, "test/new/newfile");
			fSys.Mkdirs(dst.GetParent());
			Rename(src, dst, true, false, true, Options.Rename.Overwrite);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameFileToItself()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/file");
			CreateFile(src);
			try
			{
				Rename(src, src, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Renamed file to itself");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Also fails with overwrite
			try
			{
				Rename(src, src, false, true, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Renamed file to itself");
			}
			catch (IOException)
			{
			}
		}

		// worked
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameFileAsExistingFile()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/file");
			CreateFile(src);
			Path dst = GetTestRootPath(fSys, "test/new/existingFile");
			CreateFile(dst);
			// Fails without overwrite option
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Succeeds with overwrite option
			Rename(src, dst, true, false, true, Options.Rename.Overwrite);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameFileAsExistingDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/file");
			CreateFile(src);
			Path dst = GetTestRootPath(fSys, "test/new/existingDir");
			fSys.Mkdirs(dst);
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
		[NUnit.Framework.Test]
		public virtual void TestRenameDirectoryToItself()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/dir");
			fSys.Mkdirs(src);
			try
			{
				Rename(src, src, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Renamed directory to itself");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Also fails with overwrite
			try
			{
				Rename(src, src, false, true, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Renamed directory to itself");
			}
			catch (IOException)
			{
			}
		}

		// worked      
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameDirectoryToNonExistentParent()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/dir");
			fSys.Mkdirs(src);
			Path dst = GetTestRootPath(fSys, "test/nonExistent/newdir");
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				IOException ioException = UnwrapException(e);
				if (!(ioException is FileNotFoundException))
				{
					throw ioException;
				}
			}
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				IOException ioException = UnwrapException(e);
				if (!(ioException is FileNotFoundException))
				{
					throw ioException;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameDirectoryAsNonExistentDirectory()
		{
			DoTestRenameDirectoryAsNonExistentDirectory(Options.Rename.None);
			TearDown();
			DoTestRenameDirectoryAsNonExistentDirectory(Options.Rename.Overwrite);
		}

		/// <exception cref="System.Exception"/>
		private void DoTestRenameDirectoryAsNonExistentDirectory(params Options.Rename[] 
			options)
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/dir");
			fSys.Mkdirs(src);
			CreateFile(GetTestRootPath(fSys, "test/hadoop/dir/file1"));
			CreateFile(GetTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));
			Path dst = GetTestRootPath(fSys, "test/new/newdir");
			fSys.Mkdirs(dst.GetParent());
			Rename(src, dst, true, false, true, options);
			NUnit.Framework.Assert.IsFalse("Nested file1 exists", Exists(fSys, GetTestRootPath
				(fSys, "test/hadoop/dir/file1")));
			NUnit.Framework.Assert.IsFalse("Nested file2 exists", Exists(fSys, GetTestRootPath
				(fSys, "test/hadoop/dir/subdir/file2")));
			NUnit.Framework.Assert.IsTrue("Renamed nested file1 exists", Exists(fSys, GetTestRootPath
				(fSys, "test/new/newdir/file1")));
			NUnit.Framework.Assert.IsTrue("Renamed nested exists", Exists(fSys, GetTestRootPath
				(fSys, "test/new/newdir/subdir/file2")));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameDirectoryAsEmptyDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/dir");
			fSys.Mkdirs(src);
			CreateFile(GetTestRootPath(fSys, "test/hadoop/dir/file1"));
			CreateFile(GetTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));
			Path dst = GetTestRootPath(fSys, "test/new/newdir");
			fSys.Mkdirs(dst);
			// Fails without overwrite option
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				// Expected (cannot over-write non-empty destination)
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Succeeds with the overwrite option
			Rename(src, dst, true, false, true, Options.Rename.Overwrite);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameDirectoryAsNonEmptyDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/dir");
			fSys.Mkdirs(src);
			CreateFile(GetTestRootPath(fSys, "test/hadoop/dir/file1"));
			CreateFile(GetTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));
			Path dst = GetTestRootPath(fSys, "test/new/newdir");
			fSys.Mkdirs(dst);
			CreateFile(GetTestRootPath(fSys, "test/new/newdir/file1"));
			// Fails without overwrite option
			try
			{
				Rename(src, dst, false, true, false, Options.Rename.None);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (IOException e)
			{
				// Expected (cannot over-write non-empty destination)
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
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
		[NUnit.Framework.Test]
		public virtual void TestRenameDirectoryAsFile()
		{
			if (!RenameSupported())
			{
				return;
			}
			Path src = GetTestRootPath(fSys, "test/hadoop/dir");
			fSys.Mkdirs(src);
			Path dst = GetTestRootPath(fSys, "test/new/newfile");
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
		[NUnit.Framework.Test]
		public virtual void TestInputStreamClosedTwice()
		{
			//HADOOP-4760 according to Closeable#close() closing already-closed 
			//streams should have no effect. 
			Path src = GetTestRootPath(fSys, "test/hadoop/file");
			CreateFile(src);
			FSDataInputStream @in = fSys.Open(src);
			@in.Close();
			@in.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestOutputStreamClosedTwice()
		{
			//HADOOP-4760 according to Closeable#close() closing already-closed 
			//streams should have no effect. 
			Path src = GetTestRootPath(fSys, "test/hadoop/file");
			FSDataOutputStream @out = fSys.Create(src);
			@out.WriteChar('H');
			//write some data
			@out.Close();
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetWrappedInputStream()
		{
			Path src = GetTestRootPath(fSys, "test/hadoop/file");
			CreateFile(src);
			FSDataInputStream @in = fSys.Open(src);
			InputStream @is = @in.GetWrappedStream();
			@in.Close();
			NUnit.Framework.Assert.IsNotNull(@is);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCopyToLocalWithUseRawLocalFileSystemOption()
		{
			Configuration conf = new Configuration();
			FileSystem fSys = new RawLocalFileSystem();
			Path fileToFS = new Path(GetTestRootDir(), "fs.txt");
			Path fileToLFS = new Path(GetTestRootDir(), "test.txt");
			Path crcFileAtLFS = new Path(GetTestRootDir(), ".test.txt.crc");
			fSys.Initialize(new URI("file:///"), conf);
			WriteFile(fSys, fileToFS);
			if (fSys.Exists(crcFileAtLFS))
			{
				NUnit.Framework.Assert.IsTrue("CRC files not deleted", fSys.Delete(crcFileAtLFS, 
					true));
			}
			fSys.CopyToLocalFile(false, fileToFS, fileToLFS, true);
			NUnit.Framework.Assert.IsFalse("CRC files are created", fSys.Exists(crcFileAtLFS)
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fs, Path name)
		{
			FSDataOutputStream stm = fs.Create(name);
			try
			{
				stm.WriteBytes("42\n");
			}
			finally
			{
				stm.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CreateFile(Path path)
		{
			CreateFile(fSys, path);
		}

		/// <exception cref="System.IO.IOException"/>
		private void Rename(Path src, Path dst, bool renameShouldSucceed, bool srcExists, 
			bool dstExists, params Options.Rename[] options)
		{
			fSys.Rename(src, dst, options);
			if (!renameShouldSucceed)
			{
				NUnit.Framework.Assert.Fail("rename should have thrown exception");
			}
			NUnit.Framework.Assert.AreEqual("Source exists", srcExists, Exists(fSys, src));
			NUnit.Framework.Assert.AreEqual("Destination exists", dstExists, Exists(fSys, dst
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private bool ContainsTestRootPath(Path path, FileStatus[] filteredPaths)
		{
			Path testRootPath = GetTestRootPath(fSys, path.ToString());
			for (int i = 0; i < filteredPaths.Length; i++)
			{
				if (testRootPath.Equals(filteredPaths[i].GetPath()))
				{
					return true;
				}
			}
			return false;
		}
	}
}
