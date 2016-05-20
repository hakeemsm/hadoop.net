using Sharpen;

namespace org.apache.hadoop.fs
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
	/// <see cref="setUp()"/>
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
	public abstract class FSMainOperationsBaseTest : org.apache.hadoop.fs.FileSystemTestHelper
	{
		private static string TEST_DIR_AAA2 = "test/hadoop2/aaa";

		private static string TEST_DIR_AAA = "test/hadoop/aaa";

		private static string TEST_DIR_AXA = "test/hadoop/axa";

		private static string TEST_DIR_AXX = "test/hadoop/axx";

		private static int numBlocks = 2;

		protected internal org.apache.hadoop.fs.FileSystem fSys;

		private sealed class _PathFilter_67 : org.apache.hadoop.fs.PathFilter
		{
			public _PathFilter_67()
			{
			}

			public bool accept(org.apache.hadoop.fs.Path file)
			{
				return true;
			}
		}

		private static readonly org.apache.hadoop.fs.PathFilter DEFAULT_FILTER = new _PathFilter_67
			();

		private sealed class _PathFilter_75 : org.apache.hadoop.fs.PathFilter
		{
			public _PathFilter_75()
			{
			}

			//A test filter with returns any path containing an "x" or "X"
			public bool accept(org.apache.hadoop.fs.Path file)
			{
				if (file.getName().contains("x") || file.getName().contains("X"))
				{
					return true;
				}
				else
				{
					return false;
				}
			}
		}

		private static readonly org.apache.hadoop.fs.PathFilter TEST_X_FILTER = new _PathFilter_75
			();

		protected internal static readonly byte[] data = getFileData(numBlocks, getDefaultBlockSize
			());

		/// <exception cref="System.Exception"/>
		protected internal abstract org.apache.hadoop.fs.FileSystem createFileSystem();

		public FSMainOperationsBaseTest()
		{
		}

		public FSMainOperationsBaseTest(string testRootDir)
			: base(testRootDir)
		{
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			fSys = createFileSystem();
			fSys.mkdirs(getTestRootPath(fSys, "test"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			fSys.delete(new org.apache.hadoop.fs.Path(getAbsoluteTestRootPath(fSys), new org.apache.hadoop.fs.Path
				("test")), true);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.Path getDefaultWorkingDirectory()
		{
			return getTestRootPath(fSys, "/user/" + Sharpen.Runtime.getProperty("user.name"))
				.makeQualified(fSys.getUri(), fSys.getWorkingDirectory());
		}

		protected internal virtual bool renameSupported()
		{
			return true;
		}

		protected internal virtual System.IO.IOException unwrapException(System.IO.IOException
			 e)
		{
			return e;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFsStatus()
		{
			org.apache.hadoop.fs.FsStatus fsStatus = fSys.getStatus(null);
			NUnit.Framework.Assert.IsNotNull(fsStatus);
			//used, free and capacity are non-negative longs
			NUnit.Framework.Assert.IsTrue(fsStatus.getUsed() >= 0);
			NUnit.Framework.Assert.IsTrue(fsStatus.getRemaining() >= 0);
			NUnit.Framework.Assert.IsTrue(fsStatus.getCapacity() >= 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWorkingDirectory()
		{
			// First we cd to our test root
			org.apache.hadoop.fs.Path workDir = new org.apache.hadoop.fs.Path(getAbsoluteTestRootPath
				(fSys), new org.apache.hadoop.fs.Path("test"));
			fSys.setWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(workDir, fSys.getWorkingDirectory());
			fSys.setWorkingDirectory(new org.apache.hadoop.fs.Path("."));
			NUnit.Framework.Assert.AreEqual(workDir, fSys.getWorkingDirectory());
			fSys.setWorkingDirectory(new org.apache.hadoop.fs.Path(".."));
			NUnit.Framework.Assert.AreEqual(workDir.getParent(), fSys.getWorkingDirectory());
			// cd using a relative path
			// Go back to our test root
			workDir = new org.apache.hadoop.fs.Path(getAbsoluteTestRootPath(fSys), new org.apache.hadoop.fs.Path
				("test"));
			fSys.setWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(workDir, fSys.getWorkingDirectory());
			org.apache.hadoop.fs.Path relativeDir = new org.apache.hadoop.fs.Path("existingDir1"
				);
			org.apache.hadoop.fs.Path absoluteDir = new org.apache.hadoop.fs.Path(workDir, "existingDir1"
				);
			fSys.mkdirs(absoluteDir);
			fSys.setWorkingDirectory(relativeDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fSys.getWorkingDirectory());
			// cd using a absolute path
			absoluteDir = getTestRootPath(fSys, "test/existingDir2");
			fSys.mkdirs(absoluteDir);
			fSys.setWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fSys.getWorkingDirectory());
			// Now open a file relative to the wd we just set above.
			org.apache.hadoop.fs.Path absolutePath = new org.apache.hadoop.fs.Path(absoluteDir
				, "foo");
			createFile(fSys, absolutePath);
			fSys.open(new org.apache.hadoop.fs.Path("foo")).close();
			// Now mkdir relative to the dir we cd'ed to
			fSys.mkdirs(new org.apache.hadoop.fs.Path("newDir"));
			NUnit.Framework.Assert.IsTrue(isDir(fSys, new org.apache.hadoop.fs.Path(absoluteDir
				, "newDir")));
		}

		// Try a URI
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testWDAbsolute()
		{
			org.apache.hadoop.fs.Path absoluteDir = new org.apache.hadoop.fs.Path(fSys.getUri
				() + "/test/existingDir");
			fSys.mkdirs(absoluteDir);
			fSys.setWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fSys.getWorkingDirectory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirs()
		{
			org.apache.hadoop.fs.Path testDir = getTestRootPath(fSys, "test/hadoop");
			NUnit.Framework.Assert.IsFalse(exists(fSys, testDir));
			NUnit.Framework.Assert.IsFalse(isFile(fSys, testDir));
			fSys.mkdirs(testDir);
			NUnit.Framework.Assert.IsTrue(exists(fSys, testDir));
			NUnit.Framework.Assert.IsFalse(isFile(fSys, testDir));
			fSys.mkdirs(testDir);
			NUnit.Framework.Assert.IsTrue(exists(fSys, testDir));
			NUnit.Framework.Assert.IsFalse(isFile(fSys, testDir));
			org.apache.hadoop.fs.Path parentDir = testDir.getParent();
			NUnit.Framework.Assert.IsTrue(exists(fSys, parentDir));
			NUnit.Framework.Assert.IsFalse(isFile(fSys, parentDir));
			org.apache.hadoop.fs.Path grandparentDir = parentDir.getParent();
			NUnit.Framework.Assert.IsTrue(exists(fSys, grandparentDir));
			NUnit.Framework.Assert.IsFalse(isFile(fSys, grandparentDir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirsFailsForSubdirectoryOfExistingFile()
		{
			org.apache.hadoop.fs.Path testDir = getTestRootPath(fSys, "test/hadoop");
			NUnit.Framework.Assert.IsFalse(exists(fSys, testDir));
			fSys.mkdirs(testDir);
			NUnit.Framework.Assert.IsTrue(exists(fSys, testDir));
			createFile(getTestRootPath(fSys, "test/hadoop/file"));
			org.apache.hadoop.fs.Path testSubDir = getTestRootPath(fSys, "test/hadoop/file/subdir"
				);
			try
			{
				fSys.mkdirs(testSubDir);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(exists(fSys, testSubDir));
			org.apache.hadoop.fs.Path testDeepSubDir = getTestRootPath(fSys, "test/hadoop/file/deep/sub/dir"
				);
			try
			{
				fSys.mkdirs(testDeepSubDir);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(exists(fSys, testDeepSubDir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetFileStatusThrowsExceptionForNonExistentFile()
		{
			try
			{
				fSys.getFileStatus(getTestRootPath(fSys, "test/hadoop/file"));
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testListStatusThrowsExceptionForNonExistentFile()
		{
			try
			{
				fSys.listStatus(getTestRootPath(fSys, "test/hadoop/file"));
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// expected
		// TODO: update after fixing HADOOP-7352
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testListStatusThrowsExceptionForUnreadableDir()
		{
			org.apache.hadoop.fs.Path testRootDir = getTestRootPath(fSys, "test/hadoop/dir");
			org.apache.hadoop.fs.Path obscuredDir = new org.apache.hadoop.fs.Path(testRootDir
				, "foo");
			org.apache.hadoop.fs.Path subDir = new org.apache.hadoop.fs.Path(obscuredDir, "bar"
				);
			//so foo is non-empty
			fSys.mkdirs(subDir);
			fSys.setPermission(obscuredDir, new org.apache.hadoop.fs.permission.FsPermission(
				(short)0));
			//no access
			try
			{
				fSys.listStatus(obscuredDir);
				NUnit.Framework.Assert.Fail("Should throw IOException");
			}
			catch (System.IO.IOException)
			{
			}
			finally
			{
				// expected
				// make sure the test directory can be deleted
				fSys.setPermission(obscuredDir, new org.apache.hadoop.fs.permission.FsPermission(
					(short)0x1ed));
			}
		}

		//default
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testListStatus()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, "test/hadoop/a"), getTestRootPath(fSys, "test/hadoop/b"), getTestRootPath
				(fSys, "test/hadoop/c/1") };
			NUnit.Framework.Assert.IsFalse(exists(fSys, testDirs[0]));
			foreach (org.apache.hadoop.fs.Path path in testDirs)
			{
				fSys.mkdirs(path);
			}
			// test listStatus that returns an array
			org.apache.hadoop.fs.FileStatus[] paths = fSys.listStatus(getTestRootPath(fSys, "test"
				));
			NUnit.Framework.Assert.AreEqual(1, paths.Length);
			NUnit.Framework.Assert.AreEqual(getTestRootPath(fSys, "test/hadoop"), paths[0].getPath
				());
			paths = fSys.listStatus(getTestRootPath(fSys, "test/hadoop"));
			NUnit.Framework.Assert.AreEqual(3, paths.Length);
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, "test/hadoop/a"
				), paths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, "test/hadoop/b"
				), paths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, "test/hadoop/c"
				), paths));
			paths = fSys.listStatus(getTestRootPath(fSys, "test/hadoop/a"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testListStatusFilterWithNoMatches()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA2), getTestRootPath(fSys, TEST_DIR_AAA), getTestRootPath(fSys
				, TEST_DIR_AXA), getTestRootPath(fSys, TEST_DIR_AXX) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			// listStatus with filters returns empty correctly
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fSys.listStatus(getTestRootPath
				(fSys, "test"), TEST_X_FILTER);
			NUnit.Framework.Assert.AreEqual(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testListStatusFilterWithSomeMatches()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA), getTestRootPath(fSys, TEST_DIR_AXA), getTestRootPath(fSys, 
				TEST_DIR_AXX), getTestRootPath(fSys, TEST_DIR_AAA2) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			// should return 2 paths ("/test/hadoop/axa" and "/test/hadoop/axx")
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fSys.listStatus(getTestRootPath
				(fSys, "test/hadoop"), TEST_X_FILTER);
			NUnit.Framework.Assert.AreEqual(2, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX
				), filteredPaths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusNonExistentFile()
		{
			org.apache.hadoop.fs.FileStatus[] paths = fSys.globStatus(getTestRootPath(fSys, "test/hadoopfsdf"
				));
			NUnit.Framework.Assert.IsNull(paths);
			paths = fSys.globStatus(getTestRootPath(fSys, "test/hadoopfsdf/?"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
			paths = fSys.globStatus(getTestRootPath(fSys, "test/hadoopfsdf/xyz*/?"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusWithNoMatchesInPath()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA), getTestRootPath(fSys, TEST_DIR_AXA), getTestRootPath(fSys, 
				TEST_DIR_AXX), getTestRootPath(fSys, TEST_DIR_AAA2) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			// should return nothing
			org.apache.hadoop.fs.FileStatus[] paths = fSys.globStatus(getTestRootPath(fSys, "test/hadoop/?"
				));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusSomeMatchesInDirectories()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA), getTestRootPath(fSys, TEST_DIR_AXA), getTestRootPath(fSys, 
				TEST_DIR_AXX), getTestRootPath(fSys, TEST_DIR_AAA2) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			// Should return two items ("/test/hadoop" and "/test/hadoop2")
			org.apache.hadoop.fs.FileStatus[] paths = fSys.globStatus(getTestRootPath(fSys, "test/hadoop*"
				));
			NUnit.Framework.Assert.AreEqual(2, paths.Length);
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, "test/hadoop"
				), paths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, "test/hadoop2"
				), paths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusWithMultipleWildCardMatches()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA), getTestRootPath(fSys, TEST_DIR_AXA), getTestRootPath(fSys, 
				TEST_DIR_AXX), getTestRootPath(fSys, TEST_DIR_AAA2) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			//Should return all 4 items ("/test/hadoop/aaa", "/test/hadoop/axa"
			//"/test/hadoop/axx", and "/test/hadoop2/axx")
			org.apache.hadoop.fs.FileStatus[] paths = fSys.globStatus(getTestRootPath(fSys, "test/hadoop*/*"
				));
			NUnit.Framework.Assert.AreEqual(4, paths.Length);
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AAA
				), paths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA
				), paths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX
				), paths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AAA2
				), paths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusWithMultipleMatchesOfSingleChar()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA), getTestRootPath(fSys, TEST_DIR_AXA), getTestRootPath(fSys, 
				TEST_DIR_AXX), getTestRootPath(fSys, TEST_DIR_AAA2) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			//Should return only 2 items ("/test/hadoop/axa", "/test/hadoop/axx")
			org.apache.hadoop.fs.FileStatus[] paths = fSys.globStatus(getTestRootPath(fSys, "test/hadoop/ax?"
				));
			NUnit.Framework.Assert.AreEqual(2, paths.Length);
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA
				), paths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX
				), paths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithEmptyPathResults()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA), getTestRootPath(fSys, TEST_DIR_AXA), getTestRootPath(fSys, 
				TEST_DIR_AXX), getTestRootPath(fSys, TEST_DIR_AXX) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			//This should return an empty set
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fSys.globStatus(getTestRootPath
				(fSys, "test/hadoop/?"), DEFAULT_FILTER);
			NUnit.Framework.Assert.AreEqual(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithSomePathMatchesAndTrivialFilter()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA), getTestRootPath(fSys, TEST_DIR_AXA), getTestRootPath(fSys, 
				TEST_DIR_AXX), getTestRootPath(fSys, TEST_DIR_AXX) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			//This should return all three (aaa, axa, axx)
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fSys.globStatus(getTestRootPath
				(fSys, "test/hadoop/*"), DEFAULT_FILTER);
			NUnit.Framework.Assert.AreEqual(3, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AAA
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX
				), filteredPaths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithMultipleWildCardMatchesAndTrivialFilter
			()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA), getTestRootPath(fSys, TEST_DIR_AXA), getTestRootPath(fSys, 
				TEST_DIR_AXX), getTestRootPath(fSys, TEST_DIR_AXX) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			//This should return all three (aaa, axa, axx)
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fSys.globStatus(getTestRootPath
				(fSys, "test/hadoop/a??"), DEFAULT_FILTER);
			NUnit.Framework.Assert.AreEqual(3, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AAA
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX
				), filteredPaths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithMultiplePathMatchesAndNonTrivialFilter
			()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA), getTestRootPath(fSys, TEST_DIR_AXA), getTestRootPath(fSys, 
				TEST_DIR_AXX), getTestRootPath(fSys, TEST_DIR_AXX) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			//This should return two (axa, axx)
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fSys.globStatus(getTestRootPath
				(fSys, "test/hadoop/*"), TEST_X_FILTER);
			NUnit.Framework.Assert.AreEqual(2, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX
				), filteredPaths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithNoMatchingPathsAndNonTrivialFilter()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA), getTestRootPath(fSys, TEST_DIR_AXA), getTestRootPath(fSys, 
				TEST_DIR_AXX), getTestRootPath(fSys, TEST_DIR_AXX) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			//This should return an empty set
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fSys.globStatus(getTestRootPath
				(fSys, "test/hadoop/?"), TEST_X_FILTER);
			NUnit.Framework.Assert.AreEqual(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter
			()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fSys, TEST_DIR_AAA), getTestRootPath(fSys, TEST_DIR_AXA), getTestRootPath(fSys, 
				TEST_DIR_AXX), getTestRootPath(fSys, TEST_DIR_AXX) };
			if (exists(fSys, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fSys.mkdirs(path);
				}
			}
			//This should return two (axa, axx)
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fSys.globStatus(getTestRootPath
				(fSys, "test/hadoop/a??"), TEST_X_FILTER);
			NUnit.Framework.Assert.AreEqual(2, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA
				), filteredPaths));
			NUnit.Framework.Assert.IsTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX
				), filteredPaths));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWriteReadAndDeleteEmptyFile()
		{
			writeReadAndDelete(0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWriteReadAndDeleteHalfABlock()
		{
			writeReadAndDelete(getDefaultBlockSize() / 2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWriteReadAndDeleteOneBlock()
		{
			writeReadAndDelete(getDefaultBlockSize());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWriteReadAndDeleteOneAndAHalfBlocks()
		{
			int blockSize = getDefaultBlockSize();
			writeReadAndDelete(blockSize + (blockSize / 2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWriteReadAndDeleteTwoBlocks()
		{
			writeReadAndDelete(getDefaultBlockSize() * 2);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void writeReadAndDelete(int len)
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(fSys, "test/hadoop/file");
			fSys.mkdirs(path.getParent());
			org.apache.hadoop.fs.FSDataOutputStream @out = fSys.create(path, false, 4096, (short
				)1, getDefaultBlockSize());
			@out.write(data, 0, len);
			@out.close();
			NUnit.Framework.Assert.IsTrue("Exists", exists(fSys, path));
			NUnit.Framework.Assert.AreEqual("Length", len, fSys.getFileStatus(path).getLen());
			org.apache.hadoop.fs.FSDataInputStream @in = fSys.open(path);
			byte[] buf = new byte[len];
			@in.readFully(0, buf);
			@in.close();
			NUnit.Framework.Assert.AreEqual(len, buf.Length);
			for (int i = 0; i < buf.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual("Position " + i, data[i], buf[i]);
			}
			NUnit.Framework.Assert.IsTrue("Deleted", fSys.delete(path, false));
			NUnit.Framework.Assert.IsFalse("No longer exists", exists(fSys, path));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testOverwrite()
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(fSys, "test/hadoop/file");
			fSys.mkdirs(path.getParent());
			createFile(path);
			NUnit.Framework.Assert.IsTrue("Exists", exists(fSys, path));
			NUnit.Framework.Assert.AreEqual("Length", data.Length, fSys.getFileStatus(path).getLen
				());
			try
			{
				createFile(path);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// Expected
			org.apache.hadoop.fs.FSDataOutputStream @out = fSys.create(path, true, 4096);
			@out.write(data, 0, data.Length);
			@out.close();
			NUnit.Framework.Assert.IsTrue("Exists", exists(fSys, path));
			NUnit.Framework.Assert.AreEqual("Length", data.Length, fSys.getFileStatus(path).getLen
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testWriteInNonExistentDirectory()
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(fSys, "test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Parent doesn't exist", exists(fSys, path.getParent
				()));
			createFile(path);
			NUnit.Framework.Assert.IsTrue("Exists", exists(fSys, path));
			NUnit.Framework.Assert.AreEqual("Length", data.Length, fSys.getFileStatus(path).getLen
				());
			NUnit.Framework.Assert.IsTrue("Parent exists", exists(fSys, path.getParent()));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteNonExistentFile()
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(fSys, "test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Doesn't exist", exists(fSys, path));
			NUnit.Framework.Assert.IsFalse("No deletion", fSys.delete(path, true));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteRecursively()
		{
			org.apache.hadoop.fs.Path dir = getTestRootPath(fSys, "test/hadoop");
			org.apache.hadoop.fs.Path file = getTestRootPath(fSys, "test/hadoop/file");
			org.apache.hadoop.fs.Path subdir = getTestRootPath(fSys, "test/hadoop/subdir");
			createFile(file);
			fSys.mkdirs(subdir);
			NUnit.Framework.Assert.IsTrue("File exists", exists(fSys, file));
			NUnit.Framework.Assert.IsTrue("Dir exists", exists(fSys, dir));
			NUnit.Framework.Assert.IsTrue("Subdir exists", exists(fSys, subdir));
			try
			{
				fSys.delete(dir, false);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsTrue("File still exists", exists(fSys, file));
			NUnit.Framework.Assert.IsTrue("Dir still exists", exists(fSys, dir));
			NUnit.Framework.Assert.IsTrue("Subdir still exists", exists(fSys, subdir));
			NUnit.Framework.Assert.IsTrue("Deleted", fSys.delete(dir, true));
			NUnit.Framework.Assert.IsFalse("File doesn't exist", exists(fSys, file));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", exists(fSys, dir));
			NUnit.Framework.Assert.IsFalse("Subdir doesn't exist", exists(fSys, subdir));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteEmptyDirectory()
		{
			org.apache.hadoop.fs.Path dir = getTestRootPath(fSys, "test/hadoop");
			fSys.mkdirs(dir);
			NUnit.Framework.Assert.IsTrue("Dir exists", exists(fSys, dir));
			NUnit.Framework.Assert.IsTrue("Deleted", fSys.delete(dir, false));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", exists(fSys, dir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameNonExistentPath()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/nonExistent");
			org.apache.hadoop.fs.Path dst = getTestRootPath(fSys, "test/new/newpath");
			try
			{
				rename(src, dst, false, false, false, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (System.IO.IOException e)
			{
				org.mortbay.log.Log.info("XXX", e);
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is java.io.FileNotFoundException
					);
			}
			try
			{
				rename(src, dst, false, false, false, org.apache.hadoop.fs.Options.Rename.OVERWRITE
					);
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is java.io.FileNotFoundException
					);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileToNonExistentDirectory()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fSys, "test/nonExistent/newfile");
			try
			{
				rename(src, dst, false, true, false, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is java.io.FileNotFoundException
					);
			}
			try
			{
				rename(src, dst, false, true, false, org.apache.hadoop.fs.Options.Rename.OVERWRITE
					);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is java.io.FileNotFoundException
					);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileToDestinationWithParentFile()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fSys, "test/parentFile/newfile");
			createFile(dst.getParent());
			try
			{
				rename(src, dst, false, true, false, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException)
			{
			}
			try
			{
				rename(src, dst, false, true, false, org.apache.hadoop.fs.Options.Rename.OVERWRITE
					);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileToExistingParent()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fSys, "test/new/newfile");
			fSys.mkdirs(dst.getParent());
			rename(src, dst, true, false, true, org.apache.hadoop.fs.Options.Rename.OVERWRITE
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileToItself()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/file");
			createFile(src);
			try
			{
				rename(src, src, false, true, false, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Renamed file to itself");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			// Also fails with overwrite
			try
			{
				rename(src, src, false, true, false, org.apache.hadoop.fs.Options.Rename.OVERWRITE
					);
				NUnit.Framework.Assert.Fail("Renamed file to itself");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// worked
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileAsExistingFile()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fSys, "test/new/existingFile");
			createFile(dst);
			// Fails without overwrite option
			try
			{
				rename(src, dst, false, true, false, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			// Succeeds with overwrite option
			rename(src, dst, true, false, true, org.apache.hadoop.fs.Options.Rename.OVERWRITE
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileAsExistingDirectory()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fSys, "test/new/existingDir");
			fSys.mkdirs(dst);
			// Fails without overwrite option
			try
			{
				rename(src, dst, false, false, true, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException)
			{
			}
			// File cannot be renamed as directory
			try
			{
				rename(src, dst, false, false, true, org.apache.hadoop.fs.Options.Rename.OVERWRITE
					);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameDirectoryToItself()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/dir");
			fSys.mkdirs(src);
			try
			{
				rename(src, src, false, true, false, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Renamed directory to itself");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			// Also fails with overwrite
			try
			{
				rename(src, src, false, true, false, org.apache.hadoop.fs.Options.Rename.OVERWRITE
					);
				NUnit.Framework.Assert.Fail("Renamed directory to itself");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// worked      
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameDirectoryToNonExistentParent()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/dir");
			fSys.mkdirs(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fSys, "test/nonExistent/newdir");
			try
			{
				rename(src, dst, false, true, false, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException e)
			{
				System.IO.IOException ioException = unwrapException(e);
				if (!(ioException is java.io.FileNotFoundException))
				{
					throw ioException;
				}
			}
			try
			{
				rename(src, dst, false, true, false, org.apache.hadoop.fs.Options.Rename.OVERWRITE
					);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException e)
			{
				System.IO.IOException ioException = unwrapException(e);
				if (!(ioException is java.io.FileNotFoundException))
				{
					throw ioException;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameDirectoryAsNonExistentDirectory()
		{
			doTestRenameDirectoryAsNonExistentDirectory(org.apache.hadoop.fs.Options.Rename.NONE
				);
			tearDown();
			doTestRenameDirectoryAsNonExistentDirectory(org.apache.hadoop.fs.Options.Rename.OVERWRITE
				);
		}

		/// <exception cref="System.Exception"/>
		private void doTestRenameDirectoryAsNonExistentDirectory(params org.apache.hadoop.fs.Options.Rename
			[] options)
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/dir");
			fSys.mkdirs(src);
			createFile(getTestRootPath(fSys, "test/hadoop/dir/file1"));
			createFile(getTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));
			org.apache.hadoop.fs.Path dst = getTestRootPath(fSys, "test/new/newdir");
			fSys.mkdirs(dst.getParent());
			rename(src, dst, true, false, true, options);
			NUnit.Framework.Assert.IsFalse("Nested file1 exists", exists(fSys, getTestRootPath
				(fSys, "test/hadoop/dir/file1")));
			NUnit.Framework.Assert.IsFalse("Nested file2 exists", exists(fSys, getTestRootPath
				(fSys, "test/hadoop/dir/subdir/file2")));
			NUnit.Framework.Assert.IsTrue("Renamed nested file1 exists", exists(fSys, getTestRootPath
				(fSys, "test/new/newdir/file1")));
			NUnit.Framework.Assert.IsTrue("Renamed nested exists", exists(fSys, getTestRootPath
				(fSys, "test/new/newdir/subdir/file2")));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameDirectoryAsEmptyDirectory()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/dir");
			fSys.mkdirs(src);
			createFile(getTestRootPath(fSys, "test/hadoop/dir/file1"));
			createFile(getTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));
			org.apache.hadoop.fs.Path dst = getTestRootPath(fSys, "test/new/newdir");
			fSys.mkdirs(dst);
			// Fails without overwrite option
			try
			{
				rename(src, dst, false, true, false, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException e)
			{
				// Expected (cannot over-write non-empty destination)
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			// Succeeds with the overwrite option
			rename(src, dst, true, false, true, org.apache.hadoop.fs.Options.Rename.OVERWRITE
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameDirectoryAsNonEmptyDirectory()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/dir");
			fSys.mkdirs(src);
			createFile(getTestRootPath(fSys, "test/hadoop/dir/file1"));
			createFile(getTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));
			org.apache.hadoop.fs.Path dst = getTestRootPath(fSys, "test/new/newdir");
			fSys.mkdirs(dst);
			createFile(getTestRootPath(fSys, "test/new/newdir/file1"));
			// Fails without overwrite option
			try
			{
				rename(src, dst, false, true, false, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException e)
			{
				// Expected (cannot over-write non-empty destination)
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			// Fails even with the overwrite option
			try
			{
				rename(src, dst, false, true, false, org.apache.hadoop.fs.Options.Rename.OVERWRITE
					);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// Expected (cannot over-write non-empty destination)
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameDirectoryAsFile()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/dir");
			fSys.mkdirs(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fSys, "test/new/newfile");
			createFile(dst);
			// Fails without overwrite option
			try
			{
				rename(src, dst, false, true, true, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException)
			{
			}
			// Directory cannot be renamed as existing file
			try
			{
				rename(src, dst, false, true, true, org.apache.hadoop.fs.Options.Rename.OVERWRITE
					);
				NUnit.Framework.Assert.Fail("Expected exception was not thrown");
			}
			catch (System.IO.IOException)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testInputStreamClosedTwice()
		{
			//HADOOP-4760 according to Closeable#close() closing already-closed 
			//streams should have no effect. 
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.FSDataInputStream @in = fSys.open(src);
			@in.close();
			@in.close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testOutputStreamClosedTwice()
		{
			//HADOOP-4760 according to Closeable#close() closing already-closed 
			//streams should have no effect. 
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/file");
			org.apache.hadoop.fs.FSDataOutputStream @out = fSys.create(src);
			@out.writeChar('H');
			//write some data
			@out.close();
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetWrappedInputStream()
		{
			org.apache.hadoop.fs.Path src = getTestRootPath(fSys, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.FSDataInputStream @in = fSys.open(src);
			java.io.InputStream @is = @in.getWrappedStream();
			@in.close();
			NUnit.Framework.Assert.IsNotNull(@is);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyToLocalWithUseRawLocalFileSystemOption()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FileSystem fSys = new org.apache.hadoop.fs.RawLocalFileSystem
				();
			org.apache.hadoop.fs.Path fileToFS = new org.apache.hadoop.fs.Path(getTestRootDir
				(), "fs.txt");
			org.apache.hadoop.fs.Path fileToLFS = new org.apache.hadoop.fs.Path(getTestRootDir
				(), "test.txt");
			org.apache.hadoop.fs.Path crcFileAtLFS = new org.apache.hadoop.fs.Path(getTestRootDir
				(), ".test.txt.crc");
			fSys.initialize(new java.net.URI("file:///"), conf);
			writeFile(fSys, fileToFS);
			if (fSys.exists(crcFileAtLFS))
			{
				NUnit.Framework.Assert.IsTrue("CRC files not deleted", fSys.delete(crcFileAtLFS, 
					true));
			}
			fSys.copyToLocalFile(false, fileToFS, fileToLFS, true);
			NUnit.Framework.Assert.IsFalse("CRC files are created", fSys.exists(crcFileAtLFS)
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeFile(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 name)
		{
			org.apache.hadoop.fs.FSDataOutputStream stm = fs.create(name);
			try
			{
				stm.writeBytes("42\n");
			}
			finally
			{
				stm.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void createFile(org.apache.hadoop.fs.Path path)
		{
			createFile(fSys, path);
		}

		/// <exception cref="System.IO.IOException"/>
		private void rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst, 
			bool renameShouldSucceed, bool srcExists, bool dstExists, params org.apache.hadoop.fs.Options.Rename
			[] options)
		{
			fSys.rename(src, dst, options);
			if (!renameShouldSucceed)
			{
				NUnit.Framework.Assert.Fail("rename should have thrown exception");
			}
			NUnit.Framework.Assert.AreEqual("Source exists", srcExists, exists(fSys, src));
			NUnit.Framework.Assert.AreEqual("Destination exists", dstExists, exists(fSys, dst
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private bool containsTestRootPath(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.FileStatus
			[] filteredPaths)
		{
			org.apache.hadoop.fs.Path testRootPath = getTestRootPath(fSys, path.ToString());
			for (int i = 0; i < filteredPaths.Length; i++)
			{
				if (testRootPath.Equals(filteredPaths[i].getPath()))
				{
					return true;
				}
			}
			return false;
		}
	}
}
