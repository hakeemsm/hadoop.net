using Sharpen;

namespace org.apache.hadoop.fs
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
	/// <see cref="setUp()"/>
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
		private static string TEST_DIR_AAA2 = "test/hadoop2/aaa";

		private static string TEST_DIR_AAA = "test/hadoop/aaa";

		private static string TEST_DIR_AXA = "test/hadoop/axa";

		private static string TEST_DIR_AXX = "test/hadoop/axx";

		private static int numBlocks = 2;

		public org.apache.hadoop.fs.Path localFsRootPath;

		protected internal readonly org.apache.hadoop.fs.FileContextTestHelper fileContextTestHelper;

		protected internal virtual org.apache.hadoop.fs.FileContextTestHelper createFileContextHelper
			()
		{
			return new org.apache.hadoop.fs.FileContextTestHelper();
		}

		protected internal static org.apache.hadoop.fs.FileContext fc;

		private sealed class _PathFilter_79 : org.apache.hadoop.fs.PathFilter
		{
			public _PathFilter_79()
			{
			}

			public bool accept(org.apache.hadoop.fs.Path file)
			{
				return true;
			}
		}

		private static readonly org.apache.hadoop.fs.PathFilter DEFAULT_FILTER = new _PathFilter_79
			();

		private sealed class _PathFilter_87 : org.apache.hadoop.fs.PathFilter
		{
			public _PathFilter_87()
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

		private static readonly org.apache.hadoop.fs.PathFilter TEST_X_FILTER = new _PathFilter_87
			();

		private static readonly byte[] data = org.apache.hadoop.fs.FileContextTestHelper.getFileData
			(numBlocks, org.apache.hadoop.fs.FileContextTestHelper.getDefaultBlockSize());

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			java.io.File testBuildData = new java.io.File(Sharpen.Runtime.getProperty("test.build.data"
				, "build/test/data"), org.apache.commons.lang.RandomStringUtils.randomAlphanumeric
				(10));
			org.apache.hadoop.fs.Path rootPath = new org.apache.hadoop.fs.Path(testBuildData.
				getAbsolutePath(), "root-uri");
			localFsRootPath = rootPath.makeQualified(org.apache.hadoop.fs.LocalFileSystem.NAME
				, null);
			fc.mkdir(getTestRootPath(fc, "test"), org.apache.hadoop.fs.FileContext.DEFAULT_PERM
				, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			bool del = fc.delete(new org.apache.hadoop.fs.Path(fileContextTestHelper.getAbsoluteTestRootPath
				(fc), new org.apache.hadoop.fs.Path("test")), true);
			NUnit.Framework.Assert.IsTrue(del);
			fc.delete(localFsRootPath, true);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.Path getDefaultWorkingDirectory()
		{
			return getTestRootPath(fc, "/user/" + Sharpen.Runtime.getProperty("user.name")).makeQualified
				(fc.getDefaultFileSystem().getUri(), fc.getWorkingDirectory());
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
			org.apache.hadoop.fs.FsStatus fsStatus = fc.getFsStatus(null);
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
			org.apache.hadoop.fs.Path workDir = new org.apache.hadoop.fs.Path(fileContextTestHelper
				.getAbsoluteTestRootPath(fc), new org.apache.hadoop.fs.Path("test"));
			fc.setWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(workDir, fc.getWorkingDirectory());
			fc.setWorkingDirectory(new org.apache.hadoop.fs.Path("."));
			NUnit.Framework.Assert.AreEqual(workDir, fc.getWorkingDirectory());
			fc.setWorkingDirectory(new org.apache.hadoop.fs.Path(".."));
			NUnit.Framework.Assert.AreEqual(workDir.getParent(), fc.getWorkingDirectory());
			// cd using a relative path
			// Go back to our test root
			workDir = new org.apache.hadoop.fs.Path(fileContextTestHelper.getAbsoluteTestRootPath
				(fc), new org.apache.hadoop.fs.Path("test"));
			fc.setWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(workDir, fc.getWorkingDirectory());
			org.apache.hadoop.fs.Path relativeDir = new org.apache.hadoop.fs.Path("existingDir1"
				);
			org.apache.hadoop.fs.Path absoluteDir = new org.apache.hadoop.fs.Path(workDir, "existingDir1"
				);
			fc.mkdir(absoluteDir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			fc.setWorkingDirectory(relativeDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fc.getWorkingDirectory());
			// cd using a absolute path
			absoluteDir = getTestRootPath(fc, "test/existingDir2");
			fc.mkdir(absoluteDir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			fc.setWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fc.getWorkingDirectory());
			// Now open a file relative to the wd we just set above.
			org.apache.hadoop.fs.Path absolutePath = new org.apache.hadoop.fs.Path(absoluteDir
				, "foo");
			fc.create(absolutePath, java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag.CREATE
				)).close();
			fc.open(new org.apache.hadoop.fs.Path("foo")).close();
			// Now mkdir relative to the dir we cd'ed to
			fc.mkdir(new org.apache.hadoop.fs.Path("newDir"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, true);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc
				, new org.apache.hadoop.fs.Path(absoluteDir, "newDir")));
			absoluteDir = getTestRootPath(fc, "nonexistingPath");
			try
			{
				fc.setWorkingDirectory(absoluteDir);
				NUnit.Framework.Assert.Fail("cd to non existing dir should have failed");
			}
			catch (System.Exception)
			{
			}
			// Exception as expected
			// Try a URI
			absoluteDir = new org.apache.hadoop.fs.Path(localFsRootPath, "existingDir");
			fc.mkdir(absoluteDir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			fc.setWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fc.getWorkingDirectory());
			org.apache.hadoop.fs.Path aRegularFile = new org.apache.hadoop.fs.Path("aRegularFile"
				);
			createFile(aRegularFile);
			try
			{
				fc.setWorkingDirectory(aRegularFile);
				NUnit.Framework.Assert.Fail("An IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// okay
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirs()
		{
			org.apache.hadoop.fs.Path testDir = getTestRootPath(fc, "test/hadoop");
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, testDir));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isFile(
				fc, testDir));
			fc.mkdir(testDir, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true
				);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc
				, testDir));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isFile(
				fc, testDir));
			fc.mkdir(testDir, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true
				);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc
				, testDir));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isFile(
				fc, testDir));
			org.apache.hadoop.fs.Path parentDir = testDir.getParent();
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc
				, parentDir));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isFile(
				fc, parentDir));
			org.apache.hadoop.fs.Path grandparentDir = parentDir.getParent();
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc
				, grandparentDir));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isFile(
				fc, grandparentDir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirsFailsForSubdirectoryOfExistingFile()
		{
			org.apache.hadoop.fs.Path testDir = getTestRootPath(fc, "test/hadoop");
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, testDir));
			fc.mkdir(testDir, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true
				);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc
				, testDir));
			createFile(getTestRootPath(fc, "test/hadoop/file"));
			org.apache.hadoop.fs.Path testSubDir = getTestRootPath(fc, "test/hadoop/file/subdir"
				);
			try
			{
				fc.mkdir(testSubDir, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true
					);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, testSubDir));
			org.apache.hadoop.fs.Path testDeepSubDir = getTestRootPath(fc, "test/hadoop/file/deep/sub/dir"
				);
			try
			{
				fc.mkdir(testDeepSubDir, org.apache.hadoop.fs.permission.FsPermission.getDefault(
					), true);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, testDeepSubDir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetFileStatusThrowsExceptionForNonExistentFile()
		{
			try
			{
				fc.getFileStatus(getTestRootPath(fc, "test/hadoop/file"));
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		public virtual void testListStatusThrowsExceptionForNonExistentFile()
		{
			try
			{
				fc.listStatus(getTestRootPath(fc, "test/hadoop/file"));
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testListStatus()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, "test/hadoop/a"), getTestRootPath(fc, "test/hadoop/b"), getTestRootPath(fc, 
				"test/hadoop/c/1") };
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, testDirs[0]));
			foreach (org.apache.hadoop.fs.Path path in testDirs)
			{
				fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
			}
			// test listStatus that returns an array
			org.apache.hadoop.fs.FileStatus[] paths = fc.util().listStatus(getTestRootPath(fc
				, "test"));
			NUnit.Framework.Assert.AreEqual(1, paths.Length);
			NUnit.Framework.Assert.AreEqual(getTestRootPath(fc, "test/hadoop"), paths[0].getPath
				());
			paths = fc.util().listStatus(getTestRootPath(fc, "test/hadoop"));
			NUnit.Framework.Assert.AreEqual(3, paths.Length);
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, "test/hadoop/a"), 
				paths));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, "test/hadoop/b"), 
				paths));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, "test/hadoop/c"), 
				paths));
			paths = fc.util().listStatus(getTestRootPath(fc, "test/hadoop/a"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
			// test listStatus that returns an iterator
			org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus> pathsIterator
				 = fc.listStatus(getTestRootPath(fc, "test"));
			NUnit.Framework.Assert.AreEqual(getTestRootPath(fc, "test/hadoop"), pathsIterator
				.next().getPath());
			NUnit.Framework.Assert.IsFalse(pathsIterator.hasNext());
			pathsIterator = fc.listStatus(getTestRootPath(fc, "test/hadoop"));
			org.apache.hadoop.fs.FileStatus[] subdirs = new org.apache.hadoop.fs.FileStatus[3
				];
			int i = 0;
			while (i < 3 && pathsIterator.hasNext())
			{
				subdirs[i++] = pathsIterator.next();
			}
			NUnit.Framework.Assert.IsFalse(pathsIterator.hasNext());
			NUnit.Framework.Assert.IsTrue(i == 3);
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, "test/hadoop/a"), 
				subdirs));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, "test/hadoop/b"), 
				subdirs));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, "test/hadoop/c"), 
				subdirs));
			pathsIterator = fc.listStatus(getTestRootPath(fc, "test/hadoop/a"));
			NUnit.Framework.Assert.IsFalse(pathsIterator.hasNext());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testListStatusFilterWithNoMatches()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA2), getTestRootPath(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA
				), getTestRootPath(fc, TEST_DIR_AXX) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			// listStatus with filters returns empty correctly
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fc.util().listStatus(getTestRootPath
				(fc, "test"), TEST_X_FILTER);
			NUnit.Framework.Assert.AreEqual(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testListStatusFilterWithSomeMatches()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA), getTestRootPath(fc, TEST_DIR_AXX
				), getTestRootPath(fc, TEST_DIR_AAA2) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			// should return 2 paths ("/test/hadoop/axa" and "/test/hadoop/axx")
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fc.util().listStatus(getTestRootPath
				(fc, "test/hadoop"), TEST_X_FILTER);
			NUnit.Framework.Assert.AreEqual(2, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA), filteredPaths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX), filteredPaths
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusNonExistentFile()
		{
			org.apache.hadoop.fs.FileStatus[] paths = fc.util().globStatus(getTestRootPath(fc
				, "test/hadoopfsdf"));
			NUnit.Framework.Assert.IsNull(paths);
			paths = fc.util().globStatus(getTestRootPath(fc, "test/hadoopfsdf/?"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
			paths = fc.util().globStatus(getTestRootPath(fc, "test/hadoopfsdf/xyz*/?"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusWithNoMatchesInPath()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA), getTestRootPath(fc, TEST_DIR_AXX
				), getTestRootPath(fc, TEST_DIR_AAA2) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			// should return nothing
			org.apache.hadoop.fs.FileStatus[] paths = fc.util().globStatus(getTestRootPath(fc
				, "test/hadoop/?"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusSomeMatchesInDirectories()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA), getTestRootPath(fc, TEST_DIR_AXX
				), getTestRootPath(fc, TEST_DIR_AAA2) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			// Should return two items ("/test/hadoop" and "/test/hadoop2")
			org.apache.hadoop.fs.FileStatus[] paths = fc.util().globStatus(getTestRootPath(fc
				, "test/hadoop*"));
			NUnit.Framework.Assert.AreEqual(2, paths.Length);
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, "test/hadoop"), paths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, "test/hadoop2"), paths
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusWithMultipleWildCardMatches()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA), getTestRootPath(fc, TEST_DIR_AXX
				), getTestRootPath(fc, TEST_DIR_AAA2) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			//Should return all 4 items ("/test/hadoop/aaa", "/test/hadoop/axa"
			//"/test/hadoop/axx", and "/test/hadoop2/axx")
			org.apache.hadoop.fs.FileStatus[] paths = fc.util().globStatus(getTestRootPath(fc
				, "test/hadoop*/*"));
			NUnit.Framework.Assert.AreEqual(4, paths.Length);
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AAA), paths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA), paths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX), paths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AAA2), paths
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusWithMultipleMatchesOfSingleChar()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA), getTestRootPath(fc, TEST_DIR_AXX
				), getTestRootPath(fc, TEST_DIR_AAA2) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			//Should return only 2 items ("/test/hadoop/axa", "/test/hadoop/axx")
			org.apache.hadoop.fs.FileStatus[] paths = fc.util().globStatus(getTestRootPath(fc
				, "test/hadoop/ax?"));
			NUnit.Framework.Assert.AreEqual(2, paths.Length);
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA), paths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX), paths
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithEmptyPathResults()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA), getTestRootPath(fc, TEST_DIR_AXX
				), getTestRootPath(fc, TEST_DIR_AXX) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			//This should return an empty set
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fc.util().globStatus(getTestRootPath
				(fc, "test/hadoop/?"), DEFAULT_FILTER);
			NUnit.Framework.Assert.AreEqual(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithSomePathMatchesAndTrivialFilter()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA), getTestRootPath(fc, TEST_DIR_AXX
				), getTestRootPath(fc, TEST_DIR_AXX) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			//This should return all three (aaa, axa, axx)
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fc.util().globStatus(getTestRootPath
				(fc, "test/hadoop/*"), DEFAULT_FILTER);
			NUnit.Framework.Assert.AreEqual(3, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AAA), filteredPaths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA), filteredPaths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX), filteredPaths
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithMultipleWildCardMatchesAndTrivialFilter
			()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA), getTestRootPath(fc, TEST_DIR_AXX
				), getTestRootPath(fc, TEST_DIR_AXX) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			//This should return all three (aaa, axa, axx)
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fc.util().globStatus(getTestRootPath
				(fc, "test/hadoop/a??"), DEFAULT_FILTER);
			NUnit.Framework.Assert.AreEqual(3, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AAA), filteredPaths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA), filteredPaths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX), filteredPaths
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithMultiplePathMatchesAndNonTrivialFilter
			()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA), getTestRootPath(fc, TEST_DIR_AXX
				), getTestRootPath(fc, TEST_DIR_AXX) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			//This should return two (axa, axx)
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fc.util().globStatus(getTestRootPath
				(fc, "test/hadoop/*"), TEST_X_FILTER);
			NUnit.Framework.Assert.AreEqual(2, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA), filteredPaths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX), filteredPaths
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithNoMatchingPathsAndNonTrivialFilter()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA), getTestRootPath(fc, TEST_DIR_AXX
				), getTestRootPath(fc, TEST_DIR_AXX) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			//This should return an empty set
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fc.util().globStatus(getTestRootPath
				(fc, "test/hadoop/?"), TEST_X_FILTER);
			NUnit.Framework.Assert.AreEqual(0, filteredPaths.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter
			()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { getTestRootPath
				(fc, TEST_DIR_AAA), getTestRootPath(fc, TEST_DIR_AXA), getTestRootPath(fc, TEST_DIR_AXX
				), getTestRootPath(fc, TEST_DIR_AXX) };
			if (org.apache.hadoop.fs.FileContextTestHelper.exists(fc, testDirs[0]) == false)
			{
				foreach (org.apache.hadoop.fs.Path path in testDirs)
				{
					fc.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
				}
			}
			//This should return two (axa, axx)
			org.apache.hadoop.fs.FileStatus[] filteredPaths = fc.util().globStatus(getTestRootPath
				(fc, "test/hadoop/a??"), TEST_X_FILTER);
			NUnit.Framework.Assert.AreEqual(2, filteredPaths.Length);
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA), filteredPaths
				));
			NUnit.Framework.Assert.IsTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX), filteredPaths
				));
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
			writeReadAndDelete(org.apache.hadoop.fs.FileContextTestHelper.getDefaultBlockSize
				() / 2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWriteReadAndDeleteOneBlock()
		{
			writeReadAndDelete(org.apache.hadoop.fs.FileContextTestHelper.getDefaultBlockSize
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWriteReadAndDeleteOneAndAHalfBlocks()
		{
			int blockSize = org.apache.hadoop.fs.FileContextTestHelper.getDefaultBlockSize();
			writeReadAndDelete(blockSize + (blockSize / 2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWriteReadAndDeleteTwoBlocks()
		{
			writeReadAndDelete(org.apache.hadoop.fs.FileContextTestHelper.getDefaultBlockSize
				() * 2);
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeReadAndDelete(int len)
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(fc, "test/hadoop/file");
			fc.mkdir(path.getParent(), org.apache.hadoop.fs.permission.FsPermission.getDefault
				(), true);
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(path, java.util.EnumSet.
				of(org.apache.hadoop.fs.CreateFlag.CREATE), org.apache.hadoop.fs.Options.CreateOpts
				.repFac((short)1), org.apache.hadoop.fs.Options.CreateOpts.blockSize(org.apache.hadoop.fs.FileContextTestHelper.getDefaultBlockSize
				()));
			@out.write(data, 0, len);
			@out.close();
			NUnit.Framework.Assert.IsTrue("Exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, path));
			NUnit.Framework.Assert.AreEqual("Length", len, fc.getFileStatus(path).getLen());
			org.apache.hadoop.fs.FSDataInputStream @in = fc.open(path);
			byte[] buf = new byte[len];
			@in.readFully(0, buf);
			@in.close();
			NUnit.Framework.Assert.AreEqual(len, buf.Length);
			for (int i = 0; i < buf.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual("Position " + i, data[i], buf[i]);
			}
			NUnit.Framework.Assert.IsTrue("Deleted", fc.delete(path, false));
			NUnit.Framework.Assert.IsFalse("No longer exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, path));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testNullCreateFlag()
		{
			org.apache.hadoop.fs.Path p = getTestRootPath(fc, "test/file");
			fc.create(p, null);
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testEmptyCreateFlag()
		{
			org.apache.hadoop.fs.Path p = getTestRootPath(fc, "test/file");
			fc.create(p, java.util.EnumSet.noneOf<org.apache.hadoop.fs.CreateFlag>());
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateFlagCreateExistingFile()
		{
			org.apache.hadoop.fs.Path p = getTestRootPath(fc, "test/testCreateFlagCreateExistingFile"
				);
			createFile(p);
			fc.create(p, java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag.CREATE));
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateFlagOverwriteNonExistingFile()
		{
			org.apache.hadoop.fs.Path p = getTestRootPath(fc, "test/testCreateFlagOverwriteNonExistingFile"
				);
			fc.create(p, java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag.OVERWRITE));
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateFlagOverwriteExistingFile()
		{
			org.apache.hadoop.fs.Path p = getTestRootPath(fc, "test/testCreateFlagOverwriteExistingFile"
				);
			createFile(p);
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(p, java.util.EnumSet.of(
				org.apache.hadoop.fs.CreateFlag.OVERWRITE));
			writeData(fc, p, @out, data, data.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateFlagAppendNonExistingFile()
		{
			org.apache.hadoop.fs.Path p = getTestRootPath(fc, "test/testCreateFlagAppendNonExistingFile"
				);
			fc.create(p, java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag.APPEND));
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateFlagAppendExistingFile()
		{
			org.apache.hadoop.fs.Path p = getTestRootPath(fc, "test/testCreateFlagAppendExistingFile"
				);
			createFile(p);
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(p, java.util.EnumSet.of(
				org.apache.hadoop.fs.CreateFlag.APPEND));
			writeData(fc, p, @out, data, 2 * data.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateFlagCreateAppendNonExistingFile()
		{
			org.apache.hadoop.fs.Path p = getTestRootPath(fc, "test/testCreateFlagCreateAppendNonExistingFile"
				);
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(p, java.util.EnumSet.of(
				org.apache.hadoop.fs.CreateFlag.CREATE, org.apache.hadoop.fs.CreateFlag.APPEND));
			writeData(fc, p, @out, data, data.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateFlagCreateAppendExistingFile()
		{
			org.apache.hadoop.fs.Path p = getTestRootPath(fc, "test/testCreateFlagCreateAppendExistingFile"
				);
			createFile(p);
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(p, java.util.EnumSet.of(
				org.apache.hadoop.fs.CreateFlag.CREATE, org.apache.hadoop.fs.CreateFlag.APPEND));
			writeData(fc, p, @out, data, 2 * data.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateFlagAppendOverwrite()
		{
			org.apache.hadoop.fs.Path p = getTestRootPath(fc, "test/nonExistent");
			fc.create(p, java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag.APPEND, org.apache.hadoop.fs.CreateFlag
				.OVERWRITE));
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateFlagAppendCreateOverwrite()
		{
			org.apache.hadoop.fs.Path p = getTestRootPath(fc, "test/nonExistent");
			fc.create(p, java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag.CREATE, org.apache.hadoop.fs.CreateFlag
				.APPEND, org.apache.hadoop.fs.CreateFlag.OVERWRITE));
			NUnit.Framework.Assert.Fail("Excepted exception not thrown");
		}

		/// <exception cref="System.IO.IOException"/>
		private static void writeData(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 p, org.apache.hadoop.fs.FSDataOutputStream @out, byte[] data, long expectedLen)
		{
			@out.write(data, 0, data.Length);
			@out.close();
			NUnit.Framework.Assert.IsTrue("Exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, p));
			NUnit.Framework.Assert.AreEqual("Length", expectedLen, fc.getFileStatus(p).getLen
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testWriteInNonExistentDirectory()
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(fc, "test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Parent doesn't exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, path.getParent()));
			createFile(path);
			NUnit.Framework.Assert.IsTrue("Exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, path));
			NUnit.Framework.Assert.AreEqual("Length", data.Length, fc.getFileStatus(path).getLen
				());
			NUnit.Framework.Assert.IsTrue("Parent exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, path.getParent()));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteNonExistentFile()
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(fc, "test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Doesn't exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, path));
			NUnit.Framework.Assert.IsFalse("No deletion", fc.delete(path, true));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteRecursively()
		{
			org.apache.hadoop.fs.Path dir = getTestRootPath(fc, "test/hadoop");
			org.apache.hadoop.fs.Path file = getTestRootPath(fc, "test/hadoop/file");
			org.apache.hadoop.fs.Path subdir = getTestRootPath(fc, "test/hadoop/subdir");
			createFile(file);
			fc.mkdir(subdir, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
			NUnit.Framework.Assert.IsTrue("File exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, file));
			NUnit.Framework.Assert.IsTrue("Dir exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, dir));
			NUnit.Framework.Assert.IsTrue("Subdir exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, subdir));
			try
			{
				fc.delete(dir, false);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsTrue("File still exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, file));
			NUnit.Framework.Assert.IsTrue("Dir still exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, dir));
			NUnit.Framework.Assert.IsTrue("Subdir still exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, subdir));
			NUnit.Framework.Assert.IsTrue("Deleted", fc.delete(dir, true));
			NUnit.Framework.Assert.IsFalse("File doesn't exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, file));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, dir));
			NUnit.Framework.Assert.IsFalse("Subdir doesn't exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, subdir));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteEmptyDirectory()
		{
			org.apache.hadoop.fs.Path dir = getTestRootPath(fc, "test/hadoop");
			fc.mkdir(dir, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
			NUnit.Framework.Assert.IsTrue("Dir exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, dir));
			NUnit.Framework.Assert.IsTrue("Deleted", fc.delete(dir, false));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, dir));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameNonExistentPath()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/nonExistent");
			org.apache.hadoop.fs.Path dst = getTestRootPath(fc, "test/new/newpath");
			try
			{
				rename(src, dst, false, false, false, org.apache.hadoop.fs.Options.Rename.NONE);
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (System.IO.IOException e)
			{
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
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fc, "test/nonExistent/newfile");
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
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fc, "test/parentFile/newfile");
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
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fc, "test/new/newfile");
			fc.mkdir(dst.getParent(), org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
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
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/file");
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
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameFileAsExistingFile()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fc, "test/new/existingFile");
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
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fc, "test/new/existingDir");
			fc.mkdir(dst, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
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
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/dir");
			fc.mkdir(src, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
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
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameDirectoryToNonExistentParent()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/dir");
			fc.mkdir(src, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fc, "test/nonExistent/newdir");
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
		public virtual void testRenameDirectoryAsNonExistentDirectory()
		{
			testRenameDirectoryAsNonExistentDirectory(org.apache.hadoop.fs.Options.Rename.NONE
				);
			tearDown();
			testRenameDirectoryAsNonExistentDirectory(org.apache.hadoop.fs.Options.Rename.OVERWRITE
				);
		}

		/// <exception cref="System.Exception"/>
		private void testRenameDirectoryAsNonExistentDirectory(params org.apache.hadoop.fs.Options.Rename
			[] options)
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/dir");
			fc.mkdir(src, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			createFile(getTestRootPath(fc, "test/hadoop/dir/file1"));
			createFile(getTestRootPath(fc, "test/hadoop/dir/subdir/file2"));
			org.apache.hadoop.fs.Path dst = getTestRootPath(fc, "test/new/newdir");
			fc.mkdir(dst.getParent(), org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			rename(src, dst, true, false, true, options);
			NUnit.Framework.Assert.IsFalse("Nested file1 exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, getTestRootPath(fc, "test/hadoop/dir/file1")));
			NUnit.Framework.Assert.IsFalse("Nested file2 exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, getTestRootPath(fc, "test/hadoop/dir/subdir/file2")));
			NUnit.Framework.Assert.IsTrue("Renamed nested file1 exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, getTestRootPath(fc, "test/new/newdir/file1")));
			NUnit.Framework.Assert.IsTrue("Renamed nested exists", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, getTestRootPath(fc, "test/new/newdir/subdir/file2")));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRenameDirectoryAsEmptyDirectory()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/dir");
			fc.mkdir(src, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			createFile(getTestRootPath(fc, "test/hadoop/dir/file1"));
			createFile(getTestRootPath(fc, "test/hadoop/dir/subdir/file2"));
			org.apache.hadoop.fs.Path dst = getTestRootPath(fc, "test/new/newdir");
			fc.mkdir(dst, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
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
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/dir");
			fc.mkdir(src, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			createFile(getTestRootPath(fc, "test/hadoop/dir/file1"));
			createFile(getTestRootPath(fc, "test/hadoop/dir/subdir/file2"));
			org.apache.hadoop.fs.Path dst = getTestRootPath(fc, "test/new/newdir");
			fc.mkdir(dst, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			createFile(getTestRootPath(fc, "test/new/newdir/file1"));
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
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/dir");
			fc.mkdir(src, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			org.apache.hadoop.fs.Path dst = getTestRootPath(fc, "test/new/newfile");
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
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.FSDataInputStream @in = fc.open(src);
			@in.close();
			@in.close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testOutputStreamClosedTwice()
		{
			//HADOOP-4760 according to Closeable#close() closing already-closed 
			//streams should have no effect. 
			org.apache.hadoop.fs.Path src = getTestRootPath(fc, "test/hadoop/file");
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(src, java.util.EnumSet.of
				(org.apache.hadoop.fs.CreateFlag.CREATE), org.apache.hadoop.fs.Options.CreateOpts
				.createParent());
			@out.writeChar('H');
			//write some data
			@out.close();
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testUnsupportedSymlink()
		{
			org.apache.hadoop.fs.Path file = getTestRootPath(fc, "file");
			org.apache.hadoop.fs.Path link = getTestRootPath(fc, "linkToFile");
			if (!fc.getDefaultFileSystem().supportsSymlinks())
			{
				try
				{
					fc.createSymlink(file, link, false);
					NUnit.Framework.Assert.Fail("Created a symlink on a file system that " + "does not support symlinks."
						);
				}
				catch (System.IO.IOException)
				{
				}
				// Expected
				createFile(file);
				try
				{
					fc.getLinkTarget(file);
					NUnit.Framework.Assert.Fail("Got a link target on a file system that " + "does not support symlinks."
						);
				}
				catch (System.IO.IOException)
				{
				}
				// Expected
				NUnit.Framework.Assert.AreEqual(fc.getFileStatus(file), fc.getFileLinkStatus(file
					));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void createFile(org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(path, java.util.EnumSet.
				of(org.apache.hadoop.fs.CreateFlag.CREATE), org.apache.hadoop.fs.Options.CreateOpts
				.createParent());
			@out.write(data, 0, data.Length);
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst, 
			bool renameShouldSucceed, bool srcExists, bool dstExists, params org.apache.hadoop.fs.Options.Rename
			[] options)
		{
			fc.rename(src, dst, options);
			if (!renameShouldSucceed)
			{
				NUnit.Framework.Assert.Fail("rename should have thrown exception");
			}
			NUnit.Framework.Assert.AreEqual("Source exists", srcExists, org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, src));
			NUnit.Framework.Assert.AreEqual("Destination exists", dstExists, org.apache.hadoop.fs.FileContextTestHelper.exists
				(fc, dst));
		}

		/// <exception cref="System.IO.IOException"/>
		private bool containsPath(org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.FileStatus
			[] filteredPaths)
		{
			for (int i = 0; i < filteredPaths.Length; i++)
			{
				if (getTestRootPath(fc, path.ToString()).Equals(filteredPaths[i].getPath()))
				{
					return true;
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testOpen2()
		{
			org.apache.hadoop.fs.Path rootPath = getTestRootPath(fc, "test");
			//final Path rootPath = getAbsoluteTestRootPath(fc);
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(rootPath, "zoo");
			createFile(path);
			long length = fc.getFileStatus(path).getLen();
			org.apache.hadoop.fs.FSDataInputStream fsdis = fc.open(path, 2048);
			try
			{
				byte[] bb = new byte[(int)length];
				fsdis.readFully(bb);
				NUnit.Framework.Assert.assertArrayEquals(data, bb);
			}
			finally
			{
				fsdis.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testSetVerifyChecksum()
		{
			org.apache.hadoop.fs.Path rootPath = getTestRootPath(fc, "test");
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(rootPath, "zoo");
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(path, java.util.EnumSet.
				of(org.apache.hadoop.fs.CreateFlag.CREATE), org.apache.hadoop.fs.Options.CreateOpts
				.createParent());
			try
			{
				// instruct FS to verify checksum through the FileContext:
				fc.setVerifyChecksum(true, path);
				@out.write(data, 0, data.Length);
			}
			finally
			{
				@out.close();
			}
			// NB: underlying FS may be different (this is an abstract test),
			// so we cannot assert .zoo.crc existence.
			// Instead, we check that the file is read correctly:
			org.apache.hadoop.fs.FileStatus fileStatus = fc.getFileStatus(path);
			long len = fileStatus.getLen();
			NUnit.Framework.Assert.IsTrue(len == data.Length);
			byte[] bb = new byte[(int)len];
			org.apache.hadoop.fs.FSDataInputStream fsdis = fc.open(path);
			try
			{
				fsdis.read(bb);
			}
			finally
			{
				fsdis.close();
			}
			NUnit.Framework.Assert.assertArrayEquals(data, bb);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testListCorruptFileBlocks()
		{
			org.apache.hadoop.fs.Path rootPath = getTestRootPath(fc, "test");
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(rootPath, "zoo");
			createFile(path);
			try
			{
				org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.Path> remoteIterator = fc
					.listCorruptFileBlocks(path);
				if (listCorruptedBlocksSupported())
				{
					NUnit.Framework.Assert.IsTrue(remoteIterator != null);
					org.apache.hadoop.fs.Path p;
					while (remoteIterator.hasNext())
					{
						p = remoteIterator.next();
						System.Console.Out.WriteLine("corrupted block: " + p);
					}
					try
					{
						remoteIterator.next();
						NUnit.Framework.Assert.Fail();
					}
					catch (java.util.NoSuchElementException)
					{
					}
				}
				else
				{
					// okay
					NUnit.Framework.Assert.Fail();
				}
			}
			catch (System.NotSupportedException uoe)
			{
				if (listCorruptedBlocksSupported())
				{
					NUnit.Framework.Assert.Fail(uoe.ToString());
				}
			}
		}

		// okay
		protected internal abstract bool listCorruptedBlocksSupported();

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteOnExitUnexisting()
		{
			org.apache.hadoop.fs.Path rootPath = getTestRootPath(fc, "test");
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(rootPath, "zoo");
			bool registered = fc.deleteOnExit(path);
			// because "zoo" does not exist:
			NUnit.Framework.Assert.IsTrue(!registered);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFileContextStatistics()
		{
			org.apache.hadoop.fs.FileContext.clearStatistics();
			org.apache.hadoop.fs.Path rootPath = getTestRootPath(fc, "test");
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(rootPath, "zoo");
			createFile(path);
			byte[] bb = new byte[data.Length];
			org.apache.hadoop.fs.FSDataInputStream fsdis = fc.open(path);
			try
			{
				fsdis.read(bb);
			}
			finally
			{
				fsdis.close();
			}
			NUnit.Framework.Assert.assertArrayEquals(data, bb);
			org.apache.hadoop.fs.FileContext.printStatistics();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetFileContext1()
		{
			/*
			* Test method
			*  org.apache.hadoop.fs.FileContext.getFileContext(AbstractFileSystem)
			*/
			org.apache.hadoop.fs.Path rootPath = getTestRootPath(fc, "test");
			org.apache.hadoop.fs.AbstractFileSystem asf = fc.getDefaultFileSystem();
			// create FileContext using the protected #getFileContext(1) method:
			org.apache.hadoop.fs.FileContext fc2 = org.apache.hadoop.fs.FileContext.getFileContext
				(asf);
			// Now just check that this context can do something reasonable:
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(rootPath, "zoo");
			org.apache.hadoop.fs.FSDataOutputStream @out = fc2.create(path, java.util.EnumSet
				.of(org.apache.hadoop.fs.CreateFlag.CREATE), org.apache.hadoop.fs.Options.CreateOpts
				.createParent());
			@out.close();
			org.apache.hadoop.fs.Path pathResolved = fc2.resolvePath(path);
			NUnit.Framework.Assert.AreEqual(pathResolved.toUri().getPath(), path.toUri().getPath
				());
		}

		private org.apache.hadoop.fs.Path getTestRootPath(org.apache.hadoop.fs.FileContext
			 fc, string pathString)
		{
			return fileContextTestHelper.getTestRootPath(fc, pathString);
		}

		public FileContextMainOperationsBaseTest()
		{
			fileContextTestHelper = createFileContextHelper();
		}
	}
}
