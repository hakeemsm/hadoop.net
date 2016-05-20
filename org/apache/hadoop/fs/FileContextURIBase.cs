using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// <p>
	/// A collection of tests for the
	/// <see cref="FileContext"/>
	/// to test path names passed
	/// as URIs. This test should be used for testing an instance of FileContext that
	/// has been initialized to a specific default FileSystem such a LocalFileSystem,
	/// HDFS,S3, etc, and where path names are passed that are URIs in a different
	/// FileSystem.
	/// </p>
	/// <p>
	/// To test a given
	/// <see cref="FileSystem"/>
	/// implementation create a subclass of this
	/// test and override
	/// <see cref="setUp()"/>
	/// to initialize the <code>fc1</code> and
	/// <code>fc2</code>
	/// The tests will do operations on fc1 that use a URI in fc2
	/// <see cref="FileContext"/>
	/// instance variable.
	/// </p>
	/// </summary>
	public abstract class FileContextURIBase
	{
		private static readonly string basePath = Sharpen.Runtime.getProperty("test.build.data"
			, "build/test/data") + "/testContextURI";

		private static readonly org.apache.hadoop.fs.Path BASE = new org.apache.hadoop.fs.Path
			(basePath);

		private static readonly java.util.regex.Pattern WIN_INVALID_FILE_NAME_PATTERN = java.util.regex.Pattern
			.compile("^(.*?[<>\\:\"\\|\\?\\*].*?)|(.*?[ \\.])$");

		protected internal org.apache.hadoop.fs.FileContext fc1;

		protected internal org.apache.hadoop.fs.FileContext fc2;

		// Matches anything containing <, >, :, ", |, ?, *, or anything that ends with
		// space or dot.
		//Helper method to make path qualified
		protected internal virtual org.apache.hadoop.fs.Path qualifiedPath(string path, org.apache.hadoop.fs.FileContext
			 fc)
		{
			return fc.makeQualified(new org.apache.hadoop.fs.Path(BASE, path));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			// Clean up after test completion
			// No need to clean fc1 as fc1 and fc2 points same location
			fc2.delete(BASE, true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateFile()
		{
			string[] fileNames = new string[] { "testFile", "test File", "test*File", "test#File"
				, "test1234", "1234Test", "test)File", "test_File", "()&^%$#@!~_+}{><?", "  ", "^ "
				 };
			foreach (string f in fileNames)
			{
				if (!isTestableFileNameOnPlatform(f))
				{
					continue;
				}
				// Create a file on fc2's file system using fc1
				org.apache.hadoop.fs.Path testPath = qualifiedPath(f, fc2);
				// Ensure file does not exist
				NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
					fc2, testPath));
				// Now create file
				org.apache.hadoop.fs.FileContextTestHelper.createFile(fc1, testPath);
				// Ensure fc2 has the created file
				NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
					, testPath));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateFileWithNullName()
		{
			string fileName = null;
			try
			{
				org.apache.hadoop.fs.Path testPath = qualifiedPath(fileName, fc2);
				// Ensure file does not exist
				NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
					fc2, testPath));
				// Create a file on fc2's file system using fc1
				org.apache.hadoop.fs.FileContextTestHelper.createFile(fc1, testPath);
				NUnit.Framework.Assert.Fail("Create file with null name should throw IllegalArgumentException."
					);
			}
			catch (System.ArgumentException)
			{
			}
		}

		// expected
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateExistingFile()
		{
			string fileName = "testFile";
			org.apache.hadoop.fs.Path testPath = qualifiedPath(fileName, fc2);
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testPath));
			// Create a file on fc2's file system using fc1
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc1, testPath);
			// Create same file with fc1
			try
			{
				org.apache.hadoop.fs.FileContextTestHelper.createFile(fc2, testPath);
				NUnit.Framework.Assert.Fail("Create existing file should throw an IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			// Ensure fc2 has the created file
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
				, testPath));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateFileInNonExistingDirectory()
		{
			string fileName = "testDir/testFile";
			org.apache.hadoop.fs.Path testPath = qualifiedPath(fileName, fc2);
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testPath));
			// Create a file on fc2's file system using fc1
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc1, testPath);
			// Ensure using fc2 that file is created
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc2
				, testPath.getParent()));
			NUnit.Framework.Assert.AreEqual("testDir", testPath.getParent().getName());
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
				, testPath));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateDirectory()
		{
			org.apache.hadoop.fs.Path path = qualifiedPath("test/hadoop", fc2);
			org.apache.hadoop.fs.Path falsePath = qualifiedPath("path/doesnot.exist", fc2);
			org.apache.hadoop.fs.Path subDirPath = qualifiedPath("dir0", fc2);
			// Ensure that testPath does not exist in fc1
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc1, path));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isFile(
				fc1, path));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc1
				, path));
			// Create a directory on fc2's file system using fc1
			fc1.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
			// Ensure fc2 has directory
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc2
				, path));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
				, path));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isFile(
				fc2, path));
			// Test to create same dir twice, (HDFS mkdir is similar to mkdir -p )
			fc1.mkdir(subDirPath, org.apache.hadoop.fs.permission.FsPermission.getDefault(), 
				true);
			// This should not throw exception
			fc1.mkdir(subDirPath, org.apache.hadoop.fs.permission.FsPermission.getDefault(), 
				true);
			// Create Sub Dirs
			fc1.mkdir(subDirPath, org.apache.hadoop.fs.permission.FsPermission.getDefault(), 
				true);
			// Check parent dir
			org.apache.hadoop.fs.Path parentDir = path.getParent();
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
				, parentDir));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isFile(
				fc2, parentDir));
			// Check parent parent dir
			org.apache.hadoop.fs.Path grandparentDir = parentDir.getParent();
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
				, grandparentDir));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isFile(
				fc2, grandparentDir));
			// Negative test cases
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, falsePath));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc2
				, falsePath));
			// TestCase - Create multiple directories
			string[] dirNames = new string[] { "createTest/testDir", "createTest/test Dir", "deleteTest/test*Dir"
				, "deleteTest/test#Dir", "deleteTest/test1234", "deleteTest/test_DIr", "deleteTest/1234Test"
				, "deleteTest/test)Dir", "deleteTest/()&^%$#@!~_+}{><?", "  ", "^ " };
			foreach (string f in dirNames)
			{
				if (!isTestableFileNameOnPlatform(f))
				{
					continue;
				}
				// Create a file on fc2's file system using fc1
				org.apache.hadoop.fs.Path testPath = qualifiedPath(f, fc2);
				// Ensure file does not exist
				NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
					fc2, testPath));
				// Now create directory
				fc1.mkdir(testPath, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true
					);
				// Ensure fc2 has the created directory
				NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
					, testPath));
				NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc2
					, testPath));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirsFailsForSubdirectoryOfExistingFile()
		{
			org.apache.hadoop.fs.Path testDir = qualifiedPath("test/hadoop", fc2);
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testDir));
			fc2.mkdir(testDir, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true
				);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
				, testDir));
			// Create file on fc1 using fc2 context
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc1, qualifiedPath("test/hadoop/file"
				, fc2));
			org.apache.hadoop.fs.Path testSubDir = qualifiedPath("test/hadoop/file/subdir", fc2
				);
			try
			{
				fc1.mkdir(testSubDir, org.apache.hadoop.fs.permission.FsPermission.getDefault(), 
					true);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc1, testSubDir));
			org.apache.hadoop.fs.Path testDeepSubDir = qualifiedPath("test/hadoop/file/deep/sub/dir"
				, fc1);
			try
			{
				fc2.mkdir(testDeepSubDir, org.apache.hadoop.fs.permission.FsPermission.getDefault
					(), true);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc1, testDeepSubDir));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testIsDirectory()
		{
			string dirName = "dirTest";
			string invalidDir = "nonExistantDir";
			string rootDir = "/";
			org.apache.hadoop.fs.Path existingPath = qualifiedPath(dirName, fc2);
			org.apache.hadoop.fs.Path nonExistingPath = qualifiedPath(invalidDir, fc2);
			org.apache.hadoop.fs.Path pathToRootDir = qualifiedPath(rootDir, fc2);
			// Create a directory on fc2's file system using fc1
			fc1.mkdir(existingPath, org.apache.hadoop.fs.permission.FsPermission.getDefault()
				, true);
			// Ensure fc2 has directory
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc2
				, existingPath));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc2
				, pathToRootDir));
			// Negative test case
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc2
				, nonExistingPath));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteFile()
		{
			org.apache.hadoop.fs.Path testPath = qualifiedPath("testFile", fc2);
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testPath));
			// First create a file on file system using fc1
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc1, testPath);
			// Ensure file exist
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
				, testPath));
			// Delete file using fc2
			fc2.delete(testPath, false);
			// Ensure fc2 does not have deleted file
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testPath));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteNonExistingFile()
		{
			string testFileName = "testFile";
			org.apache.hadoop.fs.Path testPath = qualifiedPath(testFileName, fc2);
			// TestCase1 : Test delete on file never existed
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testPath));
			// Delete on non existing file should return false
			NUnit.Framework.Assert.IsFalse(fc2.delete(testPath, false));
			// TestCase2 : Create , Delete , Delete file
			// Create a file on fc2's file system using fc1
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc1, testPath);
			// Ensure file exist
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
				, testPath));
			// Delete test file, deleting existing file should return true
			NUnit.Framework.Assert.IsTrue(fc2.delete(testPath, false));
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testPath));
			// Delete on non existing file should return false
			NUnit.Framework.Assert.IsFalse(fc2.delete(testPath, false));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteNonExistingFileInDir()
		{
			string testFileInDir = "testDir/testDir/TestFile";
			org.apache.hadoop.fs.Path testPath = qualifiedPath(testFileInDir, fc2);
			// TestCase1 : Test delete on file never existed
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testPath));
			// Delete on non existing file should return false
			NUnit.Framework.Assert.IsFalse(fc2.delete(testPath, false));
			// TestCase2 : Create , Delete , Delete file
			// Create a file on fc2's file system using fc1
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc1, testPath);
			// Ensure file exist
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
				, testPath));
			// Delete test file, deleting existing file should return true
			NUnit.Framework.Assert.IsTrue(fc2.delete(testPath, false));
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testPath));
			// Delete on non existing file should return false
			NUnit.Framework.Assert.IsFalse(fc2.delete(testPath, false));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteDirectory()
		{
			string dirName = "dirTest";
			org.apache.hadoop.fs.Path testDirPath = qualifiedPath(dirName, fc2);
			// Ensure directory does not exist
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testDirPath));
			// Create a directory on fc2's file system using fc1
			fc1.mkdir(testDirPath, org.apache.hadoop.fs.permission.FsPermission.getDefault(), 
				true);
			// Ensure dir is created
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
				, testDirPath));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc2
				, testDirPath));
			fc2.delete(testDirPath, true);
			// Ensure that directory is deleted
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc2
				, testDirPath));
			// TestCase - Create and delete multiple directories
			string[] dirNames = new string[] { "deleteTest/testDir", "deleteTest/test Dir", "deleteTest/test*Dir"
				, "deleteTest/test#Dir", "deleteTest/test1234", "deleteTest/1234Test", "deleteTest/test)Dir"
				, "deleteTest/test_DIr", "deleteTest/()&^%$#@!~_+}{><?", "  ", "^ " };
			foreach (string f in dirNames)
			{
				if (!isTestableFileNameOnPlatform(f))
				{
					continue;
				}
				// Create a file on fc2's file system using fc1
				org.apache.hadoop.fs.Path testPath = qualifiedPath(f, fc2);
				// Ensure file does not exist
				NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
					fc2, testPath));
				// Now create directory
				fc1.mkdir(testPath, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true
					);
				// Ensure fc2 has the created directory
				NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
					, testPath));
				NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc2
					, testPath));
				// Delete dir
				NUnit.Framework.Assert.IsTrue(fc2.delete(testPath, true));
				// verify if directory is deleted
				NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
					fc2, testPath));
				NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc2
					, testPath));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteNonExistingDirectory()
		{
			string testDirName = "testFile";
			org.apache.hadoop.fs.Path testPath = qualifiedPath(testDirName, fc2);
			// TestCase1 : Test delete on directory never existed
			// Ensure directory does not exist
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testPath));
			// Delete on non existing directory should return false
			NUnit.Framework.Assert.IsFalse(fc2.delete(testPath, false));
			// TestCase2 : Create dir, Delete dir, Delete dir
			// Create a file on fc2's file system using fc1
			fc1.mkdir(testPath, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true
				);
			// Ensure dir exist
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fc2
				, testPath));
			// Delete test file, deleting existing file should return true
			NUnit.Framework.Assert.IsTrue(fc2.delete(testPath, false));
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc2, testPath));
			// Delete on non existing file should return false
			NUnit.Framework.Assert.IsFalse(fc2.delete(testPath, false));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testModificationTime()
		{
			string testFile = "file1";
			long fc2ModificationTime;
			long fc1ModificationTime;
			org.apache.hadoop.fs.Path testPath = qualifiedPath(testFile, fc2);
			// Create a file on fc2's file system using fc1
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc1, testPath);
			// Get modification time using fc2 and fc1
			fc1ModificationTime = fc1.getFileStatus(testPath).getModificationTime();
			fc2ModificationTime = fc2.getFileStatus(testPath).getModificationTime();
			// Ensure fc1 and fc2 reports same modification time
			NUnit.Framework.Assert.AreEqual(fc1ModificationTime, fc2ModificationTime);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFileStatus()
		{
			string fileName = "file1";
			org.apache.hadoop.fs.Path path2 = fc2.makeQualified(new org.apache.hadoop.fs.Path
				(BASE, fileName));
			// Create a file on fc2's file system using fc1
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc1, path2);
			org.apache.hadoop.fs.FsStatus fc2Status = fc2.getFsStatus(path2);
			// FsStatus , used, free and capacity are non-negative longs
			NUnit.Framework.Assert.IsNotNull(fc2Status);
			NUnit.Framework.Assert.IsTrue(fc2Status.getCapacity() > 0);
			NUnit.Framework.Assert.IsTrue(fc2Status.getRemaining() > 0);
			NUnit.Framework.Assert.IsTrue(fc2Status.getUsed() > 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetFileStatusThrowsExceptionForNonExistentFile()
		{
			string testFile = "test/hadoop/fileDoesNotExist";
			org.apache.hadoop.fs.Path testPath = qualifiedPath(testFile, fc2);
			try
			{
				fc1.getFileStatus(testPath);
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
			string testFile = "test/hadoop/file";
			org.apache.hadoop.fs.Path testPath = qualifiedPath(testFile, fc2);
			try
			{
				fc1.listStatus(testPath);
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
			string hPrefix = "test/hadoop";
			string[] dirs = new string[] { hPrefix + "/a", hPrefix + "/b", hPrefix + "/c", hPrefix
				 + "/1", hPrefix + "/#@#@", hPrefix + "/&*#$#$@234" };
			System.Collections.Generic.List<org.apache.hadoop.fs.Path> testDirs = new System.Collections.Generic.List
				<org.apache.hadoop.fs.Path>();
			foreach (string d in dirs)
			{
				if (!isTestableFileNameOnPlatform(d))
				{
					continue;
				}
				testDirs.add(qualifiedPath(d, fc2));
			}
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc1, testDirs[0]));
			foreach (org.apache.hadoop.fs.Path path in testDirs)
			{
				fc1.mkdir(path, org.apache.hadoop.fs.permission.FsPermission.getDefault(), true);
			}
			// test listStatus that returns an array of FileStatus
			org.apache.hadoop.fs.FileStatus[] paths = fc1.util().listStatus(qualifiedPath("test"
				, fc1));
			NUnit.Framework.Assert.AreEqual(1, paths.Length);
			NUnit.Framework.Assert.AreEqual(qualifiedPath(hPrefix, fc1), paths[0].getPath());
			paths = fc1.util().listStatus(qualifiedPath(hPrefix, fc1));
			NUnit.Framework.Assert.AreEqual(testDirs.Count, paths.Length);
			for (int i = 0; i < testDirs.Count; i++)
			{
				bool found = false;
				for (int j = 0; j < paths.Length; j++)
				{
					if (qualifiedPath(testDirs[i].ToString(), fc1).Equals(paths[j].getPath()))
					{
						found = true;
					}
				}
				NUnit.Framework.Assert.IsTrue(testDirs[i] + " not found", found);
			}
			paths = fc1.util().listStatus(qualifiedPath(dirs[0], fc1));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
			// test listStatus that returns an iterator of FileStatus
			org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus> pathsItor = 
				fc1.listStatus(qualifiedPath("test", fc1));
			NUnit.Framework.Assert.AreEqual(qualifiedPath(hPrefix, fc1), pathsItor.next().getPath
				());
			NUnit.Framework.Assert.IsFalse(pathsItor.hasNext());
			pathsItor = fc1.listStatus(qualifiedPath(hPrefix, fc1));
			int dirLen = 0;
			for (; pathsItor.hasNext(); dirLen++)
			{
				bool found = false;
				org.apache.hadoop.fs.FileStatus stat = pathsItor.next();
				for (int j = 0; j < dirs.Length; j++)
				{
					if (qualifiedPath(dirs[j], fc1).Equals(stat.getPath()))
					{
						found = true;
						break;
					}
				}
				NUnit.Framework.Assert.IsTrue(stat.getPath() + " not found", found);
			}
			NUnit.Framework.Assert.AreEqual(testDirs.Count, dirLen);
			pathsItor = fc1.listStatus(qualifiedPath(dirs[0], fc1));
			NUnit.Framework.Assert.IsFalse(pathsItor.hasNext());
		}

		/// <summary>
		/// Returns true if the argument is a file name that is testable on the platform
		/// currently running the test.
		/// </summary>
		/// <remarks>
		/// Returns true if the argument is a file name that is testable on the platform
		/// currently running the test.  This is intended for use by tests so that they
		/// can skip checking file names that aren't supported by the underlying
		/// platform.  The current implementation specifically checks for patterns that
		/// are not valid file names on Windows when the tests are running on Windows.
		/// </remarks>
		/// <param name="fileName">String file name to check</param>
		/// <returns>boolean true if the argument is valid as a file name</returns>
		private static bool isTestableFileNameOnPlatform(string fileName)
		{
			bool valid = true;
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				// Disallow reserved characters: <, >, :, ", |, ?, *.
				// Disallow trailing space or period.
				// See http://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
				valid = !WIN_INVALID_FILE_NAME_PATTERN.matcher(fileName).matches();
			}
			return valid;
		}
	}
}
