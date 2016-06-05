using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
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
	/// <see cref="SetUp()"/>
	/// to initialize the <code>fc1</code> and
	/// <code>fc2</code>
	/// The tests will do operations on fc1 that use a URI in fc2
	/// <see cref="FileContext"/>
	/// instance variable.
	/// </p>
	/// </summary>
	public abstract class FileContextURIBase
	{
		private static readonly string basePath = Runtime.GetProperty("test.build.data", 
			"build/test/data") + "/testContextURI";

		private static readonly Path Base = new Path(basePath);

		private static readonly Sharpen.Pattern WinInvalidFileNamePattern = Sharpen.Pattern
			.Compile("^(.*?[<>\\:\"\\|\\?\\*].*?)|(.*?[ \\.])$");

		protected internal FileContext fc1;

		protected internal FileContext fc2;

		// Matches anything containing <, >, :, ", |, ?, *, or anything that ends with
		// space or dot.
		//Helper method to make path qualified
		protected internal virtual Path QualifiedPath(string path, FileContext fc)
		{
			return fc.MakeQualified(new Path(Base, path));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			// Clean up after test completion
			// No need to clean fc1 as fc1 and fc2 points same location
			fc2.Delete(Base, true);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateFile()
		{
			string[] fileNames = new string[] { "testFile", "test File", "test*File", "test#File"
				, "test1234", "1234Test", "test)File", "test_File", "()&^%$#@!~_+}{><?", "  ", "^ "
				 };
			foreach (string f in fileNames)
			{
				if (!IsTestableFileNameOnPlatform(f))
				{
					continue;
				}
				// Create a file on fc2's file system using fc1
				Path testPath = QualifiedPath(f, fc2);
				// Ensure file does not exist
				NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
				// Now create file
				FileContextTestHelper.CreateFile(fc1, testPath);
				// Ensure fc2 has the created file
				Assert.True(FileContextTestHelper.Exists(fc2, testPath));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateFileWithNullName()
		{
			string fileName = null;
			try
			{
				Path testPath = QualifiedPath(fileName, fc2);
				// Ensure file does not exist
				NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
				// Create a file on fc2's file system using fc1
				FileContextTestHelper.CreateFile(fc1, testPath);
				NUnit.Framework.Assert.Fail("Create file with null name should throw IllegalArgumentException."
					);
			}
			catch (ArgumentException)
			{
			}
		}

		// expected
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateExistingFile()
		{
			string fileName = "testFile";
			Path testPath = QualifiedPath(fileName, fc2);
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
			// Create a file on fc2's file system using fc1
			FileContextTestHelper.CreateFile(fc1, testPath);
			// Create same file with fc1
			try
			{
				FileContextTestHelper.CreateFile(fc2, testPath);
				NUnit.Framework.Assert.Fail("Create existing file should throw an IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			// Ensure fc2 has the created file
			Assert.True(FileContextTestHelper.Exists(fc2, testPath));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateFileInNonExistingDirectory()
		{
			string fileName = "testDir/testFile";
			Path testPath = QualifiedPath(fileName, fc2);
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
			// Create a file on fc2's file system using fc1
			FileContextTestHelper.CreateFile(fc1, testPath);
			// Ensure using fc2 that file is created
			Assert.True(FileContextTestHelper.IsDir(fc2, testPath.GetParent
				()));
			Assert.Equal("testDir", testPath.GetParent().GetName());
			Assert.True(FileContextTestHelper.Exists(fc2, testPath));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateDirectory()
		{
			Path path = QualifiedPath("test/hadoop", fc2);
			Path falsePath = QualifiedPath("path/doesnot.exist", fc2);
			Path subDirPath = QualifiedPath("dir0", fc2);
			// Ensure that testPath does not exist in fc1
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc1, path));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsFile(fc1, path));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsDir(fc1, path));
			// Create a directory on fc2's file system using fc1
			fc1.Mkdir(path, FsPermission.GetDefault(), true);
			// Ensure fc2 has directory
			Assert.True(FileContextTestHelper.IsDir(fc2, path));
			Assert.True(FileContextTestHelper.Exists(fc2, path));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsFile(fc2, path));
			// Test to create same dir twice, (HDFS mkdir is similar to mkdir -p )
			fc1.Mkdir(subDirPath, FsPermission.GetDefault(), true);
			// This should not throw exception
			fc1.Mkdir(subDirPath, FsPermission.GetDefault(), true);
			// Create Sub Dirs
			fc1.Mkdir(subDirPath, FsPermission.GetDefault(), true);
			// Check parent dir
			Path parentDir = path.GetParent();
			Assert.True(FileContextTestHelper.Exists(fc2, parentDir));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsFile(fc2, parentDir));
			// Check parent parent dir
			Path grandparentDir = parentDir.GetParent();
			Assert.True(FileContextTestHelper.Exists(fc2, grandparentDir));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsFile(fc2, grandparentDir));
			// Negative test cases
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, falsePath));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsDir(fc2, falsePath));
			// TestCase - Create multiple directories
			string[] dirNames = new string[] { "createTest/testDir", "createTest/test Dir", "deleteTest/test*Dir"
				, "deleteTest/test#Dir", "deleteTest/test1234", "deleteTest/test_DIr", "deleteTest/1234Test"
				, "deleteTest/test)Dir", "deleteTest/()&^%$#@!~_+}{><?", "  ", "^ " };
			foreach (string f in dirNames)
			{
				if (!IsTestableFileNameOnPlatform(f))
				{
					continue;
				}
				// Create a file on fc2's file system using fc1
				Path testPath = QualifiedPath(f, fc2);
				// Ensure file does not exist
				NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
				// Now create directory
				fc1.Mkdir(testPath, FsPermission.GetDefault(), true);
				// Ensure fc2 has the created directory
				Assert.True(FileContextTestHelper.Exists(fc2, testPath));
				Assert.True(FileContextTestHelper.IsDir(fc2, testPath));
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMkdirsFailsForSubdirectoryOfExistingFile()
		{
			Path testDir = QualifiedPath("test/hadoop", fc2);
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testDir));
			fc2.Mkdir(testDir, FsPermission.GetDefault(), true);
			Assert.True(FileContextTestHelper.Exists(fc2, testDir));
			// Create file on fc1 using fc2 context
			FileContextTestHelper.CreateFile(fc1, QualifiedPath("test/hadoop/file", fc2));
			Path testSubDir = QualifiedPath("test/hadoop/file/subdir", fc2);
			try
			{
				fc1.Mkdir(testSubDir, FsPermission.GetDefault(), true);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc1, testSubDir));
			Path testDeepSubDir = QualifiedPath("test/hadoop/file/deep/sub/dir", fc1);
			try
			{
				fc2.Mkdir(testDeepSubDir, FsPermission.GetDefault(), true);
				NUnit.Framework.Assert.Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc1, testDeepSubDir));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestIsDirectory()
		{
			string dirName = "dirTest";
			string invalidDir = "nonExistantDir";
			string rootDir = "/";
			Path existingPath = QualifiedPath(dirName, fc2);
			Path nonExistingPath = QualifiedPath(invalidDir, fc2);
			Path pathToRootDir = QualifiedPath(rootDir, fc2);
			// Create a directory on fc2's file system using fc1
			fc1.Mkdir(existingPath, FsPermission.GetDefault(), true);
			// Ensure fc2 has directory
			Assert.True(FileContextTestHelper.IsDir(fc2, existingPath));
			Assert.True(FileContextTestHelper.IsDir(fc2, pathToRootDir));
			// Negative test case
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsDir(fc2, nonExistingPath));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteFile()
		{
			Path testPath = QualifiedPath("testFile", fc2);
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
			// First create a file on file system using fc1
			FileContextTestHelper.CreateFile(fc1, testPath);
			// Ensure file exist
			Assert.True(FileContextTestHelper.Exists(fc2, testPath));
			// Delete file using fc2
			fc2.Delete(testPath, false);
			// Ensure fc2 does not have deleted file
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteNonExistingFile()
		{
			string testFileName = "testFile";
			Path testPath = QualifiedPath(testFileName, fc2);
			// TestCase1 : Test delete on file never existed
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
			// Delete on non existing file should return false
			NUnit.Framework.Assert.IsFalse(fc2.Delete(testPath, false));
			// TestCase2 : Create , Delete , Delete file
			// Create a file on fc2's file system using fc1
			FileContextTestHelper.CreateFile(fc1, testPath);
			// Ensure file exist
			Assert.True(FileContextTestHelper.Exists(fc2, testPath));
			// Delete test file, deleting existing file should return true
			Assert.True(fc2.Delete(testPath, false));
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
			// Delete on non existing file should return false
			NUnit.Framework.Assert.IsFalse(fc2.Delete(testPath, false));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteNonExistingFileInDir()
		{
			string testFileInDir = "testDir/testDir/TestFile";
			Path testPath = QualifiedPath(testFileInDir, fc2);
			// TestCase1 : Test delete on file never existed
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
			// Delete on non existing file should return false
			NUnit.Framework.Assert.IsFalse(fc2.Delete(testPath, false));
			// TestCase2 : Create , Delete , Delete file
			// Create a file on fc2's file system using fc1
			FileContextTestHelper.CreateFile(fc1, testPath);
			// Ensure file exist
			Assert.True(FileContextTestHelper.Exists(fc2, testPath));
			// Delete test file, deleting existing file should return true
			Assert.True(fc2.Delete(testPath, false));
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
			// Delete on non existing file should return false
			NUnit.Framework.Assert.IsFalse(fc2.Delete(testPath, false));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteDirectory()
		{
			string dirName = "dirTest";
			Path testDirPath = QualifiedPath(dirName, fc2);
			// Ensure directory does not exist
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testDirPath));
			// Create a directory on fc2's file system using fc1
			fc1.Mkdir(testDirPath, FsPermission.GetDefault(), true);
			// Ensure dir is created
			Assert.True(FileContextTestHelper.Exists(fc2, testDirPath));
			Assert.True(FileContextTestHelper.IsDir(fc2, testDirPath));
			fc2.Delete(testDirPath, true);
			// Ensure that directory is deleted
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsDir(fc2, testDirPath));
			// TestCase - Create and delete multiple directories
			string[] dirNames = new string[] { "deleteTest/testDir", "deleteTest/test Dir", "deleteTest/test*Dir"
				, "deleteTest/test#Dir", "deleteTest/test1234", "deleteTest/1234Test", "deleteTest/test)Dir"
				, "deleteTest/test_DIr", "deleteTest/()&^%$#@!~_+}{><?", "  ", "^ " };
			foreach (string f in dirNames)
			{
				if (!IsTestableFileNameOnPlatform(f))
				{
					continue;
				}
				// Create a file on fc2's file system using fc1
				Path testPath = QualifiedPath(f, fc2);
				// Ensure file does not exist
				NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
				// Now create directory
				fc1.Mkdir(testPath, FsPermission.GetDefault(), true);
				// Ensure fc2 has the created directory
				Assert.True(FileContextTestHelper.Exists(fc2, testPath));
				Assert.True(FileContextTestHelper.IsDir(fc2, testPath));
				// Delete dir
				Assert.True(fc2.Delete(testPath, true));
				// verify if directory is deleted
				NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
				NUnit.Framework.Assert.IsFalse(FileContextTestHelper.IsDir(fc2, testPath));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteNonExistingDirectory()
		{
			string testDirName = "testFile";
			Path testPath = QualifiedPath(testDirName, fc2);
			// TestCase1 : Test delete on directory never existed
			// Ensure directory does not exist
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
			// Delete on non existing directory should return false
			NUnit.Framework.Assert.IsFalse(fc2.Delete(testPath, false));
			// TestCase2 : Create dir, Delete dir, Delete dir
			// Create a file on fc2's file system using fc1
			fc1.Mkdir(testPath, FsPermission.GetDefault(), true);
			// Ensure dir exist
			Assert.True(FileContextTestHelper.Exists(fc2, testPath));
			// Delete test file, deleting existing file should return true
			Assert.True(fc2.Delete(testPath, false));
			// Ensure file does not exist
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc2, testPath));
			// Delete on non existing file should return false
			NUnit.Framework.Assert.IsFalse(fc2.Delete(testPath, false));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestModificationTime()
		{
			string testFile = "file1";
			long fc2ModificationTime;
			long fc1ModificationTime;
			Path testPath = QualifiedPath(testFile, fc2);
			// Create a file on fc2's file system using fc1
			FileContextTestHelper.CreateFile(fc1, testPath);
			// Get modification time using fc2 and fc1
			fc1ModificationTime = fc1.GetFileStatus(testPath).GetModificationTime();
			fc2ModificationTime = fc2.GetFileStatus(testPath).GetModificationTime();
			// Ensure fc1 and fc2 reports same modification time
			Assert.Equal(fc1ModificationTime, fc2ModificationTime);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestFileStatus()
		{
			string fileName = "file1";
			Path path2 = fc2.MakeQualified(new Path(Base, fileName));
			// Create a file on fc2's file system using fc1
			FileContextTestHelper.CreateFile(fc1, path2);
			FsStatus fc2Status = fc2.GetFsStatus(path2);
			// FsStatus , used, free and capacity are non-negative longs
			NUnit.Framework.Assert.IsNotNull(fc2Status);
			Assert.True(fc2Status.GetCapacity() > 0);
			Assert.True(fc2Status.GetRemaining() > 0);
			Assert.True(fc2Status.GetUsed() > 0);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetFileStatusThrowsExceptionForNonExistentFile()
		{
			string testFile = "test/hadoop/fileDoesNotExist";
			Path testPath = QualifiedPath(testFile, fc2);
			try
			{
				fc1.GetFileStatus(testPath);
				NUnit.Framework.Assert.Fail("Should throw FileNotFoundException");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestListStatusThrowsExceptionForNonExistentFile()
		{
			string testFile = "test/hadoop/file";
			Path testPath = QualifiedPath(testFile, fc2);
			try
			{
				fc1.ListStatus(testPath);
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
			string hPrefix = "test/hadoop";
			string[] dirs = new string[] { hPrefix + "/a", hPrefix + "/b", hPrefix + "/c", hPrefix
				 + "/1", hPrefix + "/#@#@", hPrefix + "/&*#$#$@234" };
			AList<Path> testDirs = new AList<Path>();
			foreach (string d in dirs)
			{
				if (!IsTestableFileNameOnPlatform(d))
				{
					continue;
				}
				testDirs.AddItem(QualifiedPath(d, fc2));
			}
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc1, testDirs[0]));
			foreach (Path path in testDirs)
			{
				fc1.Mkdir(path, FsPermission.GetDefault(), true);
			}
			// test listStatus that returns an array of FileStatus
			FileStatus[] paths = fc1.Util().ListStatus(QualifiedPath("test", fc1));
			Assert.Equal(1, paths.Length);
			Assert.Equal(QualifiedPath(hPrefix, fc1), paths[0].GetPath());
			paths = fc1.Util().ListStatus(QualifiedPath(hPrefix, fc1));
			Assert.Equal(testDirs.Count, paths.Length);
			for (int i = 0; i < testDirs.Count; i++)
			{
				bool found = false;
				for (int j = 0; j < paths.Length; j++)
				{
					if (QualifiedPath(testDirs[i].ToString(), fc1).Equals(paths[j].GetPath()))
					{
						found = true;
					}
				}
				Assert.True(testDirs[i] + " not found", found);
			}
			paths = fc1.Util().ListStatus(QualifiedPath(dirs[0], fc1));
			Assert.Equal(0, paths.Length);
			// test listStatus that returns an iterator of FileStatus
			RemoteIterator<FileStatus> pathsItor = fc1.ListStatus(QualifiedPath("test", fc1));
			Assert.Equal(QualifiedPath(hPrefix, fc1), pathsItor.Next().GetPath
				());
			NUnit.Framework.Assert.IsFalse(pathsItor.HasNext());
			pathsItor = fc1.ListStatus(QualifiedPath(hPrefix, fc1));
			int dirLen = 0;
			for (; pathsItor.HasNext(); dirLen++)
			{
				bool found = false;
				FileStatus stat = pathsItor.Next();
				for (int j = 0; j < dirs.Length; j++)
				{
					if (QualifiedPath(dirs[j], fc1).Equals(stat.GetPath()))
					{
						found = true;
						break;
					}
				}
				Assert.True(stat.GetPath() + " not found", found);
			}
			Assert.Equal(testDirs.Count, dirLen);
			pathsItor = fc1.ListStatus(QualifiedPath(dirs[0], fc1));
			NUnit.Framework.Assert.IsFalse(pathsItor.HasNext());
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
		private static bool IsTestableFileNameOnPlatform(string fileName)
		{
			bool valid = true;
			if (Shell.Windows)
			{
				// Disallow reserved characters: <, >, :, ", |, ?, *.
				// Disallow trailing space or period.
				// See http://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
				valid = !WinInvalidFileNamePattern.Matcher(fileName).Matches();
			}
			return valid;
		}
	}
}
