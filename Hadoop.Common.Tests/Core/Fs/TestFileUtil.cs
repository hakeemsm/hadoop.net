using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Tools.Tar;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.FS
{
	public class TestFileUtil
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestFileUtil));

		private static readonly string TestRootDir = Runtime.GetProperty("test.build.data"
			, "/tmp") + "/fu";

		private static readonly FilePath TestDir = new FilePath(TestRootDir);

		private const string File = "x";

		private const string Link = "y";

		private const string Dir = "dir";

		private readonly FilePath del = new FilePath(TestDir, "del");

		private readonly FilePath tmp = new FilePath(TestDir, "tmp");

		private readonly FilePath dir1;

		private readonly FilePath dir2;

		private readonly FilePath partitioned = new FilePath(TestDir, "partitioned");

		/// <summary>Creates multiple directories for testing.</summary>
		/// <remarks>
		/// Creates multiple directories for testing.
		/// Contents of them are
		/// dir:tmp:
		/// file: x
		/// dir:del:
		/// file: x
		/// dir: dir1 : file:x
		/// dir: dir2 : file:x
		/// link: y to tmp/x
		/// link: tmpDir to tmp
		/// dir:partitioned:
		/// file: part-r-00000, contents: "foo"
		/// file: part-r-00001, contents: "bar"
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void SetupDirs()
		{
			NUnit.Framework.Assert.IsFalse(del.Exists());
			NUnit.Framework.Assert.IsFalse(tmp.Exists());
			NUnit.Framework.Assert.IsFalse(partitioned.Exists());
			del.Mkdirs();
			tmp.Mkdirs();
			partitioned.Mkdirs();
			new FilePath(del, File).CreateNewFile();
			FilePath tmpFile = new FilePath(tmp, File);
			tmpFile.CreateNewFile();
			// create directories 
			dir1.Mkdirs();
			dir2.Mkdirs();
			new FilePath(dir1, File).CreateNewFile();
			new FilePath(dir2, File).CreateNewFile();
			// create a symlink to file
			FilePath link = new FilePath(del, Link);
			FileUtil.SymLink(tmpFile.ToString(), link.ToString());
			// create a symlink to dir
			FilePath linkDir = new FilePath(del, "tmpDir");
			FileUtil.SymLink(tmp.ToString(), linkDir.ToString());
			Assert.Equal(5, del.ListFiles().Length);
			// create files in partitioned directories
			CreateFile(partitioned, "part-r-00000", "foo");
			CreateFile(partitioned, "part-r-00001", "bar");
			// create a cycle using symlinks. Cycles should be handled
			FileUtil.SymLink(del.ToString(), dir1.ToString() + "/cycle");
		}

		/// <summary>
		/// Creates a new file in the specified directory, with the specified name and
		/// the specified file contents.
		/// </summary>
		/// <remarks>
		/// Creates a new file in the specified directory, with the specified name and
		/// the specified file contents.  This method will add a newline terminator to
		/// the end of the contents string in the destination file.
		/// </remarks>
		/// <param name="directory">File non-null destination directory.</param>
		/// <param name="name">String non-null file name.</param>
		/// <param name="contents">String non-null file contents.</param>
		/// <exception cref="System.IO.IOException">if an I/O error occurs.</exception>
		private FilePath CreateFile(FilePath directory, string name, string contents)
		{
			FilePath newFile = new FilePath(directory, name);
			PrintWriter pw = new PrintWriter(newFile);
			try
			{
				pw.WriteLine(contents);
			}
			finally
			{
				pw.Close();
			}
			return newFile;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestListFiles()
		{
			SetupDirs();
			//Test existing files case 
			FilePath[] files = FileUtil.ListFiles(partitioned);
			Assert.Equal(2, files.Length);
			//Test existing directory with no files case 
			FilePath newDir = new FilePath(tmp.GetPath(), "test");
			newDir.Mkdir();
			Assert.True("Failed to create test dir", newDir.Exists());
			files = FileUtil.ListFiles(newDir);
			Assert.Equal(0, files.Length);
			newDir.Delete();
			NUnit.Framework.Assert.IsFalse("Failed to delete test dir", newDir.Exists());
			//Test non-existing directory case, this throws 
			//IOException
			try
			{
				files = FileUtil.ListFiles(newDir);
				NUnit.Framework.Assert.Fail("IOException expected on listFiles() for non-existent dir "
					 + newDir.ToString());
			}
			catch (IOException)
			{
			}
		}

		//Expected an IOException
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestListAPI()
		{
			SetupDirs();
			//Test existing files case 
			string[] files = FileUtil.List(partitioned);
			Assert.Equal("Unexpected number of pre-existing files", 2, files
				.Length);
			//Test existing directory with no files case 
			FilePath newDir = new FilePath(tmp.GetPath(), "test");
			newDir.Mkdir();
			Assert.True("Failed to create test dir", newDir.Exists());
			files = FileUtil.List(newDir);
			Assert.Equal("New directory unexpectedly contains files", 0, files
				.Length);
			newDir.Delete();
			NUnit.Framework.Assert.IsFalse("Failed to delete test dir", newDir.Exists());
			//Test non-existing directory case, this throws 
			//IOException
			try
			{
				files = FileUtil.List(newDir);
				NUnit.Framework.Assert.Fail("IOException expected on list() for non-existent dir "
					 + newDir.ToString());
			}
			catch (IOException)
			{
			}
		}

		//Expected an IOException
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Before()
		{
			CleanupImpl();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			CleanupImpl();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupImpl()
		{
			FileUtil.FullyDelete(del, true);
			Assert.True(!del.Exists());
			FileUtil.FullyDelete(tmp, true);
			Assert.True(!tmp.Exists());
			FileUtil.FullyDelete(partitioned, true);
			Assert.True(!partitioned.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFullyDelete()
		{
			SetupDirs();
			bool ret = FileUtil.FullyDelete(del);
			Assert.True(ret);
			NUnit.Framework.Assert.IsFalse(del.Exists());
			ValidateTmpDir();
		}

		/// <summary>
		/// Tests if fullyDelete deletes
		/// (a) symlink to file only and not the file pointed to by symlink.
		/// </summary>
		/// <remarks>
		/// Tests if fullyDelete deletes
		/// (a) symlink to file only and not the file pointed to by symlink.
		/// (b) symlink to dir only and not the dir pointed to by symlink.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFullyDeleteSymlinks()
		{
			SetupDirs();
			FilePath link = new FilePath(del, Link);
			Assert.Equal(5, del.List().Length);
			// Since tmpDir is symlink to tmp, fullyDelete(tmpDir) should not
			// delete contents of tmp. See setupDirs for details.
			bool ret = FileUtil.FullyDelete(link);
			Assert.True(ret);
			NUnit.Framework.Assert.IsFalse(link.Exists());
			Assert.Equal(4, del.List().Length);
			ValidateTmpDir();
			FilePath linkDir = new FilePath(del, "tmpDir");
			// Since tmpDir is symlink to tmp, fullyDelete(tmpDir) should not
			// delete contents of tmp. See setupDirs for details.
			ret = FileUtil.FullyDelete(linkDir);
			Assert.True(ret);
			NUnit.Framework.Assert.IsFalse(linkDir.Exists());
			Assert.Equal(3, del.List().Length);
			ValidateTmpDir();
		}

		/// <summary>
		/// Tests if fullyDelete deletes
		/// (a) dangling symlink to file properly
		/// (b) dangling symlink to directory properly
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFullyDeleteDanglingSymlinks()
		{
			SetupDirs();
			// delete the directory tmp to make tmpDir a dangling link to dir tmp and
			// to make y as a dangling link to file tmp/x
			bool ret = FileUtil.FullyDelete(tmp);
			Assert.True(ret);
			NUnit.Framework.Assert.IsFalse(tmp.Exists());
			// dangling symlink to file
			FilePath link = new FilePath(del, Link);
			Assert.Equal(5, del.List().Length);
			// Even though 'y' is dangling symlink to file tmp/x, fullyDelete(y)
			// should delete 'y' properly.
			ret = FileUtil.FullyDelete(link);
			Assert.True(ret);
			Assert.Equal(4, del.List().Length);
			// dangling symlink to directory
			FilePath linkDir = new FilePath(del, "tmpDir");
			// Even though tmpDir is dangling symlink to tmp, fullyDelete(tmpDir) should
			// delete tmpDir properly.
			ret = FileUtil.FullyDelete(linkDir);
			Assert.True(ret);
			Assert.Equal(3, del.List().Length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFullyDeleteContents()
		{
			SetupDirs();
			bool ret = FileUtil.FullyDeleteContents(del);
			Assert.True(ret);
			Assert.True(del.Exists());
			Assert.Equal(0, del.ListFiles().Length);
			ValidateTmpDir();
		}

		private void ValidateTmpDir()
		{
			Assert.True(tmp.Exists());
			Assert.Equal(1, tmp.ListFiles().Length);
			Assert.True(new FilePath(tmp, File).Exists());
		}

		private readonly FilePath xSubDir;

		private readonly FilePath xSubSubDir;

		private readonly FilePath ySubDir;

		private const string file1Name = "file1";

		private readonly FilePath file2;

		private readonly FilePath file22;

		private readonly FilePath file3;

		private readonly FilePath zlink;

		/// <summary>Creates a directory which can not be deleted completely.</summary>
		/// <remarks>
		/// Creates a directory which can not be deleted completely.
		/// Directory structure. The naming is important in that
		/// <see cref="MyFile"/>
		/// is used to return them in alphabetical order when listed.
		/// del(+w)
		/// |
		/// .---------------------------------------,
		/// |            |              |           |
		/// file1(!w)   xSubDir(-rwx)   ySubDir(+w)   zlink
		/// |  |              |
		/// | file2(-rwx)   file3
		/// |
		/// xSubSubDir(-rwx)
		/// |
		/// file22(-rwx)
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void SetupDirsAndNonWritablePermissions()
		{
			NUnit.Framework.Assert.IsFalse("The directory del should not have existed!", del.
				Exists());
			del.Mkdirs();
			new TestFileUtil.MyFile(del, file1Name).CreateNewFile();
			// "file1" is non-deletable by default, see MyFile.delete().
			xSubDir.Mkdirs();
			file2.CreateNewFile();
			xSubSubDir.Mkdirs();
			file22.CreateNewFile();
			RevokePermissions(file22);
			RevokePermissions(xSubSubDir);
			RevokePermissions(file2);
			RevokePermissions(xSubDir);
			ySubDir.Mkdirs();
			file3.CreateNewFile();
			NUnit.Framework.Assert.IsFalse("The directory tmp should not have existed!", tmp.
				Exists());
			tmp.Mkdirs();
			FilePath tmpFile = new FilePath(tmp, File);
			tmpFile.CreateNewFile();
			FileUtil.SymLink(tmpFile.ToString(), zlink.ToString());
		}

		private static void GrantPermissions(FilePath f)
		{
			FileUtil.SetReadable(f, true);
			FileUtil.SetWritable(f, true);
			FileUtil.SetExecutable(f, true);
		}

		private static void RevokePermissions(FilePath f)
		{
			FileUtil.SetWritable(f, false);
			FileUtil.SetExecutable(f, false);
			FileUtil.SetReadable(f, false);
		}

		// Validates the return value.
		// Validates the existence of the file "file1"
		private void ValidateAndSetWritablePermissions(bool expectedRevokedPermissionDirsExist
			, bool ret)
		{
			GrantPermissions(xSubDir);
			GrantPermissions(xSubSubDir);
			NUnit.Framework.Assert.IsFalse("The return value should have been false.", ret);
			Assert.True("The file file1 should not have been deleted.", new 
				FilePath(del, file1Name).Exists());
			Assert.Equal("The directory xSubDir *should* not have been deleted."
				, expectedRevokedPermissionDirsExist, xSubDir.Exists());
			Assert.Equal("The file file2 *should* not have been deleted.", 
				expectedRevokedPermissionDirsExist, file2.Exists());
			Assert.Equal("The directory xSubSubDir *should* not have been deleted."
				, expectedRevokedPermissionDirsExist, xSubSubDir.Exists());
			Assert.Equal("The file file22 *should* not have been deleted."
				, expectedRevokedPermissionDirsExist, file22.Exists());
			NUnit.Framework.Assert.IsFalse("The directory ySubDir should have been deleted.", 
				ySubDir.Exists());
			NUnit.Framework.Assert.IsFalse("The link zlink should have been deleted.", zlink.
				Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailFullyDelete()
		{
			if (Shell.Windows)
			{
				// windows Dir.setWritable(false) does not work for directories
				return;
			}
			Log.Info("Running test to verify failure of fullyDelete()");
			SetupDirsAndNonWritablePermissions();
			bool ret = FileUtil.FullyDelete(new TestFileUtil.MyFile(del));
			ValidateAndSetWritablePermissions(true, ret);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailFullyDeleteGrantPermissions()
		{
			SetupDirsAndNonWritablePermissions();
			bool ret = FileUtil.FullyDelete(new TestFileUtil.MyFile(del), true);
			// this time the directories with revoked permissions *should* be deleted:
			ValidateAndSetWritablePermissions(false, ret);
		}

		/// <summary>
		/// Extend
		/// <see cref="Sharpen.FilePath"/>
		/// . Same as
		/// <see cref="Sharpen.FilePath"/>
		/// except for two things: (1) This
		/// treats file1Name as a very special file which is not delete-able
		/// irrespective of it's parent-dir's permissions, a peculiar file instance for
		/// testing. (2) It returns the files in alphabetically sorted order when
		/// listed.
		/// </summary>
		[System.Serializable]
		public class MyFile : FilePath
		{
			private const long serialVersionUID = 1L;

			public MyFile(FilePath f)
				: base(f.GetAbsolutePath())
			{
			}

			public MyFile(FilePath parent, string child)
				: base(parent, child)
			{
			}

			/// <summary>
			/// Same as
			/// <see cref="Sharpen.FilePath.Delete()"/>
			/// except for file1Name which will never be
			/// deleted (hard-coded)
			/// </summary>
			public override bool Delete()
			{
				Log.Info("Trying to delete myFile " + GetAbsolutePath());
				bool @bool = false;
				if (GetName().Equals(file1Name))
				{
					@bool = false;
				}
				else
				{
					@bool = base.Delete();
				}
				if (@bool)
				{
					Log.Info("Deleted " + GetAbsolutePath() + " successfully");
				}
				else
				{
					Log.Info("Cannot delete " + GetAbsolutePath());
				}
				return @bool;
			}

			/// <summary>Return the list of files in an alphabetically sorted order</summary>
			public override FilePath[] ListFiles()
			{
				FilePath[] files = base.ListFiles();
				if (files == null)
				{
					return null;
				}
				IList<FilePath> filesList = Arrays.AsList(files);
				filesList.Sort();
				FilePath[] myFiles = new TestFileUtil.MyFile[files.Length];
				int i = 0;
				foreach (FilePath f in filesList)
				{
					myFiles[i++] = new TestFileUtil.MyFile(f);
				}
				return myFiles;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailFullyDeleteContents()
		{
			if (Shell.Windows)
			{
				// windows Dir.setWritable(false) does not work for directories
				return;
			}
			Log.Info("Running test to verify failure of fullyDeleteContents()");
			SetupDirsAndNonWritablePermissions();
			bool ret = FileUtil.FullyDeleteContents(new TestFileUtil.MyFile(del));
			ValidateAndSetWritablePermissions(true, ret);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailFullyDeleteContentsGrantPermissions()
		{
			SetupDirsAndNonWritablePermissions();
			bool ret = FileUtil.FullyDeleteContents(new TestFileUtil.MyFile(del), true);
			// this time the directories with revoked permissions *should* be deleted:
			ValidateAndSetWritablePermissions(false, ret);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCopyMergeSingleDirectory()
		{
			SetupDirs();
			bool copyMergeResult = CopyMerge("partitioned", "tmp/merged");
			Assert.True("Expected successful copyMerge result.", copyMergeResult
				);
			FilePath merged = new FilePath(TestDir, "tmp/merged");
			Assert.True("File tmp/merged must exist after copyMerge.", merged
				.Exists());
			BufferedReader rdr = new BufferedReader(new FileReader(merged));
			try
			{
				Assert.Equal("Line 1 of merged file must contain \"foo\".", "foo"
					, rdr.ReadLine());
				Assert.Equal("Line 2 of merged file must contain \"bar\".", "bar"
					, rdr.ReadLine());
				NUnit.Framework.Assert.IsNull("Expected end of file reading merged file.", rdr.ReadLine
					());
			}
			finally
			{
				rdr.Close();
			}
		}

		/// <summary>Calls FileUtil.copyMerge using the specified source and destination paths.
		/// 	</summary>
		/// <remarks>
		/// Calls FileUtil.copyMerge using the specified source and destination paths.
		/// Both source and destination are assumed to be on the local file system.
		/// The call will not delete source on completion and will not add an
		/// additional string between files.
		/// </remarks>
		/// <param name="src">String non-null source path.</param>
		/// <param name="dst">String non-null destination path.</param>
		/// <returns>boolean true if the call to FileUtil.copyMerge was successful.</returns>
		/// <exception cref="System.IO.IOException">if an I/O error occurs.</exception>
		private bool CopyMerge(string src, string dst)
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			bool result;
			try
			{
				Path srcPath = new Path(TestRootDir, src);
				Path dstPath = new Path(TestRootDir, dst);
				bool deleteSource = false;
				string addString = null;
				result = FileUtil.CopyMerge(fs, srcPath, fs, dstPath, deleteSource, conf, addString
					);
			}
			finally
			{
				fs.Close();
			}
			return result;
		}

		/// <summary>
		/// Test that getDU is able to handle cycles caused due to symbolic links
		/// and that directory sizes are not added to the final calculated size
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetDU()
		{
			SetupDirs();
			long du = FileUtil.GetDU(TestDir);
			// Only two files (in partitioned).  Each has 3 characters + system-specific
			// line separator.
			long expected = 2 * (3 + Runtime.GetProperty("line.separator").Length);
			Assert.Equal(expected, du);
			// target file does not exist:
			FilePath doesNotExist = new FilePath(tmp, "QuickBrownFoxJumpsOverTheLazyDog");
			long duDoesNotExist = FileUtil.GetDU(doesNotExist);
			Assert.Equal(0, duDoesNotExist);
			// target file is not a directory:
			FilePath notADirectory = new FilePath(partitioned, "part-r-00000");
			long duNotADirectoryActual = FileUtil.GetDU(notADirectory);
			long duNotADirectoryExpected = 3 + Runtime.GetProperty("line.separator").Length;
			Assert.Equal(duNotADirectoryExpected, duNotADirectoryActual);
			try
			{
				// one of target files is not accessible, but the containing directory
				// is accessible:
				try
				{
					FileUtil.Chmod(notADirectory.GetAbsolutePath(), "0000");
				}
				catch (Exception ie)
				{
					// should never happen since that method never throws InterruptedException.      
					NUnit.Framework.Assert.IsNull(ie);
				}
				NUnit.Framework.Assert.IsFalse(FileUtil.CanRead(notADirectory));
				long du3 = FileUtil.GetDU(partitioned);
				Assert.Equal(expected, du3);
				// some target files and containing directory are not accessible:
				try
				{
					FileUtil.Chmod(partitioned.GetAbsolutePath(), "0000");
				}
				catch (Exception ie)
				{
					// should never happen since that method never throws InterruptedException.      
					NUnit.Framework.Assert.IsNull(ie);
				}
				NUnit.Framework.Assert.IsFalse(FileUtil.CanRead(partitioned));
				long du4 = FileUtil.GetDU(partitioned);
				Assert.Equal(0, du4);
			}
			finally
			{
				// Restore the permissions so that we can delete the folder 
				// in @After method:
				FileUtil.Chmod(partitioned.GetAbsolutePath(), "0777", true);
			}
		}

		/*recursive*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUnTar()
		{
			SetupDirs();
			// make a simple tar:
			FilePath simpleTar = new FilePath(del, File);
			OutputStream os = new FileOutputStream(simpleTar);
			TarOutputStream tos = new TarOutputStream(os);
			try
			{
				TarEntry te = new TarEntry("/bar/foo");
				byte[] data = Sharpen.Runtime.GetBytesForString("some-content", "UTF-8");
				te.SetSize(data.Length);
				tos.PutNextEntry(te);
				tos.Write(data);
				tos.CloseEntry();
				tos.Flush();
				tos.Finish();
			}
			finally
			{
				tos.Close();
			}
			// successfully untar it into an existing dir:
			FileUtil.UnTar(simpleTar, tmp);
			// check result:
			Assert.True(new FilePath(tmp, "/bar/foo").Exists());
			Assert.Equal(12, new FilePath(tmp, "/bar/foo").Length());
			FilePath regularFile = new FilePath(tmp, "QuickBrownFoxJumpsOverTheLazyDog");
			regularFile.CreateNewFile();
			Assert.True(regularFile.Exists());
			try
			{
				FileUtil.UnTar(simpleTar, regularFile);
				Assert.True("An IOException expected.", false);
			}
			catch (IOException)
			{
			}
		}

		// okay
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReplaceFile()
		{
			SetupDirs();
			FilePath srcFile = new FilePath(tmp, "src");
			// src exists, and target does not exist:
			srcFile.CreateNewFile();
			Assert.True(srcFile.Exists());
			FilePath targetFile = new FilePath(tmp, "target");
			Assert.True(!targetFile.Exists());
			FileUtil.ReplaceFile(srcFile, targetFile);
			Assert.True(!srcFile.Exists());
			Assert.True(targetFile.Exists());
			// src exists and target is a regular file: 
			srcFile.CreateNewFile();
			Assert.True(srcFile.Exists());
			FileUtil.ReplaceFile(srcFile, targetFile);
			Assert.True(!srcFile.Exists());
			Assert.True(targetFile.Exists());
			// src exists, and target is a non-empty directory: 
			srcFile.CreateNewFile();
			Assert.True(srcFile.Exists());
			targetFile.Delete();
			targetFile.Mkdirs();
			FilePath obstacle = new FilePath(targetFile, "obstacle");
			obstacle.CreateNewFile();
			Assert.True(obstacle.Exists());
			Assert.True(targetFile.Exists() && targetFile.IsDirectory());
			try
			{
				FileUtil.ReplaceFile(srcFile, targetFile);
				Assert.True(false);
			}
			catch (IOException)
			{
			}
			// okay
			// check up the post-condition: nothing is deleted:
			Assert.True(srcFile.Exists());
			Assert.True(targetFile.Exists() && targetFile.IsDirectory());
			Assert.True(obstacle.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLocalTempFile()
		{
			SetupDirs();
			FilePath baseFile = new FilePath(tmp, "base");
			FilePath tmp1 = FileUtil.CreateLocalTempFile(baseFile, "foo", false);
			FilePath tmp2 = FileUtil.CreateLocalTempFile(baseFile, "foo", true);
			NUnit.Framework.Assert.IsFalse(tmp1.GetAbsolutePath().Equals(baseFile.GetAbsolutePath
				()));
			NUnit.Framework.Assert.IsFalse(tmp2.GetAbsolutePath().Equals(baseFile.GetAbsolutePath
				()));
			Assert.True(tmp1.Exists() && tmp2.Exists());
			Assert.True(tmp1.CanWrite() && tmp2.CanWrite());
			Assert.True(tmp1.CanRead() && tmp2.CanRead());
			tmp1.Delete();
			tmp2.Delete();
			Assert.True(!tmp1.Exists() && !tmp2.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUnZip()
		{
			// make sa simple zip
			SetupDirs();
			// make a simple tar:
			FilePath simpleZip = new FilePath(del, File);
			OutputStream os = new FileOutputStream(simpleZip);
			ZipOutputStream tos = new ZipOutputStream(os);
			try
			{
				ZipEntry ze = new ZipEntry("foo");
				byte[] data = Sharpen.Runtime.GetBytesForString("some-content", "UTF-8");
				ze.SetSize(data.Length);
				tos.PutNextEntry(ze);
				tos.Write(data);
				tos.CloseEntry();
				tos.Flush();
				tos.Finish();
			}
			finally
			{
				tos.Close();
			}
			// successfully untar it into an existing dir:
			FileUtil.UnZip(simpleZip, tmp);
			// check result:
			Assert.True(new FilePath(tmp, "foo").Exists());
			Assert.Equal(12, new FilePath(tmp, "foo").Length());
			FilePath regularFile = new FilePath(tmp, "QuickBrownFoxJumpsOverTheLazyDog");
			regularFile.CreateNewFile();
			Assert.True(regularFile.Exists());
			try
			{
				FileUtil.UnZip(simpleZip, regularFile);
				Assert.True("An IOException expected.", false);
			}
			catch (IOException)
			{
			}
		}

		// okay
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCopy5()
		{
			/*
			* Test method copy(FileSystem srcFS, Path src, File dst, boolean deleteSource, Configuration conf)
			*/
			SetupDirs();
			URI uri = tmp.ToURI();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.NewInstance(uri, conf);
			string content = "some-content";
			FilePath srcFile = CreateFile(tmp, "src", content);
			Path srcPath = new Path(srcFile.ToURI());
			// copy regular file:
			FilePath dest = new FilePath(del, "dest");
			bool result = FileUtil.Copy(fs, srcPath, dest, false, conf);
			Assert.True(result);
			Assert.True(dest.Exists());
			Assert.Equal(Sharpen.Runtime.GetBytesForString(content).Length
				 + Sharpen.Runtime.GetBytesForString(Runtime.GetProperty("line.separator")).Length
				, dest.Length());
			Assert.True(srcFile.Exists());
			// should not be deleted
			// copy regular file, delete src:
			dest.Delete();
			Assert.True(!dest.Exists());
			result = FileUtil.Copy(fs, srcPath, dest, true, conf);
			Assert.True(result);
			Assert.True(dest.Exists());
			Assert.Equal(Sharpen.Runtime.GetBytesForString(content).Length
				 + Sharpen.Runtime.GetBytesForString(Runtime.GetProperty("line.separator")).Length
				, dest.Length());
			Assert.True(!srcFile.Exists());
			// should be deleted
			// copy a dir:
			dest.Delete();
			Assert.True(!dest.Exists());
			srcPath = new Path(partitioned.ToURI());
			result = FileUtil.Copy(fs, srcPath, dest, true, conf);
			Assert.True(result);
			Assert.True(dest.Exists() && dest.IsDirectory());
			FilePath[] files = dest.ListFiles();
			Assert.True(files != null);
			Assert.Equal(2, files.Length);
			foreach (FilePath f in files)
			{
				Assert.Equal(3 + Sharpen.Runtime.GetBytesForString(Runtime.GetProperty
					("line.separator")).Length, f.Length());
			}
			Assert.True(!partitioned.Exists());
		}

		// should be deleted
		public virtual void TestStat2Paths1()
		{
			NUnit.Framework.Assert.IsNull(FileUtil.Stat2Paths(null));
			FileStatus[] fileStatuses = new FileStatus[0];
			Path[] paths = FileUtil.Stat2Paths(fileStatuses);
			Assert.Equal(0, paths.Length);
			Path path1 = new Path("file://foo");
			Path path2 = new Path("file://moo");
			fileStatuses = new FileStatus[] { new FileStatus(3, false, 0, 0, 0, path1), new FileStatus
				(3, false, 0, 0, 0, path2) };
			paths = FileUtil.Stat2Paths(fileStatuses);
			Assert.Equal(2, paths.Length);
			Assert.Equal(paths[0], path1);
			Assert.Equal(paths[1], path2);
		}

		public virtual void TestStat2Paths2()
		{
			Path defaultPath = new Path("file://default");
			Path[] paths = FileUtil.Stat2Paths(null, defaultPath);
			Assert.Equal(1, paths.Length);
			Assert.Equal(defaultPath, paths[0]);
			paths = FileUtil.Stat2Paths(null, null);
			Assert.True(paths != null);
			Assert.Equal(1, paths.Length);
			Assert.Equal(null, paths[0]);
			Path path1 = new Path("file://foo");
			Path path2 = new Path("file://moo");
			FileStatus[] fileStatuses = new FileStatus[] { new FileStatus(3, false, 0, 0, 0, 
				path1), new FileStatus(3, false, 0, 0, 0, path2) };
			paths = FileUtil.Stat2Paths(fileStatuses, defaultPath);
			Assert.Equal(2, paths.Length);
			Assert.Equal(paths[0], path1);
			Assert.Equal(paths[1], path2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSymlink()
		{
			NUnit.Framework.Assert.IsFalse(del.Exists());
			del.Mkdirs();
			byte[] data = Sharpen.Runtime.GetBytesForString("testSymLink");
			FilePath file = new FilePath(del, File);
			FilePath link = new FilePath(del, "_link");
			//write some data to the file
			FileOutputStream os = new FileOutputStream(file);
			os.Write(data);
			os.Close();
			//create the symlink
			FileUtil.SymLink(file.GetAbsolutePath(), link.GetAbsolutePath());
			//ensure that symlink length is correctly reported by Java
			Assert.Equal(data.Length, file.Length());
			Assert.Equal(data.Length, link.Length());
			//ensure that we can read from link.
			FileInputStream @in = new FileInputStream(link);
			long len = 0;
			while (@in.Read() > 0)
			{
				len++;
			}
			@in.Close();
			Assert.Equal(data.Length, len);
		}

		/// <summary>Test that rename on a symlink works as expected.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSymlinkRenameTo()
		{
			NUnit.Framework.Assert.IsFalse(del.Exists());
			del.Mkdirs();
			FilePath file = new FilePath(del, File);
			file.CreateNewFile();
			FilePath link = new FilePath(del, "_link");
			// create the symlink
			FileUtil.SymLink(file.GetAbsolutePath(), link.GetAbsolutePath());
			Assert.True(file.Exists());
			Assert.True(link.Exists());
			FilePath link2 = new FilePath(del, "_link2");
			// Rename the symlink
			Assert.True(link.RenameTo(link2));
			// Make sure the file still exists
			// (NOTE: this would fail on Java6 on Windows if we didn't
			// copy the file in FileUtil#symlink)
			Assert.True(file.Exists());
			Assert.True(link2.Exists());
			NUnit.Framework.Assert.IsFalse(link.Exists());
		}

		/// <summary>Test that deletion of a symlink works as expected.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSymlinkDelete()
		{
			NUnit.Framework.Assert.IsFalse(del.Exists());
			del.Mkdirs();
			FilePath file = new FilePath(del, File);
			file.CreateNewFile();
			FilePath link = new FilePath(del, "_link");
			// create the symlink
			FileUtil.SymLink(file.GetAbsolutePath(), link.GetAbsolutePath());
			Assert.True(file.Exists());
			Assert.True(link.Exists());
			// make sure that deleting a symlink works properly
			Assert.True(link.Delete());
			NUnit.Framework.Assert.IsFalse(link.Exists());
			Assert.True(file.Exists());
		}

		/// <summary>Test that length on a symlink works as expected.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSymlinkLength()
		{
			NUnit.Framework.Assert.IsFalse(del.Exists());
			del.Mkdirs();
			byte[] data = Sharpen.Runtime.GetBytesForString("testSymLinkData");
			FilePath file = new FilePath(del, File);
			FilePath link = new FilePath(del, "_link");
			// write some data to the file
			FileOutputStream os = new FileOutputStream(file);
			os.Write(data);
			os.Close();
			Assert.Equal(0, link.Length());
			// create the symlink
			FileUtil.SymLink(file.GetAbsolutePath(), link.GetAbsolutePath());
			// ensure that File#length returns the target file and link size
			Assert.Equal(data.Length, file.Length());
			Assert.Equal(data.Length, link.Length());
			file.Delete();
			NUnit.Framework.Assert.IsFalse(file.Exists());
			if (Shell.Windows && !Shell.IsJava7OrAbove())
			{
				// On Java6 on Windows, we copied the file
				Assert.Equal(data.Length, link.Length());
			}
			else
			{
				// Otherwise, the target file size is zero
				Assert.Equal(0, link.Length());
			}
			link.Delete();
			NUnit.Framework.Assert.IsFalse(link.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoUntarAndVerify(FilePath tarFile, FilePath untarDir)
		{
			if (untarDir.Exists() && !FileUtil.FullyDelete(untarDir))
			{
				throw new IOException("Could not delete directory '" + untarDir + "'");
			}
			FileUtil.UnTar(tarFile, untarDir);
			string parentDir = untarDir.GetCanonicalPath() + Path.Separator + "name";
			FilePath testFile = new FilePath(parentDir + Path.Separator + "version");
			Assert.True(testFile.Exists());
			Assert.True(testFile.Length() == 0);
			string imageDir = parentDir + Path.Separator + "image";
			testFile = new FilePath(imageDir + Path.Separator + "fsimage");
			Assert.True(testFile.Exists());
			Assert.True(testFile.Length() == 157);
			string currentDir = parentDir + Path.Separator + "current";
			testFile = new FilePath(currentDir + Path.Separator + "fsimage");
			Assert.True(testFile.Exists());
			Assert.True(testFile.Length() == 4331);
			testFile = new FilePath(currentDir + Path.Separator + "edits");
			Assert.True(testFile.Exists());
			Assert.True(testFile.Length() == 1033);
			testFile = new FilePath(currentDir + Path.Separator + "fstime");
			Assert.True(testFile.Exists());
			Assert.True(testFile.Length() == 8);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUntar()
		{
			string tarGzFileName = Runtime.GetProperty("test.cache.data", "build/test/cache")
				 + "/test-untar.tgz";
			string tarFileName = Runtime.GetProperty("test.cache.data", "build/test/cache") +
				 "/test-untar.tar";
			string dataDir = Runtime.GetProperty("test.build.data", "build/test/data");
			FilePath untarDir = new FilePath(dataDir, "untarDir");
			DoUntarAndVerify(new FilePath(tarGzFileName), untarDir);
			DoUntarAndVerify(new FilePath(tarFileName), untarDir);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateJarWithClassPath()
		{
			// setup test directory for files
			NUnit.Framework.Assert.IsFalse(tmp.Exists());
			Assert.True(tmp.Mkdirs());
			// create files expected to match a wildcard
			IList<FilePath> wildcardMatches = Arrays.AsList(new FilePath(tmp, "wildcard1.jar"
				), new FilePath(tmp, "wildcard2.jar"), new FilePath(tmp, "wildcard3.JAR"), new FilePath
				(tmp, "wildcard4.JAR"));
			foreach (FilePath wildcardMatch in wildcardMatches)
			{
				Assert.True("failure creating file: " + wildcardMatch, wildcardMatch
					.CreateNewFile());
			}
			// create non-jar files, which we expect to not be included in the classpath
			Assert.True(new FilePath(tmp, "text.txt").CreateNewFile());
			Assert.True(new FilePath(tmp, "executable.exe").CreateNewFile()
				);
			Assert.True(new FilePath(tmp, "README").CreateNewFile());
			// create classpath jar
			string wildcardPath = tmp.GetCanonicalPath() + FilePath.separator + "*";
			string nonExistentSubdir = tmp.GetCanonicalPath() + Path.Separator + "subdir" + Path
				.Separator;
			IList<string> classPaths = Arrays.AsList(string.Empty, "cp1.jar", "cp2.jar", wildcardPath
				, "cp3.jar", nonExistentSubdir);
			string inputClassPath = StringUtils.Join(FilePath.pathSeparator, classPaths);
			string[] jarCp = FileUtil.CreateJarWithClassPath(inputClassPath + FilePath.pathSeparator
				 + "unexpandedwildcard/*", new Path(tmp.GetCanonicalPath()), Sharpen.Runtime.GetEnv
				());
			string classPathJar = jarCp[0];
			Assert.AssertNotEquals("Unexpanded wildcard was not placed in extra classpath", jarCp
				[1].IndexOf("unexpanded"), -1);
			// verify classpath by reading manifest from jar file
			JarFile jarFile = null;
			try
			{
				jarFile = new JarFile(classPathJar);
				Manifest jarManifest = jarFile.GetManifest();
				NUnit.Framework.Assert.IsNotNull(jarManifest);
				Attributes mainAttributes = jarManifest.GetMainAttributes();
				NUnit.Framework.Assert.IsNotNull(mainAttributes);
				Assert.True(mainAttributes.Contains(Attributes.Name.ClassPath));
				string classPathAttr = mainAttributes.GetValue(Attributes.Name.ClassPath);
				NUnit.Framework.Assert.IsNotNull(classPathAttr);
				IList<string> expectedClassPaths = new AList<string>();
				foreach (string classPath in classPaths)
				{
					if (classPath.Length == 0)
					{
						continue;
					}
					if (wildcardPath.Equals(classPath))
					{
						// add wildcard matches
						foreach (FilePath wildcardMatch_1 in wildcardMatches)
						{
							expectedClassPaths.AddItem(wildcardMatch_1.ToURI().ToURL().ToExternalForm());
						}
					}
					else
					{
						FilePath fileCp = null;
						if (!new Path(classPath).IsAbsolute())
						{
							fileCp = new FilePath(tmp, classPath);
						}
						else
						{
							fileCp = new FilePath(classPath);
						}
						if (nonExistentSubdir.Equals(classPath))
						{
							// expect to maintain trailing path separator if present in input, even
							// if directory doesn't exist yet
							expectedClassPaths.AddItem(fileCp.ToURI().ToURL().ToExternalForm() + Path.Separator
								);
						}
						else
						{
							expectedClassPaths.AddItem(fileCp.ToURI().ToURL().ToExternalForm());
						}
					}
				}
				IList<string> actualClassPaths = Arrays.AsList(classPathAttr.Split(" "));
				expectedClassPaths.Sort();
				actualClassPaths.Sort();
				Assert.Equal(expectedClassPaths, actualClassPaths);
			}
			finally
			{
				if (jarFile != null)
				{
					try
					{
						jarFile.Close();
					}
					catch (IOException e)
					{
						Log.Warn("exception closing jarFile: " + classPathJar, e);
					}
				}
			}
		}

		public TestFileUtil()
		{
			dir1 = new FilePath(del, Dir + "1");
			dir2 = new FilePath(del, Dir + "2");
			xSubDir = new FilePath(del, "xSubDir");
			xSubSubDir = new FilePath(xSubDir, "xSubSubDir");
			ySubDir = new FilePath(del, "ySubDir");
			file2 = new FilePath(xSubDir, "file2");
			file22 = new FilePath(xSubSubDir, "file22");
			file3 = new FilePath(ySubDir, "file3");
			zlink = new FilePath(del, "zlink");
		}
	}
}
