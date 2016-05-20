using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFileUtil
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestFileUtil
			)));

		private static readonly string TEST_ROOT_DIR = Sharpen.Runtime.getProperty("test.build.data"
			, "/tmp") + "/fu";

		private static readonly java.io.File TEST_DIR = new java.io.File(TEST_ROOT_DIR);

		private const string FILE = "x";

		private const string LINK = "y";

		private const string DIR = "dir";

		private readonly java.io.File del = new java.io.File(TEST_DIR, "del");

		private readonly java.io.File tmp = new java.io.File(TEST_DIR, "tmp");

		private readonly java.io.File dir1;

		private readonly java.io.File dir2;

		private readonly java.io.File partitioned = new java.io.File(TEST_DIR, "partitioned"
			);

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
		private void setupDirs()
		{
			NUnit.Framework.Assert.IsFalse(del.exists());
			NUnit.Framework.Assert.IsFalse(tmp.exists());
			NUnit.Framework.Assert.IsFalse(partitioned.exists());
			del.mkdirs();
			tmp.mkdirs();
			partitioned.mkdirs();
			new java.io.File(del, FILE).createNewFile();
			java.io.File tmpFile = new java.io.File(tmp, FILE);
			tmpFile.createNewFile();
			// create directories 
			dir1.mkdirs();
			dir2.mkdirs();
			new java.io.File(dir1, FILE).createNewFile();
			new java.io.File(dir2, FILE).createNewFile();
			// create a symlink to file
			java.io.File link = new java.io.File(del, LINK);
			org.apache.hadoop.fs.FileUtil.symLink(tmpFile.ToString(), link.ToString());
			// create a symlink to dir
			java.io.File linkDir = new java.io.File(del, "tmpDir");
			org.apache.hadoop.fs.FileUtil.symLink(tmp.ToString(), linkDir.ToString());
			NUnit.Framework.Assert.AreEqual(5, del.listFiles().Length);
			// create files in partitioned directories
			createFile(partitioned, "part-r-00000", "foo");
			createFile(partitioned, "part-r-00001", "bar");
			// create a cycle using symlinks. Cycles should be handled
			org.apache.hadoop.fs.FileUtil.symLink(del.ToString(), dir1.ToString() + "/cycle");
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
		private java.io.File createFile(java.io.File directory, string name, string contents
			)
		{
			java.io.File newFile = new java.io.File(directory, name);
			java.io.PrintWriter pw = new java.io.PrintWriter(newFile);
			try
			{
				pw.println(contents);
			}
			finally
			{
				pw.close();
			}
			return newFile;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testListFiles()
		{
			setupDirs();
			//Test existing files case 
			java.io.File[] files = org.apache.hadoop.fs.FileUtil.listFiles(partitioned);
			NUnit.Framework.Assert.AreEqual(2, files.Length);
			//Test existing directory with no files case 
			java.io.File newDir = new java.io.File(tmp.getPath(), "test");
			newDir.mkdir();
			NUnit.Framework.Assert.IsTrue("Failed to create test dir", newDir.exists());
			files = org.apache.hadoop.fs.FileUtil.listFiles(newDir);
			NUnit.Framework.Assert.AreEqual(0, files.Length);
			newDir.delete();
			NUnit.Framework.Assert.IsFalse("Failed to delete test dir", newDir.exists());
			//Test non-existing directory case, this throws 
			//IOException
			try
			{
				files = org.apache.hadoop.fs.FileUtil.listFiles(newDir);
				NUnit.Framework.Assert.Fail("IOException expected on listFiles() for non-existent dir "
					 + newDir.ToString());
			}
			catch (System.IO.IOException)
			{
			}
		}

		//Expected an IOException
		/// <exception cref="System.IO.IOException"/>
		public virtual void testListAPI()
		{
			setupDirs();
			//Test existing files case 
			string[] files = org.apache.hadoop.fs.FileUtil.list(partitioned);
			NUnit.Framework.Assert.AreEqual("Unexpected number of pre-existing files", 2, files
				.Length);
			//Test existing directory with no files case 
			java.io.File newDir = new java.io.File(tmp.getPath(), "test");
			newDir.mkdir();
			NUnit.Framework.Assert.IsTrue("Failed to create test dir", newDir.exists());
			files = org.apache.hadoop.fs.FileUtil.list(newDir);
			NUnit.Framework.Assert.AreEqual("New directory unexpectedly contains files", 0, files
				.Length);
			newDir.delete();
			NUnit.Framework.Assert.IsFalse("Failed to delete test dir", newDir.exists());
			//Test non-existing directory case, this throws 
			//IOException
			try
			{
				files = org.apache.hadoop.fs.FileUtil.list(newDir);
				NUnit.Framework.Assert.Fail("IOException expected on list() for non-existent dir "
					 + newDir.ToString());
			}
			catch (System.IO.IOException)
			{
			}
		}

		//Expected an IOException
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void before()
		{
			cleanupImpl();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			cleanupImpl();
		}

		/// <exception cref="System.IO.IOException"/>
		private void cleanupImpl()
		{
			org.apache.hadoop.fs.FileUtil.fullyDelete(del, true);
			NUnit.Framework.Assert.IsTrue(!del.exists());
			org.apache.hadoop.fs.FileUtil.fullyDelete(tmp, true);
			NUnit.Framework.Assert.IsTrue(!tmp.exists());
			org.apache.hadoop.fs.FileUtil.fullyDelete(partitioned, true);
			NUnit.Framework.Assert.IsTrue(!partitioned.exists());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFullyDelete()
		{
			setupDirs();
			bool ret = org.apache.hadoop.fs.FileUtil.fullyDelete(del);
			NUnit.Framework.Assert.IsTrue(ret);
			NUnit.Framework.Assert.IsFalse(del.exists());
			validateTmpDir();
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
		public virtual void testFullyDeleteSymlinks()
		{
			setupDirs();
			java.io.File link = new java.io.File(del, LINK);
			NUnit.Framework.Assert.AreEqual(5, del.list().Length);
			// Since tmpDir is symlink to tmp, fullyDelete(tmpDir) should not
			// delete contents of tmp. See setupDirs for details.
			bool ret = org.apache.hadoop.fs.FileUtil.fullyDelete(link);
			NUnit.Framework.Assert.IsTrue(ret);
			NUnit.Framework.Assert.IsFalse(link.exists());
			NUnit.Framework.Assert.AreEqual(4, del.list().Length);
			validateTmpDir();
			java.io.File linkDir = new java.io.File(del, "tmpDir");
			// Since tmpDir is symlink to tmp, fullyDelete(tmpDir) should not
			// delete contents of tmp. See setupDirs for details.
			ret = org.apache.hadoop.fs.FileUtil.fullyDelete(linkDir);
			NUnit.Framework.Assert.IsTrue(ret);
			NUnit.Framework.Assert.IsFalse(linkDir.exists());
			NUnit.Framework.Assert.AreEqual(3, del.list().Length);
			validateTmpDir();
		}

		/// <summary>
		/// Tests if fullyDelete deletes
		/// (a) dangling symlink to file properly
		/// (b) dangling symlink to directory properly
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFullyDeleteDanglingSymlinks()
		{
			setupDirs();
			// delete the directory tmp to make tmpDir a dangling link to dir tmp and
			// to make y as a dangling link to file tmp/x
			bool ret = org.apache.hadoop.fs.FileUtil.fullyDelete(tmp);
			NUnit.Framework.Assert.IsTrue(ret);
			NUnit.Framework.Assert.IsFalse(tmp.exists());
			// dangling symlink to file
			java.io.File link = new java.io.File(del, LINK);
			NUnit.Framework.Assert.AreEqual(5, del.list().Length);
			// Even though 'y' is dangling symlink to file tmp/x, fullyDelete(y)
			// should delete 'y' properly.
			ret = org.apache.hadoop.fs.FileUtil.fullyDelete(link);
			NUnit.Framework.Assert.IsTrue(ret);
			NUnit.Framework.Assert.AreEqual(4, del.list().Length);
			// dangling symlink to directory
			java.io.File linkDir = new java.io.File(del, "tmpDir");
			// Even though tmpDir is dangling symlink to tmp, fullyDelete(tmpDir) should
			// delete tmpDir properly.
			ret = org.apache.hadoop.fs.FileUtil.fullyDelete(linkDir);
			NUnit.Framework.Assert.IsTrue(ret);
			NUnit.Framework.Assert.AreEqual(3, del.list().Length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFullyDeleteContents()
		{
			setupDirs();
			bool ret = org.apache.hadoop.fs.FileUtil.fullyDeleteContents(del);
			NUnit.Framework.Assert.IsTrue(ret);
			NUnit.Framework.Assert.IsTrue(del.exists());
			NUnit.Framework.Assert.AreEqual(0, del.listFiles().Length);
			validateTmpDir();
		}

		private void validateTmpDir()
		{
			NUnit.Framework.Assert.IsTrue(tmp.exists());
			NUnit.Framework.Assert.AreEqual(1, tmp.listFiles().Length);
			NUnit.Framework.Assert.IsTrue(new java.io.File(tmp, FILE).exists());
		}

		private readonly java.io.File xSubDir;

		private readonly java.io.File xSubSubDir;

		private readonly java.io.File ySubDir;

		private const string file1Name = "file1";

		private readonly java.io.File file2;

		private readonly java.io.File file22;

		private readonly java.io.File file3;

		private readonly java.io.File zlink;

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
		private void setupDirsAndNonWritablePermissions()
		{
			NUnit.Framework.Assert.IsFalse("The directory del should not have existed!", del.
				exists());
			del.mkdirs();
			new org.apache.hadoop.fs.TestFileUtil.MyFile(del, file1Name).createNewFile();
			// "file1" is non-deletable by default, see MyFile.delete().
			xSubDir.mkdirs();
			file2.createNewFile();
			xSubSubDir.mkdirs();
			file22.createNewFile();
			revokePermissions(file22);
			revokePermissions(xSubSubDir);
			revokePermissions(file2);
			revokePermissions(xSubDir);
			ySubDir.mkdirs();
			file3.createNewFile();
			NUnit.Framework.Assert.IsFalse("The directory tmp should not have existed!", tmp.
				exists());
			tmp.mkdirs();
			java.io.File tmpFile = new java.io.File(tmp, FILE);
			tmpFile.createNewFile();
			org.apache.hadoop.fs.FileUtil.symLink(tmpFile.ToString(), zlink.ToString());
		}

		private static void grantPermissions(java.io.File f)
		{
			org.apache.hadoop.fs.FileUtil.setReadable(f, true);
			org.apache.hadoop.fs.FileUtil.setWritable(f, true);
			org.apache.hadoop.fs.FileUtil.setExecutable(f, true);
		}

		private static void revokePermissions(java.io.File f)
		{
			org.apache.hadoop.fs.FileUtil.setWritable(f, false);
			org.apache.hadoop.fs.FileUtil.setExecutable(f, false);
			org.apache.hadoop.fs.FileUtil.setReadable(f, false);
		}

		// Validates the return value.
		// Validates the existence of the file "file1"
		private void validateAndSetWritablePermissions(bool expectedRevokedPermissionDirsExist
			, bool ret)
		{
			grantPermissions(xSubDir);
			grantPermissions(xSubSubDir);
			NUnit.Framework.Assert.IsFalse("The return value should have been false.", ret);
			NUnit.Framework.Assert.IsTrue("The file file1 should not have been deleted.", new 
				java.io.File(del, file1Name).exists());
			NUnit.Framework.Assert.AreEqual("The directory xSubDir *should* not have been deleted."
				, expectedRevokedPermissionDirsExist, xSubDir.exists());
			NUnit.Framework.Assert.AreEqual("The file file2 *should* not have been deleted.", 
				expectedRevokedPermissionDirsExist, file2.exists());
			NUnit.Framework.Assert.AreEqual("The directory xSubSubDir *should* not have been deleted."
				, expectedRevokedPermissionDirsExist, xSubSubDir.exists());
			NUnit.Framework.Assert.AreEqual("The file file22 *should* not have been deleted."
				, expectedRevokedPermissionDirsExist, file22.exists());
			NUnit.Framework.Assert.IsFalse("The directory ySubDir should have been deleted.", 
				ySubDir.exists());
			NUnit.Framework.Assert.IsFalse("The link zlink should have been deleted.", zlink.
				exists());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailFullyDelete()
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				// windows Dir.setWritable(false) does not work for directories
				return;
			}
			LOG.info("Running test to verify failure of fullyDelete()");
			setupDirsAndNonWritablePermissions();
			bool ret = org.apache.hadoop.fs.FileUtil.fullyDelete(new org.apache.hadoop.fs.TestFileUtil.MyFile
				(del));
			validateAndSetWritablePermissions(true, ret);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailFullyDeleteGrantPermissions()
		{
			setupDirsAndNonWritablePermissions();
			bool ret = org.apache.hadoop.fs.FileUtil.fullyDelete(new org.apache.hadoop.fs.TestFileUtil.MyFile
				(del), true);
			// this time the directories with revoked permissions *should* be deleted:
			validateAndSetWritablePermissions(false, ret);
		}

		/// <summary>
		/// Extend
		/// <see cref="java.io.File"/>
		/// . Same as
		/// <see cref="java.io.File"/>
		/// except for two things: (1) This
		/// treats file1Name as a very special file which is not delete-able
		/// irrespective of it's parent-dir's permissions, a peculiar file instance for
		/// testing. (2) It returns the files in alphabetically sorted order when
		/// listed.
		/// </summary>
		[System.Serializable]
		public class MyFile : java.io.File
		{
			private const long serialVersionUID = 1L;

			public MyFile(java.io.File f)
				: base(f.getAbsolutePath())
			{
			}

			public MyFile(java.io.File parent, string child)
				: base(parent, child)
			{
			}

			/// <summary>
			/// Same as
			/// <see cref="java.io.File.delete()"/>
			/// except for file1Name which will never be
			/// deleted (hard-coded)
			/// </summary>
			public override bool delete()
			{
				LOG.info("Trying to delete myFile " + getAbsolutePath());
				bool @bool = false;
				if (getName().Equals(file1Name))
				{
					@bool = false;
				}
				else
				{
					@bool = base.delete();
				}
				if (@bool)
				{
					LOG.info("Deleted " + getAbsolutePath() + " successfully");
				}
				else
				{
					LOG.info("Cannot delete " + getAbsolutePath());
				}
				return @bool;
			}

			/// <summary>Return the list of files in an alphabetically sorted order</summary>
			public override java.io.File[] listFiles()
			{
				java.io.File[] files = base.listFiles();
				if (files == null)
				{
					return null;
				}
				System.Collections.Generic.IList<java.io.File> filesList = java.util.Arrays.asList
					(files);
				filesList.Sort();
				java.io.File[] myFiles = new org.apache.hadoop.fs.TestFileUtil.MyFile[files.Length
					];
				int i = 0;
				foreach (java.io.File f in filesList)
				{
					myFiles[i++] = new org.apache.hadoop.fs.TestFileUtil.MyFile(f);
				}
				return myFiles;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailFullyDeleteContents()
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				// windows Dir.setWritable(false) does not work for directories
				return;
			}
			LOG.info("Running test to verify failure of fullyDeleteContents()");
			setupDirsAndNonWritablePermissions();
			bool ret = org.apache.hadoop.fs.FileUtil.fullyDeleteContents(new org.apache.hadoop.fs.TestFileUtil.MyFile
				(del));
			validateAndSetWritablePermissions(true, ret);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailFullyDeleteContentsGrantPermissions()
		{
			setupDirsAndNonWritablePermissions();
			bool ret = org.apache.hadoop.fs.FileUtil.fullyDeleteContents(new org.apache.hadoop.fs.TestFileUtil.MyFile
				(del), true);
			// this time the directories with revoked permissions *should* be deleted:
			validateAndSetWritablePermissions(false, ret);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCopyMergeSingleDirectory()
		{
			setupDirs();
			bool copyMergeResult = copyMerge("partitioned", "tmp/merged");
			NUnit.Framework.Assert.IsTrue("Expected successful copyMerge result.", copyMergeResult
				);
			java.io.File merged = new java.io.File(TEST_DIR, "tmp/merged");
			NUnit.Framework.Assert.IsTrue("File tmp/merged must exist after copyMerge.", merged
				.exists());
			java.io.BufferedReader rdr = new java.io.BufferedReader(new java.io.FileReader(merged
				));
			try
			{
				NUnit.Framework.Assert.AreEqual("Line 1 of merged file must contain \"foo\".", "foo"
					, rdr.readLine());
				NUnit.Framework.Assert.AreEqual("Line 2 of merged file must contain \"bar\".", "bar"
					, rdr.readLine());
				NUnit.Framework.Assert.IsNull("Expected end of file reading merged file.", rdr.readLine
					());
			}
			finally
			{
				rdr.close();
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
		private bool copyMerge(string src, string dst)
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			bool result;
			try
			{
				org.apache.hadoop.fs.Path srcPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
					src);
				org.apache.hadoop.fs.Path dstPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, 
					dst);
				bool deleteSource = false;
				string addString = null;
				result = org.apache.hadoop.fs.FileUtil.copyMerge(fs, srcPath, fs, dstPath, deleteSource
					, conf, addString);
			}
			finally
			{
				fs.close();
			}
			return result;
		}

		/// <summary>
		/// Test that getDU is able to handle cycles caused due to symbolic links
		/// and that directory sizes are not added to the final calculated size
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void testGetDU()
		{
			setupDirs();
			long du = org.apache.hadoop.fs.FileUtil.getDU(TEST_DIR);
			// Only two files (in partitioned).  Each has 3 characters + system-specific
			// line separator.
			long expected = 2 * (3 + Sharpen.Runtime.getProperty("line.separator").Length);
			NUnit.Framework.Assert.AreEqual(expected, du);
			// target file does not exist:
			java.io.File doesNotExist = new java.io.File(tmp, "QuickBrownFoxJumpsOverTheLazyDog"
				);
			long duDoesNotExist = org.apache.hadoop.fs.FileUtil.getDU(doesNotExist);
			NUnit.Framework.Assert.AreEqual(0, duDoesNotExist);
			// target file is not a directory:
			java.io.File notADirectory = new java.io.File(partitioned, "part-r-00000");
			long duNotADirectoryActual = org.apache.hadoop.fs.FileUtil.getDU(notADirectory);
			long duNotADirectoryExpected = 3 + Sharpen.Runtime.getProperty("line.separator").
				Length;
			NUnit.Framework.Assert.AreEqual(duNotADirectoryExpected, duNotADirectoryActual);
			try
			{
				// one of target files is not accessible, but the containing directory
				// is accessible:
				try
				{
					org.apache.hadoop.fs.FileUtil.chmod(notADirectory.getAbsolutePath(), "0000");
				}
				catch (System.Exception ie)
				{
					// should never happen since that method never throws InterruptedException.      
					NUnit.Framework.Assert.IsNull(ie);
				}
				NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileUtil.canRead(notADirectory
					));
				long du3 = org.apache.hadoop.fs.FileUtil.getDU(partitioned);
				NUnit.Framework.Assert.AreEqual(expected, du3);
				// some target files and containing directory are not accessible:
				try
				{
					org.apache.hadoop.fs.FileUtil.chmod(partitioned.getAbsolutePath(), "0000");
				}
				catch (System.Exception ie)
				{
					// should never happen since that method never throws InterruptedException.      
					NUnit.Framework.Assert.IsNull(ie);
				}
				NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileUtil.canRead(partitioned)
					);
				long du4 = org.apache.hadoop.fs.FileUtil.getDU(partitioned);
				NUnit.Framework.Assert.AreEqual(0, du4);
			}
			finally
			{
				// Restore the permissions so that we can delete the folder 
				// in @After method:
				org.apache.hadoop.fs.FileUtil.chmod(partitioned.getAbsolutePath(), "0777", true);
			}
		}

		/*recursive*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void testUnTar()
		{
			setupDirs();
			// make a simple tar:
			java.io.File simpleTar = new java.io.File(del, FILE);
			java.io.OutputStream os = new java.io.FileOutputStream(simpleTar);
			org.apache.tools.tar.TarOutputStream tos = new org.apache.tools.tar.TarOutputStream
				(os);
			try
			{
				org.apache.tools.tar.TarEntry te = new org.apache.tools.tar.TarEntry("/bar/foo");
				byte[] data = Sharpen.Runtime.getBytesForString("some-content", "UTF-8");
				te.setSize(data.Length);
				tos.putNextEntry(te);
				tos.write(data);
				tos.closeEntry();
				tos.flush();
				tos.finish();
			}
			finally
			{
				tos.close();
			}
			// successfully untar it into an existing dir:
			org.apache.hadoop.fs.FileUtil.unTar(simpleTar, tmp);
			// check result:
			NUnit.Framework.Assert.IsTrue(new java.io.File(tmp, "/bar/foo").exists());
			NUnit.Framework.Assert.AreEqual(12, new java.io.File(tmp, "/bar/foo").length());
			java.io.File regularFile = new java.io.File(tmp, "QuickBrownFoxJumpsOverTheLazyDog"
				);
			regularFile.createNewFile();
			NUnit.Framework.Assert.IsTrue(regularFile.exists());
			try
			{
				org.apache.hadoop.fs.FileUtil.unTar(simpleTar, regularFile);
				NUnit.Framework.Assert.IsTrue("An IOException expected.", false);
			}
			catch (System.IO.IOException)
			{
			}
		}

		// okay
		/// <exception cref="System.IO.IOException"/>
		public virtual void testReplaceFile()
		{
			setupDirs();
			java.io.File srcFile = new java.io.File(tmp, "src");
			// src exists, and target does not exist:
			srcFile.createNewFile();
			NUnit.Framework.Assert.IsTrue(srcFile.exists());
			java.io.File targetFile = new java.io.File(tmp, "target");
			NUnit.Framework.Assert.IsTrue(!targetFile.exists());
			org.apache.hadoop.fs.FileUtil.replaceFile(srcFile, targetFile);
			NUnit.Framework.Assert.IsTrue(!srcFile.exists());
			NUnit.Framework.Assert.IsTrue(targetFile.exists());
			// src exists and target is a regular file: 
			srcFile.createNewFile();
			NUnit.Framework.Assert.IsTrue(srcFile.exists());
			org.apache.hadoop.fs.FileUtil.replaceFile(srcFile, targetFile);
			NUnit.Framework.Assert.IsTrue(!srcFile.exists());
			NUnit.Framework.Assert.IsTrue(targetFile.exists());
			// src exists, and target is a non-empty directory: 
			srcFile.createNewFile();
			NUnit.Framework.Assert.IsTrue(srcFile.exists());
			targetFile.delete();
			targetFile.mkdirs();
			java.io.File obstacle = new java.io.File(targetFile, "obstacle");
			obstacle.createNewFile();
			NUnit.Framework.Assert.IsTrue(obstacle.exists());
			NUnit.Framework.Assert.IsTrue(targetFile.exists() && targetFile.isDirectory());
			try
			{
				org.apache.hadoop.fs.FileUtil.replaceFile(srcFile, targetFile);
				NUnit.Framework.Assert.IsTrue(false);
			}
			catch (System.IO.IOException)
			{
			}
			// okay
			// check up the post-condition: nothing is deleted:
			NUnit.Framework.Assert.IsTrue(srcFile.exists());
			NUnit.Framework.Assert.IsTrue(targetFile.exists() && targetFile.isDirectory());
			NUnit.Framework.Assert.IsTrue(obstacle.exists());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLocalTempFile()
		{
			setupDirs();
			java.io.File baseFile = new java.io.File(tmp, "base");
			java.io.File tmp1 = org.apache.hadoop.fs.FileUtil.createLocalTempFile(baseFile, "foo"
				, false);
			java.io.File tmp2 = org.apache.hadoop.fs.FileUtil.createLocalTempFile(baseFile, "foo"
				, true);
			NUnit.Framework.Assert.IsFalse(tmp1.getAbsolutePath().Equals(baseFile.getAbsolutePath
				()));
			NUnit.Framework.Assert.IsFalse(tmp2.getAbsolutePath().Equals(baseFile.getAbsolutePath
				()));
			NUnit.Framework.Assert.IsTrue(tmp1.exists() && tmp2.exists());
			NUnit.Framework.Assert.IsTrue(tmp1.canWrite() && tmp2.canWrite());
			NUnit.Framework.Assert.IsTrue(tmp1.canRead() && tmp2.canRead());
			tmp1.delete();
			tmp2.delete();
			NUnit.Framework.Assert.IsTrue(!tmp1.exists() && !tmp2.exists());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testUnZip()
		{
			// make sa simple zip
			setupDirs();
			// make a simple tar:
			java.io.File simpleZip = new java.io.File(del, FILE);
			java.io.OutputStream os = new java.io.FileOutputStream(simpleZip);
			java.util.zip.ZipOutputStream tos = new java.util.zip.ZipOutputStream(os);
			try
			{
				java.util.zip.ZipEntry ze = new java.util.zip.ZipEntry("foo");
				byte[] data = Sharpen.Runtime.getBytesForString("some-content", "UTF-8");
				ze.setSize(data.Length);
				tos.putNextEntry(ze);
				tos.write(data);
				tos.closeEntry();
				tos.flush();
				tos.finish();
			}
			finally
			{
				tos.close();
			}
			// successfully untar it into an existing dir:
			org.apache.hadoop.fs.FileUtil.unZip(simpleZip, tmp);
			// check result:
			NUnit.Framework.Assert.IsTrue(new java.io.File(tmp, "foo").exists());
			NUnit.Framework.Assert.AreEqual(12, new java.io.File(tmp, "foo").length());
			java.io.File regularFile = new java.io.File(tmp, "QuickBrownFoxJumpsOverTheLazyDog"
				);
			regularFile.createNewFile();
			NUnit.Framework.Assert.IsTrue(regularFile.exists());
			try
			{
				org.apache.hadoop.fs.FileUtil.unZip(simpleZip, regularFile);
				NUnit.Framework.Assert.IsTrue("An IOException expected.", false);
			}
			catch (System.IO.IOException)
			{
			}
		}

		// okay
		/// <exception cref="System.IO.IOException"/>
		public virtual void testCopy5()
		{
			/*
			* Test method copy(FileSystem srcFS, Path src, File dst, boolean deleteSource, Configuration conf)
			*/
			setupDirs();
			java.net.URI uri = tmp.toURI();
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.newInstance(
				uri, conf);
			string content = "some-content";
			java.io.File srcFile = createFile(tmp, "src", content);
			org.apache.hadoop.fs.Path srcPath = new org.apache.hadoop.fs.Path(srcFile.toURI()
				);
			// copy regular file:
			java.io.File dest = new java.io.File(del, "dest");
			bool result = org.apache.hadoop.fs.FileUtil.copy(fs, srcPath, dest, false, conf);
			NUnit.Framework.Assert.IsTrue(result);
			NUnit.Framework.Assert.IsTrue(dest.exists());
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getBytesForString(content).Length
				 + Sharpen.Runtime.getBytesForString(Sharpen.Runtime.getProperty("line.separator"
				)).Length, dest.length());
			NUnit.Framework.Assert.IsTrue(srcFile.exists());
			// should not be deleted
			// copy regular file, delete src:
			dest.delete();
			NUnit.Framework.Assert.IsTrue(!dest.exists());
			result = org.apache.hadoop.fs.FileUtil.copy(fs, srcPath, dest, true, conf);
			NUnit.Framework.Assert.IsTrue(result);
			NUnit.Framework.Assert.IsTrue(dest.exists());
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getBytesForString(content).Length
				 + Sharpen.Runtime.getBytesForString(Sharpen.Runtime.getProperty("line.separator"
				)).Length, dest.length());
			NUnit.Framework.Assert.IsTrue(!srcFile.exists());
			// should be deleted
			// copy a dir:
			dest.delete();
			NUnit.Framework.Assert.IsTrue(!dest.exists());
			srcPath = new org.apache.hadoop.fs.Path(partitioned.toURI());
			result = org.apache.hadoop.fs.FileUtil.copy(fs, srcPath, dest, true, conf);
			NUnit.Framework.Assert.IsTrue(result);
			NUnit.Framework.Assert.IsTrue(dest.exists() && dest.isDirectory());
			java.io.File[] files = dest.listFiles();
			NUnit.Framework.Assert.IsTrue(files != null);
			NUnit.Framework.Assert.AreEqual(2, files.Length);
			foreach (java.io.File f in files)
			{
				NUnit.Framework.Assert.AreEqual(3 + Sharpen.Runtime.getBytesForString(Sharpen.Runtime
					.getProperty("line.separator")).Length, f.length());
			}
			NUnit.Framework.Assert.IsTrue(!partitioned.exists());
		}

		// should be deleted
		public virtual void testStat2Paths1()
		{
			NUnit.Framework.Assert.IsNull(org.apache.hadoop.fs.FileUtil.stat2Paths(null));
			org.apache.hadoop.fs.FileStatus[] fileStatuses = new org.apache.hadoop.fs.FileStatus
				[0];
			org.apache.hadoop.fs.Path[] paths = org.apache.hadoop.fs.FileUtil.stat2Paths(fileStatuses
				);
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
			org.apache.hadoop.fs.Path path1 = new org.apache.hadoop.fs.Path("file://foo");
			org.apache.hadoop.fs.Path path2 = new org.apache.hadoop.fs.Path("file://moo");
			fileStatuses = new org.apache.hadoop.fs.FileStatus[] { new org.apache.hadoop.fs.FileStatus
				(3, false, 0, 0, 0, path1), new org.apache.hadoop.fs.FileStatus(3, false, 0, 0, 
				0, path2) };
			paths = org.apache.hadoop.fs.FileUtil.stat2Paths(fileStatuses);
			NUnit.Framework.Assert.AreEqual(2, paths.Length);
			NUnit.Framework.Assert.AreEqual(paths[0], path1);
			NUnit.Framework.Assert.AreEqual(paths[1], path2);
		}

		public virtual void testStat2Paths2()
		{
			org.apache.hadoop.fs.Path defaultPath = new org.apache.hadoop.fs.Path("file://default"
				);
			org.apache.hadoop.fs.Path[] paths = org.apache.hadoop.fs.FileUtil.stat2Paths(null
				, defaultPath);
			NUnit.Framework.Assert.AreEqual(1, paths.Length);
			NUnit.Framework.Assert.AreEqual(defaultPath, paths[0]);
			paths = org.apache.hadoop.fs.FileUtil.stat2Paths(null, null);
			NUnit.Framework.Assert.IsTrue(paths != null);
			NUnit.Framework.Assert.AreEqual(1, paths.Length);
			NUnit.Framework.Assert.AreEqual(null, paths[0]);
			org.apache.hadoop.fs.Path path1 = new org.apache.hadoop.fs.Path("file://foo");
			org.apache.hadoop.fs.Path path2 = new org.apache.hadoop.fs.Path("file://moo");
			org.apache.hadoop.fs.FileStatus[] fileStatuses = new org.apache.hadoop.fs.FileStatus
				[] { new org.apache.hadoop.fs.FileStatus(3, false, 0, 0, 0, path1), new org.apache.hadoop.fs.FileStatus
				(3, false, 0, 0, 0, path2) };
			paths = org.apache.hadoop.fs.FileUtil.stat2Paths(fileStatuses, defaultPath);
			NUnit.Framework.Assert.AreEqual(2, paths.Length);
			NUnit.Framework.Assert.AreEqual(paths[0], path1);
			NUnit.Framework.Assert.AreEqual(paths[1], path2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testSymlink()
		{
			NUnit.Framework.Assert.IsFalse(del.exists());
			del.mkdirs();
			byte[] data = Sharpen.Runtime.getBytesForString("testSymLink");
			java.io.File file = new java.io.File(del, FILE);
			java.io.File link = new java.io.File(del, "_link");
			//write some data to the file
			java.io.FileOutputStream os = new java.io.FileOutputStream(file);
			os.write(data);
			os.close();
			//create the symlink
			org.apache.hadoop.fs.FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath
				());
			//ensure that symlink length is correctly reported by Java
			NUnit.Framework.Assert.AreEqual(data.Length, file.length());
			NUnit.Framework.Assert.AreEqual(data.Length, link.length());
			//ensure that we can read from link.
			java.io.FileInputStream @in = new java.io.FileInputStream(link);
			long len = 0;
			while (@in.read() > 0)
			{
				len++;
			}
			@in.close();
			NUnit.Framework.Assert.AreEqual(data.Length, len);
		}

		/// <summary>Test that rename on a symlink works as expected.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testSymlinkRenameTo()
		{
			NUnit.Framework.Assert.IsFalse(del.exists());
			del.mkdirs();
			java.io.File file = new java.io.File(del, FILE);
			file.createNewFile();
			java.io.File link = new java.io.File(del, "_link");
			// create the symlink
			org.apache.hadoop.fs.FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath
				());
			NUnit.Framework.Assert.IsTrue(file.exists());
			NUnit.Framework.Assert.IsTrue(link.exists());
			java.io.File link2 = new java.io.File(del, "_link2");
			// Rename the symlink
			NUnit.Framework.Assert.IsTrue(link.renameTo(link2));
			// Make sure the file still exists
			// (NOTE: this would fail on Java6 on Windows if we didn't
			// copy the file in FileUtil#symlink)
			NUnit.Framework.Assert.IsTrue(file.exists());
			NUnit.Framework.Assert.IsTrue(link2.exists());
			NUnit.Framework.Assert.IsFalse(link.exists());
		}

		/// <summary>Test that deletion of a symlink works as expected.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testSymlinkDelete()
		{
			NUnit.Framework.Assert.IsFalse(del.exists());
			del.mkdirs();
			java.io.File file = new java.io.File(del, FILE);
			file.createNewFile();
			java.io.File link = new java.io.File(del, "_link");
			// create the symlink
			org.apache.hadoop.fs.FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath
				());
			NUnit.Framework.Assert.IsTrue(file.exists());
			NUnit.Framework.Assert.IsTrue(link.exists());
			// make sure that deleting a symlink works properly
			NUnit.Framework.Assert.IsTrue(link.delete());
			NUnit.Framework.Assert.IsFalse(link.exists());
			NUnit.Framework.Assert.IsTrue(file.exists());
		}

		/// <summary>Test that length on a symlink works as expected.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testSymlinkLength()
		{
			NUnit.Framework.Assert.IsFalse(del.exists());
			del.mkdirs();
			byte[] data = Sharpen.Runtime.getBytesForString("testSymLinkData");
			java.io.File file = new java.io.File(del, FILE);
			java.io.File link = new java.io.File(del, "_link");
			// write some data to the file
			java.io.FileOutputStream os = new java.io.FileOutputStream(file);
			os.write(data);
			os.close();
			NUnit.Framework.Assert.AreEqual(0, link.length());
			// create the symlink
			org.apache.hadoop.fs.FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath
				());
			// ensure that File#length returns the target file and link size
			NUnit.Framework.Assert.AreEqual(data.Length, file.length());
			NUnit.Framework.Assert.AreEqual(data.Length, link.length());
			file.delete();
			NUnit.Framework.Assert.IsFalse(file.exists());
			if (org.apache.hadoop.util.Shell.WINDOWS && !org.apache.hadoop.util.Shell.isJava7OrAbove
				())
			{
				// On Java6 on Windows, we copied the file
				NUnit.Framework.Assert.AreEqual(data.Length, link.length());
			}
			else
			{
				// Otherwise, the target file size is zero
				NUnit.Framework.Assert.AreEqual(0, link.length());
			}
			link.delete();
			NUnit.Framework.Assert.IsFalse(link.exists());
		}

		/// <exception cref="System.IO.IOException"/>
		private void doUntarAndVerify(java.io.File tarFile, java.io.File untarDir)
		{
			if (untarDir.exists() && !org.apache.hadoop.fs.FileUtil.fullyDelete(untarDir))
			{
				throw new System.IO.IOException("Could not delete directory '" + untarDir + "'");
			}
			org.apache.hadoop.fs.FileUtil.unTar(tarFile, untarDir);
			string parentDir = untarDir.getCanonicalPath() + org.apache.hadoop.fs.Path.SEPARATOR
				 + "name";
			java.io.File testFile = new java.io.File(parentDir + org.apache.hadoop.fs.Path.SEPARATOR
				 + "version");
			NUnit.Framework.Assert.IsTrue(testFile.exists());
			NUnit.Framework.Assert.IsTrue(testFile.length() == 0);
			string imageDir = parentDir + org.apache.hadoop.fs.Path.SEPARATOR + "image";
			testFile = new java.io.File(imageDir + org.apache.hadoop.fs.Path.SEPARATOR + "fsimage"
				);
			NUnit.Framework.Assert.IsTrue(testFile.exists());
			NUnit.Framework.Assert.IsTrue(testFile.length() == 157);
			string currentDir = parentDir + org.apache.hadoop.fs.Path.SEPARATOR + "current";
			testFile = new java.io.File(currentDir + org.apache.hadoop.fs.Path.SEPARATOR + "fsimage"
				);
			NUnit.Framework.Assert.IsTrue(testFile.exists());
			NUnit.Framework.Assert.IsTrue(testFile.length() == 4331);
			testFile = new java.io.File(currentDir + org.apache.hadoop.fs.Path.SEPARATOR + "edits"
				);
			NUnit.Framework.Assert.IsTrue(testFile.exists());
			NUnit.Framework.Assert.IsTrue(testFile.length() == 1033);
			testFile = new java.io.File(currentDir + org.apache.hadoop.fs.Path.SEPARATOR + "fstime"
				);
			NUnit.Framework.Assert.IsTrue(testFile.exists());
			NUnit.Framework.Assert.IsTrue(testFile.length() == 8);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testUntar()
		{
			string tarGzFileName = Sharpen.Runtime.getProperty("test.cache.data", "build/test/cache"
				) + "/test-untar.tgz";
			string tarFileName = Sharpen.Runtime.getProperty("test.cache.data", "build/test/cache"
				) + "/test-untar.tar";
			string dataDir = Sharpen.Runtime.getProperty("test.build.data", "build/test/data"
				);
			java.io.File untarDir = new java.io.File(dataDir, "untarDir");
			doUntarAndVerify(new java.io.File(tarGzFileName), untarDir);
			doUntarAndVerify(new java.io.File(tarFileName), untarDir);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testCreateJarWithClassPath()
		{
			// setup test directory for files
			NUnit.Framework.Assert.IsFalse(tmp.exists());
			NUnit.Framework.Assert.IsTrue(tmp.mkdirs());
			// create files expected to match a wildcard
			System.Collections.Generic.IList<java.io.File> wildcardMatches = java.util.Arrays
				.asList(new java.io.File(tmp, "wildcard1.jar"), new java.io.File(tmp, "wildcard2.jar"
				), new java.io.File(tmp, "wildcard3.JAR"), new java.io.File(tmp, "wildcard4.JAR"
				));
			foreach (java.io.File wildcardMatch in wildcardMatches)
			{
				NUnit.Framework.Assert.IsTrue("failure creating file: " + wildcardMatch, wildcardMatch
					.createNewFile());
			}
			// create non-jar files, which we expect to not be included in the classpath
			NUnit.Framework.Assert.IsTrue(new java.io.File(tmp, "text.txt").createNewFile());
			NUnit.Framework.Assert.IsTrue(new java.io.File(tmp, "executable.exe").createNewFile
				());
			NUnit.Framework.Assert.IsTrue(new java.io.File(tmp, "README").createNewFile());
			// create classpath jar
			string wildcardPath = tmp.getCanonicalPath() + java.io.File.separator + "*";
			string nonExistentSubdir = tmp.getCanonicalPath() + org.apache.hadoop.fs.Path.SEPARATOR
				 + "subdir" + org.apache.hadoop.fs.Path.SEPARATOR;
			System.Collections.Generic.IList<string> classPaths = java.util.Arrays.asList(string.Empty
				, "cp1.jar", "cp2.jar", wildcardPath, "cp3.jar", nonExistentSubdir);
			string inputClassPath = org.apache.hadoop.util.StringUtils.join(java.io.File.pathSeparator
				, classPaths);
			string[] jarCp = org.apache.hadoop.fs.FileUtil.createJarWithClassPath(inputClassPath
				 + java.io.File.pathSeparator + "unexpandedwildcard/*", new org.apache.hadoop.fs.Path
				(tmp.getCanonicalPath()), Sharpen.Runtime.getenv());
			string classPathJar = jarCp[0];
			NUnit.Framework.Assert.assertNotEquals("Unexpanded wildcard was not placed in extra classpath"
				, jarCp[1].IndexOf("unexpanded"), -1);
			// verify classpath by reading manifest from jar file
			java.util.jar.JarFile jarFile = null;
			try
			{
				jarFile = new java.util.jar.JarFile(classPathJar);
				java.util.jar.Manifest jarManifest = jarFile.getManifest();
				NUnit.Framework.Assert.IsNotNull(jarManifest);
				java.util.jar.Attributes mainAttributes = jarManifest.getMainAttributes();
				NUnit.Framework.Assert.IsNotNull(mainAttributes);
				NUnit.Framework.Assert.IsTrue(mainAttributes.Contains(java.util.jar.Attributes.Name
					.CLASS_PATH));
				string classPathAttr = mainAttributes.getValue(java.util.jar.Attributes.Name.CLASS_PATH
					);
				NUnit.Framework.Assert.IsNotNull(classPathAttr);
				System.Collections.Generic.IList<string> expectedClassPaths = new System.Collections.Generic.List
					<string>();
				foreach (string classPath in classPaths)
				{
					if (classPath.Length == 0)
					{
						continue;
					}
					if (wildcardPath.Equals(classPath))
					{
						// add wildcard matches
						foreach (java.io.File wildcardMatch_1 in wildcardMatches)
						{
							expectedClassPaths.add(wildcardMatch_1.toURI().toURL().toExternalForm());
						}
					}
					else
					{
						java.io.File fileCp = null;
						if (!new org.apache.hadoop.fs.Path(classPath).isAbsolute())
						{
							fileCp = new java.io.File(tmp, classPath);
						}
						else
						{
							fileCp = new java.io.File(classPath);
						}
						if (nonExistentSubdir.Equals(classPath))
						{
							// expect to maintain trailing path separator if present in input, even
							// if directory doesn't exist yet
							expectedClassPaths.add(fileCp.toURI().toURL().toExternalForm() + org.apache.hadoop.fs.Path
								.SEPARATOR);
						}
						else
						{
							expectedClassPaths.add(fileCp.toURI().toURL().toExternalForm());
						}
					}
				}
				System.Collections.Generic.IList<string> actualClassPaths = java.util.Arrays.asList
					(classPathAttr.split(" "));
				expectedClassPaths.Sort();
				actualClassPaths.Sort();
				NUnit.Framework.Assert.AreEqual(expectedClassPaths, actualClassPaths);
			}
			finally
			{
				if (jarFile != null)
				{
					try
					{
						jarFile.close();
					}
					catch (System.IO.IOException e)
					{
						LOG.warn("exception closing jarFile: " + classPathJar, e);
					}
				}
			}
		}

		public TestFileUtil()
		{
			dir1 = new java.io.File(del, DIR + "1");
			dir2 = new java.io.File(del, DIR + "2");
			xSubDir = new java.io.File(del, "xSubDir");
			xSubSubDir = new java.io.File(xSubDir, "xSubSubDir");
			ySubDir = new java.io.File(del, "ySubDir");
			file2 = new java.io.File(xSubDir, "file2");
			file22 = new java.io.File(xSubSubDir, "file22");
			file3 = new java.io.File(ySubDir, "file3");
			zlink = new java.io.File(del, "zlink");
		}
	}
}
