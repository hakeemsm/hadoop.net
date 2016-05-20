using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// <p>
	/// A collection of tests for the contract of the
	/// <see cref="FileSystem"/>
	/// .
	/// This test should be used for general-purpose implementations of
	/// <see cref="FileSystem"/>
	/// , that is, implementations that provide implementations
	/// of all of the functionality of
	/// <see cref="FileSystem"/>
	/// .
	/// </p>
	/// <p>
	/// To test a given
	/// <see cref="FileSystem"/>
	/// implementation create a subclass of this
	/// test and override
	/// <see cref="NUnit.Framework.TestCase.setUp()"/>
	/// to initialize the <code>fs</code>
	/// <see cref="FileSystem"/>
	/// instance variable.
	/// </p>
	/// </summary>
	public abstract class FileSystemContractBaseTest : NUnit.Framework.TestCase
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystemContractBaseTest
			)));

		protected internal const string TEST_UMASK = "062";

		protected internal org.apache.hadoop.fs.FileSystem fs;

		protected internal byte[] data;

		/// <exception cref="System.Exception"/>
		protected override void tearDown()
		{
			fs.delete(path("/test"), true);
		}

		protected internal virtual int getBlockSize()
		{
			return 1024;
		}

		protected internal virtual string getDefaultWorkingDirectory()
		{
			return "/user/" + Sharpen.Runtime.getProperty("user.name");
		}

		protected internal virtual bool renameSupported()
		{
			return true;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testFsStatus()
		{
			org.apache.hadoop.fs.FsStatus fsStatus = fs.getStatus();
			NUnit.Framework.Assert.IsNotNull(fsStatus);
			//used, free and capacity are non-negative longs
			NUnit.Framework.Assert.IsTrue(fsStatus.getUsed() >= 0);
			NUnit.Framework.Assert.IsTrue(fsStatus.getRemaining() >= 0);
			NUnit.Framework.Assert.IsTrue(fsStatus.getCapacity() >= 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testWorkingDirectory()
		{
			org.apache.hadoop.fs.Path workDir = path(getDefaultWorkingDirectory());
			NUnit.Framework.Assert.AreEqual(workDir, fs.getWorkingDirectory());
			fs.setWorkingDirectory(path("."));
			NUnit.Framework.Assert.AreEqual(workDir, fs.getWorkingDirectory());
			fs.setWorkingDirectory(path(".."));
			NUnit.Framework.Assert.AreEqual(workDir.getParent(), fs.getWorkingDirectory());
			org.apache.hadoop.fs.Path relativeDir = path("hadoop");
			fs.setWorkingDirectory(relativeDir);
			NUnit.Framework.Assert.AreEqual(relativeDir, fs.getWorkingDirectory());
			org.apache.hadoop.fs.Path absoluteDir = path("/test/hadoop");
			fs.setWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fs.getWorkingDirectory());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testMkdirs()
		{
			org.apache.hadoop.fs.Path testDir = path("/test/hadoop");
			NUnit.Framework.Assert.IsFalse(fs.exists(testDir));
			NUnit.Framework.Assert.IsFalse(fs.isFile(testDir));
			NUnit.Framework.Assert.IsTrue(fs.mkdirs(testDir));
			NUnit.Framework.Assert.IsTrue(fs.exists(testDir));
			NUnit.Framework.Assert.IsFalse(fs.isFile(testDir));
			NUnit.Framework.Assert.IsTrue(fs.mkdirs(testDir));
			NUnit.Framework.Assert.IsTrue(fs.exists(testDir));
			NUnit.Framework.Assert.IsFalse(fs.isFile(testDir));
			org.apache.hadoop.fs.Path parentDir = testDir.getParent();
			NUnit.Framework.Assert.IsTrue(fs.exists(parentDir));
			NUnit.Framework.Assert.IsFalse(fs.isFile(parentDir));
			org.apache.hadoop.fs.Path grandparentDir = parentDir.getParent();
			NUnit.Framework.Assert.IsTrue(fs.exists(grandparentDir));
			NUnit.Framework.Assert.IsFalse(fs.isFile(grandparentDir));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testMkdirsFailsForSubdirectoryOfExistingFile()
		{
			org.apache.hadoop.fs.Path testDir = path("/test/hadoop");
			NUnit.Framework.Assert.IsFalse(fs.exists(testDir));
			NUnit.Framework.Assert.IsTrue(fs.mkdirs(testDir));
			NUnit.Framework.Assert.IsTrue(fs.exists(testDir));
			createFile(path("/test/hadoop/file"));
			org.apache.hadoop.fs.Path testSubDir = path("/test/hadoop/file/subdir");
			try
			{
				fs.mkdirs(testSubDir);
				fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(fs.exists(testSubDir));
			org.apache.hadoop.fs.Path testDeepSubDir = path("/test/hadoop/file/deep/sub/dir");
			try
			{
				fs.mkdirs(testDeepSubDir);
				fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(fs.exists(testDeepSubDir));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testMkdirsWithUmask()
		{
			if (fs.getScheme().Equals("s3") || fs.getScheme().Equals("s3n"))
			{
				// skip permission tests for S3FileSystem until HDFS-1333 is fixed.
				return;
			}
			org.apache.hadoop.conf.Configuration conf = fs.getConf();
			string oldUmask = conf.get(org.apache.hadoop.fs.CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY
				);
			try
			{
				conf.set(org.apache.hadoop.fs.CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, TEST_UMASK
					);
				org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path("/test/newDir");
				NUnit.Framework.Assert.IsTrue(fs.mkdirs(dir, new org.apache.hadoop.fs.permission.FsPermission
					((short)0x1ff)));
				org.apache.hadoop.fs.FileStatus status = fs.getFileStatus(dir);
				NUnit.Framework.Assert.IsTrue(status.isDirectory());
				NUnit.Framework.Assert.AreEqual((short)0x1cd, status.getPermission().toShort());
			}
			finally
			{
				conf.set(org.apache.hadoop.fs.CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, oldUmask
					);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGetFileStatusThrowsExceptionForNonExistentFile()
		{
			try
			{
				fs.getFileStatus(path("/test/hadoop/file"));
				fail("Should throw FileNotFoundException");
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
				fs.listStatus(path("/test/hadoop/file"));
				fail("Should throw FileNotFoundException");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		public virtual void testListStatus()
		{
			org.apache.hadoop.fs.Path[] testDirs = new org.apache.hadoop.fs.Path[] { path("/test/hadoop/a"
				), path("/test/hadoop/b"), path("/test/hadoop/c/1") };
			NUnit.Framework.Assert.IsFalse(fs.exists(testDirs[0]));
			foreach (org.apache.hadoop.fs.Path path in testDirs)
			{
				NUnit.Framework.Assert.IsTrue(fs.mkdirs(path));
			}
			org.apache.hadoop.fs.FileStatus[] paths = fs.listStatus(path("/test"));
			NUnit.Framework.Assert.AreEqual(1, paths.Length);
			NUnit.Framework.Assert.AreEqual(path("/test/hadoop"), paths[0].getPath());
			paths = fs.listStatus(path("/test/hadoop"));
			NUnit.Framework.Assert.AreEqual(3, paths.Length);
			NUnit.Framework.Assert.AreEqual(path("/test/hadoop/a"), paths[0].getPath());
			NUnit.Framework.Assert.AreEqual(path("/test/hadoop/b"), paths[1].getPath());
			NUnit.Framework.Assert.AreEqual(path("/test/hadoop/c"), paths[2].getPath());
			paths = fs.listStatus(path("/test/hadoop/a"));
			NUnit.Framework.Assert.AreEqual(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testWriteReadAndDeleteEmptyFile()
		{
			writeReadAndDelete(0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testWriteReadAndDeleteHalfABlock()
		{
			writeReadAndDelete(getBlockSize() / 2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testWriteReadAndDeleteOneBlock()
		{
			writeReadAndDelete(getBlockSize());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testWriteReadAndDeleteOneAndAHalfBlocks()
		{
			writeReadAndDelete(getBlockSize() + (getBlockSize() / 2));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testWriteReadAndDeleteTwoBlocks()
		{
			writeReadAndDelete(getBlockSize() * 2);
		}

		/// <summary>Write a dataset, read it back in and verify that they match.</summary>
		/// <remarks>
		/// Write a dataset, read it back in and verify that they match.
		/// Afterwards, the file is deleted.
		/// </remarks>
		/// <param name="len">length of data</param>
		/// <exception cref="System.IO.IOException">on IO failures</exception>
		protected internal virtual void writeReadAndDelete(int len)
		{
			org.apache.hadoop.fs.Path path = path("/test/hadoop/file");
			writeAndRead(path, data, len, false, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testOverwrite()
		{
			org.apache.hadoop.fs.Path path = path("/test/hadoop/file");
			fs.mkdirs(path.getParent());
			createFile(path);
			NUnit.Framework.Assert.IsTrue("Exists", fs.exists(path));
			NUnit.Framework.Assert.AreEqual("Length", data.Length, fs.getFileStatus(path).getLen
				());
			try
			{
				fs.create(path, false).close();
				fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// Expected
			org.apache.hadoop.fs.FSDataOutputStream @out = fs.create(path, true);
			@out.write(data, 0, data.Length);
			@out.close();
			NUnit.Framework.Assert.IsTrue("Exists", fs.exists(path));
			NUnit.Framework.Assert.AreEqual("Length", data.Length, fs.getFileStatus(path).getLen
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testWriteInNonExistentDirectory()
		{
			org.apache.hadoop.fs.Path path = path("/test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Parent exists", fs.exists(path.getParent()));
			createFile(path);
			NUnit.Framework.Assert.IsTrue("Exists", fs.exists(path));
			NUnit.Framework.Assert.AreEqual("Length", data.Length, fs.getFileStatus(path).getLen
				());
			NUnit.Framework.Assert.IsTrue("Parent exists", fs.exists(path.getParent()));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testDeleteNonExistentFile()
		{
			org.apache.hadoop.fs.Path path = path("/test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Path exists: " + path, fs.exists(path));
			NUnit.Framework.Assert.IsFalse("No deletion", fs.delete(path, true));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testDeleteRecursively()
		{
			org.apache.hadoop.fs.Path dir = path("/test/hadoop");
			org.apache.hadoop.fs.Path file = path("/test/hadoop/file");
			org.apache.hadoop.fs.Path subdir = path("/test/hadoop/subdir");
			createFile(file);
			NUnit.Framework.Assert.IsTrue("Created subdir", fs.mkdirs(subdir));
			NUnit.Framework.Assert.IsTrue("File exists", fs.exists(file));
			NUnit.Framework.Assert.IsTrue("Dir exists", fs.exists(dir));
			NUnit.Framework.Assert.IsTrue("Subdir exists", fs.exists(subdir));
			try
			{
				fs.delete(dir, false);
				fail("Should throw IOException.");
			}
			catch (System.IO.IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsTrue("File still exists", fs.exists(file));
			NUnit.Framework.Assert.IsTrue("Dir still exists", fs.exists(dir));
			NUnit.Framework.Assert.IsTrue("Subdir still exists", fs.exists(subdir));
			NUnit.Framework.Assert.IsTrue("Deleted", fs.delete(dir, true));
			NUnit.Framework.Assert.IsFalse("File doesn't exist", fs.exists(file));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", fs.exists(dir));
			NUnit.Framework.Assert.IsFalse("Subdir doesn't exist", fs.exists(subdir));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testDeleteEmptyDirectory()
		{
			org.apache.hadoop.fs.Path dir = path("/test/hadoop");
			NUnit.Framework.Assert.IsTrue(fs.mkdirs(dir));
			NUnit.Framework.Assert.IsTrue("Dir exists", fs.exists(dir));
			NUnit.Framework.Assert.IsTrue("Deleted", fs.delete(dir, false));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", fs.exists(dir));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRenameNonExistentPath()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = path("/test/hadoop/path");
			org.apache.hadoop.fs.Path dst = path("/test/new/newpath");
			rename(src, dst, false, false, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRenameFileMoveToNonExistentDirectory()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = path("/test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = path("/test/new/newfile");
			rename(src, dst, false, true, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRenameFileMoveToExistingDirectory()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = path("/test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = path("/test/new/newfile");
			fs.mkdirs(dst.getParent());
			rename(src, dst, true, false, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRenameFileAsExistingFile()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = path("/test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = path("/test/new/newfile");
			createFile(dst);
			rename(src, dst, false, true, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRenameFileAsExistingDirectory()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = path("/test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.Path dst = path("/test/new/newdir");
			fs.mkdirs(dst);
			rename(src, dst, true, false, true);
			NUnit.Framework.Assert.IsTrue("Destination changed", fs.exists(path("/test/new/newdir/file"
				)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRenameDirectoryMoveToNonExistentDirectory()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = path("/test/hadoop/dir");
			fs.mkdirs(src);
			org.apache.hadoop.fs.Path dst = path("/test/new/newdir");
			rename(src, dst, false, true, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRenameDirectoryMoveToExistingDirectory()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = path("/test/hadoop/dir");
			fs.mkdirs(src);
			createFile(path("/test/hadoop/dir/file1"));
			createFile(path("/test/hadoop/dir/subdir/file2"));
			org.apache.hadoop.fs.Path dst = path("/test/new/newdir");
			fs.mkdirs(dst.getParent());
			rename(src, dst, true, false, true);
			NUnit.Framework.Assert.IsFalse("Nested file1 exists", fs.exists(path("/test/hadoop/dir/file1"
				)));
			NUnit.Framework.Assert.IsFalse("Nested file2 exists", fs.exists(path("/test/hadoop/dir/subdir/file2"
				)));
			NUnit.Framework.Assert.IsTrue("Renamed nested file1 exists", fs.exists(path("/test/new/newdir/file1"
				)));
			NUnit.Framework.Assert.IsTrue("Renamed nested exists", fs.exists(path("/test/new/newdir/subdir/file2"
				)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRenameDirectoryAsExistingFile()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = path("/test/hadoop/dir");
			fs.mkdirs(src);
			org.apache.hadoop.fs.Path dst = path("/test/new/newfile");
			createFile(dst);
			rename(src, dst, false, true, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRenameDirectoryAsExistingDirectory()
		{
			if (!renameSupported())
			{
				return;
			}
			org.apache.hadoop.fs.Path src = path("/test/hadoop/dir");
			fs.mkdirs(src);
			createFile(path("/test/hadoop/dir/file1"));
			createFile(path("/test/hadoop/dir/subdir/file2"));
			org.apache.hadoop.fs.Path dst = path("/test/new/newdir");
			fs.mkdirs(dst);
			rename(src, dst, true, false, true);
			NUnit.Framework.Assert.IsTrue("Destination changed", fs.exists(path("/test/new/newdir/dir"
				)));
			NUnit.Framework.Assert.IsFalse("Nested file1 exists", fs.exists(path("/test/hadoop/dir/file1"
				)));
			NUnit.Framework.Assert.IsFalse("Nested file2 exists", fs.exists(path("/test/hadoop/dir/subdir/file2"
				)));
			NUnit.Framework.Assert.IsTrue("Renamed nested file1 exists", fs.exists(path("/test/new/newdir/dir/file1"
				)));
			NUnit.Framework.Assert.IsTrue("Renamed nested exists", fs.exists(path("/test/new/newdir/dir/subdir/file2"
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInputStreamClosedTwice()
		{
			//HADOOP-4760 according to Closeable#close() closing already-closed 
			//streams should have no effect. 
			org.apache.hadoop.fs.Path src = path("/test/hadoop/file");
			createFile(src);
			org.apache.hadoop.fs.FSDataInputStream @in = fs.open(src);
			@in.close();
			@in.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testOutputStreamClosedTwice()
		{
			//HADOOP-4760 according to Closeable#close() closing already-closed 
			//streams should have no effect. 
			org.apache.hadoop.fs.Path src = path("/test/hadoop/file");
			org.apache.hadoop.fs.FSDataOutputStream @out = fs.create(src);
			@out.writeChar('H');
			//write some data
			@out.close();
			@out.close();
		}

		protected internal virtual org.apache.hadoop.fs.Path path(string pathString)
		{
			return new org.apache.hadoop.fs.Path(pathString).makeQualified(fs);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void createFile(org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = fs.create(path);
			@out.write(data, 0, data.Length);
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst, bool renameSucceeded, bool srcExists, bool dstExists)
		{
			NUnit.Framework.Assert.AreEqual("Rename result", renameSucceeded, fs.rename(src, 
				dst));
			NUnit.Framework.Assert.AreEqual("Source exists", srcExists, fs.exists(src));
			NUnit.Framework.Assert.AreEqual("Destination exists" + dst, dstExists, fs.exists(
				dst));
		}

		/// <summary>
		/// Verify that if you take an existing file and overwrite it, the new values
		/// get picked up.
		/// </summary>
		/// <remarks>
		/// Verify that if you take an existing file and overwrite it, the new values
		/// get picked up.
		/// This is a test for the behavior of eventually consistent
		/// filesystems.
		/// </remarks>
		/// <exception cref="System.Exception">on any failure</exception>
		public virtual void testOverWriteAndRead()
		{
			int blockSize = getBlockSize();
			byte[] filedata1 = dataset(blockSize * 2, 'A', 26);
			byte[] filedata2 = dataset(blockSize * 2, 'a', 26);
			org.apache.hadoop.fs.Path path = path("/test/hadoop/file-overwrite");
			writeAndRead(path, filedata1, blockSize, true, false);
			writeAndRead(path, filedata2, blockSize, true, false);
			writeAndRead(path, filedata1, blockSize * 2, true, false);
			writeAndRead(path, filedata2, blockSize * 2, true, false);
			writeAndRead(path, filedata1, blockSize, true, false);
			writeAndRead(path, filedata2, blockSize * 2, true, false);
		}

		/// <summary>Write a file and read it in, validating the result.</summary>
		/// <remarks>
		/// Write a file and read it in, validating the result. Optional flags control
		/// whether file overwrite operations should be enabled, and whether the
		/// file should be deleted afterwards.
		/// If there is a mismatch between what was written and what was expected,
		/// a small range of bytes either side of the first error are logged to aid
		/// diagnosing what problem occurred -whether it was a previous file
		/// or a corrupting of the current file. This assumes that two
		/// sequential runs to the same path use datasets with different character
		/// moduli.
		/// </remarks>
		/// <param name="path">path to write to</param>
		/// <param name="len">length of data</param>
		/// <param name="overwrite">should the create option allow overwrites?</param>
		/// <param name="delete">
		/// should the file be deleted afterwards? -with a verification
		/// that it worked. Deletion is not attempted if an assertion has failed
		/// earlier -it is not in a <code>finally{}</code> block.
		/// </param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		protected internal virtual void writeAndRead(org.apache.hadoop.fs.Path path, byte
			[] src, int len, bool overwrite, bool delete)
		{
			NUnit.Framework.Assert.IsTrue("Not enough data in source array to write " + len +
				 " bytes", src.Length >= len);
			fs.mkdirs(path.getParent());
			org.apache.hadoop.fs.FSDataOutputStream @out = fs.create(path, overwrite, fs.getConf
				().getInt("io.file.buffer.size", 4096), (short)1, getBlockSize());
			@out.write(src, 0, len);
			@out.close();
			NUnit.Framework.Assert.IsTrue("Exists", fs.exists(path));
			NUnit.Framework.Assert.AreEqual("Length", len, fs.getFileStatus(path).getLen());
			org.apache.hadoop.fs.FSDataInputStream @in = fs.open(path);
			byte[] buf = new byte[len];
			@in.readFully(0, buf);
			@in.close();
			NUnit.Framework.Assert.AreEqual(len, buf.Length);
			int errors = 0;
			int first_error_byte = -1;
			for (int i = 0; i < len; i++)
			{
				if (src[i] != buf[i])
				{
					if (errors == 0)
					{
						first_error_byte = i;
					}
					errors++;
				}
			}
			if (errors > 0)
			{
				string message = string.format(" %d errors in file of length %d", errors, len);
				LOG.warn(message);
				// the range either side of the first error to print
				// this is a purely arbitrary number, to aid user debugging
				int overlap = 10;
				for (int i_1 = System.Math.max(0, first_error_byte - overlap); i_1 < System.Math.
					min(first_error_byte + overlap, len); i_1++)
				{
					byte actual = buf[i_1];
					byte expected = src[i_1];
					string letter = toChar(actual);
					string line = string.format("[%04d] %2x %s\n", i_1, actual, letter);
					if (expected != actual)
					{
						line = string.format("[%04d] %2x %s -expected %2x %s\n", i_1, actual, letter, expected
							, toChar(expected));
					}
					LOG.warn(line);
				}
				fail(message);
			}
			if (delete)
			{
				bool deleted = fs.delete(path, false);
				NUnit.Framework.Assert.IsTrue("Deleted", deleted);
				NUnit.Framework.Assert.IsFalse("No longer exists", fs.exists(path));
			}
		}

		/// <summary>Convert a byte to a character for printing.</summary>
		/// <remarks>
		/// Convert a byte to a character for printing. If the
		/// byte value is &lt; 32 -and hence unprintable- the byte is
		/// returned as a two digit hex value
		/// </remarks>
		/// <param name="b">byte</param>
		/// <returns>the printable character string</returns>
		protected internal virtual string toChar(byte b)
		{
			if (b >= unchecked((int)(0x20)))
			{
				return char.toString((char)b);
			}
			else
			{
				return string.format("%02x", b);
			}
		}

		/// <summary>
		/// Create a dataset for use in the tests; all data is in the range
		/// base to (base+modulo-1) inclusive
		/// </summary>
		/// <param name="len">length of data</param>
		/// <param name="base">base of the data</param>
		/// <param name="modulo">the modulo</param>
		/// <returns>the newly generated dataset</returns>
		protected internal virtual byte[] dataset(int len, int @base, int modulo)
		{
			byte[] dataset = new byte[len];
			for (int i = 0; i < len; i++)
			{
				dataset[i] = unchecked((byte)(@base + (i % modulo)));
			}
			return dataset;
		}

		public FileSystemContractBaseTest()
		{
			data = dataset(getBlockSize() * 2, 0, 255);
		}
	}
}
