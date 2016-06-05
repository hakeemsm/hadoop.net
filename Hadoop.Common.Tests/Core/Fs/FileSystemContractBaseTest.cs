using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.FS
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
	/// <see cref="NUnit.Framework.TestCase.SetUp()"/>
	/// to initialize the <code>fs</code>
	/// <see cref="FileSystem"/>
	/// instance variable.
	/// </p>
	/// </summary>
	public abstract class FileSystemContractBaseTest : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(FileSystemContractBaseTest
			));

		protected internal const string TestUmask = "062";

		protected internal FileSystem fs;

		protected internal byte[] data;

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			fs.Delete(Path("/test"), true);
		}

		protected internal virtual int GetBlockSize()
		{
			return 1024;
		}

		protected internal virtual string GetDefaultWorkingDirectory()
		{
			return "/user/" + Runtime.GetProperty("user.name");
		}

		protected internal virtual bool RenameSupported()
		{
			return true;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFsStatus()
		{
			FsStatus fsStatus = fs.GetStatus();
			NUnit.Framework.Assert.IsNotNull(fsStatus);
			//used, free and capacity are non-negative longs
			Assert.True(fsStatus.GetUsed() >= 0);
			Assert.True(fsStatus.GetRemaining() >= 0);
			Assert.True(fsStatus.GetCapacity() >= 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWorkingDirectory()
		{
			Org.Apache.Hadoop.FS.Path workDir = Path(GetDefaultWorkingDirectory());
			Assert.Equal(workDir, fs.GetWorkingDirectory());
			fs.SetWorkingDirectory(Path("."));
			Assert.Equal(workDir, fs.GetWorkingDirectory());
			fs.SetWorkingDirectory(Path(".."));
			Assert.Equal(workDir.GetParent(), fs.GetWorkingDirectory());
			Org.Apache.Hadoop.FS.Path relativeDir = Path("hadoop");
			fs.SetWorkingDirectory(relativeDir);
			Assert.Equal(relativeDir, fs.GetWorkingDirectory());
			Org.Apache.Hadoop.FS.Path absoluteDir = Path("/test/hadoop");
			fs.SetWorkingDirectory(absoluteDir);
			Assert.Equal(absoluteDir, fs.GetWorkingDirectory());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMkdirs()
		{
			Org.Apache.Hadoop.FS.Path testDir = Path("/test/hadoop");
			NUnit.Framework.Assert.IsFalse(fs.Exists(testDir));
			NUnit.Framework.Assert.IsFalse(fs.IsFile(testDir));
			Assert.True(fs.Mkdirs(testDir));
			Assert.True(fs.Exists(testDir));
			NUnit.Framework.Assert.IsFalse(fs.IsFile(testDir));
			Assert.True(fs.Mkdirs(testDir));
			Assert.True(fs.Exists(testDir));
			NUnit.Framework.Assert.IsFalse(fs.IsFile(testDir));
			Org.Apache.Hadoop.FS.Path parentDir = testDir.GetParent();
			Assert.True(fs.Exists(parentDir));
			NUnit.Framework.Assert.IsFalse(fs.IsFile(parentDir));
			Org.Apache.Hadoop.FS.Path grandparentDir = parentDir.GetParent();
			Assert.True(fs.Exists(grandparentDir));
			NUnit.Framework.Assert.IsFalse(fs.IsFile(grandparentDir));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMkdirsFailsForSubdirectoryOfExistingFile()
		{
			Org.Apache.Hadoop.FS.Path testDir = Path("/test/hadoop");
			NUnit.Framework.Assert.IsFalse(fs.Exists(testDir));
			Assert.True(fs.Mkdirs(testDir));
			Assert.True(fs.Exists(testDir));
			CreateFile(Path("/test/hadoop/file"));
			Org.Apache.Hadoop.FS.Path testSubDir = Path("/test/hadoop/file/subdir");
			try
			{
				fs.Mkdirs(testSubDir);
				Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(fs.Exists(testSubDir));
			Org.Apache.Hadoop.FS.Path testDeepSubDir = Path("/test/hadoop/file/deep/sub/dir");
			try
			{
				fs.Mkdirs(testDeepSubDir);
				Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse(fs.Exists(testDeepSubDir));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMkdirsWithUmask()
		{
			if (fs.GetScheme().Equals("s3") || fs.GetScheme().Equals("s3n"))
			{
				// skip permission tests for S3FileSystem until HDFS-1333 is fixed.
				return;
			}
			Configuration conf = fs.GetConf();
			string oldUmask = conf.Get(CommonConfigurationKeys.FsPermissionsUmaskKey);
			try
			{
				conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, TestUmask);
				Org.Apache.Hadoop.FS.Path dir = new Org.Apache.Hadoop.FS.Path("/test/newDir");
				Assert.True(fs.Mkdirs(dir, new FsPermission((short)0x1ff)));
				FileStatus status = fs.GetFileStatus(dir);
				Assert.True(status.IsDirectory());
				Assert.Equal((short)0x1cd, status.GetPermission().ToShort());
			}
			finally
			{
				conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, oldUmask);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetFileStatusThrowsExceptionForNonExistentFile()
		{
			try
			{
				fs.GetFileStatus(Path("/test/hadoop/file"));
				Fail("Should throw FileNotFoundException");
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
				fs.ListStatus(Path("/test/hadoop/file"));
				Fail("Should throw FileNotFoundException");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		public virtual void TestListStatus()
		{
			Org.Apache.Hadoop.FS.Path[] testDirs = new Org.Apache.Hadoop.FS.Path[] { Path("/test/hadoop/a"
				), Path("/test/hadoop/b"), Path("/test/hadoop/c/1") };
			NUnit.Framework.Assert.IsFalse(fs.Exists(testDirs[0]));
			foreach (Org.Apache.Hadoop.FS.Path path in testDirs)
			{
				Assert.True(fs.Mkdirs(path));
			}
			FileStatus[] paths = fs.ListStatus(Path("/test"));
			Assert.Equal(1, paths.Length);
			Assert.Equal(Path("/test/hadoop"), paths[0].GetPath());
			paths = fs.ListStatus(Path("/test/hadoop"));
			Assert.Equal(3, paths.Length);
			Assert.Equal(Path("/test/hadoop/a"), paths[0].GetPath());
			Assert.Equal(Path("/test/hadoop/b"), paths[1].GetPath());
			Assert.Equal(Path("/test/hadoop/c"), paths[2].GetPath());
			paths = fs.ListStatus(Path("/test/hadoop/a"));
			Assert.Equal(0, paths.Length);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWriteReadAndDeleteEmptyFile()
		{
			WriteReadAndDelete(0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWriteReadAndDeleteHalfABlock()
		{
			WriteReadAndDelete(GetBlockSize() / 2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWriteReadAndDeleteOneBlock()
		{
			WriteReadAndDelete(GetBlockSize());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWriteReadAndDeleteOneAndAHalfBlocks()
		{
			WriteReadAndDelete(GetBlockSize() + (GetBlockSize() / 2));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWriteReadAndDeleteTwoBlocks()
		{
			WriteReadAndDelete(GetBlockSize() * 2);
		}

		/// <summary>Write a dataset, read it back in and verify that they match.</summary>
		/// <remarks>
		/// Write a dataset, read it back in and verify that they match.
		/// Afterwards, the file is deleted.
		/// </remarks>
		/// <param name="len">length of data</param>
		/// <exception cref="System.IO.IOException">on IO failures</exception>
		protected internal virtual void WriteReadAndDelete(int len)
		{
			Org.Apache.Hadoop.FS.Path path = Path("/test/hadoop/file");
			WriteAndRead(path, data, len, false, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOverwrite()
		{
			Org.Apache.Hadoop.FS.Path path = Path("/test/hadoop/file");
			fs.Mkdirs(path.GetParent());
			CreateFile(path);
			Assert.True("Exists", fs.Exists(path));
			Assert.Equal("Length", data.Length, fs.GetFileStatus(path).GetLen
				());
			try
			{
				fs.Create(path, false).Close();
				Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// Expected
			FSDataOutputStream @out = fs.Create(path, true);
			@out.Write(data, 0, data.Length);
			@out.Close();
			Assert.True("Exists", fs.Exists(path));
			Assert.Equal("Length", data.Length, fs.GetFileStatus(path).GetLen
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWriteInNonExistentDirectory()
		{
			Org.Apache.Hadoop.FS.Path path = Path("/test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Parent exists", fs.Exists(path.GetParent()));
			CreateFile(path);
			Assert.True("Exists", fs.Exists(path));
			Assert.Equal("Length", data.Length, fs.GetFileStatus(path).GetLen
				());
			Assert.True("Parent exists", fs.Exists(path.GetParent()));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDeleteNonExistentFile()
		{
			Org.Apache.Hadoop.FS.Path path = Path("/test/hadoop/file");
			NUnit.Framework.Assert.IsFalse("Path exists: " + path, fs.Exists(path));
			NUnit.Framework.Assert.IsFalse("No deletion", fs.Delete(path, true));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDeleteRecursively()
		{
			Org.Apache.Hadoop.FS.Path dir = Path("/test/hadoop");
			Org.Apache.Hadoop.FS.Path file = Path("/test/hadoop/file");
			Org.Apache.Hadoop.FS.Path subdir = Path("/test/hadoop/subdir");
			CreateFile(file);
			Assert.True("Created subdir", fs.Mkdirs(subdir));
			Assert.True("File exists", fs.Exists(file));
			Assert.True("Dir exists", fs.Exists(dir));
			Assert.True("Subdir exists", fs.Exists(subdir));
			try
			{
				fs.Delete(dir, false);
				Fail("Should throw IOException.");
			}
			catch (IOException)
			{
			}
			// expected
			Assert.True("File still exists", fs.Exists(file));
			Assert.True("Dir still exists", fs.Exists(dir));
			Assert.True("Subdir still exists", fs.Exists(subdir));
			Assert.True("Deleted", fs.Delete(dir, true));
			NUnit.Framework.Assert.IsFalse("File doesn't exist", fs.Exists(file));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", fs.Exists(dir));
			NUnit.Framework.Assert.IsFalse("Subdir doesn't exist", fs.Exists(subdir));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDeleteEmptyDirectory()
		{
			Org.Apache.Hadoop.FS.Path dir = Path("/test/hadoop");
			Assert.True(fs.Mkdirs(dir));
			Assert.True("Dir exists", fs.Exists(dir));
			Assert.True("Deleted", fs.Delete(dir, false));
			NUnit.Framework.Assert.IsFalse("Dir doesn't exist", fs.Exists(dir));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameNonExistentPath()
		{
			if (!RenameSupported())
			{
				return;
			}
			Org.Apache.Hadoop.FS.Path src = Path("/test/hadoop/path");
			Org.Apache.Hadoop.FS.Path dst = Path("/test/new/newpath");
			Rename(src, dst, false, false, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFileMoveToNonExistentDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Org.Apache.Hadoop.FS.Path src = Path("/test/hadoop/file");
			CreateFile(src);
			Org.Apache.Hadoop.FS.Path dst = Path("/test/new/newfile");
			Rename(src, dst, false, true, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFileMoveToExistingDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Org.Apache.Hadoop.FS.Path src = Path("/test/hadoop/file");
			CreateFile(src);
			Org.Apache.Hadoop.FS.Path dst = Path("/test/new/newfile");
			fs.Mkdirs(dst.GetParent());
			Rename(src, dst, true, false, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFileAsExistingFile()
		{
			if (!RenameSupported())
			{
				return;
			}
			Org.Apache.Hadoop.FS.Path src = Path("/test/hadoop/file");
			CreateFile(src);
			Org.Apache.Hadoop.FS.Path dst = Path("/test/new/newfile");
			CreateFile(dst);
			Rename(src, dst, false, true, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFileAsExistingDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Org.Apache.Hadoop.FS.Path src = Path("/test/hadoop/file");
			CreateFile(src);
			Org.Apache.Hadoop.FS.Path dst = Path("/test/new/newdir");
			fs.Mkdirs(dst);
			Rename(src, dst, true, false, true);
			Assert.True("Destination changed", fs.Exists(Path("/test/new/newdir/file"
				)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameDirectoryMoveToNonExistentDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Org.Apache.Hadoop.FS.Path src = Path("/test/hadoop/dir");
			fs.Mkdirs(src);
			Org.Apache.Hadoop.FS.Path dst = Path("/test/new/newdir");
			Rename(src, dst, false, true, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameDirectoryMoveToExistingDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Org.Apache.Hadoop.FS.Path src = Path("/test/hadoop/dir");
			fs.Mkdirs(src);
			CreateFile(Path("/test/hadoop/dir/file1"));
			CreateFile(Path("/test/hadoop/dir/subdir/file2"));
			Org.Apache.Hadoop.FS.Path dst = Path("/test/new/newdir");
			fs.Mkdirs(dst.GetParent());
			Rename(src, dst, true, false, true);
			NUnit.Framework.Assert.IsFalse("Nested file1 exists", fs.Exists(Path("/test/hadoop/dir/file1"
				)));
			NUnit.Framework.Assert.IsFalse("Nested file2 exists", fs.Exists(Path("/test/hadoop/dir/subdir/file2"
				)));
			Assert.True("Renamed nested file1 exists", fs.Exists(Path("/test/new/newdir/file1"
				)));
			Assert.True("Renamed nested exists", fs.Exists(Path("/test/new/newdir/subdir/file2"
				)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameDirectoryAsExistingFile()
		{
			if (!RenameSupported())
			{
				return;
			}
			Org.Apache.Hadoop.FS.Path src = Path("/test/hadoop/dir");
			fs.Mkdirs(src);
			Org.Apache.Hadoop.FS.Path dst = Path("/test/new/newfile");
			CreateFile(dst);
			Rename(src, dst, false, true, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameDirectoryAsExistingDirectory()
		{
			if (!RenameSupported())
			{
				return;
			}
			Org.Apache.Hadoop.FS.Path src = Path("/test/hadoop/dir");
			fs.Mkdirs(src);
			CreateFile(Path("/test/hadoop/dir/file1"));
			CreateFile(Path("/test/hadoop/dir/subdir/file2"));
			Org.Apache.Hadoop.FS.Path dst = Path("/test/new/newdir");
			fs.Mkdirs(dst);
			Rename(src, dst, true, false, true);
			Assert.True("Destination changed", fs.Exists(Path("/test/new/newdir/dir"
				)));
			NUnit.Framework.Assert.IsFalse("Nested file1 exists", fs.Exists(Path("/test/hadoop/dir/file1"
				)));
			NUnit.Framework.Assert.IsFalse("Nested file2 exists", fs.Exists(Path("/test/hadoop/dir/subdir/file2"
				)));
			Assert.True("Renamed nested file1 exists", fs.Exists(Path("/test/new/newdir/dir/file1"
				)));
			Assert.True("Renamed nested exists", fs.Exists(Path("/test/new/newdir/dir/subdir/file2"
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInputStreamClosedTwice()
		{
			//HADOOP-4760 according to Closeable#close() closing already-closed 
			//streams should have no effect. 
			Org.Apache.Hadoop.FS.Path src = Path("/test/hadoop/file");
			CreateFile(src);
			FSDataInputStream @in = fs.Open(src);
			@in.Close();
			@in.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOutputStreamClosedTwice()
		{
			//HADOOP-4760 according to Closeable#close() closing already-closed 
			//streams should have no effect. 
			Org.Apache.Hadoop.FS.Path src = Path("/test/hadoop/file");
			FSDataOutputStream @out = fs.Create(src);
			@out.WriteChar('H');
			//write some data
			@out.Close();
			@out.Close();
		}

		protected internal virtual Org.Apache.Hadoop.FS.Path Path(string pathString)
		{
			return new Org.Apache.Hadoop.FS.Path(pathString).MakeQualified(fs);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CreateFile(Org.Apache.Hadoop.FS.Path path)
		{
			FSDataOutputStream @out = fs.Create(path);
			@out.Write(data, 0, data.Length);
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void Rename(Org.Apache.Hadoop.FS.Path src, Org.Apache.Hadoop.FS.Path
			 dst, bool renameSucceeded, bool srcExists, bool dstExists)
		{
			Assert.Equal("Rename result", renameSucceeded, fs.Rename(src, 
				dst));
			Assert.Equal("Source exists", srcExists, fs.Exists(src));
			Assert.Equal("Destination exists" + dst, dstExists, fs.Exists(
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
		public virtual void TestOverWriteAndRead()
		{
			int blockSize = GetBlockSize();
			byte[] filedata1 = Dataset(blockSize * 2, 'A', 26);
			byte[] filedata2 = Dataset(blockSize * 2, 'a', 26);
			Org.Apache.Hadoop.FS.Path path = Path("/test/hadoop/file-overwrite");
			WriteAndRead(path, filedata1, blockSize, true, false);
			WriteAndRead(path, filedata2, blockSize, true, false);
			WriteAndRead(path, filedata1, blockSize * 2, true, false);
			WriteAndRead(path, filedata2, blockSize * 2, true, false);
			WriteAndRead(path, filedata1, blockSize, true, false);
			WriteAndRead(path, filedata2, blockSize * 2, true, false);
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
		protected internal virtual void WriteAndRead(Org.Apache.Hadoop.FS.Path path, byte
			[] src, int len, bool overwrite, bool delete)
		{
			Assert.True("Not enough data in source array to write " + len +
				 " bytes", src.Length >= len);
			fs.Mkdirs(path.GetParent());
			FSDataOutputStream @out = fs.Create(path, overwrite, fs.GetConf().GetInt("io.file.buffer.size"
				, 4096), (short)1, GetBlockSize());
			@out.Write(src, 0, len);
			@out.Close();
			Assert.True("Exists", fs.Exists(path));
			Assert.Equal("Length", len, fs.GetFileStatus(path).GetLen());
			FSDataInputStream @in = fs.Open(path);
			byte[] buf = new byte[len];
			@in.ReadFully(0, buf);
			@in.Close();
			Assert.Equal(len, buf.Length);
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
				string message = string.Format(" %d errors in file of length %d", errors, len);
				Log.Warn(message);
				// the range either side of the first error to print
				// this is a purely arbitrary number, to aid user debugging
				int overlap = 10;
				for (int i_1 = Math.Max(0, first_error_byte - overlap); i_1 < Math.Min(first_error_byte
					 + overlap, len); i_1++)
				{
					byte actual = buf[i_1];
					byte expected = src[i_1];
					string letter = ToChar(actual);
					string line = string.Format("[%04d] %2x %s\n", i_1, actual, letter);
					if (expected != actual)
					{
						line = string.Format("[%04d] %2x %s -expected %2x %s\n", i_1, actual, letter, expected
							, ToChar(expected));
					}
					Log.Warn(line);
				}
				Fail(message);
			}
			if (delete)
			{
				bool deleted = fs.Delete(path, false);
				Assert.True("Deleted", deleted);
				NUnit.Framework.Assert.IsFalse("No longer exists", fs.Exists(path));
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
		protected internal virtual string ToChar(byte b)
		{
			if (b >= unchecked((int)(0x20)))
			{
				return char.ToString((char)b);
			}
			else
			{
				return string.Format("%02x", b);
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
		protected internal virtual byte[] Dataset(int len, int @base, int modulo)
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
			data = Dataset(GetBlockSize() * 2, 0, 255);
		}
	}
}
