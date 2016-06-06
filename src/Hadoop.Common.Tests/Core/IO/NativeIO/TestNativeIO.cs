using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.IO;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.Nativeio
{
	public class TestNativeIO
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestNativeIO));

		internal static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			), "testnativeio");

		[SetUp]
		public virtual void CheckLoaded()
		{
			Assume.AssumeTrue(NativeCodeLoader.IsNativeCodeLoaded());
		}

		[SetUp]
		public virtual void SetupTestDir()
		{
			FileUtil.FullyDelete(TestDir);
			TestDir.Mkdirs();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFstat()
		{
			FileOutputStream fos = new FileOutputStream(new FilePath(TestDir, "testfstat"));
			NativeIO.POSIX.Stat stat = NativeIO.POSIX.GetFstat(fos.GetFD());
			fos.Close();
			Log.Info("Stat: " + stat.ToString());
			string owner = stat.GetOwner();
			string expectedOwner = Runtime.GetProperty("user.name");
			if (Path.Windows)
			{
				UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(expectedOwner);
				string adminsGroupString = "Administrators";
				if (Arrays.AsList(ugi.GetGroupNames()).Contains(adminsGroupString))
				{
					expectedOwner = adminsGroupString;
				}
			}
			Assert.Equal(expectedOwner, owner);
			NUnit.Framework.Assert.IsNotNull(stat.GetGroup());
			Assert.True(!stat.GetGroup().IsEmpty());
			Assert.Equal("Stat mode field should indicate a regular file", 
				NativeIO.POSIX.Stat.SIfreg, stat.GetMode() & NativeIO.POSIX.Stat.SIfmt);
		}

		/// <summary>
		/// Test for races in fstat usage
		/// NOTE: this test is likely to fail on RHEL 6.0 which has a non-threadsafe
		/// implementation of getpwuid_r.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestMultiThreadedFstat()
		{
			if (Path.Windows)
			{
				return;
			}
			FileOutputStream fos = new FileOutputStream(new FilePath(TestDir, "testfstat"));
			AtomicReference<Exception> thrown = new AtomicReference<Exception>();
			IList<Thread> statters = new AList<Thread>();
			for (int i = 0; i < 10; i++)
			{
				Thread statter = new _Thread_120(fos, thrown);
				statters.AddItem(statter);
				statter.Start();
			}
			foreach (Thread t in statters)
			{
				t.Join();
			}
			fos.Close();
			if (thrown.Get() != null)
			{
				throw new RuntimeException(thrown.Get());
			}
		}

		private sealed class _Thread_120 : Thread
		{
			public _Thread_120(FileOutputStream fos, AtomicReference<Exception> thrown)
			{
				this.fos = fos;
				this.thrown = thrown;
			}

			public override void Run()
			{
				long et = Time.Now() + 5000;
				while (Time.Now() < et)
				{
					try
					{
						NativeIO.POSIX.Stat stat = NativeIO.POSIX.GetFstat(fos.GetFD());
						Assert.Equal(Runtime.GetProperty("user.name"), stat.GetOwner()
							);
						NUnit.Framework.Assert.IsNotNull(stat.GetGroup());
						Assert.True(!stat.GetGroup().IsEmpty());
						Assert.Equal("Stat mode field should indicate a regular file", 
							NativeIO.POSIX.Stat.SIfreg, stat.GetMode() & NativeIO.POSIX.Stat.SIfmt);
					}
					catch (Exception t)
					{
						thrown.Set(t);
					}
				}
			}

			private readonly FileOutputStream fos;

			private readonly AtomicReference<Exception> thrown;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFstatClosedFd()
		{
			FileOutputStream fos = new FileOutputStream(new FilePath(TestDir, "testfstat2"));
			fos.Close();
			try
			{
				NativeIO.POSIX.Stat stat = NativeIO.POSIX.GetFstat(fos.GetFD());
			}
			catch (NativeIOException nioe)
			{
				Log.Info("Got expected exception", nioe);
				Assert.Equal(Errno.Ebadf, nioe.GetErrno());
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetFilePointer()
		{
			if (!Path.Windows)
			{
				return;
			}
			Log.Info("Set a file pointer on Windows");
			try
			{
				FilePath testfile = new FilePath(TestDir, "testSetFilePointer");
				Assert.True("Create test subject", testfile.Exists() || testfile
					.CreateNewFile());
				FileWriter writer = new FileWriter(testfile);
				try
				{
					for (int i = 0; i < 200; i++)
					{
						if (i < 100)
						{
							writer.Write('a');
						}
						else
						{
							writer.Write('b');
						}
					}
					writer.Flush();
				}
				catch (Exception writerException)
				{
					NUnit.Framework.Assert.Fail("Got unexpected exception: " + writerException.Message
						);
				}
				finally
				{
					writer.Close();
				}
				FileDescriptor fd = NativeIO.Windows.CreateFile(testfile.GetCanonicalPath(), NativeIO.Windows
					.GenericRead, NativeIO.Windows.FileShareRead | NativeIO.Windows.FileShareWrite |
					 NativeIO.Windows.FileShareDelete, NativeIO.Windows.OpenExisting);
				NativeIO.Windows.SetFilePointer(fd, 120, NativeIO.Windows.FileBegin);
				FileReader reader = new FileReader(fd);
				try
				{
					int c = reader.Read();
					Assert.True("Unexpected character: " + c, c == 'b');
				}
				catch (Exception readerException)
				{
					NUnit.Framework.Assert.Fail("Got unexpected exception: " + readerException.Message
						);
				}
				finally
				{
					reader.Close();
				}
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("Got unexpected exception: " + e.Message);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateFile()
		{
			if (!Path.Windows)
			{
				return;
			}
			Log.Info("Open a file on Windows with SHARE_DELETE shared mode");
			try
			{
				FilePath testfile = new FilePath(TestDir, "testCreateFile");
				Assert.True("Create test subject", testfile.Exists() || testfile
					.CreateNewFile());
				FileDescriptor fd = NativeIO.Windows.CreateFile(testfile.GetCanonicalPath(), NativeIO.Windows
					.GenericRead, NativeIO.Windows.FileShareRead | NativeIO.Windows.FileShareWrite |
					 NativeIO.Windows.FileShareDelete, NativeIO.Windows.OpenExisting);
				FileInputStream fin = new FileInputStream(fd);
				try
				{
					fin.Read();
					FilePath newfile = new FilePath(TestDir, "testRenamedFile");
					bool renamed = testfile.RenameTo(newfile);
					Assert.True("Rename failed.", renamed);
					fin.Read();
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail("Got unexpected exception: " + e.Message);
				}
				finally
				{
					fin.Close();
				}
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("Got unexpected exception: " + e.Message);
			}
		}

		/// <summary>Validate access checks on Windows</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAccess()
		{
			if (!Path.Windows)
			{
				return;
			}
			FilePath testFile = new FilePath(TestDir, "testfileaccess");
			Assert.True(testFile.CreateNewFile());
			// Validate ACCESS_READ
			FileUtil.SetReadable(testFile, false);
			NUnit.Framework.Assert.IsFalse(NativeIO.Windows.Access(testFile.GetAbsolutePath()
				, NativeIO.Windows.AccessRight.AccessRead));
			FileUtil.SetReadable(testFile, true);
			Assert.True(NativeIO.Windows.Access(testFile.GetAbsolutePath(), 
				NativeIO.Windows.AccessRight.AccessRead));
			// Validate ACCESS_WRITE
			FileUtil.SetWritable(testFile, false);
			NUnit.Framework.Assert.IsFalse(NativeIO.Windows.Access(testFile.GetAbsolutePath()
				, NativeIO.Windows.AccessRight.AccessWrite));
			FileUtil.SetWritable(testFile, true);
			Assert.True(NativeIO.Windows.Access(testFile.GetAbsolutePath(), 
				NativeIO.Windows.AccessRight.AccessWrite));
			// Validate ACCESS_EXECUTE
			FileUtil.SetExecutable(testFile, false);
			NUnit.Framework.Assert.IsFalse(NativeIO.Windows.Access(testFile.GetAbsolutePath()
				, NativeIO.Windows.AccessRight.AccessExecute));
			FileUtil.SetExecutable(testFile, true);
			Assert.True(NativeIO.Windows.Access(testFile.GetAbsolutePath(), 
				NativeIO.Windows.AccessRight.AccessExecute));
			// Validate that access checks work as expected for long paths
			// Assemble a path longer then 260 chars (MAX_PATH)
			string testFileRelativePath = string.Empty;
			for (int i = 0; i < 15; ++i)
			{
				testFileRelativePath += "testfileaccessfolder\\";
			}
			testFileRelativePath += "testfileaccess";
			testFile = new FilePath(TestDir, testFileRelativePath);
			Assert.True(testFile.GetParentFile().Mkdirs());
			Assert.True(testFile.CreateNewFile());
			// Validate ACCESS_READ
			FileUtil.SetReadable(testFile, false);
			NUnit.Framework.Assert.IsFalse(NativeIO.Windows.Access(testFile.GetAbsolutePath()
				, NativeIO.Windows.AccessRight.AccessRead));
			FileUtil.SetReadable(testFile, true);
			Assert.True(NativeIO.Windows.Access(testFile.GetAbsolutePath(), 
				NativeIO.Windows.AccessRight.AccessRead));
			// Validate ACCESS_WRITE
			FileUtil.SetWritable(testFile, false);
			NUnit.Framework.Assert.IsFalse(NativeIO.Windows.Access(testFile.GetAbsolutePath()
				, NativeIO.Windows.AccessRight.AccessWrite));
			FileUtil.SetWritable(testFile, true);
			Assert.True(NativeIO.Windows.Access(testFile.GetAbsolutePath(), 
				NativeIO.Windows.AccessRight.AccessWrite));
			// Validate ACCESS_EXECUTE
			FileUtil.SetExecutable(testFile, false);
			NUnit.Framework.Assert.IsFalse(NativeIO.Windows.Access(testFile.GetAbsolutePath()
				, NativeIO.Windows.AccessRight.AccessExecute));
			FileUtil.SetExecutable(testFile, true);
			Assert.True(NativeIO.Windows.Access(testFile.GetAbsolutePath(), 
				NativeIO.Windows.AccessRight.AccessExecute));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestOpenMissingWithoutCreate()
		{
			if (Path.Windows)
			{
				return;
			}
			Log.Info("Open a missing file without O_CREAT and it should fail");
			try
			{
				FileDescriptor fd = NativeIO.POSIX.Open(new FilePath(TestDir, "doesntexist").GetAbsolutePath
					(), NativeIO.POSIX.OWronly, 0x1c0);
				NUnit.Framework.Assert.Fail("Able to open a new file without O_CREAT");
			}
			catch (NativeIOException nioe)
			{
				Log.Info("Got expected exception", nioe);
				Assert.Equal(Errno.Enoent, nioe.GetErrno());
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestOpenWithCreate()
		{
			if (Path.Windows)
			{
				return;
			}
			Log.Info("Test creating a file with O_CREAT");
			FileDescriptor fd = NativeIO.POSIX.Open(new FilePath(TestDir, "testWorkingOpen").
				GetAbsolutePath(), NativeIO.POSIX.OWronly | NativeIO.POSIX.OCreat, 0x1c0);
			NUnit.Framework.Assert.IsNotNull(true);
			Assert.True(fd.Valid());
			FileOutputStream fos = new FileOutputStream(fd);
			fos.Write(Runtime.GetBytesForString("foo"));
			fos.Close();
			NUnit.Framework.Assert.IsFalse(fd.Valid());
			Log.Info("Test exclusive create");
			try
			{
				fd = NativeIO.POSIX.Open(new FilePath(TestDir, "testWorkingOpen").GetAbsolutePath
					(), NativeIO.POSIX.OWronly | NativeIO.POSIX.OCreat | NativeIO.POSIX.OExcl, 0x1c0
					);
				NUnit.Framework.Assert.Fail("Was able to create existing file with O_EXCL");
			}
			catch (NativeIOException nioe)
			{
				Log.Info("Got expected exception for failed exclusive create", nioe);
				Assert.Equal(Errno.Eexist, nioe.GetErrno());
			}
		}

		/// <summary>
		/// Test that opens and closes a file 10000 times - this would crash with
		/// "Too many open files" if we leaked fds using this access pattern.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFDDoesntLeak()
		{
			if (Path.Windows)
			{
				return;
			}
			for (int i = 0; i < 10000; i++)
			{
				FileDescriptor fd = NativeIO.POSIX.Open(new FilePath(TestDir, "testNoFdLeak").GetAbsolutePath
					(), NativeIO.POSIX.OWronly | NativeIO.POSIX.OCreat, 0x1c0);
				NUnit.Framework.Assert.IsNotNull(true);
				Assert.True(fd.Valid());
				FileOutputStream fos = new FileOutputStream(fd);
				fos.Write(Runtime.GetBytesForString("foo"));
				fos.Close();
			}
		}

		/// <summary>Test basic chmod operation</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestChmod()
		{
			if (Path.Windows)
			{
				return;
			}
			try
			{
				NativeIO.POSIX.Chmod("/this/file/doesnt/exist", 777);
				NUnit.Framework.Assert.Fail("Chmod of non-existent file didn't fail");
			}
			catch (NativeIOException nioe)
			{
				Assert.Equal(Errno.Enoent, nioe.GetErrno());
			}
			FilePath toChmod = new FilePath(TestDir, "testChmod");
			Assert.True("Create test subject", toChmod.Exists() || toChmod.
				Mkdir());
			NativeIO.POSIX.Chmod(toChmod.GetAbsolutePath(), 0x1ff);
			AssertPermissions(toChmod, 0x1ff);
			NativeIO.POSIX.Chmod(toChmod.GetAbsolutePath(), 0000);
			AssertPermissions(toChmod, 0000);
			NativeIO.POSIX.Chmod(toChmod.GetAbsolutePath(), 0x1a4);
			AssertPermissions(toChmod, 0x1a4);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPosixFadvise()
		{
			if (Path.Windows)
			{
				return;
			}
			FileInputStream fis = new FileInputStream("/dev/zero");
			try
			{
				NativeIO.POSIX.Posix_fadvise(fis.GetFD(), 0, 0, NativeIO.POSIX.PosixFadvSequential
					);
			}
			catch (NotSupportedException)
			{
				// we should just skip the unit test on machines where we don't
				// have fadvise support
				Assume.AssumeTrue(false);
			}
			catch (NativeIOException)
			{
			}
			finally
			{
				// ignore this error as FreeBSD returns EBADF even if length is zero
				fis.Close();
			}
			try
			{
				NativeIO.POSIX.Posix_fadvise(fis.GetFD(), 0, 1024, NativeIO.POSIX.PosixFadvSequential
					);
				NUnit.Framework.Assert.Fail("Did not throw on bad file");
			}
			catch (NativeIOException nioe)
			{
				Assert.Equal(Errno.Ebadf, nioe.GetErrno());
			}
			try
			{
				NativeIO.POSIX.Posix_fadvise(null, 0, 1024, NativeIO.POSIX.PosixFadvSequential);
				NUnit.Framework.Assert.Fail("Did not throw on null file");
			}
			catch (ArgumentNullException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		public virtual void TestSyncFileRange()
		{
			FileOutputStream fos = new FileOutputStream(new FilePath(TestDir, "testSyncFileRange"
				));
			try
			{
				fos.Write(Runtime.GetBytesForString("foo"));
				NativeIO.POSIX.Sync_file_range(fos.GetFD(), 0, 1024, NativeIO.POSIX.SyncFileRangeWrite
					);
			}
			catch (NotSupportedException)
			{
				// no way to verify that this actually has synced,
				// but if it doesn't throw, we can assume it worked
				// we should just skip the unit test on machines where we don't
				// have fadvise support
				Assume.AssumeTrue(false);
			}
			finally
			{
				fos.Close();
			}
			try
			{
				NativeIO.POSIX.Sync_file_range(fos.GetFD(), 0, 1024, NativeIO.POSIX.SyncFileRangeWrite
					);
				NUnit.Framework.Assert.Fail("Did not throw on bad file");
			}
			catch (NativeIOException nioe)
			{
				Assert.Equal(Errno.Ebadf, nioe.GetErrno());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertPermissions(FilePath f, int expected)
		{
			FileSystem localfs = FileSystem.GetLocal(new Configuration());
			FsPermission perms = localfs.GetFileStatus(new Path(f.GetAbsolutePath())).GetPermission
				();
			Assert.Equal(expected, perms.ToShort());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetUserName()
		{
			if (Path.Windows)
			{
				return;
			}
			NUnit.Framework.Assert.IsFalse(NativeIO.POSIX.GetUserName(0).IsEmpty());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetGroupName()
		{
			if (Path.Windows)
			{
				return;
			}
			NUnit.Framework.Assert.IsFalse(NativeIO.POSIX.GetGroupName(0).IsEmpty());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameTo()
		{
			FilePath TestDir = new FilePath(new FilePath(Runtime.GetProperty("test.build.data"
				, "build/test/data")), "renameTest");
			Assume.AssumeTrue(TestDir.Mkdirs());
			FilePath nonExistentFile = new FilePath(TestDir, "nonexistent");
			FilePath targetFile = new FilePath(TestDir, "target");
			// Test attempting to rename a nonexistent file.
			try
			{
				NativeIO.RenameTo(nonExistentFile, targetFile);
				NUnit.Framework.Assert.Fail();
			}
			catch (NativeIOException e)
			{
				if (Path.Windows)
				{
					Assert.Equal(string.Format("The system cannot find the file specified.%n"
						), e.Message);
				}
				else
				{
					Assert.Equal(Errno.Enoent, e.GetErrno());
				}
			}
			// Test renaming a file to itself.  It should succeed and do nothing.
			FilePath sourceFile = new FilePath(TestDir, "source");
			Assert.True(sourceFile.CreateNewFile());
			NativeIO.RenameTo(sourceFile, sourceFile);
			// Test renaming a source to a destination.
			NativeIO.RenameTo(sourceFile, targetFile);
			// Test renaming a source to a path which uses a file as a directory.
			sourceFile = new FilePath(TestDir, "source");
			Assert.True(sourceFile.CreateNewFile());
			FilePath badTarget = new FilePath(targetFile, "subdir");
			try
			{
				NativeIO.RenameTo(sourceFile, badTarget);
				NUnit.Framework.Assert.Fail();
			}
			catch (NativeIOException e)
			{
				if (Path.Windows)
				{
					Assert.Equal(string.Format("The parameter is incorrect.%n"), e
						.Message);
				}
				else
				{
					Assert.Equal(Errno.Enotdir, e.GetErrno());
				}
			}
			FileUtils.DeleteQuietly(TestDir);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMlock()
		{
			Assume.AssumeTrue(NativeIO.IsAvailable());
			FilePath TestFile = new FilePath(new FilePath(Runtime.GetProperty("test.build.data"
				, "build/test/data")), "testMlockFile");
			int BufLen = 12289;
			byte[] buf = new byte[BufLen];
			int bufSum = 0;
			for (int i = 0; i < buf.Length; i++)
			{
				buf[i] = unchecked((byte)(i % 60));
				bufSum += buf[i];
			}
			FileOutputStream fos = new FileOutputStream(TestFile);
			try
			{
				fos.Write(buf);
				fos.GetChannel().Force(true);
			}
			finally
			{
				fos.Close();
			}
			FileInputStream fis = null;
			FileChannel channel = null;
			try
			{
				// Map file into memory
				fis = new FileInputStream(TestFile);
				channel = fis.GetChannel();
				long fileSize = channel.Size();
				MappedByteBuffer mapbuf = channel.Map(FileChannel.MapMode.ReadOnly, 0, fileSize);
				// mlock the buffer
				NativeIO.POSIX.Mlock(mapbuf, fileSize);
				// Read the buffer
				int sum = 0;
				for (int i_1 = 0; i_1 < fileSize; i_1++)
				{
					sum += mapbuf.Get(i_1);
				}
				Assert.Equal("Expected sums to be equal", bufSum, sum);
				// munmap the buffer, which also implicitly unlocks it
				NativeIO.POSIX.Munmap(mapbuf);
			}
			finally
			{
				if (channel != null)
				{
					channel.Close();
				}
				if (fis != null)
				{
					fis.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetMemlockLimit()
		{
			Assume.AssumeTrue(NativeIO.IsAvailable());
			NativeIO.GetMemlockLimit();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCopyFileUnbuffered()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			FilePath srcFile = new FilePath(TestDir, MethodName + ".src.dat");
			FilePath dstFile = new FilePath(TestDir, MethodName + ".dst.dat");
			int fileSize = unchecked((int)(0x8000000));
			// 128 MB
			int Seed = unchecked((int)(0xBEEF));
			int batchSize = 4096;
			int numBatches = fileSize / batchSize;
			Random rb = new Random(Seed);
			FileChannel channel = null;
			RandomAccessFile raSrcFile = null;
			try
			{
				raSrcFile = new RandomAccessFile(srcFile, "rw");
				channel = raSrcFile.GetChannel();
				byte[] bytesToWrite = new byte[batchSize];
				MappedByteBuffer mapBuf;
				mapBuf = channel.Map(FileChannel.MapMode.ReadWrite, 0, fileSize);
				for (int i = 0; i < numBatches; i++)
				{
					rb.NextBytes(bytesToWrite);
					mapBuf.Put(bytesToWrite);
				}
				NativeIO.CopyFileUnbuffered(srcFile, dstFile);
				Assert.Equal(srcFile.Length(), dstFile.Length());
			}
			finally
			{
				IOUtils.Cleanup(Log, channel);
				IOUtils.Cleanup(Log, raSrcFile);
				FileUtils.DeleteQuietly(TestDir);
			}
		}
	}
}
