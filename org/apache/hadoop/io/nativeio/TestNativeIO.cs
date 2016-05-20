using Sharpen;

namespace org.apache.hadoop.io.nativeio
{
	public class TestNativeIO
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.nativeio.TestNativeIO
			)));

		internal static readonly java.io.File TEST_DIR = new java.io.File(Sharpen.Runtime
			.getProperty("test.build.data"), "testnativeio");

		[NUnit.Framework.SetUp]
		public virtual void checkLoaded()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded
				());
		}

		[NUnit.Framework.SetUp]
		public virtual void setupTestDir()
		{
			org.apache.hadoop.fs.FileUtil.fullyDelete(TEST_DIR);
			TEST_DIR.mkdirs();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testFstat()
		{
			java.io.FileOutputStream fos = new java.io.FileOutputStream(new java.io.File(TEST_DIR
				, "testfstat"));
			org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat stat = org.apache.hadoop.io.nativeio.NativeIO.POSIX
				.getFstat(fos.getFD());
			fos.close();
			LOG.info("Stat: " + Sharpen.Runtime.getStringValueOf(stat));
			string owner = stat.getOwner();
			string expectedOwner = Sharpen.Runtime.getProperty("user.name");
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				org.apache.hadoop.security.UserGroupInformation ugi = org.apache.hadoop.security.UserGroupInformation
					.createRemoteUser(expectedOwner);
				string adminsGroupString = "Administrators";
				if (java.util.Arrays.asList(ugi.getGroupNames()).contains(adminsGroupString))
				{
					expectedOwner = adminsGroupString;
				}
			}
			NUnit.Framework.Assert.AreEqual(expectedOwner, owner);
			NUnit.Framework.Assert.IsNotNull(stat.getGroup());
			NUnit.Framework.Assert.IsTrue(!stat.getGroup().isEmpty());
			NUnit.Framework.Assert.AreEqual("Stat mode field should indicate a regular file", 
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat.S_IFREG, stat.getMode() & org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat
				.S_IFMT);
		}

		/// <summary>
		/// Test for races in fstat usage
		/// NOTE: this test is likely to fail on RHEL 6.0 which has a non-threadsafe
		/// implementation of getpwuid_r.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testMultiThreadedFstat()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			java.io.FileOutputStream fos = new java.io.FileOutputStream(new java.io.File(TEST_DIR
				, "testfstat"));
			java.util.concurrent.atomic.AtomicReference<System.Exception> thrown = new java.util.concurrent.atomic.AtomicReference
				<System.Exception>();
			System.Collections.Generic.IList<java.lang.Thread> statters = new System.Collections.Generic.List
				<java.lang.Thread>();
			for (int i = 0; i < 10; i++)
			{
				java.lang.Thread statter = new _Thread_120(fos, thrown);
				statters.add(statter);
				statter.start();
			}
			foreach (java.lang.Thread t in statters)
			{
				t.join();
			}
			fos.close();
			if (thrown.get() != null)
			{
				throw new System.Exception(thrown.get());
			}
		}

		private sealed class _Thread_120 : java.lang.Thread
		{
			public _Thread_120(java.io.FileOutputStream fos, java.util.concurrent.atomic.AtomicReference
				<System.Exception> thrown)
			{
				this.fos = fos;
				this.thrown = thrown;
			}

			public override void run()
			{
				long et = org.apache.hadoop.util.Time.now() + 5000;
				while (org.apache.hadoop.util.Time.now() < et)
				{
					try
					{
						org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat stat = org.apache.hadoop.io.nativeio.NativeIO.POSIX
							.getFstat(fos.getFD());
						NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getProperty("user.name"), stat.getOwner
							());
						NUnit.Framework.Assert.IsNotNull(stat.getGroup());
						NUnit.Framework.Assert.IsTrue(!stat.getGroup().isEmpty());
						NUnit.Framework.Assert.AreEqual("Stat mode field should indicate a regular file", 
							org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat.S_IFREG, stat.getMode() & org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat
							.S_IFMT);
					}
					catch (System.Exception t)
					{
						thrown.set(t);
					}
				}
			}

			private readonly java.io.FileOutputStream fos;

			private readonly java.util.concurrent.atomic.AtomicReference<System.Exception> thrown;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testFstatClosedFd()
		{
			java.io.FileOutputStream fos = new java.io.FileOutputStream(new java.io.File(TEST_DIR
				, "testfstat2"));
			fos.close();
			try
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat stat = org.apache.hadoop.io.nativeio.NativeIO.POSIX
					.getFstat(fos.getFD());
			}
			catch (org.apache.hadoop.io.nativeio.NativeIOException nioe)
			{
				LOG.info("Got expected exception", nioe);
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.io.nativeio.Errno.EBADF, nioe.getErrno
					());
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testSetFilePointer()
		{
			if (!org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			LOG.info("Set a file pointer on Windows");
			try
			{
				java.io.File testfile = new java.io.File(TEST_DIR, "testSetFilePointer");
				NUnit.Framework.Assert.IsTrue("Create test subject", testfile.exists() || testfile
					.createNewFile());
				java.io.FileWriter writer = new java.io.FileWriter(testfile);
				try
				{
					for (int i = 0; i < 200; i++)
					{
						if (i < 100)
						{
							writer.write('a');
						}
						else
						{
							writer.write('b');
						}
					}
					writer.flush();
				}
				catch (System.Exception writerException)
				{
					NUnit.Framework.Assert.Fail("Got unexpected exception: " + writerException.Message
						);
				}
				finally
				{
					writer.close();
				}
				java.io.FileDescriptor fd = org.apache.hadoop.io.nativeio.NativeIO.Windows.createFile
					(testfile.getCanonicalPath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.GENERIC_READ
					, org.apache.hadoop.io.nativeio.NativeIO.Windows.FILE_SHARE_READ | org.apache.hadoop.io.nativeio.NativeIO.Windows
					.FILE_SHARE_WRITE | org.apache.hadoop.io.nativeio.NativeIO.Windows.FILE_SHARE_DELETE
					, org.apache.hadoop.io.nativeio.NativeIO.Windows.OPEN_EXISTING);
				org.apache.hadoop.io.nativeio.NativeIO.Windows.setFilePointer(fd, 120, org.apache.hadoop.io.nativeio.NativeIO.Windows
					.FILE_BEGIN);
				java.io.FileReader reader = new java.io.FileReader(fd);
				try
				{
					int c = reader.read();
					NUnit.Framework.Assert.IsTrue("Unexpected character: " + c, c == 'b');
				}
				catch (System.Exception readerException)
				{
					NUnit.Framework.Assert.Fail("Got unexpected exception: " + readerException.Message
						);
				}
				finally
				{
					reader.close();
				}
			}
			catch (System.Exception e)
			{
				NUnit.Framework.Assert.Fail("Got unexpected exception: " + e.Message);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testCreateFile()
		{
			if (!org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			LOG.info("Open a file on Windows with SHARE_DELETE shared mode");
			try
			{
				java.io.File testfile = new java.io.File(TEST_DIR, "testCreateFile");
				NUnit.Framework.Assert.IsTrue("Create test subject", testfile.exists() || testfile
					.createNewFile());
				java.io.FileDescriptor fd = org.apache.hadoop.io.nativeio.NativeIO.Windows.createFile
					(testfile.getCanonicalPath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.GENERIC_READ
					, org.apache.hadoop.io.nativeio.NativeIO.Windows.FILE_SHARE_READ | org.apache.hadoop.io.nativeio.NativeIO.Windows
					.FILE_SHARE_WRITE | org.apache.hadoop.io.nativeio.NativeIO.Windows.FILE_SHARE_DELETE
					, org.apache.hadoop.io.nativeio.NativeIO.Windows.OPEN_EXISTING);
				java.io.FileInputStream fin = new java.io.FileInputStream(fd);
				try
				{
					fin.read();
					java.io.File newfile = new java.io.File(TEST_DIR, "testRenamedFile");
					bool renamed = testfile.renameTo(newfile);
					NUnit.Framework.Assert.IsTrue("Rename failed.", renamed);
					fin.read();
				}
				catch (System.Exception e)
				{
					NUnit.Framework.Assert.Fail("Got unexpected exception: " + e.Message);
				}
				finally
				{
					fin.close();
				}
			}
			catch (System.Exception e)
			{
				NUnit.Framework.Assert.Fail("Got unexpected exception: " + e.Message);
			}
		}

		/// <summary>Validate access checks on Windows</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testAccess()
		{
			if (!org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			java.io.File testFile = new java.io.File(TEST_DIR, "testfileaccess");
			NUnit.Framework.Assert.IsTrue(testFile.createNewFile());
			// Validate ACCESS_READ
			org.apache.hadoop.fs.FileUtil.setReadable(testFile, false);
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_READ));
			org.apache.hadoop.fs.FileUtil.setReadable(testFile, true);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_READ));
			// Validate ACCESS_WRITE
			org.apache.hadoop.fs.FileUtil.setWritable(testFile, false);
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_WRITE));
			org.apache.hadoop.fs.FileUtil.setWritable(testFile, true);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_WRITE));
			// Validate ACCESS_EXECUTE
			org.apache.hadoop.fs.FileUtil.setExecutable(testFile, false);
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_EXECUTE));
			org.apache.hadoop.fs.FileUtil.setExecutable(testFile, true);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_EXECUTE));
			// Validate that access checks work as expected for long paths
			// Assemble a path longer then 260 chars (MAX_PATH)
			string testFileRelativePath = string.Empty;
			for (int i = 0; i < 15; ++i)
			{
				testFileRelativePath += "testfileaccessfolder\\";
			}
			testFileRelativePath += "testfileaccess";
			testFile = new java.io.File(TEST_DIR, testFileRelativePath);
			NUnit.Framework.Assert.IsTrue(testFile.getParentFile().mkdirs());
			NUnit.Framework.Assert.IsTrue(testFile.createNewFile());
			// Validate ACCESS_READ
			org.apache.hadoop.fs.FileUtil.setReadable(testFile, false);
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_READ));
			org.apache.hadoop.fs.FileUtil.setReadable(testFile, true);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_READ));
			// Validate ACCESS_WRITE
			org.apache.hadoop.fs.FileUtil.setWritable(testFile, false);
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_WRITE));
			org.apache.hadoop.fs.FileUtil.setWritable(testFile, true);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_WRITE));
			// Validate ACCESS_EXECUTE
			org.apache.hadoop.fs.FileUtil.setExecutable(testFile, false);
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_EXECUTE));
			org.apache.hadoop.fs.FileUtil.setExecutable(testFile, true);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.io.nativeio.NativeIO.Windows.access
				(testFile.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight
				.ACCESS_EXECUTE));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testOpenMissingWithoutCreate()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			LOG.info("Open a missing file without O_CREAT and it should fail");
			try
			{
				java.io.FileDescriptor fd = org.apache.hadoop.io.nativeio.NativeIO.POSIX.open(new 
					java.io.File(TEST_DIR, "doesntexist").getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.POSIX
					.O_WRONLY, 0x1c0);
				NUnit.Framework.Assert.Fail("Able to open a new file without O_CREAT");
			}
			catch (org.apache.hadoop.io.nativeio.NativeIOException nioe)
			{
				LOG.info("Got expected exception", nioe);
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.io.nativeio.Errno.ENOENT, nioe.
					getErrno());
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testOpenWithCreate()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			LOG.info("Test creating a file with O_CREAT");
			java.io.FileDescriptor fd = org.apache.hadoop.io.nativeio.NativeIO.POSIX.open(new 
				java.io.File(TEST_DIR, "testWorkingOpen").getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.POSIX
				.O_WRONLY | org.apache.hadoop.io.nativeio.NativeIO.POSIX.O_CREAT, 0x1c0);
			NUnit.Framework.Assert.IsNotNull(true);
			NUnit.Framework.Assert.IsTrue(fd.valid());
			java.io.FileOutputStream fos = new java.io.FileOutputStream(fd);
			fos.write(Sharpen.Runtime.getBytesForString("foo"));
			fos.close();
			NUnit.Framework.Assert.IsFalse(fd.valid());
			LOG.info("Test exclusive create");
			try
			{
				fd = org.apache.hadoop.io.nativeio.NativeIO.POSIX.open(new java.io.File(TEST_DIR, 
					"testWorkingOpen").getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.POSIX
					.O_WRONLY | org.apache.hadoop.io.nativeio.NativeIO.POSIX.O_CREAT | org.apache.hadoop.io.nativeio.NativeIO.POSIX
					.O_EXCL, 0x1c0);
				NUnit.Framework.Assert.Fail("Was able to create existing file with O_EXCL");
			}
			catch (org.apache.hadoop.io.nativeio.NativeIOException nioe)
			{
				LOG.info("Got expected exception for failed exclusive create", nioe);
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.io.nativeio.Errno.EEXIST, nioe.
					getErrno());
			}
		}

		/// <summary>
		/// Test that opens and closes a file 10000 times - this would crash with
		/// "Too many open files" if we leaked fds using this access pattern.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFDDoesntLeak()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			for (int i = 0; i < 10000; i++)
			{
				java.io.FileDescriptor fd = org.apache.hadoop.io.nativeio.NativeIO.POSIX.open(new 
					java.io.File(TEST_DIR, "testNoFdLeak").getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.POSIX
					.O_WRONLY | org.apache.hadoop.io.nativeio.NativeIO.POSIX.O_CREAT, 0x1c0);
				NUnit.Framework.Assert.IsNotNull(true);
				NUnit.Framework.Assert.IsTrue(fd.valid());
				java.io.FileOutputStream fos = new java.io.FileOutputStream(fd);
				fos.write(Sharpen.Runtime.getBytesForString("foo"));
				fos.close();
			}
		}

		/// <summary>Test basic chmod operation</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testChmod()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			try
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.chmod("/this/file/doesnt/exist", 777
					);
				NUnit.Framework.Assert.Fail("Chmod of non-existent file didn't fail");
			}
			catch (org.apache.hadoop.io.nativeio.NativeIOException nioe)
			{
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.io.nativeio.Errno.ENOENT, nioe.
					getErrno());
			}
			java.io.File toChmod = new java.io.File(TEST_DIR, "testChmod");
			NUnit.Framework.Assert.IsTrue("Create test subject", toChmod.exists() || toChmod.
				mkdir());
			org.apache.hadoop.io.nativeio.NativeIO.POSIX.chmod(toChmod.getAbsolutePath(), 0x1ff
				);
			assertPermissions(toChmod, 0x1ff);
			org.apache.hadoop.io.nativeio.NativeIO.POSIX.chmod(toChmod.getAbsolutePath(), 0000
				);
			assertPermissions(toChmod, 0000);
			org.apache.hadoop.io.nativeio.NativeIO.POSIX.chmod(toChmod.getAbsolutePath(), 0x1a4
				);
			assertPermissions(toChmod, 0x1a4);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testPosixFadvise()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			java.io.FileInputStream fis = new java.io.FileInputStream("/dev/zero");
			try
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.posix_fadvise(fis.getFD(), 0, 0, org.apache.hadoop.io.nativeio.NativeIO.POSIX
					.POSIX_FADV_SEQUENTIAL);
			}
			catch (System.NotSupportedException)
			{
				// we should just skip the unit test on machines where we don't
				// have fadvise support
				NUnit.Framework.Assume.assumeTrue(false);
			}
			catch (org.apache.hadoop.io.nativeio.NativeIOException)
			{
			}
			finally
			{
				// ignore this error as FreeBSD returns EBADF even if length is zero
				fis.close();
			}
			try
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.posix_fadvise(fis.getFD(), 0, 1024, 
					org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_SEQUENTIAL);
				NUnit.Framework.Assert.Fail("Did not throw on bad file");
			}
			catch (org.apache.hadoop.io.nativeio.NativeIOException nioe)
			{
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.io.nativeio.Errno.EBADF, nioe.getErrno
					());
			}
			try
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.posix_fadvise(null, 0, 1024, org.apache.hadoop.io.nativeio.NativeIO.POSIX
					.POSIX_FADV_SEQUENTIAL);
				NUnit.Framework.Assert.Fail("Did not throw on null file");
			}
			catch (System.ArgumentNullException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		public virtual void testSyncFileRange()
		{
			java.io.FileOutputStream fos = new java.io.FileOutputStream(new java.io.File(TEST_DIR
				, "testSyncFileRange"));
			try
			{
				fos.write(Sharpen.Runtime.getBytesForString("foo"));
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.sync_file_range(fos.getFD(), 0, 1024
					, org.apache.hadoop.io.nativeio.NativeIO.POSIX.SYNC_FILE_RANGE_WRITE);
			}
			catch (System.NotSupportedException)
			{
				// no way to verify that this actually has synced,
				// but if it doesn't throw, we can assume it worked
				// we should just skip the unit test on machines where we don't
				// have fadvise support
				NUnit.Framework.Assume.assumeTrue(false);
			}
			finally
			{
				fos.close();
			}
			try
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.sync_file_range(fos.getFD(), 0, 1024
					, org.apache.hadoop.io.nativeio.NativeIO.POSIX.SYNC_FILE_RANGE_WRITE);
				NUnit.Framework.Assert.Fail("Did not throw on bad file");
			}
			catch (org.apache.hadoop.io.nativeio.NativeIOException nioe)
			{
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.io.nativeio.Errno.EBADF, nioe.getErrno
					());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void assertPermissions(java.io.File f, int expected)
		{
			org.apache.hadoop.fs.FileSystem localfs = org.apache.hadoop.fs.FileSystem.getLocal
				(new org.apache.hadoop.conf.Configuration());
			org.apache.hadoop.fs.permission.FsPermission perms = localfs.getFileStatus(new org.apache.hadoop.fs.Path
				(f.getAbsolutePath())).getPermission();
			NUnit.Framework.Assert.AreEqual(expected, perms.toShort());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetUserName()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.io.nativeio.NativeIO.POSIX.getUserName
				(0).isEmpty());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetGroupName()
		{
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				return;
			}
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.io.nativeio.NativeIO.POSIX.getGroupName
				(0).isEmpty());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRenameTo()
		{
			java.io.File TEST_DIR = new java.io.File(new java.io.File(Sharpen.Runtime.getProperty
				("test.build.data", "build/test/data")), "renameTest");
			NUnit.Framework.Assume.assumeTrue(TEST_DIR.mkdirs());
			java.io.File nonExistentFile = new java.io.File(TEST_DIR, "nonexistent");
			java.io.File targetFile = new java.io.File(TEST_DIR, "target");
			// Test attempting to rename a nonexistent file.
			try
			{
				org.apache.hadoop.io.nativeio.NativeIO.renameTo(nonExistentFile, targetFile);
				NUnit.Framework.Assert.Fail();
			}
			catch (org.apache.hadoop.io.nativeio.NativeIOException e)
			{
				if (org.apache.hadoop.fs.Path.WINDOWS)
				{
					NUnit.Framework.Assert.AreEqual(string.format("The system cannot find the file specified.%n"
						), e.Message);
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(org.apache.hadoop.io.nativeio.Errno.ENOENT, e.getErrno
						());
				}
			}
			// Test renaming a file to itself.  It should succeed and do nothing.
			java.io.File sourceFile = new java.io.File(TEST_DIR, "source");
			NUnit.Framework.Assert.IsTrue(sourceFile.createNewFile());
			org.apache.hadoop.io.nativeio.NativeIO.renameTo(sourceFile, sourceFile);
			// Test renaming a source to a destination.
			org.apache.hadoop.io.nativeio.NativeIO.renameTo(sourceFile, targetFile);
			// Test renaming a source to a path which uses a file as a directory.
			sourceFile = new java.io.File(TEST_DIR, "source");
			NUnit.Framework.Assert.IsTrue(sourceFile.createNewFile());
			java.io.File badTarget = new java.io.File(targetFile, "subdir");
			try
			{
				org.apache.hadoop.io.nativeio.NativeIO.renameTo(sourceFile, badTarget);
				NUnit.Framework.Assert.Fail();
			}
			catch (org.apache.hadoop.io.nativeio.NativeIOException e)
			{
				if (org.apache.hadoop.fs.Path.WINDOWS)
				{
					NUnit.Framework.Assert.AreEqual(string.format("The parameter is incorrect.%n"), e
						.Message);
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(org.apache.hadoop.io.nativeio.Errno.ENOTDIR, e.getErrno
						());
				}
			}
			org.apache.commons.io.FileUtils.deleteQuietly(TEST_DIR);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testMlock()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.io.nativeio.NativeIO.isAvailable
				());
			java.io.File TEST_FILE = new java.io.File(new java.io.File(Sharpen.Runtime.getProperty
				("test.build.data", "build/test/data")), "testMlockFile");
			int BUF_LEN = 12289;
			byte[] buf = new byte[BUF_LEN];
			int bufSum = 0;
			for (int i = 0; i < buf.Length; i++)
			{
				buf[i] = unchecked((byte)(i % 60));
				bufSum += buf[i];
			}
			java.io.FileOutputStream fos = new java.io.FileOutputStream(TEST_FILE);
			try
			{
				fos.write(buf);
				fos.getChannel().force(true);
			}
			finally
			{
				fos.close();
			}
			java.io.FileInputStream fis = null;
			java.nio.channels.FileChannel channel = null;
			try
			{
				// Map file into memory
				fis = new java.io.FileInputStream(TEST_FILE);
				channel = fis.getChannel();
				long fileSize = channel.size();
				java.nio.MappedByteBuffer mapbuf = channel.map(java.nio.channels.FileChannel.MapMode
					.READ_ONLY, 0, fileSize);
				// mlock the buffer
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.mlock(mapbuf, fileSize);
				// Read the buffer
				int sum = 0;
				for (int i_1 = 0; i_1 < fileSize; i_1++)
				{
					sum += mapbuf.get(i_1);
				}
				NUnit.Framework.Assert.AreEqual("Expected sums to be equal", bufSum, sum);
				// munmap the buffer, which also implicitly unlocks it
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.munmap(mapbuf);
			}
			finally
			{
				if (channel != null)
				{
					channel.close();
				}
				if (fis != null)
				{
					fis.close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGetMemlockLimit()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.io.nativeio.NativeIO.isAvailable
				());
			org.apache.hadoop.io.nativeio.NativeIO.getMemlockLimit();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testCopyFileUnbuffered()
		{
			string METHOD_NAME = org.apache.hadoop.test.GenericTestUtils.getMethodName();
			java.io.File srcFile = new java.io.File(TEST_DIR, METHOD_NAME + ".src.dat");
			java.io.File dstFile = new java.io.File(TEST_DIR, METHOD_NAME + ".dst.dat");
			int fileSize = unchecked((int)(0x8000000));
			// 128 MB
			int SEED = unchecked((int)(0xBEEF));
			int batchSize = 4096;
			int numBatches = fileSize / batchSize;
			java.util.Random rb = new java.util.Random(SEED);
			java.nio.channels.FileChannel channel = null;
			java.io.RandomAccessFile raSrcFile = null;
			try
			{
				raSrcFile = new java.io.RandomAccessFile(srcFile, "rw");
				channel = raSrcFile.getChannel();
				byte[] bytesToWrite = new byte[batchSize];
				java.nio.MappedByteBuffer mapBuf;
				mapBuf = channel.map(java.nio.channels.FileChannel.MapMode.READ_WRITE, 0, fileSize
					);
				for (int i = 0; i < numBatches; i++)
				{
					rb.nextBytes(bytesToWrite);
					mapBuf.put(bytesToWrite);
				}
				org.apache.hadoop.io.nativeio.NativeIO.copyFileUnbuffered(srcFile, dstFile);
				NUnit.Framework.Assert.AreEqual(srcFile.length(), dstFile.length());
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(LOG, channel);
				org.apache.hadoop.io.IOUtils.cleanup(LOG, raSrcFile);
				org.apache.commons.io.FileUtils.deleteQuietly(TEST_DIR);
			}
		}
	}
}
