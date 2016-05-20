using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	public class TestChRootedFs
	{
		internal org.apache.hadoop.fs.FileContextTestHelper fileContextTestHelper = new org.apache.hadoop.fs.FileContextTestHelper
			();

		internal org.apache.hadoop.fs.FileContext fc;

		internal org.apache.hadoop.fs.FileContext fcTarget;

		internal org.apache.hadoop.fs.Path chrootedTo;

		// The ChRoootedFs
		// 
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			// create the test root on local_fs
			fcTarget = org.apache.hadoop.fs.FileContext.getLocalFSFileContext();
			chrootedTo = fileContextTestHelper.getAbsoluteTestRootPath(fcTarget);
			// In case previous test was killed before cleanup
			fcTarget.delete(chrootedTo, true);
			fcTarget.mkdir(chrootedTo, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			// ChRoot to the root of the testDirectory
			fc = org.apache.hadoop.fs.FileContext.getFileContext(new org.apache.hadoop.fs.viewfs.ChRootedFs
				(fcTarget.getDefaultFileSystem(), chrootedTo), conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			fcTarget.delete(chrootedTo, true);
		}

		[NUnit.Framework.Test]
		public virtual void testBasicPaths()
		{
			java.net.URI uri = fc.getDefaultFileSystem().getUri();
			NUnit.Framework.Assert.AreEqual(chrootedTo.toUri(), uri);
			NUnit.Framework.Assert.AreEqual(fc.makeQualified(new org.apache.hadoop.fs.Path(Sharpen.Runtime
				.getProperty("user.home"))), fc.getWorkingDirectory());
			NUnit.Framework.Assert.AreEqual(fc.makeQualified(new org.apache.hadoop.fs.Path(Sharpen.Runtime
				.getProperty("user.home"))), fc.getHomeDirectory());
			/*
			* ChRootedFs as its uri like file:///chrootRoot.
			* This is questionable since path.makequalified(uri, path) ignores
			* the pathPart of a uri. So our notion of chrooted URI is questionable.
			* But if we were to fix Path#makeQualified() then  the next test should
			*  have been:
			
			Assert.assertEquals(
			new Path(chrootedTo + "/foo/bar").makeQualified(
			FsConstants.LOCAL_FS_URI, null),
			fc.makeQualified(new Path( "/foo/bar")));
			*/
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar").makeQualified
				(org.apache.hadoop.fs.FsConstants.LOCAL_FS_URI, null), fc.makeQualified(new org.apache.hadoop.fs.Path
				("/foo/bar")));
		}

		/// <summary>
		/// Test modify operations (create, mkdir, delete, etc)
		/// Verify the operation via chrootedfs (ie fc) and *also* via the
		/// target file system (ie fclocal) that has been chrooted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateDelete()
		{
			// Create file 
			fileContextTestHelper.createFileNonRecursive(fc, "/foo");
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fc
				, new org.apache.hadoop.fs.Path("/foo")));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fcTarget
				, new org.apache.hadoop.fs.Path(chrootedTo, "foo")));
			// Create file with recursive dir
			fileContextTestHelper.createFile(fc, "/newDir/foo");
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fc
				, new org.apache.hadoop.fs.Path("/newDir/foo")));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fcTarget
				, new org.apache.hadoop.fs.Path(chrootedTo, "newDir/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue(fc.delete(new org.apache.hadoop.fs.Path("/newDir/foo"
				), false));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, new org.apache.hadoop.fs.Path("/newDir/foo")));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fcTarget, new org.apache.hadoop.fs.Path(chrootedTo, "newDir/foo")));
			// Create file with a 2 component dirs recursively
			fileContextTestHelper.createFile(fc, "/newDir/newDir2/foo");
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fc
				, new org.apache.hadoop.fs.Path("/newDir/newDir2/foo")));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fcTarget
				, new org.apache.hadoop.fs.Path(chrootedTo, "newDir/newDir2/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue(fc.delete(new org.apache.hadoop.fs.Path("/newDir/newDir2/foo"
				), false));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, new org.apache.hadoop.fs.Path("/newDir/newDir2/foo")));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fcTarget, new org.apache.hadoop.fs.Path(chrootedTo, "newDir/newDir2/foo")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirDelete()
		{
			fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "/dirX"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc
				, new org.apache.hadoop.fs.Path("/dirX")));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fcTarget
				, new org.apache.hadoop.fs.Path(chrootedTo, "dirX")));
			fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "/dirX/dirY"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc
				, new org.apache.hadoop.fs.Path("/dirX/dirY")));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fcTarget
				, new org.apache.hadoop.fs.Path(chrootedTo, "dirX/dirY")));
			// Delete the created dir
			NUnit.Framework.Assert.IsTrue(fc.delete(new org.apache.hadoop.fs.Path("/dirX/dirY"
				), false));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, new org.apache.hadoop.fs.Path("/dirX/dirY")));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fcTarget, new org.apache.hadoop.fs.Path(chrootedTo, "dirX/dirY")));
			NUnit.Framework.Assert.IsTrue(fc.delete(new org.apache.hadoop.fs.Path("/dirX"), false
				));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, new org.apache.hadoop.fs.Path("/dirX")));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fcTarget, new org.apache.hadoop.fs.Path(chrootedTo, "dirX")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testRename()
		{
			// Rename a file
			fileContextTestHelper.createFile(fc, "/newDir/foo");
			fc.rename(new org.apache.hadoop.fs.Path("/newDir/foo"), new org.apache.hadoop.fs.Path
				("/newDir/fooBar"));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, new org.apache.hadoop.fs.Path("/newDir/foo")));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fcTarget, new org.apache.hadoop.fs.Path(chrootedTo, "newDir/foo")));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fc
				, fileContextTestHelper.getTestRootPath(fc, "/newDir/fooBar")));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fcTarget
				, new org.apache.hadoop.fs.Path(chrootedTo, "newDir/fooBar")));
			// Rename a dir
			fc.mkdir(new org.apache.hadoop.fs.Path("/newDir/dirFoo"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			fc.rename(new org.apache.hadoop.fs.Path("/newDir/dirFoo"), new org.apache.hadoop.fs.Path
				("/newDir/dirFooBar"));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, new org.apache.hadoop.fs.Path("/newDir/dirFoo")));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fcTarget, new org.apache.hadoop.fs.Path(chrootedTo, "newDir/dirFoo")));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc
				, fileContextTestHelper.getTestRootPath(fc, "/newDir/dirFooBar")));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fcTarget
				, new org.apache.hadoop.fs.Path(chrootedTo, "newDir/dirFooBar")));
		}

		/// <summary>
		/// We would have liked renames across file system to fail but
		/// Unfortunately there is not way to distinguish the two file systems
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testRenameAcrossFs()
		{
			fc.mkdir(new org.apache.hadoop.fs.Path("/newDir/dirFoo"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, true);
			// the root will get interpreted to the root of the chrooted fs.
			fc.rename(new org.apache.hadoop.fs.Path("/newDir/dirFoo"), new org.apache.hadoop.fs.Path
				("file:///dirFooBar"));
			org.apache.hadoop.fs.FileContextTestHelper.isDir(fc, new org.apache.hadoop.fs.Path
				("/dirFooBar"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testList()
		{
			org.apache.hadoop.fs.FileStatus fs = fc.getFileStatus(new org.apache.hadoop.fs.Path
				("/"));
			NUnit.Framework.Assert.IsTrue(fs.isDirectory());
			//  should return the full path not the chrooted path
			NUnit.Framework.Assert.AreEqual(fs.getPath(), chrootedTo);
			// list on Slash
			org.apache.hadoop.fs.FileStatus[] dirPaths = fc.util().listStatus(new org.apache.hadoop.fs.Path
				("/"));
			NUnit.Framework.Assert.AreEqual(0, dirPaths.Length);
			fileContextTestHelper.createFileNonRecursive(fc, "/foo");
			fileContextTestHelper.createFileNonRecursive(fc, "/bar");
			fc.mkdir(new org.apache.hadoop.fs.Path("/dirX"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "/dirY"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			fc.mkdir(new org.apache.hadoop.fs.Path("/dirX/dirXX"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			dirPaths = fc.util().listStatus(new org.apache.hadoop.fs.Path("/"));
			NUnit.Framework.Assert.AreEqual(4, dirPaths.Length);
			// Note the the file status paths are the full paths on target
			fs = fileContextTestHelper.containsPath(fcTarget, "foo", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.isFile());
			fs = fileContextTestHelper.containsPath(fcTarget, "bar", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.isFile());
			fs = fileContextTestHelper.containsPath(fcTarget, "dirX", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.isDirectory());
			fs = fileContextTestHelper.containsPath(fcTarget, "dirY", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.isDirectory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWorkingDirectory()
		{
			// First we cd to our test root
			fc.mkdir(new org.apache.hadoop.fs.Path("/testWd"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			org.apache.hadoop.fs.Path workDir = new org.apache.hadoop.fs.Path("/testWd");
			org.apache.hadoop.fs.Path fqWd = fc.makeQualified(workDir);
			fc.setWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(fqWd, fc.getWorkingDirectory());
			fc.setWorkingDirectory(new org.apache.hadoop.fs.Path("."));
			NUnit.Framework.Assert.AreEqual(fqWd, fc.getWorkingDirectory());
			fc.setWorkingDirectory(new org.apache.hadoop.fs.Path(".."));
			NUnit.Framework.Assert.AreEqual(fqWd.getParent(), fc.getWorkingDirectory());
			// cd using a relative path
			// Go back to our test root
			workDir = new org.apache.hadoop.fs.Path("/testWd");
			fqWd = fc.makeQualified(workDir);
			fc.setWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(fqWd, fc.getWorkingDirectory());
			org.apache.hadoop.fs.Path relativeDir = new org.apache.hadoop.fs.Path("existingDir1"
				);
			org.apache.hadoop.fs.Path absoluteDir = new org.apache.hadoop.fs.Path(workDir, "existingDir1"
				);
			fc.mkdir(absoluteDir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			org.apache.hadoop.fs.Path fqAbsoluteDir = fc.makeQualified(absoluteDir);
			fc.setWorkingDirectory(relativeDir);
			NUnit.Framework.Assert.AreEqual(fqAbsoluteDir, fc.getWorkingDirectory());
			// cd using a absolute path
			absoluteDir = new org.apache.hadoop.fs.Path("/test/existingDir2");
			fqAbsoluteDir = fc.makeQualified(absoluteDir);
			fc.mkdir(absoluteDir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			fc.setWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(fqAbsoluteDir, fc.getWorkingDirectory());
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
			absoluteDir = fileContextTestHelper.getTestRootPath(fc, "nonexistingPath");
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
			string LOCAL_FS_ROOT_URI = "file:///tmp/test";
			absoluteDir = new org.apache.hadoop.fs.Path(LOCAL_FS_ROOT_URI + "/existingDir");
			fc.mkdir(absoluteDir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			fc.setWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fc.getWorkingDirectory());
		}

		/*
		* Test resolvePath(p)
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testResolvePath()
		{
			NUnit.Framework.Assert.AreEqual(chrootedTo, fc.getDefaultFileSystem().resolvePath
				(new org.apache.hadoop.fs.Path("/")));
			fileContextTestHelper.createFile(fc, "/foo");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(chrootedTo, "foo"), 
				fc.getDefaultFileSystem().resolvePath(new org.apache.hadoop.fs.Path("/foo")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testResolvePathNonExisting()
		{
			fc.getDefaultFileSystem().resolvePath(new org.apache.hadoop.fs.Path("/nonExisting"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testIsValidNameValidInBaseFs()
		{
			org.apache.hadoop.fs.AbstractFileSystem baseFs = org.mockito.Mockito.spy(fc.getDefaultFileSystem
				());
			org.apache.hadoop.fs.viewfs.ChRootedFs chRootedFs = new org.apache.hadoop.fs.viewfs.ChRootedFs
				(baseFs, new org.apache.hadoop.fs.Path("/chroot"));
			org.mockito.Mockito.doReturn(true).when(baseFs).isValidName(org.mockito.Mockito.anyString
				());
			NUnit.Framework.Assert.IsTrue(chRootedFs.isValidName("/test"));
			org.mockito.Mockito.verify(baseFs).isValidName("/chroot/test");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testIsValidNameInvalidInBaseFs()
		{
			org.apache.hadoop.fs.AbstractFileSystem baseFs = org.mockito.Mockito.spy(fc.getDefaultFileSystem
				());
			org.apache.hadoop.fs.viewfs.ChRootedFs chRootedFs = new org.apache.hadoop.fs.viewfs.ChRootedFs
				(baseFs, new org.apache.hadoop.fs.Path("/chroot"));
			org.mockito.Mockito.doReturn(false).when(baseFs).isValidName(org.mockito.Mockito.
				anyString());
			NUnit.Framework.Assert.IsFalse(chRootedFs.isValidName("/test"));
			org.mockito.Mockito.verify(baseFs).isValidName("/chroot/test");
		}
	}
}
