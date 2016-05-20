using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	public class TestChRootedFileSystem
	{
		internal org.apache.hadoop.fs.FileSystem fSys;

		internal org.apache.hadoop.fs.FileSystem fSysTarget;

		internal org.apache.hadoop.fs.Path chrootedTo;

		internal org.apache.hadoop.fs.FileSystemTestHelper fileSystemTestHelper;

		// The ChRoootedFs
		//
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			// create the test root on local_fs
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			fSysTarget = org.apache.hadoop.fs.FileSystem.getLocal(conf);
			fileSystemTestHelper = new org.apache.hadoop.fs.FileSystemTestHelper();
			chrootedTo = fileSystemTestHelper.getAbsoluteTestRootPath(fSysTarget);
			// In case previous test was killed before cleanup
			fSysTarget.delete(chrootedTo, true);
			fSysTarget.mkdirs(chrootedTo);
			// ChRoot to the root of the testDirectory
			fSys = new org.apache.hadoop.fs.viewfs.ChRootedFileSystem(chrootedTo.toUri(), conf
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			fSysTarget.delete(chrootedTo, true);
		}

		[NUnit.Framework.Test]
		public virtual void testURI()
		{
			java.net.URI uri = fSys.getUri();
			NUnit.Framework.Assert.AreEqual(chrootedTo.toUri(), uri);
		}

		[NUnit.Framework.Test]
		public virtual void testBasicPaths()
		{
			java.net.URI uri = fSys.getUri();
			NUnit.Framework.Assert.AreEqual(chrootedTo.toUri(), uri);
			NUnit.Framework.Assert.AreEqual(fSys.makeQualified(new org.apache.hadoop.fs.Path(
				Sharpen.Runtime.getProperty("user.home"))), fSys.getWorkingDirectory());
			NUnit.Framework.Assert.AreEqual(fSys.makeQualified(new org.apache.hadoop.fs.Path(
				Sharpen.Runtime.getProperty("user.home"))), fSys.getHomeDirectory());
			/*
			* ChRootedFs as its uri like file:///chrootRoot.
			* This is questionable since path.makequalified(uri, path) ignores
			* the pathPart of a uri. So our notion of chrooted URI is questionable.
			* But if we were to fix Path#makeQualified() then  the next test should
			*  have been:
			
			Assert.assertEquals(
			new Path(chrootedTo + "/foo/bar").makeQualified(
			FsConstants.LOCAL_FS_URI, null),
			fSys.makeQualified(new Path( "/foo/bar")));
			*/
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar").makeQualified
				(org.apache.hadoop.fs.FsConstants.LOCAL_FS_URI, null), fSys.makeQualified(new org.apache.hadoop.fs.Path
				("/foo/bar")));
		}

		/// <summary>
		/// Test modify operations (create, mkdir, delete, etc)
		/// Verify the operation via chrootedfs (ie fSys) and *also* via the
		/// target file system (ie fSysTarget) that has been chrooted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateDelete()
		{
			// Create file 
			fileSystemTestHelper.createFile(fSys, "/foo");
			NUnit.Framework.Assert.IsTrue(fSys.isFile(new org.apache.hadoop.fs.Path("/foo")));
			NUnit.Framework.Assert.IsTrue(fSysTarget.isFile(new org.apache.hadoop.fs.Path(chrootedTo
				, "foo")));
			// Create file with recursive dir
			fileSystemTestHelper.createFile(fSys, "/newDir/foo");
			NUnit.Framework.Assert.IsTrue(fSys.isFile(new org.apache.hadoop.fs.Path("/newDir/foo"
				)));
			NUnit.Framework.Assert.IsTrue(fSysTarget.isFile(new org.apache.hadoop.fs.Path(chrootedTo
				, "newDir/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue(fSys.delete(new org.apache.hadoop.fs.Path("/newDir/foo"
				), false));
			NUnit.Framework.Assert.IsFalse(fSys.exists(new org.apache.hadoop.fs.Path("/newDir/foo"
				)));
			NUnit.Framework.Assert.IsFalse(fSysTarget.exists(new org.apache.hadoop.fs.Path(chrootedTo
				, "newDir/foo")));
			// Create file with a 2 component dirs recursively
			fileSystemTestHelper.createFile(fSys, "/newDir/newDir2/foo");
			NUnit.Framework.Assert.IsTrue(fSys.isFile(new org.apache.hadoop.fs.Path("/newDir/newDir2/foo"
				)));
			NUnit.Framework.Assert.IsTrue(fSysTarget.isFile(new org.apache.hadoop.fs.Path(chrootedTo
				, "newDir/newDir2/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue(fSys.delete(new org.apache.hadoop.fs.Path("/newDir/newDir2/foo"
				), false));
			NUnit.Framework.Assert.IsFalse(fSys.exists(new org.apache.hadoop.fs.Path("/newDir/newDir2/foo"
				)));
			NUnit.Framework.Assert.IsFalse(fSysTarget.exists(new org.apache.hadoop.fs.Path(chrootedTo
				, "newDir/newDir2/foo")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirDelete()
		{
			fSys.mkdirs(fileSystemTestHelper.getTestRootPath(fSys, "/dirX"));
			NUnit.Framework.Assert.IsTrue(fSys.isDirectory(new org.apache.hadoop.fs.Path("/dirX"
				)));
			NUnit.Framework.Assert.IsTrue(fSysTarget.isDirectory(new org.apache.hadoop.fs.Path
				(chrootedTo, "dirX")));
			fSys.mkdirs(fileSystemTestHelper.getTestRootPath(fSys, "/dirX/dirY"));
			NUnit.Framework.Assert.IsTrue(fSys.isDirectory(new org.apache.hadoop.fs.Path("/dirX/dirY"
				)));
			NUnit.Framework.Assert.IsTrue(fSysTarget.isDirectory(new org.apache.hadoop.fs.Path
				(chrootedTo, "dirX/dirY")));
			// Delete the created dir
			NUnit.Framework.Assert.IsTrue(fSys.delete(new org.apache.hadoop.fs.Path("/dirX/dirY"
				), false));
			NUnit.Framework.Assert.IsFalse(fSys.exists(new org.apache.hadoop.fs.Path("/dirX/dirY"
				)));
			NUnit.Framework.Assert.IsFalse(fSysTarget.exists(new org.apache.hadoop.fs.Path(chrootedTo
				, "dirX/dirY")));
			NUnit.Framework.Assert.IsTrue(fSys.delete(new org.apache.hadoop.fs.Path("/dirX"), 
				false));
			NUnit.Framework.Assert.IsFalse(fSys.exists(new org.apache.hadoop.fs.Path("/dirX")
				));
			NUnit.Framework.Assert.IsFalse(fSysTarget.exists(new org.apache.hadoop.fs.Path(chrootedTo
				, "dirX")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testRename()
		{
			// Rename a file
			fileSystemTestHelper.createFile(fSys, "/newDir/foo");
			fSys.rename(new org.apache.hadoop.fs.Path("/newDir/foo"), new org.apache.hadoop.fs.Path
				("/newDir/fooBar"));
			NUnit.Framework.Assert.IsFalse(fSys.exists(new org.apache.hadoop.fs.Path("/newDir/foo"
				)));
			NUnit.Framework.Assert.IsFalse(fSysTarget.exists(new org.apache.hadoop.fs.Path(chrootedTo
				, "newDir/foo")));
			NUnit.Framework.Assert.IsTrue(fSys.isFile(fileSystemTestHelper.getTestRootPath(fSys
				, "/newDir/fooBar")));
			NUnit.Framework.Assert.IsTrue(fSysTarget.isFile(new org.apache.hadoop.fs.Path(chrootedTo
				, "newDir/fooBar")));
			// Rename a dir
			fSys.mkdirs(new org.apache.hadoop.fs.Path("/newDir/dirFoo"));
			fSys.rename(new org.apache.hadoop.fs.Path("/newDir/dirFoo"), new org.apache.hadoop.fs.Path
				("/newDir/dirFooBar"));
			NUnit.Framework.Assert.IsFalse(fSys.exists(new org.apache.hadoop.fs.Path("/newDir/dirFoo"
				)));
			NUnit.Framework.Assert.IsFalse(fSysTarget.exists(new org.apache.hadoop.fs.Path(chrootedTo
				, "newDir/dirFoo")));
			NUnit.Framework.Assert.IsTrue(fSys.isDirectory(fileSystemTestHelper.getTestRootPath
				(fSys, "/newDir/dirFooBar")));
			NUnit.Framework.Assert.IsTrue(fSysTarget.isDirectory(new org.apache.hadoop.fs.Path
				(chrootedTo, "newDir/dirFooBar")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetContentSummary()
		{
			// GetContentSummary of a dir
			fSys.mkdirs(new org.apache.hadoop.fs.Path("/newDir/dirFoo"));
			org.apache.hadoop.fs.ContentSummary cs = fSys.getContentSummary(new org.apache.hadoop.fs.Path
				("/newDir/dirFoo"));
			NUnit.Framework.Assert.AreEqual(-1L, cs.getQuota());
			NUnit.Framework.Assert.AreEqual(-1L, cs.getSpaceQuota());
		}

		/// <summary>
		/// We would have liked renames across file system to fail but
		/// Unfortunately there is not way to distinguish the two file systems
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testRenameAcrossFs()
		{
			fSys.mkdirs(new org.apache.hadoop.fs.Path("/newDir/dirFoo"));
			fSys.rename(new org.apache.hadoop.fs.Path("/newDir/dirFoo"), new org.apache.hadoop.fs.Path
				("file:///tmp/dirFooBar"));
			org.apache.hadoop.fs.FileSystemTestHelper.isDir(fSys, new org.apache.hadoop.fs.Path
				("/tmp/dirFooBar"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testList()
		{
			org.apache.hadoop.fs.FileStatus fs = fSys.getFileStatus(new org.apache.hadoop.fs.Path
				("/"));
			NUnit.Framework.Assert.IsTrue(fs.isDirectory());
			//  should return the full path not the chrooted path
			NUnit.Framework.Assert.AreEqual(fs.getPath(), chrootedTo);
			// list on Slash
			org.apache.hadoop.fs.FileStatus[] dirPaths = fSys.listStatus(new org.apache.hadoop.fs.Path
				("/"));
			NUnit.Framework.Assert.AreEqual(0, dirPaths.Length);
			fileSystemTestHelper.createFile(fSys, "/foo");
			fileSystemTestHelper.createFile(fSys, "/bar");
			fSys.mkdirs(new org.apache.hadoop.fs.Path("/dirX"));
			fSys.mkdirs(fileSystemTestHelper.getTestRootPath(fSys, "/dirY"));
			fSys.mkdirs(new org.apache.hadoop.fs.Path("/dirX/dirXX"));
			dirPaths = fSys.listStatus(new org.apache.hadoop.fs.Path("/"));
			NUnit.Framework.Assert.AreEqual(4, dirPaths.Length);
			// note 2 crc files
			// Note the the file status paths are the full paths on target
			fs = org.apache.hadoop.fs.FileSystemTestHelper.containsPath(new org.apache.hadoop.fs.Path
				(chrootedTo, "foo"), dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.isFile());
			fs = org.apache.hadoop.fs.FileSystemTestHelper.containsPath(new org.apache.hadoop.fs.Path
				(chrootedTo, "bar"), dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.isFile());
			fs = org.apache.hadoop.fs.FileSystemTestHelper.containsPath(new org.apache.hadoop.fs.Path
				(chrootedTo, "dirX"), dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.isDirectory());
			fs = org.apache.hadoop.fs.FileSystemTestHelper.containsPath(new org.apache.hadoop.fs.Path
				(chrootedTo, "dirY"), dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.isDirectory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWorkingDirectory()
		{
			// First we cd to our test root
			fSys.mkdirs(new org.apache.hadoop.fs.Path("/testWd"));
			org.apache.hadoop.fs.Path workDir = new org.apache.hadoop.fs.Path("/testWd");
			fSys.setWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(workDir, fSys.getWorkingDirectory());
			fSys.setWorkingDirectory(new org.apache.hadoop.fs.Path("."));
			NUnit.Framework.Assert.AreEqual(workDir, fSys.getWorkingDirectory());
			fSys.setWorkingDirectory(new org.apache.hadoop.fs.Path(".."));
			NUnit.Framework.Assert.AreEqual(workDir.getParent(), fSys.getWorkingDirectory());
			// cd using a relative path
			// Go back to our test root
			workDir = new org.apache.hadoop.fs.Path("/testWd");
			fSys.setWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(workDir, fSys.getWorkingDirectory());
			org.apache.hadoop.fs.Path relativeDir = new org.apache.hadoop.fs.Path("existingDir1"
				);
			org.apache.hadoop.fs.Path absoluteDir = new org.apache.hadoop.fs.Path(workDir, "existingDir1"
				);
			fSys.mkdirs(absoluteDir);
			fSys.setWorkingDirectory(relativeDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fSys.getWorkingDirectory());
			// cd using a absolute path
			absoluteDir = new org.apache.hadoop.fs.Path("/test/existingDir2");
			fSys.mkdirs(absoluteDir);
			fSys.setWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fSys.getWorkingDirectory());
			// Now open a file relative to the wd we just set above.
			org.apache.hadoop.fs.Path absoluteFooPath = new org.apache.hadoop.fs.Path(absoluteDir
				, "foo");
			fSys.create(absoluteFooPath).close();
			fSys.open(new org.apache.hadoop.fs.Path("foo")).close();
			// Now mkdir relative to the dir we cd'ed to
			fSys.mkdirs(new org.apache.hadoop.fs.Path("newDir"));
			NUnit.Framework.Assert.IsTrue(fSys.isDirectory(new org.apache.hadoop.fs.Path(absoluteDir
				, "newDir")));
			/* Filesystem impls (RawLocal and DistributedFileSystem do not check
			* for existing of working dir
			absoluteDir = getTestRootPath(fSys, "nonexistingPath");
			try {
			fSys.setWorkingDirectory(absoluteDir);
			Assert.fail("cd to non existing dir should have failed");
			} catch (Exception e) {
			// Exception as expected
			}
			*/
			// Try a URI
			string LOCAL_FS_ROOT_URI = "file:///tmp/test";
			absoluteDir = new org.apache.hadoop.fs.Path(LOCAL_FS_ROOT_URI + "/existingDir");
			fSys.mkdirs(absoluteDir);
			fSys.setWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fSys.getWorkingDirectory());
		}

		/*
		* Test resolvePath(p)
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testResolvePath()
		{
			NUnit.Framework.Assert.AreEqual(chrootedTo, fSys.resolvePath(new org.apache.hadoop.fs.Path
				("/")));
			fileSystemTestHelper.createFile(fSys, "/foo");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(chrootedTo, "foo"), 
				fSys.resolvePath(new org.apache.hadoop.fs.Path("/foo")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testResolvePathNonExisting()
		{
			fSys.resolvePath(new org.apache.hadoop.fs.Path("/nonExisting"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteOnExitPathHandling()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setClass("fs.mockfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.viewfs.TestChRootedFileSystem.MockFileSystem
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			java.net.URI chrootUri = java.net.URI.create("mockfs://foo/a/b");
			org.apache.hadoop.fs.viewfs.ChRootedFileSystem chrootFs = new org.apache.hadoop.fs.viewfs.ChRootedFileSystem
				(chrootUri, conf);
			org.apache.hadoop.fs.FileSystem mockFs = ((org.apache.hadoop.fs.FilterFileSystem)
				chrootFs.getRawFileSystem()).getRawFileSystem();
			// ensure delete propagates the correct path
			org.apache.hadoop.fs.Path chrootPath = new org.apache.hadoop.fs.Path("/c");
			org.apache.hadoop.fs.Path rawPath = new org.apache.hadoop.fs.Path("/a/b/c");
			chrootFs.delete(chrootPath, false);
			org.mockito.Mockito.verify(mockFs).delete(eq(rawPath), eq(false));
			org.mockito.Mockito.reset(mockFs);
			// fake that the path exists for deleteOnExit
			org.apache.hadoop.fs.FileStatus stat = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileStatus
				>();
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(rawPath))).thenReturn(stat);
			// ensure deleteOnExit propagates the correct path
			chrootFs.deleteOnExit(chrootPath);
			chrootFs.close();
			org.mockito.Mockito.verify(mockFs).delete(eq(rawPath), eq(true));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testURIEmptyPath()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setClass("fs.mockfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.viewfs.TestChRootedFileSystem.MockFileSystem
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			java.net.URI chrootUri = java.net.URI.create("mockfs://foo");
			new org.apache.hadoop.fs.viewfs.ChRootedFileSystem(chrootUri, conf);
		}

		/// <summary>
		/// Tests that ChRootedFileSystem delegates calls for every ACL method to the
		/// underlying FileSystem with all Path arguments translated as required to
		/// enforce chroot.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testAclMethodsPathTranslation()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setClass("fs.mockfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.viewfs.TestChRootedFileSystem.MockFileSystem
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			java.net.URI chrootUri = java.net.URI.create("mockfs://foo/a/b");
			org.apache.hadoop.fs.viewfs.ChRootedFileSystem chrootFs = new org.apache.hadoop.fs.viewfs.ChRootedFileSystem
				(chrootUri, conf);
			org.apache.hadoop.fs.FileSystem mockFs = ((org.apache.hadoop.fs.FilterFileSystem)
				chrootFs.getRawFileSystem()).getRawFileSystem();
			org.apache.hadoop.fs.Path chrootPath = new org.apache.hadoop.fs.Path("/c");
			org.apache.hadoop.fs.Path rawPath = new org.apache.hadoop.fs.Path("/a/b/c");
			System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> entries
				 = java.util.Collections.emptyList();
			chrootFs.modifyAclEntries(chrootPath, entries);
			org.mockito.Mockito.verify(mockFs).modifyAclEntries(rawPath, entries);
			chrootFs.removeAclEntries(chrootPath, entries);
			org.mockito.Mockito.verify(mockFs).removeAclEntries(rawPath, entries);
			chrootFs.removeDefaultAcl(chrootPath);
			org.mockito.Mockito.verify(mockFs).removeDefaultAcl(rawPath);
			chrootFs.removeAcl(chrootPath);
			org.mockito.Mockito.verify(mockFs).removeAcl(rawPath);
			chrootFs.setAcl(chrootPath, entries);
			org.mockito.Mockito.verify(mockFs).setAcl(rawPath, entries);
			chrootFs.getAclStatus(chrootPath);
			org.mockito.Mockito.verify(mockFs).getAclStatus(rawPath);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testListLocatedFileStatus()
		{
			org.apache.hadoop.fs.Path mockMount = new org.apache.hadoop.fs.Path("mockfs://foo/user"
				);
			org.apache.hadoop.fs.Path mockPath = new org.apache.hadoop.fs.Path("/usermock");
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setClass("fs.mockfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.viewfs.TestChRootedFileSystem.MockFileSystem
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, mockPath.ToString(), mockMount
				.toUri());
			org.apache.hadoop.fs.FileSystem vfs = org.apache.hadoop.fs.FileSystem.get(java.net.URI
				.create("viewfs:///"), conf);
			vfs.listLocatedStatus(mockPath);
			org.apache.hadoop.fs.FileSystem mockFs = ((org.apache.hadoop.fs.viewfs.TestChRootedFileSystem.MockFileSystem
				)mockMount.getFileSystem(conf)).getRawFileSystem();
			org.mockito.Mockito.verify(mockFs).listLocatedStatus(new org.apache.hadoop.fs.Path
				(mockMount.toUri().getPath()));
		}

		internal class MockFileSystem : org.apache.hadoop.fs.FilterFileSystem
		{
			internal MockFileSystem()
				: base(org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem>())
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void initialize(java.net.URI name, org.apache.hadoop.conf.Configuration
				 conf)
			{
			}
		}
	}
}
