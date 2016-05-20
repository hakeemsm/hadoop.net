using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>
	/// <p>
	/// A collection of tests for the
	/// <see cref="ViewFs"/>
	/// .
	/// This test should be used for testing ViewFs that has mount links to
	/// a target file system such  localFs or Hdfs etc.
	/// </p>
	/// <p>
	/// To test a given target file system create a subclass of this
	/// test and override
	/// <see cref="setUp()"/>
	/// to initialize the <code>fcTarget</code>
	/// to point to the file system to which you want the mount targets
	/// Since this a junit 4 you can also do a single setup before
	/// the start of any tests.
	/// E.g.
	/// </summary>
	/// <BeforeClass>public static void clusterSetupAtBegining()</BeforeClass>
	/// <AfterClass>
	/// public static void ClusterShutdownAtEnd()
	/// </p>
	/// </AfterClass>
	public class ViewFsBaseTest
	{
		internal org.apache.hadoop.fs.FileContext fcView;

		internal org.apache.hadoop.fs.FileContext fcTarget;

		internal org.apache.hadoop.fs.Path targetTestRoot;

		internal org.apache.hadoop.conf.Configuration conf;

		internal org.apache.hadoop.fs.FileContext xfcViewWithAuthority;

		internal java.net.URI schemeWithAuthority;

		internal readonly org.apache.hadoop.fs.FileContextTestHelper fileContextTestHelper;

		// the view file system - the mounts are here
		// the target file system - the mount will point here
		// same as fsView but with authority
		protected internal virtual org.apache.hadoop.fs.FileContextTestHelper createFileContextHelper
			()
		{
			return new org.apache.hadoop.fs.FileContextTestHelper();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			initializeTargetTestRoot();
			// Make  user and data dirs - we creates links to them in the mount table
			fcTarget.mkdir(new org.apache.hadoop.fs.Path(targetTestRoot, "user"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, true);
			fcTarget.mkdir(new org.apache.hadoop.fs.Path(targetTestRoot, "data"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, true);
			fcTarget.mkdir(new org.apache.hadoop.fs.Path(targetTestRoot, "dir2"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, true);
			fcTarget.mkdir(new org.apache.hadoop.fs.Path(targetTestRoot, "dir3"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, true);
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fcTarget, new org.apache.hadoop.fs.Path
				(targetTestRoot, "aFile"));
			// Now we use the mount fs to set links to user and dir
			// in the test root
			// Set up the defaultMT in the config with our mount point links
			conf = new org.apache.hadoop.conf.Configuration();
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/targetRoot", targetTestRoot
				.toUri());
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/user", new org.apache.hadoop.fs.Path
				(targetTestRoot, "user").toUri());
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/user2", new org.apache.hadoop.fs.Path
				(targetTestRoot, "user").toUri());
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/data", new org.apache.hadoop.fs.Path
				(targetTestRoot, "data").toUri());
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/internalDir/linkToDir2", new 
				org.apache.hadoop.fs.Path(targetTestRoot, "dir2").toUri());
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/internalDir/internalDir2/linkToDir3"
				, new org.apache.hadoop.fs.Path(targetTestRoot, "dir3").toUri());
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/danglingLink", new org.apache.hadoop.fs.Path
				(targetTestRoot, "missingTarget").toUri());
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/linkToAFile", new org.apache.hadoop.fs.Path
				(targetTestRoot, "aFile").toUri());
			fcView = org.apache.hadoop.fs.FileContext.getFileContext(org.apache.hadoop.fs.FsConstants
				.VIEWFS_URI, conf);
		}

		// Also try viewfs://default/    - note authority is name of mount table
		/// <exception cref="System.IO.IOException"/>
		internal virtual void initializeTargetTestRoot()
		{
			targetTestRoot = fileContextTestHelper.getAbsoluteTestRootPath(fcTarget);
			// In case previous test was killed before cleanup
			fcTarget.delete(targetTestRoot, true);
			fcTarget.mkdir(targetTestRoot, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			fcTarget.delete(fileContextTestHelper.getTestRootPath(fcTarget), true);
		}

		[NUnit.Framework.Test]
		public virtual void testGetMountPoints()
		{
			org.apache.hadoop.fs.viewfs.ViewFs viewfs = (org.apache.hadoop.fs.viewfs.ViewFs)fcView
				.getDefaultFileSystem();
			org.apache.hadoop.fs.viewfs.ViewFs.MountPoint[] mountPoints = viewfs.getMountPoints
				();
			NUnit.Framework.Assert.AreEqual(8, mountPoints.Length);
		}

		internal virtual int getExpectedDelegationTokenCount()
		{
			return 0;
		}

		/// <summary>
		/// This default implementation is when viewfs has mount points
		/// into file systems, such as LocalFs that do no have delegation tokens.
		/// </summary>
		/// <remarks>
		/// This default implementation is when viewfs has mount points
		/// into file systems, such as LocalFs that do no have delegation tokens.
		/// It should be overridden for when mount points into hdfs.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetDelegationTokens()
		{
			System.Collections.Generic.IList<org.apache.hadoop.security.token.Token<object>> 
				delTokens = fcView.getDelegationTokens(new org.apache.hadoop.fs.Path("/"), "sanjay"
				);
			NUnit.Framework.Assert.AreEqual(getExpectedDelegationTokenCount(), delTokens.Count
				);
		}

		[NUnit.Framework.Test]
		public virtual void testBasicPaths()
		{
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.FsConstants.VIEWFS_URI, fcView
				.getDefaultFileSystem().getUri());
			NUnit.Framework.Assert.AreEqual(fcView.makeQualified(new org.apache.hadoop.fs.Path
				("/user/" + Sharpen.Runtime.getProperty("user.name"))), fcView.getWorkingDirectory
				());
			NUnit.Framework.Assert.AreEqual(fcView.makeQualified(new org.apache.hadoop.fs.Path
				("/user/" + Sharpen.Runtime.getProperty("user.name"))), fcView.getHomeDirectory(
				));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar").makeQualified
				(org.apache.hadoop.fs.FsConstants.VIEWFS_URI, null), fcView.makeQualified(new org.apache.hadoop.fs.Path
				("/foo/bar")));
		}

		/// <summary>
		/// Test modify operations (create, mkdir, delete, etc)
		/// on the mount file system where the pathname references through
		/// the mount points.
		/// </summary>
		/// <remarks>
		/// Test modify operations (create, mkdir, delete, etc)
		/// on the mount file system where the pathname references through
		/// the mount points.  Hence these operation will modify the target
		/// file system.
		/// Verify the operation via mountfs (ie fc) and *also* via the
		/// target file system (ie fclocal) that the mount link points-to.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testOperationsThroughMountLinks()
		{
			// Create file 
			fileContextTestHelper.createFileNonRecursive(fcView, "/user/foo");
			NUnit.Framework.Assert.IsTrue("Create file should be file", org.apache.hadoop.fs.FileContextTestHelper.isFile
				(fcView, new org.apache.hadoop.fs.Path("/user/foo")));
			NUnit.Framework.Assert.IsTrue("Target of created file should be type file", org.apache.hadoop.fs.FileContextTestHelper.isFile
				(fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "user/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue("Delete should succeed", fcView.delete(new org.apache.hadoop.fs.Path
				("/user/foo"), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcView, new org.apache.hadoop.fs.Path("/user/foo")));
			NUnit.Framework.Assert.IsFalse("Target File should not exist after delete", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "user/foo")));
			// Create file with a 2 component dirs
			fileContextTestHelper.createFileNonRecursive(fcView, "/internalDir/linkToDir2/foo"
				);
			NUnit.Framework.Assert.IsTrue("Created file should be type file", org.apache.hadoop.fs.FileContextTestHelper.isFile
				(fcView, new org.apache.hadoop.fs.Path("/internalDir/linkToDir2/foo")));
			NUnit.Framework.Assert.IsTrue("Target of created file should be type file", org.apache.hadoop.fs.FileContextTestHelper.isFile
				(fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "dir2/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue("Delete should suceed", fcView.delete(new org.apache.hadoop.fs.Path
				("/internalDir/linkToDir2/foo"), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after deletion", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcView, new org.apache.hadoop.fs.Path("/internalDir/linkToDir2/foo")));
			NUnit.Framework.Assert.IsFalse("Target should not exist after deletion", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "dir2/foo")));
			// Create file with a 3 component dirs
			fileContextTestHelper.createFileNonRecursive(fcView, "/internalDir/internalDir2/linkToDir3/foo"
				);
			NUnit.Framework.Assert.IsTrue("Created file should be of type file", org.apache.hadoop.fs.FileContextTestHelper.isFile
				(fcView, new org.apache.hadoop.fs.Path("/internalDir/internalDir2/linkToDir3/foo"
				)));
			NUnit.Framework.Assert.IsTrue("Target of created file should also be type file", 
				org.apache.hadoop.fs.FileContextTestHelper.isFile(fcTarget, new org.apache.hadoop.fs.Path
				(targetTestRoot, "dir3/foo")));
			// Recursive Create file with missing dirs
			fileContextTestHelper.createFile(fcView, "/internalDir/linkToDir2/missingDir/miss2/foo"
				);
			NUnit.Framework.Assert.IsTrue("Created file should be of type file", org.apache.hadoop.fs.FileContextTestHelper.isFile
				(fcView, new org.apache.hadoop.fs.Path("/internalDir/linkToDir2/missingDir/miss2/foo"
				)));
			NUnit.Framework.Assert.IsTrue("Target of created file should also be type file", 
				org.apache.hadoop.fs.FileContextTestHelper.isFile(fcTarget, new org.apache.hadoop.fs.Path
				(targetTestRoot, "dir2/missingDir/miss2/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue("Delete should succeed", fcView.delete(new org.apache.hadoop.fs.Path
				("/internalDir/internalDir2/linkToDir3/foo"), false));
			NUnit.Framework.Assert.IsFalse("Deleted File should not exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcView, new org.apache.hadoop.fs.Path("/internalDir/internalDir2/linkToDir3/foo"
				)));
			NUnit.Framework.Assert.IsFalse("Target of deleted file should not exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "dir3/foo")));
			// mkdir
			fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/user/dirX"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			NUnit.Framework.Assert.IsTrue("New dir should be type dir", org.apache.hadoop.fs.FileContextTestHelper.isDir
				(fcView, new org.apache.hadoop.fs.Path("/user/dirX")));
			NUnit.Framework.Assert.IsTrue("Target of new dir should be of type dir", org.apache.hadoop.fs.FileContextTestHelper.isDir
				(fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirX")));
			fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/user/dirX/dirY"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			NUnit.Framework.Assert.IsTrue("New dir should be type dir", org.apache.hadoop.fs.FileContextTestHelper.isDir
				(fcView, new org.apache.hadoop.fs.Path("/user/dirX/dirY")));
			NUnit.Framework.Assert.IsTrue("Target of new dir should be of type dir", org.apache.hadoop.fs.FileContextTestHelper.isDir
				(fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirX/dirY")));
			// Delete the created dir
			NUnit.Framework.Assert.IsTrue("Delete should succeed", fcView.delete(new org.apache.hadoop.fs.Path
				("/user/dirX/dirY"), false));
			NUnit.Framework.Assert.IsFalse("Deleted File should not exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcView, new org.apache.hadoop.fs.Path("/user/dirX/dirY")));
			NUnit.Framework.Assert.IsFalse("Deleted Target should not exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirX/dirY")));
			NUnit.Framework.Assert.IsTrue("Delete should succeed", fcView.delete(new org.apache.hadoop.fs.Path
				("/user/dirX"), false));
			NUnit.Framework.Assert.IsFalse("Deleted File should not exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcView, new org.apache.hadoop.fs.Path("/user/dirX")));
			NUnit.Framework.Assert.IsFalse("Deleted Target should not exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirX")));
			// Rename a file 
			fileContextTestHelper.createFile(fcView, "/user/foo");
			fcView.rename(new org.apache.hadoop.fs.Path("/user/foo"), new org.apache.hadoop.fs.Path
				("/user/fooBar"));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcView, new org.apache.hadoop.fs.Path("/user/foo")));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "user/foo")));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fcView
				, fileContextTestHelper.getTestRootPath(fcView, "/user/fooBar")));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fcTarget
				, new org.apache.hadoop.fs.Path(targetTestRoot, "user/fooBar")));
			fcView.mkdir(new org.apache.hadoop.fs.Path("/user/dirFoo"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			fcView.rename(new org.apache.hadoop.fs.Path("/user/dirFoo"), new org.apache.hadoop.fs.Path
				("/user/dirFooBar"));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcView, new org.apache.hadoop.fs.Path("/user/dirFoo")));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist in target", org.apache.hadoop.fs.FileContextTestHelper.exists
				(fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirFoo")));
			NUnit.Framework.Assert.IsTrue("Renamed dest should  exist as dir", org.apache.hadoop.fs.FileContextTestHelper.isDir
				(fcView, fileContextTestHelper.getTestRootPath(fcView, "/user/dirFooBar")));
			NUnit.Framework.Assert.IsTrue("Renamed dest should  exist as dir in target", org.apache.hadoop.fs.FileContextTestHelper.isDir
				(fcTarget, new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirFooBar")));
			// Make a directory under a directory that's mounted from the root of another FS
			fcView.mkdir(new org.apache.hadoop.fs.Path("/targetRoot/dirFoo"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.exists(fcView
				, new org.apache.hadoop.fs.Path("/targetRoot/dirFoo")));
			bool dirFooPresent = false;
			org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus> dirContents = 
				fcView.listStatus(new org.apache.hadoop.fs.Path("/targetRoot/"));
			while (dirContents.hasNext())
			{
				org.apache.hadoop.fs.FileStatus fileStatus = dirContents.next();
				if (fileStatus.getPath().getName().Equals("dirFoo"))
				{
					dirFooPresent = true;
				}
			}
			NUnit.Framework.Assert.IsTrue(dirFooPresent);
		}

		// rename across mount points that point to same target also fail 
		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameAcrossMounts1()
		{
			fileContextTestHelper.createFile(fcView, "/user/foo");
			fcView.rename(new org.apache.hadoop.fs.Path("/user/foo"), new org.apache.hadoop.fs.Path
				("/user2/fooBarBar"));
		}

		/* - code if we had wanted this to succeed
		Assert.assertFalse(exists(fc, new Path("/user/foo")));
		Assert.assertFalse(exists(fclocal, new Path(targetTestRoot,"user/foo")));
		Assert.assertTrue(isFile(fc,
		FileContextTestHelper.getTestRootPath(fc,"/user2/fooBarBar")));
		Assert.assertTrue(isFile(fclocal,
		new Path(targetTestRoot,"user/fooBarBar")));
		*/
		// rename across mount points fail if the mount link targets are different
		// even if the targets are part of the same target FS
		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameAcrossMounts2()
		{
			fileContextTestHelper.createFile(fcView, "/user/foo");
			fcView.rename(new org.apache.hadoop.fs.Path("/user/foo"), new org.apache.hadoop.fs.Path
				("/data/fooBar"));
		}

		protected internal static bool SupportsBlocks = false;

		//  local fs use 1 block
		// override for HDFS
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetBlockLocations()
		{
			org.apache.hadoop.fs.Path targetFilePath = new org.apache.hadoop.fs.Path(targetTestRoot
				, "data/largeFile");
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fcTarget, targetFilePath, 10
				, 1024);
			org.apache.hadoop.fs.Path viewFilePath = new org.apache.hadoop.fs.Path("/data/largeFile"
				);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileStatus(fcView, viewFilePath.ToString
				(), org.apache.hadoop.fs.FileContextTestHelper.fileType.isFile);
			org.apache.hadoop.fs.BlockLocation[] viewBL = fcView.getFileBlockLocations(viewFilePath
				, 0, 10240 + 100);
			NUnit.Framework.Assert.AreEqual(SupportsBlocks ? 10 : 1, viewBL.Length);
			org.apache.hadoop.fs.BlockLocation[] targetBL = fcTarget.getFileBlockLocations(targetFilePath
				, 0, 10240 + 100);
			compareBLs(viewBL, targetBL);
			// Same test but now get it via the FileStatus Parameter
			fcView.getFileBlockLocations(viewFilePath, 0, 10240 + 100);
			targetBL = fcTarget.getFileBlockLocations(targetFilePath, 0, 10240 + 100);
			compareBLs(viewBL, targetBL);
		}

		internal virtual void compareBLs(org.apache.hadoop.fs.BlockLocation[] viewBL, org.apache.hadoop.fs.BlockLocation
			[] targetBL)
		{
			NUnit.Framework.Assert.AreEqual(targetBL.Length, viewBL.Length);
			int i = 0;
			foreach (org.apache.hadoop.fs.BlockLocation vbl in viewBL)
			{
				NUnit.Framework.Assert.AreEqual(vbl.ToString(), targetBL[i].ToString());
				NUnit.Framework.Assert.AreEqual(targetBL[i].getOffset(), vbl.getOffset());
				NUnit.Framework.Assert.AreEqual(targetBL[i].getLength(), vbl.getLength());
				i++;
			}
		}

		/// <summary>Test "readOps" (e.g.</summary>
		/// <remarks>
		/// Test "readOps" (e.g. list, listStatus)
		/// on internal dirs of mount table
		/// These operations should succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testListOnInternalDirsOfMountTable()
		{
			// test list on internal dirs of mount table 
			// list on Slash
			org.apache.hadoop.fs.FileStatus[] dirPaths = fcView.util().listStatus(new org.apache.hadoop.fs.Path
				("/"));
			org.apache.hadoop.fs.FileStatus fs;
			NUnit.Framework.Assert.AreEqual(7, dirPaths.Length);
			fs = fileContextTestHelper.containsPath(fcView, "/user", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isSymlink());
			fs = fileContextTestHelper.containsPath(fcView, "/data", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isSymlink());
			fs = fileContextTestHelper.containsPath(fcView, "/internalDir", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("InternalDirs should appear as dir", fs.isDirectory
				());
			fs = fileContextTestHelper.containsPath(fcView, "/danglingLink", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isSymlink());
			fs = fileContextTestHelper.containsPath(fcView, "/linkToAFile", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isSymlink());
			// list on internal dir
			dirPaths = fcView.util().listStatus(new org.apache.hadoop.fs.Path("/internalDir")
				);
			NUnit.Framework.Assert.AreEqual(2, dirPaths.Length);
			fs = fileContextTestHelper.containsPath(fcView, "/internalDir/internalDir2", dirPaths
				);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("InternalDirs should appear as dir", fs.isDirectory
				());
			fs = fileContextTestHelper.containsPath(fcView, "/internalDir/linkToDir2", dirPaths
				);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFileStatusOnMountLink()
		{
			NUnit.Framework.Assert.IsTrue("Slash should appear as dir", fcView.getFileStatus(
				new org.apache.hadoop.fs.Path("/")).isDirectory());
			org.apache.hadoop.fs.FileContextTestHelper.checkFileStatus(fcView, "/", org.apache.hadoop.fs.FileContextTestHelper.fileType
				.isDir);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileStatus(fcView, "/user", org.apache.hadoop.fs.FileContextTestHelper.fileType
				.isDir);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileStatus(fcView, "/data", org.apache.hadoop.fs.FileContextTestHelper.fileType
				.isDir);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileStatus(fcView, "/internalDir"
				, org.apache.hadoop.fs.FileContextTestHelper.fileType.isDir);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileStatus(fcView, "/internalDir/linkToDir2"
				, org.apache.hadoop.fs.FileContextTestHelper.fileType.isDir);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileStatus(fcView, "/internalDir/internalDir2/linkToDir3"
				, org.apache.hadoop.fs.FileContextTestHelper.fileType.isDir);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileStatus(fcView, "/linkToAFile"
				, org.apache.hadoop.fs.FileContextTestHelper.fileType.isFile);
			try
			{
				fcView.getFileStatus(new org.apache.hadoop.fs.Path("/danglingLink"));
				NUnit.Framework.Assert.Fail("Excepted a not found exception here");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// as excepted
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetFileChecksum()
		{
			org.apache.hadoop.fs.AbstractFileSystem mockAFS = org.mockito.Mockito.mock<org.apache.hadoop.fs.AbstractFileSystem
				>();
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				> res = new org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.AbstractFileSystem
				>(null, mockAFS, null, new org.apache.hadoop.fs.Path("someFile"));
			org.apache.hadoop.fs.viewfs.InodeTree<org.apache.hadoop.fs.AbstractFileSystem> fsState
				 = org.mockito.Mockito.mock<org.apache.hadoop.fs.viewfs.InodeTree>();
			org.mockito.Mockito.when(fsState.resolve(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.anyBoolean())).thenReturn(res);
			org.apache.hadoop.fs.viewfs.ViewFs vfs = org.mockito.Mockito.mock<org.apache.hadoop.fs.viewfs.ViewFs
				>();
			vfs.fsState = fsState;
			org.mockito.Mockito.when(vfs.getFileChecksum(new org.apache.hadoop.fs.Path("/tmp/someFile"
				))).thenCallRealMethod();
			vfs.getFileChecksum(new org.apache.hadoop.fs.Path("/tmp/someFile"));
			org.mockito.Mockito.verify(mockAFS).getFileChecksum(new org.apache.hadoop.fs.Path
				("someFile"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testgetFSonDanglingLink()
		{
			fcView.getFileStatus(new org.apache.hadoop.fs.Path("/danglingLink"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testgetFSonNonExistingInternalDir()
		{
			fcView.getFileStatus(new org.apache.hadoop.fs.Path("/internalDir/nonExisting"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testgetFileLinkStatus()
		{
			org.apache.hadoop.fs.FileContextTestHelper.checkFileLinkStatus(fcView, "/user", org.apache.hadoop.fs.FileContextTestHelper.fileType
				.isSymlink);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileLinkStatus(fcView, "/data", org.apache.hadoop.fs.FileContextTestHelper.fileType
				.isSymlink);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileLinkStatus(fcView, "/internalDir/linkToDir2"
				, org.apache.hadoop.fs.FileContextTestHelper.fileType.isSymlink);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileLinkStatus(fcView, "/internalDir/internalDir2/linkToDir3"
				, org.apache.hadoop.fs.FileContextTestHelper.fileType.isSymlink);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileLinkStatus(fcView, "/linkToAFile"
				, org.apache.hadoop.fs.FileContextTestHelper.fileType.isSymlink);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileLinkStatus(fcView, "/internalDir"
				, org.apache.hadoop.fs.FileContextTestHelper.fileType.isDir);
			org.apache.hadoop.fs.FileContextTestHelper.checkFileLinkStatus(fcView, "/internalDir/internalDir2"
				, org.apache.hadoop.fs.FileContextTestHelper.fileType.isDir);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testgetFileLinkStatusonNonExistingInternalDir()
		{
			fcView.getFileLinkStatus(new org.apache.hadoop.fs.Path("/internalDir/nonExisting"
				));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testSymlinkTarget()
		{
			// get link target`
			NUnit.Framework.Assert.AreEqual(fcView.getLinkTarget(new org.apache.hadoop.fs.Path
				("/user")), (new org.apache.hadoop.fs.Path(targetTestRoot, "user")));
			NUnit.Framework.Assert.AreEqual(fcView.getLinkTarget(new org.apache.hadoop.fs.Path
				("/data")), (new org.apache.hadoop.fs.Path(targetTestRoot, "data")));
			NUnit.Framework.Assert.AreEqual(fcView.getLinkTarget(new org.apache.hadoop.fs.Path
				("/internalDir/linkToDir2")), (new org.apache.hadoop.fs.Path(targetTestRoot, "dir2"
				)));
			NUnit.Framework.Assert.AreEqual(fcView.getLinkTarget(new org.apache.hadoop.fs.Path
				("/internalDir/internalDir2/linkToDir3")), (new org.apache.hadoop.fs.Path(targetTestRoot
				, "dir3")));
			NUnit.Framework.Assert.AreEqual(fcView.getLinkTarget(new org.apache.hadoop.fs.Path
				("/linkToAFile")), (new org.apache.hadoop.fs.Path(targetTestRoot, "aFile")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testgetLinkTargetOnNonLink()
		{
			fcView.getLinkTarget(new org.apache.hadoop.fs.Path("/internalDir/internalDir2"));
		}

		/*
		* Test resolvePath(p)
		* TODO In the tests below replace
		* fcView.getDefaultFileSystem().resolvePath() fcView.resolvePath()
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testResolvePathInternalPaths()
		{
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/"), fcView.resolvePath
				(new org.apache.hadoop.fs.Path("/")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/internalDir"), fcView
				.resolvePath(new org.apache.hadoop.fs.Path("/internalDir")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testResolvePathMountPoints()
		{
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "user"
				), fcView.resolvePath(new org.apache.hadoop.fs.Path("/user")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "data"
				), fcView.resolvePath(new org.apache.hadoop.fs.Path("/data")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "dir2"
				), fcView.resolvePath(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "dir3"
				), fcView.resolvePath(new org.apache.hadoop.fs.Path("/internalDir/internalDir2/linkToDir3"
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testResolvePathThroughMountPoints()
		{
			fileContextTestHelper.createFile(fcView, "/user/foo");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "user/foo"
				), fcView.resolvePath(new org.apache.hadoop.fs.Path("/user/foo")));
			fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/user/dirX"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirX"
				), fcView.resolvePath(new org.apache.hadoop.fs.Path("/user/dirX")));
			fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/user/dirX/dirY"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirX/dirY"
				), fcView.resolvePath(new org.apache.hadoop.fs.Path("/user/dirX/dirY")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testResolvePathDanglingLink()
		{
			fcView.resolvePath(new org.apache.hadoop.fs.Path("/danglingLink"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testResolvePathMissingThroughMountPoints()
		{
			fcView.resolvePath(new org.apache.hadoop.fs.Path("/user/nonExisting"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testResolvePathMissingThroughMountPoints2()
		{
			fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/user/dirX"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
			fcView.resolvePath(new org.apache.hadoop.fs.Path("/user/dirX/nonExisting"));
		}

		/// <summary>
		/// Test modify operations (create, mkdir, rename, etc)
		/// on internal dirs of mount table
		/// These operations should fail since the mount table is read-only or
		/// because the internal dir that it is trying to create already
		/// exits.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalMkdirSlash()
		{
			// Mkdir on internal mount table should fail
			fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalMkdirExisting1()
		{
			fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/internalDir"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalMkdirExisting2()
		{
			fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/internalDir/linkToDir2"
				), org.apache.hadoop.fs.FileContext.DEFAULT_PERM, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalMkdirNew()
		{
			fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/dirNew"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalMkdirNew2()
		{
			fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/internalDir/dirNew")
				, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, false);
		}

		// Create on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalCreate1()
		{
			fileContextTestHelper.createFileNonRecursive(fcView, "/foo");
		}

		// 1 component
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalCreate2()
		{
			// 2 component
			fileContextTestHelper.createFileNonRecursive(fcView, "/internalDir/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalCreateMissingDir()
		{
			fileContextTestHelper.createFile(fcView, "/missingDir/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalCreateMissingDir2()
		{
			fileContextTestHelper.createFile(fcView, "/missingDir/miss2/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalCreateMissingDir3()
		{
			fileContextTestHelper.createFile(fcView, "/internalDir/miss2/foo");
		}

		// Delete on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalDeleteNonExisting()
		{
			fcView.delete(new org.apache.hadoop.fs.Path("/NonExisting"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalDeleteNonExisting2()
		{
			fcView.delete(new org.apache.hadoop.fs.Path("/internalDir/NonExisting"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalDeleteExisting()
		{
			fcView.delete(new org.apache.hadoop.fs.Path("/internalDir"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalDeleteExisting2()
		{
			NUnit.Framework.Assert.IsTrue("Delete of link to dir should succeed", fcView.getFileStatus
				(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2")).isDirectory());
			fcView.delete(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2"), false);
		}

		// Rename on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRename1()
		{
			fcView.rename(new org.apache.hadoop.fs.Path("/internalDir"), new org.apache.hadoop.fs.Path
				("/newDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRename2()
		{
			NUnit.Framework.Assert.IsTrue("linkTODir2 should be a dir", fcView.getFileStatus(
				new org.apache.hadoop.fs.Path("/internalDir/linkToDir2")).isDirectory());
			fcView.rename(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2"), new org.apache.hadoop.fs.Path
				("/internalDir/dir1"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRename3()
		{
			fcView.rename(new org.apache.hadoop.fs.Path("/user"), new org.apache.hadoop.fs.Path
				("/internalDir/linkToDir2"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRenameToSlash()
		{
			fcView.rename(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2/foo"), new org.apache.hadoop.fs.Path
				("/"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRenameFromSlash()
		{
			fcView.rename(new org.apache.hadoop.fs.Path("/"), new org.apache.hadoop.fs.Path("/bar"
				));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalSetOwner()
		{
			fcView.setOwner(new org.apache.hadoop.fs.Path("/internalDir"), "foo", "bar");
		}

		/// <summary>
		/// Verify the behavior of ACL operations on paths above the root of
		/// any mount table entry.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalModifyAclEntries()
		{
			fcView.modifyAclEntries(new org.apache.hadoop.fs.Path("/internalDir"), new System.Collections.Generic.List
				<org.apache.hadoop.fs.permission.AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRemoveAclEntries()
		{
			fcView.removeAclEntries(new org.apache.hadoop.fs.Path("/internalDir"), new System.Collections.Generic.List
				<org.apache.hadoop.fs.permission.AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRemoveDefaultAcl()
		{
			fcView.removeDefaultAcl(new org.apache.hadoop.fs.Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRemoveAcl()
		{
			fcView.removeAcl(new org.apache.hadoop.fs.Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalSetAcl()
		{
			fcView.setAcl(new org.apache.hadoop.fs.Path("/internalDir"), new System.Collections.Generic.List
				<org.apache.hadoop.fs.permission.AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testInternalGetAclStatus()
		{
			org.apache.hadoop.security.UserGroupInformation currentUser = org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser();
			org.apache.hadoop.fs.permission.AclStatus aclStatus = fcView.getAclStatus(new org.apache.hadoop.fs.Path
				("/internalDir"));
			NUnit.Framework.Assert.AreEqual(aclStatus.getOwner(), currentUser.getUserName());
			NUnit.Framework.Assert.AreEqual(aclStatus.getGroup(), currentUser.getGroupNames()
				[0]);
			NUnit.Framework.Assert.AreEqual(aclStatus.getEntries(), org.apache.hadoop.fs.permission.AclUtil
				.getMinimalAcl(org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555));
			NUnit.Framework.Assert.IsFalse(aclStatus.isStickyBit());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalSetXAttr()
		{
			fcView.setXAttr(new org.apache.hadoop.fs.Path("/internalDir"), "xattrName", null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalGetXAttr()
		{
			fcView.getXAttr(new org.apache.hadoop.fs.Path("/internalDir"), "xattrName");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalGetXAttrs()
		{
			fcView.getXAttrs(new org.apache.hadoop.fs.Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalGetXAttrsWithNames()
		{
			fcView.getXAttrs(new org.apache.hadoop.fs.Path("/internalDir"), new System.Collections.Generic.List
				<string>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalListXAttr()
		{
			fcView.listXAttrs(new org.apache.hadoop.fs.Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRemoveXAttr()
		{
			fcView.removeXAttr(new org.apache.hadoop.fs.Path("/internalDir"), "xattrName");
		}

		public ViewFsBaseTest()
		{
			fileContextTestHelper = createFileContextHelper();
		}
	}
}
