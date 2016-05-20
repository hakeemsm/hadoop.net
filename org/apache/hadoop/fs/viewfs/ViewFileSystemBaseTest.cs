using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>
	/// <p>
	/// A collection of tests for the
	/// <see cref="ViewFileSystem"/>
	/// .
	/// This test should be used for testing ViewFileSystem that has mount links to
	/// a target file system such  localFs or Hdfs etc.
	/// </p>
	/// <p>
	/// To test a given target file system create a subclass of this
	/// test and override
	/// <see cref="setUp()"/>
	/// to initialize the <code>fsTarget</code>
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
	public class ViewFileSystemBaseTest
	{
		internal org.apache.hadoop.fs.FileSystem fsView;

		internal org.apache.hadoop.fs.FileSystem fsTarget;

		internal org.apache.hadoop.fs.Path targetTestRoot;

		internal org.apache.hadoop.conf.Configuration conf;

		internal readonly org.apache.hadoop.fs.FileSystemTestHelper fileSystemTestHelper;

		public ViewFileSystemBaseTest()
		{
			// the view file system - the mounts are here
			// the target file system - the mount will point here
			this.fileSystemTestHelper = createFileSystemHelper();
		}

		protected internal virtual org.apache.hadoop.fs.FileSystemTestHelper createFileSystemHelper
			()
		{
			return new org.apache.hadoop.fs.FileSystemTestHelper();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			initializeTargetTestRoot();
			// Make  user and data dirs - we creates links to them in the mount table
			fsTarget.mkdirs(new org.apache.hadoop.fs.Path(targetTestRoot, "user"));
			fsTarget.mkdirs(new org.apache.hadoop.fs.Path(targetTestRoot, "data"));
			fsTarget.mkdirs(new org.apache.hadoop.fs.Path(targetTestRoot, "dir2"));
			fsTarget.mkdirs(new org.apache.hadoop.fs.Path(targetTestRoot, "dir3"));
			org.apache.hadoop.fs.FileSystemTestHelper.createFile(fsTarget, new org.apache.hadoop.fs.Path
				(targetTestRoot, "aFile"));
			// Now we use the mount fs to set links to user and dir
			// in the test root
			// Set up the defaultMT in the config with our mount point links
			conf = org.apache.hadoop.fs.viewfs.ViewFileSystemTestSetup.createConfig();
			setupMountPoints();
			fsView = org.apache.hadoop.fs.FileSystem.get(org.apache.hadoop.fs.FsConstants.VIEWFS_URI
				, conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void initializeTargetTestRoot()
		{
			targetTestRoot = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);
			// In case previous test was killed before cleanup
			fsTarget.delete(targetTestRoot, true);
			fsTarget.mkdirs(targetTestRoot);
		}

		internal virtual void setupMountPoints()
		{
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
		}

		[NUnit.Framework.Test]
		public virtual void testGetMountPoints()
		{
			org.apache.hadoop.fs.viewfs.ViewFileSystem viewfs = (org.apache.hadoop.fs.viewfs.ViewFileSystem
				)fsView;
			org.apache.hadoop.fs.viewfs.ViewFileSystem.MountPoint[] mountPoints = viewfs.getMountPoints
				();
			NUnit.Framework.Assert.AreEqual(getExpectedMountPoints(), mountPoints.Length);
		}

		internal virtual int getExpectedMountPoints()
		{
			return 8;
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
			org.apache.hadoop.security.token.Token<object>[] delTokens = fsView.addDelegationTokens
				("sanjay", new org.apache.hadoop.security.Credentials());
			NUnit.Framework.Assert.AreEqual(getExpectedDelegationTokenCount(), delTokens.Length
				);
		}

		internal virtual int getExpectedDelegationTokenCount()
		{
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetDelegationTokensWithCredentials()
		{
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			System.Collections.Generic.IList<org.apache.hadoop.security.token.Token<object>> 
				delTokens = java.util.Arrays.asList(fsView.addDelegationTokens("sanjay", credentials
				));
			int expectedTokenCount = getExpectedDelegationTokenCountWithCredentials();
			NUnit.Framework.Assert.AreEqual(expectedTokenCount, delTokens.Count);
			org.apache.hadoop.security.Credentials newCredentials = new org.apache.hadoop.security.Credentials
				();
			for (int i = 0; i < expectedTokenCount / 2; i++)
			{
				org.apache.hadoop.security.token.Token<object> token = delTokens[i];
				newCredentials.addToken(token.getService(), token);
			}
			System.Collections.Generic.IList<org.apache.hadoop.security.token.Token<object>> 
				delTokens2 = java.util.Arrays.asList(fsView.addDelegationTokens("sanjay", newCredentials
				));
			NUnit.Framework.Assert.AreEqual((expectedTokenCount + 1) / 2, delTokens2.Count);
		}

		internal virtual int getExpectedDelegationTokenCountWithCredentials()
		{
			return 0;
		}

		[NUnit.Framework.Test]
		public virtual void testBasicPaths()
		{
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.FsConstants.VIEWFS_URI, fsView
				.getUri());
			NUnit.Framework.Assert.AreEqual(fsView.makeQualified(new org.apache.hadoop.fs.Path
				("/user/" + Sharpen.Runtime.getProperty("user.name"))), fsView.getWorkingDirectory
				());
			NUnit.Framework.Assert.AreEqual(fsView.makeQualified(new org.apache.hadoop.fs.Path
				("/user/" + Sharpen.Runtime.getProperty("user.name"))), fsView.getHomeDirectory(
				));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar").makeQualified
				(org.apache.hadoop.fs.FsConstants.VIEWFS_URI, null), fsView.makeQualified(new org.apache.hadoop.fs.Path
				("/foo/bar")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testLocatedOperationsThroughMountLinks()
		{
			testOperationsThroughMountLinksInternal(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testOperationsThroughMountLinks()
		{
			testOperationsThroughMountLinksInternal(false);
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
		/// Verify the operation via mountfs (ie fSys) and *also* via the
		/// target file system (ie fSysLocal) that the mount link points-to.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void testOperationsThroughMountLinksInternal(bool located)
		{
			// Create file
			fileSystemTestHelper.createFile(fsView, "/user/foo");
			NUnit.Framework.Assert.IsTrue("Created file should be type file", fsView.isFile(new 
				org.apache.hadoop.fs.Path("/user/foo")));
			NUnit.Framework.Assert.IsTrue("Target of created file should be type file", fsTarget
				.isFile(new org.apache.hadoop.fs.Path(targetTestRoot, "user/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue("Delete should suceed", fsView.delete(new org.apache.hadoop.fs.Path
				("/user/foo"), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", fsView.exists
				(new org.apache.hadoop.fs.Path("/user/foo")));
			NUnit.Framework.Assert.IsFalse("Target File should not exist after delete", fsTarget
				.exists(new org.apache.hadoop.fs.Path(targetTestRoot, "user/foo")));
			// Create file with a 2 component dirs
			fileSystemTestHelper.createFile(fsView, "/internalDir/linkToDir2/foo");
			NUnit.Framework.Assert.IsTrue("Created file should be type file", fsView.isFile(new 
				org.apache.hadoop.fs.Path("/internalDir/linkToDir2/foo")));
			NUnit.Framework.Assert.IsTrue("Target of created file should be type file", fsTarget
				.isFile(new org.apache.hadoop.fs.Path(targetTestRoot, "dir2/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue("Delete should suceed", fsView.delete(new org.apache.hadoop.fs.Path
				("/internalDir/linkToDir2/foo"), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", fsView.exists
				(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2/foo")));
			NUnit.Framework.Assert.IsFalse("Target File should not exist after delete", fsTarget
				.exists(new org.apache.hadoop.fs.Path(targetTestRoot, "dir2/foo")));
			// Create file with a 3 component dirs
			fileSystemTestHelper.createFile(fsView, "/internalDir/internalDir2/linkToDir3/foo"
				);
			NUnit.Framework.Assert.IsTrue("Created file should be type file", fsView.isFile(new 
				org.apache.hadoop.fs.Path("/internalDir/internalDir2/linkToDir3/foo")));
			NUnit.Framework.Assert.IsTrue("Target of created file should be type file", fsTarget
				.isFile(new org.apache.hadoop.fs.Path(targetTestRoot, "dir3/foo")));
			// Recursive Create file with missing dirs
			fileSystemTestHelper.createFile(fsView, "/internalDir/linkToDir2/missingDir/miss2/foo"
				);
			NUnit.Framework.Assert.IsTrue("Created file should be type file", fsView.isFile(new 
				org.apache.hadoop.fs.Path("/internalDir/linkToDir2/missingDir/miss2/foo")));
			NUnit.Framework.Assert.IsTrue("Target of created file should be type file", fsTarget
				.isFile(new org.apache.hadoop.fs.Path(targetTestRoot, "dir2/missingDir/miss2/foo"
				)));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue("Delete should succeed", fsView.delete(new org.apache.hadoop.fs.Path
				("/internalDir/internalDir2/linkToDir3/foo"), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", fsView.exists
				(new org.apache.hadoop.fs.Path("/internalDir/internalDir2/linkToDir3/foo")));
			NUnit.Framework.Assert.IsFalse("Target File should not exist after delete", fsTarget
				.exists(new org.apache.hadoop.fs.Path(targetTestRoot, "dir3/foo")));
			// mkdir
			fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX"));
			NUnit.Framework.Assert.IsTrue("New dir should be type dir", fsView.isDirectory(new 
				org.apache.hadoop.fs.Path("/user/dirX")));
			NUnit.Framework.Assert.IsTrue("Target of new dir should be of type dir", fsTarget
				.isDirectory(new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirX")));
			fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX/dirY"));
			NUnit.Framework.Assert.IsTrue("New dir should be type dir", fsView.isDirectory(new 
				org.apache.hadoop.fs.Path("/user/dirX/dirY")));
			NUnit.Framework.Assert.IsTrue("Target of new dir should be of type dir", fsTarget
				.isDirectory(new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirX/dirY")));
			// Delete the created dir
			NUnit.Framework.Assert.IsTrue("Delete should succeed", fsView.delete(new org.apache.hadoop.fs.Path
				("/user/dirX/dirY"), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", fsView.exists
				(new org.apache.hadoop.fs.Path("/user/dirX/dirY")));
			NUnit.Framework.Assert.IsFalse("Target File should not exist after delete", fsTarget
				.exists(new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirX/dirY")));
			NUnit.Framework.Assert.IsTrue("Delete should succeed", fsView.delete(new org.apache.hadoop.fs.Path
				("/user/dirX"), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", fsView.exists
				(new org.apache.hadoop.fs.Path("/user/dirX")));
			NUnit.Framework.Assert.IsFalse(fsTarget.exists(new org.apache.hadoop.fs.Path(targetTestRoot
				, "user/dirX")));
			// Rename a file 
			fileSystemTestHelper.createFile(fsView, "/user/foo");
			fsView.rename(new org.apache.hadoop.fs.Path("/user/foo"), new org.apache.hadoop.fs.Path
				("/user/fooBar"));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist", fsView.exists(new 
				org.apache.hadoop.fs.Path("/user/foo")));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist in target", fsTarget
				.exists(new org.apache.hadoop.fs.Path(targetTestRoot, "user/foo")));
			NUnit.Framework.Assert.IsTrue("Renamed dest should  exist as file", fsView.isFile
				(fileSystemTestHelper.getTestRootPath(fsView, "/user/fooBar")));
			NUnit.Framework.Assert.IsTrue("Renamed dest should  exist as file in target", fsTarget
				.isFile(new org.apache.hadoop.fs.Path(targetTestRoot, "user/fooBar")));
			fsView.mkdirs(new org.apache.hadoop.fs.Path("/user/dirFoo"));
			fsView.rename(new org.apache.hadoop.fs.Path("/user/dirFoo"), new org.apache.hadoop.fs.Path
				("/user/dirFooBar"));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist", fsView.exists(new 
				org.apache.hadoop.fs.Path("/user/dirFoo")));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist in target", fsTarget
				.exists(new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirFoo")));
			NUnit.Framework.Assert.IsTrue("Renamed dest should  exist as dir", fsView.isDirectory
				(fileSystemTestHelper.getTestRootPath(fsView, "/user/dirFooBar")));
			NUnit.Framework.Assert.IsTrue("Renamed dest should  exist as dir in target", fsTarget
				.isDirectory(new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirFooBar")));
			// Make a directory under a directory that's mounted from the root of another FS
			fsView.mkdirs(new org.apache.hadoop.fs.Path("/targetRoot/dirFoo"));
			NUnit.Framework.Assert.IsTrue(fsView.exists(new org.apache.hadoop.fs.Path("/targetRoot/dirFoo"
				)));
			bool dirFooPresent = false;
			foreach (org.apache.hadoop.fs.FileStatus fileStatus in listStatusInternal(located
				, new org.apache.hadoop.fs.Path("/targetRoot/")))
			{
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
			fileSystemTestHelper.createFile(fsView, "/user/foo");
			fsView.rename(new org.apache.hadoop.fs.Path("/user/foo"), new org.apache.hadoop.fs.Path
				("/user2/fooBarBar"));
		}

		/* - code if we had wanted this to suceed
		Assert.assertFalse(fSys.exists(new Path("/user/foo")));
		Assert.assertFalse(fSysLocal.exists(new Path(targetTestRoot,"user/foo")));
		Assert.assertTrue(fSys.isFile(FileSystemTestHelper.getTestRootPath(fSys,"/user2/fooBarBar")));
		Assert.assertTrue(fSysLocal.isFile(new Path(targetTestRoot,"user/fooBarBar")));
		*/
		// rename across mount points fail if the mount link targets are different
		// even if the targets are part of the same target FS
		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameAcrossMounts2()
		{
			fileSystemTestHelper.createFile(fsView, "/user/foo");
			fsView.rename(new org.apache.hadoop.fs.Path("/user/foo"), new org.apache.hadoop.fs.Path
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
			org.apache.hadoop.fs.FileSystemTestHelper.createFile(fsTarget, targetFilePath, 10
				, 1024);
			org.apache.hadoop.fs.Path viewFilePath = new org.apache.hadoop.fs.Path("/data/largeFile"
				);
			NUnit.Framework.Assert.IsTrue("Created File should be type File", fsView.isFile(viewFilePath
				));
			org.apache.hadoop.fs.BlockLocation[] viewBL = fsView.getFileBlockLocations(fsView
				.getFileStatus(viewFilePath), 0, 10240 + 100);
			NUnit.Framework.Assert.AreEqual(SupportsBlocks ? 10 : 1, viewBL.Length);
			org.apache.hadoop.fs.BlockLocation[] targetBL = fsTarget.getFileBlockLocations(fsTarget
				.getFileStatus(targetFilePath), 0, 10240 + 100);
			compareBLs(viewBL, targetBL);
			// Same test but now get it via the FileStatus Parameter
			fsView.getFileBlockLocations(fsView.getFileStatus(viewFilePath), 0, 10240 + 100);
			targetBL = fsTarget.getFileBlockLocations(fsTarget.getFileStatus(targetFilePath), 
				0, 10240 + 100);
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

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testLocatedListOnInternalDirsOfMountTable()
		{
			testListOnInternalDirsOfMountTableInternal(true);
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
			testListOnInternalDirsOfMountTableInternal(false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void testListOnInternalDirsOfMountTableInternal(bool located)
		{
			// list on Slash
			org.apache.hadoop.fs.FileStatus[] dirPaths = listStatusInternal(located, new org.apache.hadoop.fs.Path
				("/"));
			org.apache.hadoop.fs.FileStatus fs;
			verifyRootChildren(dirPaths);
			// list on internal dir
			dirPaths = listStatusInternal(located, new org.apache.hadoop.fs.Path("/internalDir"
				));
			NUnit.Framework.Assert.AreEqual(2, dirPaths.Length);
			fs = fileSystemTestHelper.containsPath(fsView, "/internalDir/internalDir2", dirPaths
				);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isDirectory(
				));
			fs = fileSystemTestHelper.containsPath(fsView, "/internalDir/linkToDir2", dirPaths
				);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		private void verifyRootChildren(org.apache.hadoop.fs.FileStatus[] dirPaths)
		{
			org.apache.hadoop.fs.FileStatus fs;
			NUnit.Framework.Assert.AreEqual(getExpectedDirPaths(), dirPaths.Length);
			fs = fileSystemTestHelper.containsPath(fsView, "/user", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isSymlink());
			fs = fileSystemTestHelper.containsPath(fsView, "/data", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isSymlink());
			fs = fileSystemTestHelper.containsPath(fsView, "/internalDir", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isDirectory(
				));
			fs = fileSystemTestHelper.containsPath(fsView, "/danglingLink", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isSymlink());
			fs = fileSystemTestHelper.containsPath(fsView, "/linkToAFile", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.isSymlink());
		}

		internal virtual int getExpectedDirPaths()
		{
			return 7;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testListOnMountTargetDirs()
		{
			testListOnMountTargetDirsInternal(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testLocatedListOnMountTargetDirs()
		{
			testListOnMountTargetDirsInternal(true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void testListOnMountTargetDirsInternal(bool located)
		{
			org.apache.hadoop.fs.Path dataPath = new org.apache.hadoop.fs.Path("/data");
			org.apache.hadoop.fs.FileStatus[] dirPaths = listStatusInternal(located, dataPath
				);
			org.apache.hadoop.fs.FileStatus fs;
			NUnit.Framework.Assert.AreEqual(0, dirPaths.Length);
			// add a file
			long len = fileSystemTestHelper.createFile(fsView, "/data/foo");
			dirPaths = listStatusInternal(located, dataPath);
			NUnit.Framework.Assert.AreEqual(1, dirPaths.Length);
			fs = fileSystemTestHelper.containsPath(fsView, "/data/foo", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("Created file shoudl appear as a file", fs.isFile()
				);
			NUnit.Framework.Assert.AreEqual(len, fs.getLen());
			// add a dir
			fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/data/dirX"));
			dirPaths = listStatusInternal(located, dataPath);
			NUnit.Framework.Assert.AreEqual(2, dirPaths.Length);
			fs = fileSystemTestHelper.containsPath(fsView, "/data/foo", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("Created file shoudl appear as a file", fs.isFile()
				);
			fs = fileSystemTestHelper.containsPath(fsView, "/data/dirX", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("Created dir should appear as a dir", fs.isDirectory
				());
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.FileStatus[] listStatusInternal(bool located, org.apache.hadoop.fs.Path
			 dataPath)
		{
			org.apache.hadoop.fs.FileStatus[] dirPaths = new org.apache.hadoop.fs.FileStatus[
				0];
			if (located)
			{
				org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> statIter
					 = fsView.listLocatedStatus(dataPath);
				System.Collections.Generic.List<org.apache.hadoop.fs.LocatedFileStatus> tmp = new 
					System.Collections.Generic.List<org.apache.hadoop.fs.LocatedFileStatus>(10);
				while (statIter.hasNext())
				{
					tmp.add(statIter.next());
				}
				dirPaths = Sharpen.Collections.ToArray(tmp, dirPaths);
			}
			else
			{
				dirPaths = fsView.listStatus(dataPath);
			}
			return dirPaths;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFileStatusOnMountLink()
		{
			NUnit.Framework.Assert.IsTrue(fsView.getFileStatus(new org.apache.hadoop.fs.Path(
				"/")).isDirectory());
			org.apache.hadoop.fs.FileSystemTestHelper.checkFileStatus(fsView, "/", org.apache.hadoop.fs.FileSystemTestHelper.fileType
				.isDir);
			org.apache.hadoop.fs.FileSystemTestHelper.checkFileStatus(fsView, "/user", org.apache.hadoop.fs.FileSystemTestHelper.fileType
				.isDir);
			// link followed => dir
			org.apache.hadoop.fs.FileSystemTestHelper.checkFileStatus(fsView, "/data", org.apache.hadoop.fs.FileSystemTestHelper.fileType
				.isDir);
			org.apache.hadoop.fs.FileSystemTestHelper.checkFileStatus(fsView, "/internalDir", 
				org.apache.hadoop.fs.FileSystemTestHelper.fileType.isDir);
			org.apache.hadoop.fs.FileSystemTestHelper.checkFileStatus(fsView, "/internalDir/linkToDir2"
				, org.apache.hadoop.fs.FileSystemTestHelper.fileType.isDir);
			org.apache.hadoop.fs.FileSystemTestHelper.checkFileStatus(fsView, "/internalDir/internalDir2/linkToDir3"
				, org.apache.hadoop.fs.FileSystemTestHelper.fileType.isDir);
			org.apache.hadoop.fs.FileSystemTestHelper.checkFileStatus(fsView, "/linkToAFile", 
				org.apache.hadoop.fs.FileSystemTestHelper.fileType.isFile);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testgetFSonDanglingLink()
		{
			fsView.getFileStatus(new org.apache.hadoop.fs.Path("/danglingLink"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testgetFSonNonExistingInternalDir()
		{
			fsView.getFileStatus(new org.apache.hadoop.fs.Path("/internalDir/nonExisting"));
		}

		/*
		* Test resolvePath(p)
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testResolvePathInternalPaths()
		{
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/"), fsView.resolvePath
				(new org.apache.hadoop.fs.Path("/")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/internalDir"), fsView
				.resolvePath(new org.apache.hadoop.fs.Path("/internalDir")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testResolvePathMountPoints()
		{
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "user"
				), fsView.resolvePath(new org.apache.hadoop.fs.Path("/user")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "data"
				), fsView.resolvePath(new org.apache.hadoop.fs.Path("/data")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "dir2"
				), fsView.resolvePath(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2")));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "dir3"
				), fsView.resolvePath(new org.apache.hadoop.fs.Path("/internalDir/internalDir2/linkToDir3"
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testResolvePathThroughMountPoints()
		{
			fileSystemTestHelper.createFile(fsView, "/user/foo");
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "user/foo"
				), fsView.resolvePath(new org.apache.hadoop.fs.Path("/user/foo")));
			fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirX"
				), fsView.resolvePath(new org.apache.hadoop.fs.Path("/user/dirX")));
			fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX/dirY"));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(targetTestRoot, "user/dirX/dirY"
				), fsView.resolvePath(new org.apache.hadoop.fs.Path("/user/dirX/dirY")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testResolvePathDanglingLink()
		{
			fsView.resolvePath(new org.apache.hadoop.fs.Path("/danglingLink"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testResolvePathMissingThroughMountPoints()
		{
			fsView.resolvePath(new org.apache.hadoop.fs.Path("/user/nonExisting"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testResolvePathMissingThroughMountPoints2()
		{
			fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX"));
			fsView.resolvePath(new org.apache.hadoop.fs.Path("/user/dirX/nonExisting"));
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
			// Mkdir on existing internal mount table succeed except for /
			fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalMkdirExisting1()
		{
			NUnit.Framework.Assert.IsTrue("mkdir of existing dir should succeed", fsView.mkdirs
				(fileSystemTestHelper.getTestRootPath(fsView, "/internalDir")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalMkdirExisting2()
		{
			NUnit.Framework.Assert.IsTrue("mkdir of existing dir should succeed", fsView.mkdirs
				(fileSystemTestHelper.getTestRootPath(fsView, "/internalDir/linkToDir2")));
		}

		// Mkdir for new internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalMkdirNew()
		{
			fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/dirNew"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalMkdirNew2()
		{
			fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/internalDir/dirNew")
				);
		}

		// Create File on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalCreate1()
		{
			fileSystemTestHelper.createFile(fsView, "/foo");
		}

		// 1 component
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalCreate2()
		{
			// 2 component
			fileSystemTestHelper.createFile(fsView, "/internalDir/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalCreateMissingDir()
		{
			fileSystemTestHelper.createFile(fsView, "/missingDir/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalCreateMissingDir2()
		{
			fileSystemTestHelper.createFile(fsView, "/missingDir/miss2/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalCreateMissingDir3()
		{
			fileSystemTestHelper.createFile(fsView, "/internalDir/miss2/foo");
		}

		// Delete on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalDeleteNonExisting()
		{
			fsView.delete(new org.apache.hadoop.fs.Path("/NonExisting"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalDeleteNonExisting2()
		{
			fsView.delete(new org.apache.hadoop.fs.Path("/internalDir/NonExisting"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalDeleteExisting()
		{
			fsView.delete(new org.apache.hadoop.fs.Path("/internalDir"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalDeleteExisting2()
		{
			fsView.getFileStatus(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2")).isDirectory
				();
			fsView.delete(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirOfMountLink()
		{
			// data exists - mkdirs returns true even though no permission in internal
			// mount table
			NUnit.Framework.Assert.IsTrue("mkdir of existing mount link should succeed", fsView
				.mkdirs(new org.apache.hadoop.fs.Path("/data")));
		}

		// Rename on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRename1()
		{
			fsView.rename(new org.apache.hadoop.fs.Path("/internalDir"), new org.apache.hadoop.fs.Path
				("/newDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRename2()
		{
			fsView.getFileStatus(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2")).isDirectory
				();
			fsView.rename(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2"), new org.apache.hadoop.fs.Path
				("/internalDir/dir1"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRename3()
		{
			fsView.rename(new org.apache.hadoop.fs.Path("/user"), new org.apache.hadoop.fs.Path
				("/internalDir/linkToDir2"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRenameToSlash()
		{
			fsView.rename(new org.apache.hadoop.fs.Path("/internalDir/linkToDir2/foo"), new org.apache.hadoop.fs.Path
				("/"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRenameFromSlash()
		{
			fsView.rename(new org.apache.hadoop.fs.Path("/"), new org.apache.hadoop.fs.Path("/bar"
				));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalSetOwner()
		{
			fsView.setOwner(new org.apache.hadoop.fs.Path("/internalDir"), "foo", "bar");
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateNonRecursive()
		{
			org.apache.hadoop.fs.Path path = fileSystemTestHelper.getTestRootPath(fsView, "/user/foo"
				);
			fsView.createNonRecursive(path, false, 1024, (short)1, 1024L, null);
			org.apache.hadoop.fs.FileStatus status = fsView.getFileStatus(new org.apache.hadoop.fs.Path
				("/user/foo"));
			NUnit.Framework.Assert.IsTrue("Created file should be type file", fsView.isFile(new 
				org.apache.hadoop.fs.Path("/user/foo")));
			NUnit.Framework.Assert.IsTrue("Target of created file should be type file", fsTarget
				.isFile(new org.apache.hadoop.fs.Path(targetTestRoot, "user/foo")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testRootReadableExecutable()
		{
			testRootReadableExecutableInternal(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testLocatedRootReadableExecutable()
		{
			testRootReadableExecutableInternal(true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void testRootReadableExecutableInternal(bool located)
		{
			// verify executable permission on root: cd /
			//
			NUnit.Framework.Assert.IsFalse("In root before cd", fsView.getWorkingDirectory().
				isRoot());
			fsView.setWorkingDirectory(new org.apache.hadoop.fs.Path("/"));
			NUnit.Framework.Assert.IsTrue("Not in root dir after cd", fsView.getWorkingDirectory
				().isRoot());
			// verify readable
			//
			verifyRootChildren(listStatusInternal(located, fsView.getWorkingDirectory()));
			// verify permissions
			//
			org.apache.hadoop.fs.FileStatus rootStatus = fsView.getFileStatus(fsView.getWorkingDirectory
				());
			org.apache.hadoop.fs.permission.FsPermission perms = rootStatus.getPermission();
			NUnit.Framework.Assert.IsTrue("User-executable permission not set!", perms.getUserAction
				().implies(org.apache.hadoop.fs.permission.FsAction.EXECUTE));
			NUnit.Framework.Assert.IsTrue("User-readable permission not set!", perms.getUserAction
				().implies(org.apache.hadoop.fs.permission.FsAction.READ));
			NUnit.Framework.Assert.IsTrue("Group-executable permission not set!", perms.getGroupAction
				().implies(org.apache.hadoop.fs.permission.FsAction.EXECUTE));
			NUnit.Framework.Assert.IsTrue("Group-readable permission not set!", perms.getGroupAction
				().implies(org.apache.hadoop.fs.permission.FsAction.READ));
			NUnit.Framework.Assert.IsTrue("Other-executable permission not set!", perms.getOtherAction
				().implies(org.apache.hadoop.fs.permission.FsAction.EXECUTE));
			NUnit.Framework.Assert.IsTrue("Other-readable permission not set!", perms.getOtherAction
				().implies(org.apache.hadoop.fs.permission.FsAction.READ));
		}

		/// <summary>
		/// Verify the behavior of ACL operations on paths above the root of
		/// any mount table entry.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalModifyAclEntries()
		{
			fsView.modifyAclEntries(new org.apache.hadoop.fs.Path("/internalDir"), new System.Collections.Generic.List
				<org.apache.hadoop.fs.permission.AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRemoveAclEntries()
		{
			fsView.removeAclEntries(new org.apache.hadoop.fs.Path("/internalDir"), new System.Collections.Generic.List
				<org.apache.hadoop.fs.permission.AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRemoveDefaultAcl()
		{
			fsView.removeDefaultAcl(new org.apache.hadoop.fs.Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRemoveAcl()
		{
			fsView.removeAcl(new org.apache.hadoop.fs.Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalSetAcl()
		{
			fsView.setAcl(new org.apache.hadoop.fs.Path("/internalDir"), new System.Collections.Generic.List
				<org.apache.hadoop.fs.permission.AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testInternalGetAclStatus()
		{
			org.apache.hadoop.security.UserGroupInformation currentUser = org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser();
			org.apache.hadoop.fs.permission.AclStatus aclStatus = fsView.getAclStatus(new org.apache.hadoop.fs.Path
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
			fsView.setXAttr(new org.apache.hadoop.fs.Path("/internalDir"), "xattrName", null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalGetXAttr()
		{
			fsView.getXAttr(new org.apache.hadoop.fs.Path("/internalDir"), "xattrName");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalGetXAttrs()
		{
			fsView.getXAttrs(new org.apache.hadoop.fs.Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalGetXAttrsWithNames()
		{
			fsView.getXAttrs(new org.apache.hadoop.fs.Path("/internalDir"), new System.Collections.Generic.List
				<string>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalListXAttr()
		{
			fsView.listXAttrs(new org.apache.hadoop.fs.Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testInternalRemoveXAttr()
		{
			fsView.removeXAttr(new org.apache.hadoop.fs.Path("/internalDir"), "xattrName");
		}
	}
}
