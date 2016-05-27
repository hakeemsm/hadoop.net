using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
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
	/// <see cref="SetUp()"/>
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
		internal FileContext fcView;

		internal FileContext fcTarget;

		internal Path targetTestRoot;

		internal Configuration conf;

		internal FileContext xfcViewWithAuthority;

		internal URI schemeWithAuthority;

		internal readonly FileContextTestHelper fileContextTestHelper;

		// the view file system - the mounts are here
		// the target file system - the mount will point here
		// same as fsView but with authority
		protected internal virtual FileContextTestHelper CreateFileContextHelper()
		{
			return new FileContextTestHelper();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			InitializeTargetTestRoot();
			// Make  user and data dirs - we creates links to them in the mount table
			fcTarget.Mkdir(new Path(targetTestRoot, "user"), FileContext.DefaultPerm, true);
			fcTarget.Mkdir(new Path(targetTestRoot, "data"), FileContext.DefaultPerm, true);
			fcTarget.Mkdir(new Path(targetTestRoot, "dir2"), FileContext.DefaultPerm, true);
			fcTarget.Mkdir(new Path(targetTestRoot, "dir3"), FileContext.DefaultPerm, true);
			FileContextTestHelper.CreateFile(fcTarget, new Path(targetTestRoot, "aFile"));
			// Now we use the mount fs to set links to user and dir
			// in the test root
			// Set up the defaultMT in the config with our mount point links
			conf = new Configuration();
			ConfigUtil.AddLink(conf, "/targetRoot", targetTestRoot.ToUri());
			ConfigUtil.AddLink(conf, "/user", new Path(targetTestRoot, "user").ToUri());
			ConfigUtil.AddLink(conf, "/user2", new Path(targetTestRoot, "user").ToUri());
			ConfigUtil.AddLink(conf, "/data", new Path(targetTestRoot, "data").ToUri());
			ConfigUtil.AddLink(conf, "/internalDir/linkToDir2", new Path(targetTestRoot, "dir2"
				).ToUri());
			ConfigUtil.AddLink(conf, "/internalDir/internalDir2/linkToDir3", new Path(targetTestRoot
				, "dir3").ToUri());
			ConfigUtil.AddLink(conf, "/danglingLink", new Path(targetTestRoot, "missingTarget"
				).ToUri());
			ConfigUtil.AddLink(conf, "/linkToAFile", new Path(targetTestRoot, "aFile").ToUri(
				));
			fcView = FileContext.GetFileContext(FsConstants.ViewfsUri, conf);
		}

		// Also try viewfs://default/    - note authority is name of mount table
		/// <exception cref="System.IO.IOException"/>
		internal virtual void InitializeTargetTestRoot()
		{
			targetTestRoot = fileContextTestHelper.GetAbsoluteTestRootPath(fcTarget);
			// In case previous test was killed before cleanup
			fcTarget.Delete(targetTestRoot, true);
			fcTarget.Mkdir(targetTestRoot, FileContext.DefaultPerm, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fcTarget.Delete(fileContextTestHelper.GetTestRootPath(fcTarget), true);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetMountPoints()
		{
			ViewFs viewfs = (ViewFs)fcView.GetDefaultFileSystem();
			ViewFs.MountPoint[] mountPoints = viewfs.GetMountPoints();
			NUnit.Framework.Assert.AreEqual(8, mountPoints.Length);
		}

		internal virtual int GetExpectedDelegationTokenCount()
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
		public virtual void TestGetDelegationTokens()
		{
			IList<Org.Apache.Hadoop.Security.Token.Token<object>> delTokens = fcView.GetDelegationTokens
				(new Path("/"), "sanjay");
			NUnit.Framework.Assert.AreEqual(GetExpectedDelegationTokenCount(), delTokens.Count
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestBasicPaths()
		{
			NUnit.Framework.Assert.AreEqual(FsConstants.ViewfsUri, fcView.GetDefaultFileSystem
				().GetUri());
			NUnit.Framework.Assert.AreEqual(fcView.MakeQualified(new Path("/user/" + Runtime.
				GetProperty("user.name"))), fcView.GetWorkingDirectory());
			NUnit.Framework.Assert.AreEqual(fcView.MakeQualified(new Path("/user/" + Runtime.
				GetProperty("user.name"))), fcView.GetHomeDirectory());
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar").MakeQualified(FsConstants.ViewfsUri
				, null), fcView.MakeQualified(new Path("/foo/bar")));
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
		public virtual void TestOperationsThroughMountLinks()
		{
			// Create file 
			fileContextTestHelper.CreateFileNonRecursive(fcView, "/user/foo");
			NUnit.Framework.Assert.IsTrue("Create file should be file", FileContextTestHelper.IsFile
				(fcView, new Path("/user/foo")));
			NUnit.Framework.Assert.IsTrue("Target of created file should be type file", FileContextTestHelper.IsFile
				(fcTarget, new Path(targetTestRoot, "user/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue("Delete should succeed", fcView.Delete(new Path("/user/foo"
				), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", FileContextTestHelper.Exists
				(fcView, new Path("/user/foo")));
			NUnit.Framework.Assert.IsFalse("Target File should not exist after delete", FileContextTestHelper.Exists
				(fcTarget, new Path(targetTestRoot, "user/foo")));
			// Create file with a 2 component dirs
			fileContextTestHelper.CreateFileNonRecursive(fcView, "/internalDir/linkToDir2/foo"
				);
			NUnit.Framework.Assert.IsTrue("Created file should be type file", FileContextTestHelper.IsFile
				(fcView, new Path("/internalDir/linkToDir2/foo")));
			NUnit.Framework.Assert.IsTrue("Target of created file should be type file", FileContextTestHelper.IsFile
				(fcTarget, new Path(targetTestRoot, "dir2/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue("Delete should suceed", fcView.Delete(new Path("/internalDir/linkToDir2/foo"
				), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after deletion", FileContextTestHelper.Exists
				(fcView, new Path("/internalDir/linkToDir2/foo")));
			NUnit.Framework.Assert.IsFalse("Target should not exist after deletion", FileContextTestHelper.Exists
				(fcTarget, new Path(targetTestRoot, "dir2/foo")));
			// Create file with a 3 component dirs
			fileContextTestHelper.CreateFileNonRecursive(fcView, "/internalDir/internalDir2/linkToDir3/foo"
				);
			NUnit.Framework.Assert.IsTrue("Created file should be of type file", FileContextTestHelper.IsFile
				(fcView, new Path("/internalDir/internalDir2/linkToDir3/foo")));
			NUnit.Framework.Assert.IsTrue("Target of created file should also be type file", 
				FileContextTestHelper.IsFile(fcTarget, new Path(targetTestRoot, "dir3/foo")));
			// Recursive Create file with missing dirs
			fileContextTestHelper.CreateFile(fcView, "/internalDir/linkToDir2/missingDir/miss2/foo"
				);
			NUnit.Framework.Assert.IsTrue("Created file should be of type file", FileContextTestHelper.IsFile
				(fcView, new Path("/internalDir/linkToDir2/missingDir/miss2/foo")));
			NUnit.Framework.Assert.IsTrue("Target of created file should also be type file", 
				FileContextTestHelper.IsFile(fcTarget, new Path(targetTestRoot, "dir2/missingDir/miss2/foo"
				)));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue("Delete should succeed", fcView.Delete(new Path("/internalDir/internalDir2/linkToDir3/foo"
				), false));
			NUnit.Framework.Assert.IsFalse("Deleted File should not exist", FileContextTestHelper.Exists
				(fcView, new Path("/internalDir/internalDir2/linkToDir3/foo")));
			NUnit.Framework.Assert.IsFalse("Target of deleted file should not exist", FileContextTestHelper.Exists
				(fcTarget, new Path(targetTestRoot, "dir3/foo")));
			// mkdir
			fcView.Mkdir(fileContextTestHelper.GetTestRootPath(fcView, "/user/dirX"), FileContext
				.DefaultPerm, false);
			NUnit.Framework.Assert.IsTrue("New dir should be type dir", FileContextTestHelper.IsDir
				(fcView, new Path("/user/dirX")));
			NUnit.Framework.Assert.IsTrue("Target of new dir should be of type dir", FileContextTestHelper.IsDir
				(fcTarget, new Path(targetTestRoot, "user/dirX")));
			fcView.Mkdir(fileContextTestHelper.GetTestRootPath(fcView, "/user/dirX/dirY"), FileContext
				.DefaultPerm, false);
			NUnit.Framework.Assert.IsTrue("New dir should be type dir", FileContextTestHelper.IsDir
				(fcView, new Path("/user/dirX/dirY")));
			NUnit.Framework.Assert.IsTrue("Target of new dir should be of type dir", FileContextTestHelper.IsDir
				(fcTarget, new Path(targetTestRoot, "user/dirX/dirY")));
			// Delete the created dir
			NUnit.Framework.Assert.IsTrue("Delete should succeed", fcView.Delete(new Path("/user/dirX/dirY"
				), false));
			NUnit.Framework.Assert.IsFalse("Deleted File should not exist", FileContextTestHelper.Exists
				(fcView, new Path("/user/dirX/dirY")));
			NUnit.Framework.Assert.IsFalse("Deleted Target should not exist", FileContextTestHelper.Exists
				(fcTarget, new Path(targetTestRoot, "user/dirX/dirY")));
			NUnit.Framework.Assert.IsTrue("Delete should succeed", fcView.Delete(new Path("/user/dirX"
				), false));
			NUnit.Framework.Assert.IsFalse("Deleted File should not exist", FileContextTestHelper.Exists
				(fcView, new Path("/user/dirX")));
			NUnit.Framework.Assert.IsFalse("Deleted Target should not exist", FileContextTestHelper.Exists
				(fcTarget, new Path(targetTestRoot, "user/dirX")));
			// Rename a file 
			fileContextTestHelper.CreateFile(fcView, "/user/foo");
			fcView.Rename(new Path("/user/foo"), new Path("/user/fooBar"));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist", FileContextTestHelper.Exists
				(fcView, new Path("/user/foo")));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fcTarget, new Path(targetTestRoot
				, "user/foo")));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fcView, fileContextTestHelper
				.GetTestRootPath(fcView, "/user/fooBar")));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fcTarget, new Path(targetTestRoot
				, "user/fooBar")));
			fcView.Mkdir(new Path("/user/dirFoo"), FileContext.DefaultPerm, false);
			fcView.Rename(new Path("/user/dirFoo"), new Path("/user/dirFooBar"));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist", FileContextTestHelper.Exists
				(fcView, new Path("/user/dirFoo")));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist in target", FileContextTestHelper.Exists
				(fcTarget, new Path(targetTestRoot, "user/dirFoo")));
			NUnit.Framework.Assert.IsTrue("Renamed dest should  exist as dir", FileContextTestHelper.IsDir
				(fcView, fileContextTestHelper.GetTestRootPath(fcView, "/user/dirFooBar")));
			NUnit.Framework.Assert.IsTrue("Renamed dest should  exist as dir in target", FileContextTestHelper.IsDir
				(fcTarget, new Path(targetTestRoot, "user/dirFooBar")));
			// Make a directory under a directory that's mounted from the root of another FS
			fcView.Mkdir(new Path("/targetRoot/dirFoo"), FileContext.DefaultPerm, false);
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.Exists(fcView, new Path("/targetRoot/dirFoo"
				)));
			bool dirFooPresent = false;
			RemoteIterator<FileStatus> dirContents = fcView.ListStatus(new Path("/targetRoot/"
				));
			while (dirContents.HasNext())
			{
				FileStatus fileStatus = dirContents.Next();
				if (fileStatus.GetPath().GetName().Equals("dirFoo"))
				{
					dirFooPresent = true;
				}
			}
			NUnit.Framework.Assert.IsTrue(dirFooPresent);
		}

		// rename across mount points that point to same target also fail 
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameAcrossMounts1()
		{
			fileContextTestHelper.CreateFile(fcView, "/user/foo");
			fcView.Rename(new Path("/user/foo"), new Path("/user2/fooBarBar"));
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
		public virtual void TestRenameAcrossMounts2()
		{
			fileContextTestHelper.CreateFile(fcView, "/user/foo");
			fcView.Rename(new Path("/user/foo"), new Path("/data/fooBar"));
		}

		protected internal static bool SupportsBlocks = false;

		//  local fs use 1 block
		// override for HDFS
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetBlockLocations()
		{
			Path targetFilePath = new Path(targetTestRoot, "data/largeFile");
			FileContextTestHelper.CreateFile(fcTarget, targetFilePath, 10, 1024);
			Path viewFilePath = new Path("/data/largeFile");
			FileContextTestHelper.CheckFileStatus(fcView, viewFilePath.ToString(), FileContextTestHelper.FileType
				.isFile);
			BlockLocation[] viewBL = fcView.GetFileBlockLocations(viewFilePath, 0, 10240 + 100
				);
			NUnit.Framework.Assert.AreEqual(SupportsBlocks ? 10 : 1, viewBL.Length);
			BlockLocation[] targetBL = fcTarget.GetFileBlockLocations(targetFilePath, 0, 10240
				 + 100);
			CompareBLs(viewBL, targetBL);
			// Same test but now get it via the FileStatus Parameter
			fcView.GetFileBlockLocations(viewFilePath, 0, 10240 + 100);
			targetBL = fcTarget.GetFileBlockLocations(targetFilePath, 0, 10240 + 100);
			CompareBLs(viewBL, targetBL);
		}

		internal virtual void CompareBLs(BlockLocation[] viewBL, BlockLocation[] targetBL
			)
		{
			NUnit.Framework.Assert.AreEqual(targetBL.Length, viewBL.Length);
			int i = 0;
			foreach (BlockLocation vbl in viewBL)
			{
				NUnit.Framework.Assert.AreEqual(vbl.ToString(), targetBL[i].ToString());
				NUnit.Framework.Assert.AreEqual(targetBL[i].GetOffset(), vbl.GetOffset());
				NUnit.Framework.Assert.AreEqual(targetBL[i].GetLength(), vbl.GetLength());
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
		public virtual void TestListOnInternalDirsOfMountTable()
		{
			// test list on internal dirs of mount table 
			// list on Slash
			FileStatus[] dirPaths = fcView.Util().ListStatus(new Path("/"));
			FileStatus fs;
			NUnit.Framework.Assert.AreEqual(7, dirPaths.Length);
			fs = fileContextTestHelper.ContainsPath(fcView, "/user", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.IsSymlink());
			fs = fileContextTestHelper.ContainsPath(fcView, "/data", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.IsSymlink());
			fs = fileContextTestHelper.ContainsPath(fcView, "/internalDir", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("InternalDirs should appear as dir", fs.IsDirectory
				());
			fs = fileContextTestHelper.ContainsPath(fcView, "/danglingLink", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.IsSymlink());
			fs = fileContextTestHelper.ContainsPath(fcView, "/linkToAFile", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.IsSymlink());
			// list on internal dir
			dirPaths = fcView.Util().ListStatus(new Path("/internalDir"));
			NUnit.Framework.Assert.AreEqual(2, dirPaths.Length);
			fs = fileContextTestHelper.ContainsPath(fcView, "/internalDir/internalDir2", dirPaths
				);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("InternalDirs should appear as dir", fs.IsDirectory
				());
			fs = fileContextTestHelper.ContainsPath(fcView, "/internalDir/linkToDir2", dirPaths
				);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue("A mount should appear as symlink", fs.IsSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileStatusOnMountLink()
		{
			NUnit.Framework.Assert.IsTrue("Slash should appear as dir", fcView.GetFileStatus(
				new Path("/")).IsDirectory());
			FileContextTestHelper.CheckFileStatus(fcView, "/", FileContextTestHelper.FileType
				.isDir);
			FileContextTestHelper.CheckFileStatus(fcView, "/user", FileContextTestHelper.FileType
				.isDir);
			FileContextTestHelper.CheckFileStatus(fcView, "/data", FileContextTestHelper.FileType
				.isDir);
			FileContextTestHelper.CheckFileStatus(fcView, "/internalDir", FileContextTestHelper.FileType
				.isDir);
			FileContextTestHelper.CheckFileStatus(fcView, "/internalDir/linkToDir2", FileContextTestHelper.FileType
				.isDir);
			FileContextTestHelper.CheckFileStatus(fcView, "/internalDir/internalDir2/linkToDir3"
				, FileContextTestHelper.FileType.isDir);
			FileContextTestHelper.CheckFileStatus(fcView, "/linkToAFile", FileContextTestHelper.FileType
				.isFile);
			try
			{
				fcView.GetFileStatus(new Path("/danglingLink"));
				NUnit.Framework.Assert.Fail("Excepted a not found exception here");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// as excepted
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFileChecksum()
		{
			AbstractFileSystem mockAFS = Org.Mockito.Mockito.Mock<AbstractFileSystem>();
			InodeTree.ResolveResult<AbstractFileSystem> res = new InodeTree.ResolveResult<AbstractFileSystem
				>(null, mockAFS, null, new Path("someFile"));
			InodeTree<AbstractFileSystem> fsState = Org.Mockito.Mockito.Mock<InodeTree>();
			Org.Mockito.Mockito.When(fsState.Resolve(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.AnyBoolean())).ThenReturn(res);
			ViewFs vfs = Org.Mockito.Mockito.Mock<ViewFs>();
			vfs.fsState = fsState;
			Org.Mockito.Mockito.When(vfs.GetFileChecksum(new Path("/tmp/someFile"))).ThenCallRealMethod
				();
			vfs.GetFileChecksum(new Path("/tmp/someFile"));
			Org.Mockito.Mockito.Verify(mockAFS).GetFileChecksum(new Path("someFile"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestgetFSonDanglingLink()
		{
			fcView.GetFileStatus(new Path("/danglingLink"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestgetFSonNonExistingInternalDir()
		{
			fcView.GetFileStatus(new Path("/internalDir/nonExisting"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestgetFileLinkStatus()
		{
			FileContextTestHelper.CheckFileLinkStatus(fcView, "/user", FileContextTestHelper.FileType
				.isSymlink);
			FileContextTestHelper.CheckFileLinkStatus(fcView, "/data", FileContextTestHelper.FileType
				.isSymlink);
			FileContextTestHelper.CheckFileLinkStatus(fcView, "/internalDir/linkToDir2", FileContextTestHelper.FileType
				.isSymlink);
			FileContextTestHelper.CheckFileLinkStatus(fcView, "/internalDir/internalDir2/linkToDir3"
				, FileContextTestHelper.FileType.isSymlink);
			FileContextTestHelper.CheckFileLinkStatus(fcView, "/linkToAFile", FileContextTestHelper.FileType
				.isSymlink);
			FileContextTestHelper.CheckFileLinkStatus(fcView, "/internalDir", FileContextTestHelper.FileType
				.isDir);
			FileContextTestHelper.CheckFileLinkStatus(fcView, "/internalDir/internalDir2", FileContextTestHelper.FileType
				.isDir);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestgetFileLinkStatusonNonExistingInternalDir()
		{
			fcView.GetFileLinkStatus(new Path("/internalDir/nonExisting"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSymlinkTarget()
		{
			// get link target`
			NUnit.Framework.Assert.AreEqual(fcView.GetLinkTarget(new Path("/user")), (new Path
				(targetTestRoot, "user")));
			NUnit.Framework.Assert.AreEqual(fcView.GetLinkTarget(new Path("/data")), (new Path
				(targetTestRoot, "data")));
			NUnit.Framework.Assert.AreEqual(fcView.GetLinkTarget(new Path("/internalDir/linkToDir2"
				)), (new Path(targetTestRoot, "dir2")));
			NUnit.Framework.Assert.AreEqual(fcView.GetLinkTarget(new Path("/internalDir/internalDir2/linkToDir3"
				)), (new Path(targetTestRoot, "dir3")));
			NUnit.Framework.Assert.AreEqual(fcView.GetLinkTarget(new Path("/linkToAFile")), (
				new Path(targetTestRoot, "aFile")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestgetLinkTargetOnNonLink()
		{
			fcView.GetLinkTarget(new Path("/internalDir/internalDir2"));
		}

		/*
		* Test resolvePath(p)
		* TODO In the tests below replace
		* fcView.getDefaultFileSystem().resolvePath() fcView.resolvePath()
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestResolvePathInternalPaths()
		{
			NUnit.Framework.Assert.AreEqual(new Path("/"), fcView.ResolvePath(new Path("/")));
			NUnit.Framework.Assert.AreEqual(new Path("/internalDir"), fcView.ResolvePath(new 
				Path("/internalDir")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestResolvePathMountPoints()
		{
			NUnit.Framework.Assert.AreEqual(new Path(targetTestRoot, "user"), fcView.ResolvePath
				(new Path("/user")));
			NUnit.Framework.Assert.AreEqual(new Path(targetTestRoot, "data"), fcView.ResolvePath
				(new Path("/data")));
			NUnit.Framework.Assert.AreEqual(new Path(targetTestRoot, "dir2"), fcView.ResolvePath
				(new Path("/internalDir/linkToDir2")));
			NUnit.Framework.Assert.AreEqual(new Path(targetTestRoot, "dir3"), fcView.ResolvePath
				(new Path("/internalDir/internalDir2/linkToDir3")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestResolvePathThroughMountPoints()
		{
			fileContextTestHelper.CreateFile(fcView, "/user/foo");
			NUnit.Framework.Assert.AreEqual(new Path(targetTestRoot, "user/foo"), fcView.ResolvePath
				(new Path("/user/foo")));
			fcView.Mkdir(fileContextTestHelper.GetTestRootPath(fcView, "/user/dirX"), FileContext
				.DefaultPerm, false);
			NUnit.Framework.Assert.AreEqual(new Path(targetTestRoot, "user/dirX"), fcView.ResolvePath
				(new Path("/user/dirX")));
			fcView.Mkdir(fileContextTestHelper.GetTestRootPath(fcView, "/user/dirX/dirY"), FileContext
				.DefaultPerm, false);
			NUnit.Framework.Assert.AreEqual(new Path(targetTestRoot, "user/dirX/dirY"), fcView
				.ResolvePath(new Path("/user/dirX/dirY")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestResolvePathDanglingLink()
		{
			fcView.ResolvePath(new Path("/danglingLink"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestResolvePathMissingThroughMountPoints()
		{
			fcView.ResolvePath(new Path("/user/nonExisting"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestResolvePathMissingThroughMountPoints2()
		{
			fcView.Mkdir(fileContextTestHelper.GetTestRootPath(fcView, "/user/dirX"), FileContext
				.DefaultPerm, false);
			fcView.ResolvePath(new Path("/user/dirX/nonExisting"));
		}

		/// <summary>
		/// Test modify operations (create, mkdir, rename, etc)
		/// on internal dirs of mount table
		/// These operations should fail since the mount table is read-only or
		/// because the internal dir that it is trying to create already
		/// exits.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalMkdirSlash()
		{
			// Mkdir on internal mount table should fail
			fcView.Mkdir(fileContextTestHelper.GetTestRootPath(fcView, "/"), FileContext.DefaultPerm
				, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalMkdirExisting1()
		{
			fcView.Mkdir(fileContextTestHelper.GetTestRootPath(fcView, "/internalDir"), FileContext
				.DefaultPerm, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalMkdirExisting2()
		{
			fcView.Mkdir(fileContextTestHelper.GetTestRootPath(fcView, "/internalDir/linkToDir2"
				), FileContext.DefaultPerm, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalMkdirNew()
		{
			fcView.Mkdir(fileContextTestHelper.GetTestRootPath(fcView, "/dirNew"), FileContext
				.DefaultPerm, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalMkdirNew2()
		{
			fcView.Mkdir(fileContextTestHelper.GetTestRootPath(fcView, "/internalDir/dirNew")
				, FileContext.DefaultPerm, false);
		}

		// Create on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalCreate1()
		{
			fileContextTestHelper.CreateFileNonRecursive(fcView, "/foo");
		}

		// 1 component
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalCreate2()
		{
			// 2 component
			fileContextTestHelper.CreateFileNonRecursive(fcView, "/internalDir/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalCreateMissingDir()
		{
			fileContextTestHelper.CreateFile(fcView, "/missingDir/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalCreateMissingDir2()
		{
			fileContextTestHelper.CreateFile(fcView, "/missingDir/miss2/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalCreateMissingDir3()
		{
			fileContextTestHelper.CreateFile(fcView, "/internalDir/miss2/foo");
		}

		// Delete on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalDeleteNonExisting()
		{
			fcView.Delete(new Path("/NonExisting"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalDeleteNonExisting2()
		{
			fcView.Delete(new Path("/internalDir/NonExisting"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalDeleteExisting()
		{
			fcView.Delete(new Path("/internalDir"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalDeleteExisting2()
		{
			NUnit.Framework.Assert.IsTrue("Delete of link to dir should succeed", fcView.GetFileStatus
				(new Path("/internalDir/linkToDir2")).IsDirectory());
			fcView.Delete(new Path("/internalDir/linkToDir2"), false);
		}

		// Rename on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRename1()
		{
			fcView.Rename(new Path("/internalDir"), new Path("/newDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRename2()
		{
			NUnit.Framework.Assert.IsTrue("linkTODir2 should be a dir", fcView.GetFileStatus(
				new Path("/internalDir/linkToDir2")).IsDirectory());
			fcView.Rename(new Path("/internalDir/linkToDir2"), new Path("/internalDir/dir1"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRename3()
		{
			fcView.Rename(new Path("/user"), new Path("/internalDir/linkToDir2"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRenameToSlash()
		{
			fcView.Rename(new Path("/internalDir/linkToDir2/foo"), new Path("/"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRenameFromSlash()
		{
			fcView.Rename(new Path("/"), new Path("/bar"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalSetOwner()
		{
			fcView.SetOwner(new Path("/internalDir"), "foo", "bar");
		}

		/// <summary>
		/// Verify the behavior of ACL operations on paths above the root of
		/// any mount table entry.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalModifyAclEntries()
		{
			fcView.ModifyAclEntries(new Path("/internalDir"), new AList<AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRemoveAclEntries()
		{
			fcView.RemoveAclEntries(new Path("/internalDir"), new AList<AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRemoveDefaultAcl()
		{
			fcView.RemoveDefaultAcl(new Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRemoveAcl()
		{
			fcView.RemoveAcl(new Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalSetAcl()
		{
			fcView.SetAcl(new Path("/internalDir"), new AList<AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInternalGetAclStatus()
		{
			UserGroupInformation currentUser = UserGroupInformation.GetCurrentUser();
			AclStatus aclStatus = fcView.GetAclStatus(new Path("/internalDir"));
			NUnit.Framework.Assert.AreEqual(aclStatus.GetOwner(), currentUser.GetUserName());
			NUnit.Framework.Assert.AreEqual(aclStatus.GetGroup(), currentUser.GetGroupNames()
				[0]);
			NUnit.Framework.Assert.AreEqual(aclStatus.GetEntries(), AclUtil.GetMinimalAcl(Constants
				.Permission555));
			NUnit.Framework.Assert.IsFalse(aclStatus.IsStickyBit());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalSetXAttr()
		{
			fcView.SetXAttr(new Path("/internalDir"), "xattrName", null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalGetXAttr()
		{
			fcView.GetXAttr(new Path("/internalDir"), "xattrName");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalGetXAttrs()
		{
			fcView.GetXAttrs(new Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalGetXAttrsWithNames()
		{
			fcView.GetXAttrs(new Path("/internalDir"), new AList<string>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalListXAttr()
		{
			fcView.ListXAttrs(new Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRemoveXAttr()
		{
			fcView.RemoveXAttr(new Path("/internalDir"), "xattrName");
		}

		public ViewFsBaseTest()
		{
			fileContextTestHelper = CreateFileContextHelper();
		}
	}
}
