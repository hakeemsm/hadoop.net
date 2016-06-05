using System.Collections.Generic;
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
	/// <see cref="ViewFileSystem"/>
	/// .
	/// This test should be used for testing ViewFileSystem that has mount links to
	/// a target file system such  localFs or Hdfs etc.
	/// </p>
	/// <p>
	/// To test a given target file system create a subclass of this
	/// test and override
	/// <see cref="SetUp()"/>
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
		internal FileSystem fsView;

		internal FileSystem fsTarget;

		internal Path targetTestRoot;

		internal Configuration conf;

		internal readonly FileSystemTestHelper fileSystemTestHelper;

		public ViewFileSystemBaseTest()
		{
			// the view file system - the mounts are here
			// the target file system - the mount will point here
			this.fileSystemTestHelper = CreateFileSystemHelper();
		}

		protected internal virtual FileSystemTestHelper CreateFileSystemHelper()
		{
			return new FileSystemTestHelper();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			InitializeTargetTestRoot();
			// Make  user and data dirs - we creates links to them in the mount table
			fsTarget.Mkdirs(new Path(targetTestRoot, "user"));
			fsTarget.Mkdirs(new Path(targetTestRoot, "data"));
			fsTarget.Mkdirs(new Path(targetTestRoot, "dir2"));
			fsTarget.Mkdirs(new Path(targetTestRoot, "dir3"));
			FileSystemTestHelper.CreateFile(fsTarget, new Path(targetTestRoot, "aFile"));
			// Now we use the mount fs to set links to user and dir
			// in the test root
			// Set up the defaultMT in the config with our mount point links
			conf = ViewFileSystemTestSetup.CreateConfig();
			SetupMountPoints();
			fsView = FileSystem.Get(FsConstants.ViewfsUri, conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fsTarget.Delete(fileSystemTestHelper.GetTestRootPath(fsTarget), true);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void InitializeTargetTestRoot()
		{
			targetTestRoot = fileSystemTestHelper.GetAbsoluteTestRootPath(fsTarget);
			// In case previous test was killed before cleanup
			fsTarget.Delete(targetTestRoot, true);
			fsTarget.Mkdirs(targetTestRoot);
		}

		internal virtual void SetupMountPoints()
		{
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
		}

		[Fact]
		public virtual void TestGetMountPoints()
		{
			ViewFileSystem viewfs = (ViewFileSystem)fsView;
			ViewFileSystem.MountPoint[] mountPoints = viewfs.GetMountPoints();
			Assert.Equal(GetExpectedMountPoints(), mountPoints.Length);
		}

		internal virtual int GetExpectedMountPoints()
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
		[Fact]
		public virtual void TestGetDelegationTokens()
		{
			Org.Apache.Hadoop.Security.Token.Token<object>[] delTokens = fsView.AddDelegationTokens
				("sanjay", new Credentials());
			Assert.Equal(GetExpectedDelegationTokenCount(), delTokens.Length
				);
		}

		internal virtual int GetExpectedDelegationTokenCount()
		{
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGetDelegationTokensWithCredentials()
		{
			Credentials credentials = new Credentials();
			IList<Org.Apache.Hadoop.Security.Token.Token<object>> delTokens = Arrays.AsList(fsView
				.AddDelegationTokens("sanjay", credentials));
			int expectedTokenCount = GetExpectedDelegationTokenCountWithCredentials();
			Assert.Equal(expectedTokenCount, delTokens.Count);
			Credentials newCredentials = new Credentials();
			for (int i = 0; i < expectedTokenCount / 2; i++)
			{
				Org.Apache.Hadoop.Security.Token.Token<object> token = delTokens[i];
				newCredentials.AddToken(token.GetService(), token);
			}
			IList<Org.Apache.Hadoop.Security.Token.Token<object>> delTokens2 = Arrays.AsList(
				fsView.AddDelegationTokens("sanjay", newCredentials));
			Assert.Equal((expectedTokenCount + 1) / 2, delTokens2.Count);
		}

		internal virtual int GetExpectedDelegationTokenCountWithCredentials()
		{
			return 0;
		}

		[Fact]
		public virtual void TestBasicPaths()
		{
			Assert.Equal(FsConstants.ViewfsUri, fsView.GetUri());
			Assert.Equal(fsView.MakeQualified(new Path("/user/" + Runtime.
				GetProperty("user.name"))), fsView.GetWorkingDirectory());
			Assert.Equal(fsView.MakeQualified(new Path("/user/" + Runtime.
				GetProperty("user.name"))), fsView.GetHomeDirectory());
			Assert.Equal(new Path("/foo/bar").MakeQualified(FsConstants.ViewfsUri
				, null), fsView.MakeQualified(new Path("/foo/bar")));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestLocatedOperationsThroughMountLinks()
		{
			TestOperationsThroughMountLinksInternal(true);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestOperationsThroughMountLinks()
		{
			TestOperationsThroughMountLinksInternal(false);
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
		private void TestOperationsThroughMountLinksInternal(bool located)
		{
			// Create file
			fileSystemTestHelper.CreateFile(fsView, "/user/foo");
			Assert.True("Created file should be type file", fsView.IsFile(new 
				Path("/user/foo")));
			Assert.True("Target of created file should be type file", fsTarget
				.IsFile(new Path(targetTestRoot, "user/foo")));
			// Delete the created file
			Assert.True("Delete should suceed", fsView.Delete(new Path("/user/foo"
				), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", fsView.Exists
				(new Path("/user/foo")));
			NUnit.Framework.Assert.IsFalse("Target File should not exist after delete", fsTarget
				.Exists(new Path(targetTestRoot, "user/foo")));
			// Create file with a 2 component dirs
			fileSystemTestHelper.CreateFile(fsView, "/internalDir/linkToDir2/foo");
			Assert.True("Created file should be type file", fsView.IsFile(new 
				Path("/internalDir/linkToDir2/foo")));
			Assert.True("Target of created file should be type file", fsTarget
				.IsFile(new Path(targetTestRoot, "dir2/foo")));
			// Delete the created file
			Assert.True("Delete should suceed", fsView.Delete(new Path("/internalDir/linkToDir2/foo"
				), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", fsView.Exists
				(new Path("/internalDir/linkToDir2/foo")));
			NUnit.Framework.Assert.IsFalse("Target File should not exist after delete", fsTarget
				.Exists(new Path(targetTestRoot, "dir2/foo")));
			// Create file with a 3 component dirs
			fileSystemTestHelper.CreateFile(fsView, "/internalDir/internalDir2/linkToDir3/foo"
				);
			Assert.True("Created file should be type file", fsView.IsFile(new 
				Path("/internalDir/internalDir2/linkToDir3/foo")));
			Assert.True("Target of created file should be type file", fsTarget
				.IsFile(new Path(targetTestRoot, "dir3/foo")));
			// Recursive Create file with missing dirs
			fileSystemTestHelper.CreateFile(fsView, "/internalDir/linkToDir2/missingDir/miss2/foo"
				);
			Assert.True("Created file should be type file", fsView.IsFile(new 
				Path("/internalDir/linkToDir2/missingDir/miss2/foo")));
			Assert.True("Target of created file should be type file", fsTarget
				.IsFile(new Path(targetTestRoot, "dir2/missingDir/miss2/foo")));
			// Delete the created file
			Assert.True("Delete should succeed", fsView.Delete(new Path("/internalDir/internalDir2/linkToDir3/foo"
				), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", fsView.Exists
				(new Path("/internalDir/internalDir2/linkToDir3/foo")));
			NUnit.Framework.Assert.IsFalse("Target File should not exist after delete", fsTarget
				.Exists(new Path(targetTestRoot, "dir3/foo")));
			// mkdir
			fsView.Mkdirs(fileSystemTestHelper.GetTestRootPath(fsView, "/user/dirX"));
			Assert.True("New dir should be type dir", fsView.IsDirectory(new 
				Path("/user/dirX")));
			Assert.True("Target of new dir should be of type dir", fsTarget
				.IsDirectory(new Path(targetTestRoot, "user/dirX")));
			fsView.Mkdirs(fileSystemTestHelper.GetTestRootPath(fsView, "/user/dirX/dirY"));
			Assert.True("New dir should be type dir", fsView.IsDirectory(new 
				Path("/user/dirX/dirY")));
			Assert.True("Target of new dir should be of type dir", fsTarget
				.IsDirectory(new Path(targetTestRoot, "user/dirX/dirY")));
			// Delete the created dir
			Assert.True("Delete should succeed", fsView.Delete(new Path("/user/dirX/dirY"
				), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", fsView.Exists
				(new Path("/user/dirX/dirY")));
			NUnit.Framework.Assert.IsFalse("Target File should not exist after delete", fsTarget
				.Exists(new Path(targetTestRoot, "user/dirX/dirY")));
			Assert.True("Delete should succeed", fsView.Delete(new Path("/user/dirX"
				), false));
			NUnit.Framework.Assert.IsFalse("File should not exist after delete", fsView.Exists
				(new Path("/user/dirX")));
			NUnit.Framework.Assert.IsFalse(fsTarget.Exists(new Path(targetTestRoot, "user/dirX"
				)));
			// Rename a file 
			fileSystemTestHelper.CreateFile(fsView, "/user/foo");
			fsView.Rename(new Path("/user/foo"), new Path("/user/fooBar"));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist", fsView.Exists(new 
				Path("/user/foo")));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist in target", fsTarget
				.Exists(new Path(targetTestRoot, "user/foo")));
			Assert.True("Renamed dest should  exist as file", fsView.IsFile
				(fileSystemTestHelper.GetTestRootPath(fsView, "/user/fooBar")));
			Assert.True("Renamed dest should  exist as file in target", fsTarget
				.IsFile(new Path(targetTestRoot, "user/fooBar")));
			fsView.Mkdirs(new Path("/user/dirFoo"));
			fsView.Rename(new Path("/user/dirFoo"), new Path("/user/dirFooBar"));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist", fsView.Exists(new 
				Path("/user/dirFoo")));
			NUnit.Framework.Assert.IsFalse("Renamed src should not exist in target", fsTarget
				.Exists(new Path(targetTestRoot, "user/dirFoo")));
			Assert.True("Renamed dest should  exist as dir", fsView.IsDirectory
				(fileSystemTestHelper.GetTestRootPath(fsView, "/user/dirFooBar")));
			Assert.True("Renamed dest should  exist as dir in target", fsTarget
				.IsDirectory(new Path(targetTestRoot, "user/dirFooBar")));
			// Make a directory under a directory that's mounted from the root of another FS
			fsView.Mkdirs(new Path("/targetRoot/dirFoo"));
			Assert.True(fsView.Exists(new Path("/targetRoot/dirFoo")));
			bool dirFooPresent = false;
			foreach (FileStatus fileStatus in ListStatusInternal(located, new Path("/targetRoot/"
				)))
			{
				if (fileStatus.GetPath().GetName().Equals("dirFoo"))
				{
					dirFooPresent = true;
				}
			}
			Assert.True(dirFooPresent);
		}

		// rename across mount points that point to same target also fail 
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameAcrossMounts1()
		{
			fileSystemTestHelper.CreateFile(fsView, "/user/foo");
			fsView.Rename(new Path("/user/foo"), new Path("/user2/fooBarBar"));
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
		public virtual void TestRenameAcrossMounts2()
		{
			fileSystemTestHelper.CreateFile(fsView, "/user/foo");
			fsView.Rename(new Path("/user/foo"), new Path("/data/fooBar"));
		}

		protected internal static bool SupportsBlocks = false;

		//  local fs use 1 block
		// override for HDFS
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGetBlockLocations()
		{
			Path targetFilePath = new Path(targetTestRoot, "data/largeFile");
			FileSystemTestHelper.CreateFile(fsTarget, targetFilePath, 10, 1024);
			Path viewFilePath = new Path("/data/largeFile");
			Assert.True("Created File should be type File", fsView.IsFile(viewFilePath
				));
			BlockLocation[] viewBL = fsView.GetFileBlockLocations(fsView.GetFileStatus(viewFilePath
				), 0, 10240 + 100);
			Assert.Equal(SupportsBlocks ? 10 : 1, viewBL.Length);
			BlockLocation[] targetBL = fsTarget.GetFileBlockLocations(fsTarget.GetFileStatus(
				targetFilePath), 0, 10240 + 100);
			CompareBLs(viewBL, targetBL);
			// Same test but now get it via the FileStatus Parameter
			fsView.GetFileBlockLocations(fsView.GetFileStatus(viewFilePath), 0, 10240 + 100);
			targetBL = fsTarget.GetFileBlockLocations(fsTarget.GetFileStatus(targetFilePath), 
				0, 10240 + 100);
			CompareBLs(viewBL, targetBL);
		}

		internal virtual void CompareBLs(BlockLocation[] viewBL, BlockLocation[] targetBL
			)
		{
			Assert.Equal(targetBL.Length, viewBL.Length);
			int i = 0;
			foreach (BlockLocation vbl in viewBL)
			{
				Assert.Equal(vbl.ToString(), targetBL[i].ToString());
				Assert.Equal(targetBL[i].GetOffset(), vbl.GetOffset());
				Assert.Equal(targetBL[i].GetLength(), vbl.GetLength());
				i++;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestLocatedListOnInternalDirsOfMountTable()
		{
			TestListOnInternalDirsOfMountTableInternal(true);
		}

		/// <summary>Test "readOps" (e.g.</summary>
		/// <remarks>
		/// Test "readOps" (e.g. list, listStatus)
		/// on internal dirs of mount table
		/// These operations should succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestListOnInternalDirsOfMountTable()
		{
			// test list on internal dirs of mount table 
			TestListOnInternalDirsOfMountTableInternal(false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestListOnInternalDirsOfMountTableInternal(bool located)
		{
			// list on Slash
			FileStatus[] dirPaths = ListStatusInternal(located, new Path("/"));
			FileStatus fs;
			VerifyRootChildren(dirPaths);
			// list on internal dir
			dirPaths = ListStatusInternal(located, new Path("/internalDir"));
			Assert.Equal(2, dirPaths.Length);
			fs = fileSystemTestHelper.ContainsPath(fsView, "/internalDir/internalDir2", dirPaths
				);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True("A mount should appear as symlink", fs.IsDirectory(
				));
			fs = fileSystemTestHelper.ContainsPath(fsView, "/internalDir/linkToDir2", dirPaths
				);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True("A mount should appear as symlink", fs.IsSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyRootChildren(FileStatus[] dirPaths)
		{
			FileStatus fs;
			Assert.Equal(GetExpectedDirPaths(), dirPaths.Length);
			fs = fileSystemTestHelper.ContainsPath(fsView, "/user", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True("A mount should appear as symlink", fs.IsSymlink());
			fs = fileSystemTestHelper.ContainsPath(fsView, "/data", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True("A mount should appear as symlink", fs.IsSymlink());
			fs = fileSystemTestHelper.ContainsPath(fsView, "/internalDir", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True("A mount should appear as symlink", fs.IsDirectory(
				));
			fs = fileSystemTestHelper.ContainsPath(fsView, "/danglingLink", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True("A mount should appear as symlink", fs.IsSymlink());
			fs = fileSystemTestHelper.ContainsPath(fsView, "/linkToAFile", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True("A mount should appear as symlink", fs.IsSymlink());
		}

		internal virtual int GetExpectedDirPaths()
		{
			return 7;
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestListOnMountTargetDirs()
		{
			TestListOnMountTargetDirsInternal(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestLocatedListOnMountTargetDirs()
		{
			TestListOnMountTargetDirsInternal(true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestListOnMountTargetDirsInternal(bool located)
		{
			Path dataPath = new Path("/data");
			FileStatus[] dirPaths = ListStatusInternal(located, dataPath);
			FileStatus fs;
			Assert.Equal(0, dirPaths.Length);
			// add a file
			long len = fileSystemTestHelper.CreateFile(fsView, "/data/foo");
			dirPaths = ListStatusInternal(located, dataPath);
			Assert.Equal(1, dirPaths.Length);
			fs = fileSystemTestHelper.ContainsPath(fsView, "/data/foo", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True("Created file shoudl appear as a file", fs.IsFile()
				);
			Assert.Equal(len, fs.GetLen());
			// add a dir
			fsView.Mkdirs(fileSystemTestHelper.GetTestRootPath(fsView, "/data/dirX"));
			dirPaths = ListStatusInternal(located, dataPath);
			Assert.Equal(2, dirPaths.Length);
			fs = fileSystemTestHelper.ContainsPath(fsView, "/data/foo", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True("Created file shoudl appear as a file", fs.IsFile()
				);
			fs = fileSystemTestHelper.ContainsPath(fsView, "/data/dirX", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True("Created dir should appear as a dir", fs.IsDirectory
				());
		}

		/// <exception cref="System.IO.IOException"/>
		private FileStatus[] ListStatusInternal(bool located, Path dataPath)
		{
			FileStatus[] dirPaths = new FileStatus[0];
			if (located)
			{
				RemoteIterator<LocatedFileStatus> statIter = fsView.ListLocatedStatus(dataPath);
				AList<LocatedFileStatus> tmp = new AList<LocatedFileStatus>(10);
				while (statIter.HasNext())
				{
					tmp.AddItem(statIter.Next());
				}
				dirPaths = Sharpen.Collections.ToArray(tmp, dirPaths);
			}
			else
			{
				dirPaths = fsView.ListStatus(dataPath);
			}
			return dirPaths;
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestFileStatusOnMountLink()
		{
			Assert.True(fsView.GetFileStatus(new Path("/")).IsDirectory());
			FileSystemTestHelper.CheckFileStatus(fsView, "/", FileSystemTestHelper.FileType.isDir
				);
			FileSystemTestHelper.CheckFileStatus(fsView, "/user", FileSystemTestHelper.FileType
				.isDir);
			// link followed => dir
			FileSystemTestHelper.CheckFileStatus(fsView, "/data", FileSystemTestHelper.FileType
				.isDir);
			FileSystemTestHelper.CheckFileStatus(fsView, "/internalDir", FileSystemTestHelper.FileType
				.isDir);
			FileSystemTestHelper.CheckFileStatus(fsView, "/internalDir/linkToDir2", FileSystemTestHelper.FileType
				.isDir);
			FileSystemTestHelper.CheckFileStatus(fsView, "/internalDir/internalDir2/linkToDir3"
				, FileSystemTestHelper.FileType.isDir);
			FileSystemTestHelper.CheckFileStatus(fsView, "/linkToAFile", FileSystemTestHelper.FileType
				.isFile);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestgetFSonDanglingLink()
		{
			fsView.GetFileStatus(new Path("/danglingLink"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestgetFSonNonExistingInternalDir()
		{
			fsView.GetFileStatus(new Path("/internalDir/nonExisting"));
		}

		/*
		* Test resolvePath(p)
		*/
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestResolvePathInternalPaths()
		{
			Assert.Equal(new Path("/"), fsView.ResolvePath(new Path("/")));
			Assert.Equal(new Path("/internalDir"), fsView.ResolvePath(new 
				Path("/internalDir")));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestResolvePathMountPoints()
		{
			Assert.Equal(new Path(targetTestRoot, "user"), fsView.ResolvePath
				(new Path("/user")));
			Assert.Equal(new Path(targetTestRoot, "data"), fsView.ResolvePath
				(new Path("/data")));
			Assert.Equal(new Path(targetTestRoot, "dir2"), fsView.ResolvePath
				(new Path("/internalDir/linkToDir2")));
			Assert.Equal(new Path(targetTestRoot, "dir3"), fsView.ResolvePath
				(new Path("/internalDir/internalDir2/linkToDir3")));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestResolvePathThroughMountPoints()
		{
			fileSystemTestHelper.CreateFile(fsView, "/user/foo");
			Assert.Equal(new Path(targetTestRoot, "user/foo"), fsView.ResolvePath
				(new Path("/user/foo")));
			fsView.Mkdirs(fileSystemTestHelper.GetTestRootPath(fsView, "/user/dirX"));
			Assert.Equal(new Path(targetTestRoot, "user/dirX"), fsView.ResolvePath
				(new Path("/user/dirX")));
			fsView.Mkdirs(fileSystemTestHelper.GetTestRootPath(fsView, "/user/dirX/dirY"));
			Assert.Equal(new Path(targetTestRoot, "user/dirX/dirY"), fsView
				.ResolvePath(new Path("/user/dirX/dirY")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestResolvePathDanglingLink()
		{
			fsView.ResolvePath(new Path("/danglingLink"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestResolvePathMissingThroughMountPoints()
		{
			fsView.ResolvePath(new Path("/user/nonExisting"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestResolvePathMissingThroughMountPoints2()
		{
			fsView.Mkdirs(fileSystemTestHelper.GetTestRootPath(fsView, "/user/dirX"));
			fsView.ResolvePath(new Path("/user/dirX/nonExisting"));
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
			// Mkdir on existing internal mount table succeed except for /
			fsView.Mkdirs(fileSystemTestHelper.GetTestRootPath(fsView, "/"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalMkdirExisting1()
		{
			Assert.True("mkdir of existing dir should succeed", fsView.Mkdirs
				(fileSystemTestHelper.GetTestRootPath(fsView, "/internalDir")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalMkdirExisting2()
		{
			Assert.True("mkdir of existing dir should succeed", fsView.Mkdirs
				(fileSystemTestHelper.GetTestRootPath(fsView, "/internalDir/linkToDir2")));
		}

		// Mkdir for new internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalMkdirNew()
		{
			fsView.Mkdirs(fileSystemTestHelper.GetTestRootPath(fsView, "/dirNew"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalMkdirNew2()
		{
			fsView.Mkdirs(fileSystemTestHelper.GetTestRootPath(fsView, "/internalDir/dirNew")
				);
		}

		// Create File on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalCreate1()
		{
			fileSystemTestHelper.CreateFile(fsView, "/foo");
		}

		// 1 component
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalCreate2()
		{
			// 2 component
			fileSystemTestHelper.CreateFile(fsView, "/internalDir/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalCreateMissingDir()
		{
			fileSystemTestHelper.CreateFile(fsView, "/missingDir/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalCreateMissingDir2()
		{
			fileSystemTestHelper.CreateFile(fsView, "/missingDir/miss2/foo");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalCreateMissingDir3()
		{
			fileSystemTestHelper.CreateFile(fsView, "/internalDir/miss2/foo");
		}

		// Delete on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalDeleteNonExisting()
		{
			fsView.Delete(new Path("/NonExisting"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalDeleteNonExisting2()
		{
			fsView.Delete(new Path("/internalDir/NonExisting"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalDeleteExisting()
		{
			fsView.Delete(new Path("/internalDir"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalDeleteExisting2()
		{
			fsView.GetFileStatus(new Path("/internalDir/linkToDir2")).IsDirectory();
			fsView.Delete(new Path("/internalDir/linkToDir2"), false);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestMkdirOfMountLink()
		{
			// data exists - mkdirs returns true even though no permission in internal
			// mount table
			Assert.True("mkdir of existing mount link should succeed", fsView
				.Mkdirs(new Path("/data")));
		}

		// Rename on internal mount table should fail
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRename1()
		{
			fsView.Rename(new Path("/internalDir"), new Path("/newDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRename2()
		{
			fsView.GetFileStatus(new Path("/internalDir/linkToDir2")).IsDirectory();
			fsView.Rename(new Path("/internalDir/linkToDir2"), new Path("/internalDir/dir1"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRename3()
		{
			fsView.Rename(new Path("/user"), new Path("/internalDir/linkToDir2"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRenameToSlash()
		{
			fsView.Rename(new Path("/internalDir/linkToDir2/foo"), new Path("/"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRenameFromSlash()
		{
			fsView.Rename(new Path("/"), new Path("/bar"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalSetOwner()
		{
			fsView.SetOwner(new Path("/internalDir"), "foo", "bar");
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateNonRecursive()
		{
			Path path = fileSystemTestHelper.GetTestRootPath(fsView, "/user/foo");
			fsView.CreateNonRecursive(path, false, 1024, (short)1, 1024L, null);
			FileStatus status = fsView.GetFileStatus(new Path("/user/foo"));
			Assert.True("Created file should be type file", fsView.IsFile(new 
				Path("/user/foo")));
			Assert.True("Target of created file should be type file", fsTarget
				.IsFile(new Path(targetTestRoot, "user/foo")));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestRootReadableExecutable()
		{
			TestRootReadableExecutableInternal(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestLocatedRootReadableExecutable()
		{
			TestRootReadableExecutableInternal(true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestRootReadableExecutableInternal(bool located)
		{
			// verify executable permission on root: cd /
			//
			NUnit.Framework.Assert.IsFalse("In root before cd", fsView.GetWorkingDirectory().
				IsRoot());
			fsView.SetWorkingDirectory(new Path("/"));
			Assert.True("Not in root dir after cd", fsView.GetWorkingDirectory
				().IsRoot());
			// verify readable
			//
			VerifyRootChildren(ListStatusInternal(located, fsView.GetWorkingDirectory()));
			// verify permissions
			//
			FileStatus rootStatus = fsView.GetFileStatus(fsView.GetWorkingDirectory());
			FsPermission perms = rootStatus.GetPermission();
			Assert.True("User-executable permission not set!", perms.GetUserAction
				().Implies(FsAction.Execute));
			Assert.True("User-readable permission not set!", perms.GetUserAction
				().Implies(FsAction.Read));
			Assert.True("Group-executable permission not set!", perms.GetGroupAction
				().Implies(FsAction.Execute));
			Assert.True("Group-readable permission not set!", perms.GetGroupAction
				().Implies(FsAction.Read));
			Assert.True("Other-executable permission not set!", perms.GetOtherAction
				().Implies(FsAction.Execute));
			Assert.True("Other-readable permission not set!", perms.GetOtherAction
				().Implies(FsAction.Read));
		}

		/// <summary>
		/// Verify the behavior of ACL operations on paths above the root of
		/// any mount table entry.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalModifyAclEntries()
		{
			fsView.ModifyAclEntries(new Path("/internalDir"), new AList<AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRemoveAclEntries()
		{
			fsView.RemoveAclEntries(new Path("/internalDir"), new AList<AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRemoveDefaultAcl()
		{
			fsView.RemoveDefaultAcl(new Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRemoveAcl()
		{
			fsView.RemoveAcl(new Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalSetAcl()
		{
			fsView.SetAcl(new Path("/internalDir"), new AList<AclEntry>());
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestInternalGetAclStatus()
		{
			UserGroupInformation currentUser = UserGroupInformation.GetCurrentUser();
			AclStatus aclStatus = fsView.GetAclStatus(new Path("/internalDir"));
			Assert.Equal(aclStatus.GetOwner(), currentUser.GetUserName());
			Assert.Equal(aclStatus.GetGroup(), currentUser.GetGroupNames()
				[0]);
			Assert.Equal(aclStatus.GetEntries(), AclUtil.GetMinimalAcl(Constants
				.Permission555));
			NUnit.Framework.Assert.IsFalse(aclStatus.IsStickyBit());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalSetXAttr()
		{
			fsView.SetXAttr(new Path("/internalDir"), "xattrName", null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalGetXAttr()
		{
			fsView.GetXAttr(new Path("/internalDir"), "xattrName");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalGetXAttrs()
		{
			fsView.GetXAttrs(new Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalGetXAttrsWithNames()
		{
			fsView.GetXAttrs(new Path("/internalDir"), new AList<string>());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalListXAttr()
		{
			fsView.ListXAttrs(new Path("/internalDir"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInternalRemoveXAttr()
		{
			fsView.RemoveXAttr(new Path("/internalDir"), "xattrName");
		}
	}
}
