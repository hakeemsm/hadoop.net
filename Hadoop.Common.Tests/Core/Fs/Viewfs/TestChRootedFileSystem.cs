using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestChRootedFileSystem
	{
		internal FileSystem fSys;

		internal FileSystem fSysTarget;

		internal Path chrootedTo;

		internal FileSystemTestHelper fileSystemTestHelper;

		// The ChRoootedFs
		//
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			// create the test root on local_fs
			Configuration conf = new Configuration();
			fSysTarget = FileSystem.GetLocal(conf);
			fileSystemTestHelper = new FileSystemTestHelper();
			chrootedTo = fileSystemTestHelper.GetAbsoluteTestRootPath(fSysTarget);
			// In case previous test was killed before cleanup
			fSysTarget.Delete(chrootedTo, true);
			fSysTarget.Mkdirs(chrootedTo);
			// ChRoot to the root of the testDirectory
			fSys = new ChRootedFileSystem(chrootedTo.ToUri(), conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fSysTarget.Delete(chrootedTo, true);
		}

		[Fact]
		public virtual void TestURI()
		{
			URI uri = fSys.GetUri();
			Assert.Equal(chrootedTo.ToUri(), uri);
		}

		[Fact]
		public virtual void TestBasicPaths()
		{
			URI uri = fSys.GetUri();
			Assert.Equal(chrootedTo.ToUri(), uri);
			Assert.Equal(fSys.MakeQualified(new Path(Runtime.GetProperty("user.home"
				))), fSys.GetWorkingDirectory());
			Assert.Equal(fSys.MakeQualified(new Path(Runtime.GetProperty("user.home"
				))), fSys.GetHomeDirectory());
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
			Assert.Equal(new Path("/foo/bar").MakeQualified(FsConstants.LocalFsUri
				, null), fSys.MakeQualified(new Path("/foo/bar")));
		}

		/// <summary>
		/// Test modify operations (create, mkdir, delete, etc)
		/// Verify the operation via chrootedfs (ie fSys) and *also* via the
		/// target file system (ie fSysTarget) that has been chrooted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateDelete()
		{
			// Create file 
			fileSystemTestHelper.CreateFile(fSys, "/foo");
			Assert.True(fSys.IsFile(new Path("/foo")));
			Assert.True(fSysTarget.IsFile(new Path(chrootedTo, "foo")));
			// Create file with recursive dir
			fileSystemTestHelper.CreateFile(fSys, "/newDir/foo");
			Assert.True(fSys.IsFile(new Path("/newDir/foo")));
			Assert.True(fSysTarget.IsFile(new Path(chrootedTo, "newDir/foo"
				)));
			// Delete the created file
			Assert.True(fSys.Delete(new Path("/newDir/foo"), false));
			NUnit.Framework.Assert.IsFalse(fSys.Exists(new Path("/newDir/foo")));
			NUnit.Framework.Assert.IsFalse(fSysTarget.Exists(new Path(chrootedTo, "newDir/foo"
				)));
			// Create file with a 2 component dirs recursively
			fileSystemTestHelper.CreateFile(fSys, "/newDir/newDir2/foo");
			Assert.True(fSys.IsFile(new Path("/newDir/newDir2/foo")));
			Assert.True(fSysTarget.IsFile(new Path(chrootedTo, "newDir/newDir2/foo"
				)));
			// Delete the created file
			Assert.True(fSys.Delete(new Path("/newDir/newDir2/foo"), false)
				);
			NUnit.Framework.Assert.IsFalse(fSys.Exists(new Path("/newDir/newDir2/foo")));
			NUnit.Framework.Assert.IsFalse(fSysTarget.Exists(new Path(chrootedTo, "newDir/newDir2/foo"
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestMkdirDelete()
		{
			fSys.Mkdirs(fileSystemTestHelper.GetTestRootPath(fSys, "/dirX"));
			Assert.True(fSys.IsDirectory(new Path("/dirX")));
			Assert.True(fSysTarget.IsDirectory(new Path(chrootedTo, "dirX")
				));
			fSys.Mkdirs(fileSystemTestHelper.GetTestRootPath(fSys, "/dirX/dirY"));
			Assert.True(fSys.IsDirectory(new Path("/dirX/dirY")));
			Assert.True(fSysTarget.IsDirectory(new Path(chrootedTo, "dirX/dirY"
				)));
			// Delete the created dir
			Assert.True(fSys.Delete(new Path("/dirX/dirY"), false));
			NUnit.Framework.Assert.IsFalse(fSys.Exists(new Path("/dirX/dirY")));
			NUnit.Framework.Assert.IsFalse(fSysTarget.Exists(new Path(chrootedTo, "dirX/dirY"
				)));
			Assert.True(fSys.Delete(new Path("/dirX"), false));
			NUnit.Framework.Assert.IsFalse(fSys.Exists(new Path("/dirX")));
			NUnit.Framework.Assert.IsFalse(fSysTarget.Exists(new Path(chrootedTo, "dirX")));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestRename()
		{
			// Rename a file
			fileSystemTestHelper.CreateFile(fSys, "/newDir/foo");
			fSys.Rename(new Path("/newDir/foo"), new Path("/newDir/fooBar"));
			NUnit.Framework.Assert.IsFalse(fSys.Exists(new Path("/newDir/foo")));
			NUnit.Framework.Assert.IsFalse(fSysTarget.Exists(new Path(chrootedTo, "newDir/foo"
				)));
			Assert.True(fSys.IsFile(fileSystemTestHelper.GetTestRootPath(fSys
				, "/newDir/fooBar")));
			Assert.True(fSysTarget.IsFile(new Path(chrootedTo, "newDir/fooBar"
				)));
			// Rename a dir
			fSys.Mkdirs(new Path("/newDir/dirFoo"));
			fSys.Rename(new Path("/newDir/dirFoo"), new Path("/newDir/dirFooBar"));
			NUnit.Framework.Assert.IsFalse(fSys.Exists(new Path("/newDir/dirFoo")));
			NUnit.Framework.Assert.IsFalse(fSysTarget.Exists(new Path(chrootedTo, "newDir/dirFoo"
				)));
			Assert.True(fSys.IsDirectory(fileSystemTestHelper.GetTestRootPath
				(fSys, "/newDir/dirFooBar")));
			Assert.True(fSysTarget.IsDirectory(new Path(chrootedTo, "newDir/dirFooBar"
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGetContentSummary()
		{
			// GetContentSummary of a dir
			fSys.Mkdirs(new Path("/newDir/dirFoo"));
			ContentSummary cs = fSys.GetContentSummary(new Path("/newDir/dirFoo"));
			Assert.Equal(-1L, cs.GetQuota());
			Assert.Equal(-1L, cs.GetSpaceQuota());
		}

		/// <summary>
		/// We would have liked renames across file system to fail but
		/// Unfortunately there is not way to distinguish the two file systems
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestRenameAcrossFs()
		{
			fSys.Mkdirs(new Path("/newDir/dirFoo"));
			fSys.Rename(new Path("/newDir/dirFoo"), new Path("file:///tmp/dirFooBar"));
			FileSystemTestHelper.IsDir(fSys, new Path("/tmp/dirFooBar"));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestList()
		{
			FileStatus fs = fSys.GetFileStatus(new Path("/"));
			Assert.True(fs.IsDirectory());
			//  should return the full path not the chrooted path
			Assert.Equal(fs.GetPath(), chrootedTo);
			// list on Slash
			FileStatus[] dirPaths = fSys.ListStatus(new Path("/"));
			Assert.Equal(0, dirPaths.Length);
			fileSystemTestHelper.CreateFile(fSys, "/foo");
			fileSystemTestHelper.CreateFile(fSys, "/bar");
			fSys.Mkdirs(new Path("/dirX"));
			fSys.Mkdirs(fileSystemTestHelper.GetTestRootPath(fSys, "/dirY"));
			fSys.Mkdirs(new Path("/dirX/dirXX"));
			dirPaths = fSys.ListStatus(new Path("/"));
			Assert.Equal(4, dirPaths.Length);
			// note 2 crc files
			// Note the the file status paths are the full paths on target
			fs = FileSystemTestHelper.ContainsPath(new Path(chrootedTo, "foo"), dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True(fs.IsFile());
			fs = FileSystemTestHelper.ContainsPath(new Path(chrootedTo, "bar"), dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True(fs.IsFile());
			fs = FileSystemTestHelper.ContainsPath(new Path(chrootedTo, "dirX"), dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True(fs.IsDirectory());
			fs = FileSystemTestHelper.ContainsPath(new Path(chrootedTo, "dirY"), dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			Assert.True(fs.IsDirectory());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWorkingDirectory()
		{
			// First we cd to our test root
			fSys.Mkdirs(new Path("/testWd"));
			Path workDir = new Path("/testWd");
			fSys.SetWorkingDirectory(workDir);
			Assert.Equal(workDir, fSys.GetWorkingDirectory());
			fSys.SetWorkingDirectory(new Path("."));
			Assert.Equal(workDir, fSys.GetWorkingDirectory());
			fSys.SetWorkingDirectory(new Path(".."));
			Assert.Equal(workDir.GetParent(), fSys.GetWorkingDirectory());
			// cd using a relative path
			// Go back to our test root
			workDir = new Path("/testWd");
			fSys.SetWorkingDirectory(workDir);
			Assert.Equal(workDir, fSys.GetWorkingDirectory());
			Path relativeDir = new Path("existingDir1");
			Path absoluteDir = new Path(workDir, "existingDir1");
			fSys.Mkdirs(absoluteDir);
			fSys.SetWorkingDirectory(relativeDir);
			Assert.Equal(absoluteDir, fSys.GetWorkingDirectory());
			// cd using a absolute path
			absoluteDir = new Path("/test/existingDir2");
			fSys.Mkdirs(absoluteDir);
			fSys.SetWorkingDirectory(absoluteDir);
			Assert.Equal(absoluteDir, fSys.GetWorkingDirectory());
			// Now open a file relative to the wd we just set above.
			Path absoluteFooPath = new Path(absoluteDir, "foo");
			fSys.Create(absoluteFooPath).Close();
			fSys.Open(new Path("foo")).Close();
			// Now mkdir relative to the dir we cd'ed to
			fSys.Mkdirs(new Path("newDir"));
			Assert.True(fSys.IsDirectory(new Path(absoluteDir, "newDir")));
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
			string LocalFsRootUri = "file:///tmp/test";
			absoluteDir = new Path(LocalFsRootUri + "/existingDir");
			fSys.Mkdirs(absoluteDir);
			fSys.SetWorkingDirectory(absoluteDir);
			Assert.Equal(absoluteDir, fSys.GetWorkingDirectory());
		}

		/*
		* Test resolvePath(p)
		*/
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestResolvePath()
		{
			Assert.Equal(chrootedTo, fSys.ResolvePath(new Path("/")));
			fileSystemTestHelper.CreateFile(fSys, "/foo");
			Assert.Equal(new Path(chrootedTo, "foo"), fSys.ResolvePath(new 
				Path("/foo")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestResolvePathNonExisting()
		{
			fSys.ResolvePath(new Path("/nonExisting"));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeleteOnExitPathHandling()
		{
			Configuration conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestChRootedFileSystem.MockFileSystem), typeof(
				FileSystem));
			URI chrootUri = URI.Create("mockfs://foo/a/b");
			ChRootedFileSystem chrootFs = new ChRootedFileSystem(chrootUri, conf);
			FileSystem mockFs = ((FilterFileSystem)chrootFs.GetRawFileSystem()).GetRawFileSystem
				();
			// ensure delete propagates the correct path
			Path chrootPath = new Path("/c");
			Path rawPath = new Path("/a/b/c");
			chrootFs.Delete(chrootPath, false);
			Org.Mockito.Mockito.Verify(mockFs).Delete(Eq(rawPath), Eq(false));
			Org.Mockito.Mockito.Reset(mockFs);
			// fake that the path exists for deleteOnExit
			FileStatus stat = Org.Mockito.Mockito.Mock<FileStatus>();
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(rawPath))).ThenReturn(stat);
			// ensure deleteOnExit propagates the correct path
			chrootFs.DeleteOnExit(chrootPath);
			chrootFs.Close();
			Org.Mockito.Mockito.Verify(mockFs).Delete(Eq(rawPath), Eq(true));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestURIEmptyPath()
		{
			Configuration conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestChRootedFileSystem.MockFileSystem), typeof(
				FileSystem));
			URI chrootUri = URI.Create("mockfs://foo");
			new ChRootedFileSystem(chrootUri, conf);
		}

		/// <summary>
		/// Tests that ChRootedFileSystem delegates calls for every ACL method to the
		/// underlying FileSystem with all Path arguments translated as required to
		/// enforce chroot.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestAclMethodsPathTranslation()
		{
			Configuration conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestChRootedFileSystem.MockFileSystem), typeof(
				FileSystem));
			URI chrootUri = URI.Create("mockfs://foo/a/b");
			ChRootedFileSystem chrootFs = new ChRootedFileSystem(chrootUri, conf);
			FileSystem mockFs = ((FilterFileSystem)chrootFs.GetRawFileSystem()).GetRawFileSystem
				();
			Path chrootPath = new Path("/c");
			Path rawPath = new Path("/a/b/c");
			IList<AclEntry> entries = Collections.EmptyList();
			chrootFs.ModifyAclEntries(chrootPath, entries);
			Org.Mockito.Mockito.Verify(mockFs).ModifyAclEntries(rawPath, entries);
			chrootFs.RemoveAclEntries(chrootPath, entries);
			Org.Mockito.Mockito.Verify(mockFs).RemoveAclEntries(rawPath, entries);
			chrootFs.RemoveDefaultAcl(chrootPath);
			Org.Mockito.Mockito.Verify(mockFs).RemoveDefaultAcl(rawPath);
			chrootFs.RemoveAcl(chrootPath);
			Org.Mockito.Mockito.Verify(mockFs).RemoveAcl(rawPath);
			chrootFs.SetAcl(chrootPath, entries);
			Org.Mockito.Mockito.Verify(mockFs).SetAcl(rawPath, entries);
			chrootFs.GetAclStatus(chrootPath);
			Org.Mockito.Mockito.Verify(mockFs).GetAclStatus(rawPath);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestListLocatedFileStatus()
		{
			Path mockMount = new Path("mockfs://foo/user");
			Path mockPath = new Path("/usermock");
			Configuration conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestChRootedFileSystem.MockFileSystem), typeof(
				FileSystem));
			ConfigUtil.AddLink(conf, mockPath.ToString(), mockMount.ToUri());
			FileSystem vfs = FileSystem.Get(URI.Create("viewfs:///"), conf);
			vfs.ListLocatedStatus(mockPath);
			FileSystem mockFs = ((TestChRootedFileSystem.MockFileSystem)mockMount.GetFileSystem
				(conf)).GetRawFileSystem();
			Org.Mockito.Mockito.Verify(mockFs).ListLocatedStatus(new Path(mockMount.ToUri().GetPath
				()));
		}

		internal class MockFileSystem : FilterFileSystem
		{
			internal MockFileSystem()
				: base(Org.Mockito.Mockito.Mock<FileSystem>())
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI name, Configuration conf)
			{
			}
		}
	}
}
