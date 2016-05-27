using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestChRootedFs
	{
		internal FileContextTestHelper fileContextTestHelper = new FileContextTestHelper(
			);

		internal FileContext fc;

		internal FileContext fcTarget;

		internal Path chrootedTo;

		// The ChRoootedFs
		// 
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			// create the test root on local_fs
			fcTarget = FileContext.GetLocalFSFileContext();
			chrootedTo = fileContextTestHelper.GetAbsoluteTestRootPath(fcTarget);
			// In case previous test was killed before cleanup
			fcTarget.Delete(chrootedTo, true);
			fcTarget.Mkdir(chrootedTo, FileContext.DefaultPerm, true);
			Configuration conf = new Configuration();
			// ChRoot to the root of the testDirectory
			fc = FileContext.GetFileContext(new ChRootedFs(fcTarget.GetDefaultFileSystem(), chrootedTo
				), conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fcTarget.Delete(chrootedTo, true);
		}

		[NUnit.Framework.Test]
		public virtual void TestBasicPaths()
		{
			URI uri = fc.GetDefaultFileSystem().GetUri();
			NUnit.Framework.Assert.AreEqual(chrootedTo.ToUri(), uri);
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(new Path(Runtime.GetProperty("user.home"
				))), fc.GetWorkingDirectory());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(new Path(Runtime.GetProperty("user.home"
				))), fc.GetHomeDirectory());
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
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar").MakeQualified(FsConstants.LocalFsUri
				, null), fc.MakeQualified(new Path("/foo/bar")));
		}

		/// <summary>
		/// Test modify operations (create, mkdir, delete, etc)
		/// Verify the operation via chrootedfs (ie fc) and *also* via the
		/// target file system (ie fclocal) that has been chrooted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateDelete()
		{
			// Create file 
			fileContextTestHelper.CreateFileNonRecursive(fc, "/foo");
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fc, new Path("/foo")));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fcTarget, new Path(chrootedTo
				, "foo")));
			// Create file with recursive dir
			fileContextTestHelper.CreateFile(fc, "/newDir/foo");
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fc, new Path("/newDir/foo"
				)));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fcTarget, new Path(chrootedTo
				, "newDir/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue(fc.Delete(new Path("/newDir/foo"), false));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, new Path("/newDir/foo"
				)));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fcTarget, new Path(chrootedTo
				, "newDir/foo")));
			// Create file with a 2 component dirs recursively
			fileContextTestHelper.CreateFile(fc, "/newDir/newDir2/foo");
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fc, new Path("/newDir/newDir2/foo"
				)));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fcTarget, new Path(chrootedTo
				, "newDir/newDir2/foo")));
			// Delete the created file
			NUnit.Framework.Assert.IsTrue(fc.Delete(new Path("/newDir/newDir2/foo"), false));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, new Path("/newDir/newDir2/foo"
				)));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fcTarget, new Path(chrootedTo
				, "newDir/newDir2/foo")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirDelete()
		{
			fc.Mkdir(fileContextTestHelper.GetTestRootPath(fc, "/dirX"), FileContext.DefaultPerm
				, false);
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsDir(fc, new Path("/dirX")));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsDir(fcTarget, new Path(chrootedTo
				, "dirX")));
			fc.Mkdir(fileContextTestHelper.GetTestRootPath(fc, "/dirX/dirY"), FileContext.DefaultPerm
				, false);
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsDir(fc, new Path("/dirX/dirY"
				)));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsDir(fcTarget, new Path(chrootedTo
				, "dirX/dirY")));
			// Delete the created dir
			NUnit.Framework.Assert.IsTrue(fc.Delete(new Path("/dirX/dirY"), false));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, new Path("/dirX/dirY"
				)));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fcTarget, new Path(chrootedTo
				, "dirX/dirY")));
			NUnit.Framework.Assert.IsTrue(fc.Delete(new Path("/dirX"), false));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, new Path("/dirX")
				));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fcTarget, new Path(chrootedTo
				, "dirX")));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRename()
		{
			// Rename a file
			fileContextTestHelper.CreateFile(fc, "/newDir/foo");
			fc.Rename(new Path("/newDir/foo"), new Path("/newDir/fooBar"));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, new Path("/newDir/foo"
				)));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fcTarget, new Path(chrootedTo
				, "newDir/foo")));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fc, fileContextTestHelper
				.GetTestRootPath(fc, "/newDir/fooBar")));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fcTarget, new Path(chrootedTo
				, "newDir/fooBar")));
			// Rename a dir
			fc.Mkdir(new Path("/newDir/dirFoo"), FileContext.DefaultPerm, false);
			fc.Rename(new Path("/newDir/dirFoo"), new Path("/newDir/dirFooBar"));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, new Path("/newDir/dirFoo"
				)));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fcTarget, new Path(chrootedTo
				, "newDir/dirFoo")));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsDir(fc, fileContextTestHelper
				.GetTestRootPath(fc, "/newDir/dirFooBar")));
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsDir(fcTarget, new Path(chrootedTo
				, "newDir/dirFooBar")));
		}

		/// <summary>
		/// We would have liked renames across file system to fail but
		/// Unfortunately there is not way to distinguish the two file systems
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameAcrossFs()
		{
			fc.Mkdir(new Path("/newDir/dirFoo"), FileContext.DefaultPerm, true);
			// the root will get interpreted to the root of the chrooted fs.
			fc.Rename(new Path("/newDir/dirFoo"), new Path("file:///dirFooBar"));
			FileContextTestHelper.IsDir(fc, new Path("/dirFooBar"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestList()
		{
			FileStatus fs = fc.GetFileStatus(new Path("/"));
			NUnit.Framework.Assert.IsTrue(fs.IsDirectory());
			//  should return the full path not the chrooted path
			NUnit.Framework.Assert.AreEqual(fs.GetPath(), chrootedTo);
			// list on Slash
			FileStatus[] dirPaths = fc.Util().ListStatus(new Path("/"));
			NUnit.Framework.Assert.AreEqual(0, dirPaths.Length);
			fileContextTestHelper.CreateFileNonRecursive(fc, "/foo");
			fileContextTestHelper.CreateFileNonRecursive(fc, "/bar");
			fc.Mkdir(new Path("/dirX"), FileContext.DefaultPerm, false);
			fc.Mkdir(fileContextTestHelper.GetTestRootPath(fc, "/dirY"), FileContext.DefaultPerm
				, false);
			fc.Mkdir(new Path("/dirX/dirXX"), FileContext.DefaultPerm, false);
			dirPaths = fc.Util().ListStatus(new Path("/"));
			NUnit.Framework.Assert.AreEqual(4, dirPaths.Length);
			// Note the the file status paths are the full paths on target
			fs = fileContextTestHelper.ContainsPath(fcTarget, "foo", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.IsFile());
			fs = fileContextTestHelper.ContainsPath(fcTarget, "bar", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.IsFile());
			fs = fileContextTestHelper.ContainsPath(fcTarget, "dirX", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.IsDirectory());
			fs = fileContextTestHelper.ContainsPath(fcTarget, "dirY", dirPaths);
			NUnit.Framework.Assert.IsNotNull(fs);
			NUnit.Framework.Assert.IsTrue(fs.IsDirectory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWorkingDirectory()
		{
			// First we cd to our test root
			fc.Mkdir(new Path("/testWd"), FileContext.DefaultPerm, false);
			Path workDir = new Path("/testWd");
			Path fqWd = fc.MakeQualified(workDir);
			fc.SetWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(fqWd, fc.GetWorkingDirectory());
			fc.SetWorkingDirectory(new Path("."));
			NUnit.Framework.Assert.AreEqual(fqWd, fc.GetWorkingDirectory());
			fc.SetWorkingDirectory(new Path(".."));
			NUnit.Framework.Assert.AreEqual(fqWd.GetParent(), fc.GetWorkingDirectory());
			// cd using a relative path
			// Go back to our test root
			workDir = new Path("/testWd");
			fqWd = fc.MakeQualified(workDir);
			fc.SetWorkingDirectory(workDir);
			NUnit.Framework.Assert.AreEqual(fqWd, fc.GetWorkingDirectory());
			Path relativeDir = new Path("existingDir1");
			Path absoluteDir = new Path(workDir, "existingDir1");
			fc.Mkdir(absoluteDir, FileContext.DefaultPerm, true);
			Path fqAbsoluteDir = fc.MakeQualified(absoluteDir);
			fc.SetWorkingDirectory(relativeDir);
			NUnit.Framework.Assert.AreEqual(fqAbsoluteDir, fc.GetWorkingDirectory());
			// cd using a absolute path
			absoluteDir = new Path("/test/existingDir2");
			fqAbsoluteDir = fc.MakeQualified(absoluteDir);
			fc.Mkdir(absoluteDir, FileContext.DefaultPerm, true);
			fc.SetWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(fqAbsoluteDir, fc.GetWorkingDirectory());
			// Now open a file relative to the wd we just set above.
			Path absolutePath = new Path(absoluteDir, "foo");
			fc.Create(absolutePath, EnumSet.Of(CreateFlag.Create)).Close();
			fc.Open(new Path("foo")).Close();
			// Now mkdir relative to the dir we cd'ed to
			fc.Mkdir(new Path("newDir"), FileContext.DefaultPerm, true);
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsDir(fc, new Path(absoluteDir
				, "newDir")));
			absoluteDir = fileContextTestHelper.GetTestRootPath(fc, "nonexistingPath");
			try
			{
				fc.SetWorkingDirectory(absoluteDir);
				NUnit.Framework.Assert.Fail("cd to non existing dir should have failed");
			}
			catch (Exception)
			{
			}
			// Exception as expected
			// Try a URI
			string LocalFsRootUri = "file:///tmp/test";
			absoluteDir = new Path(LocalFsRootUri + "/existingDir");
			fc.Mkdir(absoluteDir, FileContext.DefaultPerm, true);
			fc.SetWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fc.GetWorkingDirectory());
		}

		/*
		* Test resolvePath(p)
		*/
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestResolvePath()
		{
			NUnit.Framework.Assert.AreEqual(chrootedTo, fc.GetDefaultFileSystem().ResolvePath
				(new Path("/")));
			fileContextTestHelper.CreateFile(fc, "/foo");
			NUnit.Framework.Assert.AreEqual(new Path(chrootedTo, "foo"), fc.GetDefaultFileSystem
				().ResolvePath(new Path("/foo")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestResolvePathNonExisting()
		{
			fc.GetDefaultFileSystem().ResolvePath(new Path("/nonExisting"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIsValidNameValidInBaseFs()
		{
			AbstractFileSystem baseFs = Org.Mockito.Mockito.Spy(fc.GetDefaultFileSystem());
			ChRootedFs chRootedFs = new ChRootedFs(baseFs, new Path("/chroot"));
			Org.Mockito.Mockito.DoReturn(true).When(baseFs).IsValidName(Org.Mockito.Mockito.AnyString
				());
			NUnit.Framework.Assert.IsTrue(chRootedFs.IsValidName("/test"));
			Org.Mockito.Mockito.Verify(baseFs).IsValidName("/chroot/test");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIsValidNameInvalidInBaseFs()
		{
			AbstractFileSystem baseFs = Org.Mockito.Mockito.Spy(fc.GetDefaultFileSystem());
			ChRootedFs chRootedFs = new ChRootedFs(baseFs, new Path("/chroot"));
			Org.Mockito.Mockito.DoReturn(false).When(baseFs).IsValidName(Org.Mockito.Mockito.
				AnyString());
			NUnit.Framework.Assert.IsFalse(chRootedFs.IsValidName("/test"));
			Org.Mockito.Mockito.Verify(baseFs).IsValidName("/chroot/test");
		}
	}
}
