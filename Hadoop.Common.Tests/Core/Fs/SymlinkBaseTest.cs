using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Base test for symbolic links</summary>
	public abstract class SymlinkBaseTest
	{
		static SymlinkBaseTest()
		{
			// Re-enable symlinks for tests, see HADOOP-10020 and HADOOP-10052
			FileSystem.EnableSymlinks();
		}

		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 8192;

		internal const int fileSize = 16384;

		internal const int numBlocks = fileSize / blockSize;

		protected internal static FSTestWrapper wrapper;

		protected internal abstract string GetScheme();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract string TestBaseDir1();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract string TestBaseDir2();

		protected internal abstract URI TestURI();

		// Returns true if the filesystem is emulating symlink support. Certain
		// checks will be bypassed if that is the case.
		//
		protected internal virtual bool EmulatingSymlinksOnWindows()
		{
			return false;
		}

		protected internal virtual IOException UnwrapException(IOException e)
		{
			return e;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal static void CreateAndWriteFile(Path p)
		{
			CreateAndWriteFile(wrapper, p);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal static void CreateAndWriteFile(FSTestWrapper wrapper, Path p)
		{
			wrapper.CreateFile(p, numBlocks, Options.CreateOpts.CreateParent(), Options.CreateOpts
				.RepFac((short)1), Options.CreateOpts.BlockSize(blockSize));
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal static void ReadFile(Path p)
		{
			wrapper.ReadFile(p, fileSize);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal static void AppendToFile(Path p)
		{
			wrapper.AppendToFile(p, numBlocks, Options.CreateOpts.BlockSize(blockSize));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			wrapper.Mkdir(new Path(TestBaseDir1()), FileContext.DefaultPerm, true);
			wrapper.Mkdir(new Path(TestBaseDir2()), FileContext.DefaultPerm, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			wrapper.Delete(new Path(TestBaseDir1()), true);
			wrapper.Delete(new Path(TestBaseDir2()), true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStatRoot()
		{
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileLinkStatus(new Path("/")).IsSymlink
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetWDNotResolvesLinks()
		{
			Path dir = new Path(TestBaseDir1());
			Path linkToDir = new Path(TestBaseDir1() + "/link");
			wrapper.CreateSymlink(dir, linkToDir, false);
			wrapper.SetWorkingDirectory(linkToDir);
			NUnit.Framework.Assert.AreEqual(linkToDir.GetName(), wrapper.GetWorkingDirectory(
				).GetName());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateDanglingLink()
		{
			Path file = new Path("/noSuchFile");
			Path link = new Path(TestBaseDir1() + "/link");
			wrapper.CreateSymlink(file, link, false);
			try
			{
				wrapper.GetFileStatus(link);
				NUnit.Framework.Assert.Fail("Got file status of non-existant file");
			}
			catch (FileNotFoundException)
			{
			}
			// Expected
			wrapper.Delete(link, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkToNullEmpty()
		{
			Path link = new Path(TestBaseDir1() + "/link");
			try
			{
				wrapper.CreateSymlink(null, link, false);
				NUnit.Framework.Assert.Fail("Can't create symlink to null");
			}
			catch (ArgumentNullException)
			{
			}
			// Expected, create* with null yields NPEs
			try
			{
				wrapper.CreateSymlink(new Path(string.Empty), link, false);
				NUnit.Framework.Assert.Fail("Can't create symlink to empty string");
			}
			catch (ArgumentException)
			{
			}
		}

		// Expected, Path("") is invalid
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkCanCreateParent()
		{
			Path file = new Path(TestBaseDir1() + "/file");
			Path link = new Path(TestBaseDir2() + "/linkToFile");
			CreateAndWriteFile(file);
			wrapper.Delete(new Path(TestBaseDir2()), true);
			try
			{
				wrapper.CreateSymlink(file, link, false);
				NUnit.Framework.Assert.Fail("Created link without first creating parent dir");
			}
			catch (IOException)
			{
			}
			// Expected. Need to create testBaseDir2() first.
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(new Path(TestBaseDir2())));
			wrapper.CreateSymlink(file, link, true);
			ReadFile(link);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMkdirExistingLink()
		{
			Path file = new Path(TestBaseDir1() + "/targetFile");
			CreateAndWriteFile(file);
			Path dir = new Path(TestBaseDir1() + "/link");
			wrapper.CreateSymlink(file, dir, false);
			try
			{
				wrapper.Mkdir(dir, FileContext.DefaultPerm, false);
				NUnit.Framework.Assert.Fail("Created a dir where a symlink exists");
			}
			catch (FileAlreadyExistsException)
			{
			}
			catch (IOException)
			{
				// Expected. The symlink already exists.
				// LocalFs just throws an IOException
				NUnit.Framework.Assert.AreEqual("file", GetScheme());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateFileViaDanglingLinkParent()
		{
			Path dir = new Path(TestBaseDir1() + "/dangling");
			Path file = new Path(TestBaseDir1() + "/dangling/file");
			wrapper.CreateSymlink(new Path("/doesNotExist"), dir, false);
			FSDataOutputStream @out;
			try
			{
				@out = wrapper.Create(file, EnumSet.Of(CreateFlag.Create), Options.CreateOpts.RepFac
					((short)1), Options.CreateOpts.BlockSize(blockSize));
				@out.Close();
				NUnit.Framework.Assert.Fail("Created a link with dangling link parent");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// Expected. The parent is dangling.
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDeleteLink()
		{
			Path file = new Path(TestBaseDir1() + "/file");
			Path link = new Path(TestBaseDir1() + "/linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			ReadFile(link);
			wrapper.Delete(link, false);
			try
			{
				ReadFile(link);
				NUnit.Framework.Assert.Fail("Symlink should have been deleted");
			}
			catch (IOException)
			{
			}
			// Expected
			// If we deleted the link we can put it back
			wrapper.CreateSymlink(file, link, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOpenResolvesLinks()
		{
			Path file = new Path(TestBaseDir1() + "/noSuchFile");
			Path link = new Path(TestBaseDir1() + "/link");
			wrapper.CreateSymlink(file, link, false);
			try
			{
				wrapper.Open(link);
				NUnit.Framework.Assert.Fail("link target does not exist");
			}
			catch (FileNotFoundException)
			{
			}
			// Expected
			wrapper.Delete(link, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStatLinkToFile()
		{
			Assume.AssumeTrue(!EmulatingSymlinksOnWindows());
			Path file = new Path(TestBaseDir1() + "/file");
			Path linkToFile = new Path(TestBaseDir1() + "/linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, linkToFile, false);
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileLinkStatus(linkToFile).IsDirectory(
				));
			NUnit.Framework.Assert.IsTrue(wrapper.IsSymlink(linkToFile));
			NUnit.Framework.Assert.IsTrue(wrapper.IsFile(linkToFile));
			NUnit.Framework.Assert.IsFalse(wrapper.IsDir(linkToFile));
			NUnit.Framework.Assert.AreEqual(file, wrapper.GetLinkTarget(linkToFile));
			// The local file system does not fully resolve the link
			// when obtaining the file status
			if (!"file".Equals(GetScheme()))
			{
				NUnit.Framework.Assert.AreEqual(wrapper.GetFileStatus(file), wrapper.GetFileStatus
					(linkToFile));
				NUnit.Framework.Assert.AreEqual(wrapper.MakeQualified(file), wrapper.GetFileStatus
					(linkToFile).GetPath());
				NUnit.Framework.Assert.AreEqual(wrapper.MakeQualified(linkToFile), wrapper.GetFileLinkStatus
					(linkToFile).GetPath());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStatRelLinkToFile()
		{
			Assume.AssumeTrue(!"file".Equals(GetScheme()));
			Path file = new Path(TestBaseDir1(), "file");
			Path linkToFile = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(new Path("file"), linkToFile, false);
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileStatus(file), wrapper.GetFileStatus
				(linkToFile));
			NUnit.Framework.Assert.AreEqual(wrapper.MakeQualified(file), wrapper.GetFileStatus
				(linkToFile).GetPath());
			NUnit.Framework.Assert.AreEqual(wrapper.MakeQualified(linkToFile), wrapper.GetFileLinkStatus
				(linkToFile).GetPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStatLinkToDir()
		{
			Path dir = new Path(TestBaseDir1());
			Path linkToDir = new Path(TestBaseDir1() + "/linkToDir");
			wrapper.CreateSymlink(dir, linkToDir, false);
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileStatus(linkToDir).IsSymlink());
			NUnit.Framework.Assert.IsTrue(wrapper.IsDir(linkToDir));
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileLinkStatus(linkToDir).IsDirectory()
				);
			NUnit.Framework.Assert.IsTrue(wrapper.GetFileLinkStatus(linkToDir).IsSymlink());
			NUnit.Framework.Assert.IsFalse(wrapper.IsFile(linkToDir));
			NUnit.Framework.Assert.IsTrue(wrapper.IsDir(linkToDir));
			NUnit.Framework.Assert.AreEqual(dir, wrapper.GetLinkTarget(linkToDir));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStatDanglingLink()
		{
			Path file = new Path("/noSuchFile");
			Path link = new Path(TestBaseDir1() + "/link");
			wrapper.CreateSymlink(file, link, false);
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileLinkStatus(link).IsDirectory());
			NUnit.Framework.Assert.IsTrue(wrapper.GetFileLinkStatus(link).IsSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStatNonExistentFiles()
		{
			Path fileAbs = new Path("/doesNotExist");
			try
			{
				wrapper.GetFileLinkStatus(fileAbs);
				NUnit.Framework.Assert.Fail("Got FileStatus for non-existant file");
			}
			catch (FileNotFoundException)
			{
			}
			// Expected
			try
			{
				wrapper.GetLinkTarget(fileAbs);
				NUnit.Framework.Assert.Fail("Got link target for non-existant file");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStatNonLinks()
		{
			Path dir = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1() + "/file");
			CreateAndWriteFile(file);
			try
			{
				wrapper.GetLinkTarget(dir);
				NUnit.Framework.Assert.Fail("Lstat'd a non-symlink");
			}
			catch (IOException)
			{
			}
			// Expected.
			try
			{
				wrapper.GetLinkTarget(file);
				NUnit.Framework.Assert.Fail("Lstat'd a non-symlink");
			}
			catch (IOException)
			{
			}
		}

		// Expected.
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRecursiveLinks()
		{
			Path link1 = new Path(TestBaseDir1() + "/link1");
			Path link2 = new Path(TestBaseDir1() + "/link2");
			wrapper.CreateSymlink(link1, link2, false);
			wrapper.CreateSymlink(link2, link1, false);
			try
			{
				ReadFile(link1);
				NUnit.Framework.Assert.Fail("Read recursive link");
			}
			catch (FileNotFoundException)
			{
			}
			catch (IOException x)
			{
				// LocalFs throws sub class of IOException, since File.exists
				// returns false for a link to link.
				NUnit.Framework.Assert.AreEqual("Possible cyclic loop while following symbolic link "
					 + link1.ToString(), x.Message);
			}
		}

		/* Assert that the given link to a file behaves as expected. */
		/// <exception cref="System.IO.IOException"/>
		private void CheckLink(Path linkAbs, Path expectedTarget, Path targetQual)
		{
			// If we are emulating symlinks then many of these checks will fail
			// so we skip them.
			//
			Assume.AssumeTrue(!EmulatingSymlinksOnWindows());
			Path dir = new Path(TestBaseDir1());
			// isFile/Directory
			NUnit.Framework.Assert.IsTrue(wrapper.IsFile(linkAbs));
			NUnit.Framework.Assert.IsFalse(wrapper.IsDir(linkAbs));
			// Check getFileStatus
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileStatus(linkAbs).IsSymlink());
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileStatus(linkAbs).IsDirectory());
			NUnit.Framework.Assert.AreEqual(fileSize, wrapper.GetFileStatus(linkAbs).GetLen()
				);
			// Check getFileLinkStatus
			NUnit.Framework.Assert.IsTrue(wrapper.IsSymlink(linkAbs));
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileLinkStatus(linkAbs).IsDirectory());
			// Check getSymlink always returns a qualified target, except
			// when partially qualified paths are used (see tests below).
			NUnit.Framework.Assert.AreEqual(targetQual.ToString(), wrapper.GetFileLinkStatus(
				linkAbs).GetSymlink().ToString());
			NUnit.Framework.Assert.AreEqual(targetQual, wrapper.GetFileLinkStatus(linkAbs).GetSymlink
				());
			// Check that the target is qualified using the file system of the
			// path used to access the link (if the link target was not specified
			// fully qualified, in that case we use the link target verbatim).
			if (!"file".Equals(GetScheme()))
			{
				FileContext localFc = FileContext.GetLocalFSFileContext();
				Path linkQual = new Path(TestURI().ToString(), linkAbs);
				NUnit.Framework.Assert.AreEqual(targetQual, localFc.GetFileLinkStatus(linkQual).GetSymlink
					());
			}
			// Check getLinkTarget
			NUnit.Framework.Assert.AreEqual(expectedTarget, wrapper.GetLinkTarget(linkAbs));
			// Now read using all path types..
			wrapper.SetWorkingDirectory(dir);
			ReadFile(new Path("linkToFile"));
			ReadFile(linkAbs);
			// And fully qualified.. (NB: for local fs this is partially qualified)
			ReadFile(new Path(TestURI().ToString(), linkAbs));
			// And partially qualified..
			bool failureExpected = true;
			// local files are special cased, no authority
			if ("file".Equals(GetScheme()))
			{
				failureExpected = false;
			}
			else
			{
				// FileSystem automatically adds missing authority if scheme matches default
				if (wrapper is FileSystemTestWrapper)
				{
					failureExpected = false;
				}
			}
			try
			{
				ReadFile(new Path(GetScheme() + ":///" + TestBaseDir1() + "/linkToFile"));
				NUnit.Framework.Assert.IsFalse(failureExpected);
			}
			catch (Exception e)
			{
				if (!failureExpected)
				{
					throw new IOException(e);
				}
			}
			//assertTrue(failureExpected);
			// Now read using a different file context (for HDFS at least)
			if (wrapper is FileContextTestWrapper && !"file".Equals(GetScheme()))
			{
				FSTestWrapper localWrapper = wrapper.GetLocalFSWrapper();
				localWrapper.ReadFile(new Path(TestURI().ToString(), linkAbs), fileSize);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkUsingRelPaths()
		{
			Path fileAbs = new Path(TestBaseDir1(), "file");
			Path linkAbs = new Path(TestBaseDir1(), "linkToFile");
			Path schemeAuth = new Path(TestURI().ToString());
			Path fileQual = new Path(schemeAuth, TestBaseDir1() + "/file");
			CreateAndWriteFile(fileAbs);
			wrapper.SetWorkingDirectory(new Path(TestBaseDir1()));
			wrapper.CreateSymlink(new Path("file"), new Path("linkToFile"), false);
			CheckLink(linkAbs, new Path("file"), fileQual);
			// Now rename the link's parent. Because the target was specified
			// with a relative path the link should still resolve.
			Path dir1 = new Path(TestBaseDir1());
			Path dir2 = new Path(TestBaseDir2());
			Path linkViaDir2 = new Path(TestBaseDir2(), "linkToFile");
			Path fileViaDir2 = new Path(schemeAuth, TestBaseDir2() + "/file");
			wrapper.Rename(dir1, dir2, Options.Rename.Overwrite);
			FileStatus[] stats = wrapper.ListStatus(dir2);
			NUnit.Framework.Assert.AreEqual(fileViaDir2, wrapper.GetFileLinkStatus(linkViaDir2
				).GetSymlink());
			ReadFile(linkViaDir2);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkUsingAbsPaths()
		{
			Path fileAbs = new Path(TestBaseDir1() + "/file");
			Path linkAbs = new Path(TestBaseDir1() + "/linkToFile");
			Path schemeAuth = new Path(TestURI().ToString());
			Path fileQual = new Path(schemeAuth, TestBaseDir1() + "/file");
			CreateAndWriteFile(fileAbs);
			wrapper.CreateSymlink(fileAbs, linkAbs, false);
			CheckLink(linkAbs, fileAbs, fileQual);
			// Now rename the link's parent. The target doesn't change and
			// now no longer exists so accessing the link should fail.
			Path dir1 = new Path(TestBaseDir1());
			Path dir2 = new Path(TestBaseDir2());
			Path linkViaDir2 = new Path(TestBaseDir2(), "linkToFile");
			wrapper.Rename(dir1, dir2, Options.Rename.Overwrite);
			NUnit.Framework.Assert.AreEqual(fileQual, wrapper.GetFileLinkStatus(linkViaDir2).
				GetSymlink());
			try
			{
				ReadFile(linkViaDir2);
				NUnit.Framework.Assert.Fail("The target should not exist");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkUsingFullyQualPaths()
		{
			Path fileAbs = new Path(TestBaseDir1(), "file");
			Path linkAbs = new Path(TestBaseDir1(), "linkToFile");
			Path fileQual = new Path(TestURI().ToString(), fileAbs);
			Path linkQual = new Path(TestURI().ToString(), linkAbs);
			CreateAndWriteFile(fileAbs);
			wrapper.CreateSymlink(fileQual, linkQual, false);
			CheckLink(linkAbs, "file".Equals(GetScheme()) ? fileAbs : fileQual, fileQual);
			// Now rename the link's parent. The target doesn't change and
			// now no longer exists so accessing the link should fail.
			Path dir1 = new Path(TestBaseDir1());
			Path dir2 = new Path(TestBaseDir2());
			Path linkViaDir2 = new Path(TestBaseDir2(), "linkToFile");
			wrapper.Rename(dir1, dir2, Options.Rename.Overwrite);
			NUnit.Framework.Assert.AreEqual(fileQual, wrapper.GetFileLinkStatus(linkViaDir2).
				GetSymlink());
			try
			{
				ReadFile(linkViaDir2);
				NUnit.Framework.Assert.Fail("The target should not exist");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkUsingPartQualPath1()
		{
			// Partially qualified paths are covered for local file systems
			// in the previous test.
			Assume.AssumeTrue(!"file".Equals(GetScheme()));
			Path schemeAuth = new Path(TestURI().ToString());
			Path fileWoHost = new Path(GetScheme() + "://" + TestBaseDir1() + "/file");
			Path link = new Path(TestBaseDir1() + "/linkToFile");
			Path linkQual = new Path(schemeAuth, TestBaseDir1() + "/linkToFile");
			FSTestWrapper localWrapper = wrapper.GetLocalFSWrapper();
			wrapper.CreateSymlink(fileWoHost, link, false);
			// Partially qualified path is stored
			NUnit.Framework.Assert.AreEqual(fileWoHost, wrapper.GetLinkTarget(linkQual));
			// NB: We do not add an authority
			NUnit.Framework.Assert.AreEqual(fileWoHost.ToString(), wrapper.GetFileLinkStatus(
				link).GetSymlink().ToString());
			NUnit.Framework.Assert.AreEqual(fileWoHost.ToString(), wrapper.GetFileLinkStatus(
				linkQual).GetSymlink().ToString());
			// Ditto even from another file system
			if (wrapper is FileContextTestWrapper)
			{
				NUnit.Framework.Assert.AreEqual(fileWoHost.ToString(), localWrapper.GetFileLinkStatus
					(linkQual).GetSymlink().ToString());
			}
			// Same as if we accessed a partially qualified path directly
			try
			{
				ReadFile(link);
				NUnit.Framework.Assert.Fail("DFS requires URIs with schemes have an authority");
			}
			catch (RuntimeException)
			{
				NUnit.Framework.Assert.IsTrue(wrapper is FileContextTestWrapper);
			}
			catch (FileNotFoundException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(wrapper is FileSystemTestWrapper);
				GenericTestUtils.AssertExceptionContains("File does not exist: /test1/file", e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkUsingPartQualPath2()
		{
			Path link = new Path(TestBaseDir1(), "linkToFile");
			Path fileWoScheme = new Path("//" + TestURI().GetAuthority() + TestBaseDir1() + "/file"
				);
			if ("file".Equals(GetScheme()))
			{
				return;
			}
			wrapper.CreateSymlink(fileWoScheme, link, false);
			NUnit.Framework.Assert.AreEqual(fileWoScheme, wrapper.GetLinkTarget(link));
			NUnit.Framework.Assert.AreEqual(fileWoScheme.ToString(), wrapper.GetFileLinkStatus
				(link).GetSymlink().ToString());
			try
			{
				ReadFile(link);
				NUnit.Framework.Assert.Fail("Accessed a file with w/o scheme");
			}
			catch (IOException e)
			{
				// Expected
				if (wrapper is FileContextTestWrapper)
				{
					GenericTestUtils.AssertExceptionContains(AbstractFileSystem.NoAbstractFsError, e);
				}
				else
				{
					if (wrapper is FileSystemTestWrapper)
					{
						NUnit.Framework.Assert.AreEqual("No FileSystem for scheme: null", e.Message);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLinkStatusAndTargetWithNonLink()
		{
			Path schemeAuth = new Path(TestURI().ToString());
			Path dir = new Path(TestBaseDir1());
			Path dirQual = new Path(schemeAuth, dir.ToString());
			Path file = new Path(TestBaseDir1(), "file");
			Path fileQual = new Path(schemeAuth, file.ToString());
			CreateAndWriteFile(file);
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileStatus(file), wrapper.GetFileLinkStatus
				(file));
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileStatus(dir), wrapper.GetFileLinkStatus
				(dir));
			try
			{
				wrapper.GetLinkTarget(file);
				NUnit.Framework.Assert.Fail("Get link target on non-link should throw an IOException"
					);
			}
			catch (IOException x)
			{
				NUnit.Framework.Assert.AreEqual("Path " + fileQual + " is not a symbolic link", x
					.Message);
			}
			try
			{
				wrapper.GetLinkTarget(dir);
				NUnit.Framework.Assert.Fail("Get link target on non-link should throw an IOException"
					);
			}
			catch (IOException x)
			{
				NUnit.Framework.Assert.AreEqual("Path " + dirQual + " is not a symbolic link", x.
					Message);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkToDirectory()
		{
			Path dir1 = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(dir1, linkToDir, false);
			NUnit.Framework.Assert.IsFalse(wrapper.IsFile(linkToDir));
			NUnit.Framework.Assert.IsTrue(wrapper.IsDir(linkToDir));
			NUnit.Framework.Assert.IsTrue(wrapper.GetFileStatus(linkToDir).IsDirectory());
			NUnit.Framework.Assert.IsTrue(wrapper.GetFileLinkStatus(linkToDir).IsSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateFileViaSymlink()
		{
			Path dir = new Path(TestBaseDir1());
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path fileViaLink = new Path(linkToDir, "file");
			wrapper.CreateSymlink(dir, linkToDir, false);
			CreateAndWriteFile(fileViaLink);
			NUnit.Framework.Assert.IsTrue(wrapper.IsFile(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.IsDir(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileLinkStatus(fileViaLink).IsSymlink()
				);
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileStatus(fileViaLink).IsDirectory());
			ReadFile(fileViaLink);
			wrapper.Delete(fileViaLink, true);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(fileViaLink));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateDirViaSymlink()
		{
			Path dir1 = new Path(TestBaseDir1());
			Path subDir = new Path(TestBaseDir1(), "subDir");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path subDirViaLink = new Path(linkToDir, "subDir");
			wrapper.CreateSymlink(dir1, linkToDir, false);
			wrapper.Mkdir(subDirViaLink, FileContext.DefaultPerm, true);
			NUnit.Framework.Assert.IsTrue(wrapper.IsDir(subDirViaLink));
			wrapper.Delete(subDirViaLink, false);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(subDirViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(subDir));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkViaLink()
		{
			Assume.AssumeTrue(!EmulatingSymlinksOnWindows());
			Path dir1 = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path fileViaLink = new Path(linkToDir, "file");
			Path linkToFile = new Path(linkToDir, "linkToFile");
			/*
			* /b2/linkToDir            -> /b1
			* /b2/linkToDir/linkToFile -> /b2/linkToDir/file
			*/
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(dir1, linkToDir, false);
			wrapper.CreateSymlink(fileViaLink, linkToFile, false);
			NUnit.Framework.Assert.IsTrue(wrapper.IsFile(linkToFile));
			NUnit.Framework.Assert.IsTrue(wrapper.GetFileLinkStatus(linkToFile).IsSymlink());
			ReadFile(linkToFile);
			NUnit.Framework.Assert.AreEqual(fileSize, wrapper.GetFileStatus(linkToFile).GetLen
				());
			NUnit.Framework.Assert.AreEqual(fileViaLink, wrapper.GetLinkTarget(linkToFile));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestListStatusUsingLink()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "link");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(new Path(TestBaseDir1()), link, false);
			// The size of the result is file system dependent, Hdfs is 2 (file
			// and link) and LocalFs is 3 (file, link, file crc).
			FileStatus[] stats = wrapper.ListStatus(link);
			NUnit.Framework.Assert.IsTrue(stats.Length == 2 || stats.Length == 3);
			RemoteIterator<FileStatus> statsItor = wrapper.ListStatusIterator(link);
			int dirLen = 0;
			while (statsItor.HasNext())
			{
				statsItor.Next();
				dirLen++;
			}
			NUnit.Framework.Assert.IsTrue(dirLen == 2 || dirLen == 3);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkTwice()
		{
			Assume.AssumeTrue(!EmulatingSymlinksOnWindows());
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			try
			{
				wrapper.CreateSymlink(file, link, false);
				NUnit.Framework.Assert.Fail("link already exists");
			}
			catch (IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkToLink()
		{
			Path dir1 = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path linkToLink = new Path(TestBaseDir2(), "linkToLink");
			Path fileViaLink = new Path(TestBaseDir2(), "linkToLink/file");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(dir1, linkToDir, false);
			wrapper.CreateSymlink(linkToDir, linkToLink, false);
			NUnit.Framework.Assert.IsTrue(wrapper.IsFile(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.IsDir(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileLinkStatus(fileViaLink).IsSymlink()
				);
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileStatus(fileViaLink).IsDirectory());
			ReadFile(fileViaLink);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateFileDirExistingLink()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			try
			{
				CreateAndWriteFile(link);
				NUnit.Framework.Assert.Fail("link already exists");
			}
			catch (IOException)
			{
			}
			// Expected
			try
			{
				wrapper.Mkdir(link, FsPermission.GetDefault(), false);
				NUnit.Framework.Assert.Fail("link already exists");
			}
			catch (IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUseLinkAferDeleteLink()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			wrapper.Delete(link, false);
			try
			{
				ReadFile(link);
				NUnit.Framework.Assert.Fail("link was deleted");
			}
			catch (IOException)
			{
			}
			// Expected
			ReadFile(file);
			wrapper.CreateSymlink(file, link, false);
			ReadFile(link);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkToDot()
		{
			Path dir = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "linkToDot");
			CreateAndWriteFile(file);
			wrapper.SetWorkingDirectory(dir);
			try
			{
				wrapper.CreateSymlink(new Path("."), link, false);
				NUnit.Framework.Assert.Fail("Created symlink to dot");
			}
			catch (IOException)
			{
			}
		}

		// Expected. Path(".") resolves to "" because URI normalizes
		// the dot away and AbstractFileSystem considers "" invalid.
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkToDotDot()
		{
			Path file = new Path(TestBaseDir1(), "test/file");
			Path dotDot = new Path(TestBaseDir1(), "test/..");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path fileViaLink = new Path(linkToDir, "test/file");
			// Symlink to .. is not a problem since the .. is squashed early
			NUnit.Framework.Assert.AreEqual(new Path(TestBaseDir1()), dotDot);
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(dotDot, linkToDir, false);
			ReadFile(fileViaLink);
			NUnit.Framework.Assert.AreEqual(fileSize, wrapper.GetFileStatus(fileViaLink).GetLen
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkToDotDotPrefix()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path dir = new Path(TestBaseDir1(), "test");
			Path link = new Path(TestBaseDir1(), "test/link");
			CreateAndWriteFile(file);
			wrapper.Mkdir(dir, FsPermission.GetDefault(), false);
			wrapper.SetWorkingDirectory(dir);
			wrapper.CreateSymlink(new Path("../file"), link, false);
			ReadFile(link);
			NUnit.Framework.Assert.AreEqual(new Path("../file"), wrapper.GetLinkTarget(link));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameFileViaSymlink()
		{
			Path dir = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path fileViaLink = new Path(linkToDir, "file");
			Path fileNewViaLink = new Path(linkToDir, "fileNew");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(dir, linkToDir, false);
			wrapper.Rename(fileViaLink, fileNewViaLink);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(file));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(fileNewViaLink));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameFileToDestViaSymlink()
		{
			Path dir = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path subDir = new Path(linkToDir, "subDir");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(dir, linkToDir, false);
			wrapper.Mkdir(subDir, FileContext.DefaultPerm, false);
			try
			{
				wrapper.Rename(file, subDir);
				NUnit.Framework.Assert.Fail("Renamed file to a directory");
			}
			catch (IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is IOException);
			}
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(file));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameDirViaSymlink()
		{
			Path baseDir = new Path(TestBaseDir1());
			Path dir = new Path(baseDir, "dir");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path dirViaLink = new Path(linkToDir, "dir");
			Path dirNewViaLink = new Path(linkToDir, "dirNew");
			wrapper.Mkdir(dir, FileContext.DefaultPerm, false);
			wrapper.CreateSymlink(baseDir, linkToDir, false);
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(dirViaLink));
			wrapper.Rename(dirViaLink, dirNewViaLink);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(dirViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(dir));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(dirNewViaLink));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameSymlinkViaSymlink()
		{
			Path baseDir = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "link");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path linkViaLink = new Path(linkToDir, "link");
			Path linkNewViaLink = new Path(linkToDir, "linkNew");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			wrapper.CreateSymlink(baseDir, linkToDir, false);
			wrapper.Rename(linkViaLink, linkNewViaLink);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(linkViaLink));
			// Check that we didn't rename the link target
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(file));
			NUnit.Framework.Assert.IsTrue(wrapper.GetFileLinkStatus(linkNewViaLink).IsSymlink
				() || EmulatingSymlinksOnWindows());
			ReadFile(linkNewViaLink);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameDirToSymlinkToDir()
		{
			Path dir1 = new Path(TestBaseDir1());
			Path subDir = new Path(TestBaseDir2(), "subDir");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			wrapper.Mkdir(subDir, FileContext.DefaultPerm, false);
			wrapper.CreateSymlink(subDir, linkToDir, false);
			try
			{
				wrapper.Rename(dir1, linkToDir, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Renamed directory to a symlink");
			}
			catch (IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is IOException);
			}
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(dir1));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(linkToDir));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameDirToSymlinkToFile()
		{
			Path dir1 = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir2(), "file");
			Path linkToFile = new Path(TestBaseDir2(), "linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, linkToFile, false);
			try
			{
				wrapper.Rename(dir1, linkToFile, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Renamed directory to a symlink");
			}
			catch (IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is IOException);
			}
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(dir1));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(linkToFile));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameDirToDanglingSymlink()
		{
			Path dir = new Path(TestBaseDir1());
			Path link = new Path(TestBaseDir2(), "linkToFile");
			wrapper.CreateSymlink(new Path("/doesNotExist"), link, false);
			try
			{
				wrapper.Rename(dir, link, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Renamed directory to a symlink");
			}
			catch (IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is IOException);
			}
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(dir));
			NUnit.Framework.Assert.IsTrue(wrapper.GetFileLinkStatus(link) != null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameFileToSymlinkToDir()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path subDir = new Path(TestBaseDir1(), "subDir");
			Path link = new Path(TestBaseDir1(), "link");
			wrapper.Mkdir(subDir, FileContext.DefaultPerm, false);
			wrapper.CreateSymlink(subDir, link, false);
			CreateAndWriteFile(file);
			try
			{
				wrapper.Rename(file, link);
				NUnit.Framework.Assert.Fail("Renamed file to symlink w/o overwrite");
			}
			catch (IOException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			wrapper.Rename(file, link, Options.Rename.Overwrite);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(file));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.IsFile(link));
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileLinkStatus(link).IsSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameFileToSymlinkToFile()
		{
			Path file1 = new Path(TestBaseDir1(), "file1");
			Path file2 = new Path(TestBaseDir1(), "file2");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file1);
			CreateAndWriteFile(file2);
			wrapper.CreateSymlink(file2, link, false);
			try
			{
				wrapper.Rename(file1, link);
				NUnit.Framework.Assert.Fail("Renamed file to symlink w/o overwrite");
			}
			catch (IOException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			wrapper.Rename(file1, link, Options.Rename.Overwrite);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(file1));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.IsFile(link));
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileLinkStatus(link).IsSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameFileToDanglingSymlink()
		{
			/* NB: Local file system doesn't handle dangling links correctly
			* since File.exists(danglinLink) returns false. */
			if ("file".Equals(GetScheme()))
			{
				return;
			}
			Path file1 = new Path(TestBaseDir1(), "file1");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file1);
			wrapper.CreateSymlink(new Path("/doesNotExist"), link, false);
			try
			{
				wrapper.Rename(file1, link);
			}
			catch (IOException)
			{
			}
			// Expected
			wrapper.Rename(file1, link, Options.Rename.Overwrite);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(file1));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.IsFile(link));
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileLinkStatus(link).IsSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameSymlinkNonExistantDest()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path link1 = new Path(TestBaseDir1(), "linkToFile1");
			Path link2 = new Path(TestBaseDir1(), "linkToFile2");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link1, false);
			wrapper.Rename(link1, link2);
			NUnit.Framework.Assert.IsTrue(wrapper.GetFileLinkStatus(link2).IsSymlink() || EmulatingSymlinksOnWindows
				());
			ReadFile(link2);
			ReadFile(file);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(link1));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameSymlinkToExistingFile()
		{
			Path file1 = new Path(TestBaseDir1(), "file");
			Path file2 = new Path(TestBaseDir1(), "someFile");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file1);
			CreateAndWriteFile(file2);
			wrapper.CreateSymlink(file2, link, false);
			try
			{
				wrapper.Rename(link, file1);
				NUnit.Framework.Assert.Fail("Renamed w/o passing overwrite");
			}
			catch (IOException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			wrapper.Rename(link, file1, Options.Rename.Overwrite);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(link));
			if (!EmulatingSymlinksOnWindows())
			{
				NUnit.Framework.Assert.IsTrue(wrapper.GetFileLinkStatus(file1).IsSymlink());
				NUnit.Framework.Assert.AreEqual(file2, wrapper.GetLinkTarget(file1));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameSymlinkToExistingDir()
		{
			Path dir1 = new Path(TestBaseDir1());
			Path dir2 = new Path(TestBaseDir2());
			Path subDir = new Path(TestBaseDir2(), "subDir");
			Path link = new Path(TestBaseDir1(), "linkToDir");
			wrapper.CreateSymlink(dir1, link, false);
			try
			{
				wrapper.Rename(link, dir2);
				NUnit.Framework.Assert.Fail("Renamed link to a directory");
			}
			catch (IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is IOException);
			}
			try
			{
				wrapper.Rename(link, dir2, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Renamed link to a directory");
			}
			catch (IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is IOException);
			}
			// Also fails when dir2 has a sub-directory
			wrapper.Mkdir(subDir, FsPermission.GetDefault(), false);
			try
			{
				wrapper.Rename(link, dir2, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Renamed link to a directory");
			}
			catch (IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is IOException);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameSymlinkToItself()
		{
			Path file = new Path(TestBaseDir1(), "file");
			CreateAndWriteFile(file);
			Path link = new Path(TestBaseDir1(), "linkToFile1");
			wrapper.CreateSymlink(file, link, false);
			try
			{
				wrapper.Rename(link, link);
				NUnit.Framework.Assert.Fail("Failed to get expected IOException");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Fails with overwrite as well
			try
			{
				wrapper.Rename(link, link, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Failed to get expected IOException");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameSymlink()
		{
			Assume.AssumeTrue(!EmulatingSymlinksOnWindows());
			Path file = new Path(TestBaseDir1(), "file");
			Path link1 = new Path(TestBaseDir1(), "linkToFile1");
			Path link2 = new Path(TestBaseDir1(), "linkToFile2");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link1, false);
			wrapper.Rename(link1, link2);
			NUnit.Framework.Assert.IsTrue(wrapper.GetFileLinkStatus(link2).IsSymlink());
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileStatus(link2).IsDirectory());
			ReadFile(link2);
			ReadFile(file);
			try
			{
				CreateAndWriteFile(link2);
				NUnit.Framework.Assert.Fail("link was not renamed");
			}
			catch (IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameSymlinkToFileItLinksTo()
		{
			/* NB: The rename is not atomic, so file is deleted before renaming
			* linkToFile. In this interval linkToFile is dangling and local file
			* system does not handle dangling links because File.exists returns
			* false for dangling links. */
			if ("file".Equals(GetScheme()))
			{
				return;
			}
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			try
			{
				wrapper.Rename(link, file);
				NUnit.Framework.Assert.Fail("Renamed symlink to its target");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Check the rename didn't happen
			NUnit.Framework.Assert.IsTrue(wrapper.IsFile(file));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.IsSymlink(link));
			NUnit.Framework.Assert.AreEqual(file, wrapper.GetLinkTarget(link));
			try
			{
				wrapper.Rename(link, file, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Renamed symlink to its target");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Check the rename didn't happen
			NUnit.Framework.Assert.IsTrue(wrapper.IsFile(file));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.IsSymlink(link));
			NUnit.Framework.Assert.AreEqual(file, wrapper.GetLinkTarget(link));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameSymlinkToDirItLinksTo()
		{
			/* NB: The rename is not atomic, so dir is deleted before renaming
			* linkToFile. In this interval linkToFile is dangling and local file
			* system does not handle dangling links because File.exists returns
			* false for dangling links. */
			if ("file".Equals(GetScheme()))
			{
				return;
			}
			Path dir = new Path(TestBaseDir1(), "dir");
			Path link = new Path(TestBaseDir1(), "linkToDir");
			wrapper.Mkdir(dir, FileContext.DefaultPerm, false);
			wrapper.CreateSymlink(dir, link, false);
			try
			{
				wrapper.Rename(link, dir);
				NUnit.Framework.Assert.Fail("Renamed symlink to its target");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Check the rename didn't happen
			NUnit.Framework.Assert.IsTrue(wrapper.IsDir(dir));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.IsSymlink(link));
			NUnit.Framework.Assert.AreEqual(dir, wrapper.GetLinkTarget(link));
			try
			{
				wrapper.Rename(link, dir, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Renamed symlink to its target");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Check the rename didn't happen
			NUnit.Framework.Assert.IsTrue(wrapper.IsDir(dir));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.IsSymlink(link));
			NUnit.Framework.Assert.AreEqual(dir, wrapper.GetLinkTarget(link));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameLinkTarget()
		{
			Assume.AssumeTrue(!EmulatingSymlinksOnWindows());
			Path file = new Path(TestBaseDir1(), "file");
			Path fileNew = new Path(TestBaseDir1(), "fileNew");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			wrapper.Rename(file, fileNew, Options.Rename.Overwrite);
			try
			{
				ReadFile(link);
				NUnit.Framework.Assert.Fail("Link should be dangling");
			}
			catch (IOException)
			{
			}
			// Expected
			wrapper.Rename(fileNew, file, Options.Rename.Overwrite);
			ReadFile(link);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameFileWithDestParentSymlink()
		{
			Path link = new Path(TestBaseDir1(), "link");
			Path file1 = new Path(TestBaseDir1(), "file1");
			Path file2 = new Path(TestBaseDir1(), "file2");
			Path file3 = new Path(link, "file3");
			Path dir2 = new Path(TestBaseDir2());
			// Renaming /dir1/file1 to non-existant file /dir1/link/file3 is OK
			// if link points to a directory...
			wrapper.CreateSymlink(dir2, link, false);
			CreateAndWriteFile(file1);
			wrapper.Rename(file1, file3);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(file1));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(file3));
			wrapper.Rename(file3, file1);
			// But fails if link is dangling...
			wrapper.Delete(link, false);
			wrapper.CreateSymlink(file2, link, false);
			try
			{
				wrapper.Rename(file1, file3);
			}
			catch (IOException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is FileNotFoundException);
			}
			// And if link points to a file...
			CreateAndWriteFile(file2);
			try
			{
				wrapper.Rename(file1, file3);
			}
			catch (IOException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(UnwrapException(e) is ParentNotDirectoryException);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAccessFileViaInterSymlinkAbsTarget()
		{
			Path baseDir = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path fileNew = new Path(baseDir, "fileNew");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path fileViaLink = new Path(linkToDir, "file");
			Path fileNewViaLink = new Path(linkToDir, "fileNew");
			wrapper.CreateSymlink(baseDir, linkToDir, false);
			CreateAndWriteFile(fileViaLink);
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(fileViaLink));
			NUnit.Framework.Assert.IsTrue(wrapper.IsFile(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.IsDir(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.GetFileLinkStatus(fileViaLink).IsSymlink()
				);
			NUnit.Framework.Assert.IsFalse(wrapper.IsDir(fileViaLink));
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileStatus(file), wrapper.GetFileLinkStatus
				(file));
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileStatus(fileViaLink), wrapper.GetFileLinkStatus
				(fileViaLink));
			ReadFile(fileViaLink);
			AppendToFile(fileViaLink);
			wrapper.Rename(fileViaLink, fileNewViaLink);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(fileViaLink));
			NUnit.Framework.Assert.IsTrue(wrapper.Exists(fileNewViaLink));
			ReadFile(fileNewViaLink);
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileBlockLocations(fileNew, 0, 1).Length
				, wrapper.GetFileBlockLocations(fileNewViaLink, 0, 1).Length);
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileChecksum(fileNew), wrapper.GetFileChecksum
				(fileNewViaLink));
			wrapper.Delete(fileNewViaLink, true);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(fileNewViaLink));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAccessFileViaInterSymlinkQualTarget()
		{
			Path baseDir = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path fileViaLink = new Path(linkToDir, "file");
			wrapper.CreateSymlink(wrapper.MakeQualified(baseDir), linkToDir, false);
			CreateAndWriteFile(fileViaLink);
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileStatus(file), wrapper.GetFileLinkStatus
				(file));
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileStatus(fileViaLink), wrapper.GetFileLinkStatus
				(fileViaLink));
			ReadFile(fileViaLink);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAccessFileViaInterSymlinkRelTarget()
		{
			Assume.AssumeTrue(!"file".Equals(GetScheme()));
			Path dir = new Path(TestBaseDir1(), "dir");
			Path file = new Path(dir, "file");
			Path linkToDir = new Path(TestBaseDir1(), "linkToDir");
			Path fileViaLink = new Path(linkToDir, "file");
			wrapper.Mkdir(dir, FileContext.DefaultPerm, false);
			wrapper.CreateSymlink(new Path("dir"), linkToDir, false);
			CreateAndWriteFile(fileViaLink);
			// Note that getFileStatus returns fully qualified paths even
			// when called on an absolute path.
			NUnit.Framework.Assert.AreEqual(wrapper.MakeQualified(file), wrapper.GetFileStatus
				(file).GetPath());
			// In each case getFileLinkStatus returns the same FileStatus
			// as getFileStatus since we're not calling it on a link and
			// FileStatus objects are compared by Path.
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileStatus(file), wrapper.GetFileLinkStatus
				(file));
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileStatus(fileViaLink), wrapper.GetFileLinkStatus
				(fileViaLink));
			NUnit.Framework.Assert.AreEqual(wrapper.GetFileStatus(fileViaLink), wrapper.GetFileLinkStatus
				(file));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAccessDirViaSymlink()
		{
			Path baseDir = new Path(TestBaseDir1());
			Path dir = new Path(TestBaseDir1(), "dir");
			Path linkToDir = new Path(TestBaseDir2(), "linkToDir");
			Path dirViaLink = new Path(linkToDir, "dir");
			wrapper.CreateSymlink(baseDir, linkToDir, false);
			wrapper.Mkdir(dirViaLink, FileContext.DefaultPerm, true);
			NUnit.Framework.Assert.IsTrue(wrapper.GetFileStatus(dirViaLink).IsDirectory());
			FileStatus[] stats = wrapper.ListStatus(dirViaLink);
			NUnit.Framework.Assert.AreEqual(0, stats.Length);
			RemoteIterator<FileStatus> statsItor = wrapper.ListStatusIterator(dirViaLink);
			NUnit.Framework.Assert.IsFalse(statsItor.HasNext());
			wrapper.Delete(dirViaLink, false);
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(dirViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.Exists(dir));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetTimes()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			long at = wrapper.GetFileLinkStatus(link).GetAccessTime();
			wrapper.SetTimes(link, 2L, 3L);
			// NB: local file systems don't implement setTimes
			if (!"file".Equals(GetScheme()))
			{
				NUnit.Framework.Assert.AreEqual(at, wrapper.GetFileLinkStatus(link).GetAccessTime
					());
				NUnit.Framework.Assert.AreEqual(3, wrapper.GetFileStatus(file).GetAccessTime());
				NUnit.Framework.Assert.AreEqual(2, wrapper.GetFileStatus(file).GetModificationTime
					());
			}
		}
	}
}
