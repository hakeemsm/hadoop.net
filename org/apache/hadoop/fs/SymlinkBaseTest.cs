using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Base test for symbolic links</summary>
	public abstract class SymlinkBaseTest
	{
		static SymlinkBaseTest()
		{
			// Re-enable symlinks for tests, see HADOOP-10020 and HADOOP-10052
			org.apache.hadoop.fs.FileSystem.enableSymlinks();
		}

		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 8192;

		internal const int fileSize = 16384;

		internal const int numBlocks = fileSize / blockSize;

		protected internal static org.apache.hadoop.fs.FSTestWrapper wrapper;

		protected internal abstract string getScheme();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract string testBaseDir1();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract string testBaseDir2();

		protected internal abstract java.net.URI testURI();

		// Returns true if the filesystem is emulating symlink support. Certain
		// checks will be bypassed if that is the case.
		//
		protected internal virtual bool emulatingSymlinksOnWindows()
		{
			return false;
		}

		protected internal virtual System.IO.IOException unwrapException(System.IO.IOException
			 e)
		{
			return e;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal static void createAndWriteFile(org.apache.hadoop.fs.Path p)
		{
			createAndWriteFile(wrapper, p);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal static void createAndWriteFile(org.apache.hadoop.fs.FSTestWrapper
			 wrapper, org.apache.hadoop.fs.Path p)
		{
			wrapper.createFile(p, numBlocks, org.apache.hadoop.fs.Options.CreateOpts.createParent
				(), org.apache.hadoop.fs.Options.CreateOpts.repFac((short)1), org.apache.hadoop.fs.Options.CreateOpts
				.blockSize(blockSize));
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal static void readFile(org.apache.hadoop.fs.Path p)
		{
			wrapper.readFile(p, fileSize);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal static void appendToFile(org.apache.hadoop.fs.Path p)
		{
			wrapper.appendToFile(p, numBlocks, org.apache.hadoop.fs.Options.CreateOpts.blockSize
				(blockSize));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			wrapper.mkdir(new org.apache.hadoop.fs.Path(testBaseDir1()), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, true);
			wrapper.mkdir(new org.apache.hadoop.fs.Path(testBaseDir2()), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			wrapper.delete(new org.apache.hadoop.fs.Path(testBaseDir1()), true);
			wrapper.delete(new org.apache.hadoop.fs.Path(testBaseDir2()), true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testStatRoot()
		{
			NUnit.Framework.Assert.IsFalse(wrapper.getFileLinkStatus(new org.apache.hadoop.fs.Path
				("/")).isSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testSetWDNotResolvesLinks()
		{
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir1(
				) + "/link");
			wrapper.createSymlink(dir, linkToDir, false);
			wrapper.setWorkingDirectory(linkToDir);
			NUnit.Framework.Assert.AreEqual(linkToDir.getName(), wrapper.getWorkingDirectory(
				).getName());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateDanglingLink()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path("/noSuchFile");
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1() + "/link"
				);
			wrapper.createSymlink(file, link, false);
			try
			{
				wrapper.getFileStatus(link);
				NUnit.Framework.Assert.Fail("Got file status of non-existant file");
			}
			catch (java.io.FileNotFoundException)
			{
			}
			// Expected
			wrapper.delete(link, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkToNullEmpty()
		{
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1() + "/link"
				);
			try
			{
				wrapper.createSymlink(null, link, false);
				NUnit.Framework.Assert.Fail("Can't create symlink to null");
			}
			catch (System.ArgumentNullException)
			{
			}
			// Expected, create* with null yields NPEs
			try
			{
				wrapper.createSymlink(new org.apache.hadoop.fs.Path(string.Empty), link, false);
				NUnit.Framework.Assert.Fail("Can't create symlink to empty string");
			}
			catch (System.ArgumentException)
			{
			}
		}

		// Expected, Path("") is invalid
		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkCanCreateParent()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1() + "/file"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir2() + "/linkToFile"
				);
			createAndWriteFile(file);
			wrapper.delete(new org.apache.hadoop.fs.Path(testBaseDir2()), true);
			try
			{
				wrapper.createSymlink(file, link, false);
				NUnit.Framework.Assert.Fail("Created link without first creating parent dir");
			}
			catch (System.IO.IOException)
			{
			}
			// Expected. Need to create testBaseDir2() first.
			NUnit.Framework.Assert.IsFalse(wrapper.exists(new org.apache.hadoop.fs.Path(testBaseDir2
				())));
			wrapper.createSymlink(file, link, true);
			readFile(link);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testMkdirExistingLink()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1() + "/targetFile"
				);
			createAndWriteFile(file);
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1() + "/link"
				);
			wrapper.createSymlink(file, dir, false);
			try
			{
				wrapper.mkdir(dir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, false);
				NUnit.Framework.Assert.Fail("Created a dir where a symlink exists");
			}
			catch (org.apache.hadoop.fs.FileAlreadyExistsException)
			{
			}
			catch (System.IO.IOException)
			{
				// Expected. The symlink already exists.
				// LocalFs just throws an IOException
				NUnit.Framework.Assert.AreEqual("file", getScheme());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateFileViaDanglingLinkParent()
		{
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1() + "/dangling"
				);
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1() + "/dangling/file"
				);
			wrapper.createSymlink(new org.apache.hadoop.fs.Path("/doesNotExist"), dir, false);
			org.apache.hadoop.fs.FSDataOutputStream @out;
			try
			{
				@out = wrapper.create(file, java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag.
					CREATE), org.apache.hadoop.fs.Options.CreateOpts.repFac((short)1), org.apache.hadoop.fs.Options.CreateOpts
					.blockSize(blockSize));
				@out.close();
				NUnit.Framework.Assert.Fail("Created a link with dangling link parent");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// Expected. The parent is dangling.
		/// <exception cref="System.IO.IOException"/>
		public virtual void testDeleteLink()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1() + "/file"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1() + "/linkToFile"
				);
			createAndWriteFile(file);
			wrapper.createSymlink(file, link, false);
			readFile(link);
			wrapper.delete(link, false);
			try
			{
				readFile(link);
				NUnit.Framework.Assert.Fail("Symlink should have been deleted");
			}
			catch (System.IO.IOException)
			{
			}
			// Expected
			// If we deleted the link we can put it back
			wrapper.createSymlink(file, link, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testOpenResolvesLinks()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1() + "/noSuchFile"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1() + "/link"
				);
			wrapper.createSymlink(file, link, false);
			try
			{
				wrapper.open(link);
				NUnit.Framework.Assert.Fail("link target does not exist");
			}
			catch (java.io.FileNotFoundException)
			{
			}
			// Expected
			wrapper.delete(link, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testStatLinkToFile()
		{
			NUnit.Framework.Assume.assumeTrue(!emulatingSymlinksOnWindows());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1() + "/file"
				);
			org.apache.hadoop.fs.Path linkToFile = new org.apache.hadoop.fs.Path(testBaseDir1
				() + "/linkToFile");
			createAndWriteFile(file);
			wrapper.createSymlink(file, linkToFile, false);
			NUnit.Framework.Assert.IsFalse(wrapper.getFileLinkStatus(linkToFile).isDirectory(
				));
			NUnit.Framework.Assert.IsTrue(wrapper.isSymlink(linkToFile));
			NUnit.Framework.Assert.IsTrue(wrapper.isFile(linkToFile));
			NUnit.Framework.Assert.IsFalse(wrapper.isDir(linkToFile));
			NUnit.Framework.Assert.AreEqual(file, wrapper.getLinkTarget(linkToFile));
			// The local file system does not fully resolve the link
			// when obtaining the file status
			if (!"file".Equals(getScheme()))
			{
				NUnit.Framework.Assert.AreEqual(wrapper.getFileStatus(file), wrapper.getFileStatus
					(linkToFile));
				NUnit.Framework.Assert.AreEqual(wrapper.makeQualified(file), wrapper.getFileStatus
					(linkToFile).getPath());
				NUnit.Framework.Assert.AreEqual(wrapper.makeQualified(linkToFile), wrapper.getFileLinkStatus
					(linkToFile).getPath());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testStatRelLinkToFile()
		{
			NUnit.Framework.Assume.assumeTrue(!"file".Equals(getScheme()));
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path linkToFile = new org.apache.hadoop.fs.Path(testBaseDir1
				(), "linkToFile");
			createAndWriteFile(file);
			wrapper.createSymlink(new org.apache.hadoop.fs.Path("file"), linkToFile, false);
			NUnit.Framework.Assert.AreEqual(wrapper.getFileStatus(file), wrapper.getFileStatus
				(linkToFile));
			NUnit.Framework.Assert.AreEqual(wrapper.makeQualified(file), wrapper.getFileStatus
				(linkToFile).getPath());
			NUnit.Framework.Assert.AreEqual(wrapper.makeQualified(linkToFile), wrapper.getFileLinkStatus
				(linkToFile).getPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testStatLinkToDir()
		{
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir1(
				) + "/linkToDir");
			wrapper.createSymlink(dir, linkToDir, false);
			NUnit.Framework.Assert.IsFalse(wrapper.getFileStatus(linkToDir).isSymlink());
			NUnit.Framework.Assert.IsTrue(wrapper.isDir(linkToDir));
			NUnit.Framework.Assert.IsFalse(wrapper.getFileLinkStatus(linkToDir).isDirectory()
				);
			NUnit.Framework.Assert.IsTrue(wrapper.getFileLinkStatus(linkToDir).isSymlink());
			NUnit.Framework.Assert.IsFalse(wrapper.isFile(linkToDir));
			NUnit.Framework.Assert.IsTrue(wrapper.isDir(linkToDir));
			NUnit.Framework.Assert.AreEqual(dir, wrapper.getLinkTarget(linkToDir));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testStatDanglingLink()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path("/noSuchFile");
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1() + "/link"
				);
			wrapper.createSymlink(file, link, false);
			NUnit.Framework.Assert.IsFalse(wrapper.getFileLinkStatus(link).isDirectory());
			NUnit.Framework.Assert.IsTrue(wrapper.getFileLinkStatus(link).isSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testStatNonExistentFiles()
		{
			org.apache.hadoop.fs.Path fileAbs = new org.apache.hadoop.fs.Path("/doesNotExist"
				);
			try
			{
				wrapper.getFileLinkStatus(fileAbs);
				NUnit.Framework.Assert.Fail("Got FileStatus for non-existant file");
			}
			catch (java.io.FileNotFoundException)
			{
			}
			// Expected
			try
			{
				wrapper.getLinkTarget(fileAbs);
				NUnit.Framework.Assert.Fail("Got link target for non-existant file");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void testStatNonLinks()
		{
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1() + "/file"
				);
			createAndWriteFile(file);
			try
			{
				wrapper.getLinkTarget(dir);
				NUnit.Framework.Assert.Fail("Lstat'd a non-symlink");
			}
			catch (System.IO.IOException)
			{
			}
			// Expected.
			try
			{
				wrapper.getLinkTarget(file);
				NUnit.Framework.Assert.Fail("Lstat'd a non-symlink");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// Expected.
		/// <exception cref="System.IO.IOException"/>
		public virtual void testRecursiveLinks()
		{
			org.apache.hadoop.fs.Path link1 = new org.apache.hadoop.fs.Path(testBaseDir1() + 
				"/link1");
			org.apache.hadoop.fs.Path link2 = new org.apache.hadoop.fs.Path(testBaseDir1() + 
				"/link2");
			wrapper.createSymlink(link1, link2, false);
			wrapper.createSymlink(link2, link1, false);
			try
			{
				readFile(link1);
				NUnit.Framework.Assert.Fail("Read recursive link");
			}
			catch (java.io.FileNotFoundException)
			{
			}
			catch (System.IO.IOException x)
			{
				// LocalFs throws sub class of IOException, since File.exists
				// returns false for a link to link.
				NUnit.Framework.Assert.AreEqual("Possible cyclic loop while following symbolic link "
					 + link1.ToString(), x.Message);
			}
		}

		/* Assert that the given link to a file behaves as expected. */
		/// <exception cref="System.IO.IOException"/>
		private void checkLink(org.apache.hadoop.fs.Path linkAbs, org.apache.hadoop.fs.Path
			 expectedTarget, org.apache.hadoop.fs.Path targetQual)
		{
			// If we are emulating symlinks then many of these checks will fail
			// so we skip them.
			//
			NUnit.Framework.Assume.assumeTrue(!emulatingSymlinksOnWindows());
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1());
			// isFile/Directory
			NUnit.Framework.Assert.IsTrue(wrapper.isFile(linkAbs));
			NUnit.Framework.Assert.IsFalse(wrapper.isDir(linkAbs));
			// Check getFileStatus
			NUnit.Framework.Assert.IsFalse(wrapper.getFileStatus(linkAbs).isSymlink());
			NUnit.Framework.Assert.IsFalse(wrapper.getFileStatus(linkAbs).isDirectory());
			NUnit.Framework.Assert.AreEqual(fileSize, wrapper.getFileStatus(linkAbs).getLen()
				);
			// Check getFileLinkStatus
			NUnit.Framework.Assert.IsTrue(wrapper.isSymlink(linkAbs));
			NUnit.Framework.Assert.IsFalse(wrapper.getFileLinkStatus(linkAbs).isDirectory());
			// Check getSymlink always returns a qualified target, except
			// when partially qualified paths are used (see tests below).
			NUnit.Framework.Assert.AreEqual(targetQual.ToString(), wrapper.getFileLinkStatus(
				linkAbs).getSymlink().ToString());
			NUnit.Framework.Assert.AreEqual(targetQual, wrapper.getFileLinkStatus(linkAbs).getSymlink
				());
			// Check that the target is qualified using the file system of the
			// path used to access the link (if the link target was not specified
			// fully qualified, in that case we use the link target verbatim).
			if (!"file".Equals(getScheme()))
			{
				org.apache.hadoop.fs.FileContext localFc = org.apache.hadoop.fs.FileContext.getLocalFSFileContext
					();
				org.apache.hadoop.fs.Path linkQual = new org.apache.hadoop.fs.Path(testURI().ToString
					(), linkAbs);
				NUnit.Framework.Assert.AreEqual(targetQual, localFc.getFileLinkStatus(linkQual).getSymlink
					());
			}
			// Check getLinkTarget
			NUnit.Framework.Assert.AreEqual(expectedTarget, wrapper.getLinkTarget(linkAbs));
			// Now read using all path types..
			wrapper.setWorkingDirectory(dir);
			readFile(new org.apache.hadoop.fs.Path("linkToFile"));
			readFile(linkAbs);
			// And fully qualified.. (NB: for local fs this is partially qualified)
			readFile(new org.apache.hadoop.fs.Path(testURI().ToString(), linkAbs));
			// And partially qualified..
			bool failureExpected = true;
			// local files are special cased, no authority
			if ("file".Equals(getScheme()))
			{
				failureExpected = false;
			}
			else
			{
				// FileSystem automatically adds missing authority if scheme matches default
				if (wrapper is org.apache.hadoop.fs.FileSystemTestWrapper)
				{
					failureExpected = false;
				}
			}
			try
			{
				readFile(new org.apache.hadoop.fs.Path(getScheme() + ":///" + testBaseDir1() + "/linkToFile"
					));
				NUnit.Framework.Assert.IsFalse(failureExpected);
			}
			catch (System.Exception e)
			{
				if (!failureExpected)
				{
					throw new System.IO.IOException(e);
				}
			}
			//assertTrue(failureExpected);
			// Now read using a different file context (for HDFS at least)
			if (wrapper is org.apache.hadoop.fs.FileContextTestWrapper && !"file".Equals(getScheme
				()))
			{
				org.apache.hadoop.fs.FSTestWrapper localWrapper = wrapper.getLocalFSWrapper();
				localWrapper.readFile(new org.apache.hadoop.fs.Path(testURI().ToString(), linkAbs
					), fileSize);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkUsingRelPaths()
		{
			org.apache.hadoop.fs.Path fileAbs = new org.apache.hadoop.fs.Path(testBaseDir1(), 
				"file");
			org.apache.hadoop.fs.Path linkAbs = new org.apache.hadoop.fs.Path(testBaseDir1(), 
				"linkToFile");
			org.apache.hadoop.fs.Path schemeAuth = new org.apache.hadoop.fs.Path(testURI().ToString
				());
			org.apache.hadoop.fs.Path fileQual = new org.apache.hadoop.fs.Path(schemeAuth, testBaseDir1
				() + "/file");
			createAndWriteFile(fileAbs);
			wrapper.setWorkingDirectory(new org.apache.hadoop.fs.Path(testBaseDir1()));
			wrapper.createSymlink(new org.apache.hadoop.fs.Path("file"), new org.apache.hadoop.fs.Path
				("linkToFile"), false);
			checkLink(linkAbs, new org.apache.hadoop.fs.Path("file"), fileQual);
			// Now rename the link's parent. Because the target was specified
			// with a relative path the link should still resolve.
			org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path dir2 = new org.apache.hadoop.fs.Path(testBaseDir2());
			org.apache.hadoop.fs.Path linkViaDir2 = new org.apache.hadoop.fs.Path(testBaseDir2
				(), "linkToFile");
			org.apache.hadoop.fs.Path fileViaDir2 = new org.apache.hadoop.fs.Path(schemeAuth, 
				testBaseDir2() + "/file");
			wrapper.rename(dir1, dir2, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
			org.apache.hadoop.fs.FileStatus[] stats = wrapper.listStatus(dir2);
			NUnit.Framework.Assert.AreEqual(fileViaDir2, wrapper.getFileLinkStatus(linkViaDir2
				).getSymlink());
			readFile(linkViaDir2);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkUsingAbsPaths()
		{
			org.apache.hadoop.fs.Path fileAbs = new org.apache.hadoop.fs.Path(testBaseDir1() 
				+ "/file");
			org.apache.hadoop.fs.Path linkAbs = new org.apache.hadoop.fs.Path(testBaseDir1() 
				+ "/linkToFile");
			org.apache.hadoop.fs.Path schemeAuth = new org.apache.hadoop.fs.Path(testURI().ToString
				());
			org.apache.hadoop.fs.Path fileQual = new org.apache.hadoop.fs.Path(schemeAuth, testBaseDir1
				() + "/file");
			createAndWriteFile(fileAbs);
			wrapper.createSymlink(fileAbs, linkAbs, false);
			checkLink(linkAbs, fileAbs, fileQual);
			// Now rename the link's parent. The target doesn't change and
			// now no longer exists so accessing the link should fail.
			org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path dir2 = new org.apache.hadoop.fs.Path(testBaseDir2());
			org.apache.hadoop.fs.Path linkViaDir2 = new org.apache.hadoop.fs.Path(testBaseDir2
				(), "linkToFile");
			wrapper.rename(dir1, dir2, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
			NUnit.Framework.Assert.AreEqual(fileQual, wrapper.getFileLinkStatus(linkViaDir2).
				getSymlink());
			try
			{
				readFile(linkViaDir2);
				NUnit.Framework.Assert.Fail("The target should not exist");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkUsingFullyQualPaths()
		{
			org.apache.hadoop.fs.Path fileAbs = new org.apache.hadoop.fs.Path(testBaseDir1(), 
				"file");
			org.apache.hadoop.fs.Path linkAbs = new org.apache.hadoop.fs.Path(testBaseDir1(), 
				"linkToFile");
			org.apache.hadoop.fs.Path fileQual = new org.apache.hadoop.fs.Path(testURI().ToString
				(), fileAbs);
			org.apache.hadoop.fs.Path linkQual = new org.apache.hadoop.fs.Path(testURI().ToString
				(), linkAbs);
			createAndWriteFile(fileAbs);
			wrapper.createSymlink(fileQual, linkQual, false);
			checkLink(linkAbs, "file".Equals(getScheme()) ? fileAbs : fileQual, fileQual);
			// Now rename the link's parent. The target doesn't change and
			// now no longer exists so accessing the link should fail.
			org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path dir2 = new org.apache.hadoop.fs.Path(testBaseDir2());
			org.apache.hadoop.fs.Path linkViaDir2 = new org.apache.hadoop.fs.Path(testBaseDir2
				(), "linkToFile");
			wrapper.rename(dir1, dir2, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
			NUnit.Framework.Assert.AreEqual(fileQual, wrapper.getFileLinkStatus(linkViaDir2).
				getSymlink());
			try
			{
				readFile(linkViaDir2);
				NUnit.Framework.Assert.Fail("The target should not exist");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkUsingPartQualPath1()
		{
			// Partially qualified paths are covered for local file systems
			// in the previous test.
			NUnit.Framework.Assume.assumeTrue(!"file".Equals(getScheme()));
			org.apache.hadoop.fs.Path schemeAuth = new org.apache.hadoop.fs.Path(testURI().ToString
				());
			org.apache.hadoop.fs.Path fileWoHost = new org.apache.hadoop.fs.Path(getScheme() 
				+ "://" + testBaseDir1() + "/file");
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1() + "/linkToFile"
				);
			org.apache.hadoop.fs.Path linkQual = new org.apache.hadoop.fs.Path(schemeAuth, testBaseDir1
				() + "/linkToFile");
			org.apache.hadoop.fs.FSTestWrapper localWrapper = wrapper.getLocalFSWrapper();
			wrapper.createSymlink(fileWoHost, link, false);
			// Partially qualified path is stored
			NUnit.Framework.Assert.AreEqual(fileWoHost, wrapper.getLinkTarget(linkQual));
			// NB: We do not add an authority
			NUnit.Framework.Assert.AreEqual(fileWoHost.ToString(), wrapper.getFileLinkStatus(
				link).getSymlink().ToString());
			NUnit.Framework.Assert.AreEqual(fileWoHost.ToString(), wrapper.getFileLinkStatus(
				linkQual).getSymlink().ToString());
			// Ditto even from another file system
			if (wrapper is org.apache.hadoop.fs.FileContextTestWrapper)
			{
				NUnit.Framework.Assert.AreEqual(fileWoHost.ToString(), localWrapper.getFileLinkStatus
					(linkQual).getSymlink().ToString());
			}
			// Same as if we accessed a partially qualified path directly
			try
			{
				readFile(link);
				NUnit.Framework.Assert.Fail("DFS requires URIs with schemes have an authority");
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.IsTrue(wrapper is org.apache.hadoop.fs.FileContextTestWrapper
					);
			}
			catch (java.io.FileNotFoundException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(wrapper is org.apache.hadoop.fs.FileSystemTestWrapper
					);
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("File does not exist: /test1/file"
					, e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkUsingPartQualPath2()
		{
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile"
				);
			org.apache.hadoop.fs.Path fileWoScheme = new org.apache.hadoop.fs.Path("//" + testURI
				().getAuthority() + testBaseDir1() + "/file");
			if ("file".Equals(getScheme()))
			{
				return;
			}
			wrapper.createSymlink(fileWoScheme, link, false);
			NUnit.Framework.Assert.AreEqual(fileWoScheme, wrapper.getLinkTarget(link));
			NUnit.Framework.Assert.AreEqual(fileWoScheme.ToString(), wrapper.getFileLinkStatus
				(link).getSymlink().ToString());
			try
			{
				readFile(link);
				NUnit.Framework.Assert.Fail("Accessed a file with w/o scheme");
			}
			catch (System.IO.IOException e)
			{
				// Expected
				if (wrapper is org.apache.hadoop.fs.FileContextTestWrapper)
				{
					org.apache.hadoop.test.GenericTestUtils.assertExceptionContains(org.apache.hadoop.fs.AbstractFileSystem
						.NO_ABSTRACT_FS_ERROR, e);
				}
				else
				{
					if (wrapper is org.apache.hadoop.fs.FileSystemTestWrapper)
					{
						NUnit.Framework.Assert.AreEqual("No FileSystem for scheme: null", e.Message);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testLinkStatusAndTargetWithNonLink()
		{
			org.apache.hadoop.fs.Path schemeAuth = new org.apache.hadoop.fs.Path(testURI().ToString
				());
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path dirQual = new org.apache.hadoop.fs.Path(schemeAuth, dir
				.ToString());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path fileQual = new org.apache.hadoop.fs.Path(schemeAuth, file
				.ToString());
			createAndWriteFile(file);
			NUnit.Framework.Assert.AreEqual(wrapper.getFileStatus(file), wrapper.getFileLinkStatus
				(file));
			NUnit.Framework.Assert.AreEqual(wrapper.getFileStatus(dir), wrapper.getFileLinkStatus
				(dir));
			try
			{
				wrapper.getLinkTarget(file);
				NUnit.Framework.Assert.Fail("Get link target on non-link should throw an IOException"
					);
			}
			catch (System.IO.IOException x)
			{
				NUnit.Framework.Assert.AreEqual("Path " + fileQual + " is not a symbolic link", x
					.Message);
			}
			try
			{
				wrapper.getLinkTarget(dir);
				NUnit.Framework.Assert.Fail("Get link target on non-link should throw an IOException"
					);
			}
			catch (System.IO.IOException x)
			{
				NUnit.Framework.Assert.AreEqual("Path " + dirQual + " is not a symbolic link", x.
					Message);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkToDirectory()
		{
			org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			createAndWriteFile(file);
			wrapper.createSymlink(dir1, linkToDir, false);
			NUnit.Framework.Assert.IsFalse(wrapper.isFile(linkToDir));
			NUnit.Framework.Assert.IsTrue(wrapper.isDir(linkToDir));
			NUnit.Framework.Assert.IsTrue(wrapper.getFileStatus(linkToDir).isDirectory());
			NUnit.Framework.Assert.IsTrue(wrapper.getFileLinkStatus(linkToDir).isSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateFileViaSymlink()
		{
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path fileViaLink = new org.apache.hadoop.fs.Path(linkToDir, 
				"file");
			wrapper.createSymlink(dir, linkToDir, false);
			createAndWriteFile(fileViaLink);
			NUnit.Framework.Assert.IsTrue(wrapper.isFile(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.isDir(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.getFileLinkStatus(fileViaLink).isSymlink()
				);
			NUnit.Framework.Assert.IsFalse(wrapper.getFileStatus(fileViaLink).isDirectory());
			readFile(fileViaLink);
			wrapper.delete(fileViaLink, true);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(fileViaLink));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateDirViaSymlink()
		{
			org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path subDir = new org.apache.hadoop.fs.Path(testBaseDir1(), 
				"subDir");
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path subDirViaLink = new org.apache.hadoop.fs.Path(linkToDir
				, "subDir");
			wrapper.createSymlink(dir1, linkToDir, false);
			wrapper.mkdir(subDirViaLink, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			NUnit.Framework.Assert.IsTrue(wrapper.isDir(subDirViaLink));
			wrapper.delete(subDirViaLink, false);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(subDirViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.exists(subDir));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkViaLink()
		{
			NUnit.Framework.Assume.assumeTrue(!emulatingSymlinksOnWindows());
			org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path fileViaLink = new org.apache.hadoop.fs.Path(linkToDir, 
				"file");
			org.apache.hadoop.fs.Path linkToFile = new org.apache.hadoop.fs.Path(linkToDir, "linkToFile"
				);
			/*
			* /b2/linkToDir            -> /b1
			* /b2/linkToDir/linkToFile -> /b2/linkToDir/file
			*/
			createAndWriteFile(file);
			wrapper.createSymlink(dir1, linkToDir, false);
			wrapper.createSymlink(fileViaLink, linkToFile, false);
			NUnit.Framework.Assert.IsTrue(wrapper.isFile(linkToFile));
			NUnit.Framework.Assert.IsTrue(wrapper.getFileLinkStatus(linkToFile).isSymlink());
			readFile(linkToFile);
			NUnit.Framework.Assert.AreEqual(fileSize, wrapper.getFileStatus(linkToFile).getLen
				());
			NUnit.Framework.Assert.AreEqual(fileViaLink, wrapper.getLinkTarget(linkToFile));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testListStatusUsingLink()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "link"
				);
			createAndWriteFile(file);
			wrapper.createSymlink(new org.apache.hadoop.fs.Path(testBaseDir1()), link, false);
			// The size of the result is file system dependent, Hdfs is 2 (file
			// and link) and LocalFs is 3 (file, link, file crc).
			org.apache.hadoop.fs.FileStatus[] stats = wrapper.listStatus(link);
			NUnit.Framework.Assert.IsTrue(stats.Length == 2 || stats.Length == 3);
			org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus> statsItor = 
				wrapper.listStatusIterator(link);
			int dirLen = 0;
			while (statsItor.hasNext())
			{
				statsItor.next();
				dirLen++;
			}
			NUnit.Framework.Assert.IsTrue(dirLen == 2 || dirLen == 3);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkTwice()
		{
			NUnit.Framework.Assume.assumeTrue(!emulatingSymlinksOnWindows());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile"
				);
			createAndWriteFile(file);
			wrapper.createSymlink(file, link, false);
			try
			{
				wrapper.createSymlink(file, link, false);
				NUnit.Framework.Assert.Fail("link already exists");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkToLink()
		{
			org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path linkToLink = new org.apache.hadoop.fs.Path(testBaseDir2
				(), "linkToLink");
			org.apache.hadoop.fs.Path fileViaLink = new org.apache.hadoop.fs.Path(testBaseDir2
				(), "linkToLink/file");
			createAndWriteFile(file);
			wrapper.createSymlink(dir1, linkToDir, false);
			wrapper.createSymlink(linkToDir, linkToLink, false);
			NUnit.Framework.Assert.IsTrue(wrapper.isFile(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.isDir(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.getFileLinkStatus(fileViaLink).isSymlink()
				);
			NUnit.Framework.Assert.IsFalse(wrapper.getFileStatus(fileViaLink).isDirectory());
			readFile(fileViaLink);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateFileDirExistingLink()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile"
				);
			createAndWriteFile(file);
			wrapper.createSymlink(file, link, false);
			try
			{
				createAndWriteFile(link);
				NUnit.Framework.Assert.Fail("link already exists");
			}
			catch (System.IO.IOException)
			{
			}
			// Expected
			try
			{
				wrapper.mkdir(link, org.apache.hadoop.fs.permission.FsPermission.getDefault(), false
					);
				NUnit.Framework.Assert.Fail("link already exists");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void testUseLinkAferDeleteLink()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile"
				);
			createAndWriteFile(file);
			wrapper.createSymlink(file, link, false);
			wrapper.delete(link, false);
			try
			{
				readFile(link);
				NUnit.Framework.Assert.Fail("link was deleted");
			}
			catch (System.IO.IOException)
			{
			}
			// Expected
			readFile(file);
			wrapper.createSymlink(file, link, false);
			readFile(link);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkToDot()
		{
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToDot"
				);
			createAndWriteFile(file);
			wrapper.setWorkingDirectory(dir);
			try
			{
				wrapper.createSymlink(new org.apache.hadoop.fs.Path("."), link, false);
				NUnit.Framework.Assert.Fail("Created symlink to dot");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// Expected. Path(".") resolves to "" because URI normalizes
		// the dot away and AbstractFileSystem considers "" invalid.
		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkToDotDot()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "test/file"
				);
			org.apache.hadoop.fs.Path dotDot = new org.apache.hadoop.fs.Path(testBaseDir1(), 
				"test/..");
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path fileViaLink = new org.apache.hadoop.fs.Path(linkToDir, 
				"test/file");
			// Symlink to .. is not a problem since the .. is squashed early
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(testBaseDir1()), dotDot
				);
			createAndWriteFile(file);
			wrapper.createSymlink(dotDot, linkToDir, false);
			readFile(fileViaLink);
			NUnit.Framework.Assert.AreEqual(fileSize, wrapper.getFileStatus(fileViaLink).getLen
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateLinkToDotDotPrefix()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1(), "test"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "test/link"
				);
			createAndWriteFile(file);
			wrapper.mkdir(dir, org.apache.hadoop.fs.permission.FsPermission.getDefault(), false
				);
			wrapper.setWorkingDirectory(dir);
			wrapper.createSymlink(new org.apache.hadoop.fs.Path("../file"), link, false);
			readFile(link);
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("../file"), wrapper
				.getLinkTarget(link));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameFileViaSymlink()
		{
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path fileViaLink = new org.apache.hadoop.fs.Path(linkToDir, 
				"file");
			org.apache.hadoop.fs.Path fileNewViaLink = new org.apache.hadoop.fs.Path(linkToDir
				, "fileNew");
			createAndWriteFile(file);
			wrapper.createSymlink(dir, linkToDir, false);
			wrapper.rename(fileViaLink, fileNewViaLink);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.exists(file));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(fileNewViaLink));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameFileToDestViaSymlink()
		{
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path subDir = new org.apache.hadoop.fs.Path(linkToDir, "subDir"
				);
			createAndWriteFile(file);
			wrapper.createSymlink(dir, linkToDir, false);
			wrapper.mkdir(subDir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, false);
			try
			{
				wrapper.rename(file, subDir);
				NUnit.Framework.Assert.Fail("Renamed file to a directory");
			}
			catch (System.IO.IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is System.IO.IOException);
			}
			NUnit.Framework.Assert.IsTrue(wrapper.exists(file));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameDirViaSymlink()
		{
			org.apache.hadoop.fs.Path baseDir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(baseDir, "dir");
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path dirViaLink = new org.apache.hadoop.fs.Path(linkToDir, "dir"
				);
			org.apache.hadoop.fs.Path dirNewViaLink = new org.apache.hadoop.fs.Path(linkToDir
				, "dirNew");
			wrapper.mkdir(dir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, false);
			wrapper.createSymlink(baseDir, linkToDir, false);
			NUnit.Framework.Assert.IsTrue(wrapper.exists(dirViaLink));
			wrapper.rename(dirViaLink, dirNewViaLink);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(dirViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.exists(dir));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(dirNewViaLink));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameSymlinkViaSymlink()
		{
			org.apache.hadoop.fs.Path baseDir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "link"
				);
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path linkViaLink = new org.apache.hadoop.fs.Path(linkToDir, 
				"link");
			org.apache.hadoop.fs.Path linkNewViaLink = new org.apache.hadoop.fs.Path(linkToDir
				, "linkNew");
			createAndWriteFile(file);
			wrapper.createSymlink(file, link, false);
			wrapper.createSymlink(baseDir, linkToDir, false);
			wrapper.rename(linkViaLink, linkNewViaLink);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(linkViaLink));
			// Check that we didn't rename the link target
			NUnit.Framework.Assert.IsTrue(wrapper.exists(file));
			NUnit.Framework.Assert.IsTrue(wrapper.getFileLinkStatus(linkNewViaLink).isSymlink
				() || emulatingSymlinksOnWindows());
			readFile(linkNewViaLink);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameDirToSymlinkToDir()
		{
			org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path subDir = new org.apache.hadoop.fs.Path(testBaseDir2(), 
				"subDir");
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			wrapper.mkdir(subDir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, false);
			wrapper.createSymlink(subDir, linkToDir, false);
			try
			{
				wrapper.rename(dir1, linkToDir, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
				NUnit.Framework.Assert.Fail("Renamed directory to a symlink");
			}
			catch (System.IO.IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is System.IO.IOException);
			}
			NUnit.Framework.Assert.IsTrue(wrapper.exists(dir1));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(linkToDir));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameDirToSymlinkToFile()
		{
			org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir2(), "file"
				);
			org.apache.hadoop.fs.Path linkToFile = new org.apache.hadoop.fs.Path(testBaseDir2
				(), "linkToFile");
			createAndWriteFile(file);
			wrapper.createSymlink(file, linkToFile, false);
			try
			{
				wrapper.rename(dir1, linkToFile, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
				NUnit.Framework.Assert.Fail("Renamed directory to a symlink");
			}
			catch (System.IO.IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is System.IO.IOException);
			}
			NUnit.Framework.Assert.IsTrue(wrapper.exists(dir1));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(linkToFile));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameDirToDanglingSymlink()
		{
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir2(), "linkToFile"
				);
			wrapper.createSymlink(new org.apache.hadoop.fs.Path("/doesNotExist"), link, false
				);
			try
			{
				wrapper.rename(dir, link, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
				NUnit.Framework.Assert.Fail("Renamed directory to a symlink");
			}
			catch (System.IO.IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is System.IO.IOException);
			}
			NUnit.Framework.Assert.IsTrue(wrapper.exists(dir));
			NUnit.Framework.Assert.IsTrue(wrapper.getFileLinkStatus(link) != null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameFileToSymlinkToDir()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path subDir = new org.apache.hadoop.fs.Path(testBaseDir1(), 
				"subDir");
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "link"
				);
			wrapper.mkdir(subDir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, false);
			wrapper.createSymlink(subDir, link, false);
			createAndWriteFile(file);
			try
			{
				wrapper.rename(file, link);
				NUnit.Framework.Assert.Fail("Renamed file to symlink w/o overwrite");
			}
			catch (System.IO.IOException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			wrapper.rename(file, link, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(file));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.isFile(link));
			NUnit.Framework.Assert.IsFalse(wrapper.getFileLinkStatus(link).isSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameFileToSymlinkToFile()
		{
			org.apache.hadoop.fs.Path file1 = new org.apache.hadoop.fs.Path(testBaseDir1(), "file1"
				);
			org.apache.hadoop.fs.Path file2 = new org.apache.hadoop.fs.Path(testBaseDir1(), "file2"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile"
				);
			createAndWriteFile(file1);
			createAndWriteFile(file2);
			wrapper.createSymlink(file2, link, false);
			try
			{
				wrapper.rename(file1, link);
				NUnit.Framework.Assert.Fail("Renamed file to symlink w/o overwrite");
			}
			catch (System.IO.IOException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			wrapper.rename(file1, link, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(file1));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.isFile(link));
			NUnit.Framework.Assert.IsFalse(wrapper.getFileLinkStatus(link).isSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameFileToDanglingSymlink()
		{
			/* NB: Local file system doesn't handle dangling links correctly
			* since File.exists(danglinLink) returns false. */
			if ("file".Equals(getScheme()))
			{
				return;
			}
			org.apache.hadoop.fs.Path file1 = new org.apache.hadoop.fs.Path(testBaseDir1(), "file1"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile"
				);
			createAndWriteFile(file1);
			wrapper.createSymlink(new org.apache.hadoop.fs.Path("/doesNotExist"), link, false
				);
			try
			{
				wrapper.rename(file1, link);
			}
			catch (System.IO.IOException)
			{
			}
			// Expected
			wrapper.rename(file1, link, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(file1));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.isFile(link));
			NUnit.Framework.Assert.IsFalse(wrapper.getFileLinkStatus(link).isSymlink());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameSymlinkNonExistantDest()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path link1 = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile1"
				);
			org.apache.hadoop.fs.Path link2 = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile2"
				);
			createAndWriteFile(file);
			wrapper.createSymlink(file, link1, false);
			wrapper.rename(link1, link2);
			NUnit.Framework.Assert.IsTrue(wrapper.getFileLinkStatus(link2).isSymlink() || emulatingSymlinksOnWindows
				());
			readFile(link2);
			readFile(file);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(link1));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameSymlinkToExistingFile()
		{
			org.apache.hadoop.fs.Path file1 = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path file2 = new org.apache.hadoop.fs.Path(testBaseDir1(), "someFile"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile"
				);
			createAndWriteFile(file1);
			createAndWriteFile(file2);
			wrapper.createSymlink(file2, link, false);
			try
			{
				wrapper.rename(link, file1);
				NUnit.Framework.Assert.Fail("Renamed w/o passing overwrite");
			}
			catch (System.IO.IOException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			wrapper.rename(link, file1, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(link));
			if (!emulatingSymlinksOnWindows())
			{
				NUnit.Framework.Assert.IsTrue(wrapper.getFileLinkStatus(file1).isSymlink());
				NUnit.Framework.Assert.AreEqual(file2, wrapper.getLinkTarget(file1));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameSymlinkToExistingDir()
		{
			org.apache.hadoop.fs.Path dir1 = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path dir2 = new org.apache.hadoop.fs.Path(testBaseDir2());
			org.apache.hadoop.fs.Path subDir = new org.apache.hadoop.fs.Path(testBaseDir2(), 
				"subDir");
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToDir"
				);
			wrapper.createSymlink(dir1, link, false);
			try
			{
				wrapper.rename(link, dir2);
				NUnit.Framework.Assert.Fail("Renamed link to a directory");
			}
			catch (System.IO.IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is System.IO.IOException);
			}
			try
			{
				wrapper.rename(link, dir2, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
				NUnit.Framework.Assert.Fail("Renamed link to a directory");
			}
			catch (System.IO.IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is System.IO.IOException);
			}
			// Also fails when dir2 has a sub-directory
			wrapper.mkdir(subDir, org.apache.hadoop.fs.permission.FsPermission.getDefault(), 
				false);
			try
			{
				wrapper.rename(link, dir2, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
				NUnit.Framework.Assert.Fail("Renamed link to a directory");
			}
			catch (System.IO.IOException e)
			{
				// Expected. Both must be directories.
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is System.IO.IOException);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameSymlinkToItself()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			createAndWriteFile(file);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile1"
				);
			wrapper.createSymlink(file, link, false);
			try
			{
				wrapper.rename(link, link);
				NUnit.Framework.Assert.Fail("Failed to get expected IOException");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			// Fails with overwrite as well
			try
			{
				wrapper.rename(link, link, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
				NUnit.Framework.Assert.Fail("Failed to get expected IOException");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameSymlink()
		{
			NUnit.Framework.Assume.assumeTrue(!emulatingSymlinksOnWindows());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path link1 = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile1"
				);
			org.apache.hadoop.fs.Path link2 = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile2"
				);
			createAndWriteFile(file);
			wrapper.createSymlink(file, link1, false);
			wrapper.rename(link1, link2);
			NUnit.Framework.Assert.IsTrue(wrapper.getFileLinkStatus(link2).isSymlink());
			NUnit.Framework.Assert.IsFalse(wrapper.getFileStatus(link2).isDirectory());
			readFile(link2);
			readFile(file);
			try
			{
				createAndWriteFile(link2);
				NUnit.Framework.Assert.Fail("link was not renamed");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameSymlinkToFileItLinksTo()
		{
			/* NB: The rename is not atomic, so file is deleted before renaming
			* linkToFile. In this interval linkToFile is dangling and local file
			* system does not handle dangling links because File.exists returns
			* false for dangling links. */
			if ("file".Equals(getScheme()))
			{
				return;
			}
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile"
				);
			createAndWriteFile(file);
			wrapper.createSymlink(file, link, false);
			try
			{
				wrapper.rename(link, file);
				NUnit.Framework.Assert.Fail("Renamed symlink to its target");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			// Check the rename didn't happen
			NUnit.Framework.Assert.IsTrue(wrapper.isFile(file));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.isSymlink(link));
			NUnit.Framework.Assert.AreEqual(file, wrapper.getLinkTarget(link));
			try
			{
				wrapper.rename(link, file, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
				NUnit.Framework.Assert.Fail("Renamed symlink to its target");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			// Check the rename didn't happen
			NUnit.Framework.Assert.IsTrue(wrapper.isFile(file));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.isSymlink(link));
			NUnit.Framework.Assert.AreEqual(file, wrapper.getLinkTarget(link));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameSymlinkToDirItLinksTo()
		{
			/* NB: The rename is not atomic, so dir is deleted before renaming
			* linkToFile. In this interval linkToFile is dangling and local file
			* system does not handle dangling links because File.exists returns
			* false for dangling links. */
			if ("file".Equals(getScheme()))
			{
				return;
			}
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1(), "dir"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToDir"
				);
			wrapper.mkdir(dir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, false);
			wrapper.createSymlink(dir, link, false);
			try
			{
				wrapper.rename(link, dir);
				NUnit.Framework.Assert.Fail("Renamed symlink to its target");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			// Check the rename didn't happen
			NUnit.Framework.Assert.IsTrue(wrapper.isDir(dir));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.isSymlink(link));
			NUnit.Framework.Assert.AreEqual(dir, wrapper.getLinkTarget(link));
			try
			{
				wrapper.rename(link, dir, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
				NUnit.Framework.Assert.Fail("Renamed symlink to its target");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					);
			}
			// Check the rename didn't happen
			NUnit.Framework.Assert.IsTrue(wrapper.isDir(dir));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(link));
			NUnit.Framework.Assert.IsTrue(wrapper.isSymlink(link));
			NUnit.Framework.Assert.AreEqual(dir, wrapper.getLinkTarget(link));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameLinkTarget()
		{
			NUnit.Framework.Assume.assumeTrue(!emulatingSymlinksOnWindows());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path fileNew = new org.apache.hadoop.fs.Path(testBaseDir1(), 
				"fileNew");
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile"
				);
			createAndWriteFile(file);
			wrapper.createSymlink(file, link, false);
			wrapper.rename(file, fileNew, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
			try
			{
				readFile(link);
				NUnit.Framework.Assert.Fail("Link should be dangling");
			}
			catch (System.IO.IOException)
			{
			}
			// Expected
			wrapper.rename(fileNew, file, org.apache.hadoop.fs.Options.Rename.OVERWRITE);
			readFile(link);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRenameFileWithDestParentSymlink()
		{
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "link"
				);
			org.apache.hadoop.fs.Path file1 = new org.apache.hadoop.fs.Path(testBaseDir1(), "file1"
				);
			org.apache.hadoop.fs.Path file2 = new org.apache.hadoop.fs.Path(testBaseDir1(), "file2"
				);
			org.apache.hadoop.fs.Path file3 = new org.apache.hadoop.fs.Path(link, "file3");
			org.apache.hadoop.fs.Path dir2 = new org.apache.hadoop.fs.Path(testBaseDir2());
			// Renaming /dir1/file1 to non-existant file /dir1/link/file3 is OK
			// if link points to a directory...
			wrapper.createSymlink(dir2, link, false);
			createAndWriteFile(file1);
			wrapper.rename(file1, file3);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(file1));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(file3));
			wrapper.rename(file3, file1);
			// But fails if link is dangling...
			wrapper.delete(link, false);
			wrapper.createSymlink(file2, link, false);
			try
			{
				wrapper.rename(file1, file3);
			}
			catch (System.IO.IOException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is java.io.FileNotFoundException
					);
			}
			// And if link points to a file...
			createAndWriteFile(file2);
			try
			{
				wrapper.rename(file1, file3);
			}
			catch (System.IO.IOException e)
			{
				// Expected
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.ParentNotDirectoryException
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testAccessFileViaInterSymlinkAbsTarget()
		{
			org.apache.hadoop.fs.Path baseDir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path fileNew = new org.apache.hadoop.fs.Path(baseDir, "fileNew"
				);
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path fileViaLink = new org.apache.hadoop.fs.Path(linkToDir, 
				"file");
			org.apache.hadoop.fs.Path fileNewViaLink = new org.apache.hadoop.fs.Path(linkToDir
				, "fileNew");
			wrapper.createSymlink(baseDir, linkToDir, false);
			createAndWriteFile(fileViaLink);
			NUnit.Framework.Assert.IsTrue(wrapper.exists(fileViaLink));
			NUnit.Framework.Assert.IsTrue(wrapper.isFile(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.isDir(fileViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.getFileLinkStatus(fileViaLink).isSymlink()
				);
			NUnit.Framework.Assert.IsFalse(wrapper.isDir(fileViaLink));
			NUnit.Framework.Assert.AreEqual(wrapper.getFileStatus(file), wrapper.getFileLinkStatus
				(file));
			NUnit.Framework.Assert.AreEqual(wrapper.getFileStatus(fileViaLink), wrapper.getFileLinkStatus
				(fileViaLink));
			readFile(fileViaLink);
			appendToFile(fileViaLink);
			wrapper.rename(fileViaLink, fileNewViaLink);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(fileViaLink));
			NUnit.Framework.Assert.IsTrue(wrapper.exists(fileNewViaLink));
			readFile(fileNewViaLink);
			NUnit.Framework.Assert.AreEqual(wrapper.getFileBlockLocations(fileNew, 0, 1).Length
				, wrapper.getFileBlockLocations(fileNewViaLink, 0, 1).Length);
			NUnit.Framework.Assert.AreEqual(wrapper.getFileChecksum(fileNew), wrapper.getFileChecksum
				(fileNewViaLink));
			wrapper.delete(fileNewViaLink, true);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(fileNewViaLink));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testAccessFileViaInterSymlinkQualTarget()
		{
			org.apache.hadoop.fs.Path baseDir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path fileViaLink = new org.apache.hadoop.fs.Path(linkToDir, 
				"file");
			wrapper.createSymlink(wrapper.makeQualified(baseDir), linkToDir, false);
			createAndWriteFile(fileViaLink);
			NUnit.Framework.Assert.AreEqual(wrapper.getFileStatus(file), wrapper.getFileLinkStatus
				(file));
			NUnit.Framework.Assert.AreEqual(wrapper.getFileStatus(fileViaLink), wrapper.getFileLinkStatus
				(fileViaLink));
			readFile(fileViaLink);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testAccessFileViaInterSymlinkRelTarget()
		{
			NUnit.Framework.Assume.assumeTrue(!"file".Equals(getScheme()));
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1(), "dir"
				);
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(dir, "file");
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir1(
				), "linkToDir");
			org.apache.hadoop.fs.Path fileViaLink = new org.apache.hadoop.fs.Path(linkToDir, 
				"file");
			wrapper.mkdir(dir, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, false);
			wrapper.createSymlink(new org.apache.hadoop.fs.Path("dir"), linkToDir, false);
			createAndWriteFile(fileViaLink);
			// Note that getFileStatus returns fully qualified paths even
			// when called on an absolute path.
			NUnit.Framework.Assert.AreEqual(wrapper.makeQualified(file), wrapper.getFileStatus
				(file).getPath());
			// In each case getFileLinkStatus returns the same FileStatus
			// as getFileStatus since we're not calling it on a link and
			// FileStatus objects are compared by Path.
			NUnit.Framework.Assert.AreEqual(wrapper.getFileStatus(file), wrapper.getFileLinkStatus
				(file));
			NUnit.Framework.Assert.AreEqual(wrapper.getFileStatus(fileViaLink), wrapper.getFileLinkStatus
				(fileViaLink));
			NUnit.Framework.Assert.AreEqual(wrapper.getFileStatus(fileViaLink), wrapper.getFileLinkStatus
				(file));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testAccessDirViaSymlink()
		{
			org.apache.hadoop.fs.Path baseDir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1(), "dir"
				);
			org.apache.hadoop.fs.Path linkToDir = new org.apache.hadoop.fs.Path(testBaseDir2(
				), "linkToDir");
			org.apache.hadoop.fs.Path dirViaLink = new org.apache.hadoop.fs.Path(linkToDir, "dir"
				);
			wrapper.createSymlink(baseDir, linkToDir, false);
			wrapper.mkdir(dirViaLink, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			NUnit.Framework.Assert.IsTrue(wrapper.getFileStatus(dirViaLink).isDirectory());
			org.apache.hadoop.fs.FileStatus[] stats = wrapper.listStatus(dirViaLink);
			NUnit.Framework.Assert.AreEqual(0, stats.Length);
			org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus> statsItor = 
				wrapper.listStatusIterator(dirViaLink);
			NUnit.Framework.Assert.IsFalse(statsItor.hasNext());
			wrapper.delete(dirViaLink, false);
			NUnit.Framework.Assert.IsFalse(wrapper.exists(dirViaLink));
			NUnit.Framework.Assert.IsFalse(wrapper.exists(dir));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testSetTimes()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(testBaseDir1(), "file"
				);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1(), "linkToFile"
				);
			createAndWriteFile(file);
			wrapper.createSymlink(file, link, false);
			long at = wrapper.getFileLinkStatus(link).getAccessTime();
			wrapper.setTimes(link, 2L, 3L);
			// NB: local file systems don't implement setTimes
			if (!"file".Equals(getScheme()))
			{
				NUnit.Framework.Assert.AreEqual(at, wrapper.getFileLinkStatus(link).getAccessTime
					());
				NUnit.Framework.Assert.AreEqual(3, wrapper.getFileStatus(file).getAccessTime());
				NUnit.Framework.Assert.AreEqual(2, wrapper.getFileStatus(file).getModificationTime
					());
			}
		}
	}
}
