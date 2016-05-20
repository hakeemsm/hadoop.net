using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Test symbolic links using LocalFs.</summary>
	public abstract class TestSymlinkLocalFS : org.apache.hadoop.fs.SymlinkBaseTest
	{
		static TestSymlinkLocalFS()
		{
			// Workaround for HADOOP-9652
			org.apache.hadoop.fs.RawLocalFileSystem.useStatIfAvailable();
		}

		protected internal override string getScheme()
		{
			return "file";
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override string testBaseDir1()
		{
			return wrapper.getAbsoluteTestRootDir() + "/test1";
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override string testBaseDir2()
		{
			return wrapper.getAbsoluteTestRootDir() + "/test2";
		}

		protected internal override java.net.URI testURI()
		{
			try
			{
				return new java.net.URI("file:///");
			}
			catch (java.net.URISyntaxException)
			{
				return null;
			}
		}

		protected internal override bool emulatingSymlinksOnWindows()
		{
			// Java 6 on Windows has very poor symlink support. Specifically
			// Specifically File#length and File#renameTo do not work as expected.
			// (see HADOOP-9061 for additional details)
			// Hence some symlink tests will be skipped.
			//
			return (org.apache.hadoop.util.Shell.WINDOWS && !org.apache.hadoop.util.Shell.isJava7OrAbove
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testCreateDanglingLink()
		{
			// Dangling symlinks are not supported on Windows local file system.
			NUnit.Framework.Assume.assumeTrue(!org.apache.hadoop.fs.Path.WINDOWS);
			base.testCreateDanglingLink();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testCreateFileViaDanglingLinkParent()
		{
			NUnit.Framework.Assume.assumeTrue(!org.apache.hadoop.fs.Path.WINDOWS);
			base.testCreateFileViaDanglingLinkParent();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testOpenResolvesLinks()
		{
			NUnit.Framework.Assume.assumeTrue(!org.apache.hadoop.fs.Path.WINDOWS);
			base.testOpenResolvesLinks();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testRecursiveLinks()
		{
			NUnit.Framework.Assume.assumeTrue(!org.apache.hadoop.fs.Path.WINDOWS);
			base.testRecursiveLinks();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testRenameDirToDanglingSymlink()
		{
			NUnit.Framework.Assume.assumeTrue(!org.apache.hadoop.fs.Path.WINDOWS);
			base.testRenameDirToDanglingSymlink();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testStatDanglingLink()
		{
			NUnit.Framework.Assume.assumeTrue(!org.apache.hadoop.fs.Path.WINDOWS);
			base.testStatDanglingLink();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testDanglingLinkFilePartQual()
		{
			org.apache.hadoop.fs.Path filePartQual = new org.apache.hadoop.fs.Path(getScheme(
				) + ":///doesNotExist");
			try
			{
				wrapper.getFileLinkStatus(filePartQual);
				NUnit.Framework.Assert.Fail("Got FileStatus for non-existant file");
			}
			catch (java.io.FileNotFoundException)
			{
			}
			// Expected
			try
			{
				wrapper.getLinkTarget(filePartQual);
				NUnit.Framework.Assert.Fail("Got link target for non-existant file");
			}
			catch (java.io.FileNotFoundException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void testDanglingLink()
		{
			NUnit.Framework.Assume.assumeTrue(!org.apache.hadoop.fs.Path.WINDOWS);
			org.apache.hadoop.fs.Path fileAbs = new org.apache.hadoop.fs.Path(testBaseDir1() 
				+ "/file");
			org.apache.hadoop.fs.Path fileQual = new org.apache.hadoop.fs.Path(testURI().ToString
				(), fileAbs);
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1() + "/linkToFile"
				);
			org.apache.hadoop.fs.Path linkQual = new org.apache.hadoop.fs.Path(testURI().ToString
				(), link.ToString());
			wrapper.createSymlink(fileAbs, link, false);
			// Deleting the link using FileContext currently fails because
			// resolve looks up LocalFs rather than RawLocalFs for the path 
			// so we call ChecksumFs delete (which doesn't delete dangling 
			// links) instead of delegating to delete in RawLocalFileSystem 
			// which deletes via fullyDelete. testDeleteLink above works 
			// because the link is not dangling.
			//assertTrue(fc.delete(link, false));
			org.apache.hadoop.fs.FileUtil.fullyDelete(new java.io.File(link.toUri().getPath()
				));
			wrapper.createSymlink(fileAbs, link, false);
			try
			{
				wrapper.getFileStatus(link);
				NUnit.Framework.Assert.Fail("Got FileStatus for dangling link");
			}
			catch (java.io.FileNotFoundException)
			{
			}
			// Expected. File's exists method returns false for dangling links
			// We can stat a dangling link
			org.apache.hadoop.security.UserGroupInformation user = org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser();
			org.apache.hadoop.fs.FileStatus fsd = wrapper.getFileLinkStatus(link);
			NUnit.Framework.Assert.AreEqual(fileQual, fsd.getSymlink());
			NUnit.Framework.Assert.IsTrue(fsd.isSymlink());
			NUnit.Framework.Assert.IsFalse(fsd.isDirectory());
			NUnit.Framework.Assert.AreEqual(user.getUserName(), fsd.getOwner());
			// Compare against user's primary group
			NUnit.Framework.Assert.AreEqual(user.getGroupNames()[0], fsd.getGroup());
			NUnit.Framework.Assert.AreEqual(linkQual, fsd.getPath());
			// Accessing the link 
			try
			{
				readFile(link);
				NUnit.Framework.Assert.Fail("Got FileStatus for dangling link");
			}
			catch (java.io.FileNotFoundException)
			{
			}
			// Ditto.
			// Creating the file makes the link work
			createAndWriteFile(fileAbs);
			wrapper.getFileStatus(link);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetLinkStatusPartQualTarget()
		{
			NUnit.Framework.Assume.assumeTrue(!emulatingSymlinksOnWindows());
			org.apache.hadoop.fs.Path fileAbs = new org.apache.hadoop.fs.Path(testBaseDir1() 
				+ "/file");
			org.apache.hadoop.fs.Path fileQual = new org.apache.hadoop.fs.Path(testURI().ToString
				(), fileAbs);
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(testBaseDir1());
			org.apache.hadoop.fs.Path link = new org.apache.hadoop.fs.Path(testBaseDir1() + "/linkToFile"
				);
			org.apache.hadoop.fs.Path dirNew = new org.apache.hadoop.fs.Path(testBaseDir2());
			org.apache.hadoop.fs.Path linkNew = new org.apache.hadoop.fs.Path(testBaseDir2() 
				+ "/linkToFile");
			wrapper.delete(dirNew, true);
			createAndWriteFile(fileQual);
			wrapper.setWorkingDirectory(dir);
			// Link target is partially qualified, we get the same back.
			wrapper.createSymlink(fileQual, link, false);
			NUnit.Framework.Assert.AreEqual(fileQual, wrapper.getFileLinkStatus(link).getSymlink
				());
			// Because the target was specified with an absolute path the
			// link fails to resolve after moving the parent directory. 
			wrapper.rename(dir, dirNew);
			// The target is still the old path
			NUnit.Framework.Assert.AreEqual(fileQual, wrapper.getFileLinkStatus(linkNew).getSymlink
				());
			try
			{
				readFile(linkNew);
				NUnit.Framework.Assert.Fail("The link should be dangling now.");
			}
			catch (java.io.FileNotFoundException)
			{
			}
			// Expected.
			// RawLocalFs only maintains the path part, not the URI, and
			// therefore does not support links to other file systems.
			org.apache.hadoop.fs.Path anotherFs = new org.apache.hadoop.fs.Path("hdfs://host:1000/dir/file"
				);
			org.apache.hadoop.fs.FileUtil.fullyDelete(new java.io.File(linkNew.ToString()));
			try
			{
				wrapper.createSymlink(anotherFs, linkNew, false);
				NUnit.Framework.Assert.Fail("Created a local fs link to a non-local fs");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// Excpected.
		/// <summary>Test create symlink to .</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void testCreateLinkToDot()
		{
			try
			{
				base.testCreateLinkToDot();
			}
			catch (System.ArgumentException)
			{
			}
		}
		// Expected.
	}
}
