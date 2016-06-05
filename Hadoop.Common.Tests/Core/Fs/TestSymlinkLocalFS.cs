using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>Test symbolic links using LocalFs.</summary>
	public abstract class TestSymlinkLocalFS : SymlinkBaseTest
	{
		static TestSymlinkLocalFS()
		{
			// Workaround for HADOOP-9652
			RawLocalFileSystem.UseStatIfAvailable();
		}

		protected internal override string GetScheme()
		{
			return "file";
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override string TestBaseDir1()
		{
			return wrapper.GetAbsoluteTestRootDir() + "/test1";
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override string TestBaseDir2()
		{
			return wrapper.GetAbsoluteTestRootDir() + "/test2";
		}

		protected internal override URI TestURI()
		{
			try
			{
				return new URI("file:///");
			}
			catch (URISyntaxException)
			{
				return null;
			}
		}

		protected internal override bool EmulatingSymlinksOnWindows()
		{
			// Java 6 on Windows has very poor symlink support. Specifically
			// Specifically File#length and File#renameTo do not work as expected.
			// (see HADOOP-9061 for additional details)
			// Hence some symlink tests will be skipped.
			//
			return (Shell.Windows && !Shell.IsJava7OrAbove());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestCreateDanglingLink()
		{
			// Dangling symlinks are not supported on Windows local file system.
			Assume.AssumeTrue(!Path.Windows);
			base.TestCreateDanglingLink();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestCreateFileViaDanglingLinkParent()
		{
			Assume.AssumeTrue(!Path.Windows);
			base.TestCreateFileViaDanglingLinkParent();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestOpenResolvesLinks()
		{
			Assume.AssumeTrue(!Path.Windows);
			base.TestOpenResolvesLinks();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestRecursiveLinks()
		{
			Assume.AssumeTrue(!Path.Windows);
			base.TestRecursiveLinks();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestRenameDirToDanglingSymlink()
		{
			Assume.AssumeTrue(!Path.Windows);
			base.TestRenameDirToDanglingSymlink();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestStatDanglingLink()
		{
			Assume.AssumeTrue(!Path.Windows);
			base.TestStatDanglingLink();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDanglingLinkFilePartQual()
		{
			Path filePartQual = new Path(GetScheme() + ":///doesNotExist");
			try
			{
				wrapper.GetFileLinkStatus(filePartQual);
				NUnit.Framework.Assert.Fail("Got FileStatus for non-existant file");
			}
			catch (FileNotFoundException)
			{
			}
			// Expected
			try
			{
				wrapper.GetLinkTarget(filePartQual);
				NUnit.Framework.Assert.Fail("Got link target for non-existant file");
			}
			catch (FileNotFoundException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDanglingLink()
		{
			Assume.AssumeTrue(!Path.Windows);
			Path fileAbs = new Path(TestBaseDir1() + "/file");
			Path fileQual = new Path(TestURI().ToString(), fileAbs);
			Path link = new Path(TestBaseDir1() + "/linkToFile");
			Path linkQual = new Path(TestURI().ToString(), link.ToString());
			wrapper.CreateSymlink(fileAbs, link, false);
			// Deleting the link using FileContext currently fails because
			// resolve looks up LocalFs rather than RawLocalFs for the path 
			// so we call ChecksumFs delete (which doesn't delete dangling 
			// links) instead of delegating to delete in RawLocalFileSystem 
			// which deletes via fullyDelete. testDeleteLink above works 
			// because the link is not dangling.
			//assertTrue(fc.delete(link, false));
			FileUtil.FullyDelete(new FilePath(link.ToUri().GetPath()));
			wrapper.CreateSymlink(fileAbs, link, false);
			try
			{
				wrapper.GetFileStatus(link);
				NUnit.Framework.Assert.Fail("Got FileStatus for dangling link");
			}
			catch (FileNotFoundException)
			{
			}
			// Expected. File's exists method returns false for dangling links
			// We can stat a dangling link
			UserGroupInformation user = UserGroupInformation.GetCurrentUser();
			FileStatus fsd = wrapper.GetFileLinkStatus(link);
			Assert.Equal(fileQual, fsd.GetSymlink());
			Assert.True(fsd.IsSymlink());
			NUnit.Framework.Assert.IsFalse(fsd.IsDirectory());
			Assert.Equal(user.GetUserName(), fsd.GetOwner());
			// Compare against user's primary group
			Assert.Equal(user.GetGroupNames()[0], fsd.GetGroup());
			Assert.Equal(linkQual, fsd.GetPath());
			// Accessing the link 
			try
			{
				ReadFile(link);
				NUnit.Framework.Assert.Fail("Got FileStatus for dangling link");
			}
			catch (FileNotFoundException)
			{
			}
			// Ditto.
			// Creating the file makes the link work
			CreateAndWriteFile(fileAbs);
			wrapper.GetFileStatus(link);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetLinkStatusPartQualTarget()
		{
			Assume.AssumeTrue(!EmulatingSymlinksOnWindows());
			Path fileAbs = new Path(TestBaseDir1() + "/file");
			Path fileQual = new Path(TestURI().ToString(), fileAbs);
			Path dir = new Path(TestBaseDir1());
			Path link = new Path(TestBaseDir1() + "/linkToFile");
			Path dirNew = new Path(TestBaseDir2());
			Path linkNew = new Path(TestBaseDir2() + "/linkToFile");
			wrapper.Delete(dirNew, true);
			CreateAndWriteFile(fileQual);
			wrapper.SetWorkingDirectory(dir);
			// Link target is partially qualified, we get the same back.
			wrapper.CreateSymlink(fileQual, link, false);
			Assert.Equal(fileQual, wrapper.GetFileLinkStatus(link).GetSymlink
				());
			// Because the target was specified with an absolute path the
			// link fails to resolve after moving the parent directory. 
			wrapper.Rename(dir, dirNew);
			// The target is still the old path
			Assert.Equal(fileQual, wrapper.GetFileLinkStatus(linkNew).GetSymlink
				());
			try
			{
				ReadFile(linkNew);
				NUnit.Framework.Assert.Fail("The link should be dangling now.");
			}
			catch (FileNotFoundException)
			{
			}
			// Expected.
			// RawLocalFs only maintains the path part, not the URI, and
			// therefore does not support links to other file systems.
			Path anotherFs = new Path("hdfs://host:1000/dir/file");
			FileUtil.FullyDelete(new FilePath(linkNew.ToString()));
			try
			{
				wrapper.CreateSymlink(anotherFs, linkNew, false);
				NUnit.Framework.Assert.Fail("Created a local fs link to a non-local fs");
			}
			catch (IOException)
			{
			}
		}

		// Excpected.
		/// <summary>Test create symlink to .</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void TestCreateLinkToDot()
		{
			try
			{
				base.TestCreateLinkToDot();
			}
			catch (ArgumentException)
			{
			}
		}
		// Expected.
	}
}
