using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestSymlinkLocalFSFileSystem : org.apache.hadoop.fs.TestSymlinkLocalFS
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void testSetup()
		{
			org.apache.hadoop.fs.FileSystem filesystem = org.apache.hadoop.fs.FileSystem.getLocal
				(new org.apache.hadoop.conf.Configuration());
			wrapper = new org.apache.hadoop.fs.FileSystemTestWrapper(filesystem);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testMkdirExistingLink()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testCreateFileViaDanglingLinkParent()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testCreateFileDirExistingLink()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testAccessFileViaInterSymlinkAbsTarget()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testRenameFileWithDestParentSymlink()
		{
			NUnit.Framework.Assume.assumeTrue(!org.apache.hadoop.util.Shell.WINDOWS);
			base.testRenameFileWithDestParentSymlink();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testRenameSymlinkToItself()
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
				// Todo: Fix this test when HADOOP-9819 is fixed.
				NUnit.Framework.Assert.IsTrue(unwrapException(e) is org.apache.hadoop.fs.FileAlreadyExistsException
					 || unwrapException(e) is java.io.FileNotFoundException);
			}
		}
	}
}
