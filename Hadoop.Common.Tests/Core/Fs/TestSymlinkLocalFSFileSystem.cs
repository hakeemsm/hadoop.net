using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestSymlinkLocalFSFileSystem : TestSymlinkLocalFS
	{
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void TestSetup()
		{
			FileSystem filesystem = FileSystem.GetLocal(new Configuration());
			wrapper = new FileSystemTestWrapper(filesystem);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestMkdirExistingLink()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestCreateFileViaDanglingLinkParent()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestCreateFileDirExistingLink()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestAccessFileViaInterSymlinkAbsTarget()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestRenameFileWithDestParentSymlink()
		{
			Assume.AssumeTrue(!Shell.Windows);
			base.TestRenameFileWithDestParentSymlink();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestRenameSymlinkToItself()
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
				Assert.True(UnwrapException(e) is FileAlreadyExistsException);
			}
			// Fails with overwrite as well
			try
			{
				wrapper.Rename(link, link, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Failed to get expected IOException");
			}
			catch (IOException e)
			{
				// Todo: Fix this test when HADOOP-9819 is fixed.
				Assert.True(UnwrapException(e) is FileAlreadyExistsException ||
					 UnwrapException(e) is FileNotFoundException);
			}
		}
	}
}
