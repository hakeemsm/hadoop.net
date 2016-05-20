using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestSymlinkLocalFSFileContext : org.apache.hadoop.fs.TestSymlinkLocalFS
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void testSetup()
		{
			org.apache.hadoop.fs.FileContext context = org.apache.hadoop.fs.FileContext.getLocalFSFileContext
				();
			wrapper = new org.apache.hadoop.fs.FileContextTestWrapper(context);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testRenameFileWithDestParentSymlink()
		{
			NUnit.Framework.Assume.assumeTrue(!org.apache.hadoop.util.Shell.WINDOWS);
			base.testRenameFileWithDestParentSymlink();
		}
	}
}
