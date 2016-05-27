using NUnit.Framework;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestSymlinkLocalFSFileContext : TestSymlinkLocalFS
	{
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void TestSetup()
		{
			FileContext context = FileContext.GetLocalFSFileContext();
			wrapper = new FileContextTestWrapper(context);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestRenameFileWithDestParentSymlink()
		{
			Assume.AssumeTrue(!Shell.Windows);
			base.TestRenameFileWithDestParentSymlink();
		}
	}
}
