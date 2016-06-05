using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestSymlinkHdfsFileContext : TestSymlinkHdfs
	{
		private static FileContext fc;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void TestSetup()
		{
			fc = FileContext.GetFileContext(cluster.GetURI(0));
			wrapper = new FileContextTestWrapper(fc, "/tmp/TestSymlinkHdfsFileContext");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAccessLinkFromAbstractFileSystem()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			try
			{
				AbstractFileSystem afs = fc.GetDefaultFileSystem();
				afs.Open(link);
				NUnit.Framework.Assert.Fail("Opened a link using AFS");
			}
			catch (UnresolvedLinkException)
			{
			}
		}
		// Expected
	}
}
