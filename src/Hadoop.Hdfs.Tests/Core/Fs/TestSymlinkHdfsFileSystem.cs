using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestSymlinkHdfsFileSystem : TestSymlinkHdfs
	{
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void TestSetup()
		{
			wrapper = new FileSystemTestWrapper(dfs, "/tmp/TestSymlinkHdfsFileSystem");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestCreateWithPartQualPathFails()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestCreateFileViaDanglingLinkParent()
		{
		}

		// Additional tests for DFS-only methods
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRecoverLease()
		{
			Path dir = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "link");
			wrapper.SetWorkingDirectory(dir);
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			// Attempt recoverLease through a symlink
			bool closed = dfs.RecoverLease(link);
			NUnit.Framework.Assert.IsTrue("Expected recoverLease to return true", closed);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIsFileClosed()
		{
			Path dir = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "link");
			wrapper.SetWorkingDirectory(dir);
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			// Attempt recoverLease through a symlink
			bool closed = dfs.IsFileClosed(link);
			NUnit.Framework.Assert.IsTrue("Expected isFileClosed to return true", closed);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestConcat()
		{
			Path dir = new Path(TestBaseDir1());
			Path link = new Path(TestBaseDir1(), "link");
			Path dir2 = new Path(TestBaseDir2());
			wrapper.CreateSymlink(dir2, link, false);
			wrapper.SetWorkingDirectory(dir);
			// Concat with a target and srcs through a link
			Path target = new Path(link, "target");
			CreateAndWriteFile(target);
			Path[] srcs = new Path[3];
			for (int i = 0; i < srcs.Length; i++)
			{
				srcs[i] = new Path(link, "src-" + i);
				CreateAndWriteFile(srcs[i]);
			}
			dfs.Concat(target, srcs);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshot()
		{
			Path dir = new Path(TestBaseDir1());
			Path link = new Path(TestBaseDir1(), "link");
			Path dir2 = new Path(TestBaseDir2());
			wrapper.CreateSymlink(dir2, link, false);
			wrapper.SetWorkingDirectory(dir);
			dfs.AllowSnapshot(link);
			dfs.DisallowSnapshot(link);
			dfs.AllowSnapshot(link);
			dfs.CreateSnapshot(link, "mcmillan");
			dfs.RenameSnapshot(link, "mcmillan", "seaborg");
			dfs.DeleteSnapshot(link, "seaborg");
		}
	}
}
