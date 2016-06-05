using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestLocalFSFileContextMainOperations : FileContextMainOperationsBaseTest
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			fc = FileContext.GetLocalFSFileContext();
			base.SetUp();
		}

		internal static Path wd = null;

		/// <exception cref="System.IO.IOException"/>
		protected internal override Path GetDefaultWorkingDirectory()
		{
			if (wd == null)
			{
				wd = FileSystem.GetLocal(new Configuration()).GetWorkingDirectory();
			}
			return wd;
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		[Fact]
		public virtual void TestFileContextNoCache()
		{
			FileContext fc1 = FileContext.GetLocalFSFileContext();
			Assert.True(fc1 != fc);
		}

		protected internal override bool ListCorruptedBlocksSupported()
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDefaultFilePermission()
		{
			Path file = fileContextTestHelper.GetTestRootPath(fc, "testDefaultFilePermission"
				);
			FileContextTestHelper.CreateFile(fc, file);
			FsPermission expect = FileContext.FileDefaultPerm.ApplyUMask(fc.GetUMask());
			Assert.Equal(expect, fc.GetFileStatus(file).GetPermission());
		}
	}
}
