using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestLocalFSFileContextMainOperations : org.apache.hadoop.fs.FileContextMainOperationsBaseTest
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			fc = org.apache.hadoop.fs.FileContext.getLocalFSFileContext();
			base.setUp();
		}

		internal static org.apache.hadoop.fs.Path wd = null;

		/// <exception cref="System.IO.IOException"/>
		protected internal override org.apache.hadoop.fs.Path getDefaultWorkingDirectory(
			)
		{
			if (wd == null)
			{
				wd = org.apache.hadoop.fs.FileSystem.getLocal(new org.apache.hadoop.conf.Configuration
					()).getWorkingDirectory();
			}
			return wd;
		}

		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		[NUnit.Framework.Test]
		public virtual void testFileContextNoCache()
		{
			org.apache.hadoop.fs.FileContext fc1 = org.apache.hadoop.fs.FileContext.getLocalFSFileContext
				();
			NUnit.Framework.Assert.IsTrue(fc1 != fc);
		}

		protected internal override bool listCorruptedBlocksSupported()
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDefaultFilePermission()
		{
			org.apache.hadoop.fs.Path file = fileContextTestHelper.getTestRootPath(fc, "testDefaultFilePermission"
				);
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc, file);
			org.apache.hadoop.fs.permission.FsPermission expect = org.apache.hadoop.fs.FileContext
				.FILE_DEFAULT_PERM.applyUMask(fc.getUMask());
			NUnit.Framework.Assert.AreEqual(expect, fc.getFileStatus(file).getPermission());
		}
	}
}
