using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Tests
	/// <see>FileContext.#deleteOnExit(Path)</see>
	/// functionality.
	/// </summary>
	public class TestFileContextDeleteOnExit
	{
		private static int blockSize = 1024;

		private static int numBlocks = 2;

		private readonly FileContextTestHelper helper = new FileContextTestHelper();

		private FileContext fc;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			fc = FileContext.GetLocalFSFileContext();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fc.Delete(helper.GetTestRootPath(fc), true);
		}

		private void CheckDeleteOnExitData(int size, FileContext fc, params Path[] paths)
		{
			Assert.Equal(size, FileContext.DeleteOnExit.Count);
			ICollection<Path> set = FileContext.DeleteOnExit[fc];
			Assert.Equal(paths.Length, (set == null ? 0 : set.Count));
			foreach (Path path in paths)
			{
				Assert.True(set.Contains(path));
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDeleteOnExit()
		{
			// Create deleteOnExit entries
			Path file1 = helper.GetTestRootPath(fc, "file1");
			FileContextTestHelper.CreateFile(fc, file1, numBlocks, blockSize);
			fc.DeleteOnExit(file1);
			CheckDeleteOnExitData(1, fc, file1);
			// Ensure shutdown hook is added
			Assert.True(ShutdownHookManager.Get().HasShutdownHook(FileContext
				.Finalizer));
			Path file2 = helper.GetTestRootPath(fc, "dir1/file2");
			FileContextTestHelper.CreateFile(fc, file2, numBlocks, blockSize);
			fc.DeleteOnExit(file2);
			CheckDeleteOnExitData(1, fc, file1, file2);
			Path dir = helper.GetTestRootPath(fc, "dir3/dir4/dir5/dir6");
			FileContextTestHelper.CreateFile(fc, dir, numBlocks, blockSize);
			fc.DeleteOnExit(dir);
			CheckDeleteOnExitData(1, fc, file1, file2, dir);
			// trigger deleteOnExit and ensure the registered
			// paths are cleaned up
			FileContext.Finalizer.Run();
			CheckDeleteOnExitData(0, fc, new Path[0]);
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, file1));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, file2));
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, dir));
		}
	}
}
