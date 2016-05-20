using Sharpen;

namespace org.apache.hadoop.fs
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

		private readonly org.apache.hadoop.fs.FileContextTestHelper helper = new org.apache.hadoop.fs.FileContextTestHelper
			();

		private org.apache.hadoop.fs.FileContext fc;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			fc = org.apache.hadoop.fs.FileContext.getLocalFSFileContext();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			fc.delete(helper.getTestRootPath(fc), true);
		}

		private void checkDeleteOnExitData(int size, org.apache.hadoop.fs.FileContext fc, 
			params org.apache.hadoop.fs.Path[] paths)
		{
			NUnit.Framework.Assert.AreEqual(size, org.apache.hadoop.fs.FileContext.DELETE_ON_EXIT
				.Count);
			System.Collections.Generic.ICollection<org.apache.hadoop.fs.Path> set = org.apache.hadoop.fs.FileContext
				.DELETE_ON_EXIT[fc];
			NUnit.Framework.Assert.AreEqual(paths.Length, (set == null ? 0 : set.Count));
			foreach (org.apache.hadoop.fs.Path path in paths)
			{
				NUnit.Framework.Assert.IsTrue(set.contains(path));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteOnExit()
		{
			// Create deleteOnExit entries
			org.apache.hadoop.fs.Path file1 = helper.getTestRootPath(fc, "file1");
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc, file1, numBlocks, blockSize
				);
			fc.deleteOnExit(file1);
			checkDeleteOnExitData(1, fc, file1);
			// Ensure shutdown hook is added
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.util.ShutdownHookManager.get().hasShutdownHook
				(org.apache.hadoop.fs.FileContext.FINALIZER));
			org.apache.hadoop.fs.Path file2 = helper.getTestRootPath(fc, "dir1/file2");
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc, file2, numBlocks, blockSize
				);
			fc.deleteOnExit(file2);
			checkDeleteOnExitData(1, fc, file1, file2);
			org.apache.hadoop.fs.Path dir = helper.getTestRootPath(fc, "dir3/dir4/dir5/dir6");
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc, dir, numBlocks, blockSize
				);
			fc.deleteOnExit(dir);
			checkDeleteOnExitData(1, fc, file1, file2, dir);
			// trigger deleteOnExit and ensure the registered
			// paths are cleaned up
			org.apache.hadoop.fs.FileContext.FINALIZER.run();
			checkDeleteOnExitData(0, fc, new org.apache.hadoop.fs.Path[0]);
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, file1));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, file2));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.FileContextTestHelper.exists(
				fc, dir));
		}
	}
}
