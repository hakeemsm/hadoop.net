using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// <p>
	/// Tests the File Context Statistics for
	/// <see cref="LocalFileSystem"/>
	/// </p>
	/// </summary>
	public class TestLocalFsFCStatistics : org.apache.hadoop.fs.FCStatisticsBaseTest
	{
		internal const string LOCAL_FS_ROOT_URI = "file:///tmp/test";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			fc = org.apache.hadoop.fs.FileContext.getLocalFSFileContext();
			fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "test"), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			fc.delete(fileContextTestHelper.getTestRootPath(fc, "test"), true);
		}

		protected internal override void verifyReadBytes(org.apache.hadoop.fs.FileSystem.Statistics
			 stats)
		{
			// one blockSize for read, one for pread
			NUnit.Framework.Assert.AreEqual(2 * blockSize, stats.getBytesRead());
		}

		protected internal override void verifyWrittenBytes(org.apache.hadoop.fs.FileSystem.Statistics
			 stats)
		{
			//Extra 12 bytes are written apart from the block.
			NUnit.Framework.Assert.AreEqual(blockSize + 12, stats.getBytesWritten());
		}

		protected internal override java.net.URI getFsUri()
		{
			return java.net.URI.create(LOCAL_FS_ROOT_URI);
		}
	}
}
