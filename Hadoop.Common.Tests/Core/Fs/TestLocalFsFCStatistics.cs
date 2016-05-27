using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// <p>
	/// Tests the File Context Statistics for
	/// <see cref="LocalFileSystem"/>
	/// </p>
	/// </summary>
	public class TestLocalFsFCStatistics : FCStatisticsBaseTest
	{
		internal const string LocalFsRootUri = "file:///tmp/test";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			fc = FileContext.GetLocalFSFileContext();
			fc.Mkdir(fileContextTestHelper.GetTestRootPath(fc, "test"), FileContext.DefaultPerm
				, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fc.Delete(fileContextTestHelper.GetTestRootPath(fc, "test"), true);
		}

		protected internal override void VerifyReadBytes(FileSystem.Statistics stats)
		{
			// one blockSize for read, one for pread
			NUnit.Framework.Assert.AreEqual(2 * blockSize, stats.GetBytesRead());
		}

		protected internal override void VerifyWrittenBytes(FileSystem.Statistics stats)
		{
			//Extra 12 bytes are written apart from the block.
			NUnit.Framework.Assert.AreEqual(blockSize + 12, stats.GetBytesWritten());
		}

		protected internal override URI GetFsUri()
		{
			return URI.Create(LocalFsRootUri);
		}
	}
}
