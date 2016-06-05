using System.Collections.Generic;
using Com.Google.Common.Util.Concurrent;
using NUnit.Framework;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// <p>
	/// Base class to test
	/// <see cref="FileContext"/>
	/// Statistics.
	/// </p>
	/// </summary>
	public abstract class FCStatisticsBaseTest
	{
		protected internal static int blockSize = 512;

		protected internal static int numBlocks = 1;

		protected internal readonly FileContextTestHelper fileContextTestHelper = new FileContextTestHelper
			();

		protected internal static FileContext fc = null;

		//fc should be set appropriately by the deriving test.
		/// <exception cref="System.Exception"/>
		public virtual void TestStatisticsOperations()
		{
			FileSystem.Statistics stats = new FileSystem.Statistics("file");
			Assert.Equal(0L, stats.GetBytesRead());
			Assert.Equal(0L, stats.GetBytesWritten());
			Assert.Equal(0, stats.GetWriteOps());
			stats.IncrementBytesWritten(1000);
			Assert.Equal(1000L, stats.GetBytesWritten());
			Assert.Equal(0, stats.GetWriteOps());
			stats.IncrementWriteOps(123);
			Assert.Equal(123, stats.GetWriteOps());
			Thread thread = new _Thread_61(stats);
			thread.Start();
			Uninterruptibles.JoinUninterruptibly(thread);
			Assert.Equal(124, stats.GetWriteOps());
			// Test copy constructor and reset function
			FileSystem.Statistics stats2 = new FileSystem.Statistics(stats);
			stats.Reset();
			Assert.Equal(0, stats.GetWriteOps());
			Assert.Equal(0L, stats.GetBytesWritten());
			Assert.Equal(0L, stats.GetBytesRead());
			Assert.Equal(124, stats2.GetWriteOps());
			Assert.Equal(1000L, stats2.GetBytesWritten());
			Assert.Equal(0L, stats2.GetBytesRead());
		}

		private sealed class _Thread_61 : Thread
		{
			public _Thread_61(FileSystem.Statistics stats)
			{
				this.stats = stats;
			}

			public override void Run()
			{
				stats.IncrementWriteOps(1);
			}

			private readonly FileSystem.Statistics stats;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="URISyntaxException"/>
		[Fact]
		public virtual void TestStatistics()
		{
			URI fsUri = GetFsUri();
			FileSystem.Statistics stats = FileContext.GetStatistics(fsUri);
			Assert.Equal(0, stats.GetBytesRead());
			Path filePath = fileContextTestHelper.GetTestRootPath(fc, "file1");
			FileContextTestHelper.CreateFile(fc, filePath, numBlocks, blockSize);
			Assert.Equal(0, stats.GetBytesRead());
			VerifyWrittenBytes(stats);
			FSDataInputStream fstr = fc.Open(filePath);
			byte[] buf = new byte[blockSize];
			int bytesRead = fstr.Read(buf, 0, blockSize);
			fstr.Read(0, buf, 0, blockSize);
			Assert.Equal(blockSize, bytesRead);
			VerifyReadBytes(stats);
			VerifyWrittenBytes(stats);
			VerifyReadBytes(FileContext.GetStatistics(GetFsUri()));
			IDictionary<URI, FileSystem.Statistics> statsMap = FileContext.GetAllStatistics();
			URI exactUri = GetSchemeAuthorityUri();
			VerifyWrittenBytes(statsMap[exactUri]);
			fc.Delete(filePath, true);
		}

		/// <summary>Bytes read may be different for different file systems.</summary>
		/// <remarks>
		/// Bytes read may be different for different file systems. This method should
		/// throw assertion error if bytes read are incorrect.
		/// </remarks>
		/// <param name="stats"/>
		protected internal abstract void VerifyReadBytes(FileSystem.Statistics stats);

		/// <summary>Bytes written may be different for different file systems.</summary>
		/// <remarks>
		/// Bytes written may be different for different file systems. This method should
		/// throw assertion error if bytes written are incorrect.
		/// </remarks>
		/// <param name="stats"/>
		protected internal abstract void VerifyWrittenBytes(FileSystem.Statistics stats);

		/// <summary>Returns the filesystem uri.</summary>
		/// <remarks>Returns the filesystem uri. Should be set</remarks>
		/// <returns>URI</returns>
		protected internal abstract URI GetFsUri();

		protected internal virtual URI GetSchemeAuthorityUri()
		{
			URI uri = GetFsUri();
			string SchemeAuthString = uri.GetScheme() + "://";
			if (uri.GetAuthority() == null)
			{
				SchemeAuthString += "/";
			}
			else
			{
				SchemeAuthString += uri.GetAuthority();
			}
			return URI.Create(SchemeAuthString);
		}
	}
}
