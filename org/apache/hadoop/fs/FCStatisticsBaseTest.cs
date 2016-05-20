using Sharpen;

namespace org.apache.hadoop.fs
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

		protected internal readonly org.apache.hadoop.fs.FileContextTestHelper fileContextTestHelper
			 = new org.apache.hadoop.fs.FileContextTestHelper();

		protected internal static org.apache.hadoop.fs.FileContext fc = null;

		//fc should be set appropriately by the deriving test.
		/// <exception cref="System.Exception"/>
		public virtual void testStatisticsOperations()
		{
			org.apache.hadoop.fs.FileSystem.Statistics stats = new org.apache.hadoop.fs.FileSystem.Statistics
				("file");
			NUnit.Framework.Assert.AreEqual(0L, stats.getBytesRead());
			NUnit.Framework.Assert.AreEqual(0L, stats.getBytesWritten());
			NUnit.Framework.Assert.AreEqual(0, stats.getWriteOps());
			stats.incrementBytesWritten(1000);
			NUnit.Framework.Assert.AreEqual(1000L, stats.getBytesWritten());
			NUnit.Framework.Assert.AreEqual(0, stats.getWriteOps());
			stats.incrementWriteOps(123);
			NUnit.Framework.Assert.AreEqual(123, stats.getWriteOps());
			java.lang.Thread thread = new _Thread_61(stats);
			thread.start();
			com.google.common.util.concurrent.Uninterruptibles.joinUninterruptibly(thread);
			NUnit.Framework.Assert.AreEqual(124, stats.getWriteOps());
			// Test copy constructor and reset function
			org.apache.hadoop.fs.FileSystem.Statistics stats2 = new org.apache.hadoop.fs.FileSystem.Statistics
				(stats);
			stats.reset();
			NUnit.Framework.Assert.AreEqual(0, stats.getWriteOps());
			NUnit.Framework.Assert.AreEqual(0L, stats.getBytesWritten());
			NUnit.Framework.Assert.AreEqual(0L, stats.getBytesRead());
			NUnit.Framework.Assert.AreEqual(124, stats2.getWriteOps());
			NUnit.Framework.Assert.AreEqual(1000L, stats2.getBytesWritten());
			NUnit.Framework.Assert.AreEqual(0L, stats2.getBytesRead());
		}

		private sealed class _Thread_61 : java.lang.Thread
		{
			public _Thread_61(org.apache.hadoop.fs.FileSystem.Statistics stats)
			{
				this.stats = stats;
			}

			public override void run()
			{
				stats.incrementWriteOps(1);
			}

			private readonly org.apache.hadoop.fs.FileSystem.Statistics stats;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void testStatistics()
		{
			java.net.URI fsUri = getFsUri();
			org.apache.hadoop.fs.FileSystem.Statistics stats = org.apache.hadoop.fs.FileContext
				.getStatistics(fsUri);
			NUnit.Framework.Assert.AreEqual(0, stats.getBytesRead());
			org.apache.hadoop.fs.Path filePath = fileContextTestHelper.getTestRootPath(fc, "file1"
				);
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc, filePath, numBlocks, blockSize
				);
			NUnit.Framework.Assert.AreEqual(0, stats.getBytesRead());
			verifyWrittenBytes(stats);
			org.apache.hadoop.fs.FSDataInputStream fstr = fc.open(filePath);
			byte[] buf = new byte[blockSize];
			int bytesRead = fstr.read(buf, 0, blockSize);
			fstr.read(0, buf, 0, blockSize);
			NUnit.Framework.Assert.AreEqual(blockSize, bytesRead);
			verifyReadBytes(stats);
			verifyWrittenBytes(stats);
			verifyReadBytes(org.apache.hadoop.fs.FileContext.getStatistics(getFsUri()));
			System.Collections.Generic.IDictionary<java.net.URI, org.apache.hadoop.fs.FileSystem.Statistics
				> statsMap = org.apache.hadoop.fs.FileContext.getAllStatistics();
			java.net.URI exactUri = getSchemeAuthorityUri();
			verifyWrittenBytes(statsMap[exactUri]);
			fc.delete(filePath, true);
		}

		/// <summary>Bytes read may be different for different file systems.</summary>
		/// <remarks>
		/// Bytes read may be different for different file systems. This method should
		/// throw assertion error if bytes read are incorrect.
		/// </remarks>
		/// <param name="stats"/>
		protected internal abstract void verifyReadBytes(org.apache.hadoop.fs.FileSystem.Statistics
			 stats);

		/// <summary>Bytes written may be different for different file systems.</summary>
		/// <remarks>
		/// Bytes written may be different for different file systems. This method should
		/// throw assertion error if bytes written are incorrect.
		/// </remarks>
		/// <param name="stats"/>
		protected internal abstract void verifyWrittenBytes(org.apache.hadoop.fs.FileSystem.Statistics
			 stats);

		/// <summary>Returns the filesystem uri.</summary>
		/// <remarks>Returns the filesystem uri. Should be set</remarks>
		/// <returns>URI</returns>
		protected internal abstract java.net.URI getFsUri();

		protected internal virtual java.net.URI getSchemeAuthorityUri()
		{
			java.net.URI uri = getFsUri();
			string SchemeAuthString = uri.getScheme() + "://";
			if (uri.getAuthority() == null)
			{
				SchemeAuthString += "/";
			}
			else
			{
				SchemeAuthString += uri.getAuthority();
			}
			return java.net.URI.create(SchemeAuthString);
		}
	}
}
