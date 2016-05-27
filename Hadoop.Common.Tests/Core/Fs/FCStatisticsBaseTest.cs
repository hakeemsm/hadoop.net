using System.Collections.Generic;
using Com.Google.Common.Util.Concurrent;
using NUnit.Framework;
using Sharpen;

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
			NUnit.Framework.Assert.AreEqual(0L, stats.GetBytesRead());
			NUnit.Framework.Assert.AreEqual(0L, stats.GetBytesWritten());
			NUnit.Framework.Assert.AreEqual(0, stats.GetWriteOps());
			stats.IncrementBytesWritten(1000);
			NUnit.Framework.Assert.AreEqual(1000L, stats.GetBytesWritten());
			NUnit.Framework.Assert.AreEqual(0, stats.GetWriteOps());
			stats.IncrementWriteOps(123);
			NUnit.Framework.Assert.AreEqual(123, stats.GetWriteOps());
			Sharpen.Thread thread = new _Thread_61(stats);
			thread.Start();
			Uninterruptibles.JoinUninterruptibly(thread);
			NUnit.Framework.Assert.AreEqual(124, stats.GetWriteOps());
			// Test copy constructor and reset function
			FileSystem.Statistics stats2 = new FileSystem.Statistics(stats);
			stats.Reset();
			NUnit.Framework.Assert.AreEqual(0, stats.GetWriteOps());
			NUnit.Framework.Assert.AreEqual(0L, stats.GetBytesWritten());
			NUnit.Framework.Assert.AreEqual(0L, stats.GetBytesRead());
			NUnit.Framework.Assert.AreEqual(124, stats2.GetWriteOps());
			NUnit.Framework.Assert.AreEqual(1000L, stats2.GetBytesWritten());
			NUnit.Framework.Assert.AreEqual(0L, stats2.GetBytesRead());
		}

		private sealed class _Thread_61 : Sharpen.Thread
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
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestStatistics()
		{
			URI fsUri = GetFsUri();
			FileSystem.Statistics stats = FileContext.GetStatistics(fsUri);
			NUnit.Framework.Assert.AreEqual(0, stats.GetBytesRead());
			Path filePath = fileContextTestHelper.GetTestRootPath(fc, "file1");
			FileContextTestHelper.CreateFile(fc, filePath, numBlocks, blockSize);
			NUnit.Framework.Assert.AreEqual(0, stats.GetBytesRead());
			VerifyWrittenBytes(stats);
			FSDataInputStream fstr = fc.Open(filePath);
			byte[] buf = new byte[blockSize];
			int bytesRead = fstr.Read(buf, 0, blockSize);
			fstr.Read(0, buf, 0, blockSize);
			NUnit.Framework.Assert.AreEqual(blockSize, bytesRead);
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
