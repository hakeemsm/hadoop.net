using System;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// A generic abstract class to support journaling of edits logs into
	/// a persistent storage.
	/// </summary>
	public abstract class EditLogOutputStream : IDisposable
	{
		private long numSync;

		private long totalTimeSync;

		/// <exception cref="System.IO.IOException"/>
		public EditLogOutputStream()
		{
			// these are statistics counters
			// number of sync(s) to disk
			// total time to sync
			numSync = totalTimeSync = 0;
		}

		/// <summary>Write edits log operation to the stream.</summary>
		/// <param name="op">operation</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Write(FSEditLogOp op);

		/// <summary>Write raw data to an edit log.</summary>
		/// <remarks>
		/// Write raw data to an edit log. This data should already have
		/// the transaction ID, checksum, etc included. It is for use
		/// within the BackupNode when replicating edits from the
		/// NameNode.
		/// </remarks>
		/// <param name="bytes">the bytes to write.</param>
		/// <param name="offset">offset in the bytes to write from</param>
		/// <param name="length">number of bytes to write</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void WriteRaw(byte[] bytes, int offset, int length);

		/// <summary>Create and initialize underlying persistent edits log storage.</summary>
		/// <param name="layoutVersion">The LayoutVersion of the journal</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Create(int layoutVersion);

		/// <summary>Close the journal.</summary>
		/// <exception cref="System.IO.IOException">
		/// if the journal can't be closed,
		/// or if there are unflushed edits
		/// </exception>
		public abstract void Close();

		/// <summary>Close the stream without necessarily flushing any pending data.</summary>
		/// <remarks>
		/// Close the stream without necessarily flushing any pending data.
		/// This may be called after a previous write or close threw an exception.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Abort();

		/// <summary>All data that has been written to the stream so far will be flushed.</summary>
		/// <remarks>
		/// All data that has been written to the stream so far will be flushed.
		/// New data can be still written to the stream while flushing is performed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void SetReadyToFlush();

		/// <summary>
		/// Flush and sync all data that is ready to be flush
		/// <see cref="SetReadyToFlush()"/>
		/// into underlying persistent store.
		/// </summary>
		/// <param name="durable">
		/// if true, the edits should be made truly durable before
		/// returning
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void FlushAndSync(bool durable);

		/// <summary>Flush data to persistent store.</summary>
		/// <remarks>
		/// Flush data to persistent store.
		/// Collect sync metrics.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Flush()
		{
			Flush(true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Flush(bool durable)
		{
			numSync++;
			long start = Time.MonotonicNow();
			FlushAndSync(durable);
			long end = Time.MonotonicNow();
			totalTimeSync += (end - start);
		}

		/// <summary>
		/// Implement the policy when to automatically sync the buffered edits log
		/// The buffered edits can be flushed when the buffer becomes full or
		/// a certain period of time is elapsed.
		/// </summary>
		/// <returns>true if the buffered data should be automatically synced to disk</returns>
		public virtual bool ShouldForceSync()
		{
			return false;
		}

		/// <summary>
		/// Return total time spent in
		/// <see cref="FlushAndSync(bool)"/>
		/// </summary>
		internal virtual long GetTotalSyncTime()
		{
			return totalTimeSync;
		}

		/// <summary>
		/// Return number of calls to
		/// <see cref="FlushAndSync(bool)"/>
		/// </summary>
		protected internal virtual long GetNumSync()
		{
			return numSync;
		}

		/// <returns>
		/// a short text snippet suitable for describing the current
		/// status of the stream
		/// </returns>
		public virtual string GenerateReport()
		{
			return ToString();
		}
	}
}
