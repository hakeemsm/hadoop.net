using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Manages a pool of threads which can issue readahead requests on file descriptors.
	/// 	</summary>
	public class ReadaheadPool
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ReadaheadPool
			)));

		private const int POOL_SIZE = 4;

		private const int MAX_POOL_SIZE = 16;

		private const int CAPACITY = 1024;

		private readonly java.util.concurrent.ThreadPoolExecutor pool;

		private static org.apache.hadoop.io.ReadaheadPool instance;

		/// <summary>Return the singleton instance for the current process.</summary>
		public static org.apache.hadoop.io.ReadaheadPool getInstance()
		{
			lock (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ReadaheadPool))
				)
			{
				if (instance == null && org.apache.hadoop.io.nativeio.NativeIO.isAvailable())
				{
					instance = new org.apache.hadoop.io.ReadaheadPool();
				}
				return instance;
			}
		}

		private ReadaheadPool()
		{
			pool = new java.util.concurrent.ThreadPoolExecutor(POOL_SIZE, MAX_POOL_SIZE, 3L, 
				java.util.concurrent.TimeUnit.SECONDS, new java.util.concurrent.ArrayBlockingQueue
				<java.lang.Runnable>(CAPACITY));
			pool.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy
				());
			pool.setThreadFactory(new com.google.common.util.concurrent.ThreadFactoryBuilder(
				).setDaemon(true).setNameFormat("Readahead Thread #%d").build());
		}

		/// <summary>Issue a request to readahead on the given file descriptor.</summary>
		/// <param name="identifier">
		/// a textual identifier that will be used in error
		/// messages (e.g. the file name)
		/// </param>
		/// <param name="fd">the file descriptor to read ahead</param>
		/// <param name="curPos">the current offset at which reads are being issued</param>
		/// <param name="readaheadLength">the configured length to read ahead</param>
		/// <param name="maxOffsetToRead">
		/// the maximum offset that will be readahead
		/// (useful if, for example, only some segment of the file is
		/// requested by the user). Pass
		/// <see cref="Long.MAX_VALUE"/>
		/// to allow
		/// readahead to the end of the file.
		/// </param>
		/// <param name="lastReadahead">
		/// the result returned by the previous invocation
		/// of this function on this file descriptor, or null if this is
		/// the first call
		/// </param>
		/// <returns>
		/// an object representing this outstanding request, or null
		/// if no readahead was performed
		/// </returns>
		public virtual org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest readaheadStream
			(string identifier, java.io.FileDescriptor fd, long curPos, long readaheadLength
			, long maxOffsetToRead, org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest lastReadahead
			)
		{
			com.google.common.@base.Preconditions.checkArgument(curPos <= maxOffsetToRead, "Readahead position %s higher than maxOffsetToRead %s"
				, curPos, maxOffsetToRead);
			if (readaheadLength <= 0)
			{
				return null;
			}
			long lastOffset = long.MinValue;
			if (lastReadahead != null)
			{
				lastOffset = lastReadahead.getOffset();
			}
			// trigger each readahead when we have reached the halfway mark
			// in the previous readahead. This gives the system time
			// to satisfy the readahead before we start reading the data.
			long nextOffset = lastOffset + readaheadLength / 2;
			if (curPos >= nextOffset)
			{
				// cancel any currently pending readahead, to avoid
				// piling things up in the queue. Each reader should have at most
				// one outstanding request in the queue.
				if (lastReadahead != null)
				{
					lastReadahead.cancel();
					lastReadahead = null;
				}
				long length = System.Math.min(readaheadLength, maxOffsetToRead - curPos);
				if (length <= 0)
				{
					// we've reached the end of the stream
					return null;
				}
				return submitReadahead(identifier, fd, curPos, length);
			}
			else
			{
				return lastReadahead;
			}
		}

		/// <summary>Submit a request to readahead on the given file descriptor.</summary>
		/// <param name="identifier">a textual identifier used in error messages, etc.</param>
		/// <param name="fd">the file descriptor to readahead</param>
		/// <param name="off">the offset at which to start the readahead</param>
		/// <param name="len">the number of bytes to read</param>
		/// <returns>an object representing this pending request</returns>
		public virtual org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest submitReadahead
			(string identifier, java.io.FileDescriptor fd, long off, long len)
		{
			org.apache.hadoop.io.ReadaheadPool.ReadaheadRequestImpl req = new org.apache.hadoop.io.ReadaheadPool.ReadaheadRequestImpl
				(identifier, fd, off, len);
			pool.execute(req);
			if (LOG.isTraceEnabled())
			{
				LOG.trace("submit readahead: " + req);
			}
			return req;
		}

		/// <summary>
		/// An outstanding readahead request that has been submitted to
		/// the pool.
		/// </summary>
		/// <remarks>
		/// An outstanding readahead request that has been submitted to
		/// the pool. This request may be pending or may have been
		/// completed.
		/// </remarks>
		public interface ReadaheadRequest
		{
			/// <summary>Cancels the request for readahead.</summary>
			/// <remarks>
			/// Cancels the request for readahead. This should be used
			/// if the reader no longer needs the requested data, <em>before</em>
			/// closing the related file descriptor.
			/// It is safe to use even if the readahead request has already
			/// been fulfilled.
			/// </remarks>
			void cancel();

			/// <returns>the requested offset</returns>
			long getOffset();

			/// <returns>the requested length</returns>
			long getLength();
		}

		private class ReadaheadRequestImpl : java.lang.Runnable, org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest
		{
			private readonly string identifier;

			private readonly java.io.FileDescriptor fd;

			private readonly long off;

			private readonly long len;

			private volatile bool canceled = false;

			private ReadaheadRequestImpl(string identifier, java.io.FileDescriptor fd, long off
				, long len)
			{
				this.identifier = identifier;
				this.fd = fd;
				this.off = off;
				this.len = len;
			}

			public virtual void run()
			{
				if (canceled)
				{
					return;
				}
				// There's a very narrow race here that the file will close right at
				// this instant. But if that happens, we'll likely receive an EBADF
				// error below, and see that it's canceled, ignoring the error.
				// It's also possible that we'll end up requesting readahead on some
				// other FD, which may be wasted work, but won't cause a problem.
				try
				{
					org.apache.hadoop.io.nativeio.NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible
						(identifier, fd, off, len, org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_WILLNEED
						);
				}
				catch (System.IO.IOException ioe)
				{
					if (canceled)
					{
						// no big deal - the reader canceled the request and closed
						// the file.
						return;
					}
					LOG.warn("Failed readahead on " + identifier, ioe);
				}
			}

			public virtual void cancel()
			{
				canceled = true;
			}

			// We could attempt to remove it from the work queue, but that would
			// add complexity. In practice, the work queues remain very short,
			// so removing canceled requests has no gain.
			public virtual long getOffset()
			{
				return off;
			}

			public virtual long getLength()
			{
				return len;
			}

			public override string ToString()
			{
				return "ReadaheadRequestImpl [identifier='" + identifier + "', fd=" + fd + ", off="
					 + off + ", len=" + len + "]";
			}
		}
	}
}
