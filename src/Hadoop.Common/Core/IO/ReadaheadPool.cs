using System;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO.Nativeio;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>Manages a pool of threads which can issue readahead requests on file descriptors.
	/// 	</summary>
	public class ReadaheadPool
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.ReadaheadPool
			));

		private const int PoolSize = 4;

		private const int MaxPoolSize = 16;

		private const int Capacity = 1024;

		private readonly ThreadPoolExecutor pool;

		private static Org.Apache.Hadoop.IO.ReadaheadPool instance;

		/// <summary>Return the singleton instance for the current process.</summary>
		public static Org.Apache.Hadoop.IO.ReadaheadPool GetInstance()
		{
			lock (typeof(Org.Apache.Hadoop.IO.ReadaheadPool))
			{
				if (instance == null && NativeIO.IsAvailable())
				{
					instance = new Org.Apache.Hadoop.IO.ReadaheadPool();
				}
				return instance;
			}
		}

		private ReadaheadPool()
		{
			pool = new ThreadPoolExecutor(PoolSize, MaxPoolSize, 3L, TimeUnit.Seconds, new ArrayBlockingQueue
				<Runnable>(Capacity));
			pool.SetRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy());
			pool.SetThreadFactory(new ThreadFactoryBuilder().SetDaemon(true).SetNameFormat("Readahead Thread #%d"
				).Build());
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
		public virtual ReadaheadPool.ReadaheadRequest ReadaheadStream(string identifier, 
			FileDescriptor fd, long curPos, long readaheadLength, long maxOffsetToRead, ReadaheadPool.ReadaheadRequest
			 lastReadahead)
		{
			Preconditions.CheckArgument(curPos <= maxOffsetToRead, "Readahead position %s higher than maxOffsetToRead %s"
				, curPos, maxOffsetToRead);
			if (readaheadLength <= 0)
			{
				return null;
			}
			long lastOffset = long.MinValue;
			if (lastReadahead != null)
			{
				lastOffset = lastReadahead.GetOffset();
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
					lastReadahead.Cancel();
					lastReadahead = null;
				}
				long length = Math.Min(readaheadLength, maxOffsetToRead - curPos);
				if (length <= 0)
				{
					// we've reached the end of the stream
					return null;
				}
				return SubmitReadahead(identifier, fd, curPos, length);
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
		public virtual ReadaheadPool.ReadaheadRequest SubmitReadahead(string identifier, 
			FileDescriptor fd, long off, long len)
		{
			ReadaheadPool.ReadaheadRequestImpl req = new ReadaheadPool.ReadaheadRequestImpl(identifier
				, fd, off, len);
			pool.Execute(req);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("submit readahead: " + req);
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
			void Cancel();

			/// <returns>the requested offset</returns>
			long GetOffset();

			/// <returns>the requested length</returns>
			long GetLength();
		}

		private class ReadaheadRequestImpl : Runnable, ReadaheadPool.ReadaheadRequest
		{
			private readonly string identifier;

			private readonly FileDescriptor fd;

			private readonly long off;

			private readonly long len;

			private volatile bool canceled = false;

			private ReadaheadRequestImpl(string identifier, FileDescriptor fd, long off, long
				 len)
			{
				this.identifier = identifier;
				this.fd = fd;
				this.off = off;
				this.len = len;
			}

			public virtual void Run()
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
					NativeIO.POSIX.GetCacheManipulator().PosixFadviseIfPossible(identifier, fd, off, 
						len, NativeIO.POSIX.PosixFadvWillneed);
				}
				catch (IOException ioe)
				{
					if (canceled)
					{
						// no big deal - the reader canceled the request and closed
						// the file.
						return;
					}
					Log.Warn("Failed readahead on " + identifier, ioe);
				}
			}

			public virtual void Cancel()
			{
				canceled = true;
			}

			// We could attempt to remove it from the work queue, but that would
			// add complexity. In practice, the work queues remain very short,
			// so removing canceled requests has no gain.
			public virtual long GetOffset()
			{
				return off;
			}

			public virtual long GetLength()
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
