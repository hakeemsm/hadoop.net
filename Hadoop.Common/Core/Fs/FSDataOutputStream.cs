using System;
using System.IO;
using Hadoop.Common.Core.Fs;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Utility that wraps a
	/// <see cref="System.IO.OutputStream"/>
	/// in a
	/// <see cref="System.IO.DataOutputStream"/>
	/// .
	/// </summary>
	public class FSDataOutputStream : DataOutputStream, Syncable, CanSetDropBehind
	{
		private readonly OutputStream wrappedStream;

		private class PositionCache : FilterOutputStream
		{
			private FileSystem.Statistics statistics;

			internal long position;

			/// <exception cref="System.IO.IOException"/>
			public PositionCache(OutputStream @out, FileSystem.Statistics stats, long pos)
				: base(@out)
			{
				statistics = stats;
				position = pos;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(int b)
			{
				@out.Write(b);
				position++;
				if (statistics != null)
				{
					statistics.IncrementBytesWritten(1);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b, int off, int len)
			{
				@out.Write(b, off, len);
				position += len;
				// update position
				if (statistics != null)
				{
					statistics.IncrementBytesWritten(len);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPos()
			{
				return position;
			}

			// return cached position
			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				// ensure close works even if a null reference was passed in
				if (@out != null)
				{
					@out.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public FSDataOutputStream(OutputStream @out)
			: this(@out, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public FSDataOutputStream(OutputStream @out, FileSystem.Statistics stats)
			: this(@out, stats, 0)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public FSDataOutputStream(OutputStream @out, FileSystem.Statistics stats, long startPosition
			)
			: base(new FSDataOutputStream.PositionCache(@out, stats, startPosition))
		{
			wrappedStream = @out;
		}

		/// <summary>Get the current position in the output stream.</summary>
		/// <returns>the current position in the output stream</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			return ((FSDataOutputStream.PositionCache)@out).GetPos();
		}

		/// <summary>Close the underlying output stream.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			@out.Close();
		}

		// This invokes PositionCache.close()
		/// <summary>Get a reference to the wrapped output stream.</summary>
		/// <returns>the underlying output stream</returns>
		public virtual OutputStream GetWrappedStream()
		{
			return wrappedStream;
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public virtual void Sync()
		{
			// Syncable
			if (wrappedStream is Syncable)
			{
				((Syncable)wrappedStream).Sync();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Hflush()
		{
			// Syncable
			if (wrappedStream is Syncable)
			{
				((Syncable)wrappedStream).Hflush();
			}
			else
			{
				wrappedStream.Flush();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Hsync()
		{
			// Syncable
			if (wrappedStream is Syncable)
			{
				((Syncable)wrappedStream).Hsync();
			}
			else
			{
				wrappedStream.Flush();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetDropBehind(bool dropBehind)
		{
			try
			{
				((CanSetDropBehind)wrappedStream).SetDropBehind(dropBehind);
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("the wrapped stream does " + "not support setting the drop-behind caching setting."
					);
			}
		}
	}
}
