using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Utility that wraps a
	/// <see cref="java.io.OutputStream"/>
	/// in a
	/// <see cref="java.io.DataOutputStream"/>
	/// .
	/// </summary>
	public class FSDataOutputStream : java.io.DataOutputStream, org.apache.hadoop.fs.Syncable
		, org.apache.hadoop.fs.CanSetDropBehind
	{
		private readonly java.io.OutputStream wrappedStream;

		private class PositionCache : java.io.FilterOutputStream
		{
			private org.apache.hadoop.fs.FileSystem.Statistics statistics;

			internal long position;

			/// <exception cref="System.IO.IOException"/>
			public PositionCache(java.io.OutputStream @out, org.apache.hadoop.fs.FileSystem.Statistics
				 stats, long pos)
				: base(@out)
			{
				statistics = stats;
				position = pos;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(int b)
			{
				@out.write(b);
				position++;
				if (statistics != null)
				{
					statistics.incrementBytesWritten(1);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(byte[] b, int off, int len)
			{
				@out.write(b, off, len);
				position += len;
				// update position
				if (statistics != null)
				{
					statistics.incrementBytesWritten(len);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long getPos()
			{
				return position;
			}

			// return cached position
			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				// ensure close works even if a null reference was passed in
				if (@out != null)
				{
					@out.close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		public FSDataOutputStream(java.io.OutputStream @out)
			: this(@out, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public FSDataOutputStream(java.io.OutputStream @out, org.apache.hadoop.fs.FileSystem.Statistics
			 stats)
			: this(@out, stats, 0)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public FSDataOutputStream(java.io.OutputStream @out, org.apache.hadoop.fs.FileSystem.Statistics
			 stats, long startPosition)
			: base(new org.apache.hadoop.fs.FSDataOutputStream.PositionCache(@out, stats, startPosition
				))
		{
			wrappedStream = @out;
		}

		/// <summary>Get the current position in the output stream.</summary>
		/// <returns>the current position in the output stream</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual long getPos()
		{
			return ((org.apache.hadoop.fs.FSDataOutputStream.PositionCache)@out).getPos();
		}

		/// <summary>Close the underlying output stream.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			@out.close();
		}

		// This invokes PositionCache.close()
		/// <summary>Get a reference to the wrapped output stream.</summary>
		/// <returns>the underlying output stream</returns>
		public virtual java.io.OutputStream getWrappedStream()
		{
			return wrappedStream;
		}

		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		public virtual void sync()
		{
			// Syncable
			if (wrappedStream is org.apache.hadoop.fs.Syncable)
			{
				((org.apache.hadoop.fs.Syncable)wrappedStream).sync();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void hflush()
		{
			// Syncable
			if (wrappedStream is org.apache.hadoop.fs.Syncable)
			{
				((org.apache.hadoop.fs.Syncable)wrappedStream).hflush();
			}
			else
			{
				wrappedStream.flush();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void hsync()
		{
			// Syncable
			if (wrappedStream is org.apache.hadoop.fs.Syncable)
			{
				((org.apache.hadoop.fs.Syncable)wrappedStream).hsync();
			}
			else
			{
				wrappedStream.flush();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void setDropBehind(bool dropBehind)
		{
			try
			{
				((org.apache.hadoop.fs.CanSetDropBehind)wrappedStream).setDropBehind(dropBehind);
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("the wrapped stream does " + "not support setting the drop-behind caching setting."
					);
			}
		}
	}
}
