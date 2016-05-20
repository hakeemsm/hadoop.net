using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// FSInputStream is a generic old InputStream with a little bit
	/// of RAF-style seek ability.
	/// </summary>
	public abstract class FSInputStream : java.io.InputStream, org.apache.hadoop.fs.Seekable
		, org.apache.hadoop.fs.PositionedReadable
	{
		/// <summary>Seek to the given offset from the start of the file.</summary>
		/// <remarks>
		/// Seek to the given offset from the start of the file.
		/// The next read() will be from that location.  Can't
		/// seek past the end of the file.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void seek(long pos);

		/// <summary>Return the current offset from the start of the file</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract long getPos();

		/// <summary>Seeks a different copy of the data.</summary>
		/// <remarks>
		/// Seeks a different copy of the data.  Returns true if
		/// found a new source, false otherwise.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool seekToNewSource(long targetPos);

		/// <exception cref="System.IO.IOException"/>
		public virtual int read(long position, byte[] buffer, int offset, int length)
		{
			lock (this)
			{
				long oldPos = getPos();
				int nread = -1;
				try
				{
					seek(position);
					nread = read(buffer, offset, length);
				}
				finally
				{
					seek(oldPos);
				}
				return nread;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFully(long position, byte[] buffer, int offset, int length
			)
		{
			int nread = 0;
			while (nread < length)
			{
				int nbytes = read(position + nread, buffer, offset + nread, length - nread);
				if (nbytes < 0)
				{
					throw new java.io.EOFException("End of file reached before reading fully.");
				}
				nread += nbytes;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFully(long position, byte[] buffer)
		{
			readFully(position, buffer, 0, buffer.Length);
		}
	}
}
