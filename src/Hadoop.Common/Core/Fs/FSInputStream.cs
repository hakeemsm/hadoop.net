using System.IO;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// FSInputStream is a generic old InputStream with a little bit
	/// of RAF-style seek ability.
	/// </summary>
	public abstract class FSInputStream : InputStream, Seekable, PositionedReadable
	{
		/// <summary>Seek to the given offset from the start of the file.</summary>
		/// <remarks>
		/// Seek to the given offset from the start of the file.
		/// The next read() will be from that location.  Can't
		/// seek past the end of the file.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Seek(long pos);

		/// <summary>Return the current offset from the start of the file</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract long GetPos();

		/// <summary>Seeks a different copy of the data.</summary>
		/// <remarks>
		/// Seeks a different copy of the data.  Returns true if
		/// found a new source, false otherwise.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool SeekToNewSource(long targetPos);

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(long position, byte[] buffer, int offset, int length)
		{
			lock (this)
			{
				long oldPos = GetPos();
				int nread = -1;
				try
				{
					Seek(position);
					nread = Read(buffer, offset, length);
				}
				finally
				{
					Seek(oldPos);
				}
				return nread;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(long position, byte[] buffer, int offset, int length
			)
		{
			int nread = 0;
			while (nread < length)
			{
				int nbytes = Read(position + nread, buffer, offset + nread, length - nread);
				if (nbytes < 0)
				{
					throw new EOFException("End of file reached before reading fully.");
				}
				nread += nbytes;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(long position, byte[] buffer)
		{
			ReadFully(position, buffer, 0, buffer.Length);
		}
	}
}
