using System;
using System.IO;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>
	/// BoundedRangeFIleInputStream abstracts a contiguous region of a Hadoop
	/// FSDataInputStream as a regular input stream.
	/// </summary>
	/// <remarks>
	/// BoundedRangeFIleInputStream abstracts a contiguous region of a Hadoop
	/// FSDataInputStream as a regular input stream. One can create multiple
	/// BoundedRangeFileInputStream on top of the same FSDataInputStream and they
	/// would not interfere with each other.
	/// </remarks>
	internal class BoundedRangeFileInputStream : InputStream
	{
		private FSDataInputStream @in;

		private long pos;

		private long end;

		private long mark;

		private readonly byte[] oneByte = new byte[1];

		/// <summary>Constructor</summary>
		/// <param name="in">The FSDataInputStream we connect to.</param>
		/// <param name="offset">Begining offset of the region.</param>
		/// <param name="length">
		/// Length of the region.
		/// The actual length of the region may be smaller if (off_begin +
		/// length) goes beyond the end of FS input stream.
		/// </param>
		public BoundedRangeFileInputStream(FSDataInputStream @in, long offset, long length
			)
		{
			if (offset < 0 || length < 0)
			{
				throw new IndexOutOfRangeException("Invalid offset/length: " + offset + "/" + length
					);
			}
			this.@in = @in;
			this.pos = offset;
			this.end = offset + length;
			this.mark = -1;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Available()
		{
			int avail = @in.Available();
			if (pos + avail > end)
			{
				avail = (int)(end - pos);
			}
			return avail;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			int ret = Read(oneByte);
			if (ret == 1)
			{
				return oneByte[0] & unchecked((int)(0xff));
			}
			return -1;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] b)
		{
			return Read(b, 0, b.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] b, int off, int len)
		{
			if ((off | len | (off + len) | (b.Length - (off + len))) < 0)
			{
				throw new IndexOutOfRangeException();
			}
			int n = (int)Math.Min(int.MaxValue, Math.Min(len, (end - pos)));
			if (n == 0)
			{
				return -1;
			}
			int ret = 0;
			lock (@in)
			{
				@in.Seek(pos);
				ret = @in.Read(b, off, n);
			}
			if (ret < 0)
			{
				end = pos;
				return -1;
			}
			pos += ret;
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long Skip(long n)
		{
			/*
			* We may skip beyond the end of the file.
			*/
			long len = Math.Min(n, end - pos);
			pos += len;
			return len;
		}

		public override void Mark(int readlimit)
		{
			lock (this)
			{
				mark = pos;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Reset()
		{
			lock (this)
			{
				if (mark < 0)
				{
					throw new IOException("Resetting to invalid mark");
				}
				pos = mark;
			}
		}

		public override bool MarkSupported()
		{
			return true;
		}

		public override void Close()
		{
			// Invalidate the state of the stream.
			@in = null;
			pos = end;
			mark = -1;
		}
	}
}
