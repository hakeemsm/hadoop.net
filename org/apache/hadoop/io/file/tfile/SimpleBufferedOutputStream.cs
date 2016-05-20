using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>
	/// A simplified BufferedOutputStream with borrowed buffer, and allow users to
	/// see how much data have been buffered.
	/// </summary>
	internal class SimpleBufferedOutputStream : java.io.FilterOutputStream
	{
		protected internal byte[] buf;

		protected internal int count = 0;

		public SimpleBufferedOutputStream(java.io.OutputStream @out, byte[] buf)
			: base(@out)
		{
			// the borrowed buffer
			// bytes used in buffer.
			// Constructor
			this.buf = buf;
		}

		/// <exception cref="System.IO.IOException"/>
		private void flushBuffer()
		{
			if (count > 0)
			{
				@out.write(buf, 0, count);
				count = 0;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(int b)
		{
			if (count >= buf.Length)
			{
				flushBuffer();
			}
			buf[count++] = unchecked((byte)b);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(byte[] b, int off, int len)
		{
			if (len >= buf.Length)
			{
				flushBuffer();
				@out.write(b, off, len);
				return;
			}
			if (len > buf.Length - count)
			{
				flushBuffer();
			}
			System.Array.Copy(b, off, buf, count, len);
			count += len;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void flush()
		{
			lock (this)
			{
				flushBuffer();
				@out.flush();
			}
		}

		// Get the size of internal buffer being used.
		public virtual int size()
		{
			return count;
		}
	}
}
