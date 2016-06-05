using System.IO;


namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>
	/// A simplified BufferedOutputStream with borrowed buffer, and allow users to
	/// see how much data have been buffered.
	/// </summary>
	internal class SimpleBufferedOutputStream : FilterOutputStream
	{
		protected internal byte[] buf;

		protected internal int count = 0;

		public SimpleBufferedOutputStream(OutputStream @out, byte[] buf)
			: base(@out)
		{
			// the borrowed buffer
			// bytes used in buffer.
			// Constructor
			this.buf = buf;
		}

		/// <exception cref="System.IO.IOException"/>
		private void FlushBuffer()
		{
			if (count > 0)
			{
				@out.Write(buf, 0, count);
				count = 0;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(int b)
		{
			if (count >= buf.Length)
			{
				FlushBuffer();
			}
			buf[count++] = unchecked((byte)b);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] b, int off, int len)
		{
			if (len >= buf.Length)
			{
				FlushBuffer();
				@out.Write(b, off, len);
				return;
			}
			if (len > buf.Length - count)
			{
				FlushBuffer();
			}
			System.Array.Copy(b, off, buf, count, len);
			count += len;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
			lock (this)
			{
				FlushBuffer();
				@out.Flush();
			}
		}

		// Get the size of internal buffer being used.
		public virtual int Size()
		{
			return count;
		}
	}
}
