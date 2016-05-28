using System;
using System.IO;
using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// An InputStream implementations which reads from some other InputStream
	/// but expects an exact number of bytes.
	/// </summary>
	/// <remarks>
	/// An InputStream implementations which reads from some other InputStream
	/// but expects an exact number of bytes. Any attempts to read past the
	/// specified number of bytes will return as if the end of the stream
	/// was reached. If the end of the underlying stream is reached prior to
	/// the specified number of bytes, an EOFException is thrown.
	/// </remarks>
	public class ExactSizeInputStream : FilterInputStream
	{
		private int remaining;

		/// <summary>
		/// Construct an input stream that will read no more than
		/// 'numBytes' bytes.
		/// </summary>
		/// <remarks>
		/// Construct an input stream that will read no more than
		/// 'numBytes' bytes.
		/// If an EOF occurs on the underlying stream before numBytes
		/// bytes have been read, an EOFException will be thrown.
		/// </remarks>
		/// <param name="in">the inputstream to wrap</param>
		/// <param name="numBytes">the number of bytes to read</param>
		public ExactSizeInputStream(InputStream @in, int numBytes)
			: base(@in)
		{
			Preconditions.CheckArgument(numBytes >= 0, "Negative expected bytes: ", numBytes);
			this.remaining = numBytes;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Available()
		{
			return Math.Min(base.Available(), remaining);
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			// EOF if we reached our limit
			if (remaining <= 0)
			{
				return -1;
			}
			int result = base.Read();
			if (result >= 0)
			{
				--remaining;
			}
			else
			{
				if (remaining > 0)
				{
					// Underlying stream reached EOF but we haven't read the expected
					// number of bytes.
					throw new EOFException("Premature EOF. Expected " + remaining + "more bytes");
				}
			}
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] b, int off, int len)
		{
			if (remaining <= 0)
			{
				return -1;
			}
			len = Math.Min(len, remaining);
			int result = base.Read(b, off, len);
			if (result >= 0)
			{
				remaining -= result;
			}
			else
			{
				if (remaining > 0)
				{
					// Underlying stream reached EOF but we haven't read the expected
					// number of bytes.
					throw new EOFException("Premature EOF. Expected " + remaining + "more bytes");
				}
			}
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long Skip(long n)
		{
			long result = base.Skip(Math.Min(n, remaining));
			if (result > 0)
			{
				remaining -= result;
			}
			else
			{
				if (remaining > 0)
				{
					// Underlying stream reached EOF but we haven't read the expected
					// number of bytes.
					throw new EOFException("Premature EOF. Expected " + remaining + "more bytes");
				}
			}
			return result;
		}

		public override bool MarkSupported()
		{
			return false;
		}

		public override void Mark(int readlimit)
		{
			throw new NotSupportedException();
		}
	}
}
