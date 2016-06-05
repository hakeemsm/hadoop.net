using System;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.FS
{
	public sealed class ByteBufferUtil
	{
		/// <summary>Determine if a stream can do a byte buffer read via read(ByteBuffer buf)
		/// 	</summary>
		private static bool StreamHasByteBufferRead(InputStream stream)
		{
			if (!(stream is ByteBufferReadable))
			{
				return false;
			}
			if (!(stream is FSDataInputStream))
			{
				return true;
			}
			return ((FSDataInputStream)stream).GetWrappedStream() is ByteBufferReadable;
		}

		/// <summary>Perform a fallback read.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static ByteBuffer FallbackRead(InputStream stream, ByteBufferPool bufferPool
			, int maxLength)
		{
			if (bufferPool == null)
			{
				throw new NotSupportedException("zero-copy reads " + "were not available, and you did not provide a fallback "
					 + "ByteBufferPool.");
			}
			bool useDirect = StreamHasByteBufferRead(stream);
			ByteBuffer buffer = bufferPool.GetBuffer(useDirect, maxLength);
			if (buffer == null)
			{
				throw new NotSupportedException("zero-copy reads " + "were not available, and the ByteBufferPool did not provide "
					 + "us with " + (useDirect ? "a direct" : "an indirect") + "buffer.");
			}
			Preconditions.CheckState(buffer.Capacity() > 0);
			Preconditions.CheckState(buffer.IsDirect() == useDirect);
			maxLength = Math.Min(maxLength, buffer.Capacity());
			bool success = false;
			try
			{
				if (useDirect)
				{
					buffer.Clear();
					buffer.Limit(maxLength);
					ByteBufferReadable readable = (ByteBufferReadable)stream;
					int totalRead = 0;
					while (true)
					{
						if (totalRead >= maxLength)
						{
							success = true;
							break;
						}
						int nRead = readable.Read(buffer);
						if (nRead < 0)
						{
							if (totalRead > 0)
							{
								success = true;
							}
							break;
						}
						totalRead += nRead;
					}
					buffer.Flip();
				}
				else
				{
					buffer.Clear();
					int nRead = stream.Read(((byte[])buffer.Array()), buffer.ArrayOffset(), maxLength
						);
					if (nRead >= 0)
					{
						buffer.Limit(nRead);
						success = true;
					}
				}
			}
			finally
			{
				if (!success)
				{
					// If we got an error while reading, or if we are at EOF, we 
					// don't need the buffer any more.  We can give it back to the
					// bufferPool.
					bufferPool.PutBuffer(buffer);
					buffer = null;
				}
			}
			return buffer;
		}
	}
}
