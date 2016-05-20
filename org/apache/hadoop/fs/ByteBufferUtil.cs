using Sharpen;

namespace org.apache.hadoop.fs
{
	public sealed class ByteBufferUtil
	{
		/// <summary>Determine if a stream can do a byte buffer read via read(ByteBuffer buf)
		/// 	</summary>
		private static bool streamHasByteBufferRead(java.io.InputStream stream)
		{
			if (!(stream is org.apache.hadoop.fs.ByteBufferReadable))
			{
				return false;
			}
			if (!(stream is org.apache.hadoop.fs.FSDataInputStream))
			{
				return true;
			}
			return ((org.apache.hadoop.fs.FSDataInputStream)stream).getWrappedStream() is org.apache.hadoop.fs.ByteBufferReadable;
		}

		/// <summary>Perform a fallback read.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static java.nio.ByteBuffer fallbackRead(java.io.InputStream stream, org.apache.hadoop.io.ByteBufferPool
			 bufferPool, int maxLength)
		{
			if (bufferPool == null)
			{
				throw new System.NotSupportedException("zero-copy reads " + "were not available, and you did not provide a fallback "
					 + "ByteBufferPool.");
			}
			bool useDirect = streamHasByteBufferRead(stream);
			java.nio.ByteBuffer buffer = bufferPool.getBuffer(useDirect, maxLength);
			if (buffer == null)
			{
				throw new System.NotSupportedException("zero-copy reads " + "were not available, and the ByteBufferPool did not provide "
					 + "us with " + (useDirect ? "a direct" : "an indirect") + "buffer.");
			}
			com.google.common.@base.Preconditions.checkState(buffer.capacity() > 0);
			com.google.common.@base.Preconditions.checkState(buffer.isDirect() == useDirect);
			maxLength = System.Math.min(maxLength, buffer.capacity());
			bool success = false;
			try
			{
				if (useDirect)
				{
					buffer.clear();
					buffer.limit(maxLength);
					org.apache.hadoop.fs.ByteBufferReadable readable = (org.apache.hadoop.fs.ByteBufferReadable
						)stream;
					int totalRead = 0;
					while (true)
					{
						if (totalRead >= maxLength)
						{
							success = true;
							break;
						}
						int nRead = readable.read(buffer);
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
					buffer.flip();
				}
				else
				{
					buffer.clear();
					int nRead = stream.read(((byte[])buffer.array()), buffer.arrayOffset(), maxLength
						);
					if (nRead >= 0)
					{
						buffer.limit(nRead);
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
					bufferPool.putBuffer(buffer);
					buffer = null;
				}
			}
			return buffer;
		}
	}
}
