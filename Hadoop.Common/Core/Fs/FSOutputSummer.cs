using System;
using System.IO;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// This is a generic output stream for generating checksums for
	/// data before it is written to the underlying stream
	/// </summary>
	public abstract class FSOutputSummer : OutputStream
	{
		private readonly DataChecksum sum;

		private byte[] buf;

		private byte[] checksum;

		private int count;

		private const int BufferNumChunks = 9;

		protected internal FSOutputSummer(DataChecksum sum)
		{
			// data checksum
			// internal buffer for storing data before it is checksumed
			// internal buffer for storing checksum
			// The number of valid bytes in the buffer.
			// We want this value to be a multiple of 3 because the native code checksums
			// 3 chunks simultaneously. The chosen value of 9 strikes a balance between
			// limiting the number of JNI calls and flushing to the underlying stream
			// relatively frequently.
			this.sum = sum;
			this.buf = new byte[sum.GetBytesPerChecksum() * BufferNumChunks];
			this.checksum = new byte[GetChecksumSize() * BufferNumChunks];
			this.count = 0;
		}

		/* write the data chunk in <code>b</code> staring at <code>offset</code> with
		* a length of <code>len > 0</code>, and its checksum
		*/
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void WriteChunk(byte[] b, int bOffset, int bLen, byte
			[] checksum, int checksumOffset, int checksumLen);

		/// <summary>
		/// Check if the implementing OutputStream is closed and should no longer
		/// accept writes.
		/// </summary>
		/// <remarks>
		/// Check if the implementing OutputStream is closed and should no longer
		/// accept writes. Implementations should do nothing if this stream is not
		/// closed, and should throw an
		/// <see cref="System.IO.IOException"/>
		/// if it is closed.
		/// </remarks>
		/// <exception cref="System.IO.IOException">if this stream is already closed.</exception>
		protected internal abstract void CheckClosed();

		/// <summary>Write one byte</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Write(int b)
		{
			lock (this)
			{
				buf[count++] = unchecked((byte)b);
				if (count == buf.Length)
				{
					FlushBuffer();
				}
			}
		}

		/// <summary>
		/// Writes <code>len</code> bytes from the specified byte array
		/// starting at offset <code>off</code> and generate a checksum for
		/// each data chunk.
		/// </summary>
		/// <remarks>
		/// Writes <code>len</code> bytes from the specified byte array
		/// starting at offset <code>off</code> and generate a checksum for
		/// each data chunk.
		/// <p> This method stores bytes from the given array into this
		/// stream's buffer before it gets checksumed. The buffer gets checksumed
		/// and flushed to the underlying output stream when all data
		/// in a checksum chunk are in the buffer.  If the buffer is empty and
		/// requested length is at least as large as the size of next checksum chunk
		/// size, this method will checksum and write the chunk directly
		/// to the underlying output stream.  Thus it avoids uneccessary data copy.
		/// </remarks>
		/// <param name="b">the data.</param>
		/// <param name="off">the start offset in the data.</param>
		/// <param name="len">the number of bytes to write.</param>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] b, int off, int len)
		{
			lock (this)
			{
				CheckClosed();
				if (off < 0 || len < 0 || off > b.Length - len)
				{
					throw new IndexOutOfRangeException();
				}
				for (int n = 0; n < len; n += Write1(b, off + n, len - n))
				{
				}
			}
		}

		/// <summary>
		/// Write a portion of an array, flushing to the underlying
		/// stream at most once if necessary.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private int Write1(byte[] b, int off, int len)
		{
			if (count == 0 && len >= buf.Length)
			{
				// local buffer is empty and user buffer size >= local buffer size, so
				// simply checksum the user buffer and send it directly to the underlying
				// stream
				int length = buf.Length;
				WriteChecksumChunks(b, off, length);
				return length;
			}
			// copy user data to local buffer
			int bytesToCopy = buf.Length - count;
			bytesToCopy = (len < bytesToCopy) ? len : bytesToCopy;
			System.Array.Copy(b, off, buf, count, bytesToCopy);
			count += bytesToCopy;
			if (count == buf.Length)
			{
				// local buffer is full
				FlushBuffer();
			}
			return bytesToCopy;
		}

		/* Forces any buffered output bytes to be checksumed and written out to
		* the underlying output stream.
		*/
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void FlushBuffer()
		{
			lock (this)
			{
				FlushBuffer(false, true);
			}
		}

		/* Forces buffered output bytes to be checksummed and written out to
		* the underlying output stream. If there is a trailing partial chunk in the
		* buffer,
		* 1) flushPartial tells us whether to flush that chunk
		* 2) if flushPartial is true, keep tells us whether to keep that chunk in the
		* buffer (if flushPartial is false, it is always kept in the buffer)
		*
		* Returns the number of bytes that were flushed but are still left in the
		* buffer (can only be non-zero if keep is true).
		*/
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual int FlushBuffer(bool keep, bool flushPartial)
		{
			lock (this)
			{
				int bufLen = count;
				int partialLen = bufLen % sum.GetBytesPerChecksum();
				int lenToFlush = flushPartial ? bufLen : bufLen - partialLen;
				if (lenToFlush != 0)
				{
					WriteChecksumChunks(buf, 0, lenToFlush);
					if (!flushPartial || keep)
					{
						count = partialLen;
						System.Array.Copy(buf, bufLen - count, buf, 0, count);
					}
					else
					{
						count = 0;
					}
				}
				// total bytes left minus unflushed bytes left
				return count - (bufLen - lenToFlush);
			}
		}

		/// <summary>
		/// Checksums all complete data chunks and flushes them to the underlying
		/// stream.
		/// </summary>
		/// <remarks>
		/// Checksums all complete data chunks and flushes them to the underlying
		/// stream. If there is a trailing partial chunk, it is not flushed and is
		/// maintained in the buffer.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
			FlushBuffer(false, false);
		}

		/// <summary>Return the number of valid bytes currently in the buffer.</summary>
		protected internal virtual int GetBufferedDataSize()
		{
			lock (this)
			{
				return count;
			}
		}

		/// <returns>the size for a checksum.</returns>
		protected internal virtual int GetChecksumSize()
		{
			return sum.GetChecksumSize();
		}

		/// <summary>
		/// Generate checksums for the given data chunks and output chunks & checksums
		/// to the underlying output stream.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void WriteChecksumChunks(byte[] b, int off, int len)
		{
			sum.CalculateChunkedSums(b, off, len, checksum, 0);
			for (int i = 0; i < len; i += sum.GetBytesPerChecksum())
			{
				int chunkLen = Math.Min(sum.GetBytesPerChecksum(), len - i);
				int ckOffset = i / sum.GetBytesPerChecksum() * GetChecksumSize();
				WriteChunk(b, off + i, chunkLen, checksum, ckOffset, GetChecksumSize());
			}
		}

		/// <summary>Converts a checksum integer value to a byte stream</summary>
		public static byte[] ConvertToByteStream(Checksum sum, int checksumSize)
		{
			return Int2byte((int)sum.GetValue(), new byte[checksumSize]);
		}

		internal static byte[] Int2byte(int integer, byte[] bytes)
		{
			if (bytes.Length != 0)
			{
				bytes[0] = unchecked((byte)(((int)(((uint)integer) >> 24)) & unchecked((int)(0xFF
					))));
				bytes[1] = unchecked((byte)(((int)(((uint)integer) >> 16)) & unchecked((int)(0xFF
					))));
				bytes[2] = unchecked((byte)(((int)(((uint)integer) >> 8)) & unchecked((int)(0xFF)
					)));
				bytes[3] = unchecked((byte)(((int)(((uint)integer) >> 0)) & unchecked((int)(0xFF)
					)));
				return bytes;
			}
			return bytes;
		}

		/// <summary>Resets existing buffer with a new one of the specified size.</summary>
		protected internal virtual void SetChecksumBufSize(int size)
		{
			lock (this)
			{
				this.buf = new byte[size];
				this.checksum = new byte[sum.GetChecksumSize(size)];
				this.count = 0;
			}
		}

		protected internal virtual void ResetChecksumBufSize()
		{
			lock (this)
			{
				SetChecksumBufSize(sum.GetBytesPerChecksum() * BufferNumChunks);
			}
		}
	}
}
