using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>
	/// Several related classes to support chunk-encoded sub-streams on top of a
	/// regular stream.
	/// </summary>
	internal sealed class Chunk
	{
		/// <summary>Prevent the instantiation of class.</summary>
		private Chunk()
		{
		}

		/// <summary>
		/// Decoding a chain of chunks encoded through ChunkEncoder or
		/// SingleChunkEncoder.
		/// </summary>
		public class ChunkDecoder : java.io.InputStream
		{
			private java.io.DataInputStream @in = null;

			private bool lastChunk;

			private int remain = 0;

			private bool closed;

			public ChunkDecoder()
			{
				// nothing
				lastChunk = true;
				closed = true;
			}

			public virtual void reset(java.io.DataInputStream downStream)
			{
				// no need to wind forward the old input.
				@in = downStream;
				lastChunk = false;
				remain = 0;
				closed = false;
			}

			/// <summary>Constructor</summary>
			/// <param name="in">
			/// The source input stream which contains chunk-encoded data
			/// stream.
			/// </param>
			public ChunkDecoder(java.io.DataInputStream @in)
			{
				this.@in = @in;
				lastChunk = false;
				closed = false;
			}

			/// <summary>Have we reached the last chunk.</summary>
			/// <returns>true if we have reached the last chunk.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool isLastChunk()
			{
				checkEOF();
				return lastChunk;
			}

			/// <summary>How many bytes remain in the current chunk?</summary>
			/// <returns>remaining bytes left in the current chunk.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual int getRemain()
			{
				checkEOF();
				return remain;
			}

			/// <summary>Reading the length of next chunk.</summary>
			/// <exception cref="System.IO.IOException">when no more data is available.</exception>
			private void readLength()
			{
				remain = org.apache.hadoop.io.file.tfile.Utils.readVInt(@in);
				if (remain >= 0)
				{
					lastChunk = true;
				}
				else
				{
					remain = -remain;
				}
			}

			/// <summary>Check whether we reach the end of the stream.</summary>
			/// <returns>
			/// false if the chunk encoded stream has more data to read (in which
			/// case available() will be greater than 0); true otherwise.
			/// </returns>
			/// <exception cref="System.IO.IOException">on I/O errors.</exception>
			private bool checkEOF()
			{
				if (isClosed())
				{
					return true;
				}
				while (true)
				{
					if (remain > 0)
					{
						return false;
					}
					if (lastChunk)
					{
						return true;
					}
					readLength();
				}
			}

			public override int available()
			{
				/*
				* This method never blocks the caller. Returning 0 does not mean we reach
				* the end of the stream.
				*/
				return remain;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read()
			{
				if (checkEOF())
				{
					return -1;
				}
				int ret = @in.read();
				if (ret < 0)
				{
					throw new System.IO.IOException("Corrupted chunk encoding stream");
				}
				--remain;
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read(byte[] b)
			{
				return read(b, 0, b.Length);
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read(byte[] b, int off, int len)
			{
				if ((off | len | (off + len) | (b.Length - (off + len))) < 0)
				{
					throw new System.IndexOutOfRangeException();
				}
				if (!checkEOF())
				{
					int n = System.Math.min(remain, len);
					int ret = @in.read(b, off, n);
					if (ret < 0)
					{
						throw new System.IO.IOException("Corrupted chunk encoding stream");
					}
					remain -= ret;
					return ret;
				}
				return -1;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long skip(long n)
			{
				if (!checkEOF())
				{
					long ret = @in.skip(System.Math.min(remain, n));
					remain -= ret;
					return ret;
				}
				return 0;
			}

			public override bool markSupported()
			{
				return false;
			}

			public virtual bool isClosed()
			{
				return closed;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				if (closed == false)
				{
					try
					{
						while (!checkEOF())
						{
							skip(int.MaxValue);
						}
					}
					finally
					{
						closed = true;
					}
				}
			}
		}

		/// <summary>Chunk Encoder.</summary>
		/// <remarks>
		/// Chunk Encoder. Encoding the output data into a chain of chunks in the
		/// following sequences: -len1, byte[len1], -len2, byte[len2], ... len_n,
		/// byte[len_n]. Where len1, len2, ..., len_n are the lengths of the data
		/// chunks. Non-terminal chunks have their lengths negated. Non-terminal chunks
		/// cannot have length 0. All lengths are in the range of 0 to
		/// Integer.MAX_VALUE and are encoded in Utils.VInt format.
		/// </remarks>
		public class ChunkEncoder : java.io.OutputStream
		{
			/// <summary>The data output stream it connects to.</summary>
			private java.io.DataOutputStream @out;

			/// <summary>
			/// The internal buffer that is only used when we do not know the advertised
			/// size.
			/// </summary>
			private byte[] buf;

			/// <summary>The number of valid bytes in the buffer.</summary>
			/// <remarks>
			/// The number of valid bytes in the buffer. This value is always in the
			/// range <tt>0</tt> through <tt>buf.length</tt>; elements <tt>buf[0]</tt>
			/// through <tt>buf[count-1]</tt> contain valid byte data.
			/// </remarks>
			private int count;

			/// <summary>Constructor.</summary>
			/// <param name="out">the underlying output stream.</param>
			/// <param name="buf">
			/// user-supplied buffer. The buffer would be used exclusively by
			/// the ChunkEncoder during its life cycle.
			/// </param>
			public ChunkEncoder(java.io.DataOutputStream @out, byte[] buf)
			{
				this.@out = @out;
				this.buf = buf;
				this.count = 0;
			}

			/// <summary>Write out a chunk.</summary>
			/// <param name="chunk">The chunk buffer.</param>
			/// <param name="offset">Offset to chunk buffer for the beginning of chunk.</param>
			/// <param name="len"/>
			/// <param name="last">Is this the last call to flushBuffer?</param>
			/// <exception cref="System.IO.IOException"/>
			private void writeChunk(byte[] chunk, int offset, int len, bool last)
			{
				if (last)
				{
					// always write out the length for the last chunk.
					org.apache.hadoop.io.file.tfile.Utils.writeVInt(@out, len);
					if (len > 0)
					{
						@out.write(chunk, offset, len);
					}
				}
				else
				{
					if (len > 0)
					{
						org.apache.hadoop.io.file.tfile.Utils.writeVInt(@out, -len);
						@out.write(chunk, offset, len);
					}
				}
			}

			/// <summary>
			/// Write out a chunk that is a concatenation of the internal buffer plus
			/// user supplied data.
			/// </summary>
			/// <remarks>
			/// Write out a chunk that is a concatenation of the internal buffer plus
			/// user supplied data. This will never be the last block.
			/// </remarks>
			/// <param name="data">User supplied data buffer.</param>
			/// <param name="offset">Offset to user data buffer.</param>
			/// <param name="len">User data buffer size.</param>
			/// <exception cref="System.IO.IOException"/>
			private void writeBufData(byte[] data, int offset, int len)
			{
				if (count + len > 0)
				{
					org.apache.hadoop.io.file.tfile.Utils.writeVInt(@out, -(count + len));
					@out.write(buf, 0, count);
					count = 0;
					@out.write(data, offset, len);
				}
			}

			/// <summary>Flush the internal buffer.</summary>
			/// <remarks>
			/// Flush the internal buffer.
			/// Is this the last call to flushBuffer?
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			private void flushBuffer()
			{
				if (count > 0)
				{
					writeChunk(buf, 0, count, false);
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
			public override void write(byte[] b)
			{
				write(b, 0, b.Length);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(byte[] b, int off, int len)
			{
				if ((len + count) >= buf.Length)
				{
					/*
					* If the input data do not fit in buffer, flush the output buffer and
					* then write the data directly. In this way buffered streams will
					* cascade harmlessly.
					*/
					writeBufData(b, off, len);
					return;
				}
				System.Array.Copy(b, off, buf, count, len);
				count += len;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void flush()
			{
				flushBuffer();
				@out.flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				if (buf != null)
				{
					try
					{
						writeChunk(buf, 0, count, true);
					}
					finally
					{
						buf = null;
						@out = null;
					}
				}
			}
		}

		/// <summary>Encode the whole stream as a single chunk.</summary>
		/// <remarks>
		/// Encode the whole stream as a single chunk. Expecting to know the size of
		/// the chunk up-front.
		/// </remarks>
		public class SingleChunkEncoder : java.io.OutputStream
		{
			/// <summary>The data output stream it connects to.</summary>
			private readonly java.io.DataOutputStream @out;

			/// <summary>The remaining bytes to be written.</summary>
			private int remain;

			private bool closed = false;

			/// <summary>Constructor.</summary>
			/// <param name="out">the underlying output stream.</param>
			/// <param name="size">The total # of bytes to be written as a single chunk.</param>
			/// <exception cref="System.IO.IOException">if an I/O error occurs.</exception>
			public SingleChunkEncoder(java.io.DataOutputStream @out, int size)
			{
				this.@out = @out;
				this.remain = size;
				org.apache.hadoop.io.file.tfile.Utils.writeVInt(@out, size);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(int b)
			{
				if (remain > 0)
				{
					@out.write(b);
					--remain;
				}
				else
				{
					throw new System.IO.IOException("Writing more bytes than advertised size.");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(byte[] b)
			{
				write(b, 0, b.Length);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(byte[] b, int off, int len)
			{
				if (remain >= len)
				{
					@out.write(b, off, len);
					remain -= len;
				}
				else
				{
					throw new System.IO.IOException("Writing more bytes than advertised size.");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void flush()
			{
				@out.flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				if (closed == true)
				{
					return;
				}
				try
				{
					if (remain > 0)
					{
						throw new System.IO.IOException("Writing less bytes than advertised size.");
					}
				}
				finally
				{
					closed = true;
				}
			}
		}
	}
}
