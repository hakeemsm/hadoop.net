using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
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
		public class ChunkDecoder : InputStream
		{
			private DataInputStream @in = null;

			private bool lastChunk;

			private int remain = 0;

			private bool closed;

			public ChunkDecoder()
			{
				// nothing
				lastChunk = true;
				closed = true;
			}

			public virtual void Reset(DataInputStream downStream)
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
			public ChunkDecoder(DataInputStream @in)
			{
				this.@in = @in;
				lastChunk = false;
				closed = false;
			}

			/// <summary>Have we reached the last chunk.</summary>
			/// <returns>true if we have reached the last chunk.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool IsLastChunk()
			{
				CheckEOF();
				return lastChunk;
			}

			/// <summary>How many bytes remain in the current chunk?</summary>
			/// <returns>remaining bytes left in the current chunk.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual int GetRemain()
			{
				CheckEOF();
				return remain;
			}

			/// <summary>Reading the length of next chunk.</summary>
			/// <exception cref="System.IO.IOException">when no more data is available.</exception>
			private void ReadLength()
			{
				remain = Utils.ReadVInt(@in);
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
			private bool CheckEOF()
			{
				if (IsClosed())
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
					ReadLength();
				}
			}

			public override int Available()
			{
				/*
				* This method never blocks the caller. Returning 0 does not mean we reach
				* the end of the stream.
				*/
				return remain;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				if (CheckEOF())
				{
					return -1;
				}
				int ret = @in.Read();
				if (ret < 0)
				{
					throw new IOException("Corrupted chunk encoding stream");
				}
				--remain;
				return ret;
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
				if (!CheckEOF())
				{
					int n = Math.Min(remain, len);
					int ret = @in.Read(b, off, n);
					if (ret < 0)
					{
						throw new IOException("Corrupted chunk encoding stream");
					}
					remain -= ret;
					return ret;
				}
				return -1;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long Skip(long n)
			{
				if (!CheckEOF())
				{
					long ret = @in.Skip(Math.Min(remain, n));
					remain -= ret;
					return ret;
				}
				return 0;
			}

			public override bool MarkSupported()
			{
				return false;
			}

			public virtual bool IsClosed()
			{
				return closed;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				if (closed == false)
				{
					try
					{
						while (!CheckEOF())
						{
							Skip(int.MaxValue);
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
		public class ChunkEncoder : OutputStream
		{
			/// <summary>The data output stream it connects to.</summary>
			private DataOutputStream @out;

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
			public ChunkEncoder(DataOutputStream @out, byte[] buf)
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
			private void WriteChunk(byte[] chunk, int offset, int len, bool last)
			{
				if (last)
				{
					// always write out the length for the last chunk.
					Utils.WriteVInt(@out, len);
					if (len > 0)
					{
						@out.Write(chunk, offset, len);
					}
				}
				else
				{
					if (len > 0)
					{
						Utils.WriteVInt(@out, -len);
						@out.Write(chunk, offset, len);
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
			private void WriteBufData(byte[] data, int offset, int len)
			{
				if (count + len > 0)
				{
					Utils.WriteVInt(@out, -(count + len));
					@out.Write(buf, 0, count);
					count = 0;
					@out.Write(data, offset, len);
				}
			}

			/// <summary>Flush the internal buffer.</summary>
			/// <remarks>
			/// Flush the internal buffer.
			/// Is this the last call to flushBuffer?
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			private void FlushBuffer()
			{
				if (count > 0)
				{
					WriteChunk(buf, 0, count, false);
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
			public override void Write(byte[] b)
			{
				Write(b, 0, b.Length);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b, int off, int len)
			{
				if ((len + count) >= buf.Length)
				{
					/*
					* If the input data do not fit in buffer, flush the output buffer and
					* then write the data directly. In this way buffered streams will
					* cascade harmlessly.
					*/
					WriteBufData(b, off, len);
					return;
				}
				System.Array.Copy(b, off, buf, count, len);
				count += len;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				FlushBuffer();
				@out.Flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				if (buf != null)
				{
					try
					{
						WriteChunk(buf, 0, count, true);
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
		public class SingleChunkEncoder : OutputStream
		{
			/// <summary>The data output stream it connects to.</summary>
			private readonly DataOutputStream @out;

			/// <summary>The remaining bytes to be written.</summary>
			private int remain;

			private bool closed = false;

			/// <summary>Constructor.</summary>
			/// <param name="out">the underlying output stream.</param>
			/// <param name="size">The total # of bytes to be written as a single chunk.</param>
			/// <exception cref="System.IO.IOException">if an I/O error occurs.</exception>
			public SingleChunkEncoder(DataOutputStream @out, int size)
			{
				this.@out = @out;
				this.remain = size;
				Utils.WriteVInt(@out, size);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(int b)
			{
				if (remain > 0)
				{
					@out.Write(b);
					--remain;
				}
				else
				{
					throw new IOException("Writing more bytes than advertised size.");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b)
			{
				Write(b, 0, b.Length);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b, int off, int len)
			{
				if (remain >= len)
				{
					@out.Write(b, off, len);
					remain -= len;
				}
				else
				{
					throw new IOException("Writing more bytes than advertised size.");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				@out.Flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				if (closed == true)
				{
					return;
				}
				try
				{
					if (remain > 0)
					{
						throw new IOException("Writing less bytes than advertised size.");
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
