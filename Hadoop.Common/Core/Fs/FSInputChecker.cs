using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// This is a generic input stream for verifying checksums for
	/// data before it is read by a user.
	/// </summary>
	public abstract class FSInputChecker : FSInputStream
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.FSInputChecker
			));

		/// <summary>The file name from which data is read from</summary>
		protected internal Path file;

		private Checksum sum;

		private bool verifyChecksum = true;

		private int maxChunkSize;

		private byte[] buf;

		private byte[] checksum;

		private IntBuffer checksumInts;

		private int pos;

		private int count;

		private int numOfRetries;

		private long chunkPos = 0;

		private const int ChunksPerRead = 32;

		protected internal const int ChecksumSize = 4;

		/// <summary>Constructor</summary>
		/// <param name="file">The name of the file to be read</param>
		/// <param name="numOfRetries">Number of read retries when ChecksumError occurs</param>
		protected internal FSInputChecker(Path file, int numOfRetries)
		{
			// data bytes for checksum (eg 512)
			// buffer for non-chunk-aligned reading
			// wrapper on checksum buffer
			// the position of the reader inside buf
			// the number of bytes currently in buf
			// cached file position
			// this should always be a multiple of maxChunkSize
			// Number of checksum chunks that can be read at once into a user
			// buffer. Chosen by benchmarks - higher values do not reduce
			// CPU usage. The size of the data reads made to the underlying stream
			// will be CHUNKS_PER_READ * maxChunkSize.
			// 32-bit checksum
			this.file = file;
			this.numOfRetries = numOfRetries;
		}

		/// <summary>Constructor</summary>
		/// <param name="file">The name of the file to be read</param>
		/// <param name="numOfRetries">Number of read retries when ChecksumError occurs</param>
		/// <param name="sum">the type of Checksum engine</param>
		/// <param name="chunkSize">maximun chunk size</param>
		/// <param name="checksumSize">the number byte of each checksum</param>
		protected internal FSInputChecker(Path file, int numOfRetries, bool verifyChecksum
			, Checksum sum, int chunkSize, int checksumSize)
			: this(file, numOfRetries)
		{
			Set(verifyChecksum, sum, chunkSize, checksumSize);
		}

		/// <summary>
		/// Reads in checksum chunks into <code>buf</code> at <code>offset</code>
		/// and checksum into <code>checksum</code>.
		/// </summary>
		/// <remarks>
		/// Reads in checksum chunks into <code>buf</code> at <code>offset</code>
		/// and checksum into <code>checksum</code>.
		/// Since checksums can be disabled, there are two cases implementors need
		/// to worry about:
		/// (a) needChecksum() will return false:
		/// - len can be any positive value
		/// - checksum will be null
		/// Implementors should simply pass through to the underlying data stream.
		/// or
		/// (b) needChecksum() will return true:
		/// - len &gt;= maxChunkSize
		/// - checksum.length is a multiple of CHECKSUM_SIZE
		/// Implementors should read an integer number of data chunks into
		/// buf. The amount read should be bounded by len or by
		/// checksum.length / CHECKSUM_SIZE * maxChunkSize. Note that len may
		/// be a value that is not a multiple of maxChunkSize, in which case
		/// the implementation may return less than len.
		/// The method is used for implementing read, therefore, it should be optimized
		/// for sequential reading.
		/// </remarks>
		/// <param name="pos">chunkPos</param>
		/// <param name="buf">desitination buffer</param>
		/// <param name="offset">offset in buf at which to store data</param>
		/// <param name="len">maximum number of bytes to read</param>
		/// <param name="checksum">the data buffer into which to write checksums</param>
		/// <returns>number of bytes read</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract int ReadChunk(long pos, byte[] buf, int offset, int len
			, byte[] checksum);

		/// <summary>Return position of beginning of chunk containing pos.</summary>
		/// <param name="pos">a postion in the file</param>
		/// <returns>the starting position of the chunk which contains the byte</returns>
		protected internal abstract long GetChunkPosition(long pos);

		/// <summary>Return true if there is a need for checksum verification</summary>
		protected internal virtual bool NeedChecksum()
		{
			lock (this)
			{
				return verifyChecksum && sum != null;
			}
		}

		/// <summary>Read one checksum-verified byte</summary>
		/// <returns>
		/// the next byte of data, or <code>-1</code> if the end of the
		/// stream is reached.
		/// </returns>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			lock (this)
			{
				if (pos >= count)
				{
					Fill();
					if (pos >= count)
					{
						return -1;
					}
				}
				return buf[pos++] & unchecked((int)(0xff));
			}
		}

		/// <summary>
		/// Read checksum verified bytes from this byte-input stream into
		/// the specified byte array, starting at the given offset.
		/// </summary>
		/// <remarks>
		/// Read checksum verified bytes from this byte-input stream into
		/// the specified byte array, starting at the given offset.
		/// <p> This method implements the general contract of the corresponding
		/// <code>
		/// <see cref="System.IO.InputStream.Read(byte[], int, int)">read</see>
		/// </code> method of
		/// the <code>
		/// <see cref="System.IO.InputStream"/>
		/// </code> class.  As an additional
		/// convenience, it attempts to read as many bytes as possible by repeatedly
		/// invoking the <code>read</code> method of the underlying stream.  This
		/// iterated <code>read</code> continues until one of the following
		/// conditions becomes true: <ul>
		/// <li> The specified number of bytes have been read,
		/// <li> The <code>read</code> method of the underlying stream returns
		/// <code>-1</code>, indicating end-of-file.
		/// </ul> If the first <code>read</code> on the underlying stream returns
		/// <code>-1</code> to indicate end-of-file then this method returns
		/// <code>-1</code>.  Otherwise this method returns the number of bytes
		/// actually read.
		/// </remarks>
		/// <param name="b">destination buffer.</param>
		/// <param name="off">offset at which to start storing bytes.</param>
		/// <param name="len">maximum number of bytes to read.</param>
		/// <returns>
		/// the number of bytes read, or <code>-1</code> if the end of
		/// the stream has been reached.
		/// </returns>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// ChecksumException if any checksum error occurs
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] b, int off, int len)
		{
			lock (this)
			{
				// parameter check
				if ((off | len | (off + len) | (b.Length - (off + len))) < 0)
				{
					throw new IndexOutOfRangeException();
				}
				else
				{
					if (len == 0)
					{
						return 0;
					}
				}
				int n = 0;
				for (; ; )
				{
					int nread = Read1(b, off + n, len - n);
					if (nread <= 0)
					{
						return (n == 0) ? nread : n;
					}
					n += nread;
					if (n >= len)
					{
						return n;
					}
				}
			}
		}

		/// <summary>Fills the buffer with a chunk data.</summary>
		/// <remarks>
		/// Fills the buffer with a chunk data.
		/// No mark is supported.
		/// This method assumes that all data in the buffer has already been read in,
		/// hence pos &gt; count.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void Fill()
		{
			System.Diagnostics.Debug.Assert((pos >= count));
			// fill internal buffer
			count = ReadChecksumChunk(buf, 0, maxChunkSize);
			if (count < 0)
			{
				count = 0;
			}
		}

		/*
		* Read characters into a portion of an array, reading from the underlying
		* stream at most once if necessary.
		*/
		/// <exception cref="System.IO.IOException"/>
		private int Read1(byte[] b, int off, int len)
		{
			int avail = count - pos;
			if (avail <= 0)
			{
				if (len >= maxChunkSize)
				{
					// read a chunk to user buffer directly; avoid one copy
					int nread = ReadChecksumChunk(b, off, len);
					return nread;
				}
				else
				{
					// read a chunk into the local buffer
					Fill();
					if (count <= 0)
					{
						return -1;
					}
					else
					{
						avail = count;
					}
				}
			}
			// copy content of the local buffer to the user buffer
			int cnt = (avail < len) ? avail : len;
			System.Array.Copy(buf, pos, b, off, cnt);
			pos += cnt;
			return cnt;
		}

		/* Read up one or more checksum chunk to array <i>b</i> at pos <i>off</i>
		* It requires at least one checksum chunk boundary
		* in between <cur_pos, cur_pos+len>
		* and it stops reading at the last boundary or at the end of the stream;
		* Otherwise an IllegalArgumentException is thrown.
		* This makes sure that all data read are checksum verified.
		*
		* @param b   the buffer into which the data is read.
		* @param off the start offset in array <code>b</code>
		*            at which the data is written.
		* @param len the maximum number of bytes to read.
		* @return    the total number of bytes read into the buffer, or
		*            <code>-1</code> if there is no more data because the end of
		*            the stream has been reached.
		* @throws IOException if an I/O error occurs.
		*/
		/// <exception cref="System.IO.IOException"/>
		private int ReadChecksumChunk(byte[] b, int off, int len)
		{
			// invalidate buffer
			count = pos = 0;
			int read = 0;
			bool retry = true;
			int retriesLeft = numOfRetries;
			do
			{
				retriesLeft--;
				try
				{
					read = ReadChunk(chunkPos, b, off, len, checksum);
					if (read > 0)
					{
						if (NeedChecksum())
						{
							VerifySums(b, off, read);
						}
						chunkPos += read;
					}
					retry = false;
				}
				catch (ChecksumException ce)
				{
					Log.Info("Found checksum error: b[" + off + ", " + (off + read) + "]=" + StringUtils
						.ByteToHexString(b, off, off + read), ce);
					if (retriesLeft == 0)
					{
						throw;
					}
					// try a new replica
					if (SeekToNewSource(chunkPos))
					{
						// Since at least one of the sources is different, 
						// the read might succeed, so we'll retry.
						Seek(chunkPos);
					}
					else
					{
						// Neither the data stream nor the checksum stream are being read
						// from different sources, meaning we'll still get a checksum error 
						// if we try to do the read again.  We throw an exception instead.
						throw;
					}
				}
			}
			while (retry);
			return read;
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		private void VerifySums(byte[] b, int off, int read)
		{
			int leftToVerify = read;
			int verifyOff = 0;
			checksumInts.Rewind();
			checksumInts.Limit((read - 1) / maxChunkSize + 1);
			while (leftToVerify > 0)
			{
				sum.Update(b, off + verifyOff, Math.Min(leftToVerify, maxChunkSize));
				int expected = checksumInts.Get();
				int calculated = (int)sum.GetValue();
				sum.Reset();
				if (expected != calculated)
				{
					long errPos = chunkPos + verifyOff;
					throw new ChecksumException("Checksum error: " + file + " at " + errPos + " exp: "
						 + expected + " got: " + calculated, errPos);
				}
				leftToVerify -= maxChunkSize;
				verifyOff += maxChunkSize;
			}
		}

		/// <summary>
		/// Convert a checksum byte array to a long
		/// This is deprecated since 0.22 since it is no longer in use
		/// by this class.
		/// </summary>
		[Obsolete]
		public static long Checksum2long(byte[] checksum)
		{
			long crc = 0L;
			for (int i = 0; i < checksum.Length; i++)
			{
				crc |= (unchecked((long)(0xffL)) & (long)checksum[i]) << ((checksum.Length - i - 
					1) * 8);
			}
			return crc;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetPos()
		{
			lock (this)
			{
				return chunkPos - Math.Max(0L, count - pos);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Available()
		{
			lock (this)
			{
				return Math.Max(0, count - pos);
			}
		}

		/// <summary>
		/// Skips over and discards <code>n</code> bytes of data from the
		/// input stream.
		/// </summary>
		/// <remarks>
		/// Skips over and discards <code>n</code> bytes of data from the
		/// input stream.
		/// <p>This method may skip more bytes than are remaining in the backing
		/// file. This produces no exception and the number of bytes skipped
		/// may include some number of bytes that were beyond the EOF of the
		/// backing file. Attempting to read from the stream after skipping past
		/// the end will result in -1 indicating the end of the file.
		/// <p>If <code>n</code> is negative, no bytes are skipped.
		/// </remarks>
		/// <param name="n">the number of bytes to be skipped.</param>
		/// <returns>the actual number of bytes skipped.</returns>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// ChecksumException if the chunk to skip to is corrupted
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override long Skip(long n)
		{
			lock (this)
			{
				if (n <= 0)
				{
					return 0;
				}
				Seek(GetPos() + n);
				return n;
			}
		}

		/// <summary>Seek to the given position in the stream.</summary>
		/// <remarks>
		/// Seek to the given position in the stream.
		/// The next read() will be from that position.
		/// <p>This method may seek past the end of the file.
		/// This produces no exception and an attempt to read from
		/// the stream will result in -1 indicating the end of the file.
		/// </remarks>
		/// <param name="pos">the postion to seek to.</param>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// ChecksumException if the chunk to seek to is corrupted
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override void Seek(long pos)
		{
			lock (this)
			{
				if (pos < 0)
				{
					throw new EOFException(FSExceptionMessages.NegativeSeek);
				}
				// optimize: check if the pos is in the buffer
				long start = chunkPos - this.count;
				if (pos >= start && pos < chunkPos)
				{
					this.pos = (int)(pos - start);
					return;
				}
				// reset the current state
				ResetState();
				// seek to a checksum boundary
				chunkPos = GetChunkPosition(pos);
				// scan to the desired position
				int delta = (int)(pos - chunkPos);
				if (delta > 0)
				{
					ReadFully(this, new byte[delta], 0, delta);
				}
			}
		}

		/// <summary>
		/// A utility function that tries to read up to <code>len</code> bytes from
		/// <code>stm</code>
		/// </summary>
		/// <param name="stm">an input stream</param>
		/// <param name="buf">destiniation buffer</param>
		/// <param name="offset">offset at which to store data</param>
		/// <param name="len">number of bytes to read</param>
		/// <returns>actual number of bytes read</returns>
		/// <exception cref="System.IO.IOException">if there is any IO error</exception>
		protected internal static int ReadFully(InputStream stm, byte[] buf, int offset, 
			int len)
		{
			int n = 0;
			for (; ; )
			{
				int nread = stm.Read(buf, offset + n, len - n);
				if (nread <= 0)
				{
					return (n == 0) ? nread : n;
				}
				n += nread;
				if (n >= len)
				{
					return n;
				}
			}
		}

		/// <summary>Set the checksum related parameters</summary>
		/// <param name="verifyChecksum">whether to verify checksum</param>
		/// <param name="sum">which type of checksum to use</param>
		/// <param name="maxChunkSize">maximun chunk size</param>
		/// <param name="checksumSize">checksum size</param>
		protected internal void Set(bool verifyChecksum, Checksum sum, int maxChunkSize, 
			int checksumSize)
		{
			lock (this)
			{
				// The code makes assumptions that checksums are always 32-bit.
				System.Diagnostics.Debug.Assert(!verifyChecksum || sum == null || checksumSize ==
					 ChecksumSize);
				this.maxChunkSize = maxChunkSize;
				this.verifyChecksum = verifyChecksum;
				this.sum = sum;
				this.buf = new byte[maxChunkSize];
				// The size of the checksum array here determines how much we can
				// read in a single call to readChunk
				this.checksum = new byte[ChunksPerRead * checksumSize];
				this.checksumInts = ByteBuffer.Wrap(checksum).AsIntBuffer();
				this.count = 0;
				this.pos = 0;
			}
		}

		public sealed override bool MarkSupported()
		{
			return false;
		}

		public sealed override void Mark(int readlimit)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public sealed override void Reset()
		{
			throw new IOException("mark/reset not supported");
		}

		/* reset this FSInputChecker's state */
		private void ResetState()
		{
			// invalidate buffer
			count = 0;
			pos = 0;
			// reset Checksum
			if (sum != null)
			{
				sum.Reset();
			}
		}
	}
}
