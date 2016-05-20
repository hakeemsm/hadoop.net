using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// This is a generic input stream for verifying checksums for
	/// data before it is read by a user.
	/// </summary>
	public abstract class FSInputChecker : org.apache.hadoop.fs.FSInputStream
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FSInputChecker
			)));

		/// <summary>The file name from which data is read from</summary>
		protected internal org.apache.hadoop.fs.Path file;

		private java.util.zip.Checksum sum;

		private bool verifyChecksum = true;

		private int maxChunkSize;

		private byte[] buf;

		private byte[] checksum;

		private java.nio.IntBuffer checksumInts;

		private int pos;

		private int count;

		private int numOfRetries;

		private long chunkPos = 0;

		private const int CHUNKS_PER_READ = 32;

		protected internal const int CHECKSUM_SIZE = 4;

		/// <summary>Constructor</summary>
		/// <param name="file">The name of the file to be read</param>
		/// <param name="numOfRetries">Number of read retries when ChecksumError occurs</param>
		protected internal FSInputChecker(org.apache.hadoop.fs.Path file, int numOfRetries
			)
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
		protected internal FSInputChecker(org.apache.hadoop.fs.Path file, int numOfRetries
			, bool verifyChecksum, java.util.zip.Checksum sum, int chunkSize, int checksumSize
			)
			: this(file, numOfRetries)
		{
			set(verifyChecksum, sum, chunkSize, checksumSize);
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
		protected internal abstract int readChunk(long pos, byte[] buf, int offset, int len
			, byte[] checksum);

		/// <summary>Return position of beginning of chunk containing pos.</summary>
		/// <param name="pos">a postion in the file</param>
		/// <returns>the starting position of the chunk which contains the byte</returns>
		protected internal abstract long getChunkPosition(long pos);

		/// <summary>Return true if there is a need for checksum verification</summary>
		protected internal virtual bool needChecksum()
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
		public override int read()
		{
			lock (this)
			{
				if (pos >= count)
				{
					fill();
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
		/// <see cref="java.io.InputStream.read(byte[], int, int)">read</see>
		/// </code> method of
		/// the <code>
		/// <see cref="java.io.InputStream"/>
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
		public override int read(byte[] b, int off, int len)
		{
			lock (this)
			{
				// parameter check
				if ((off | len | (off + len) | (b.Length - (off + len))) < 0)
				{
					throw new System.IndexOutOfRangeException();
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
					int nread = read1(b, off + n, len - n);
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
		private void fill()
		{
			System.Diagnostics.Debug.Assert((pos >= count));
			// fill internal buffer
			count = readChecksumChunk(buf, 0, maxChunkSize);
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
		private int read1(byte[] b, int off, int len)
		{
			int avail = count - pos;
			if (avail <= 0)
			{
				if (len >= maxChunkSize)
				{
					// read a chunk to user buffer directly; avoid one copy
					int nread = readChecksumChunk(b, off, len);
					return nread;
				}
				else
				{
					// read a chunk into the local buffer
					fill();
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
		private int readChecksumChunk(byte[] b, int off, int len)
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
					read = readChunk(chunkPos, b, off, len, checksum);
					if (read > 0)
					{
						if (needChecksum())
						{
							verifySums(b, off, read);
						}
						chunkPos += read;
					}
					retry = false;
				}
				catch (org.apache.hadoop.fs.ChecksumException ce)
				{
					LOG.info("Found checksum error: b[" + off + ", " + (off + read) + "]=" + org.apache.hadoop.util.StringUtils
						.byteToHexString(b, off, off + read), ce);
					if (retriesLeft == 0)
					{
						throw;
					}
					// try a new replica
					if (seekToNewSource(chunkPos))
					{
						// Since at least one of the sources is different, 
						// the read might succeed, so we'll retry.
						seek(chunkPos);
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

		/// <exception cref="org.apache.hadoop.fs.ChecksumException"/>
		private void verifySums(byte[] b, int off, int read)
		{
			int leftToVerify = read;
			int verifyOff = 0;
			checksumInts.rewind();
			checksumInts.limit((read - 1) / maxChunkSize + 1);
			while (leftToVerify > 0)
			{
				sum.update(b, off + verifyOff, System.Math.min(leftToVerify, maxChunkSize));
				int expected = checksumInts.get();
				int calculated = (int)sum.getValue();
				sum.reset();
				if (expected != calculated)
				{
					long errPos = chunkPos + verifyOff;
					throw new org.apache.hadoop.fs.ChecksumException("Checksum error: " + file + " at "
						 + errPos + " exp: " + expected + " got: " + calculated, errPos);
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
		[System.Obsolete]
		public static long checksum2long(byte[] checksum)
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
		public override long getPos()
		{
			lock (this)
			{
				return chunkPos - System.Math.max(0L, count - pos);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int available()
		{
			lock (this)
			{
				return System.Math.max(0, count - pos);
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
		public override long skip(long n)
		{
			lock (this)
			{
				if (n <= 0)
				{
					return 0;
				}
				seek(getPos() + n);
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
		public override void seek(long pos)
		{
			lock (this)
			{
				if (pos < 0)
				{
					throw new java.io.EOFException(org.apache.hadoop.fs.FSExceptionMessages.NEGATIVE_SEEK
						);
				}
				// optimize: check if the pos is in the buffer
				long start = chunkPos - this.count;
				if (pos >= start && pos < chunkPos)
				{
					this.pos = (int)(pos - start);
					return;
				}
				// reset the current state
				resetState();
				// seek to a checksum boundary
				chunkPos = getChunkPosition(pos);
				// scan to the desired position
				int delta = (int)(pos - chunkPos);
				if (delta > 0)
				{
					readFully(this, new byte[delta], 0, delta);
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
		protected internal static int readFully(java.io.InputStream stm, byte[] buf, int 
			offset, int len)
		{
			int n = 0;
			for (; ; )
			{
				int nread = stm.read(buf, offset + n, len - n);
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
		protected internal void set(bool verifyChecksum, java.util.zip.Checksum sum, int 
			maxChunkSize, int checksumSize)
		{
			lock (this)
			{
				// The code makes assumptions that checksums are always 32-bit.
				System.Diagnostics.Debug.Assert(!verifyChecksum || sum == null || checksumSize ==
					 CHECKSUM_SIZE);
				this.maxChunkSize = maxChunkSize;
				this.verifyChecksum = verifyChecksum;
				this.sum = sum;
				this.buf = new byte[maxChunkSize];
				// The size of the checksum array here determines how much we can
				// read in a single call to readChunk
				this.checksum = new byte[CHUNKS_PER_READ * checksumSize];
				this.checksumInts = java.nio.ByteBuffer.wrap(checksum).asIntBuffer();
				this.count = 0;
				this.pos = 0;
			}
		}

		public sealed override bool markSupported()
		{
			return false;
		}

		public sealed override void mark(int readlimit)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public sealed override void reset()
		{
			throw new System.IO.IOException("mark/reset not supported");
		}

		/* reset this FSInputChecker's state */
		private void resetState()
		{
			// invalidate buffer
			count = 0;
			pos = 0;
			// reset Checksum
			if (sum != null)
			{
				sum.reset();
			}
		}
	}
}
