using Sharpen;

namespace org.apache.hadoop.crypto
{
	/// <summary>CryptoInputStream decrypts data.</summary>
	/// <remarks>
	/// CryptoInputStream decrypts data. It is not thread-safe. AES CTR mode is
	/// required in order to ensure that the plain text and cipher text have a 1:1
	/// mapping. The decryption is buffer based. The key points of the decryption
	/// are (1) calculating the counter and (2) padding through stream position:
	/// <p/>
	/// counter = base + pos/(algorithm blocksize);
	/// padding = pos%(algorithm blocksize);
	/// <p/>
	/// The underlying stream offset is maintained as state.
	/// </remarks>
	public class CryptoInputStream : java.io.FilterInputStream, org.apache.hadoop.fs.Seekable
		, org.apache.hadoop.fs.PositionedReadable, org.apache.hadoop.fs.ByteBufferReadable
		, org.apache.hadoop.fs.HasFileDescriptor, org.apache.hadoop.fs.CanSetDropBehind, 
		org.apache.hadoop.fs.CanSetReadahead, org.apache.hadoop.fs.HasEnhancedByteBufferAccess
		, java.nio.channels.ReadableByteChannel
	{
		private readonly byte[] oneByteBuf = new byte[1];

		private readonly org.apache.hadoop.crypto.CryptoCodec codec;

		private readonly org.apache.hadoop.crypto.Decryptor decryptor;

		private readonly int bufferSize;

		/// <summary>Input data buffer.</summary>
		/// <remarks>
		/// Input data buffer. The data starts at inBuffer.position() and ends at
		/// to inBuffer.limit().
		/// </remarks>
		private java.nio.ByteBuffer inBuffer;

		/// <summary>The decrypted data buffer.</summary>
		/// <remarks>
		/// The decrypted data buffer. The data starts at outBuffer.position() and
		/// ends at outBuffer.limit();
		/// </remarks>
		private java.nio.ByteBuffer outBuffer;

		private long streamOffset = 0;

		/// <summary>
		/// Whether the underlying stream supports
		/// <see cref="org.apache.hadoop.fs.ByteBufferReadable"/>
		/// </summary>
		private bool usingByteBufferRead = null;

		/// <summary>
		/// Padding = pos%(algorithm blocksize); Padding is put into
		/// <see cref="inBuffer"/>
		/// 
		/// before any other data goes in. The purpose of padding is to put the input
		/// data at proper position.
		/// </summary>
		private byte padding;

		private bool closed;

		private readonly byte[] key;

		private readonly byte[] initIV;

		private byte[] iv;

		private readonly bool isByteBufferReadable;

		private readonly bool isReadableByteChannel;

		/// <summary>DirectBuffer pool</summary>
		private readonly java.util.Queue<java.nio.ByteBuffer> bufferPool = new java.util.concurrent.ConcurrentLinkedQueue
			<java.nio.ByteBuffer>();

		/// <summary>Decryptor pool</summary>
		private readonly java.util.Queue<org.apache.hadoop.crypto.Decryptor> decryptorPool
			 = new java.util.concurrent.ConcurrentLinkedQueue<org.apache.hadoop.crypto.Decryptor
			>();

		/// <exception cref="System.IO.IOException"/>
		public CryptoInputStream(java.io.InputStream @in, org.apache.hadoop.crypto.CryptoCodec
			 codec, int bufferSize, byte[] key, byte[] iv)
			: this(@in, codec, bufferSize, key, iv, org.apache.hadoop.crypto.CryptoStreamUtils
				.getInputStreamOffset(@in))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoInputStream(java.io.InputStream @in, org.apache.hadoop.crypto.CryptoCodec
			 codec, int bufferSize, byte[] key, byte[] iv, long streamOffset)
			: base(@in)
		{
			// Underlying stream offset.
			org.apache.hadoop.crypto.CryptoStreamUtils.checkCodec(codec);
			this.bufferSize = org.apache.hadoop.crypto.CryptoStreamUtils.checkBufferSize(codec
				, bufferSize);
			this.codec = codec;
			this.key = key.MemberwiseClone();
			this.initIV = iv.MemberwiseClone();
			this.iv = iv.MemberwiseClone();
			this.streamOffset = streamOffset;
			isByteBufferReadable = @in is org.apache.hadoop.fs.ByteBufferReadable;
			isReadableByteChannel = @in is java.nio.channels.ReadableByteChannel;
			inBuffer = java.nio.ByteBuffer.allocateDirect(this.bufferSize);
			outBuffer = java.nio.ByteBuffer.allocateDirect(this.bufferSize);
			decryptor = getDecryptor();
			resetStreamOffset(streamOffset);
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoInputStream(java.io.InputStream @in, org.apache.hadoop.crypto.CryptoCodec
			 codec, byte[] key, byte[] iv)
			: this(@in, codec, org.apache.hadoop.crypto.CryptoStreamUtils.getBufferSize(codec
				.getConf()), key, iv)
		{
		}

		public virtual java.io.InputStream getWrappedStream()
		{
			return @in;
		}

		/// <summary>Decryption is buffer based.</summary>
		/// <remarks>
		/// Decryption is buffer based.
		/// If there is data in
		/// <see cref="outBuffer"/>
		/// , then read it out of this buffer.
		/// If there is no data in
		/// <see cref="outBuffer"/>
		/// , then read more from the
		/// underlying stream and do the decryption.
		/// </remarks>
		/// <param name="b">the buffer into which the decrypted data is read.</param>
		/// <param name="off">the buffer offset.</param>
		/// <param name="len">the maximum number of decrypted data bytes to read.</param>
		/// <returns>int the total number of decrypted data bytes read into the buffer.</returns>
		/// <exception cref="System.IO.IOException"/>
		public override int read(byte[] b, int off, int len)
		{
			checkStream();
			if (b == null)
			{
				throw new System.ArgumentNullException();
			}
			else
			{
				if (off < 0 || len < 0 || len > b.Length - off)
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
			}
			int remaining = outBuffer.remaining();
			if (remaining > 0)
			{
				int n = System.Math.min(len, remaining);
				outBuffer.get(b, off, n);
				return n;
			}
			else
			{
				int n = 0;
				/*
				* Check whether the underlying stream is {@link ByteBufferReadable},
				* it can avoid bytes copy.
				*/
				if (usingByteBufferRead == null)
				{
					if (isByteBufferReadable || isReadableByteChannel)
					{
						try
						{
							n = isByteBufferReadable ? ((org.apache.hadoop.fs.ByteBufferReadable)@in).read(inBuffer
								) : ((java.nio.channels.ReadableByteChannel)@in).read(inBuffer);
							usingByteBufferRead = true;
						}
						catch (System.NotSupportedException)
						{
							usingByteBufferRead = false;
						}
					}
					else
					{
						usingByteBufferRead = false;
					}
					if (!usingByteBufferRead)
					{
						n = readFromUnderlyingStream(inBuffer);
					}
				}
				else
				{
					if (usingByteBufferRead)
					{
						n = isByteBufferReadable ? ((org.apache.hadoop.fs.ByteBufferReadable)@in).read(inBuffer
							) : ((java.nio.channels.ReadableByteChannel)@in).read(inBuffer);
					}
					else
					{
						n = readFromUnderlyingStream(inBuffer);
					}
				}
				if (n <= 0)
				{
					return n;
				}
				streamOffset += n;
				// Read n bytes
				decrypt(decryptor, inBuffer, outBuffer, padding);
				padding = afterDecryption(decryptor, inBuffer, streamOffset, iv);
				n = System.Math.min(len, outBuffer.remaining());
				outBuffer.get(b, off, n);
				return n;
			}
		}

		/// <summary>Read data from underlying stream.</summary>
		/// <exception cref="System.IO.IOException"/>
		private int readFromUnderlyingStream(java.nio.ByteBuffer inBuffer)
		{
			int toRead = inBuffer.remaining();
			byte[] tmp = getTmpBuf();
			int n = @in.read(tmp, 0, toRead);
			if (n > 0)
			{
				inBuffer.put(tmp, 0, n);
			}
			return n;
		}

		private byte[] tmpBuf;

		private byte[] getTmpBuf()
		{
			if (tmpBuf == null)
			{
				tmpBuf = new byte[bufferSize];
			}
			return tmpBuf;
		}

		/// <summary>Do the decryption using inBuffer as input and outBuffer as output.</summary>
		/// <remarks>
		/// Do the decryption using inBuffer as input and outBuffer as output.
		/// Upon return, inBuffer is cleared; the decrypted data starts at
		/// outBuffer.position() and ends at outBuffer.limit();
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void decrypt(org.apache.hadoop.crypto.Decryptor decryptor, java.nio.ByteBuffer
			 inBuffer, java.nio.ByteBuffer outBuffer, byte padding)
		{
			com.google.common.@base.Preconditions.checkState(inBuffer.position() >= padding);
			if (inBuffer.position() == padding)
			{
				// There is no real data in inBuffer.
				return;
			}
			inBuffer.flip();
			outBuffer.clear();
			decryptor.decrypt(inBuffer, outBuffer);
			inBuffer.clear();
			outBuffer.flip();
			if (padding > 0)
			{
				/*
				* The plain text and cipher text have a 1:1 mapping, they start at the
				* same position.
				*/
				outBuffer.position(padding);
			}
		}

		/// <summary>This method is executed immediately after decryption.</summary>
		/// <remarks>
		/// This method is executed immediately after decryption. Check whether
		/// decryptor should be updated and recalculate padding if needed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private byte afterDecryption(org.apache.hadoop.crypto.Decryptor decryptor, java.nio.ByteBuffer
			 inBuffer, long position, byte[] iv)
		{
			byte padding = 0;
			if (decryptor.isContextReset())
			{
				/*
				* This code is generally not executed since the decryptor usually
				* maintains decryption context (e.g. the counter) internally. However,
				* some implementations can't maintain context so a re-init is necessary
				* after each decryption call.
				*/
				updateDecryptor(decryptor, position, iv);
				padding = getPadding(position);
				inBuffer.position(padding);
			}
			return padding;
		}

		private long getCounter(long position)
		{
			return position / codec.getCipherSuite().getAlgorithmBlockSize();
		}

		private byte getPadding(long position)
		{
			return unchecked((byte)(position % codec.getCipherSuite().getAlgorithmBlockSize()
				));
		}

		/// <summary>Calculate the counter and iv, update the decryptor.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void updateDecryptor(org.apache.hadoop.crypto.Decryptor decryptor, long position
			, byte[] iv)
		{
			long counter = getCounter(position);
			codec.calculateIV(initIV, counter, iv);
			decryptor.init(key, iv);
		}

		/// <summary>
		/// Reset the underlying stream offset; clear
		/// <see cref="inBuffer"/>
		/// and
		/// <see cref="outBuffer"/>
		/// . This Typically happens during
		/// <see cref="seek(long)"/>
		/// 
		/// or
		/// <see cref="skip(long)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void resetStreamOffset(long offset)
		{
			streamOffset = offset;
			inBuffer.clear();
			outBuffer.clear();
			outBuffer.limit(0);
			updateDecryptor(decryptor, offset, iv);
			padding = getPadding(offset);
			inBuffer.position(padding);
		}

		// Set proper position for input data.
		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			if (closed)
			{
				return;
			}
			base.close();
			freeBuffers();
			closed = true;
		}

		/// <summary>Positioned read.</summary>
		/// <remarks>Positioned read. It is thread-safe</remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual int read(long position, byte[] buffer, int offset, int length)
		{
			checkStream();
			try
			{
				int n = ((org.apache.hadoop.fs.PositionedReadable)@in).read(position, buffer, offset
					, length);
				if (n > 0)
				{
					// This operation does not change the current offset of the file
					decrypt(position, buffer, offset, n);
				}
				return n;
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("This stream does not support " + "positioned read."
					);
			}
		}

		/// <summary>Decrypt length bytes in buffer starting at offset.</summary>
		/// <remarks>
		/// Decrypt length bytes in buffer starting at offset. Output is also put
		/// into buffer starting at offset. It is thread-safe.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void decrypt(long position, byte[] buffer, int offset, int length)
		{
			java.nio.ByteBuffer inBuffer = getBuffer();
			java.nio.ByteBuffer outBuffer = getBuffer();
			org.apache.hadoop.crypto.Decryptor decryptor = null;
			try
			{
				decryptor = getDecryptor();
				byte[] iv = initIV.MemberwiseClone();
				updateDecryptor(decryptor, position, iv);
				byte padding = getPadding(position);
				inBuffer.position(padding);
				// Set proper position for input data.
				int n = 0;
				while (n < length)
				{
					int toDecrypt = System.Math.min(length - n, inBuffer.remaining());
					inBuffer.put(buffer, offset + n, toDecrypt);
					// Do decryption
					decrypt(decryptor, inBuffer, outBuffer, padding);
					outBuffer.get(buffer, offset + n, toDecrypt);
					n += toDecrypt;
					padding = afterDecryption(decryptor, inBuffer, position + n, iv);
				}
			}
			finally
			{
				returnBuffer(inBuffer);
				returnBuffer(outBuffer);
				returnDecryptor(decryptor);
			}
		}

		/// <summary>Positioned read fully.</summary>
		/// <remarks>Positioned read fully. It is thread-safe</remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFully(long position, byte[] buffer, int offset, int length
			)
		{
			checkStream();
			try
			{
				((org.apache.hadoop.fs.PositionedReadable)@in).readFully(position, buffer, offset
					, length);
				if (length > 0)
				{
					// This operation does not change the current offset of the file
					decrypt(position, buffer, offset, length);
				}
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("This stream does not support " + "positioned readFully."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFully(long position, byte[] buffer)
		{
			readFully(position, buffer, 0, buffer.Length);
		}

		/// <summary>Seek to a position.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void seek(long pos)
		{
			com.google.common.@base.Preconditions.checkArgument(pos >= 0, "Cannot seek to negative offset."
				);
			checkStream();
			try
			{
				/*
				* If data of target pos in the underlying stream has already been read
				* and decrypted in outBuffer, we just need to re-position outBuffer.
				*/
				if (pos <= streamOffset && pos >= (streamOffset - outBuffer.remaining()))
				{
					int forward = (int)(pos - (streamOffset - outBuffer.remaining()));
					if (forward > 0)
					{
						outBuffer.position(outBuffer.position() + forward);
					}
				}
				else
				{
					((org.apache.hadoop.fs.Seekable)@in).seek(pos);
					resetStreamOffset(pos);
				}
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("This stream does not support " + "seek.");
			}
		}

		/// <summary>Skip n bytes</summary>
		/// <exception cref="System.IO.IOException"/>
		public override long skip(long n)
		{
			com.google.common.@base.Preconditions.checkArgument(n >= 0, "Negative skip length."
				);
			checkStream();
			if (n == 0)
			{
				return 0;
			}
			else
			{
				if (n <= outBuffer.remaining())
				{
					int pos = outBuffer.position() + (int)n;
					outBuffer.position(pos);
					return n;
				}
				else
				{
					/*
					* Subtract outBuffer.remaining() to see how many bytes we need to
					* skip in the underlying stream. Add outBuffer.remaining() to the
					* actual number of skipped bytes in the underlying stream to get the
					* number of skipped bytes from the user's point of view.
					*/
					n -= outBuffer.remaining();
					long skipped = @in.skip(n);
					if (skipped < 0)
					{
						skipped = 0;
					}
					long pos = streamOffset + skipped;
					skipped += outBuffer.remaining();
					resetStreamOffset(pos);
					return skipped;
				}
			}
		}

		/// <summary>Get underlying stream position.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long getPos()
		{
			checkStream();
			// Equals: ((Seekable) in).getPos() - outBuffer.remaining()
			return streamOffset - outBuffer.remaining();
		}

		/// <summary>ByteBuffer read.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual int read(java.nio.ByteBuffer buf)
		{
			checkStream();
			if (isByteBufferReadable || isReadableByteChannel)
			{
				int unread = outBuffer.remaining();
				if (unread > 0)
				{
					// Have unread decrypted data in buffer.
					int toRead = buf.remaining();
					if (toRead <= unread)
					{
						int limit = outBuffer.limit();
						outBuffer.limit(outBuffer.position() + toRead);
						buf.put(outBuffer);
						outBuffer.limit(limit);
						return toRead;
					}
					else
					{
						buf.put(outBuffer);
					}
				}
				int pos = buf.position();
				int n = isByteBufferReadable ? ((org.apache.hadoop.fs.ByteBufferReadable)@in).read
					(buf) : ((java.nio.channels.ReadableByteChannel)@in).read(buf);
				if (n > 0)
				{
					streamOffset += n;
					// Read n bytes
					decrypt(buf, n, pos);
				}
				if (n >= 0)
				{
					return unread + n;
				}
				else
				{
					if (unread == 0)
					{
						return -1;
					}
					else
					{
						return unread;
					}
				}
			}
			else
			{
				int n = 0;
				if (buf.hasArray())
				{
					n = read(((byte[])buf.array()), buf.position(), buf.remaining());
					if (n > 0)
					{
						buf.position(buf.position() + n);
					}
				}
				else
				{
					byte[] tmp = new byte[buf.remaining()];
					n = read(tmp);
					if (n > 0)
					{
						buf.put(tmp, 0, n);
					}
				}
				return n;
			}
		}

		/// <summary>Decrypt all data in buf: total n bytes from given start position.</summary>
		/// <remarks>
		/// Decrypt all data in buf: total n bytes from given start position.
		/// Output is also buf and same start position.
		/// buf.position() and buf.limit() should be unchanged after decryption.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void decrypt(java.nio.ByteBuffer buf, int n, int start)
		{
			int pos = buf.position();
			int limit = buf.limit();
			int len = 0;
			while (len < n)
			{
				buf.position(start + len);
				buf.limit(start + len + System.Math.min(n - len, inBuffer.remaining()));
				inBuffer.put(buf);
				// Do decryption
				try
				{
					decrypt(decryptor, inBuffer, outBuffer, padding);
					buf.position(start + len);
					buf.limit(limit);
					len += outBuffer.remaining();
					buf.put(outBuffer);
				}
				finally
				{
					padding = afterDecryption(decryptor, inBuffer, streamOffset - (n - len), iv);
				}
			}
			buf.position(pos);
		}

		/// <exception cref="System.IO.IOException"/>
		public override int available()
		{
			checkStream();
			return @in.available() + outBuffer.remaining();
		}

		public override bool markSupported()
		{
			return false;
		}

		public override void mark(int readLimit)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void reset()
		{
			throw new System.IO.IOException("Mark/reset not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool seekToNewSource(long targetPos)
		{
			com.google.common.@base.Preconditions.checkArgument(targetPos >= 0, "Cannot seek to negative offset."
				);
			checkStream();
			try
			{
				bool result = ((org.apache.hadoop.fs.Seekable)@in).seekToNewSource(targetPos);
				resetStreamOffset(targetPos);
				return result;
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("This stream does not support " + "seekToNewSource."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual java.nio.ByteBuffer read(org.apache.hadoop.io.ByteBufferPool bufferPool
			, int maxLength, java.util.EnumSet<org.apache.hadoop.fs.ReadOption> opts)
		{
			checkStream();
			try
			{
				if (outBuffer.remaining() > 0)
				{
					// Have some decrypted data unread, need to reset.
					((org.apache.hadoop.fs.Seekable)@in).seek(getPos());
					resetStreamOffset(getPos());
				}
				java.nio.ByteBuffer buffer = ((org.apache.hadoop.fs.HasEnhancedByteBufferAccess)@in
					).read(bufferPool, maxLength, opts);
				if (buffer != null)
				{
					int n = buffer.remaining();
					if (n > 0)
					{
						streamOffset += buffer.remaining();
						// Read n bytes
						int pos = buffer.position();
						decrypt(buffer, n, pos);
					}
				}
				return buffer;
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("This stream does not support " + "enhanced byte buffer access."
					);
			}
		}

		public virtual void releaseBuffer(java.nio.ByteBuffer buffer)
		{
			try
			{
				((org.apache.hadoop.fs.HasEnhancedByteBufferAccess)@in).releaseBuffer(buffer);
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("This stream does not support " + "release buffer."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void setReadahead(long readahead)
		{
			try
			{
				((org.apache.hadoop.fs.CanSetReadahead)@in).setReadahead(readahead);
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("This stream does not support " + "setting the readahead caching strategy."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void setDropBehind(bool dropCache)
		{
			try
			{
				((org.apache.hadoop.fs.CanSetDropBehind)@in).setDropBehind(dropCache);
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("This stream does not " + "support setting the drop-behind caching setting."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual java.io.FileDescriptor getFileDescriptor()
		{
			if (@in is org.apache.hadoop.fs.HasFileDescriptor)
			{
				return ((org.apache.hadoop.fs.HasFileDescriptor)@in).getFileDescriptor();
			}
			else
			{
				if (@in is java.io.FileInputStream)
				{
					return ((java.io.FileInputStream)@in).getFD();
				}
				else
				{
					return null;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int read()
		{
			return (read(oneByteBuf, 0, 1) == -1) ? -1 : (oneByteBuf[0] & unchecked((int)(0xff
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		private void checkStream()
		{
			if (closed)
			{
				throw new System.IO.IOException("Stream closed");
			}
		}

		/// <summary>Get direct buffer from pool</summary>
		private java.nio.ByteBuffer getBuffer()
		{
			java.nio.ByteBuffer buffer = bufferPool.poll();
			if (buffer == null)
			{
				buffer = java.nio.ByteBuffer.allocateDirect(bufferSize);
			}
			return buffer;
		}

		/// <summary>Return direct buffer to pool</summary>
		private void returnBuffer(java.nio.ByteBuffer buf)
		{
			if (buf != null)
			{
				buf.clear();
				bufferPool.add(buf);
			}
		}

		/// <summary>Forcibly free the direct buffers.</summary>
		private void freeBuffers()
		{
			org.apache.hadoop.crypto.CryptoStreamUtils.freeDB(inBuffer);
			org.apache.hadoop.crypto.CryptoStreamUtils.freeDB(outBuffer);
			cleanBufferPool();
		}

		/// <summary>Clean direct buffer pool</summary>
		private void cleanBufferPool()
		{
			java.nio.ByteBuffer buf;
			while ((buf = bufferPool.poll()) != null)
			{
				org.apache.hadoop.crypto.CryptoStreamUtils.freeDB(buf);
			}
		}

		/// <summary>Get decryptor from pool</summary>
		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.crypto.Decryptor getDecryptor()
		{
			org.apache.hadoop.crypto.Decryptor decryptor = decryptorPool.poll();
			if (decryptor == null)
			{
				try
				{
					decryptor = codec.createDecryptor();
				}
				catch (java.security.GeneralSecurityException e)
				{
					throw new System.IO.IOException(e);
				}
			}
			return decryptor;
		}

		/// <summary>Return decryptor to pool</summary>
		private void returnDecryptor(org.apache.hadoop.crypto.Decryptor decryptor)
		{
			if (decryptor != null)
			{
				decryptorPool.add(decryptor);
			}
		}

		public virtual bool isOpen()
		{
			return !closed;
		}
	}
}
