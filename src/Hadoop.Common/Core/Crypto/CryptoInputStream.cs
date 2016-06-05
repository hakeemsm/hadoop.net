using System;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.Crypto
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
	public class CryptoInputStream : FilterInputStream, Seekable, PositionedReadable, 
		ByteBufferReadable, HasFileDescriptor, CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess
		, ReadableByteChannel
	{
		private readonly byte[] oneByteBuf = new byte[1];

		private readonly CryptoCodec codec;

		private readonly Decryptor decryptor;

		private readonly int bufferSize;

		/// <summary>Input data buffer.</summary>
		/// <remarks>
		/// Input data buffer. The data starts at inBuffer.position() and ends at
		/// to inBuffer.limit().
		/// </remarks>
		private ByteBuffer inBuffer;

		/// <summary>The decrypted data buffer.</summary>
		/// <remarks>
		/// The decrypted data buffer. The data starts at outBuffer.position() and
		/// ends at outBuffer.limit();
		/// </remarks>
		private ByteBuffer outBuffer;

		private long streamOffset = 0;

		/// <summary>
		/// Whether the underlying stream supports
		/// <see cref="Org.Apache.Hadoop.FS.ByteBufferReadable"/>
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
		private readonly Queue<ByteBuffer> bufferPool = new ConcurrentLinkedQueue<ByteBuffer
			>();

		/// <summary>Decryptor pool</summary>
		private readonly Queue<Decryptor> decryptorPool = new ConcurrentLinkedQueue<Decryptor
			>();

		/// <exception cref="System.IO.IOException"/>
		public CryptoInputStream(InputStream @in, CryptoCodec codec, int bufferSize, byte
			[] key, byte[] iv)
			: this(@in, codec, bufferSize, key, iv, CryptoStreamUtils.GetInputStreamOffset(@in
				))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoInputStream(InputStream @in, CryptoCodec codec, int bufferSize, byte
			[] key, byte[] iv, long streamOffset)
			: base(@in)
		{
			// Underlying stream offset.
			CryptoStreamUtils.CheckCodec(codec);
			this.bufferSize = CryptoStreamUtils.CheckBufferSize(codec, bufferSize);
			this.codec = codec;
			this.key = key.MemberwiseClone();
			this.initIV = iv.MemberwiseClone();
			this.iv = iv.MemberwiseClone();
			this.streamOffset = streamOffset;
			isByteBufferReadable = @in is ByteBufferReadable;
			isReadableByteChannel = @in is ReadableByteChannel;
			inBuffer = ByteBuffer.AllocateDirect(this.bufferSize);
			outBuffer = ByteBuffer.AllocateDirect(this.bufferSize);
			decryptor = GetDecryptor();
			ResetStreamOffset(streamOffset);
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoInputStream(InputStream @in, CryptoCodec codec, byte[] key, byte[] iv
			)
			: this(@in, codec, CryptoStreamUtils.GetBufferSize(codec.GetConf()), key, iv)
		{
		}

		public virtual InputStream GetWrappedStream()
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
		public override int Read(byte[] b, int off, int len)
		{
			CheckStream();
			if (b == null)
			{
				throw new ArgumentNullException();
			}
			else
			{
				if (off < 0 || len < 0 || len > b.Length - off)
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
			}
			int remaining = outBuffer.Remaining();
			if (remaining > 0)
			{
				int n = Math.Min(len, remaining);
				outBuffer.Get(b, off, n);
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
							n = isByteBufferReadable ? ((ByteBufferReadable)@in).Read(inBuffer) : ((ReadableByteChannel
								)@in).Read(inBuffer);
							usingByteBufferRead = true;
						}
						catch (NotSupportedException)
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
						n = ReadFromUnderlyingStream(inBuffer);
					}
				}
				else
				{
					if (usingByteBufferRead)
					{
						n = isByteBufferReadable ? ((ByteBufferReadable)@in).Read(inBuffer) : ((ReadableByteChannel
							)@in).Read(inBuffer);
					}
					else
					{
						n = ReadFromUnderlyingStream(inBuffer);
					}
				}
				if (n <= 0)
				{
					return n;
				}
				streamOffset += n;
				// Read n bytes
				Decrypt(decryptor, inBuffer, outBuffer, padding);
				padding = AfterDecryption(decryptor, inBuffer, streamOffset, iv);
				n = Math.Min(len, outBuffer.Remaining());
				outBuffer.Get(b, off, n);
				return n;
			}
		}

		/// <summary>Read data from underlying stream.</summary>
		/// <exception cref="System.IO.IOException"/>
		private int ReadFromUnderlyingStream(ByteBuffer inBuffer)
		{
			int toRead = inBuffer.Remaining();
			byte[] tmp = GetTmpBuf();
			int n = @in.Read(tmp, 0, toRead);
			if (n > 0)
			{
				inBuffer.Put(tmp, 0, n);
			}
			return n;
		}

		private byte[] tmpBuf;

		private byte[] GetTmpBuf()
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
		private void Decrypt(Decryptor decryptor, ByteBuffer inBuffer, ByteBuffer outBuffer
			, byte padding)
		{
			Preconditions.CheckState(inBuffer.Position() >= padding);
			if (inBuffer.Position() == padding)
			{
				// There is no real data in inBuffer.
				return;
			}
			inBuffer.Flip();
			outBuffer.Clear();
			decryptor.Decrypt(inBuffer, outBuffer);
			inBuffer.Clear();
			outBuffer.Flip();
			if (padding > 0)
			{
				/*
				* The plain text and cipher text have a 1:1 mapping, they start at the
				* same position.
				*/
				outBuffer.Position(padding);
			}
		}

		/// <summary>This method is executed immediately after decryption.</summary>
		/// <remarks>
		/// This method is executed immediately after decryption. Check whether
		/// decryptor should be updated and recalculate padding if needed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private byte AfterDecryption(Decryptor decryptor, ByteBuffer inBuffer, long position
			, byte[] iv)
		{
			byte padding = 0;
			if (decryptor.IsContextReset())
			{
				/*
				* This code is generally not executed since the decryptor usually
				* maintains decryption context (e.g. the counter) internally. However,
				* some implementations can't maintain context so a re-init is necessary
				* after each decryption call.
				*/
				UpdateDecryptor(decryptor, position, iv);
				padding = GetPadding(position);
				inBuffer.Position(padding);
			}
			return padding;
		}

		private long GetCounter(long position)
		{
			return position / codec.GetCipherSuite().GetAlgorithmBlockSize();
		}

		private byte GetPadding(long position)
		{
			return unchecked((byte)(position % codec.GetCipherSuite().GetAlgorithmBlockSize()
				));
		}

		/// <summary>Calculate the counter and iv, update the decryptor.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void UpdateDecryptor(Decryptor decryptor, long position, byte[] iv)
		{
			long counter = GetCounter(position);
			codec.CalculateIV(initIV, counter, iv);
			decryptor.Init(key, iv);
		}

		/// <summary>
		/// Reset the underlying stream offset; clear
		/// <see cref="inBuffer"/>
		/// and
		/// <see cref="outBuffer"/>
		/// . This Typically happens during
		/// <see cref="Seek(long)"/>
		/// 
		/// or
		/// <see cref="Skip(long)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void ResetStreamOffset(long offset)
		{
			streamOffset = offset;
			inBuffer.Clear();
			outBuffer.Clear();
			outBuffer.Limit(0);
			UpdateDecryptor(decryptor, offset, iv);
			padding = GetPadding(offset);
			inBuffer.Position(padding);
		}

		// Set proper position for input data.
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (closed)
			{
				return;
			}
			base.Close();
			FreeBuffers();
			closed = true;
		}

		/// <summary>Positioned read.</summary>
		/// <remarks>Positioned read. It is thread-safe</remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(long position, byte[] buffer, int offset, int length)
		{
			CheckStream();
			try
			{
				int n = ((PositionedReadable)@in).Read(position, buffer, offset, length);
				if (n > 0)
				{
					// This operation does not change the current offset of the file
					Decrypt(position, buffer, offset, n);
				}
				return n;
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("This stream does not support " + "positioned read."
					);
			}
		}

		/// <summary>Decrypt length bytes in buffer starting at offset.</summary>
		/// <remarks>
		/// Decrypt length bytes in buffer starting at offset. Output is also put
		/// into buffer starting at offset. It is thread-safe.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void Decrypt(long position, byte[] buffer, int offset, int length)
		{
			ByteBuffer inBuffer = GetBuffer();
			ByteBuffer outBuffer = GetBuffer();
			Decryptor decryptor = null;
			try
			{
				decryptor = GetDecryptor();
				byte[] iv = initIV.MemberwiseClone();
				UpdateDecryptor(decryptor, position, iv);
				byte padding = GetPadding(position);
				inBuffer.Position(padding);
				// Set proper position for input data.
				int n = 0;
				while (n < length)
				{
					int toDecrypt = Math.Min(length - n, inBuffer.Remaining());
					inBuffer.Put(buffer, offset + n, toDecrypt);
					// Do decryption
					Decrypt(decryptor, inBuffer, outBuffer, padding);
					outBuffer.Get(buffer, offset + n, toDecrypt);
					n += toDecrypt;
					padding = AfterDecryption(decryptor, inBuffer, position + n, iv);
				}
			}
			finally
			{
				ReturnBuffer(inBuffer);
				ReturnBuffer(outBuffer);
				ReturnDecryptor(decryptor);
			}
		}

		/// <summary>Positioned read fully.</summary>
		/// <remarks>Positioned read fully. It is thread-safe</remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(long position, byte[] buffer, int offset, int length
			)
		{
			CheckStream();
			try
			{
				((PositionedReadable)@in).ReadFully(position, buffer, offset, length);
				if (length > 0)
				{
					// This operation does not change the current offset of the file
					Decrypt(position, buffer, offset, length);
				}
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("This stream does not support " + "positioned readFully."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(long position, byte[] buffer)
		{
			ReadFully(position, buffer, 0, buffer.Length);
		}

		/// <summary>Seek to a position.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Seek(long pos)
		{
			Preconditions.CheckArgument(pos >= 0, "Cannot seek to negative offset.");
			CheckStream();
			try
			{
				/*
				* If data of target pos in the underlying stream has already been read
				* and decrypted in outBuffer, we just need to re-position outBuffer.
				*/
				if (pos <= streamOffset && pos >= (streamOffset - outBuffer.Remaining()))
				{
					int forward = (int)(pos - (streamOffset - outBuffer.Remaining()));
					if (forward > 0)
					{
						outBuffer.Position(outBuffer.Position() + forward);
					}
				}
				else
				{
					((Seekable)@in).Seek(pos);
					ResetStreamOffset(pos);
				}
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("This stream does not support " + "seek.");
			}
		}

		/// <summary>Skip n bytes</summary>
		/// <exception cref="System.IO.IOException"/>
		public override long Skip(long n)
		{
			Preconditions.CheckArgument(n >= 0, "Negative skip length.");
			CheckStream();
			if (n == 0)
			{
				return 0;
			}
			else
			{
				if (n <= outBuffer.Remaining())
				{
					int pos = outBuffer.Position() + (int)n;
					outBuffer.Position(pos);
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
					n -= outBuffer.Remaining();
					long skipped = @in.Skip(n);
					if (skipped < 0)
					{
						skipped = 0;
					}
					long pos = streamOffset + skipped;
					skipped += outBuffer.Remaining();
					ResetStreamOffset(pos);
					return skipped;
				}
			}
		}

		/// <summary>Get underlying stream position.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			CheckStream();
			// Equals: ((Seekable) in).getPos() - outBuffer.remaining()
			return streamOffset - outBuffer.Remaining();
		}

		/// <summary>ByteBuffer read.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(ByteBuffer buf)
		{
			CheckStream();
			if (isByteBufferReadable || isReadableByteChannel)
			{
				int unread = outBuffer.Remaining();
				if (unread > 0)
				{
					// Have unread decrypted data in buffer.
					int toRead = buf.Remaining();
					if (toRead <= unread)
					{
						int limit = outBuffer.Limit();
						outBuffer.Limit(outBuffer.Position() + toRead);
						buf.Put(outBuffer);
						outBuffer.Limit(limit);
						return toRead;
					}
					else
					{
						buf.Put(outBuffer);
					}
				}
				int pos = buf.Position();
				int n = isByteBufferReadable ? ((ByteBufferReadable)@in).Read(buf) : ((ReadableByteChannel
					)@in).Read(buf);
				if (n > 0)
				{
					streamOffset += n;
					// Read n bytes
					Decrypt(buf, n, pos);
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
				if (buf.HasArray())
				{
					n = Read(((byte[])buf.Array()), buf.Position(), buf.Remaining());
					if (n > 0)
					{
						buf.Position(buf.Position() + n);
					}
				}
				else
				{
					byte[] tmp = new byte[buf.Remaining()];
					n = Read(tmp);
					if (n > 0)
					{
						buf.Put(tmp, 0, n);
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
		private void Decrypt(ByteBuffer buf, int n, int start)
		{
			int pos = buf.Position();
			int limit = buf.Limit();
			int len = 0;
			while (len < n)
			{
				buf.Position(start + len);
				buf.Limit(start + len + Math.Min(n - len, inBuffer.Remaining()));
				inBuffer.Put(buf);
				// Do decryption
				try
				{
					Decrypt(decryptor, inBuffer, outBuffer, padding);
					buf.Position(start + len);
					buf.Limit(limit);
					len += outBuffer.Remaining();
					buf.Put(outBuffer);
				}
				finally
				{
					padding = AfterDecryption(decryptor, inBuffer, streamOffset - (n - len), iv);
				}
			}
			buf.Position(pos);
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Available()
		{
			CheckStream();
			return @in.Available() + outBuffer.Remaining();
		}

		public override bool MarkSupported()
		{
			return false;
		}

		public override void Mark(int readLimit)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Reset()
		{
			throw new IOException("Mark/reset not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool SeekToNewSource(long targetPos)
		{
			Preconditions.CheckArgument(targetPos >= 0, "Cannot seek to negative offset.");
			CheckStream();
			try
			{
				bool result = ((Seekable)@in).SeekToNewSource(targetPos);
				ResetStreamOffset(targetPos);
				return result;
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("This stream does not support " + "seekToNewSource."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual ByteBuffer Read(ByteBufferPool bufferPool, int maxLength, EnumSet<
			ReadOption> opts)
		{
			CheckStream();
			try
			{
				if (outBuffer.Remaining() > 0)
				{
					// Have some decrypted data unread, need to reset.
					((Seekable)@in).Seek(GetPos());
					ResetStreamOffset(GetPos());
				}
				ByteBuffer buffer = ((HasEnhancedByteBufferAccess)@in).Read(bufferPool, maxLength
					, opts);
				if (buffer != null)
				{
					int n = buffer.Remaining();
					if (n > 0)
					{
						streamOffset += buffer.Remaining();
						// Read n bytes
						int pos = buffer.Position();
						Decrypt(buffer, n, pos);
					}
				}
				return buffer;
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("This stream does not support " + "enhanced byte buffer access."
					);
			}
		}

		public virtual void ReleaseBuffer(ByteBuffer buffer)
		{
			try
			{
				((HasEnhancedByteBufferAccess)@in).ReleaseBuffer(buffer);
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("This stream does not support " + "release buffer."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void SetReadahead(long readahead)
		{
			try
			{
				((CanSetReadahead)@in).SetReadahead(readahead);
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("This stream does not support " + "setting the readahead caching strategy."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void SetDropBehind(bool dropCache)
		{
			try
			{
				((CanSetDropBehind)@in).SetDropBehind(dropCache);
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("This stream does not " + "support setting the drop-behind caching setting."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual FileDescriptor GetFileDescriptor()
		{
			if (@in is HasFileDescriptor)
			{
				return ((HasFileDescriptor)@in).GetFileDescriptor();
			}
			else
			{
				if (@in is FileInputStream)
				{
					return ((FileInputStream)@in).GetFD();
				}
				else
				{
					return null;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			return (Read(oneByteBuf, 0, 1) == -1) ? -1 : (oneByteBuf[0] & unchecked((int)(0xff
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckStream()
		{
			if (closed)
			{
				throw new IOException("Stream closed");
			}
		}

		/// <summary>Get direct buffer from pool</summary>
		private ByteBuffer GetBuffer()
		{
			ByteBuffer buffer = bufferPool.Poll();
			if (buffer == null)
			{
				buffer = ByteBuffer.AllocateDirect(bufferSize);
			}
			return buffer;
		}

		/// <summary>Return direct buffer to pool</summary>
		private void ReturnBuffer(ByteBuffer buf)
		{
			if (buf != null)
			{
				buf.Clear();
				bufferPool.AddItem(buf);
			}
		}

		/// <summary>Forcibly free the direct buffers.</summary>
		private void FreeBuffers()
		{
			CryptoStreamUtils.FreeDB(inBuffer);
			CryptoStreamUtils.FreeDB(outBuffer);
			CleanBufferPool();
		}

		/// <summary>Clean direct buffer pool</summary>
		private void CleanBufferPool()
		{
			ByteBuffer buf;
			while ((buf = bufferPool.Poll()) != null)
			{
				CryptoStreamUtils.FreeDB(buf);
			}
		}

		/// <summary>Get decryptor from pool</summary>
		/// <exception cref="System.IO.IOException"/>
		private Decryptor GetDecryptor()
		{
			Decryptor decryptor = decryptorPool.Poll();
			if (decryptor == null)
			{
				try
				{
					decryptor = codec.CreateDecryptor();
				}
				catch (GeneralSecurityException e)
				{
					throw new IOException(e);
				}
			}
			return decryptor;
		}

		/// <summary>Return decryptor to pool</summary>
		private void ReturnDecryptor(Decryptor decryptor)
		{
			if (decryptor != null)
			{
				decryptorPool.AddItem(decryptor);
			}
		}

		public virtual bool IsOpen()
		{
			return !closed;
		}
	}
}
