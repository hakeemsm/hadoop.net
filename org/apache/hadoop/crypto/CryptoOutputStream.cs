using Sharpen;

namespace org.apache.hadoop.crypto
{
	/// <summary>CryptoOutputStream encrypts data.</summary>
	/// <remarks>
	/// CryptoOutputStream encrypts data. It is not thread-safe. AES CTR mode is
	/// required in order to ensure that the plain text and cipher text have a 1:1
	/// mapping. The encryption is buffer based. The key points of the encryption are
	/// (1) calculating counter and (2) padding through stream position.
	/// <p/>
	/// counter = base + pos/(algorithm blocksize);
	/// padding = pos%(algorithm blocksize);
	/// <p/>
	/// The underlying stream offset is maintained as state.
	/// Note that while some of this class' methods are synchronized, this is just to
	/// match the threadsafety behavior of DFSOutputStream. See HADOOP-11710.
	/// </remarks>
	public class CryptoOutputStream : java.io.FilterOutputStream, org.apache.hadoop.fs.Syncable
		, org.apache.hadoop.fs.CanSetDropBehind
	{
		private readonly byte[] oneByteBuf = new byte[1];

		private readonly org.apache.hadoop.crypto.CryptoCodec codec;

		private readonly org.apache.hadoop.crypto.Encryptor encryptor;

		private readonly int bufferSize;

		/// <summary>Input data buffer.</summary>
		/// <remarks>
		/// Input data buffer. The data starts at inBuffer.position() and ends at
		/// inBuffer.limit().
		/// </remarks>
		private java.nio.ByteBuffer inBuffer;

		/// <summary>Encrypted data buffer.</summary>
		/// <remarks>
		/// Encrypted data buffer. The data starts at outBuffer.position() and ends at
		/// outBuffer.limit();
		/// </remarks>
		private java.nio.ByteBuffer outBuffer;

		private long streamOffset = 0;

		/// <summary>
		/// Padding = pos%(algorithm blocksize); Padding is put into
		/// <see cref="inBuffer"/>
		/// 
		/// before any other data goes in. The purpose of padding is to put input data
		/// at proper position.
		/// </summary>
		private byte padding;

		private bool closed;

		private readonly byte[] key;

		private readonly byte[] initIV;

		private byte[] iv;

		/// <exception cref="System.IO.IOException"/>
		public CryptoOutputStream(java.io.OutputStream @out, org.apache.hadoop.crypto.CryptoCodec
			 codec, int bufferSize, byte[] key, byte[] iv)
			: this(@out, codec, bufferSize, key, iv, 0)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoOutputStream(java.io.OutputStream @out, org.apache.hadoop.crypto.CryptoCodec
			 codec, int bufferSize, byte[] key, byte[] iv, long streamOffset)
			: base(@out)
		{
			// Underlying stream offset.
			org.apache.hadoop.crypto.CryptoStreamUtils.checkCodec(codec);
			this.bufferSize = org.apache.hadoop.crypto.CryptoStreamUtils.checkBufferSize(codec
				, bufferSize);
			this.codec = codec;
			this.key = key.MemberwiseClone();
			this.initIV = iv.MemberwiseClone();
			this.iv = iv.MemberwiseClone();
			inBuffer = java.nio.ByteBuffer.allocateDirect(this.bufferSize);
			outBuffer = java.nio.ByteBuffer.allocateDirect(this.bufferSize);
			this.streamOffset = streamOffset;
			try
			{
				encryptor = codec.createEncryptor();
			}
			catch (java.security.GeneralSecurityException e)
			{
				throw new System.IO.IOException(e);
			}
			updateEncryptor();
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoOutputStream(java.io.OutputStream @out, org.apache.hadoop.crypto.CryptoCodec
			 codec, byte[] key, byte[] iv)
			: this(@out, codec, key, iv, 0)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoOutputStream(java.io.OutputStream @out, org.apache.hadoop.crypto.CryptoCodec
			 codec, byte[] key, byte[] iv, long streamOffset)
			: this(@out, codec, org.apache.hadoop.crypto.CryptoStreamUtils.getBufferSize(codec
				.getConf()), key, iv, streamOffset)
		{
		}

		public virtual java.io.OutputStream getWrappedStream()
		{
			return @out;
		}

		/// <summary>Encryption is buffer based.</summary>
		/// <remarks>
		/// Encryption is buffer based.
		/// If there is enough room in
		/// <see cref="inBuffer"/>
		/// , then write to this buffer.
		/// If
		/// <see cref="inBuffer"/>
		/// is full, then do encryption and write data to the
		/// underlying stream.
		/// </remarks>
		/// <param name="b">the data.</param>
		/// <param name="off">the start offset in the data.</param>
		/// <param name="len">the number of bytes to write.</param>
		/// <exception cref="System.IO.IOException"/>
		public override void write(byte[] b, int off, int len)
		{
			lock (this)
			{
				checkStream();
				if (b == null)
				{
					throw new System.ArgumentNullException();
				}
				else
				{
					if (off < 0 || len < 0 || off > b.Length || len > b.Length - off)
					{
						throw new System.IndexOutOfRangeException();
					}
				}
				while (len > 0)
				{
					int remaining = inBuffer.remaining();
					if (len < remaining)
					{
						inBuffer.put(b, off, len);
						len = 0;
					}
					else
					{
						inBuffer.put(b, off, remaining);
						off += remaining;
						len -= remaining;
						encrypt();
					}
				}
			}
		}

		/// <summary>
		/// Do the encryption, input is
		/// <see cref="inBuffer"/>
		/// and output is
		/// <see cref="outBuffer"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void encrypt()
		{
			com.google.common.@base.Preconditions.checkState(inBuffer.position() >= padding);
			if (inBuffer.position() == padding)
			{
				// There is no real data in the inBuffer.
				return;
			}
			inBuffer.flip();
			outBuffer.clear();
			encryptor.encrypt(inBuffer, outBuffer);
			inBuffer.clear();
			outBuffer.flip();
			if (padding > 0)
			{
				/*
				* The plain text and cipher text have a 1:1 mapping, they start at the
				* same position.
				*/
				outBuffer.position(padding);
				padding = 0;
			}
			int len = outBuffer.remaining();
			/*
			* If underlying stream supports {@link ByteBuffer} write in future, needs
			* refine here.
			*/
			byte[] tmp = getTmpBuf();
			outBuffer.get(tmp, 0, len);
			@out.write(tmp, 0, len);
			streamOffset += len;
			if (encryptor.isContextReset())
			{
				/*
				* This code is generally not executed since the encryptor usually
				* maintains encryption context (e.g. the counter) internally. However,
				* some implementations can't maintain context so a re-init is necessary
				* after each encryption call.
				*/
				updateEncryptor();
			}
		}

		/// <summary>
		/// Update the
		/// <see cref="encryptor"/>
		/// : calculate counter and
		/// <see cref="padding"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void updateEncryptor()
		{
			long counter = streamOffset / codec.getCipherSuite().getAlgorithmBlockSize();
			padding = unchecked((byte)(streamOffset % codec.getCipherSuite().getAlgorithmBlockSize
				()));
			inBuffer.position(padding);
			// Set proper position for input data.
			codec.calculateIV(initIV, counter, iv);
			encryptor.init(key, iv);
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

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			lock (this)
			{
				if (closed)
				{
					return;
				}
				try
				{
					base.close();
					freeBuffers();
				}
				finally
				{
					closed = true;
				}
			}
		}

		/// <summary>
		/// To flush, we need to encrypt the data in the buffer and write to the
		/// underlying stream, then do the flush.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override void flush()
		{
			lock (this)
			{
				checkStream();
				encrypt();
				base.flush();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(int b)
		{
			oneByteBuf[0] = unchecked((byte)(b & unchecked((int)(0xff))));
			write(oneByteBuf, 0, oneByteBuf.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		private void checkStream()
		{
			if (closed)
			{
				throw new System.IO.IOException("Stream closed");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void setDropBehind(bool dropCache)
		{
			try
			{
				((org.apache.hadoop.fs.CanSetDropBehind)@out).setDropBehind(dropCache);
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("This stream does not " + "support setting the drop-behind caching."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		public virtual void sync()
		{
			hflush();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void hflush()
		{
			flush();
			if (@out is org.apache.hadoop.fs.Syncable)
			{
				((org.apache.hadoop.fs.Syncable)@out).hflush();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void hsync()
		{
			flush();
			if (@out is org.apache.hadoop.fs.Syncable)
			{
				((org.apache.hadoop.fs.Syncable)@out).hsync();
			}
		}

		/// <summary>Forcibly free the direct buffers.</summary>
		private void freeBuffers()
		{
			org.apache.hadoop.crypto.CryptoStreamUtils.freeDB(inBuffer);
			org.apache.hadoop.crypto.CryptoStreamUtils.freeDB(outBuffer);
		}
	}
}
