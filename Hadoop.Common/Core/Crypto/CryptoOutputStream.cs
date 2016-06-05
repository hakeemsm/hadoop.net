using System;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.Crypto
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
	public class CryptoOutputStream : FilterOutputStream, Syncable, CanSetDropBehind
	{
		private readonly byte[] oneByteBuf = new byte[1];

		private readonly CryptoCodec codec;

		private readonly Encryptor encryptor;

		private readonly int bufferSize;

		/// <summary>Input data buffer.</summary>
		/// <remarks>
		/// Input data buffer. The data starts at inBuffer.position() and ends at
		/// inBuffer.limit().
		/// </remarks>
		private ByteBuffer inBuffer;

		/// <summary>Encrypted data buffer.</summary>
		/// <remarks>
		/// Encrypted data buffer. The data starts at outBuffer.position() and ends at
		/// outBuffer.limit();
		/// </remarks>
		private ByteBuffer outBuffer;

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
		public CryptoOutputStream(OutputStream @out, CryptoCodec codec, int bufferSize, byte
			[] key, byte[] iv)
			: this(@out, codec, bufferSize, key, iv, 0)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoOutputStream(OutputStream @out, CryptoCodec codec, int bufferSize, byte
			[] key, byte[] iv, long streamOffset)
			: base(@out)
		{
			// Underlying stream offset.
			CryptoStreamUtils.CheckCodec(codec);
			this.bufferSize = CryptoStreamUtils.CheckBufferSize(codec, bufferSize);
			this.codec = codec;
			this.key = key.MemberwiseClone();
			this.initIV = iv.MemberwiseClone();
			this.iv = iv.MemberwiseClone();
			inBuffer = ByteBuffer.AllocateDirect(this.bufferSize);
			outBuffer = ByteBuffer.AllocateDirect(this.bufferSize);
			this.streamOffset = streamOffset;
			try
			{
				encryptor = codec.CreateEncryptor();
			}
			catch (GeneralSecurityException e)
			{
				throw new IOException(e);
			}
			UpdateEncryptor();
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoOutputStream(OutputStream @out, CryptoCodec codec, byte[] key, byte[]
			 iv)
			: this(@out, codec, key, iv, 0)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoOutputStream(OutputStream @out, CryptoCodec codec, byte[] key, byte[]
			 iv, long streamOffset)
			: this(@out, codec, CryptoStreamUtils.GetBufferSize(codec.GetConf()), key, iv, streamOffset
				)
		{
		}

		public virtual OutputStream GetWrappedStream()
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
		public override void Write(byte[] b, int off, int len)
		{
			lock (this)
			{
				CheckStream();
				if (b == null)
				{
					throw new ArgumentNullException();
				}
				else
				{
					if (off < 0 || len < 0 || off > b.Length || len > b.Length - off)
					{
						throw new IndexOutOfRangeException();
					}
				}
				while (len > 0)
				{
					int remaining = inBuffer.Remaining();
					if (len < remaining)
					{
						inBuffer.Put(b, off, len);
						len = 0;
					}
					else
					{
						inBuffer.Put(b, off, remaining);
						off += remaining;
						len -= remaining;
						Encrypt();
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
		private void Encrypt()
		{
			Preconditions.CheckState(inBuffer.Position() >= padding);
			if (inBuffer.Position() == padding)
			{
				// There is no real data in the inBuffer.
				return;
			}
			inBuffer.Flip();
			outBuffer.Clear();
			encryptor.Encrypt(inBuffer, outBuffer);
			inBuffer.Clear();
			outBuffer.Flip();
			if (padding > 0)
			{
				/*
				* The plain text and cipher text have a 1:1 mapping, they start at the
				* same position.
				*/
				outBuffer.Position(padding);
				padding = 0;
			}
			int len = outBuffer.Remaining();
			/*
			* If underlying stream supports {@link ByteBuffer} write in future, needs
			* refine here.
			*/
			byte[] tmp = GetTmpBuf();
			outBuffer.Get(tmp, 0, len);
			@out.Write(tmp, 0, len);
			streamOffset += len;
			if (encryptor.IsContextReset())
			{
				/*
				* This code is generally not executed since the encryptor usually
				* maintains encryption context (e.g. the counter) internally. However,
				* some implementations can't maintain context so a re-init is necessary
				* after each encryption call.
				*/
				UpdateEncryptor();
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
		private void UpdateEncryptor()
		{
			long counter = streamOffset / codec.GetCipherSuite().GetAlgorithmBlockSize();
			padding = unchecked((byte)(streamOffset % codec.GetCipherSuite().GetAlgorithmBlockSize
				()));
			inBuffer.Position(padding);
			// Set proper position for input data.
			codec.CalculateIV(initIV, counter, iv);
			encryptor.Init(key, iv);
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

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				if (closed)
				{
					return;
				}
				try
				{
					base.Close();
					FreeBuffers();
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
		public override void Flush()
		{
			lock (this)
			{
				CheckStream();
				Encrypt();
				base.Flush();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(int b)
		{
			oneByteBuf[0] = unchecked((byte)(b & unchecked((int)(0xff))));
			Write(oneByteBuf, 0, oneByteBuf.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckStream()
		{
			if (closed)
			{
				throw new IOException("Stream closed");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void SetDropBehind(bool dropCache)
		{
			try
			{
				((CanSetDropBehind)@out).SetDropBehind(dropCache);
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("This stream does not " + "support setting the drop-behind caching."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public virtual void Sync()
		{
			Hflush();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Hflush()
		{
			Flush();
			if (@out is Syncable)
			{
				((Syncable)@out).Hflush();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Hsync()
		{
			Flush();
			if (@out is Syncable)
			{
				((Syncable)@out).Hsync();
			}
		}

		/// <summary>Forcibly free the direct buffers.</summary>
		private void FreeBuffers()
		{
			CryptoStreamUtils.FreeDB(inBuffer);
			CryptoStreamUtils.FreeDB(outBuffer);
		}
	}
}
