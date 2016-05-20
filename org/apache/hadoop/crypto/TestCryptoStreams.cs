using Sharpen;

namespace org.apache.hadoop.crypto
{
	public class TestCryptoStreams : org.apache.hadoop.crypto.CryptoStreamsTestBase
	{
		/// <summary>Data storage.</summary>
		/// <remarks>
		/// Data storage.
		/// <see cref="CryptoStreamsTestBase.getOutputStream(int)"/>
		/// will write to this buf.
		/// <see cref="CryptoStreamsTestBase.getInputStream(int)"/>
		/// will read from this buf.
		/// </remarks>
		private byte[] buf;

		private int bufLen;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void init()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			codec = org.apache.hadoop.crypto.CryptoCodec.getInstance(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.AfterClass]
		public static void shutdown()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override java.io.OutputStream getOutputStream(int bufferSize, 
			byte[] key, byte[] iv)
		{
			org.apache.hadoop.io.DataOutputBuffer @out = new _DataOutputBuffer_66(this);
			return new org.apache.hadoop.crypto.CryptoOutputStream(new org.apache.hadoop.crypto.TestCryptoStreams.FakeOutputStream
				(this, @out), codec, bufferSize, key, iv);
		}

		private sealed class _DataOutputBuffer_66 : org.apache.hadoop.io.DataOutputBuffer
		{
			public _DataOutputBuffer_66(TestCryptoStreams _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void flush()
			{
				this._enclosing.buf = this.getData();
				this._enclosing.bufLen = this.getLength();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				this._enclosing.buf = this.getData();
				this._enclosing.bufLen = this.getLength();
			}

			private readonly TestCryptoStreams _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override java.io.InputStream getInputStream(int bufferSize, byte
			[] key, byte[] iv)
		{
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			@in.reset(buf, 0, bufLen);
			return new org.apache.hadoop.crypto.CryptoInputStream(new org.apache.hadoop.crypto.TestCryptoStreams.FakeInputStream
				(@in), codec, bufferSize, key, iv);
		}

		private class FakeOutputStream : java.io.OutputStream, org.apache.hadoop.fs.Syncable
			, org.apache.hadoop.fs.CanSetDropBehind
		{
			private readonly byte[] oneByteBuf = new byte[1];

			private readonly org.apache.hadoop.io.DataOutputBuffer @out;

			private bool closed;

			public FakeOutputStream(TestCryptoStreams _enclosing, org.apache.hadoop.io.DataOutputBuffer
				 @out)
			{
				this._enclosing = _enclosing;
				this.@out = @out;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(byte[] b, int off, int len)
			{
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
							return;
						}
					}
				}
				this.checkStream();
				this.@out.write(b, off, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void flush()
			{
				this.checkStream();
				this.@out.flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				if (this.closed)
				{
					return;
				}
				this.@out.close();
				this.closed = true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(int b)
			{
				this.oneByteBuf[0] = unchecked((byte)(b & unchecked((int)(0xff))));
				this.write(this.oneByteBuf, 0, this.oneByteBuf.Length);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.NotSupportedException"/>
			public virtual void setDropBehind(bool dropCache)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void sync()
			{
				this.hflush();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void hflush()
			{
				this.checkStream();
				this.flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void hsync()
			{
				this.checkStream();
				this.flush();
			}

			/// <exception cref="System.IO.IOException"/>
			private void checkStream()
			{
				if (this.closed)
				{
					throw new System.IO.IOException("Stream is closed!");
				}
			}

			private readonly TestCryptoStreams _enclosing;
		}

		public class FakeInputStream : java.io.InputStream, org.apache.hadoop.fs.Seekable
			, org.apache.hadoop.fs.PositionedReadable, org.apache.hadoop.fs.ByteBufferReadable
			, org.apache.hadoop.fs.HasFileDescriptor, org.apache.hadoop.fs.CanSetDropBehind, 
			org.apache.hadoop.fs.CanSetReadahead, org.apache.hadoop.fs.HasEnhancedByteBufferAccess
		{
			private readonly byte[] oneByteBuf = new byte[1];

			private int pos = 0;

			private readonly byte[] data;

			private readonly int length;

			private bool closed = false;

			public FakeInputStream(org.apache.hadoop.io.DataInputBuffer @in)
			{
				data = @in.getData();
				length = @in.getLength();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void seek(long pos)
			{
				if (pos > length)
				{
					throw new System.IO.IOException("Cannot seek after EOF.");
				}
				if (pos < 0)
				{
					throw new System.IO.IOException("Cannot seek to negative offset.");
				}
				checkStream();
				this.pos = (int)pos;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long getPos()
			{
				return pos;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int available()
			{
				return length - pos;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read(byte[] b, int off, int len)
			{
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
				checkStream();
				if (pos < length)
				{
					int n = (int)System.Math.min(len, length - pos);
					System.Array.Copy(data, pos, b, off, n);
					pos += n;
					return n;
				}
				return -1;
			}

			/// <exception cref="System.IO.IOException"/>
			private void checkStream()
			{
				if (closed)
				{
					throw new System.IO.IOException("Stream is closed!");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int read(java.nio.ByteBuffer buf)
			{
				checkStream();
				if (pos < length)
				{
					int n = (int)System.Math.min(buf.remaining(), length - pos);
					if (n > 0)
					{
						buf.put(data, pos, n);
					}
					pos += n;
					return n;
				}
				return -1;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long skip(long n)
			{
				checkStream();
				if (n > 0)
				{
					if (n + pos > length)
					{
						n = length - pos;
					}
					pos += n;
					return n;
				}
				return n < 0 ? -1 : 0;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				closed = true;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int read(long position, byte[] b, int off, int len)
			{
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
				if (position > length)
				{
					throw new System.IO.IOException("Cannot read after EOF.");
				}
				if (position < 0)
				{
					throw new System.IO.IOException("Cannot read to negative offset.");
				}
				checkStream();
				if (position < length)
				{
					int n = (int)System.Math.min(len, length - position);
					System.Array.Copy(data, (int)position, b, off, n);
					return n;
				}
				return -1;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void readFully(long position, byte[] b, int off, int len)
			{
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
							return;
						}
					}
				}
				if (position > length)
				{
					throw new System.IO.IOException("Cannot read after EOF.");
				}
				if (position < 0)
				{
					throw new System.IO.IOException("Cannot read to negative offset.");
				}
				checkStream();
				if (position + len > length)
				{
					throw new java.io.EOFException("Reach the end of stream.");
				}
				System.Array.Copy(data, (int)position, b, off, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void readFully(long position, byte[] buffer)
			{
				readFully(position, buffer, 0, buffer.Length);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.NotSupportedException"/>
			public virtual java.nio.ByteBuffer read(org.apache.hadoop.io.ByteBufferPool bufferPool
				, int maxLength, java.util.EnumSet<org.apache.hadoop.fs.ReadOption> opts)
			{
				if (bufferPool == null)
				{
					throw new System.IO.IOException("Please specify buffer pool.");
				}
				java.nio.ByteBuffer buffer = bufferPool.getBuffer(true, maxLength);
				int pos = buffer.position();
				int n = read(buffer);
				if (n >= 0)
				{
					buffer.position(pos);
					return buffer;
				}
				return null;
			}

			public virtual void releaseBuffer(java.nio.ByteBuffer buffer)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.NotSupportedException"/>
			public virtual void setReadahead(long readahead)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.NotSupportedException"/>
			public virtual void setDropBehind(bool dropCache)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual java.io.FileDescriptor getFileDescriptor()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool seekToNewSource(long targetPos)
			{
				if (targetPos > length)
				{
					throw new System.IO.IOException("Attempted to read past end of file.");
				}
				if (targetPos < 0)
				{
					throw new System.IO.IOException("Cannot seek after EOF.");
				}
				checkStream();
				this.pos = (int)targetPos;
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read()
			{
				int ret = read(oneByteBuf, 0, 1);
				return (ret <= 0) ? -1 : (oneByteBuf[0] & unchecked((int)(0xff)));
			}
		}
	}
}
