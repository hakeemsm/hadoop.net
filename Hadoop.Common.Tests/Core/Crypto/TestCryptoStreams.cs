using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	public class TestCryptoStreams : CryptoStreamsTestBase
	{
		/// <summary>Data storage.</summary>
		/// <remarks>
		/// Data storage.
		/// <see cref="CryptoStreamsTestBase.GetOutputStream(int)"/>
		/// will write to this buf.
		/// <see cref="CryptoStreamsTestBase.GetInputStream(int)"/>
		/// will read from this buf.
		/// </remarks>
		private byte[] buf;

		private int bufLen;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			Configuration conf = new Configuration();
			codec = CryptoCodec.GetInstance(conf);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Shutdown()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override OutputStream GetOutputStream(int bufferSize, byte[] key
			, byte[] iv)
		{
			DataOutputBuffer @out = new _DataOutputBuffer_66(this);
			return new CryptoOutputStream(new TestCryptoStreams.FakeOutputStream(this, @out), 
				codec, bufferSize, key, iv);
		}

		private sealed class _DataOutputBuffer_66 : DataOutputBuffer
		{
			public _DataOutputBuffer_66(TestCryptoStreams _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				this._enclosing.buf = this.GetData();
				this._enclosing.bufLen = this.GetLength();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				this._enclosing.buf = this.GetData();
				this._enclosing.bufLen = this.GetLength();
			}

			private readonly TestCryptoStreams _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override InputStream GetInputStream(int bufferSize, byte[] key
			, byte[] iv)
		{
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(buf, 0, bufLen);
			return new CryptoInputStream(new TestCryptoStreams.FakeInputStream(@in), codec, bufferSize
				, key, iv);
		}

		private class FakeOutputStream : OutputStream, Syncable, CanSetDropBehind
		{
			private readonly byte[] oneByteBuf = new byte[1];

			private readonly DataOutputBuffer @out;

			private bool closed;

			public FakeOutputStream(TestCryptoStreams _enclosing, DataOutputBuffer @out)
			{
				this._enclosing = _enclosing;
				this.@out = @out;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b, int off, int len)
			{
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
							return;
						}
					}
				}
				this.CheckStream();
				this.@out.Write(b, off, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				this.CheckStream();
				this.@out.Flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				if (this.closed)
				{
					return;
				}
				this.@out.Close();
				this.closed = true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(int b)
			{
				this.oneByteBuf[0] = unchecked((byte)(b & unchecked((int)(0xff))));
				this.Write(this.oneByteBuf, 0, this.oneByteBuf.Length);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.NotSupportedException"/>
			public virtual void SetDropBehind(bool dropCache)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Sync()
			{
				this.Hflush();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Hflush()
			{
				this.CheckStream();
				this.Flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Hsync()
			{
				this.CheckStream();
				this.Flush();
			}

			/// <exception cref="System.IO.IOException"/>
			private void CheckStream()
			{
				if (this.closed)
				{
					throw new IOException("Stream is closed!");
				}
			}

			private readonly TestCryptoStreams _enclosing;
		}

		public class FakeInputStream : InputStream, Seekable, PositionedReadable, ByteBufferReadable
			, HasFileDescriptor, CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess
		{
			private readonly byte[] oneByteBuf = new byte[1];

			private int pos = 0;

			private readonly byte[] data;

			private readonly int length;

			private bool closed = false;

			public FakeInputStream(DataInputBuffer @in)
			{
				data = @in.GetData();
				length = @in.GetLength();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Seek(long pos)
			{
				if (pos > length)
				{
					throw new IOException("Cannot seek after EOF.");
				}
				if (pos < 0)
				{
					throw new IOException("Cannot seek to negative offset.");
				}
				CheckStream();
				this.pos = (int)pos;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPos()
			{
				return pos;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Available()
			{
				return length - pos;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b, int off, int len)
			{
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
				CheckStream();
				if (pos < length)
				{
					int n = (int)Math.Min(len, length - pos);
					System.Array.Copy(data, pos, b, off, n);
					pos += n;
					return n;
				}
				return -1;
			}

			/// <exception cref="System.IO.IOException"/>
			private void CheckStream()
			{
				if (closed)
				{
					throw new IOException("Stream is closed!");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Read(ByteBuffer buf)
			{
				CheckStream();
				if (pos < length)
				{
					int n = (int)Math.Min(buf.Remaining(), length - pos);
					if (n > 0)
					{
						buf.Put(data, pos, n);
					}
					pos += n;
					return n;
				}
				return -1;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long Skip(long n)
			{
				CheckStream();
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
			public override void Close()
			{
				closed = true;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Read(long position, byte[] b, int off, int len)
			{
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
				if (position > length)
				{
					throw new IOException("Cannot read after EOF.");
				}
				if (position < 0)
				{
					throw new IOException("Cannot read to negative offset.");
				}
				CheckStream();
				if (position < length)
				{
					int n = (int)Math.Min(len, length - position);
					System.Array.Copy(data, (int)position, b, off, n);
					return n;
				}
				return -1;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFully(long position, byte[] b, int off, int len)
			{
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
							return;
						}
					}
				}
				if (position > length)
				{
					throw new IOException("Cannot read after EOF.");
				}
				if (position < 0)
				{
					throw new IOException("Cannot read to negative offset.");
				}
				CheckStream();
				if (position + len > length)
				{
					throw new EOFException("Reach the end of stream.");
				}
				System.Array.Copy(data, (int)position, b, off, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFully(long position, byte[] buffer)
			{
				ReadFully(position, buffer, 0, buffer.Length);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.NotSupportedException"/>
			public virtual ByteBuffer Read(ByteBufferPool bufferPool, int maxLength, EnumSet<
				ReadOption> opts)
			{
				if (bufferPool == null)
				{
					throw new IOException("Please specify buffer pool.");
				}
				ByteBuffer buffer = bufferPool.GetBuffer(true, maxLength);
				int pos = buffer.Position();
				int n = Read(buffer);
				if (n >= 0)
				{
					buffer.Position(pos);
					return buffer;
				}
				return null;
			}

			public virtual void ReleaseBuffer(ByteBuffer buffer)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.NotSupportedException"/>
			public virtual void SetReadahead(long readahead)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.NotSupportedException"/>
			public virtual void SetDropBehind(bool dropCache)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FileDescriptor GetFileDescriptor()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool SeekToNewSource(long targetPos)
			{
				if (targetPos > length)
				{
					throw new IOException("Attempted to read past end of file.");
				}
				if (targetPos < 0)
				{
					throw new IOException("Cannot seek after EOF.");
				}
				CheckStream();
				this.pos = (int)targetPos;
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				int ret = Read(oneByteBuf, 0, 1);
				return (ret <= 0) ? -1 : (oneByteBuf[0] & unchecked((int)(0xff)));
			}
		}
	}
}
