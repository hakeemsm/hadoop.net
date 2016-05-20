using Sharpen;

namespace org.apache.hadoop.crypto
{
	/// <summary>
	/// Test crypto streams using normal stream which does not support the
	/// additional interfaces that the Hadoop FileSystem streams implement
	/// (Seekable, PositionedReadable, ByteBufferReadable, HasFileDescriptor,
	/// CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess, Syncable,
	/// CanSetDropBehind)
	/// </summary>
	public class TestCryptoStreamsNormal : org.apache.hadoop.crypto.CryptoStreamsTestBase
	{
		/// <summary>Data storage.</summary>
		/// <remarks>
		/// Data storage.
		/// <see cref="getOutputStream(int, byte[], byte[])"/>
		/// will write to this buffer.
		/// <see cref="getInputStream(int, byte[], byte[])"/>
		/// will read from this buffer.
		/// </remarks>
		private byte[] buffer;

		private int bufferLen;

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
			java.io.OutputStream @out = new _ByteArrayOutputStream_61(this);
			return new org.apache.hadoop.crypto.CryptoOutputStream(@out, codec, bufferSize, key
				, iv);
		}

		private sealed class _ByteArrayOutputStream_61 : java.io.ByteArrayOutputStream
		{
			public _ByteArrayOutputStream_61(TestCryptoStreamsNormal _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void flush()
			{
				this._enclosing.buffer = this.buf;
				this._enclosing.bufferLen = this.count;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				this._enclosing.buffer = this.buf;
				this._enclosing.bufferLen = this.count;
			}

			private readonly TestCryptoStreamsNormal _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override java.io.InputStream getInputStream(int bufferSize, byte
			[] key, byte[] iv)
		{
			java.io.ByteArrayInputStream @in = new java.io.ByteArrayInputStream(buffer, 0, bufferLen
				);
			return new org.apache.hadoop.crypto.CryptoInputStream(@in, codec, bufferSize, key
				, iv);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testSyncable()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testPositionedRead()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testReadFully()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testSeek()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testByteBufferRead()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testCombinedOp()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testSeekToNewSource()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void testHasEnhancedByteBufferAccess()
		{
		}
	}
}
