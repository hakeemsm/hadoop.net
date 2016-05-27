using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	/// <summary>
	/// Test crypto streams using normal stream which does not support the
	/// additional interfaces that the Hadoop FileSystem streams implement
	/// (Seekable, PositionedReadable, ByteBufferReadable, HasFileDescriptor,
	/// CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess, Syncable,
	/// CanSetDropBehind)
	/// </summary>
	public class TestCryptoStreamsNormal : CryptoStreamsTestBase
	{
		/// <summary>Data storage.</summary>
		/// <remarks>
		/// Data storage.
		/// <see cref="GetOutputStream(int, byte[], byte[])"/>
		/// will write to this buffer.
		/// <see cref="GetInputStream(int, byte[], byte[])"/>
		/// will read from this buffer.
		/// </remarks>
		private byte[] buffer;

		private int bufferLen;

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
			OutputStream @out = new _ByteArrayOutputStream_61(this);
			return new CryptoOutputStream(@out, codec, bufferSize, key, iv);
		}

		private sealed class _ByteArrayOutputStream_61 : ByteArrayOutputStream
		{
			public _ByteArrayOutputStream_61(TestCryptoStreamsNormal _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				this._enclosing.buffer = this.buf;
				this._enclosing.bufferLen = this.count;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				this._enclosing.buffer = this.buf;
				this._enclosing.bufferLen = this.count;
			}

			private readonly TestCryptoStreamsNormal _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override InputStream GetInputStream(int bufferSize, byte[] key
			, byte[] iv)
		{
			ByteArrayInputStream @in = new ByteArrayInputStream(buffer, 0, bufferLen);
			return new CryptoInputStream(@in, codec, bufferSize, key, iv);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestSyncable()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestPositionedRead()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestReadFully()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestSeek()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestByteBufferRead()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestCombinedOp()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestSeekToNewSource()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestHasEnhancedByteBufferAccess()
		{
		}
	}
}
