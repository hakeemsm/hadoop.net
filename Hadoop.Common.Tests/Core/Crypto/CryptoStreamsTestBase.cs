using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	public abstract class CryptoStreamsTestBase
	{
		protected internal static readonly Log Log = LogFactory.GetLog(typeof(CryptoStreamsTestBase
			));

		protected internal static CryptoCodec codec;

		private static readonly byte[] key = new byte[] { unchecked((int)(0x01)), unchecked(
			(int)(0x02)), unchecked((int)(0x03)), unchecked((int)(0x04)), unchecked((int)(0x05
			)), unchecked((int)(0x06)), unchecked((int)(0x07)), unchecked((int)(0x08)), unchecked(
			(int)(0x09)), unchecked((int)(0x10)), unchecked((int)(0x11)), unchecked((int)(0x12
			)), unchecked((int)(0x13)), unchecked((int)(0x14)), unchecked((int)(0x15)), unchecked(
			(int)(0x16)) };

		private static readonly byte[] iv = new byte[] { unchecked((int)(0x01)), unchecked(
			(int)(0x02)), unchecked((int)(0x03)), unchecked((int)(0x04)), unchecked((int)(0x05
			)), unchecked((int)(0x06)), unchecked((int)(0x07)), unchecked((int)(0x08)), unchecked(
			(int)(0x01)), unchecked((int)(0x02)), unchecked((int)(0x03)), unchecked((int)(0x04
			)), unchecked((int)(0x05)), unchecked((int)(0x06)), unchecked((int)(0x07)), unchecked(
			(int)(0x08)) };

		protected internal const int count = 10000;

		protected internal static int defaultBufferSize = 8192;

		protected internal static int smallBufferSize = 1024;

		private byte[] data;

		private int dataLen;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			// Generate data
			int seed = new Random().Next();
			DataOutputBuffer dataBuf = new DataOutputBuffer();
			RandomDatum.Generator generator = new RandomDatum.Generator(seed);
			for (int i = 0; i < count; ++i)
			{
				generator.Next();
				RandomDatum key = generator.GetKey();
				RandomDatum value = generator.GetValue();
				key.Write(dataBuf);
				value.Write(dataBuf);
			}
			Log.Info("Generated " + count + " records");
			data = dataBuf.GetData();
			dataLen = dataBuf.GetLength();
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void WriteData(OutputStream @out)
		{
			@out.Write(data, 0, dataLen);
			@out.Close();
		}

		protected internal virtual int GetDataLen()
		{
			return dataLen;
		}

		/// <exception cref="System.IO.IOException"/>
		private int ReadAll(InputStream @in, byte[] b, int off, int len)
		{
			int n = 0;
			int total = 0;
			while (n != -1)
			{
				total += n;
				if (total >= len)
				{
					break;
				}
				n = @in.Read(b, off + total, len - total);
			}
			return total;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual OutputStream GetOutputStream(int bufferSize)
		{
			return GetOutputStream(bufferSize, key, iv);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract OutputStream GetOutputStream(int bufferSize, byte[] key
			, byte[] iv);

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual InputStream GetInputStream(int bufferSize)
		{
			return GetInputStream(bufferSize, key, iv);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract InputStream GetInputStream(int bufferSize, byte[] key
			, byte[] iv);

		/// <summary>Test crypto reading with different buffer size.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRead()
		{
			OutputStream @out = GetOutputStream(defaultBufferSize);
			WriteData(@out);
			// Default buffer size
			InputStream @in = GetInputStream(defaultBufferSize);
			ReadCheck(@in);
			@in.Close();
			// Small buffer size
			@in = GetInputStream(smallBufferSize);
			ReadCheck(@in);
			@in.Close();
		}

		/// <exception cref="System.Exception"/>
		private void ReadCheck(InputStream @in)
		{
			byte[] result = new byte[dataLen];
			int n = ReadAll(@in, result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(dataLen, n);
			byte[] expectedData = new byte[n];
			System.Array.Copy(data, 0, expectedData, 0, n);
			Assert.AssertArrayEquals(result, expectedData);
			// EOF
			n = @in.Read(result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(n, -1);
			@in.Close();
		}

		/// <summary>Test crypto writing with different buffer size.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestWrite()
		{
			// Default buffer size
			WriteCheck(defaultBufferSize);
			// Small buffer size
			WriteCheck(smallBufferSize);
		}

		/// <exception cref="System.Exception"/>
		private void WriteCheck(int bufferSize)
		{
			OutputStream @out = GetOutputStream(bufferSize);
			WriteData(@out);
			if (@out is FSDataOutputStream)
			{
				NUnit.Framework.Assert.AreEqual(((FSDataOutputStream)@out).GetPos(), GetDataLen()
					);
			}
		}

		/// <summary>Test crypto with different IV.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCryptoIV()
		{
			byte[] iv1 = iv.MemberwiseClone();
			// Counter base: Long.MAX_VALUE
			SetCounterBaseForIV(iv1, long.MaxValue);
			CryptoCheck(iv1);
			// Counter base: Long.MAX_VALUE - 1
			SetCounterBaseForIV(iv1, long.MaxValue - 1);
			CryptoCheck(iv1);
			// Counter base: Integer.MAX_VALUE
			SetCounterBaseForIV(iv1, int.MaxValue);
			CryptoCheck(iv1);
			// Counter base: 0
			SetCounterBaseForIV(iv1, 0);
			CryptoCheck(iv1);
			// Counter base: -1
			SetCounterBaseForIV(iv1, -1);
			CryptoCheck(iv1);
		}

		/// <exception cref="System.Exception"/>
		private void CryptoCheck(byte[] iv)
		{
			OutputStream @out = GetOutputStream(defaultBufferSize, key, iv);
			WriteData(@out);
			InputStream @in = GetInputStream(defaultBufferSize, key, iv);
			ReadCheck(@in);
			@in.Close();
		}

		private void SetCounterBaseForIV(byte[] iv, long counterBase)
		{
			ByteBuffer buf = ByteBuffer.Wrap(iv);
			buf.Order(ByteOrder.BigEndian);
			buf.PutLong(iv.Length - 8, counterBase);
		}

		/// <summary>Test hflush/hsync of crypto output stream, and with different buffer size.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSyncable()
		{
			SyncableCheck();
		}

		/// <exception cref="System.IO.IOException"/>
		private void SyncableCheck()
		{
			OutputStream @out = GetOutputStream(smallBufferSize);
			try
			{
				int bytesWritten = dataLen / 3;
				@out.Write(data, 0, bytesWritten);
				((Syncable)@out).Hflush();
				InputStream @in = GetInputStream(defaultBufferSize);
				Verify(@in, bytesWritten, data);
				@in.Close();
				@out.Write(data, bytesWritten, dataLen - bytesWritten);
				((Syncable)@out).Hsync();
				@in = GetInputStream(defaultBufferSize);
				Verify(@in, dataLen, data);
				@in.Close();
			}
			finally
			{
				@out.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void Verify(InputStream @in, int bytesToVerify, byte[] expectedBytes)
		{
			byte[] readBuf = new byte[bytesToVerify];
			ReadAll(@in, readBuf, 0, bytesToVerify);
			for (int i = 0; i < bytesToVerify; i++)
			{
				NUnit.Framework.Assert.AreEqual(expectedBytes[i], readBuf[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int ReadAll(InputStream @in, long pos, byte[] b, int off, int len)
		{
			int n = 0;
			int total = 0;
			while (n != -1)
			{
				total += n;
				if (total >= len)
				{
					break;
				}
				n = ((PositionedReadable)@in).Read(pos + total, b, off + total, len - total);
			}
			return total;
		}

		/// <summary>Test positioned read.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestPositionedRead()
		{
			OutputStream @out = GetOutputStream(defaultBufferSize);
			WriteData(@out);
			InputStream @in = GetInputStream(defaultBufferSize);
			// Pos: 1/3 dataLen
			PositionedReadCheck(@in, dataLen / 3);
			// Pos: 1/2 dataLen
			PositionedReadCheck(@in, dataLen / 2);
			@in.Close();
		}

		/// <exception cref="System.Exception"/>
		private void PositionedReadCheck(InputStream @in, int pos)
		{
			byte[] result = new byte[dataLen];
			int n = ReadAll(@in, pos, result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(dataLen, n + pos);
			byte[] readData = new byte[n];
			System.Array.Copy(result, 0, readData, 0, n);
			byte[] expectedData = new byte[n];
			System.Array.Copy(data, pos, expectedData, 0, n);
			Assert.AssertArrayEquals(readData, expectedData);
		}

		/// <summary>Test read fully</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReadFully()
		{
			OutputStream @out = GetOutputStream(defaultBufferSize);
			WriteData(@out);
			InputStream @in = GetInputStream(defaultBufferSize);
			int len1 = dataLen / 4;
			// Read len1 bytes
			byte[] readData = new byte[len1];
			ReadAll(@in, readData, 0, len1);
			byte[] expectedData = new byte[len1];
			System.Array.Copy(data, 0, expectedData, 0, len1);
			Assert.AssertArrayEquals(readData, expectedData);
			// Pos: 1/3 dataLen
			ReadFullyCheck(@in, dataLen / 3);
			// Read len1 bytes
			readData = new byte[len1];
			ReadAll(@in, readData, 0, len1);
			expectedData = new byte[len1];
			System.Array.Copy(data, len1, expectedData, 0, len1);
			Assert.AssertArrayEquals(readData, expectedData);
			// Pos: 1/2 dataLen
			ReadFullyCheck(@in, dataLen / 2);
			// Read len1 bytes
			readData = new byte[len1];
			ReadAll(@in, readData, 0, len1);
			expectedData = new byte[len1];
			System.Array.Copy(data, 2 * len1, expectedData, 0, len1);
			Assert.AssertArrayEquals(readData, expectedData);
			@in.Close();
		}

		/// <exception cref="System.Exception"/>
		private void ReadFullyCheck(InputStream @in, int pos)
		{
			byte[] result = new byte[dataLen - pos];
			((PositionedReadable)@in).ReadFully(pos, result);
			byte[] expectedData = new byte[dataLen - pos];
			System.Array.Copy(data, pos, expectedData, 0, dataLen - pos);
			Assert.AssertArrayEquals(result, expectedData);
			result = new byte[dataLen];
			// Exceeds maximum length 
			try
			{
				((PositionedReadable)@in).ReadFully(pos, result);
				NUnit.Framework.Assert.Fail("Read fully exceeds maximum length should fail.");
			}
			catch (IOException)
			{
			}
		}

		/// <summary>Test seek to different position.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSeek()
		{
			OutputStream @out = GetOutputStream(defaultBufferSize);
			WriteData(@out);
			InputStream @in = GetInputStream(defaultBufferSize);
			// Pos: 1/3 dataLen
			SeekCheck(@in, dataLen / 3);
			// Pos: 0
			SeekCheck(@in, 0);
			// Pos: 1/2 dataLen
			SeekCheck(@in, dataLen / 2);
			long pos = ((Seekable)@in).GetPos();
			// Pos: -3
			try
			{
				SeekCheck(@in, -3);
				NUnit.Framework.Assert.Fail("Seek to negative offset should fail.");
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot seek to negative " + "offset", e
					);
			}
			NUnit.Framework.Assert.AreEqual(pos, ((Seekable)@in).GetPos());
			// Pos: dataLen + 3
			try
			{
				SeekCheck(@in, dataLen + 3);
				NUnit.Framework.Assert.Fail("Seek after EOF should fail.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot seek after EOF", e);
			}
			NUnit.Framework.Assert.AreEqual(pos, ((Seekable)@in).GetPos());
			@in.Close();
		}

		/// <exception cref="System.Exception"/>
		private void SeekCheck(InputStream @in, int pos)
		{
			byte[] result = new byte[dataLen];
			((Seekable)@in).Seek(pos);
			int n = ReadAll(@in, result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(dataLen, n + pos);
			byte[] readData = new byte[n];
			System.Array.Copy(result, 0, readData, 0, n);
			byte[] expectedData = new byte[n];
			System.Array.Copy(data, pos, expectedData, 0, n);
			Assert.AssertArrayEquals(readData, expectedData);
		}

		/// <summary>Test get position.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetPos()
		{
			OutputStream @out = GetOutputStream(defaultBufferSize);
			WriteData(@out);
			// Default buffer size
			InputStream @in = GetInputStream(defaultBufferSize);
			byte[] result = new byte[dataLen];
			int n1 = ReadAll(@in, result, 0, dataLen / 3);
			NUnit.Framework.Assert.AreEqual(n1, ((Seekable)@in).GetPos());
			int n2 = ReadAll(@in, result, n1, dataLen - n1);
			NUnit.Framework.Assert.AreEqual(n1 + n2, ((Seekable)@in).GetPos());
			@in.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAvailable()
		{
			OutputStream @out = GetOutputStream(defaultBufferSize);
			WriteData(@out);
			// Default buffer size
			InputStream @in = GetInputStream(defaultBufferSize);
			byte[] result = new byte[dataLen];
			int n1 = ReadAll(@in, result, 0, dataLen / 3);
			NUnit.Framework.Assert.AreEqual(@in.Available(), dataLen - n1);
			int n2 = ReadAll(@in, result, n1, dataLen - n1);
			NUnit.Framework.Assert.AreEqual(@in.Available(), dataLen - n1 - n2);
			@in.Close();
		}

		/// <summary>Test skip.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSkip()
		{
			OutputStream @out = GetOutputStream(defaultBufferSize);
			WriteData(@out);
			// Default buffer size
			InputStream @in = GetInputStream(defaultBufferSize);
			byte[] result = new byte[dataLen];
			int n1 = ReadAll(@in, result, 0, dataLen / 3);
			NUnit.Framework.Assert.AreEqual(n1, ((Seekable)@in).GetPos());
			long skipped = @in.Skip(dataLen / 3);
			int n2 = ReadAll(@in, result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(dataLen, n1 + skipped + n2);
			byte[] readData = new byte[n2];
			System.Array.Copy(result, 0, readData, 0, n2);
			byte[] expectedData = new byte[n2];
			System.Array.Copy(data, dataLen - n2, expectedData, 0, n2);
			Assert.AssertArrayEquals(readData, expectedData);
			try
			{
				skipped = @in.Skip(-3);
				NUnit.Framework.Assert.Fail("Skip Negative length should fail.");
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Negative skip length", e);
			}
			// Skip after EOF
			skipped = @in.Skip(3);
			NUnit.Framework.Assert.AreEqual(skipped, 0);
			@in.Close();
		}

		/// <exception cref="System.Exception"/>
		private void ByteBufferReadCheck(InputStream @in, ByteBuffer buf, int bufPos)
		{
			buf.Position(bufPos);
			int n = ((ByteBufferReadable)@in).Read(buf);
			NUnit.Framework.Assert.AreEqual(bufPos + n, buf.Position());
			byte[] readData = new byte[n];
			buf.Rewind();
			buf.Position(bufPos);
			buf.Get(readData);
			byte[] expectedData = new byte[n];
			System.Array.Copy(data, 0, expectedData, 0, n);
			Assert.AssertArrayEquals(readData, expectedData);
		}

		/// <summary>Test byte buffer read with different buffer size.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestByteBufferRead()
		{
			OutputStream @out = GetOutputStream(defaultBufferSize);
			WriteData(@out);
			// Default buffer size, initial buffer position is 0
			InputStream @in = GetInputStream(defaultBufferSize);
			ByteBuffer buf = ByteBuffer.Allocate(dataLen + 100);
			ByteBufferReadCheck(@in, buf, 0);
			@in.Close();
			// Default buffer size, initial buffer position is not 0
			@in = GetInputStream(defaultBufferSize);
			buf.Clear();
			ByteBufferReadCheck(@in, buf, 11);
			@in.Close();
			// Small buffer size, initial buffer position is 0
			@in = GetInputStream(smallBufferSize);
			buf.Clear();
			ByteBufferReadCheck(@in, buf, 0);
			@in.Close();
			// Small buffer size, initial buffer position is not 0
			@in = GetInputStream(smallBufferSize);
			buf.Clear();
			ByteBufferReadCheck(@in, buf, 11);
			@in.Close();
			// Direct buffer, default buffer size, initial buffer position is 0
			@in = GetInputStream(defaultBufferSize);
			buf = ByteBuffer.AllocateDirect(dataLen + 100);
			ByteBufferReadCheck(@in, buf, 0);
			@in.Close();
			// Direct buffer, default buffer size, initial buffer position is not 0
			@in = GetInputStream(defaultBufferSize);
			buf.Clear();
			ByteBufferReadCheck(@in, buf, 11);
			@in.Close();
			// Direct buffer, small buffer size, initial buffer position is 0
			@in = GetInputStream(smallBufferSize);
			buf.Clear();
			ByteBufferReadCheck(@in, buf, 0);
			@in.Close();
			// Direct buffer, small buffer size, initial buffer position is not 0
			@in = GetInputStream(smallBufferSize);
			buf.Clear();
			ByteBufferReadCheck(@in, buf, 11);
			@in.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCombinedOp()
		{
			OutputStream @out = GetOutputStream(defaultBufferSize);
			WriteData(@out);
			int len1 = dataLen / 8;
			int len2 = dataLen / 10;
			InputStream @in = GetInputStream(defaultBufferSize);
			// Read len1 data.
			byte[] readData = new byte[len1];
			ReadAll(@in, readData, 0, len1);
			byte[] expectedData = new byte[len1];
			System.Array.Copy(data, 0, expectedData, 0, len1);
			Assert.AssertArrayEquals(readData, expectedData);
			long pos = ((Seekable)@in).GetPos();
			NUnit.Framework.Assert.AreEqual(len1, pos);
			// Seek forward len2
			((Seekable)@in).Seek(pos + len2);
			// Skip forward len2
			long n = @in.Skip(len2);
			NUnit.Framework.Assert.AreEqual(len2, n);
			// Pos: 1/4 dataLen
			PositionedReadCheck(@in, dataLen / 4);
			// Pos should be len1 + len2 + len2
			pos = ((Seekable)@in).GetPos();
			NUnit.Framework.Assert.AreEqual(len1 + len2 + len2, pos);
			// Read forward len1
			ByteBuffer buf = ByteBuffer.Allocate(len1);
			int nRead = ((ByteBufferReadable)@in).Read(buf);
			NUnit.Framework.Assert.AreEqual(nRead, buf.Position());
			readData = new byte[nRead];
			buf.Rewind();
			buf.Get(readData);
			expectedData = new byte[nRead];
			System.Array.Copy(data, (int)pos, expectedData, 0, nRead);
			Assert.AssertArrayEquals(readData, expectedData);
			long lastPos = pos;
			// Pos should be lastPos + nRead
			pos = ((Seekable)@in).GetPos();
			NUnit.Framework.Assert.AreEqual(lastPos + nRead, pos);
			// Pos: 1/3 dataLen
			PositionedReadCheck(@in, dataLen / 3);
			// Read forward len1
			readData = new byte[len1];
			ReadAll(@in, readData, 0, len1);
			expectedData = new byte[len1];
			System.Array.Copy(data, (int)pos, expectedData, 0, len1);
			Assert.AssertArrayEquals(readData, expectedData);
			lastPos = pos;
			// Pos should be lastPos + len1
			pos = ((Seekable)@in).GetPos();
			NUnit.Framework.Assert.AreEqual(lastPos + len1, pos);
			// Read forward len1
			buf = ByteBuffer.Allocate(len1);
			nRead = ((ByteBufferReadable)@in).Read(buf);
			NUnit.Framework.Assert.AreEqual(nRead, buf.Position());
			readData = new byte[nRead];
			buf.Rewind();
			buf.Get(readData);
			expectedData = new byte[nRead];
			System.Array.Copy(data, (int)pos, expectedData, 0, nRead);
			Assert.AssertArrayEquals(readData, expectedData);
			lastPos = pos;
			// Pos should be lastPos + nRead
			pos = ((Seekable)@in).GetPos();
			NUnit.Framework.Assert.AreEqual(lastPos + nRead, pos);
			// ByteBuffer read after EOF
			((Seekable)@in).Seek(dataLen);
			buf.Clear();
			n = ((ByteBufferReadable)@in).Read(buf);
			NUnit.Framework.Assert.AreEqual(n, -1);
			@in.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSeekToNewSource()
		{
			OutputStream @out = GetOutputStream(defaultBufferSize);
			WriteData(@out);
			InputStream @in = GetInputStream(defaultBufferSize);
			int len1 = dataLen / 8;
			byte[] readData = new byte[len1];
			ReadAll(@in, readData, 0, len1);
			// Pos: 1/3 dataLen
			SeekToNewSourceCheck(@in, dataLen / 3);
			// Pos: 0
			SeekToNewSourceCheck(@in, 0);
			// Pos: 1/2 dataLen
			SeekToNewSourceCheck(@in, dataLen / 2);
			// Pos: -3
			try
			{
				SeekToNewSourceCheck(@in, -3);
				NUnit.Framework.Assert.Fail("Seek to negative offset should fail.");
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot seek to negative " + "offset", e
					);
			}
			// Pos: dataLen + 3
			try
			{
				SeekToNewSourceCheck(@in, dataLen + 3);
				NUnit.Framework.Assert.Fail("Seek after EOF should fail.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Attempted to read past " + "end of file"
					, e);
			}
			@in.Close();
		}

		/// <exception cref="System.Exception"/>
		private void SeekToNewSourceCheck(InputStream @in, int targetPos)
		{
			byte[] result = new byte[dataLen];
			((Seekable)@in).SeekToNewSource(targetPos);
			int n = ReadAll(@in, result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(dataLen, n + targetPos);
			byte[] readData = new byte[n];
			System.Array.Copy(result, 0, readData, 0, n);
			byte[] expectedData = new byte[n];
			System.Array.Copy(data, targetPos, expectedData, 0, n);
			Assert.AssertArrayEquals(readData, expectedData);
		}

		private ByteBufferPool GetBufferPool()
		{
			return new _ByteBufferPool_681();
		}

		private sealed class _ByteBufferPool_681 : ByteBufferPool
		{
			public _ByteBufferPool_681()
			{
			}

			public ByteBuffer GetBuffer(bool direct, int length)
			{
				return ByteBuffer.AllocateDirect(length);
			}

			public void PutBuffer(ByteBuffer buffer)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHasEnhancedByteBufferAccess()
		{
			OutputStream @out = GetOutputStream(defaultBufferSize);
			WriteData(@out);
			InputStream @in = GetInputStream(defaultBufferSize);
			int len1 = dataLen / 8;
			// ByteBuffer size is len1
			ByteBuffer buffer = ((HasEnhancedByteBufferAccess)@in).Read(GetBufferPool(), len1
				, EnumSet.Of(ReadOption.SkipChecksums));
			int n1 = buffer.Remaining();
			byte[] readData = new byte[n1];
			buffer.Get(readData);
			byte[] expectedData = new byte[n1];
			System.Array.Copy(data, 0, expectedData, 0, n1);
			Assert.AssertArrayEquals(readData, expectedData);
			((HasEnhancedByteBufferAccess)@in).ReleaseBuffer(buffer);
			// Read len1 bytes
			readData = new byte[len1];
			ReadAll(@in, readData, 0, len1);
			expectedData = new byte[len1];
			System.Array.Copy(data, n1, expectedData, 0, len1);
			Assert.AssertArrayEquals(readData, expectedData);
			// ByteBuffer size is len1
			buffer = ((HasEnhancedByteBufferAccess)@in).Read(GetBufferPool(), len1, EnumSet.Of
				(ReadOption.SkipChecksums));
			int n2 = buffer.Remaining();
			readData = new byte[n2];
			buffer.Get(readData);
			expectedData = new byte[n2];
			System.Array.Copy(data, n1 + len1, expectedData, 0, n2);
			Assert.AssertArrayEquals(readData, expectedData);
			((HasEnhancedByteBufferAccess)@in).ReleaseBuffer(buffer);
			@in.Close();
		}
	}
}
