using Sharpen;

namespace org.apache.hadoop.crypto
{
	public abstract class CryptoStreamsTestBase
	{
		protected internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.crypto.CryptoStreamsTestBase
			)));

		protected internal static org.apache.hadoop.crypto.CryptoCodec codec;

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
		public virtual void setUp()
		{
			// Generate data
			int seed = new java.util.Random().nextInt();
			org.apache.hadoop.io.DataOutputBuffer dataBuf = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.RandomDatum.Generator generator = new org.apache.hadoop.io.RandomDatum.Generator
				(seed);
			for (int i = 0; i < count; ++i)
			{
				generator.next();
				org.apache.hadoop.io.RandomDatum key = generator.getKey();
				org.apache.hadoop.io.RandomDatum value = generator.getValue();
				key.write(dataBuf);
				value.write(dataBuf);
			}
			LOG.info("Generated " + count + " records");
			data = dataBuf.getData();
			dataLen = dataBuf.getLength();
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void writeData(java.io.OutputStream @out)
		{
			@out.write(data, 0, dataLen);
			@out.close();
		}

		protected internal virtual int getDataLen()
		{
			return dataLen;
		}

		/// <exception cref="System.IO.IOException"/>
		private int readAll(java.io.InputStream @in, byte[] b, int off, int len)
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
				n = @in.read(b, off + total, len - total);
			}
			return total;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual java.io.OutputStream getOutputStream(int bufferSize)
		{
			return getOutputStream(bufferSize, key, iv);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract java.io.OutputStream getOutputStream(int bufferSize, 
			byte[] key, byte[] iv);

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual java.io.InputStream getInputStream(int bufferSize)
		{
			return getInputStream(bufferSize, key, iv);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract java.io.InputStream getInputStream(int bufferSize, byte
			[] key, byte[] iv);

		/// <summary>Test crypto reading with different buffer size.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testRead()
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize);
			writeData(@out);
			// Default buffer size
			java.io.InputStream @in = getInputStream(defaultBufferSize);
			readCheck(@in);
			@in.close();
			// Small buffer size
			@in = getInputStream(smallBufferSize);
			readCheck(@in);
			@in.close();
		}

		/// <exception cref="System.Exception"/>
		private void readCheck(java.io.InputStream @in)
		{
			byte[] result = new byte[dataLen];
			int n = readAll(@in, result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(dataLen, n);
			byte[] expectedData = new byte[n];
			System.Array.Copy(data, 0, expectedData, 0, n);
			NUnit.Framework.Assert.assertArrayEquals(result, expectedData);
			// EOF
			n = @in.read(result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(n, -1);
			@in.close();
		}

		/// <summary>Test crypto writing with different buffer size.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testWrite()
		{
			// Default buffer size
			writeCheck(defaultBufferSize);
			// Small buffer size
			writeCheck(smallBufferSize);
		}

		/// <exception cref="System.Exception"/>
		private void writeCheck(int bufferSize)
		{
			java.io.OutputStream @out = getOutputStream(bufferSize);
			writeData(@out);
			if (@out is org.apache.hadoop.fs.FSDataOutputStream)
			{
				NUnit.Framework.Assert.AreEqual(((org.apache.hadoop.fs.FSDataOutputStream)@out).getPos
					(), getDataLen());
			}
		}

		/// <summary>Test crypto with different IV.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testCryptoIV()
		{
			byte[] iv1 = iv.MemberwiseClone();
			// Counter base: Long.MAX_VALUE
			setCounterBaseForIV(iv1, long.MaxValue);
			cryptoCheck(iv1);
			// Counter base: Long.MAX_VALUE - 1
			setCounterBaseForIV(iv1, long.MaxValue - 1);
			cryptoCheck(iv1);
			// Counter base: Integer.MAX_VALUE
			setCounterBaseForIV(iv1, int.MaxValue);
			cryptoCheck(iv1);
			// Counter base: 0
			setCounterBaseForIV(iv1, 0);
			cryptoCheck(iv1);
			// Counter base: -1
			setCounterBaseForIV(iv1, -1);
			cryptoCheck(iv1);
		}

		/// <exception cref="System.Exception"/>
		private void cryptoCheck(byte[] iv)
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize, key, iv);
			writeData(@out);
			java.io.InputStream @in = getInputStream(defaultBufferSize, key, iv);
			readCheck(@in);
			@in.close();
		}

		private void setCounterBaseForIV(byte[] iv, long counterBase)
		{
			java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(iv);
			buf.order(java.nio.ByteOrder.BIG_ENDIAN);
			buf.putLong(iv.Length - 8, counterBase);
		}

		/// <summary>Test hflush/hsync of crypto output stream, and with different buffer size.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testSyncable()
		{
			syncableCheck();
		}

		/// <exception cref="System.IO.IOException"/>
		private void syncableCheck()
		{
			java.io.OutputStream @out = getOutputStream(smallBufferSize);
			try
			{
				int bytesWritten = dataLen / 3;
				@out.write(data, 0, bytesWritten);
				((org.apache.hadoop.fs.Syncable)@out).hflush();
				java.io.InputStream @in = getInputStream(defaultBufferSize);
				verify(@in, bytesWritten, data);
				@in.close();
				@out.write(data, bytesWritten, dataLen - bytesWritten);
				((org.apache.hadoop.fs.Syncable)@out).hsync();
				@in = getInputStream(defaultBufferSize);
				verify(@in, dataLen, data);
				@in.close();
			}
			finally
			{
				@out.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void verify(java.io.InputStream @in, int bytesToVerify, byte[] expectedBytes
			)
		{
			byte[] readBuf = new byte[bytesToVerify];
			readAll(@in, readBuf, 0, bytesToVerify);
			for (int i = 0; i < bytesToVerify; i++)
			{
				NUnit.Framework.Assert.AreEqual(expectedBytes[i], readBuf[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int readAll(java.io.InputStream @in, long pos, byte[] b, int off, int len
			)
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
				n = ((org.apache.hadoop.fs.PositionedReadable)@in).read(pos + total, b, off + total
					, len - total);
			}
			return total;
		}

		/// <summary>Test positioned read.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testPositionedRead()
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize);
			writeData(@out);
			java.io.InputStream @in = getInputStream(defaultBufferSize);
			// Pos: 1/3 dataLen
			positionedReadCheck(@in, dataLen / 3);
			// Pos: 1/2 dataLen
			positionedReadCheck(@in, dataLen / 2);
			@in.close();
		}

		/// <exception cref="System.Exception"/>
		private void positionedReadCheck(java.io.InputStream @in, int pos)
		{
			byte[] result = new byte[dataLen];
			int n = readAll(@in, pos, result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(dataLen, n + pos);
			byte[] readData = new byte[n];
			System.Array.Copy(result, 0, readData, 0, n);
			byte[] expectedData = new byte[n];
			System.Array.Copy(data, pos, expectedData, 0, n);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
		}

		/// <summary>Test read fully</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testReadFully()
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize);
			writeData(@out);
			java.io.InputStream @in = getInputStream(defaultBufferSize);
			int len1 = dataLen / 4;
			// Read len1 bytes
			byte[] readData = new byte[len1];
			readAll(@in, readData, 0, len1);
			byte[] expectedData = new byte[len1];
			System.Array.Copy(data, 0, expectedData, 0, len1);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
			// Pos: 1/3 dataLen
			readFullyCheck(@in, dataLen / 3);
			// Read len1 bytes
			readData = new byte[len1];
			readAll(@in, readData, 0, len1);
			expectedData = new byte[len1];
			System.Array.Copy(data, len1, expectedData, 0, len1);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
			// Pos: 1/2 dataLen
			readFullyCheck(@in, dataLen / 2);
			// Read len1 bytes
			readData = new byte[len1];
			readAll(@in, readData, 0, len1);
			expectedData = new byte[len1];
			System.Array.Copy(data, 2 * len1, expectedData, 0, len1);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
			@in.close();
		}

		/// <exception cref="System.Exception"/>
		private void readFullyCheck(java.io.InputStream @in, int pos)
		{
			byte[] result = new byte[dataLen - pos];
			((org.apache.hadoop.fs.PositionedReadable)@in).readFully(pos, result);
			byte[] expectedData = new byte[dataLen - pos];
			System.Array.Copy(data, pos, expectedData, 0, dataLen - pos);
			NUnit.Framework.Assert.assertArrayEquals(result, expectedData);
			result = new byte[dataLen];
			// Exceeds maximum length 
			try
			{
				((org.apache.hadoop.fs.PositionedReadable)@in).readFully(pos, result);
				NUnit.Framework.Assert.Fail("Read fully exceeds maximum length should fail.");
			}
			catch (System.IO.IOException)
			{
			}
		}

		/// <summary>Test seek to different position.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testSeek()
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize);
			writeData(@out);
			java.io.InputStream @in = getInputStream(defaultBufferSize);
			// Pos: 1/3 dataLen
			seekCheck(@in, dataLen / 3);
			// Pos: 0
			seekCheck(@in, 0);
			// Pos: 1/2 dataLen
			seekCheck(@in, dataLen / 2);
			long pos = ((org.apache.hadoop.fs.Seekable)@in).getPos();
			// Pos: -3
			try
			{
				seekCheck(@in, -3);
				NUnit.Framework.Assert.Fail("Seek to negative offset should fail.");
			}
			catch (System.ArgumentException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Cannot seek to negative "
					 + "offset", e);
			}
			NUnit.Framework.Assert.AreEqual(pos, ((org.apache.hadoop.fs.Seekable)@in).getPos(
				));
			// Pos: dataLen + 3
			try
			{
				seekCheck(@in, dataLen + 3);
				NUnit.Framework.Assert.Fail("Seek after EOF should fail.");
			}
			catch (System.IO.IOException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Cannot seek after EOF"
					, e);
			}
			NUnit.Framework.Assert.AreEqual(pos, ((org.apache.hadoop.fs.Seekable)@in).getPos(
				));
			@in.close();
		}

		/// <exception cref="System.Exception"/>
		private void seekCheck(java.io.InputStream @in, int pos)
		{
			byte[] result = new byte[dataLen];
			((org.apache.hadoop.fs.Seekable)@in).seek(pos);
			int n = readAll(@in, result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(dataLen, n + pos);
			byte[] readData = new byte[n];
			System.Array.Copy(result, 0, readData, 0, n);
			byte[] expectedData = new byte[n];
			System.Array.Copy(data, pos, expectedData, 0, n);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
		}

		/// <summary>Test get position.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testGetPos()
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize);
			writeData(@out);
			// Default buffer size
			java.io.InputStream @in = getInputStream(defaultBufferSize);
			byte[] result = new byte[dataLen];
			int n1 = readAll(@in, result, 0, dataLen / 3);
			NUnit.Framework.Assert.AreEqual(n1, ((org.apache.hadoop.fs.Seekable)@in).getPos()
				);
			int n2 = readAll(@in, result, n1, dataLen - n1);
			NUnit.Framework.Assert.AreEqual(n1 + n2, ((org.apache.hadoop.fs.Seekable)@in).getPos
				());
			@in.close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAvailable()
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize);
			writeData(@out);
			// Default buffer size
			java.io.InputStream @in = getInputStream(defaultBufferSize);
			byte[] result = new byte[dataLen];
			int n1 = readAll(@in, result, 0, dataLen / 3);
			NUnit.Framework.Assert.AreEqual(@in.available(), dataLen - n1);
			int n2 = readAll(@in, result, n1, dataLen - n1);
			NUnit.Framework.Assert.AreEqual(@in.available(), dataLen - n1 - n2);
			@in.close();
		}

		/// <summary>Test skip.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testSkip()
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize);
			writeData(@out);
			// Default buffer size
			java.io.InputStream @in = getInputStream(defaultBufferSize);
			byte[] result = new byte[dataLen];
			int n1 = readAll(@in, result, 0, dataLen / 3);
			NUnit.Framework.Assert.AreEqual(n1, ((org.apache.hadoop.fs.Seekable)@in).getPos()
				);
			long skipped = @in.skip(dataLen / 3);
			int n2 = readAll(@in, result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(dataLen, n1 + skipped + n2);
			byte[] readData = new byte[n2];
			System.Array.Copy(result, 0, readData, 0, n2);
			byte[] expectedData = new byte[n2];
			System.Array.Copy(data, dataLen - n2, expectedData, 0, n2);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
			try
			{
				skipped = @in.skip(-3);
				NUnit.Framework.Assert.Fail("Skip Negative length should fail.");
			}
			catch (System.ArgumentException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Negative skip length"
					, e);
			}
			// Skip after EOF
			skipped = @in.skip(3);
			NUnit.Framework.Assert.AreEqual(skipped, 0);
			@in.close();
		}

		/// <exception cref="System.Exception"/>
		private void byteBufferReadCheck(java.io.InputStream @in, java.nio.ByteBuffer buf
			, int bufPos)
		{
			buf.position(bufPos);
			int n = ((org.apache.hadoop.fs.ByteBufferReadable)@in).read(buf);
			NUnit.Framework.Assert.AreEqual(bufPos + n, buf.position());
			byte[] readData = new byte[n];
			buf.rewind();
			buf.position(bufPos);
			buf.get(readData);
			byte[] expectedData = new byte[n];
			System.Array.Copy(data, 0, expectedData, 0, n);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
		}

		/// <summary>Test byte buffer read with different buffer size.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testByteBufferRead()
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize);
			writeData(@out);
			// Default buffer size, initial buffer position is 0
			java.io.InputStream @in = getInputStream(defaultBufferSize);
			java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(dataLen + 100);
			byteBufferReadCheck(@in, buf, 0);
			@in.close();
			// Default buffer size, initial buffer position is not 0
			@in = getInputStream(defaultBufferSize);
			buf.clear();
			byteBufferReadCheck(@in, buf, 11);
			@in.close();
			// Small buffer size, initial buffer position is 0
			@in = getInputStream(smallBufferSize);
			buf.clear();
			byteBufferReadCheck(@in, buf, 0);
			@in.close();
			// Small buffer size, initial buffer position is not 0
			@in = getInputStream(smallBufferSize);
			buf.clear();
			byteBufferReadCheck(@in, buf, 11);
			@in.close();
			// Direct buffer, default buffer size, initial buffer position is 0
			@in = getInputStream(defaultBufferSize);
			buf = java.nio.ByteBuffer.allocateDirect(dataLen + 100);
			byteBufferReadCheck(@in, buf, 0);
			@in.close();
			// Direct buffer, default buffer size, initial buffer position is not 0
			@in = getInputStream(defaultBufferSize);
			buf.clear();
			byteBufferReadCheck(@in, buf, 11);
			@in.close();
			// Direct buffer, small buffer size, initial buffer position is 0
			@in = getInputStream(smallBufferSize);
			buf.clear();
			byteBufferReadCheck(@in, buf, 0);
			@in.close();
			// Direct buffer, small buffer size, initial buffer position is not 0
			@in = getInputStream(smallBufferSize);
			buf.clear();
			byteBufferReadCheck(@in, buf, 11);
			@in.close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testCombinedOp()
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize);
			writeData(@out);
			int len1 = dataLen / 8;
			int len2 = dataLen / 10;
			java.io.InputStream @in = getInputStream(defaultBufferSize);
			// Read len1 data.
			byte[] readData = new byte[len1];
			readAll(@in, readData, 0, len1);
			byte[] expectedData = new byte[len1];
			System.Array.Copy(data, 0, expectedData, 0, len1);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
			long pos = ((org.apache.hadoop.fs.Seekable)@in).getPos();
			NUnit.Framework.Assert.AreEqual(len1, pos);
			// Seek forward len2
			((org.apache.hadoop.fs.Seekable)@in).seek(pos + len2);
			// Skip forward len2
			long n = @in.skip(len2);
			NUnit.Framework.Assert.AreEqual(len2, n);
			// Pos: 1/4 dataLen
			positionedReadCheck(@in, dataLen / 4);
			// Pos should be len1 + len2 + len2
			pos = ((org.apache.hadoop.fs.Seekable)@in).getPos();
			NUnit.Framework.Assert.AreEqual(len1 + len2 + len2, pos);
			// Read forward len1
			java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(len1);
			int nRead = ((org.apache.hadoop.fs.ByteBufferReadable)@in).read(buf);
			NUnit.Framework.Assert.AreEqual(nRead, buf.position());
			readData = new byte[nRead];
			buf.rewind();
			buf.get(readData);
			expectedData = new byte[nRead];
			System.Array.Copy(data, (int)pos, expectedData, 0, nRead);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
			long lastPos = pos;
			// Pos should be lastPos + nRead
			pos = ((org.apache.hadoop.fs.Seekable)@in).getPos();
			NUnit.Framework.Assert.AreEqual(lastPos + nRead, pos);
			// Pos: 1/3 dataLen
			positionedReadCheck(@in, dataLen / 3);
			// Read forward len1
			readData = new byte[len1];
			readAll(@in, readData, 0, len1);
			expectedData = new byte[len1];
			System.Array.Copy(data, (int)pos, expectedData, 0, len1);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
			lastPos = pos;
			// Pos should be lastPos + len1
			pos = ((org.apache.hadoop.fs.Seekable)@in).getPos();
			NUnit.Framework.Assert.AreEqual(lastPos + len1, pos);
			// Read forward len1
			buf = java.nio.ByteBuffer.allocate(len1);
			nRead = ((org.apache.hadoop.fs.ByteBufferReadable)@in).read(buf);
			NUnit.Framework.Assert.AreEqual(nRead, buf.position());
			readData = new byte[nRead];
			buf.rewind();
			buf.get(readData);
			expectedData = new byte[nRead];
			System.Array.Copy(data, (int)pos, expectedData, 0, nRead);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
			lastPos = pos;
			// Pos should be lastPos + nRead
			pos = ((org.apache.hadoop.fs.Seekable)@in).getPos();
			NUnit.Framework.Assert.AreEqual(lastPos + nRead, pos);
			// ByteBuffer read after EOF
			((org.apache.hadoop.fs.Seekable)@in).seek(dataLen);
			buf.clear();
			n = ((org.apache.hadoop.fs.ByteBufferReadable)@in).read(buf);
			NUnit.Framework.Assert.AreEqual(n, -1);
			@in.close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testSeekToNewSource()
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize);
			writeData(@out);
			java.io.InputStream @in = getInputStream(defaultBufferSize);
			int len1 = dataLen / 8;
			byte[] readData = new byte[len1];
			readAll(@in, readData, 0, len1);
			// Pos: 1/3 dataLen
			seekToNewSourceCheck(@in, dataLen / 3);
			// Pos: 0
			seekToNewSourceCheck(@in, 0);
			// Pos: 1/2 dataLen
			seekToNewSourceCheck(@in, dataLen / 2);
			// Pos: -3
			try
			{
				seekToNewSourceCheck(@in, -3);
				NUnit.Framework.Assert.Fail("Seek to negative offset should fail.");
			}
			catch (System.ArgumentException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Cannot seek to negative "
					 + "offset", e);
			}
			// Pos: dataLen + 3
			try
			{
				seekToNewSourceCheck(@in, dataLen + 3);
				NUnit.Framework.Assert.Fail("Seek after EOF should fail.");
			}
			catch (System.IO.IOException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Attempted to read past "
					 + "end of file", e);
			}
			@in.close();
		}

		/// <exception cref="System.Exception"/>
		private void seekToNewSourceCheck(java.io.InputStream @in, int targetPos)
		{
			byte[] result = new byte[dataLen];
			((org.apache.hadoop.fs.Seekable)@in).seekToNewSource(targetPos);
			int n = readAll(@in, result, 0, dataLen);
			NUnit.Framework.Assert.AreEqual(dataLen, n + targetPos);
			byte[] readData = new byte[n];
			System.Array.Copy(result, 0, readData, 0, n);
			byte[] expectedData = new byte[n];
			System.Array.Copy(data, targetPos, expectedData, 0, n);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
		}

		private org.apache.hadoop.io.ByteBufferPool getBufferPool()
		{
			return new _ByteBufferPool_681();
		}

		private sealed class _ByteBufferPool_681 : org.apache.hadoop.io.ByteBufferPool
		{
			public _ByteBufferPool_681()
			{
			}

			public java.nio.ByteBuffer getBuffer(bool direct, int length)
			{
				return java.nio.ByteBuffer.allocateDirect(length);
			}

			public void putBuffer(java.nio.ByteBuffer buffer)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testHasEnhancedByteBufferAccess()
		{
			java.io.OutputStream @out = getOutputStream(defaultBufferSize);
			writeData(@out);
			java.io.InputStream @in = getInputStream(defaultBufferSize);
			int len1 = dataLen / 8;
			// ByteBuffer size is len1
			java.nio.ByteBuffer buffer = ((org.apache.hadoop.fs.HasEnhancedByteBufferAccess)@in
				).read(getBufferPool(), len1, java.util.EnumSet.of(org.apache.hadoop.fs.ReadOption
				.SKIP_CHECKSUMS));
			int n1 = buffer.remaining();
			byte[] readData = new byte[n1];
			buffer.get(readData);
			byte[] expectedData = new byte[n1];
			System.Array.Copy(data, 0, expectedData, 0, n1);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
			((org.apache.hadoop.fs.HasEnhancedByteBufferAccess)@in).releaseBuffer(buffer);
			// Read len1 bytes
			readData = new byte[len1];
			readAll(@in, readData, 0, len1);
			expectedData = new byte[len1];
			System.Array.Copy(data, n1, expectedData, 0, len1);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
			// ByteBuffer size is len1
			buffer = ((org.apache.hadoop.fs.HasEnhancedByteBufferAccess)@in).read(getBufferPool
				(), len1, java.util.EnumSet.of(org.apache.hadoop.fs.ReadOption.SKIP_CHECKSUMS));
			int n2 = buffer.remaining();
			readData = new byte[n2];
			buffer.get(readData);
			expectedData = new byte[n2];
			System.Array.Copy(data, n1 + len1, expectedData, 0, n2);
			NUnit.Framework.Assert.assertArrayEquals(readData, expectedData);
			((org.apache.hadoop.fs.HasEnhancedByteBufferAccess)@in).releaseBuffer(buffer);
			@in.close();
		}
	}
}
