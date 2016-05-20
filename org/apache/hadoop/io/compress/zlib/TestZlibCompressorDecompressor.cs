using Sharpen;

namespace org.apache.hadoop.io.compress.zlib
{
	public class TestZlibCompressorDecompressor
	{
		private static readonly java.util.Random random = new java.util.Random(12345L);

		[NUnit.Framework.SetUp]
		public virtual void before()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.io.compress.zlib.ZlibFactory.
				isNativeZlibLoaded(new org.apache.hadoop.conf.Configuration()));
		}

		[NUnit.Framework.Test]
		public virtual void testZlibCompressorDecompressor()
		{
			try
			{
				int SIZE = 44 * 1024;
				byte[] rawData = generate(SIZE);
				org.apache.hadoop.io.compress.CompressDecompressTester.of(rawData).withCompressDecompressPair
					(new org.apache.hadoop.io.compress.zlib.ZlibCompressor(), new org.apache.hadoop.io.compress.zlib.ZlibDecompressor
					()).withTestCases(com.google.common.collect.ImmutableSet.of(org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
					.COMPRESS_DECOMPRESS_SINGLE_BLOCK, org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
					.COMPRESS_DECOMPRESS_BLOCK, org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
					.COMPRESS_DECOMPRESS_ERRORS, org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
					.COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM)).test();
			}
			catch (System.Exception ex)
			{
				NUnit.Framework.Assert.Fail("testCompressorDecompressor error !!!" + ex);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testCompressorDecompressorWithExeedBufferLimit()
		{
			int BYTE_SIZE = 100 * 1024;
			byte[] rawData = generate(BYTE_SIZE);
			try
			{
				org.apache.hadoop.io.compress.CompressDecompressTester.of(rawData).withCompressDecompressPair
					(new org.apache.hadoop.io.compress.zlib.ZlibCompressor(org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
					.BEST_COMPRESSION, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
					.DEFAULT_STRATEGY, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader
					.DEFAULT_HEADER, BYTE_SIZE), new org.apache.hadoop.io.compress.zlib.ZlibDecompressor
					(org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader.DEFAULT_HEADER
					, BYTE_SIZE)).withTestCases(com.google.common.collect.ImmutableSet.of(org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
					.COMPRESS_DECOMPRESS_SINGLE_BLOCK, org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
					.COMPRESS_DECOMPRESS_BLOCK, org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
					.COMPRESS_DECOMPRESS_ERRORS, org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
					.COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM)).test();
			}
			catch (System.Exception ex)
			{
				NUnit.Framework.Assert.Fail("testCompressorDecompressorWithExeedBufferLimit error !!!"
					 + ex);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testZlibCompressorDecompressorWithConfiguration()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, true);
			if (org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf))
			{
				byte[] rawData;
				int tryNumber = 5;
				int BYTE_SIZE = 10 * 1024;
				org.apache.hadoop.io.compress.Compressor zlibCompressor = org.apache.hadoop.io.compress.zlib.ZlibFactory
					.getZlibCompressor(conf);
				org.apache.hadoop.io.compress.Decompressor zlibDecompressor = org.apache.hadoop.io.compress.zlib.ZlibFactory
					.getZlibDecompressor(conf);
				rawData = generate(BYTE_SIZE);
				try
				{
					for (int i = 0; i < tryNumber; i++)
					{
						compressDecompressZlib(rawData, (org.apache.hadoop.io.compress.zlib.ZlibCompressor
							)zlibCompressor, (org.apache.hadoop.io.compress.zlib.ZlibDecompressor)zlibDecompressor
							);
					}
					zlibCompressor.reinit(conf);
				}
				catch (System.Exception ex)
				{
					NUnit.Framework.Assert.Fail("testZlibCompressorDecompressorWithConfiguration ex error "
						 + ex);
				}
			}
			else
			{
				NUnit.Framework.Assert.IsTrue("ZlibFactory is using native libs against request", 
					org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf));
			}
		}

		[NUnit.Framework.Test]
		public virtual void testZlibCompressDecompress()
		{
			byte[] rawData = null;
			int rawDataSize = 0;
			rawDataSize = 1024 * 64;
			rawData = generate(rawDataSize);
			try
			{
				org.apache.hadoop.io.compress.zlib.ZlibCompressor compressor = new org.apache.hadoop.io.compress.zlib.ZlibCompressor
					();
				org.apache.hadoop.io.compress.zlib.ZlibDecompressor decompressor = new org.apache.hadoop.io.compress.zlib.ZlibDecompressor
					();
				NUnit.Framework.Assert.IsFalse("testZlibCompressDecompress finished error", compressor
					.finished());
				compressor.setInput(rawData, 0, rawData.Length);
				NUnit.Framework.Assert.IsTrue("testZlibCompressDecompress getBytesRead before error"
					, compressor.getBytesRead() == 0);
				compressor.finish();
				byte[] compressedResult = new byte[rawDataSize];
				int cSize = compressor.compress(compressedResult, 0, rawDataSize);
				NUnit.Framework.Assert.IsTrue("testZlibCompressDecompress getBytesRead ather error"
					, compressor.getBytesRead() == rawDataSize);
				NUnit.Framework.Assert.IsTrue("testZlibCompressDecompress compressed size no less then original size"
					, cSize < rawDataSize);
				decompressor.setInput(compressedResult, 0, cSize);
				byte[] decompressedBytes = new byte[rawDataSize];
				decompressor.decompress(decompressedBytes, 0, decompressedBytes.Length);
				NUnit.Framework.Assert.assertArrayEquals("testZlibCompressDecompress arrays not equals "
					, rawData, decompressedBytes);
				compressor.reset();
				decompressor.reset();
			}
			catch (System.IO.IOException ex)
			{
				NUnit.Framework.Assert.Fail("testZlibCompressDecompress ex !!!" + ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void compressDecompressLoop(int rawDataSize)
		{
			byte[] rawData = null;
			rawData = generate(rawDataSize);
			java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream(rawDataSize
				 + 12);
			java.util.zip.DeflaterOutputStream dos = new java.util.zip.DeflaterOutputStream(baos
				);
			dos.write(rawData);
			dos.flush();
			dos.close();
			byte[] compressedResult = baos.toByteArray();
			int compressedSize = compressedResult.Length;
			org.apache.hadoop.io.compress.zlib.ZlibDecompressor.ZlibDirectDecompressor decompressor
				 = new org.apache.hadoop.io.compress.zlib.ZlibDecompressor.ZlibDirectDecompressor
				();
			java.nio.ByteBuffer inBuf = java.nio.ByteBuffer.allocateDirect(compressedSize);
			java.nio.ByteBuffer outBuf = java.nio.ByteBuffer.allocateDirect(rawDataSize);
			inBuf.put(compressedResult, 0, compressedSize);
			inBuf.flip();
			java.nio.ByteBuffer expected = java.nio.ByteBuffer.wrap(rawData);
			outBuf.clear();
			while (!decompressor.finished())
			{
				decompressor.decompress(inBuf, outBuf);
				if (outBuf.remaining() == 0)
				{
					outBuf.flip();
					while (outBuf.remaining() > 0)
					{
						NUnit.Framework.Assert.AreEqual(expected.get(), outBuf.get());
					}
					outBuf.clear();
				}
			}
			outBuf.flip();
			while (outBuf.remaining() > 0)
			{
				NUnit.Framework.Assert.AreEqual(expected.get(), outBuf.get());
			}
			outBuf.clear();
			NUnit.Framework.Assert.AreEqual(0, expected.remaining());
		}

		[NUnit.Framework.Test]
		public virtual void testZlibDirectCompressDecompress()
		{
			int[] size = new int[] { 1, 4, 16, 4 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024 };
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded
				());
			try
			{
				for (int i = 0; i < size.Length; i++)
				{
					compressDecompressLoop(size[i]);
				}
			}
			catch (System.IO.IOException ex)
			{
				NUnit.Framework.Assert.Fail("testZlibDirectCompressDecompress ex !!!" + ex);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testZlibCompressorDecompressorSetDictionary()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, true);
			if (org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf))
			{
				org.apache.hadoop.io.compress.Compressor zlibCompressor = org.apache.hadoop.io.compress.zlib.ZlibFactory
					.getZlibCompressor(conf);
				org.apache.hadoop.io.compress.Decompressor zlibDecompressor = org.apache.hadoop.io.compress.zlib.ZlibFactory
					.getZlibDecompressor(conf);
				checkSetDictionaryNullPointerException(zlibCompressor);
				checkSetDictionaryNullPointerException(zlibDecompressor);
				checkSetDictionaryArrayIndexOutOfBoundsException(zlibDecompressor);
				checkSetDictionaryArrayIndexOutOfBoundsException(zlibCompressor);
			}
			else
			{
				NUnit.Framework.Assert.IsTrue("ZlibFactory is using native libs against request", 
					org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf));
			}
		}

		[NUnit.Framework.Test]
		public virtual void testZlibFactory()
		{
			org.apache.hadoop.conf.Configuration cfg = new org.apache.hadoop.conf.Configuration
				();
			NUnit.Framework.Assert.IsTrue("testZlibFactory compression level error !!!", org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				.DEFAULT_COMPRESSION == org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionLevel
				(cfg));
			NUnit.Framework.Assert.IsTrue("testZlibFactory compression strategy error !!!", org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				.DEFAULT_STRATEGY == org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionStrategy
				(cfg));
			org.apache.hadoop.io.compress.zlib.ZlibFactory.setCompressionLevel(cfg, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				.BEST_COMPRESSION);
			NUnit.Framework.Assert.IsTrue("testZlibFactory compression strategy error !!!", org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				.BEST_COMPRESSION == org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionLevel
				(cfg));
			org.apache.hadoop.io.compress.zlib.ZlibFactory.setCompressionStrategy(cfg, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				.FILTERED);
			NUnit.Framework.Assert.IsTrue("testZlibFactory compression strategy error !!!", org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				.FILTERED == org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionStrategy
				(cfg));
		}

		private bool checkSetDictionaryNullPointerException(org.apache.hadoop.io.compress.Decompressor
			 decompressor)
		{
			try
			{
				decompressor.setDictionary(null, 0, 1);
			}
			catch (System.ArgumentNullException)
			{
				return true;
			}
			catch (System.Exception)
			{
			}
			return false;
		}

		private bool checkSetDictionaryNullPointerException(org.apache.hadoop.io.compress.Compressor
			 compressor)
		{
			try
			{
				compressor.setDictionary(null, 0, 1);
			}
			catch (System.ArgumentNullException)
			{
				return true;
			}
			catch (System.Exception)
			{
			}
			return false;
		}

		private bool checkSetDictionaryArrayIndexOutOfBoundsException(org.apache.hadoop.io.compress.Compressor
			 compressor)
		{
			try
			{
				compressor.setDictionary(new byte[] { unchecked((byte)0) }, 0, -1);
			}
			catch (System.IndexOutOfRangeException)
			{
				return true;
			}
			catch (System.Exception)
			{
			}
			return false;
		}

		private bool checkSetDictionaryArrayIndexOutOfBoundsException(org.apache.hadoop.io.compress.Decompressor
			 decompressor)
		{
			try
			{
				decompressor.setDictionary(new byte[] { unchecked((byte)0) }, 0, -1);
			}
			catch (System.IndexOutOfRangeException)
			{
				return true;
			}
			catch (System.Exception)
			{
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		private byte[] compressDecompressZlib(byte[] rawData, org.apache.hadoop.io.compress.zlib.ZlibCompressor
			 zlibCompressor, org.apache.hadoop.io.compress.zlib.ZlibDecompressor zlibDecompressor
			)
		{
			int cSize = 0;
			byte[] compressedByte = new byte[rawData.Length];
			byte[] decompressedRawData = new byte[rawData.Length];
			zlibCompressor.setInput(rawData, 0, rawData.Length);
			zlibCompressor.finish();
			while (!zlibCompressor.finished())
			{
				cSize = zlibCompressor.compress(compressedByte, 0, compressedByte.Length);
			}
			zlibCompressor.reset();
			NUnit.Framework.Assert.IsTrue(zlibDecompressor.getBytesWritten() == 0);
			NUnit.Framework.Assert.IsTrue(zlibDecompressor.getBytesRead() == 0);
			NUnit.Framework.Assert.IsTrue(zlibDecompressor.needsInput());
			zlibDecompressor.setInput(compressedByte, 0, cSize);
			NUnit.Framework.Assert.IsFalse(zlibDecompressor.needsInput());
			while (!zlibDecompressor.finished())
			{
				zlibDecompressor.decompress(decompressedRawData, 0, decompressedRawData.Length);
			}
			NUnit.Framework.Assert.IsTrue(zlibDecompressor.getBytesWritten() == rawData.Length
				);
			NUnit.Framework.Assert.IsTrue(zlibDecompressor.getBytesRead() == cSize);
			zlibDecompressor.reset();
			NUnit.Framework.Assert.IsTrue(zlibDecompressor.getRemaining() == 0);
			NUnit.Framework.Assert.assertArrayEquals("testZlibCompressorDecompressorWithConfiguration array equals error"
				, rawData, decompressedRawData);
			return decompressedRawData;
		}

		[NUnit.Framework.Test]
		public virtual void testBuiltInGzipDecompressorExceptions()
		{
			org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor decompresser = new org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor
				();
			try
			{
				decompresser.setInput(null, 0, 1);
			}
			catch (System.ArgumentNullException)
			{
			}
			catch (System.Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("testBuiltInGzipDecompressorExceptions npe error " + 
					ex);
			}
			try
			{
				decompresser.setInput(new byte[] { 0 }, 0, -1);
			}
			catch (System.IndexOutOfRangeException)
			{
			}
			catch (System.Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("testBuiltInGzipDecompressorExceptions aioob error" +
					 ex);
			}
			NUnit.Framework.Assert.IsTrue("decompresser.getBytesRead error", decompresser.getBytesRead
				() == 0);
			NUnit.Framework.Assert.IsTrue("decompresser.getRemaining error", decompresser.getRemaining
				() == 0);
			decompresser.reset();
			decompresser.end();
			java.io.InputStream decompStream = null;
			try
			{
				// invalid 0 and 1 bytes , must be 31, -117
				int buffSize = 1 * 1024;
				byte[] buffer = new byte[buffSize];
				org.apache.hadoop.io.compress.Decompressor decompressor = new org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor
					();
				org.apache.hadoop.io.DataInputBuffer gzbuf = new org.apache.hadoop.io.DataInputBuffer
					();
				decompStream = new org.apache.hadoop.io.compress.DecompressorStream(gzbuf, decompressor
					);
				gzbuf.reset(new byte[] { 0, 0, 1, 1, 1, 1, 11, 1, 1, 1, 1 }, 11);
				decompStream.read(buffer);
			}
			catch (System.IO.IOException)
			{
			}
			catch (System.Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("invalid 0 and 1 byte in gzip stream" + ex);
			}
			// invalid 2 byte, must be 8
			try
			{
				int buffSize = 1 * 1024;
				byte[] buffer = new byte[buffSize];
				org.apache.hadoop.io.compress.Decompressor decompressor = new org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor
					();
				org.apache.hadoop.io.DataInputBuffer gzbuf = new org.apache.hadoop.io.DataInputBuffer
					();
				decompStream = new org.apache.hadoop.io.compress.DecompressorStream(gzbuf, decompressor
					);
				gzbuf.reset(new byte[] { 31, unchecked((byte)(-117)), 7, 1, 1, 1, 1, 11, 1, 1, 1, 
					1 }, 11);
				decompStream.read(buffer);
			}
			catch (System.IO.IOException)
			{
			}
			catch (System.Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("invalid 2 byte in gzip stream" + ex);
			}
			try
			{
				int buffSize = 1 * 1024;
				byte[] buffer = new byte[buffSize];
				org.apache.hadoop.io.compress.Decompressor decompressor = new org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor
					();
				org.apache.hadoop.io.DataInputBuffer gzbuf = new org.apache.hadoop.io.DataInputBuffer
					();
				decompStream = new org.apache.hadoop.io.compress.DecompressorStream(gzbuf, decompressor
					);
				gzbuf.reset(new byte[] { 31, unchecked((byte)(-117)), 8, unchecked((byte)(-32)), 
					1, 1, 1, 11, 1, 1, 1, 1 }, 11);
				decompStream.read(buffer);
			}
			catch (System.IO.IOException)
			{
			}
			catch (System.Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("invalid 3 byte in gzip stream" + ex);
			}
			try
			{
				int buffSize = 1 * 1024;
				byte[] buffer = new byte[buffSize];
				org.apache.hadoop.io.compress.Decompressor decompressor = new org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor
					();
				org.apache.hadoop.io.DataInputBuffer gzbuf = new org.apache.hadoop.io.DataInputBuffer
					();
				decompStream = new org.apache.hadoop.io.compress.DecompressorStream(gzbuf, decompressor
					);
				gzbuf.reset(new byte[] { 31, unchecked((byte)(-117)), 8, 4, 1, 1, 1, 11, 1, 1, 1, 
					1 }, 11);
				decompStream.read(buffer);
			}
			catch (System.IO.IOException)
			{
			}
			catch (System.Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("invalid 3 byte make hasExtraField" + ex);
			}
		}

		public static byte[] generate(int size)
		{
			byte[] data = new byte[size];
			for (int i = 0; i < size; i++)
			{
				data[i] = unchecked((byte)random.nextInt(16));
			}
			return data;
		}
	}
}
