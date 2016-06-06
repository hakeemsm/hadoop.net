using System;
using System.IO;
using Com.Google.Common.Collect;
using Hadoop.Common.Core.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.Compress.Zlib
{
	public class TestZlibCompressorDecompressor
	{
		private static readonly Random random = new Random(12345L);

		[SetUp]
		public virtual void Before()
		{
			Assume.AssumeTrue(ZlibFactory.IsNativeZlibLoaded(new Configuration()));
		}

		[Fact]
		public virtual void TestZlibCompressorDecompressor()
		{
			try
			{
				int Size = 44 * 1024;
				byte[] rawData = Generate(Size);
				CompressDecompressTester.Of(rawData).WithCompressDecompressPair(new ZlibCompressor
					(), new ZlibDecompressor()).WithTestCases(ImmutableSet.Of(CompressDecompressTester.CompressionTestStrategy
					.CompressDecompressSingleBlock, CompressDecompressTester.CompressionTestStrategy
					.CompressDecompressBlock, CompressDecompressTester.CompressionTestStrategy.CompressDecompressErrors
					, CompressDecompressTester.CompressionTestStrategy.CompressDecompressWithEmptyStream
					)).Test();
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.Fail("testCompressorDecompressor error !!!" + ex);
			}
		}

		[Fact]
		public virtual void TestCompressorDecompressorWithExeedBufferLimit()
		{
			int ByteSize = 100 * 1024;
			byte[] rawData = Generate(ByteSize);
			try
			{
				CompressDecompressTester.Of(rawData).WithCompressDecompressPair(new ZlibCompressor
					(ZlibCompressor.CompressionLevel.BestCompression, ZlibCompressor.CompressionStrategy
					.DefaultStrategy, ZlibCompressor.CompressionHeader.DefaultHeader, ByteSize), new 
					ZlibDecompressor(ZlibDecompressor.CompressionHeader.DefaultHeader, ByteSize)).WithTestCases
					(ImmutableSet.Of(CompressDecompressTester.CompressionTestStrategy.CompressDecompressSingleBlock
					, CompressDecompressTester.CompressionTestStrategy.CompressDecompressBlock, CompressDecompressTester.CompressionTestStrategy
					.CompressDecompressErrors, CompressDecompressTester.CompressionTestStrategy.CompressDecompressWithEmptyStream
					)).Test();
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.Fail("testCompressorDecompressorWithExeedBufferLimit error !!!"
					 + ex);
			}
		}

		[Fact]
		public virtual void TestZlibCompressorDecompressorWithConfiguration()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, true);
			if (ZlibFactory.IsNativeZlibLoaded(conf))
			{
				byte[] rawData;
				int tryNumber = 5;
				int ByteSize = 10 * 1024;
				Compressor zlibCompressor = ZlibFactory.GetZlibCompressor(conf);
				Decompressor zlibDecompressor = ZlibFactory.GetZlibDecompressor(conf);
				rawData = Generate(ByteSize);
				try
				{
					for (int i = 0; i < tryNumber; i++)
					{
						CompressDecompressZlib(rawData, (ZlibCompressor)zlibCompressor, (ZlibDecompressor
							)zlibDecompressor);
					}
					zlibCompressor.Reinit(conf);
				}
				catch (Exception ex)
				{
					NUnit.Framework.Assert.Fail("testZlibCompressorDecompressorWithConfiguration ex error "
						 + ex);
				}
			}
			else
			{
				Assert.True("ZlibFactory is using native libs against request", 
					ZlibFactory.IsNativeZlibLoaded(conf));
			}
		}

		[Fact]
		public virtual void TestZlibCompressDecompress()
		{
			byte[] rawData = null;
			int rawDataSize = 0;
			rawDataSize = 1024 * 64;
			rawData = Generate(rawDataSize);
			try
			{
				ZlibCompressor compressor = new ZlibCompressor();
				ZlibDecompressor decompressor = new ZlibDecompressor();
				NUnit.Framework.Assert.IsFalse("testZlibCompressDecompress finished error", compressor
					.Finished());
				compressor.SetInput(rawData, 0, rawData.Length);
				Assert.True("testZlibCompressDecompress getBytesRead before error"
					, compressor.GetBytesRead() == 0);
				compressor.Finish();
				byte[] compressedResult = new byte[rawDataSize];
				int cSize = compressor.Compress(compressedResult, 0, rawDataSize);
				Assert.True("testZlibCompressDecompress getBytesRead ather error"
					, compressor.GetBytesRead() == rawDataSize);
				Assert.True("testZlibCompressDecompress compressed size no less then original size"
					, cSize < rawDataSize);
				decompressor.SetInput(compressedResult, 0, cSize);
				byte[] decompressedBytes = new byte[rawDataSize];
				decompressor.Decompress(decompressedBytes, 0, decompressedBytes.Length);
				Assert.AssertArrayEquals("testZlibCompressDecompress arrays not equals ", rawData
					, decompressedBytes);
				compressor.Reset();
				decompressor.Reset();
			}
			catch (IOException ex)
			{
				NUnit.Framework.Assert.Fail("testZlibCompressDecompress ex !!!" + ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CompressDecompressLoop(int rawDataSize)
		{
			byte[] rawData = null;
			rawData = Generate(rawDataSize);
			ByteArrayOutputStream baos = new ByteArrayOutputStream(rawDataSize + 12);
			DeflaterOutputStream dos = new DeflaterOutputStream(baos);
			dos.Write(rawData);
			dos.Flush();
			dos.Close();
			byte[] compressedResult = baos.ToByteArray();
			int compressedSize = compressedResult.Length;
			ZlibDecompressor.ZlibDirectDecompressor decompressor = new ZlibDecompressor.ZlibDirectDecompressor
				();
			ByteBuffer inBuf = ByteBuffer.AllocateDirect(compressedSize);
			ByteBuffer outBuf = ByteBuffer.AllocateDirect(rawDataSize);
			inBuf.Put(compressedResult, 0, compressedSize);
			inBuf.Flip();
			ByteBuffer expected = ByteBuffer.Wrap(rawData);
			outBuf.Clear();
			while (!decompressor.Finished())
			{
				decompressor.Decompress(inBuf, outBuf);
				if (outBuf.Remaining() == 0)
				{
					outBuf.Flip();
					while (outBuf.Remaining() > 0)
					{
						Assert.Equal(expected.Get(), outBuf.Get());
					}
					outBuf.Clear();
				}
			}
			outBuf.Flip();
			while (outBuf.Remaining() > 0)
			{
				Assert.Equal(expected.Get(), outBuf.Get());
			}
			outBuf.Clear();
			Assert.Equal(0, expected.Remaining());
		}

		[Fact]
		public virtual void TestZlibDirectCompressDecompress()
		{
			int[] size = new int[] { 1, 4, 16, 4 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024 };
			Assume.AssumeTrue(NativeCodeLoader.IsNativeCodeLoaded());
			try
			{
				for (int i = 0; i < size.Length; i++)
				{
					CompressDecompressLoop(size[i]);
				}
			}
			catch (IOException ex)
			{
				NUnit.Framework.Assert.Fail("testZlibDirectCompressDecompress ex !!!" + ex);
			}
		}

		[Fact]
		public virtual void TestZlibCompressorDecompressorSetDictionary()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, true);
			if (ZlibFactory.IsNativeZlibLoaded(conf))
			{
				Compressor zlibCompressor = ZlibFactory.GetZlibCompressor(conf);
				Decompressor zlibDecompressor = ZlibFactory.GetZlibDecompressor(conf);
				CheckSetDictionaryNullPointerException(zlibCompressor);
				CheckSetDictionaryNullPointerException(zlibDecompressor);
				CheckSetDictionaryArrayIndexOutOfBoundsException(zlibDecompressor);
				CheckSetDictionaryArrayIndexOutOfBoundsException(zlibCompressor);
			}
			else
			{
				Assert.True("ZlibFactory is using native libs against request", 
					ZlibFactory.IsNativeZlibLoaded(conf));
			}
		}

		[Fact]
		public virtual void TestZlibFactory()
		{
			Configuration cfg = new Configuration();
			Assert.True("testZlibFactory compression level error !!!", ZlibCompressor.CompressionLevel
				.DefaultCompression == ZlibFactory.GetCompressionLevel(cfg));
			Assert.True("testZlibFactory compression strategy error !!!", ZlibCompressor.CompressionStrategy
				.DefaultStrategy == ZlibFactory.GetCompressionStrategy(cfg));
			ZlibFactory.SetCompressionLevel(cfg, ZlibCompressor.CompressionLevel.BestCompression
				);
			Assert.True("testZlibFactory compression strategy error !!!", ZlibCompressor.CompressionLevel
				.BestCompression == ZlibFactory.GetCompressionLevel(cfg));
			ZlibFactory.SetCompressionStrategy(cfg, ZlibCompressor.CompressionStrategy.Filtered
				);
			Assert.True("testZlibFactory compression strategy error !!!", ZlibCompressor.CompressionStrategy
				.Filtered == ZlibFactory.GetCompressionStrategy(cfg));
		}

		private bool CheckSetDictionaryNullPointerException(Decompressor decompressor)
		{
			try
			{
				decompressor.SetDictionary(null, 0, 1);
			}
			catch (ArgumentNullException)
			{
				return true;
			}
			catch (Exception)
			{
			}
			return false;
		}

		private bool CheckSetDictionaryNullPointerException(Compressor compressor)
		{
			try
			{
				compressor.SetDictionary(null, 0, 1);
			}
			catch (ArgumentNullException)
			{
				return true;
			}
			catch (Exception)
			{
			}
			return false;
		}

		private bool CheckSetDictionaryArrayIndexOutOfBoundsException(Compressor compressor
			)
		{
			try
			{
				compressor.SetDictionary(new byte[] { unchecked((byte)0) }, 0, -1);
			}
			catch (IndexOutOfRangeException)
			{
				return true;
			}
			catch (Exception)
			{
			}
			return false;
		}

		private bool CheckSetDictionaryArrayIndexOutOfBoundsException(Decompressor decompressor
			)
		{
			try
			{
				decompressor.SetDictionary(new byte[] { unchecked((byte)0) }, 0, -1);
			}
			catch (IndexOutOfRangeException)
			{
				return true;
			}
			catch (Exception)
			{
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		private byte[] CompressDecompressZlib(byte[] rawData, ZlibCompressor zlibCompressor
			, ZlibDecompressor zlibDecompressor)
		{
			int cSize = 0;
			byte[] compressedByte = new byte[rawData.Length];
			byte[] decompressedRawData = new byte[rawData.Length];
			zlibCompressor.SetInput(rawData, 0, rawData.Length);
			zlibCompressor.Finish();
			while (!zlibCompressor.Finished())
			{
				cSize = zlibCompressor.Compress(compressedByte, 0, compressedByte.Length);
			}
			zlibCompressor.Reset();
			Assert.True(zlibDecompressor.GetBytesWritten() == 0);
			Assert.True(zlibDecompressor.GetBytesRead() == 0);
			Assert.True(zlibDecompressor.NeedsInput());
			zlibDecompressor.SetInput(compressedByte, 0, cSize);
			NUnit.Framework.Assert.IsFalse(zlibDecompressor.NeedsInput());
			while (!zlibDecompressor.Finished())
			{
				zlibDecompressor.Decompress(decompressedRawData, 0, decompressedRawData.Length);
			}
			Assert.True(zlibDecompressor.GetBytesWritten() == rawData.Length
				);
			Assert.True(zlibDecompressor.GetBytesRead() == cSize);
			zlibDecompressor.Reset();
			Assert.True(zlibDecompressor.GetRemaining() == 0);
			Assert.AssertArrayEquals("testZlibCompressorDecompressorWithConfiguration array equals error"
				, rawData, decompressedRawData);
			return decompressedRawData;
		}

		[Fact]
		public virtual void TestBuiltInGzipDecompressorExceptions()
		{
			BuiltInGzipDecompressor decompresser = new BuiltInGzipDecompressor();
			try
			{
				decompresser.SetInput(null, 0, 1);
			}
			catch (ArgumentNullException)
			{
			}
			catch (Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("testBuiltInGzipDecompressorExceptions npe error " + 
					ex);
			}
			try
			{
				decompresser.SetInput(new byte[] { 0 }, 0, -1);
			}
			catch (IndexOutOfRangeException)
			{
			}
			catch (Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("testBuiltInGzipDecompressorExceptions aioob error" +
					 ex);
			}
			Assert.True("decompresser.getBytesRead error", decompresser.GetBytesRead
				() == 0);
			Assert.True("decompresser.getRemaining error", decompresser.GetRemaining
				() == 0);
			decompresser.Reset();
			decompresser.End();
			InputStream decompStream = null;
			try
			{
				// invalid 0 and 1 bytes , must be 31, -117
				int buffSize = 1 * 1024;
				byte[] buffer = new byte[buffSize];
				Decompressor decompressor = new BuiltInGzipDecompressor();
				DataInputBuffer gzbuf = new DataInputBuffer();
				decompStream = new DecompressorStream(gzbuf, decompressor);
				gzbuf.Reset(new byte[] { 0, 0, 1, 1, 1, 1, 11, 1, 1, 1, 1 }, 11);
				decompStream.Read(buffer);
			}
			catch (IOException)
			{
			}
			catch (Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("invalid 0 and 1 byte in gzip stream" + ex);
			}
			// invalid 2 byte, must be 8
			try
			{
				int buffSize = 1 * 1024;
				byte[] buffer = new byte[buffSize];
				Decompressor decompressor = new BuiltInGzipDecompressor();
				DataInputBuffer gzbuf = new DataInputBuffer();
				decompStream = new DecompressorStream(gzbuf, decompressor);
				gzbuf.Reset(new byte[] { 31, unchecked((byte)(-117)), 7, 1, 1, 1, 1, 11, 1, 1, 1, 
					1 }, 11);
				decompStream.Read(buffer);
			}
			catch (IOException)
			{
			}
			catch (Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("invalid 2 byte in gzip stream" + ex);
			}
			try
			{
				int buffSize = 1 * 1024;
				byte[] buffer = new byte[buffSize];
				Decompressor decompressor = new BuiltInGzipDecompressor();
				DataInputBuffer gzbuf = new DataInputBuffer();
				decompStream = new DecompressorStream(gzbuf, decompressor);
				gzbuf.Reset(new byte[] { 31, unchecked((byte)(-117)), 8, unchecked((byte)(-32)), 
					1, 1, 1, 11, 1, 1, 1, 1 }, 11);
				decompStream.Read(buffer);
			}
			catch (IOException)
			{
			}
			catch (Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("invalid 3 byte in gzip stream" + ex);
			}
			try
			{
				int buffSize = 1 * 1024;
				byte[] buffer = new byte[buffSize];
				Decompressor decompressor = new BuiltInGzipDecompressor();
				DataInputBuffer gzbuf = new DataInputBuffer();
				decompStream = new DecompressorStream(gzbuf, decompressor);
				gzbuf.Reset(new byte[] { 31, unchecked((byte)(-117)), 8, 4, 1, 1, 1, 11, 1, 1, 1, 
					1 }, 11);
				decompStream.Read(buffer);
			}
			catch (IOException)
			{
			}
			catch (Exception ex)
			{
				// expected
				NUnit.Framework.Assert.Fail("invalid 3 byte make hasExtraField" + ex);
			}
		}

		public static byte[] Generate(int size)
		{
			byte[] data = new byte[size];
			for (int i = 0; i < size; i++)
			{
				data[i] = unchecked((byte)random.Next(16));
			}
			return data;
		}
	}
}
