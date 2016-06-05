using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.IO.Compress.Snappy
{
	public class TestSnappyCompressorDecompressor
	{
		[SetUp]
		public virtual void Before()
		{
			Assume.AssumeTrue(SnappyCodec.IsNativeCodeLoaded());
		}

		[Fact]
		public virtual void TestSnappyCompressorSetInputNullPointerException()
		{
			try
			{
				SnappyCompressor compressor = new SnappyCompressor();
				compressor.SetInput(null, 0, 10);
				NUnit.Framework.Assert.Fail("testSnappyCompressorSetInputNullPointerException error !!!"
					);
			}
			catch (ArgumentNullException)
			{
			}
			catch (Exception)
			{
				// excepted
				NUnit.Framework.Assert.Fail("testSnappyCompressorSetInputNullPointerException ex error !!!"
					);
			}
		}

		[Fact]
		public virtual void TestSnappyDecompressorSetInputNullPointerException()
		{
			try
			{
				SnappyDecompressor decompressor = new SnappyDecompressor();
				decompressor.SetInput(null, 0, 10);
				NUnit.Framework.Assert.Fail("testSnappyDecompressorSetInputNullPointerException error !!!"
					);
			}
			catch (ArgumentNullException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyDecompressorSetInputNullPointerException ex error !!!"
					);
			}
		}

		[Fact]
		public virtual void TestSnappyCompressorSetInputAIOBException()
		{
			try
			{
				SnappyCompressor compressor = new SnappyCompressor();
				compressor.SetInput(new byte[] {  }, -5, 10);
				NUnit.Framework.Assert.Fail("testSnappyCompressorSetInputAIOBException error !!!"
					);
			}
			catch (IndexOutOfRangeException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyCompressorSetInputAIOBException ex error !!!"
					);
			}
		}

		[Fact]
		public virtual void TestSnappyDecompressorSetInputAIOUBException()
		{
			try
			{
				SnappyDecompressor decompressor = new SnappyDecompressor();
				decompressor.SetInput(new byte[] {  }, -5, 10);
				NUnit.Framework.Assert.Fail("testSnappyDecompressorSetInputAIOUBException error !!!"
					);
			}
			catch (IndexOutOfRangeException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyDecompressorSetInputAIOUBException ex error !!!"
					);
			}
		}

		[Fact]
		public virtual void TestSnappyCompressorCompressNullPointerException()
		{
			try
			{
				SnappyCompressor compressor = new SnappyCompressor();
				byte[] bytes = TestSnappyCompressorDecompressor.BytesGenerator.Get(1024 * 6);
				compressor.SetInput(bytes, 0, bytes.Length);
				compressor.Compress(null, 0, 0);
				NUnit.Framework.Assert.Fail("testSnappyCompressorCompressNullPointerException error !!!"
					);
			}
			catch (ArgumentNullException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyCompressorCompressNullPointerException ex error !!!"
					);
			}
		}

		[Fact]
		public virtual void TestSnappyDecompressorCompressNullPointerException()
		{
			try
			{
				SnappyDecompressor decompressor = new SnappyDecompressor();
				byte[] bytes = TestSnappyCompressorDecompressor.BytesGenerator.Get(1024 * 6);
				decompressor.SetInput(bytes, 0, bytes.Length);
				decompressor.Decompress(null, 0, 0);
				NUnit.Framework.Assert.Fail("testSnappyDecompressorCompressNullPointerException error !!!"
					);
			}
			catch (ArgumentNullException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyDecompressorCompressNullPointerException ex error !!!"
					);
			}
		}

		[Fact]
		public virtual void TestSnappyCompressorCompressAIOBException()
		{
			try
			{
				SnappyCompressor compressor = new SnappyCompressor();
				byte[] bytes = TestSnappyCompressorDecompressor.BytesGenerator.Get(1024 * 6);
				compressor.SetInput(bytes, 0, bytes.Length);
				compressor.Compress(new byte[] {  }, 0, -1);
				NUnit.Framework.Assert.Fail("testSnappyCompressorCompressAIOBException error !!!"
					);
			}
			catch (IndexOutOfRangeException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyCompressorCompressAIOBException ex error !!!"
					);
			}
		}

		[Fact]
		public virtual void TestSnappyDecompressorCompressAIOBException()
		{
			try
			{
				SnappyDecompressor decompressor = new SnappyDecompressor();
				byte[] bytes = TestSnappyCompressorDecompressor.BytesGenerator.Get(1024 * 6);
				decompressor.SetInput(bytes, 0, bytes.Length);
				decompressor.Decompress(new byte[] {  }, 0, -1);
				NUnit.Framework.Assert.Fail("testSnappyDecompressorCompressAIOBException error !!!"
					);
			}
			catch (IndexOutOfRangeException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyDecompressorCompressAIOBException ex error !!!"
					);
			}
		}

		[Fact]
		public virtual void TestSnappyCompressDecompress()
		{
			int ByteSize = 1024 * 54;
			byte[] bytes = TestSnappyCompressorDecompressor.BytesGenerator.Get(ByteSize);
			SnappyCompressor compressor = new SnappyCompressor();
			try
			{
				compressor.SetInput(bytes, 0, bytes.Length);
				Assert.True("SnappyCompressDecompress getBytesRead error !!!", 
					compressor.GetBytesRead() > 0);
				Assert.True("SnappyCompressDecompress getBytesWritten before compress error !!!"
					, compressor.GetBytesWritten() == 0);
				byte[] compressed = new byte[ByteSize];
				int cSize = compressor.Compress(compressed, 0, compressed.Length);
				Assert.True("SnappyCompressDecompress getBytesWritten after compress error !!!"
					, compressor.GetBytesWritten() > 0);
				SnappyDecompressor decompressor = new SnappyDecompressor(ByteSize);
				// set as input for decompressor only compressed data indicated with cSize
				decompressor.SetInput(compressed, 0, cSize);
				byte[] decompressed = new byte[ByteSize];
				decompressor.Decompress(decompressed, 0, decompressed.Length);
				Assert.True("testSnappyCompressDecompress finished error !!!", 
					decompressor.Finished());
				Assert.AssertArrayEquals(bytes, decompressed);
				compressor.Reset();
				decompressor.Reset();
				Assert.True("decompressor getRemaining error !!!", decompressor
					.GetRemaining() == 0);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("testSnappyCompressDecompress ex error!!!");
			}
		}

		[Fact]
		public virtual void TestCompressorDecompressorEmptyStreamLogic()
		{
			ByteArrayInputStream bytesIn = null;
			ByteArrayOutputStream bytesOut = null;
			byte[] buf = null;
			BlockDecompressorStream blockDecompressorStream = null;
			try
			{
				// compress empty stream
				bytesOut = new ByteArrayOutputStream();
				BlockCompressorStream blockCompressorStream = new BlockCompressorStream(bytesOut, 
					new SnappyCompressor(), 1024, 0);
				// close without write
				blockCompressorStream.Close();
				// check compressed output
				buf = bytesOut.ToByteArray();
				Assert.Equal("empty stream compressed output size != 4", 4, buf
					.Length);
				// use compressed output as input for decompression
				bytesIn = new ByteArrayInputStream(buf);
				// create decompression stream
				blockDecompressorStream = new BlockDecompressorStream(bytesIn, new SnappyDecompressor
					(), 1024);
				// no byte is available because stream was closed
				Assert.Equal("return value is not -1", -1, blockDecompressorStream
					.Read());
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("testCompressorDecompressorEmptyStreamLogic ex error !!!"
					 + e.Message);
			}
			finally
			{
				if (blockDecompressorStream != null)
				{
					try
					{
						bytesIn.Close();
						bytesOut.Close();
						blockDecompressorStream.Close();
					}
					catch (IOException)
					{
					}
				}
			}
		}

		[Fact]
		public virtual void TestSnappyBlockCompression()
		{
			int ByteSize = 1024 * 50;
			int BlockSize = 512;
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			byte[] block = new byte[BlockSize];
			byte[] bytes = TestSnappyCompressorDecompressor.BytesGenerator.Get(ByteSize);
			try
			{
				// Use default of 512 as bufferSize and compressionOverhead of
				// (1% of bufferSize + 12 bytes) = 18 bytes (zlib algorithm).
				SnappyCompressor compressor = new SnappyCompressor();
				int off = 0;
				int len = ByteSize;
				int maxSize = BlockSize - 18;
				if (ByteSize > maxSize)
				{
					do
					{
						int bufLen = Math.Min(len, maxSize);
						compressor.SetInput(bytes, off, bufLen);
						compressor.Finish();
						while (!compressor.Finished())
						{
							compressor.Compress(block, 0, block.Length);
							@out.Write(block);
						}
						compressor.Reset();
						off += bufLen;
						len -= bufLen;
					}
					while (len > 0);
				}
				Assert.True("testSnappyBlockCompression error !!!", @out.ToByteArray
					().Length > 0);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("testSnappyBlockCompression ex error !!!");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CompressDecompressLoop(int rawDataSize)
		{
			byte[] rawData = TestSnappyCompressorDecompressor.BytesGenerator.Get(rawDataSize);
			byte[] compressedResult = new byte[rawDataSize + 20];
			int directBufferSize = Math.Max(rawDataSize * 2, 64 * 1024);
			SnappyCompressor compressor = new SnappyCompressor(directBufferSize);
			compressor.SetInput(rawData, 0, rawDataSize);
			int compressedSize = compressor.Compress(compressedResult, 0, compressedResult.Length
				);
			SnappyDecompressor.SnappyDirectDecompressor decompressor = new SnappyDecompressor.SnappyDirectDecompressor
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
		public virtual void TestSnappyDirectBlockCompression()
		{
			int[] size = new int[] { 4 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024 };
			Assume.AssumeTrue(SnappyCodec.IsNativeCodeLoaded());
			try
			{
				for (int i = 0; i < size.Length; i++)
				{
					CompressDecompressLoop(size[i]);
				}
			}
			catch (IOException ex)
			{
				NUnit.Framework.Assert.Fail("testSnappyDirectBlockCompression ex !!!" + ex);
			}
		}

		[Fact]
		public virtual void TestSnappyCompressorDecopressorLogicWithCompressionStreams()
		{
			int ByteSize = 1024 * 100;
			byte[] bytes = TestSnappyCompressorDecompressor.BytesGenerator.Get(ByteSize);
			int bufferSize = 262144;
			int compressionOverhead = (bufferSize / 6) + 32;
			DataOutputStream deflateOut = null;
			DataInputStream inflateIn = null;
			try
			{
				DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
				CompressionOutputStream deflateFilter = new BlockCompressorStream(compressedDataBuffer
					, new SnappyCompressor(bufferSize), bufferSize, compressionOverhead);
				deflateOut = new DataOutputStream(new BufferedOutputStream(deflateFilter));
				deflateOut.Write(bytes, 0, bytes.Length);
				deflateOut.Flush();
				deflateFilter.Finish();
				DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
				deCompressedDataBuffer.Reset(compressedDataBuffer.GetData(), 0, compressedDataBuffer
					.GetLength());
				CompressionInputStream inflateFilter = new BlockDecompressorStream(deCompressedDataBuffer
					, new SnappyDecompressor(bufferSize), bufferSize);
				inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));
				byte[] result = new byte[ByteSize];
				inflateIn.Read(result);
				Assert.AssertArrayEquals("original array not equals compress/decompressed array", 
					result, bytes);
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.Fail("testSnappyCompressorDecopressorLogicWithCompressionStreams ex error !!!"
					);
			}
			finally
			{
				try
				{
					if (deflateOut != null)
					{
						deflateOut.Close();
					}
					if (inflateIn != null)
					{
						inflateIn.Close();
					}
				}
				catch (Exception)
				{
				}
			}
		}

		internal sealed class BytesGenerator
		{
			private BytesGenerator()
			{
			}

			private static readonly byte[] Cache = new byte[] { unchecked((int)(0x0)), unchecked(
				(int)(0x1)), unchecked((int)(0x2)), unchecked((int)(0x3)), unchecked((int)(0x4))
				, unchecked((int)(0x5)), unchecked((int)(0x6)), unchecked((int)(0x7)), unchecked(
				(int)(0x8)), unchecked((int)(0x9)), unchecked((int)(0xA)), unchecked((int)(0xB))
				, unchecked((int)(0xC)), unchecked((int)(0xD)), unchecked((int)(0xE)), unchecked(
				(int)(0xF)) };

			private static readonly Random rnd = new Random(12345l);

			public static byte[] Get(int size)
			{
				byte[] array = (byte[])System.Array.CreateInstance(typeof(byte), size);
				for (int i = 0; i < size; i++)
				{
					array[i] = Cache[rnd.Next(Cache.Length - 1)];
				}
				return array;
			}
		}
	}
}
