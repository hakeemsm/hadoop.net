using Sharpen;

namespace org.apache.hadoop.io.compress.snappy
{
	public class TestSnappyCompressorDecompressor
	{
		[NUnit.Framework.SetUp]
		public virtual void before()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.io.compress.SnappyCodec.isNativeCodeLoaded
				());
		}

		[NUnit.Framework.Test]
		public virtual void testSnappyCompressorSetInputNullPointerException()
		{
			try
			{
				org.apache.hadoop.io.compress.snappy.SnappyCompressor compressor = new org.apache.hadoop.io.compress.snappy.SnappyCompressor
					();
				compressor.setInput(null, 0, 10);
				NUnit.Framework.Assert.Fail("testSnappyCompressorSetInputNullPointerException error !!!"
					);
			}
			catch (System.ArgumentNullException)
			{
			}
			catch (System.Exception)
			{
				// excepted
				NUnit.Framework.Assert.Fail("testSnappyCompressorSetInputNullPointerException ex error !!!"
					);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testSnappyDecompressorSetInputNullPointerException()
		{
			try
			{
				org.apache.hadoop.io.compress.snappy.SnappyDecompressor decompressor = new org.apache.hadoop.io.compress.snappy.SnappyDecompressor
					();
				decompressor.setInput(null, 0, 10);
				NUnit.Framework.Assert.Fail("testSnappyDecompressorSetInputNullPointerException error !!!"
					);
			}
			catch (System.ArgumentNullException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyDecompressorSetInputNullPointerException ex error !!!"
					);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testSnappyCompressorSetInputAIOBException()
		{
			try
			{
				org.apache.hadoop.io.compress.snappy.SnappyCompressor compressor = new org.apache.hadoop.io.compress.snappy.SnappyCompressor
					();
				compressor.setInput(new byte[] {  }, -5, 10);
				NUnit.Framework.Assert.Fail("testSnappyCompressorSetInputAIOBException error !!!"
					);
			}
			catch (System.IndexOutOfRangeException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyCompressorSetInputAIOBException ex error !!!"
					);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testSnappyDecompressorSetInputAIOUBException()
		{
			try
			{
				org.apache.hadoop.io.compress.snappy.SnappyDecompressor decompressor = new org.apache.hadoop.io.compress.snappy.SnappyDecompressor
					();
				decompressor.setInput(new byte[] {  }, -5, 10);
				NUnit.Framework.Assert.Fail("testSnappyDecompressorSetInputAIOUBException error !!!"
					);
			}
			catch (System.IndexOutOfRangeException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyDecompressorSetInputAIOUBException ex error !!!"
					);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testSnappyCompressorCompressNullPointerException()
		{
			try
			{
				org.apache.hadoop.io.compress.snappy.SnappyCompressor compressor = new org.apache.hadoop.io.compress.snappy.SnappyCompressor
					();
				byte[] bytes = org.apache.hadoop.io.compress.snappy.TestSnappyCompressorDecompressor.BytesGenerator
					.get(1024 * 6);
				compressor.setInput(bytes, 0, bytes.Length);
				compressor.compress(null, 0, 0);
				NUnit.Framework.Assert.Fail("testSnappyCompressorCompressNullPointerException error !!!"
					);
			}
			catch (System.ArgumentNullException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyCompressorCompressNullPointerException ex error !!!"
					);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testSnappyDecompressorCompressNullPointerException()
		{
			try
			{
				org.apache.hadoop.io.compress.snappy.SnappyDecompressor decompressor = new org.apache.hadoop.io.compress.snappy.SnappyDecompressor
					();
				byte[] bytes = org.apache.hadoop.io.compress.snappy.TestSnappyCompressorDecompressor.BytesGenerator
					.get(1024 * 6);
				decompressor.setInput(bytes, 0, bytes.Length);
				decompressor.decompress(null, 0, 0);
				NUnit.Framework.Assert.Fail("testSnappyDecompressorCompressNullPointerException error !!!"
					);
			}
			catch (System.ArgumentNullException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyDecompressorCompressNullPointerException ex error !!!"
					);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testSnappyCompressorCompressAIOBException()
		{
			try
			{
				org.apache.hadoop.io.compress.snappy.SnappyCompressor compressor = new org.apache.hadoop.io.compress.snappy.SnappyCompressor
					();
				byte[] bytes = org.apache.hadoop.io.compress.snappy.TestSnappyCompressorDecompressor.BytesGenerator
					.get(1024 * 6);
				compressor.setInput(bytes, 0, bytes.Length);
				compressor.compress(new byte[] {  }, 0, -1);
				NUnit.Framework.Assert.Fail("testSnappyCompressorCompressAIOBException error !!!"
					);
			}
			catch (System.IndexOutOfRangeException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyCompressorCompressAIOBException ex error !!!"
					);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testSnappyDecompressorCompressAIOBException()
		{
			try
			{
				org.apache.hadoop.io.compress.snappy.SnappyDecompressor decompressor = new org.apache.hadoop.io.compress.snappy.SnappyDecompressor
					();
				byte[] bytes = org.apache.hadoop.io.compress.snappy.TestSnappyCompressorDecompressor.BytesGenerator
					.get(1024 * 6);
				decompressor.setInput(bytes, 0, bytes.Length);
				decompressor.decompress(new byte[] {  }, 0, -1);
				NUnit.Framework.Assert.Fail("testSnappyDecompressorCompressAIOBException error !!!"
					);
			}
			catch (System.IndexOutOfRangeException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testSnappyDecompressorCompressAIOBException ex error !!!"
					);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testSnappyCompressDecompress()
		{
			int BYTE_SIZE = 1024 * 54;
			byte[] bytes = org.apache.hadoop.io.compress.snappy.TestSnappyCompressorDecompressor.BytesGenerator
				.get(BYTE_SIZE);
			org.apache.hadoop.io.compress.snappy.SnappyCompressor compressor = new org.apache.hadoop.io.compress.snappy.SnappyCompressor
				();
			try
			{
				compressor.setInput(bytes, 0, bytes.Length);
				NUnit.Framework.Assert.IsTrue("SnappyCompressDecompress getBytesRead error !!!", 
					compressor.getBytesRead() > 0);
				NUnit.Framework.Assert.IsTrue("SnappyCompressDecompress getBytesWritten before compress error !!!"
					, compressor.getBytesWritten() == 0);
				byte[] compressed = new byte[BYTE_SIZE];
				int cSize = compressor.compress(compressed, 0, compressed.Length);
				NUnit.Framework.Assert.IsTrue("SnappyCompressDecompress getBytesWritten after compress error !!!"
					, compressor.getBytesWritten() > 0);
				org.apache.hadoop.io.compress.snappy.SnappyDecompressor decompressor = new org.apache.hadoop.io.compress.snappy.SnappyDecompressor
					(BYTE_SIZE);
				// set as input for decompressor only compressed data indicated with cSize
				decompressor.setInput(compressed, 0, cSize);
				byte[] decompressed = new byte[BYTE_SIZE];
				decompressor.decompress(decompressed, 0, decompressed.Length);
				NUnit.Framework.Assert.IsTrue("testSnappyCompressDecompress finished error !!!", 
					decompressor.finished());
				NUnit.Framework.Assert.assertArrayEquals(bytes, decompressed);
				compressor.reset();
				decompressor.reset();
				NUnit.Framework.Assert.IsTrue("decompressor getRemaining error !!!", decompressor
					.getRemaining() == 0);
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("testSnappyCompressDecompress ex error!!!");
			}
		}

		[NUnit.Framework.Test]
		public virtual void testCompressorDecompressorEmptyStreamLogic()
		{
			java.io.ByteArrayInputStream bytesIn = null;
			java.io.ByteArrayOutputStream bytesOut = null;
			byte[] buf = null;
			org.apache.hadoop.io.compress.BlockDecompressorStream blockDecompressorStream = null;
			try
			{
				// compress empty stream
				bytesOut = new java.io.ByteArrayOutputStream();
				org.apache.hadoop.io.compress.BlockCompressorStream blockCompressorStream = new org.apache.hadoop.io.compress.BlockCompressorStream
					(bytesOut, new org.apache.hadoop.io.compress.snappy.SnappyCompressor(), 1024, 0);
				// close without write
				blockCompressorStream.close();
				// check compressed output
				buf = bytesOut.toByteArray();
				NUnit.Framework.Assert.AreEqual("empty stream compressed output size != 4", 4, buf
					.Length);
				// use compressed output as input for decompression
				bytesIn = new java.io.ByteArrayInputStream(buf);
				// create decompression stream
				blockDecompressorStream = new org.apache.hadoop.io.compress.BlockDecompressorStream
					(bytesIn, new org.apache.hadoop.io.compress.snappy.SnappyDecompressor(), 1024);
				// no byte is available because stream was closed
				NUnit.Framework.Assert.AreEqual("return value is not -1", -1, blockDecompressorStream
					.read());
			}
			catch (System.Exception e)
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
						bytesIn.close();
						bytesOut.close();
						blockDecompressorStream.close();
					}
					catch (System.IO.IOException)
					{
					}
				}
			}
		}

		[NUnit.Framework.Test]
		public virtual void testSnappyBlockCompression()
		{
			int BYTE_SIZE = 1024 * 50;
			int BLOCK_SIZE = 512;
			java.io.ByteArrayOutputStream @out = new java.io.ByteArrayOutputStream();
			byte[] block = new byte[BLOCK_SIZE];
			byte[] bytes = org.apache.hadoop.io.compress.snappy.TestSnappyCompressorDecompressor.BytesGenerator
				.get(BYTE_SIZE);
			try
			{
				// Use default of 512 as bufferSize and compressionOverhead of
				// (1% of bufferSize + 12 bytes) = 18 bytes (zlib algorithm).
				org.apache.hadoop.io.compress.snappy.SnappyCompressor compressor = new org.apache.hadoop.io.compress.snappy.SnappyCompressor
					();
				int off = 0;
				int len = BYTE_SIZE;
				int maxSize = BLOCK_SIZE - 18;
				if (BYTE_SIZE > maxSize)
				{
					do
					{
						int bufLen = System.Math.min(len, maxSize);
						compressor.setInput(bytes, off, bufLen);
						compressor.finish();
						while (!compressor.finished())
						{
							compressor.compress(block, 0, block.Length);
							@out.write(block);
						}
						compressor.reset();
						off += bufLen;
						len -= bufLen;
					}
					while (len > 0);
				}
				NUnit.Framework.Assert.IsTrue("testSnappyBlockCompression error !!!", @out.toByteArray
					().Length > 0);
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("testSnappyBlockCompression ex error !!!");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void compressDecompressLoop(int rawDataSize)
		{
			byte[] rawData = org.apache.hadoop.io.compress.snappy.TestSnappyCompressorDecompressor.BytesGenerator
				.get(rawDataSize);
			byte[] compressedResult = new byte[rawDataSize + 20];
			int directBufferSize = System.Math.max(rawDataSize * 2, 64 * 1024);
			org.apache.hadoop.io.compress.snappy.SnappyCompressor compressor = new org.apache.hadoop.io.compress.snappy.SnappyCompressor
				(directBufferSize);
			compressor.setInput(rawData, 0, rawDataSize);
			int compressedSize = compressor.compress(compressedResult, 0, compressedResult.Length
				);
			org.apache.hadoop.io.compress.snappy.SnappyDecompressor.SnappyDirectDecompressor 
				decompressor = new org.apache.hadoop.io.compress.snappy.SnappyDecompressor.SnappyDirectDecompressor
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
		public virtual void testSnappyDirectBlockCompression()
		{
			int[] size = new int[] { 4 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024 };
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.io.compress.SnappyCodec.isNativeCodeLoaded
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
				NUnit.Framework.Assert.Fail("testSnappyDirectBlockCompression ex !!!" + ex);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testSnappyCompressorDecopressorLogicWithCompressionStreams()
		{
			int BYTE_SIZE = 1024 * 100;
			byte[] bytes = org.apache.hadoop.io.compress.snappy.TestSnappyCompressorDecompressor.BytesGenerator
				.get(BYTE_SIZE);
			int bufferSize = 262144;
			int compressionOverhead = (bufferSize / 6) + 32;
			java.io.DataOutputStream deflateOut = null;
			java.io.DataInputStream inflateIn = null;
			try
			{
				org.apache.hadoop.io.DataOutputBuffer compressedDataBuffer = new org.apache.hadoop.io.DataOutputBuffer
					();
				org.apache.hadoop.io.compress.CompressionOutputStream deflateFilter = new org.apache.hadoop.io.compress.BlockCompressorStream
					(compressedDataBuffer, new org.apache.hadoop.io.compress.snappy.SnappyCompressor
					(bufferSize), bufferSize, compressionOverhead);
				deflateOut = new java.io.DataOutputStream(new java.io.BufferedOutputStream(deflateFilter
					));
				deflateOut.write(bytes, 0, bytes.Length);
				deflateOut.flush();
				deflateFilter.finish();
				org.apache.hadoop.io.DataInputBuffer deCompressedDataBuffer = new org.apache.hadoop.io.DataInputBuffer
					();
				deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0, compressedDataBuffer
					.getLength());
				org.apache.hadoop.io.compress.CompressionInputStream inflateFilter = new org.apache.hadoop.io.compress.BlockDecompressorStream
					(deCompressedDataBuffer, new org.apache.hadoop.io.compress.snappy.SnappyDecompressor
					(bufferSize), bufferSize);
				inflateIn = new java.io.DataInputStream(new java.io.BufferedInputStream(inflateFilter
					));
				byte[] result = new byte[BYTE_SIZE];
				inflateIn.read(result);
				NUnit.Framework.Assert.assertArrayEquals("original array not equals compress/decompressed array"
					, result, bytes);
			}
			catch (System.IO.IOException)
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
						deflateOut.close();
					}
					if (inflateIn != null)
					{
						inflateIn.close();
					}
				}
				catch (System.Exception)
				{
				}
			}
		}

		internal sealed class BytesGenerator
		{
			private BytesGenerator()
			{
			}

			private static readonly byte[] CACHE = new byte[] { unchecked((int)(0x0)), unchecked(
				(int)(0x1)), unchecked((int)(0x2)), unchecked((int)(0x3)), unchecked((int)(0x4))
				, unchecked((int)(0x5)), unchecked((int)(0x6)), unchecked((int)(0x7)), unchecked(
				(int)(0x8)), unchecked((int)(0x9)), unchecked((int)(0xA)), unchecked((int)(0xB))
				, unchecked((int)(0xC)), unchecked((int)(0xD)), unchecked((int)(0xE)), unchecked(
				(int)(0xF)) };

			private static readonly java.util.Random rnd = new java.util.Random(12345l);

			public static byte[] get(int size)
			{
				byte[] array = (byte[])java.lang.reflect.Array.newInstance(Sharpen.Runtime.getClassForType
					(typeof(byte)), size);
				for (int i = 0; i < size; i++)
				{
					array[i] = CACHE[rnd.nextInt(CACHE.Length - 1)];
				}
				return array;
			}
		}
	}
}
