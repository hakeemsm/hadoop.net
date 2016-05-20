using Sharpen;

namespace org.apache.hadoop.io.compress.lz4
{
	public class TestLz4CompressorDecompressor
	{
		private static readonly java.util.Random rnd = new java.util.Random(12345l);

		[NUnit.Framework.SetUp]
		public virtual void before()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.io.compress.Lz4Codec.isNativeCodeLoaded
				());
		}

		//test on NullPointerException in {@code compressor.setInput()} 
		[NUnit.Framework.Test]
		public virtual void testCompressorSetInputNullPointerException()
		{
			try
			{
				org.apache.hadoop.io.compress.lz4.Lz4Compressor compressor = new org.apache.hadoop.io.compress.lz4.Lz4Compressor
					();
				compressor.setInput(null, 0, 10);
				NUnit.Framework.Assert.Fail("testCompressorSetInputNullPointerException error !!!"
					);
			}
			catch (System.ArgumentNullException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testCompressorSetInputNullPointerException ex error !!!"
					);
			}
		}

		//test on NullPointerException in {@code decompressor.setInput()}
		[NUnit.Framework.Test]
		public virtual void testDecompressorSetInputNullPointerException()
		{
			try
			{
				org.apache.hadoop.io.compress.lz4.Lz4Decompressor decompressor = new org.apache.hadoop.io.compress.lz4.Lz4Decompressor
					();
				decompressor.setInput(null, 0, 10);
				NUnit.Framework.Assert.Fail("testDecompressorSetInputNullPointerException error !!!"
					);
			}
			catch (System.ArgumentNullException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testDecompressorSetInputNullPointerException ex error !!!"
					);
			}
		}

		//test on ArrayIndexOutOfBoundsException in {@code compressor.setInput()}
		[NUnit.Framework.Test]
		public virtual void testCompressorSetInputAIOBException()
		{
			try
			{
				org.apache.hadoop.io.compress.lz4.Lz4Compressor compressor = new org.apache.hadoop.io.compress.lz4.Lz4Compressor
					();
				compressor.setInput(new byte[] {  }, -5, 10);
				NUnit.Framework.Assert.Fail("testCompressorSetInputAIOBException error !!!");
			}
			catch (System.IndexOutOfRangeException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testCompressorSetInputAIOBException ex error !!!");
			}
		}

		//test on ArrayIndexOutOfBoundsException in {@code decompressor.setInput()}
		[NUnit.Framework.Test]
		public virtual void testDecompressorSetInputAIOUBException()
		{
			try
			{
				org.apache.hadoop.io.compress.lz4.Lz4Decompressor decompressor = new org.apache.hadoop.io.compress.lz4.Lz4Decompressor
					();
				decompressor.setInput(new byte[] {  }, -5, 10);
				NUnit.Framework.Assert.Fail("testDecompressorSetInputAIOBException error !!!");
			}
			catch (System.IndexOutOfRangeException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testDecompressorSetInputAIOBException ex error !!!");
			}
		}

		//test on NullPointerException in {@code compressor.compress()}  
		[NUnit.Framework.Test]
		public virtual void testCompressorCompressNullPointerException()
		{
			try
			{
				org.apache.hadoop.io.compress.lz4.Lz4Compressor compressor = new org.apache.hadoop.io.compress.lz4.Lz4Compressor
					();
				byte[] bytes = generate(1024 * 6);
				compressor.setInput(bytes, 0, bytes.Length);
				compressor.compress(null, 0, 0);
				NUnit.Framework.Assert.Fail("testCompressorCompressNullPointerException error !!!"
					);
			}
			catch (System.ArgumentNullException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testCompressorCompressNullPointerException ex error !!!"
					);
			}
		}

		//test on NullPointerException in {@code decompressor.decompress()}  
		[NUnit.Framework.Test]
		public virtual void testDecompressorCompressNullPointerException()
		{
			try
			{
				org.apache.hadoop.io.compress.lz4.Lz4Decompressor decompressor = new org.apache.hadoop.io.compress.lz4.Lz4Decompressor
					();
				byte[] bytes = generate(1024 * 6);
				decompressor.setInput(bytes, 0, bytes.Length);
				decompressor.decompress(null, 0, 0);
				NUnit.Framework.Assert.Fail("testDecompressorCompressNullPointerException error !!!"
					);
			}
			catch (System.ArgumentNullException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testDecompressorCompressNullPointerException ex error !!!"
					);
			}
		}

		//test on ArrayIndexOutOfBoundsException in {@code compressor.compress()}  
		[NUnit.Framework.Test]
		public virtual void testCompressorCompressAIOBException()
		{
			try
			{
				org.apache.hadoop.io.compress.lz4.Lz4Compressor compressor = new org.apache.hadoop.io.compress.lz4.Lz4Compressor
					();
				byte[] bytes = generate(1024 * 6);
				compressor.setInput(bytes, 0, bytes.Length);
				compressor.compress(new byte[] {  }, 0, -1);
				NUnit.Framework.Assert.Fail("testCompressorCompressAIOBException error !!!");
			}
			catch (System.IndexOutOfRangeException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testCompressorCompressAIOBException ex error !!!");
			}
		}

		//test on ArrayIndexOutOfBoundsException in decompressor.decompress()  
		[NUnit.Framework.Test]
		public virtual void testDecompressorCompressAIOBException()
		{
			try
			{
				org.apache.hadoop.io.compress.lz4.Lz4Decompressor decompressor = new org.apache.hadoop.io.compress.lz4.Lz4Decompressor
					();
				byte[] bytes = generate(1024 * 6);
				decompressor.setInput(bytes, 0, bytes.Length);
				decompressor.decompress(new byte[] {  }, 0, -1);
				NUnit.Framework.Assert.Fail("testDecompressorCompressAIOBException error !!!");
			}
			catch (System.IndexOutOfRangeException)
			{
			}
			catch (System.Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testDecompressorCompressAIOBException ex error !!!");
			}
		}

		// test Lz4Compressor compressor.compress()  
		[NUnit.Framework.Test]
		public virtual void testSetInputWithBytesSizeMoreThenDefaultLz4CompressorByfferSize
			()
		{
			int BYTES_SIZE = 1024 * 64 + 1;
			try
			{
				org.apache.hadoop.io.compress.lz4.Lz4Compressor compressor = new org.apache.hadoop.io.compress.lz4.Lz4Compressor
					();
				byte[] bytes = generate(BYTES_SIZE);
				NUnit.Framework.Assert.IsTrue("needsInput error !!!", compressor.needsInput());
				compressor.setInput(bytes, 0, bytes.Length);
				byte[] emptyBytes = new byte[BYTES_SIZE];
				int csize = compressor.compress(emptyBytes, 0, bytes.Length);
				NUnit.Framework.Assert.IsTrue("testSetInputWithBytesSizeMoreThenDefaultLz4CompressorByfferSize error !!!"
					, csize != 0);
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("testSetInputWithBytesSizeMoreThenDefaultLz4CompressorByfferSize ex error !!!"
					);
			}
		}

		// test compress/decompress process 
		[NUnit.Framework.Test]
		public virtual void testCompressDecompress()
		{
			int BYTE_SIZE = 1024 * 54;
			byte[] bytes = generate(BYTE_SIZE);
			org.apache.hadoop.io.compress.lz4.Lz4Compressor compressor = new org.apache.hadoop.io.compress.lz4.Lz4Compressor
				();
			try
			{
				compressor.setInput(bytes, 0, bytes.Length);
				NUnit.Framework.Assert.IsTrue("Lz4CompressDecompress getBytesRead error !!!", compressor
					.getBytesRead() > 0);
				NUnit.Framework.Assert.IsTrue("Lz4CompressDecompress getBytesWritten before compress error !!!"
					, compressor.getBytesWritten() == 0);
				byte[] compressed = new byte[BYTE_SIZE];
				int cSize = compressor.compress(compressed, 0, compressed.Length);
				NUnit.Framework.Assert.IsTrue("Lz4CompressDecompress getBytesWritten after compress error !!!"
					, compressor.getBytesWritten() > 0);
				org.apache.hadoop.io.compress.lz4.Lz4Decompressor decompressor = new org.apache.hadoop.io.compress.lz4.Lz4Decompressor
					();
				// set as input for decompressor only compressed data indicated with cSize
				decompressor.setInput(compressed, 0, cSize);
				byte[] decompressed = new byte[BYTE_SIZE];
				decompressor.decompress(decompressed, 0, decompressed.Length);
				NUnit.Framework.Assert.IsTrue("testLz4CompressDecompress finished error !!!", decompressor
					.finished());
				NUnit.Framework.Assert.assertArrayEquals(bytes, decompressed);
				compressor.reset();
				decompressor.reset();
				NUnit.Framework.Assert.IsTrue("decompressor getRemaining error !!!", decompressor
					.getRemaining() == 0);
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("testLz4CompressDecompress ex error!!!");
			}
		}

		// test compress/decompress with empty stream
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
					(bytesOut, new org.apache.hadoop.io.compress.lz4.Lz4Compressor(), 1024, 0);
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
					(bytesIn, new org.apache.hadoop.io.compress.lz4.Lz4Decompressor(), 1024);
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

		// test compress/decompress process through CompressionOutputStream/CompressionInputStream api 
		[NUnit.Framework.Test]
		public virtual void testCompressorDecopressorLogicWithCompressionStreams()
		{
			java.io.DataOutputStream deflateOut = null;
			java.io.DataInputStream inflateIn = null;
			int BYTE_SIZE = 1024 * 100;
			byte[] bytes = generate(BYTE_SIZE);
			int bufferSize = 262144;
			int compressionOverhead = (bufferSize / 6) + 32;
			try
			{
				org.apache.hadoop.io.DataOutputBuffer compressedDataBuffer = new org.apache.hadoop.io.DataOutputBuffer
					();
				org.apache.hadoop.io.compress.CompressionOutputStream deflateFilter = new org.apache.hadoop.io.compress.BlockCompressorStream
					(compressedDataBuffer, new org.apache.hadoop.io.compress.lz4.Lz4Compressor(bufferSize
					), bufferSize, compressionOverhead);
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
					(deCompressedDataBuffer, new org.apache.hadoop.io.compress.lz4.Lz4Decompressor(bufferSize
					), bufferSize);
				inflateIn = new java.io.DataInputStream(new java.io.BufferedInputStream(inflateFilter
					));
				byte[] result = new byte[BYTE_SIZE];
				inflateIn.read(result);
				NUnit.Framework.Assert.assertArrayEquals("original array not equals compress/decompressed array"
					, result, bytes);
			}
			catch (System.IO.IOException)
			{
				NUnit.Framework.Assert.Fail("testLz4CompressorDecopressorLogicWithCompressionStreams ex error !!!"
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

		public static byte[] generate(int size)
		{
			byte[] array = new byte[size];
			for (int i = 0; i < size; i++)
			{
				array[i] = unchecked((byte)rnd.nextInt(16));
			}
			return array;
		}
	}
}
