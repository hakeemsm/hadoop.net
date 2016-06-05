using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress.Lz4
{
	public class TestLz4CompressorDecompressor
	{
		private static readonly Random rnd = new Random(12345l);

		[SetUp]
		public virtual void Before()
		{
			Assume.AssumeTrue(Lz4Codec.IsNativeCodeLoaded());
		}

		//test on NullPointerException in {@code compressor.setInput()} 
		[Fact]
		public virtual void TestCompressorSetInputNullPointerException()
		{
			try
			{
				Lz4Compressor compressor = new Lz4Compressor();
				compressor.SetInput(null, 0, 10);
				NUnit.Framework.Assert.Fail("testCompressorSetInputNullPointerException error !!!"
					);
			}
			catch (ArgumentNullException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testCompressorSetInputNullPointerException ex error !!!"
					);
			}
		}

		//test on NullPointerException in {@code decompressor.setInput()}
		[Fact]
		public virtual void TestDecompressorSetInputNullPointerException()
		{
			try
			{
				Lz4Decompressor decompressor = new Lz4Decompressor();
				decompressor.SetInput(null, 0, 10);
				NUnit.Framework.Assert.Fail("testDecompressorSetInputNullPointerException error !!!"
					);
			}
			catch (ArgumentNullException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testDecompressorSetInputNullPointerException ex error !!!"
					);
			}
		}

		//test on ArrayIndexOutOfBoundsException in {@code compressor.setInput()}
		[Fact]
		public virtual void TestCompressorSetInputAIOBException()
		{
			try
			{
				Lz4Compressor compressor = new Lz4Compressor();
				compressor.SetInput(new byte[] {  }, -5, 10);
				NUnit.Framework.Assert.Fail("testCompressorSetInputAIOBException error !!!");
			}
			catch (IndexOutOfRangeException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testCompressorSetInputAIOBException ex error !!!");
			}
		}

		//test on ArrayIndexOutOfBoundsException in {@code decompressor.setInput()}
		[Fact]
		public virtual void TestDecompressorSetInputAIOUBException()
		{
			try
			{
				Lz4Decompressor decompressor = new Lz4Decompressor();
				decompressor.SetInput(new byte[] {  }, -5, 10);
				NUnit.Framework.Assert.Fail("testDecompressorSetInputAIOBException error !!!");
			}
			catch (IndexOutOfRangeException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testDecompressorSetInputAIOBException ex error !!!");
			}
		}

		//test on NullPointerException in {@code compressor.compress()}  
		[Fact]
		public virtual void TestCompressorCompressNullPointerException()
		{
			try
			{
				Lz4Compressor compressor = new Lz4Compressor();
				byte[] bytes = Generate(1024 * 6);
				compressor.SetInput(bytes, 0, bytes.Length);
				compressor.Compress(null, 0, 0);
				NUnit.Framework.Assert.Fail("testCompressorCompressNullPointerException error !!!"
					);
			}
			catch (ArgumentNullException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testCompressorCompressNullPointerException ex error !!!"
					);
			}
		}

		//test on NullPointerException in {@code decompressor.decompress()}  
		[Fact]
		public virtual void TestDecompressorCompressNullPointerException()
		{
			try
			{
				Lz4Decompressor decompressor = new Lz4Decompressor();
				byte[] bytes = Generate(1024 * 6);
				decompressor.SetInput(bytes, 0, bytes.Length);
				decompressor.Decompress(null, 0, 0);
				NUnit.Framework.Assert.Fail("testDecompressorCompressNullPointerException error !!!"
					);
			}
			catch (ArgumentNullException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testDecompressorCompressNullPointerException ex error !!!"
					);
			}
		}

		//test on ArrayIndexOutOfBoundsException in {@code compressor.compress()}  
		[Fact]
		public virtual void TestCompressorCompressAIOBException()
		{
			try
			{
				Lz4Compressor compressor = new Lz4Compressor();
				byte[] bytes = Generate(1024 * 6);
				compressor.SetInput(bytes, 0, bytes.Length);
				compressor.Compress(new byte[] {  }, 0, -1);
				NUnit.Framework.Assert.Fail("testCompressorCompressAIOBException error !!!");
			}
			catch (IndexOutOfRangeException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testCompressorCompressAIOBException ex error !!!");
			}
		}

		//test on ArrayIndexOutOfBoundsException in decompressor.decompress()  
		[Fact]
		public virtual void TestDecompressorCompressAIOBException()
		{
			try
			{
				Lz4Decompressor decompressor = new Lz4Decompressor();
				byte[] bytes = Generate(1024 * 6);
				decompressor.SetInput(bytes, 0, bytes.Length);
				decompressor.Decompress(new byte[] {  }, 0, -1);
				NUnit.Framework.Assert.Fail("testDecompressorCompressAIOBException error !!!");
			}
			catch (IndexOutOfRangeException)
			{
			}
			catch (Exception)
			{
				// expected
				NUnit.Framework.Assert.Fail("testDecompressorCompressAIOBException ex error !!!");
			}
		}

		// test Lz4Compressor compressor.compress()  
		[Fact]
		public virtual void TestSetInputWithBytesSizeMoreThenDefaultLz4CompressorByfferSize
			()
		{
			int BytesSize = 1024 * 64 + 1;
			try
			{
				Lz4Compressor compressor = new Lz4Compressor();
				byte[] bytes = Generate(BytesSize);
				Assert.True("needsInput error !!!", compressor.NeedsInput());
				compressor.SetInput(bytes, 0, bytes.Length);
				byte[] emptyBytes = new byte[BytesSize];
				int csize = compressor.Compress(emptyBytes, 0, bytes.Length);
				Assert.True("testSetInputWithBytesSizeMoreThenDefaultLz4CompressorByfferSize error !!!"
					, csize != 0);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("testSetInputWithBytesSizeMoreThenDefaultLz4CompressorByfferSize ex error !!!"
					);
			}
		}

		// test compress/decompress process 
		[Fact]
		public virtual void TestCompressDecompress()
		{
			int ByteSize = 1024 * 54;
			byte[] bytes = Generate(ByteSize);
			Lz4Compressor compressor = new Lz4Compressor();
			try
			{
				compressor.SetInput(bytes, 0, bytes.Length);
				Assert.True("Lz4CompressDecompress getBytesRead error !!!", compressor
					.GetBytesRead() > 0);
				Assert.True("Lz4CompressDecompress getBytesWritten before compress error !!!"
					, compressor.GetBytesWritten() == 0);
				byte[] compressed = new byte[ByteSize];
				int cSize = compressor.Compress(compressed, 0, compressed.Length);
				Assert.True("Lz4CompressDecompress getBytesWritten after compress error !!!"
					, compressor.GetBytesWritten() > 0);
				Lz4Decompressor decompressor = new Lz4Decompressor();
				// set as input for decompressor only compressed data indicated with cSize
				decompressor.SetInput(compressed, 0, cSize);
				byte[] decompressed = new byte[ByteSize];
				decompressor.Decompress(decompressed, 0, decompressed.Length);
				Assert.True("testLz4CompressDecompress finished error !!!", decompressor
					.Finished());
				Assert.AssertArrayEquals(bytes, decompressed);
				compressor.Reset();
				decompressor.Reset();
				Assert.True("decompressor getRemaining error !!!", decompressor
					.GetRemaining() == 0);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("testLz4CompressDecompress ex error!!!");
			}
		}

		// test compress/decompress with empty stream
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
					new Lz4Compressor(), 1024, 0);
				// close without write
				blockCompressorStream.Close();
				// check compressed output
				buf = bytesOut.ToByteArray();
				Assert.Equal("empty stream compressed output size != 4", 4, buf
					.Length);
				// use compressed output as input for decompression
				bytesIn = new ByteArrayInputStream(buf);
				// create decompression stream
				blockDecompressorStream = new BlockDecompressorStream(bytesIn, new Lz4Decompressor
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

		// test compress/decompress process through CompressionOutputStream/CompressionInputStream api 
		[Fact]
		public virtual void TestCompressorDecopressorLogicWithCompressionStreams()
		{
			DataOutputStream deflateOut = null;
			DataInputStream inflateIn = null;
			int ByteSize = 1024 * 100;
			byte[] bytes = Generate(ByteSize);
			int bufferSize = 262144;
			int compressionOverhead = (bufferSize / 6) + 32;
			try
			{
				DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
				CompressionOutputStream deflateFilter = new BlockCompressorStream(compressedDataBuffer
					, new Lz4Compressor(bufferSize), bufferSize, compressionOverhead);
				deflateOut = new DataOutputStream(new BufferedOutputStream(deflateFilter));
				deflateOut.Write(bytes, 0, bytes.Length);
				deflateOut.Flush();
				deflateFilter.Finish();
				DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
				deCompressedDataBuffer.Reset(compressedDataBuffer.GetData(), 0, compressedDataBuffer
					.GetLength());
				CompressionInputStream inflateFilter = new BlockDecompressorStream(deCompressedDataBuffer
					, new Lz4Decompressor(bufferSize), bufferSize);
				inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));
				byte[] result = new byte[ByteSize];
				inflateIn.Read(result);
				Assert.AssertArrayEquals("original array not equals compress/decompressed array", 
					result, bytes);
			}
			catch (IOException)
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

		public static byte[] Generate(int size)
		{
			byte[] array = new byte[size];
			for (int i = 0; i < size; i++)
			{
				array[i] = unchecked((byte)rnd.Next(16));
			}
			return array;
		}
	}
}
