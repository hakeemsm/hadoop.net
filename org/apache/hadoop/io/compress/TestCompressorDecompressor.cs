/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using Sharpen;

namespace org.apache.hadoop.io.compress
{
	/// <summary>
	/// Test for pairs:
	/// <pre>
	/// SnappyCompressor/SnappyDecompressor
	/// Lz4Compressor/Lz4Decompressor
	/// BuiltInZlibDeflater/new BuiltInZlibInflater
	/// Note: we can't use ZlibCompressor/ZlibDecompressor here
	/// because his constructor can throw exception (if native libraries not found)
	/// For ZlibCompressor/ZlibDecompressor pair testing used
	/// <c>TestZlibCompressorDecompressor</c>
	/// 
	/// </pre>
	/// </summary>
	public class TestCompressorDecompressor
	{
		private static readonly java.util.Random rnd = new java.util.Random(12345L);

		[NUnit.Framework.Test]
		public virtual void testCompressorDecompressor()
		{
			// no more for this data
			int SIZE = 44 * 1024;
			byte[] rawData = generate(SIZE);
			try
			{
				org.apache.hadoop.io.compress.CompressDecompressTester.of(rawData).withCompressDecompressPair
					(new org.apache.hadoop.io.compress.snappy.SnappyCompressor(), new org.apache.hadoop.io.compress.snappy.SnappyDecompressor
					()).withCompressDecompressPair(new org.apache.hadoop.io.compress.lz4.Lz4Compressor
					(), new org.apache.hadoop.io.compress.lz4.Lz4Decompressor()).withCompressDecompressPair
					(new org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater(), new org.apache.hadoop.io.compress.zlib.BuiltInZlibInflater
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
					(new org.apache.hadoop.io.compress.snappy.SnappyCompressor(BYTE_SIZE + BYTE_SIZE
					 / 2), new org.apache.hadoop.io.compress.snappy.SnappyDecompressor(BYTE_SIZE + BYTE_SIZE
					 / 2)).withCompressDecompressPair(new org.apache.hadoop.io.compress.lz4.Lz4Compressor
					(BYTE_SIZE), new org.apache.hadoop.io.compress.lz4.Lz4Decompressor(BYTE_SIZE)).withTestCases
					(com.google.common.collect.ImmutableSet.of(org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
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
