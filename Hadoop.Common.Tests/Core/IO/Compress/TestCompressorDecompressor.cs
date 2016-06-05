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
using System;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.IO.Compress.Lz4;
using Org.Apache.Hadoop.IO.Compress.Snappy;
using Org.Apache.Hadoop.IO.Compress.Zlib;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
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
		private static readonly Random rnd = new Random(12345L);

		[Fact]
		public virtual void TestCompressorDecompressor()
		{
			// no more for this data
			int Size = 44 * 1024;
			byte[] rawData = Generate(Size);
			try
			{
				CompressDecompressTester.Of(rawData).WithCompressDecompressPair(new SnappyCompressor
					(), new SnappyDecompressor()).WithCompressDecompressPair(new Lz4Compressor(), new 
					Lz4Decompressor()).WithCompressDecompressPair(new BuiltInZlibDeflater(), new BuiltInZlibInflater
					()).WithTestCases(ImmutableSet.Of(CompressDecompressTester.CompressionTestStrategy
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
				CompressDecompressTester.Of(rawData).WithCompressDecompressPair(new SnappyCompressor
					(ByteSize + ByteSize / 2), new SnappyDecompressor(ByteSize + ByteSize / 2)).WithCompressDecompressPair
					(new Lz4Compressor(ByteSize), new Lz4Decompressor(ByteSize)).WithTestCases(ImmutableSet
					.Of(CompressDecompressTester.CompressionTestStrategy.CompressDecompressSingleBlock
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
