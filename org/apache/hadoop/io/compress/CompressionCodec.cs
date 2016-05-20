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
	/// <summary>This class encapsulates a streaming compression/decompression pair.</summary>
	public abstract class CompressionCodec
	{
		/// <summary>
		/// Create a
		/// <see cref="CompressionOutputStream"/>
		/// that will write to the given
		/// <see cref="java.io.OutputStream"/>
		/// .
		/// </summary>
		/// <param name="out">the location for the final output stream</param>
		/// <returns>a stream the user can write uncompressed data to have it compressed</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
			(java.io.OutputStream @out);

		/// <summary>
		/// Create a
		/// <see cref="CompressionOutputStream"/>
		/// that will write to the given
		/// <see cref="java.io.OutputStream"/>
		/// with the given
		/// <see cref="Compressor"/>
		/// .
		/// </summary>
		/// <param name="out">the location for the final output stream</param>
		/// <param name="compressor">compressor to use</param>
		/// <returns>a stream the user can write uncompressed data to have it compressed</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
			(java.io.OutputStream @out, org.apache.hadoop.io.compress.Compressor compressor);

		/// <summary>
		/// Get the type of
		/// <see cref="Compressor"/>
		/// needed by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>the type of compressor needed by this codec.</returns>
		public abstract java.lang.Class getCompressorType();

		/// <summary>
		/// Create a new
		/// <see cref="Compressor"/>
		/// for use by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>a new compressor for use by this codec</returns>
		public abstract org.apache.hadoop.io.compress.Compressor createCompressor();

		/// <summary>
		/// Create a
		/// <see cref="CompressionInputStream"/>
		/// that will read from the given
		/// input stream.
		/// </summary>
		/// <param name="in">the stream to read compressed bytes from</param>
		/// <returns>a stream to read uncompressed bytes from</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.io.compress.CompressionInputStream createInputStream
			(java.io.InputStream @in);

		/// <summary>
		/// Create a
		/// <see cref="CompressionInputStream"/>
		/// that will read from the given
		/// <see cref="java.io.InputStream"/>
		/// with the given
		/// <see cref="Decompressor"/>
		/// .
		/// </summary>
		/// <param name="in">the stream to read compressed bytes from</param>
		/// <param name="decompressor">decompressor to use</param>
		/// <returns>a stream to read uncompressed bytes from</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.io.compress.CompressionInputStream createInputStream
			(java.io.InputStream @in, org.apache.hadoop.io.compress.Decompressor decompressor
			);

		/// <summary>
		/// Get the type of
		/// <see cref="Decompressor"/>
		/// needed by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>the type of decompressor needed by this codec.</returns>
		public abstract java.lang.Class getDecompressorType();

		/// <summary>
		/// Create a new
		/// <see cref="Decompressor"/>
		/// for use by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>a new decompressor for use by this codec</returns>
		public abstract org.apache.hadoop.io.compress.Decompressor createDecompressor();

		/// <summary>Get the default filename extension for this kind of compression.</summary>
		/// <returns>the extension including the '.'</returns>
		public abstract string getDefaultExtension();

		public class Util
		{
			/// <summary>Create an output stream with a codec taken from the global CodecPool.</summary>
			/// <param name="codec">The codec to use to create the output stream.</param>
			/// <param name="conf">The configuration to use if we need to create a new codec.</param>
			/// <param name="out">The output stream to wrap.</param>
			/// <returns>The new output stream</returns>
			/// <exception cref="System.IO.IOException"/>
			internal static org.apache.hadoop.io.compress.CompressionOutputStream createOutputStreamWithCodecPool
				(org.apache.hadoop.io.compress.CompressionCodec codec, org.apache.hadoop.conf.Configuration
				 conf, java.io.OutputStream @out)
			{
				org.apache.hadoop.io.compress.Compressor compressor = org.apache.hadoop.io.compress.CodecPool
					.getCompressor(codec, conf);
				org.apache.hadoop.io.compress.CompressionOutputStream stream = null;
				try
				{
					stream = codec.createOutputStream(@out, compressor);
				}
				finally
				{
					if (stream == null)
					{
						org.apache.hadoop.io.compress.CodecPool.returnCompressor(compressor);
					}
					else
					{
						stream.setTrackedCompressor(compressor);
					}
				}
				return stream;
			}

			/// <summary>Create an input stream with a codec taken from the global CodecPool.</summary>
			/// <param name="codec">The codec to use to create the input stream.</param>
			/// <param name="conf">The configuration to use if we need to create a new codec.</param>
			/// <param name="in">The input stream to wrap.</param>
			/// <returns>The new input stream</returns>
			/// <exception cref="System.IO.IOException"/>
			internal static org.apache.hadoop.io.compress.CompressionInputStream createInputStreamWithCodecPool
				(org.apache.hadoop.io.compress.CompressionCodec codec, org.apache.hadoop.conf.Configuration
				 conf, java.io.InputStream @in)
			{
				org.apache.hadoop.io.compress.Decompressor decompressor = org.apache.hadoop.io.compress.CodecPool
					.getDecompressor(codec);
				org.apache.hadoop.io.compress.CompressionInputStream stream = null;
				try
				{
					stream = codec.createInputStream(@in, decompressor);
				}
				finally
				{
					if (stream == null)
					{
						org.apache.hadoop.io.compress.CodecPool.returnDecompressor(decompressor);
					}
					else
					{
						stream.setTrackedDecompressor(decompressor);
					}
				}
				return stream;
			}
		}
	}

	public static class CompressionCodecConstants
	{
	}
}
