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
using System.IO;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	/// <summary>This class encapsulates a streaming compression/decompression pair.</summary>
	public abstract class CompressionCodec
	{
		/// <summary>
		/// Create a
		/// <see cref="CompressionOutputStream"/>
		/// that will write to the given
		/// <see cref="System.IO.OutputStream"/>
		/// .
		/// </summary>
		/// <param name="out">the location for the final output stream</param>
		/// <returns>a stream the user can write uncompressed data to have it compressed</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract CompressionOutputStream CreateOutputStream(OutputStream @out);

		/// <summary>
		/// Create a
		/// <see cref="CompressionOutputStream"/>
		/// that will write to the given
		/// <see cref="System.IO.OutputStream"/>
		/// with the given
		/// <see cref="Compressor"/>
		/// .
		/// </summary>
		/// <param name="out">the location for the final output stream</param>
		/// <param name="compressor">compressor to use</param>
		/// <returns>a stream the user can write uncompressed data to have it compressed</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract CompressionOutputStream CreateOutputStream(OutputStream @out, Compressor
			 compressor);

		/// <summary>
		/// Get the type of
		/// <see cref="Compressor"/>
		/// needed by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>the type of compressor needed by this codec.</returns>
		public abstract Type GetCompressorType();

		/// <summary>
		/// Create a new
		/// <see cref="Compressor"/>
		/// for use by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>a new compressor for use by this codec</returns>
		public abstract Compressor CreateCompressor();

		/// <summary>
		/// Create a
		/// <see cref="CompressionInputStream"/>
		/// that will read from the given
		/// input stream.
		/// </summary>
		/// <param name="in">the stream to read compressed bytes from</param>
		/// <returns>a stream to read uncompressed bytes from</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract CompressionInputStream CreateInputStream(InputStream @in);

		/// <summary>
		/// Create a
		/// <see cref="CompressionInputStream"/>
		/// that will read from the given
		/// <see cref="System.IO.InputStream"/>
		/// with the given
		/// <see cref="Decompressor"/>
		/// .
		/// </summary>
		/// <param name="in">the stream to read compressed bytes from</param>
		/// <param name="decompressor">decompressor to use</param>
		/// <returns>a stream to read uncompressed bytes from</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract CompressionInputStream CreateInputStream(InputStream @in, Decompressor
			 decompressor);

		/// <summary>
		/// Get the type of
		/// <see cref="Decompressor"/>
		/// needed by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>the type of decompressor needed by this codec.</returns>
		public abstract Type GetDecompressorType();

		/// <summary>
		/// Create a new
		/// <see cref="Decompressor"/>
		/// for use by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>a new decompressor for use by this codec</returns>
		public abstract Decompressor CreateDecompressor();

		/// <summary>Get the default filename extension for this kind of compression.</summary>
		/// <returns>the extension including the '.'</returns>
		public abstract string GetDefaultExtension();

		public class Util
		{
			/// <summary>Create an output stream with a codec taken from the global CodecPool.</summary>
			/// <param name="codec">The codec to use to create the output stream.</param>
			/// <param name="conf">The configuration to use if we need to create a new codec.</param>
			/// <param name="out">The output stream to wrap.</param>
			/// <returns>The new output stream</returns>
			/// <exception cref="System.IO.IOException"/>
			internal static CompressionOutputStream CreateOutputStreamWithCodecPool(CompressionCodec
				 codec, Configuration conf, OutputStream @out)
			{
				Compressor compressor = CodecPool.GetCompressor(codec, conf);
				CompressionOutputStream stream = null;
				try
				{
					stream = codec.CreateOutputStream(@out, compressor);
				}
				finally
				{
					if (stream == null)
					{
						CodecPool.ReturnCompressor(compressor);
					}
					else
					{
						stream.SetTrackedCompressor(compressor);
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
			internal static CompressionInputStream CreateInputStreamWithCodecPool(CompressionCodec
				 codec, Configuration conf, InputStream @in)
			{
				Decompressor decompressor = CodecPool.GetDecompressor(codec);
				CompressionInputStream stream = null;
				try
				{
					stream = codec.CreateInputStream(@in, decompressor);
				}
				finally
				{
					if (stream == null)
					{
						CodecPool.ReturnDecompressor(decompressor);
					}
					else
					{
						stream.SetTrackedDecompressor(decompressor);
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
