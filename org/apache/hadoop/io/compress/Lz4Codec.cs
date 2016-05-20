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
	/// <summary>This class creates lz4 compressors/decompressors.</summary>
	public class Lz4Codec : org.apache.hadoop.conf.Configurable, org.apache.hadoop.io.compress.CompressionCodec
	{
		static Lz4Codec()
		{
			org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded();
		}

		internal org.apache.hadoop.conf.Configuration conf;

		/// <summary>Set the configuration to be used by this object.</summary>
		/// <param name="conf">the configuration object.</param>
		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}

		/// <summary>Return the configuration used by this object.</summary>
		/// <returns>the configuration object used by this objec.</returns>
		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}

		/// <summary>Are the native lz4 libraries loaded & initialized?</summary>
		/// <returns>true if loaded & initialized, otherwise false</returns>
		public static bool isNativeCodeLoaded()
		{
			return org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded();
		}

		public static string getLibraryName()
		{
			return org.apache.hadoop.io.compress.lz4.Lz4Compressor.getLibraryName();
		}

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
		public virtual org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
			(java.io.OutputStream @out)
		{
			return org.apache.hadoop.io.compress.CompressionCodec.Util.createOutputStreamWithCodecPool
				(this, conf, @out);
		}

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
		public virtual org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
			(java.io.OutputStream @out, org.apache.hadoop.io.compress.Compressor compressor)
		{
			if (!isNativeCodeLoaded())
			{
				throw new System.Exception("native lz4 library not available");
			}
			int bufferSize = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT
				);
			int compressionOverhead = bufferSize / 255 + 16;
			return new org.apache.hadoop.io.compress.BlockCompressorStream(@out, compressor, 
				bufferSize, compressionOverhead);
		}

		/// <summary>
		/// Get the type of
		/// <see cref="Compressor"/>
		/// needed by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>the type of compressor needed by this codec.</returns>
		public virtual java.lang.Class getCompressorType()
		{
			if (!isNativeCodeLoaded())
			{
				throw new System.Exception("native lz4 library not available");
			}
			return Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.lz4.Lz4Compressor
				));
		}

		/// <summary>
		/// Create a new
		/// <see cref="Compressor"/>
		/// for use by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>a new compressor for use by this codec</returns>
		public virtual org.apache.hadoop.io.compress.Compressor createCompressor()
		{
			if (!isNativeCodeLoaded())
			{
				throw new System.Exception("native lz4 library not available");
			}
			int bufferSize = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT
				);
			bool useLz4HC = conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_DEFAULT
				);
			return new org.apache.hadoop.io.compress.lz4.Lz4Compressor(bufferSize, useLz4HC);
		}

		/// <summary>
		/// Create a
		/// <see cref="CompressionInputStream"/>
		/// that will read from the given
		/// input stream.
		/// </summary>
		/// <param name="in">the stream to read compressed bytes from</param>
		/// <returns>a stream to read uncompressed bytes from</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.io.compress.CompressionInputStream createInputStream
			(java.io.InputStream @in)
		{
			return org.apache.hadoop.io.compress.CompressionCodec.Util.createInputStreamWithCodecPool
				(this, conf, @in);
		}

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
		public virtual org.apache.hadoop.io.compress.CompressionInputStream createInputStream
			(java.io.InputStream @in, org.apache.hadoop.io.compress.Decompressor decompressor
			)
		{
			if (!isNativeCodeLoaded())
			{
				throw new System.Exception("native lz4 library not available");
			}
			return new org.apache.hadoop.io.compress.BlockDecompressorStream(@in, decompressor
				, conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT
				));
		}

		/// <summary>
		/// Get the type of
		/// <see cref="Decompressor"/>
		/// needed by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>the type of decompressor needed by this codec.</returns>
		public virtual java.lang.Class getDecompressorType()
		{
			if (!isNativeCodeLoaded())
			{
				throw new System.Exception("native lz4 library not available");
			}
			return Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.lz4.Lz4Decompressor
				));
		}

		/// <summary>
		/// Create a new
		/// <see cref="Decompressor"/>
		/// for use by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>a new decompressor for use by this codec</returns>
		public virtual org.apache.hadoop.io.compress.Decompressor createDecompressor()
		{
			if (!isNativeCodeLoaded())
			{
				throw new System.Exception("native lz4 library not available");
			}
			int bufferSize = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT
				);
			return new org.apache.hadoop.io.compress.lz4.Lz4Decompressor(bufferSize);
		}

		/// <summary>Get the default filename extension for this kind of compression.</summary>
		/// <returns><code>.lz4</code>.</returns>
		public virtual string getDefaultExtension()
		{
			return ".lz4";
		}
	}
}
