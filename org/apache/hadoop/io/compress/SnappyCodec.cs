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
	/// <summary>This class creates snappy compressors/decompressors.</summary>
	public class SnappyCodec : org.apache.hadoop.conf.Configurable, org.apache.hadoop.io.compress.CompressionCodec
		, org.apache.hadoop.io.compress.DirectDecompressionCodec
	{
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

		/// <summary>Are the native snappy libraries loaded & initialized?</summary>
		public static void checkNativeCodeLoaded()
		{
			if (!org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded() || !org.apache.hadoop.util.NativeCodeLoader
				.buildSupportsSnappy())
			{
				throw new System.Exception("native snappy library not available: " + "this version of libhadoop was built without "
					 + "snappy support.");
			}
			if (!org.apache.hadoop.io.compress.snappy.SnappyCompressor.isNativeCodeLoaded())
			{
				throw new System.Exception("native snappy library not available: " + "SnappyCompressor has not been loaded."
					);
			}
			if (!org.apache.hadoop.io.compress.snappy.SnappyDecompressor.isNativeCodeLoaded())
			{
				throw new System.Exception("native snappy library not available: " + "SnappyDecompressor has not been loaded."
					);
			}
		}

		public static bool isNativeCodeLoaded()
		{
			return org.apache.hadoop.io.compress.snappy.SnappyCompressor.isNativeCodeLoaded()
				 && org.apache.hadoop.io.compress.snappy.SnappyDecompressor.isNativeCodeLoaded();
		}

		public static string getLibraryName()
		{
			return org.apache.hadoop.io.compress.snappy.SnappyCompressor.getLibraryName();
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
			checkNativeCodeLoaded();
			int bufferSize = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT
				);
			int compressionOverhead = (bufferSize / 6) + 32;
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
			checkNativeCodeLoaded();
			return Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.snappy.SnappyCompressor
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
			checkNativeCodeLoaded();
			int bufferSize = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT
				);
			return new org.apache.hadoop.io.compress.snappy.SnappyCompressor(bufferSize);
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
			checkNativeCodeLoaded();
			return new org.apache.hadoop.io.compress.BlockDecompressorStream(@in, decompressor
				, conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT
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
			checkNativeCodeLoaded();
			return Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.snappy.SnappyDecompressor
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
			checkNativeCodeLoaded();
			int bufferSize = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT
				);
			return new org.apache.hadoop.io.compress.snappy.SnappyDecompressor(bufferSize);
		}

		/// <summary><inheritDoc/></summary>
		public virtual org.apache.hadoop.io.compress.DirectDecompressor createDirectDecompressor
			()
		{
			return isNativeCodeLoaded() ? new org.apache.hadoop.io.compress.snappy.SnappyDecompressor.SnappyDirectDecompressor
				() : null;
		}

		/// <summary>Get the default filename extension for this kind of compression.</summary>
		/// <returns><code>.snappy</code>.</returns>
		public virtual string getDefaultExtension()
		{
			return ".snappy";
		}
	}
}
