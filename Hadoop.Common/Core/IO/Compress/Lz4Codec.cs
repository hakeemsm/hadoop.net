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
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress.Lz4;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	/// <summary>This class creates lz4 compressors/decompressors.</summary>
	public class Lz4Codec : Configurable, CompressionCodec
	{
		static Lz4Codec()
		{
			NativeCodeLoader.IsNativeCodeLoaded();
		}

		internal Configuration conf;

		/// <summary>Set the configuration to be used by this object.</summary>
		/// <param name="conf">the configuration object.</param>
		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		/// <summary>Return the configuration used by this object.</summary>
		/// <returns>the configuration object used by this objec.</returns>
		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>Are the native lz4 libraries loaded & initialized?</summary>
		/// <returns>true if loaded & initialized, otherwise false</returns>
		public static bool IsNativeCodeLoaded()
		{
			return NativeCodeLoader.IsNativeCodeLoaded();
		}

		public static string GetLibraryName()
		{
			return Lz4Compressor.GetLibraryName();
		}

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
		public virtual CompressionOutputStream CreateOutputStream(OutputStream @out)
		{
			return CompressionCodec.Util.CreateOutputStreamWithCodecPool(this, conf, @out);
		}

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
		public virtual CompressionOutputStream CreateOutputStream(OutputStream @out, Compressor
			 compressor)
		{
			if (!IsNativeCodeLoaded())
			{
				throw new RuntimeException("native lz4 library not available");
			}
			int bufferSize = conf.GetInt(CommonConfigurationKeys.IoCompressionCodecLz4BuffersizeKey
				, CommonConfigurationKeys.IoCompressionCodecLz4BuffersizeDefault);
			int compressionOverhead = bufferSize / 255 + 16;
			return new BlockCompressorStream(@out, compressor, bufferSize, compressionOverhead
				);
		}

		/// <summary>
		/// Get the type of
		/// <see cref="Compressor"/>
		/// needed by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>the type of compressor needed by this codec.</returns>
		public virtual Type GetCompressorType()
		{
			if (!IsNativeCodeLoaded())
			{
				throw new RuntimeException("native lz4 library not available");
			}
			return typeof(Lz4Compressor);
		}

		/// <summary>
		/// Create a new
		/// <see cref="Compressor"/>
		/// for use by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>a new compressor for use by this codec</returns>
		public virtual Compressor CreateCompressor()
		{
			if (!IsNativeCodeLoaded())
			{
				throw new RuntimeException("native lz4 library not available");
			}
			int bufferSize = conf.GetInt(CommonConfigurationKeys.IoCompressionCodecLz4BuffersizeKey
				, CommonConfigurationKeys.IoCompressionCodecLz4BuffersizeDefault);
			bool useLz4HC = conf.GetBoolean(CommonConfigurationKeys.IoCompressionCodecLz4Uselz4hcKey
				, CommonConfigurationKeys.IoCompressionCodecLz4Uselz4hcDefault);
			return new Lz4Compressor(bufferSize, useLz4HC);
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
		public virtual CompressionInputStream CreateInputStream(InputStream @in)
		{
			return CompressionCodec.Util.CreateInputStreamWithCodecPool(this, conf, @in);
		}

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
		public virtual CompressionInputStream CreateInputStream(InputStream @in, Decompressor
			 decompressor)
		{
			if (!IsNativeCodeLoaded())
			{
				throw new RuntimeException("native lz4 library not available");
			}
			return new BlockDecompressorStream(@in, decompressor, conf.GetInt(CommonConfigurationKeys
				.IoCompressionCodecLz4BuffersizeKey, CommonConfigurationKeys.IoCompressionCodecLz4BuffersizeDefault
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
		public virtual Type GetDecompressorType()
		{
			if (!IsNativeCodeLoaded())
			{
				throw new RuntimeException("native lz4 library not available");
			}
			return typeof(Lz4Decompressor);
		}

		/// <summary>
		/// Create a new
		/// <see cref="Decompressor"/>
		/// for use by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>a new decompressor for use by this codec</returns>
		public virtual Decompressor CreateDecompressor()
		{
			if (!IsNativeCodeLoaded())
			{
				throw new RuntimeException("native lz4 library not available");
			}
			int bufferSize = conf.GetInt(CommonConfigurationKeys.IoCompressionCodecLz4BuffersizeKey
				, CommonConfigurationKeys.IoCompressionCodecLz4BuffersizeDefault);
			return new Lz4Decompressor(bufferSize);
		}

		/// <summary>Get the default filename extension for this kind of compression.</summary>
		/// <returns><code>.lz4</code>.</returns>
		public virtual string GetDefaultExtension()
		{
			return ".lz4";
		}
	}
}
