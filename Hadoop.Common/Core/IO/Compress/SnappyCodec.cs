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
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress.Snappy;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	/// <summary>This class creates snappy compressors/decompressors.</summary>
	public class SnappyCodec : Configurable, CompressionCodec, DirectDecompressionCodec
	{
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

		/// <summary>Are the native snappy libraries loaded & initialized?</summary>
		public static void CheckNativeCodeLoaded()
		{
			if (!NativeCodeLoader.IsNativeCodeLoaded() || !NativeCodeLoader.BuildSupportsSnappy
				())
			{
				throw new RuntimeException("native snappy library not available: " + "this version of libhadoop was built without "
					 + "snappy support.");
			}
			if (!SnappyCompressor.IsNativeCodeLoaded())
			{
				throw new RuntimeException("native snappy library not available: " + "SnappyCompressor has not been loaded."
					);
			}
			if (!SnappyDecompressor.IsNativeCodeLoaded())
			{
				throw new RuntimeException("native snappy library not available: " + "SnappyDecompressor has not been loaded."
					);
			}
		}

		public static bool IsNativeCodeLoaded()
		{
			return SnappyCompressor.IsNativeCodeLoaded() && SnappyDecompressor.IsNativeCodeLoaded
				();
		}

		public static string GetLibraryName()
		{
			return SnappyCompressor.GetLibraryName();
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
			CheckNativeCodeLoaded();
			int bufferSize = conf.GetInt(CommonConfigurationKeys.IoCompressionCodecSnappyBuffersizeKey
				, CommonConfigurationKeys.IoCompressionCodecSnappyBuffersizeDefault);
			int compressionOverhead = (bufferSize / 6) + 32;
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
			CheckNativeCodeLoaded();
			return typeof(SnappyCompressor);
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
			CheckNativeCodeLoaded();
			int bufferSize = conf.GetInt(CommonConfigurationKeys.IoCompressionCodecSnappyBuffersizeKey
				, CommonConfigurationKeys.IoCompressionCodecSnappyBuffersizeDefault);
			return new SnappyCompressor(bufferSize);
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
			CheckNativeCodeLoaded();
			return new BlockDecompressorStream(@in, decompressor, conf.GetInt(CommonConfigurationKeys
				.IoCompressionCodecSnappyBuffersizeKey, CommonConfigurationKeys.IoCompressionCodecSnappyBuffersizeDefault
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
			CheckNativeCodeLoaded();
			return typeof(SnappyDecompressor);
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
			CheckNativeCodeLoaded();
			int bufferSize = conf.GetInt(CommonConfigurationKeys.IoCompressionCodecSnappyBuffersizeKey
				, CommonConfigurationKeys.IoCompressionCodecSnappyBuffersizeDefault);
			return new SnappyDecompressor(bufferSize);
		}

		/// <summary><inheritDoc/></summary>
		public virtual DirectDecompressor CreateDirectDecompressor()
		{
			return IsNativeCodeLoaded() ? new SnappyDecompressor.SnappyDirectDecompressor() : 
				null;
		}

		/// <summary>Get the default filename extension for this kind of compression.</summary>
		/// <returns><code>.snappy</code>.</returns>
		public virtual string GetDefaultExtension()
		{
			return ".snappy";
		}
	}
}
