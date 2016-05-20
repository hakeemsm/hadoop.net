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

namespace org.apache.hadoop.io.compress.zlib
{
	/// <summary>
	/// A collection of factories to create the right
	/// zlib/gzip compressor/decompressor instances.
	/// </summary>
	public class ZlibFactory
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.ZlibFactory
			)));

		private static bool nativeZlibLoaded = false;

		static ZlibFactory()
		{
			if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
			{
				nativeZlibLoaded = org.apache.hadoop.io.compress.zlib.ZlibCompressor.isNativeZlibLoaded
					() && org.apache.hadoop.io.compress.zlib.ZlibDecompressor.isNativeZlibLoaded();
				if (nativeZlibLoaded)
				{
					LOG.info("Successfully loaded & initialized native-zlib library");
				}
				else
				{
					LOG.warn("Failed to load/initialize native-zlib library");
				}
			}
		}

		/// <summary>
		/// Check if native-zlib code is loaded & initialized correctly and
		/// can be loaded for this job.
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <returns>
		/// <code>true</code> if native-zlib is loaded & initialized
		/// and can be loaded for this job, else <code>false</code>
		/// </returns>
		public static bool isNativeZlibLoaded(org.apache.hadoop.conf.Configuration conf)
		{
			return nativeZlibLoaded && conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys
				.IO_NATIVE_LIB_AVAILABLE_KEY, org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_DEFAULT
				);
		}

		public static string getLibraryName()
		{
			return org.apache.hadoop.io.compress.zlib.ZlibCompressor.getLibraryName();
		}

		/// <summary>Return the appropriate type of the zlib compressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate type of the zlib compressor.</returns>
		public static java.lang.Class getZlibCompressorType(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return (isNativeZlibLoaded(conf)) ? Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.ZlibCompressor
				)) : Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater
				));
		}

		/// <summary>Return the appropriate implementation of the zlib compressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate implementation of the zlib compressor.</returns>
		public static org.apache.hadoop.io.compress.Compressor getZlibCompressor(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return (isNativeZlibLoaded(conf)) ? new org.apache.hadoop.io.compress.zlib.ZlibCompressor
				(conf) : new org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater(org.apache.hadoop.io.compress.zlib.ZlibFactory
				.getCompressionLevel(conf).compressionLevel());
		}

		/// <summary>Return the appropriate type of the zlib decompressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate type of the zlib decompressor.</returns>
		public static java.lang.Class getZlibDecompressorType(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return (isNativeZlibLoaded(conf)) ? Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.ZlibDecompressor
				)) : Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.BuiltInZlibInflater
				));
		}

		/// <summary>Return the appropriate implementation of the zlib decompressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate implementation of the zlib decompressor.</returns>
		public static org.apache.hadoop.io.compress.Decompressor getZlibDecompressor(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return (isNativeZlibLoaded(conf)) ? new org.apache.hadoop.io.compress.zlib.ZlibDecompressor
				() : new org.apache.hadoop.io.compress.zlib.BuiltInZlibInflater();
		}

		/// <summary>Return the appropriate implementation of the zlib direct decompressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate implementation of the zlib decompressor.</returns>
		public static org.apache.hadoop.io.compress.DirectDecompressor getZlibDirectDecompressor
			(org.apache.hadoop.conf.Configuration conf)
		{
			return (isNativeZlibLoaded(conf)) ? new org.apache.hadoop.io.compress.zlib.ZlibDecompressor.ZlibDirectDecompressor
				() : null;
		}

		public static void setCompressionStrategy(org.apache.hadoop.conf.Configuration conf
			, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy strategy
			)
		{
			conf.setEnum("zlib.compress.strategy", strategy);
		}

		public static org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
			 getCompressionStrategy(org.apache.hadoop.conf.Configuration conf)
		{
			return conf.getEnum("zlib.compress.strategy", org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				.DEFAULT_STRATEGY);
		}

		public static void setCompressionLevel(org.apache.hadoop.conf.Configuration conf, 
			org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel level)
		{
			conf.setEnum("zlib.compress.level", level);
		}

		public static org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel 
			getCompressionLevel(org.apache.hadoop.conf.Configuration conf)
		{
			return conf.getEnum("zlib.compress.level", org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				.DEFAULT_COMPRESSION);
		}
	}
}
