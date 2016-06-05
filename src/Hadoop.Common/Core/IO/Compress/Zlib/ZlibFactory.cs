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
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.Compress.Zlib
{
	/// <summary>
	/// A collection of factories to create the right
	/// zlib/gzip compressor/decompressor instances.
	/// </summary>
	public class ZlibFactory
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ZlibFactory));

		private static bool nativeZlibLoaded = false;

		static ZlibFactory()
		{
			if (NativeCodeLoader.IsNativeCodeLoaded())
			{
				nativeZlibLoaded = ZlibCompressor.IsNativeZlibLoaded() && ZlibDecompressor.IsNativeZlibLoaded
					();
				if (nativeZlibLoaded)
				{
					Log.Info("Successfully loaded & initialized native-zlib library");
				}
				else
				{
					Log.Warn("Failed to load/initialize native-zlib library");
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
		public static bool IsNativeZlibLoaded(Configuration conf)
		{
			return nativeZlibLoaded && conf.GetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey
				, CommonConfigurationKeys.IoNativeLibAvailableDefault);
		}

		public static string GetLibraryName()
		{
			return ZlibCompressor.GetLibraryName();
		}

		/// <summary>Return the appropriate type of the zlib compressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate type of the zlib compressor.</returns>
		public static Type GetZlibCompressorType(Configuration conf)
		{
			return (IsNativeZlibLoaded(conf)) ? typeof(ZlibCompressor) : typeof(BuiltInZlibDeflater
				);
		}

		/// <summary>Return the appropriate implementation of the zlib compressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate implementation of the zlib compressor.</returns>
		public static Compressor GetZlibCompressor(Configuration conf)
		{
			return (IsNativeZlibLoaded(conf)) ? new ZlibCompressor(conf) : new BuiltInZlibDeflater
				(ZlibFactory.GetCompressionLevel(conf).CompressionLevel());
		}

		/// <summary>Return the appropriate type of the zlib decompressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate type of the zlib decompressor.</returns>
		public static Type GetZlibDecompressorType(Configuration conf)
		{
			return (IsNativeZlibLoaded(conf)) ? typeof(ZlibDecompressor) : typeof(BuiltInZlibInflater
				);
		}

		/// <summary>Return the appropriate implementation of the zlib decompressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate implementation of the zlib decompressor.</returns>
		public static Decompressor GetZlibDecompressor(Configuration conf)
		{
			return (IsNativeZlibLoaded(conf)) ? new ZlibDecompressor() : new BuiltInZlibInflater
				();
		}

		/// <summary>Return the appropriate implementation of the zlib direct decompressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate implementation of the zlib decompressor.</returns>
		public static DirectDecompressor GetZlibDirectDecompressor(Configuration conf)
		{
			return (IsNativeZlibLoaded(conf)) ? new ZlibDecompressor.ZlibDirectDecompressor()
				 : null;
		}

		public static void SetCompressionStrategy(Configuration conf, ZlibCompressor.CompressionStrategy
			 strategy)
		{
			conf.SetEnum("zlib.compress.strategy", strategy);
		}

		public static ZlibCompressor.CompressionStrategy GetCompressionStrategy(Configuration
			 conf)
		{
			return conf.GetEnum("zlib.compress.strategy", ZlibCompressor.CompressionStrategy.
				DefaultStrategy);
		}

		public static void SetCompressionLevel(Configuration conf, ZlibCompressor.CompressionLevel
			 level)
		{
			conf.SetEnum("zlib.compress.level", level);
		}

		public static ZlibCompressor.CompressionLevel GetCompressionLevel(Configuration conf
			)
		{
			return conf.GetEnum("zlib.compress.level", ZlibCompressor.CompressionLevel.DefaultCompression
				);
		}
	}
}
