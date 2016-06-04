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
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress.Bzip2
{
	/// <summary>
	/// A collection of factories to create the right
	/// bzip2 compressor/decompressor instances.
	/// </summary>
	public class Bzip2Factory
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Bzip2Factory));

		private static string bzip2LibraryName = string.Empty;

		private static bool nativeBzip2Loaded;

		/// <summary>
		/// Check if native-bzip2 code is loaded & initialized correctly and
		/// can be loaded for this job.
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <returns>
		/// <code>true</code> if native-bzip2 is loaded & initialized
		/// and can be loaded for this job, else <code>false</code>
		/// </returns>
		public static bool IsNativeBzip2Loaded(Configuration conf)
		{
			lock (typeof(Bzip2Factory))
			{
				string libname = conf.Get("io.compression.codec.bzip2.library", "system-native");
				if (!bzip2LibraryName.Equals(libname))
				{
					nativeBzip2Loaded = false;
					bzip2LibraryName = libname;
					if (libname.Equals("java-builtin"))
					{
						Log.Info("Using pure-Java version of bzip2 library");
					}
					else
					{
						if (conf.GetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, CommonConfigurationKeys
							.IoNativeLibAvailableDefault) && NativeCodeLoader.IsNativeCodeLoaded())
						{
							try
							{
								// Initialize the native library.
								Bzip2Compressor.InitSymbols(libname);
								Bzip2Decompressor.InitSymbols(libname);
								nativeBzip2Loaded = true;
								Log.Info("Successfully loaded & initialized native-bzip2 library " + libname);
							}
							catch
							{
								Log.Warn("Failed to load/initialize native-bzip2 library " + libname + ", will use pure-Java version"
									);
							}
						}
					}
				}
				return nativeBzip2Loaded;
			}
		}

		public static string GetLibraryName(Configuration conf)
		{
			if (IsNativeBzip2Loaded(conf))
			{
				return Bzip2Compressor.GetLibraryName();
			}
			else
			{
				return bzip2LibraryName;
			}
		}

		/// <summary>Return the appropriate type of the bzip2 compressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate type of the bzip2 compressor.</returns>
		public static Type GetBzip2CompressorType(Configuration conf)
		{
			return IsNativeBzip2Loaded(conf) ? typeof(Bzip2Compressor) : typeof(BZip2DummyCompressor
				);
		}

		/// <summary>Return the appropriate implementation of the bzip2 compressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate implementation of the bzip2 compressor.</returns>
		public static Compressor GetBzip2Compressor(Configuration conf)
		{
			return IsNativeBzip2Loaded(conf) ? new Bzip2Compressor(conf) : new BZip2DummyCompressor
				();
		}

		/// <summary>Return the appropriate type of the bzip2 decompressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate type of the bzip2 decompressor.</returns>
		public static Type GetBzip2DecompressorType(Configuration conf)
		{
			return IsNativeBzip2Loaded(conf) ? typeof(Bzip2Decompressor) : typeof(BZip2DummyDecompressor
				);
		}

		/// <summary>Return the appropriate implementation of the bzip2 decompressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate implementation of the bzip2 decompressor.</returns>
		public static Decompressor GetBzip2Decompressor(Configuration conf)
		{
			return IsNativeBzip2Loaded(conf) ? new Bzip2Decompressor() : new BZip2DummyDecompressor
				();
		}

		public static void SetBlockSize(Configuration conf, int blockSize)
		{
			conf.SetInt("bzip2.compress.blocksize", blockSize);
		}

		public static int GetBlockSize(Configuration conf)
		{
			return conf.GetInt("bzip2.compress.blocksize", Bzip2Compressor.DefaultBlockSize);
		}

		public static void SetWorkFactor(Configuration conf, int workFactor)
		{
			conf.SetInt("bzip2.compress.workfactor", workFactor);
		}

		public static int GetWorkFactor(Configuration conf)
		{
			return conf.GetInt("bzip2.compress.workfactor", Bzip2Compressor.DefaultWorkFactor
				);
		}
	}
}
