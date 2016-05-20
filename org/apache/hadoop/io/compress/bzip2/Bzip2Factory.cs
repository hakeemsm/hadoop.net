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

namespace org.apache.hadoop.io.compress.bzip2
{
	/// <summary>
	/// A collection of factories to create the right
	/// bzip2 compressor/decompressor instances.
	/// </summary>
	public class Bzip2Factory
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.bzip2.Bzip2Factory
			)));

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
		public static bool isNativeBzip2Loaded(org.apache.hadoop.conf.Configuration conf)
		{
			lock (typeof(Bzip2Factory))
			{
				string libname = conf.get("io.compression.codec.bzip2.library", "system-native");
				if (!bzip2LibraryName.Equals(libname))
				{
					nativeBzip2Loaded = false;
					bzip2LibraryName = libname;
					if (libname.Equals("java-builtin"))
					{
						LOG.info("Using pure-Java version of bzip2 library");
					}
					else
					{
						if (conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
							, org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_DEFAULT) 
							&& org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
						{
							try
							{
								// Initialize the native library.
								org.apache.hadoop.io.compress.bzip2.Bzip2Compressor.initSymbols(libname);
								org.apache.hadoop.io.compress.bzip2.Bzip2Decompressor.initSymbols(libname);
								nativeBzip2Loaded = true;
								LOG.info("Successfully loaded & initialized native-bzip2 library " + libname);
							}
							catch
							{
								LOG.warn("Failed to load/initialize native-bzip2 library " + libname + ", will use pure-Java version"
									);
							}
						}
					}
				}
				return nativeBzip2Loaded;
			}
		}

		public static string getLibraryName(org.apache.hadoop.conf.Configuration conf)
		{
			if (isNativeBzip2Loaded(conf))
			{
				return org.apache.hadoop.io.compress.bzip2.Bzip2Compressor.getLibraryName();
			}
			else
			{
				return bzip2LibraryName;
			}
		}

		/// <summary>Return the appropriate type of the bzip2 compressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate type of the bzip2 compressor.</returns>
		public static java.lang.Class getBzip2CompressorType(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return isNativeBzip2Loaded(conf) ? Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.bzip2.Bzip2Compressor
				)) : Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.bzip2.BZip2DummyCompressor
				));
		}

		/// <summary>Return the appropriate implementation of the bzip2 compressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate implementation of the bzip2 compressor.</returns>
		public static org.apache.hadoop.io.compress.Compressor getBzip2Compressor(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return isNativeBzip2Loaded(conf) ? new org.apache.hadoop.io.compress.bzip2.Bzip2Compressor
				(conf) : new org.apache.hadoop.io.compress.bzip2.BZip2DummyCompressor();
		}

		/// <summary>Return the appropriate type of the bzip2 decompressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate type of the bzip2 decompressor.</returns>
		public static java.lang.Class getBzip2DecompressorType(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return isNativeBzip2Loaded(conf) ? Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.bzip2.Bzip2Decompressor
				)) : Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.bzip2.BZip2DummyDecompressor
				));
		}

		/// <summary>Return the appropriate implementation of the bzip2 decompressor.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the appropriate implementation of the bzip2 decompressor.</returns>
		public static org.apache.hadoop.io.compress.Decompressor getBzip2Decompressor(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return isNativeBzip2Loaded(conf) ? new org.apache.hadoop.io.compress.bzip2.Bzip2Decompressor
				() : new org.apache.hadoop.io.compress.bzip2.BZip2DummyDecompressor();
		}

		public static void setBlockSize(org.apache.hadoop.conf.Configuration conf, int blockSize
			)
		{
			conf.setInt("bzip2.compress.blocksize", blockSize);
		}

		public static int getBlockSize(org.apache.hadoop.conf.Configuration conf)
		{
			return conf.getInt("bzip2.compress.blocksize", org.apache.hadoop.io.compress.bzip2.Bzip2Compressor
				.DEFAULT_BLOCK_SIZE);
		}

		public static void setWorkFactor(org.apache.hadoop.conf.Configuration conf, int workFactor
			)
		{
			conf.setInt("bzip2.compress.workfactor", workFactor);
		}

		public static int getWorkFactor(org.apache.hadoop.conf.Configuration conf)
		{
			return conf.getInt("bzip2.compress.workfactor", org.apache.hadoop.io.compress.bzip2.Bzip2Compressor
				.DEFAULT_WORK_FACTOR);
		}
	}
}
