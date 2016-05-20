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
	/// <summary>This class creates gzip compressors/decompressors.</summary>
	public class GzipCodec : org.apache.hadoop.io.compress.DefaultCodec
	{
		/// <summary>
		/// A bridge that wraps around a DeflaterOutputStream to make it
		/// a CompressionOutputStream.
		/// </summary>
		protected internal class GzipOutputStream : org.apache.hadoop.io.compress.CompressorStream
		{
			private class ResetableGZIPOutputStream : java.util.zip.GZIPOutputStream
			{
				private const int TRAILER_SIZE = 8;

				public static readonly string JVMVersion = Sharpen.Runtime.getProperty("java.version"
					);

				private static readonly bool HAS_BROKEN_FINISH = (org.apache.hadoop.util.PlatformName
					.IBM_JAVA && JVMVersion.contains("1.6.0"));

				/// <exception cref="System.IO.IOException"/>
				public ResetableGZIPOutputStream(java.io.OutputStream @out)
					: base(@out)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void resetState()
				{
					def.reset();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public GzipOutputStream(java.io.OutputStream @out)
				: base(new org.apache.hadoop.io.compress.GzipCodec.GzipOutputStream.ResetableGZIPOutputStream
					(@out))
			{
			}

			/// <summary>Allow children types to put a different type in here.</summary>
			/// <param name="out">the Deflater stream to use</param>
			protected internal GzipOutputStream(org.apache.hadoop.io.compress.CompressorStream
				 @out)
				: base(@out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				@out.close();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void flush()
			{
				@out.flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(int b)
			{
				@out.write(b);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(byte[] data, int offset, int length)
			{
				@out.write(data, offset, length);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void finish()
			{
				((org.apache.hadoop.io.compress.GzipCodec.GzipOutputStream.ResetableGZIPOutputStream
					)@out).finish();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void resetState()
			{
				((org.apache.hadoop.io.compress.GzipCodec.GzipOutputStream.ResetableGZIPOutputStream
					)@out).resetState();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
			(java.io.OutputStream @out)
		{
			if (!org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf))
			{
				return new org.apache.hadoop.io.compress.GzipCodec.GzipOutputStream(@out);
			}
			return org.apache.hadoop.io.compress.CompressionCodec.Util.createOutputStreamWithCodecPool
				(this, conf, @out);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
			(java.io.OutputStream @out, org.apache.hadoop.io.compress.Compressor compressor)
		{
			return (compressor != null) ? new org.apache.hadoop.io.compress.CompressorStream(
				@out, compressor, conf.getInt("io.file.buffer.size", 4 * 1024)) : createOutputStream
				(@out);
		}

		public override org.apache.hadoop.io.compress.Compressor createCompressor()
		{
			return (org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf)) ? 
				new org.apache.hadoop.io.compress.GzipCodec.GzipZlibCompressor(conf) : null;
		}

		public override java.lang.Class getCompressorType()
		{
			return org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf) ? 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec.GzipZlibCompressor
				)) : null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.io.compress.CompressionInputStream createInputStream
			(java.io.InputStream @in)
		{
			return org.apache.hadoop.io.compress.CompressionCodec.Util.createInputStreamWithCodecPool
				(this, conf, @in);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.io.compress.CompressionInputStream createInputStream
			(java.io.InputStream @in, org.apache.hadoop.io.compress.Decompressor decompressor
			)
		{
			if (decompressor == null)
			{
				decompressor = createDecompressor();
			}
			// always succeeds (or throws)
			return new org.apache.hadoop.io.compress.DecompressorStream(@in, decompressor, conf
				.getInt("io.file.buffer.size", 4 * 1024));
		}

		public override org.apache.hadoop.io.compress.Decompressor createDecompressor()
		{
			return (org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf)) ? 
				new org.apache.hadoop.io.compress.GzipCodec.GzipZlibDecompressor() : new org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor
				();
		}

		public override java.lang.Class getDecompressorType()
		{
			return org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf) ? 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec.GzipZlibDecompressor
				)) : Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor
				));
		}

		public override org.apache.hadoop.io.compress.DirectDecompressor createDirectDecompressor
			()
		{
			return org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf) ? 
				new org.apache.hadoop.io.compress.zlib.ZlibDecompressor.ZlibDirectDecompressor(org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader
				.AUTODETECT_GZIP_ZLIB, 0) : null;
		}

		public override string getDefaultExtension()
		{
			return ".gz";
		}

		internal sealed class GzipZlibCompressor : org.apache.hadoop.io.compress.zlib.ZlibCompressor
		{
			public GzipZlibCompressor()
				: base(org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION
					, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY
					, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader.GZIP_FORMAT
					, 64 * 1024)
			{
			}

			public GzipZlibCompressor(org.apache.hadoop.conf.Configuration conf)
				: base(org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionLevel(conf), 
					org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionStrategy(conf), org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader
					.GZIP_FORMAT, 64 * 1024)
			{
			}
		}

		internal sealed class GzipZlibDecompressor : org.apache.hadoop.io.compress.zlib.ZlibDecompressor
		{
			public GzipZlibDecompressor()
				: base(org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader.AUTODETECT_GZIP_ZLIB
					, 64 * 1024)
			{
			}
		}
	}
}
