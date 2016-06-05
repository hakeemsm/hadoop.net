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
using Org.Apache.Hadoop.IO.Compress.Zlib;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.Compress
{
	/// <summary>This class creates gzip compressors/decompressors.</summary>
	public class GzipCodec : DefaultCodec
	{
		/// <summary>
		/// A bridge that wraps around a DeflaterOutputStream to make it
		/// a CompressionOutputStream.
		/// </summary>
		protected internal class GzipOutputStream : CompressorStream
		{
			private class ResetableGZIPOutputStream : GZIPOutputStream
			{
				private const int TrailerSize = 8;

				public static readonly string JVMVersion = Runtime.GetProperty("java.version");

				private static readonly bool HasBrokenFinish = (PlatformName.IbmJava && JVMVersion
					.Contains("1.6.0"));

				/// <exception cref="System.IO.IOException"/>
				public ResetableGZIPOutputStream(OutputStream @out)
					: base(@out)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void ResetState()
				{
					def.Reset();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public GzipOutputStream(OutputStream @out)
				: base(new GzipCodec.GzipOutputStream.ResetableGZIPOutputStream(@out))
			{
			}

			/// <summary>Allow children types to put a different type in here.</summary>
			/// <param name="out">the Deflater stream to use</param>
			protected internal GzipOutputStream(CompressorStream @out)
				: base(@out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				@out.Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				@out.Flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(int b)
			{
				@out.Write(b);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] data, int offset, int length)
			{
				@out.Write(data, offset, length);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Finish()
			{
				((GzipCodec.GzipOutputStream.ResetableGZIPOutputStream)@out).Finish();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ResetState()
			{
				((GzipCodec.GzipOutputStream.ResetableGZIPOutputStream)@out).ResetState();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override CompressionOutputStream CreateOutputStream(OutputStream @out)
		{
			if (!ZlibFactory.IsNativeZlibLoaded(conf))
			{
				return new GzipCodec.GzipOutputStream(@out);
			}
			return CompressionCodec.Util.CreateOutputStreamWithCodecPool(this, conf, @out);
		}

		/// <exception cref="System.IO.IOException"/>
		public override CompressionOutputStream CreateOutputStream(OutputStream @out, Compressor
			 compressor)
		{
			return (compressor != null) ? new CompressorStream(@out, compressor, conf.GetInt(
				"io.file.buffer.size", 4 * 1024)) : CreateOutputStream(@out);
		}

		public override Compressor CreateCompressor()
		{
			return (ZlibFactory.IsNativeZlibLoaded(conf)) ? new GzipCodec.GzipZlibCompressor(
				conf) : null;
		}

		public override Type GetCompressorType()
		{
			return ZlibFactory.IsNativeZlibLoaded(conf) ? typeof(GzipCodec.GzipZlibCompressor
				) : null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override CompressionInputStream CreateInputStream(InputStream @in)
		{
			return CompressionCodec.Util.CreateInputStreamWithCodecPool(this, conf, @in);
		}

		/// <exception cref="System.IO.IOException"/>
		public override CompressionInputStream CreateInputStream(InputStream @in, Decompressor
			 decompressor)
		{
			if (decompressor == null)
			{
				decompressor = CreateDecompressor();
			}
			// always succeeds (or throws)
			return new DecompressorStream(@in, decompressor, conf.GetInt("io.file.buffer.size"
				, 4 * 1024));
		}

		public override Decompressor CreateDecompressor()
		{
			return (ZlibFactory.IsNativeZlibLoaded(conf)) ? new GzipCodec.GzipZlibDecompressor
				() : new BuiltInGzipDecompressor();
		}

		public override Type GetDecompressorType()
		{
			return ZlibFactory.IsNativeZlibLoaded(conf) ? typeof(GzipCodec.GzipZlibDecompressor
				) : typeof(BuiltInGzipDecompressor);
		}

		public override DirectDecompressor CreateDirectDecompressor()
		{
			return ZlibFactory.IsNativeZlibLoaded(conf) ? new ZlibDecompressor.ZlibDirectDecompressor
				(ZlibDecompressor.CompressionHeader.AutodetectGzipZlib, 0) : null;
		}

		public override string GetDefaultExtension()
		{
			return ".gz";
		}

		internal sealed class GzipZlibCompressor : ZlibCompressor
		{
			public GzipZlibCompressor()
				: base(ZlibCompressor.CompressionLevel.DefaultCompression, ZlibCompressor.CompressionStrategy
					.DefaultStrategy, ZlibCompressor.CompressionHeader.GzipFormat, 64 * 1024)
			{
			}

			public GzipZlibCompressor(Configuration conf)
				: base(ZlibFactory.GetCompressionLevel(conf), ZlibFactory.GetCompressionStrategy(
					conf), ZlibCompressor.CompressionHeader.GzipFormat, 64 * 1024)
			{
			}
		}

		internal sealed class GzipZlibDecompressor : ZlibDecompressor
		{
			public GzipZlibDecompressor()
				: base(ZlibDecompressor.CompressionHeader.AutodetectGzipZlib, 64 * 1024)
			{
			}
		}
	}
}
