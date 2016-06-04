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
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress.Zlib
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.IO.Compress.Compressor"/>
	/// based on the popular
	/// zlib compression algorithm.
	/// http://www.zlib.net/
	/// </summary>
	public class ZlibCompressor : Compressor
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.Compress.Zlib.ZlibCompressor
			));

		private const int DefaultDirectBufferSize = 64 * 1024;

		private static Type clazz = typeof(Org.Apache.Hadoop.IO.Compress.Zlib.ZlibCompressor
			);

		private long stream;

		private ZlibCompressor.CompressionLevel level;

		private ZlibCompressor.CompressionStrategy strategy;

		private readonly ZlibCompressor.CompressionHeader windowBits;

		private int directBufferSize;

		private byte[] userBuf = null;

		private int userBufOff = 0;

		private int userBufLen = 0;

		private Buffer uncompressedDirectBuf = null;

		private int uncompressedDirectBufOff = 0;

		private int uncompressedDirectBufLen = 0;

		private bool keepUncompressedBuf = false;

		private Buffer compressedDirectBuf = null;

		private bool finish;

		private bool finished;

		/// <summary>The compression level for zlib library.</summary>
		[System.Serializable]
		public sealed class CompressionLevel
		{
			/// <summary>Compression level for no compression.</summary>
			public static readonly ZlibCompressor.CompressionLevel NoCompression = new ZlibCompressor.CompressionLevel
				(0);

			/// <summary>Compression level for fastest compression.</summary>
			public static readonly ZlibCompressor.CompressionLevel BestSpeed = new ZlibCompressor.CompressionLevel
				(1);

			/// <summary>Compression level for best compression.</summary>
			public static readonly ZlibCompressor.CompressionLevel BestCompression = new ZlibCompressor.CompressionLevel
				(9);

			/// <summary>Default compression level.</summary>
			public static readonly ZlibCompressor.CompressionLevel DefaultCompression = new ZlibCompressor.CompressionLevel
				(-1);

			private readonly int compressionLevel;

			internal CompressionLevel(int level)
			{
				// HACK - Use this as a global lock in the JNI layer
				ZlibCompressor.CompressionLevel.compressionLevel = level;
			}

			internal int CompressionLevel()
			{
				return ZlibCompressor.CompressionLevel.compressionLevel;
			}
		}

		/// <summary>The compression level for zlib library.</summary>
		[System.Serializable]
		public sealed class CompressionStrategy
		{
			/// <summary>
			/// Compression strategy best used for data consisting mostly of small
			/// values with a somewhat random distribution.
			/// </summary>
			/// <remarks>
			/// Compression strategy best used for data consisting mostly of small
			/// values with a somewhat random distribution. Forces more Huffman coding
			/// and less string matching.
			/// </remarks>
			public static readonly ZlibCompressor.CompressionStrategy Filtered = new ZlibCompressor.CompressionStrategy
				(1);

			/// <summary>Compression strategy for Huffman coding only.</summary>
			public static readonly ZlibCompressor.CompressionStrategy HuffmanOnly = new ZlibCompressor.CompressionStrategy
				(2);

			/// <summary>
			/// Compression strategy to limit match distances to one
			/// (run-length encoding).
			/// </summary>
			public static readonly ZlibCompressor.CompressionStrategy Rle = new ZlibCompressor.CompressionStrategy
				(3);

			/// <summary>
			/// Compression strategy to prevent the use of dynamic Huffman codes,
			/// allowing for a simpler decoder for special applications.
			/// </summary>
			public static readonly ZlibCompressor.CompressionStrategy Fixed = new ZlibCompressor.CompressionStrategy
				(4);

			/// <summary>Default compression strategy.</summary>
			public static readonly ZlibCompressor.CompressionStrategy DefaultStrategy = new ZlibCompressor.CompressionStrategy
				(0);

			private readonly int compressionStrategy;

			internal CompressionStrategy(int strategy)
			{
				ZlibCompressor.CompressionStrategy.compressionStrategy = strategy;
			}

			internal int CompressionStrategy()
			{
				return ZlibCompressor.CompressionStrategy.compressionStrategy;
			}
		}

		/// <summary>The type of header for compressed data.</summary>
		[System.Serializable]
		public sealed class CompressionHeader
		{
			/// <summary>No headers/trailers/checksums.</summary>
			public static readonly ZlibCompressor.CompressionHeader NoHeader = new ZlibCompressor.CompressionHeader
				(-15);

			/// <summary>Default headers/trailers/checksums.</summary>
			public static readonly ZlibCompressor.CompressionHeader DefaultHeader = new ZlibCompressor.CompressionHeader
				(15);

			/// <summary>Simple gzip headers/trailers.</summary>
			public static readonly ZlibCompressor.CompressionHeader GzipFormat = new ZlibCompressor.CompressionHeader
				(31);

			private readonly int windowBits;

			internal CompressionHeader(int windowBits)
			{
				this.windowBits = windowBits;
			}

			public int WindowBits()
			{
				return ZlibCompressor.CompressionHeader.windowBits;
			}
		}

		private static bool nativeZlibLoaded = false;

		static ZlibCompressor()
		{
			if (NativeCodeLoader.IsNativeCodeLoaded())
			{
				try
				{
					// Initialize the native library
					InitIDs();
					nativeZlibLoaded = true;
				}
				catch
				{
				}
			}
		}

		// Ignore failure to load/initialize native-zlib
		internal static bool IsNativeZlibLoaded()
		{
			return nativeZlibLoaded;
		}

		protected internal void Construct(ZlibCompressor.CompressionLevel level, ZlibCompressor.CompressionStrategy
			 strategy, ZlibCompressor.CompressionHeader header, int directBufferSize)
		{
		}

		/// <summary>Creates a new compressor with the default compression level.</summary>
		/// <remarks>
		/// Creates a new compressor with the default compression level.
		/// Compressed data will be generated in ZLIB format.
		/// </remarks>
		public ZlibCompressor()
			: this(ZlibCompressor.CompressionLevel.DefaultCompression, ZlibCompressor.CompressionStrategy
				.DefaultStrategy, ZlibCompressor.CompressionHeader.DefaultHeader, DefaultDirectBufferSize
				)
		{
		}

		/// <summary>Creates a new compressor, taking settings from the configuration.</summary>
		public ZlibCompressor(Configuration conf)
			: this(ZlibFactory.GetCompressionLevel(conf), ZlibFactory.GetCompressionStrategy(
				conf), ZlibCompressor.CompressionHeader.DefaultHeader, DefaultDirectBufferSize)
		{
		}

		/// <summary>Creates a new compressor using the specified compression level.</summary>
		/// <remarks>
		/// Creates a new compressor using the specified compression level.
		/// Compressed data will be generated in ZLIB format.
		/// </remarks>
		/// <param name="level">Compression level #CompressionLevel</param>
		/// <param name="strategy">Compression strategy #CompressionStrategy</param>
		/// <param name="header">Compression header #CompressionHeader</param>
		/// <param name="directBufferSize">Size of the direct buffer to be used.</param>
		public ZlibCompressor(ZlibCompressor.CompressionLevel level, ZlibCompressor.CompressionStrategy
			 strategy, ZlibCompressor.CompressionHeader header, int directBufferSize)
		{
			this.level = level;
			this.strategy = strategy;
			this.windowBits = header;
			stream = Init(this.level.CompressionLevel(), this.strategy.CompressionStrategy(), 
				this.windowBits.WindowBits());
			this.directBufferSize = directBufferSize;
			uncompressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			compressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			compressedDirectBuf.Position(directBufferSize);
		}

		/// <summary>
		/// Prepare the compressor to be used in a new stream with settings defined in
		/// the given Configuration.
		/// </summary>
		/// <remarks>
		/// Prepare the compressor to be used in a new stream with settings defined in
		/// the given Configuration. It will reset the compressor's compression level
		/// and compression strategy.
		/// </remarks>
		/// <param name="conf">Configuration storing new settings</param>
		public virtual void Reinit(Configuration conf)
		{
			Reset();
			if (conf == null)
			{
				return;
			}
			End(stream);
			level = ZlibFactory.GetCompressionLevel(conf);
			strategy = ZlibFactory.GetCompressionStrategy(conf);
			stream = Init(level.CompressionLevel(), strategy.CompressionStrategy(), windowBits
				.WindowBits());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Reinit compressor with new compression configuration");
			}
		}

		public virtual void SetInput(byte[] b, int off, int len)
		{
			if (b == null)
			{
				throw new ArgumentNullException();
			}
			if (off < 0 || len < 0 || off > b.Length - len)
			{
				throw new IndexOutOfRangeException();
			}
			this.userBuf = b;
			this.userBufOff = off;
			this.userBufLen = len;
			uncompressedDirectBufOff = 0;
			SetInputFromSavedData();
			// Reinitialize zlib's output direct buffer 
			compressedDirectBuf.Limit(directBufferSize);
			compressedDirectBuf.Position(directBufferSize);
		}

		//copy enough data from userBuf to uncompressedDirectBuf
		internal virtual void SetInputFromSavedData()
		{
			int len = Math.Min(userBufLen, uncompressedDirectBuf.Remaining());
			((ByteBuffer)uncompressedDirectBuf).Put(userBuf, userBufOff, len);
			userBufLen -= len;
			userBufOff += len;
			uncompressedDirectBufLen = uncompressedDirectBuf.Position();
		}

		public virtual void SetDictionary(byte[] b, int off, int len)
		{
			if (stream == 0 || b == null)
			{
				throw new ArgumentNullException();
			}
			if (off < 0 || len < 0 || off > b.Length - len)
			{
				throw new IndexOutOfRangeException();
			}
			SetDictionary(stream, b, off, len);
		}

		public virtual bool NeedsInput()
		{
			// Consume remaining compressed data?
			if (compressedDirectBuf.Remaining() > 0)
			{
				return false;
			}
			// Check if zlib has consumed all input
			// compress should be invoked if keepUncompressedBuf true
			if (keepUncompressedBuf && uncompressedDirectBufLen > 0)
			{
				return false;
			}
			if (uncompressedDirectBuf.Remaining() > 0)
			{
				// Check if we have consumed all user-input
				if (userBufLen <= 0)
				{
					return true;
				}
				else
				{
					// copy enough data from userBuf to uncompressedDirectBuf
					SetInputFromSavedData();
					if (uncompressedDirectBuf.Remaining() > 0)
					{
						// uncompressedDirectBuf is not full
						return true;
					}
					else
					{
						return false;
					}
				}
			}
			return false;
		}

		public virtual void Finish()
		{
			finish = true;
		}

		public virtual bool Finished()
		{
			// Check if 'zlib' says its 'finished' and
			// all compressed data has been consumed
			return (finished && compressedDirectBuf.Remaining() == 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Compress(byte[] b, int off, int len)
		{
			if (b == null)
			{
				throw new ArgumentNullException();
			}
			if (off < 0 || len < 0 || off > b.Length - len)
			{
				throw new IndexOutOfRangeException();
			}
			int n = 0;
			// Check if there is compressed data
			n = compressedDirectBuf.Remaining();
			if (n > 0)
			{
				n = Math.Min(n, len);
				((ByteBuffer)compressedDirectBuf).Get(b, off, n);
				return n;
			}
			// Re-initialize the zlib's output direct buffer
			compressedDirectBuf.Rewind();
			compressedDirectBuf.Limit(directBufferSize);
			// Compress data
			n = DeflateBytesDirect();
			compressedDirectBuf.Limit(n);
			// Check if zlib consumed all input buffer
			// set keepUncompressedBuf properly
			if (uncompressedDirectBufLen <= 0)
			{
				// zlib consumed all input buffer
				keepUncompressedBuf = false;
				uncompressedDirectBuf.Clear();
				uncompressedDirectBufOff = 0;
				uncompressedDirectBufLen = 0;
			}
			else
			{
				// zlib did not consume all input buffer
				keepUncompressedBuf = true;
			}
			// Get atmost 'len' bytes
			n = Math.Min(n, len);
			((ByteBuffer)compressedDirectBuf).Get(b, off, n);
			return n;
		}

		/// <summary>Returns the total number of compressed bytes output so far.</summary>
		/// <returns>the total (non-negative) number of compressed bytes output so far</returns>
		public virtual long GetBytesWritten()
		{
			CheckStream();
			return GetBytesWritten(stream);
		}

		/// <summary>Returns the total number of uncompressed bytes input so far.</p></summary>
		/// <returns>the total (non-negative) number of uncompressed bytes input so far</returns>
		public virtual long GetBytesRead()
		{
			CheckStream();
			return GetBytesRead(stream);
		}

		public virtual void Reset()
		{
			CheckStream();
			Reset(stream);
			finish = false;
			finished = false;
			uncompressedDirectBuf.Rewind();
			uncompressedDirectBufOff = uncompressedDirectBufLen = 0;
			keepUncompressedBuf = false;
			compressedDirectBuf.Limit(directBufferSize);
			compressedDirectBuf.Position(directBufferSize);
			userBufOff = userBufLen = 0;
		}

		public virtual void End()
		{
			if (stream != 0)
			{
				End(stream);
				stream = 0;
			}
		}

		private void CheckStream()
		{
			if (stream == 0)
			{
				throw new ArgumentNullException();
			}
		}

		private static void InitIDs()
		{
		}

		private static long Init(int level, int strategy, int windowBits)
		{
		}

		private static void SetDictionary(long strm, byte[] b, int off, int len)
		{
		}

		private int DeflateBytesDirect()
		{
		}

		private static long GetBytesRead(long strm)
		{
		}

		private static long GetBytesWritten(long strm)
		{
		}

		private static void Reset(long strm)
		{
		}

		private static void End(long strm)
		{
		}

		public static string GetLibraryName()
		{
		}
	}
}
