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
	/// A
	/// <see cref="org.apache.hadoop.io.compress.Compressor"/>
	/// based on the popular
	/// zlib compression algorithm.
	/// http://www.zlib.net/
	/// </summary>
	public class ZlibCompressor : org.apache.hadoop.io.compress.Compressor
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.ZlibCompressor
			)));

		private const int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

		private static java.lang.Class clazz = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.ZlibCompressor
			));

		private long stream;

		private org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel level;

		private org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy strategy;

		private readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader
			 windowBits;

		private int directBufferSize;

		private byte[] userBuf = null;

		private int userBufOff = 0;

		private int userBufLen = 0;

		private java.nio.Buffer uncompressedDirectBuf = null;

		private int uncompressedDirectBufOff = 0;

		private int uncompressedDirectBufLen = 0;

		private bool keepUncompressedBuf = false;

		private java.nio.Buffer compressedDirectBuf = null;

		private bool finish;

		private bool finished;

		/// <summary>The compression level for zlib library.</summary>
		[System.Serializable]
		public sealed class CompressionLevel
		{
			/// <summary>Compression level for no compression.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				 NO_COMPRESSION = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				(0);

			/// <summary>Compression level for fastest compression.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				 BEST_SPEED = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				(1);

			/// <summary>Compression level for best compression.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				 BEST_COMPRESSION = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				(9);

			/// <summary>Default compression level.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				 DEFAULT_COMPRESSION = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				(-1);

			private readonly int compressionLevel;

			internal CompressionLevel(int level)
			{
				// HACK - Use this as a global lock in the JNI layer
				org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel.compressionLevel
					 = level;
			}

			internal int compressionLevel()
			{
				return org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel.compressionLevel;
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
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				 FILTERED = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				(1);

			/// <summary>Compression strategy for Huffman coding only.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				 HUFFMAN_ONLY = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				(2);

			/// <summary>
			/// Compression strategy to limit match distances to one
			/// (run-length encoding).
			/// </summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				 RLE = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				(3);

			/// <summary>
			/// Compression strategy to prevent the use of dynamic Huffman codes,
			/// allowing for a simpler decoder for special applications.
			/// </summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				 FIXED = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				(4);

			/// <summary>Default compression strategy.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				 DEFAULT_STRATEGY = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				(0);

			private readonly int compressionStrategy;

			internal CompressionStrategy(int strategy)
			{
				org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy.compressionStrategy
					 = strategy;
			}

			internal int compressionStrategy()
			{
				return org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy.compressionStrategy;
			}
		}

		/// <summary>The type of header for compressed data.</summary>
		[System.Serializable]
		public sealed class CompressionHeader
		{
			/// <summary>No headers/trailers/checksums.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader
				 NO_HEADER = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader
				(-15);

			/// <summary>Default headers/trailers/checksums.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader
				 DEFAULT_HEADER = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader
				(15);

			/// <summary>Simple gzip headers/trailers.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader
				 GZIP_FORMAT = new org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader
				(31);

			private readonly int windowBits;

			internal CompressionHeader(int windowBits)
			{
				this.windowBits = windowBits;
			}

			public int windowBits()
			{
				return org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader.windowBits;
			}
		}

		private static bool nativeZlibLoaded = false;

		static ZlibCompressor()
		{
			if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
			{
				try
				{
					// Initialize the native library
					initIDs();
					nativeZlibLoaded = true;
				}
				catch
				{
				}
			}
		}

		// Ignore failure to load/initialize native-zlib
		internal static bool isNativeZlibLoaded()
		{
			return nativeZlibLoaded;
		}

		protected internal void construct(org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
			 level, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy strategy
			, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader header, int
			 directBufferSize)
		{
		}

		/// <summary>Creates a new compressor with the default compression level.</summary>
		/// <remarks>
		/// Creates a new compressor with the default compression level.
		/// Compressed data will be generated in ZLIB format.
		/// </remarks>
		public ZlibCompressor()
			: this(org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION
				, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY
				, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader.DEFAULT_HEADER
				, DEFAULT_DIRECT_BUFFER_SIZE)
		{
		}

		/// <summary>Creates a new compressor, taking settings from the configuration.</summary>
		public ZlibCompressor(org.apache.hadoop.conf.Configuration conf)
			: this(org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionLevel(conf), 
				org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionStrategy(conf), org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader
				.DEFAULT_HEADER, DEFAULT_DIRECT_BUFFER_SIZE)
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
		public ZlibCompressor(org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
			 level, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy strategy
			, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionHeader header, int
			 directBufferSize)
		{
			this.level = level;
			this.strategy = strategy;
			this.windowBits = header;
			stream = init(this.level.compressionLevel(), this.strategy.compressionStrategy(), 
				this.windowBits.windowBits());
			this.directBufferSize = directBufferSize;
			uncompressedDirectBuf = java.nio.ByteBuffer.allocateDirect(directBufferSize);
			compressedDirectBuf = java.nio.ByteBuffer.allocateDirect(directBufferSize);
			compressedDirectBuf.position(directBufferSize);
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
		public virtual void reinit(org.apache.hadoop.conf.Configuration conf)
		{
			reset();
			if (conf == null)
			{
				return;
			}
			end(stream);
			level = org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionLevel(conf);
			strategy = org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionStrategy(
				conf);
			stream = init(level.compressionLevel(), strategy.compressionStrategy(), windowBits
				.windowBits());
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Reinit compressor with new compression configuration");
			}
		}

		public virtual void setInput(byte[] b, int off, int len)
		{
			if (b == null)
			{
				throw new System.ArgumentNullException();
			}
			if (off < 0 || len < 0 || off > b.Length - len)
			{
				throw new System.IndexOutOfRangeException();
			}
			this.userBuf = b;
			this.userBufOff = off;
			this.userBufLen = len;
			uncompressedDirectBufOff = 0;
			setInputFromSavedData();
			// Reinitialize zlib's output direct buffer 
			compressedDirectBuf.limit(directBufferSize);
			compressedDirectBuf.position(directBufferSize);
		}

		//copy enough data from userBuf to uncompressedDirectBuf
		internal virtual void setInputFromSavedData()
		{
			int len = System.Math.min(userBufLen, uncompressedDirectBuf.remaining());
			((java.nio.ByteBuffer)uncompressedDirectBuf).put(userBuf, userBufOff, len);
			userBufLen -= len;
			userBufOff += len;
			uncompressedDirectBufLen = uncompressedDirectBuf.position();
		}

		public virtual void setDictionary(byte[] b, int off, int len)
		{
			if (stream == 0 || b == null)
			{
				throw new System.ArgumentNullException();
			}
			if (off < 0 || len < 0 || off > b.Length - len)
			{
				throw new System.IndexOutOfRangeException();
			}
			setDictionary(stream, b, off, len);
		}

		public virtual bool needsInput()
		{
			// Consume remaining compressed data?
			if (compressedDirectBuf.remaining() > 0)
			{
				return false;
			}
			// Check if zlib has consumed all input
			// compress should be invoked if keepUncompressedBuf true
			if (keepUncompressedBuf && uncompressedDirectBufLen > 0)
			{
				return false;
			}
			if (uncompressedDirectBuf.remaining() > 0)
			{
				// Check if we have consumed all user-input
				if (userBufLen <= 0)
				{
					return true;
				}
				else
				{
					// copy enough data from userBuf to uncompressedDirectBuf
					setInputFromSavedData();
					if (uncompressedDirectBuf.remaining() > 0)
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

		public virtual void finish()
		{
			finish = true;
		}

		public virtual bool finished()
		{
			// Check if 'zlib' says its 'finished' and
			// all compressed data has been consumed
			return (finished && compressedDirectBuf.remaining() == 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int compress(byte[] b, int off, int len)
		{
			if (b == null)
			{
				throw new System.ArgumentNullException();
			}
			if (off < 0 || len < 0 || off > b.Length - len)
			{
				throw new System.IndexOutOfRangeException();
			}
			int n = 0;
			// Check if there is compressed data
			n = compressedDirectBuf.remaining();
			if (n > 0)
			{
				n = System.Math.min(n, len);
				((java.nio.ByteBuffer)compressedDirectBuf).get(b, off, n);
				return n;
			}
			// Re-initialize the zlib's output direct buffer
			compressedDirectBuf.rewind();
			compressedDirectBuf.limit(directBufferSize);
			// Compress data
			n = deflateBytesDirect();
			compressedDirectBuf.limit(n);
			// Check if zlib consumed all input buffer
			// set keepUncompressedBuf properly
			if (uncompressedDirectBufLen <= 0)
			{
				// zlib consumed all input buffer
				keepUncompressedBuf = false;
				uncompressedDirectBuf.clear();
				uncompressedDirectBufOff = 0;
				uncompressedDirectBufLen = 0;
			}
			else
			{
				// zlib did not consume all input buffer
				keepUncompressedBuf = true;
			}
			// Get atmost 'len' bytes
			n = System.Math.min(n, len);
			((java.nio.ByteBuffer)compressedDirectBuf).get(b, off, n);
			return n;
		}

		/// <summary>Returns the total number of compressed bytes output so far.</summary>
		/// <returns>the total (non-negative) number of compressed bytes output so far</returns>
		public virtual long getBytesWritten()
		{
			checkStream();
			return getBytesWritten(stream);
		}

		/// <summary>Returns the total number of uncompressed bytes input so far.</p></summary>
		/// <returns>the total (non-negative) number of uncompressed bytes input so far</returns>
		public virtual long getBytesRead()
		{
			checkStream();
			return getBytesRead(stream);
		}

		public virtual void reset()
		{
			checkStream();
			reset(stream);
			finish = false;
			finished = false;
			uncompressedDirectBuf.rewind();
			uncompressedDirectBufOff = uncompressedDirectBufLen = 0;
			keepUncompressedBuf = false;
			compressedDirectBuf.limit(directBufferSize);
			compressedDirectBuf.position(directBufferSize);
			userBufOff = userBufLen = 0;
		}

		public virtual void end()
		{
			if (stream != 0)
			{
				end(stream);
				stream = 0;
			}
		}

		private void checkStream()
		{
			if (stream == 0)
			{
				throw new System.ArgumentNullException();
			}
		}

		private static void initIDs()
		{
		}

		private static long init(int level, int strategy, int windowBits)
		{
		}

		private static void setDictionary(long strm, byte[] b, int off, int len)
		{
		}

		private int deflateBytesDirect()
		{
		}

		private static long getBytesRead(long strm)
		{
		}

		private static long getBytesWritten(long strm)
		{
		}

		private static void reset(long strm)
		{
		}

		private static void end(long strm)
		{
		}

		public static string getLibraryName()
		{
		}
	}
}
