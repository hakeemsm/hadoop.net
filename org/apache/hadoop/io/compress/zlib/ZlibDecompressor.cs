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
	/// <see cref="org.apache.hadoop.io.compress.Decompressor"/>
	/// based on the popular
	/// zlib compression algorithm.
	/// http://www.zlib.net/
	/// </summary>
	public class ZlibDecompressor : org.apache.hadoop.io.compress.Decompressor
	{
		private const int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

		private static java.lang.Class clazz = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.ZlibDecompressor
			));

		private long stream;

		private org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader header;

		private int directBufferSize;

		private java.nio.Buffer compressedDirectBuf = null;

		private int compressedDirectBufOff;

		private int compressedDirectBufLen;

		private java.nio.Buffer uncompressedDirectBuf = null;

		private byte[] userBuf = null;

		private int userBufOff = 0;

		private int userBufLen = 0;

		private bool finished;

		private bool needDict;

		/// <summary>The headers to detect from compressed data.</summary>
		[System.Serializable]
		public sealed class CompressionHeader
		{
			/// <summary>No headers/trailers/checksums.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader
				 NO_HEADER = new org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader
				(-15);

			/// <summary>Default headers/trailers/checksums.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader
				 DEFAULT_HEADER = new org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader
				(15);

			/// <summary>Simple gzip headers/trailers.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader
				 GZIP_FORMAT = new org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader
				(31);

			/// <summary>Autodetect gzip/zlib headers/trailers.</summary>
			public static readonly org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader
				 AUTODETECT_GZIP_ZLIB = new org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader
				(47);

			private readonly int windowBits;

			internal CompressionHeader(int windowBits)
			{
				// HACK - Use this as a global lock in the JNI layer
				this.windowBits = windowBits;
			}

			public int windowBits()
			{
				return org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader.windowBits;
			}
		}

		private static bool nativeZlibLoaded = false;

		static ZlibDecompressor()
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

		/// <summary>Creates a new decompressor.</summary>
		public ZlibDecompressor(org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader
			 header, int directBufferSize)
		{
			this.header = header;
			this.directBufferSize = directBufferSize;
			compressedDirectBuf = java.nio.ByteBuffer.allocateDirect(directBufferSize);
			uncompressedDirectBuf = java.nio.ByteBuffer.allocateDirect(directBufferSize);
			uncompressedDirectBuf.position(directBufferSize);
			stream = init(this.header.windowBits());
		}

		public ZlibDecompressor()
			: this(org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader.DEFAULT_HEADER
				, DEFAULT_DIRECT_BUFFER_SIZE)
		{
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
			setInputFromSavedData();
			// Reinitialize zlib's output direct buffer 
			uncompressedDirectBuf.limit(directBufferSize);
			uncompressedDirectBuf.position(directBufferSize);
		}

		internal virtual void setInputFromSavedData()
		{
			compressedDirectBufOff = 0;
			compressedDirectBufLen = userBufLen;
			if (compressedDirectBufLen > directBufferSize)
			{
				compressedDirectBufLen = directBufferSize;
			}
			// Reinitialize zlib's input direct buffer
			compressedDirectBuf.rewind();
			((java.nio.ByteBuffer)compressedDirectBuf).put(userBuf, userBufOff, compressedDirectBufLen
				);
			// Note how much data is being fed to zlib
			userBufOff += compressedDirectBufLen;
			userBufLen -= compressedDirectBufLen;
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
			needDict = false;
		}

		public virtual bool needsInput()
		{
			// Consume remaining compressed data?
			if (uncompressedDirectBuf.remaining() > 0)
			{
				return false;
			}
			// Check if zlib has consumed all input
			if (compressedDirectBufLen <= 0)
			{
				// Check if we have consumed all user-input
				if (userBufLen <= 0)
				{
					return true;
				}
				else
				{
					setInputFromSavedData();
				}
			}
			return false;
		}

		public virtual bool needsDictionary()
		{
			return needDict;
		}

		public virtual bool finished()
		{
			// Check if 'zlib' says it's 'finished' and
			// all compressed data has been consumed
			return (finished && uncompressedDirectBuf.remaining() == 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int decompress(byte[] b, int off, int len)
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
			// Check if there is uncompressed data
			n = uncompressedDirectBuf.remaining();
			if (n > 0)
			{
				n = System.Math.min(n, len);
				((java.nio.ByteBuffer)uncompressedDirectBuf).get(b, off, n);
				return n;
			}
			// Re-initialize the zlib's output direct buffer
			uncompressedDirectBuf.rewind();
			uncompressedDirectBuf.limit(directBufferSize);
			// Decompress data
			n = inflateBytesDirect();
			uncompressedDirectBuf.limit(n);
			// Get at most 'len' bytes
			n = System.Math.min(n, len);
			((java.nio.ByteBuffer)uncompressedDirectBuf).get(b, off, n);
			return n;
		}

		/// <summary>Returns the total number of uncompressed bytes output so far.</summary>
		/// <returns>the total (non-negative) number of uncompressed bytes output so far</returns>
		public virtual long getBytesWritten()
		{
			checkStream();
			return getBytesWritten(stream);
		}

		/// <summary>Returns the total number of compressed bytes input so far.</p></summary>
		/// <returns>the total (non-negative) number of compressed bytes input so far</returns>
		public virtual long getBytesRead()
		{
			checkStream();
			return getBytesRead(stream);
		}

		/// <summary>
		/// Returns the number of bytes remaining in the input buffers; normally
		/// called when finished() is true to determine amount of post-gzip-stream
		/// data.</p>
		/// </summary>
		/// <returns>the total (non-negative) number of unprocessed bytes in input</returns>
		public virtual int getRemaining()
		{
			checkStream();
			return userBufLen + getRemaining(stream);
		}

		// userBuf + compressedDirectBuf
		/// <summary>Resets everything including the input buffers (user and direct).</p></summary>
		public virtual void reset()
		{
			checkStream();
			reset(stream);
			finished = false;
			needDict = false;
			compressedDirectBufOff = compressedDirectBufLen = 0;
			uncompressedDirectBuf.limit(directBufferSize);
			uncompressedDirectBuf.position(directBufferSize);
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

		~ZlibDecompressor()
		{
			end();
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

		private static long init(int windowBits)
		{
		}

		private static void setDictionary(long strm, byte[] b, int off, int len)
		{
		}

		private int inflateBytesDirect()
		{
		}

		private static long getBytesRead(long strm)
		{
		}

		private static long getBytesWritten(long strm)
		{
		}

		private static int getRemaining(long strm)
		{
		}

		private static void reset(long strm)
		{
		}

		private static void end(long strm)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual int inflateDirect(java.nio.ByteBuffer src, java.nio.ByteBuffer dst
			)
		{
			System.Diagnostics.Debug.Assert((this is org.apache.hadoop.io.compress.zlib.ZlibDecompressor.ZlibDirectDecompressor
				));
			java.nio.ByteBuffer presliced = dst;
			if (dst.position() > 0)
			{
				presliced = dst;
				dst = dst.slice();
			}
			java.nio.Buffer originalCompressed = compressedDirectBuf;
			java.nio.Buffer originalUncompressed = uncompressedDirectBuf;
			int originalBufferSize = directBufferSize;
			compressedDirectBuf = src;
			compressedDirectBufOff = src.position();
			compressedDirectBufLen = src.remaining();
			uncompressedDirectBuf = dst;
			directBufferSize = dst.remaining();
			int n = 0;
			try
			{
				n = inflateBytesDirect();
				presliced.position(presliced.position() + n);
				if (compressedDirectBufLen > 0)
				{
					src.position(compressedDirectBufOff);
				}
				else
				{
					src.position(src.limit());
				}
			}
			finally
			{
				compressedDirectBuf = originalCompressed;
				uncompressedDirectBuf = originalUncompressed;
				compressedDirectBufOff = 0;
				compressedDirectBufLen = 0;
				directBufferSize = originalBufferSize;
			}
			return n;
		}

		public class ZlibDirectDecompressor : org.apache.hadoop.io.compress.zlib.ZlibDecompressor
			, org.apache.hadoop.io.compress.DirectDecompressor
		{
			public ZlibDirectDecompressor()
				: base(org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader.DEFAULT_HEADER
					, 0)
			{
			}

			public ZlibDirectDecompressor(org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader
				 header, int directBufferSize)
				: base(header, directBufferSize)
			{
			}

			public override bool finished()
			{
				return (endOfInput && base.finished());
			}

			public override void reset()
			{
				base.reset();
				endOfInput = true;
			}

			private bool endOfInput;

			/// <exception cref="System.IO.IOException"/>
			public virtual void decompress(java.nio.ByteBuffer src, java.nio.ByteBuffer dst)
			{
				System.Diagnostics.Debug.Assert(dst.isDirect(), "dst.isDirect()");
				System.Diagnostics.Debug.Assert(src.isDirect(), "src.isDirect()");
				System.Diagnostics.Debug.Assert(dst.remaining() > 0, "dst.remaining() > 0");
				this.inflateDirect(src, dst);
				endOfInput = !src.hasRemaining();
			}

			public override void setDictionary(byte[] b, int off, int len)
			{
				throw new System.NotSupportedException("byte[] arrays are not supported for DirectDecompressor"
					);
			}

			public override int decompress(byte[] b, int off, int len)
			{
				throw new System.NotSupportedException("byte[] arrays are not supported for DirectDecompressor"
					);
			}
		}
	}
}
