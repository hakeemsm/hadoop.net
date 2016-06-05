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
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.Compress.Zlib
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.IO.Compress.Decompressor"/>
	/// based on the popular
	/// zlib compression algorithm.
	/// http://www.zlib.net/
	/// </summary>
	public class ZlibDecompressor : Decompressor
	{
		private const int DefaultDirectBufferSize = 64 * 1024;

		private static Type clazz = typeof(Org.Apache.Hadoop.IO.Compress.Zlib.ZlibDecompressor
			);

		private long stream;

		private ZlibDecompressor.CompressionHeader header;

		private int directBufferSize;

		private Buffer compressedDirectBuf = null;

		private int compressedDirectBufOff;

		private int compressedDirectBufLen;

		private Buffer uncompressedDirectBuf = null;

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
			public static readonly ZlibDecompressor.CompressionHeader NoHeader = new ZlibDecompressor.CompressionHeader
				(-15);

			/// <summary>Default headers/trailers/checksums.</summary>
			public static readonly ZlibDecompressor.CompressionHeader DefaultHeader = new ZlibDecompressor.CompressionHeader
				(15);

			/// <summary>Simple gzip headers/trailers.</summary>
			public static readonly ZlibDecompressor.CompressionHeader GzipFormat = new ZlibDecompressor.CompressionHeader
				(31);

			/// <summary>Autodetect gzip/zlib headers/trailers.</summary>
			public static readonly ZlibDecompressor.CompressionHeader AutodetectGzipZlib = new 
				ZlibDecompressor.CompressionHeader(47);

			private readonly int windowBits;

			internal CompressionHeader(int windowBits)
			{
				// HACK - Use this as a global lock in the JNI layer
				this.windowBits = windowBits;
			}

			public int WindowBits()
			{
				return ZlibDecompressor.CompressionHeader.windowBits;
			}
		}

		private static bool nativeZlibLoaded = false;

		static ZlibDecompressor()
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

		/// <summary>Creates a new decompressor.</summary>
		public ZlibDecompressor(ZlibDecompressor.CompressionHeader header, int directBufferSize
			)
		{
			this.header = header;
			this.directBufferSize = directBufferSize;
			compressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			uncompressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			uncompressedDirectBuf.Position(directBufferSize);
			stream = Init(this.header.WindowBits());
		}

		public ZlibDecompressor()
			: this(ZlibDecompressor.CompressionHeader.DefaultHeader, DefaultDirectBufferSize)
		{
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
			SetInputFromSavedData();
			// Reinitialize zlib's output direct buffer 
			uncompressedDirectBuf.Limit(directBufferSize);
			uncompressedDirectBuf.Position(directBufferSize);
		}

		internal virtual void SetInputFromSavedData()
		{
			compressedDirectBufOff = 0;
			compressedDirectBufLen = userBufLen;
			if (compressedDirectBufLen > directBufferSize)
			{
				compressedDirectBufLen = directBufferSize;
			}
			// Reinitialize zlib's input direct buffer
			compressedDirectBuf.Rewind();
			((ByteBuffer)compressedDirectBuf).Put(userBuf, userBufOff, compressedDirectBufLen
				);
			// Note how much data is being fed to zlib
			userBufOff += compressedDirectBufLen;
			userBufLen -= compressedDirectBufLen;
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
			needDict = false;
		}

		public virtual bool NeedsInput()
		{
			// Consume remaining compressed data?
			if (uncompressedDirectBuf.Remaining() > 0)
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
					SetInputFromSavedData();
				}
			}
			return false;
		}

		public virtual bool NeedsDictionary()
		{
			return needDict;
		}

		public virtual bool Finished()
		{
			// Check if 'zlib' says it's 'finished' and
			// all compressed data has been consumed
			return (finished && uncompressedDirectBuf.Remaining() == 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Decompress(byte[] b, int off, int len)
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
			// Check if there is uncompressed data
			n = uncompressedDirectBuf.Remaining();
			if (n > 0)
			{
				n = Math.Min(n, len);
				((ByteBuffer)uncompressedDirectBuf).Get(b, off, n);
				return n;
			}
			// Re-initialize the zlib's output direct buffer
			uncompressedDirectBuf.Rewind();
			uncompressedDirectBuf.Limit(directBufferSize);
			// Decompress data
			n = InflateBytesDirect();
			uncompressedDirectBuf.Limit(n);
			// Get at most 'len' bytes
			n = Math.Min(n, len);
			((ByteBuffer)uncompressedDirectBuf).Get(b, off, n);
			return n;
		}

		/// <summary>Returns the total number of uncompressed bytes output so far.</summary>
		/// <returns>the total (non-negative) number of uncompressed bytes output so far</returns>
		public virtual long GetBytesWritten()
		{
			CheckStream();
			return GetBytesWritten(stream);
		}

		/// <summary>Returns the total number of compressed bytes input so far.</p></summary>
		/// <returns>the total (non-negative) number of compressed bytes input so far</returns>
		public virtual long GetBytesRead()
		{
			CheckStream();
			return GetBytesRead(stream);
		}

		/// <summary>
		/// Returns the number of bytes remaining in the input buffers; normally
		/// called when finished() is true to determine amount of post-gzip-stream
		/// data.</p>
		/// </summary>
		/// <returns>the total (non-negative) number of unprocessed bytes in input</returns>
		public virtual int GetRemaining()
		{
			CheckStream();
			return userBufLen + GetRemaining(stream);
		}

		// userBuf + compressedDirectBuf
		/// <summary>Resets everything including the input buffers (user and direct).</p></summary>
		public virtual void Reset()
		{
			CheckStream();
			Reset(stream);
			finished = false;
			needDict = false;
			compressedDirectBufOff = compressedDirectBufLen = 0;
			uncompressedDirectBuf.Limit(directBufferSize);
			uncompressedDirectBuf.Position(directBufferSize);
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

		~ZlibDecompressor()
		{
			End();
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

		private static long Init(int windowBits)
		{
		}

		private static void SetDictionary(long strm, byte[] b, int off, int len)
		{
		}

		private int InflateBytesDirect()
		{
		}

		private static long GetBytesRead(long strm)
		{
		}

		private static long GetBytesWritten(long strm)
		{
		}

		private static int GetRemaining(long strm)
		{
		}

		private static void Reset(long strm)
		{
		}

		private static void End(long strm)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual int InflateDirect(ByteBuffer src, ByteBuffer dst)
		{
			System.Diagnostics.Debug.Assert((this is ZlibDecompressor.ZlibDirectDecompressor)
				);
			ByteBuffer presliced = dst;
			if (dst.Position() > 0)
			{
				presliced = dst;
				dst = dst.Slice();
			}
			Buffer originalCompressed = compressedDirectBuf;
			Buffer originalUncompressed = uncompressedDirectBuf;
			int originalBufferSize = directBufferSize;
			compressedDirectBuf = src;
			compressedDirectBufOff = src.Position();
			compressedDirectBufLen = src.Remaining();
			uncompressedDirectBuf = dst;
			directBufferSize = dst.Remaining();
			int n = 0;
			try
			{
				n = InflateBytesDirect();
				presliced.Position(presliced.Position() + n);
				if (compressedDirectBufLen > 0)
				{
					src.Position(compressedDirectBufOff);
				}
				else
				{
					src.Position(src.Limit());
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

		public class ZlibDirectDecompressor : ZlibDecompressor, DirectDecompressor
		{
			public ZlibDirectDecompressor()
				: base(ZlibDecompressor.CompressionHeader.DefaultHeader, 0)
			{
			}

			public ZlibDirectDecompressor(ZlibDecompressor.CompressionHeader header, int directBufferSize
				)
				: base(header, directBufferSize)
			{
			}

			public override bool Finished()
			{
				return (endOfInput && base.Finished());
			}

			public override void Reset()
			{
				base.Reset();
				endOfInput = true;
			}

			private bool endOfInput;

			/// <exception cref="System.IO.IOException"/>
			public virtual void Decompress(ByteBuffer src, ByteBuffer dst)
			{
				System.Diagnostics.Debug.Assert(dst.IsDirect(), "dst.isDirect()");
				System.Diagnostics.Debug.Assert(src.IsDirect(), "src.isDirect()");
				System.Diagnostics.Debug.Assert(dst.Remaining() > 0, "dst.remaining() > 0");
				this.InflateDirect(src, dst);
				endOfInput = !src.HasRemaining();
			}

			public override void SetDictionary(byte[] b, int off, int len)
			{
				throw new NotSupportedException("byte[] arrays are not supported for DirectDecompressor"
					);
			}

			public override int Decompress(byte[] b, int off, int len)
			{
				throw new NotSupportedException("byte[] arrays are not supported for DirectDecompressor"
					);
			}
		}
	}
}
