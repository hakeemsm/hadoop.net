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
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.Compress.Snappy
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.IO.Compress.Decompressor"/>
	/// based on the snappy compression algorithm.
	/// http://code.google.com/p/snappy/
	/// </summary>
	public class SnappyDecompressor : Decompressor
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(SnappyCompressor).FullName
			);

		private const int DefaultDirectBufferSize = 64 * 1024;

		private static Type clazz = typeof(Org.Apache.Hadoop.IO.Compress.Snappy.SnappyDecompressor
			);

		private int directBufferSize;

		private Buffer compressedDirectBuf = null;

		private int compressedDirectBufLen;

		private Buffer uncompressedDirectBuf = null;

		private byte[] userBuf = null;

		private int userBufOff = 0;

		private int userBufLen = 0;

		private bool finished;

		private static bool nativeSnappyLoaded = false;

		static SnappyDecompressor()
		{
			// HACK - Use this as a global lock in the JNI layer
			if (NativeCodeLoader.IsNativeCodeLoaded() && NativeCodeLoader.BuildSupportsSnappy
				())
			{
				try
				{
					InitIDs();
					nativeSnappyLoaded = true;
				}
				catch (Exception t)
				{
					Log.Error("failed to load SnappyDecompressor", t);
				}
			}
		}

		public static bool IsNativeCodeLoaded()
		{
			return nativeSnappyLoaded;
		}

		/// <summary>Creates a new compressor.</summary>
		/// <param name="directBufferSize">size of the direct buffer to be used.</param>
		public SnappyDecompressor(int directBufferSize)
		{
			this.directBufferSize = directBufferSize;
			compressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			uncompressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			uncompressedDirectBuf.Position(directBufferSize);
		}

		/// <summary>Creates a new decompressor with the default buffer size.</summary>
		public SnappyDecompressor()
			: this(DefaultDirectBufferSize)
		{
		}

		/// <summary>Sets input data for decompression.</summary>
		/// <remarks>
		/// Sets input data for decompression.
		/// This should be called if and only if
		/// <see cref="NeedsInput()"/>
		/// returns
		/// <code>true</code> indicating that more input data is required.
		/// (Both native and non-native versions of various Decompressors require
		/// that the data passed in via <code>b[]</code> remain unmodified until
		/// the caller is explicitly notified--via
		/// <see cref="NeedsInput()"/>
		/// --that the
		/// buffer may be safely modified.  With this requirement, an extra
		/// buffer-copy can be avoided.)
		/// </remarks>
		/// <param name="b">Input data</param>
		/// <param name="off">Start offset</param>
		/// <param name="len">Length</param>
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
			// Reinitialize snappy's output direct-buffer
			uncompressedDirectBuf.Limit(directBufferSize);
			uncompressedDirectBuf.Position(directBufferSize);
		}

		/// <summary>
		/// If a write would exceed the capacity of the direct buffers, it is set
		/// aside to be loaded by this function while the compressed data are
		/// consumed.
		/// </summary>
		internal virtual void SetInputFromSavedData()
		{
			compressedDirectBufLen = Math.Min(userBufLen, directBufferSize);
			// Reinitialize snappy's input direct buffer
			compressedDirectBuf.Rewind();
			((ByteBuffer)compressedDirectBuf).Put(userBuf, userBufOff, compressedDirectBufLen
				);
			// Note how much data is being fed to snappy
			userBufOff += compressedDirectBufLen;
			userBufLen -= compressedDirectBufLen;
		}

		/// <summary>Does nothing.</summary>
		public virtual void SetDictionary(byte[] b, int off, int len)
		{
		}

		// do nothing
		/// <summary>
		/// Returns true if the input data buffer is empty and
		/// <see cref="SetInput(byte[], int, int)"/>
		/// should be called to
		/// provide more input.
		/// </summary>
		/// <returns>
		/// <code>true</code> if the input data buffer is empty and
		/// <see cref="SetInput(byte[], int, int)"/>
		/// should be called in
		/// order to provide more input.
		/// </returns>
		public virtual bool NeedsInput()
		{
			// Consume remaining compressed data?
			if (uncompressedDirectBuf.Remaining() > 0)
			{
				return false;
			}
			// Check if snappy has consumed all input
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

		/// <summary>Returns <code>false</code>.</summary>
		/// <returns><code>false</code>.</returns>
		public virtual bool NeedsDictionary()
		{
			return false;
		}

		/// <summary>
		/// Returns true if the end of the decompressed
		/// data output stream has been reached.
		/// </summary>
		/// <returns>
		/// <code>true</code> if the end of the decompressed
		/// data output stream has been reached.
		/// </returns>
		public virtual bool Finished()
		{
			return (finished && uncompressedDirectBuf.Remaining() == 0);
		}

		/// <summary>Fills specified buffer with uncompressed data.</summary>
		/// <remarks>
		/// Fills specified buffer with uncompressed data. Returns actual number
		/// of bytes of uncompressed data. A return value of 0 indicates that
		/// <see cref="NeedsInput()"/>
		/// should be called in order to determine if more
		/// input data is required.
		/// </remarks>
		/// <param name="b">Buffer for the compressed data</param>
		/// <param name="off">Start offset of the data</param>
		/// <param name="len">Size of the buffer</param>
		/// <returns>The actual number of bytes of compressed data.</returns>
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
			if (compressedDirectBufLen > 0)
			{
				// Re-initialize the snappy's output direct buffer
				uncompressedDirectBuf.Rewind();
				uncompressedDirectBuf.Limit(directBufferSize);
				// Decompress data
				n = DecompressBytesDirect();
				uncompressedDirectBuf.Limit(n);
				if (userBufLen <= 0)
				{
					finished = true;
				}
				// Get atmost 'len' bytes
				n = Math.Min(n, len);
				((ByteBuffer)uncompressedDirectBuf).Get(b, off, n);
			}
			return n;
		}

		/// <summary>Returns <code>0</code>.</summary>
		/// <returns><code>0</code>.</returns>
		public virtual int GetRemaining()
		{
			// Never use this function in BlockDecompressorStream.
			return 0;
		}

		public virtual void Reset()
		{
			finished = false;
			compressedDirectBufLen = 0;
			uncompressedDirectBuf.Limit(directBufferSize);
			uncompressedDirectBuf.Position(directBufferSize);
			userBufOff = userBufLen = 0;
		}

		/// <summary>
		/// Resets decompressor and input and output buffers so that a new set of
		/// input data can be processed.
		/// </summary>
		public virtual void End()
		{
		}

		// do nothing
		private static void InitIDs()
		{
		}

		private int DecompressBytesDirect()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual int DecompressDirect(ByteBuffer src, ByteBuffer dst)
		{
			System.Diagnostics.Debug.Assert((this is SnappyDecompressor.SnappyDirectDecompressor
				));
			ByteBuffer presliced = dst;
			if (dst.Position() > 0)
			{
				presliced = dst;
				dst = dst.Slice();
			}
			Buffer originalCompressed = compressedDirectBuf;
			Buffer originalUncompressed = uncompressedDirectBuf;
			int originalBufferSize = directBufferSize;
			compressedDirectBuf = src.Slice();
			compressedDirectBufLen = src.Remaining();
			uncompressedDirectBuf = dst;
			directBufferSize = dst.Remaining();
			int n = 0;
			try
			{
				n = DecompressBytesDirect();
				presliced.Position(presliced.Position() + n);
				// SNAPPY always consumes the whole buffer or throws an exception
				src.Position(src.Limit());
				finished = true;
			}
			finally
			{
				compressedDirectBuf = originalCompressed;
				uncompressedDirectBuf = originalUncompressed;
				compressedDirectBufLen = 0;
				directBufferSize = originalBufferSize;
			}
			return n;
		}

		public class SnappyDirectDecompressor : SnappyDecompressor, DirectDecompressor
		{
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
				this.DecompressDirect(src, dst);
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
