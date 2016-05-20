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

namespace org.apache.hadoop.io.compress.snappy
{
	/// <summary>
	/// A
	/// <see cref="org.apache.hadoop.io.compress.Decompressor"/>
	/// based on the snappy compression algorithm.
	/// http://code.google.com/p/snappy/
	/// </summary>
	public class SnappyDecompressor : org.apache.hadoop.io.compress.Decompressor
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.snappy.SnappyCompressor
			)).getName());

		private const int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

		private static java.lang.Class clazz = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.snappy.SnappyDecompressor
			));

		private int directBufferSize;

		private java.nio.Buffer compressedDirectBuf = null;

		private int compressedDirectBufLen;

		private java.nio.Buffer uncompressedDirectBuf = null;

		private byte[] userBuf = null;

		private int userBufOff = 0;

		private int userBufLen = 0;

		private bool finished;

		private static bool nativeSnappyLoaded = false;

		static SnappyDecompressor()
		{
			// HACK - Use this as a global lock in the JNI layer
			if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded() && org.apache.hadoop.util.NativeCodeLoader
				.buildSupportsSnappy())
			{
				try
				{
					initIDs();
					nativeSnappyLoaded = true;
				}
				catch (System.Exception t)
				{
					LOG.error("failed to load SnappyDecompressor", t);
				}
			}
		}

		public static bool isNativeCodeLoaded()
		{
			return nativeSnappyLoaded;
		}

		/// <summary>Creates a new compressor.</summary>
		/// <param name="directBufferSize">size of the direct buffer to be used.</param>
		public SnappyDecompressor(int directBufferSize)
		{
			this.directBufferSize = directBufferSize;
			compressedDirectBuf = java.nio.ByteBuffer.allocateDirect(directBufferSize);
			uncompressedDirectBuf = java.nio.ByteBuffer.allocateDirect(directBufferSize);
			uncompressedDirectBuf.position(directBufferSize);
		}

		/// <summary>Creates a new decompressor with the default buffer size.</summary>
		public SnappyDecompressor()
			: this(DEFAULT_DIRECT_BUFFER_SIZE)
		{
		}

		/// <summary>Sets input data for decompression.</summary>
		/// <remarks>
		/// Sets input data for decompression.
		/// This should be called if and only if
		/// <see cref="needsInput()"/>
		/// returns
		/// <code>true</code> indicating that more input data is required.
		/// (Both native and non-native versions of various Decompressors require
		/// that the data passed in via <code>b[]</code> remain unmodified until
		/// the caller is explicitly notified--via
		/// <see cref="needsInput()"/>
		/// --that the
		/// buffer may be safely modified.  With this requirement, an extra
		/// buffer-copy can be avoided.)
		/// </remarks>
		/// <param name="b">Input data</param>
		/// <param name="off">Start offset</param>
		/// <param name="len">Length</param>
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
			// Reinitialize snappy's output direct-buffer
			uncompressedDirectBuf.limit(directBufferSize);
			uncompressedDirectBuf.position(directBufferSize);
		}

		/// <summary>
		/// If a write would exceed the capacity of the direct buffers, it is set
		/// aside to be loaded by this function while the compressed data are
		/// consumed.
		/// </summary>
		internal virtual void setInputFromSavedData()
		{
			compressedDirectBufLen = System.Math.min(userBufLen, directBufferSize);
			// Reinitialize snappy's input direct buffer
			compressedDirectBuf.rewind();
			((java.nio.ByteBuffer)compressedDirectBuf).put(userBuf, userBufOff, compressedDirectBufLen
				);
			// Note how much data is being fed to snappy
			userBufOff += compressedDirectBufLen;
			userBufLen -= compressedDirectBufLen;
		}

		/// <summary>Does nothing.</summary>
		public virtual void setDictionary(byte[] b, int off, int len)
		{
		}

		// do nothing
		/// <summary>
		/// Returns true if the input data buffer is empty and
		/// <see cref="setInput(byte[], int, int)"/>
		/// should be called to
		/// provide more input.
		/// </summary>
		/// <returns>
		/// <code>true</code> if the input data buffer is empty and
		/// <see cref="setInput(byte[], int, int)"/>
		/// should be called in
		/// order to provide more input.
		/// </returns>
		public virtual bool needsInput()
		{
			// Consume remaining compressed data?
			if (uncompressedDirectBuf.remaining() > 0)
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
					setInputFromSavedData();
				}
			}
			return false;
		}

		/// <summary>Returns <code>false</code>.</summary>
		/// <returns><code>false</code>.</returns>
		public virtual bool needsDictionary()
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
		public virtual bool finished()
		{
			return (finished && uncompressedDirectBuf.remaining() == 0);
		}

		/// <summary>Fills specified buffer with uncompressed data.</summary>
		/// <remarks>
		/// Fills specified buffer with uncompressed data. Returns actual number
		/// of bytes of uncompressed data. A return value of 0 indicates that
		/// <see cref="needsInput()"/>
		/// should be called in order to determine if more
		/// input data is required.
		/// </remarks>
		/// <param name="b">Buffer for the compressed data</param>
		/// <param name="off">Start offset of the data</param>
		/// <param name="len">Size of the buffer</param>
		/// <returns>The actual number of bytes of compressed data.</returns>
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
			if (compressedDirectBufLen > 0)
			{
				// Re-initialize the snappy's output direct buffer
				uncompressedDirectBuf.rewind();
				uncompressedDirectBuf.limit(directBufferSize);
				// Decompress data
				n = decompressBytesDirect();
				uncompressedDirectBuf.limit(n);
				if (userBufLen <= 0)
				{
					finished = true;
				}
				// Get atmost 'len' bytes
				n = System.Math.min(n, len);
				((java.nio.ByteBuffer)uncompressedDirectBuf).get(b, off, n);
			}
			return n;
		}

		/// <summary>Returns <code>0</code>.</summary>
		/// <returns><code>0</code>.</returns>
		public virtual int getRemaining()
		{
			// Never use this function in BlockDecompressorStream.
			return 0;
		}

		public virtual void reset()
		{
			finished = false;
			compressedDirectBufLen = 0;
			uncompressedDirectBuf.limit(directBufferSize);
			uncompressedDirectBuf.position(directBufferSize);
			userBufOff = userBufLen = 0;
		}

		/// <summary>
		/// Resets decompressor and input and output buffers so that a new set of
		/// input data can be processed.
		/// </summary>
		public virtual void end()
		{
		}

		// do nothing
		private static void initIDs()
		{
		}

		private int decompressBytesDirect()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual int decompressDirect(java.nio.ByteBuffer src, java.nio.ByteBuffer
			 dst)
		{
			System.Diagnostics.Debug.Assert((this is org.apache.hadoop.io.compress.snappy.SnappyDecompressor.SnappyDirectDecompressor
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
			compressedDirectBuf = src.slice();
			compressedDirectBufLen = src.remaining();
			uncompressedDirectBuf = dst;
			directBufferSize = dst.remaining();
			int n = 0;
			try
			{
				n = decompressBytesDirect();
				presliced.position(presliced.position() + n);
				// SNAPPY always consumes the whole buffer or throws an exception
				src.position(src.limit());
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

		public class SnappyDirectDecompressor : org.apache.hadoop.io.compress.snappy.SnappyDecompressor
			, org.apache.hadoop.io.compress.DirectDecompressor
		{
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
				this.decompressDirect(src, dst);
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
