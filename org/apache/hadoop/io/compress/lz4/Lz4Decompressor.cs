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

namespace org.apache.hadoop.io.compress.lz4
{
	/// <summary>
	/// A
	/// <see cref="org.apache.hadoop.io.compress.Decompressor"/>
	/// based on the lz4 compression algorithm.
	/// http://code.google.com/p/lz4/
	/// </summary>
	public class Lz4Decompressor : org.apache.hadoop.io.compress.Decompressor
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.lz4.Lz4Compressor
			)).getName());

		private const int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

		private static java.lang.Class clazz = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.lz4.Lz4Decompressor
			));

		private int directBufferSize;

		private java.nio.Buffer compressedDirectBuf = null;

		private int compressedDirectBufLen;

		private java.nio.Buffer uncompressedDirectBuf = null;

		private byte[] userBuf = null;

		private int userBufOff = 0;

		private int userBufLen = 0;

		private bool finished;

		static Lz4Decompressor()
		{
			// HACK - Use this as a global lock in the JNI layer
			if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
			{
				// Initialize the native library
				try
				{
					initIDs();
				}
				catch (System.Exception t)
				{
					// Ignore failure to load/initialize lz4
					LOG.warn(t.ToString());
				}
			}
			else
			{
				LOG.error("Cannot load " + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.lz4.Lz4Compressor
					)).getName() + " without native hadoop library!");
			}
		}

		/// <summary>Creates a new compressor.</summary>
		/// <param name="directBufferSize">size of the direct buffer to be used.</param>
		public Lz4Decompressor(int directBufferSize)
		{
			this.directBufferSize = directBufferSize;
			compressedDirectBuf = java.nio.ByteBuffer.allocateDirect(directBufferSize);
			uncompressedDirectBuf = java.nio.ByteBuffer.allocateDirect(directBufferSize);
			uncompressedDirectBuf.position(directBufferSize);
		}

		/// <summary>Creates a new decompressor with the default buffer size.</summary>
		public Lz4Decompressor()
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
			lock (this)
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
				// Reinitialize lz4's output direct-buffer
				uncompressedDirectBuf.limit(directBufferSize);
				uncompressedDirectBuf.position(directBufferSize);
			}
		}

		/// <summary>
		/// If a write would exceed the capacity of the direct buffers, it is set
		/// aside to be loaded by this function while the compressed data are
		/// consumed.
		/// </summary>
		internal virtual void setInputFromSavedData()
		{
			lock (this)
			{
				compressedDirectBufLen = System.Math.min(userBufLen, directBufferSize);
				// Reinitialize lz4's input direct buffer
				compressedDirectBuf.rewind();
				((java.nio.ByteBuffer)compressedDirectBuf).put(userBuf, userBufOff, compressedDirectBufLen
					);
				// Note how much data is being fed to lz4
				userBufOff += compressedDirectBufLen;
				userBufLen -= compressedDirectBufLen;
			}
		}

		/// <summary>Does nothing.</summary>
		public virtual void setDictionary(byte[] b, int off, int len)
		{
			lock (this)
			{
			}
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
			lock (this)
			{
				// Consume remaining compressed data?
				if (uncompressedDirectBuf.remaining() > 0)
				{
					return false;
				}
				// Check if lz4 has consumed all input
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
		}

		/// <summary>Returns <code>false</code>.</summary>
		/// <returns><code>false</code>.</returns>
		public virtual bool needsDictionary()
		{
			lock (this)
			{
				return false;
			}
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
			lock (this)
			{
				return (finished && uncompressedDirectBuf.remaining() == 0);
			}
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
			lock (this)
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
					// Re-initialize the lz4's output direct buffer
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
		}

		/// <summary>Returns <code>0</code>.</summary>
		/// <returns><code>0</code>.</returns>
		public virtual int getRemaining()
		{
			lock (this)
			{
				// Never use this function in BlockDecompressorStream.
				return 0;
			}
		}

		public virtual void reset()
		{
			lock (this)
			{
				finished = false;
				compressedDirectBufLen = 0;
				uncompressedDirectBuf.limit(directBufferSize);
				uncompressedDirectBuf.position(directBufferSize);
				userBufOff = userBufLen = 0;
			}
		}

		/// <summary>
		/// Resets decompressor and input and output buffers so that a new set of
		/// input data can be processed.
		/// </summary>
		public virtual void end()
		{
			lock (this)
			{
			}
		}

		// do nothing
		private static void initIDs()
		{
		}

		private int decompressBytesDirect()
		{
		}
	}
}
