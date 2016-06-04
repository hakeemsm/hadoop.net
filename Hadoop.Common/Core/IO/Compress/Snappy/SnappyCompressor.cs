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

namespace Org.Apache.Hadoop.IO.Compress.Snappy
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.IO.Compress.Compressor"/>
	/// based on the snappy compression algorithm.
	/// http://code.google.com/p/snappy/
	/// </summary>
	public class SnappyCompressor : Compressor
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.Compress.Snappy.SnappyCompressor
			).FullName);

		private const int DefaultDirectBufferSize = 64 * 1024;

		private static Type clazz = typeof(Org.Apache.Hadoop.IO.Compress.Snappy.SnappyCompressor
			);

		private int directBufferSize;

		private Buffer compressedDirectBuf = null;

		private int uncompressedDirectBufLen;

		private Buffer uncompressedDirectBuf = null;

		private byte[] userBuf = null;

		private int userBufOff = 0;

		private int userBufLen = 0;

		private bool finish;

		private bool finished;

		private long bytesRead = 0L;

		private long bytesWritten = 0L;

		private static bool nativeSnappyLoaded = false;

		static SnappyCompressor()
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
					Log.Error("failed to load SnappyCompressor", t);
				}
			}
		}

		public static bool IsNativeCodeLoaded()
		{
			return nativeSnappyLoaded;
		}

		/// <summary>Creates a new compressor.</summary>
		/// <param name="directBufferSize">size of the direct buffer to be used.</param>
		public SnappyCompressor(int directBufferSize)
		{
			this.directBufferSize = directBufferSize;
			uncompressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			compressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			compressedDirectBuf.Position(directBufferSize);
		}

		/// <summary>Creates a new compressor with the default buffer size.</summary>
		public SnappyCompressor()
			: this(DefaultDirectBufferSize)
		{
		}

		/// <summary>Sets input data for compression.</summary>
		/// <remarks>
		/// Sets input data for compression.
		/// This should be called whenever #needsInput() returns
		/// <code>true</code> indicating that more input data is required.
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
			finished = false;
			if (len > uncompressedDirectBuf.Remaining())
			{
				// save data; now !needsInput
				this.userBuf = b;
				this.userBufOff = off;
				this.userBufLen = len;
			}
			else
			{
				((ByteBuffer)uncompressedDirectBuf).Put(b, off, len);
				uncompressedDirectBufLen = uncompressedDirectBuf.Position();
			}
			bytesRead += len;
		}

		/// <summary>
		/// If a write would exceed the capacity of the direct buffers, it is set
		/// aside to be loaded by this function while the compressed data are
		/// consumed.
		/// </summary>
		internal virtual void SetInputFromSavedData()
		{
			if (0 >= userBufLen)
			{
				return;
			}
			finished = false;
			uncompressedDirectBufLen = Math.Min(userBufLen, directBufferSize);
			((ByteBuffer)uncompressedDirectBuf).Put(userBuf, userBufOff, uncompressedDirectBufLen
				);
			// Note how much data is being fed to snappy
			userBufOff += uncompressedDirectBufLen;
			userBufLen -= uncompressedDirectBufLen;
		}

		/// <summary>Does nothing.</summary>
		public virtual void SetDictionary(byte[] b, int off, int len)
		{
		}

		// do nothing
		/// <summary>
		/// Returns true if the input data buffer is empty and
		/// #setInput() should be called to provide more input.
		/// </summary>
		/// <returns>
		/// <code>true</code> if the input data buffer is empty and
		/// #setInput() should be called in order to provide more input.
		/// </returns>
		public virtual bool NeedsInput()
		{
			return !(compressedDirectBuf.Remaining() > 0 || uncompressedDirectBuf.Remaining()
				 == 0 || userBufLen > 0);
		}

		/// <summary>
		/// When called, indicates that compression should end
		/// with the current contents of the input buffer.
		/// </summary>
		public virtual void Finish()
		{
			finish = true;
		}

		/// <summary>
		/// Returns true if the end of the compressed
		/// data output stream has been reached.
		/// </summary>
		/// <returns>
		/// <code>true</code> if the end of the compressed
		/// data output stream has been reached.
		/// </returns>
		public virtual bool Finished()
		{
			// Check if all uncompressed data has been consumed
			return (finish && finished && compressedDirectBuf.Remaining() == 0);
		}

		/// <summary>Fills specified buffer with compressed data.</summary>
		/// <remarks>
		/// Fills specified buffer with compressed data. Returns actual number
		/// of bytes of compressed data. A return value of 0 indicates that
		/// needsInput() should be called in order to determine if more input
		/// data is required.
		/// </remarks>
		/// <param name="b">Buffer for the compressed data</param>
		/// <param name="off">Start offset of the data</param>
		/// <param name="len">Size of the buffer</param>
		/// <returns>The actual number of bytes of compressed data.</returns>
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
			// Check if there is compressed data
			int n = compressedDirectBuf.Remaining();
			if (n > 0)
			{
				n = Math.Min(n, len);
				((ByteBuffer)compressedDirectBuf).Get(b, off, n);
				bytesWritten += n;
				return n;
			}
			// Re-initialize the snappy's output direct-buffer
			compressedDirectBuf.Clear();
			compressedDirectBuf.Limit(0);
			if (0 == uncompressedDirectBuf.Position())
			{
				// No compressed data, so we should have !needsInput or !finished
				SetInputFromSavedData();
				if (0 == uncompressedDirectBuf.Position())
				{
					// Called without data; write nothing
					finished = true;
					return 0;
				}
			}
			// Compress data
			n = CompressBytesDirect();
			compressedDirectBuf.Limit(n);
			uncompressedDirectBuf.Clear();
			// snappy consumes all buffer input
			// Set 'finished' if snapy has consumed all user-data
			if (0 == userBufLen)
			{
				finished = true;
			}
			// Get atmost 'len' bytes
			n = Math.Min(n, len);
			bytesWritten += n;
			((ByteBuffer)compressedDirectBuf).Get(b, off, n);
			return n;
		}

		/// <summary>Resets compressor so that a new set of input data can be processed.</summary>
		public virtual void Reset()
		{
			finish = false;
			finished = false;
			uncompressedDirectBuf.Clear();
			uncompressedDirectBufLen = 0;
			compressedDirectBuf.Clear();
			compressedDirectBuf.Limit(0);
			userBufOff = userBufLen = 0;
			bytesRead = bytesWritten = 0L;
		}

		/// <summary>
		/// Prepare the compressor to be used in a new stream with settings defined in
		/// the given Configuration
		/// </summary>
		/// <param name="conf">Configuration from which new setting are fetched</param>
		public virtual void Reinit(Configuration conf)
		{
			Reset();
		}

		/// <summary>Return number of bytes given to this compressor since last reset.</summary>
		public virtual long GetBytesRead()
		{
			return bytesRead;
		}

		/// <summary>Return number of bytes consumed by callers of compress since last reset.
		/// 	</summary>
		public virtual long GetBytesWritten()
		{
			return bytesWritten;
		}

		/// <summary>Closes the compressor and discards any unprocessed input.</summary>
		public virtual void End()
		{
		}

		private static void InitIDs()
		{
		}

		private int CompressBytesDirect()
		{
		}

		public static string GetLibraryName()
		{
		}
	}
}
