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
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress.Bzip2
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.IO.Compress.Decompressor"/>
	/// based on the popular
	/// bzip2 compression algorithm.
	/// http://www.bzip2.org/
	/// </summary>
	public class Bzip2Decompressor : Decompressor
	{
		private const int DefaultDirectBufferSize = 64 * 1024;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.Compress.Bzip2.Bzip2Decompressor
			));

		private static Type clazz = typeof(Org.Apache.Hadoop.IO.Compress.Bzip2.Bzip2Decompressor
			);

		private long stream;

		private bool conserveMemory;

		private int directBufferSize;

		private Buffer compressedDirectBuf = null;

		private int compressedDirectBufOff;

		private int compressedDirectBufLen;

		private Buffer uncompressedDirectBuf = null;

		private byte[] userBuf = null;

		private int userBufOff = 0;

		private int userBufLen = 0;

		private bool finished;

		/// <summary>Creates a new decompressor.</summary>
		public Bzip2Decompressor(bool conserveMemory, int directBufferSize)
		{
			// HACK - Use this as a global lock in the JNI layer.
			this.conserveMemory = conserveMemory;
			this.directBufferSize = directBufferSize;
			compressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			uncompressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			uncompressedDirectBuf.Position(directBufferSize);
			stream = Init(conserveMemory ? 1 : 0);
		}

		public Bzip2Decompressor()
			: this(false, DefaultDirectBufferSize)
		{
		}

		public virtual void SetInput(byte[] b, int off, int len)
		{
			lock (this)
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
				// Reinitialize bzip2's output direct buffer.
				uncompressedDirectBuf.Limit(directBufferSize);
				uncompressedDirectBuf.Position(directBufferSize);
			}
		}

		internal virtual void SetInputFromSavedData()
		{
			lock (this)
			{
				compressedDirectBufOff = 0;
				compressedDirectBufLen = userBufLen;
				if (compressedDirectBufLen > directBufferSize)
				{
					compressedDirectBufLen = directBufferSize;
				}
				// Reinitialize bzip2's input direct buffer.
				compressedDirectBuf.Rewind();
				((ByteBuffer)compressedDirectBuf).Put(userBuf, userBufOff, compressedDirectBufLen
					);
				// Note how much data is being fed to bzip2.
				userBufOff += compressedDirectBufLen;
				userBufLen -= compressedDirectBufLen;
			}
		}

		public virtual void SetDictionary(byte[] b, int off, int len)
		{
			lock (this)
			{
				throw new NotSupportedException();
			}
		}

		public virtual bool NeedsInput()
		{
			lock (this)
			{
				// Consume remaining compressed data?
				if (uncompressedDirectBuf.Remaining() > 0)
				{
					return false;
				}
				// Check if bzip2 has consumed all input.
				if (compressedDirectBufLen <= 0)
				{
					// Check if we have consumed all user-input.
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
		}

		public virtual bool NeedsDictionary()
		{
			lock (this)
			{
				return false;
			}
		}

		public virtual bool Finished()
		{
			lock (this)
			{
				// Check if bzip2 says it has finished and
				// all compressed data has been consumed.
				return (finished && uncompressedDirectBuf.Remaining() == 0);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Decompress(byte[] b, int off, int len)
		{
			lock (this)
			{
				if (b == null)
				{
					throw new ArgumentNullException();
				}
				if (off < 0 || len < 0 || off > b.Length - len)
				{
					throw new IndexOutOfRangeException();
				}
				// Check if there is uncompressed data.
				int n = uncompressedDirectBuf.Remaining();
				if (n > 0)
				{
					n = Math.Min(n, len);
					((ByteBuffer)uncompressedDirectBuf).Get(b, off, n);
					return n;
				}
				// Re-initialize bzip2's output direct buffer.
				uncompressedDirectBuf.Rewind();
				uncompressedDirectBuf.Limit(directBufferSize);
				// Decompress the data.
				n = finished ? 0 : InflateBytesDirect();
				uncompressedDirectBuf.Limit(n);
				// Get at most 'len' bytes.
				n = Math.Min(n, len);
				((ByteBuffer)uncompressedDirectBuf).Get(b, off, n);
				return n;
			}
		}

		/// <summary>Returns the total number of uncompressed bytes output so far.</summary>
		/// <returns>the total (non-negative) number of uncompressed bytes output so far</returns>
		public virtual long GetBytesWritten()
		{
			lock (this)
			{
				CheckStream();
				return GetBytesWritten(stream);
			}
		}

		/// <summary>Returns the total number of compressed bytes input so far.</p></summary>
		/// <returns>the total (non-negative) number of compressed bytes input so far</returns>
		public virtual long GetBytesRead()
		{
			lock (this)
			{
				CheckStream();
				return GetBytesRead(stream);
			}
		}

		/// <summary>
		/// Returns the number of bytes remaining in the input buffers; normally
		/// called when finished() is true to determine amount of post-gzip-stream
		/// data.</p>
		/// </summary>
		/// <returns>the total (non-negative) number of unprocessed bytes in input</returns>
		public virtual int GetRemaining()
		{
			lock (this)
			{
				CheckStream();
				return userBufLen + GetRemaining(stream);
			}
		}

		// userBuf + compressedDirectBuf
		/// <summary>Resets everything including the input buffers (user and direct).</p></summary>
		public virtual void Reset()
		{
			lock (this)
			{
				CheckStream();
				End(stream);
				stream = Init(conserveMemory ? 1 : 0);
				finished = false;
				compressedDirectBufOff = compressedDirectBufLen = 0;
				uncompressedDirectBuf.Limit(directBufferSize);
				uncompressedDirectBuf.Position(directBufferSize);
				userBufOff = userBufLen = 0;
			}
		}

		public virtual void End()
		{
			lock (this)
			{
				if (stream != 0)
				{
					End(stream);
					stream = 0;
				}
			}
		}

		internal static void InitSymbols(string libname)
		{
			InitIDs(libname);
		}

		private void CheckStream()
		{
			if (stream == 0)
			{
				throw new ArgumentNullException();
			}
		}

		private static void InitIDs(string libname)
		{
		}

		private static long Init(int conserveMemory)
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

		private static void End(long strm)
		{
		}
	}
}
