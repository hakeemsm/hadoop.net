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

namespace org.apache.hadoop.io.compress.bzip2
{
	/// <summary>
	/// A
	/// <see cref="org.apache.hadoop.io.compress.Decompressor"/>
	/// based on the popular
	/// bzip2 compression algorithm.
	/// http://www.bzip2.org/
	/// </summary>
	public class Bzip2Decompressor : org.apache.hadoop.io.compress.Decompressor
	{
		private const int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.bzip2.Bzip2Decompressor
			)));

		private static java.lang.Class clazz = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.bzip2.Bzip2Decompressor
			));

		private long stream;

		private bool conserveMemory;

		private int directBufferSize;

		private java.nio.Buffer compressedDirectBuf = null;

		private int compressedDirectBufOff;

		private int compressedDirectBufLen;

		private java.nio.Buffer uncompressedDirectBuf = null;

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
			compressedDirectBuf = java.nio.ByteBuffer.allocateDirect(directBufferSize);
			uncompressedDirectBuf = java.nio.ByteBuffer.allocateDirect(directBufferSize);
			uncompressedDirectBuf.position(directBufferSize);
			stream = init(conserveMemory ? 1 : 0);
		}

		public Bzip2Decompressor()
			: this(false, DEFAULT_DIRECT_BUFFER_SIZE)
		{
		}

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
				// Reinitialize bzip2's output direct buffer.
				uncompressedDirectBuf.limit(directBufferSize);
				uncompressedDirectBuf.position(directBufferSize);
			}
		}

		internal virtual void setInputFromSavedData()
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
				compressedDirectBuf.rewind();
				((java.nio.ByteBuffer)compressedDirectBuf).put(userBuf, userBufOff, compressedDirectBufLen
					);
				// Note how much data is being fed to bzip2.
				userBufOff += compressedDirectBufLen;
				userBufLen -= compressedDirectBufLen;
			}
		}

		public virtual void setDictionary(byte[] b, int off, int len)
		{
			lock (this)
			{
				throw new System.NotSupportedException();
			}
		}

		public virtual bool needsInput()
		{
			lock (this)
			{
				// Consume remaining compressed data?
				if (uncompressedDirectBuf.remaining() > 0)
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
						setInputFromSavedData();
					}
				}
				return false;
			}
		}

		public virtual bool needsDictionary()
		{
			lock (this)
			{
				return false;
			}
		}

		public virtual bool finished()
		{
			lock (this)
			{
				// Check if bzip2 says it has finished and
				// all compressed data has been consumed.
				return (finished && uncompressedDirectBuf.remaining() == 0);
			}
		}

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
				// Check if there is uncompressed data.
				int n = uncompressedDirectBuf.remaining();
				if (n > 0)
				{
					n = System.Math.min(n, len);
					((java.nio.ByteBuffer)uncompressedDirectBuf).get(b, off, n);
					return n;
				}
				// Re-initialize bzip2's output direct buffer.
				uncompressedDirectBuf.rewind();
				uncompressedDirectBuf.limit(directBufferSize);
				// Decompress the data.
				n = finished ? 0 : inflateBytesDirect();
				uncompressedDirectBuf.limit(n);
				// Get at most 'len' bytes.
				n = System.Math.min(n, len);
				((java.nio.ByteBuffer)uncompressedDirectBuf).get(b, off, n);
				return n;
			}
		}

		/// <summary>Returns the total number of uncompressed bytes output so far.</summary>
		/// <returns>the total (non-negative) number of uncompressed bytes output so far</returns>
		public virtual long getBytesWritten()
		{
			lock (this)
			{
				checkStream();
				return getBytesWritten(stream);
			}
		}

		/// <summary>Returns the total number of compressed bytes input so far.</p></summary>
		/// <returns>the total (non-negative) number of compressed bytes input so far</returns>
		public virtual long getBytesRead()
		{
			lock (this)
			{
				checkStream();
				return getBytesRead(stream);
			}
		}

		/// <summary>
		/// Returns the number of bytes remaining in the input buffers; normally
		/// called when finished() is true to determine amount of post-gzip-stream
		/// data.</p>
		/// </summary>
		/// <returns>the total (non-negative) number of unprocessed bytes in input</returns>
		public virtual int getRemaining()
		{
			lock (this)
			{
				checkStream();
				return userBufLen + getRemaining(stream);
			}
		}

		// userBuf + compressedDirectBuf
		/// <summary>Resets everything including the input buffers (user and direct).</p></summary>
		public virtual void reset()
		{
			lock (this)
			{
				checkStream();
				end(stream);
				stream = init(conserveMemory ? 1 : 0);
				finished = false;
				compressedDirectBufOff = compressedDirectBufLen = 0;
				uncompressedDirectBuf.limit(directBufferSize);
				uncompressedDirectBuf.position(directBufferSize);
				userBufOff = userBufLen = 0;
			}
		}

		public virtual void end()
		{
			lock (this)
			{
				if (stream != 0)
				{
					end(stream);
					stream = 0;
				}
			}
		}

		internal static void initSymbols(string libname)
		{
			initIDs(libname);
		}

		private void checkStream()
		{
			if (stream == 0)
			{
				throw new System.ArgumentNullException();
			}
		}

		private static void initIDs(string libname)
		{
		}

		private static long init(int conserveMemory)
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

		private static void end(long strm)
		{
		}
	}
}
