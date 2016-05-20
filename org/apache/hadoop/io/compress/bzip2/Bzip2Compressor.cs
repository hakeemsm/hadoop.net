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
	/// <see cref="org.apache.hadoop.io.compress.Compressor"/>
	/// based on the popular
	/// bzip2 compression algorithm.
	/// http://www.bzip2.org/
	/// </summary>
	public class Bzip2Compressor : org.apache.hadoop.io.compress.Compressor
	{
		private const int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

		internal const int DEFAULT_BLOCK_SIZE = 9;

		internal const int DEFAULT_WORK_FACTOR = 30;

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.bzip2.Bzip2Compressor
			)));

		private static java.lang.Class clazz = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.bzip2.Bzip2Compressor
			));

		private long stream;

		private int blockSize;

		private int workFactor;

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

		/// <summary>
		/// Creates a new compressor with a default values for the
		/// compression block size and work factor.
		/// </summary>
		/// <remarks>
		/// Creates a new compressor with a default values for the
		/// compression block size and work factor.  Compressed data will be
		/// generated in bzip2 format.
		/// </remarks>
		public Bzip2Compressor()
			: this(DEFAULT_BLOCK_SIZE, DEFAULT_WORK_FACTOR, DEFAULT_DIRECT_BUFFER_SIZE)
		{
		}

		/// <summary>Creates a new compressor, taking settings from the configuration.</summary>
		public Bzip2Compressor(org.apache.hadoop.conf.Configuration conf)
			: this(org.apache.hadoop.io.compress.bzip2.Bzip2Factory.getBlockSize(conf), org.apache.hadoop.io.compress.bzip2.Bzip2Factory
				.getWorkFactor(conf), DEFAULT_DIRECT_BUFFER_SIZE)
		{
		}

		/// <summary>Creates a new compressor using the specified block size.</summary>
		/// <remarks>
		/// Creates a new compressor using the specified block size.
		/// Compressed data will be generated in bzip2 format.
		/// </remarks>
		/// <param name="blockSize">
		/// The block size to be used for compression.  This is
		/// an integer from 1 through 9, which is multiplied by 100,000 to
		/// obtain the actual block size in bytes.
		/// </param>
		/// <param name="workFactor">
		/// This parameter is a threshold that determines when a
		/// fallback algorithm is used for pathological data.  It ranges from
		/// 0 to 250.
		/// </param>
		/// <param name="directBufferSize">Size of the direct buffer to be used.</param>
		public Bzip2Compressor(int blockSize, int workFactor, int directBufferSize)
		{
			// The default values for the block size and work factor are the same 
			// those in Julian Seward's original bzip2 implementation.
			// HACK - Use this as a global lock in the JNI layer.
			this.blockSize = blockSize;
			this.workFactor = workFactor;
			this.directBufferSize = directBufferSize;
			stream = init(blockSize, workFactor);
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
		/// the given Configuration. It will reset the compressor's block size and
		/// and work factor.
		/// </remarks>
		/// <param name="conf">Configuration storing new settings</param>
		public virtual void reinit(org.apache.hadoop.conf.Configuration conf)
		{
			lock (this)
			{
				reset();
				end(stream);
				if (conf == null)
				{
					stream = init(blockSize, workFactor);
					return;
				}
				blockSize = org.apache.hadoop.io.compress.bzip2.Bzip2Factory.getBlockSize(conf);
				workFactor = org.apache.hadoop.io.compress.bzip2.Bzip2Factory.getWorkFactor(conf);
				stream = init(blockSize, workFactor);
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Reinit compressor with new compression configuration");
				}
			}
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
				uncompressedDirectBufOff = 0;
				setInputFromSavedData();
				// Reinitialize bzip2's output direct buffer.
				compressedDirectBuf.limit(directBufferSize);
				compressedDirectBuf.position(directBufferSize);
			}
		}

		// Copy enough data from userBuf to uncompressedDirectBuf.
		internal virtual void setInputFromSavedData()
		{
			lock (this)
			{
				int len = System.Math.min(userBufLen, uncompressedDirectBuf.remaining());
				((java.nio.ByteBuffer)uncompressedDirectBuf).put(userBuf, userBufOff, len);
				userBufLen -= len;
				userBufOff += len;
				uncompressedDirectBufLen = uncompressedDirectBuf.position();
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
				// Compressed data still available?
				if (compressedDirectBuf.remaining() > 0)
				{
					return false;
				}
				// Uncompressed data available in either the direct buffer or user buffer?
				if (keepUncompressedBuf && uncompressedDirectBufLen > 0)
				{
					return false;
				}
				if (uncompressedDirectBuf.remaining() > 0)
				{
					// Check if we have consumed all data in the user buffer.
					if (userBufLen <= 0)
					{
						return true;
					}
					else
					{
						// Copy enough data from userBuf to uncompressedDirectBuf.
						setInputFromSavedData();
						return uncompressedDirectBuf.remaining() > 0;
					}
				}
				return false;
			}
		}

		public virtual void finish()
		{
			lock (this)
			{
				finish = true;
			}
		}

		public virtual bool finished()
		{
			lock (this)
			{
				// Check if bzip2 says it has finished and
				// all compressed data has been consumed.
				return (finished && compressedDirectBuf.remaining() == 0);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int compress(byte[] b, int off, int len)
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
				// Check if there is compressed data.
				int n = compressedDirectBuf.remaining();
				if (n > 0)
				{
					n = System.Math.min(n, len);
					((java.nio.ByteBuffer)compressedDirectBuf).get(b, off, n);
					return n;
				}
				// Re-initialize bzip2's output direct buffer.
				compressedDirectBuf.rewind();
				compressedDirectBuf.limit(directBufferSize);
				// Compress the data.
				n = deflateBytesDirect();
				compressedDirectBuf.limit(n);
				// Check if bzip2 has consumed the entire input buffer.
				// Set keepUncompressedBuf properly.
				if (uncompressedDirectBufLen <= 0)
				{
					// bzip2 consumed all input
					keepUncompressedBuf = false;
					uncompressedDirectBuf.clear();
					uncompressedDirectBufOff = 0;
					uncompressedDirectBufLen = 0;
				}
				else
				{
					keepUncompressedBuf = true;
				}
				// Get at most 'len' bytes.
				n = System.Math.min(n, len);
				((java.nio.ByteBuffer)compressedDirectBuf).get(b, off, n);
				return n;
			}
		}

		/// <summary>Returns the total number of compressed bytes output so far.</summary>
		/// <returns>the total (non-negative) number of compressed bytes output so far</returns>
		public virtual long getBytesWritten()
		{
			lock (this)
			{
				checkStream();
				return getBytesWritten(stream);
			}
		}

		/// <summary>Returns the total number of uncompressed bytes input so far.</p></summary>
		/// <returns>the total (non-negative) number of uncompressed bytes input so far</returns>
		public virtual long getBytesRead()
		{
			lock (this)
			{
				checkStream();
				return getBytesRead(stream);
			}
		}

		public virtual void reset()
		{
			lock (this)
			{
				checkStream();
				end(stream);
				stream = init(blockSize, workFactor);
				finish = false;
				finished = false;
				uncompressedDirectBuf.rewind();
				uncompressedDirectBufOff = uncompressedDirectBufLen = 0;
				keepUncompressedBuf = false;
				compressedDirectBuf.limit(directBufferSize);
				compressedDirectBuf.position(directBufferSize);
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

		private static long init(int blockSize, int workFactor)
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

		private static void end(long strm)
		{
		}

		public static string getLibraryName()
		{
		}
	}
}
