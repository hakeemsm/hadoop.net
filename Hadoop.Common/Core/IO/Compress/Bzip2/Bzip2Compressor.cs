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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress.Bzip2
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.IO.Compress.Compressor"/>
	/// based on the popular
	/// bzip2 compression algorithm.
	/// http://www.bzip2.org/
	/// </summary>
	public class Bzip2Compressor : Compressor
	{
		private const int DefaultDirectBufferSize = 64 * 1024;

		internal const int DefaultBlockSize = 9;

		internal const int DefaultWorkFactor = 30;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.Compress.Bzip2.Bzip2Compressor
			));

		private static Type clazz = typeof(Org.Apache.Hadoop.IO.Compress.Bzip2.Bzip2Compressor
			);

		private long stream;

		private int blockSize;

		private int workFactor;

		private int directBufferSize;

		private byte[] userBuf = null;

		private int userBufOff = 0;

		private int userBufLen = 0;

		private Buffer uncompressedDirectBuf = null;

		private int uncompressedDirectBufOff = 0;

		private int uncompressedDirectBufLen = 0;

		private bool keepUncompressedBuf = false;

		private Buffer compressedDirectBuf = null;

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
			: this(DefaultBlockSize, DefaultWorkFactor, DefaultDirectBufferSize)
		{
		}

		/// <summary>Creates a new compressor, taking settings from the configuration.</summary>
		public Bzip2Compressor(Configuration conf)
			: this(Bzip2Factory.GetBlockSize(conf), Bzip2Factory.GetWorkFactor(conf), DefaultDirectBufferSize
				)
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
			stream = Init(blockSize, workFactor);
			uncompressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			compressedDirectBuf = ByteBuffer.AllocateDirect(directBufferSize);
			compressedDirectBuf.Position(directBufferSize);
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
		public virtual void Reinit(Configuration conf)
		{
			lock (this)
			{
				Reset();
				End(stream);
				if (conf == null)
				{
					stream = Init(blockSize, workFactor);
					return;
				}
				blockSize = Bzip2Factory.GetBlockSize(conf);
				workFactor = Bzip2Factory.GetWorkFactor(conf);
				stream = Init(blockSize, workFactor);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Reinit compressor with new compression configuration");
				}
			}
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
				uncompressedDirectBufOff = 0;
				SetInputFromSavedData();
				// Reinitialize bzip2's output direct buffer.
				compressedDirectBuf.Limit(directBufferSize);
				compressedDirectBuf.Position(directBufferSize);
			}
		}

		// Copy enough data from userBuf to uncompressedDirectBuf.
		internal virtual void SetInputFromSavedData()
		{
			lock (this)
			{
				int len = Math.Min(userBufLen, uncompressedDirectBuf.Remaining());
				((ByteBuffer)uncompressedDirectBuf).Put(userBuf, userBufOff, len);
				userBufLen -= len;
				userBufOff += len;
				uncompressedDirectBufLen = uncompressedDirectBuf.Position();
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
				// Compressed data still available?
				if (compressedDirectBuf.Remaining() > 0)
				{
					return false;
				}
				// Uncompressed data available in either the direct buffer or user buffer?
				if (keepUncompressedBuf && uncompressedDirectBufLen > 0)
				{
					return false;
				}
				if (uncompressedDirectBuf.Remaining() > 0)
				{
					// Check if we have consumed all data in the user buffer.
					if (userBufLen <= 0)
					{
						return true;
					}
					else
					{
						// Copy enough data from userBuf to uncompressedDirectBuf.
						SetInputFromSavedData();
						return uncompressedDirectBuf.Remaining() > 0;
					}
				}
				return false;
			}
		}

		public virtual void Finish()
		{
			lock (this)
			{
				finish = true;
			}
		}

		public virtual bool Finished()
		{
			lock (this)
			{
				// Check if bzip2 says it has finished and
				// all compressed data has been consumed.
				return (finished && compressedDirectBuf.Remaining() == 0);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Compress(byte[] b, int off, int len)
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
				// Check if there is compressed data.
				int n = compressedDirectBuf.Remaining();
				if (n > 0)
				{
					n = Math.Min(n, len);
					((ByteBuffer)compressedDirectBuf).Get(b, off, n);
					return n;
				}
				// Re-initialize bzip2's output direct buffer.
				compressedDirectBuf.Rewind();
				compressedDirectBuf.Limit(directBufferSize);
				// Compress the data.
				n = DeflateBytesDirect();
				compressedDirectBuf.Limit(n);
				// Check if bzip2 has consumed the entire input buffer.
				// Set keepUncompressedBuf properly.
				if (uncompressedDirectBufLen <= 0)
				{
					// bzip2 consumed all input
					keepUncompressedBuf = false;
					uncompressedDirectBuf.Clear();
					uncompressedDirectBufOff = 0;
					uncompressedDirectBufLen = 0;
				}
				else
				{
					keepUncompressedBuf = true;
				}
				// Get at most 'len' bytes.
				n = Math.Min(n, len);
				((ByteBuffer)compressedDirectBuf).Get(b, off, n);
				return n;
			}
		}

		/// <summary>Returns the total number of compressed bytes output so far.</summary>
		/// <returns>the total (non-negative) number of compressed bytes output so far</returns>
		public virtual long GetBytesWritten()
		{
			lock (this)
			{
				CheckStream();
				return GetBytesWritten(stream);
			}
		}

		/// <summary>Returns the total number of uncompressed bytes input so far.</p></summary>
		/// <returns>the total (non-negative) number of uncompressed bytes input so far</returns>
		public virtual long GetBytesRead()
		{
			lock (this)
			{
				CheckStream();
				return GetBytesRead(stream);
			}
		}

		public virtual void Reset()
		{
			lock (this)
			{
				CheckStream();
				End(stream);
				stream = Init(blockSize, workFactor);
				finish = false;
				finished = false;
				uncompressedDirectBuf.Rewind();
				uncompressedDirectBufOff = uncompressedDirectBufLen = 0;
				keepUncompressedBuf = false;
				compressedDirectBuf.Limit(directBufferSize);
				compressedDirectBuf.Position(directBufferSize);
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

		private static long Init(int blockSize, int workFactor)
		{
		}

		private int DeflateBytesDirect()
		{
		}

		private static long GetBytesRead(long strm)
		{
		}

		private static long GetBytesWritten(long strm)
		{
		}

		private static void End(long strm)
		{
		}

		public static string GetLibraryName()
		{
		}
	}
}
