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

namespace org.apache.hadoop.io.compress
{
	/// <summary>
	/// A
	/// <see cref="CompressorStream"/>
	/// which works
	/// with 'block-based' based compression algorithms, as opposed to
	/// 'stream-based' compression algorithms.
	/// It should be noted that this wrapper does not guarantee that blocks will
	/// be sized for the compressor. If the
	/// <see cref="Compressor"/>
	/// requires buffering to
	/// effect meaningful compression, it is responsible for it.
	/// </summary>
	public class BlockCompressorStream : org.apache.hadoop.io.compress.CompressorStream
	{
		private readonly int MAX_INPUT_SIZE;

		/// <summary>
		/// Create a
		/// <see cref="BlockCompressorStream"/>
		/// .
		/// </summary>
		/// <param name="out">stream</param>
		/// <param name="compressor">compressor to be used</param>
		/// <param name="bufferSize">size of buffer</param>
		/// <param name="compressionOverhead">
		/// maximum 'overhead' of the compression
		/// algorithm with given bufferSize
		/// </param>
		public BlockCompressorStream(java.io.OutputStream @out, org.apache.hadoop.io.compress.Compressor
			 compressor, int bufferSize, int compressionOverhead)
			: base(@out, compressor, bufferSize)
		{
			// The 'maximum' size of input data to be compressed, to account
			// for the overhead of the compression algorithm.
			MAX_INPUT_SIZE = bufferSize - compressionOverhead;
		}

		/// <summary>
		/// Create a
		/// <see cref="BlockCompressorStream"/>
		/// with given output-stream and
		/// compressor.
		/// Use default of 512 as bufferSize and compressionOverhead of
		/// (1% of bufferSize + 12 bytes) =  18 bytes (zlib algorithm).
		/// </summary>
		/// <param name="out">stream</param>
		/// <param name="compressor">compressor to be used</param>
		public BlockCompressorStream(java.io.OutputStream @out, org.apache.hadoop.io.compress.Compressor
			 compressor)
			: this(@out, compressor, 512, 18)
		{
		}

		/// <summary>
		/// Write the data provided to the compression codec, compressing no more
		/// than the buffer size less the compression overhead as specified during
		/// construction for each block.
		/// </summary>
		/// <remarks>
		/// Write the data provided to the compression codec, compressing no more
		/// than the buffer size less the compression overhead as specified during
		/// construction for each block.
		/// Each block contains the uncompressed length for the block, followed by
		/// one or more length-prefixed blocks of compressed data.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void write(byte[] b, int off, int len)
		{
			// Sanity checks
			if (compressor.finished())
			{
				throw new System.IO.IOException("write beyond end of stream");
			}
			if (b == null)
			{
				throw new System.ArgumentNullException();
			}
			else
			{
				if ((off < 0) || (off > b.Length) || (len < 0) || ((off + len) > b.Length))
				{
					throw new System.IndexOutOfRangeException();
				}
				else
				{
					if (len == 0)
					{
						return;
					}
				}
			}
			long limlen = compressor.getBytesRead();
			if (len + limlen > MAX_INPUT_SIZE && limlen > 0)
			{
				// Adding this segment would exceed the maximum size.
				// Flush data if we have it.
				finish();
				compressor.reset();
			}
			if (len > MAX_INPUT_SIZE)
			{
				// The data we're given exceeds the maximum size. Any data
				// we had have been flushed, so we write out this chunk in segments
				// not exceeding the maximum size until it is exhausted.
				rawWriteInt(len);
				do
				{
					int bufLen = System.Math.min(len, MAX_INPUT_SIZE);
					compressor.setInput(b, off, bufLen);
					compressor.finish();
					while (!compressor.finished())
					{
						compress();
					}
					compressor.reset();
					off += bufLen;
					len -= bufLen;
				}
				while (len > 0);
				return;
			}
			// Give data to the compressor
			compressor.setInput(b, off, len);
			if (!compressor.needsInput())
			{
				// compressor buffer size might be smaller than the maximum
				// size, so we permit it to flush if required.
				rawWriteInt((int)compressor.getBytesRead());
				do
				{
					compress();
				}
				while (!compressor.needsInput());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void finish()
		{
			if (!compressor.finished())
			{
				rawWriteInt((int)compressor.getBytesRead());
				compressor.finish();
				while (!compressor.finished())
				{
					compress();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void compress()
		{
			int len = compressor.compress(buffer, 0, buffer.Length);
			if (len > 0)
			{
				// Write out the compressed chunk
				rawWriteInt(len);
				@out.write(buffer, 0, len);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void rawWriteInt(int v)
		{
			@out.write(((int)(((uint)v) >> 24)) & unchecked((int)(0xFF)));
			@out.write(((int)(((uint)v) >> 16)) & unchecked((int)(0xFF)));
			@out.write(((int)(((uint)v) >> 8)) & unchecked((int)(0xFF)));
			@out.write(((int)(((uint)v) >> 0)) & unchecked((int)(0xFF)));
		}
	}
}
