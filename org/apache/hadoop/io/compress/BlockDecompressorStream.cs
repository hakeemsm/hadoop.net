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
	/// <see cref="DecompressorStream"/>
	/// which works
	/// with 'block-based' based compression algorithms, as opposed to
	/// 'stream-based' compression algorithms.
	/// </summary>
	public class BlockDecompressorStream : org.apache.hadoop.io.compress.DecompressorStream
	{
		private int originalBlockSize = 0;

		private int noUncompressedBytes = 0;

		/// <summary>
		/// Create a
		/// <see cref="BlockDecompressorStream"/>
		/// .
		/// </summary>
		/// <param name="in">input stream</param>
		/// <param name="decompressor">decompressor to use</param>
		/// <param name="bufferSize">size of buffer</param>
		/// <exception cref="System.IO.IOException"/>
		public BlockDecompressorStream(java.io.InputStream @in, org.apache.hadoop.io.compress.Decompressor
			 decompressor, int bufferSize)
			: base(@in, decompressor, bufferSize)
		{
		}

		/// <summary>
		/// Create a
		/// <see cref="BlockDecompressorStream"/>
		/// .
		/// </summary>
		/// <param name="in">input stream</param>
		/// <param name="decompressor">decompressor to use</param>
		/// <exception cref="System.IO.IOException"/>
		public BlockDecompressorStream(java.io.InputStream @in, org.apache.hadoop.io.compress.Decompressor
			 decompressor)
			: base(@in, decompressor)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal BlockDecompressorStream(java.io.InputStream @in)
			: base(@in)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override int decompress(byte[] b, int off, int len)
		{
			// Check if we are the beginning of a block
			if (noUncompressedBytes == originalBlockSize)
			{
				// Get original data size
				try
				{
					originalBlockSize = rawReadInt();
				}
				catch (System.IO.IOException)
				{
					return -1;
				}
				noUncompressedBytes = 0;
				// EOF if originalBlockSize is 0
				// This will occur only when decompressing previous compressed empty file
				if (originalBlockSize == 0)
				{
					eof = true;
					return -1;
				}
			}
			int n = 0;
			while ((n = decompressor.decompress(b, off, len)) == 0)
			{
				if (decompressor.finished() || decompressor.needsDictionary())
				{
					if (noUncompressedBytes >= originalBlockSize)
					{
						eof = true;
						return -1;
					}
				}
				if (decompressor.needsInput())
				{
					int m;
					try
					{
						m = getCompressedData();
					}
					catch (java.io.EOFException)
					{
						eof = true;
						return -1;
					}
					// Send the read data to the decompressor
					decompressor.setInput(buffer, 0, m);
				}
			}
			// Note the no. of decompressed bytes read from 'current' block
			noUncompressedBytes += n;
			return n;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override int getCompressedData()
		{
			checkStream();
			// Get the size of the compressed chunk (always non-negative)
			int len = rawReadInt();
			// Read len bytes from underlying stream 
			if (len > buffer.Length)
			{
				buffer = new byte[len];
			}
			int n = 0;
			int off = 0;
			while (n < len)
			{
				int count = @in.read(buffer, off + n, len - n);
				if (count < 0)
				{
					throw new java.io.EOFException("Unexpected end of block in input stream");
				}
				n += count;
			}
			return len;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void resetState()
		{
			originalBlockSize = 0;
			noUncompressedBytes = 0;
			base.resetState();
		}

		/// <exception cref="System.IO.IOException"/>
		private int rawReadInt()
		{
			int b1 = @in.read();
			int b2 = @in.read();
			int b3 = @in.read();
			int b4 = @in.read();
			if ((b1 | b2 | b3 | b4) < 0)
			{
				throw new java.io.EOFException();
			}
			return ((b1 << 24) + (b2 << 16) + (b3 << 8) + (b4 << 0));
		}
	}
}
