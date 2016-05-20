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
	public class DecompressorStream : org.apache.hadoop.io.compress.CompressionInputStream
	{
		protected internal org.apache.hadoop.io.compress.Decompressor decompressor = null;

		protected internal byte[] buffer;

		protected internal bool eof = false;

		protected internal bool closed = false;

		private int lastBytesSent = 0;

		/// <exception cref="System.IO.IOException"/>
		public DecompressorStream(java.io.InputStream @in, org.apache.hadoop.io.compress.Decompressor
			 decompressor, int bufferSize)
			: base(@in)
		{
			if (decompressor == null)
			{
				throw new System.ArgumentNullException();
			}
			else
			{
				if (bufferSize <= 0)
				{
					throw new System.ArgumentException("Illegal bufferSize");
				}
			}
			this.decompressor = decompressor;
			buffer = new byte[bufferSize];
		}

		/// <exception cref="System.IO.IOException"/>
		public DecompressorStream(java.io.InputStream @in, org.apache.hadoop.io.compress.Decompressor
			 decompressor)
			: this(@in, decompressor, 512)
		{
		}

		/// <summary>Allow derived classes to directly set the underlying stream.</summary>
		/// <param name="in">Underlying input stream.</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal DecompressorStream(java.io.InputStream @in)
			: base(@in)
		{
		}

		private byte[] oneByte = new byte[1];

		/// <exception cref="System.IO.IOException"/>
		public override int read()
		{
			checkStream();
			return (read(oneByte, 0, oneByte.Length) == -1) ? -1 : (oneByte[0] & unchecked((int
				)(0xff)));
		}

		/// <exception cref="System.IO.IOException"/>
		public override int read(byte[] b, int off, int len)
		{
			checkStream();
			if ((off | len | (off + len) | (b.Length - (off + len))) < 0)
			{
				throw new System.IndexOutOfRangeException();
			}
			else
			{
				if (len == 0)
				{
					return 0;
				}
			}
			return decompress(b, off, len);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual int decompress(byte[] b, int off, int len)
		{
			int n = 0;
			while ((n = decompressor.decompress(b, off, len)) == 0)
			{
				if (decompressor.needsDictionary())
				{
					eof = true;
					return -1;
				}
				if (decompressor.finished())
				{
					// First see if there was any leftover buffered input from previous
					// stream; if not, attempt to refill buffer.  If refill -> EOF, we're
					// all done; else reset, fix up input buffer, and get ready for next
					// concatenated substream/"member".
					int nRemaining = decompressor.getRemaining();
					if (nRemaining == 0)
					{
						int m = getCompressedData();
						if (m == -1)
						{
							// apparently the previous end-of-stream was also end-of-file:
							// return success, as if we had never called getCompressedData()
							eof = true;
							return -1;
						}
						decompressor.reset();
						decompressor.setInput(buffer, 0, m);
						lastBytesSent = m;
					}
					else
					{
						// looks like it's a concatenated stream:  reset low-level zlib (or
						// other engine) and buffers, then "resend" remaining input data
						decompressor.reset();
						int leftoverOffset = lastBytesSent - nRemaining;
						System.Diagnostics.Debug.Assert((leftoverOffset >= 0));
						// this recopies userBuf -> direct buffer if using native libraries:
						decompressor.setInput(buffer, leftoverOffset, nRemaining);
					}
				}
				else
				{
					// NOTE:  this is the one place we do NOT want to save the number
					// of bytes sent (nRemaining here) into lastBytesSent:  since we
					// are resending what we've already sent before, offset is nonzero
					// in general (only way it could be zero is if it already equals
					// nRemaining), which would then screw up the offset calculation
					// _next_ time around.  IOW, getRemaining() is in terms of the
					// original, zero-offset bufferload, so lastBytesSent must be as
					// well.  Cheesy ASCII art:
					//
					//          <------------ m, lastBytesSent ----------->
					//          +===============================================+
					// buffer:  |1111111111|22222222222222222|333333333333|     |
					//          +===============================================+
					//     #1:  <-- off -->|<-------- nRemaining --------->
					//     #2:  <----------- off ----------->|<-- nRem. -->
					//     #3:  (final substream:  nRemaining == 0; eof = true)
					//
					// If lastBytesSent is anything other than m, as shown, then "off"
					// will be calculated incorrectly.
					if (decompressor.needsInput())
					{
						int m = getCompressedData();
						if (m == -1)
						{
							throw new java.io.EOFException("Unexpected end of input stream");
						}
						decompressor.setInput(buffer, 0, m);
						lastBytesSent = m;
					}
				}
			}
			return n;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual int getCompressedData()
		{
			checkStream();
			// note that the _caller_ is now required to call setInput() or throw
			return @in.read(buffer, 0, buffer.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void checkStream()
		{
			if (closed)
			{
				throw new System.IO.IOException("Stream closed");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void resetState()
		{
			decompressor.reset();
		}

		private byte[] skipBytes = new byte[512];

		/// <exception cref="System.IO.IOException"/>
		public override long skip(long n)
		{
			// Sanity checks
			if (n < 0)
			{
				throw new System.ArgumentException("negative skip length");
			}
			checkStream();
			// Read 'n' bytes
			int skipped = 0;
			while (skipped < n)
			{
				int len = System.Math.min(((int)n - skipped), skipBytes.Length);
				len = read(skipBytes, 0, len);
				if (len == -1)
				{
					eof = true;
					break;
				}
				skipped += len;
			}
			return skipped;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int available()
		{
			checkStream();
			return (eof) ? 0 : 1;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			if (!closed)
			{
				@in.close();
				closed = true;
			}
		}

		public override bool markSupported()
		{
			return false;
		}

		public override void mark(int readlimit)
		{
			lock (this)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void reset()
		{
			lock (this)
			{
				throw new System.IO.IOException("mark/reset not supported");
			}
		}
	}
}
