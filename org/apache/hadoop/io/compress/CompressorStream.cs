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
	public class CompressorStream : org.apache.hadoop.io.compress.CompressionOutputStream
	{
		protected internal org.apache.hadoop.io.compress.Compressor compressor;

		protected internal byte[] buffer;

		protected internal bool closed = false;

		public CompressorStream(java.io.OutputStream @out, org.apache.hadoop.io.compress.Compressor
			 compressor, int bufferSize)
			: base(@out)
		{
			if (@out == null || compressor == null)
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
			this.compressor = compressor;
			buffer = new byte[bufferSize];
		}

		public CompressorStream(java.io.OutputStream @out, org.apache.hadoop.io.compress.Compressor
			 compressor)
			: this(@out, compressor, 512)
		{
		}

		/// <summary>Allow derived classes to directly set the underlying stream.</summary>
		/// <param name="out">Underlying output stream.</param>
		protected internal CompressorStream(java.io.OutputStream @out)
			: base(@out)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(byte[] b, int off, int len)
		{
			// Sanity checks
			if (compressor.finished())
			{
				throw new System.IO.IOException("write beyond end of stream");
			}
			if ((off | len | (off + len) | (b.Length - (off + len))) < 0)
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
			compressor.setInput(b, off, len);
			while (!compressor.needsInput())
			{
				compress();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void compress()
		{
			int len = compressor.compress(buffer, 0, buffer.Length);
			if (len > 0)
			{
				@out.write(buffer, 0, len);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void finish()
		{
			if (!compressor.finished())
			{
				compressor.finish();
				while (!compressor.finished())
				{
					compress();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void resetState()
		{
			compressor.reset();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			if (!closed)
			{
				try
				{
					finish();
				}
				finally
				{
					@out.close();
					closed = true;
				}
			}
		}

		private byte[] oneByte = new byte[1];

		/// <exception cref="System.IO.IOException"/>
		public override void write(int b)
		{
			oneByte[0] = unchecked((byte)(b & unchecked((int)(0xff))));
			write(oneByte, 0, oneByte.Length);
		}
	}
}
