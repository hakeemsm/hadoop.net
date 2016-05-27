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
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	public class CompressorStream : CompressionOutputStream
	{
		protected internal Compressor compressor;

		protected internal byte[] buffer;

		protected internal bool closed = false;

		public CompressorStream(OutputStream @out, Compressor compressor, int bufferSize)
			: base(@out)
		{
			if (@out == null || compressor == null)
			{
				throw new ArgumentNullException();
			}
			else
			{
				if (bufferSize <= 0)
				{
					throw new ArgumentException("Illegal bufferSize");
				}
			}
			this.compressor = compressor;
			buffer = new byte[bufferSize];
		}

		public CompressorStream(OutputStream @out, Compressor compressor)
			: this(@out, compressor, 512)
		{
		}

		/// <summary>Allow derived classes to directly set the underlying stream.</summary>
		/// <param name="out">Underlying output stream.</param>
		protected internal CompressorStream(OutputStream @out)
			: base(@out)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] b, int off, int len)
		{
			// Sanity checks
			if (compressor.Finished())
			{
				throw new IOException("write beyond end of stream");
			}
			if ((off | len | (off + len) | (b.Length - (off + len))) < 0)
			{
				throw new IndexOutOfRangeException();
			}
			else
			{
				if (len == 0)
				{
					return;
				}
			}
			compressor.SetInput(b, off, len);
			while (!compressor.NeedsInput())
			{
				Compress();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void Compress()
		{
			int len = compressor.Compress(buffer, 0, buffer.Length);
			if (len > 0)
			{
				@out.Write(buffer, 0, len);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Finish()
		{
			if (!compressor.Finished())
			{
				compressor.Finish();
				while (!compressor.Finished())
				{
					Compress();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ResetState()
		{
			compressor.Reset();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (!closed)
			{
				try
				{
					Finish();
				}
				finally
				{
					@out.Close();
					closed = true;
				}
			}
		}

		private byte[] oneByte = new byte[1];

		/// <exception cref="System.IO.IOException"/>
		public override void Write(int b)
		{
			oneByte[0] = unchecked((byte)(b & unchecked((int)(0xff))));
			Write(oneByte, 0, oneByte.Length);
		}
	}
}
