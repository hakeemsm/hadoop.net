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
	/// <summary>A compression output stream.</summary>
	public abstract class CompressionOutputStream : java.io.OutputStream
	{
		/// <summary>The output stream to be compressed.</summary>
		protected internal readonly java.io.OutputStream @out;

		/// <summary>
		/// If non-null, this is the Compressor object that we should call
		/// CodecPool#returnCompressor on when this stream is closed.
		/// </summary>
		private org.apache.hadoop.io.compress.Compressor trackedCompressor;

		/// <summary>
		/// Create a compression output stream that writes
		/// the compressed bytes to the given stream.
		/// </summary>
		/// <param name="out"/>
		protected internal CompressionOutputStream(java.io.OutputStream @out)
		{
			this.@out = @out;
		}

		internal virtual void setTrackedCompressor(org.apache.hadoop.io.compress.Compressor
			 compressor)
		{
			trackedCompressor = compressor;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			finish();
			@out.close();
			if (trackedCompressor != null)
			{
				org.apache.hadoop.io.compress.CodecPool.returnCompressor(trackedCompressor);
				trackedCompressor = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void flush()
		{
			@out.flush();
		}

		/// <summary>Write compressed bytes to the stream.</summary>
		/// <remarks>
		/// Write compressed bytes to the stream.
		/// Made abstract to prevent leakage to underlying stream.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract override void write(byte[] b, int off, int len);

		/// <summary>
		/// Finishes writing compressed data to the output stream
		/// without closing the underlying stream.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void finish();

		/// <summary>Reset the compression to the initial state.</summary>
		/// <remarks>
		/// Reset the compression to the initial state.
		/// Does not reset the underlying stream.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void resetState();
	}
}
