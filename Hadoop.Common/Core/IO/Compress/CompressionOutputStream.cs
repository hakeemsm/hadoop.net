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
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	/// <summary>A compression output stream.</summary>
	public abstract class CompressionOutputStream : OutputStream
	{
		/// <summary>The output stream to be compressed.</summary>
		protected internal readonly OutputStream @out;

		/// <summary>
		/// If non-null, this is the Compressor object that we should call
		/// CodecPool#returnCompressor on when this stream is closed.
		/// </summary>
		private Compressor trackedCompressor;

		/// <summary>
		/// Create a compression output stream that writes
		/// the compressed bytes to the given stream.
		/// </summary>
		/// <param name="out"/>
		protected internal CompressionOutputStream(OutputStream @out)
		{
			this.@out = @out;
		}

		internal virtual void SetTrackedCompressor(Compressor compressor)
		{
			trackedCompressor = compressor;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			Finish();
			@out.Close();
			if (trackedCompressor != null)
			{
				CodecPool.ReturnCompressor(trackedCompressor);
				trackedCompressor = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
			@out.Flush();
		}

		/// <summary>Write compressed bytes to the stream.</summary>
		/// <remarks>
		/// Write compressed bytes to the stream.
		/// Made abstract to prevent leakage to underlying stream.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract override void Write(byte[] b, int off, int len);

		/// <summary>
		/// Finishes writing compressed data to the output stream
		/// without closing the underlying stream.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Finish();

		/// <summary>Reset the compression to the initial state.</summary>
		/// <remarks>
		/// Reset the compression to the initial state.
		/// Does not reset the underlying stream.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void ResetState();
	}
}
