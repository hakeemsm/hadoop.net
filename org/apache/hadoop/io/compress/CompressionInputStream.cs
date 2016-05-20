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
	/// <summary>A compression input stream.</summary>
	/// <remarks>
	/// A compression input stream.
	/// <p>Implementations are assumed to be buffered.  This permits clients to
	/// reposition the underlying input stream then call
	/// <see cref="resetState()"/>
	/// ,
	/// without having to also synchronize client buffers.
	/// </remarks>
	public abstract class CompressionInputStream : java.io.InputStream, org.apache.hadoop.fs.Seekable
	{
		/// <summary>The input stream to be compressed.</summary>
		protected internal readonly java.io.InputStream @in;

		protected internal long maxAvailableData = 0L;

		private org.apache.hadoop.io.compress.Decompressor trackedDecompressor;

		/// <summary>
		/// Create a compression input stream that reads
		/// the decompressed bytes from the given stream.
		/// </summary>
		/// <param name="in">The input stream to be compressed.</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal CompressionInputStream(java.io.InputStream @in)
		{
			if (!(@in is org.apache.hadoop.fs.Seekable) || !(@in is org.apache.hadoop.fs.PositionedReadable
				))
			{
				this.maxAvailableData = @in.available();
			}
			this.@in = @in;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			@in.close();
			if (trackedDecompressor != null)
			{
				org.apache.hadoop.io.compress.CodecPool.returnDecompressor(trackedDecompressor);
				trackedDecompressor = null;
			}
		}

		/// <summary>Read bytes from the stream.</summary>
		/// <remarks>
		/// Read bytes from the stream.
		/// Made abstract to prevent leakage to underlying stream.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract override int read(byte[] b, int off, int len);

		/// <summary>
		/// Reset the decompressor to its initial state and discard any buffered data,
		/// as the underlying stream may have been repositioned.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void resetState();

		/// <summary>This method returns the current position in the stream.</summary>
		/// <returns>Current position in stream as a long</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual long getPos()
		{
			if (!(@in is org.apache.hadoop.fs.Seekable) || !(@in is org.apache.hadoop.fs.PositionedReadable
				))
			{
				//This way of getting the current position will not work for file
				//size which can be fit in an int and hence can not be returned by
				//available method.
				return (this.maxAvailableData - this.@in.available());
			}
			else
			{
				return ((org.apache.hadoop.fs.Seekable)this.@in).getPos();
			}
		}

		/// <summary>This method is current not supported.</summary>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void seek(long pos)
		{
			throw new System.NotSupportedException();
		}

		/// <summary>This method is current not supported.</summary>
		/// <exception cref="System.NotSupportedException"/>
		public virtual bool seekToNewSource(long targetPos)
		{
			throw new System.NotSupportedException();
		}

		internal virtual void setTrackedDecompressor(org.apache.hadoop.io.compress.Decompressor
			 decompressor)
		{
			trackedDecompressor = decompressor;
		}
	}
}
