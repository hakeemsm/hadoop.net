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
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	/// <summary>A compression input stream.</summary>
	/// <remarks>
	/// A compression input stream.
	/// <p>Implementations are assumed to be buffered.  This permits clients to
	/// reposition the underlying input stream then call
	/// <see cref="ResetState()"/>
	/// ,
	/// without having to also synchronize client buffers.
	/// </remarks>
	public abstract class CompressionInputStream : InputStream, Seekable
	{
		/// <summary>The input stream to be compressed.</summary>
		protected internal readonly InputStream @in;

		protected internal long maxAvailableData = 0L;

		private Decompressor trackedDecompressor;

		/// <summary>
		/// Create a compression input stream that reads
		/// the decompressed bytes from the given stream.
		/// </summary>
		/// <param name="in">The input stream to be compressed.</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal CompressionInputStream(InputStream @in)
		{
			if (!(@in is Seekable) || !(@in is PositionedReadable))
			{
				this.maxAvailableData = @in.Available();
			}
			this.@in = @in;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			@in.Close();
			if (trackedDecompressor != null)
			{
				CodecPool.ReturnDecompressor(trackedDecompressor);
				trackedDecompressor = null;
			}
		}

		/// <summary>Read bytes from the stream.</summary>
		/// <remarks>
		/// Read bytes from the stream.
		/// Made abstract to prevent leakage to underlying stream.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract override int Read(byte[] b, int off, int len);

		/// <summary>
		/// Reset the decompressor to its initial state and discard any buffered data,
		/// as the underlying stream may have been repositioned.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void ResetState();

		/// <summary>This method returns the current position in the stream.</summary>
		/// <returns>Current position in stream as a long</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			if (!(@in is Seekable) || !(@in is PositionedReadable))
			{
				//This way of getting the current position will not work for file
				//size which can be fit in an int and hence can not be returned by
				//available method.
				return (this.maxAvailableData - this.@in.Available());
			}
			else
			{
				return ((Seekable)this.@in).GetPos();
			}
		}

		/// <summary>This method is current not supported.</summary>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void Seek(long pos)
		{
			throw new NotSupportedException();
		}

		/// <summary>This method is current not supported.</summary>
		/// <exception cref="System.NotSupportedException"/>
		public virtual bool SeekToNewSource(long targetPos)
		{
			throw new NotSupportedException();
		}

		internal virtual void SetTrackedDecompressor(Decompressor decompressor)
		{
			trackedDecompressor = decompressor;
		}
	}
}
