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

using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	/// <summary>
	/// Specification of a stream-based 'compressor' which can be
	/// plugged into a
	/// <see cref="CompressionOutputStream"/>
	/// to compress data.
	/// This is modelled after
	/// <see cref="ICSharpCode.SharpZipLib.Zip.Compression.Deflater"/>
	/// </summary>
	public interface Compressor
	{
		/// <summary>Sets input data for compression.</summary>
		/// <remarks>
		/// Sets input data for compression.
		/// This should be called whenever #needsInput() returns
		/// <code>true</code> indicating that more input data is required.
		/// </remarks>
		/// <param name="b">Input data</param>
		/// <param name="off">Start offset</param>
		/// <param name="len">Length</param>
		void SetInput(byte[] b, int off, int len);

		/// <summary>
		/// Returns true if the input data buffer is empty and
		/// #setInput() should be called to provide more input.
		/// </summary>
		/// <returns>
		/// <code>true</code> if the input data buffer is empty and
		/// #setInput() should be called in order to provide more input.
		/// </returns>
		bool NeedsInput();

		/// <summary>Sets preset dictionary for compression.</summary>
		/// <remarks>
		/// Sets preset dictionary for compression. A preset dictionary
		/// is used when the history buffer can be predetermined.
		/// </remarks>
		/// <param name="b">Dictionary data bytes</param>
		/// <param name="off">Start offset</param>
		/// <param name="len">Length</param>
		void SetDictionary(byte[] b, int off, int len);

		/// <summary>Return number of uncompressed bytes input so far.</summary>
		long GetBytesRead();

		/// <summary>Return number of compressed bytes output so far.</summary>
		long GetBytesWritten();

		/// <summary>
		/// When called, indicates that compression should end
		/// with the current contents of the input buffer.
		/// </summary>
		void Finish();

		/// <summary>
		/// Returns true if the end of the compressed
		/// data output stream has been reached.
		/// </summary>
		/// <returns>
		/// <code>true</code> if the end of the compressed
		/// data output stream has been reached.
		/// </returns>
		bool Finished();

		/// <summary>Fills specified buffer with compressed data.</summary>
		/// <remarks>
		/// Fills specified buffer with compressed data. Returns actual number
		/// of bytes of compressed data. A return value of 0 indicates that
		/// needsInput() should be called in order to determine if more input
		/// data is required.
		/// </remarks>
		/// <param name="b">Buffer for the compressed data</param>
		/// <param name="off">Start offset of the data</param>
		/// <param name="len">Size of the buffer</param>
		/// <returns>The actual number of bytes of compressed data.</returns>
		/// <exception cref="System.IO.IOException"/>
		int Compress(byte[] b, int off, int len);

		/// <summary>Resets compressor so that a new set of input data can be processed.</summary>
		void Reset();

		/// <summary>Closes the compressor and discards any unprocessed input.</summary>
		void End();

		/// <summary>
		/// Prepare the compressor to be used in a new stream with settings defined in
		/// the given Configuration
		/// </summary>
		/// <param name="conf">Configuration from which new setting are fetched</param>
		void Reinit(Configuration conf);
	}
}
