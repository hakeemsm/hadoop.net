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
	/// Specification of a stream-based 'de-compressor' which can be
	/// plugged into a
	/// <see cref="CompressionInputStream"/>
	/// to compress data.
	/// This is modelled after
	/// <see cref="java.util.zip.Inflater"/>
	/// </summary>
	public interface Decompressor
	{
		/// <summary>Sets input data for decompression.</summary>
		/// <remarks>
		/// Sets input data for decompression.
		/// This should be called if and only if
		/// <see cref="needsInput()"/>
		/// returns
		/// <code>true</code> indicating that more input data is required.
		/// (Both native and non-native versions of various Decompressors require
		/// that the data passed in via <code>b[]</code> remain unmodified until
		/// the caller is explicitly notified--via
		/// <see cref="needsInput()"/>
		/// --that the
		/// buffer may be safely modified.  With this requirement, an extra
		/// buffer-copy can be avoided.)
		/// </remarks>
		/// <param name="b">Input data</param>
		/// <param name="off">Start offset</param>
		/// <param name="len">Length</param>
		void setInput(byte[] b, int off, int len);

		/// <summary>
		/// Returns <code>true</code> if the input data buffer is empty and
		/// <see cref="setInput(byte[], int, int)"/>
		/// should be called to
		/// provide more input.
		/// </summary>
		/// <returns>
		/// <code>true</code> if the input data buffer is empty and
		/// <see cref="setInput(byte[], int, int)"/>
		/// should be called in
		/// order to provide more input.
		/// </returns>
		bool needsInput();

		/// <summary>Sets preset dictionary for compression.</summary>
		/// <remarks>
		/// Sets preset dictionary for compression. A preset dictionary
		/// is used when the history buffer can be predetermined.
		/// </remarks>
		/// <param name="b">Dictionary data bytes</param>
		/// <param name="off">Start offset</param>
		/// <param name="len">Length</param>
		void setDictionary(byte[] b, int off, int len);

		/// <summary>Returns <code>true</code> if a preset dictionary is needed for decompression.
		/// 	</summary>
		/// <returns><code>true</code> if a preset dictionary is needed for decompression</returns>
		bool needsDictionary();

		/// <summary>
		/// Returns <code>true</code> if the end of the decompressed
		/// data output stream has been reached.
		/// </summary>
		/// <remarks>
		/// Returns <code>true</code> if the end of the decompressed
		/// data output stream has been reached. Indicates a concatenated data stream
		/// when finished() returns <code>true</code> and
		/// <see cref="getRemaining()"/>
		/// returns a positive value. finished() will be reset with the
		/// <see cref="reset()"/>
		/// method.
		/// </remarks>
		/// <returns>
		/// <code>true</code> if the end of the decompressed
		/// data output stream has been reached.
		/// </returns>
		bool finished();

		/// <summary>Fills specified buffer with uncompressed data.</summary>
		/// <remarks>
		/// Fills specified buffer with uncompressed data. Returns actual number
		/// of bytes of uncompressed data. A return value of 0 indicates that
		/// <see cref="needsInput()"/>
		/// should be called in order to determine if more
		/// input data is required.
		/// </remarks>
		/// <param name="b">Buffer for the compressed data</param>
		/// <param name="off">Start offset of the data</param>
		/// <param name="len">Size of the buffer</param>
		/// <returns>The actual number of bytes of compressed data.</returns>
		/// <exception cref="System.IO.IOException"/>
		int decompress(byte[] b, int off, int len);

		/// <summary>Returns the number of bytes remaining in the compressed data buffer.</summary>
		/// <remarks>
		/// Returns the number of bytes remaining in the compressed data buffer.
		/// Indicates a concatenated data stream if
		/// <see cref="finished()"/>
		/// returns
		/// <code>true</code> and getRemaining() returns a positive value. If
		/// <see cref="finished()"/>
		/// returns <code>true</code> and getRemaining() returns
		/// a zero value, indicates that the end of data stream has been reached and
		/// is not a concatenated data stream.
		/// </remarks>
		/// <returns>The number of bytes remaining in the compressed data buffer.</returns>
		int getRemaining();

		/// <summary>
		/// Resets decompressor and input and output buffers so that a new set of
		/// input data can be processed.
		/// </summary>
		/// <remarks>
		/// Resets decompressor and input and output buffers so that a new set of
		/// input data can be processed. If
		/// <see cref="finished()"/>
		/// } returns
		/// <code>true</code> and
		/// <see cref="getRemaining()"/>
		/// returns a positive value,
		/// reset() is called before processing of the next data stream in the
		/// concatenated data stream.
		/// <see cref="finished()"/>
		/// will be reset and will
		/// return <code>false</code> when reset() is called.
		/// </remarks>
		void reset();

		/// <summary>Closes the decompressor and discards any unprocessed input.</summary>
		void end();
	}
}
