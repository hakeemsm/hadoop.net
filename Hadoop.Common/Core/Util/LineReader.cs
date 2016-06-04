using System;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>A class that provides a line reader from an input stream.</summary>
	/// <remarks>
	/// A class that provides a line reader from an input stream.
	/// Depending on the constructor used, lines will either be terminated by:
	/// <ul>
	/// <li>one of the following: '\n' (LF) , '\r' (CR),
	/// or '\r\n' (CR+LF).</li>
	/// <li><em>or</em>, a custom byte sequence delimiter</li>
	/// </ul>
	/// In both cases, EOF also terminates an otherwise unterminated
	/// line.
	/// </remarks>
	public class LineReader : IDisposable
	{
		private const int DefaultBufferSize = 64 * 1024;

		private int bufferSize = DefaultBufferSize;

		private InputStream @in;

		private byte[] buffer;

		private int bufferLength = 0;

		private int bufferPosn = 0;

		private const byte Cr = (byte)('\r');

		private const byte Lf = (byte)('\n');

		private readonly byte[] recordDelimiterBytes;

		/// <summary>
		/// Create a line reader that reads from the given stream using the
		/// default buffer-size (64k).
		/// </summary>
		/// <param name="in">The input stream</param>
		/// <exception cref="System.IO.IOException"/>
		public LineReader(InputStream @in)
			: this(@in, DefaultBufferSize)
		{
		}

		/// <summary>
		/// Create a line reader that reads from the given stream using the
		/// given buffer-size.
		/// </summary>
		/// <param name="in">The input stream</param>
		/// <param name="bufferSize">Size of the read buffer</param>
		/// <exception cref="System.IO.IOException"/>
		public LineReader(InputStream @in, int bufferSize)
		{
			// the number of bytes of real data in the buffer
			// the current position in the buffer
			// The line delimiter
			this.@in = @in;
			this.bufferSize = bufferSize;
			this.buffer = new byte[this.bufferSize];
			this.recordDelimiterBytes = null;
		}

		/// <summary>
		/// Create a line reader that reads from the given stream using the
		/// <code>io.file.buffer.size</code> specified in the given
		/// <code>Configuration</code>.
		/// </summary>
		/// <param name="in">input stream</param>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public LineReader(InputStream @in, Configuration conf)
			: this(@in, conf.GetInt("io.file.buffer.size", DefaultBufferSize))
		{
		}

		/// <summary>
		/// Create a line reader that reads from the given stream using the
		/// default buffer-size, and using a custom delimiter of array of
		/// bytes.
		/// </summary>
		/// <param name="in">The input stream</param>
		/// <param name="recordDelimiterBytes">The delimiter</param>
		public LineReader(InputStream @in, byte[] recordDelimiterBytes)
		{
			this.@in = @in;
			this.bufferSize = DefaultBufferSize;
			this.buffer = new byte[this.bufferSize];
			this.recordDelimiterBytes = recordDelimiterBytes;
		}

		/// <summary>
		/// Create a line reader that reads from the given stream using the
		/// given buffer-size, and using a custom delimiter of array of
		/// bytes.
		/// </summary>
		/// <param name="in">The input stream</param>
		/// <param name="bufferSize">Size of the read buffer</param>
		/// <param name="recordDelimiterBytes">The delimiter</param>
		/// <exception cref="System.IO.IOException"/>
		public LineReader(InputStream @in, int bufferSize, byte[] recordDelimiterBytes)
		{
			this.@in = @in;
			this.bufferSize = bufferSize;
			this.buffer = new byte[this.bufferSize];
			this.recordDelimiterBytes = recordDelimiterBytes;
		}

		/// <summary>
		/// Create a line reader that reads from the given stream using the
		/// <code>io.file.buffer.size</code> specified in the given
		/// <code>Configuration</code>, and using a custom delimiter of array of
		/// bytes.
		/// </summary>
		/// <param name="in">input stream</param>
		/// <param name="conf">configuration</param>
		/// <param name="recordDelimiterBytes">The delimiter</param>
		/// <exception cref="System.IO.IOException"/>
		public LineReader(InputStream @in, Configuration conf, byte[] recordDelimiterBytes
			)
		{
			this.@in = @in;
			this.bufferSize = conf.GetInt("io.file.buffer.size", DefaultBufferSize);
			this.buffer = new byte[this.bufferSize];
			this.recordDelimiterBytes = recordDelimiterBytes;
		}

		/// <summary>Close the underlying stream.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			@in.Close();
		}

		/// <summary>Read one line from the InputStream into the given Text.</summary>
		/// <param name="str">the object to store the given line (without newline)</param>
		/// <param name="maxLineLength">
		/// the maximum number of bytes to store into str;
		/// the rest of the line is silently discarded.
		/// </param>
		/// <param name="maxBytesToConsume">
		/// the maximum number of bytes to consume
		/// in this call.  This is only a hint, because if the line cross
		/// this threshold, we allow it to happen.  It can overshoot
		/// potentially by as much as one buffer length.
		/// </param>
		/// <returns>
		/// the number of bytes read including the (longest) newline
		/// found.
		/// </returns>
		/// <exception cref="System.IO.IOException">if the underlying stream throws</exception>
		public virtual int ReadLine(Text str, int maxLineLength, int maxBytesToConsume)
		{
			if (this.recordDelimiterBytes != null)
			{
				return ReadCustomLine(str, maxLineLength, maxBytesToConsume);
			}
			else
			{
				return ReadDefaultLine(str, maxLineLength, maxBytesToConsume);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual int FillBuffer(InputStream @in, byte[] buffer, bool inDelimiter
			)
		{
			return @in.Read(buffer);
		}

		/// <summary>Read a line terminated by one of CR, LF, or CRLF.</summary>
		/// <exception cref="System.IO.IOException"/>
		private int ReadDefaultLine(Text str, int maxLineLength, int maxBytesToConsume)
		{
			/* We're reading data from in, but the head of the stream may be
			* already buffered in buffer, so we have several cases:
			* 1. No newline characters are in the buffer, so we need to copy
			*    everything and read another buffer from the stream.
			* 2. An unambiguously terminated line is in buffer, so we just
			*    copy to str.
			* 3. Ambiguously terminated line is in buffer, i.e. buffer ends
			*    in CR.  In this case we copy everything up to CR to str, but
			*    we also need to see what follows CR: if it's LF, then we
			*    need consume LF as well, so next call to readLine will read
			*    from after that.
			* We use a flag prevCharCR to signal if previous character was CR
			* and, if it happens to be at the end of the buffer, delay
			* consuming it until we have a chance to look at the char that
			* follows.
			*/
			str.Clear();
			int txtLength = 0;
			//tracks str.getLength(), as an optimization
			int newlineLength = 0;
			//length of terminating newline
			bool prevCharCR = false;
			//true of prev char was CR
			long bytesConsumed = 0;
			do
			{
				int startPosn = bufferPosn;
				//starting from where we left off the last time
				if (bufferPosn >= bufferLength)
				{
					startPosn = bufferPosn = 0;
					if (prevCharCR)
					{
						++bytesConsumed;
					}
					//account for CR from previous read
					bufferLength = FillBuffer(@in, buffer, prevCharCR);
					if (bufferLength <= 0)
					{
						break;
					}
				}
				// EOF
				for (; bufferPosn < bufferLength; ++bufferPosn)
				{
					//search for newline
					if (buffer[bufferPosn] == Lf)
					{
						newlineLength = (prevCharCR) ? 2 : 1;
						++bufferPosn;
						// at next invocation proceed from following byte
						break;
					}
					if (prevCharCR)
					{
						//CR + notLF, we are at notLF
						newlineLength = 1;
						break;
					}
					prevCharCR = (buffer[bufferPosn] == Cr);
				}
				int readLength = bufferPosn - startPosn;
				if (prevCharCR && newlineLength == 0)
				{
					--readLength;
				}
				//CR at the end of the buffer
				bytesConsumed += readLength;
				int appendLength = readLength - newlineLength;
				if (appendLength > maxLineLength - txtLength)
				{
					appendLength = maxLineLength - txtLength;
				}
				if (appendLength > 0)
				{
					str.Append(buffer, startPosn, appendLength);
					txtLength += appendLength;
				}
			}
			while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);
			if (bytesConsumed > int.MaxValue)
			{
				throw new IOException("Too many bytes before newline: " + bytesConsumed);
			}
			return (int)bytesConsumed;
		}

		/// <summary>Read a line terminated by a custom delimiter.</summary>
		/// <exception cref="System.IO.IOException"/>
		private int ReadCustomLine(Text str, int maxLineLength, int maxBytesToConsume)
		{
			/* We're reading data from inputStream, but the head of the stream may be
			*  already captured in the previous buffer, so we have several cases:
			*
			* 1. The buffer tail does not contain any character sequence which
			*    matches with the head of delimiter. We count it as a
			*    ambiguous byte count = 0
			*
			* 2. The buffer tail contains a X number of characters,
			*    that forms a sequence, which matches with the
			*    head of delimiter. We count ambiguous byte count = X
			*
			*    // ***  eg: A segment of input file is as follows
			*
			*    " record 1792: I found this bug very interesting and
			*     I have completely read about it. record 1793: This bug
			*     can be solved easily record 1794: This ."
			*
			*    delimiter = "record";
			*
			*    supposing:- String at the end of buffer =
			*    "I found this bug very interesting and I have completely re"
			*    There for next buffer = "ad about it. record 179       ...."
			*
			*     The matching characters in the input
			*     buffer tail and delimiter head = "re"
			*     Therefore, ambiguous byte count = 2 ****   //
			*
			*     2.1 If the following bytes are the remaining characters of
			*         the delimiter, then we have to capture only up to the starting
			*         position of delimiter. That means, we need not include the
			*         ambiguous characters in str.
			*
			*     2.2 If the following bytes are not the remaining characters of
			*         the delimiter ( as mentioned in the example ),
			*         then we have to include the ambiguous characters in str.
			*/
			str.Clear();
			int txtLength = 0;
			// tracks str.getLength(), as an optimization
			long bytesConsumed = 0;
			int delPosn = 0;
			int ambiguousByteCount = 0;
			do
			{
				// To capture the ambiguous characters count
				int startPosn = bufferPosn;
				// Start from previous end position
				if (bufferPosn >= bufferLength)
				{
					startPosn = bufferPosn = 0;
					bufferLength = FillBuffer(@in, buffer, ambiguousByteCount > 0);
					if (bufferLength <= 0)
					{
						if (ambiguousByteCount > 0)
						{
							str.Append(recordDelimiterBytes, 0, ambiguousByteCount);
							bytesConsumed += ambiguousByteCount;
						}
						break;
					}
				}
				// EOF
				for (; bufferPosn < bufferLength; ++bufferPosn)
				{
					if (buffer[bufferPosn] == recordDelimiterBytes[delPosn])
					{
						delPosn++;
						if (delPosn >= recordDelimiterBytes.Length)
						{
							bufferPosn++;
							break;
						}
					}
					else
					{
						if (delPosn != 0)
						{
							bufferPosn--;
							delPosn = 0;
						}
					}
				}
				int readLength = bufferPosn - startPosn;
				bytesConsumed += readLength;
				int appendLength = readLength - delPosn;
				if (appendLength > maxLineLength - txtLength)
				{
					appendLength = maxLineLength - txtLength;
				}
				bytesConsumed += ambiguousByteCount;
				if (appendLength >= 0 && ambiguousByteCount > 0)
				{
					//appending the ambiguous characters (refer case 2.2)
					str.Append(recordDelimiterBytes, 0, ambiguousByteCount);
					ambiguousByteCount = 0;
					// since it is now certain that the split did not split a delimiter we
					// should not read the next record: clear the flag otherwise duplicate
					// records could be generated
					UnsetNeedAdditionalRecordAfterSplit();
				}
				if (appendLength > 0)
				{
					str.Append(buffer, startPosn, appendLength);
					txtLength += appendLength;
				}
				if (bufferPosn >= bufferLength)
				{
					if (delPosn > 0 && delPosn < recordDelimiterBytes.Length)
					{
						ambiguousByteCount = delPosn;
						bytesConsumed -= ambiguousByteCount;
					}
				}
			}
			while (delPosn < recordDelimiterBytes.Length && bytesConsumed < maxBytesToConsume);
			//to be consumed in next
			if (bytesConsumed > int.MaxValue)
			{
				throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
			}
			return (int)bytesConsumed;
		}

		/// <summary>Read from the InputStream into the given Text.</summary>
		/// <param name="str">the object to store the given line</param>
		/// <param name="maxLineLength">the maximum number of bytes to store into str.</param>
		/// <returns>the number of bytes read including the newline</returns>
		/// <exception cref="System.IO.IOException">if the underlying stream throws</exception>
		public virtual int ReadLine(Text str, int maxLineLength)
		{
			return ReadLine(str, maxLineLength, int.MaxValue);
		}

		/// <summary>Read from the InputStream into the given Text.</summary>
		/// <param name="str">the object to store the given line</param>
		/// <returns>the number of bytes read including the newline</returns>
		/// <exception cref="System.IO.IOException">if the underlying stream throws</exception>
		public virtual int ReadLine(Text str)
		{
			return ReadLine(str, int.MaxValue, int.MaxValue);
		}

		protected internal virtual int GetBufferPosn()
		{
			return bufferPosn;
		}

		protected internal virtual int GetBufferSize()
		{
			return bufferSize;
		}

		protected internal virtual void UnsetNeedAdditionalRecordAfterSplit()
		{
		}
		// needed for custom multi byte line delimiters only
		// see MAPREDUCE-6549 for details
	}
}
