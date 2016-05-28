using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Class which handles generating data (creating and appending) along with
	/// ensuring the correct data is written out for the given path name so that it
	/// can be later verified
	/// </summary>
	internal class DataWriter
	{
		/// <summary>Header size in bytes</summary>
		private const int HeaderLength = (Constants.BytesPerLong * 2);

		private int bufferSize;

		private Random rnd;

		/// <summary>
		/// Class used to hold the number of bytes written and time taken for write
		/// operations for callers to use
		/// </summary>
		internal class GenerateOutput
		{
			private long bytes;

			private long time;

			internal GenerateOutput(long bytesWritten, long timeTaken)
			{
				this.bytes = bytesWritten;
				this.time = timeTaken;
			}

			internal virtual long GetBytesWritten()
			{
				return bytes;
			}

			internal virtual long GetTimeTaken()
			{
				return time;
			}

			public override string ToString()
			{
				return "Wrote " + GetBytesWritten() + " bytes " + " which took " + GetTimeTaken()
					 + " milliseconds";
			}
		}

		/// <summary>Class used to hold a byte buffer and offset position for generating data
		/// 	</summary>
		private class GenerateResult
		{
			private long offset;

			private ByteBuffer buffer;

			internal GenerateResult(long offset, ByteBuffer buffer)
			{
				this.offset = offset;
				this.buffer = buffer;
			}

			internal virtual long GetOffset()
			{
				return offset;
			}

			internal virtual ByteBuffer GetBuffer()
			{
				return buffer;
			}
		}

		/// <summary>
		/// What a header write output returns need the hash value to use and the time
		/// taken to perform the write + bytes written
		/// </summary>
		private class WriteInfo
		{
			private long hashValue;

			private long bytesWritten;

			private long timeTaken;

			internal WriteInfo(long hashValue, long bytesWritten, long timeTaken)
			{
				this.hashValue = hashValue;
				this.bytesWritten = bytesWritten;
				this.timeTaken = timeTaken;
			}

			internal virtual long GetHashValue()
			{
				return hashValue;
			}

			internal virtual long GetTimeTaken()
			{
				return timeTaken;
			}

			internal virtual long GetBytesWritten()
			{
				return bytesWritten;
			}
		}

		/// <summary>
		/// Inits with given buffer size (must be greater than bytes per long and a
		/// multiple of bytes per long)
		/// </summary>
		/// <param name="rnd">random number generator to use for hash value creation</param>
		/// <param name="bufferSize">
		/// size which must be greater than BYTES_PER_LONG and which also must
		/// be a multiple of BYTES_PER_LONG
		/// </param>
		internal DataWriter(Random rnd, int bufferSize)
		{
			if (bufferSize < Constants.BytesPerLong)
			{
				throw new ArgumentException("Buffer size must be greater than or equal to " + Constants
					.BytesPerLong);
			}
			if ((bufferSize % Constants.BytesPerLong) != 0)
			{
				throw new ArgumentException("Buffer size must be a multiple of " + Constants.BytesPerLong
					);
			}
			this.bufferSize = bufferSize;
			this.rnd = rnd;
		}

		/// <summary>Inits with default buffer size</summary>
		internal DataWriter(Random rnd)
			: this(rnd, Constants.Buffersize)
		{
		}

		/// <summary>Generates a partial segment which is less than bytes per long size</summary>
		/// <param name="byteAm">the number of bytes to generate (less than bytes per long)</param>
		/// <param name="offset">the staring offset</param>
		/// <param name="hasher">hasher to use for generating data given an offset</param>
		/// <returns>GenerateResult containing new offset and byte buffer</returns>
		private DataWriter.GenerateResult GeneratePartialSegment(int byteAm, long offset, 
			DataHasher hasher)
		{
			if (byteAm > Constants.BytesPerLong)
			{
				throw new ArgumentException("Partial bytes must be less or equal to " + Constants
					.BytesPerLong);
			}
			if (byteAm <= 0)
			{
				throw new ArgumentException("Partial bytes must be greater than zero and not " + 
					byteAm);
			}
			ByteBuffer buf = ByteBuffer.Wrap(new byte[Constants.BytesPerLong]);
			buf.PutLong(hasher.Generate(offset));
			ByteBuffer allBytes = ByteBuffer.Wrap(new byte[byteAm]);
			buf.Rewind();
			for (int i = 0; i < byteAm; ++i)
			{
				allBytes.Put(buf.Get());
			}
			allBytes.Rewind();
			return new DataWriter.GenerateResult(offset, allBytes);
		}

		/// <summary>
		/// Generates a full segment (aligned to bytes per long) of the given byte
		/// amount size
		/// </summary>
		/// <param name="byteAm">long aligned size</param>
		/// <param name="startOffset">starting hash offset</param>
		/// <param name="hasher">hasher to use for generating data given an offset</param>
		/// <returns>GenerateResult containing new offset and byte buffer</returns>
		private DataWriter.GenerateResult GenerateFullSegment(int byteAm, long startOffset
			, DataHasher hasher)
		{
			if (byteAm <= 0)
			{
				throw new ArgumentException("Byte amount must be greater than zero and not " + byteAm
					);
			}
			if ((byteAm % Constants.BytesPerLong) != 0)
			{
				throw new ArgumentException("Byte amount " + byteAm + " must be a multiple of " +
					 Constants.BytesPerLong);
			}
			// generate all the segments
			ByteBuffer allBytes = ByteBuffer.Wrap(new byte[byteAm]);
			long offset = startOffset;
			ByteBuffer buf = ByteBuffer.Wrap(new byte[Constants.BytesPerLong]);
			for (long i = 0; i < byteAm; i += Constants.BytesPerLong)
			{
				buf.Rewind();
				buf.PutLong(hasher.Generate(offset));
				buf.Rewind();
				allBytes.Put(buf);
				offset += Constants.BytesPerLong;
			}
			allBytes.Rewind();
			return new DataWriter.GenerateResult(offset, allBytes);
		}

		/// <summary>
		/// Writes a set of bytes to the output stream, for full segments it will write
		/// out the complete segment but for partial segments, ie when the last
		/// position does not fill up a full long then a partial set will be written
		/// out containing the needed bytes from the expected full segment
		/// </summary>
		/// <param name="byteAm">the amount of bytes to write</param>
		/// <param name="startPos">a BYTES_PER_LONG aligned start position</param>
		/// <param name="hasher">hasher to use for generating data given an offset</param>
		/// <param name="out">the output stream to write to</param>
		/// <returns>how many bytes were written</returns>
		/// <exception cref="System.IO.IOException"/>
		private DataWriter.GenerateOutput WritePieces(long byteAm, long startPos, DataHasher
			 hasher, OutputStream @out)
		{
			if (byteAm <= 0)
			{
				return new DataWriter.GenerateOutput(0, 0);
			}
			if (startPos < 0)
			{
				startPos = 0;
			}
			int leftOver = (int)(byteAm % bufferSize);
			long fullPieces = byteAm / bufferSize;
			long offset = startPos;
			long bytesWritten = 0;
			long timeTaken = 0;
			// write the full pieces that fit in the buffer size
			for (long i = 0; i < fullPieces; ++i)
			{
				DataWriter.GenerateResult genData = GenerateFullSegment(bufferSize, offset, hasher
					);
				offset = genData.GetOffset();
				ByteBuffer gBuf = genData.GetBuffer();
				{
					byte[] buf = ((byte[])gBuf.Array());
					long startTime = Timer.Now();
					@out.Write(buf);
					timeTaken += Timer.Elapsed(startTime);
					bytesWritten += buf.Length;
				}
			}
			if (leftOver > 0)
			{
				ByteBuffer leftOverBuf = ByteBuffer.Wrap(new byte[leftOver]);
				int bytesLeft = leftOver % Constants.BytesPerLong;
				leftOver = leftOver - bytesLeft;
				// collect the piece which do not fit in the buffer size but is
				// also greater or eq than BYTES_PER_LONG and a multiple of it
				if (leftOver > 0)
				{
					DataWriter.GenerateResult genData = GenerateFullSegment(leftOver, offset, hasher);
					offset = genData.GetOffset();
					leftOverBuf.Put(genData.GetBuffer());
				}
				// collect any single partial byte segment
				if (bytesLeft > 0)
				{
					DataWriter.GenerateResult genData = GeneratePartialSegment(bytesLeft, offset, hasher
						);
					offset = genData.GetOffset();
					leftOverBuf.Put(genData.GetBuffer());
				}
				// do the write of both
				leftOverBuf.Rewind();
				{
					byte[] buf = ((byte[])leftOverBuf.Array());
					long startTime = Timer.Now();
					@out.Write(buf);
					timeTaken += Timer.Elapsed(startTime);
					bytesWritten += buf.Length;
				}
			}
			return new DataWriter.GenerateOutput(bytesWritten, timeTaken);
		}

		/// <summary>Writes to a stream the given number of bytes specified</summary>
		/// <param name="byteAm">the file size in number of bytes to write</param>
		/// <param name="out">the outputstream to write to</param>
		/// <returns>the number of bytes written + time taken</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual DataWriter.GenerateOutput WriteSegment(long byteAm, OutputStream
			 @out)
		{
			long headerLen = GetHeaderLength();
			if (byteAm < headerLen)
			{
				// not enough bytes to write even the header
				return new DataWriter.GenerateOutput(0, 0);
			}
			// adjust for header length
			byteAm -= headerLen;
			if (byteAm < 0)
			{
				byteAm = 0;
			}
			DataWriter.WriteInfo header = WriteHeader(@out, byteAm);
			DataHasher hasher = new DataHasher(header.GetHashValue());
			DataWriter.GenerateOutput pRes = WritePieces(byteAm, 0, hasher, @out);
			long bytesWritten = pRes.GetBytesWritten() + header.GetBytesWritten();
			long timeTaken = header.GetTimeTaken() + pRes.GetTimeTaken();
			return new DataWriter.GenerateOutput(bytesWritten, timeTaken);
		}

		/// <summary>Gets the header length</summary>
		/// <returns>int</returns>
		internal static int GetHeaderLength()
		{
			return HeaderLength;
		}

		/// <summary>Writes a header to the given output stream</summary>
		/// <param name="os">output stream to write to</param>
		/// <param name="fileSize">the file size to write</param>
		/// <returns>WriteInfo</returns>
		/// <exception cref="System.IO.IOException">if a write failure occurs</exception>
		internal virtual DataWriter.WriteInfo WriteHeader(OutputStream os, long fileSize)
		{
			int headerLen = GetHeaderLength();
			ByteBuffer buf = ByteBuffer.Wrap(new byte[headerLen]);
			long hash = rnd.NextLong();
			buf.PutLong(hash);
			buf.PutLong(fileSize);
			buf.Rewind();
			byte[] headerData = ((byte[])buf.Array());
			long elapsed = 0;
			{
				long startTime = Timer.Now();
				os.Write(headerData);
				elapsed += Timer.Elapsed(startTime);
			}
			return new DataWriter.WriteInfo(hash, headerLen, elapsed);
		}
	}
}
