using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>Class which reads in and verifies bytes that have been read in</summary>
	internal class DataVerifier
	{
		private const int BytesPerLong = Constants.BytesPerLong;

		private int bufferSize;

		/// <summary>
		/// The output from verification includes the number of chunks that were the
		/// same as expected and the number of segments that were different than what
		/// was expected and the number of total bytes read
		/// </summary>
		internal class VerifyOutput
		{
			private long same;

			private long different;

			private long read;

			private long readTime;

			internal VerifyOutput(long sameChunks, long differentChunks, long readBytes, long
				 readTime)
			{
				this.same = sameChunks;
				this.different = differentChunks;
				this.read = readBytes;
				this.readTime = readTime;
			}

			internal virtual long GetReadTime()
			{
				return this.readTime;
			}

			internal virtual long GetBytesRead()
			{
				return this.read;
			}

			internal virtual long GetChunksSame()
			{
				return same;
			}

			internal virtual long GetChunksDifferent()
			{
				return different;
			}

			public override string ToString()
			{
				return "Bytes read = " + GetBytesRead() + " same = " + GetChunksSame() + " different = "
					 + GetChunksDifferent() + " in " + GetReadTime() + " milliseconds";
			}
		}

		/// <summary>Class used to hold the result of a read on a header</summary>
		private class ReadInfo
		{
			private long byteAm;

			private long hash;

			private long timeTaken;

			private long bytesRead;

			internal ReadInfo(long byteAm, long hash, long timeTaken, long bytesRead)
			{
				this.byteAm = byteAm;
				this.hash = hash;
				this.timeTaken = timeTaken;
				this.bytesRead = bytesRead;
			}

			internal virtual long GetByteAm()
			{
				return byteAm;
			}

			internal virtual long GetHashValue()
			{
				return hash;
			}

			internal virtual long GetTimeTaken()
			{
				return timeTaken;
			}

			internal virtual long GetBytesRead()
			{
				return bytesRead;
			}
		}

		/// <summary>
		/// Storage class used to hold the chunks same and different for buffered reads
		/// and the resultant verification
		/// </summary>
		private class VerifyInfo
		{
			internal VerifyInfo(long same, long different)
			{
				this.same = same;
				this.different = different;
			}

			internal virtual long GetSame()
			{
				return same;
			}

			internal virtual long GetDifferent()
			{
				return different;
			}

			private long same;

			private long different;
		}

		/// <summary>
		/// Inits with given buffer size (must be greater than bytes per long and a
		/// multiple of bytes per long)
		/// </summary>
		/// <param name="bufferSize">
		/// size which must be greater than BYTES_PER_LONG and which also must
		/// be a multiple of BYTES_PER_LONG
		/// </param>
		internal DataVerifier(int bufferSize)
		{
			if (bufferSize < BytesPerLong)
			{
				throw new ArgumentException("Buffer size must be greater than or equal to " + BytesPerLong
					);
			}
			if ((bufferSize % BytesPerLong) != 0)
			{
				throw new ArgumentException("Buffer size must be a multiple of " + BytesPerLong);
			}
			this.bufferSize = bufferSize;
		}

		/// <summary>Inits with the default buffer size</summary>
		internal DataVerifier()
			: this(Constants.Buffersize)
		{
		}

		/// <summary>Verifies a buffer of a given size using the given start hash offset</summary>
		/// <param name="buf">the buffer to verify</param>
		/// <param name="size">the number of bytes to be used in that buffer</param>
		/// <param name="startOffset">the start hash offset</param>
		/// <param name="hasher">the hasher to use for calculating expected values</param>
		/// <returns>ResumeBytes a set of data about the next offset and chunks analyzed</returns>
		private DataVerifier.VerifyInfo VerifyBuffer(ByteBuffer buf, int size, long startOffset
			, DataHasher hasher)
		{
			ByteBuffer cmpBuf = ByteBuffer.Wrap(new byte[BytesPerLong]);
			long hashOffset = startOffset;
			long chunksSame = 0;
			long chunksDifferent = 0;
			for (long i = 0; i < size; ++i)
			{
				cmpBuf.Put(buf.Get());
				if (!cmpBuf.HasRemaining())
				{
					cmpBuf.Rewind();
					long receivedData = cmpBuf.GetLong();
					cmpBuf.Rewind();
					long expected = hasher.Generate(hashOffset);
					hashOffset += BytesPerLong;
					if (receivedData == expected)
					{
						++chunksSame;
					}
					else
					{
						++chunksDifferent;
					}
				}
			}
			// any left over??
			if (cmpBuf.HasRemaining() && cmpBuf.Position() != 0)
			{
				// partial capture
				// zero fill and compare with zero filled
				int curSize = cmpBuf.Position();
				while (cmpBuf.HasRemaining())
				{
					cmpBuf.Put(unchecked((byte)0));
				}
				long expected = hasher.Generate(hashOffset);
				ByteBuffer tempBuf = ByteBuffer.Wrap(new byte[BytesPerLong]);
				tempBuf.PutLong(expected);
				tempBuf.Position(curSize);
				while (tempBuf.HasRemaining())
				{
					tempBuf.Put(unchecked((byte)0));
				}
				cmpBuf.Rewind();
				tempBuf.Rewind();
				if (cmpBuf.Equals(tempBuf))
				{
					++chunksSame;
				}
				else
				{
					++chunksDifferent;
				}
			}
			return new DataVerifier.VerifyInfo(chunksSame, chunksDifferent);
		}

		/// <summary>Determines the offset to use given a byte counter</summary>
		/// <param name="byteRead"/>
		/// <returns>offset position</returns>
		private long DetermineOffset(long byteRead)
		{
			if (byteRead < 0)
			{
				byteRead = 0;
			}
			return (byteRead / BytesPerLong) * BytesPerLong;
		}

		/// <summary>
		/// Verifies a given number of bytes from a file - less number of bytes may be
		/// read if a header can not be read in due to the byte limit
		/// </summary>
		/// <param name="byteAm">
		/// the byte amount to limit to (should be less than or equal to file
		/// size)
		/// </param>
		/// <param name="in">the input stream to read from</param>
		/// <returns>VerifyOutput with data about reads</returns>
		/// <exception cref="System.IO.IOException">if a read failure occurs</exception>
		/// <exception cref="BadFileException">
		/// if a header can not be read or end of file is reached
		/// unexpectedly
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.Slive.BadFileException"/>
		internal virtual DataVerifier.VerifyOutput VerifyFile(long byteAm, DataInputStream
			 @in)
		{
			return VerifyBytes(byteAm, 0, @in);
		}

		/// <summary>
		/// Verifies a given number of bytes from a file - less number of bytes may be
		/// read if a header can not be read in due to the byte limit
		/// </summary>
		/// <param name="byteAm">
		/// the byte amount to limit to (should be less than or equal to file
		/// size)
		/// </param>
		/// <param name="bytesRead">the starting byte location</param>
		/// <param name="in">the input stream to read from</param>
		/// <returns>VerifyOutput with data about reads</returns>
		/// <exception cref="System.IO.IOException">if a read failure occurs</exception>
		/// <exception cref="BadFileException">
		/// if a header can not be read or end of file is reached
		/// unexpectedly
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.Slive.BadFileException"/>
		private DataVerifier.VerifyOutput VerifyBytes(long byteAm, long bytesRead, DataInputStream
			 @in)
		{
			if (byteAm <= 0)
			{
				return new DataVerifier.VerifyOutput(0, 0, 0, 0);
			}
			long chunksSame = 0;
			long chunksDifferent = 0;
			long readTime = 0;
			long bytesLeft = byteAm;
			long bufLeft = 0;
			long bufRead = 0;
			long seqNum = 0;
			DataHasher hasher = null;
			ByteBuffer readBuf = ByteBuffer.Wrap(new byte[bufferSize]);
			while (bytesLeft > 0)
			{
				if (bufLeft <= 0)
				{
					if (bytesLeft < DataWriter.GetHeaderLength())
					{
						// no bytes left to read a header
						break;
					}
					// time to read a new header
					DataVerifier.ReadInfo header = null;
					try
					{
						header = ReadHeader(@in);
					}
					catch (EOFException)
					{
						// eof ok on header reads
						// but not on data readers
						break;
					}
					++seqNum;
					hasher = new DataHasher(header.GetHashValue());
					bufLeft = header.GetByteAm();
					readTime += header.GetTimeTaken();
					bytesRead += header.GetBytesRead();
					bytesLeft -= header.GetBytesRead();
					bufRead = 0;
					// number of bytes to read greater than how many we want to read
					if (bufLeft > bytesLeft)
					{
						bufLeft = bytesLeft;
					}
					// does the buffer amount have anything??
					if (bufLeft <= 0)
					{
						continue;
					}
				}
				// figure out the buffer size to read
				int bufSize = bufferSize;
				if (bytesLeft < bufSize)
				{
					bufSize = (int)bytesLeft;
				}
				if (bufLeft < bufSize)
				{
					bufSize = (int)bufLeft;
				}
				// read it in
				try
				{
					readBuf.Rewind();
					long startTime = Timer.Now();
					@in.ReadFully(((byte[])readBuf.Array()), 0, bufSize);
					readTime += Timer.Elapsed(startTime);
				}
				catch (EOFException e)
				{
					throw new BadFileException("Could not read the number of expected data bytes " + 
						bufSize + " due to unexpected end of file during sequence " + seqNum, e);
				}
				// update the counters
				bytesRead += bufSize;
				bytesLeft -= bufSize;
				bufLeft -= bufSize;
				// verify what we read
				readBuf.Rewind();
				// figure out the expected hash offset start point
				long vOffset = DetermineOffset(bufRead);
				// now update for new position
				bufRead += bufSize;
				// verify
				DataVerifier.VerifyInfo verifyRes = VerifyBuffer(readBuf, bufSize, vOffset, hasher
					);
				// update the verification counters
				chunksSame += verifyRes.GetSame();
				chunksDifferent += verifyRes.GetDifferent();
			}
			return new DataVerifier.VerifyOutput(chunksSame, chunksDifferent, bytesRead, readTime
				);
		}

		/// <summary>Reads a header from the given input stream</summary>
		/// <param name="in">input stream to read from</param>
		/// <returns>ReadInfo</returns>
		/// <exception cref="System.IO.IOException">if a read error occurs or EOF occurs</exception>
		/// <exception cref="BadFileException">if end of file occurs or the byte amount read is invalid
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.Slive.BadFileException"/>
		internal virtual DataVerifier.ReadInfo ReadHeader(DataInputStream @in)
		{
			int headerLen = DataWriter.GetHeaderLength();
			ByteBuffer headerBuf = ByteBuffer.Wrap(new byte[headerLen]);
			long elapsed = 0;
			{
				long startTime = Timer.Now();
				@in.ReadFully(((byte[])headerBuf.Array()));
				elapsed += Timer.Elapsed(startTime);
			}
			headerBuf.Rewind();
			long hashValue = headerBuf.GetLong();
			long byteAvailable = headerBuf.GetLong();
			if (byteAvailable < 0)
			{
				throw new BadFileException("Invalid negative amount " + byteAvailable + " determined for header data amount"
					);
			}
			return new DataVerifier.ReadInfo(byteAvailable, hashValue, elapsed, headerLen);
		}
	}
}
