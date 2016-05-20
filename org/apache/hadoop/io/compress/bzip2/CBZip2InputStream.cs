/*
*  Licensed to the Apache Software Foundation (ASF) under one or more
*  contributor license agreements.  See the NOTICE file distributed with
*  this work for additional information regarding copyright ownership.
*  The ASF licenses this file to You under the Apache License, Version 2.0
*  (the "License"); you may not use this file except in compliance with
*  the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
*/
/*
* This package is based on the work done by Keiron Liddle, Aftex Software
* <keiron@aftexsw.com> to whom the Ant project is very grateful for his
* great code.
*/
using Sharpen;

namespace org.apache.hadoop.io.compress.bzip2
{
	/// <summary>
	/// An input stream that decompresses from the BZip2 format (without the file
	/// header chars) to be read as any other stream.
	/// </summary>
	/// <remarks>
	/// An input stream that decompresses from the BZip2 format (without the file
	/// header chars) to be read as any other stream.
	/// <p>
	/// The decompression requires large amounts of memory. Thus you should call the
	/// <see cref="close()">close()</see>
	/// method as soon as possible, to force
	/// <tt>CBZip2InputStream</tt> to release the allocated memory. See
	/// <see cref="CBZip2OutputStream">CBZip2OutputStream</see>
	/// for information about memory
	/// usage.
	/// </p>
	/// <p>
	/// <tt>CBZip2InputStream</tt> reads bytes from the compressed source stream via
	/// the single byte
	/// <see cref="java.io.InputStream.read()">read()</see>
	/// method exclusively.
	/// Thus you should consider to use a buffered source stream.
	/// </p>
	/// <p>
	/// This Ant code was enhanced so that it can de-compress blocks of bzip2 data.
	/// Current position in the stream is an important statistic for Hadoop. For
	/// example in LineRecordReader, we solely depend on the current position in the
	/// stream to know about the progess. The notion of position becomes complicated
	/// for compressed files. The Hadoop splitting is done in terms of compressed
	/// file. But a compressed file deflates to a large amount of data. So we have
	/// handled this problem in the following way.
	/// On object creation time, we find the next block start delimiter. Once such a
	/// marker is found, the stream stops there (we discard any read compressed data
	/// in this process) and the position is updated (i.e. the caller of this class
	/// will find out the stream location). At this point we are ready for actual
	/// reading (i.e. decompression) of data.
	/// The subsequent read calls give out data. The position is updated when the
	/// caller of this class has read off the current block + 1 bytes. In between the
	/// block reading, position is not updated. (We can only update the postion on
	/// block boundaries).
	/// </p>
	/// <p>
	/// Instances of this class are not threadsafe.
	/// </p>
	/// </remarks>
	public class CBZip2InputStream : java.io.InputStream, org.apache.hadoop.io.compress.bzip2.BZip2Constants
	{
		public const long BLOCK_DELIMITER = 0X314159265359L;

		public const long EOS_DELIMITER = 0X177245385090L;

		private const int DELIMITER_BIT_LENGTH = 48;

		internal org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE readMode
			 = org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE.CONTINUOUS;

		private long reportedBytesReadFromCompressedStream = 0L;

		private long bytesReadFromCompressedStream = 0L;

		private bool lazyInitialization = false;

		private byte[] array = new byte[1];

		/// <summary>Index of the last char in the block, so the block size == last + 1.</summary>
		private int last;

		/// <summary>Index in zptr[] of original string after sorting.</summary>
		private int origPtr;

		/// <summary>always: in the range 0 ..</summary>
		/// <remarks>
		/// always: in the range 0 .. 9. The current block size is 100000 * this
		/// number.
		/// </remarks>
		private int blockSize100k;

		private bool blockRandomised = false;

		private long bsBuff;

		private long bsLive;

		private readonly org.apache.hadoop.io.compress.bzip2.CRC crc = new org.apache.hadoop.io.compress.bzip2.CRC
			();

		private int nInUse;

		private java.io.BufferedInputStream @in;

		private int currentChar = -1;

		/// <summary>A state machine to keep track of current state of the de-coder</summary>
		public enum STATE
		{
			EOF,
			START_BLOCK_STATE,
			RAND_PART_A_STATE,
			RAND_PART_B_STATE,
			RAND_PART_C_STATE,
			NO_RAND_PART_A_STATE,
			NO_RAND_PART_B_STATE,
			NO_RAND_PART_C_STATE,
			NO_PROCESS_STATE
		}

		private org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE currentState = 
			org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.START_BLOCK_STATE;

		private int storedBlockCRC;

		private int storedCombinedCRC;

		private int computedBlockCRC;

		private int computedCombinedCRC;

		private bool skipResult = false;

		private bool skipDecompression = false;

		private int su_count;

		private int su_ch2;

		private int su_chPrev;

		private int su_i2;

		private int su_j2;

		private int su_rNToGo;

		private int su_rTPos;

		private int su_tPos;

		private char su_z;

		/// <summary>All memory intensive stuff.</summary>
		/// <remarks>All memory intensive stuff. This field is initialized by initBlock().</remarks>
		private org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.Data data;

		// start of block
		// end of bzip2 stream
		// The variable records the current advertised position of the stream.
		// The following variable keep record of compressed bytes read.
		// used by skipToNextMarker
		// Variables used by setup* methods exclusively
		/// <summary>This method reports the processed bytes so far.</summary>
		/// <remarks>
		/// This method reports the processed bytes so far. Please note that this
		/// statistic is only updated on block boundaries and only when the stream is
		/// initiated in BYBLOCK mode.
		/// </remarks>
		public virtual long getProcessedByteCount()
		{
			return reportedBytesReadFromCompressedStream;
		}

		/// <summary>
		/// This method keeps track of raw processed compressed
		/// bytes.
		/// </summary>
		/// <param name="count">
		/// count is the number of bytes to be
		/// added to raw processed bytes
		/// </param>
		protected internal virtual void updateProcessedByteCount(int count)
		{
			this.bytesReadFromCompressedStream += count;
		}

		/// <summary>
		/// This method is called by the client of this
		/// class in case there are any corrections in
		/// the stream position.
		/// </summary>
		/// <remarks>
		/// This method is called by the client of this
		/// class in case there are any corrections in
		/// the stream position.  One common example is
		/// when client of this code removes starting BZ
		/// characters from the compressed stream.
		/// </remarks>
		/// <param name="count">count bytes are added to the reported bytes</param>
		public virtual void updateReportedByteCount(int count)
		{
			this.reportedBytesReadFromCompressedStream += count;
			this.updateProcessedByteCount(count);
		}

		/// <summary>This method reads a Byte from the compressed stream.</summary>
		/// <remarks>
		/// This method reads a Byte from the compressed stream. Whenever we need to
		/// read from the underlying compressed stream, this method should be called
		/// instead of directly calling the read method of the underlying compressed
		/// stream. This method does important record keeping to have the statistic
		/// that how many bytes have been read off the compressed stream.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private int readAByte(java.io.InputStream inStream)
		{
			int read = inStream.read();
			if (read >= 0)
			{
				this.updateProcessedByteCount(1);
			}
			return read;
		}

		/// <summary>
		/// This method tries to find the marker (passed to it as the first parameter)
		/// in the stream.
		/// </summary>
		/// <remarks>
		/// This method tries to find the marker (passed to it as the first parameter)
		/// in the stream.  It can find bit patterns of length &lt;= 63 bits.  Specifically
		/// this method is used in CBZip2InputStream to find the end of block (EOB)
		/// delimiter in the stream, starting from the current position of the stream.
		/// If marker is found, the stream position will be right after marker at the
		/// end of this call.
		/// </remarks>
		/// <param name="marker">The bit pattern to be found in the stream</param>
		/// <param name="markerBitLength">No of bits in the marker</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.ArgumentException">if marketBitLength is greater than 63</exception>
		public virtual bool skipToNextMarker(long marker, int markerBitLength)
		{
			try
			{
				if (markerBitLength > 63)
				{
					throw new System.ArgumentException("skipToNextMarker can not find patterns greater than 63 bits"
						);
				}
				// pick next marketBitLength bits in the stream
				long bytes = 0;
				bytes = this.bsR(markerBitLength);
				if (bytes == -1)
				{
					return false;
				}
				while (true)
				{
					if (bytes == marker)
					{
						return true;
					}
					else
					{
						bytes = bytes << 1;
						bytes = bytes & ((1L << markerBitLength) - 1);
						int oneBit = (int)this.bsR(1);
						if (oneBit != -1)
						{
							bytes = bytes | oneBit;
						}
						else
						{
							return false;
						}
					}
				}
			}
			catch (System.IO.IOException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void reportCRCError()
		{
			throw new System.IO.IOException("crc error");
		}

		private void makeMaps()
		{
			bool[] inUse = this.data.inUse;
			byte[] seqToUnseq = this.data.seqToUnseq;
			int nInUseShadow = 0;
			for (int i = 0; i < 256; i++)
			{
				if (inUse[i])
				{
					seqToUnseq[nInUseShadow++] = unchecked((byte)i);
				}
			}
			this.nInUse = nInUseShadow;
		}

		/// <summary>
		/// Constructs a new CBZip2InputStream which decompresses bytes read from the
		/// specified stream.
		/// </summary>
		/// <remarks>
		/// Constructs a new CBZip2InputStream which decompresses bytes read from the
		/// specified stream.
		/// <p>
		/// Although BZip2 headers are marked with the magic <tt>"Bz"</tt> this
		/// constructor expects the next byte in the stream to be the first one after
		/// the magic. Thus callers have to skip the first two bytes. Otherwise this
		/// constructor will throw an exception.
		/// </p>
		/// </remarks>
		/// <exception cref="System.IO.IOException">if the stream content is malformed or an I/O error occurs.
		/// 	</exception>
		/// <exception cref="System.ArgumentNullException">if <tt>in == null</tt></exception>
		public CBZip2InputStream(java.io.InputStream @in, org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE
			 readMode)
			: this(@in, readMode, false)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private CBZip2InputStream(java.io.InputStream @in, org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE
			 readMode, bool skipDecompression)
			: base()
		{
			int blockSize = 0X39;
			// i.e 9
			this.blockSize100k = blockSize - '0';
			this.@in = new java.io.BufferedInputStream(@in, 1024 * 9);
			// >1 MB buffer
			this.readMode = readMode;
			this.skipDecompression = skipDecompression;
			if (readMode == org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE
				.CONTINUOUS)
			{
				currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.START_BLOCK_STATE;
				lazyInitialization = (@in.available() == 0) ? true : false;
				if (!lazyInitialization)
				{
					init();
				}
			}
			else
			{
				if (readMode == org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE
					.BYBLOCK)
				{
					this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.NO_PROCESS_STATE;
					skipResult = this.skipToNextMarker(org.apache.hadoop.io.compress.bzip2.CBZip2InputStream
						.BLOCK_DELIMITER, DELIMITER_BIT_LENGTH);
					this.reportedBytesReadFromCompressedStream = this.bytesReadFromCompressedStream;
					if (!skipDecompression)
					{
						changeStateToProcessABlock();
					}
				}
			}
		}

		/// <summary>
		/// Returns the number of bytes between the current stream position
		/// and the immediate next BZip2 block marker.
		/// </summary>
		/// <param name="in">The InputStream</param>
		/// <returns>
		/// long Number of bytes between current stream position and the
		/// next BZip2 block start marker.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static long numberOfBytesTillNextMarker(java.io.InputStream @in)
		{
			org.apache.hadoop.io.compress.bzip2.CBZip2InputStream anObject = new org.apache.hadoop.io.compress.bzip2.CBZip2InputStream
				(@in, org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE.BYBLOCK
				, true);
			return anObject.getProcessedByteCount();
		}

		/// <exception cref="System.IO.IOException"/>
		public CBZip2InputStream(java.io.InputStream @in)
			: this(@in, org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE.CONTINUOUS
				)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private void changeStateToProcessABlock()
		{
			if (skipResult == true)
			{
				initBlock();
				setupBlock();
			}
			else
			{
				this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.EOF;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int read()
		{
			if (this.@in != null)
			{
				int result = this.read(array, 0, 1);
				int value = 0XFF & array[0];
				return (result > 0 ? value : result);
			}
			else
			{
				throw new System.IO.IOException("stream closed");
			}
		}

		/// <summary>
		/// In CONTINOUS reading mode, this read method starts from the
		/// start of the compressed stream and end at the end of file by
		/// emitting un-compressed data.
		/// </summary>
		/// <remarks>
		/// In CONTINOUS reading mode, this read method starts from the
		/// start of the compressed stream and end at the end of file by
		/// emitting un-compressed data.  In this mode stream positioning
		/// is not announced and should be ignored.
		/// In BYBLOCK reading mode, this read method informs about the end
		/// of a BZip2 block by returning EOB.  At this event, the compressed
		/// stream position is also announced.  This announcement tells that
		/// how much of the compressed stream has been de-compressed and read
		/// out of this class.  In between EOB events, the stream position is
		/// not updated.
		/// </remarks>
		/// <exception cref="System.IO.IOException">if the stream content is malformed or an I/O error occurs.
		/// 	</exception>
		/// <returns>
		/// int The return value greater than 0 are the bytes read.  A value
		/// of -1 means end of stream while -2 represents end of block
		/// </returns>
		public override int read(byte[] dest, int offs, int len)
		{
			if (offs < 0)
			{
				throw new System.IndexOutOfRangeException("offs(" + offs + ") < 0.");
			}
			if (len < 0)
			{
				throw new System.IndexOutOfRangeException("len(" + len + ") < 0.");
			}
			if (offs + len > dest.Length)
			{
				throw new System.IndexOutOfRangeException("offs(" + offs + ") + len(" + len + ") > dest.length("
					 + dest.Length + ").");
			}
			if (this.@in == null)
			{
				throw new System.IO.IOException("stream closed");
			}
			if (lazyInitialization)
			{
				this.init();
				this.lazyInitialization = false;
			}
			if (skipDecompression)
			{
				changeStateToProcessABlock();
				skipDecompression = false;
			}
			int hi = offs + len;
			int destOffs = offs;
			int b = 0;
			for (; ((destOffs < hi) && ((b = read0())) >= 0); )
			{
				dest[destOffs++] = unchecked((byte)b);
			}
			int result = destOffs - offs;
			if (result == 0)
			{
				//report 'end of block' or 'end of stream'
				result = b;
				skipResult = this.skipToNextMarker(org.apache.hadoop.io.compress.bzip2.CBZip2InputStream
					.BLOCK_DELIMITER, DELIMITER_BIT_LENGTH);
				//Exactly when we are about to start a new block, we advertise the stream position.
				this.reportedBytesReadFromCompressedStream = this.bytesReadFromCompressedStream;
				changeStateToProcessABlock();
			}
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		private int read0()
		{
			int retChar = this.currentChar;
			switch (this.currentState)
			{
				case org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.EOF:
				{
					return END_OF_STREAM;
				}

				case org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.NO_PROCESS_STATE
					:
				{
					// return -1
					return END_OF_BLOCK;
				}

				case org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.START_BLOCK_STATE
					:
				{
					// return -2
					throw new System.InvalidOperationException();
				}

				case org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.RAND_PART_A_STATE
					:
				{
					throw new System.InvalidOperationException();
				}

				case org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.RAND_PART_B_STATE
					:
				{
					setupRandPartB();
					break;
				}

				case org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.RAND_PART_C_STATE
					:
				{
					setupRandPartC();
					break;
				}

				case org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.NO_RAND_PART_A_STATE
					:
				{
					throw new System.InvalidOperationException();
				}

				case org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.NO_RAND_PART_B_STATE
					:
				{
					setupNoRandPartB();
					break;
				}

				case org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.NO_RAND_PART_C_STATE
					:
				{
					setupNoRandPartC();
					break;
				}

				default:
				{
					throw new System.InvalidOperationException();
				}
			}
			return retChar;
		}

		/// <exception cref="System.IO.IOException"/>
		private void init()
		{
			int magic2 = this.readAByte(@in);
			if (magic2 != 'h')
			{
				throw new System.IO.IOException("Stream is not BZip2 formatted: expected 'h'" + " as first byte but got '"
					 + (char)magic2 + "'");
			}
			int blockSize = this.readAByte(@in);
			if ((blockSize < '1') || (blockSize > '9'))
			{
				throw new System.IO.IOException("Stream is not BZip2 formatted: illegal " + "blocksize "
					 + (char)blockSize);
			}
			this.blockSize100k = blockSize - '0';
			initBlock();
			setupBlock();
		}

		/// <exception cref="System.IO.IOException"/>
		private void initBlock()
		{
			if (this.readMode == org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE
				.BYBLOCK)
			{
				// this.checkBlockIntegrity();
				this.storedBlockCRC = bsGetInt();
				this.blockRandomised = bsR(1) == 1;
				if (this.data == null)
				{
					this.data = new org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.Data(this.blockSize100k
						);
				}
				// currBlockNo++;
				getAndMoveToFrontDecode();
				this.crc.initialiseCRC();
				this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.START_BLOCK_STATE;
				return;
			}
			char magic0 = bsGetUByte();
			char magic1 = bsGetUByte();
			char magic2 = bsGetUByte();
			char magic3 = bsGetUByte();
			char magic4 = bsGetUByte();
			char magic5 = bsGetUByte();
			if (magic0 == unchecked((int)(0x17)) && magic1 == unchecked((int)(0x72)) && magic2
				 == unchecked((int)(0x45)) && magic3 == unchecked((int)(0x38)) && magic4 == unchecked(
				(int)(0x50)) && magic5 == unchecked((int)(0x90)))
			{
				complete();
			}
			else
			{
				// end of file
				if (magic0 != unchecked((int)(0x31)) || magic1 != unchecked((int)(0x41)) || magic2
					 != unchecked((int)(0x59)) || magic3 != unchecked((int)(0x26)) || magic4 != unchecked(
					(int)(0x53)) || magic5 != unchecked((int)(0x59)))
				{
					// '1'
					// ')'
					// 'Y'
					// '&'
					// 'S'
					// 'Y'
					this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.EOF;
					throw new System.IO.IOException("bad block header");
				}
				else
				{
					this.storedBlockCRC = bsGetInt();
					this.blockRandomised = bsR(1) == 1;
					if (this.data == null)
					{
						this.data = new org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.Data(this.blockSize100k
							);
					}
					// currBlockNo++;
					getAndMoveToFrontDecode();
					this.crc.initialiseCRC();
					this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.START_BLOCK_STATE;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void endBlock()
		{
			this.computedBlockCRC = this.crc.getFinalCRC();
			// A bad CRC is considered a fatal error.
			if (this.storedBlockCRC != this.computedBlockCRC)
			{
				// make next blocks readable without error
				// (repair feature, not yet documented, not tested)
				this.computedCombinedCRC = (this.storedCombinedCRC << 1) | ((int)(((uint)this.storedCombinedCRC
					) >> 31));
				this.computedCombinedCRC ^= this.storedBlockCRC;
				reportCRCError();
			}
			this.computedCombinedCRC = (this.computedCombinedCRC << 1) | ((int)(((uint)this.computedCombinedCRC
				) >> 31));
			this.computedCombinedCRC ^= this.computedBlockCRC;
		}

		/// <exception cref="System.IO.IOException"/>
		private void complete()
		{
			this.storedCombinedCRC = bsGetInt();
			this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.EOF;
			this.data = null;
			if (this.storedCombinedCRC != this.computedCombinedCRC)
			{
				reportCRCError();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			java.io.InputStream inShadow = this.@in;
			if (inShadow != null)
			{
				try
				{
					if (inShadow != Sharpen.Runtime.@in)
					{
						inShadow.close();
					}
				}
				finally
				{
					this.data = null;
					this.@in = null;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private long bsR(long n)
		{
			long bsLiveShadow = this.bsLive;
			long bsBuffShadow = this.bsBuff;
			if (bsLiveShadow < n)
			{
				java.io.InputStream inShadow = this.@in;
				do
				{
					int thech = readAByte(inShadow);
					if (thech < 0)
					{
						throw new System.IO.IOException("unexpected end of stream");
					}
					bsBuffShadow = (bsBuffShadow << 8) | thech;
					bsLiveShadow += 8;
				}
				while (bsLiveShadow < n);
				this.bsBuff = bsBuffShadow;
			}
			this.bsLive = bsLiveShadow - n;
			return (bsBuffShadow >> (bsLiveShadow - n)) & ((1L << n) - 1);
		}

		/// <exception cref="System.IO.IOException"/>
		private bool bsGetBit()
		{
			long bsLiveShadow = this.bsLive;
			long bsBuffShadow = this.bsBuff;
			if (bsLiveShadow < 1)
			{
				int thech = this.readAByte(@in);
				if (thech < 0)
				{
					throw new System.IO.IOException("unexpected end of stream");
				}
				bsBuffShadow = (bsBuffShadow << 8) | thech;
				bsLiveShadow += 8;
				this.bsBuff = bsBuffShadow;
			}
			this.bsLive = bsLiveShadow - 1;
			return ((bsBuffShadow >> (bsLiveShadow - 1)) & 1) != 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private char bsGetUByte()
		{
			return (char)bsR(8);
		}

		/// <exception cref="System.IO.IOException"/>
		private int bsGetInt()
		{
			return (int)((((((bsR(8) << 8) | bsR(8)) << 8) | bsR(8)) << 8) | bsR(8));
		}

		/// <summary>Called by createHuffmanDecodingTables() exclusively.</summary>
		private static void hbCreateDecodeTables(int[] limit, int[] @base, int[] perm, char
			[] length, int minLen, int maxLen, int alphaSize)
		{
			for (int i = minLen; i <= maxLen; i++)
			{
				for (int j = 0; j < alphaSize; j++)
				{
					if (length[j] == i)
					{
						perm[pp++] = j;
					}
				}
			}
			for (int i_1 = MAX_CODE_LEN; --i_1 > 0; )
			{
				@base[i_1] = 0;
				limit[i_1] = 0;
			}
			for (int i_2 = 0; i_2 < alphaSize; i_2++)
			{
				@base[length[i_2] + 1]++;
			}
			for (int i_3 = 1; i_3 < MAX_CODE_LEN; i_3++)
			{
				b += @base[i_3];
				@base[i_3] = b;
			}
			for (int i_4 = minLen; i_4 <= maxLen; i_4++)
			{
				int nb = @base[i_4 + 1];
				vec += nb - b;
				b = nb;
				limit[i_4] = vec - 1;
				vec <<= 1;
			}
			for (int i_5 = minLen + 1; i_5 <= maxLen; i_5++)
			{
				@base[i_5] = ((limit[i_5 - 1] + 1) << 1) - @base[i_5];
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void recvDecodingTables()
		{
			org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.Data dataShadow = this.data;
			bool[] inUse = dataShadow.inUse;
			byte[] pos = dataShadow.recvDecodingTables_pos;
			byte[] selector = dataShadow.selector;
			byte[] selectorMtf = dataShadow.selectorMtf;
			int inUse16 = 0;
			/* Receive the mapping table */
			for (int i = 0; i < 16; i++)
			{
				if (bsGetBit())
				{
					inUse16 |= 1 << i;
				}
			}
			for (int i_1 = 256; --i_1 >= 0; )
			{
				inUse[i_1] = false;
			}
			for (int i_2 = 0; i_2 < 16; i_2++)
			{
				if ((inUse16 & (1 << i_2)) != 0)
				{
					int i16 = i_2 << 4;
					for (int j = 0; j < 16; j++)
					{
						if (bsGetBit())
						{
							inUse[i16 + j] = true;
						}
					}
				}
			}
			makeMaps();
			int alphaSize = this.nInUse + 2;
			/* Now the selectors */
			int nGroups = (int)bsR(3);
			int nSelectors = (int)bsR(15);
			for (int i_3 = 0; i_3 < nSelectors; i_3++)
			{
				int j = 0;
				while (bsGetBit())
				{
					j++;
				}
				selectorMtf[i_3] = unchecked((byte)j);
			}
			/* Undo the MTF values for the selectors. */
			for (int v = nGroups; --v >= 0; )
			{
				pos[v] = unchecked((byte)v);
			}
			for (int i_4 = 0; i_4 < nSelectors; i_4++)
			{
				int v_1 = selectorMtf[i_4] & unchecked((int)(0xff));
				byte tmp = pos[v_1];
				while (v_1 > 0)
				{
					// nearly all times v is zero, 4 in most other cases
					pos[v_1] = pos[v_1 - 1];
					v_1--;
				}
				pos[0] = tmp;
				selector[i_4] = tmp;
			}
			char[][] len = dataShadow.temp_charArray2d;
			/* Now the coding tables */
			for (int t = 0; t < nGroups; t++)
			{
				int curr = (int)bsR(5);
				char[] len_t = len[t];
				for (int i_5 = 0; i_5 < alphaSize; i_5++)
				{
					while (bsGetBit())
					{
						curr += bsGetBit() ? -1 : 1;
					}
					len_t[i_5] = (char)curr;
				}
			}
			// finally create the Huffman tables
			createHuffmanDecodingTables(alphaSize, nGroups);
		}

		/// <summary>Called by recvDecodingTables() exclusively.</summary>
		private void createHuffmanDecodingTables(int alphaSize, int nGroups)
		{
			org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.Data dataShadow = this.data;
			char[][] len = dataShadow.temp_charArray2d;
			int[] minLens = dataShadow.minLens;
			int[][] limit = dataShadow.limit;
			int[][] @base = dataShadow.@base;
			int[][] perm = dataShadow.perm;
			for (int t = 0; t < nGroups; t++)
			{
				int minLen = 32;
				int maxLen = 0;
				char[] len_t = len[t];
				for (int i = alphaSize; --i >= 0; )
				{
					char lent = len_t[i];
					if (lent > maxLen)
					{
						maxLen = lent;
					}
					if (lent < minLen)
					{
						minLen = lent;
					}
				}
				hbCreateDecodeTables(limit[t], @base[t], perm[t], len[t], minLen, maxLen, alphaSize
					);
				minLens[t] = minLen;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void getAndMoveToFrontDecode()
		{
			this.origPtr = (int)bsR(24);
			recvDecodingTables();
			java.io.InputStream inShadow = this.@in;
			org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.Data dataShadow = this.data;
			byte[] ll8 = dataShadow.ll8;
			int[] unzftab = dataShadow.unzftab;
			byte[] selector = dataShadow.selector;
			byte[] seqToUnseq = dataShadow.seqToUnseq;
			char[] yy = dataShadow.getAndMoveToFrontDecode_yy;
			int[] minLens = dataShadow.minLens;
			int[][] limit = dataShadow.limit;
			int[][] @base = dataShadow.@base;
			int[][] perm = dataShadow.perm;
			int limitLast = this.blockSize100k * 100000;
			/*
			* Setting up the unzftab entries here is not strictly necessary, but it
			* does save having to do it later in a separate pass, and so saves a
			* block's worth of cache misses.
			*/
			for (int i = 256; --i >= 0; )
			{
				yy[i] = (char)i;
				unzftab[i] = 0;
			}
			int groupNo = 0;
			int groupPos = G_SIZE - 1;
			int eob = this.nInUse + 1;
			int nextSym = getAndMoveToFrontDecode0(0);
			int bsBuffShadow = (int)this.bsBuff;
			int bsLiveShadow = (int)this.bsLive;
			int lastShadow = -1;
			int zt = selector[groupNo] & unchecked((int)(0xff));
			int[] base_zt = @base[zt];
			int[] limit_zt = limit[zt];
			int[] perm_zt = perm[zt];
			int minLens_zt = minLens[zt];
			while (nextSym != eob)
			{
				if ((nextSym == RUNA) || (nextSym == RUNB))
				{
					int s = -1;
					for (int n = 1; true; n <<= 1)
					{
						if (nextSym == RUNA)
						{
							s += n;
						}
						else
						{
							if (nextSym == RUNB)
							{
								s += n << 1;
							}
							else
							{
								break;
							}
						}
						if (groupPos == 0)
						{
							groupPos = G_SIZE - 1;
							zt = selector[++groupNo] & unchecked((int)(0xff));
							base_zt = @base[zt];
							limit_zt = limit[zt];
							perm_zt = perm[zt];
							minLens_zt = minLens[zt];
						}
						else
						{
							groupPos--;
						}
						int zn = minLens_zt;
						while (bsLiveShadow < zn)
						{
							int thech = readAByte(inShadow);
							if (thech >= 0)
							{
								bsBuffShadow = (bsBuffShadow << 8) | thech;
								bsLiveShadow += 8;
								continue;
							}
							else
							{
								throw new System.IO.IOException("unexpected end of stream");
							}
						}
						long zvec = (bsBuffShadow >> (bsLiveShadow - zn)) & ((1 << zn) - 1);
						bsLiveShadow -= zn;
						while (zvec > limit_zt[zn])
						{
							zn++;
							while (bsLiveShadow < 1)
							{
								int thech = readAByte(inShadow);
								if (thech >= 0)
								{
									bsBuffShadow = (bsBuffShadow << 8) | thech;
									bsLiveShadow += 8;
									continue;
								}
								else
								{
									throw new System.IO.IOException("unexpected end of stream");
								}
							}
							bsLiveShadow--;
							zvec = (zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1);
						}
						nextSym = perm_zt[(int)(zvec - base_zt[zn])];
					}
					byte ch = seqToUnseq[yy[0]];
					unzftab[ch & unchecked((int)(0xff))] += s + 1;
					while (s-- >= 0)
					{
						ll8[++lastShadow] = ch;
					}
					if (lastShadow >= limitLast)
					{
						throw new System.IO.IOException("block overrun");
					}
				}
				else
				{
					if (++lastShadow >= limitLast)
					{
						throw new System.IO.IOException("block overrun");
					}
					char tmp = yy[nextSym - 1];
					unzftab[seqToUnseq[tmp] & unchecked((int)(0xff))]++;
					ll8[lastShadow] = seqToUnseq[tmp];
					/*
					* This loop is hammered during decompression, hence avoid
					* native method call overhead of System.arraycopy for very
					* small ranges to copy.
					*/
					if (nextSym <= 16)
					{
						for (int j = nextSym - 1; j > 0; )
						{
							yy[j] = yy[--j];
						}
					}
					else
					{
						System.Array.Copy(yy, 0, yy, 1, nextSym - 1);
					}
					yy[0] = tmp;
					if (groupPos == 0)
					{
						groupPos = G_SIZE - 1;
						zt = selector[++groupNo] & unchecked((int)(0xff));
						base_zt = @base[zt];
						limit_zt = limit[zt];
						perm_zt = perm[zt];
						minLens_zt = minLens[zt];
					}
					else
					{
						groupPos--;
					}
					int zn = minLens_zt;
					while (bsLiveShadow < zn)
					{
						int thech = readAByte(inShadow);
						if (thech >= 0)
						{
							bsBuffShadow = (bsBuffShadow << 8) | thech;
							bsLiveShadow += 8;
							continue;
						}
						else
						{
							throw new System.IO.IOException("unexpected end of stream");
						}
					}
					int zvec = (bsBuffShadow >> (bsLiveShadow - zn)) & ((1 << zn) - 1);
					bsLiveShadow -= zn;
					while (zvec > limit_zt[zn])
					{
						zn++;
						while (bsLiveShadow < 1)
						{
							int thech = readAByte(inShadow);
							if (thech >= 0)
							{
								bsBuffShadow = (bsBuffShadow << 8) | thech;
								bsLiveShadow += 8;
								continue;
							}
							else
							{
								throw new System.IO.IOException("unexpected end of stream");
							}
						}
						bsLiveShadow--;
						zvec = ((zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1));
					}
					nextSym = perm_zt[zvec - base_zt[zn]];
				}
			}
			this.last = lastShadow;
			this.bsLive = bsLiveShadow;
			this.bsBuff = bsBuffShadow;
		}

		/// <exception cref="System.IO.IOException"/>
		private int getAndMoveToFrontDecode0(int groupNo)
		{
			java.io.InputStream inShadow = this.@in;
			org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.Data dataShadow = this.data;
			int zt = dataShadow.selector[groupNo] & unchecked((int)(0xff));
			int[] limit_zt = dataShadow.limit[zt];
			int zn = dataShadow.minLens[zt];
			int zvec = (int)bsR(zn);
			int bsLiveShadow = (int)this.bsLive;
			int bsBuffShadow = (int)this.bsBuff;
			while (zvec > limit_zt[zn])
			{
				zn++;
				while (bsLiveShadow < 1)
				{
					int thech = readAByte(inShadow);
					if (thech >= 0)
					{
						bsBuffShadow = (bsBuffShadow << 8) | thech;
						bsLiveShadow += 8;
						continue;
					}
					else
					{
						throw new System.IO.IOException("unexpected end of stream");
					}
				}
				bsLiveShadow--;
				zvec = (zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1);
			}
			this.bsLive = bsLiveShadow;
			this.bsBuff = bsBuffShadow;
			return dataShadow.perm[zt][zvec - dataShadow.@base[zt][zn]];
		}

		/// <exception cref="System.IO.IOException"/>
		private void setupBlock()
		{
			if (this.data == null)
			{
				return;
			}
			int[] cftab = this.data.cftab;
			int[] tt = this.data.initTT(this.last + 1);
			byte[] ll8 = this.data.ll8;
			cftab[0] = 0;
			System.Array.Copy(this.data.unzftab, 0, cftab, 1, 256);
			for (int i = 1; i <= 256; i++)
			{
				c += cftab[i];
				cftab[i] = c;
			}
			for (int i_1 = 0; i_1 <= lastShadow; i_1++)
			{
				tt[cftab[ll8[i_1] & unchecked((int)(0xff))]++] = i_1;
			}
			if ((this.origPtr < 0) || (this.origPtr >= tt.Length))
			{
				throw new System.IO.IOException("stream corrupted");
			}
			this.su_tPos = tt[this.origPtr];
			this.su_count = 0;
			this.su_i2 = 0;
			this.su_ch2 = 256;
			/* not a char and not EOF */
			if (this.blockRandomised)
			{
				this.su_rNToGo = 0;
				this.su_rTPos = 0;
				setupRandPartA();
			}
			else
			{
				setupNoRandPartA();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void setupRandPartA()
		{
			if (this.su_i2 <= this.last)
			{
				this.su_chPrev = this.su_ch2;
				int su_ch2Shadow = this.data.ll8[this.su_tPos] & unchecked((int)(0xff));
				this.su_tPos = this.data.tt[this.su_tPos];
				if (this.su_rNToGo == 0)
				{
					this.su_rNToGo = org.apache.hadoop.io.compress.bzip2.BZip2Constants.rNums[this.su_rTPos
						] - 1;
					if (++this.su_rTPos == 512)
					{
						this.su_rTPos = 0;
					}
				}
				else
				{
					this.su_rNToGo--;
				}
				this.su_ch2 = su_ch2Shadow ^= (this.su_rNToGo == 1) ? 1 : 0;
				this.su_i2++;
				this.currentChar = su_ch2Shadow;
				this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.RAND_PART_B_STATE;
				this.crc.updateCRC(su_ch2Shadow);
			}
			else
			{
				endBlock();
				if (readMode == org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE
					.CONTINUOUS)
				{
					initBlock();
					setupBlock();
				}
				else
				{
					if (readMode == org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE
						.BYBLOCK)
					{
						this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.NO_PROCESS_STATE;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void setupNoRandPartA()
		{
			if (this.su_i2 <= this.last)
			{
				this.su_chPrev = this.su_ch2;
				int su_ch2Shadow = this.data.ll8[this.su_tPos] & unchecked((int)(0xff));
				this.su_ch2 = su_ch2Shadow;
				this.su_tPos = this.data.tt[this.su_tPos];
				this.su_i2++;
				this.currentChar = su_ch2Shadow;
				this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.NO_RAND_PART_B_STATE;
				this.crc.updateCRC(su_ch2Shadow);
			}
			else
			{
				this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.NO_RAND_PART_A_STATE;
				endBlock();
				if (readMode == org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE
					.CONTINUOUS)
				{
					initBlock();
					setupBlock();
				}
				else
				{
					if (readMode == org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE
						.BYBLOCK)
					{
						this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.NO_PROCESS_STATE;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void setupRandPartB()
		{
			if (this.su_ch2 != this.su_chPrev)
			{
				this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.RAND_PART_A_STATE;
				this.su_count = 1;
				setupRandPartA();
			}
			else
			{
				if (++this.su_count >= 4)
				{
					this.su_z = (char)(this.data.ll8[this.su_tPos] & unchecked((int)(0xff)));
					this.su_tPos = this.data.tt[this.su_tPos];
					if (this.su_rNToGo == 0)
					{
						this.su_rNToGo = org.apache.hadoop.io.compress.bzip2.BZip2Constants.rNums[this.su_rTPos
							] - 1;
						if (++this.su_rTPos == 512)
						{
							this.su_rTPos = 0;
						}
					}
					else
					{
						this.su_rNToGo--;
					}
					this.su_j2 = 0;
					this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.RAND_PART_C_STATE;
					if (this.su_rNToGo == 1)
					{
						this.su_z ^= (char)1;
					}
					setupRandPartC();
				}
				else
				{
					this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.RAND_PART_A_STATE;
					setupRandPartA();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void setupRandPartC()
		{
			if (this.su_j2 < this.su_z)
			{
				this.currentChar = this.su_ch2;
				this.crc.updateCRC(this.su_ch2);
				this.su_j2++;
			}
			else
			{
				this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.RAND_PART_A_STATE;
				this.su_i2++;
				this.su_count = 0;
				setupRandPartA();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void setupNoRandPartB()
		{
			if (this.su_ch2 != this.su_chPrev)
			{
				this.su_count = 1;
				setupNoRandPartA();
			}
			else
			{
				if (++this.su_count >= 4)
				{
					this.su_z = (char)(this.data.ll8[this.su_tPos] & unchecked((int)(0xff)));
					this.su_tPos = this.data.tt[this.su_tPos];
					this.su_j2 = 0;
					setupNoRandPartC();
				}
				else
				{
					setupNoRandPartA();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void setupNoRandPartC()
		{
			if (this.su_j2 < this.su_z)
			{
				int su_ch2Shadow = this.su_ch2;
				this.currentChar = su_ch2Shadow;
				this.crc.updateCRC(su_ch2Shadow);
				this.su_j2++;
				this.currentState = org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.STATE.NO_RAND_PART_C_STATE;
			}
			else
			{
				this.su_i2++;
				this.su_count = 0;
				setupNoRandPartA();
			}
		}

		private sealed class Data : object
		{
			internal readonly bool[] inUse = new bool[256];

			internal readonly byte[] seqToUnseq = new byte[256];

			internal readonly byte[] selector = new byte[MAX_SELECTORS];

			internal readonly byte[] selectorMtf = new byte[MAX_SELECTORS];

			/// <summary>
			/// Freq table collected to save a pass over the data during
			/// decompression.
			/// </summary>
			internal readonly int[] unzftab = new int[256];

			internal readonly int[][] limit = new int[][] { new int[MAX_ALPHA_SIZE], new int[
				MAX_ALPHA_SIZE], new int[MAX_ALPHA_SIZE], new int[MAX_ALPHA_SIZE], new int[MAX_ALPHA_SIZE
				], new int[MAX_ALPHA_SIZE] };

			internal readonly int[][] @base = new int[][] { new int[MAX_ALPHA_SIZE], new int[
				MAX_ALPHA_SIZE], new int[MAX_ALPHA_SIZE], new int[MAX_ALPHA_SIZE], new int[MAX_ALPHA_SIZE
				], new int[MAX_ALPHA_SIZE] };

			internal readonly int[][] perm = new int[][] { new int[MAX_ALPHA_SIZE], new int[MAX_ALPHA_SIZE
				], new int[MAX_ALPHA_SIZE], new int[MAX_ALPHA_SIZE], new int[MAX_ALPHA_SIZE], new 
				int[MAX_ALPHA_SIZE] };

			internal readonly int[] minLens = new int[N_GROUPS];

			internal readonly int[] cftab = new int[257];

			internal readonly char[] getAndMoveToFrontDecode_yy = new char[256];

			internal readonly char[][] temp_charArray2d = new char[][] { new char[MAX_ALPHA_SIZE
				], new char[MAX_ALPHA_SIZE], new char[MAX_ALPHA_SIZE], new char[MAX_ALPHA_SIZE], 
				new char[MAX_ALPHA_SIZE], new char[MAX_ALPHA_SIZE] };

			internal readonly byte[] recvDecodingTables_pos = new byte[N_GROUPS];

			internal int[] tt;

			internal byte[] ll8;

			internal Data(int blockSize100k)
				: base()
			{
				// (with blockSize 900k)
				// 256 byte
				// 256 byte
				// 18002 byte
				// 18002 byte
				// 1024 byte
				// 6192 byte
				// 6192 byte
				// 6192 byte
				// 24 byte
				// 1028 byte
				// 512 byte
				// 3096
				// byte
				// 6 byte
				// ---------------
				// 60798 byte
				// 3600000 byte
				// 900000 byte
				// ---------------
				// 4560782 byte
				// ===============
				this.ll8 = new byte[blockSize100k * org.apache.hadoop.io.compress.bzip2.BZip2Constants
					.baseBlockSize];
			}

			/// <summary>
			/// Initializes the
			/// <see cref="tt"/>
			/// array.
			/// This method is called when the required length of the array is known.
			/// I don't initialize it at construction time to avoid unneccessary
			/// memory allocation when compressing small files.
			/// </summary>
			internal int[] initTT(int length)
			{
				int[] ttShadow = this.tt;
				// tt.length should always be >= length, but theoretically
				// it can happen, if the compressor mixed small and large
				// blocks. Normally only the last block will be smaller
				// than others.
				if ((ttShadow == null) || (ttShadow.Length < length))
				{
					this.tt = ttShadow = new int[length];
				}
				return ttShadow;
			}
		}
	}
}
