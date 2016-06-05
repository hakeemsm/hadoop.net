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
using ICSharpCode.SharpZipLib;
using ICSharpCode.SharpZipLib.Zip.Compression;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.Compress.Zlib
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.IO.Compress.Decompressor"/>
	/// based on the popular gzip compressed file format.
	/// http://www.gzip.org/
	/// </summary>
	public class BuiltInGzipDecompressor : Decompressor
	{
		private const int GzipMagicId = unchecked((int)(0x8b1f));

		private const int GzipDeflateMethod = 8;

		private const int GzipFlagbitHeaderCrc = unchecked((int)(0x02));

		private const int GzipFlagbitExtraField = unchecked((int)(0x04));

		private const int GzipFlagbitFilename = unchecked((int)(0x08));

		private const int GzipFlagbitComment = unchecked((int)(0x10));

		private const int GzipFlagbitsReserved = unchecked((int)(0xe0));

		private Inflater inflater = new Inflater(true);

		private byte[] userBuf = null;

		private int userBufOff = 0;

		private int userBufLen = 0;

		private byte[] localBuf = new byte[256];

		private int localBufOff = 0;

		private int headerBytesRead = 0;

		private int trailerBytesRead = 0;

		private int numExtraFieldBytesRemaining = -1;

		private Checksum crc = DataChecksum.NewCrc32();

		private bool hasExtraField = false;

		private bool hasFilename = false;

		private bool hasComment = false;

		private bool hasHeaderCRC = false;

		private BuiltInGzipDecompressor.GzipStateLabel state;

		/// <summary>The current state of the gzip decoder, external to the Inflater context.
		/// 	</summary>
		/// <remarks>
		/// The current state of the gzip decoder, external to the Inflater context.
		/// (Technically, the private variables localBuf through hasHeaderCRC are
		/// also part of the state, so this enum is merely the label for it.)
		/// </remarks>
		private enum GzipStateLabel
		{
			HeaderBasic,
			HeaderExtraField,
			HeaderFilename,
			HeaderComment,
			HeaderCrc,
			DeflateStream,
			TrailerCrc,
			TrailerSize,
			Finished
		}

		/// <summary>Creates a new (pure Java) gzip decompressor.</summary>
		public BuiltInGzipDecompressor()
		{
			// if read as LE short int
			// 'true' (nowrap) => Inflater will handle raw deflate stream only
			state = BuiltInGzipDecompressor.GzipStateLabel.HeaderBasic;
			crc.Reset();
		}

		// FIXME? Inflater docs say:  'it is also necessary to provide an extra
		//        "dummy" byte as input. This is required by the ZLIB native
		//        library in order to support certain optimizations.'  However,
		//        this does not appear to be true, and in any case, it's not
		//        entirely clear where the byte should go or what its value
		//        should be.  Perhaps it suffices to have some deflated bytes
		//        in the first buffer load?  (But how else would one do it?)
		public virtual bool NeedsInput()
		{
			lock (this)
			{
				if (state == BuiltInGzipDecompressor.GzipStateLabel.DeflateStream)
				{
					// most common case
					return inflater.IsNeedingInput;
				}
				// see userBufLen comment at top of decompress(); currently no need to
				// verify userBufLen <= 0
				return (state != BuiltInGzipDecompressor.GzipStateLabel.Finished);
			}
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual void SetInput(byte[] b, int off, int len)
		{
			lock (this)
			{
				/*
				* In our case, the input data includes both gzip header/trailer bytes (which
				* we handle in executeState()) and deflate-stream bytes (which we hand off
				* to Inflater).
				*
				* NOTE:  This code assumes the data passed in via b[] remains unmodified
				*        until _we_ signal that it's safe to modify it (via needsInput()).
				*        The alternative would require an additional buffer-copy even for
				*        the bulk deflate stream, which is a performance hit we don't want
				*        to absorb.  (Decompressor now documents this requirement.)
				*/
				if (b == null)
				{
					throw new ArgumentNullException();
				}
				if (off < 0 || len < 0 || off > b.Length - len)
				{
					throw new IndexOutOfRangeException();
				}
				userBuf = b;
				userBufOff = off;
				userBufLen = len;
			}
		}

		// note:  might be zero
		/// <summary>
		/// Decompress the data (gzip header, deflate stream, gzip trailer) in the
		/// provided buffer.
		/// </summary>
		/// <returns>the number of decompressed bytes placed into b</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int Decompress(byte[] b, int off, int len)
		{
			lock (this)
			{
				/* From the caller's perspective, this is where the state machine lives.
				* The code is written such that we never return from decompress() with
				* data remaining in userBuf unless we're in FINISHED state and there was
				* data beyond the current gzip member (e.g., we're within a concatenated
				* gzip stream).  If this ever changes, {@link #needsInput()} will also
				* need to be modified (i.e., uncomment the userBufLen condition).
				*
				* The actual deflate-stream processing (decompression) is handled by
				* Java's Inflater class.  Unlike the gzip header/trailer code (execute*
				* methods below), the deflate stream is never copied; Inflater operates
				* directly on the user's buffer.
				*/
				int numAvailBytes = 0;
				if (state != BuiltInGzipDecompressor.GzipStateLabel.DeflateStream)
				{
					ExecuteHeaderState();
					if (userBufLen <= 0)
					{
						return numAvailBytes;
					}
				}
				// "executeDeflateStreamState()"
				if (state == BuiltInGzipDecompressor.GzipStateLabel.DeflateStream)
				{
					// hand off user data (or what's left of it) to Inflater--but note that
					// Inflater may not have consumed all of previous bufferload (e.g., if
					// data highly compressed or output buffer very small), in which case
					// userBufLen will be zero
					if (userBufLen > 0)
					{
						inflater.SetInput(userBuf, userBufOff, userBufLen);
						userBufOff += userBufLen;
						userBufLen = 0;
					}
					// now decompress it into b[]
					try
					{
						numAvailBytes = inflater.Inflate(b, off, len);
					}
					catch (SharpZipBaseException dfe)
					{
						throw new IOException(dfe.Message);
					}
					crc.Update(b, off, numAvailBytes);
					// CRC-32 is on _uncompressed_ data
					if (inflater.IsFinished)
					{
						state = BuiltInGzipDecompressor.GzipStateLabel.TrailerCrc;
						int bytesRemaining = inflater.RemainingInput;
						System.Diagnostics.Debug.Assert((bytesRemaining >= 0), "logic error: Inflater finished; byte-count is inconsistent"
							);
						// could save a copy of userBufLen at call to inflater.setInput() and
						// verify that bytesRemaining <= origUserBufLen, but would have to
						// be a (class) member variable...seems excessive for a sanity check
						userBufOff -= bytesRemaining;
						userBufLen = bytesRemaining;
					}
					else
					{
						// or "+=", but guaranteed 0 coming in
						return numAvailBytes;
					}
				}
				// minor optimization
				ExecuteTrailerState();
				return numAvailBytes;
			}
		}

		/// <summary>Parse the gzip header (assuming we're in the appropriate state).</summary>
		/// <remarks>
		/// Parse the gzip header (assuming we're in the appropriate state).
		/// In order to deal with degenerate cases (e.g., user buffer is one byte
		/// long), we copy (some) header bytes to another buffer.  (Filename,
		/// comment, and extra-field bytes are simply skipped.)</p>
		/// See http://www.ietf.org/rfc/rfc1952.txt for the gzip spec.  Note that
		/// no version of gzip to date (at least through 1.4.0, 2010-01-20) supports
		/// the FHCRC header-CRC16 flagbit; instead, the implementation treats it
		/// as a multi-file continuation flag (which it also doesn't support). :-(
		/// Sun's JDK v6 (1.6) supports the header CRC, however, and so do we.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void ExecuteHeaderState()
		{
			// this can happen because DecompressorStream's decompress() is written
			// to call decompress() first, setInput() second:
			if (userBufLen <= 0)
			{
				return;
			}
			// "basic"/required header:  somewhere in first 10 bytes
			if (state == BuiltInGzipDecompressor.GzipStateLabel.HeaderBasic)
			{
				int n = Math.Min(userBufLen, 10 - localBufOff);
				// (or 10-headerBytesRead)
				CheckAndCopyBytesToLocal(n);
				// modifies userBufLen, etc.
				if (localBufOff >= 10)
				{
					// should be strictly ==
					ProcessBasicHeader();
					// sig, compression method, flagbits
					localBufOff = 0;
					// no further need for basic header
					state = BuiltInGzipDecompressor.GzipStateLabel.HeaderExtraField;
				}
			}
			if (userBufLen <= 0)
			{
				return;
			}
			// optional header stuff (extra field, filename, comment, header CRC)
			if (state == BuiltInGzipDecompressor.GzipStateLabel.HeaderExtraField)
			{
				if (hasExtraField)
				{
					// 2 substates:  waiting for 2 bytes => get numExtraFieldBytesRemaining,
					// or already have 2 bytes & waiting to finish skipping specified length
					if (numExtraFieldBytesRemaining < 0)
					{
						int n = Math.Min(userBufLen, 2 - localBufOff);
						CheckAndCopyBytesToLocal(n);
						if (localBufOff >= 2)
						{
							numExtraFieldBytesRemaining = ReadUShortLE(localBuf, 0);
							localBufOff = 0;
						}
					}
					if (numExtraFieldBytesRemaining > 0 && userBufLen > 0)
					{
						int n = Math.Min(userBufLen, numExtraFieldBytesRemaining);
						CheckAndSkipBytes(n);
						// modifies userBufLen, etc.
						numExtraFieldBytesRemaining -= n;
					}
					if (numExtraFieldBytesRemaining == 0)
					{
						state = BuiltInGzipDecompressor.GzipStateLabel.HeaderFilename;
					}
				}
				else
				{
					state = BuiltInGzipDecompressor.GzipStateLabel.HeaderFilename;
				}
			}
			if (userBufLen <= 0)
			{
				return;
			}
			if (state == BuiltInGzipDecompressor.GzipStateLabel.HeaderFilename)
			{
				if (hasFilename)
				{
					bool doneWithFilename = CheckAndSkipBytesUntilNull();
					if (!doneWithFilename)
					{
						return;
					}
				}
				// exit early:  used up entire buffer without hitting NULL
				state = BuiltInGzipDecompressor.GzipStateLabel.HeaderComment;
			}
			if (userBufLen <= 0)
			{
				return;
			}
			if (state == BuiltInGzipDecompressor.GzipStateLabel.HeaderComment)
			{
				if (hasComment)
				{
					bool doneWithComment = CheckAndSkipBytesUntilNull();
					if (!doneWithComment)
					{
						return;
					}
				}
				// exit early:  used up entire buffer
				state = BuiltInGzipDecompressor.GzipStateLabel.HeaderCrc;
			}
			if (userBufLen <= 0)
			{
				return;
			}
			if (state == BuiltInGzipDecompressor.GzipStateLabel.HeaderCrc)
			{
				if (hasHeaderCRC)
				{
					System.Diagnostics.Debug.Assert((localBufOff < 2));
					int n = Math.Min(userBufLen, 2 - localBufOff);
					CopyBytesToLocal(n);
					if (localBufOff >= 2)
					{
						long headerCRC = ReadUShortLE(localBuf, 0);
						if (headerCRC != (crc.GetValue() & unchecked((int)(0xffff))))
						{
							throw new IOException("gzip header CRC failure");
						}
						localBufOff = 0;
						crc.Reset();
						state = BuiltInGzipDecompressor.GzipStateLabel.DeflateStream;
					}
				}
				else
				{
					crc.Reset();
					// will reuse for CRC-32 of uncompressed data
					state = BuiltInGzipDecompressor.GzipStateLabel.DeflateStream;
				}
			}
		}

		// switching to Inflater now
		/// <summary>Parse the gzip trailer (assuming we're in the appropriate state).</summary>
		/// <remarks>
		/// Parse the gzip trailer (assuming we're in the appropriate state).
		/// In order to deal with degenerate cases (e.g., user buffer is one byte
		/// long), we copy trailer bytes (all 8 of 'em) to a local buffer.</p>
		/// See http://www.ietf.org/rfc/rfc1952.txt for the gzip spec.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void ExecuteTrailerState()
		{
			if (userBufLen <= 0)
			{
				return;
			}
			// verify that the CRC-32 of the decompressed stream matches the value
			// stored in the gzip trailer
			if (state == BuiltInGzipDecompressor.GzipStateLabel.TrailerCrc)
			{
				// localBuf was empty before we handed off to Inflater, so we handle this
				// exactly like header fields
				System.Diagnostics.Debug.Assert((localBufOff < 4));
				// initially 0, but may need multiple calls
				int n = Math.Min(userBufLen, 4 - localBufOff);
				CopyBytesToLocal(n);
				if (localBufOff >= 4)
				{
					long streamCRC = ReadUIntLE(localBuf, 0);
					if (streamCRC != crc.GetValue())
					{
						throw new IOException("gzip stream CRC failure");
					}
					localBufOff = 0;
					crc.Reset();
					state = BuiltInGzipDecompressor.GzipStateLabel.TrailerSize;
				}
			}
			if (userBufLen <= 0)
			{
				return;
			}
			// verify that the mod-2^32 decompressed stream size matches the value
			// stored in the gzip trailer
			if (state == BuiltInGzipDecompressor.GzipStateLabel.TrailerSize)
			{
				System.Diagnostics.Debug.Assert((localBufOff < 4));
				// initially 0, but may need multiple calls
				int n = Math.Min(userBufLen, 4 - localBufOff);
				CopyBytesToLocal(n);
				// modifies userBufLen, etc.
				if (localBufOff >= 4)
				{
					// should be strictly ==
					long inputSize = ReadUIntLE(localBuf, 0);
					if (inputSize != (inflater.GetBytesWritten() & unchecked((long)(0xffffffffL))))
					{
						throw new IOException("stored gzip size doesn't match decompressed size");
					}
					localBufOff = 0;
					state = BuiltInGzipDecompressor.GzipStateLabel.Finished;
				}
			}
			if (state == BuiltInGzipDecompressor.GzipStateLabel.Finished)
			{
				return;
			}
		}

		/// <summary>
		/// Returns the total number of compressed bytes input so far, including
		/// gzip header/trailer bytes.</p>
		/// </summary>
		/// <returns>the total (non-negative) number of compressed bytes read so far</returns>
		public virtual long GetBytesRead()
		{
			lock (this)
			{
				return headerBytesRead + inflater.TotalIn + trailerBytesRead;
			}
		}

		/// <summary>
		/// Returns the number of bytes remaining in the input buffer; normally
		/// called when finished() is true to determine amount of post-gzip-stream
		/// data.
		/// </summary>
		/// <remarks>
		/// Returns the number of bytes remaining in the input buffer; normally
		/// called when finished() is true to determine amount of post-gzip-stream
		/// data.  Note that, other than the finished state with concatenated data
		/// after the end of the current gzip stream, this will never return a
		/// non-zero value unless called after
		/// <see cref="SetInput(byte[], int, int)"/>
		/// and before
		/// <see cref="Decompress(byte[], int, int)"/>
		/// .
		/// (That is, after
		/// <see cref="Decompress(byte[], int, int)"/>
		/// it
		/// always returns zero, except in finished state with concatenated data.)</p>
		/// </remarks>
		/// <returns>the total (non-negative) number of unprocessed bytes in input</returns>
		public virtual int GetRemaining()
		{
			lock (this)
			{
				return userBufLen;
			}
		}

		public virtual bool NeedsDictionary()
		{
			lock (this)
			{
				return inflater.NeedsDictionary();
			}
		}

		public virtual void SetDictionary(byte[] b, int off, int len)
		{
			lock (this)
			{
				inflater.SetDictionary(b, off, len);
			}
		}

		/// <summary>
		/// Returns true if the end of the gzip substream (single "member") has been
		/// reached.</p>
		/// </summary>
		public virtual bool Finished()
		{
			lock (this)
			{
				return (state == BuiltInGzipDecompressor.GzipStateLabel.Finished);
			}
		}

		/// <summary>
		/// Resets everything, including the input buffer, regardless of whether the
		/// current gzip substream is finished.</p>
		/// </summary>
		public virtual void Reset()
		{
			lock (this)
			{
				// could optionally emit INFO message if state != GzipStateLabel.FINISHED
				inflater.Reset();
				state = BuiltInGzipDecompressor.GzipStateLabel.HeaderBasic;
				crc.Reset();
				userBufOff = userBufLen = 0;
				localBufOff = 0;
				headerBytesRead = 0;
				trailerBytesRead = 0;
				numExtraFieldBytesRemaining = -1;
				hasExtraField = false;
				hasFilename = false;
				hasComment = false;
				hasHeaderCRC = false;
			}
		}

		public virtual void End()
		{
			lock (this)
			{
				inflater.Finish();
			}
		}

		/// <summary>
		/// Check ID bytes (throw if necessary), compression method (throw if not 8),
		/// and flag bits (set hasExtraField, hasFilename, hasComment, hasHeaderCRC).
		/// </summary>
		/// <remarks>
		/// Check ID bytes (throw if necessary), compression method (throw if not 8),
		/// and flag bits (set hasExtraField, hasFilename, hasComment, hasHeaderCRC).
		/// Ignore MTIME, XFL, OS.  Caller must ensure we have at least 10 bytes (at
		/// the start of localBuf).</p>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void ProcessBasicHeader()
		{
			/*
			* Flag bits (remainder are reserved and must be zero):
			*   bit 0   FTEXT
			*   bit 1   FHCRC   (never implemented in gzip, at least through version
			*                   1.4.0; instead interpreted as "continuation of multi-
			*                   part gzip file," which is unsupported through 1.4.0)
			*   bit 2   FEXTRA
			*   bit 3   FNAME
			*   bit 4   FCOMMENT
			*  [bit 5   encrypted]
			*/
			if (ReadUShortLE(localBuf, 0) != GzipMagicId)
			{
				throw new IOException("not a gzip file");
			}
			if (ReadUByte(localBuf, 2) != GzipDeflateMethod)
			{
				throw new IOException("gzip data not compressed with deflate method");
			}
			int flg = ReadUByte(localBuf, 3);
			if ((flg & GzipFlagbitsReserved) != 0)
			{
				throw new IOException("unknown gzip format (reserved flagbits set)");
			}
			hasExtraField = ((flg & GzipFlagbitExtraField) != 0);
			hasFilename = ((flg & GzipFlagbitFilename) != 0);
			hasComment = ((flg & GzipFlagbitComment) != 0);
			hasHeaderCRC = ((flg & GzipFlagbitHeaderCrc) != 0);
		}

		private void CheckAndCopyBytesToLocal(int len)
		{
			System.Array.Copy(userBuf, userBufOff, localBuf, localBufOff, len);
			localBufOff += len;
			// alternatively, could call checkAndSkipBytes(len) for rest...
			crc.Update(userBuf, userBufOff, len);
			userBufOff += len;
			userBufLen -= len;
			headerBytesRead += len;
		}

		private void CheckAndSkipBytes(int len)
		{
			crc.Update(userBuf, userBufOff, len);
			userBufOff += len;
			userBufLen -= len;
			headerBytesRead += len;
		}

		// returns true if saw NULL, false if ran out of buffer first; called _only_
		// during gzip-header processing (not trailer)
		// (caller can check before/after state of userBufLen to compute num bytes)
		private bool CheckAndSkipBytesUntilNull()
		{
			bool hitNull = false;
			if (userBufLen > 0)
			{
				do
				{
					hitNull = (userBuf[userBufOff] == 0);
					crc.Update(userBuf[userBufOff]);
					++userBufOff;
					--userBufLen;
					++headerBytesRead;
				}
				while (userBufLen > 0 && !hitNull);
			}
			return hitNull;
		}

		// this one doesn't update the CRC and does support trailer processing but
		// otherwise is same as its "checkAnd" sibling
		private void CopyBytesToLocal(int len)
		{
			System.Array.Copy(userBuf, userBufOff, localBuf, localBufOff, len);
			localBufOff += len;
			userBufOff += len;
			userBufLen -= len;
			if (state == BuiltInGzipDecompressor.GzipStateLabel.TrailerCrc || state == BuiltInGzipDecompressor.GzipStateLabel
				.TrailerSize)
			{
				trailerBytesRead += len;
			}
			else
			{
				headerBytesRead += len;
			}
		}

		private int ReadUByte(byte[] b, int off)
		{
			return ((int)b[off] & unchecked((int)(0xff)));
		}

		// caller is responsible for not overrunning buffer
		private int ReadUShortLE(byte[] b, int off)
		{
			return ((((b[off + 1] & unchecked((int)(0xff))) << 8) | ((b[off] & unchecked((int
				)(0xff))))) & unchecked((int)(0xffff)));
		}

		// caller is responsible for not overrunning buffer
		private long ReadUIntLE(byte[] b, int off)
		{
			return ((((long)(b[off + 3] & unchecked((int)(0xff))) << 24) | ((long)(b[off + 2]
				 & unchecked((int)(0xff))) << 16) | ((long)(b[off + 1] & unchecked((int)(0xff)))
				 << 8) | ((long)(b[off] & unchecked((int)(0xff))))) & unchecked((long)(0xffffffffL
				)));
		}
	}
}
