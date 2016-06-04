using System;
using System.IO;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress.Bzip2;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	/// <summary>
	/// This class provides output and input streams for bzip2 compression
	/// and decompression.
	/// </summary>
	/// <remarks>
	/// This class provides output and input streams for bzip2 compression
	/// and decompression.  It uses the native bzip2 library on the system
	/// if possible, else it uses a pure-Java implementation of the bzip2
	/// algorithm.  The configuration parameter
	/// io.compression.codec.bzip2.library can be used to control this
	/// behavior.
	/// In the pure-Java mode, the Compressor and Decompressor interfaces
	/// are not implemented.  Therefore, in that mode, those methods of
	/// CompressionCodec which have a Compressor or Decompressor type
	/// argument, throw UnsupportedOperationException.
	/// Currently, support for splittability is available only in the
	/// pure-Java mode; therefore, if a SplitCompressionInputStream is
	/// requested, the pure-Java implementation is used, regardless of the
	/// setting of the configuration parameter mentioned above.
	/// </remarks>
	public class BZip2Codec : Configurable, SplittableCompressionCodec
	{
		private const string Header = "BZ";

		private static readonly int HeaderLen = Header.Length;

		private const string SubHeader = "h9";

		private static readonly int SubHeaderLen = SubHeader.Length;

		private Configuration conf;

		/// <summary>Set the configuration to be used by this object.</summary>
		/// <param name="conf">the configuration object.</param>
		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		/// <summary>Return the configuration used by this object.</summary>
		/// <returns>the configuration object used by this objec.</returns>
		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>Creates a new instance of BZip2Codec.</summary>
		public BZip2Codec()
		{
		}

		/// <summary>
		/// Create a
		/// <see cref="CompressionOutputStream"/>
		/// that will write to the given
		/// <see cref="System.IO.OutputStream"/>
		/// .
		/// </summary>
		/// <param name="out">the location for the final output stream</param>
		/// <returns>
		/// a stream the user can write uncompressed data to, to have it
		/// compressed
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual CompressionOutputStream CreateOutputStream(OutputStream @out)
		{
			return CompressionCodec.Util.CreateOutputStreamWithCodecPool(this, conf, @out);
		}

		/// <summary>
		/// Create a
		/// <see cref="CompressionOutputStream"/>
		/// that will write to the given
		/// <see cref="System.IO.OutputStream"/>
		/// with the given
		/// <see cref="Compressor"/>
		/// .
		/// </summary>
		/// <param name="out">the location for the final output stream</param>
		/// <param name="compressor">compressor to use</param>
		/// <returns>
		/// a stream the user can write uncompressed data to, to have it
		/// compressed
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual CompressionOutputStream CreateOutputStream(OutputStream @out, Compressor
			 compressor)
		{
			return Bzip2Factory.IsNativeBzip2Loaded(conf) ? new CompressorStream(@out, compressor
				, conf.GetInt("io.file.buffer.size", 4 * 1024)) : new BZip2Codec.BZip2CompressionOutputStream
				(@out);
		}

		/// <summary>
		/// Get the type of
		/// <see cref="Compressor"/>
		/// needed by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>the type of compressor needed by this codec.</returns>
		public virtual Type GetCompressorType()
		{
			return Bzip2Factory.GetBzip2CompressorType(conf);
		}

		/// <summary>
		/// Create a new
		/// <see cref="Compressor"/>
		/// for use by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>a new compressor for use by this codec</returns>
		public virtual Compressor CreateCompressor()
		{
			return Bzip2Factory.GetBzip2Compressor(conf);
		}

		/// <summary>
		/// Create a
		/// <see cref="CompressionInputStream"/>
		/// that will read from the given
		/// input stream and return a stream for uncompressed data.
		/// </summary>
		/// <param name="in">the stream to read compressed bytes from</param>
		/// <returns>a stream to read uncompressed bytes from</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual CompressionInputStream CreateInputStream(InputStream @in)
		{
			return CompressionCodec.Util.CreateInputStreamWithCodecPool(this, conf, @in);
		}

		/// <summary>
		/// Create a
		/// <see cref="CompressionInputStream"/>
		/// that will read from the given
		/// <see cref="System.IO.InputStream"/>
		/// with the given
		/// <see cref="Decompressor"/>
		/// , and return a
		/// stream for uncompressed data.
		/// </summary>
		/// <param name="in">the stream to read compressed bytes from</param>
		/// <param name="decompressor">decompressor to use</param>
		/// <returns>a stream to read uncompressed bytes from</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual CompressionInputStream CreateInputStream(InputStream @in, Decompressor
			 decompressor)
		{
			return Bzip2Factory.IsNativeBzip2Loaded(conf) ? new DecompressorStream(@in, decompressor
				, conf.GetInt("io.file.buffer.size", 4 * 1024)) : new BZip2Codec.BZip2CompressionInputStream
				(@in);
		}

		/// <summary>
		/// Creates CompressionInputStream to be used to read off uncompressed data
		/// in one of the two reading modes.
		/// </summary>
		/// <remarks>
		/// Creates CompressionInputStream to be used to read off uncompressed data
		/// in one of the two reading modes. i.e. Continuous or Blocked reading modes
		/// </remarks>
		/// <param name="seekableIn">The InputStream</param>
		/// <param name="start">The start offset into the compressed stream</param>
		/// <param name="end">The end offset into the compressed stream</param>
		/// <param name="readMode">
		/// Controls whether progress is reported continuously or
		/// only at block boundaries.
		/// </param>
		/// <returns>CompressionInputStream for BZip2 aligned at block boundaries</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual SplitCompressionInputStream CreateInputStream(InputStream seekableIn
			, Decompressor decompressor, long start, long end, SplittableCompressionCodec.READ_MODE
			 readMode)
		{
			if (!(seekableIn is Seekable))
			{
				throw new IOException("seekableIn must be an instance of " + typeof(Seekable).FullName
					);
			}
			//find the position of first BZip2 start up marker
			((Seekable)seekableIn).Seek(0);
			// BZip2 start of block markers are of 6 bytes.  But the very first block
			// also has "BZh9", making it 10 bytes.  This is the common case.  But at
			// time stream might start without a leading BZ.
			long FirstBzip2BlockMarkerPosition = CBZip2InputStream.NumberOfBytesTillNextMarker
				(seekableIn);
			long adjStart = Math.Max(0L, start - FirstBzip2BlockMarkerPosition);
			((Seekable)seekableIn).Seek(adjStart);
			SplitCompressionInputStream @in = new BZip2Codec.BZip2CompressionInputStream(seekableIn
				, adjStart, end, readMode);
			// The following if clause handles the following case:
			// Assume the following scenario in BZip2 compressed stream where
			// . represent compressed data.
			// .....[48 bit Block].....[48 bit   Block].....[48 bit Block]...
			// ........................[47 bits][1 bit].....[48 bit Block]...
			// ................................^[Assume a Byte alignment here]
			// ........................................^^[current position of stream]
			// .....................^^[We go back 10 Bytes in stream and find a Block marker]
			// ........................................^^[We align at wrong position!]
			// ...........................................................^^[While this pos is correct]
			if (@in.GetPos() < start)
			{
				((Seekable)seekableIn).Seek(start);
				@in = new BZip2Codec.BZip2CompressionInputStream(seekableIn, start, end, readMode
					);
			}
			return @in;
		}

		/// <summary>
		/// Get the type of
		/// <see cref="Decompressor"/>
		/// needed by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>the type of decompressor needed by this codec.</returns>
		public virtual Type GetDecompressorType()
		{
			return Bzip2Factory.GetBzip2DecompressorType(conf);
		}

		/// <summary>
		/// Create a new
		/// <see cref="Decompressor"/>
		/// for use by this
		/// <see cref="CompressionCodec"/>
		/// .
		/// </summary>
		/// <returns>a new decompressor for use by this codec</returns>
		public virtual Decompressor CreateDecompressor()
		{
			return Bzip2Factory.GetBzip2Decompressor(conf);
		}

		/// <summary>.bz2 is recognized as the default extension for compressed BZip2 files</summary>
		/// <returns>A String telling the default bzip2 file extension</returns>
		public virtual string GetDefaultExtension()
		{
			return ".bz2";
		}

		private class BZip2CompressionOutputStream : CompressionOutputStream
		{
			private CBZip2OutputStream output;

			private bool needsReset;

			/// <exception cref="System.IO.IOException"/>
			public BZip2CompressionOutputStream(OutputStream @out)
				: base(@out)
			{
				// class data starts here//
				// class data ends here//
				needsReset = true;
			}

			/// <exception cref="System.IO.IOException"/>
			private void WriteStreamHeader()
			{
				if (base.@out != null)
				{
					// The compressed bzip2 stream should start with the
					// identifying characters BZ. Caller of CBZip2OutputStream
					// i.e. this class must write these characters.
					@out.Write(Sharpen.Runtime.GetBytesForString(Header, Charsets.Utf8));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Finish()
			{
				if (needsReset)
				{
					// In the case that nothing is written to this stream, we still need to
					// write out the header before closing, otherwise the stream won't be
					// recognized by BZip2CompressionInputStream.
					InternalReset();
				}
				this.output.Finish();
				needsReset = true;
			}

			/// <exception cref="System.IO.IOException"/>
			private void InternalReset()
			{
				if (needsReset)
				{
					needsReset = false;
					WriteStreamHeader();
					this.output = new CBZip2OutputStream(@out);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ResetState()
			{
				// Cannot write to out at this point because out might not be ready
				// yet, as in SequenceFile.Writer implementation.
				needsReset = true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(int b)
			{
				if (needsReset)
				{
					InternalReset();
				}
				this.output.Write(b);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b, int off, int len)
			{
				if (needsReset)
				{
					InternalReset();
				}
				this.output.Write(b, off, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				if (needsReset)
				{
					// In the case that nothing is written to this stream, we still need to
					// write out the header before closing, otherwise the stream won't be
					// recognized by BZip2CompressionInputStream.
					InternalReset();
				}
				this.output.Flush();
				this.output.Close();
				needsReset = true;
			}
			// end of class BZip2CompressionOutputStream
		}

		/// <summary>
		/// This class is capable to de-compress BZip2 data in two modes;
		/// CONTINOUS and BYBLOCK.
		/// </summary>
		/// <remarks>
		/// This class is capable to de-compress BZip2 data in two modes;
		/// CONTINOUS and BYBLOCK.  BYBLOCK mode makes it possible to
		/// do decompression starting any arbitrary position in the stream.
		/// So this facility can easily be used to parallelize decompression
		/// of a large BZip2 file for performance reasons.  (It is exactly
		/// done so for Hadoop framework.  See LineRecordReader for an
		/// example).  So one can break the file (of course logically) into
		/// chunks for parallel processing.  These "splits" should be like
		/// default Hadoop splits (e.g as in FileInputFormat getSplit metod).
		/// So this code is designed and tested for FileInputFormat's way
		/// of splitting only.
		/// </remarks>
		private class BZip2CompressionInputStream : SplitCompressionInputStream
		{
			private CBZip2InputStream input;

			internal bool needsReset;

			private BufferedInputStream bufferedIn;

			private bool isHeaderStripped = false;

			private bool isSubHeaderStripped = false;

			private SplittableCompressionCodec.READ_MODE readMode = SplittableCompressionCodec.READ_MODE
				.Continuous;

			private long startingPos = 0L;

			private enum POS_ADVERTISEMENT_STATE_MACHINE
			{
				Hold,
				Advertise
			}

			internal BZip2Codec.BZip2CompressionInputStream.POS_ADVERTISEMENT_STATE_MACHINE posSM
				 = BZip2Codec.BZip2CompressionInputStream.POS_ADVERTISEMENT_STATE_MACHINE.Hold;

			internal long compressedStreamPosition = 0;

			/// <exception cref="System.IO.IOException"/>
			public BZip2CompressionInputStream(InputStream @in)
				: this(@in, 0L, long.MaxValue, SplittableCompressionCodec.READ_MODE.Continuous)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public BZip2CompressionInputStream(InputStream @in, long start, long end, SplittableCompressionCodec.READ_MODE
				 readMode)
				: base(@in, start, end)
			{
				// class data starts here//
				// Following state machine handles different states of compressed stream
				// position
				// HOLD : Don't advertise compressed stream position
				// ADVERTISE : Read 1 more character and advertise stream position
				// See more comments about it before updatePos method.
				// class data ends here//
				needsReset = false;
				bufferedIn = new BufferedInputStream(base.@in);
				this.startingPos = base.GetPos();
				this.readMode = readMode;
				if (this.startingPos == 0)
				{
					// We only strip header if it is start of file
					bufferedIn = ReadStreamHeader();
				}
				input = new CBZip2InputStream(bufferedIn, readMode);
				if (this.isHeaderStripped)
				{
					input.UpdateReportedByteCount(HeaderLen);
				}
				if (this.isSubHeaderStripped)
				{
					input.UpdateReportedByteCount(SubHeaderLen);
				}
				this.UpdatePos(false);
			}

			/// <exception cref="System.IO.IOException"/>
			private BufferedInputStream ReadStreamHeader()
			{
				// We are flexible enough to allow the compressed stream not to
				// start with the header of BZ. So it works fine either we have
				// the header or not.
				if (base.@in != null)
				{
					bufferedIn.Mark(HeaderLen);
					byte[] headerBytes = new byte[HeaderLen];
					int actualRead = bufferedIn.Read(headerBytes, 0, HeaderLen);
					if (actualRead != -1)
					{
						string header = new string(headerBytes, Charsets.Utf8);
						if (string.CompareOrdinal(header, Header) != 0)
						{
							bufferedIn.Reset();
						}
						else
						{
							this.isHeaderStripped = true;
							// In case of BYBLOCK mode, we also want to strip off
							// remaining two character of the header.
							if (this.readMode == SplittableCompressionCodec.READ_MODE.Byblock)
							{
								actualRead = bufferedIn.Read(headerBytes, 0, SubHeaderLen);
								if (actualRead != -1)
								{
									this.isSubHeaderStripped = true;
								}
							}
						}
					}
				}
				if (bufferedIn == null)
				{
					throw new IOException("Failed to read bzip2 stream.");
				}
				return bufferedIn;
			}

			// end of method
			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				if (!needsReset)
				{
					input.Close();
					needsReset = true;
				}
			}

			/// <summary>
			/// This method updates compressed stream position exactly when the
			/// client of this code has read off at least one byte passed any BZip2
			/// end of block marker.
			/// </summary>
			/// <remarks>
			/// This method updates compressed stream position exactly when the
			/// client of this code has read off at least one byte passed any BZip2
			/// end of block marker.
			/// This mechanism is very helpful to deal with data level record
			/// boundaries. Please see constructor and next methods of
			/// org.apache.hadoop.mapred.LineRecordReader as an example usage of this
			/// feature.  We elaborate it with an example in the following:
			/// Assume two different scenarios of the BZip2 compressed stream, where
			/// [m] represent end of block, \n is line delimiter and . represent compressed
			/// data.
			/// ............[m]......\n.......
			/// ..........\n[m]......\n.......
			/// Assume that end is right after [m].  In the first case the reading
			/// will stop at \n and there is no need to read one more line.  (To see the
			/// reason of reading one more line in the next() method is explained in LineRecordReader.)
			/// While in the second example LineRecordReader needs to read one more line
			/// (till the second \n).  Now since BZip2Codecs only update position
			/// at least one byte passed a maker, so it is straight forward to differentiate
			/// between the two cases mentioned.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b, int off, int len)
			{
				if (needsReset)
				{
					InternalReset();
				}
				int result = 0;
				result = this.input.Read(b, off, len);
				if (result == BZip2Constants.EndOfBlock)
				{
					this.posSM = BZip2Codec.BZip2CompressionInputStream.POS_ADVERTISEMENT_STATE_MACHINE
						.Advertise;
				}
				if (this.posSM == BZip2Codec.BZip2CompressionInputStream.POS_ADVERTISEMENT_STATE_MACHINE
					.Advertise)
				{
					result = this.input.Read(b, off, off + 1);
					// This is the precise time to update compressed stream position
					// to the client of this code.
					this.UpdatePos(true);
					this.posSM = BZip2Codec.BZip2CompressionInputStream.POS_ADVERTISEMENT_STATE_MACHINE
						.Hold;
				}
				return result;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				byte[] b = new byte[1];
				int result = this.Read(b, 0, 1);
				return (result < 0) ? result : (b[0] & unchecked((int)(0xff)));
			}

			/// <exception cref="System.IO.IOException"/>
			private void InternalReset()
			{
				if (needsReset)
				{
					needsReset = false;
					BufferedInputStream bufferedIn = ReadStreamHeader();
					input = new CBZip2InputStream(bufferedIn, this.readMode);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ResetState()
			{
				// Cannot read from bufferedIn at this point because bufferedIn
				// might not be ready
				// yet, as in SequenceFile.Reader implementation.
				needsReset = true;
			}

			public override long GetPos()
			{
				return this.compressedStreamPosition;
			}

			/*
			* As the comments before read method tell that
			* compressed stream is advertised when at least
			* one byte passed EOB have been read off.  But
			* there is an exception to this rule.  When we
			* construct the stream we advertise the position
			* exactly at EOB.  In the following method
			* shouldAddOn boolean captures this exception.
			*
			*/
			private void UpdatePos(bool shouldAddOn)
			{
				int addOn = shouldAddOn ? 1 : 0;
				this.compressedStreamPosition = this.startingPos + this.input.GetProcessedByteCount
					() + addOn;
			}
			// end of BZip2CompressionInputStream
		}
	}
}
