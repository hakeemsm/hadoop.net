using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Treats keys as offset in file and value as line.</summary>
	public class LineRecordReader : RecordReader<LongWritable, Text>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.LineRecordReader
			).FullName);

		private CompressionCodecFactory compressionCodecs = null;

		private long start;

		private long pos;

		private long end;

		private SplitLineReader @in;

		private FSDataInputStream fileIn;

		private readonly Seekable filePosition;

		internal int maxLineLength;

		private CompressionCodec codec;

		private Decompressor decompressor;

		/// <summary>A class that provides a line reader from an input stream.</summary>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Util.LineReader instead.")]
		public class LineReader : Org.Apache.Hadoop.Util.LineReader
		{
			internal LineReader(InputStream @in)
				: base(@in)
			{
			}

			internal LineReader(InputStream @in, int bufferSize)
				: base(@in, bufferSize)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public LineReader(InputStream @in, Configuration conf)
				: base(@in, conf)
			{
			}

			internal LineReader(InputStream @in, byte[] recordDelimiter)
				: base(@in, recordDelimiter)
			{
			}

			internal LineReader(InputStream @in, int bufferSize, byte[] recordDelimiter)
				: base(@in, bufferSize, recordDelimiter)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public LineReader(InputStream @in, Configuration conf, byte[] recordDelimiter)
				: base(@in, conf, recordDelimiter)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public LineRecordReader(Configuration job, FileSplit split)
			: this(job, split, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public LineRecordReader(Configuration job, FileSplit split, byte[] recordDelimiter
			)
		{
			this.maxLineLength = job.GetInt(LineRecordReader.MaxLineLength, int.MaxValue);
			start = split.GetStart();
			end = start + split.GetLength();
			Path file = split.GetPath();
			compressionCodecs = new CompressionCodecFactory(job);
			codec = compressionCodecs.GetCodec(file);
			// open the file and seek to the start of the split
			FileSystem fs = file.GetFileSystem(job);
			fileIn = fs.Open(file);
			if (IsCompressedInput())
			{
				decompressor = CodecPool.GetDecompressor(codec);
				if (codec is SplittableCompressionCodec)
				{
					SplitCompressionInputStream cIn = ((SplittableCompressionCodec)codec).CreateInputStream
						(fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.Byblock);
					@in = new CompressedSplitLineReader(cIn, job, recordDelimiter);
					start = cIn.GetAdjustedStart();
					end = cIn.GetAdjustedEnd();
					filePosition = cIn;
				}
				else
				{
					// take pos from compressed stream
					@in = new SplitLineReader(codec.CreateInputStream(fileIn, decompressor), job, recordDelimiter
						);
					filePosition = fileIn;
				}
			}
			else
			{
				fileIn.Seek(start);
				@in = new UncompressedSplitLineReader(fileIn, job, recordDelimiter, split.GetLength
					());
				filePosition = fileIn;
			}
			// If this is not the first split, we always throw away first record
			// because we always (except the last split) read one extra line in
			// next() method.
			if (start != 0)
			{
				start += @in.ReadLine(new Text(), 0, MaxBytesToConsume(start));
			}
			this.pos = start;
		}

		public LineRecordReader(InputStream @in, long offset, long endOffset, int maxLineLength
			)
			: this(@in, offset, endOffset, maxLineLength, null)
		{
		}

		public LineRecordReader(InputStream @in, long offset, long endOffset, int maxLineLength
			, byte[] recordDelimiter)
		{
			this.maxLineLength = maxLineLength;
			this.@in = new SplitLineReader(@in, recordDelimiter);
			this.start = offset;
			this.pos = offset;
			this.end = endOffset;
			filePosition = null;
		}

		/// <exception cref="System.IO.IOException"/>
		public LineRecordReader(InputStream @in, long offset, long endOffset, Configuration
			 job)
			: this(@in, offset, endOffset, job, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public LineRecordReader(InputStream @in, long offset, long endOffset, Configuration
			 job, byte[] recordDelimiter)
		{
			this.maxLineLength = job.GetInt(LineRecordReader.MaxLineLength, int.MaxValue);
			this.@in = new SplitLineReader(@in, job, recordDelimiter);
			this.start = offset;
			this.pos = offset;
			this.end = endOffset;
			filePosition = null;
		}

		public virtual LongWritable CreateKey()
		{
			return new LongWritable();
		}

		public virtual Text CreateValue()
		{
			return new Text();
		}

		private bool IsCompressedInput()
		{
			return (codec != null);
		}

		private int MaxBytesToConsume(long pos)
		{
			return IsCompressedInput() ? int.MaxValue : (int)Math.Max(Math.Min(int.MaxValue, 
				end - pos), maxLineLength);
		}

		/// <exception cref="System.IO.IOException"/>
		private long GetFilePosition()
		{
			long retVal;
			if (IsCompressedInput() && null != filePosition)
			{
				retVal = filePosition.GetPos();
			}
			else
			{
				retVal = pos;
			}
			return retVal;
		}

		/// <exception cref="System.IO.IOException"/>
		private int SkipUtfByteOrderMark(Text value)
		{
			// Strip BOM(Byte Order Mark)
			// Text only support UTF-8, we only need to check UTF-8 BOM
			// (0xEF,0xBB,0xBF) at the start of the text stream.
			int newMaxLineLength = (int)Math.Min(3L + (long)maxLineLength, int.MaxValue);
			int newSize = @in.ReadLine(value, newMaxLineLength, MaxBytesToConsume(pos));
			// Even we read 3 extra bytes for the first line,
			// we won't alter existing behavior (no backwards incompat issue).
			// Because the newSize is less than maxLineLength and
			// the number of bytes copied to Text is always no more than newSize.
			// If the return size from readLine is not less than maxLineLength,
			// we will discard the current line and read the next line.
			pos += newSize;
			int textLength = value.GetLength();
			byte[] textBytes = value.GetBytes();
			if ((textLength >= 3) && (textBytes[0] == unchecked((byte)unchecked((int)(0xEF)))
				) && (textBytes[1] == unchecked((byte)unchecked((int)(0xBB)))) && (textBytes[2] 
				== unchecked((byte)unchecked((int)(0xBF)))))
			{
				// find UTF-8 BOM, strip it.
				Log.Info("Found UTF-8 BOM and skipped it");
				textLength -= 3;
				newSize -= 3;
				if (textLength > 0)
				{
					// It may work to use the same buffer and not do the copyBytes
					textBytes = value.CopyBytes();
					value.Set(textBytes, 3, textLength);
				}
				else
				{
					value.Clear();
				}
			}
			return newSize;
		}

		/// <summary>Read a line.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Next(LongWritable key, Text value)
		{
			lock (this)
			{
				// We always read one extra line, which lies outside the upper
				// split limit i.e. (end - 1)
				while (GetFilePosition() <= end || @in.NeedAdditionalRecordAfterSplit())
				{
					key.Set(pos);
					int newSize = 0;
					if (pos == 0)
					{
						newSize = SkipUtfByteOrderMark(value);
					}
					else
					{
						newSize = @in.ReadLine(value, maxLineLength, MaxBytesToConsume(pos));
						pos += newSize;
					}
					if (newSize == 0)
					{
						return false;
					}
					if (newSize < maxLineLength)
					{
						return true;
					}
					// line too long. try again
					Log.Info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
				}
				return false;
			}
		}

		/// <summary>Get the progress within the split</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual float GetProgress()
		{
			lock (this)
			{
				if (start == end)
				{
					return 0.0f;
				}
				else
				{
					return Math.Min(1.0f, (GetFilePosition() - start) / (float)(end - start));
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			lock (this)
			{
				return pos;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				try
				{
					if (@in != null)
					{
						@in.Close();
					}
				}
				finally
				{
					if (decompressor != null)
					{
						CodecPool.ReturnDecompressor(decompressor);
						decompressor = null;
					}
				}
			}
		}
	}
}
