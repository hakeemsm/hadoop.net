using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>Treats keys as offset in file and value as line.</summary>
	public class LineRecordReader : RecordReader<LongWritable, Text>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.Input.LineRecordReader
			));

		public const string MaxLineLength = "mapreduce.input.linerecordreader.line.maxlength";

		private long start;

		private long pos;

		private long end;

		private SplitLineReader @in;

		private FSDataInputStream fileIn;

		private Seekable filePosition;

		private int maxLineLength;

		private LongWritable key;

		private Text value;

		private bool isCompressedInput;

		private Decompressor decompressor;

		private byte[] recordDelimiterBytes;

		public LineRecordReader()
		{
		}

		public LineRecordReader(byte[] recordDelimiter)
		{
			this.recordDelimiterBytes = recordDelimiter;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(InputSplit genericSplit, TaskAttemptContext context
			)
		{
			FileSplit split = (FileSplit)genericSplit;
			Configuration job = context.GetConfiguration();
			this.maxLineLength = job.GetInt(MaxLineLength, int.MaxValue);
			start = split.GetStart();
			end = start + split.GetLength();
			Path file = split.GetPath();
			// open the file and seek to the start of the split
			FileSystem fs = file.GetFileSystem(job);
			fileIn = fs.Open(file);
			CompressionCodec codec = new CompressionCodecFactory(job).GetCodec(file);
			if (null != codec)
			{
				isCompressedInput = true;
				decompressor = CodecPool.GetDecompressor(codec);
				if (codec is SplittableCompressionCodec)
				{
					SplitCompressionInputStream cIn = ((SplittableCompressionCodec)codec).CreateInputStream
						(fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.Byblock);
					@in = new CompressedSplitLineReader(cIn, job, this.recordDelimiterBytes);
					start = cIn.GetAdjustedStart();
					end = cIn.GetAdjustedEnd();
					filePosition = cIn;
				}
				else
				{
					@in = new SplitLineReader(codec.CreateInputStream(fileIn, decompressor), job, this
						.recordDelimiterBytes);
					filePosition = fileIn;
				}
			}
			else
			{
				fileIn.Seek(start);
				@in = new UncompressedSplitLineReader(fileIn, job, this.recordDelimiterBytes, split
					.GetLength());
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

		private int MaxBytesToConsume(long pos)
		{
			return isCompressedInput ? int.MaxValue : (int)Math.Max(Math.Min(int.MaxValue, end
				 - pos), maxLineLength);
		}

		/// <exception cref="System.IO.IOException"/>
		private long GetFilePosition()
		{
			long retVal;
			if (isCompressedInput && null != filePosition)
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
		private int SkipUtfByteOrderMark()
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

		/// <exception cref="System.IO.IOException"/>
		public override bool NextKeyValue()
		{
			if (key == null)
			{
				key = new LongWritable();
			}
			key.Set(pos);
			if (value == null)
			{
				value = new Text();
			}
			int newSize = 0;
			// We always read one extra line, which lies outside the upper
			// split limit i.e. (end - 1)
			while (GetFilePosition() <= end || @in.NeedAdditionalRecordAfterSplit())
			{
				if (pos == 0)
				{
					newSize = SkipUtfByteOrderMark();
				}
				else
				{
					newSize = @in.ReadLine(value, maxLineLength, MaxBytesToConsume(pos));
					pos += newSize;
				}
				if ((newSize == 0) || (newSize < maxLineLength))
				{
					break;
				}
				// line too long. try again
				Log.Info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
			}
			if (newSize == 0)
			{
				key = null;
				value = null;
				return false;
			}
			else
			{
				return true;
			}
		}

		public override LongWritable GetCurrentKey()
		{
			return key;
		}

		public override Text GetCurrentValue()
		{
			return value;
		}

		/// <summary>Get the progress within the split</summary>
		/// <exception cref="System.IO.IOException"/>
		public override float GetProgress()
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

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
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
