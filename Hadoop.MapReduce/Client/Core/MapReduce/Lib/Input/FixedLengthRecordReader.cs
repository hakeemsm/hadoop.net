using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>A reader to read fixed length records from a split.</summary>
	/// <remarks>
	/// A reader to read fixed length records from a split.  Record offset is
	/// returned as key and the record as bytes is returned in value.
	/// </remarks>
	public class FixedLengthRecordReader : RecordReader<LongWritable, BytesWritable>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.Input.FixedLengthRecordReader
			));

		private int recordLength;

		private long start;

		private long pos;

		private long end;

		private long numRecordsRemainingInSplit;

		private FSDataInputStream fileIn;

		private Seekable filePosition;

		private LongWritable key;

		private BytesWritable value;

		private bool isCompressedInput;

		private Decompressor decompressor;

		private InputStream inputStream;

		public FixedLengthRecordReader(int recordLength)
		{
			this.recordLength = recordLength;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(InputSplit genericSplit, TaskAttemptContext context
			)
		{
			FileSplit split = (FileSplit)genericSplit;
			Configuration job = context.GetConfiguration();
			Path file = split.GetPath();
			Initialize(job, split.GetStart(), split.GetLength(), file);
		}

		// This is also called from the old FixedLengthRecordReader API implementation
		/// <exception cref="System.IO.IOException"/>
		public virtual void Initialize(Configuration job, long splitStart, long splitLength
			, Path file)
		{
			start = splitStart;
			end = start + splitLength;
			long partialRecordLength = start % recordLength;
			long numBytesToSkip = 0;
			if (partialRecordLength != 0)
			{
				numBytesToSkip = recordLength - partialRecordLength;
			}
			// open the file and seek to the start of the split
			FileSystem fs = file.GetFileSystem(job);
			fileIn = fs.Open(file);
			CompressionCodec codec = new CompressionCodecFactory(job).GetCodec(file);
			if (null != codec)
			{
				isCompressedInput = true;
				decompressor = CodecPool.GetDecompressor(codec);
				CompressionInputStream cIn = codec.CreateInputStream(fileIn, decompressor);
				filePosition = cIn;
				inputStream = cIn;
				numRecordsRemainingInSplit = long.MaxValue;
				Log.Info("Compressed input; cannot compute number of records in the split");
			}
			else
			{
				fileIn.Seek(start);
				filePosition = fileIn;
				inputStream = fileIn;
				long splitSize = end - start - numBytesToSkip;
				numRecordsRemainingInSplit = (splitSize + recordLength - 1) / recordLength;
				if (numRecordsRemainingInSplit < 0)
				{
					numRecordsRemainingInSplit = 0;
				}
				Log.Info("Expecting " + numRecordsRemainingInSplit + " records each with a length of "
					 + recordLength + " bytes in the split with an effective size of " + splitSize +
					 " bytes");
			}
			if (numBytesToSkip != 0)
			{
				start += inputStream.Skip(numBytesToSkip);
			}
			this.pos = start;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool NextKeyValue()
		{
			lock (this)
			{
				if (key == null)
				{
					key = new LongWritable();
				}
				if (value == null)
				{
					value = new BytesWritable(new byte[recordLength]);
				}
				bool dataRead = false;
				value.SetSize(recordLength);
				byte[] record = value.GetBytes();
				if (numRecordsRemainingInSplit > 0)
				{
					key.Set(pos);
					int offset = 0;
					int numBytesToRead = recordLength;
					int numBytesRead = 0;
					while (numBytesToRead > 0)
					{
						numBytesRead = inputStream.Read(record, offset, numBytesToRead);
						if (numBytesRead == -1)
						{
							// EOF
							break;
						}
						offset += numBytesRead;
						numBytesToRead -= numBytesRead;
					}
					numBytesRead = recordLength - numBytesToRead;
					pos += numBytesRead;
					if (numBytesRead > 0)
					{
						dataRead = true;
						if (numBytesRead >= recordLength)
						{
							if (!isCompressedInput)
							{
								numRecordsRemainingInSplit--;
							}
						}
						else
						{
							throw new IOException("Partial record(length = " + numBytesRead + ") found at the end of split."
								);
						}
					}
					else
					{
						numRecordsRemainingInSplit = 0L;
					}
				}
				// End of input.
				return dataRead;
			}
		}

		public override LongWritable GetCurrentKey()
		{
			return key;
		}

		public override BytesWritable GetCurrentValue()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public override float GetProgress()
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
		public override void Close()
		{
			lock (this)
			{
				try
				{
					if (inputStream != null)
					{
						inputStream.Close();
						inputStream = null;
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

		// This is called from the old FixedLengthRecordReader API implementation.
		public virtual long GetPos()
		{
			return pos;
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
	}
}
