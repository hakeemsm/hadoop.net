using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>SplitLineReader for uncompressed files.</summary>
	/// <remarks>
	/// SplitLineReader for uncompressed files.
	/// This class can split the file correctly even if the delimiter is multi-bytes.
	/// </remarks>
	public class UncompressedSplitLineReader : SplitLineReader
	{
		private bool needAdditionalRecord = false;

		private long splitLength;

		/// <summary>Total bytes read from the input stream.</summary>
		private long totalBytesRead = 0;

		private bool finished = false;

		private bool usingCRLF;

		/// <exception cref="System.IO.IOException"/>
		public UncompressedSplitLineReader(FSDataInputStream @in, Configuration conf, byte
			[] recordDelimiterBytes, long splitLength)
			: base(@in, conf, recordDelimiterBytes)
		{
			this.splitLength = splitLength;
			usingCRLF = (recordDelimiterBytes == null);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override int FillBuffer(InputStream @in, byte[] buffer, bool inDelimiter
			)
		{
			int maxBytesToRead = buffer.Length;
			if (totalBytesRead < splitLength)
			{
				maxBytesToRead = Math.Min(maxBytesToRead, (int)(splitLength - totalBytesRead));
			}
			int bytesRead = @in.Read(buffer, 0, maxBytesToRead);
			// If the split ended in the middle of a record delimiter then we need
			// to read one additional record, as the consumer of the next split will
			// not recognize the partial delimiter as a record.
			// However if using the default delimiter and the next character is a
			// linefeed then next split will treat it as a delimiter all by itself
			// and the additional record read should not be performed.
			if (totalBytesRead == splitLength && inDelimiter && bytesRead > 0)
			{
				if (usingCRLF)
				{
					needAdditionalRecord = (buffer[0] != '\n');
				}
				else
				{
					needAdditionalRecord = true;
				}
			}
			if (bytesRead > 0)
			{
				totalBytesRead += bytesRead;
			}
			return bytesRead;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int ReadLine(Text str, int maxLineLength, int maxBytesToConsume)
		{
			int bytesRead = 0;
			if (!finished)
			{
				// only allow at most one more record to be read after the stream
				// reports the split ended
				if (totalBytesRead > splitLength)
				{
					finished = true;
				}
				bytesRead = base.ReadLine(str, maxLineLength, maxBytesToConsume);
			}
			return bytesRead;
		}

		public override bool NeedAdditionalRecordAfterSplit()
		{
			return !finished && needAdditionalRecord;
		}

		protected override void UnsetNeedAdditionalRecordAfterSplit()
		{
			needAdditionalRecord = false;
		}
	}
}
