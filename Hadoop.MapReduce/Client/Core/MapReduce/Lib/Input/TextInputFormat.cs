using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
	/// for plain text files.  Files are broken into lines.
	/// Either linefeed or carriage-return are used to signal end of line.  Keys are
	/// the position in the file, and values are the line of text..
	/// </summary>
	public class TextInputFormat : FileInputFormat<LongWritable, Text>
	{
		public override RecordReader<LongWritable, Text> CreateRecordReader(InputSplit split
			, TaskAttemptContext context)
		{
			string delimiter = context.GetConfiguration().Get("textinputformat.record.delimiter"
				);
			byte[] recordDelimiterBytes = null;
			if (null != delimiter)
			{
				recordDelimiterBytes = Sharpen.Runtime.GetBytesForString(delimiter, Charsets.Utf8
					);
			}
			return new LineRecordReader(recordDelimiterBytes);
		}

		protected internal override bool IsSplitable(JobContext context, Path file)
		{
			CompressionCodec codec = new CompressionCodecFactory(context.GetConfiguration()).
				GetCodec(file);
			if (null == codec)
			{
				return true;
			}
			return codec is SplittableCompressionCodec;
		}
	}
}
