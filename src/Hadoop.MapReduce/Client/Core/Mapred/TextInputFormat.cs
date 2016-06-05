using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// An
	/// <see cref="InputFormat{K, V}"/>
	/// for plain text files.  Files are broken into lines.
	/// Either linefeed or carriage-return are used to signal end of line.  Keys are
	/// the position in the file, and values are the line of text..
	/// </summary>
	public class TextInputFormat : FileInputFormat<LongWritable, Text>, JobConfigurable
	{
		private CompressionCodecFactory compressionCodecs = null;

		public virtual void Configure(JobConf conf)
		{
			compressionCodecs = new CompressionCodecFactory(conf);
		}

		protected internal override bool IsSplitable(FileSystem fs, Path file)
		{
			CompressionCodec codec = compressionCodecs.GetCodec(file);
			if (null == codec)
			{
				return true;
			}
			return codec is SplittableCompressionCodec;
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<LongWritable, Text> GetRecordReader(InputSplit genericSplit
			, JobConf job, Reporter reporter)
		{
			reporter.SetStatus(genericSplit.ToString());
			string delimiter = job.Get("textinputformat.record.delimiter");
			byte[] recordDelimiterBytes = null;
			if (null != delimiter)
			{
				recordDelimiterBytes = Sharpen.Runtime.GetBytesForString(delimiter, Charsets.Utf8
					);
			}
			return new LineRecordReader(job, (FileSplit)genericSplit, recordDelimiterBytes);
		}
	}
}
