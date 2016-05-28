using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// FixedLengthInputFormat is an input format used to read input files
	/// which contain fixed length records.
	/// </summary>
	/// <remarks>
	/// FixedLengthInputFormat is an input format used to read input files
	/// which contain fixed length records.  The content of a record need not be
	/// text.  It can be arbitrary binary data.  Users must configure the record
	/// length property by calling:
	/// FixedLengthInputFormat.setRecordLength(conf, recordLength);<br /><br /> or
	/// conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, recordLength);
	/// <br /><br />
	/// </remarks>
	/// <seealso cref="FixedLengthRecordReader"/>
	public class FixedLengthInputFormat : FileInputFormat<LongWritable, BytesWritable
		>
	{
		public const string FixedRecordLength = "fixedlengthinputformat.record.length";

		/// <summary>Set the length of each record</summary>
		/// <param name="conf">configuration</param>
		/// <param name="recordLength">the length of a record</param>
		public static void SetRecordLength(Configuration conf, int recordLength)
		{
			conf.SetInt(FixedRecordLength, recordLength);
		}

		/// <summary>Get record length value</summary>
		/// <param name="conf">configuration</param>
		/// <returns>the record length, zero means none was set</returns>
		public static int GetRecordLength(Configuration conf)
		{
			return conf.GetInt(FixedRecordLength, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override RecordReader<LongWritable, BytesWritable> CreateRecordReader(InputSplit
			 split, TaskAttemptContext context)
		{
			int recordLength = GetRecordLength(context.GetConfiguration());
			if (recordLength <= 0)
			{
				throw new IOException("Fixed record length " + recordLength + " is invalid.  It should be set to a value greater than zero"
					);
			}
			return new FixedLengthRecordReader(recordLength);
		}

		protected internal override bool IsSplitable(JobContext context, Path file)
		{
			CompressionCodec codec = new CompressionCodecFactory(context.GetConfiguration()).
				GetCodec(file);
			return (null == codec);
		}
	}
}
