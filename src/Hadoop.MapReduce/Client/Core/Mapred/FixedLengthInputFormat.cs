using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
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
		>, JobConfigurable
	{
		private CompressionCodecFactory compressionCodecs = null;

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

		public virtual void Configure(JobConf conf)
		{
			compressionCodecs = new CompressionCodecFactory(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<LongWritable, BytesWritable> GetRecordReader(InputSplit
			 genericSplit, JobConf job, Reporter reporter)
		{
			reporter.SetStatus(genericSplit.ToString());
			int recordLength = GetRecordLength(job);
			if (recordLength <= 0)
			{
				throw new IOException("Fixed record length " + recordLength + " is invalid.  It should be set to a value greater than zero"
					);
			}
			return new FixedLengthRecordReader(job, (FileSplit)genericSplit, recordLength);
		}

		protected internal override bool IsSplitable(FileSystem fs, Path file)
		{
			CompressionCodec codec = compressionCodecs.GetCodec(file);
			return (null == codec);
		}
	}
}
