using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// An
	/// <see cref="InputFormat{K, V}"/>
	/// for plain text files. Files are broken into lines.
	/// Either linefeed or carriage-return are used to signal end of line. Each line
	/// is divided into key and value parts by a separator byte. If no such a byte
	/// exists, the key will be the entire line and value will be empty.
	/// </summary>
	public class KeyValueTextInputFormat : FileInputFormat<Text, Text>, JobConfigurable
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
		public override RecordReader<Text, Text> GetRecordReader(InputSplit genericSplit, 
			JobConf job, Reporter reporter)
		{
			reporter.SetStatus(genericSplit.ToString());
			return new KeyValueLineRecordReader(job, (FileSplit)genericSplit);
		}
	}
}
