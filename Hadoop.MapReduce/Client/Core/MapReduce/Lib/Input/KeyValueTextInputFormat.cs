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
	/// for plain text files. Files are broken into lines.
	/// Either line feed or carriage-return are used to signal end of line.
	/// Each line is divided into key and value parts by a separator byte. If no
	/// such a byte exists, the key will be the entire line and value will be empty.
	/// The separator byte can be specified in config file under the attribute name
	/// mapreduce.input.keyvaluelinerecordreader.key.value.separator. The default
	/// is the tab character ('\t').
	/// </summary>
	public class KeyValueTextInputFormat : FileInputFormat<Text, Text>
	{
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

		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<Text, Text> CreateRecordReader(InputSplit genericSplit
			, TaskAttemptContext context)
		{
			context.SetStatus(genericSplit.ToString());
			return new KeyValueLineRecordReader(context.GetConfiguration());
		}
	}
}
