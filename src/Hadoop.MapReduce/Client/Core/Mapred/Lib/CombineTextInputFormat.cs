using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// Input format that is a <code>CombineFileInputFormat</code>-equivalent for
	/// <code>TextInputFormat</code>.
	/// </summary>
	/// <seealso cref="CombineFileInputFormat{K, V}"/>
	public class CombineTextInputFormat : CombineFileInputFormat<LongWritable, Text>
	{
		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<LongWritable, Text> GetRecordReader(InputSplit split
			, JobConf conf, Reporter reporter)
		{
			return new CombineFileRecordReader(conf, (CombineFileSplit)split, reporter, typeof(
				CombineTextInputFormat.TextRecordReaderWrapper));
		}

		/// <summary>
		/// A record reader that may be passed to <code>CombineFileRecordReader</code>
		/// so that it can be used in a <code>CombineFileInputFormat</code>-equivalent
		/// for <code>TextInputFormat</code>.
		/// </summary>
		/// <seealso cref="CombineFileRecordReader{K, V}"/>
		/// <seealso cref="CombineFileInputFormat{K, V}"/>
		/// <seealso cref="Org.Apache.Hadoop.Mapred.TextInputFormat"/>
		private class TextRecordReaderWrapper : CombineFileRecordReaderWrapper<LongWritable
			, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			public TextRecordReaderWrapper(CombineFileSplit split, Configuration conf, Reporter
				 reporter, int idx)
				: base(new TextInputFormat(), split, conf, reporter, idx)
			{
			}
			// this constructor signature is required by CombineFileRecordReader
		}
	}
}
