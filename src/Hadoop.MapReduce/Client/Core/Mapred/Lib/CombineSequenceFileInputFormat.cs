using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// Input format that is a <code>CombineFileInputFormat</code>-equivalent for
	/// <code>SequenceFileInputFormat</code>.
	/// </summary>
	/// <seealso cref="CombineFileInputFormat{K, V}"/>
	public class CombineSequenceFileInputFormat<K, V> : CombineFileInputFormat<K, V>
	{
		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<K, V> GetRecordReader(InputSplit split, JobConf conf
			, Reporter reporter)
		{
			return new CombineFileRecordReader(conf, (CombineFileSplit)split, reporter, typeof(
				CombineSequenceFileInputFormat.SequenceFileRecordReaderWrapper));
		}

		/// <summary>
		/// A record reader that may be passed to <code>CombineFileRecordReader</code>
		/// so that it can be used in a <code>CombineFileInputFormat</code>-equivalent
		/// for <code>SequenceFileInputFormat</code>.
		/// </summary>
		/// <seealso cref="CombineFileRecordReader{K, V}"/>
		/// <seealso cref="CombineFileInputFormat{K, V}"/>
		/// <seealso cref="Org.Apache.Hadoop.Mapred.SequenceFileInputFormat{K, V}"/>
		private class SequenceFileRecordReaderWrapper<K, V> : CombineFileRecordReaderWrapper
			<K, V>
		{
			/// <exception cref="System.IO.IOException"/>
			public SequenceFileRecordReaderWrapper(CombineFileSplit split, Configuration conf
				, Reporter reporter, int idx)
				: base(new SequenceFileInputFormat<K, V>(), split, conf, reporter, idx)
			{
			}
			// this constructor signature is required by CombineFileRecordReader
		}
	}
}
