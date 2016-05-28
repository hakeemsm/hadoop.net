using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// Input format that is a <code>CombineFileInputFormat</code>-equivalent for
	/// <code>SequenceFileInputFormat</code>.
	/// </summary>
	/// <seealso cref="CombineFileInputFormat{K, V}"/>
	public class CombineSequenceFileInputFormat<K, V> : CombineFileInputFormat<K, V>
	{
		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<K, V> CreateRecordReader(InputSplit split, TaskAttemptContext
			 context)
		{
			return new CombineFileRecordReader((CombineFileSplit)split, context, typeof(CombineSequenceFileInputFormat.SequenceFileRecordReaderWrapper
				));
		}

		/// <summary>
		/// A record reader that may be passed to <code>CombineFileRecordReader</code>
		/// so that it can be used in a <code>CombineFileInputFormat</code>-equivalent
		/// for <code>SequenceFileInputFormat</code>.
		/// </summary>
		/// <seealso cref="CombineFileRecordReader{K, V}"/>
		/// <seealso cref="CombineFileInputFormat{K, V}"/>
		/// <seealso cref="SequenceFileInputFormat{K, V}"/>
		private class SequenceFileRecordReaderWrapper<K, V> : CombineFileRecordReaderWrapper
			<K, V>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public SequenceFileRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext
				 context, int idx)
				: base(new SequenceFileInputFormat<K, V>(), split, context, idx)
			{
			}
			// this constructor signature is required by CombineFileRecordReader
		}
	}
}
