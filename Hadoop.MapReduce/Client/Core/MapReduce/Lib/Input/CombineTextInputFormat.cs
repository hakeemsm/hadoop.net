using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// Input format that is a <code>CombineFileInputFormat</code>-equivalent for
	/// <code>TextInputFormat</code>.
	/// </summary>
	/// <seealso cref="CombineFileInputFormat{K, V}"/>
	public class CombineTextInputFormat : CombineFileInputFormat<LongWritable, Text>
	{
		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<LongWritable, Text> CreateRecordReader(InputSplit split
			, TaskAttemptContext context)
		{
			return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit)split, context
				, typeof(CombineTextInputFormat.TextRecordReaderWrapper));
		}

		/// <summary>
		/// A record reader that may be passed to <code>CombineFileRecordReader</code>
		/// so that it can be used in a <code>CombineFileInputFormat</code>-equivalent
		/// for <code>TextInputFormat</code>.
		/// </summary>
		/// <seealso cref="CombineFileRecordReader{K, V}"/>
		/// <seealso cref="CombineFileInputFormat{K, V}"/>
		/// <seealso cref="TextInputFormat"/>
		private class TextRecordReaderWrapper : CombineFileRecordReaderWrapper<LongWritable
			, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public TextRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context
				, int idx)
				: base(new TextInputFormat(), split, context, idx)
			{
			}
			// this constructor signature is required by CombineFileRecordReader
		}
	}
}
