using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// This class is similar to SequenceFileInputFormat, except it generates
	/// SequenceFileAsTextRecordReader which converts the input keys and values
	/// to their String forms by calling toString() method.
	/// </summary>
	public class SequenceFileAsTextInputFormat : SequenceFileInputFormat<Text, Text>
	{
		public SequenceFileAsTextInputFormat()
			: base()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<Text, Text> CreateRecordReader(InputSplit split, TaskAttemptContext
			 context)
		{
			context.SetStatus(split.ToString());
			return new SequenceFileAsTextRecordReader();
		}
	}
}
