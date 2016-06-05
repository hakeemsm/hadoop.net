using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This class is similar to SequenceFileInputFormat,
	/// except it generates SequenceFileAsTextRecordReader
	/// which converts the input keys and values to their
	/// String forms by calling toString() method.
	/// </summary>
	public class SequenceFileAsTextInputFormat : SequenceFileInputFormat<Text, Text>
	{
		public SequenceFileAsTextInputFormat()
			: base()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<Text, Text> GetRecordReader(InputSplit split, JobConf
			 job, Reporter reporter)
		{
			reporter.SetStatus(split.ToString());
			return new SequenceFileAsTextRecordReader(job, (FileSplit)split);
		}
	}
}
