using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This class converts the input keys and values to their String forms by calling toString()
	/// method.
	/// </summary>
	/// <remarks>
	/// This class converts the input keys and values to their String forms by calling toString()
	/// method. This class to SequenceFileAsTextInputFormat class is as LineRecordReader
	/// class to TextInputFormat class.
	/// </remarks>
	public class SequenceFileAsTextRecordReader : RecordReader<Text, Text>
	{
		private readonly SequenceFileRecordReader<WritableComparable, Writable> sequenceFileRecordReader;

		private WritableComparable innerKey;

		private Writable innerValue;

		/// <exception cref="System.IO.IOException"/>
		public SequenceFileAsTextRecordReader(Configuration conf, FileSplit split)
		{
			sequenceFileRecordReader = new SequenceFileRecordReader<WritableComparable, Writable
				>(conf, split);
			innerKey = sequenceFileRecordReader.CreateKey();
			innerValue = sequenceFileRecordReader.CreateValue();
		}

		public virtual Text CreateKey()
		{
			return new Text();
		}

		public virtual Text CreateValue()
		{
			return new Text();
		}

		/// <summary>Read key/value pair in a line.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Next(Text key, Text value)
		{
			lock (this)
			{
				Text tKey = key;
				Text tValue = value;
				if (!sequenceFileRecordReader.Next(innerKey, innerValue))
				{
					return false;
				}
				tKey.Set(innerKey.ToString());
				tValue.Set(innerValue.ToString());
				return true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual float GetProgress()
		{
			return sequenceFileRecordReader.GetProgress();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			lock (this)
			{
				return sequenceFileRecordReader.GetPos();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				sequenceFileRecordReader.Close();
			}
		}
	}
}
