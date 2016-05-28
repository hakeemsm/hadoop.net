using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// This class converts the input keys and values to their String forms by
	/// calling toString() method.
	/// </summary>
	/// <remarks>
	/// This class converts the input keys and values to their String forms by
	/// calling toString() method. This class to SequenceFileAsTextInputFormat
	/// class is as LineRecordReader class to TextInputFormat class.
	/// </remarks>
	public class SequenceFileAsTextRecordReader : RecordReader<Text, Text>
	{
		private readonly SequenceFileRecordReader<WritableComparable<object>, Writable> sequenceFileRecordReader;

		private Text key;

		private Text value;

		/// <exception cref="System.IO.IOException"/>
		public SequenceFileAsTextRecordReader()
		{
			sequenceFileRecordReader = new SequenceFileRecordReader<WritableComparable<object
				>, Writable>();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Initialize(InputSplit split, TaskAttemptContext context)
		{
			sequenceFileRecordReader.Initialize(split, context);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override Text GetCurrentKey()
		{
			return key;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override Text GetCurrentValue()
		{
			return value;
		}

		/// <summary>Read key/value pair in a line.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override bool NextKeyValue()
		{
			lock (this)
			{
				if (!sequenceFileRecordReader.NextKeyValue())
				{
					return false;
				}
				if (key == null)
				{
					key = new Text();
				}
				if (value == null)
				{
					value = new Text();
				}
				key.Set(sequenceFileRecordReader.GetCurrentKey().ToString());
				value.Set(sequenceFileRecordReader.GetCurrentValue().ToString());
				return true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override float GetProgress()
		{
			return sequenceFileRecordReader.GetProgress();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				sequenceFileRecordReader.Close();
			}
		}
	}
}
