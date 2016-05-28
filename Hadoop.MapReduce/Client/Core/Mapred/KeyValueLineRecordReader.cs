using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This class treats a line in the input as a key/value pair separated by a
	/// separator character.
	/// </summary>
	/// <remarks>
	/// This class treats a line in the input as a key/value pair separated by a
	/// separator character. The separator can be specified in config file
	/// under the attribute name mapreduce.input.keyvaluelinerecordreader.key.value.separator. The default
	/// separator is the tab character ('\t').
	/// </remarks>
	public class KeyValueLineRecordReader : RecordReader<Text, Text>
	{
		private readonly LineRecordReader lineRecordReader;

		private byte separator = unchecked((byte)(byte)('\t'));

		private LongWritable dummyKey;

		private Text innerValue;

		public virtual Type GetKeyClass()
		{
			return typeof(Text);
		}

		public virtual Text CreateKey()
		{
			return new Text();
		}

		public virtual Text CreateValue()
		{
			return new Text();
		}

		/// <exception cref="System.IO.IOException"/>
		public KeyValueLineRecordReader(Configuration job, FileSplit split)
		{
			lineRecordReader = new LineRecordReader(job, split);
			dummyKey = lineRecordReader.CreateKey();
			innerValue = lineRecordReader.CreateValue();
			string sepStr = job.Get("mapreduce.input.keyvaluelinerecordreader.key.value.separator"
				, "\t");
			this.separator = unchecked((byte)sepStr[0]);
		}

		public static int FindSeparator(byte[] utf, int start, int length, byte sep)
		{
			return Org.Apache.Hadoop.Mapreduce.Lib.Input.KeyValueLineRecordReader.FindSeparator
				(utf, start, length, sep);
		}

		/// <summary>Read key/value pair in a line.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Next(Text key, Text value)
		{
			lock (this)
			{
				byte[] line = null;
				int lineLen = -1;
				if (lineRecordReader.Next(dummyKey, innerValue))
				{
					line = innerValue.GetBytes();
					lineLen = innerValue.GetLength();
				}
				else
				{
					return false;
				}
				if (line == null)
				{
					return false;
				}
				int pos = FindSeparator(line, 0, lineLen, this.separator);
				Org.Apache.Hadoop.Mapreduce.Lib.Input.KeyValueLineRecordReader.SetKeyValue(key, value
					, line, lineLen, pos);
				return true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual float GetProgress()
		{
			return lineRecordReader.GetProgress();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			lock (this)
			{
				return lineRecordReader.GetPos();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				lineRecordReader.Close();
			}
		}
	}
}
