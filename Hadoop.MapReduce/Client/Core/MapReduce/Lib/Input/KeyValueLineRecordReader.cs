using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
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
		public const string KeyValueSeperator = "mapreduce.input.keyvaluelinerecordreader.key.value.separator";

		private readonly LineRecordReader lineRecordReader;

		private byte separator = unchecked((byte)(byte)('\t'));

		private Text innerValue;

		private Text key;

		private Text value;

		public virtual Type GetKeyClass()
		{
			return typeof(Text);
		}

		/// <exception cref="System.IO.IOException"/>
		public KeyValueLineRecordReader(Configuration conf)
		{
			lineRecordReader = new LineRecordReader();
			string sepStr = conf.Get(KeyValueSeperator, "\t");
			this.separator = unchecked((byte)sepStr[0]);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(InputSplit genericSplit, TaskAttemptContext context
			)
		{
			lineRecordReader.Initialize(genericSplit, context);
		}

		public static int FindSeparator(byte[] utf, int start, int length, byte sep)
		{
			for (int i = start; i < (start + length); i++)
			{
				if (utf[i] == sep)
				{
					return i;
				}
			}
			return -1;
		}

		public static void SetKeyValue(Text key, Text value, byte[] line, int lineLen, int
			 pos)
		{
			if (pos == -1)
			{
				key.Set(line, 0, lineLen);
				value.Set(string.Empty);
			}
			else
			{
				key.Set(line, 0, pos);
				value.Set(line, pos + 1, lineLen - pos - 1);
			}
		}

		/// <summary>Read key/value pair in a line.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool NextKeyValue()
		{
			lock (this)
			{
				byte[] line = null;
				int lineLen = -1;
				if (lineRecordReader.NextKeyValue())
				{
					innerValue = lineRecordReader.GetCurrentValue();
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
				if (key == null)
				{
					key = new Text();
				}
				if (value == null)
				{
					value = new Text();
				}
				int pos = FindSeparator(line, 0, lineLen, this.separator);
				SetKeyValue(key, value, line, lineLen, pos);
				return true;
			}
		}

		public override Text GetCurrentKey()
		{
			return key;
		}

		public override Text GetCurrentValue()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public override float GetProgress()
		{
			return lineRecordReader.GetProgress();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				lineRecordReader.Close();
			}
		}
	}
}
