using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	/// <summary>This is a support class to test Hadoop Pipes when using C++ RecordReaders.
	/// 	</summary>
	/// <remarks>
	/// This is a support class to test Hadoop Pipes when using C++ RecordReaders.
	/// It defines an InputFormat with InputSplits that are just strings. The
	/// RecordReaders are not implemented in Java, naturally...
	/// </remarks>
	public class WordCountInputFormat : FileInputFormat<IntWritable, Text>
	{
		internal class WordCountInputSplit : InputSplit
		{
			private string filename;

			internal WordCountInputSplit()
			{
			}

			internal WordCountInputSplit(Path filename)
			{
				this.filename = filename.ToUri().GetPath();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				Text.WriteString(@out, filename);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				filename = Text.ReadString(@in);
			}

			public virtual long GetLength()
			{
				return 0L;
			}

			public virtual string[] GetLocations()
			{
				return new string[0];
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override InputSplit[] GetSplits(JobConf conf, int numSplits)
		{
			AList<InputSplit> result = new AList<InputSplit>();
			FileSystem local = FileSystem.GetLocal(conf);
			foreach (Path dir in GetInputPaths(conf))
			{
				foreach (FileStatus file in local.ListStatus(dir))
				{
					result.AddItem(new WordCountInputFormat.WordCountInputSplit(file.GetPath()));
				}
			}
			return Sharpen.Collections.ToArray(result, new InputSplit[result.Count]);
		}

		public override RecordReader<IntWritable, Text> GetRecordReader(InputSplit split, 
			JobConf conf, Reporter reporter)
		{
			return new _RecordReader_66();
		}

		private sealed class _RecordReader_66 : RecordReader<IntWritable, Text>
		{
			public _RecordReader_66()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public bool Next(IntWritable key, Text value)
			{
				return false;
			}

			public IntWritable CreateKey()
			{
				return new IntWritable();
			}

			public Text CreateValue()
			{
				return new Text();
			}

			public long GetPos()
			{
				return 0;
			}

			public void Close()
			{
			}

			public float GetProgress()
			{
				return 0.0f;
			}
		}
	}
}
