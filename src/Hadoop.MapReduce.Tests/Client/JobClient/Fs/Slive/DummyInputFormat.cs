using System.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>A input format which returns one dummy key and value</summary>
	internal class DummyInputFormat : InputFormat<object, object>
	{
		internal class EmptySplit : InputSplit
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
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
		public virtual InputSplit[] GetSplits(JobConf job, int numSplits)
		{
			InputSplit[] splits = new InputSplit[numSplits];
			for (int i = 0; i < splits.Length; ++i)
			{
				splits[i] = new DummyInputFormat.EmptySplit();
			}
			return splits;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RecordReader<object, object> GetRecordReader(InputSplit split, JobConf
			 job, Reporter reporter)
		{
			return new _RecordReader_62();
		}

		private sealed class _RecordReader_62 : RecordReader<object, object>
		{
			public _RecordReader_62()
			{
				this.once = false;
			}

			internal bool once;

			/// <exception cref="System.IO.IOException"/>
			public bool Next(object key, object value)
			{
				if (!this.once)
				{
					this.once = true;
					return true;
				}
				return false;
			}

			public object CreateKey()
			{
				return new object();
			}

			public object CreateValue()
			{
				return new object();
			}

			/// <exception cref="System.IO.IOException"/>
			public long GetPos()
			{
				return 0L;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Close()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public float GetProgress()
			{
				return 0.0f;
			}
		}
	}
}
