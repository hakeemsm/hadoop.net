using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// InputFormat which simulates the absence of input data
	/// by returning zero split.
	/// </summary>
	public class EmptyInputFormat<K, V> : InputFormat<K, V>
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual InputSplit[] GetSplits(JobConf job, int numSplits)
		{
			return new InputSplit[0];
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RecordReader<K, V> GetRecordReader(InputSplit split, JobConf job, 
			Reporter reporter)
		{
			return new _RecordReader_36();
		}

		private sealed class _RecordReader_36 : RecordReader<K, V>
		{
			public _RecordReader_36()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public bool Next(K key, V value)
			{
				return false;
			}

			public K CreateKey()
			{
				return null;
			}

			public V CreateValue()
			{
				return null;
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
