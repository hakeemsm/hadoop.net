using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>Consume all outputs and put them in /dev/null.</summary>
	public class NullOutputFormat<K, V> : OutputFormat<K, V>
	{
		public virtual RecordWriter<K, V> GetRecordWriter(FileSystem ignored, JobConf job
			, string name, Progressable progress)
		{
			return new _RecordWriter_39();
		}

		private sealed class _RecordWriter_39 : RecordWriter<K, V>
		{
			public _RecordWriter_39()
			{
			}

			public void Write(K key, V value)
			{
			}

			public void Close(Reporter reporter)
			{
			}
		}

		public virtual void CheckOutputSpecs(FileSystem ignored, JobConf job)
		{
		}
	}
}
