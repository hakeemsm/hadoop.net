using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>
	/// Refinement of InputFormat requiring implementors to provide
	/// ComposableRecordReader instead of RecordReader.
	/// </summary>
	public abstract class ComposableInputFormat<K, V> : InputFormat<K, V>
		where K : WritableComparable<object>
		where V : Writable
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract override RecordReader<K, V> CreateRecordReader(InputSplit split, 
			TaskAttemptContext context);
	}
}
