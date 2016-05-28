using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>
	/// Refinement of InputFormat requiring implementors to provide
	/// ComposableRecordReader instead of RecordReader.
	/// </summary>
	public interface ComposableInputFormat<K, V> : InputFormat<K, V>
		where K : WritableComparable
		where V : Writable
	{
		/// <exception cref="System.IO.IOException"/>
		ComposableRecordReader<K, V> GetRecordReader(InputSplit split, JobConf job, Reporter
			 reporter);
	}
}
