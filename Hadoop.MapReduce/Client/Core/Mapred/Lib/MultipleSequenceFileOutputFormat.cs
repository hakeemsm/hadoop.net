using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// This class extends the MultipleOutputFormat, allowing to write the output data
	/// to different output files in sequence file output format.
	/// </summary>
	public class MultipleSequenceFileOutputFormat<K, V> : MultipleOutputFormat<K, V>
	{
		private SequenceFileOutputFormat<K, V> theSequenceFileOutputFormat = null;

		/// <exception cref="System.IO.IOException"/>
		protected internal override RecordWriter<K, V> GetBaseRecordWriter(FileSystem fs, 
			JobConf job, string name, Progressable arg3)
		{
			if (theSequenceFileOutputFormat == null)
			{
				theSequenceFileOutputFormat = new SequenceFileOutputFormat<K, V>();
			}
			return theSequenceFileOutputFormat.GetRecordWriter(fs, job, name, arg3);
		}
	}
}
