using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// This class extends the MultipleOutputFormat, allowing to write the output
	/// data to different output files in Text output format.
	/// </summary>
	public class MultipleTextOutputFormat<K, V> : MultipleOutputFormat<K, V>
	{
		private TextOutputFormat<K, V> theTextOutputFormat = null;

		/// <exception cref="System.IO.IOException"/>
		protected internal override RecordWriter<K, V> GetBaseRecordWriter(FileSystem fs, 
			JobConf job, string name, Progressable arg3)
		{
			if (theTextOutputFormat == null)
			{
				theTextOutputFormat = new TextOutputFormat<K, V>();
			}
			return theTextOutputFormat.GetRecordWriter(fs, job, name, arg3);
		}
	}
}
