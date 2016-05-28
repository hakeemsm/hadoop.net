using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>A wrapper class for a record reader that handles a single file split.</summary>
	/// <remarks>
	/// A wrapper class for a record reader that handles a single file split. It
	/// delegates most of the methods to the wrapped instance. A concrete subclass
	/// needs to provide a constructor that calls this parent constructor with the
	/// appropriate input format. The subclass constructor must satisfy the specific
	/// constructor signature that is required by
	/// <code>CombineFileRecordReader</code>.
	/// Subclassing is needed to get a concrete record reader wrapper because of the
	/// constructor requirement.
	/// </remarks>
	/// <seealso cref="CombineFileRecordReader{K, V}"/>
	/// <seealso cref="CombineFileInputFormat{K, V}"/>
	public abstract class CombineFileRecordReaderWrapper<K, V> : RecordReader<K, V>
	{
		private readonly RecordReader<K, V> delegate_;

		/// <exception cref="System.IO.IOException"/>
		protected internal CombineFileRecordReaderWrapper(FileInputFormat<K, V> inputFormat
			, CombineFileSplit split, Configuration conf, Reporter reporter, int idx)
		{
			FileSplit fileSplit = new FileSplit(split.GetPath(idx), split.GetOffset(idx), split
				.GetLength(idx), split.GetLocations());
			delegate_ = inputFormat.GetRecordReader(fileSplit, (JobConf)conf, reporter);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Next(K key, V value)
		{
			return delegate_.Next(key, value);
		}

		public virtual K CreateKey()
		{
			return delegate_.CreateKey();
		}

		public virtual V CreateValue()
		{
			return delegate_.CreateValue();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			return delegate_.GetPos();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			delegate_.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual float GetProgress()
		{
			return delegate_.GetProgress();
		}
	}
}
