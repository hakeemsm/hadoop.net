using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
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
		private readonly FileSplit fileSplit;

		private readonly RecordReader<K, V> delegate_;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal CombineFileRecordReaderWrapper(FileInputFormat<K, V> inputFormat
			, CombineFileSplit split, TaskAttemptContext context, int idx)
		{
			fileSplit = new FileSplit(split.GetPath(idx), split.GetOffset(idx), split.GetLength
				(idx), split.GetLocations());
			delegate_ = inputFormat.CreateRecordReader(fileSplit, context);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Initialize(InputSplit split, TaskAttemptContext context)
		{
			// it really should be the same file split at the time the wrapper instance
			// was created
			System.Diagnostics.Debug.Assert(FileSplitIsValid(context));
			delegate_.Initialize(fileSplit, context);
		}

		private bool FileSplitIsValid(TaskAttemptContext context)
		{
			Configuration conf = context.GetConfiguration();
			long offset = conf.GetLong(MRJobConfig.MapInputStart, 0L);
			if (fileSplit.GetStart() != offset)
			{
				return false;
			}
			long length = conf.GetLong(MRJobConfig.MapInputPath, 0L);
			if (fileSplit.GetLength() != length)
			{
				return false;
			}
			string path = conf.Get(MRJobConfig.MapInputFile);
			if (!fileSplit.GetPath().ToString().Equals(path))
			{
				return false;
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override bool NextKeyValue()
		{
			return delegate_.NextKeyValue();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override K GetCurrentKey()
		{
			return delegate_.GetCurrentKey();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override V GetCurrentValue()
		{
			return delegate_.GetCurrentValue();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override float GetProgress()
		{
			return delegate_.GetProgress();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			delegate_.Close();
		}
	}
}
