using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// This is a delegating RecordReader, which delegates the functionality to the
	/// underlying record reader in
	/// <see cref="TaggedInputSplit"/>
	/// 
	/// </summary>
	public class DelegatingRecordReader<K, V> : RecordReader<K, V>
	{
		internal RecordReader<K, V> originalRR;

		/// <summary>Constructs the DelegatingRecordReader.</summary>
		/// <param name="split">TaggegInputSplit object</param>
		/// <param name="context">TaskAttemptContext object</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public DelegatingRecordReader(InputSplit split, TaskAttemptContext context)
		{
			// Find the InputFormat and then the RecordReader from the
			// TaggedInputSplit.
			TaggedInputSplit taggedInputSplit = (TaggedInputSplit)split;
			InputFormat<K, V> inputFormat = (InputFormat<K, V>)ReflectionUtils.NewInstance(taggedInputSplit
				.GetInputFormatClass(), context.GetConfiguration());
			originalRR = inputFormat.CreateRecordReader(taggedInputSplit.GetInputSplit(), context
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			originalRR.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override K GetCurrentKey()
		{
			return originalRR.GetCurrentKey();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override V GetCurrentValue()
		{
			return originalRR.GetCurrentValue();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override float GetProgress()
		{
			return originalRR.GetProgress();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Initialize(InputSplit split, TaskAttemptContext context)
		{
			originalRR.Initialize(((TaggedInputSplit)split).GetInputSplit(), context);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override bool NextKeyValue()
		{
			return originalRR.NextKeyValue();
		}
	}
}
