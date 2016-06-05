using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// A generic RecordReader that can hand out different recordReaders
	/// for each chunk in a
	/// <see cref="CombineFileSplit"/>
	/// .
	/// A CombineFileSplit can combine data chunks from multiple files.
	/// This class allows using different RecordReaders for processing
	/// these data chunks from different files.
	/// </summary>
	/// <seealso cref="CombineFileSplit"/>
	public class CombineFileRecordReader<K, V> : RecordReader<K, V>
	{
		internal static readonly Type[] constructorSignature = new Type[] { typeof(CombineFileSplit
			), typeof(TaskAttemptContext), typeof(int) };

		protected internal CombineFileSplit split;

		protected internal Constructor<RecordReader<K, V>> rrConstructor;

		protected internal TaskAttemptContext context;

		protected internal int idx;

		protected internal long progress;

		protected internal RecordReader<K, V> curReader;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Initialize(InputSplit split, TaskAttemptContext context)
		{
			this.split = (CombineFileSplit)split;
			this.context = context;
			if (null != this.curReader)
			{
				this.curReader.Initialize(split, context);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override bool NextKeyValue()
		{
			while ((curReader == null) || !curReader.NextKeyValue())
			{
				if (!InitNextRecordReader())
				{
					return false;
				}
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override K GetCurrentKey()
		{
			return curReader.GetCurrentKey();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override V GetCurrentValue()
		{
			return curReader.GetCurrentValue();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (curReader != null)
			{
				curReader.Close();
				curReader = null;
			}
		}

		/// <summary>return progress based on the amount of data processed so far.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override float GetProgress()
		{
			long subprogress = 0;
			// bytes processed in current split
			if (null != curReader)
			{
				// idx is always one past the current subsplit's true index.
				subprogress = (long)(curReader.GetProgress() * split.GetLength(idx - 1));
			}
			return Math.Min(1.0f, (progress + subprogress) / (float)(split.GetLength()));
		}

		/// <summary>
		/// A generic RecordReader that can hand out different recordReaders
		/// for each chunk in the CombineFileSplit.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public CombineFileRecordReader(CombineFileSplit split, TaskAttemptContext context
			, Type rrClass)
		{
			this.split = split;
			this.context = context;
			this.idx = 0;
			this.curReader = null;
			this.progress = 0;
			try
			{
				rrConstructor = rrClass.GetDeclaredConstructor(constructorSignature);
			}
			catch (Exception e)
			{
				throw new RuntimeException(rrClass.FullName + " does not have valid constructor", 
					e);
			}
			InitNextRecordReader();
		}

		/// <summary>Get the record reader for the next chunk in this CombineFileSplit.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool InitNextRecordReader()
		{
			if (curReader != null)
			{
				curReader.Close();
				curReader = null;
				if (idx > 0)
				{
					progress += split.GetLength(idx - 1);
				}
			}
			// done processing so far
			// if all chunks have been processed, nothing more to do.
			if (idx == split.GetNumPaths())
			{
				return false;
			}
			context.Progress();
			// get a record reader for the idx-th chunk
			try
			{
				Configuration conf = context.GetConfiguration();
				// setup some helper config variables.
				conf.Set(MRJobConfig.MapInputFile, split.GetPath(idx).ToString());
				conf.SetLong(MRJobConfig.MapInputStart, split.GetOffset(idx));
				conf.SetLong(MRJobConfig.MapInputPath, split.GetLength(idx));
				curReader = rrConstructor.NewInstance(new object[] { split, context, Sharpen.Extensions.ValueOf
					(idx) });
				if (idx > 0)
				{
					// initialize() for the first RecordReader will be called by MapTask;
					// we're responsible for initializing subsequent RecordReaders.
					curReader.Initialize(split, context);
				}
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
			idx++;
			return true;
		}
	}
}
