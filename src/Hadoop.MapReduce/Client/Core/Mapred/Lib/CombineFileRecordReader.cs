using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Mapred.Lib
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
			), typeof(Configuration), typeof(Reporter), typeof(int) };

		protected internal CombineFileSplit split;

		protected internal JobConf jc;

		protected internal Reporter reporter;

		protected internal Constructor<RecordReader<K, V>> rrConstructor;

		protected internal int idx;

		protected internal long progress;

		protected internal RecordReader<K, V> curReader;

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Next(K key, V value)
		{
			while ((curReader == null) || !curReader.Next(key, value))
			{
				if (!InitNextRecordReader())
				{
					return false;
				}
			}
			return true;
		}

		public virtual K CreateKey()
		{
			return curReader.CreateKey();
		}

		public virtual V CreateValue()
		{
			return curReader.CreateValue();
		}

		/// <summary>return the amount of data processed</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			return progress;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			if (curReader != null)
			{
				curReader.Close();
				curReader = null;
			}
		}

		/// <summary>return progress based on the amount of data processed so far.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual float GetProgress()
		{
			return Math.Min(1.0f, progress / (float)(split.GetLength()));
		}

		/// <summary>
		/// A generic RecordReader that can hand out different recordReaders
		/// for each chunk in the CombineFileSplit.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public CombineFileRecordReader(JobConf job, CombineFileSplit split, Reporter reporter
			, Type rrClass)
		{
			this.split = split;
			this.jc = job;
			this.reporter = reporter;
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
			reporter.Progress();
			// get a record reader for the idx-th chunk
			try
			{
				curReader = rrConstructor.NewInstance(new object[] { split, jc, reporter, Sharpen.Extensions.ValueOf
					(idx) });
				// setup some helper config variables.
				jc.Set(JobContext.MapInputFile, split.GetPath(idx).ToString());
				jc.SetLong(JobContext.MapInputStart, split.GetOffset(idx));
				jc.SetLong(JobContext.MapInputPath, split.GetLength(idx));
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
