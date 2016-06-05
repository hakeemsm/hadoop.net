using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapreduce.RecordReader{KEYIN, VALUEIN}"/>
	/// for
	/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
	/// s.
	/// </summary>
	public class SequenceFileRecordReader<K, V> : RecordReader<K, V>
	{
		private SequenceFile.Reader @in;

		private long start;

		private long end;

		private bool more = true;

		private K key = null;

		private V value = null;

		protected internal Configuration conf;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Initialize(InputSplit split, TaskAttemptContext context)
		{
			FileSplit fileSplit = (FileSplit)split;
			conf = context.GetConfiguration();
			Path path = fileSplit.GetPath();
			FileSystem fs = path.GetFileSystem(conf);
			this.@in = new SequenceFile.Reader(fs, path, conf);
			this.end = fileSplit.GetStart() + fileSplit.GetLength();
			if (fileSplit.GetStart() > @in.GetPosition())
			{
				@in.Sync(fileSplit.GetStart());
			}
			// sync to start
			this.start = @in.GetPosition();
			more = start < end;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override bool NextKeyValue()
		{
			if (!more)
			{
				return false;
			}
			long pos = @in.GetPosition();
			key = (K)@in.Next(key);
			if (key == null || (pos >= end && @in.SyncSeen()))
			{
				more = false;
				key = null;
				value = null;
			}
			else
			{
				value = (V)@in.GetCurrentValue(value);
			}
			return more;
		}

		public override K GetCurrentKey()
		{
			return key;
		}

		public override V GetCurrentValue()
		{
			return value;
		}

		/// <summary>Return the progress within the input split</summary>
		/// <returns>0.0 to 1.0 of the input byte range</returns>
		/// <exception cref="System.IO.IOException"/>
		public override float GetProgress()
		{
			if (end == start)
			{
				return 0.0f;
			}
			else
			{
				return Math.Min(1.0f, (@in.GetPosition() - start) / (float)(end - start));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				@in.Close();
			}
		}
	}
}
