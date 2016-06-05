using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// An
	/// <see cref="RecordReader{K, V}"/>
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

		protected internal Configuration conf;

		/// <exception cref="System.IO.IOException"/>
		public SequenceFileRecordReader(Configuration conf, FileSplit split)
		{
			Path path = split.GetPath();
			FileSystem fs = path.GetFileSystem(conf);
			this.@in = new SequenceFile.Reader(fs, path, conf);
			this.end = split.GetStart() + split.GetLength();
			this.conf = conf;
			if (split.GetStart() > @in.GetPosition())
			{
				@in.Sync(split.GetStart());
			}
			// sync to start
			this.start = @in.GetPosition();
			more = start < end;
		}

		/// <summary>
		/// The class of key that must be passed to
		/// <see cref="SequenceFileRecordReader{K, V}.Next(object, object)"/>
		/// ..
		/// </summary>
		public virtual Type GetKeyClass()
		{
			return @in.GetKeyClass();
		}

		/// <summary>
		/// The class of value that must be passed to
		/// <see cref="SequenceFileRecordReader{K, V}.Next(object, object)"/>
		/// ..
		/// </summary>
		public virtual Type GetValueClass()
		{
			return @in.GetValueClass();
		}

		public virtual K CreateKey()
		{
			return (K)ReflectionUtils.NewInstance(GetKeyClass(), conf);
		}

		public virtual V CreateValue()
		{
			return (V)ReflectionUtils.NewInstance(GetValueClass(), conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Next(K key, V value)
		{
			lock (this)
			{
				if (!more)
				{
					return false;
				}
				long pos = @in.GetPosition();
				bool remaining = (@in.Next(key) != null);
				if (remaining)
				{
					GetCurrentValue(value);
				}
				if (pos >= end && @in.SyncSeen())
				{
					more = false;
				}
				else
				{
					more = remaining;
				}
				return more;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool Next(K key)
		{
			lock (this)
			{
				if (!more)
				{
					return false;
				}
				long pos = @in.GetPosition();
				bool remaining = (@in.Next(key) != null);
				if (pos >= end && @in.SyncSeen())
				{
					more = false;
				}
				else
				{
					more = remaining;
				}
				return more;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void GetCurrentValue(V value)
		{
			lock (this)
			{
				@in.GetCurrentValue(value);
			}
		}

		/// <summary>Return the progress within the input split</summary>
		/// <returns>0.0 to 1.0 of the input byte range</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual float GetProgress()
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
		public virtual long GetPos()
		{
			lock (this)
			{
				return @in.GetPosition();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void Seek(long pos)
		{
			lock (this)
			{
				@in.Seek(pos);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				@in.Close();
			}
		}
	}
}
