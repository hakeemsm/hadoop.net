using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>Base class for Composite joins returning Tuples of arbitrary Writables.</summary>
	public abstract class JoinRecordReader<K> : CompositeRecordReader<K, Writable, TupleWritable
		>
		where K : WritableComparable<object>
	{
		/// <exception cref="System.IO.IOException"/>
		public JoinRecordReader(int id, Configuration conf, int capacity, Type cmpcl)
			: base(id, capacity, cmpcl)
		{
			SetConf(conf);
		}

		/// <summary>
		/// Emit the next set of key, value pairs as defined by the child
		/// RecordReaders and operation associated with this composite RR.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override bool NextKeyValue()
		{
			if (key == null)
			{
				key = CreateKey();
			}
			if (jc.Flush(value))
			{
				ReflectionUtils.Copy(conf, jc.Key(), key);
				return true;
			}
			jc.Clear();
			if (value == null)
			{
				value = CreateValue();
			}
			PriorityQueue<ComposableRecordReader<K, object>> q = GetRecordReaderQueue();
			K iterkey = CreateKey();
			while (q != null && !q.IsEmpty())
			{
				FillJoinCollector(iterkey);
				jc.Reset(iterkey);
				if (jc.Flush(value))
				{
					ReflectionUtils.Copy(conf, jc.Key(), key);
					return true;
				}
				jc.Clear();
			}
			return false;
		}

		internal override TupleWritable CreateValue()
		{
			return CreateTupleWritable();
		}

		/// <summary>Return an iterator wrapping the JoinCollector.</summary>
		protected internal override ResetableIterator<TupleWritable> GetDelegate()
		{
			return new JoinRecordReader.JoinDelegationIterator(this);
		}

		/// <summary>
		/// Since the JoinCollector is effecting our operation, we need only
		/// provide an iterator proxy wrapping its operation.
		/// </summary>
		protected internal class JoinDelegationIterator : ResetableIterator<TupleWritable
			>
		{
			public override bool HasNext()
			{
				return this._enclosing.jc.HasNext();
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(TupleWritable val)
			{
				return this._enclosing.jc.Flush(val);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Replay(TupleWritable val)
			{
				return this._enclosing.jc.Replay(val);
			}

			public override void Reset()
			{
				this._enclosing.jc.Reset(this._enclosing.jc.Key());
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Add(TupleWritable item)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				this._enclosing.jc.Close();
			}

			public override void Clear()
			{
				this._enclosing.jc.Clear();
			}

			internal JoinDelegationIterator(JoinRecordReader<K> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly JoinRecordReader<K> _enclosing;
		}
	}
}
