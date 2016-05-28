using System;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>Base class for Composite joins returning Tuples of arbitrary Writables.</summary>
	public abstract class JoinRecordReader<K> : CompositeRecordReader<K, Writable, TupleWritable
		>, ComposableRecordReader<K, TupleWritable>
		where K : WritableComparable
	{
		/// <exception cref="System.IO.IOException"/>
		public JoinRecordReader(int id, JobConf conf, int capacity, Type cmpcl)
			: base(id, capacity, cmpcl)
		{
			SetConf(conf);
		}

		/// <summary>
		/// Emit the next set of key, value pairs as defined by the child
		/// RecordReaders and operation associated with this composite RR.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Next(K key, TupleWritable value)
		{
			if (jc.Flush(value))
			{
				WritableUtils.CloneInto(key, jc.Key());
				return true;
			}
			jc.Clear();
			K iterkey = CreateKey();
			PriorityQueue<ComposableRecordReader<K, object>> q = GetRecordReaderQueue();
			while (!q.IsEmpty())
			{
				FillJoinCollector(iterkey);
				jc.Reset(iterkey);
				if (jc.Flush(value))
				{
					WritableUtils.CloneInto(key, jc.Key());
					return true;
				}
				jc.Clear();
			}
			return false;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual TupleWritable CreateValue()
		{
			return CreateInternalValue();
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
			public virtual bool HasNext()
			{
				return this._enclosing.jc.HasNext();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next(TupleWritable val)
			{
				return this._enclosing.jc.Flush(val);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Replay(TupleWritable val)
			{
				return this._enclosing.jc.Replay(val);
			}

			public virtual void Reset()
			{
				this._enclosing.jc.Reset(this._enclosing.jc.Key());
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Add(TupleWritable item)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				this._enclosing.jc.Close();
			}

			public virtual void Clear()
			{
				this._enclosing.jc.Clear();
			}

			internal JoinDelegationIterator(JoinRecordReader<K> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly JoinRecordReader<K> _enclosing;
		}

		public abstract int CompareTo(ComposableRecordReader<K, object> arg1);

		public abstract void Accept(CompositeRecordReader.JoinCollector arg1, K arg2);

		public abstract void Key(K arg1);

		public abstract void Skip(K arg1);
	}
}
