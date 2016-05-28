using System;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>
	/// Base class for Composite join returning values derived from multiple
	/// sources, but generally not tuples.
	/// </summary>
	public abstract class MultiFilterRecordReader<K, V> : CompositeRecordReader<K, V, 
		V>, ComposableRecordReader<K, V>
		where K : WritableComparable
		where V : Writable
	{
		private Type valueclass;

		private TupleWritable ivalue;

		/// <exception cref="System.IO.IOException"/>
		public MultiFilterRecordReader(int id, JobConf conf, int capacity, Type cmpcl)
			: base(id, capacity, cmpcl)
		{
			SetConf(conf);
		}

		/// <summary>
		/// For each tuple emitted, return a value (typically one of the values
		/// in the tuple).
		/// </summary>
		/// <remarks>
		/// For each tuple emitted, return a value (typically one of the values
		/// in the tuple).
		/// Modifying the Writables in the tuple is permitted and unlikely to affect
		/// join behavior in most cases, but it is not recommended. It's safer to
		/// clone first.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract V Emit(TupleWritable dst);

		/// <summary>
		/// Default implementation offers
		/// <see cref="MultiFilterRecordReader{K, V}.Emit(TupleWritable)"/>
		/// every Tuple from the
		/// collector (the outer join of child RRs).
		/// </summary>
		protected internal override bool Combine(object[] srcs, TupleWritable dst)
		{
			return true;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Next(K key, V value)
		{
			if (jc.Flush(ivalue))
			{
				WritableUtils.CloneInto(key, jc.Key());
				WritableUtils.CloneInto(value, Emit(ivalue));
				return true;
			}
			jc.Clear();
			K iterkey = CreateKey();
			PriorityQueue<ComposableRecordReader<K, object>> q = GetRecordReaderQueue();
			while (!q.IsEmpty())
			{
				FillJoinCollector(iterkey);
				jc.Reset(iterkey);
				if (jc.Flush(ivalue))
				{
					WritableUtils.CloneInto(key, jc.Key());
					WritableUtils.CloneInto(value, Emit(ivalue));
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
		public virtual V CreateValue()
		{
			// Explicit check for value class agreement
			if (null == valueclass)
			{
				Type cls = kids[0].CreateValue().GetType();
				foreach (RecordReader<K, V> rr in kids)
				{
					if (!cls.Equals(rr.CreateValue().GetType()))
					{
						throw new InvalidCastException("Child value classes fail to agree");
					}
				}
				valueclass = cls.AsSubclass<Writable>();
				ivalue = CreateInternalValue();
			}
			return (V)ReflectionUtils.NewInstance(valueclass, null);
		}

		/// <summary>Return an iterator returning a single value from the tuple.</summary>
		/// <seealso cref="MultiFilterDelegationIterator"/>
		protected internal override ResetableIterator<V> GetDelegate()
		{
			return new MultiFilterRecordReader.MultiFilterDelegationIterator(this);
		}

		/// <summary>Proxy the JoinCollector, but include callback to emit.</summary>
		protected internal class MultiFilterDelegationIterator : ResetableIterator<V>
		{
			public virtual bool HasNext()
			{
				return this._enclosing.jc.HasNext();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next(V val)
			{
				bool ret;
				if (ret = this._enclosing.jc.Flush(this._enclosing.ivalue))
				{
					WritableUtils.CloneInto(val, this._enclosing.Emit(this._enclosing.ivalue));
				}
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Replay(V val)
			{
				WritableUtils.CloneInto(val, this._enclosing.Emit(this._enclosing.ivalue));
				return true;
			}

			public virtual void Reset()
			{
				this._enclosing.jc.Reset(this._enclosing.jc.Key());
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Add(V item)
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

			internal MultiFilterDelegationIterator(MultiFilterRecordReader<K, V> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MultiFilterRecordReader<K, V> _enclosing;
		}

		public abstract int CompareTo(ComposableRecordReader<K, object> arg1);

		public abstract void Accept(CompositeRecordReader.JoinCollector arg1, K arg2);

		public abstract void Key(K arg1);

		public abstract void Skip(K arg1);
	}
}
