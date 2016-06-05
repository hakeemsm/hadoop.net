using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>
	/// Base class for Composite join returning values derived from multiple
	/// sources, but generally not tuples.
	/// </summary>
	public abstract class MultiFilterRecordReader<K, V> : CompositeRecordReader<K, V, 
		V>
		where K : WritableComparable<object>
		where V : Writable
	{
		private TupleWritable ivalue = null;

		/// <exception cref="System.IO.IOException"/>
		public MultiFilterRecordReader(int id, Configuration conf, int capacity, Type cmpcl
			)
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
		/// <exception cref="System.Exception"/>
		public override bool NextKeyValue()
		{
			if (key == null)
			{
				key = CreateKey();
			}
			if (value == null)
			{
				value = CreateValue();
			}
			if (jc.Flush(ivalue))
			{
				ReflectionUtils.Copy(conf, jc.Key(), key);
				ReflectionUtils.Copy(conf, Emit(ivalue), value);
				return true;
			}
			if (ivalue == null)
			{
				ivalue = CreateTupleWritable();
			}
			jc.Clear();
			PriorityQueue<ComposableRecordReader<K, object>> q = GetRecordReaderQueue();
			K iterkey = CreateKey();
			while (q != null && !q.IsEmpty())
			{
				FillJoinCollector(iterkey);
				jc.Reset(iterkey);
				if (jc.Flush(ivalue))
				{
					ReflectionUtils.Copy(conf, jc.Key(), key);
					ReflectionUtils.Copy(conf, Emit(ivalue), value);
					return true;
				}
				jc.Clear();
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Initialize(InputSplit split, TaskAttemptContext context)
		{
			base.Initialize(split, context);
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
			public override bool HasNext()
			{
				return this._enclosing.jc.HasNext();
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(V val)
			{
				bool ret;
				if (ret = this._enclosing.jc.Flush(this._enclosing.ivalue))
				{
					ReflectionUtils.Copy(this._enclosing.GetConf(), this._enclosing.Emit(this._enclosing
						.ivalue), val);
				}
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Replay(V val)
			{
				ReflectionUtils.Copy(this._enclosing.GetConf(), this._enclosing.Emit(this._enclosing
					.ivalue), val);
				return true;
			}

			public override void Reset()
			{
				this._enclosing.jc.Reset(this._enclosing.jc.Key());
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Add(V item)
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

			internal MultiFilterDelegationIterator(MultiFilterRecordReader<K, V> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MultiFilterRecordReader<K, V> _enclosing;
		}
	}
}
