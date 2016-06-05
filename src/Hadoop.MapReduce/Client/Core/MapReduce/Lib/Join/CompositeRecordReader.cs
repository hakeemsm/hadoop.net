using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>
	/// A RecordReader that can effect joins of RecordReaders sharing a common key
	/// type and partitioning.
	/// </summary>
	public abstract class CompositeRecordReader<K, V, X> : ComposableRecordReader<K, 
		X>, Configurable
		where K : WritableComparable<object>
		where V : Writable
		where X : Writable
	{
		private int id;

		protected internal Configuration conf;

		private readonly ResetableIterator<X> Empty = new ResetableIterator.EMPTY<X>();

		private WritableComparator cmp;

		protected internal Type keyclass = null;

		private PriorityQueue<ComposableRecordReader<K, object>> q;

		protected internal readonly CompositeRecordReader.JoinCollector jc;

		protected internal readonly ComposableRecordReader<K, V>[] kids;

		// key type
		// accepts RecordReader<K,V> as children
		// emits Writables of this type
		protected internal abstract bool Combine(object[] srcs, TupleWritable value);

		protected internal K key;

		protected internal X value;

		/// <summary>
		/// Create a RecordReader with <tt>capacity</tt> children to position
		/// <tt>id</tt> in the parent reader.
		/// </summary>
		/// <remarks>
		/// Create a RecordReader with <tt>capacity</tt> children to position
		/// <tt>id</tt> in the parent reader.
		/// The id of a root CompositeRecordReader is -1 by convention, but relying
		/// on this is not recommended.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public CompositeRecordReader(int id, int capacity, Type cmpcl)
		{
			// Generic array assignment
			System.Diagnostics.Debug.Assert(capacity > 0, "Invalid capacity");
			this.id = id;
			if (null != cmpcl)
			{
				cmp = ReflectionUtils.NewInstance(cmpcl, null);
				q = new PriorityQueue<ComposableRecordReader<K, object>>(3, new _IComparer_84(this
					));
			}
			jc = new CompositeRecordReader.JoinCollector(this, capacity);
			kids = new ComposableRecordReader[capacity];
		}

		private sealed class _IComparer_84 : IComparer<ComposableRecordReader<K, object>>
		{
			public _IComparer_84(CompositeRecordReader<K, V, X> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public int Compare(ComposableRecordReader<K, object> o1, ComposableRecordReader<K
				, object> o2)
			{
				return this._enclosing.cmp.Compare(o1.Key(), o2.Key());
			}

			private readonly CompositeRecordReader<K, V, X> _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Initialize(InputSplit split, TaskAttemptContext context)
		{
			if (kids != null)
			{
				for (int i = 0; i < kids.Length; ++i)
				{
					kids[i].Initialize(((CompositeInputSplit)split).Get(i), context);
					if (kids[i].Key() == null)
					{
						continue;
					}
					// get keyclass
					if (keyclass == null)
					{
						keyclass = kids[i].CreateKey().GetType().AsSubclass<WritableComparable>();
					}
					// create priority queue
					if (null == q)
					{
						cmp = WritableComparator.Get(keyclass, conf);
						q = new PriorityQueue<ComposableRecordReader<K, object>>(3, new _IComparer_114(this
							));
					}
					// Explicit check for key class agreement
					if (!keyclass.Equals(kids[i].Key().GetType()))
					{
						throw new InvalidCastException("Child key classes fail to agree");
					}
					// add the kid to priority queue if it has any elements
					if (kids[i].HasNext())
					{
						q.AddItem(kids[i]);
					}
				}
			}
		}

		private sealed class _IComparer_114 : IComparer<ComposableRecordReader<K, object>
			>
		{
			public _IComparer_114(CompositeRecordReader<K, V, X> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public int Compare(ComposableRecordReader<K, object> o1, ComposableRecordReader<K
				, object> o2)
			{
				return this._enclosing.cmp.Compare(o1.Key(), o2.Key());
			}

			private readonly CompositeRecordReader<K, V, X> _enclosing;
		}

		/// <summary>Return the position in the collector this class occupies.</summary>
		internal override int Id()
		{
			return id;
		}

		/// <summary><inheritDoc/></summary>
		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		/// <summary><inheritDoc/></summary>
		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>Return sorted list of RecordReaders for this composite.</summary>
		protected internal virtual PriorityQueue<ComposableRecordReader<K, object>> GetRecordReaderQueue
			()
		{
			return q;
		}

		/// <summary>
		/// Return comparator defining the ordering for RecordReaders in this
		/// composite.
		/// </summary>
		protected internal virtual WritableComparator GetComparator()
		{
			return cmp;
		}

		/// <summary>Add a RecordReader to this collection.</summary>
		/// <remarks>
		/// Add a RecordReader to this collection.
		/// The id() of a RecordReader determines where in the Tuple its
		/// entry will appear. Adding RecordReaders with the same id has
		/// undefined behavior.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Add(ComposableRecordReader<K, V> rr)
		{
			kids[rr.Id()] = rr;
		}

		/// <summary>Collector for join values.</summary>
		/// <remarks>
		/// Collector for join values.
		/// This accumulates values for a given key from the child RecordReaders. If
		/// one or more child RR contain duplicate keys, this will emit the cross
		/// product of the associated values until exhausted.
		/// </remarks>
		public class JoinCollector
		{
			private K key;

			private ResetableIterator<X>[] iters;

			private int pos = -1;

			private bool first = true;

			/// <summary>
			/// Construct a collector capable of handling the specified number of
			/// children.
			/// </summary>
			public JoinCollector(CompositeRecordReader<K, V, X> _enclosing, int card)
			{
				this._enclosing = _enclosing;
				// Generic array assignment
				this.iters = new ResetableIterator[card];
				for (int i = 0; i < this.iters.Length; ++i)
				{
					this.iters[i] = this._enclosing.Empty;
				}
			}

			/// <summary>Register a given iterator at position id.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Add(int id, ResetableIterator<X> i)
			{
				this.iters[id] = i;
			}

			/// <summary>Return the key associated with this collection.</summary>
			public virtual K Key()
			{
				return this.key;
			}

			/// <summary>Codify the contents of the collector to be iterated over.</summary>
			/// <remarks>
			/// Codify the contents of the collector to be iterated over.
			/// When this is called, all RecordReaders registered for this
			/// key should have added ResetableIterators.
			/// </remarks>
			public virtual void Reset(K key)
			{
				this.key = key;
				this.first = true;
				this.pos = this.iters.Length - 1;
				for (int i = 0; i < this.iters.Length; ++i)
				{
					this.iters[i].Reset();
				}
			}

			/// <summary>Clear all state information.</summary>
			public virtual void Clear()
			{
				this.key = null;
				this.pos = -1;
				for (int i = 0; i < this.iters.Length; ++i)
				{
					this.iters[i].Clear();
					this.iters[i] = this._enclosing.Empty;
				}
			}

			/// <summary>Returns false if exhausted or if reset(K) has not been called.</summary>
			public virtual bool HasNext()
			{
				return !(this.pos < 0);
			}

			/// <summary>Populate Tuple from iterators.</summary>
			/// <remarks>
			/// Populate Tuple from iterators.
			/// It should be the case that, given iterators i_1...i_n over values from
			/// sources s_1...s_n sharing key k, repeated calls to next should yield
			/// I x I.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			protected internal virtual bool Next(TupleWritable val)
			{
				// No static type info on Tuples
				if (this.first)
				{
					int i = -1;
					for (this.pos = 0; this.pos < this.iters.Length; ++this.pos)
					{
						if (this.iters[this.pos].HasNext() && this.iters[this.pos].Next((X)val.Get(this.pos
							)))
						{
							i = this.pos;
							val.SetWritten(i);
						}
					}
					this.pos = i;
					this.first = false;
					if (this.pos < 0)
					{
						this.Clear();
						return false;
					}
					return true;
				}
				while (0 <= this.pos && !(this.iters[this.pos].HasNext() && this.iters[this.pos].
					Next((X)val.Get(this.pos))))
				{
					--this.pos;
				}
				if (this.pos < 0)
				{
					this.Clear();
					return false;
				}
				val.SetWritten(this.pos);
				for (int i_1 = 0; i_1 < this.pos; ++i_1)
				{
					if (this.iters[i_1].Replay((X)val.Get(i_1)))
					{
						val.SetWritten(i_1);
					}
				}
				while (this.pos + 1 < this.iters.Length)
				{
					++this.pos;
					this.iters[this.pos].Reset();
					if (this.iters[this.pos].HasNext() && this.iters[this.pos].Next((X)val.Get(this.pos
						)))
					{
						val.SetWritten(this.pos);
					}
				}
				return true;
			}

			/// <summary>Replay the last Tuple emitted.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool Replay(TupleWritable val)
			{
				// No static typeinfo on Tuples
				// The last emitted tuple might have drawn on an empty source;
				// it can't be cleared prematurely, b/c there may be more duplicate
				// keys in iterator positions < pos
				System.Diagnostics.Debug.Assert(!this.first);
				bool ret = false;
				for (int i = 0; i < this.iters.Length; ++i)
				{
					if (this.iters[i].Replay((X)val.Get(i)))
					{
						val.SetWritten(i);
						ret = true;
					}
				}
				return ret;
			}

			/// <summary>Close all child iterators.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				for (int i = 0; i < this.iters.Length; ++i)
				{
					this.iters[i].Close();
				}
			}

			/// <summary>
			/// Write the next value into key, value as accepted by the operation
			/// associated with this set of RecordReaders.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool Flush(TupleWritable value)
			{
				while (this.HasNext())
				{
					value.ClearWritten();
					if (this.Next(value) && this._enclosing.Combine(this._enclosing.kids, value))
					{
						return true;
					}
				}
				return false;
			}

			private readonly CompositeRecordReader<K, V, X> _enclosing;
		}

		/// <summary>
		/// Return the key for the current join or the value at the top of the
		/// RecordReader heap.
		/// </summary>
		internal override K Key()
		{
			if (jc.HasNext())
			{
				return jc.Key();
			}
			if (!q.IsEmpty())
			{
				return q.Peek().Key();
			}
			return null;
		}

		/// <summary>Clone the key at the top of this RR into the given object.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal override void Key(K key)
		{
			ReflectionUtils.Copy(conf, Key(), key);
		}

		public override K GetCurrentKey()
		{
			return key;
		}

		/// <summary>Return true if it is possible that this could emit more values.</summary>
		internal override bool HasNext()
		{
			return jc.HasNext() || !q.IsEmpty();
		}

		/// <summary>Pass skip key to child RRs.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal override void Skip(K key)
		{
			AList<ComposableRecordReader<K, object>> tmp = new AList<ComposableRecordReader<K
				, object>>();
			while (!q.IsEmpty() && cmp.Compare(q.Peek().Key(), key) <= 0)
			{
				tmp.AddItem(q.Poll());
			}
			foreach (ComposableRecordReader<K, object> rr in tmp)
			{
				rr.Skip(key);
				if (rr.HasNext())
				{
					q.AddItem(rr);
				}
			}
		}

		/// <summary>
		/// Obtain an iterator over the child RRs apropos of the value type
		/// ultimately emitted from this join.
		/// </summary>
		protected internal abstract ResetableIterator<X> GetDelegate();

		/// <summary>
		/// If key provided matches that of this Composite, give JoinCollector
		/// iterator over values it may emit.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal override void Accept(CompositeRecordReader.JoinCollector jc, K key)
		{
			// No values from static EMPTY class
			if (HasNext() && 0 == cmp.Compare(key, Key()))
			{
				FillJoinCollector(CreateKey());
				jc.Add(id, GetDelegate());
				return;
			}
			jc.Add(id, Empty);
		}

		/// <summary>
		/// For all child RRs offering the key provided, obtain an iterator
		/// at that position in the JoinCollector.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual void FillJoinCollector(K iterkey)
		{
			if (!q.IsEmpty())
			{
				q.Peek().Key(iterkey);
				while (0 == cmp.Compare(q.Peek().Key(), iterkey))
				{
					ComposableRecordReader<K, object> t = q.Poll();
					t.Accept(jc, iterkey);
					if (t.HasNext())
					{
						q.AddItem(t);
					}
					else
					{
						if (q.IsEmpty())
						{
							return;
						}
					}
				}
			}
		}

		/// <summary>
		/// Implement Comparable contract (compare key of join or head of heap
		/// with that of another).
		/// </summary>
		public override int CompareTo(ComposableRecordReader<K, object> other)
		{
			return cmp.Compare(Key(), other.Key());
		}

		/// <summary>Create a new key common to all child RRs.</summary>
		/// <exception cref="System.InvalidCastException">if key classes differ.</exception>
		internal override K CreateKey()
		{
			if (keyclass == null || keyclass.Equals(typeof(NullWritable)))
			{
				return (K)NullWritable.Get();
			}
			return (K)ReflectionUtils.NewInstance(keyclass, GetConf());
		}

		/// <summary>Create a value to be used internally for joins.</summary>
		protected internal virtual TupleWritable CreateTupleWritable()
		{
			Writable[] vals = new Writable[kids.Length];
			for (int i = 0; i < vals.Length; ++i)
			{
				vals[i] = kids[i].CreateValue();
			}
			return new TupleWritable(vals);
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override X GetCurrentValue()
		{
			return value;
		}

		/// <summary>Close all child RRs.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (kids != null)
			{
				foreach (RecordReader<K, Writable> rr in kids)
				{
					rr.Close();
				}
			}
			if (jc != null)
			{
				jc.Close();
			}
		}

		/// <summary>Report progress as the minimum of all child RR progress.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override float GetProgress()
		{
			float ret = 1.0f;
			foreach (RecordReader<K, Writable> rr in kids)
			{
				ret = Math.Min(ret, rr.GetProgress());
			}
			return ret;
		}
	}
}
