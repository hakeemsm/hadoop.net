using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>Proxy class for a RecordReader participating in the join framework.</summary>
	/// <remarks>
	/// Proxy class for a RecordReader participating in the join framework.
	/// This class keeps track of the &quot;head&quot; key-value pair for the
	/// provided RecordReader and keeps a store of values matching a key when
	/// this source is participating in a join.
	/// </remarks>
	public class WrappedRecordReader<K, U> : ComposableRecordReader<K, U>
		where K : WritableComparable<object>
		where U : Writable
	{
		protected internal bool empty = false;

		private RecordReader<K, U> rr;

		private int id;

		protected internal WritableComparator cmp = null;

		private K key;

		private U value;

		private ResetableIterator<U> vjoin;

		private Configuration conf = new Configuration();

		private Type keyclass = null;

		private Type valueclass = null;

		protected internal WrappedRecordReader(int id)
		{
			// index at which values will be inserted in collector
			// key at the top of this RR
			// value assoc with key
			this.id = id;
			vjoin = new StreamBackedIterator<U>();
		}

		/// <summary>For a given RecordReader rr, occupy position id in collector.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal WrappedRecordReader(int id, RecordReader<K, U> rr, Type cmpcl)
		{
			this.id = id;
			this.rr = rr;
			if (cmpcl != null)
			{
				try
				{
					this.cmp = System.Activator.CreateInstance(cmpcl);
				}
				catch (InstantiationException e)
				{
					throw new IOException(e);
				}
				catch (MemberAccessException e)
				{
					throw new IOException(e);
				}
			}
			vjoin = new StreamBackedIterator<U>();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Initialize(InputSplit split, TaskAttemptContext context)
		{
			rr.Initialize(split, context);
			conf = context.GetConfiguration();
			NextKeyValue();
			if (!empty)
			{
				keyclass = key.GetType().AsSubclass<WritableComparable>();
				valueclass = value.GetType();
				if (cmp == null)
				{
					cmp = WritableComparator.Get(keyclass, conf);
				}
			}
		}

		/// <summary>Request new key from proxied RR.</summary>
		internal override K CreateKey()
		{
			if (keyclass != null)
			{
				return (K)ReflectionUtils.NewInstance(keyclass, conf);
			}
			return (K)NullWritable.Get();
		}

		internal override U CreateValue()
		{
			if (valueclass != null)
			{
				return (U)ReflectionUtils.NewInstance(valueclass, conf);
			}
			return (U)NullWritable.Get();
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		internal override int Id()
		{
			return id;
		}

		/// <summary>Return the key at the head of this RR.</summary>
		internal override K Key()
		{
			return key;
		}

		/// <summary>Clone the key at the head of this RR into the object supplied.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal override void Key(K qkey)
		{
			ReflectionUtils.Copy(conf, key, qkey);
		}

		/// <summary>
		/// Return true if the RR- including the k,v pair stored in this object-
		/// is exhausted.
		/// </summary>
		internal override bool HasNext()
		{
			return !empty;
		}

		/// <summary>Skip key-value pairs with keys less than or equal to the key provided.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal override void Skip(K key)
		{
			if (HasNext())
			{
				while (cmp.Compare(Key(), key) <= 0 && Next())
				{
				}
			}
		}

		/// <summary>
		/// Add an iterator to the collector at the position occupied by this
		/// RecordReader over the values in this stream paired with the key
		/// provided (ie register a stream of values from this source matching K
		/// with a collector).
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal override void Accept(CompositeRecordReader.JoinCollector i, K key)
		{
			vjoin.Clear();
			if (Key() != null && 0 == cmp.Compare(key, Key()))
			{
				do
				{
					vjoin.Add(value);
				}
				while (Next() && 0 == cmp.Compare(key, Key()));
			}
			i.Add(id, vjoin);
		}

		/// <summary>
		/// Read the next k,v pair into the head of this object; return true iff
		/// the RR and this are exhausted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override bool NextKeyValue()
		{
			if (HasNext())
			{
				Next();
				return true;
			}
			return false;
		}

		/// <summary>
		/// Read the next k,v pair into the head of this object; return true iff
		/// the RR and this are exhausted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private bool Next()
		{
			empty = !rr.NextKeyValue();
			key = rr.GetCurrentKey();
			value = rr.GetCurrentValue();
			return !empty;
		}

		/// <summary>Get current key</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override K GetCurrentKey()
		{
			return rr.GetCurrentKey();
		}

		/// <summary>Get current value</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override U GetCurrentValue()
		{
			return rr.GetCurrentValue();
		}

		/// <summary>Request progress from proxied RR.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override float GetProgress()
		{
			return rr.GetProgress();
		}

		/// <summary>Forward close request to proxied RR.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			rr.Close();
		}

		/// <summary>
		/// Implement Comparable contract (compare key at head of proxied RR
		/// with that of another).
		/// </summary>
		public override int CompareTo(ComposableRecordReader<K, object> other)
		{
			return cmp.Compare(Key(), other.Key());
		}

		/// <summary>Return true iff compareTo(other) retn true.</summary>
		public override bool Equals(object other)
		{
			// Explicit type check prior to cast
			return other is ComposableRecordReader && 0 == CompareTo((ComposableRecordReader)
				other);
		}

		public override int GetHashCode()
		{
			System.Diagnostics.Debug.Assert(false, "hashCode not designed");
			return 42;
		}
	}
}
