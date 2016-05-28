using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>Proxy class for a RecordReader participating in the join framework.</summary>
	/// <remarks>
	/// Proxy class for a RecordReader participating in the join framework.
	/// This class keeps track of the &quot;head&quot; key-value pair for the
	/// provided RecordReader and keeps a store of values matching a key when
	/// this source is participating in a join.
	/// </remarks>
	public class WrappedRecordReader<K, U> : ComposableRecordReader<K, U>, Configurable
		where K : WritableComparable
		where U : Writable
	{
		private bool empty = false;

		private RecordReader<K, U> rr;

		private int id;

		private K khead;

		private U vhead;

		private WritableComparator cmp;

		private Configuration conf;

		private ResetableIterator<U> vjoin;

		/// <summary>For a given RecordReader rr, occupy position id in collector.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal WrappedRecordReader(int id, RecordReader<K, U> rr, Type cmpcl)
			: this(id, rr, cmpcl, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal WrappedRecordReader(int id, RecordReader<K, U> rr, Type cmpcl, Configuration
			 conf)
		{
			// index at which values will be inserted in collector
			// key at the top of this RR
			// value assoc with khead
			this.id = id;
			this.rr = rr;
			this.conf = (conf == null) ? new Configuration() : conf;
			khead = rr.CreateKey();
			vhead = rr.CreateValue();
			try
			{
				cmp = (null == cmpcl) ? WritableComparator.Get(khead.GetType(), this.conf) : System.Activator.CreateInstance
					(cmpcl);
			}
			catch (InstantiationException e)
			{
				throw (IOException)Sharpen.Extensions.InitCause(new IOException(), e);
			}
			catch (MemberAccessException e)
			{
				throw (IOException)Sharpen.Extensions.InitCause(new IOException(), e);
			}
			vjoin = new StreamBackedIterator<U>();
			Next();
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual int Id()
		{
			return id;
		}

		/// <summary>Return the key at the head of this RR.</summary>
		public virtual K Key()
		{
			return khead;
		}

		/// <summary>Clone the key at the head of this RR into the object supplied.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Key(K qkey)
		{
			WritableUtils.CloneInto(qkey, khead);
		}

		/// <summary>
		/// Return true if the RR- including the k,v pair stored in this object-
		/// is exhausted.
		/// </summary>
		public virtual bool HasNext()
		{
			return !empty;
		}

		/// <summary>Skip key-value pairs with keys less than or equal to the key provided.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Skip(K key)
		{
			if (HasNext())
			{
				while (cmp.Compare(khead, key) <= 0 && Next())
				{
				}
			}
		}

		/// <summary>
		/// Read the next k,v pair into the head of this object; return true iff
		/// the RR and this are exhausted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool Next()
		{
			empty = !rr.Next(khead, vhead);
			return HasNext();
		}

		/// <summary>
		/// Add an iterator to the collector at the position occupied by this
		/// RecordReader over the values in this stream paired with the key
		/// provided (ie register a stream of values from this source matching K
		/// with a collector).
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Accept(CompositeRecordReader.JoinCollector i, K key)
		{
			// JoinCollector comes from parent, which has
			// no static type for the slot this sits in
			vjoin.Clear();
			if (0 == cmp.Compare(key, khead))
			{
				do
				{
					vjoin.Add(vhead);
				}
				while (Next() && 0 == cmp.Compare(key, khead));
			}
			i.Add(id, vjoin);
		}

		/// <summary>
		/// Write key-value pair at the head of this stream to the objects provided;
		/// get next key-value pair from proxied RR.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool Next(K key, U value)
		{
			if (HasNext())
			{
				WritableUtils.CloneInto(key, khead);
				WritableUtils.CloneInto(value, vhead);
				Next();
				return true;
			}
			return false;
		}

		/// <summary>Request new key from proxied RR.</summary>
		public virtual K CreateKey()
		{
			return rr.CreateKey();
		}

		/// <summary>Request new value from proxied RR.</summary>
		public virtual U CreateValue()
		{
			return rr.CreateValue();
		}

		/// <summary>Request progress from proxied RR.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual float GetProgress()
		{
			return rr.GetProgress();
		}

		/// <summary>Request position from proxied RR.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			return rr.GetPos();
		}

		/// <summary>Forward close request to proxied RR.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			rr.Close();
		}

		/// <summary>
		/// Implement Comparable contract (compare key at head of proxied RR
		/// with that of another).
		/// </summary>
		public virtual int CompareTo(ComposableRecordReader<K, object> other)
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

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}
	}
}
