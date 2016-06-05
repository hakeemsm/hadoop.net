using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Lang.Builder;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>This is a simple ByteBufferPool which just creates ByteBuffers as needed.
	/// 	</summary>
	/// <remarks>
	/// This is a simple ByteBufferPool which just creates ByteBuffers as needed.
	/// It also caches ByteBuffers after they're released.  It will always return
	/// the smallest cached buffer with at least the capacity you request.
	/// We don't try to do anything clever here like try to limit the maximum cache
	/// size.
	/// </remarks>
	public sealed class ElasticByteBufferPool : ByteBufferPool
	{
		private sealed class Key : Comparable<ElasticByteBufferPool.Key>
		{
			private readonly int capacity;

			private readonly long insertionTime;

			internal Key(int capacity, long insertionTime)
			{
				this.capacity = capacity;
				this.insertionTime = insertionTime;
			}

			public int CompareTo(ElasticByteBufferPool.Key other)
			{
				return ComparisonChain.Start().Compare(capacity, other.capacity).Compare(insertionTime
					, other.insertionTime).Result();
			}

			public override bool Equals(object rhs)
			{
				if (rhs == null)
				{
					return false;
				}
				try
				{
					ElasticByteBufferPool.Key o = (ElasticByteBufferPool.Key)rhs;
					return (CompareTo(o) == 0);
				}
				catch (InvalidCastException)
				{
					return false;
				}
			}

			public override int GetHashCode()
			{
				return new HashCodeBuilder().Append(capacity).Append(insertionTime).ToHashCode();
			}
		}

		private readonly SortedDictionary<ElasticByteBufferPool.Key, ByteBuffer> buffers = 
			new SortedDictionary<ElasticByteBufferPool.Key, ByteBuffer>();

		private readonly SortedDictionary<ElasticByteBufferPool.Key, ByteBuffer> directBuffers
			 = new SortedDictionary<ElasticByteBufferPool.Key, ByteBuffer>();

		private SortedDictionary<ElasticByteBufferPool.Key, ByteBuffer> GetBufferTree(bool
			 direct)
		{
			return direct ? directBuffers : buffers;
		}

		public ByteBuffer GetBuffer(bool direct, int length)
		{
			lock (this)
			{
				SortedDictionary<ElasticByteBufferPool.Key, ByteBuffer> tree = GetBufferTree(direct
					);
				KeyValuePair<ElasticByteBufferPool.Key, ByteBuffer> entry = tree.CeilingEntry(new 
					ElasticByteBufferPool.Key(length, 0));
				if (entry == null)
				{
					return direct ? ByteBuffer.AllocateDirect(length) : ByteBuffer.Allocate(length);
				}
				Collections.Remove(tree, entry.Key);
				return entry.Value;
			}
		}

		public void PutBuffer(ByteBuffer buffer)
		{
			lock (this)
			{
				SortedDictionary<ElasticByteBufferPool.Key, ByteBuffer> tree = GetBufferTree(buffer
					.IsDirect());
				while (true)
				{
					ElasticByteBufferPool.Key key = new ElasticByteBufferPool.Key(buffer.Capacity(), 
						Runtime.NanoTime());
					if (!tree.Contains(key))
					{
						tree[key] = buffer;
						return;
					}
				}
			}
		}
		// Buffers are indexed by (capacity, time).
		// If our key is not unique on the first try, we try again, since the
		// time will be different.  Since we use nanoseconds, it's pretty
		// unlikely that we'll loop even once, unless the system clock has a
		// poor granularity.
	}
}
