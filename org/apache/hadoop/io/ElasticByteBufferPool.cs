using Sharpen;

namespace org.apache.hadoop.io
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
	public sealed class ElasticByteBufferPool : org.apache.hadoop.io.ByteBufferPool
	{
		private sealed class Key : java.lang.Comparable<org.apache.hadoop.io.ElasticByteBufferPool.Key
			>
		{
			private readonly int capacity;

			private readonly long insertionTime;

			internal Key(int capacity, long insertionTime)
			{
				this.capacity = capacity;
				this.insertionTime = insertionTime;
			}

			public int compareTo(org.apache.hadoop.io.ElasticByteBufferPool.Key other)
			{
				return com.google.common.collect.ComparisonChain.start().compare(capacity, other.
					capacity).compare(insertionTime, other.insertionTime).result();
			}

			public override bool Equals(object rhs)
			{
				if (rhs == null)
				{
					return false;
				}
				try
				{
					org.apache.hadoop.io.ElasticByteBufferPool.Key o = (org.apache.hadoop.io.ElasticByteBufferPool.Key
						)rhs;
					return (compareTo(o) == 0);
				}
				catch (System.InvalidCastException)
				{
					return false;
				}
			}

			public override int GetHashCode()
			{
				return new org.apache.commons.lang.builder.HashCodeBuilder().append(capacity).append
					(insertionTime).toHashCode();
			}
		}

		private readonly System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.ElasticByteBufferPool.Key
			, java.nio.ByteBuffer> buffers = new System.Collections.Generic.SortedDictionary
			<org.apache.hadoop.io.ElasticByteBufferPool.Key, java.nio.ByteBuffer>();

		private readonly System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.ElasticByteBufferPool.Key
			, java.nio.ByteBuffer> directBuffers = new System.Collections.Generic.SortedDictionary
			<org.apache.hadoop.io.ElasticByteBufferPool.Key, java.nio.ByteBuffer>();

		private System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.ElasticByteBufferPool.Key
			, java.nio.ByteBuffer> getBufferTree(bool direct)
		{
			return direct ? directBuffers : buffers;
		}

		public java.nio.ByteBuffer getBuffer(bool direct, int length)
		{
			lock (this)
			{
				System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.ElasticByteBufferPool.Key
					, java.nio.ByteBuffer> tree = getBufferTree(direct);
				System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.ElasticByteBufferPool.Key
					, java.nio.ByteBuffer> entry = tree.ceilingEntry(new org.apache.hadoop.io.ElasticByteBufferPool.Key
					(length, 0));
				if (entry == null)
				{
					return direct ? java.nio.ByteBuffer.allocateDirect(length) : java.nio.ByteBuffer.
						allocate(length);
				}
				Sharpen.Collections.Remove(tree, entry.Key);
				return entry.Value;
			}
		}

		public void putBuffer(java.nio.ByteBuffer buffer)
		{
			lock (this)
			{
				System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.ElasticByteBufferPool.Key
					, java.nio.ByteBuffer> tree = getBufferTree(buffer.isDirect());
				while (true)
				{
					org.apache.hadoop.io.ElasticByteBufferPool.Key key = new org.apache.hadoop.io.ElasticByteBufferPool.Key
						(buffer.capacity(), Sharpen.Runtime.nanoTime());
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
