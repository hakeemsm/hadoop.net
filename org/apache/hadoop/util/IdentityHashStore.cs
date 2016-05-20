using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>The IdentityHashStore stores (key, value) mappings in an array.</summary>
	/// <remarks>
	/// The IdentityHashStore stores (key, value) mappings in an array.
	/// It is similar to java.util.HashTable, but much more lightweight.
	/// Neither inserting nor removing an element ever leads to any garbage
	/// getting created (assuming the array doesn't need to be enlarged).
	/// Unlike HashTable, it compares keys using
	/// <see cref="Sharpen.Runtime.identityHashCode(object)"/>
	/// and the identity operator.
	/// This is useful for types like ByteBuffer which have expensive hashCode
	/// and equals operators.
	/// We use linear probing to resolve collisions.  This avoids the need for
	/// the overhead of linked list data structures.  It also means that it is
	/// expensive to attempt to remove an element that isn't there, since we
	/// have to look at the entire array to be sure that it doesn't exist.
	/// </remarks>
	/// <?/>
	/// <?/>
	public sealed class IdentityHashStore<K, V>
	{
		/// <summary>Even elements are keys; odd elements are values.</summary>
		/// <remarks>
		/// Even elements are keys; odd elements are values.
		/// The array has size 1 + Math.pow(2, capacity).
		/// </remarks>
		private object[] buffer;

		private int numInserted = 0;

		private int capacity;

		/// <summary>The default maxCapacity value to use.</summary>
		private const int DEFAULT_MAX_CAPACITY = 2;

		public IdentityHashStore(int capacity)
		{
			com.google.common.@base.Preconditions.checkArgument(capacity >= 0);
			if (capacity == 0)
			{
				this.capacity = 0;
				this.buffer = null;
			}
			else
			{
				// Round the capacity we need up to a power of 2.
				realloc((int)System.Math.pow(2, System.Math.ceil(System.Math.log(capacity) / System.Math
					.log(2))));
			}
		}

		private void realloc(int newCapacity)
		{
			com.google.common.@base.Preconditions.checkArgument(newCapacity > 0);
			object[] prevBuffer = buffer;
			this.capacity = newCapacity;
			// Each element takes two array slots -- one for the key, 
			// and another for the value.  We also want a load factor 
			// of 0.50.  Combine those together and you get 4 * newCapacity.
			this.buffer = new object[4 * newCapacity];
			this.numInserted = 0;
			if (prevBuffer != null)
			{
				for (int i = 0; i < prevBuffer.Length; i += 2)
				{
					if (prevBuffer[i] != null)
					{
						putInternal(prevBuffer[i], prevBuffer[i + 1]);
					}
				}
			}
		}

		private void putInternal(object k, object v)
		{
			int hash = Sharpen.Runtime.identityHashCode(k);
			int numEntries = buffer.Length >> 1;
			//computing modulo with the assumption buffer.length is power of 2
			int index = hash & (numEntries - 1);
			while (true)
			{
				if (buffer[2 * index] == null)
				{
					buffer[2 * index] = k;
					buffer[1 + (2 * index)] = v;
					numInserted++;
					return;
				}
				index = (index + 1) % numEntries;
			}
		}

		/// <summary>Add a new (key, value) mapping.</summary>
		/// <remarks>
		/// Add a new (key, value) mapping.
		/// Inserting a new (key, value) never overwrites a previous one.
		/// In other words, you can insert the same key multiple times and it will
		/// lead to multiple entries.
		/// </remarks>
		public void put(K k, V v)
		{
			com.google.common.@base.Preconditions.checkNotNull(k);
			if (buffer == null)
			{
				realloc(DEFAULT_MAX_CAPACITY);
			}
			else
			{
				if (numInserted + 1 > capacity)
				{
					realloc(capacity * 2);
				}
			}
			putInternal(k, v);
		}

		private int getElementIndex(K k)
		{
			if (buffer == null)
			{
				return -1;
			}
			int numEntries = buffer.Length >> 1;
			int hash = Sharpen.Runtime.identityHashCode(k);
			//computing modulo with the assumption buffer.length is power of 2
			int index = hash & (numEntries - 1);
			int firstIndex = index;
			do
			{
				if (buffer[2 * index] == k)
				{
					return index;
				}
				index = (index + 1) % numEntries;
			}
			while (index != firstIndex);
			return -1;
		}

		/// <summary>Retrieve a value associated with a given key.</summary>
		public V get(K k)
		{
			int index = getElementIndex(k);
			if (index < 0)
			{
				return null;
			}
			return (V)buffer[1 + (2 * index)];
		}

		/// <summary>
		/// Retrieve a value associated with a given key, and delete the
		/// relevant entry.
		/// </summary>
		public V remove(K k)
		{
			int index = getElementIndex(k);
			if (index < 0)
			{
				return null;
			}
			V val = (V)buffer[1 + (2 * index)];
			buffer[2 * index] = null;
			buffer[1 + (2 * index)] = null;
			numInserted--;
			return val;
		}

		public bool isEmpty()
		{
			return numInserted == 0;
		}

		public int numElements()
		{
			return numInserted;
		}

		public int capacity()
		{
			return capacity;
		}

		public interface Visitor<K, V>
		{
			void accept(K k, V v);
		}

		/// <summary>Visit all key, value pairs in the IdentityHashStore.</summary>
		public void visitAll(org.apache.hadoop.util.IdentityHashStore.Visitor<K, V> visitor
			)
		{
			int length = buffer == null ? 0 : buffer.Length;
			for (int i = 0; i < length; i += 2)
			{
				if (buffer[i] != null)
				{
					visitor.accept((K)buffer[i], (V)buffer[i + 1]);
				}
			}
		}
	}
}
