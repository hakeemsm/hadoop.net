using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// A low memory linked hash set implementation, which uses an array for storing
	/// the elements and linked lists for collision resolution.
	/// </summary>
	/// <remarks>
	/// A low memory linked hash set implementation, which uses an array for storing
	/// the elements and linked lists for collision resolution. This class does not
	/// support null element.
	/// This class is not thread safe.
	/// </remarks>
	public class LightWeightHashSet<T> : ICollection<T>
	{
		/// <summary>
		/// Elements of
		/// <see cref="LightWeightLinkedSet{T}"/>
		/// .
		/// </summary>
		internal class LinkedElement<T>
		{
			protected internal readonly T element;

			protected internal LightWeightHashSet.LinkedElement<T> next;

			protected internal readonly int hashCode;

			public LinkedElement(T elem, int hash)
			{
				// reference to the next entry within a bucket linked list
				//hashCode of the element
				this.element = elem;
				this.next = null;
				this.hashCode = hash;
			}

			public override string ToString()
			{
				return element.ToString();
			}
		}

		protected internal const float DefaultMaxLoadFactor = 0.75f;

		protected internal const float DefautMinLoadFactor = 0.2f;

		protected internal const int MinimumCapacity = 16;

		internal const int MaximumCapacity = 1 << 30;

		private static readonly Log Log = LogFactory.GetLog(typeof(LightWeightHashSet));

		/// <summary>An internal array of entries, which are the rows of the hash table.</summary>
		/// <remarks>
		/// An internal array of entries, which are the rows of the hash table. The
		/// size must be a power of two.
		/// </remarks>
		protected internal LightWeightHashSet.LinkedElement<T>[] entries;

		/// <summary>Size of the entry table.</summary>
		private int capacity;

		/// <summary>The size of the set (not the entry array).</summary>
		protected internal int size = 0;

		/// <summary>Hashmask used for determining the bucket index</summary>
		private int hash_mask;

		/// <summary>Capacity at initialization time</summary>
		private readonly int initialCapacity;

		/// <summary>Modification version for fail-fast.</summary>
		/// <seealso cref="Sharpen.ConcurrentModificationException"/>
		protected internal int modification = 0;

		private float maxLoadFactor;

		private float minLoadFactor;

		private readonly int expandMultiplier = 2;

		private int expandThreshold;

		private int shrinkThreshold;

		/// <param name="initCapacity">Recommended size of the internal array.</param>
		/// <param name="maxLoadFactor">used to determine when to expand the internal array</param>
		/// <param name="minLoadFactor">used to determine when to shrink the internal array</param>
		public LightWeightHashSet(int initCapacity, float maxLoadFactor, float minLoadFactor
			)
		{
			if (maxLoadFactor <= 0 || maxLoadFactor > 1.0f)
			{
				throw new ArgumentException("Illegal maxload factor: " + maxLoadFactor);
			}
			if (minLoadFactor <= 0 || minLoadFactor > maxLoadFactor)
			{
				throw new ArgumentException("Illegal minload factor: " + minLoadFactor);
			}
			this.initialCapacity = ComputeCapacity(initCapacity);
			this.capacity = this.initialCapacity;
			this.hash_mask = capacity - 1;
			this.maxLoadFactor = maxLoadFactor;
			this.expandThreshold = (int)(capacity * maxLoadFactor);
			this.minLoadFactor = minLoadFactor;
			this.shrinkThreshold = (int)(capacity * minLoadFactor);
			entries = new LightWeightHashSet.LinkedElement[capacity];
			Log.Debug("initial capacity=" + initialCapacity + ", max load factor= " + maxLoadFactor
				 + ", min load factor= " + minLoadFactor);
		}

		public LightWeightHashSet()
			: this(MinimumCapacity, DefaultMaxLoadFactor, DefautMinLoadFactor)
		{
		}

		public LightWeightHashSet(int minCapacity)
			: this(minCapacity, DefaultMaxLoadFactor, DefautMinLoadFactor)
		{
		}

		/// <summary>Check if the set is empty.</summary>
		/// <returns>true is set empty, false otherwise</returns>
		public virtual bool IsEmpty()
		{
			return size == 0;
		}

		/// <summary>Return the current capacity (for testing).</summary>
		public virtual int GetCapacity()
		{
			return capacity;
		}

		/// <summary>Return the number of stored elements.</summary>
		public virtual int Count
		{
			get
			{
				return size;
			}
		}

		/// <summary>Get index in the internal table for a given hash.</summary>
		protected internal virtual int GetIndex(int hashCode)
		{
			return hashCode & hash_mask;
		}

		/// <summary>Check if the set contains given element</summary>
		/// <returns>true if element present, false otherwise.</returns>
		public virtual bool Contains(object key)
		{
			return GetElement((T)key) != null;
		}

		/// <summary>
		/// Return the element in this set which is equal to
		/// the given key, if such an element exists.
		/// </summary>
		/// <remarks>
		/// Return the element in this set which is equal to
		/// the given key, if such an element exists.
		/// Otherwise returns null.
		/// </remarks>
		public virtual T GetElement(T key)
		{
			// validate key
			if (key == null)
			{
				throw new ArgumentException("Null element is not supported.");
			}
			// find element
			int hashCode = key.GetHashCode();
			int index = GetIndex(hashCode);
			return GetContainedElem(index, key, hashCode);
		}

		/// <summary>Check if the set contains given element at given index.</summary>
		/// <remarks>
		/// Check if the set contains given element at given index. If it
		/// does, return that element.
		/// </remarks>
		/// <returns>the element, or null, if no element matches</returns>
		protected internal virtual T GetContainedElem(int index, T key, int hashCode)
		{
			for (LightWeightHashSet.LinkedElement<T> e = entries[index]; e != null; e = e.next)
			{
				// element found
				if (hashCode == e.hashCode && e.element.Equals(key))
				{
					return e.element;
				}
			}
			// element not found
			return null;
		}

		/// <summary>All all elements in the collection.</summary>
		/// <remarks>All all elements in the collection. Expand if necessary.</remarks>
		/// <param name="toAdd">- elements to add.</param>
		/// <returns>true if the set has changed, false otherwise</returns>
		public virtual bool AddAll<_T0>(ICollection<_T0> toAdd)
			where _T0 : T
		{
			bool changed = false;
			foreach (T elem in toAdd)
			{
				changed |= AddElem(elem);
			}
			ExpandIfNecessary();
			return changed;
		}

		/// <summary>Add given element to the hash table.</summary>
		/// <remarks>Add given element to the hash table. Expand table if necessary.</remarks>
		/// <returns>true if the element was not present in the table, false otherwise</returns>
		public virtual bool AddItem(T element)
		{
			bool added = AddElem(element);
			ExpandIfNecessary();
			return added;
		}

		/// <summary>Add given element to the hash table</summary>
		/// <returns>true if the element was not present in the table, false otherwise</returns>
		protected internal virtual bool AddElem(T element)
		{
			// validate element
			if (element == null)
			{
				throw new ArgumentException("Null element is not supported.");
			}
			// find hashCode & index
			int hashCode = element.GetHashCode();
			int index = GetIndex(hashCode);
			// return false if already present
			if (GetContainedElem(index, element, hashCode) != null)
			{
				return false;
			}
			modification++;
			size++;
			// update bucket linked list
			LightWeightHashSet.LinkedElement<T> le = new LightWeightHashSet.LinkedElement<T>(
				element, hashCode);
			le.next = entries[index];
			entries[index] = le;
			return true;
		}

		/// <summary>Remove the element corresponding to the key.</summary>
		/// <returns>If such element exists, return true. Otherwise, return false.</returns>
		public virtual bool Remove(object key)
		{
			// validate key
			if (key == null)
			{
				throw new ArgumentException("Null element is not supported.");
			}
			LightWeightHashSet.LinkedElement<T> removed = RemoveElem((T)key);
			ShrinkIfNecessary();
			return removed == null ? false : true;
		}

		/// <summary>Remove the element corresponding to the key, given key.hashCode() == index.
		/// 	</summary>
		/// <returns>If such element exists, return true. Otherwise, return false.</returns>
		protected internal virtual LightWeightHashSet.LinkedElement<T> RemoveElem(T key)
		{
			LightWeightHashSet.LinkedElement<T> found = null;
			int hashCode = key.GetHashCode();
			int index = GetIndex(hashCode);
			if (entries[index] == null)
			{
				return null;
			}
			else
			{
				if (hashCode == entries[index].hashCode && entries[index].element.Equals(key))
				{
					// remove the head of the bucket linked list
					modification++;
					size--;
					found = entries[index];
					entries[index] = found.next;
				}
				else
				{
					// head != null and key is not equal to head
					// search the element
					LightWeightHashSet.LinkedElement<T> prev = entries[index];
					for (found = prev.next; found != null; )
					{
						if (hashCode == found.hashCode && found.element.Equals(key))
						{
							// found the element, remove it
							modification++;
							size--;
							prev.next = found.next;
							found.next = null;
							break;
						}
						else
						{
							prev = found;
							found = found.next;
						}
					}
				}
			}
			return found;
		}

		/// <summary>Remove and return n elements from the hashtable.</summary>
		/// <remarks>
		/// Remove and return n elements from the hashtable.
		/// The order in which entries are removed is unspecified, and
		/// and may not correspond to the order in which they were inserted.
		/// </remarks>
		/// <returns>first element</returns>
		public virtual IList<T> PollN(int n)
		{
			if (n >= size)
			{
				return PollAll();
			}
			IList<T> retList = new AList<T>(n);
			if (n == 0)
			{
				return retList;
			}
			bool done = false;
			int currentBucketIndex = 0;
			while (!done)
			{
				LightWeightHashSet.LinkedElement<T> current = entries[currentBucketIndex];
				while (current != null)
				{
					retList.AddItem(current.element);
					current = current.next;
					entries[currentBucketIndex] = current;
					size--;
					modification++;
					if (--n == 0)
					{
						done = true;
						break;
					}
				}
				currentBucketIndex++;
			}
			ShrinkIfNecessary();
			return retList;
		}

		/// <summary>Remove all elements from the set and return them.</summary>
		/// <remarks>Remove all elements from the set and return them. Clear the entries.</remarks>
		public virtual IList<T> PollAll()
		{
			IList<T> retList = new AList<T>(size);
			for (int i = 0; i < entries.Length; i++)
			{
				LightWeightHashSet.LinkedElement<T> current = entries[i];
				while (current != null)
				{
					retList.AddItem(current.element);
					current = current.next;
				}
			}
			this.Clear();
			return retList;
		}

		/// <summary>Get array.length elements from the set, and put them into the array.</summary>
		public virtual T[] PollToArray(T[] array)
		{
			int currentIndex = 0;
			LightWeightHashSet.LinkedElement<T> current = null;
			if (array.Length == 0)
			{
				return array;
			}
			if (array.Length > size)
			{
				array = (T[])System.Array.CreateInstance(array.GetType().GetElementType(), size);
			}
			// do fast polling if the entire set needs to be fetched
			if (array.Length == size)
			{
				for (int i = 0; i < entries.Length; i++)
				{
					current = entries[i];
					while (current != null)
					{
						array[currentIndex++] = current.element;
						current = current.next;
					}
				}
				this.Clear();
				return array;
			}
			bool done = false;
			int currentBucketIndex = 0;
			while (!done)
			{
				current = entries[currentBucketIndex];
				while (current != null)
				{
					array[currentIndex++] = current.element;
					current = current.next;
					entries[currentBucketIndex] = current;
					size--;
					modification++;
					if (currentIndex == array.Length)
					{
						done = true;
						break;
					}
				}
				currentBucketIndex++;
			}
			ShrinkIfNecessary();
			return array;
		}

		/// <summary>Compute capacity given initial capacity.</summary>
		/// <returns>
		/// final capacity, either MIN_CAPACITY, MAX_CAPACITY, or power of 2
		/// closest to the requested capacity.
		/// </returns>
		private int ComputeCapacity(int initial)
		{
			if (initial < MinimumCapacity)
			{
				return MinimumCapacity;
			}
			if (initial > MaximumCapacity)
			{
				return MaximumCapacity;
			}
			int capacity = 1;
			while (capacity < initial)
			{
				capacity <<= 1;
			}
			return capacity;
		}

		/// <summary>Resize the internal table to given capacity.</summary>
		private void Resize(int cap)
		{
			int newCapacity = ComputeCapacity(cap);
			if (newCapacity == this.capacity)
			{
				return;
			}
			this.capacity = newCapacity;
			this.expandThreshold = (int)(capacity * maxLoadFactor);
			this.shrinkThreshold = (int)(capacity * minLoadFactor);
			this.hash_mask = capacity - 1;
			LightWeightHashSet.LinkedElement<T>[] temp = entries;
			entries = new LightWeightHashSet.LinkedElement[capacity];
			for (int i = 0; i < temp.Length; i++)
			{
				LightWeightHashSet.LinkedElement<T> curr = temp[i];
				while (curr != null)
				{
					LightWeightHashSet.LinkedElement<T> next = curr.next;
					int index = GetIndex(curr.hashCode);
					curr.next = entries[index];
					entries[index] = curr;
					curr = next;
				}
			}
		}

		/// <summary>Checks if we need to shrink, and shrinks if necessary.</summary>
		protected internal virtual void ShrinkIfNecessary()
		{
			if (size < this.shrinkThreshold && capacity > initialCapacity)
			{
				Resize(capacity / expandMultiplier);
			}
		}

		/// <summary>Checks if we need to expand, and expands if necessary.</summary>
		protected internal virtual void ExpandIfNecessary()
		{
			if (size > this.expandThreshold && capacity < MaximumCapacity)
			{
				Resize(capacity * expandMultiplier);
			}
		}

		public virtual IEnumerator<T> GetEnumerator()
		{
			return new LightWeightHashSet.LinkedSetIterator(this);
		}

		public override string ToString()
		{
			StringBuilder b = new StringBuilder(GetType().Name);
			b.Append("(size=").Append(size).Append(", modification=").Append(modification).Append
				(", entries.length=").Append(entries.Length).Append(")");
			return b.ToString();
		}

		/// <summary>Print detailed information of this object.</summary>
		public virtual void PrintDetails(TextWriter @out)
		{
			@out.Write(this + ", entries = [");
			for (int i = 0; i < entries.Length; i++)
			{
				if (entries[i] != null)
				{
					LightWeightHashSet.LinkedElement<T> e = entries[i];
					@out.Write("\n  " + i + ": " + e);
					for (e = e.next; e != null; e = e.next)
					{
						@out.Write(" -> " + e);
					}
				}
			}
			@out.WriteLine("\n]");
		}

		private class LinkedSetIterator : IEnumerator<T>
		{
			/// <summary>The starting modification for fail-fast.</summary>
			private readonly int startModification = this._enclosing.modification;

			/// <summary>The current index of the entry array.</summary>
			private int index = -1;

			/// <summary>The next element to return.</summary>
			private LightWeightHashSet.LinkedElement<T> next = this.NextNonemptyEntry();

			private LightWeightHashSet.LinkedElement<T> NextNonemptyEntry()
			{
				for (this.index++; this.index < this._enclosing.entries.Length && this._enclosing
					.entries[this.index] == null; this.index++)
				{
				}
				return this.index < this._enclosing.entries.Length ? this._enclosing.entries[this
					.index] : null;
			}

			public override bool HasNext()
			{
				return this.next != null;
			}

			public override T Next()
			{
				if (this._enclosing.modification != this.startModification)
				{
					throw new ConcurrentModificationException("modification=" + this._enclosing.modification
						 + " != startModification = " + this.startModification);
				}
				if (this.next == null)
				{
					throw new NoSuchElementException();
				}
				T e = this.next.element;
				// find the next element
				LightWeightHashSet.LinkedElement<T> n = this.next.next;
				this.next = n != null ? n : this.NextNonemptyEntry();
				return e;
			}

			public override void Remove()
			{
				throw new NotSupportedException("Remove is not supported.");
			}

			internal LinkedSetIterator(LightWeightHashSet<T> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly LightWeightHashSet<T> _enclosing;
		}

		/// <summary>Clear the set.</summary>
		/// <remarks>Clear the set. Resize it to the original capacity.</remarks>
		public virtual void Clear()
		{
			this.capacity = this.initialCapacity;
			this.hash_mask = capacity - 1;
			this.expandThreshold = (int)(capacity * maxLoadFactor);
			this.shrinkThreshold = (int)(capacity * minLoadFactor);
			entries = new LightWeightHashSet.LinkedElement[capacity];
			size = 0;
			modification++;
		}

		public virtual object[] ToArray()
		{
			object[] result = new object[size];
			return Sharpen.Collections.ToArray(this, result);
		}

		public virtual U[] ToArray<U>(U[] a)
		{
			if (a == null)
			{
				throw new ArgumentNullException("Input array can not be null");
			}
			if (a.Length < size)
			{
				a = (U[])System.Array.CreateInstance(a.GetType().GetElementType(), size);
			}
			int currentIndex = 0;
			for (int i = 0; i < entries.Length; i++)
			{
				LightWeightHashSet.LinkedElement<T> current = entries[i];
				while (current != null)
				{
					a[currentIndex++] = (U)current.element;
					current = current.next;
				}
			}
			return a;
		}

		public virtual bool ContainsAll<_T0>(ICollection<_T0> c)
		{
			IEnumerator<object> iter = c.GetEnumerator();
			while (iter.HasNext())
			{
				if (!Contains(iter.Next()))
				{
					return false;
				}
			}
			return true;
		}

		public virtual bool RemoveAll<_T0>(ICollection<_T0> c)
		{
			bool changed = false;
			IEnumerator<object> iter = c.GetEnumerator();
			while (iter.HasNext())
			{
				changed |= Remove(iter.Next());
			}
			return changed;
		}

		public virtual bool RetainAll<_T0>(ICollection<_T0> c)
		{
			throw new NotSupportedException("retainAll is not supported.");
		}
	}
}
