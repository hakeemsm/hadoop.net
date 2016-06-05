using System;
using System.Collections.Generic;
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
	/// the elements and linked lists for collision resolution. In addition it stores
	/// elements in a linked list to ensure ordered traversal. This class does not
	/// support null element.
	/// This class is not thread safe.
	/// </remarks>
	public class LightWeightLinkedSet<T> : LightWeightHashSet<T>
	{
		/// <summary>
		/// Elements of
		/// <see cref="LightWeightLinkedSet{T}"/>
		/// .
		/// </summary>
		internal class DoubleLinkedElement<T> : LightWeightHashSet.LinkedElement<T>
		{
			private LightWeightLinkedSet.DoubleLinkedElement<T> before;

			private LightWeightLinkedSet.DoubleLinkedElement<T> after;

			public DoubleLinkedElement(T elem, int hashCode)
				: base(elem, hashCode)
			{
				// references to elements within all-element linked list
				this.before = null;
				this.after = null;
			}

			public override string ToString()
			{
				return base.ToString();
			}
		}

		private LightWeightLinkedSet.DoubleLinkedElement<T> head;

		private LightWeightLinkedSet.DoubleLinkedElement<T> tail;

		/// <param name="initCapacity">Recommended size of the internal array.</param>
		/// <param name="maxLoadFactor">used to determine when to expand the internal array</param>
		/// <param name="minLoadFactor">used to determine when to shrink the internal array</param>
		public LightWeightLinkedSet(int initCapacity, float maxLoadFactor, float minLoadFactor
			)
			: base(initCapacity, maxLoadFactor, minLoadFactor)
		{
			head = null;
			tail = null;
		}

		public LightWeightLinkedSet()
			: this(MinimumCapacity, DefaultMaxLoadFactor, DefautMinLoadFactor)
		{
		}

		/// <summary>Add given element to the hash table</summary>
		/// <returns>true if the element was not present in the table, false otherwise</returns>
		protected internal override bool AddElem(T element)
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
			LightWeightLinkedSet.DoubleLinkedElement<T> le = new LightWeightLinkedSet.DoubleLinkedElement
				<T>(element, hashCode);
			le.next = entries[index];
			entries[index] = le;
			// insert to the end of the all-element linked list
			le.after = null;
			le.before = tail;
			if (tail != null)
			{
				tail.after = le;
			}
			tail = le;
			if (head == null)
			{
				head = le;
			}
			return true;
		}

		/// <summary>Remove the element corresponding to the key, given key.hashCode() == index.
		/// 	</summary>
		/// <returns>Return the entry with the element if exists. Otherwise return null.</returns>
		protected internal override LightWeightHashSet.LinkedElement<T> RemoveElem(T key)
		{
			LightWeightLinkedSet.DoubleLinkedElement<T> found = (LightWeightLinkedSet.DoubleLinkedElement
				<T>)(base.RemoveElem(key));
			if (found == null)
			{
				return null;
			}
			// update linked list
			if (found.after != null)
			{
				found.after.before = found.before;
			}
			if (found.before != null)
			{
				found.before.after = found.after;
			}
			if (head == found)
			{
				head = head.after;
			}
			if (tail == found)
			{
				tail = tail.before;
			}
			return found;
		}

		/// <summary>Remove and return first element on the linked list of all elements.</summary>
		/// <returns>first element</returns>
		public virtual T PollFirst()
		{
			if (head == null)
			{
				return null;
			}
			T first = head.element;
			this.Remove(first);
			return first;
		}

		/// <summary>Remove and return n elements from the hashtable.</summary>
		/// <remarks>
		/// Remove and return n elements from the hashtable.
		/// The order in which entries are removed is corresponds
		/// to the order in which they were inserted.
		/// </remarks>
		/// <returns>first element</returns>
		public override IList<T> PollN(int n)
		{
			if (n >= size)
			{
				// if we need to remove all elements then do fast polling
				return PollAll();
			}
			IList<T> retList = new AList<T>(n);
			while (n-- > 0 && head != null)
			{
				T curr = head.element;
				this.RemoveElem(curr);
				retList.AddItem(curr);
			}
			ShrinkIfNecessary();
			return retList;
		}

		/// <summary>Remove all elements from the set and return them in order.</summary>
		/// <remarks>
		/// Remove all elements from the set and return them in order. Traverse the
		/// link list, don't worry about hashtable - faster version of the parent
		/// method.
		/// </remarks>
		public override IList<T> PollAll()
		{
			IList<T> retList = new AList<T>(size);
			while (head != null)
			{
				retList.AddItem(head.element);
				head = head.after;
			}
			this.Clear();
			return retList;
		}

		public override U[] ToArray<U>(U[] a)
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
			LightWeightLinkedSet.DoubleLinkedElement<T> current = head;
			while (current != null)
			{
				T curr = current.element;
				a[currentIndex++] = (U)curr;
				current = current.after;
			}
			return a;
		}

		public override IEnumerator<T> GetEnumerator()
		{
			return new LightWeightLinkedSet.LinkedSetIterator(this);
		}

		private class LinkedSetIterator : IEnumerator<T>
		{
			/// <summary>The starting modification for fail-fast.</summary>
			private readonly int startModification = this._enclosing.modification;

			/// <summary>The next element to return.</summary>
			private LightWeightLinkedSet.DoubleLinkedElement<T> next = this._enclosing.head;

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
				this.next = this.next.after;
				return e;
			}

			public override void Remove()
			{
				throw new NotSupportedException("Remove is not supported.");
			}

			internal LinkedSetIterator(LightWeightLinkedSet<T> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly LightWeightLinkedSet<T> _enclosing;
		}

		/// <summary>Clear the set.</summary>
		/// <remarks>Clear the set. Resize it to the original capacity.</remarks>
		public override void Clear()
		{
			base.Clear();
			this.head = null;
			this.tail = null;
		}
	}
}
