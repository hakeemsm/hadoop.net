/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>Implements an intrusive doubly-linked list.</summary>
	/// <remarks>
	/// Implements an intrusive doubly-linked list.
	/// An intrusive linked list is one in which the elements themselves are
	/// responsible for storing the pointers to previous and next elements.
	/// This can save a lot of memory if there are many elements in the list or
	/// many lists.
	/// </remarks>
	public class IntrusiveCollection<E> : System.Collections.Generic.ICollection<E>
		where E : org.apache.hadoop.util.IntrusiveCollection.Element
	{
		/// <summary>An element contained in this list.</summary>
		/// <remarks>
		/// An element contained in this list.
		/// We pass the list itself as a parameter so that elements can belong to
		/// multiple lists.  (The element will need to store separate prev and next
		/// pointers for each.)
		/// </remarks>
		public interface Element
		{
			/// <summary>Insert this element into the list.</summary>
			/// <remarks>
			/// Insert this element into the list.  This is the first thing that will
			/// be called on the element.
			/// </remarks>
			void insertInternal<_T0>(org.apache.hadoop.util.IntrusiveCollection<_T0> list, org.apache.hadoop.util.IntrusiveCollection.Element
				 prev, org.apache.hadoop.util.IntrusiveCollection.Element next)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element;

			/// <summary>Set the prev pointer of an element already in the list.</summary>
			void setPrev<_T0>(org.apache.hadoop.util.IntrusiveCollection<_T0> list, org.apache.hadoop.util.IntrusiveCollection.Element
				 prev)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element;

			/// <summary>Set the next pointer of an element already in the list.</summary>
			void setNext<_T0>(org.apache.hadoop.util.IntrusiveCollection<_T0> list, org.apache.hadoop.util.IntrusiveCollection.Element
				 next)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element;

			/// <summary>Remove an element from the list.</summary>
			/// <remarks>
			/// Remove an element from the list.  This is the last thing that will be
			/// called on an element.
			/// </remarks>
			void removeInternal<_T0>(org.apache.hadoop.util.IntrusiveCollection<_T0> list)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element;

			/// <summary>Get the prev pointer of an element.</summary>
			org.apache.hadoop.util.IntrusiveCollection.Element getPrev<_T0>(org.apache.hadoop.util.IntrusiveCollection
				<_T0> list)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element;

			/// <summary>Get the next pointer of an element.</summary>
			org.apache.hadoop.util.IntrusiveCollection.Element getNext<_T0>(org.apache.hadoop.util.IntrusiveCollection
				<_T0> list)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element;

			/// <summary>Returns true if this element is in the provided list.</summary>
			bool isInList<_T0>(org.apache.hadoop.util.IntrusiveCollection<_T0> list)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element;
		}

		private sealed class _Element_89 : org.apache.hadoop.util.IntrusiveCollection.Element
		{
			public _Element_89(IntrusiveCollection<E> _enclosing)
			{
				this._enclosing = _enclosing;
				this.first = this;
				this.last = this;
			}

			internal org.apache.hadoop.util.IntrusiveCollection.Element first;

			internal org.apache.hadoop.util.IntrusiveCollection.Element last;

			// We keep references to the first and last elements for easy access.
			public void insertInternal<_T0>(org.apache.hadoop.util.IntrusiveCollection<_T0> list
				, org.apache.hadoop.util.IntrusiveCollection.Element prev, org.apache.hadoop.util.IntrusiveCollection.Element
				 next)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element
			{
				throw new System.Exception("Can't insert root element");
			}

			public void setPrev<_T0>(org.apache.hadoop.util.IntrusiveCollection<_T0> list, org.apache.hadoop.util.IntrusiveCollection.Element
				 prev)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element
			{
				com.google.common.@base.Preconditions.checkState(list == this._enclosing._enclosing
					);
				this.last = prev;
			}

			public void setNext<_T0>(org.apache.hadoop.util.IntrusiveCollection<_T0> list, org.apache.hadoop.util.IntrusiveCollection.Element
				 next)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element
			{
				com.google.common.@base.Preconditions.checkState(list == this._enclosing._enclosing
					);
				this.first = next;
			}

			public void removeInternal<_T0>(org.apache.hadoop.util.IntrusiveCollection<_T0> list
				)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element
			{
				throw new System.Exception("Can't remove root element");
			}

			public org.apache.hadoop.util.IntrusiveCollection.Element getNext<_T0>(org.apache.hadoop.util.IntrusiveCollection
				<_T0> list)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element
			{
				com.google.common.@base.Preconditions.checkState(list == this._enclosing._enclosing
					);
				return this.first;
			}

			public org.apache.hadoop.util.IntrusiveCollection.Element getPrev<_T0>(org.apache.hadoop.util.IntrusiveCollection
				<_T0> list)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element
			{
				com.google.common.@base.Preconditions.checkState(list == this._enclosing._enclosing
					);
				return this.last;
			}

			public bool isInList<_T0>(org.apache.hadoop.util.IntrusiveCollection<_T0> list)
				where _T0 : org.apache.hadoop.util.IntrusiveCollection.Element
			{
				return list == this._enclosing._enclosing;
			}

			public override string ToString()
			{
				return "root";
			}

			private readonly IntrusiveCollection<E> _enclosing;
		}

		private org.apache.hadoop.util.IntrusiveCollection.Element root;

		private int size = 0;

		/// <summary>An iterator over the intrusive collection.</summary>
		/// <remarks>
		/// An iterator over the intrusive collection.
		/// Currently, you can remove elements from the list using
		/// #{IntrusiveIterator#remove()}, but modifying the collection in other
		/// ways during the iteration is not supported.
		/// </remarks>
		public class IntrusiveIterator : System.Collections.Generic.IEnumerator<E>
		{
			internal org.apache.hadoop.util.IntrusiveCollection.Element cur;

			internal org.apache.hadoop.util.IntrusiveCollection.Element next;

			internal IntrusiveIterator(IntrusiveCollection<E> _enclosing)
			{
				this._enclosing = _enclosing;
				// + IntrusiveCollection.this + "]";
				this.cur = this._enclosing.root;
				this.next = null;
			}

			public override bool MoveNext()
			{
				if (this.next == null)
				{
					this.next = this.cur.getNext(this._enclosing._enclosing);
				}
				return this.next != this._enclosing.root;
			}

			public override E Current
			{
				get
				{
					if (this.next == null)
					{
						this.next = this.cur.getNext(this._enclosing._enclosing);
					}
					if (this.next == this._enclosing.root)
					{
						throw new java.util.NoSuchElementException();
					}
					this.cur = this.next;
					this.next = null;
					return (E)this.cur;
				}
			}

			public override void remove()
			{
				if (this.cur == null)
				{
					throw new System.InvalidOperationException("Already called remove " + "once on this element."
						);
				}
				this.next = this._enclosing.removeElement(this.cur);
				this.cur = null;
			}

			private readonly IntrusiveCollection<E> _enclosing;
		}

		private org.apache.hadoop.util.IntrusiveCollection.Element removeElement(org.apache.hadoop.util.IntrusiveCollection.Element
			 elem)
		{
			org.apache.hadoop.util.IntrusiveCollection.Element prev = elem.getPrev(this);
			org.apache.hadoop.util.IntrusiveCollection.Element next = elem.getNext(this);
			elem.removeInternal(this);
			prev.setNext(this, next);
			next.setPrev(this, prev);
			size--;
			return next;
		}

		/// <summary>Get an iterator over the list.</summary>
		/// <remarks>
		/// Get an iterator over the list.  This can be used to remove elements.
		/// It is not safe to do concurrent modifications from other threads while
		/// using this iterator.
		/// </remarks>
		/// <returns>The iterator.</returns>
		public virtual System.Collections.Generic.IEnumerator<E> GetEnumerator()
		{
			return new org.apache.hadoop.util.IntrusiveCollection.IntrusiveIterator(this);
		}

		public virtual int Count
		{
			get
			{
				return size;
			}
		}

		public virtual bool isEmpty()
		{
			return size == 0;
		}

		public virtual bool contains(object o)
		{
			try
			{
				org.apache.hadoop.util.IntrusiveCollection.Element element = (org.apache.hadoop.util.IntrusiveCollection.Element
					)o;
				return element.isInList(this);
			}
			catch (System.InvalidCastException)
			{
				return false;
			}
		}

		public virtual object[] toArray()
		{
			object[] ret = new object[size];
			int i = 0;
			for (System.Collections.Generic.IEnumerator<E> iter = GetEnumerator(); iter.MoveNext
				(); )
			{
				ret[i++] = iter.Current;
			}
			return ret;
		}

		public virtual T[] toArray<T>(T[] array)
		{
			if (array.Length < size)
			{
				return (T[])Sharpen.Collections.ToArray(this);
			}
			else
			{
				int i = 0;
				for (System.Collections.Generic.IEnumerator<E> iter = GetEnumerator(); iter.MoveNext
					(); )
				{
					array[i++] = (T)iter.Current;
				}
			}
			return array;
		}

		/// <summary>Add an element to the end of the list.</summary>
		/// <param name="elem">The new element to add.</param>
		public virtual bool add(E elem)
		{
			if (elem == null)
			{
				return false;
			}
			if (elem.isInList(this))
			{
				return false;
			}
			org.apache.hadoop.util.IntrusiveCollection.Element prev = root.getPrev(this);
			prev.setNext(this, elem);
			root.setPrev(this, elem);
			elem.insertInternal(this, prev, root);
			size++;
			return true;
		}

		/// <summary>Add an element to the front of the list.</summary>
		/// <param name="elem">The new element to add.</param>
		public virtual bool addFirst(org.apache.hadoop.util.IntrusiveCollection.Element elem
			)
		{
			if (elem == null)
			{
				return false;
			}
			if (elem.isInList(this))
			{
				return false;
			}
			org.apache.hadoop.util.IntrusiveCollection.Element next = root.getNext(this);
			next.setPrev(this, elem);
			root.setNext(this, elem);
			elem.insertInternal(this, root, next);
			size++;
			return true;
		}

		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.IntrusiveCollection
			)));

		public virtual bool remove(object o)
		{
			try
			{
				org.apache.hadoop.util.IntrusiveCollection.Element elem = (org.apache.hadoop.util.IntrusiveCollection.Element
					)o;
				if (!elem.isInList(this))
				{
					return false;
				}
				removeElement(elem);
				return true;
			}
			catch (System.InvalidCastException)
			{
				return false;
			}
		}

		public virtual bool containsAll<_T0>(System.Collections.Generic.ICollection<_T0> 
			collection)
		{
			foreach (object o in collection)
			{
				if (!contains(o))
				{
					return false;
				}
			}
			return true;
		}

		public virtual bool addAll<_T0>(System.Collections.Generic.ICollection<_T0> collection
			)
			where _T0 : E
		{
			bool changed = false;
			foreach (E elem in collection)
			{
				if (add(elem))
				{
					changed = true;
				}
			}
			return changed;
		}

		public virtual bool removeAll<_T0>(System.Collections.Generic.ICollection<_T0> collection
			)
		{
			bool changed = false;
			foreach (object elem in collection)
			{
				if (remove(elem))
				{
					changed = true;
				}
			}
			return changed;
		}

		public virtual bool retainAll<_T0>(System.Collections.Generic.ICollection<_T0> collection
			)
		{
			bool changed = false;
			for (System.Collections.Generic.IEnumerator<E> iter = GetEnumerator(); iter.MoveNext
				(); )
			{
				org.apache.hadoop.util.IntrusiveCollection.Element elem = iter.Current;
				if (!collection.contains(elem))
				{
					iter.remove();
					changed = true;
				}
			}
			return changed;
		}

		/// <summary>Remove all elements.</summary>
		public virtual void clear()
		{
			for (System.Collections.Generic.IEnumerator<E> iter = GetEnumerator(); iter.MoveNext
				(); )
			{
				iter.Current;
				iter.remove();
			}
		}

		public IntrusiveCollection()
		{
			root = new _Element_89(this);
		}
	}
}
