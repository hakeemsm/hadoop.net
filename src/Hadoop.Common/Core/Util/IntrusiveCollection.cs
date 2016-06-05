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
using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>Implements an intrusive doubly-linked list.</summary>
	/// <remarks>
	/// Implements an intrusive doubly-linked list.
	/// An intrusive linked list is one in which the elements themselves are
	/// responsible for storing the pointers to previous and next elements.
	/// This can save a lot of memory if there are many elements in the list or
	/// many lists.
	/// </remarks>
	public class IntrusiveCollection<E> : ICollection<E>
		where E : IntrusiveCollection.Element
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
			void InsertInternal<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element
				 prev, IntrusiveCollection.Element next)
				where _T0 : IntrusiveCollection.Element;

			/// <summary>Set the prev pointer of an element already in the list.</summary>
			void SetPrev<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element prev
				)
				where _T0 : IntrusiveCollection.Element;

			/// <summary>Set the next pointer of an element already in the list.</summary>
			void SetNext<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element next
				)
				where _T0 : IntrusiveCollection.Element;

			/// <summary>Remove an element from the list.</summary>
			/// <remarks>
			/// Remove an element from the list.  This is the last thing that will be
			/// called on an element.
			/// </remarks>
			void RemoveInternal<_T0>(IntrusiveCollection<_T0> list)
				where _T0 : IntrusiveCollection.Element;

			/// <summary>Get the prev pointer of an element.</summary>
			IntrusiveCollection.Element GetPrev<_T0>(IntrusiveCollection<_T0> list)
				where _T0 : IntrusiveCollection.Element;

			/// <summary>Get the next pointer of an element.</summary>
			IntrusiveCollection.Element GetNext<_T0>(IntrusiveCollection<_T0> list)
				where _T0 : IntrusiveCollection.Element;

			/// <summary>Returns true if this element is in the provided list.</summary>
			bool IsInList<_T0>(IntrusiveCollection<_T0> list)
				where _T0 : IntrusiveCollection.Element;
		}

		private sealed class _Element_89 : IntrusiveCollection.Element
		{
			public _Element_89(IntrusiveCollection<E> _enclosing)
			{
				this._enclosing = _enclosing;
				this.first = this;
				this.last = this;
			}

			internal IntrusiveCollection.Element first;

			internal IntrusiveCollection.Element last;

			// We keep references to the first and last elements for easy access.
			public void InsertInternal<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element
				 prev, IntrusiveCollection.Element next)
				where _T0 : IntrusiveCollection.Element
			{
				throw new RuntimeException("Can't insert root element");
			}

			public void SetPrev<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element
				 prev)
				where _T0 : IntrusiveCollection.Element
			{
				Preconditions.CheckState(list == this._enclosing._enclosing);
				this.last = prev;
			}

			public void SetNext<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element
				 next)
				where _T0 : IntrusiveCollection.Element
			{
				Preconditions.CheckState(list == this._enclosing._enclosing);
				this.first = next;
			}

			public void RemoveInternal<_T0>(IntrusiveCollection<_T0> list)
				where _T0 : IntrusiveCollection.Element
			{
				throw new RuntimeException("Can't remove root element");
			}

			public IntrusiveCollection.Element GetNext<_T0>(IntrusiveCollection<_T0> list)
				where _T0 : IntrusiveCollection.Element
			{
				Preconditions.CheckState(list == this._enclosing._enclosing);
				return this.first;
			}

			public IntrusiveCollection.Element GetPrev<_T0>(IntrusiveCollection<_T0> list)
				where _T0 : IntrusiveCollection.Element
			{
				Preconditions.CheckState(list == this._enclosing._enclosing);
				return this.last;
			}

			public bool IsInList<_T0>(IntrusiveCollection<_T0> list)
				where _T0 : IntrusiveCollection.Element
			{
				return list == this._enclosing._enclosing;
			}

			public override string ToString()
			{
				return "root";
			}

			private readonly IntrusiveCollection<E> _enclosing;
		}

		private IntrusiveCollection.Element root;

		private int size = 0;

		/// <summary>An iterator over the intrusive collection.</summary>
		/// <remarks>
		/// An iterator over the intrusive collection.
		/// Currently, you can remove elements from the list using
		/// #{IntrusiveIterator#remove()}, but modifying the collection in other
		/// ways during the iteration is not supported.
		/// </remarks>
		public class IntrusiveIterator : IEnumerator<E>
		{
			internal IntrusiveCollection.Element cur;

			internal IntrusiveCollection.Element next;

			internal IntrusiveIterator(IntrusiveCollection<E> _enclosing)
			{
				this._enclosing = _enclosing;
				// + IntrusiveCollection.this + "]";
				this.cur = this._enclosing.root;
				this.next = null;
			}

			public override bool HasNext()
			{
				if (this.next == null)
				{
					this.next = this.cur.GetNext(this._enclosing._enclosing);
				}
				return this.next != this._enclosing.root;
			}

			public override E Next()
			{
				if (this.next == null)
				{
					this.next = this.cur.GetNext(this._enclosing._enclosing);
				}
				if (this.next == this._enclosing.root)
				{
					throw new NoSuchElementException();
				}
				this.cur = this.next;
				this.next = null;
				return (E)this.cur;
			}

			public override void Remove()
			{
				if (this.cur == null)
				{
					throw new InvalidOperationException("Already called remove " + "once on this element."
						);
				}
				this.next = this._enclosing.RemoveElement(this.cur);
				this.cur = null;
			}

			private readonly IntrusiveCollection<E> _enclosing;
		}

		private IntrusiveCollection.Element RemoveElement(IntrusiveCollection.Element elem
			)
		{
			IntrusiveCollection.Element prev = elem.GetPrev(this);
			IntrusiveCollection.Element next = elem.GetNext(this);
			elem.RemoveInternal(this);
			prev.SetNext(this, next);
			next.SetPrev(this, prev);
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
		public virtual IEnumerator<E> GetEnumerator()
		{
			return new IntrusiveCollection.IntrusiveIterator(this);
		}

		public virtual int Count
		{
			get
			{
				return size;
			}
		}

		public virtual bool IsEmpty()
		{
			return size == 0;
		}

		public virtual bool Contains(object o)
		{
			try
			{
				IntrusiveCollection.Element element = (IntrusiveCollection.Element)o;
				return element.IsInList(this);
			}
			catch (InvalidCastException)
			{
				return false;
			}
		}

		public virtual object[] ToArray()
		{
			object[] ret = new object[size];
			int i = 0;
			for (IEnumerator<E> iter = GetEnumerator(); iter.HasNext(); )
			{
				ret[i++] = iter.Next();
			}
			return ret;
		}

		public virtual T[] ToArray<T>(T[] array)
		{
			if (array.Length < size)
			{
				return (T[])Collections.ToArray(this);
			}
			else
			{
				int i = 0;
				for (IEnumerator<E> iter = GetEnumerator(); iter.HasNext(); )
				{
					array[i++] = (T)iter.Next();
				}
			}
			return array;
		}

		/// <summary>Add an element to the end of the list.</summary>
		/// <param name="elem">The new element to add.</param>
		public virtual bool AddItem(E elem)
		{
			if (elem == null)
			{
				return false;
			}
			if (elem.IsInList(this))
			{
				return false;
			}
			IntrusiveCollection.Element prev = root.GetPrev(this);
			prev.SetNext(this, elem);
			root.SetPrev(this, elem);
			elem.InsertInternal(this, prev, root);
			size++;
			return true;
		}

		/// <summary>Add an element to the front of the list.</summary>
		/// <param name="elem">The new element to add.</param>
		public virtual bool AddFirst(IntrusiveCollection.Element elem)
		{
			if (elem == null)
			{
				return false;
			}
			if (elem.IsInList(this))
			{
				return false;
			}
			IntrusiveCollection.Element next = root.GetNext(this);
			next.SetPrev(this, elem);
			root.SetNext(this, elem);
			elem.InsertInternal(this, root, next);
			size++;
			return true;
		}

		public static readonly Log Log = LogFactory.GetLog(typeof(IntrusiveCollection));

		public virtual bool Remove(object o)
		{
			try
			{
				IntrusiveCollection.Element elem = (IntrusiveCollection.Element)o;
				if (!elem.IsInList(this))
				{
					return false;
				}
				RemoveElement(elem);
				return true;
			}
			catch (InvalidCastException)
			{
				return false;
			}
		}

		public virtual bool ContainsAll<_T0>(ICollection<_T0> collection)
		{
			foreach (object o in collection)
			{
				if (!Contains(o))
				{
					return false;
				}
			}
			return true;
		}

		public virtual bool AddAll<_T0>(ICollection<_T0> collection)
			where _T0 : E
		{
			bool changed = false;
			foreach (E elem in collection)
			{
				if (AddItem(elem))
				{
					changed = true;
				}
			}
			return changed;
		}

		public virtual bool RemoveAll<_T0>(ICollection<_T0> collection)
		{
			bool changed = false;
			foreach (object elem in collection)
			{
				if (Remove(elem))
				{
					changed = true;
				}
			}
			return changed;
		}

		public virtual bool RetainAll<_T0>(ICollection<_T0> collection)
		{
			bool changed = false;
			for (IEnumerator<E> iter = GetEnumerator(); iter.HasNext(); )
			{
				IntrusiveCollection.Element elem = iter.Next();
				if (!collection.Contains(elem))
				{
					iter.Remove();
					changed = true;
				}
			}
			return changed;
		}

		/// <summary>Remove all elements.</summary>
		public virtual void Clear()
		{
			for (IEnumerator<E> iter = GetEnumerator(); iter.HasNext(); )
			{
				iter.Next();
				iter.Remove();
			}
		}

		public IntrusiveCollection()
		{
			root = new _Element_89(this);
		}
	}
}
