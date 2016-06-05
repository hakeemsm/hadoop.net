using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>The difference between the current state and a previous state of a list.
	/// 	</summary>
	/// <remarks>
	/// The difference between the current state and a previous state of a list.
	/// Given a previous state of a set and a sequence of create, delete and modify
	/// operations such that the current state of the set can be obtained by applying
	/// the operations on the previous state, the following algorithm construct the
	/// difference between the current state and the previous state of the set.
	/// <pre>
	/// Two lists are maintained in the algorithm:
	/// - c-list for newly created elements
	/// - d-list for the deleted elements
	/// Denote the state of an element by the following
	/// (0, 0): neither in c-list nor d-list
	/// (c, 0): in c-list but not in d-list
	/// (0, d): in d-list but not in c-list
	/// (c, d): in both c-list and d-list
	/// For each case below, ( , ) at the end shows the result state of the element.
	/// Case 1. Suppose the element i is NOT in the previous state.           (0, 0)
	/// 1.1. create i in current: add it to c-list                          (c, 0)
	/// 1.1.1. create i in current and then create: impossible
	/// 1.1.2. create i in current and then delete: remove it from c-list   (0, 0)
	/// 1.1.3. create i in current and then modify: replace it in c-list    (c', 0)
	/// 1.2. delete i from current: impossible
	/// 1.3. modify i in current: impossible
	/// Case 2. Suppose the element i is ALREADY in the previous state.       (0, 0)
	/// 2.1. create i in current: impossible
	/// 2.2. delete i from current: add it to d-list                        (0, d)
	/// 2.2.1. delete i from current and then create: add it to c-list      (c, d)
	/// 2.2.2. delete i from current and then delete: impossible
	/// 2.2.2. delete i from current and then modify: impossible
	/// 2.3. modify i in current: put it in both c-list and d-list          (c, d)
	/// 2.3.1. modify i in current and then create: impossible
	/// 2.3.2. modify i in current and then delete: remove it from c-list   (0, d)
	/// 2.3.3. modify i in current and then modify: replace it in c-list    (c', d)
	/// </pre>
	/// </remarks>
	/// <?/>
	/// <?/>
	public class Diff<K, E>
		where E : Diff.Element<K>
	{
		public enum ListType
		{
			Created,
			Deleted
		}

		/// <summary>
		/// An interface for the elements in a
		/// <see cref="Diff{K, E}"/>
		/// .
		/// </summary>
		public interface Element<K> : Comparable<K>
		{
			/// <returns>the key of this object.</returns>
			K GetKey();
		}

		/// <summary>An interface for passing a method in order to process elements.</summary>
		public interface Processor<E>
		{
			/// <summary>Process the given element.</summary>
			void Process(E element);
		}

		/// <summary>Containing exactly one element.</summary>
		public class Container<E>
		{
			private readonly E element;

			private Container(E element)
			{
				this.element = element;
			}

			/// <returns>the element.</returns>
			public virtual E GetElement()
			{
				return element;
			}
		}

		/// <summary>
		/// Undo information for some operations such as delete(E)
		/// and
		/// <see cref="Diff{K, E}.Modify(Element{K}, Element{K})"/>
		/// .
		/// </summary>
		public class UndoInfo<E>
		{
			private readonly int createdInsertionPoint;

			private readonly E trashed;

			private readonly int deletedInsertionPoint;

			private UndoInfo(int createdInsertionPoint, E trashed, int deletedInsertionPoint)
			{
				this.createdInsertionPoint = createdInsertionPoint;
				this.trashed = trashed;
				this.deletedInsertionPoint = deletedInsertionPoint;
			}

			public virtual E GetTrashedElement()
			{
				return trashed;
			}
		}

		private const int DefaultArrayInitialCapacity = 4;

		/// <summary>Search the element from the list.</summary>
		/// <returns>
		/// -1 if the list is null; otherwise, return the insertion point
		/// defined in
		/// <see cref="Sharpen.Collections.BinarySearch{T}(System.Collections.Generic.IList{E}, object)
		/// 	"/>
		/// .
		/// Note that, when the list is null, -1 is the correct insertion point.
		/// </returns>
		protected internal static int Search<K, E>(IList<E> elements, K name)
			where E : Comparable<K>
		{
			return elements == null ? -1 : Sharpen.Collections.BinarySearch(elements, name);
		}

		private static void Remove<E>(IList<E> elements, int i, E expected)
		{
			E removed = elements.Remove(-i - 1);
			Preconditions.CheckState(removed == expected, "removed != expected=%s, removed=%s."
				, expected, removed);
		}

		/// <summary>c-list: element(s) created in current.</summary>
		private IList<E> created;

		/// <summary>d-list: element(s) deleted from current.</summary>
		private IList<E> deleted;

		protected internal Diff()
		{
		}

		protected internal Diff(IList<E> created, IList<E> deleted)
		{
			this.created = created;
			this.deleted = deleted;
		}

		/// <returns>the created list, which is never null.</returns>
		public virtual IList<E> GetList(Diff.ListType type)
		{
			IList<E> list = type == Diff.ListType.Created ? created : deleted;
			return list == null ? Sharpen.Collections.EmptyList<E>() : list;
		}

		public virtual int SearchIndex(Diff.ListType type, K name)
		{
			return Search(GetList(type), name);
		}

		/// <returns>
		/// null if the element is not found;
		/// otherwise, return the element in the created/deleted list.
		/// </returns>
		public virtual E Search(Diff.ListType type, K name)
		{
			IList<E> list = GetList(type);
			int c = Search(list, name);
			return c < 0 ? null : list[c];
		}

		/// <returns>true if no changes contained in the diff</returns>
		public virtual bool IsEmpty()
		{
			return (created == null || created.IsEmpty()) && (deleted == null || deleted.IsEmpty
				());
		}

		/// <summary>Insert the given element to the created/deleted list.</summary>
		/// <param name="i">
		/// the insertion point defined
		/// in
		/// <see cref="Sharpen.Collections.BinarySearch{T}(System.Collections.Generic.IList{E}, object)
		/// 	"/>
		/// </param>
		private void Insert(Diff.ListType type, E element, int i)
		{
			IList<E> list = type == Diff.ListType.Created ? created : deleted;
			if (i >= 0)
			{
				throw new Exception("Element already exists: element=" + element + ", " + type + 
					"=" + list);
			}
			if (list == null)
			{
				list = new AList<E>(DefaultArrayInitialCapacity);
				if (type == Diff.ListType.Created)
				{
					created = list;
				}
				else
				{
					if (type == Diff.ListType.Deleted)
					{
						deleted = list;
					}
				}
			}
			list.Add(-i - 1, element);
		}

		/// <summary>Create an element in current state.</summary>
		/// <returns>the c-list insertion point for undo.</returns>
		public virtual int Create(E element)
		{
			int c = Search(created, element.GetKey());
			Insert(Diff.ListType.Created, element, c);
			return c;
		}

		/// <summary>Undo the previous create(E) operation.</summary>
		/// <remarks>
		/// Undo the previous create(E) operation. Note that the behavior is
		/// undefined if the previous operation is not create(E).
		/// </remarks>
		public virtual void UndoCreate(E element, int insertionPoint)
		{
			Remove(created, insertionPoint, element);
		}

		/// <summary>Delete an element from current state.</summary>
		/// <returns>the undo information.</returns>
		public virtual Diff.UndoInfo<E> Delete(E element)
		{
			int c = Search(created, element.GetKey());
			E previous = null;
			int d = null;
			if (c >= 0)
			{
				// remove a newly created element
				previous = created.Remove(c);
			}
			else
			{
				// not in c-list, it must be in previous
				d = Search(deleted, element.GetKey());
				Insert(Diff.ListType.Deleted, element, d);
			}
			return new Diff.UndoInfo<E>(c, previous, d);
		}

		/// <summary>Undo the previous delete(E) operation.</summary>
		/// <remarks>
		/// Undo the previous delete(E) operation. Note that the behavior is
		/// undefined if the previous operation is not delete(E).
		/// </remarks>
		public virtual void UndoDelete(E element, Diff.UndoInfo<E> undoInfo)
		{
			int c = undoInfo.createdInsertionPoint;
			if (c >= 0)
			{
				created.Add(c, undoInfo.trashed);
			}
			else
			{
				Remove(deleted, undoInfo.deletedInsertionPoint, element);
			}
		}

		/// <summary>Modify an element in current state.</summary>
		/// <returns>the undo information.</returns>
		public virtual Diff.UndoInfo<E> Modify(E oldElement, E newElement)
		{
			Preconditions.CheckArgument(oldElement != newElement, "They are the same object: oldElement == newElement = %s"
				, newElement);
			Preconditions.CheckArgument(oldElement.CompareTo(newElement.GetKey()) == 0, "The names do not match: oldElement=%s, newElement=%s"
				, oldElement, newElement);
			int c = Search(created, newElement.GetKey());
			E previous = null;
			int d = null;
			if (c >= 0)
			{
				// Case 1.1.3 and 2.3.3: element is already in c-list,
				previous = created.Set(c, newElement);
				// For previous != oldElement, set it to oldElement
				previous = oldElement;
			}
			else
			{
				d = Search(deleted, oldElement.GetKey());
				if (d < 0)
				{
					// Case 2.3: neither in c-list nor d-list
					Insert(Diff.ListType.Created, newElement, c);
					Insert(Diff.ListType.Deleted, oldElement, d);
				}
			}
			return new Diff.UndoInfo<E>(c, previous, d);
		}

		/// <summary>Undo the previous modify(E, E) operation.</summary>
		/// <remarks>
		/// Undo the previous modify(E, E) operation. Note that the behavior
		/// is undefined if the previous operation is not modify(E, E).
		/// </remarks>
		public virtual void UndoModify(E oldElement, E newElement, Diff.UndoInfo<E> undoInfo
			)
		{
			int c = undoInfo.createdInsertionPoint;
			if (c >= 0)
			{
				created.Set(c, undoInfo.trashed);
			}
			else
			{
				int d = undoInfo.deletedInsertionPoint;
				if (d < 0)
				{
					Remove(created, c, newElement);
					Remove(deleted, d, oldElement);
				}
			}
		}

		/// <summary>Find an element in the previous state.</summary>
		/// <returns>
		/// null if the element cannot be determined in the previous state
		/// since no change is recorded and it should be determined in the
		/// current state; otherwise, return a
		/// <see cref="Container{E}"/>
		/// containing the
		/// element in the previous state. Note that the element can possibly
		/// be null which means that the element is not found in the previous
		/// state.
		/// </returns>
		public virtual Diff.Container<E> AccessPrevious(K name)
		{
			return AccessPrevious(name, created, deleted);
		}

		private static Diff.Container<E> AccessPrevious<K, E>(K name, IList<E> clist, IList
			<E> dlist)
			where E : Diff.Element<K>
		{
			int d = Search(dlist, name);
			if (d >= 0)
			{
				// the element was in previous and was once deleted in current.
				return new Diff.Container<E>(dlist[d]);
			}
			else
			{
				int c = Search(clist, name);
				// When c >= 0, the element in current is a newly created element.
				return c < 0 ? null : new Diff.Container<E>(null);
			}
		}

		/// <summary>Find an element in the current state.</summary>
		/// <returns>
		/// null if the element cannot be determined in the current state since
		/// no change is recorded and it should be determined in the previous
		/// state; otherwise, return a
		/// <see cref="Container{E}"/>
		/// containing the element in
		/// the current state. Note that the element can possibly be null which
		/// means that the element is not found in the current state.
		/// </returns>
		public virtual Diff.Container<E> AccessCurrent(K name)
		{
			return AccessPrevious(name, deleted, created);
		}

		/// <summary>Apply this diff to previous state in order to obtain current state.</summary>
		/// <returns>the current state of the list.</returns>
		public virtual IList<E> Apply2Previous(IList<E> previous)
		{
			return Apply2Previous(previous, GetList(Diff.ListType.Created), GetList(Diff.ListType
				.Deleted));
		}

		private static IList<E> Apply2Previous<K, E>(IList<E> previous, IList<E> clist, IList
			<E> dlist)
			where E : Diff.Element<K>
		{
			// Assumptions:
			// (A1) All lists are sorted.
			// (A2) All elements in dlist must be in previous.
			// (A3) All elements in clist must be not in tmp = previous - dlist.
			IList<E> tmp = new AList<E>(previous.Count - dlist.Count);
			{
				// tmp = previous - dlist
				IEnumerator<E> i = previous.GetEnumerator();
				foreach (E deleted in dlist)
				{
					E e = i.Next();
					//since dlist is non-empty, e must exist by (A2).
					int cmp = 0;
					for (; (cmp = e.CompareTo(deleted.GetKey())) < 0; e = i.Next())
					{
						tmp.AddItem(e);
					}
					Preconditions.CheckState(cmp == 0);
				}
				// check (A2)
				for (; i.HasNext(); )
				{
					tmp.AddItem(i.Next());
				}
			}
			IList<E> current = new AList<E>(tmp.Count + clist.Count);
			{
				// current = tmp + clist
				IEnumerator<E> tmpIterator = tmp.GetEnumerator();
				IEnumerator<E> cIterator = clist.GetEnumerator();
				E t = tmpIterator.HasNext() ? tmpIterator.Next() : null;
				E c = cIterator.HasNext() ? cIterator.Next() : null;
				for (; t != null || c != null; )
				{
					int cmp = c == null ? 1 : t == null ? -1 : c.CompareTo(t.GetKey());
					if (cmp < 0)
					{
						current.AddItem(c);
						c = cIterator.HasNext() ? cIterator.Next() : null;
					}
					else
					{
						if (cmp > 0)
						{
							current.AddItem(t);
							t = tmpIterator.HasNext() ? tmpIterator.Next() : null;
						}
						else
						{
							throw new Exception("Violated assumption (A3).");
						}
					}
				}
			}
			return current;
		}

		/// <summary>
		/// Apply the reverse of this diff to current state in order
		/// to obtain the previous state.
		/// </summary>
		/// <returns>the previous state of the list.</returns>
		public virtual IList<E> Apply2Current(IList<E> current)
		{
			return Apply2Previous(current, GetList(Diff.ListType.Deleted), GetList(Diff.ListType
				.Created));
		}

		/// <summary>Combine this diff with a posterior diff.</summary>
		/// <remarks>
		/// Combine this diff with a posterior diff.  We have the following cases:
		/// <pre>
		/// 1. For (c, 0) in the posterior diff, check the element in this diff:
		/// 1.1 (c', 0)  in this diff: impossible
		/// 1.2 (0, d')  in this diff: put in c-list --&gt; (c, d')
		/// 1.3 (c', d') in this diff: impossible
		/// 1.4 (0, 0)   in this diff: put in c-list --&gt; (c, 0)
		/// This is the same logic as create(E).
		/// 2. For (0, d) in the posterior diff,
		/// 2.1 (c', 0)  in this diff: remove from c-list --&gt; (0, 0)
		/// 2.2 (0, d')  in this diff: impossible
		/// 2.3 (c', d') in this diff: remove from c-list --&gt; (0, d')
		/// 2.4 (0, 0)   in this diff: put in d-list --&gt; (0, d)
		/// This is the same logic as delete(E).
		/// 3. For (c, d) in the posterior diff,
		/// 3.1 (c', 0)  in this diff: replace the element in c-list --&gt; (c, 0)
		/// 3.2 (0, d')  in this diff: impossible
		/// 3.3 (c', d') in this diff: replace the element in c-list --&gt; (c, d')
		/// 3.4 (0, 0)   in this diff: put in c-list and d-list --&gt; (c, d)
		/// This is the same logic as modify(E, E).
		/// </pre>
		/// </remarks>
		/// <param name="posterior">The posterior diff to combine with.</param>
		/// <param name="deletedProcesser">process the deleted/overwritten elements in case 2.1, 2.3, 3.1 and 3.3.
		/// 	</param>
		public virtual void CombinePosterior(Diff<K, E> posterior, Diff.Processor<E> deletedProcesser
			)
		{
			IEnumerator<E> createdIterator = posterior.GetList(Diff.ListType.Created).GetEnumerator
				();
			IEnumerator<E> deletedIterator = posterior.GetList(Diff.ListType.Deleted).GetEnumerator
				();
			E c = createdIterator.HasNext() ? createdIterator.Next() : null;
			E d = deletedIterator.HasNext() ? deletedIterator.Next() : null;
			for (; c != null || d != null; )
			{
				int cmp = c == null ? 1 : d == null ? -1 : c.CompareTo(d.GetKey());
				if (cmp < 0)
				{
					// case 1: only in c-list
					Create(c);
					c = createdIterator.HasNext() ? createdIterator.Next() : null;
				}
				else
				{
					if (cmp > 0)
					{
						// case 2: only in d-list
						Diff.UndoInfo<E> ui = Delete(d);
						if (deletedProcesser != null)
						{
							deletedProcesser.Process(ui.trashed);
						}
						d = deletedIterator.HasNext() ? deletedIterator.Next() : null;
					}
					else
					{
						// case 3: in both c-list and d-list 
						Diff.UndoInfo<E> ui = Modify(d, c);
						if (deletedProcesser != null)
						{
							deletedProcesser.Process(ui.trashed);
						}
						c = createdIterator.HasNext() ? createdIterator.Next() : null;
						d = deletedIterator.HasNext() ? deletedIterator.Next() : null;
					}
				}
			}
		}

		public override string ToString()
		{
			return GetType().Name + "{created=" + GetList(Diff.ListType.Created) + ", deleted="
				 + GetList(Diff.ListType.Deleted) + "}";
		}
	}
}
