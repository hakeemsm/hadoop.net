using System;
using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// A
	/// <see cref="ReadOnlyList{E}"/>
	/// is a unmodifiable list,
	/// which supports read-only operations.
	/// </summary>
	/// <?/>
	public abstract class ReadOnlyList<E> : IEnumerable<E>
	{
		/// <summary>Is this an empty list?</summary>
		public abstract bool IsEmpty();

		/// <returns>the size of this list.</returns>
		public abstract int Size();

		/// <returns>the i-th element.</returns>
		public abstract E Get(int i);

		/// <summary>
		/// Utilities for
		/// <see cref="ReadOnlyList{E}"/>
		/// </summary>
		public class Util
		{
			/// <returns>an empty list.</returns>
			public static ReadOnlyList<E> EmptyList<E>()
			{
				return ReadOnlyList.Util.AsReadOnlyList(Sharpen.Collections.EmptyList<E>());
			}

			/// <summary>
			/// The same as
			/// <see cref="Sharpen.Collections.BinarySearch{T}(System.Collections.Generic.IList{E}, object)
			/// 	"/>
			/// except that the list is a
			/// <see cref="ReadOnlyList{E}"/>
			/// .
			/// </summary>
			/// <returns>
			/// the insertion point defined
			/// in
			/// <see cref="Sharpen.Collections.BinarySearch{T}(System.Collections.Generic.IList{E}, object)
			/// 	"/>
			/// .
			/// </returns>
			public static int BinarySearch<K, E>(ReadOnlyList<E> list, K key)
				where E : Comparable<K>
			{
				int lower = 0;
				for (int upper = list.Size() - 1; lower <= upper; )
				{
					int mid = (int)(((uint)(upper + lower)) >> 1);
					int d = list.Get(mid).CompareTo(key);
					if (d == 0)
					{
						return mid;
					}
					else
					{
						if (d > 0)
						{
							upper = mid - 1;
						}
						else
						{
							lower = mid + 1;
						}
					}
				}
				return -(lower + 1);
			}

			/// <returns>
			/// a
			/// <see cref="ReadOnlyList{E}"/>
			/// view of the given list.
			/// </returns>
			public static ReadOnlyList<E> AsReadOnlyList<E>(IList<E> list)
			{
				return new _ReadOnlyList_89(list);
			}

			private sealed class _ReadOnlyList_89 : ReadOnlyList<E>
			{
				public _ReadOnlyList_89(IList<E> list)
				{
					this.list = list;
				}

				public IEnumerator<E> GetEnumerator()
				{
					return list.GetEnumerator();
				}

				public override bool IsEmpty()
				{
					return list.IsEmpty();
				}

				public override int Size()
				{
					return list.Count;
				}

				public override E Get(int i)
				{
					return list[i];
				}

				private readonly IList<E> list;
			}

			/// <returns>
			/// a
			/// <see cref="System.Collections.IList{E}"/>
			/// view of the given list.
			/// </returns>
			public static IList<E> AsList<E>(ReadOnlyList<E> list)
			{
				return new _IList_116(list);
			}

			private sealed class _IList_116 : IList<E>
			{
				public _IList_116(ReadOnlyList<E> list)
				{
					this.list = list;
				}

				public IEnumerator<E> GetEnumerator()
				{
					return list.GetEnumerator();
				}

				public bool IsEmpty()
				{
					return list.IsEmpty();
				}

				public int Count
				{
					get
					{
						return list.Size();
					}
				}

				public E Get(int i)
				{
					return list.Get(i);
				}

				public object[] ToArray()
				{
					object[] a = new object[this.Count];
					for (int i = 0; i < a.Length; i++)
					{
						a[i] = this[i];
					}
					return a;
				}

				//All methods below are not supported.
				public bool AddItem(E e)
				{
					throw new NotSupportedException();
				}

				public void Add(int index, E element)
				{
					throw new NotSupportedException();
				}

				public bool AddAll<_T0>(ICollection<_T0> c)
					where _T0 : E
				{
					throw new NotSupportedException();
				}

				public bool AddRange<_T0>(int index, ICollection<_T0> c)
					where _T0 : E
				{
					throw new NotSupportedException();
				}

				public void Clear()
				{
					throw new NotSupportedException();
				}

				public bool Contains(object o)
				{
					throw new NotSupportedException();
				}

				public bool ContainsAll<_T0>(ICollection<_T0> c)
				{
					throw new NotSupportedException();
				}

				public int IndexOf(object o)
				{
					throw new NotSupportedException();
				}

				public int LastIndexOf(object o)
				{
					throw new NotSupportedException();
				}

				public ListIterator<E> ListIterator()
				{
					throw new NotSupportedException();
				}

				public ListIterator<E> ListIterator(int index)
				{
					throw new NotSupportedException();
				}

				public bool Remove(object o)
				{
					throw new NotSupportedException();
				}

				public E Remove(int index)
				{
					throw new NotSupportedException();
				}

				public bool RemoveAll<_T0>(ICollection<_T0> c)
				{
					throw new NotSupportedException();
				}

				public bool RetainAll<_T0>(ICollection<_T0> c)
				{
					throw new NotSupportedException();
				}

				public E Set(int index, E element)
				{
					throw new NotSupportedException();
				}

				public IList<E> SubList(int fromIndex, int toIndex)
				{
					throw new NotSupportedException();
				}

				public T[] ToArray<T>(T[] a)
				{
					throw new NotSupportedException();
				}

				private readonly ReadOnlyList<E> list;
			}
		}
	}

	public static class ReadOnlyListConstants
	{
	}
}
