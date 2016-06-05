using System;
using System.Collections.Generic;

using Reflect;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>Contains utility methods for dealing with Java Generics.</summary>
	public class GenericsUtil
	{
		/// <summary>
		/// Returns the Class object (of type <code>Class&lt;T&gt;</code>) of the
		/// argument of type <code>T</code>.
		/// </summary>
		/// <?/>
		/// <param name="t">the object to get it class</param>
		/// <returns><code>Class&lt;T&gt;</code></returns>
		public static Type GetClass<T>(T t)
		{
			Type clazz = (Type)t.GetType();
			return clazz;
		}

		/// <summary>
		/// Converts the given <code>List&lt;T&gt;</code> to a an array of
		/// <code>T[]</code>.
		/// </summary>
		/// <param name="c">the Class object of the items in the list</param>
		/// <param name="list">the list to convert</param>
		public static T[] ToArray<T>(IList<T> list)
		{
			System.Type c = typeof(T);
			T[] ta = (T[])System.Array.CreateInstance(c, list.Count);
			for (int i = 0; i < list.Count; i++)
			{
				ta[i] = list[i];
			}
			return ta;
		}

		/// <summary>
		/// Converts the given <code>List&lt;T&gt;</code> to a an array of
		/// <code>T[]</code>.
		/// </summary>
		/// <param name="list">the list to convert</param>
		/// <exception cref="System.IndexOutOfRangeException">
		/// if the list is empty.
		/// Use
		/// <see cref="ToArray{T}(System.Type{T}, System.Collections.Generic.IList{E})"/>
		/// if the list may be empty.
		/// </exception>
		public static T[] ToArray<T>(IList<T> list)
		{
			return ToArray(GetClass(list[0]), list);
		}
	}
}
