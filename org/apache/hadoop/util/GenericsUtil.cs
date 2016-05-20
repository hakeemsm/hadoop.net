using Sharpen;

namespace org.apache.hadoop.util
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
		public static java.lang.Class getClass<T>(T t)
		{
			java.lang.Class clazz = (java.lang.Class)Sharpen.Runtime.getClassForObject(t);
			return clazz;
		}

		/// <summary>
		/// Converts the given <code>List&lt;T&gt;</code> to a an array of
		/// <code>T[]</code>.
		/// </summary>
		/// <param name="c">the Class object of the items in the list</param>
		/// <param name="list">the list to convert</param>
		public static T[] toArray<T>(System.Collections.Generic.IList<T> list)
		{
			System.Type c = typeof(T);
			T[] ta = (T[])java.lang.reflect.Array.newInstance(c, list.Count);
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
		/// <see cref="toArray{T}(java.lang.Class{T}, System.Collections.Generic.IList{E})"/>
		/// if the list may be empty.
		/// </exception>
		public static T[] toArray<T>(System.Collections.Generic.IList<T> list)
		{
			return toArray(getClass(list[0]), list);
		}
	}
}
